use axum::body::Body;
use axum::http::{Request, StatusCode, Uri};
use axum::routing::post;
use axum::{Json, Router};
use dxgate_core::{
    AgentProtocol, AgentRoute, AgentRouteMatch, Backend, BackendKind, PathMatch, RuntimeConfig,
    WeightedBackend,
};
use dxgate_proxy::{ProxyServer, ProxyState};
use hyper::{body, Client};
use serde_json::{json, Value};
use std::net::{SocketAddr, TcpListener};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio::time::sleep;

struct TestServer {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_prompts_and_resources_lists_federate() {
    let first = spawn_mcp_server("mcp-a").await;
    let second = spawn_mcp_server("mcp-b").await;
    let (proxy, _state) = spawn_proxy(two_backend_config(first.addr, second.addr)).await;

    let prompts = rpc(
        proxy.addr,
        json!({ "jsonrpc": "2.0", "id": 5, "method": "prompts/list" }),
    )
    .await;
    assert_eq!(prompts.0, StatusCode::OK);
    assert_eq!(prompts.1["id"], 5);
    let names = prompts.1["result"]["prompts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|prompt| prompt["name"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    // "shared-prompt" collides and is re-exposed under the second backend's alias.
    assert_eq!(
        names,
        [
            "shared-prompt",
            "prompt-mcp-a",
            "mcp-b__shared-prompt",
            "prompt-mcp-b"
        ]
    );

    let resources = rpc(
        proxy.addr,
        json!({ "jsonrpc": "2.0", "id": 6, "method": "resources/list" }),
    )
    .await;
    assert_eq!(resources.0, StatusCode::OK);
    let uris = resources.1["result"]["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|resource| resource["uri"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    // The shared URI is globally addressable and deduplicated, not aliased.
    assert_eq!(uris, ["file:///shared", "file:///mcp-a", "file:///mcp-b"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_tools_list_drains_backend_pagination() {
    let paged = spawn_paged_tools_server().await;
    let plain = spawn_mcp_server("mcp-b").await;
    let (proxy, _state) =
        spawn_proxy(config(vec![("paged", paged.addr), ("mcp-b", plain.addr)])).await;

    let response = rpc(
        proxy.addr,
        json!({ "jsonrpc": "2.0", "id": 7, "method": "tools/list" }),
    )
    .await;
    assert_eq!(response.0, StatusCode::OK);
    let names = response.1["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(names, ["page-one", "page-two", "shared-tool", "tool-mcp-b"]);
    // The federated aggregate never exposes a cursor.
    assert!(response.1["result"].get("nextCursor").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_tool_collision_aliases_route_to_owning_backend() {
    let first = spawn_mcp_server("mcp-a").await;
    let second = spawn_mcp_server("mcp-b").await;
    let (proxy, state) = spawn_proxy(two_backend_config(first.addr, second.addr)).await;

    let list = rpc(
        proxy.addr,
        json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
    )
    .await;
    let names = list.1["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "shared-tool",
            "tool-mcp-a",
            "mcp-b__shared-tool",
            "tool-mcp-b"
        ]
    );

    // The bare name routes to the first backend that owns it.
    let bare = rpc(
        proxy.addr,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": { "name": "shared-tool" }
        }),
    )
    .await;
    assert_eq!(bare.0, StatusCode::OK);
    assert_eq!(bare.1["backend"], "mcp-a");
    assert_eq!(bare.1["tool"], "shared-tool");

    // The alias pins the second backend and restores the upstream name.
    let aliased = rpc(
        proxy.addr,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "mcp-b__shared-tool" }
        }),
    )
    .await;
    assert_eq!(aliased.0, StatusCode::OK);
    assert_eq!(aliased.1["backend"], "mcp-b");
    assert_eq!(aliased.1["tool"], "shared-tool");

    let metrics = state.metrics();
    let tools = metrics
        .mcp_tools
        .iter()
        .map(|tool| (tool.backend.as_str(), tool.tool.as_str(), tool.calls))
        .collect::<Vec<_>>();
    assert!(tools.contains(&("mcp-a", "shared-tool", 1)));
    assert!(tools.contains(&("mcp-b", "shared-tool", 1)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_initialize_advertises_federated_capabilities() {
    let first = spawn_mcp_server("mcp-a").await;
    let second = spawn_mcp_server("mcp-b").await;
    let (proxy, _state) = spawn_proxy(two_backend_config(first.addr, second.addr)).await;

    let response = rpc(
        proxy.addr,
        json!({
            "jsonrpc": "2.0", "id": 9, "method": "initialize",
            "params": { "protocolVersion": "2025-06-18" }
        }),
    )
    .await;
    assert_eq!(response.0, StatusCode::OK);
    let capabilities = &response.1["result"]["capabilities"];
    // The backend only advertises tools; the gateway federates the rest.
    assert_eq!(capabilities["tools"]["listChanged"], false);
    assert!(capabilities["resources"].is_object());
    assert!(capabilities["prompts"].is_object());
    // initialize itself is load-balanced to a single backend.
    let server_name = response.1["result"]["serverInfo"]["name"].as_str().unwrap();
    assert!(server_name == "mcp-a" || server_name == "mcp-b");
}

// A backend exposing one shared and one uniquely named tool/prompt/resource,
// echoing tools/call with its own name.
async fn spawn_mcp_server(name: &'static str) -> TestServer {
    let addr = unused_addr();
    let app = Router::new().route(
        "/mcp",
        post(move |Json(request): Json<Value>| async move {
            let method = request["method"].as_str().unwrap_or_default();
            let id = request["id"].clone();
            let reply = match method {
                "initialize" => json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": { "tools": { "listChanged": false } },
                        "serverInfo": { "name": name }
                    }
                }),
                "tools/list" => json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": { "tools": [
                        { "name": "shared-tool", "description": name },
                        { "name": format!("tool-{name}") }
                    ] }
                }),
                "prompts/list" => json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": { "prompts": [
                        { "name": "shared-prompt", "description": name },
                        { "name": format!("prompt-{name}") }
                    ] }
                }),
                "resources/list" => json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": { "resources": [
                        { "uri": "file:///shared", "name": "shared" },
                        { "uri": format!("file:///{name}"), "name": name }
                    ] }
                }),
                "tools/call" => json!({
                    "jsonrpc": "2.0", "id": id,
                    "backend": name,
                    "tool": request["params"]["name"],
                    "result": { "content": [] }
                }),
                _ => json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": { "code": -32601, "message": "method not found" }
                }),
            };
            Json(reply)
        }),
    );
    spawn_app(addr, app).await
}

// tools/list responds in two pages linked by a cursor.
async fn spawn_paged_tools_server() -> TestServer {
    let addr = unused_addr();
    let app = Router::new().route(
        "/mcp",
        post(|Json(request): Json<Value>| async move {
            let id = request["id"].clone();
            let reply = match request["params"]["cursor"].as_str() {
                None => json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": {
                        "tools": [{ "name": "page-one" }],
                        "nextCursor": "cursor-2"
                    }
                }),
                Some("cursor-2") => json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": { "tools": [{ "name": "page-two" }] }
                }),
                Some(other) => json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": { "code": -32602, "message": format!("unknown cursor {other}") }
                }),
            };
            Json(reply)
        }),
    );
    spawn_app(addr, app).await
}

async fn spawn_app(addr: SocketAddr, app: Router) -> TestServer {
    let task = tokio::spawn(async move {
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });
    wait_until_serving(addr).await;
    TestServer { addr, task }
}

async fn spawn_proxy(cfg: RuntimeConfig) -> (TestServer, ProxyState) {
    let addr = unused_addr();
    let state = ProxyState::new(RuntimeConfig::empty("bootstrap"));
    state.apply_config(cfg).await.unwrap();
    let server_state = state.clone();
    let task = tokio::spawn(async move {
        ProxyServer::new(server_state).serve(addr).await.unwrap();
    });
    wait_until_serving(addr).await;
    (TestServer { addr, task }, state)
}

fn two_backend_config(first: SocketAddr, second: SocketAddr) -> RuntimeConfig {
    config(vec![("mcp-a", first), ("mcp-b", second)])
}

fn config(backends: Vec<(&str, SocketAddr)>) -> RuntimeConfig {
    RuntimeConfig {
        version: "mcp-test".into(),
        listeners: vec![],
        clusters: vec![],
        secrets: vec![],
        providers: vec![],
        backends: backends
            .iter()
            .map(|(name, addr)| Backend {
                name: (*name).into(),
                kind: BackendKind::Mcp {
                    endpoint: format!("http://{addr}"),
                    tools: vec![],
                },
                policies: vec![],
            })
            .collect(),
        routes: vec![AgentRoute {
            name: "mcp".into(),
            protocol: AgentProtocol::Mcp,
            matches: vec![AgentRouteMatch {
                path: PathMatch::Exact("/mcp".into()),
                host: None,
                method: Some("POST".into()),
                model: None,
                tool: None,
                agent: None,
                headers: vec![],
            }],
            weighted_backends: backends
                .iter()
                .map(|(name, _)| WeightedBackend {
                    name: (*name).into(),
                    weight: 100,
                })
                .collect(),
            policies: vec![],
        }],
        policies: vec![],
    }
}

async fn rpc(addr: SocketAddr, request: Value) -> (StatusCode, Value) {
    let uri: Uri = format!("http://{addr}/mcp").parse().unwrap();
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(request.to_string()))
        .unwrap();
    let response = Client::new().request(request).await.unwrap();
    let status = response.status();
    let bytes = body::to_bytes(response.into_body()).await.unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    (status, value)
}

async fn wait_until_serving(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let uri: Uri = format!("http://{addr}/not-found").parse().unwrap();
        if Client::new().get(uri).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "server {addr} did not become ready"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

fn unused_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}
