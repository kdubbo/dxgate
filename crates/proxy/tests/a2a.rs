use axum::body::Body;
use axum::http::{Request, Response, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::{get, post};
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
async fn a2a_requests_route_by_agent_name() {
    let planner = spawn_a2a_server("planner").await;
    let coder = spawn_a2a_server("coder").await;
    let (proxy, _state) = spawn_proxy(config(vec![
        ("planner", planner.addr, Some("planner")),
        ("coder", coder.addr, Some("coder")),
    ]))
    .await;

    for _ in 0..4 {
        let response = rpc(
            proxy.addr,
            json!({
                "jsonrpc": "2.0", "id": 1, "method": "message/send",
                "params": {
                    "agent": "coder",
                    "message": { "role": "user", "parts": [{ "kind": "text", "text": "hi" }] }
                }
            }),
        )
        .await;
        assert_eq!(response.0, StatusCode::OK);
        assert_eq!(response.1["backend"], "coder");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a2a_task_followups_stick_to_owning_backend() {
    let first = spawn_a2a_server("a2a-a").await;
    let second = spawn_a2a_server("a2a-b").await;
    let (proxy, state) = spawn_proxy(config(vec![
        ("a2a-a", first.addr, None),
        ("a2a-b", second.addr, None),
    ]))
    .await;

    let send = rpc(
        proxy.addr,
        json!({
            "jsonrpc": "2.0", "id": 1, "method": "message/send",
            "params": { "message": { "role": "user", "parts": [] } }
        }),
    )
    .await;
    assert_eq!(send.0, StatusCode::OK);
    let owner = send.1["backend"].as_str().unwrap().to_string();
    let task_id = send.1["result"]["id"].as_str().unwrap().to_string();

    // Round-robin would alternate backends; the binding must override it.
    for _ in 0..4 {
        let get = rpc(
            proxy.addr,
            json!({
                "jsonrpc": "2.0", "id": 2, "method": "tasks/get",
                "params": { "id": task_id }
            }),
        )
        .await;
        assert_eq!(get.0, StatusCode::OK);
        assert_eq!(get.1["backend"].as_str().unwrap(), owner);
    }

    let metrics = state.metrics();
    let methods = metrics
        .a2a_methods
        .iter()
        .map(|m| (m.backend.as_str(), m.method.as_str(), m.calls))
        .collect::<Vec<_>>();
    assert!(methods.contains(&(owner.as_str(), "tasks/get", 4)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a2a_agent_card_urls_are_rewritten_to_gateway() {
    let planner = spawn_a2a_server("planner").await;
    let coder = spawn_a2a_server("coder").await;
    let (proxy, _state) = spawn_proxy(config(vec![
        ("planner", planner.addr, Some("planner")),
        ("coder", coder.addr, Some("coder")),
    ]))
    .await;

    for _ in 0..3 {
        let uri: Uri = format!("http://{}/.well-known/agent-card.json", proxy.addr)
            .parse()
            .unwrap();
        let response = Client::new().get(uri).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = body::to_bytes(response.into_body()).await.unwrap();
        let card = serde_json::from_slice::<Value>(&bytes).unwrap();

        // Deterministic: always the first declared backend's card.
        assert_eq!(card["name"], "planner");
        // The backend's internal endpoint must not leak; paths survive.
        assert_eq!(
            card["url"].as_str().unwrap(),
            format!("http://{}/a2a", proxy.addr)
        );
        assert_eq!(
            card["additionalInterfaces"][0]["url"].as_str().unwrap(),
            format!("http://{}/a2a", proxy.addr)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a2a_stream_binds_task_and_gets_antibuffer_headers() {
    let first = spawn_a2a_server("a2a-a").await;
    let second = spawn_a2a_server("a2a-b").await;
    let (proxy, _state) = spawn_proxy(config(vec![
        ("a2a-a", first.addr, None),
        ("a2a-b", second.addr, None),
    ]))
    .await;

    let request = Request::builder()
        .method("POST")
        .uri(format!("http://{}/a2a", proxy.addr))
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .body(Body::from(
            json!({
                "jsonrpc": "2.0", "id": 1, "method": "message/stream",
                "params": { "message": { "role": "user", "parts": [] } }
            })
            .to_string(),
        ))
        .unwrap();
    let response = Client::new().request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-accel-buffering")
            .and_then(|value| value.to_str().ok()),
        Some("no")
    );
    let headers = response.headers().clone();
    let bytes = body::to_bytes(response.into_body()).await.unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .contains("text/event-stream"));
    let owner = if text.contains("stream-task-a2a-a") {
        "a2a-a"
    } else {
        assert!(text.contains("stream-task-a2a-b"), "unexpected SSE: {text}");
        "a2a-b"
    };
    let task_id = format!("stream-task-{owner}");

    // The task id sniffed from the stream pins follow-ups to the owner.
    for _ in 0..4 {
        let get = rpc(
            proxy.addr,
            json!({
                "jsonrpc": "2.0", "id": 2, "method": "tasks/get",
                "params": { "id": task_id }
            }),
        )
        .await;
        assert_eq!(get.1["backend"].as_str().unwrap(), owner);
    }
}

// An A2A backend that answers message/send with a task it owns, tasks/get
// with a backend marker, message/stream with an SSE task event, and serves
// an agent card pointing at its internal endpoint.
async fn spawn_a2a_server(name: &'static str) -> TestServer {
    let addr = unused_addr();
    let app = Router::new()
        .route(
            "/a2a",
            post(move |Json(request): Json<Value>| async move {
                let id = request["id"].clone();
                match request["method"].as_str().unwrap_or_default() {
                    "message/send" => Json(json!({
                        "jsonrpc": "2.0", "id": id,
                        "backend": name,
                        "result": {
                            "kind": "task",
                            "id": format!("task-{name}"),
                            "status": { "state": "submitted" }
                        }
                    }))
                    .into_response(),
                    "tasks/get" => Json(json!({
                        "jsonrpc": "2.0", "id": id,
                        "backend": name,
                        "result": {
                            "kind": "task",
                            "id": request["params"]["id"],
                            "status": { "state": "completed" }
                        }
                    }))
                    .into_response(),
                    "message/stream" => Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "text/event-stream")
                        .body(Body::from(format!(
                            "data: {{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"kind\":\"task\",\"id\":\"stream-task-{name}\",\"status\":{{\"state\":\"working\"}}}}}}\n\n"
                        )))
                        .unwrap()
                        .into_response(),
                    _ => Json(json!({
                        "jsonrpc": "2.0", "id": id,
                        "error": { "code": -32601, "message": "method not found" }
                    }))
                    .into_response(),
                }
            }),
        )
        .route(
            "/.well-known/agent-card.json",
            get(move || async move {
                Json(json!({
                    "name": name,
                    "url": format!("http://internal-{name}.svc:9999/a2a"),
                    "additionalInterfaces": [
                        { "url": format!("http://internal-{name}.svc:9999/a2a"), "transport": "JSONRPC" }
                    ],
                    "skills": []
                }))
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

fn config(backends: Vec<(&str, SocketAddr, Option<&str>)>) -> RuntimeConfig {
    RuntimeConfig {
        version: "a2a-test".into(),
        listeners: vec![],
        clusters: vec![],
        secrets: vec![],
        providers: vec![],
        backends: backends
            .iter()
            .map(|(name, addr, agent)| Backend {
                name: (*name).into(),
                kind: BackendKind::A2a {
                    endpoint: format!("http://{addr}"),
                    agent: agent.map(Into::into),
                },
                policies: vec![],
            })
            .collect(),
        routes: vec![
            AgentRoute {
                name: "a2a".into(),
                protocol: AgentProtocol::A2a,
                matches: vec![AgentRouteMatch {
                    path: PathMatch::Exact("/a2a".into()),
                    host: None,
                    method: Some("POST".into()),
                    model: None,
                    tool: None,
                    agent: None,
                    headers: vec![],
                }],
                weighted_backends: weighted(&backends),
                policies: vec![],
            },
            AgentRoute {
                name: "a2a-card".into(),
                protocol: AgentProtocol::A2a,
                matches: vec![AgentRouteMatch {
                    path: PathMatch::Exact("/.well-known/agent-card.json".into()),
                    host: None,
                    method: Some("GET".into()),
                    model: None,
                    tool: None,
                    agent: None,
                    headers: vec![],
                }],
                weighted_backends: weighted(&backends),
                policies: vec![],
            },
        ],
        policies: vec![],
    }
}

fn weighted(backends: &[(&str, SocketAddr, Option<&str>)]) -> Vec<WeightedBackend> {
    backends
        .iter()
        .map(|(name, _, _)| WeightedBackend {
            name: (*name).into(),
            weight: 100,
        })
        .collect()
}

async fn rpc(addr: SocketAddr, request: Value) -> (StatusCode, Value) {
    let uri: Uri = format!("http://{addr}/a2a").parse().unwrap();
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
