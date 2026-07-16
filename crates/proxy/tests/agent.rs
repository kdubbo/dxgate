use axum::body::Body;
use axum::http::{HeaderMap, Request, Response, StatusCode, Uri};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use dxgate_core::{
    AgentProtocol, AgentRoute, AgentRouteMatch, AuthPolicy, Backend, BackendKind, HeaderTransform,
    PathMatch, Policy, PolicyAction, Provider, ProviderKind, RateLimitKey, RateLimitPolicy,
    RuntimeConfig, WeightedBackend,
};
use dxgate_proxy::{ProxyServer, ProxyState};
use hyper::body;
use hyper::Client;
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
async fn llm_route_enforces_api_key_and_forwards_to_provider() {
    std::env::set_var("DXGATE_TEST_OPENAI_KEY", "provider-key");
    let llm = spawn_llm_backend().await;
    let proxy = spawn_proxy(agent_config(
        vec![llm_backend(llm.addr)],
        vec![llm_route()],
        vec![Policy {
            name: "caller-auth".into(),
            action: PolicyAction::Allow,
            matches: None,
            auth: Some(AuthPolicy::ApiKey {
                header: "authorization".into(),
                values: vec!["Bearer client-key".into()],
                value_env: None,
            }),
            rate_limit: None,
            token_limit: None,
            timeout_ms: Some(1000),
            retry: None,
            max_body_bytes: None,
            request_headers: HeaderTransform::default(),
            response_headers: HeaderTransform::default(),
        }],
    ))
    .await;

    let unauthorized = post_json(
        proxy.addr,
        "/v1/chat/completions",
        json!({
            "model": "gpt-test",
            "messages": []
        }),
        None,
    )
    .await;
    assert_eq!(unauthorized.0, StatusCode::UNAUTHORIZED);

    let ok = post_json(
        proxy.addr,
        "/v1/chat/completions",
        json!({
            "model": "gpt-test",
            "messages": []
        }),
        Some("Bearer client-key"),
    )
    .await;
    assert_eq!(ok.0, StatusCode::OK);
    assert_eq!(ok.1["provider_authorization"], "Bearer provider-key");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_tools_list_federates_multiple_backends() {
    let first = spawn_mcp_backend("search").await;
    let second = spawn_mcp_backend("calendar").await;
    let proxy = spawn_proxy(agent_config(
        vec![
            mcp_backend("mcp-a", first.addr),
            mcp_backend("mcp-b", second.addr),
        ],
        vec![AgentRoute {
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
            weighted_backends: vec![
                WeightedBackend {
                    name: "mcp-a".into(),
                    weight: 100,
                },
                WeightedBackend {
                    name: "mcp-b".into(),
                    weight: 100,
                },
            ],
            policies: vec![],
        }],
        vec![],
    ))
    .await;

    let response = post_json(
        proxy.addr,
        "/mcp",
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list"
        }),
        None,
    )
    .await;

    assert_eq!(response.0, StatusCode::OK);
    let tools = response.1["result"]["tools"].as_array().unwrap();
    let names = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, ["search", "calendar"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_sse_session_binds_followup_requests_to_same_backend() {
    let first = spawn_mcp_session_backend("mcp-a", "session-a").await;
    let second = spawn_mcp_session_backend("mcp-b", "session-b").await;
    let proxy = spawn_proxy(agent_config(
        vec![
            mcp_backend("mcp-a", first.addr),
            mcp_backend("mcp-b", second.addr),
        ],
        vec![AgentRoute {
            name: "mcp-stream".into(),
            protocol: AgentProtocol::Mcp,
            matches: vec![AgentRouteMatch {
                path: PathMatch::Exact("/mcp".into()),
                host: None,
                method: None,
                model: None,
                tool: None,
                agent: None,
                headers: vec![],
            }],
            weighted_backends: vec![
                WeightedBackend {
                    name: "mcp-a".into(),
                    weight: 100,
                },
                WeightedBackend {
                    name: "mcp-b".into(),
                    weight: 100,
                },
            ],
            policies: vec![],
        }],
        vec![],
    ))
    .await;

    let stream = request_text(
        Request::builder()
            .method("GET")
            .uri(format!("http://{}/mcp", proxy.addr))
            .header("accept", "text/event-stream")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(stream.0, StatusCode::OK);
    assert_eq!(
        stream
            .2
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok()),
        Some("session-a")
    );
    assert_eq!(
        stream
            .2
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-cache, no-transform")
    );
    assert!(stream.1.contains("event: message"));

    let followup = post_json_with_headers(
        proxy.addr,
        "/mcp",
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": { "name": "search" }
        }),
        &[("mcp-session-id", "session-a")],
    )
    .await;

    assert_eq!(followup.0, StatusCode::OK);
    assert_eq!(followup.1["backend"], "mcp-a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a2a_agent_card_is_proxied() {
    let a2a = spawn_a2a_backend().await;
    let proxy = spawn_proxy(agent_config(
        vec![Backend {
            name: "agent".into(),
            kind: BackendKind::A2a {
                endpoint: format!("http://{}", a2a.addr),
                agent: Some("planner".into()),
            },
            policies: vec![],
        }],
        vec![AgentRoute {
            name: "agent-card".into(),
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
            weighted_backends: vec![WeightedBackend {
                name: "agent".into(),
                weight: 100,
            }],
            policies: vec![],
        }],
        vec![],
    ))
    .await;

    let response = get_json(proxy.addr, "/.well-known/agent-card.json").await;

    assert_eq!(response.0, StatusCode::OK);
    assert_eq!(response.1["name"], "planner");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_route_applies_local_rate_limit() {
    let llm = spawn_llm_backend().await;
    let proxy = spawn_proxy(agent_config(
        vec![llm_backend(llm.addr)],
        vec![AgentRoute {
            policies: vec!["one-shot".into()],
            ..llm_route()
        }],
        vec![Policy {
            name: "one-shot".into(),
            action: PolicyAction::Allow,
            matches: None,
            auth: None,
            rate_limit: Some(RateLimitPolicy {
                requests: 1,
                window_seconds: 60,
                key: RateLimitKey::Route,
            }),
            token_limit: None,
            timeout_ms: None,
            retry: None,
            max_body_bytes: None,
            request_headers: HeaderTransform::default(),
            response_headers: HeaderTransform::default(),
        }],
    ))
    .await;

    let first = post_json(
        proxy.addr,
        "/v1/chat/completions",
        json!({ "model": "gpt-test", "messages": [] }),
        None,
    )
    .await;
    let second = post_json(
        proxy.addr,
        "/v1/chat/completions",
        json!({ "model": "gpt-test", "messages": [] }),
        None,
    )
    .await;

    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::TOO_MANY_REQUESTS);
}

async fn spawn_proxy(cfg: RuntimeConfig) -> TestServer {
    let addr = unused_addr();
    let state = ProxyState::new(RuntimeConfig::empty("bootstrap"));
    state.apply_config(cfg).await.unwrap();
    let task = tokio::spawn(async move {
        ProxyServer::new(state).serve(addr).await.unwrap();
    });
    wait_until_ready(addr).await;
    TestServer { addr, task }
}

async fn spawn_llm_backend() -> TestServer {
    let addr = unused_addr();
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|headers: HeaderMap, Json(body): Json<Value>| async move {
            Json(json!({
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "model": body["model"],
                "provider_authorization": headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("")
            }))
        }),
    );
    let server = spawn_app(addr, app);
    wait_until_serving(server.addr).await;
    server
}

async fn spawn_mcp_backend(tool_name: &'static str) -> TestServer {
    let addr = unused_addr();
    let app = Router::new().route(
        "/mcp",
        post(move || async move {
            Json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "tools": [{
                        "name": tool_name,
                        "description": format!("{tool_name} tool")
                    }]
                }
            }))
        }),
    );
    let server = spawn_app(addr, app);
    wait_until_serving(server.addr).await;
    server
}

async fn spawn_mcp_session_backend(
    backend_name: &'static str,
    session_id: &'static str,
) -> TestServer {
    let addr = unused_addr();
    let app = Router::new().route(
        "/mcp",
        get(move || async move {
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .header("mcp-session-id", session_id)
                .body(Body::from(format!(
                    "event: message\ndata: {{\"backend\":\"{backend_name}\"}}\n\n"
                )))
                .unwrap()
        })
        .post(move || async move {
            Json(json!({
                "jsonrpc": "2.0",
                "id": 2,
                "backend": backend_name,
                "result": { "ok": true }
            }))
        }),
    );
    let server = spawn_app(addr, app);
    wait_until_serving(server.addr).await;
    server
}

async fn spawn_a2a_backend() -> TestServer {
    let addr = unused_addr();
    let app = Router::new().route(
        "/.well-known/agent-card.json",
        any(|| async { Json(json!({ "name": "planner", "version": "v1" })) }),
    );
    let server = spawn_app(addr, app);
    wait_until_serving(server.addr).await;
    server
}

fn spawn_app(addr: SocketAddr, app: Router) -> TestServer {
    let task = tokio::spawn(async move {
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });
    TestServer { addr, task }
}

fn agent_config(
    backends: Vec<Backend>,
    routes: Vec<AgentRoute>,
    policies: Vec<Policy>,
) -> RuntimeConfig {
    RuntimeConfig {
        version: "agent-test".into(),
        listeners: vec![],
        clusters: vec![],
        secrets: vec![],
        providers: vec![Provider {
            name: "openai".into(),
            kind: ProviderKind::OpenAiCompatible,
            base_url: "http://unused".into(),
            api_key_env: Some("DXGATE_TEST_OPENAI_KEY".into()),
            request_headers: vec![],
        }],
        backends,
        routes,
        policies,
    }
}

fn llm_backend(addr: SocketAddr) -> Backend {
    Backend {
        name: "gpt".into(),
        kind: BackendKind::Llm {
            provider: "openai".into(),
            models: vec!["gpt-test".into()],
            endpoint: Some(format!("http://{addr}")),
            model_rewrites: Default::default(),
        },
        policies: vec![],
    }
}

fn mcp_backend(name: &str, addr: SocketAddr) -> Backend {
    Backend {
        name: name.into(),
        kind: BackendKind::Mcp {
            endpoint: format!("http://{addr}"),
            tools: vec![],
        },
        policies: vec![],
    }
}

fn llm_route() -> AgentRoute {
    AgentRoute {
        name: "chat".into(),
        protocol: AgentProtocol::Llm,
        matches: vec![AgentRouteMatch {
            path: PathMatch::Exact("/v1/chat/completions".into()),
            host: None,
            method: Some("POST".into()),
            model: Some("gpt-test".into()),
            tool: None,
            agent: None,
            headers: vec![],
        }],
        weighted_backends: vec![WeightedBackend {
            name: "gpt".into(),
            weight: 100,
        }],
        policies: vec!["caller-auth".into()],
    }
}

async fn post_json(
    addr: SocketAddr,
    path: &str,
    value: Value,
    authorization: Option<&str>,
) -> (StatusCode, Value) {
    let uri: Uri = format!("http://{addr}{path}").parse().unwrap();
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(authorization) = authorization {
        builder = builder.header("authorization", authorization);
    }
    let request = builder.body(Body::from(value.to_string())).unwrap();
    read_json(Client::new().request(request).await.unwrap()).await
}

async fn post_json_with_headers(
    addr: SocketAddr,
    path: &str,
    value: Value,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let uri: Uri = format!("http://{addr}{path}").parse().unwrap();
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let request = builder.body(Body::from(value.to_string())).unwrap();
    read_json(Client::new().request(request).await.unwrap()).await
}

async fn request_text(request: Request<Body>) -> (StatusCode, String, HeaderMap) {
    let response = Client::new().request(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = body::to_bytes(response.into_body()).await.unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap(), headers)
}

async fn get_json(addr: SocketAddr, path: &str) -> (StatusCode, Value) {
    let uri: Uri = format!("http://{addr}{path}").parse().unwrap();
    read_json(Client::new().get(uri).await.unwrap()).await
}

async fn read_json(response: hyper::Response<Body>) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = body::to_bytes(response.into_body()).await.unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    (status, value)
}

async fn wait_until_ready(addr: SocketAddr) {
    wait_until_serving(addr).await;
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
