//! End-to-end tests that drive a real dxgate proxy (and admin server) over
//! loopback TCP against mock upstreams: route matching, policy enforcement,
//! body limits, and streaming pass-through.

use dxgate_admin::AdminServer;
use dxgate_core::{
    AgentProtocol, AgentRoute, AgentRouteMatch, AuthPolicy, Backend, BackendKind, Cluster,
    Endpoint, HeaderTransform, Listener, ListenerProtocol, PathMatch, Policy, PolicyAction,
    Provider, ProviderKind, RateLimitKey, RateLimitPolicy, Route, RuntimeConfig, VirtualHost,
    WeightedBackend, WeightedCluster,
};
use dxgate_proxy::{ProxyServer, ProxyState};
use hyper::body::HttpBody;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Client, Method, Request, Response, Server, StatusCode};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout, Duration};

const SSE_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// Mock upstream: /hello returns a body, /api/* echoes agent traffic, and
/// /v1/chat/completions streams SSE frames gated on `release` so tests can
/// prove the proxy forwards chunks before the upstream response completes.
async fn spawn_upstream(release: Arc<Notify>) -> SocketAddr {
    let make_svc = make_service_fn(move |_| {
        let release = release.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                let release = release.clone();
                async move {
                    let response = match (req.method(), req.uri().path()) {
                        (&Method::GET, "/hello") => Response::builder()
                            .status(StatusCode::OK)
                            .body(Body::from("hello from upstream"))
                            .unwrap(),
                        (&Method::POST, "/v1/chat/completions") => {
                            let (mut sender, body) = Body::channel();
                            tokio::spawn(async move {
                                sender
                                    .send_data("data: {\"delta\":\"one\"}\n\n".into())
                                    .await
                                    .ok();
                                timeout(SSE_RELEASE_TIMEOUT, release.notified()).await.ok();
                                sender.send_data("data: [DONE]\n\n".into()).await.ok();
                            });
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "text/event-stream")
                                .body(body)
                                .unwrap()
                        }
                        (_, path) if path.starts_with("/api/") => Response::builder()
                            .status(StatusCode::OK)
                            .body(Body::from("agent ok"))
                            .unwrap(),
                        (&Method::POST, "/search/mcp") => mcp_tools_response("search"),
                        (&Method::POST, "/calendar/mcp") => mcp_tools_response("calendar"),
                        _ => Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Body::empty())
                            .unwrap(),
                    };
                    Ok::<_, Infallible>(response)
                }
            }))
        }
    });
    let server = Server::bind(&"127.0.0.1:0".parse().unwrap()).serve(make_svc);
    let addr = server.local_addr();
    tokio::spawn(server);
    addr
}

fn mcp_tools_response(tool: &str) -> Response<Body> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": { "tools": [{ "name": tool }] }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn reserve_port() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

async fn wait_until_accepting(addr: SocketAddr) {
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("server at {addr} never started accepting connections");
}

async fn spawn_proxy(config: RuntimeConfig) -> (SocketAddr, ProxyState) {
    let state = ProxyState::new(RuntimeConfig::empty("bootstrap"));
    state.apply_config(config).await.expect("config accepted");
    let addr = reserve_port();
    tokio::spawn(ProxyServer::new(state.clone()).serve(addr));
    wait_until_accepting(addr).await;
    (addr, state)
}

fn base_config(upstream: SocketAddr) -> RuntimeConfig {
    RuntimeConfig {
        version: "e2e".into(),
        listeners: vec![Listener {
            name: "http".into(),
            bind: "0.0.0.0:80".parse().unwrap(),
            protocol: ListenerProtocol::Http,
            virtual_hosts: vec![VirtualHost {
                name: "wildcard".into(),
                domains: vec!["*".into()],
                routes: vec![Route {
                    name: "default".into(),
                    matches: vec![],
                    weighted_clusters: vec![WeightedCluster {
                        name: "upstream".into(),
                        weight: 100,
                    }],
                }],
            }],
            tls_secret: None,
        }],
        clusters: vec![Cluster {
            name: "upstream".into(),
            endpoints: vec![Endpoint {
                address: "127.0.0.1".into(),
                port: upstream.port(),
                healthy: true,
                node_name: None,
            }],
            http2: false,
            tls: None,
            circuit_breaker: None,
            outlier_detection: None,
        }],
        secrets: vec![],
        providers: vec![],
        backends: vec![],
        routes: vec![],
        policies: vec![],
    }
}

fn agent_config(upstream: SocketAddr) -> RuntimeConfig {
    let mut cfg = base_config(upstream);
    cfg.backends = vec![Backend {
        name: "api".into(),
        kind: BackendKind::Http {
            endpoint: format!("http://127.0.0.1:{}", upstream.port()),
        },
        policies: vec![],
    }];
    cfg.routes = vec![AgentRoute {
        name: "api".into(),
        protocol: AgentProtocol::Http,
        matches: vec![AgentRouteMatch {
            path: PathMatch::Prefix("/api/".into()),
            host: None,
            method: None,
            model: None,
            tool: None,
            agent: None,
            headers: vec![],
        }],
        weighted_backends: vec![WeightedBackend {
            name: "api".into(),
            weight: 100,
        }],
        policies: vec!["guard".into()],
    }];
    cfg.policies = vec![Policy {
        name: "guard".into(),
        action: PolicyAction::Allow,
        matches: None,
        auth: Some(AuthPolicy::ApiKey {
            header: "authorization".into(),
            values: vec!["Bearer e2e".into()],
            value_env: None,
        }),
        rate_limit: Some(RateLimitPolicy {
            requests: 2,
            window_seconds: 60,
            key: RateLimitKey::Route,
        }),
        timeout_ms: None,
        retry: None,
        max_body_bytes: None,
        request_headers: HeaderTransform::default(),
        response_headers: HeaderTransform::default(),
    }];
    cfg
}

fn llm_config(upstream: SocketAddr) -> RuntimeConfig {
    let mut cfg = base_config(upstream);
    cfg.providers = vec![Provider {
        name: "local".into(),
        kind: ProviderKind::OpenAiCompatible,
        base_url: format!("http://127.0.0.1:{}", upstream.port()),
        api_key_env: None,
        request_headers: vec![],
    }];
    cfg.backends = vec![Backend {
        name: "llm".into(),
        kind: BackendKind::Llm {
            provider: "local".into(),
            models: vec![],
            endpoint: None,
        },
        policies: vec![],
    }];
    cfg.routes = vec![AgentRoute {
        name: "chat".into(),
        protocol: AgentProtocol::Llm,
        matches: vec![AgentRouteMatch {
            path: PathMatch::Prefix("/v1/".into()),
            host: None,
            method: None,
            model: None,
            tool: None,
            agent: None,
            headers: vec![],
        }],
        weighted_backends: vec![WeightedBackend {
            name: "llm".into(),
            weight: 100,
        }],
        policies: vec![],
    }];
    cfg
}

fn mcp_config(upstream: SocketAddr) -> RuntimeConfig {
    let mut cfg = base_config(upstream);
    cfg.backends = vec![
        Backend {
            name: "tools-search".into(),
            kind: BackendKind::Mcp {
                endpoint: format!("http://127.0.0.1:{}/search", upstream.port()),
                tools: vec!["search".into()],
            },
            policies: vec![],
        },
        Backend {
            name: "tools-calendar".into(),
            kind: BackendKind::Mcp {
                endpoint: format!("http://127.0.0.1:{}/calendar", upstream.port()),
                tools: vec!["calendar".into()],
            },
            policies: vec![],
        },
    ];
    cfg.routes = vec![AgentRoute {
        name: "mcp".into(),
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
                name: "tools-search".into(),
                weight: 100,
            },
            WeightedBackend {
                name: "tools-calendar".into(),
                weight: 100,
            },
        ],
        policies: vec![],
    }];
    cfg
}

#[tokio::test]
async fn http_route_proxies_to_upstream_and_reports_admin_state() {
    let upstream = spawn_upstream(Arc::new(Notify::new())).await;
    let (proxy_addr, state) = spawn_proxy(base_config(upstream)).await;

    let admin_addr = reserve_port();
    tokio::spawn(AdminServer::new(state, proxy_addr).serve(admin_addr));
    wait_until_accepting(admin_addr).await;

    let client = Client::new();
    let response = client
        .get(format!("http://{proxy_addr}/hello").parse().unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = hyper::body::to_bytes(response.into_body()).await.unwrap();
    assert_eq!(&bytes[..], b"hello from upstream");

    let readyz = client
        .get(format!("http://{admin_addr}/readyz").parse().unwrap())
        .await
        .unwrap();
    assert_eq!(readyz.status(), StatusCode::OK);

    let metrics = client
        .get(format!("http://{admin_addr}/metrics").parse().unwrap())
        .await
        .unwrap();
    assert_eq!(metrics.status(), StatusCode::OK);
    let text = hyper::body::to_bytes(metrics.into_body()).await.unwrap();
    let text = String::from_utf8(text.to_vec()).unwrap();
    assert!(text.contains("dxgate_ready 1"));
    assert!(text.contains("dxgate_http_route_requests_total{"));
    assert!(text.contains("route=\"default\""));
}

#[tokio::test]
async fn agent_route_enforces_api_key_and_rate_limit() {
    let upstream = spawn_upstream(Arc::new(Notify::new())).await;
    let (proxy_addr, _state) = spawn_proxy(agent_config(upstream)).await;
    let client = Client::new();

    let request = |auth: Option<&'static str>| {
        let mut builder = Request::builder()
            .method(Method::GET)
            .uri(format!("http://{proxy_addr}/api/items"));
        if let Some(auth) = auth {
            builder = builder.header("authorization", auth);
        }
        builder.body(Body::empty()).unwrap()
    };

    let denied = client.request(request(None)).await.unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

    let wrong = client.request(request(Some("Bearer nope"))).await.unwrap();
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

    for _ in 0..2 {
        let allowed = client.request(request(Some("Bearer e2e"))).await.unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
        let bytes = hyper::body::to_bytes(allowed.into_body()).await.unwrap();
        assert_eq!(&bytes[..], b"agent ok");
    }

    let limited = client.request(request(Some("Bearer e2e"))).await.unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn oversized_request_body_is_rejected_before_buffering() {
    let upstream = spawn_upstream(Arc::new(Notify::new())).await;
    let (proxy_addr, _state) = spawn_proxy(agent_config(upstream)).await;
    let client = Client::new();

    // 11 MiB exceeds the 10 MiB default DXGATE_MAX_BODY_BYTES cap.
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{proxy_addr}/api/upload"))
        .header("authorization", "Bearer e2e")
        .body(Body::from(vec![0u8; 11 * 1024 * 1024]))
        .unwrap();

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn mcp_tools_list_federates_across_backends() {
    let upstream = spawn_upstream(Arc::new(Notify::new())).await;
    let (proxy_addr, _state) = spawn_proxy(mcp_config(upstream)).await;
    let client = Client::new();

    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{proxy_addr}/mcp"))
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"jsonrpc":"2.0","method":"tools/list","id":7}"#,
        ))
        .unwrap();

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["id"], 7);
    let mut names: Vec<&str> = value["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["calendar", "search"]);
}

#[tokio::test]
async fn llm_sse_response_streams_through_proxy_before_upstream_completes() {
    let release = Arc::new(Notify::new());
    let upstream = spawn_upstream(release.clone()).await;
    let (proxy_addr, _state) = spawn_proxy(llm_config(upstream)).await;
    let client = Client::new();

    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{proxy_addr}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"model":"any","stream":true}"#))
        .unwrap();

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    // The upstream holds the second frame until released, so receiving the
    // first frame here proves the proxy streams instead of buffering.
    let mut body = response.into_body();
    let first = timeout(Duration::from_secs(2), body.data())
        .await
        .expect("first SSE frame should arrive before upstream completes")
        .expect("body should not end before first frame")
        .unwrap();
    assert!(String::from_utf8_lossy(&first).contains("\"delta\":\"one\""));

    release.notify_one();
    let mut rest = Vec::new();
    while let Some(chunk) = body.data().await {
        rest.extend_from_slice(&chunk.unwrap());
    }
    assert!(String::from_utf8_lossy(&rest).contains("[DONE]"));
}
