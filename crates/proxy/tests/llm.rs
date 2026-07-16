use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode, Uri};
use axum::routing::{get, post};
use axum::{Json, Router};
use dxgate_core::{
    AgentProtocol, AgentRoute, AgentRouteMatch, Backend, BackendKind, PathMatch, Policy,
    PolicyAction, Provider, ProviderKind, RateLimitKey, RuntimeConfig, TokenLimitPolicy,
    WeightedBackend,
};
use dxgate_proxy::{ProxyServer, ProxyState};
use hyper::body;
use hyper::Client;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
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

struct TestProxy {
    addr: SocketAddr,
    state: ProxyState,
    task: JoinHandle<()>,
}

impl Drop for TestProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anthropic_backend_translates_chat_completions() {
    std::env::set_var("DXGATE_TEST_ANTHROPIC_KEY", "anthro-key");
    let upstream = spawn_anthropic_backend().await;
    let proxy = spawn_proxy(llm_config(
        provider("anthropic", ProviderKind::Anthropic, upstream.addr, ""),
        vec![llm_backend("claude", "anthropic", vec![], BTreeMap::new())],
        vec![],
    ))
    .await;

    let (status, response) = post_json(
        proxy.addr,
        "/v1/chat/completions",
        json!({
            "model": "claude-3",
            "messages": [
                { "role": "system", "content": "be brief" },
                { "role": "user", "content": "hi" }
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["object"], "chat.completion");
    assert_eq!(
        response["choices"][0]["message"]["content"],
        "hello from anthropic"
    );
    assert_eq!(response["choices"][0]["finish_reason"], "stop");
    assert_eq!(response["usage"]["prompt_tokens"], 11);
    assert_eq!(response["usage"]["completion_tokens"], 7);

    let usage = &proxy.state.metrics().llm_usage;
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].backend, "claude");
    assert_eq!(usage[0].model, "claude-3");
    assert_eq!(usage[0].prompt_tokens, 11);
    assert_eq!(usage[0].completion_tokens, 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anthropic_backend_streams_openai_chunks() {
    std::env::set_var("DXGATE_TEST_ANTHROPIC_STREAM_KEY", "anthro-key");
    let upstream = spawn_anthropic_backend().await;
    let mut provider = provider("anthropic", ProviderKind::Anthropic, upstream.addr, "");
    provider.api_key_env = Some("DXGATE_TEST_ANTHROPIC_STREAM_KEY".into());
    let proxy = spawn_proxy(llm_config(
        provider,
        vec![llm_backend("claude", "anthropic", vec![], BTreeMap::new())],
        vec![],
    ))
    .await;

    let (status, headers, text) = post_json_raw(
        proxy.addr,
        "/v1/chat/completions",
        json!({
            "model": "claude-3",
            "messages": [{ "role": "user", "content": "hi" }],
            "stream": true
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .contains("text/event-stream"));
    assert!(text.contains("chat.completion.chunk"));
    assert!(text.contains("\"content\":\"Hel\""));
    assert!(text.contains("\"content\":\"lo\""));
    assert!(text.contains("\"finish_reason\":\"stop\""));
    assert!(text.trim_end().ends_with("data: [DONE]"));

    let usage = &proxy.state.metrics().llm_usage;
    assert_eq!(usage[0].prompt_tokens, 9);
    assert_eq!(usage[0].completion_tokens, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gemini_backend_translates_chat_completions() {
    std::env::set_var("DXGATE_TEST_GEMINI_KEY", "gemini-key");
    let upstream = spawn_gemini_backend().await;
    let proxy = spawn_proxy(llm_config(
        provider("gemini", ProviderKind::Gemini, upstream.addr, "/v1beta"),
        vec![llm_backend("gemini", "gemini", vec![], BTreeMap::new())],
        vec![],
    ))
    .await;

    let (status, response) = post_json(
        proxy.addr,
        "/v1/chat/completions",
        json!({
            "model": "gemini-pro",
            "messages": [{ "role": "user", "content": "hi" }]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        response["choices"][0]["message"]["content"],
        "hello from gemini"
    );
    assert_eq!(response["model"], "gemini-pro");
    assert_eq!(response["usage"]["total_tokens"], 8);

    let usage = &proxy.state.metrics().llm_usage;
    assert_eq!(usage[0].prompt_tokens, 5);
    assert_eq!(usage[0].completion_tokens, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gemini_backend_streams_openai_chunks() {
    std::env::set_var("DXGATE_TEST_GEMINI_STREAM_KEY", "gemini-key");
    let upstream = spawn_gemini_backend().await;
    let mut provider = provider("gemini", ProviderKind::Gemini, upstream.addr, "/v1beta");
    provider.api_key_env = Some("DXGATE_TEST_GEMINI_STREAM_KEY".into());
    let proxy = spawn_proxy(llm_config(
        provider,
        vec![llm_backend("gemini", "gemini", vec![], BTreeMap::new())],
        vec![],
    ))
    .await;

    let (status, _, text) = post_json_raw(
        proxy.addr,
        "/v1/chat/completions",
        json!({
            "model": "gemini-pro",
            "messages": [{ "role": "user", "content": "hi" }],
            "stream": true
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(text.contains("\"content\":\"Hi \""));
    assert!(text.contains("\"content\":\"there\""));
    assert!(text.contains("\"finish_reason\":\"stop\""));
    assert!(text.trim_end().ends_with("data: [DONE]"));

    let usage = &proxy.state.metrics().llm_usage;
    assert_eq!(usage[0].prompt_tokens, 6);
    assert_eq!(usage[0].completion_tokens, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deepseek_kind_authenticates_with_bearer_and_records_usage() {
    std::env::set_var("DXGATE_TEST_DEEPSEEK_KEY", "sk-deepseek");
    let upstream = spawn_openai_backend().await;
    let mut provider = provider("deepseek", ProviderKind::DeepSeek, upstream.addr, "/v1");
    provider.api_key_env = Some("DXGATE_TEST_DEEPSEEK_KEY".into());
    let proxy = spawn_proxy(llm_config(
        provider,
        vec![llm_backend("deepseek", "deepseek", vec![], BTreeMap::new())],
        vec![],
    ))
    .await;

    let (status, response) = post_json(
        proxy.addr,
        "/v1/chat/completions",
        json!({
            "model": "deepseek-chat",
            "messages": [{ "role": "user", "content": "hi" }]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["provider_authorization"], "Bearer sk-deepseek");
    assert_eq!(response["received_model"], "deepseek-chat");

    let usage = &proxy.state.metrics().llm_usage;
    assert_eq!(usage[0].prompt_tokens, 13);
    assert_eq!(usage[0].completion_tokens, 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn model_rewrite_renames_model_before_forwarding() {
    std::env::set_var("DXGATE_TEST_OPENAI_REWRITE_KEY", "sk-openai");
    let upstream = spawn_openai_backend().await;
    let mut provider = provider("openai", ProviderKind::OpenAi, upstream.addr, "/v1");
    provider.api_key_env = Some("DXGATE_TEST_OPENAI_REWRITE_KEY".into());
    let rewrites = BTreeMap::from([("gpt-4".to_string(), "my-deployment".to_string())]);
    let proxy = spawn_proxy(llm_config(
        provider,
        vec![llm_backend(
            "azure-gpt",
            "openai",
            vec!["gpt-4".into()],
            rewrites,
        )],
        vec![],
    ))
    .await;

    let (status, response) = post_json(
        proxy.addr,
        "/v1/chat/completions",
        json!({
            "model": "gpt-4",
            "messages": [{ "role": "user", "content": "hi" }]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["received_model"], "my-deployment");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embeddings_and_model_listing_route_as_llm() {
    let upstream = spawn_openai_backend().await;
    let proxy = spawn_proxy(llm_config(
        provider(
            "openai",
            ProviderKind::OpenAiCompatible,
            upstream.addr,
            "/v1",
        ),
        vec![llm_backend(
            "embedder",
            "openai",
            vec!["text-embed".into()],
            BTreeMap::new(),
        )],
        vec![],
    ))
    .await;

    let (status, response) = post_json(
        proxy.addr,
        "/v1/embeddings",
        json!({ "model": "text-embed", "input": "hello" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["object"], "list");
    assert_eq!(response["received_model"], "text-embed");

    let uri: Uri = format!("http://{}/v1/models", proxy.addr).parse().unwrap();
    let listing = Client::new().get(uri).await.unwrap();
    assert_eq!(listing.status(), StatusCode::OK);
    let bytes = body::to_bytes(listing.into_body()).await.unwrap();
    let value = serde_json::from_slice::<Value>(&bytes).unwrap();
    assert_eq!(value["data"][0]["id"], "text-embed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_limit_policy_denies_after_budget_is_spent() {
    let upstream = spawn_openai_backend().await;
    let mut cfg = llm_config(
        provider(
            "openai",
            ProviderKind::OpenAiCompatible,
            upstream.addr,
            "/v1",
        ),
        vec![llm_backend("gpt", "openai", vec![], BTreeMap::new())],
        vec![Policy {
            name: "budget".into(),
            action: PolicyAction::Allow,
            matches: None,
            auth: None,
            rate_limit: None,
            token_limit: Some(TokenLimitPolicy {
                tokens: 10,
                window_seconds: 60,
                key: RateLimitKey::Route,
            }),
            timeout_ms: None,
            retry: None,
            max_body_bytes: None,
            request_headers: Default::default(),
            response_headers: Default::default(),
        }],
    );
    cfg.routes[0].policies = vec!["budget".into()];
    let proxy = spawn_proxy(cfg).await;

    let request = json!({
        "model": "gpt-test",
        "messages": [{ "role": "user", "content": "hi" }]
    });
    // The upstream reports 19 tokens of usage, exhausting the 10-token budget.
    let (first, _) = post_json(proxy.addr, "/v1/chat/completions", request.clone()).await;
    assert_eq!(first, StatusCode::OK);

    // Usage is charged when the response body is consumed; wait for it to land.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if !proxy.state.metrics().llm_usage.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "usage was never recorded");
        sleep(Duration::from_millis(10)).await;
    }

    let (second, error) = post_json(proxy.addr, "/v1/chat/completions", request).await;
    assert_eq!(second, StatusCode::TOO_MANY_REQUESTS, "{error}");
}

// ---------------------------------------------------------------------------
// mock upstreams
// ---------------------------------------------------------------------------

async fn spawn_anthropic_backend() -> TestServer {
    let app = Router::new().route(
        "/v1/messages",
        post(|headers: HeaderMap, Json(request): Json<Value>| async move {
            if headers.get("x-api-key").and_then(|v| v.to_str().ok()) != Some("anthro-key") {
                return error_response(StatusCode::UNAUTHORIZED, "missing x-api-key");
            }
            if headers.get("anthropic-version").is_none() {
                return error_response(StatusCode::BAD_REQUEST, "missing anthropic-version");
            }
            if headers.get("authorization").is_some() {
                return error_response(StatusCode::BAD_REQUEST, "caller authorization leaked");
            }
            if request.get("max_tokens").and_then(Value::as_u64).is_none() {
                return error_response(StatusCode::BAD_REQUEST, "missing max_tokens");
            }
            if request.get("stream").and_then(Value::as_bool) == Some(true) {
                let sse = concat!(
                    "event: message_start\n",
                    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-3\",\"usage\":{\"input_tokens\":9}}}\n\n",
                    "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hel\"}}\n\n",
                    "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\n",
                    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}\n\n",
                    "data: {\"type\":\"message_stop\"}\n\n",
                );
                return axum::http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(sse))
                    .unwrap();
            }
            json_response(json!({
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "model": "claude-3",
                "content": [{ "type": "text", "text": "hello from anthropic" }],
                "stop_reason": "end_turn",
                "usage": { "input_tokens": 11, "output_tokens": 7 }
            }))
        }),
    );
    spawn_app(app).await
}

async fn spawn_gemini_backend() -> TestServer {
    let app = Router::new()
        .route(
            "/v1beta/models/:model_action",
            post(
                |axum::extract::Path(model_action): axum::extract::Path<String>,
                 axum::extract::RawQuery(query): axum::extract::RawQuery,
                 headers: HeaderMap,
                 Json(request): Json<Value>| async move {
                    if headers.get("x-goog-api-key").and_then(|v| v.to_str().ok())
                        != Some("gemini-key")
                    {
                        return error_response(StatusCode::UNAUTHORIZED, "missing x-goog-api-key");
                    }
                    if request.get("contents").and_then(Value::as_array).is_none() {
                        return error_response(StatusCode::BAD_REQUEST, "missing contents");
                    }
                    match model_action.as_str() {
                        "gemini-pro:generateContent" => json_response(json!({
                            "candidates": [{
                                "content": { "parts": [{ "text": "hello from gemini" }], "role": "model" },
                                "finishReason": "STOP"
                            }],
                            "usageMetadata": { "promptTokenCount": 5, "candidatesTokenCount": 3 }
                        })),
                        "gemini-pro:streamGenerateContent" => {
                            if query.as_deref() != Some("alt=sse") {
                                return error_response(StatusCode::BAD_REQUEST, "missing alt=sse");
                            }
                            let sse = concat!(
                                "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hi \"}]}}]}\n\n",
                                "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"there\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":6,\"candidatesTokenCount\":2}}\n\n",
                            );
                            axum::http::Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "text/event-stream")
                                .body(Body::from(sse))
                                .unwrap()
                        }
                        other => error_response(
                            StatusCode::NOT_FOUND,
                            &format!("unexpected model action {other}"),
                        ),
                    }
                },
            ),
        );
    spawn_app(app).await
}

async fn spawn_openai_backend() -> TestServer {
    let app = Router::new()
        .route(
            "/v1/chat/completions",
            post(
                |headers: HeaderMap, Json(request): Json<Value>| async move {
                    json_response(json!({
                        "id": "chatcmpl-1",
                        "object": "chat.completion",
                        "model": request["model"],
                        "received_model": request["model"],
                        "provider_authorization": headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or(""),
                        "choices": [{
                            "index": 0,
                            "message": { "role": "assistant", "content": "ok" },
                            "finish_reason": "stop"
                        }],
                        "usage": { "prompt_tokens": 13, "completion_tokens": 6 }
                    }))
                },
            ),
        )
        .route(
            "/v1/embeddings",
            post(|Json(request): Json<Value>| async move {
                json_response(json!({
                    "object": "list",
                    "received_model": request["model"],
                    "data": [{ "object": "embedding", "index": 0, "embedding": [0.1, 0.2] }],
                    "usage": { "prompt_tokens": 4, "completion_tokens": 0 }
                }))
            }),
        )
        .route(
            "/v1/models",
            get(|| async {
                json_response(json!({
                    "object": "list",
                    "data": [{ "id": "text-embed", "object": "model" }]
                }))
            }),
        );
    spawn_app(app).await
}

fn json_response(value: Value) -> axum::http::Response<Body> {
    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(value.to_string()))
        .unwrap()
}

fn error_response(status: StatusCode, message: &str) -> axum::http::Response<Body> {
    axum::http::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "error": { "type": "mock_assertion", "message": message } }).to_string(),
        ))
        .unwrap()
}

// ---------------------------------------------------------------------------
// config + harness helpers
// ---------------------------------------------------------------------------

fn provider(name: &str, kind: ProviderKind, addr: SocketAddr, base_path: &str) -> Provider {
    let env_name = match kind {
        ProviderKind::Anthropic => "DXGATE_TEST_ANTHROPIC_KEY",
        ProviderKind::Gemini => "DXGATE_TEST_GEMINI_KEY",
        ProviderKind::DeepSeek => "DXGATE_TEST_DEEPSEEK_KEY",
        _ => "DXGATE_TEST_OPENAI_UNSET_KEY",
    };
    Provider {
        name: name.into(),
        kind,
        base_url: format!("http://{addr}{base_path}"),
        api_key_env: Some(env_name.into()),
        request_headers: vec![],
    }
}

fn llm_backend(
    name: &str,
    provider: &str,
    models: Vec<String>,
    model_rewrites: BTreeMap<String, String>,
) -> Backend {
    Backend {
        name: name.into(),
        kind: BackendKind::Llm {
            provider: provider.into(),
            models,
            endpoint: None,
            model_rewrites,
        },
        policies: vec![],
    }
}

fn llm_config(provider: Provider, backends: Vec<Backend>, policies: Vec<Policy>) -> RuntimeConfig {
    let weighted = backends
        .iter()
        .map(|backend| WeightedBackend {
            name: backend.name.clone(),
            weight: 100,
        })
        .collect();
    RuntimeConfig {
        version: "llm-test".into(),
        listeners: vec![],
        clusters: vec![],
        secrets: vec![],
        providers: vec![provider],
        backends,
        routes: vec![AgentRoute {
            name: "llm".into(),
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
            weighted_backends: weighted,
            policies: vec![],
        }],
        policies,
    }
}

async fn spawn_proxy(cfg: RuntimeConfig) -> TestProxy {
    let addr = unused_addr();
    let state = ProxyState::new(RuntimeConfig::empty("bootstrap"));
    state.apply_config(cfg).await.unwrap();
    let proxy_state = state.clone();
    let task = tokio::spawn(async move {
        ProxyServer::new(proxy_state).serve(addr).await.unwrap();
    });
    wait_until_accepting(addr).await;
    TestProxy { addr, state, task }
}

async fn spawn_app(app: Router) -> TestServer {
    let addr = unused_addr();
    let task = tokio::spawn(async move {
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });
    wait_until_accepting(addr).await;
    TestServer { addr, task }
}

async fn post_json(addr: SocketAddr, path: &str, value: Value) -> (StatusCode, Value) {
    let (status, _, text) = post_json_raw(addr, path, value).await;
    let value = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);
    (status, value)
}

async fn post_json_raw(
    addr: SocketAddr,
    path: &str,
    value: Value,
) -> (StatusCode, HeaderMap, String) {
    let uri: Uri = format!("http://{addr}{path}").parse().unwrap();
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", "Bearer caller-key")
        .body(Body::from(value.to_string()))
        .unwrap();
    let response = Client::new().request(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = body::to_bytes(response.into_body()).await.unwrap();
    (status, headers, String::from_utf8(bytes.to_vec()).unwrap())
}

fn unused_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn wait_until_accepting(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "server at {addr} never started accepting connections"
        );
        sleep(Duration::from_millis(25)).await;
    }
}
