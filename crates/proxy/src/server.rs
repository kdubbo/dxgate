use crate::ProxyState;
use axum::body::Body;
use axum::extract::State;
use axum::http::{
    HeaderMap, HeaderName, HeaderValue as HttpHeaderValue, Method, Request, Response, StatusCode,
    Uri,
};
use axum::routing::any;
use axum::Router;
use dxgate_core::{
    AgentMatchInput, AgentProtocol, AgentRoute, AuthPolicy, Backend, BackendKind, Cluster,
    Endpoint, HeaderTransform, MatchInput, PolicyAction, Provider, RateLimitKey, RetryPolicy,
    UpstreamTls, UpstreamTlsMode, WeightedBackend, HTTP_LISTENER_PORT,
};
use hyper::body::{self, Bytes};
use hyper::client::HttpConnector;
use hyper::Client;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use rustls::client::{ServerCertVerified, ServerCertVerifier, WebPkiVerifier};
use rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::time;
use tracing::{debug, info, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

type PlainClient = Client<HttpConnector, Body>;
type WebClient = Client<HttpsConnector<HttpConnector>, Body>;
type MtlsClient = Client<HttpsConnector<HttpConnector>, Body>;
const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";

#[derive(Clone)]
pub struct ProxyServer {
    state: ProxyState,
    clients: UpstreamClients,
    policy_default: PolicyDefault,
    metrics_identity: MetricsIdentity,
    access_log: AccessLogConfig,
}

impl ProxyServer {
    pub fn new(state: ProxyState) -> Self {
        Self {
            state,
            clients: UpstreamClients::from_env(),
            policy_default: PolicyDefault::from_env(),
            metrics_identity: MetricsIdentity::from_env(),
            access_log: AccessLogConfig::from_env(),
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> std::io::Result<()> {
        let app = Router::new().fallback(any(proxy_request)).with_state(self);
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .map_err(std::io::Error::other)
    }
}

#[derive(Clone)]
struct MetricsIdentity {
    namespace: String,
    gateway: String,
}

impl MetricsIdentity {
    fn from_env() -> Self {
        Self {
            namespace: env::var("POD_NAMESPACE").unwrap_or_else(|_| "unknown".to_string()),
            gateway: env::var("DXGATE_GATEWAY_NAME")
                .or_else(|_| env::var("GATEWAY_NAME"))
                .unwrap_or_else(|_| "unknown".to_string()),
        }
    }
}

#[derive(Clone)]
struct AccessLogConfig {
    enabled: bool,
    format: AccessLogFormat,
}

impl AccessLogConfig {
    fn from_env() -> Self {
        let enabled = env::var("DXGATE_ACCESS_LOG").ok();
        let format = env::var("DXGATE_ACCESS_LOG_FORMAT").ok();
        Self::from_values(enabled.as_deref(), format.as_deref())
    }

    fn from_values(enabled: Option<&str>, format: Option<&str>) -> Self {
        Self {
            enabled: parse_access_log_enabled(enabled),
            format: parse_access_log_format(format),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AccessLogFormat {
    Text,
    Json,
}

fn parse_access_log_enabled(value: Option<&str>) -> bool {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value)
            if value.eq_ignore_ascii_case("false")
                || value.eq_ignore_ascii_case("0")
                || value.eq_ignore_ascii_case("no")
                || value.eq_ignore_ascii_case("off") =>
        {
            false
        }
        _ => true,
    }
}

fn parse_access_log_format(value: Option<&str>) -> AccessLogFormat {
    match value.map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case("json") => AccessLogFormat::Json,
        _ => AccessLogFormat::Text,
    }
}

async fn proxy_request(State(server): State<ProxyServer>, req: Request<Body>) -> Response<Body> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let parent_context = extract_trace_context(req.headers());
    let span = tracing::info_span!(
        "dxgate.request",
        http.method = %method,
        http.target = %path,
        http.status_code = tracing::field::Empty,
        http.latency_ms = tracing::field::Empty,
        gateway.namespace = tracing::field::Empty,
        gateway.name = tracing::field::Empty,
        http.route = tracing::field::Empty,
        dxgate.cluster = tracing::field::Empty,
        upstream.address = tracing::field::Empty
    );
    span.set_parent(parent_context);
    let result = forward(server, req).instrument(span.clone()).await;
    match result {
        Ok(resp) => {
            span.record("http.status_code", resp.status().as_u16());
            resp
        }
        Err((status, message)) => {
            span.record("http.status_code", status.as_u16());
            warn!(status = status.as_u16(), %message, "request failed");
            Response::builder()
                .status(status)
                .body(Body::from(message))
                .unwrap_or_else(|_| Response::new(Body::from("proxy error")))
        }
    }
}

async fn forward(
    server: ProxyServer,
    mut req: Request<Body>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let cfg = server.state.config().await;
    let protocol = detect_agent_protocol(req.uri().path()).or_else(|| {
        cfg.routes
            .iter()
            .any(|route| route.protocol == AgentProtocol::Http)
            .then_some(AgentProtocol::Http)
    });
    if let Some(protocol) = protocol {
        let (parts, body) = req.into_parts();
        let body_bytes = body::to_bytes(body)
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("read request body: {e}")))?;
        let context = AgentRequestContext::new(protocol, &parts, &body_bytes);
        if let Some(route) = cfg.agent_route_for(&context.input()).cloned() {
            return forward_agent(server, cfg, parts, body_bytes, context, route).await;
        }
        req = Request::from_parts(parts, Body::from(body_bytes));
    }

    forward_http(server, cfg, req).await
}

async fn forward_http(
    server: ProxyServer,
    cfg: dxgate_core::RuntimeConfig,
    mut req: Request<Body>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let method = req.method().as_str().to_string();
    let host = host_header(req.headers()).unwrap_or("*").to_string();
    let path = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/")
        .to_string();
    let headers = header_pairs(req.headers());
    let input = MatchInput {
        host: &host,
        path: &path,
        headers: &headers,
    };

    let route = match cfg.route_for(HTTP_LISTENER_PORT, &input) {
        Ok(route) => route,
        Err(err) => {
            record_http_observation(
                &server,
                HttpObservation {
                    route: "none",
                    cluster: "none",
                    method: &method,
                    host: &host,
                    path: &path,
                    status_code: StatusCode::NOT_FOUND.as_u16(),
                    latency_ms: 0,
                    upstream: "none",
                },
            );
            return Err((StatusCode::NOT_FOUND, err.to_string()));
        }
    };
    let route_name = route.name.clone();
    record_http_span(&server, &route_name, "none", "none", 0, 0);
    let weighted_clusters = route.weighted_clusters.clone();

    let weighted = match server.state.pick_cluster(&weighted_clusters).await {
        Some(weighted) => weighted,
        None => {
            record_http_observation(
                &server,
                HttpObservation {
                    route: &route_name,
                    cluster: "none",
                    method: &method,
                    host: &host,
                    path: &path,
                    status_code: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                    latency_ms: 0,
                    upstream: "none",
                },
            );
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "route has no clusters".to_string(),
            ));
        }
    };

    let cluster = match cfg.cluster(&weighted.name) {
        Some(cluster) => cluster.clone(),
        None => {
            record_http_observation(
                &server,
                HttpObservation {
                    route: &route_name,
                    cluster: &weighted.name,
                    method: &method,
                    host: &host,
                    path: &path,
                    status_code: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                    latency_ms: 0,
                    upstream: "none",
                },
            );
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("cluster {} not found", weighted.name),
            ));
        }
    };
    let cluster_name = cluster.name.clone();

    let endpoint = match server
        .state
        .pick_endpoint(&cluster_name, &cluster.endpoints)
        .await
    {
        Ok(endpoint) => endpoint,
        Err(err) => {
            record_http_observation(
                &server,
                HttpObservation {
                    route: &route_name,
                    cluster: &cluster_name,
                    method: &method,
                    host: &host,
                    path: &path,
                    status_code: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                    latency_ms: 0,
                    upstream: "none",
                },
            );
            return Err((StatusCode::SERVICE_UNAVAILABLE, err.to_string()));
        }
    };
    let upstream = endpoint_authority(&endpoint);
    record_http_span(&server, &route_name, &cluster_name, &upstream, 0, 0);
    let _circuit_breaker_permit = match server.state.try_acquire_circuit_breaker(&cluster) {
        Ok(permit) => permit,
        Err(_) => {
            record_http_observation(
                &server,
                HttpObservation {
                    route: &route_name,
                    cluster: &cluster_name,
                    method: &method,
                    host: &host,
                    path: &path,
                    status_code: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                    latency_ms: 0,
                    upstream: &upstream,
                },
            );
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("cluster {} circuit breaker open", cluster_name),
            ));
        }
    };

    let tls = cluster.tls.as_ref();
    let request_mode = upstream_request_mode(tls);
    let scheme = match request_mode {
        UpstreamRequestMode::PlainHttp => "http",
        UpstreamRequestMode::SimpleTls | UpstreamRequestMode::DubboMutual => "https",
    };
    let upstream_uri = format!("{}://{}{}", scheme, upstream, path)
        .parse::<Uri>()
        .map_err(|e| {
            record_http_observation(
                &server,
                HttpObservation {
                    route: &route_name,
                    cluster: &cluster_name,
                    method: &method,
                    host: &host,
                    path: &path,
                    status_code: StatusCode::BAD_GATEWAY.as_u16(),
                    latency_ms: 0,
                    upstream: &upstream,
                },
            );
            (
                StatusCode::BAD_GATEWAY,
                format!("invalid upstream uri: {e}"),
            )
        })?;

    debug!(
        route = %route_name,
        cluster = %cluster_name,
        endpoint = %endpoint.address,
        upstream_mode = ?request_mode,
        "forwarding request"
    );

    *req.uri_mut() = upstream_uri;
    req.headers_mut().remove(http::header::HOST);
    inject_trace_context(req.headers_mut());

    let started = Instant::now();
    let result = match request_mode {
        UpstreamRequestMode::PlainHttp => server.clients.request_plain(req).await,
        UpstreamRequestMode::SimpleTls => server.clients.request_web(req).await,
        UpstreamRequestMode::DubboMutual => {
            let tls = tls.expect("dubbo mutual request mode requires TLS config");
            server.clients.request_mtls(&cluster, tls, req).await
        }
    };
    let latency_ms = started.elapsed().as_millis() as u64;
    let status = result
        .as_ref()
        .map(|response| response.status().as_u16())
        .unwrap_or_else(|(status, _)| status.as_u16());
    record_http_observation(
        &server,
        HttpObservation {
            route: &route_name,
            cluster: &cluster_name,
            method: &method,
            host: &host,
            path: &path,
            status_code: status,
            latency_ms,
            upstream: &upstream,
        },
    );
    result
}

struct HttpObservation<'a> {
    route: &'a str,
    cluster: &'a str,
    method: &'a str,
    host: &'a str,
    path: &'a str,
    status_code: u16,
    latency_ms: u64,
    upstream: &'a str,
}

fn record_http_observation(server: &ProxyServer, observation: HttpObservation<'_>) {
    record_http_metric(
        server,
        observation.route,
        observation.cluster,
        observation.method,
        observation.status_code,
        observation.latency_ms,
    );
    record_http_span(
        server,
        observation.route,
        observation.cluster,
        observation.upstream,
        observation.status_code,
        observation.latency_ms,
    );
    emit_http_access_log(server, &observation);
}

fn record_http_span(
    server: &ProxyServer,
    route: &str,
    cluster: &str,
    upstream: &str,
    status_code: u16,
    latency_ms: u64,
) {
    let span = tracing::Span::current();
    span.record(
        "gateway.namespace",
        server.metrics_identity.namespace.as_str(),
    );
    span.record("gateway.name", server.metrics_identity.gateway.as_str());
    span.record("http.route", route);
    span.record("dxgate.cluster", cluster);
    span.record("upstream.address", upstream);
    if status_code > 0 {
        span.record("http.status_code", status_code);
    }
    span.record("http.latency_ms", latency_ms);
}

fn record_http_metric(
    server: &ProxyServer,
    route: &str,
    cluster: &str,
    method: &str,
    status_code: u16,
    latency_ms: u64,
) {
    server.state.record_http_request(
        &server.metrics_identity.namespace,
        &server.metrics_identity.gateway,
        route,
        cluster,
        method,
        status_code,
        latency_ms,
    );
}

async fn forward_agent(
    server: ProxyServer,
    cfg: dxgate_core::RuntimeConfig,
    parts: http::request::Parts,
    body: Bytes,
    context: AgentRequestContext,
    route: AgentRoute,
) -> Result<Response<Body>, (StatusCode, String)> {
    let eligible = route
        .weighted_backends
        .iter()
        .filter_map(|weighted| {
            let backend = cfg.backend(&weighted.name)?;
            if backend_matches_protocol(backend, context.protocol)
                && backend.supports_model(context.model.as_deref())
                && backend.supports_tool(context.tool.as_deref())
            {
                Some(weighted.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if eligible.is_empty() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("agent route {} has no eligible backends", route.name),
        ));
    }

    let primary = if let Some(bound) = mcp_session_backend(&server, &context, &eligible) {
        bound
    } else {
        server
            .state
            .pick_backend(&eligible)
            .await
            .cloned()
            .unwrap_or_else(|| eligible[0].clone())
    };
    let mut ordered = vec![primary.clone()];
    ordered.extend(eligible.into_iter().filter(|b| b.name != primary.name));

    let primary_backend = cfg.backend(&primary.name).ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("backend {} not found", primary.name),
        )
    })?;
    let policy_runtime =
        evaluate_policies(&server, &cfg, &route, primary_backend, &context, body.len())?;

    if context.protocol == AgentProtocol::Mcp
        && context.mcp_method.as_deref() == Some("tools/list")
        && ordered.len() > 1
    {
        return federate_mcp_tools(
            &server,
            &cfg,
            &route,
            &ordered,
            &parts,
            &body,
            &context,
            &policy_runtime,
        )
        .await;
    }

    request_agent_with_failover(
        &server,
        &cfg,
        &route,
        &ordered,
        &parts,
        &body,
        &context,
        &policy_runtime,
    )
    .await
}

async fn request_agent_with_failover(
    server: &ProxyServer,
    cfg: &dxgate_core::RuntimeConfig,
    route: &AgentRoute,
    ordered: &[WeightedBackend],
    parts: &http::request::Parts,
    body: &Bytes,
    context: &AgentRequestContext,
    policy_runtime: &PolicyRuntime,
) -> Result<Response<Body>, (StatusCode, String)> {
    let retry = policy_runtime.retry.clone().unwrap_or(RetryPolicy {
        attempts: 1,
        statuses: vec![502, 503, 504],
    });
    let attempts = retry.attempts.max(1) as usize;
    let mut last_error = None;

    for attempt in 0..attempts {
        for weighted in ordered {
            let Some(backend) = cfg.backend(&weighted.name) else {
                continue;
            };
            let started = Instant::now();
            let upstream_span = tracing::info_span!(
                "dxgate.agent.upstream",
                protocol = protocol_name(context.protocol),
                route = %route.name,
                backend = %backend.name,
                http.status_code = tracing::field::Empty
            );
            match request_agent_backend(server, cfg, backend, parts, body, context, policy_runtime)
                .instrument(upstream_span.clone())
                .await
            {
                Ok(mut response) => {
                    let status = response.status();
                    upstream_span.record("http.status_code", status.as_u16());
                    update_mcp_session(&server.state, backend, context, response.headers(), status);
                    server.state.record_agent_request(
                        protocol_name(context.protocol),
                        &route.name,
                        &backend.name,
                        status.as_u16(),
                        started.elapsed().as_millis() as u64,
                    );
                    apply_response_headers(
                        response.headers_mut(),
                        &policy_runtime.response_headers,
                    );
                    apply_mcp_stream_headers(response.headers_mut(), context);
                    if attempt + 1 < attempts && retry.statuses.contains(&status.as_u16()) {
                        last_error = Some((
                            status,
                            format!("upstream {} returned {}", backend.name, status),
                        ));
                        continue;
                    }
                    return Ok(response);
                }
                Err(err) => {
                    upstream_span.record("http.status_code", err.0.as_u16());
                    server.state.record_agent_request(
                        protocol_name(context.protocol),
                        &route.name,
                        &backend.name,
                        err.0.as_u16(),
                        started.elapsed().as_millis() as u64,
                    );
                    last_error = Some(err);
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        (
            StatusCode::BAD_GATEWAY,
            format!("agent route {} had no reachable backends", route.name),
        )
    }))
}

async fn request_agent_backend(
    server: &ProxyServer,
    cfg: &dxgate_core::RuntimeConfig,
    backend: &Backend,
    parts: &http::request::Parts,
    body: &Bytes,
    context: &AgentRequestContext,
    policy_runtime: &PolicyRuntime,
) -> Result<Response<Body>, (StatusCode, String)> {
    let provider = backend_provider(cfg, backend);
    let endpoint = backend.endpoint(provider).ok_or_else(|| {
        (
            StatusCode::BAD_GATEWAY,
            format!("backend {} has no endpoint", backend.name),
        )
    })?;
    let uri = compose_upstream_uri(endpoint, &context.path_and_query)?;
    let mut headers = parts.headers.clone();
    apply_request_headers(&mut headers, &policy_runtime.request_headers);
    apply_provider_headers(&mut headers, provider);
    headers.remove(http::header::HOST);
    inject_trace_context(&mut headers);

    let mut builder = Request::builder()
        .method(parts.method.clone())
        .uri(uri)
        .version(parts.version);
    *builder.headers_mut().unwrap() = headers;
    let request = builder.body(Body::from(body.clone())).map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("build upstream request: {e}"),
        )
    })?;

    let fut = server.clients.request_web(request);
    if let Some(timeout) = policy_runtime.timeout {
        return time::timeout(timeout, fut).await.map_err(|_| {
            (
                StatusCode::GATEWAY_TIMEOUT,
                format!("backend {} timed out", backend.name),
            )
        })?;
    }
    fut.await
}

async fn federate_mcp_tools(
    server: &ProxyServer,
    cfg: &dxgate_core::RuntimeConfig,
    route: &AgentRoute,
    ordered: &[WeightedBackend],
    parts: &http::request::Parts,
    body: &Bytes,
    context: &AgentRequestContext,
    policy_runtime: &PolicyRuntime,
) -> Result<Response<Body>, (StatusCode, String)> {
    let mut tools = Vec::new();
    let mut seen = BTreeSet::new();
    let mut failures = Vec::new();

    for weighted in ordered {
        let Some(backend) = cfg.backend(&weighted.name) else {
            continue;
        };
        let started = Instant::now();
        let upstream_span = tracing::info_span!(
            "dxgate.agent.upstream",
            protocol = protocol_name(context.protocol),
            route = %route.name,
            backend = %backend.name,
            http.status_code = tracing::field::Empty
        );
        match request_agent_backend(server, cfg, backend, parts, body, context, policy_runtime)
            .instrument(upstream_span.clone())
            .await
        {
            Ok(response) => {
                let status = response.status();
                upstream_span.record("http.status_code", status.as_u16());
                let bytes = body::to_bytes(response.into_body()).await.map_err(|e| {
                    (
                        StatusCode::BAD_GATEWAY,
                        format!("read MCP tools/list response from {}: {e}", backend.name),
                    )
                })?;
                server.state.record_agent_request(
                    protocol_name(context.protocol),
                    &route.name,
                    &backend.name,
                    status.as_u16(),
                    started.elapsed().as_millis() as u64,
                );
                if !status.is_success() {
                    failures.push(format!("{} returned {}", backend.name, status));
                    continue;
                }
                let value = serde_json::from_slice::<Value>(&bytes).map_err(|e| {
                    (
                        StatusCode::BAD_GATEWAY,
                        format!("parse MCP tools/list response from {}: {e}", backend.name),
                    )
                })?;
                if let Some(items) = value
                    .get("result")
                    .and_then(|result| result.get("tools"))
                    .and_then(Value::as_array)
                {
                    for item in items {
                        let name = item
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        if seen.insert(name) {
                            tools.push(item.clone());
                        }
                    }
                }
            }
            Err(err) => {
                upstream_span.record("http.status_code", err.0.as_u16());
                server.state.record_agent_request(
                    protocol_name(context.protocol),
                    &route.name,
                    &backend.name,
                    err.0.as_u16(),
                    started.elapsed().as_millis() as u64,
                );
                failures.push(format!("{}: {}", backend.name, err.1));
            }
        }
    }

    if tools.is_empty() && !failures.is_empty() {
        return Err((StatusCode::BAD_GATEWAY, failures.join("; ")));
    }

    let id = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("id").cloned())
        .unwrap_or(Value::Null);
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": { "tools": tools }
            })
            .to_string(),
        ))
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("build MCP federation response: {e}"),
            )
        })?;
    apply_response_headers(response.headers_mut(), &policy_runtime.response_headers);
    Ok(response)
}

fn detect_agent_protocol(path: &str) -> Option<AgentProtocol> {
    if path == "/v1/chat/completions" {
        Some(AgentProtocol::Llm)
    } else if path == "/mcp" || path.starts_with("/mcp/") {
        Some(AgentProtocol::Mcp)
    } else if path == "/.well-known/agent-card.json" || path == "/a2a" || path.starts_with("/a2a/")
    {
        Some(AgentProtocol::A2a)
    } else {
        None
    }
}

#[derive(Debug, Clone)]
struct AgentRequestContext {
    protocol: AgentProtocol,
    host: String,
    path: String,
    path_and_query: String,
    method: Method,
    model: Option<String>,
    tool: Option<String>,
    agent: Option<String>,
    mcp_method: Option<String>,
    mcp_session_id: Option<String>,
    mcp_stream: bool,
    headers: Vec<(String, String)>,
}

impl AgentRequestContext {
    fn new(protocol: AgentProtocol, parts: &http::request::Parts, body: &Bytes) -> Self {
        let json = serde_json::from_slice::<Value>(body).ok();
        let model = if protocol == AgentProtocol::Llm {
            json.as_ref()
                .and_then(|value| value.get("model"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        } else {
            None
        };
        let mcp_method = if protocol == AgentProtocol::Mcp {
            json.as_ref()
                .and_then(|value| value.get("method"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        } else {
            None
        };
        let tool = if protocol == AgentProtocol::Mcp {
            json.as_ref()
                .and_then(|value| value.get("params"))
                .and_then(|params| params.get("name"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        } else {
            None
        };
        let agent = if protocol == AgentProtocol::A2a {
            json.as_ref()
                .and_then(|value| {
                    value
                        .get("agent")
                        .or_else(|| value.get("params").and_then(|params| params.get("agent")))
                })
                .and_then(Value::as_str)
                .map(ToString::to_string)
        } else {
            None
        };
        let host = host_header(&parts.headers).unwrap_or("*").to_string();
        let path = parts.uri.path().to_string();
        let path_and_query = parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| path.clone());
        let mcp_session_id = if protocol == AgentProtocol::Mcp {
            parts
                .headers
                .get(MCP_SESSION_ID_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string)
        } else {
            None
        };
        let mcp_stream = protocol == AgentProtocol::Mcp
            && (parts.method == Method::GET
                || header_contains(&parts.headers, http::header::ACCEPT, "text/event-stream"));

        Self {
            protocol,
            host,
            path,
            path_and_query,
            method: parts.method.clone(),
            model,
            tool,
            agent,
            mcp_method,
            mcp_session_id,
            mcp_stream,
            headers: header_pairs(&parts.headers),
        }
    }

    fn input(&self) -> AgentMatchInput<'_> {
        AgentMatchInput {
            protocol: self.protocol,
            host: &self.host,
            path: &self.path,
            method: self.method.as_str(),
            model: self.model.as_deref(),
            tool: self.tool.as_deref(),
            agent: self.agent.as_deref(),
            headers: &self.headers,
        }
    }
}

fn mcp_session_backend(
    server: &ProxyServer,
    context: &AgentRequestContext,
    eligible: &[WeightedBackend],
) -> Option<WeightedBackend> {
    if context.protocol != AgentProtocol::Mcp {
        return None;
    }
    let backend_name = context
        .mcp_session_id
        .as_deref()
        .and_then(|session_id| server.state.mcp_session_backend(session_id))?;
    eligible
        .iter()
        .find(|weighted| weighted.name == backend_name)
        .cloned()
}

fn update_mcp_session(
    state: &ProxyState,
    backend: &Backend,
    context: &AgentRequestContext,
    headers: &HeaderMap,
    status: StatusCode,
) {
    if context.protocol != AgentProtocol::Mcp || !status.is_success() {
        return;
    }
    if let Some(session_id) = headers
        .get(MCP_SESSION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        state.bind_mcp_session(session_id, backend.name.clone());
    } else if context.method == Method::DELETE {
        if let Some(session_id) = &context.mcp_session_id {
            state.remove_mcp_session(session_id);
        }
    }
}

fn apply_mcp_stream_headers(headers: &mut HeaderMap, context: &AgentRequestContext) {
    if !context.mcp_stream {
        return;
    }
    headers
        .entry(http::header::CACHE_CONTROL)
        .or_insert_with(|| HttpHeaderValue::from_static("no-cache, no-transform"));
    headers.insert(
        HeaderName::from_static("x-accel-buffering"),
        HttpHeaderValue::from_static("no"),
    );
}

fn header_contains(headers: &HeaderMap, name: HeaderName, needle: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase().contains(needle))
        .unwrap_or(false)
}

#[derive(Debug, Clone)]
struct PolicyRuntime {
    request_headers: HeaderTransform,
    response_headers: HeaderTransform,
    timeout: Option<Duration>,
    retry: Option<RetryPolicy>,
}

fn evaluate_policies(
    server: &ProxyServer,
    cfg: &dxgate_core::RuntimeConfig,
    route: &AgentRoute,
    backend: &Backend,
    context: &AgentRequestContext,
    body_size: usize,
) -> Result<PolicyRuntime, (StatusCode, String)> {
    let mut names = route.policies.clone();
    names.extend(backend.policies.clone());
    if names.is_empty() && server.policy_default == PolicyDefault::Deny {
        server.state.record_policy_denied();
        return Err((
            StatusCode::FORBIDDEN,
            "request denied by default policy".to_string(),
        ));
    }

    let mut runtime = PolicyRuntime {
        request_headers: HeaderTransform::default(),
        response_headers: HeaderTransform::default(),
        timeout: None,
        retry: None,
    };

    for name in names {
        let Some(policy) = cfg.policy(&name) else {
            continue;
        };
        if !policy.applies_to(&context.input()) {
            continue;
        }
        if policy.action == PolicyAction::Deny {
            server.state.record_policy_denied();
            return Err((
                StatusCode::FORBIDDEN,
                format!("request denied by policy {}", policy.name),
            ));
        }
        if let Some(limit) = policy.max_body_bytes {
            if body_size > limit {
                server.state.record_policy_denied();
                return Err((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    format!("request body exceeds policy {} limit", policy.name),
                ));
            }
        }
        if let Some(auth) = &policy.auth {
            validate_auth(auth, &context.headers).map_err(|message| {
                server.state.record_policy_denied();
                (
                    StatusCode::UNAUTHORIZED,
                    format!("policy {}: {message}", policy.name),
                )
            })?;
        }
        if let Some(rate_limit) = &policy.rate_limit {
            let key = rate_limit_key(rate_limit.key, &policy.name, route, backend, context);
            if !server.state.check_rate_limit(key, rate_limit) {
                server.state.record_policy_denied();
                return Err((
                    StatusCode::TOO_MANY_REQUESTS,
                    format!("rate limit exceeded by policy {}", policy.name),
                ));
            }
        }
        merge_header_transform(&mut runtime.request_headers, &policy.request_headers);
        merge_header_transform(&mut runtime.response_headers, &policy.response_headers);
        if let Some(timeout_ms) = policy.timeout_ms {
            let timeout = Duration::from_millis(timeout_ms.max(1));
            runtime.timeout = Some(
                runtime
                    .timeout
                    .map(|old| old.min(timeout))
                    .unwrap_or(timeout),
            );
        }
        if let Some(retry) = &policy.retry {
            runtime.retry = Some(retry.clone());
        }
    }

    Ok(runtime)
}

fn validate_auth(auth: &AuthPolicy, headers: &[(String, String)]) -> Result<(), String> {
    match auth {
        AuthPolicy::ApiKey {
            header,
            values,
            value_env,
        } => {
            let actual = header_value(headers, header)
                .ok_or_else(|| format!("missing header {}", header))?;
            let mut accepted = values.clone();
            if let Some(env_name) = value_env {
                if let Ok(value) = env::var(env_name) {
                    accepted.push(value);
                }
            }
            if accepted.iter().any(|value| value == actual) {
                Ok(())
            } else {
                Err("invalid API key".to_string())
            }
        }
        AuthPolicy::Jwt {
            header,
            hmac_secret_env,
            issuer,
            audiences,
        } => {
            let raw = header_value(headers, header)
                .ok_or_else(|| format!("missing header {}", header))?;
            let token = raw.strip_prefix("Bearer ").unwrap_or(raw);
            let Some(secret_env) = hmac_secret_env else {
                return Err("JWT policy requires hmac_secret_env".to_string());
            };
            let secret = env::var(secret_env)
                .map_err(|_| format!("JWT secret env {} is not set", secret_env))?;
            let mut validation = Validation::new(Algorithm::HS256);
            if audiences.is_empty() {
                validation.validate_aud = false;
            } else {
                validation.set_audience(audiences);
            }
            if let Some(issuer) = issuer {
                validation.set_issuer(&[issuer]);
            }
            decode::<JwtClaims>(
                token,
                &DecodingKey::from_secret(secret.as_bytes()),
                &validation,
            )
            .map(|_| ())
            .map_err(|e| format!("invalid JWT: {e}"))
        }
    }
}

#[derive(Debug, Deserialize)]
struct JwtClaims {
    #[allow(dead_code)]
    sub: Option<String>,
    #[allow(dead_code)]
    exp: Option<usize>,
    #[allow(dead_code)]
    iss: Option<String>,
    #[allow(dead_code)]
    aud: Option<Value>,
}

fn rate_limit_key(
    key: RateLimitKey,
    policy: &str,
    route: &AgentRoute,
    backend: &Backend,
    context: &AgentRequestContext,
) -> String {
    match key {
        RateLimitKey::Route => format!("{policy}:route:{}", route.name),
        RateLimitKey::Backend => format!("{policy}:backend:{}", backend.name),
        RateLimitKey::Header => format!(
            "{policy}:header:{}",
            header_value(&context.headers, "authorization").unwrap_or("anonymous")
        ),
    }
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn merge_header_transform(target: &mut HeaderTransform, next: &HeaderTransform) {
    target.add.extend(next.add.clone());
    target.remove.extend(next.remove.clone());
}

fn apply_request_headers(headers: &mut HeaderMap, transform: &HeaderTransform) {
    apply_header_transform(headers, transform);
}

fn apply_response_headers(headers: &mut HeaderMap, transform: &HeaderTransform) {
    apply_header_transform(headers, transform);
}

fn apply_header_transform(headers: &mut HeaderMap, transform: &HeaderTransform) {
    for name in &transform.remove {
        if let Ok(name) = HeaderName::try_from(name.as_str()) {
            headers.remove(name);
        }
    }
    for header in &transform.add {
        if let (Ok(name), Ok(value)) = (
            HeaderName::try_from(header.name.as_str()),
            HttpHeaderValue::from_str(&header.value),
        ) {
            headers.insert(name, value);
        }
    }
}

fn apply_provider_headers(headers: &mut HeaderMap, provider: Option<&Provider>) {
    let Some(provider) = provider else {
        return;
    };
    for header in &provider.request_headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::try_from(header.name.as_str()),
            HttpHeaderValue::from_str(&header.value),
        ) {
            headers.insert(name, value);
        }
    }
    if let Some(env_name) = &provider.api_key_env {
        if let Ok(key) = env::var(env_name) {
            if let Ok(value) = HttpHeaderValue::from_str(&format!("Bearer {key}")) {
                headers.insert(http::header::AUTHORIZATION, value);
            }
        }
    }
}

fn backend_provider<'a>(
    cfg: &'a dxgate_core::RuntimeConfig,
    backend: &'a Backend,
) -> Option<&'a Provider> {
    match &backend.kind {
        BackendKind::Llm { provider, .. } => cfg.provider(provider),
        _ => None,
    }
}

fn backend_matches_protocol(backend: &Backend, protocol: AgentProtocol) -> bool {
    matches!(
        (&backend.kind, protocol),
        (BackendKind::Http { .. }, AgentProtocol::Http)
            | (BackendKind::Llm { .. }, AgentProtocol::Llm)
            | (BackendKind::Mcp { .. }, AgentProtocol::Mcp)
            | (BackendKind::A2a { .. }, AgentProtocol::A2a)
    )
}

fn compose_upstream_uri(endpoint: &str, path_and_query: &str) -> Result<Uri, (StatusCode, String)> {
    let endpoint = endpoint.trim_end_matches('/');
    let suffix = if endpoint.ends_with("/v1") && path_and_query.starts_with("/v1/") {
        path_and_query.trim_start_matches("/v1")
    } else {
        path_and_query
    };
    format!("{endpoint}{suffix}").parse::<Uri>().map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("invalid upstream uri for endpoint {endpoint}: {e}"),
        )
    })
}

fn protocol_name(protocol: AgentProtocol) -> &'static str {
    match protocol {
        AgentProtocol::Http => "http",
        AgentProtocol::Llm => "llm",
        AgentProtocol::Mcp => "mcp",
        AgentProtocol::A2a => "a2a",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyDefault {
    Allow,
    Deny,
}

impl PolicyDefault {
    fn from_env() -> Self {
        match env::var("DXGATE_POLICY_DEFAULT") {
            Ok(value) if value.eq_ignore_ascii_case("deny") => Self::Deny,
            _ => Self::Allow,
        }
    }
}

fn endpoint_authority(endpoint: &Endpoint) -> String {
    if endpoint.address.contains(':') && !endpoint.address.starts_with('[') {
        format!("[{}]:{}", endpoint.address, endpoint.port)
    } else {
        format!("{}:{}", endpoint.address, endpoint.port)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpstreamRequestMode {
    PlainHttp,
    SimpleTls,
    DubboMutual,
}

fn upstream_request_mode(tls: Option<&UpstreamTls>) -> UpstreamRequestMode {
    match tls.map(|tls| tls.mode) {
        None => UpstreamRequestMode::PlainHttp,
        Some(UpstreamTlsMode::Simple) => UpstreamRequestMode::SimpleTls,
        Some(UpstreamTlsMode::DubboMutual) => UpstreamRequestMode::DubboMutual,
    }
}

fn host_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(|host| host.split(':').next().unwrap_or(host))
}

fn header_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect()
}

fn extract_trace_context(headers: &HeaderMap) -> opentelemetry::Context {
    let extractor = HeaderExtractor(headers);
    opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&extractor))
}

fn inject_trace_context(headers: &mut HeaderMap) {
    let context = tracing::Span::current().context();
    let mut injector = HeaderInjector(headers);
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut injector)
    });
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(HeaderName::as_str).collect()
    }
}

struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::try_from(key),
            HttpHeaderValue::from_str(value.as_str()),
        ) {
            self.0.insert(name, value);
        }
    }
}

fn emit_http_access_log(server: &ProxyServer, observation: &HttpObservation<'_>) {
    if !server.access_log.enabled {
        return;
    }
    let (trace_id, span_id) = current_trace_ids();
    let event = AccessLogEvent {
        namespace: &server.metrics_identity.namespace,
        gateway: &server.metrics_identity.gateway,
        route: observation.route,
        cluster: observation.cluster,
        method: observation.method,
        host: observation.host,
        path: observation.path,
        status_code: observation.status_code,
        latency_ms: observation.latency_ms,
        upstream: observation.upstream,
        trace_id: &trace_id,
        span_id: &span_id,
    };
    let line = access_log_line(server.access_log.format, &event);
    info!(target: "dxgate.access", "{}", line);
}

fn current_trace_ids() -> (String, String) {
    let context = tracing::Span::current().context();
    let span_context = context.span().span_context().clone();
    if span_context.is_valid() {
        (
            span_context.trace_id().to_string(),
            span_context.span_id().to_string(),
        )
    } else {
        (String::new(), String::new())
    }
}

struct AccessLogEvent<'a> {
    namespace: &'a str,
    gateway: &'a str,
    route: &'a str,
    cluster: &'a str,
    method: &'a str,
    host: &'a str,
    path: &'a str,
    status_code: u16,
    latency_ms: u64,
    upstream: &'a str,
    trace_id: &'a str,
    span_id: &'a str,
}

fn access_log_line(format: AccessLogFormat, event: &AccessLogEvent<'_>) -> String {
    match format {
        AccessLogFormat::Text => format!(
            "namespace={} gateway={} route={} cluster={} method={} host={} path={} status_code={} latency_ms={} upstream={} trace_id={} span_id={}",
            event.namespace,
            event.gateway,
            event.route,
            event.cluster,
            event.method,
            event.host,
            event.path,
            event.status_code,
            event.latency_ms,
            event.upstream,
            event.trace_id,
            event.span_id
        ),
        AccessLogFormat::Json => serde_json::json!({
            "namespace": event.namespace,
            "gateway": event.gateway,
            "route": event.route,
            "cluster": event.cluster,
            "method": event.method,
            "host": event.host,
            "path": event.path,
            "status_code": event.status_code,
            "latency_ms": event.latency_ms,
            "upstream": event.upstream,
            "trace_id": event.trace_id,
            "span_id": event.span_id,
        })
        .to_string(),
    }
}

#[derive(Clone)]
struct UpstreamClients {
    plaintext: PlainClient,
    web: WebClient,
    mtls: MtlsSupport,
}

impl UpstreamClients {
    fn from_env() -> Self {
        let mtls = match env::var("GRPC_XDS_BOOTSTRAP") {
            Ok(path) if !path.is_empty() => match MtlsClientPool::from_bootstrap(&path) {
                Ok(pool) => {
                    info!(bootstrap = %path, "loaded dxgate upstream mTLS bootstrap");
                    MtlsSupport::Available(Arc::new(pool))
                }
                Err(err) => {
                    warn!(bootstrap = %path, %err, "failed loading dxgate upstream mTLS bootstrap");
                    MtlsSupport::Error(Arc::from(err))
                }
            },
            _ => MtlsSupport::Disabled,
        };
        let web_connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        Self {
            plaintext: Client::new(),
            web: Client::builder().build::<_, Body>(web_connector),
            mtls,
        }
    }

    async fn request_plain(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, (StatusCode, String)> {
        self.plaintext
            .request(req)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
    }

    async fn request_web(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, (StatusCode, String)> {
        self.web
            .request(req)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
    }

    async fn request_mtls(
        &self,
        cluster: &Cluster,
        tls: &UpstreamTls,
        req: Request<Body>,
    ) -> Result<Response<Body>, (StatusCode, String)> {
        let client = match &self.mtls {
            MtlsSupport::Available(pool) => pool.client_for(tls).map_err(|err| {
                (
                    StatusCode::BAD_GATEWAY,
                    format!("cluster {} mTLS setup failed: {err}", cluster.name),
                )
            })?,
            MtlsSupport::Disabled => {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "cluster {} requires mTLS but GRPC_XDS_BOOTSTRAP is not configured",
                        cluster.name
                    ),
                ));
            }
            MtlsSupport::Error(err) => {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "cluster {} requires mTLS but bootstrap loading failed: {err}",
                        cluster.name
                    ),
                ));
            }
        };
        client
            .request(req)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
    }
}

#[derive(Clone)]
enum MtlsSupport {
    Disabled,
    Available(Arc<MtlsClientPool>),
    Error(Arc<str>),
}

struct MtlsClientPool {
    bootstrap: GrpcBootstrap,
    clients: Mutex<HashMap<String, MtlsClient>>,
}

impl MtlsClientPool {
    fn from_bootstrap(path: &str) -> Result<Self, String> {
        let file =
            File::open(path).map_err(|e| format!("open gRPC xDS bootstrap {}: {e}", path))?;
        let bootstrap: GrpcBootstrap = serde_json::from_reader(file)
            .map_err(|e| format!("parse gRPC xDS bootstrap {}: {e}", path))?;
        Ok(Self {
            bootstrap,
            clients: Mutex::new(HashMap::new()),
        })
    }

    fn client_for(&self, tls: &UpstreamTls) -> Result<MtlsClient, String> {
        let key = mtls_cache_key(tls);
        let mut clients = self
            .clients
            .lock()
            .map_err(|_| "mTLS client cache lock poisoned".to_string())?;
        if let Some(client) = clients.get(&key) {
            return Ok(client.clone());
        }

        let config = self.tls_config(tls)?;
        let builder = HttpsConnectorBuilder::new()
            .with_tls_config(config)
            .https_only();
        let builder = match &tls.sni {
            Some(sni) if !sni.is_empty() => builder.with_server_name(sni.clone()),
            _ => builder,
        };
        let connector = builder.enable_http1().build();
        let client = Client::builder().build::<_, Body>(connector);
        clients.insert(key, client.clone());
        Ok(client)
    }

    fn tls_config(&self, tls: &UpstreamTls) -> Result<ClientConfig, String> {
        let cert_provider = tls.certificate_provider.as_deref().unwrap_or("default");
        let root_provider = tls.validation_provider.as_deref().unwrap_or("default");
        let cert_config = self.bootstrap.provider(cert_provider)?;
        let root_config = self.bootstrap.provider(root_provider)?;
        let cert_file = cert_config.required_path("certificate_file", cert_provider)?;
        let key_file = cert_config.required_path("private_key_file", cert_provider)?;
        let ca_file = root_config.required_path("ca_certificate_file", root_provider)?;

        let certs = load_certs(cert_file, "data-plane client certificate")?;
        let key = load_private_key(key_file)?;
        let roots = load_roots(ca_file)?;
        let verifier = Arc::new(SpiffeCompatibleVerifier {
            inner: WebPkiVerifier::new(roots, None),
        });
        ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(certs, key)
            .map_err(|e| format!("build data-plane mTLS client config: {e}"))
    }
}

#[derive(Debug, Deserialize)]
struct GrpcBootstrap {
    #[serde(default)]
    certificate_providers: HashMap<String, CertificateProvider>,
}

impl GrpcBootstrap {
    fn provider(&self, name: &str) -> Result<&FileWatcherConfig, String> {
        self.certificate_providers
            .get(name)
            .map(|provider| &provider.config)
            .ok_or_else(|| format!("certificate_providers[{name:?}] not found"))
    }
}

#[derive(Debug, Deserialize)]
struct CertificateProvider {
    config: FileWatcherConfig,
}

#[derive(Debug, Deserialize)]
struct FileWatcherConfig {
    certificate_file: Option<PathBuf>,
    private_key_file: Option<PathBuf>,
    ca_certificate_file: Option<PathBuf>,
}

impl FileWatcherConfig {
    fn required_path(&self, field: &str, provider: &str) -> Result<&Path, String> {
        let path = match field {
            "certificate_file" => &self.certificate_file,
            "private_key_file" => &self.private_key_file,
            "ca_certificate_file" => &self.ca_certificate_file,
            _ => return Err(format!("unknown file watcher field {field}")),
        };
        path.as_deref().ok_or_else(|| {
            format!("certificate_providers[{provider:?}].config.{field} is required")
        })
    }
}

fn mtls_cache_key(tls: &UpstreamTls) -> String {
    format!(
        "{}|{}|{}|{}",
        tls.sni.as_deref().unwrap_or_default(),
        tls.certificate_provider.as_deref().unwrap_or("default"),
        tls.validation_provider.as_deref().unwrap_or("default"),
        tls.alpn_protocols.join(",")
    )
}

fn load_certs(path: &Path, label: &str) -> Result<Vec<Certificate>, String> {
    let file = File::open(path).map_err(|e| format!("open {label} {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .map_err(|e| format!("parse {label} {}: {e}", path.display()))?
        .into_iter()
        .map(Certificate)
        .collect::<Vec<_>>();
    if certs.is_empty() {
        return Err(format!(
            "parse {label} {}: no certificates found",
            path.display()
        ));
    }
    Ok(certs)
}

fn load_roots(path: &Path) -> Result<RootCertStore, String> {
    let certs = load_certs(path, "data-plane CA certificate")?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots
            .add(&cert)
            .map_err(|e| format!("add data-plane CA certificate {}: {e}", path.display()))?;
    }
    Ok(roots)
}

fn load_private_key(path: &Path) -> Result<PrivateKey, String> {
    if let Some(key) = load_private_keys(path, KeyFormat::Pkcs8)?
        .into_iter()
        .next()
    {
        return Ok(PrivateKey(key));
    }
    if let Some(key) = load_private_keys(path, KeyFormat::Rsa)?.into_iter().next() {
        return Ok(PrivateKey(key));
    }
    Err(format!(
        "parse data-plane client private key {}: no PKCS8 or RSA keys found",
        path.display()
    ))
}

enum KeyFormat {
    Pkcs8,
    Rsa,
}

fn load_private_keys(path: &Path, format: KeyFormat) -> Result<Vec<Vec<u8>>, String> {
    let file = File::open(path)
        .map_err(|e| format!("open data-plane client private key {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    match format {
        KeyFormat::Pkcs8 => rustls_pemfile::pkcs8_private_keys(&mut reader),
        KeyFormat::Rsa => rustls_pemfile::rsa_private_keys(&mut reader),
    }
    .map_err(|e| {
        format!(
            "parse data-plane client private key {}: {e}",
            path.display()
        )
    })
}

struct SpiffeCompatibleVerifier {
    inner: WebPkiVerifier,
}

impl std::fmt::Debug for SpiffeCompatibleVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpiffeCompatibleVerifier").finish()
    }
}

impl ServerCertVerifier for SpiffeCompatibleVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        server_name: &rustls::ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: SystemTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            scts,
            ocsp_response,
            now,
        ) {
            Ok(verified) => Ok(verified),
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                Ok(ServerCertVerified::assertion())
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::body;
    use rcgen::{
        BasicConstraints, Certificate as RcgenCertificate, CertificateParams, DistinguishedName,
        DnType, IsCa,
    };
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    #[test]
    fn parses_grpc_xds_bootstrap_file_watcher_provider() {
        let bootstrap = serde_json::from_str::<GrpcBootstrap>(
            r#"{
              "certificate_providers": {
                "default": {
                  "plugin_name": "file_watcher",
                  "config": {
                    "certificate_file": "/etc/dubbo/proxy/cert-chain.pem",
                    "private_key_file": "/etc/dubbo/proxy/key.pem",
                    "ca_certificate_file": "/etc/dubbo/proxy/root-cert.pem"
                  }
                }
              }
            }"#,
        )
        .unwrap();

        let provider = bootstrap.provider("default").unwrap();
        assert_eq!(
            provider
                .required_path("certificate_file", "default")
                .unwrap(),
            Path::new("/etc/dubbo/proxy/cert-chain.pem")
        );
        assert_eq!(
            provider
                .required_path("private_key_file", "default")
                .unwrap(),
            Path::new("/etc/dubbo/proxy/key.pem")
        );
        assert_eq!(
            provider
                .required_path("ca_certificate_file", "default")
                .unwrap(),
            Path::new("/etc/dubbo/proxy/root-cert.pem")
        );
    }

    #[tokio::test]
    async fn mtls_client_connects_with_bootstrap_certificate() {
        let ca = test_ca();
        let server_cert = signed_cert("nginx.app.svc.cluster.local");
        let client_cert = signed_cert("dxgate.default.svc.cluster.local");
        let dir = temp_dir("dxgate-mtls");
        fs::create_dir_all(&dir).unwrap();

        let cert_chain = dir.join("cert-chain.pem");
        let key = dir.join("key.pem");
        let root = dir.join("root-cert.pem");
        let bootstrap = dir.join("grpc-bootstrap.json");
        fs::write(
            &cert_chain,
            client_cert.serialize_pem_with_signer(&ca).unwrap(),
        )
        .unwrap();
        fs::write(&key, client_cert.serialize_private_key_pem()).unwrap();
        fs::write(&root, ca.serialize_pem().unwrap()).unwrap();
        fs::write(
            &bootstrap,
            serde_json::json!({
                "certificate_providers": {
                    "default": {
                        "plugin_name": "file_watcher",
                        "config": {
                            "certificate_file": cert_chain,
                            "private_key_file": key,
                            "ca_certificate_file": root
                        }
                    }
                }
            })
            .to_string(),
        )
        .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server_config(&ca, &server_cert)));
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut buf = [0; 256];
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await
                .unwrap();
        });

        let pool = MtlsClientPool::from_bootstrap(bootstrap.to_str().unwrap()).unwrap();
        let client = pool
            .client_for(&UpstreamTls {
                mode: UpstreamTlsMode::DubboMutual,
                sni: Some("nginx.app.svc.cluster.local".into()),
                certificate_provider: None,
                validation_provider: None,
                alpn_protocols: vec!["h2".into()],
            })
            .unwrap();
        let uri = format!("https://127.0.0.1:{}/", addr.port())
            .parse()
            .unwrap();
        let response = client.get(uri).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = body::to_bytes(response.into_body()).await.unwrap();
        assert_eq!(&bytes[..], b"ok");

        server.await.unwrap();
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn mtls_cache_key_tracks_provider_and_alpn() {
        let tls = UpstreamTls {
            mode: UpstreamTlsMode::DubboMutual,
            sni: Some("nginx.app.svc.cluster.local".into()),
            certificate_provider: Some("workload".into()),
            validation_provider: Some("roots".into()),
            alpn_protocols: vec!["h2".into(), "http/1.1".into()],
        };

        assert_eq!(
            mtls_cache_key(&tls),
            "nginx.app.svc.cluster.local|workload|roots|h2,http/1.1"
        );
    }

    #[test]
    fn upstream_request_mode_uses_simple_tls_without_mtls_bootstrap() {
        let simple = UpstreamTls {
            mode: UpstreamTlsMode::Simple,
            sni: Some("httpbin.org".into()),
            certificate_provider: None,
            validation_provider: None,
            alpn_protocols: vec![],
        };
        let mutual = UpstreamTls {
            mode: UpstreamTlsMode::DubboMutual,
            sni: Some("nginx.app.svc.cluster.local".into()),
            certificate_provider: None,
            validation_provider: None,
            alpn_protocols: vec![],
        };

        assert_eq!(upstream_request_mode(None), UpstreamRequestMode::PlainHttp);
        assert_eq!(
            upstream_request_mode(Some(&simple)),
            UpstreamRequestMode::SimpleTls
        );
        assert_eq!(
            upstream_request_mode(Some(&mutual)),
            UpstreamRequestMode::DubboMutual
        );
    }

    #[test]
    fn extracts_traceparent_from_headers() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            HttpHeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );

        let context = extract_trace_context(&headers);
        let span_context = context.span().span_context().clone();

        assert!(span_context.is_valid());
        assert_eq!(
            span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");
    }

    #[test]
    fn access_log_config_parses_defaults_and_overrides() {
        let default = AccessLogConfig::from_values(None, None);
        assert!(default.enabled);
        assert_eq!(default.format, AccessLogFormat::Text);

        let disabled = AccessLogConfig::from_values(Some("false"), Some("json"));
        assert!(!disabled.enabled);
        assert_eq!(disabled.format, AccessLogFormat::Json);

        let invalid_format = AccessLogConfig::from_values(Some("true"), Some("yaml"));
        assert!(invalid_format.enabled);
        assert_eq!(invalid_format.format, AccessLogFormat::Text);
    }

    #[test]
    fn access_log_line_formats_text_and_json() {
        let event = AccessLogEvent {
            namespace: "default",
            gateway: "edge",
            route: "httpbin",
            cluster: "httpbin-v1",
            method: "GET",
            host: "httpbin.example",
            path: "/status/502",
            status_code: 502,
            latency_ms: 17,
            upstream: "httpbin.org:443",
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
            span_id: "00f067aa0ba902b7",
        };

        let text = access_log_line(AccessLogFormat::Text, &event);
        assert!(text.contains("route=httpbin"));
        assert!(text.contains("status_code=502"));
        assert!(text.contains("trace_id=4bf92f3577b34da6a3ce929d0e0e4736"));

        let json = access_log_line(AccessLogFormat::Json, &event);
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["namespace"], "default");
        assert_eq!(value["gateway"], "edge");
        assert_eq!(value["status_code"], 502);
        assert_eq!(value["latency_ms"], 17);
        assert_eq!(value["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(value["span_id"], "00f067aa0ba902b7");
    }

    fn test_ca() -> RcgenCertificate {
        let mut params = CertificateParams::new(vec!["dubbo.test".into()]);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "dubbo test ca");
        RcgenCertificate::from_params(params).unwrap()
    }

    fn signed_cert(dns_name: &str) -> RcgenCertificate {
        let mut params = CertificateParams::new(vec![dns_name.into()]);
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, dns_name);
        RcgenCertificate::from_params(params).unwrap()
    }

    fn server_config(
        ca: &RcgenCertificate,
        server_cert: &RcgenCertificate,
    ) -> rustls::ServerConfig {
        let mut client_roots = RootCertStore::empty();
        client_roots
            .add(&Certificate(ca.serialize_der().unwrap()))
            .unwrap();
        let client_verifier = Arc::new(rustls::server::AllowAnyAuthenticatedClient::new(
            client_roots,
        ));
        rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(
                vec![Certificate(
                    server_cert.serialize_der_with_signer(ca).unwrap(),
                )],
                PrivateKey(server_cert.serialize_private_key_der()),
            )
            .unwrap()
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{nanos}"))
    }
}
