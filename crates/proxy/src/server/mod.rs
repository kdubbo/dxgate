use crate::a2a;
use crate::llm::{self, LlmDialect, LlmUsage, UsageSink};
use crate::mcp;
use crate::ProxyState;
use axum::body::Body;
use axum::extract::State;
use axum::http::{
    HeaderMap, HeaderName, HeaderValue as HttpHeaderValue, Method, Request, Response, StatusCode,
    Uri, Version,
};
use axum::routing::any;
use axum::Router;
use dxgate_core::{
    AgentMatchInput, AgentProtocol, AgentRoute, AuthPolicy, Backend, HeaderTransform, MatchInput,
    PolicyAction, Provider, RateLimitKey, RetryPolicy, WeightedBackend, HTTP_LISTENER_PORT,
};
use hyper::body::Bytes;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use opentelemetry::trace::TraceContextExt;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeSet;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use tracing::{debug, info, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod access_log;
mod detect;
mod headers;
mod routing;
mod trace;
mod upstream;

use access_log::{access_log_line, AccessLogConfig, AccessLogEvent};
use detect::{declared_content_length, detect_agent_protocol, is_event_stream, is_grpc_request};
use headers::{
    apply_provider_headers, apply_request_headers, apply_response_headers, header_contains,
    merge_header_transform, remove_connection_headers,
};
use routing::{
    backend_matches_protocol, backend_provider, compose_upstream_uri, endpoint_authority,
    header_pairs, host_header, protocol_name, upstream_request_mode, UpstreamRequestMode,
};
use trace::{extract_trace_context, inject_trace_context};
use upstream::UpstreamClients;

const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";
const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

#[derive(Clone)]
pub struct ProxyServer {
    state: ProxyState,
    clients: UpstreamClients,
    policy_default: PolicyDefault,
    metrics_identity: MetricsIdentity,
    access_log: AccessLogConfig,
    max_body_bytes: usize,
}

impl ProxyServer {
    pub fn new(state: ProxyState) -> Self {
        Self {
            state,
            clients: UpstreamClients::from_env(),
            policy_default: PolicyDefault::from_env(),
            metrics_identity: MetricsIdentity::from_env(),
            access_log: AccessLogConfig::from_env(),
            max_body_bytes: parse_max_body_bytes(env::var("DXGATE_MAX_BODY_BYTES").ok().as_deref()),
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

fn parse_max_body_bytes(value: Option<&str>) -> usize {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|limit| *limit > 0)
        .unwrap_or(DEFAULT_MAX_BODY_BYTES)
}

// Buffered reads back agent-route retries and body inspection; the limit keeps a
// single oversized request from exhausting proxy memory before policies run.
async fn read_body_limited(
    headers: &HeaderMap,
    mut body: Body,
    limit: usize,
) -> Result<Bytes, (StatusCode, String)> {
    use hyper::body::HttpBody;

    if let Some(length) = headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
    {
        if length > limit {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("request body of {length} bytes exceeds limit of {limit} bytes"),
            ));
        }
    }

    let mut buf = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk =
            chunk.map_err(|e| (StatusCode::BAD_REQUEST, format!("read request body: {e}")))?;
        if buf.len() + chunk.len() > limit {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("request body exceeds limit of {limit} bytes"),
            ));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(buf))
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
    // gRPC and Dubbo Triple require end-to-end HTTP/2 with streaming bodies and
    // trailer propagation; buffering the body here would break both, so they skip
    // the agent path and stream straight through cluster routing.
    if is_grpc_request(req.headers()) {
        return forward_http(server, cfg, req).await;
    }
    let protocol = detect_agent_protocol(req.uri().path()).or_else(|| {
        cfg.routes
            .iter()
            .any(|route| route.protocol == AgentProtocol::Http)
            .then_some(AgentProtocol::Http)
    });
    if let Some(protocol) = protocol {
        let (parts, body) = req.into_parts();
        let body_bytes = read_body_limited(&parts.headers, body, server.max_body_bytes).await?;
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
    let upstream = endpoint_authority(endpoint);
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

    let use_h2 = cluster.http2 || is_grpc_request(req.headers());
    *req.uri_mut() = upstream_uri;
    req.headers_mut().remove(http::header::HOST);
    // The downstream and upstream HTTP versions are independent: pin the upstream
    // request version to the negotiated client protocol instead of echoing the
    // downstream version, and drop HTTP/1-only connection headers before h2.
    *req.version_mut() = if use_h2 {
        remove_connection_headers(req.headers_mut());
        Version::HTTP_2
    } else {
        Version::HTTP_11
    };
    inject_trace_context(req.headers_mut());

    let started = Instant::now();
    let result = match (request_mode, use_h2) {
        (UpstreamRequestMode::PlainHttp, false) => server.clients.request_plain(req).await,
        (UpstreamRequestMode::PlainHttp, true) | (UpstreamRequestMode::SimpleTls, true) => {
            server.clients.request_h2(req).await
        }
        (UpstreamRequestMode::SimpleTls, false) => server.clients.request_web(req).await,
        (UpstreamRequestMode::DubboMutual, h2) => {
            let tls = tls.expect("dubbo mutual request mode requires TLS config");
            server.clients.request_mtls(&cluster, tls, req, h2).await
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
    mut body: Bytes,
    mut context: AgentRequestContext,
    route: AgentRoute,
) -> Result<Response<Body>, (StatusCode, String)> {
    // A backend-prefixed name minted by list federation ("mcp-b__search")
    // pins the request to that backend and is rewritten back to the upstream
    // name before matching or forwarding.
    let mut alias_backend: Option<String> = None;
    if context.protocol == AgentProtocol::Mcp {
        let alias = context
            .tool
            .as_deref()
            .and_then(mcp::split_alias)
            .map(|(backend, original)| (backend.to_string(), original.to_string()));
        if let Some((backend_name, original)) = alias {
            if route
                .weighted_backends
                .iter()
                .any(|weighted| weighted.name == backend_name)
            {
                if let Ok(mut json) = serde_json::from_slice::<Value>(&body) {
                    json["params"]["name"] = Value::String(original.clone());
                    body = Bytes::from(json.to_string());
                    context.tool = Some(original);
                    alias_backend = Some(backend_name);
                }
            }
        }
    }

    let eligible = route
        .weighted_backends
        .iter()
        .filter_map(|weighted| {
            let backend = cfg.backend(&weighted.name)?;
            if backend_matches_protocol(backend, context.protocol)
                && backend.supports_model(context.model.as_deref())
                && backend.supports_tool(context.tool.as_deref())
                && backend.supports_agent(context.agent.as_deref())
                && backend_supports_llm_request(&cfg, backend, &context)
                && alias_backend
                    .as_deref()
                    .is_none_or(|name| weighted.name == name)
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
    } else if let Some(bound) = a2a_task_bound_backend(&server, &context, &eligible) {
        // A2A tasks are stateful: follow-ups referencing a task id must reach
        // the backend that owns the task.
        bound
    } else if context.protocol == AgentProtocol::Mcp && context.tool.is_some() {
        // Calls naming a tool/prompt must land on the backend that list
        // federation credited with the bare name: the first eligible backend
        // in route-declared order. Round-robin would let a colliding name
        // resolve to a different backend than the one whose item was listed.
        eligible[0].clone()
    } else if context.protocol == AgentProtocol::A2a && context.path == a2a::AGENT_CARD_PATH {
        // An agent card is one agent's identity; serve it deterministically
        // instead of rotating between backends' cards.
        eligible[0].clone()
    } else {
        server
            .state
            .pick_backend(&eligible)
            .await
            .cloned()
            .unwrap_or_else(|| eligible[0].clone())
    };
    let mut ordered = vec![primary.clone()];
    ordered.extend(eligible.iter().filter(|b| b.name != primary.name).cloned());

    let primary_backend = cfg.backend(&primary.name).ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("backend {} not found", primary.name),
        )
    })?;
    let policy_runtime =
        evaluate_policies(&server, &cfg, &route, primary_backend, &context, body.len())?;

    if context.protocol == AgentProtocol::Mcp && eligible.len() > 1 {
        if let Some(spec) = context.mcp_method.as_deref().and_then(mcp::list_spec) {
            // Cursors are opaque to the gateway and only valid against the
            // backend that issued them, so paged follow-ups skip federation —
            // federated responses never carry a cursor, making this reachable
            // only for clients paging a single backend directly.
            let has_cursor = serde_json::from_slice::<Value>(&body)
                .ok()
                .as_ref()
                .and_then(|json| mcp::request_cursor(json).map(ToString::to_string))
                .is_some();
            if !has_cursor {
                // Federation walks backends in route-declared order so alias
                // assignment on name collisions is stable across requests.
                return federate_mcp_list(
                    &server,
                    &cfg,
                    &route,
                    &eligible,
                    &parts,
                    &body,
                    &context,
                    &policy_runtime,
                    &spec,
                )
                .await;
            }
        }
    }

    let response = request_agent_with_failover(
        &server,
        &cfg,
        &route,
        &ordered,
        &parts,
        &body,
        &context,
        &policy_runtime,
    )
    .await?;

    // On multi-backend routes, list calls are answered by federation, so the
    // initialize handshake must advertise those capabilities even when the
    // session's own backend lacks them.
    if context.protocol == AgentProtocol::Mcp
        && context.mcp_method.as_deref() == Some("initialize")
        && eligible.len() > 1
    {
        return augment_initialize_response(&server, response).await;
    }
    Ok(response)
}

#[allow(clippy::too_many_arguments)]
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
            match request_agent_backend(
                server,
                cfg,
                route,
                backend,
                parts,
                body,
                context,
                policy_runtime,
            )
            .instrument(upstream_span.clone())
            .await
            {
                Ok(mut response) => {
                    let status = response.status();
                    upstream_span.record("http.status_code", status.as_u16());
                    update_mcp_session(&server.state, backend, context, response.headers(), status);
                    record_mcp_tool_call(server, route, backend, context, status.is_success());
                    record_a2a_method_call(server, route, backend, context, status.is_success());
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
                    apply_stream_headers(response.headers_mut(), context);
                    if attempt + 1 < attempts && retry.statuses.contains(&status.as_u16()) {
                        last_error = Some((
                            status,
                            format!("upstream {} returned {}", backend.name, status),
                        ));
                        continue;
                    }
                    if context.protocol == AgentProtocol::A2a {
                        response = process_a2a_response(server, backend, context, response).await?;
                    }
                    return Ok(response);
                }
                Err(err) => {
                    upstream_span.record("http.status_code", err.0.as_u16());
                    record_mcp_tool_call(server, route, backend, context, false);
                    record_a2a_method_call(server, route, backend, context, false);
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

// Per-tool call accounting; success tracks the HTTP status only, since
// JSON-RPC-level errors would require buffering every response body.
fn record_mcp_tool_call(
    server: &ProxyServer,
    route: &AgentRoute,
    backend: &Backend,
    context: &AgentRequestContext,
    success: bool,
) {
    if context.protocol != AgentProtocol::Mcp || context.mcp_method.as_deref() != Some("tools/call")
    {
        return;
    }
    if let Some(tool) = &context.tool {
        server
            .state
            .record_mcp_tool_call(&route.name, &backend.name, tool, success);
    }
}

fn record_a2a_method_call(
    server: &ProxyServer,
    route: &AgentRoute,
    backend: &Backend,
    context: &AgentRequestContext,
    success: bool,
) {
    if context.protocol != AgentProtocol::A2a {
        return;
    }
    if let Some(method) = &context.a2a_method {
        server
            .state
            .record_a2a_method_call(&route.name, &backend.name, method, success);
    }
}

// Post-processes an A2A upstream response: binds task ids to the owning
// backend for follow-up affinity, and rewrites agent-card URLs so clients
// keep talking to the gateway instead of the backend directly.
async fn process_a2a_response(
    server: &ProxyServer,
    backend: &Backend,
    context: &AgentRequestContext,
    response: Response<Body>,
) -> Result<Response<Body>, (StatusCode, String)> {
    if !response.status().is_success() {
        return Ok(response);
    }

    if is_event_stream(response.headers()) {
        // Streamed task creation (message/stream): bind affinity as soon as
        // the first task id appears in the stream.
        let state = server.state.clone();
        let backend_name = backend.name.clone();
        let (parts, body) = response.into_parts();
        let sniffed = a2a::sniff_task_stream(
            body,
            Box::new(move |task_id| state.bind_a2a_task(task_id, backend_name)),
        );
        return Ok(Response::from_parts(parts, sniffed));
    }

    let is_card = context.path == a2a::AGENT_CARD_PATH;
    if !is_card && context.a2a_method.is_none() {
        // Not a JSON-RPC exchange or a card fetch; nothing to learn.
        return Ok(response);
    }

    let (mut parts, body) = response.into_parts();
    let bytes = read_body_limited(&parts.headers, body, server.max_body_bytes)
        .await
        .map_err(|(_, message)| {
            (
                StatusCode::BAD_GATEWAY,
                format!("read A2A upstream response: {message}"),
            )
        })?;
    let Ok(mut value) = serde_json::from_slice::<Value>(&bytes) else {
        return Ok(Response::from_parts(parts, Body::from(bytes)));
    };

    if is_card {
        let scheme = header_value(&context.headers, "x-forwarded-proto").unwrap_or("http");
        // context.host is port-stripped for vhost matching; the card needs
        // the authority exactly as the client addressed the gateway.
        let authority = header_value(&context.headers, "host").unwrap_or(&context.host);
        if a2a::rewrite_card_urls(&mut value, scheme, authority) {
            let body = value.to_string();
            parts.headers.insert(
                http::header::CONTENT_LENGTH,
                HttpHeaderValue::from(body.len()),
            );
            return Ok(Response::from_parts(parts, Body::from(body)));
        }
        return Ok(Response::from_parts(parts, Body::from(bytes)));
    }

    if let Some(task_id) = a2a::response_task_id(&value) {
        server.state.bind_a2a_task(task_id, backend.name.clone());
    }
    Ok(Response::from_parts(parts, Body::from(bytes)))
}

#[allow(clippy::too_many_arguments)]
async fn request_agent_backend(
    server: &ProxyServer,
    cfg: &dxgate_core::RuntimeConfig,
    route: &AgentRoute,
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

    let exchange = if context.protocol == AgentProtocol::Llm {
        Some(prepare_llm_exchange(
            backend, provider, endpoint, context, body,
        )?)
    } else {
        None
    };

    let (uri, out_body) = match &exchange {
        Some(exchange) => (exchange.uri.clone(), exchange.body.clone()),
        None => (
            compose_upstream_uri(endpoint, &context.path_and_query)?,
            body.clone(),
        ),
    };

    let mut headers = parts.headers.clone();
    apply_request_headers(&mut headers, &policy_runtime.request_headers);
    if let Some(exchange) = &exchange {
        if exchange.dialect != LlmDialect::OpenAi {
            // The caller's gateway credential must not leak to a foreign-dialect
            // provider; provider auth is injected below.
            headers.remove(http::header::AUTHORIZATION);
        }
        if exchange.body_rewritten {
            headers.insert(
                http::header::CONTENT_TYPE,
                HttpHeaderValue::from_static("application/json"),
            );
        }
    }
    apply_provider_headers(&mut headers, provider);
    headers.remove(http::header::HOST);
    // The agent path forwards a fully buffered body that may have been
    // rewritten (LLM translation, MCP alias/cursor pages); make the framing
    // headers describe the bytes actually sent.
    headers.remove(http::header::TRANSFER_ENCODING);
    if !out_body.is_empty() || headers.contains_key(http::header::CONTENT_LENGTH) {
        headers.insert(
            http::header::CONTENT_LENGTH,
            HttpHeaderValue::from(out_body.len()),
        );
    }
    inject_trace_context(&mut headers);

    // Agent upstreams are reached over the HTTP/1.1 client pool; echoing the
    // downstream version would break h2c callers of a buffered agent route.
    let mut builder = Request::builder()
        .method(parts.method.clone())
        .uri(uri)
        .version(Version::HTTP_11);
    *builder.headers_mut().unwrap() = headers;
    let request = builder.body(Body::from(out_body)).map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("build upstream request: {e}"),
        )
    })?;

    let fut = server.clients.request_web(request);
    let response = if let Some(timeout) = policy_runtime.timeout {
        time::timeout(timeout, fut).await.map_err(|_| {
            (
                StatusCode::GATEWAY_TIMEOUT,
                format!("backend {} timed out", backend.name),
            )
        })??
    } else {
        fut.await?
    };

    match exchange {
        Some(exchange) => {
            let sink = usage_sink(server, route, backend, &exchange.model, policy_runtime);
            finalize_llm_response(server, response, exchange, sink).await
        }
        None => Ok(response),
    }
}

struct LlmExchange {
    uri: Uri,
    body: Bytes,
    dialect: LlmDialect,
    streaming: bool,
    body_rewritten: bool,
    model: String,
}

// Builds the upstream request for an LLM backend: translates the body for
// native-dialect providers and applies per-backend model rewrites.
fn prepare_llm_exchange(
    backend: &Backend,
    provider: Option<&Provider>,
    endpoint: &str,
    context: &AgentRequestContext,
    body: &Bytes,
) -> Result<LlmExchange, (StatusCode, String)> {
    let dialect = provider
        .map(|provider| llm::dialect_for(provider.kind))
        .unwrap_or(LlmDialect::OpenAi);
    let request_json = if body.is_empty() {
        None
    } else {
        serde_json::from_slice::<Value>(body).ok()
    };
    let streaming = request_json
        .as_ref()
        .map(llm::is_streaming_request)
        .unwrap_or(false);
    let requested_model = context.model.as_deref();
    let effective_model =
        requested_model.map(|model| backend.rewrite_model(model).unwrap_or(model).to_string());
    let model_label = effective_model
        .clone()
        .unwrap_or_else(|| "none".to_string());

    if dialect != LlmDialect::OpenAi {
        if context.path != llm::OPENAI_CHAT_COMPLETIONS_PATH {
            return Err((
                StatusCode::BAD_GATEWAY,
                format!(
                    "backend {} only supports {} for its provider dialect",
                    backend.name,
                    llm::OPENAI_CHAT_COMPLETIONS_PATH
                ),
            ));
        }
        let Some(request_json) = &request_json else {
            return Err((
                StatusCode::BAD_REQUEST,
                "LLM request body must be JSON".to_string(),
            ));
        };
        let Some(model) = effective_model else {
            return Err((
                StatusCode::BAD_REQUEST,
                "LLM request must name a model".to_string(),
            ));
        };
        let (url, translated) = match dialect {
            LlmDialect::Anthropic => (
                llm::anthropic_messages_url(endpoint),
                llm::anthropic_request(request_json, &model),
            ),
            LlmDialect::Gemini => (
                llm::gemini_generate_url(endpoint, &model, streaming),
                llm::gemini_request(request_json),
            ),
            LlmDialect::OpenAi => unreachable!("openai dialect is not translated"),
        };
        let uri = url.parse::<Uri>().map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("invalid upstream uri for backend {}: {e}", backend.name),
            )
        })?;
        return Ok(LlmExchange {
            uri,
            body: Bytes::from(translated.to_string()),
            dialect,
            streaming,
            body_rewritten: true,
            model,
        });
    }

    let uri = compose_upstream_uri(endpoint, &context.path_and_query)?;
    let rewritten = match (&request_json, requested_model, &effective_model) {
        (Some(json), Some(original), Some(effective)) if original != effective => {
            let mut json = json.clone();
            json["model"] = Value::String(effective.clone());
            Some(Bytes::from(json.to_string()))
        }
        _ => None,
    };
    let body_rewritten = rewritten.is_some();
    Ok(LlmExchange {
        uri,
        body: rewritten.unwrap_or_else(|| body.clone()),
        dialect,
        streaming,
        body_rewritten,
        model: model_label,
    })
}

fn usage_sink(
    server: &ProxyServer,
    route: &AgentRoute,
    backend: &Backend,
    model: &str,
    policy_runtime: &PolicyRuntime,
) -> UsageSink {
    let state = server.state.clone();
    let route = route.name.clone();
    let backend = backend.name.clone();
    let model = model.to_string();
    let charges = policy_runtime.token_charges.clone();
    Arc::new(move |usage: LlmUsage| {
        state.record_llm_usage(
            &route,
            &backend,
            &model,
            usage.prompt_tokens,
            usage.completion_tokens,
        );
        for charge in &charges {
            state.add_token_usage(&charge.key, charge.window_seconds, usage.total());
        }
    })
}

async fn read_llm_upstream_body(
    server: &ProxyServer,
    headers: &HeaderMap,
    body: Body,
) -> Result<Bytes, (StatusCode, String)> {
    read_body_limited(headers, body, server.max_body_bytes)
        .await
        .map_err(|(_, message)| {
            (
                StatusCode::BAD_GATEWAY,
                format!("read LLM upstream response: {message}"),
            )
        })
}

// Rewrites the upstream response back into the OpenAI dialect and hooks token
// usage extraction into the body.
async fn finalize_llm_response(
    server: &ProxyServer,
    response: Response<Body>,
    exchange: LlmExchange,
    sink: UsageSink,
) -> Result<Response<Body>, (StatusCode, String)> {
    let (mut parts, body) = response.into_parts();
    match exchange.dialect {
        LlmDialect::OpenAi => {
            if is_event_stream(&parts.headers) {
                // SSE responses carry no content-length, so hyper polls the
                // wrapped stream to its end and the usage hook always runs.
                return Ok(Response::from_parts(
                    parts,
                    llm::observe_openai_body(body, true, sink),
                ));
            }
            match declared_content_length(&parts.headers) {
                // With a content-length, hyper stops polling once those bytes
                // are written, so a stream wrapper would never observe the end
                // of the body. Buffer instead: the bytes are returned unchanged
                // and usage is extracted synchronously.
                Some(length) if length <= server.max_body_bytes => {
                    let bytes = read_llm_upstream_body(server, &parts.headers, body).await?;
                    llm::extract_openai_usage(&bytes, &sink);
                    Ok(Response::from_parts(parts, Body::from(bytes)))
                }
                // Too large to buffer for usage extraction; pass the response
                // through unmetered rather than failing it.
                Some(_) => Ok(Response::from_parts(parts, body)),
                // No declared length: hyper polls the wrapper to end-of-stream,
                // so usage can be observed without buffering.
                None => Ok(Response::from_parts(
                    parts,
                    llm::observe_openai_body(body, false, sink),
                )),
            }
        }
        LlmDialect::Anthropic | LlmDialect::Gemini => {
            if parts.status.is_success() && exchange.streaming && is_event_stream(&parts.headers) {
                parts.headers.remove(http::header::CONTENT_LENGTH);
                let translated = match exchange.dialect {
                    LlmDialect::Anthropic => {
                        llm::transcode_anthropic_stream(body, exchange.model, sink)
                    }
                    _ => llm::transcode_gemini_stream(body, exchange.model, sink),
                };
                return Ok(Response::from_parts(parts, translated));
            }

            let bytes = read_llm_upstream_body(server, &parts.headers, body).await?;
            let value = serde_json::from_slice::<Value>(&bytes).map_err(|e| {
                (
                    StatusCode::BAD_GATEWAY,
                    format!("parse LLM upstream response: {e}"),
                )
            })?;
            let translated = if parts.status.is_success() {
                let (translated, usage) = match exchange.dialect {
                    LlmDialect::Anthropic => llm::openai_from_anthropic_response(&value),
                    _ => llm::openai_from_gemini_response(&value, &exchange.model),
                };
                sink(usage);
                translated
            } else if exchange.dialect == LlmDialect::Anthropic {
                llm::openai_error_from_anthropic(&value, "upstream request failed")
            } else {
                // Gemini errors already use an {"error": {...}} envelope.
                value
            };
            parts.headers.remove(http::header::CONTENT_LENGTH);
            parts.headers.insert(
                http::header::CONTENT_TYPE,
                HttpHeaderValue::from_static("application/json"),
            );
            Ok(Response::from_parts(
                parts,
                Body::from(translated.to_string()),
            ))
        }
    }
}

// Caps how many pages of one backend's list a single federated call drains,
// bounding fan-out amplification from a misbehaving cursor loop.
const MCP_FEDERATION_MAX_PAGES: usize = 32;

// Fans a JSON-RPC list call out to every eligible backend (route-declared
// order), draining each backend's pagination, and returns the merged result.
// The merged list carries no cursor, so clients never page the aggregate.
#[allow(clippy::too_many_arguments)]
async fn federate_mcp_list(
    server: &ProxyServer,
    cfg: &dxgate_core::RuntimeConfig,
    route: &AgentRoute,
    ordered: &[WeightedBackend],
    parts: &http::request::Parts,
    body: &Bytes,
    context: &AgentRequestContext,
    policy_runtime: &PolicyRuntime,
    spec: &mcp::ListSpec,
) -> Result<Response<Body>, (StatusCode, String)> {
    let request_json = serde_json::from_slice::<Value>(body).ok();
    let mut merged = Vec::new();
    let mut seen = BTreeSet::new();
    let mut failures = Vec::new();

    for weighted in ordered {
        let Some(backend) = cfg.backend(&weighted.name) else {
            continue;
        };
        let mut cursor: Option<String> = None;
        for _ in 0..MCP_FEDERATION_MAX_PAGES {
            let page_body = match (&cursor, &request_json) {
                (Some(cursor), Some(json)) => {
                    Bytes::from(mcp::with_cursor(json, cursor).to_string())
                }
                _ => body.clone(),
            };
            let started = Instant::now();
            let upstream_span = tracing::info_span!(
                "dxgate.agent.upstream",
                protocol = protocol_name(context.protocol),
                route = %route.name,
                backend = %backend.name,
                http.status_code = tracing::field::Empty
            );
            let outcome = request_agent_backend(
                server,
                cfg,
                route,
                backend,
                parts,
                &page_body,
                context,
                policy_runtime,
            )
            .instrument(upstream_span.clone())
            .await;
            let response = match outcome {
                Ok(response) => response,
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
                    break;
                }
            };
            let status = response.status();
            upstream_span.record("http.status_code", status.as_u16());
            server.state.record_agent_request(
                protocol_name(context.protocol),
                &route.name,
                &backend.name,
                status.as_u16(),
                started.elapsed().as_millis() as u64,
            );
            let (response_parts, response_body) = response.into_parts();
            let bytes = match read_body_limited(
                &response_parts.headers,
                response_body,
                server.max_body_bytes,
            )
            .await
            {
                Ok(bytes) => bytes,
                Err((_, message)) => {
                    failures.push(format!("{}: {message}", backend.name));
                    break;
                }
            };
            if !status.is_success() {
                failures.push(format!("{} returned {}", backend.name, status));
                break;
            }
            let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
                failures.push(format!("{}: response is not JSON", backend.name));
                break;
            };
            if let Some(items) = value
                .pointer(&format!("/result/{}", spec.result_key))
                .and_then(Value::as_array)
            {
                mcp::merge_list_items(&mut merged, &mut seen, items, spec, &backend.name);
            }
            match mcp::next_cursor(&value) {
                // Paging rewrites the request body, which requires it to be JSON.
                Some(next) if request_json.is_some() => cursor = Some(next),
                _ => break,
            }
        }
    }

    if merged.is_empty() && !failures.is_empty() {
        return Err((StatusCode::BAD_GATEWAY, failures.join("; ")));
    }

    let id = request_json
        .as_ref()
        .and_then(|value| value.get("id").cloned())
        .unwrap_or(Value::Null);
    let mut result = serde_json::Map::new();
    result.insert(spec.result_key.to_string(), Value::Array(merged));
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": Value::Object(result)
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

// Buffers a successful initialize response and fills in the capability keys
// that federation makes true at the gateway level.
async fn augment_initialize_response(
    server: &ProxyServer,
    response: Response<Body>,
) -> Result<Response<Body>, (StatusCode, String)> {
    if !response.status().is_success() || is_event_stream(response.headers()) {
        return Ok(response);
    }
    let (mut parts, body) = response.into_parts();
    let bytes = read_body_limited(&parts.headers, body, server.max_body_bytes)
        .await
        .map_err(|(_, message)| {
            (
                StatusCode::BAD_GATEWAY,
                format!("read MCP initialize response: {message}"),
            )
        })?;
    let Ok(mut value) = serde_json::from_slice::<Value>(&bytes) else {
        return Ok(Response::from_parts(parts, Body::from(bytes)));
    };
    if !mcp::augment_initialize_capabilities(&mut value) {
        return Ok(Response::from_parts(parts, Body::from(bytes)));
    }
    let body = value.to_string();
    parts.headers.insert(
        http::header::CONTENT_LENGTH,
        HttpHeaderValue::from(body.len()),
    );
    Ok(Response::from_parts(parts, Body::from(body)))
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
    a2a_method: Option<String>,
    a2a_task_id: Option<String>,
    stream_hint: bool,
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
        let a2a_method = if protocol == AgentProtocol::A2a {
            json.as_ref()
                .and_then(|value| value.get("method"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        } else {
            None
        };
        let a2a_task_id = if protocol == AgentProtocol::A2a {
            json.as_ref()
                .and_then(a2a::request_task_id)
                .map(ToString::to_string)
        } else {
            None
        };
        // MCP and A2A both stream over SSE; the hint drives anti-buffering
        // response headers. A GET only implies a stream for MCP (its
        // Streamable HTTP listen channel) — an A2A GET is a card fetch.
        let accepts_sse =
            header_contains(&parts.headers, http::header::ACCEPT, "text/event-stream");
        let stream_hint = match protocol {
            AgentProtocol::Mcp => parts.method == Method::GET || accepts_sse,
            AgentProtocol::A2a => accepts_sse,
            _ => false,
        };

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
            a2a_method,
            a2a_task_id,
            stream_hint,
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

fn a2a_task_bound_backend(
    server: &ProxyServer,
    context: &AgentRequestContext,
    eligible: &[WeightedBackend],
) -> Option<WeightedBackend> {
    if context.protocol != AgentProtocol::A2a {
        return None;
    }
    let backend_name = context
        .a2a_task_id
        .as_deref()
        .and_then(|task_id| server.state.a2a_task_backend(task_id))?;
    eligible
        .iter()
        .find(|weighted| weighted.name == backend_name)
        .cloned()
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

fn apply_stream_headers(headers: &mut HeaderMap, context: &AgentRequestContext) {
    if !context.stream_hint {
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

#[derive(Debug, Clone)]
struct PolicyRuntime {
    request_headers: HeaderTransform,
    response_headers: HeaderTransform,
    timeout: Option<Duration>,
    retry: Option<RetryPolicy>,
    token_charges: Vec<TokenCharge>,
}

// A token-limit bucket this request's usage must be charged against.
#[derive(Debug, Clone)]
struct TokenCharge {
    key: String,
    window_seconds: u64,
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
        token_charges: Vec::new(),
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
        if let Some(token_limit) = &policy.token_limit {
            let key = format!(
                "tokens:{}",
                rate_limit_key(token_limit.key, &policy.name, route, backend, context)
            );
            if !server.state.check_token_limit(&key, token_limit) {
                server.state.record_policy_denied();
                return Err((
                    StatusCode::TOO_MANY_REQUESTS,
                    format!("token limit exceeded by policy {}", policy.name),
                ));
            }
            runtime.token_charges.push(TokenCharge {
                key,
                window_seconds: token_limit.window_seconds,
            });
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

fn backend_supports_llm_request(
    cfg: &dxgate_core::RuntimeConfig,
    backend: &Backend,
    context: &AgentRequestContext,
) -> bool {
    if context.protocol != AgentProtocol::Llm {
        return true;
    }
    let Some(provider) = backend_provider(cfg, backend) else {
        return true;
    };
    match llm::dialect_for(provider.kind) {
        LlmDialect::OpenAi => true,
        // Native dialects are only translated for chat completions.
        _ => context.path == llm::OPENAI_CHAT_COMPLETIONS_PATH,
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

#[cfg(test)]
mod tests {
    use super::access_log::AccessLogFormat;
    use super::upstream::{mtls_cache_key, GrpcBootstrap, MtlsClientPool};
    use super::*;
    use dxgate_core::{UpstreamTls, UpstreamTlsMode};
    use hyper::body;
    use rcgen::{
        BasicConstraints, Certificate as RcgenCertificate, CertificateParams, DistinguishedName,
        DnType, IsCa,
    };
    use rustls::{Certificate, PrivateKey, RootCertStore};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    #[test]
    fn max_body_bytes_parses_env_values() {
        assert_eq!(parse_max_body_bytes(None), DEFAULT_MAX_BODY_BYTES);
        assert_eq!(parse_max_body_bytes(Some("")), DEFAULT_MAX_BODY_BYTES);
        assert_eq!(parse_max_body_bytes(Some("0")), DEFAULT_MAX_BODY_BYTES);
        assert_eq!(parse_max_body_bytes(Some("abc")), DEFAULT_MAX_BODY_BYTES);
        assert_eq!(parse_max_body_bytes(Some(" 4096 ")), 4096);
    }

    #[tokio::test]
    async fn read_body_limited_rejects_oversized_content_length() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_LENGTH,
            HttpHeaderValue::from_static("32"),
        );

        let (status, _) = read_body_limited(&headers, Body::from("ignored"), 16)
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn read_body_limited_rejects_oversized_stream_without_content_length() {
        let (mut sender, body) = Body::channel();
        let writer = tokio::spawn(async move {
            for _ in 0..4 {
                if sender.send_data(Bytes::from(vec![0u8; 8])).await.is_err() {
                    return;
                }
            }
        });

        let (status, _) = read_body_limited(&HeaderMap::new(), body, 16)
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn read_body_limited_passes_body_within_limit() {
        let bytes = read_body_limited(&HeaderMap::new(), Body::from("hello"), 16)
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"hello");
    }

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
            .client_for(
                &UpstreamTls {
                    mode: UpstreamTlsMode::DubboMutual,
                    sni: Some("nginx.app.svc.cluster.local".into()),
                    certificate_provider: None,
                    validation_provider: None,
                    alpn_protocols: vec!["h2".into()],
                },
                false,
            )
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
