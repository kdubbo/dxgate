//! Pure routing/backend helpers: provider lookup, protocol matching, upstream
//! URI composition, TLS-mode classification, and header extraction. These
//! depend only on config types and `HeaderMap`, not on server state.

use axum::http::{HeaderMap, StatusCode, Uri};
use dxgate_core::{
    AgentProtocol, Backend, BackendKind, Endpoint, Provider, UpstreamTls, UpstreamTlsMode,
};

pub(super) fn backend_provider<'a>(
    cfg: &'a dxgate_core::RuntimeConfig,
    backend: &'a Backend,
) -> Option<&'a Provider> {
    match &backend.kind {
        BackendKind::Llm { provider, .. } => cfg.provider(provider),
        _ => None,
    }
}

pub(super) fn backend_matches_protocol(backend: &Backend, protocol: AgentProtocol) -> bool {
    matches!(
        (&backend.kind, protocol),
        (BackendKind::Http { .. }, AgentProtocol::Http)
            | (BackendKind::Llm { .. }, AgentProtocol::Llm)
            | (BackendKind::Mcp { .. }, AgentProtocol::Mcp)
            | (BackendKind::A2a { .. }, AgentProtocol::A2a)
    )
}

pub(super) fn compose_upstream_uri(
    endpoint: &str,
    path_and_query: &str,
) -> Result<Uri, (StatusCode, String)> {
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

pub(super) fn protocol_name(protocol: AgentProtocol) -> &'static str {
    match protocol {
        AgentProtocol::Http => "http",
        AgentProtocol::Llm => "llm",
        AgentProtocol::Mcp => "mcp",
        AgentProtocol::A2a => "a2a",
    }
}

pub(super) fn endpoint_authority(endpoint: &Endpoint) -> String {
    if endpoint.address.contains(':') && !endpoint.address.starts_with('[') {
        format!("[{}]:{}", endpoint.address, endpoint.port)
    } else {
        format!("{}:{}", endpoint.address, endpoint.port)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UpstreamRequestMode {
    PlainHttp,
    SimpleTls,
    DubboMutual,
}

pub(super) fn upstream_request_mode(tls: Option<&UpstreamTls>) -> UpstreamRequestMode {
    match tls.map(|tls| tls.mode) {
        None => UpstreamRequestMode::PlainHttp,
        Some(UpstreamTlsMode::Simple) => UpstreamRequestMode::SimpleTls,
        Some(UpstreamTlsMode::DubboMutual) => UpstreamRequestMode::DubboMutual,
    }
}

pub(super) fn host_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(|host| host.split(':').next().unwrap_or(host))
}

pub(super) fn header_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
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
