//! Request classification: content-type/stream detection and agent-protocol
//! routing by path. Pure functions over headers and the request path.

use super::headers::header_contains;
use axum::http::HeaderMap;
use dxgate_core::AgentProtocol;

pub(super) fn is_event_stream(headers: &HeaderMap) -> bool {
    header_contains(headers, http::header::CONTENT_TYPE, "text/event-stream")
}

pub(super) fn declared_content_length(headers: &HeaderMap) -> Option<usize> {
    headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
}

// gRPC and Dubbo Triple mark themselves via content-type and only run over
// HTTP/2. grpc-web is excluded on purpose: it is designed to cross HTTP/1
// intermediaries with trailers encoded in the body.
pub(super) fn is_grpc_request(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let content_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim();
    content_type == "application/grpc"
        || content_type.starts_with("application/grpc+")
        || content_type == "application/triple"
        || content_type.starts_with("application/triple+")
}

const LLM_API_PATHS: [&str; 5] = [
    "/v1/chat/completions",
    "/v1/completions",
    "/v1/embeddings",
    "/v1/models",
    "/v1/responses",
];

pub(super) fn detect_agent_protocol(path: &str) -> Option<AgentProtocol> {
    if LLM_API_PATHS.contains(&path) || path.starts_with("/v1/models/") {
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
