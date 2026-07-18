//! The parsed per-request agent context: protocol, routing keys, and the
//! protocol-specific fields (model / tool / MCP session / A2A task) extracted
//! from the request parts and body, plus SSE anti-buffering response headers.

use super::headers::header_contains;
use super::routing::{header_pairs, host_header};
use super::MCP_SESSION_ID_HEADER;
use crate::a2a;
use axum::http::{HeaderMap, HeaderName, HeaderValue as HttpHeaderValue, Method};
use dxgate_core::{AgentMatchInput, AgentProtocol};
use hyper::body::Bytes;
use serde_json::Value;

#[derive(Debug, Clone)]
pub(super) struct AgentRequestContext {
    pub(super) protocol: AgentProtocol,
    pub(super) host: String,
    pub(super) path: String,
    pub(super) path_and_query: String,
    pub(super) method: Method,
    pub(super) model: Option<String>,
    pub(super) tool: Option<String>,
    pub(super) agent: Option<String>,
    pub(super) mcp_method: Option<String>,
    pub(super) mcp_session_id: Option<String>,
    pub(super) a2a_method: Option<String>,
    pub(super) a2a_task_id: Option<String>,
    pub(super) stream_hint: bool,
    pub(super) headers: Vec<(String, String)>,
}

impl AgentRequestContext {
    pub(super) fn new(protocol: AgentProtocol, parts: &http::request::Parts, body: &Bytes) -> Self {
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

    pub(super) fn input(&self) -> AgentMatchInput<'_> {
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

pub(super) fn apply_stream_headers(headers: &mut HeaderMap, context: &AgentRequestContext) {
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
