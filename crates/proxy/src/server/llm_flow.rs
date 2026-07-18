//! OpenAI-compatible LLM request/response handling: builds the upstream
//! exchange (translating native dialects and applying model rewrites), wires
//! token-usage metering, and rewrites the upstream response back to OpenAI.

use super::context::AgentRequestContext;
use super::detect::{declared_content_length, is_event_stream};
use super::policy::PolicyRuntime;
use super::routing::compose_upstream_uri;
use super::{read_body_limited, ProxyServer};
use crate::llm::{self, LlmDialect, LlmUsage, UsageSink};
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue as HttpHeaderValue, Response, StatusCode, Uri};
use dxgate_core::{AgentRoute, Backend, Provider};
use hyper::body::Bytes;
use serde_json::Value;
use std::sync::Arc;

pub(super) struct LlmExchange {
    pub(super) uri: Uri,
    pub(super) body: Bytes,
    pub(super) dialect: LlmDialect,
    pub(super) streaming: bool,
    pub(super) body_rewritten: bool,
    pub(super) model: String,
}

// Builds the upstream request for an LLM backend: translates the body for
// native-dialect providers and applies per-backend model rewrites.
pub(super) fn prepare_llm_exchange(
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

pub(super) fn usage_sink(
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
pub(super) async fn finalize_llm_response(
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
