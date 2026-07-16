//! OpenAI-compatible dialect translation for LLM providers.
//!
//! Callers speak the OpenAI chat-completions API to dxgate; this module
//! rewrites requests/responses (including SSE streams) for providers with a
//! native wire format, and extracts token usage for metrics and token limits.

use axum::body::Body;
use hyper::body::Bytes;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub const OPENAI_CHAT_COMPLETIONS_PATH: &str = "/v1/chat/completions";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmDialect {
    OpenAi,
    Anthropic,
    Gemini,
}

pub fn dialect_for(kind: dxgate_core::ProviderKind) -> LlmDialect {
    use dxgate_core::ProviderKind;
    match kind {
        ProviderKind::OpenAiCompatible | ProviderKind::OpenAi | ProviderKind::DeepSeek => {
            LlmDialect::OpenAi
        }
        ProviderKind::Anthropic => LlmDialect::Anthropic,
        ProviderKind::Gemini => LlmDialect::Gemini,
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LlmUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

impl LlmUsage {
    pub fn total(&self) -> u64 {
        self.prompt_tokens + self.completion_tokens
    }

    fn is_zero(&self) -> bool {
        self.total() == 0
    }

    fn from_openai(value: &Value) -> Option<Self> {
        let usage = value.get("usage")?;
        Some(Self {
            prompt_tokens: usage.get("prompt_tokens")?.as_u64().unwrap_or(0),
            completion_tokens: usage
                .get("completion_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        })
    }
}

pub type UsageSink = Arc<dyn Fn(LlmUsage) + Send + Sync>;

pub fn is_streaming_request(body: &Value) -> bool {
    body.get("stream").and_then(Value::as_bool).unwrap_or(false)
}

fn now_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn join_text_parts(parts: &[Value]) -> String {
    parts
        .iter()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect()
}

// Flattens OpenAI message content, which is either a string or an array of
// typed parts, into plain text for providers without multi-part support.
fn message_text(message: &Value) -> String {
    match message.get("content") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => join_text_parts(parts),
        _ => String::new(),
    }
}

fn stop_sequences(body: &Value) -> Option<Value> {
    match body.get("stop") {
        Some(Value::String(stop)) => Some(json!([stop])),
        Some(Value::Array(stops)) => Some(Value::Array(stops.clone())),
        _ => None,
    }
}

fn max_output_tokens(body: &Value) -> u64 {
    body.get("max_tokens")
        .or_else(|| body.get("max_completion_tokens"))
        .and_then(Value::as_u64)
        // Anthropic requires max_tokens; OpenAI callers often omit it.
        .unwrap_or(4096)
}

// ---------------------------------------------------------------------------
// Anthropic
// ---------------------------------------------------------------------------

pub fn anthropic_messages_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with("/v1") {
        format!("{base}/messages")
    } else {
        format!("{base}/v1/messages")
    }
}

pub fn anthropic_request(body: &Value, model: &str) -> Value {
    let empty = Vec::new();
    let messages = body
        .get("messages")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    let mut system = Vec::new();
    let mut converted = Vec::new();
    for message in messages {
        let role = message.get("role").and_then(Value::as_str).unwrap_or("");
        let text = message_text(message);
        match role {
            "system" | "developer" => system.push(text),
            "assistant" => converted.push(json!({ "role": "assistant", "content": text })),
            _ => converted.push(json!({ "role": "user", "content": text })),
        }
    }

    let mut request = json!({
        "model": model,
        "messages": converted,
        "max_tokens": max_output_tokens(body),
    });
    if !system.is_empty() {
        request["system"] = Value::String(system.join("\n"));
    }
    for field in ["temperature", "top_p"] {
        if let Some(value) = body.get(field) {
            request[field] = value.clone();
        }
    }
    if let Some(stops) = stop_sequences(body) {
        request["stop_sequences"] = stops;
    }
    if is_streaming_request(body) {
        request["stream"] = Value::Bool(true);
    }
    request
}

fn anthropic_finish_reason(stop_reason: Option<&str>) -> &'static str {
    match stop_reason {
        Some("max_tokens") => "length",
        Some("tool_use") => "tool_calls",
        _ => "stop",
    }
}

pub fn openai_from_anthropic_response(value: &Value) -> (Value, LlmUsage) {
    let content = value
        .get("content")
        .and_then(Value::as_array)
        .map(|blocks| join_text_parts(blocks))
        .unwrap_or_default();
    let usage = LlmUsage {
        prompt_tokens: read_u64(value, "/usage/input_tokens"),
        completion_tokens: read_u64(value, "/usage/output_tokens"),
    };
    let response = json!({
        "id": value.get("id").cloned().unwrap_or(Value::Null),
        "object": "chat.completion",
        "created": now_epoch_seconds(),
        "model": value.get("model").cloned().unwrap_or(Value::Null),
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": content },
            "finish_reason": anthropic_finish_reason(
                value.get("stop_reason").and_then(Value::as_str)
            ),
        }],
        "usage": {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total(),
        },
    });
    (response, usage)
}

pub fn openai_error_from_anthropic(value: &Value, fallback: &str) -> Value {
    let message = value
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .unwrap_or(fallback);
    let kind = value
        .get("error")
        .and_then(|error| error.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("upstream_error");
    json!({ "error": { "message": message, "type": kind } })
}

// ---------------------------------------------------------------------------
// Gemini
// ---------------------------------------------------------------------------

pub fn gemini_generate_url(base: &str, model: &str, stream: bool) -> String {
    let base = base.trim_end_matches('/');
    if stream {
        format!("{base}/models/{model}:streamGenerateContent?alt=sse")
    } else {
        format!("{base}/models/{model}:generateContent")
    }
}

pub fn gemini_request(body: &Value) -> Value {
    let empty = Vec::new();
    let messages = body
        .get("messages")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    let mut system = Vec::new();
    let mut contents = Vec::new();
    for message in messages {
        let role = message.get("role").and_then(Value::as_str).unwrap_or("");
        let text = message_text(message);
        match role {
            "system" | "developer" => system.push(text),
            "assistant" => contents.push(json!({ "role": "model", "parts": [{ "text": text }] })),
            _ => contents.push(json!({ "role": "user", "parts": [{ "text": text }] })),
        }
    }

    let mut generation = serde_json::Map::new();
    if let Some(value) = body.get("temperature") {
        generation.insert("temperature".into(), value.clone());
    }
    if let Some(value) = body.get("top_p") {
        generation.insert("topP".into(), value.clone());
    }
    if body.get("max_tokens").is_some() || body.get("max_completion_tokens").is_some() {
        generation.insert("maxOutputTokens".into(), json!(max_output_tokens(body)));
    }
    if let Some(stops) = stop_sequences(body) {
        generation.insert("stopSequences".into(), stops);
    }

    let mut request = json!({ "contents": contents });
    if !system.is_empty() {
        request["systemInstruction"] = json!({ "parts": [{ "text": system.join("\n") }] });
    }
    if !generation.is_empty() {
        request["generationConfig"] = Value::Object(generation);
    }
    request
}

fn gemini_finish_reason(reason: Option<&str>) -> &'static str {
    match reason {
        Some("MAX_TOKENS") => "length",
        Some("SAFETY") | Some("RECITATION") | Some("PROHIBITED_CONTENT") => "content_filter",
        _ => "stop",
    }
}

fn gemini_candidate_text(value: &Value) -> String {
    value
        .pointer("/candidates/0/content/parts")
        .and_then(Value::as_array)
        .map(|parts| join_text_parts(parts))
        .unwrap_or_default()
}

fn gemini_usage(value: &Value) -> LlmUsage {
    LlmUsage {
        prompt_tokens: read_u64(value, "/usageMetadata/promptTokenCount"),
        completion_tokens: read_u64(value, "/usageMetadata/candidatesTokenCount"),
    }
}

pub fn openai_from_gemini_response(value: &Value, model: &str) -> (Value, LlmUsage) {
    let usage = gemini_usage(value);
    let response = json!({
        "id": value.get("responseId").cloned().unwrap_or(Value::Null),
        "object": "chat.completion",
        "created": now_epoch_seconds(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": gemini_candidate_text(value) },
            "finish_reason": gemini_finish_reason(
                value.pointer("/candidates/0/finishReason").and_then(Value::as_str)
            ),
        }],
        "usage": {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total(),
        },
    });
    (response, usage)
}

fn read_u64(value: &Value, pointer: &str) -> u64 {
    value.pointer(pointer).and_then(Value::as_u64).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// SSE stream transcoding
// ---------------------------------------------------------------------------

trait Transcode: Send + 'static {
    // Consumes an upstream chunk and returns bytes to emit downstream.
    fn push(&mut self, chunk: &[u8]) -> Vec<u8>;
    // Called once when the upstream body ends.
    fn finish(&mut self) -> Vec<u8>;
}

fn wrap_body<T: Transcode>(upstream: Body, transcoder: T) -> Body {
    use hyper::body::HttpBody;

    struct State<T> {
        upstream: Body,
        transcoder: T,
        done: bool,
    }

    let state = State {
        upstream,
        transcoder,
        done: false,
    };
    Body::wrap_stream(futures_util::stream::unfold(
        state,
        |mut state| async move {
            if state.done {
                return None;
            }
            loop {
                match state.upstream.data().await {
                    Some(Ok(chunk)) => {
                        let out = state.transcoder.push(&chunk);
                        if !out.is_empty() {
                            return Some((Ok(Bytes::from(out)), state));
                        }
                    }
                    Some(Err(err)) => {
                        state.done = true;
                        return Some((Err(err), state));
                    }
                    None => {
                        state.done = true;
                        let out = state.transcoder.finish();
                        if !out.is_empty() {
                            return Some((Ok(Bytes::from(out)), state));
                        }
                        return None;
                    }
                }
            }
        },
    ))
}

// Splits an SSE byte stream into complete `data:` payload lines.
#[derive(Default)]
struct SseLines {
    buf: Vec<u8>,
}

impl SseLines {
    fn push(&mut self, chunk: &[u8]) -> Vec<String> {
        self.buf.extend_from_slice(chunk);
        let mut lines = Vec::new();
        while let Some(pos) = self.buf.iter().position(|b| *b == b'\n') {
            let raw: Vec<u8> = self.buf.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&raw);
            let line = line.trim_end_matches(['\n', '\r']);
            if let Some(data) = line.strip_prefix("data:") {
                lines.push(data.trim_start().to_string());
            }
        }
        lines
    }
}

fn openai_chunk(id: &str, created: u64, model: &str, delta: Value, finish: Option<&str>) -> String {
    let chunk = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish.map(Value::from).unwrap_or(Value::Null),
        }],
    });
    format!("data: {chunk}\n\n")
}

struct AnthropicStream {
    lines: SseLines,
    id: String,
    model: String,
    created: u64,
    usage: LlmUsage,
    sink: Option<UsageSink>,
}

impl AnthropicStream {
    fn new(model: String, sink: UsageSink) -> Self {
        Self {
            lines: SseLines::default(),
            id: String::new(),
            model,
            created: now_epoch_seconds(),
            usage: LlmUsage::default(),
            sink: Some(sink),
        }
    }

    fn emit_usage(&mut self) {
        if let Some(sink) = self.sink.take() {
            sink(self.usage);
        }
    }
}

impl Transcode for AnthropicStream {
    fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        let mut out = String::new();
        for line in self.lines.push(chunk) {
            let Ok(event) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            match event.get("type").and_then(Value::as_str) {
                Some("message_start") => {
                    if let Some(id) = event.pointer("/message/id").and_then(Value::as_str) {
                        self.id = id.to_string();
                    }
                    if let Some(model) = event.pointer("/message/model").and_then(Value::as_str) {
                        self.model = model.to_string();
                    }
                    self.usage.prompt_tokens = read_u64(&event, "/message/usage/input_tokens");
                    out.push_str(&openai_chunk(
                        &self.id,
                        self.created,
                        &self.model,
                        json!({ "role": "assistant", "content": "" }),
                        None,
                    ));
                }
                Some("content_block_delta") => {
                    if let Some(text) = event.pointer("/delta/text").and_then(Value::as_str) {
                        out.push_str(&openai_chunk(
                            &self.id,
                            self.created,
                            &self.model,
                            json!({ "content": text }),
                            None,
                        ));
                    }
                }
                Some("message_delta") => {
                    let output = read_u64(&event, "/usage/output_tokens");
                    if output > 0 {
                        self.usage.completion_tokens = output;
                    }
                    let stop = event.pointer("/delta/stop_reason").and_then(Value::as_str);
                    if stop.is_some() {
                        out.push_str(&openai_chunk(
                            &self.id,
                            self.created,
                            &self.model,
                            json!({}),
                            Some(anthropic_finish_reason(stop)),
                        ));
                    }
                }
                Some("message_stop") => {
                    out.push_str("data: [DONE]\n\n");
                    self.emit_usage();
                }
                _ => {}
            }
        }
        out.into_bytes()
    }

    fn finish(&mut self) -> Vec<u8> {
        self.emit_usage();
        Vec::new()
    }
}

pub fn transcode_anthropic_stream(upstream: Body, model: String, sink: UsageSink) -> Body {
    wrap_body(upstream, AnthropicStream::new(model, sink))
}

struct GeminiStream {
    lines: SseLines,
    model: String,
    created: u64,
    usage: LlmUsage,
    started: bool,
    finished: bool,
    sink: Option<UsageSink>,
}

impl GeminiStream {
    fn new(model: String, sink: UsageSink) -> Self {
        Self {
            lines: SseLines::default(),
            model,
            created: now_epoch_seconds(),
            usage: LlmUsage::default(),
            started: false,
            finished: false,
            sink: Some(sink),
        }
    }
}

impl Transcode for GeminiStream {
    fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        let mut out = String::new();
        for line in self.lines.push(chunk) {
            let Ok(event) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            // usageMetadata is cumulative; the last observed value wins.
            let usage = gemini_usage(&event);
            if !usage.is_zero() {
                self.usage = usage;
            }
            let text = gemini_candidate_text(&event);
            if !text.is_empty() {
                if !self.started {
                    // OpenAI streams open with a role-only delta chunk.
                    self.started = true;
                    out.push_str(&openai_chunk(
                        "",
                        self.created,
                        &self.model,
                        json!({ "role": "assistant", "content": "" }),
                        None,
                    ));
                }
                out.push_str(&openai_chunk(
                    "",
                    self.created,
                    &self.model,
                    json!({ "content": text }),
                    None,
                ));
            }
            if let Some(reason) = event
                .pointer("/candidates/0/finishReason")
                .and_then(Value::as_str)
            {
                self.finished = true;
                out.push_str(&openai_chunk(
                    "",
                    self.created,
                    &self.model,
                    json!({}),
                    Some(gemini_finish_reason(Some(reason))),
                ));
            }
        }
        out.into_bytes()
    }

    fn finish(&mut self) -> Vec<u8> {
        if let Some(sink) = self.sink.take() {
            sink(self.usage);
        }
        if self.finished {
            "data: [DONE]\n\n".as_bytes().to_vec()
        } else {
            Vec::new()
        }
    }
}

pub fn transcode_gemini_stream(upstream: Body, model: String, sink: UsageSink) -> Body {
    wrap_body(upstream, GeminiStream::new(model, sink))
}

// Passes an OpenAI-format response through unchanged while capturing token
// usage: from the body JSON for non-streaming responses, or from the final
// usage-bearing SSE chunk (stream_options.include_usage) for streams.
const USAGE_CAPTURE_LIMIT: usize = 512 * 1024;

struct OpenAiObserver {
    streaming: bool,
    captured: Vec<u8>,
    truncated: bool,
    sink: Option<UsageSink>,
}

impl OpenAiObserver {
    fn new(streaming: bool, sink: UsageSink) -> Self {
        Self {
            streaming,
            captured: Vec::new(),
            truncated: false,
            sink: Some(sink),
        }
    }
}

impl Transcode for OpenAiObserver {
    fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        if self.captured.len() + chunk.len() <= USAGE_CAPTURE_LIMIT {
            self.captured.extend_from_slice(chunk);
        } else if self.streaming {
            // Keep only the tail: the usage chunk arrives last in a stream.
            self.captured.clear();
            self.captured.extend_from_slice(chunk);
        } else {
            self.truncated = true;
        }
        chunk.to_vec()
    }

    fn finish(&mut self) -> Vec<u8> {
        let Some(sink) = self.sink.take() else {
            return Vec::new();
        };
        if self.streaming {
            let mut lines = SseLines::default();
            let mut found = LlmUsage::default();
            for line in lines.push(&self.captured) {
                if !line.contains("\"usage\"") {
                    continue;
                }
                if let Ok(value) = serde_json::from_str::<Value>(&line) {
                    if let Some(usage) = LlmUsage::from_openai(&value) {
                        found = usage;
                    }
                }
            }
            if !found.is_zero() {
                sink(found);
            }
        } else if !self.truncated {
            extract_openai_usage(&self.captured, &sink);
        }
        Vec::new()
    }
}

pub fn observe_openai_body(upstream: Body, streaming: bool, sink: UsageSink) -> Body {
    wrap_body(upstream, OpenAiObserver::new(streaming, sink))
}

// Extracts usage from a buffered, non-streaming OpenAI-format response.
pub fn extract_openai_usage(bytes: &[u8], sink: &UsageSink) {
    let usage = serde_json::from_slice::<Value>(bytes)
        .ok()
        .as_ref()
        .and_then(LlmUsage::from_openai)
        .unwrap_or_default();
    if !usage.is_zero() {
        sink(usage);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::body;

    #[test]
    fn anthropic_request_maps_system_messages_and_defaults() {
        let body = json!({
            "model": "ignored",
            "messages": [
                { "role": "system", "content": "be brief" },
                { "role": "user", "content": [{ "type": "text", "text": "hi" }] },
                { "role": "assistant", "content": "hello" }
            ],
            "temperature": 0.3,
            "stop": "END",
            "stream": true
        });
        let out = anthropic_request(&body, "claude-3");
        assert_eq!(out["model"], "claude-3");
        assert_eq!(out["system"], "be brief");
        assert_eq!(out["max_tokens"], 4096);
        assert_eq!(out["temperature"], 0.3);
        assert_eq!(out["stop_sequences"], json!(["END"]));
        assert_eq!(out["stream"], true);
        assert_eq!(out["messages"][0]["role"], "user");
        assert_eq!(out["messages"][0]["content"], "hi");
        assert_eq!(out["messages"][1]["role"], "assistant");
    }

    #[test]
    fn anthropic_response_maps_to_openai_shape() {
        let value = json!({
            "id": "msg_1",
            "model": "claude-3",
            "content": [{ "type": "text", "text": "hello" }],
            "stop_reason": "max_tokens",
            "usage": { "input_tokens": 11, "output_tokens": 7 }
        });
        let (out, usage) = openai_from_anthropic_response(&value);
        assert_eq!(out["object"], "chat.completion");
        assert_eq!(out["choices"][0]["message"]["content"], "hello");
        assert_eq!(out["choices"][0]["finish_reason"], "length");
        assert_eq!(out["usage"]["total_tokens"], 18);
        assert_eq!(
            usage,
            LlmUsage {
                prompt_tokens: 11,
                completion_tokens: 7
            }
        );
    }

    #[test]
    fn gemini_request_and_response_round_trip() {
        let body = json!({
            "messages": [
                { "role": "system", "content": "be brief" },
                { "role": "user", "content": "hi" }
            ],
            "max_tokens": 64
        });
        let out = gemini_request(&body);
        assert_eq!(out["contents"][0]["role"], "user");
        assert_eq!(out["contents"][0]["parts"][0]["text"], "hi");
        assert_eq!(out["systemInstruction"]["parts"][0]["text"], "be brief");
        assert_eq!(out["generationConfig"]["maxOutputTokens"], 64);

        let reply = json!({
            "candidates": [{
                "content": { "parts": [{ "text": "hello" }], "role": "model" },
                "finishReason": "STOP"
            }],
            "usageMetadata": { "promptTokenCount": 5, "candidatesTokenCount": 3 }
        });
        let (mapped, usage) = openai_from_gemini_response(&reply, "gemini-pro");
        assert_eq!(mapped["choices"][0]["message"]["content"], "hello");
        assert_eq!(mapped["choices"][0]["finish_reason"], "stop");
        assert_eq!(usage.total(), 8);
    }

    #[test]
    fn generate_urls_handle_base_variants() {
        assert_eq!(
            anthropic_messages_url("https://api.anthropic.com"),
            "https://api.anthropic.com/v1/messages"
        );
        assert_eq!(
            anthropic_messages_url("https://api.anthropic.com/v1/"),
            "https://api.anthropic.com/v1/messages"
        );
        assert_eq!(
            gemini_generate_url("https://g.example/v1beta", "gemini-pro", true),
            "https://g.example/v1beta/models/gemini-pro:streamGenerateContent?alt=sse"
        );
    }

    #[tokio::test]
    async fn anthropic_stream_transcodes_to_openai_chunks() {
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-3\",\"usage\":{\"input_tokens\":9}}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hel\"}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}\n\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let (recorded_tx, recorded_rx) = std::sync::mpsc::channel();
        let sink: UsageSink = Arc::new(move |usage| {
            recorded_tx.send(usage).unwrap();
        });
        let out = transcode_anthropic_stream(Body::from(sse), "claude-3".into(), sink);
        let bytes = body::to_bytes(out).await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();

        assert!(text.contains("chat.completion.chunk"));
        assert!(text.contains("\"content\":\"Hel\""));
        assert!(text.contains("\"content\":\"lo\""));
        assert!(text.contains("\"finish_reason\":\"stop\""));
        assert!(text.trim_end().ends_with("data: [DONE]"));
        let usage = recorded_rx.try_recv().unwrap();
        assert_eq!(usage.prompt_tokens, 9);
        assert_eq!(usage.completion_tokens, 4);
    }

    #[tokio::test]
    async fn gemini_stream_transcodes_and_appends_done() {
        let sse = concat!(
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hi \"}]}}]}\n\n",
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"there\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":6,\"candidatesTokenCount\":2}}\n\n",
        );
        let (recorded_tx, recorded_rx) = std::sync::mpsc::channel();
        let sink: UsageSink = Arc::new(move |usage| {
            recorded_tx.send(usage).unwrap();
        });
        let out = transcode_gemini_stream(Body::from(sse), "gemini-pro".into(), sink);
        let bytes = body::to_bytes(out).await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();

        assert!(text.contains("\"content\":\"Hi \""));
        assert!(text.contains("\"content\":\"there\""));
        assert!(text.contains("\"finish_reason\":\"stop\""));
        assert!(text.trim_end().ends_with("data: [DONE]"));
        assert_eq!(recorded_rx.try_recv().unwrap().total(), 8);
    }

    #[tokio::test]
    async fn openai_observer_extracts_usage_without_altering_body() {
        let payload = json!({
            "choices": [],
            "usage": { "prompt_tokens": 3, "completion_tokens": 5 }
        })
        .to_string();
        let (recorded_tx, recorded_rx) = std::sync::mpsc::channel();
        let sink: UsageSink = Arc::new(move |usage| {
            recorded_tx.send(usage).unwrap();
        });
        let out = observe_openai_body(Body::from(payload.clone()), false, sink);
        let bytes = body::to_bytes(out).await.unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), payload);
        assert_eq!(recorded_rx.try_recv().unwrap().total(), 8);
    }

    #[tokio::test]
    async fn openai_observer_reads_streaming_usage_chunk() {
        let sse = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n\n",
            "data: [DONE]\n\n",
        );
        let (recorded_tx, recorded_rx) = std::sync::mpsc::channel();
        let sink: UsageSink = Arc::new(move |usage| {
            recorded_tx.send(usage).unwrap();
        });
        let out = observe_openai_body(Body::from(sse), true, sink);
        let bytes = body::to_bytes(out).await.unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), sse);
        assert_eq!(recorded_rx.try_recv().unwrap().total(), 12);
    }
}
