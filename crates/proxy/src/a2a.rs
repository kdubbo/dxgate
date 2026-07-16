//! A2A gateway helpers: task-affinity extraction, agent-card URL rewriting,
//! and SSE task sniffing so follow-up task calls reach the owning backend.

use crate::llm::{SseLines, Transcode};
use axum::body::Body;
use hyper::Uri;
use serde_json::Value;

pub const AGENT_CARD_PATH: &str = "/.well-known/agent-card.json";

// The task a JSON-RPC request refers to: tasks/get, tasks/cancel,
// tasks/resubscribe and pushNotificationConfig calls use params.id; a
// message/send continuing an existing task carries params.message.taskId.
pub fn request_task_id(body: &Value) -> Option<&str> {
    body.pointer("/params/id")
        .and_then(Value::as_str)
        .or_else(|| {
            body.pointer("/params/message/taskId")
                .and_then(Value::as_str)
        })
}

// The task a JSON-RPC response (or one SSE event of a stream) talks about.
// Task objects carry "id"; status/artifact update events and messages carry
// "taskId". A Message result's "id"-like field is "messageId", so a bare "id"
// is only trusted when the result is not explicitly some other kind.
pub fn response_task_id(response: &Value) -> Option<&str> {
    let result = response.get("result")?;
    if let Some(task_id) = result.get("taskId").and_then(Value::as_str) {
        return Some(task_id);
    }
    match result.get("kind").and_then(Value::as_str) {
        Some("task") | None => result.get("id").and_then(Value::as_str),
        _ => None,
    }
}

// Replaces the scheme+authority of every URL in an agent card with the
// gateway's, keeping paths intact: the card otherwise leaks the backend's
// internal endpoint, and clients following it would bypass the gateway.
pub fn rewrite_card_urls(card: &mut Value, scheme: &str, authority: &str) -> bool {
    let mut changed = false;
    if let Some(url) = card.get("url").and_then(Value::as_str) {
        if let Some(rewritten) = rewrite_origin(url, scheme, authority) {
            card["url"] = Value::String(rewritten);
            changed = true;
        }
    }
    if let Some(interfaces) = card
        .get_mut("additionalInterfaces")
        .and_then(Value::as_array_mut)
    {
        for interface in interfaces {
            if let Some(url) = interface.get("url").and_then(Value::as_str) {
                if let Some(rewritten) = rewrite_origin(url, scheme, authority) {
                    interface["url"] = Value::String(rewritten);
                    changed = true;
                }
            }
        }
    }
    changed
}

fn rewrite_origin(url: &str, scheme: &str, authority: &str) -> Option<String> {
    let uri = url.parse::<Uri>().ok()?;
    // Relative URLs have no origin to leak.
    uri.authority()?;
    let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let rewritten = format!("{scheme}://{authority}{path_and_query}");
    (rewritten != url).then_some(rewritten)
}

// Watches an SSE stream and reports the first task id it sees, so streamed
// task creation (message/stream) binds affinity as soon as the id appears
// rather than when the stream ends. Bytes pass through unchanged.
pub struct TaskSniffer {
    lines: SseLines,
    on_task: Option<Box<dyn FnOnce(String) + Send>>,
}

impl TaskSniffer {
    pub fn new(on_task: Box<dyn FnOnce(String) + Send>) -> Self {
        Self {
            lines: SseLines::default(),
            on_task: Some(on_task),
        }
    }
}

impl Transcode for TaskSniffer {
    fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        if self.on_task.is_some() {
            for line in self.lines.push(chunk) {
                let Ok(event) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if let Some(task_id) = response_task_id(&event) {
                    if let Some(on_task) = self.on_task.take() {
                        on_task(task_id.to_string());
                    }
                    break;
                }
            }
        }
        chunk.to_vec()
    }

    fn finish(&mut self) -> Vec<u8> {
        Vec::new()
    }
}

pub fn sniff_task_stream(upstream: Body, on_task: Box<dyn FnOnce(String) + Send>) -> Body {
    crate::llm::wrap_body(upstream, TaskSniffer::new(on_task))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn task_ids_are_extracted_from_requests_and_responses() {
        let get = json!({ "method": "tasks/get", "params": { "id": "task-1" } });
        assert_eq!(request_task_id(&get), Some("task-1"));
        let send = json!({
            "method": "message/send",
            "params": { "message": { "taskId": "task-2", "parts": [] } }
        });
        assert_eq!(request_task_id(&send), Some("task-2"));
        assert_eq!(request_task_id(&json!({ "method": "message/send" })), None);

        let task = json!({ "result": { "kind": "task", "id": "task-3" } });
        assert_eq!(response_task_id(&task), Some("task-3"));
        let update = json!({ "result": { "kind": "status-update", "taskId": "task-4" } });
        assert_eq!(response_task_id(&update), Some("task-4"));
        // A message result's bare id is a messageId, not a task id.
        let message = json!({ "result": { "kind": "message", "messageId": "msg-1" } });
        assert_eq!(response_task_id(&message), None);
        // kind-less results with an id are treated as tasks (older backends).
        let bare = json!({ "result": { "id": "task-5" } });
        assert_eq!(response_task_id(&bare), Some("task-5"));
    }

    #[test]
    fn card_urls_are_rewritten_to_the_gateway_origin() {
        let mut card = json!({
            "name": "planner",
            "url": "http://10.0.0.5:9999/api/a2a",
            "additionalInterfaces": [
                { "url": "https://internal.svc:8443/grpc", "transport": "GRPC" }
            ]
        });
        assert!(rewrite_card_urls(&mut card, "http", "gw.example:8080"));
        assert_eq!(card["url"], "http://gw.example:8080/api/a2a");
        assert_eq!(
            card["additionalInterfaces"][0]["url"],
            "http://gw.example:8080/grpc"
        );
        assert_eq!(card["name"], "planner");

        // Already pointing at the gateway: reported as unchanged.
        assert!(!rewrite_card_urls(&mut card, "http", "gw.example:8080"));
    }

    #[tokio::test]
    async fn task_sniffer_reports_first_task_and_passes_bytes_through() {
        let sse = concat!(
            "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"kind\":\"task\",\"id\":\"task-9\",\"status\":{\"state\":\"working\"}}}\n\n",
            "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"kind\":\"status-update\",\"taskId\":\"task-9\"}}\n\n",
        );
        let (tx, rx) = std::sync::mpsc::channel();
        let out = sniff_task_stream(
            Body::from(sse),
            Box::new(move |task_id| tx.send(task_id).unwrap()),
        );
        let bytes = hyper::body::to_bytes(out).await.unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), sse);
        assert_eq!(rx.try_recv().unwrap(), "task-9");
        // Only the first sighting binds.
        assert!(rx.try_recv().is_err());
    }
}
