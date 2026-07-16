//! MCP federation helpers: list-result merging across backends, pagination,
//! tool-name namespacing for collisions, and initialize capability surfacing.

use serde_json::{json, Value};
use std::collections::BTreeSet;

// Joins a backend name and an upstream item name when two backends expose the
// same name. "__" is valid in MCP tool/prompt names, so aliases stay callable;
// the call path resolves the prefix back to the owning backend.
pub const ALIAS_SEPARATOR: &str = "__";

// JSON-RPC list methods the gateway federates across a route's backends.
pub struct ListSpec {
    pub result_key: &'static str,
    pub dedup_key: &'static str,
    // Whether colliding items are re-exposed under a backend-prefixed alias.
    // Items keyed by URI are globally addressable, so duplicates are dropped.
    pub alias: bool,
}

pub fn list_spec(method: &str) -> Option<ListSpec> {
    match method {
        "tools/list" => Some(ListSpec {
            result_key: "tools",
            dedup_key: "name",
            alias: true,
        }),
        "prompts/list" => Some(ListSpec {
            result_key: "prompts",
            dedup_key: "name",
            alias: true,
        }),
        "resources/list" => Some(ListSpec {
            result_key: "resources",
            dedup_key: "uri",
            alias: false,
        }),
        "resources/templates/list" => Some(ListSpec {
            result_key: "resourceTemplates",
            dedup_key: "uriTemplate",
            alias: false,
        }),
        _ => None,
    }
}

pub fn make_alias(backend: &str, name: &str) -> String {
    format!("{backend}{ALIAS_SEPARATOR}{name}")
}

// Resolves a possibly backend-prefixed name back to (backend, original name).
// Only the first separator splits, so original names may contain "__".
pub fn split_alias(name: &str) -> Option<(&str, &str)> {
    let (backend, original) = name.split_once(ALIAS_SEPARATOR)?;
    if backend.is_empty() || original.is_empty() {
        return None;
    }
    Some((backend, original))
}

// Cursors are opaque and backend-specific; a request that carries one cannot
// be fanned out and must go to a single backend.
pub fn request_cursor(body: &Value) -> Option<&str> {
    body.pointer("/params/cursor").and_then(Value::as_str)
}

// Rewrites a list request to fetch the next page from one backend.
pub fn with_cursor(body: &Value, cursor: &str) -> Value {
    let mut page = body.clone();
    if !page.is_object() {
        return page;
    }
    match page.get_mut("params") {
        Some(Value::Object(params)) => {
            params.insert("cursor".into(), Value::String(cursor.to_string()));
        }
        _ => {
            page["params"] = json!({ "cursor": cursor });
        }
    }
    page
}

pub fn next_cursor(response: &Value) -> Option<String> {
    response
        .pointer("/result/nextCursor")
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

// Merges one backend's page into the federated result. The first backend (in
// route-declared order) wins a contested name; later backends' collisions are
// re-exposed under a "{backend}__{name}" alias instead of being dropped.
pub fn merge_list_items(
    merged: &mut Vec<Value>,
    seen: &mut BTreeSet<String>,
    items: &[Value],
    spec: &ListSpec,
    backend: &str,
) {
    for item in items {
        let Some(key) = item.get(spec.dedup_key).and_then(Value::as_str) else {
            continue;
        };
        if seen.insert(key.to_string()) {
            merged.push(item.clone());
        } else if spec.alias {
            let alias = make_alias(backend, key);
            if seen.insert(alias.clone()) {
                let mut item = item.clone();
                item[spec.dedup_key] = Value::String(alias);
                merged.push(item);
            }
        }
    }
}

// On multi-backend routes the gateway itself answers tools/resources/prompts
// list calls via federation, so the initialize response must advertise those
// capabilities even when the session's backend does not.
pub fn augment_initialize_capabilities(value: &mut Value) -> bool {
    let Some(result) = value.get_mut("result") else {
        return false;
    };
    if !result.is_object() {
        return false;
    }
    if !result.get("capabilities").is_some_and(Value::is_object) {
        result["capabilities"] = json!({});
    }
    let capabilities = result
        .get_mut("capabilities")
        .and_then(Value::as_object_mut)
        .expect("capabilities was just ensured to be an object");
    let mut changed = false;
    for key in ["tools", "resources", "prompts"] {
        if !capabilities.get(key).is_some_and(Value::is_object) {
            capabilities.insert(key.into(), json!({}));
            changed = true;
        }
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_specs_cover_federated_methods() {
        assert_eq!(list_spec("tools/list").unwrap().result_key, "tools");
        assert_eq!(list_spec("prompts/list").unwrap().result_key, "prompts");
        assert_eq!(list_spec("resources/list").unwrap().result_key, "resources");
        assert_eq!(
            list_spec("resources/templates/list").unwrap().result_key,
            "resourceTemplates"
        );
        assert!(list_spec("tools/call").is_none());
        assert!(list_spec("initialize").is_none());
    }

    #[test]
    fn alias_round_trip_resolves_backend_and_name() {
        let alias = make_alias("mcp-b", "search");
        assert_eq!(alias, "mcp-b__search");
        assert_eq!(split_alias(&alias), Some(("mcp-b", "search")));
        // Only the first separator splits.
        assert_eq!(split_alias("b__my__tool"), Some(("b", "my__tool")));
        assert_eq!(split_alias("plain"), None);
        assert_eq!(split_alias("__x"), None);
    }

    #[test]
    fn cursor_helpers_read_and_rewrite_params() {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" });
        assert_eq!(request_cursor(&body), None);
        let paged = with_cursor(&body, "abc");
        assert_eq!(request_cursor(&paged), Some("abc"));

        let with_params = json!({ "method": "tools/list", "params": { "cursor": "x" } });
        assert_eq!(request_cursor(&with_params), Some("x"));
        assert_eq!(request_cursor(&with_cursor(&with_params, "y")), Some("y"));

        let response = json!({ "result": { "tools": [], "nextCursor": "n" } });
        assert_eq!(next_cursor(&response), Some("n".to_string()));
        assert_eq!(next_cursor(&json!({ "result": { "tools": [] } })), None);
    }

    #[test]
    fn merge_aliases_collisions_and_dedups_uris() {
        let spec = list_spec("tools/list").unwrap();
        let mut merged = Vec::new();
        let mut seen = BTreeSet::new();
        merge_list_items(
            &mut merged,
            &mut seen,
            &[json!({ "name": "search" })],
            &spec,
            "mcp-a",
        );
        merge_list_items(
            &mut merged,
            &mut seen,
            &[json!({ "name": "search" }), json!({ "name": "extra" })],
            &spec,
            "mcp-b",
        );
        let names = merged
            .iter()
            .map(|item| item["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(names, ["search", "mcp-b__search", "extra"]);

        let spec = list_spec("resources/list").unwrap();
        let mut merged = Vec::new();
        let mut seen = BTreeSet::new();
        for backend in ["mcp-a", "mcp-b"] {
            merge_list_items(
                &mut merged,
                &mut seen,
                &[json!({ "uri": "file:///a" })],
                &spec,
                backend,
            );
        }
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn initialize_capabilities_are_augmented_in_place() {
        let mut value = json!({
            "result": {
                "capabilities": { "tools": { "listChanged": true } },
                "serverInfo": { "name": "a" }
            }
        });
        assert!(augment_initialize_capabilities(&mut value));
        assert_eq!(
            value["result"]["capabilities"]["tools"]["listChanged"],
            true
        );
        assert!(value["result"]["capabilities"]["resources"].is_object());
        assert!(value["result"]["capabilities"]["prompts"].is_object());

        // Already complete: untouched.
        assert!(!augment_initialize_capabilities(&mut value.clone()));
        // Error responses have no result: untouched.
        let mut error = json!({ "error": { "code": -32600 } });
        assert!(!augment_initialize_capabilities(&mut error));
    }
}
