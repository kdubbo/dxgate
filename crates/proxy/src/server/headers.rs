//! Pure header manipulation: hop-by-hop stripping, policy header transforms,
//! and LLM provider credential/header injection. None of these touch server
//! state; they operate only on a `HeaderMap` and config values.

use axum::http::{HeaderMap, HeaderName, HeaderValue as HttpHeaderValue};
use dxgate_core::{HeaderTransform, Provider, ProviderKind};
use std::env;

pub(super) fn remove_connection_headers(headers: &mut HeaderMap) {
    let named: Vec<String> = headers
        .get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    for name in named {
        if let Ok(name) = HeaderName::try_from(name.as_str()) {
            headers.remove(name);
        }
    }
    for name in [
        http::header::CONNECTION,
        http::header::PROXY_AUTHENTICATE,
        http::header::PROXY_AUTHORIZATION,
        http::header::TRANSFER_ENCODING,
        http::header::UPGRADE,
    ] {
        headers.remove(name);
    }
    headers.remove(HeaderName::from_static("keep-alive"));
    headers.remove(HeaderName::from_static("proxy-connection"));
    headers.remove(HeaderName::from_static("http2-settings"));
}

pub(super) fn header_contains(headers: &HeaderMap, name: HeaderName, needle: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase().contains(needle))
        .unwrap_or(false)
}

pub(super) fn merge_header_transform(target: &mut HeaderTransform, next: &HeaderTransform) {
    target.add.extend(next.add.clone());
    target.remove.extend(next.remove.clone());
}

pub(super) fn apply_request_headers(headers: &mut HeaderMap, transform: &HeaderTransform) {
    apply_header_transform(headers, transform);
}

pub(super) fn apply_response_headers(headers: &mut HeaderMap, transform: &HeaderTransform) {
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

pub(super) fn apply_provider_headers(headers: &mut HeaderMap, provider: Option<&Provider>) {
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
            let credential = match provider.kind {
                ProviderKind::Anthropic => HttpHeaderValue::from_str(&key)
                    .map(|v| (HeaderName::from_static("x-api-key"), v)),
                ProviderKind::Gemini => HttpHeaderValue::from_str(&key)
                    .map(|v| (HeaderName::from_static("x-goog-api-key"), v)),
                _ => HttpHeaderValue::from_str(&format!("Bearer {key}"))
                    .map(|v| (http::header::AUTHORIZATION, v)),
            };
            if let Ok((name, value)) = credential {
                headers.insert(name, value);
            }
        }
    }
    if provider.kind == ProviderKind::Anthropic {
        headers
            .entry(HeaderName::from_static("anthropic-version"))
            .or_insert_with(|| HttpHeaderValue::from_static("2023-06-01"));
    }
}
