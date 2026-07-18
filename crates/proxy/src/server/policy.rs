//! Policy evaluation: resolves the route/backend policy chain into a
//! `PolicyRuntime` (header transforms, timeout, retry, token charges) while
//! enforcing deny rules, body-size limits, auth, and rate/token limits.

use super::auth::validate_auth;
use super::context::AgentRequestContext;
use super::headers::merge_header_transform;
use super::{header_value, ProxyServer};
use axum::http::StatusCode;
use dxgate_core::{AgentRoute, Backend, HeaderTransform, PolicyAction, RateLimitKey, RetryPolicy};
use std::env;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(super) struct PolicyRuntime {
    pub(super) request_headers: HeaderTransform,
    pub(super) response_headers: HeaderTransform,
    pub(super) timeout: Option<Duration>,
    pub(super) retry: Option<RetryPolicy>,
    pub(super) token_charges: Vec<TokenCharge>,
}

// A token-limit bucket this request's usage must be charged against.
#[derive(Debug, Clone)]
pub(super) struct TokenCharge {
    pub(super) key: String,
    pub(super) window_seconds: u64,
}

pub(super) fn evaluate_policies(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PolicyDefault {
    Allow,
    Deny,
}

impl PolicyDefault {
    pub(super) fn from_env() -> Self {
        match env::var("DXGATE_POLICY_DEFAULT") {
            Ok(value) if value.eq_ignore_ascii_case("deny") => Self::Deny,
            _ => Self::Allow,
        }
    }
}
