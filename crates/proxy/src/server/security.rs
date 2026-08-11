//! JWT authentication and claim-based authorization projected from xDS.

use super::ProxyServer;
use axum::body::Body;
use axum::http::{HeaderName, Request, StatusCode};
use dxgate_core::{
    AuthorizationAction, AuthorizationCondition, AuthorizationPolicy, AuthorizationRule,
    AuthorizationSource, ConfigSnapshot, JwtProvider, ListenerSecurity,
};
use hyper::body::to_bytes;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

const JWKS_CACHE_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
struct CachedJwks {
    document: String,
    fetched_at: Instant,
}

#[derive(Debug, Clone, Default)]
pub(super) struct JwtKeyCache {
    entries: Arc<RwLock<HashMap<String, CachedJwks>>>,
}

#[derive(Debug, Clone, Default)]
struct AuthContext {
    request_principal: String,
    claims: Value,
}

pub(super) async fn enforce_listener_security(
    server: &ProxyServer,
    snapshot: &ConfigSnapshot,
    request: &Request<Body>,
) -> Result<(), (StatusCode, String)> {
    let Some(listener) = snapshot
        .listeners()
        .iter()
        .find(|listener| listener.bind.port() == server.listener_port)
        .or_else(|| (snapshot.listeners().len() == 1).then(|| &snapshot.listeners()[0]))
    else {
        return Ok(());
    };
    let security = &listener.security;
    if security.jwt_providers.is_empty() && security.authorization.is_empty() {
        return Ok(());
    }

    let auth = authenticate_jwt(server, security, request).await?;
    enforce_rbac(server, &security.authorization, &auth)
}

async fn authenticate_jwt(
    server: &ProxyServer,
    security: &ListenerSecurity,
    request: &Request<Body>,
) -> Result<AuthContext, (StatusCode, String)> {
    let mut candidates: HashMap<String, Vec<&JwtProvider>> = HashMap::new();
    for provider in &security.jwt_providers {
        if let Some(token) = token_from_request(provider, request) {
            candidates.entry(token).or_default().push(provider);
        }
    }
    if candidates.is_empty() {
        return Ok(AuthContext::default());
    }
    if candidates.len() != 1 {
        return Err((
            StatusCode::UNAUTHORIZED,
            "multiple JWT tokens matched request authentication providers".to_string(),
        ));
    }

    let (token, providers) = candidates.into_iter().next().expect("one JWT candidate");
    let mut last_error = None;
    for provider in providers {
        match validate_jwt(server, provider, &token).await {
            Ok(claims) => {
                let issuer = claims.get("iss").and_then(Value::as_str).unwrap_or("");
                let subject = claims.get("sub").and_then(Value::as_str).unwrap_or("");
                return Ok(AuthContext {
                    request_principal: format!("{issuer}/{subject}"),
                    claims,
                });
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            "JWT did not match a request authentication provider".to_string(),
        )
    }))
}

fn token_from_request(provider: &JwtProvider, request: &Request<Body>) -> Option<String> {
    for location in &provider.from_headers {
        let Ok(name) = HeaderName::from_str(&location.name) else {
            continue;
        };
        let Some(raw) = request
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
        else {
            continue;
        };
        let token = raw.strip_prefix(&location.prefix).unwrap_or(raw);
        return Some(token.to_string());
    }
    let query: HashMap<_, _> = request
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .filter_map(|part| part.split_once('='))
        .collect();
    for name in &provider.from_params {
        if let Some(token) = query.get(name.as_str()) {
            return Some((*token).to_string());
        }
    }
    None
}

async fn validate_jwt(
    server: &ProxyServer,
    provider: &JwtProvider,
    token: &str,
) -> Result<Value, (StatusCode, String)> {
    let header = decode_header(token).map_err(|error| {
        (
            StatusCode::UNAUTHORIZED,
            format!("invalid JWT header: {error}"),
        )
    })?;
    let mut jwks = load_jwks(server, provider, false).await?;
    let mut key = signing_key(&jwks, header.kid.as_deref());
    if key.is_none() && provider.jwks.is_empty() {
        jwks = load_jwks(server, provider, true).await?;
        key = signing_key(&jwks, header.kid.as_deref());
    }
    let key = key.ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            "JWT signing key was not found".to_string(),
        )
    })?;
    let decoding_key = DecodingKey::from_jwk(key)
        .map_err(|error| (StatusCode::UNAUTHORIZED, format!("invalid JWK: {error}")))?;
    let mut validation = Validation::new(header.alg);
    if provider.audiences.is_empty() {
        validation.validate_aud = false;
    } else {
        validation.set_audience(&provider.audiences);
    }
    if !provider.issuer.is_empty() {
        validation.set_issuer(&[provider.issuer.as_str()]);
    }
    decode::<Value>(token, &decoding_key, &validation)
        .map(|data| data.claims)
        .map_err(|error| (StatusCode::UNAUTHORIZED, format!("invalid JWT: {error}")))
}

fn signing_key<'a>(jwks: &'a JwkSet, key_id: Option<&str>) -> Option<&'a jsonwebtoken::jwk::Jwk> {
    match key_id {
        Some(key_id) => jwks.find(key_id),
        None if jwks.keys.len() == 1 => jwks.keys.first(),
        _ => None,
    }
}

async fn load_jwks(
    server: &ProxyServer,
    provider: &JwtProvider,
    force_refresh: bool,
) -> Result<JwkSet, (StatusCode, String)> {
    if !provider.jwks.is_empty() {
        return serde_json::from_str(&provider.jwks).map_err(|error| {
            (
                StatusCode::UNAUTHORIZED,
                format!("invalid inline JWKS: {error}"),
            )
        });
    }
    if provider.jwks_uri.is_empty() {
        return Err((
            StatusCode::UNAUTHORIZED,
            "JWT provider has neither jwks nor jwks_uri".to_string(),
        ));
    }
    if !force_refresh {
        if let Some(cached) = server
            .jwt_key_cache
            .entries
            .read()
            .await
            .get(&provider.jwks_uri)
            .filter(|entry| entry.fetched_at.elapsed() < JWKS_CACHE_TTL)
            .cloned()
        {
            return serde_json::from_str(&cached.document).map_err(|error| {
                (
                    StatusCode::UNAUTHORIZED,
                    format!("invalid cached JWKS: {error}"),
                )
            });
        }
    }

    let request = Request::builder()
        .method("GET")
        .uri(&provider.jwks_uri)
        .body(Body::empty())
        .map_err(|error| (StatusCode::BAD_GATEWAY, error.to_string()))?;
    let response = server.clients.request_web(request).await?;
    if !response.status().is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("JWKS endpoint returned {}", response.status()),
        ));
    }
    let bytes = to_bytes(response.into_body())
        .await
        .map_err(|error| (StatusCode::BAD_GATEWAY, format!("read JWKS: {error}")))?;
    let document = String::from_utf8(bytes.to_vec()).map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("JWKS is not UTF-8: {error}"),
        )
    })?;
    let jwks = serde_json::from_str(&document).map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("invalid remote JWKS: {error}"),
        )
    })?;
    server.jwt_key_cache.entries.write().await.insert(
        provider.jwks_uri.clone(),
        CachedJwks {
            document,
            fetched_at: Instant::now(),
        },
    );
    Ok(jwks)
}

fn enforce_rbac(
    server: &ProxyServer,
    policies: &[AuthorizationPolicy],
    auth: &AuthContext,
) -> Result<(), (StatusCode, String)> {
    let mut has_allow = false;
    let mut allow_match = false;
    for policy in policies {
        let matched = policy.rules.iter().any(|rule| rule_matches(rule, auth));
        match policy.action {
            AuthorizationAction::Deny if matched => {
                server.state.record_policy_denied();
                return Err((
                    StatusCode::FORBIDDEN,
                    "request denied by AuthorizationPolicy".to_string(),
                ));
            }
            AuthorizationAction::Allow => {
                has_allow = true;
                allow_match |= matched;
            }
            _ => {}
        }
    }
    if has_allow && !allow_match {
        server.state.record_policy_denied();
        return Err((
            StatusCode::FORBIDDEN,
            "request denied by AuthorizationPolicy allow list".to_string(),
        ));
    }
    Ok(())
}

fn rule_matches(rule: &AuthorizationRule, auth: &AuthContext) -> bool {
    (rule.sources.is_empty()
        || rule
            .sources
            .iter()
            .any(|source| source_matches(source, auth)))
        && rule
            .when
            .iter()
            .all(|condition| condition_matches(condition, auth))
}

fn source_matches(source: &AuthorizationSource, auth: &AuthContext) -> bool {
    source.principals.is_empty()
        && (source.request_principals.is_empty()
            || (!auth.request_principal.is_empty()
                && source
                    .request_principals
                    .iter()
                    .any(|pattern| wildcard_match(pattern, &auth.request_principal))))
}

fn condition_matches(condition: &AuthorizationCondition, auth: &AuthContext) -> bool {
    let Some(claim) = condition
        .key
        .strip_prefix("request.auth.claims[")
        .and_then(|key| key.strip_suffix(']'))
    else {
        return false;
    };
    claim_value(&auth.claims, claim)
        .map(|value| claim_matches(value, &condition.values, &condition.not_values))
        .unwrap_or(false)
}

fn claim_matches(value: &Value, positive: &[String], negative: &[String]) -> bool {
    let values: Vec<String> = match value {
        Value::Array(values) => values.iter().map(value_string).collect(),
        value => vec![value_string(value)],
    };
    (positive.is_empty()
        || positive
            .iter()
            .any(|pattern| values.iter().any(|value| wildcard_match(pattern, value))))
        && !negative
            .iter()
            .any(|pattern| values.iter().any(|value| wildcard_match(pattern, value)))
}

fn claim_value<'a>(claims: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(claims, |current, segment| current.get(segment))
}

fn value_string(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| serde_json::to_string(value).unwrap_or_default())
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return !value.is_empty();
    }
    let parts: Vec<_> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == value;
    }
    if !value.starts_with(parts[0]) {
        return false;
    }
    let mut remainder = &value[parts[0].len()..];
    for (index, part) in parts.iter().enumerate().skip(1) {
        if part.is_empty() {
            continue;
        }
        let Some(position) = remainder.find(part) else {
            return false;
        };
        if index == parts.len() - 1 && !remainder.ends_with(part) {
            return false;
        }
        remainder = &remainder[position + part.len()..];
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use dxgate_core::{AuthorizationCondition, AuthorizationSource};
    use serde_json::json;

    #[test]
    fn jwt_claim_rule_matches_authenticated_request() {
        let auth = AuthContext {
            request_principal: "https://issuer.example/alice".into(),
            claims: json!({"groups":["orders"],"nested":{"tenant":"blue"}}),
        };
        let rule = AuthorizationRule {
            sources: vec![AuthorizationSource {
                request_principals: vec!["https://issuer.example/*".into()],
                principals: vec![],
            }],
            when: vec![
                AuthorizationCondition {
                    key: "request.auth.claims[groups]".into(),
                    values: vec!["orders".into()],
                    not_values: vec![],
                },
                AuthorizationCondition {
                    key: "request.auth.claims[nested.tenant]".into(),
                    values: vec!["blue".into()],
                    not_values: vec![],
                },
            ],
        };
        assert!(rule_matches(&rule, &auth));
        assert!(!rule_matches(&rule, &AuthContext::default()));
    }

    #[test]
    fn token_locations_cover_header_and_query() {
        let request = Request::builder()
            .uri("/?token=query-token")
            .header("authorization", "Bearer header-token")
            .body(Body::empty())
            .unwrap();
        let mut provider = JwtProvider {
            issuer: String::new(),
            audiences: vec![],
            jwks_uri: String::new(),
            jwks: String::new(),
            from_headers: vec![dxgate_core::JwtHeader {
                name: "authorization".into(),
                prefix: "Bearer ".into(),
            }],
            from_params: vec!["token".into()],
        };
        assert_eq!(
            token_from_request(&provider, &request).as_deref(),
            Some("header-token")
        );
        provider.from_headers.clear();
        assert_eq!(
            token_from_request(&provider, &request).as_deref(),
            Some("query-token")
        );
    }
}
