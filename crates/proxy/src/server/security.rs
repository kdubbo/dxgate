//! Listener security enforcement projected from xDS HTTP filters.

use super::{header_pairs, header_value, host_header, ProxyServer};
use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode};
use dxgate_core::{
    AuthorizationAction, AuthorizationCondition, AuthorizationOperation, AuthorizationPolicy,
    AuthorizationRule, AuthorizationSource, ConfigSnapshot, ExternalAuthorization,
    ExternalAuthorizationProtocol, JwtProvider, ListenerSecurity,
};
use hyper::body::to_bytes;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;
use tracing::{info, warn};

#[derive(Debug, Clone, Default)]
pub(super) struct JwtKeyCache {
    entries: Arc<RwLock<HashMap<String, String>>>,
}

#[derive(Debug, Clone, Default)]
struct AuthContext {
    request_principal: String,
    claims: Value,
}

struct RequestContext {
    method: String,
    host: String,
    path: String,
    headers: HeaderMap,
    port: u16,
    source_ip: Option<IpAddr>,
    remote_ip: Option<IpAddr>,
    source_principal: String,
    namespace: String,
    service_account: String,
    auth: AuthContext,
}

pub(super) async fn enforce_listener_security(
    server: &ProxyServer,
    snapshot: &ConfigSnapshot,
    request: &mut Request<Body>,
    peer: Option<SocketAddr>,
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
    if security.jwt_providers.is_empty()
        && security.authorization.is_empty()
        && security.external_authorization.is_empty()
    {
        return Ok(());
    }

    let auth = authenticate_jwt(server, security, request).await?;
    let method = request.method().as_str().to_string();
    let host = host_header(request.headers()).unwrap_or("*").to_string();
    let path = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/")
        .to_string();
    let source_principal = if trust_peer_principal_header() {
        request
            .headers()
            .get("x-dubbo-peer-principal")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string()
    } else {
        String::new()
    };
    // This identity must come from the trusted mTLS terminator, never from an
    // untrusted caller or reach the application as a spoofable user header.
    request.headers_mut().remove("x-dubbo-peer-principal");
    let (namespace, service_account) = identity_parts(&source_principal);
    let remote_ip = forwarded_ip(request.headers());
    let context = RequestContext {
        method,
        host,
        path,
        headers: request.headers().clone(),
        port: listener.bind.port(),
        source_ip: peer.map(|value| value.ip()),
        remote_ip,
        source_principal,
        namespace,
        service_account,
        auth,
    };

    enforce_rbac(server, &security.authorization, &context)?;
    enforce_external_authorization(server, &security.external_authorization, &context, request)
        .await
}

fn trust_peer_principal_header() -> bool {
    static TRUST: OnceLock<bool> = OnceLock::new();
    *TRUST.get_or_init(|| {
        std::env::var("DXGATE_TRUST_PEER_PRINCIPAL_HEADER")
            .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false)
    })
}

async fn authenticate_jwt(
    server: &ProxyServer,
    security: &ListenerSecurity,
    request: &mut Request<Body>,
) -> Result<AuthContext, (StatusCode, String)> {
    let mut authenticated = AuthContext::default();
    let mut found = false;
    for provider in &security.jwt_providers {
        let Some((token, source_header)) = token_from_request(provider, request) else {
            continue;
        };
        if found {
            return Err((
                StatusCode::UNAUTHORIZED,
                "multiple JWT tokens matched request authentication providers".to_string(),
            ));
        }
        let claims = validate_jwt(server, provider, &token).await?;
        let issuer = claims.get("iss").and_then(Value::as_str).unwrap_or("");
        let subject = claims.get("sub").and_then(Value::as_str).unwrap_or("");
        authenticated.request_principal = format!("{issuer}/{subject}");
        authenticated.claims = claims;
        apply_claim_headers(provider, request.headers_mut(), &authenticated.claims)?;
        if !provider.forward_original_token {
            if let Some(name) = source_header {
                request.headers_mut().remove(name);
            }
        }
        found = true;
    }
    Ok(authenticated)
}

fn token_from_request(
    provider: &JwtProvider,
    request: &Request<Body>,
) -> Option<(String, Option<HeaderName>)> {
    for location in &provider.from_headers {
        let Ok(name) = HeaderName::from_str(&location.name) else {
            continue;
        };
        let Some(raw) = request
            .headers()
            .get(&name)
            .and_then(|value| value.to_str().ok())
        else {
            continue;
        };
        let token = raw.strip_prefix(&location.prefix).unwrap_or(raw);
        return Some((token.to_string(), Some(name)));
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
            return Some(((*token).to_string(), None));
        }
    }
    let cookies = request
        .headers()
        .get("cookie")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    for name in &provider.from_cookies {
        for cookie in cookies.split(';') {
            let Some((candidate, token)) = cookie.trim().split_once('=') else {
                continue;
            };
            if candidate == name {
                return Some((token.to_string(), None));
            }
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
    let jwks = load_jwks(server, provider).await?;
    let key = match header.kid.as_deref() {
        Some(key_id) => jwks.find(key_id),
        None if jwks.keys.len() == 1 => jwks.keys.first(),
        _ => None,
    }
    .ok_or_else(|| {
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

async fn load_jwks(
    server: &ProxyServer,
    provider: &JwtProvider,
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
    if let Some(cached) = server
        .jwt_key_cache
        .entries
        .read()
        .await
        .get(&provider.jwks_uri)
        .cloned()
    {
        return serde_json::from_str(&cached).map_err(|error| {
            (
                StatusCode::UNAUTHORIZED,
                format!("invalid cached JWKS: {error}"),
            )
        });
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
    server
        .jwt_key_cache
        .entries
        .write()
        .await
        .insert(provider.jwks_uri.clone(), document);
    Ok(jwks)
}

fn apply_claim_headers(
    provider: &JwtProvider,
    headers: &mut HeaderMap,
    claims: &Value,
) -> Result<(), (StatusCode, String)> {
    if !provider.output_payload_to_header.is_empty() {
        insert_header(
            headers,
            &provider.output_payload_to_header,
            serde_json::to_string(claims).unwrap_or_default(),
        )?;
    }
    for mapping in &provider.output_claim_to_headers {
        let Some(value) = claim_value(claims, &mapping.claim) else {
            continue;
        };
        let value = match value {
            Value::String(value) => value.clone(),
            other => serde_json::to_string(other).unwrap_or_default(),
        };
        insert_header(headers, &mapping.header, value)?;
    }
    Ok(())
}

fn insert_header(
    headers: &mut HeaderMap,
    name: &str,
    value: String,
) -> Result<(), (StatusCode, String)> {
    let name = HeaderName::from_str(name).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid output header: {error}"),
        )
    })?;
    let value = HeaderValue::from_str(&value).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid claim header: {error}"),
        )
    })?;
    headers.insert(name, value);
    Ok(())
}

fn enforce_rbac(
    server: &ProxyServer,
    policies: &[AuthorizationPolicy],
    context: &RequestContext,
) -> Result<(), (StatusCode, String)> {
    let mut has_allow = false;
    let mut allow_match = false;
    for policy in policies {
        let matched = policy.rules.iter().any(|rule| rule_matches(rule, context));
        if policy.shadow {
            if matched {
                info!(policy = %policy.name, "dry-run authorization policy matched");
            }
            continue;
        }
        match policy.action {
            AuthorizationAction::Deny if matched => {
                server.state.record_policy_denied();
                return Err((
                    StatusCode::FORBIDDEN,
                    format!("request denied by policy {}", policy.name),
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

async fn enforce_external_authorization(
    server: &ProxyServer,
    providers: &[ExternalAuthorization],
    context: &RequestContext,
    request: &mut Request<Body>,
) -> Result<(), (StatusCode, String)> {
    for provider in providers {
        if !provider.rules.is_empty()
            && !provider
                .rules
                .iter()
                .any(|rule| rule_matches(rule, context))
        {
            continue;
        }
        let uri = format!(
            "http://{}:{}{}",
            provider.service,
            provider.port,
            if provider.path_prefix.is_empty() {
                "/"
            } else {
                provider.path_prefix.as_str()
            }
        );
        let selected_headers: HashMap<String, String> = provider
            .include_request_headers_in_check
            .iter()
            .filter_map(|name| {
                header_value(&header_pairs(request.headers()), name)
                    .map(|value| (name.clone(), value.to_string()))
            })
            .collect();
        let body = json!({
            "method": context.method,
            "host": context.host,
            "path": context.path,
            "sourceIp": context.source_ip.map(|value| value.to_string()),
            "remoteIp": context.remote_ip.map(|value| value.to_string()),
            "requestPrincipal": context.auth.request_principal,
            "headers": selected_headers,
        });
        let check = Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .map_err(|error| (StatusCode::BAD_GATEWAY, error.to_string()))?;
        let timeout = Duration::from_millis(provider.timeout_ms.max(1));
        let response = match time::timeout(timeout, async {
            match provider.protocol {
                ExternalAuthorizationProtocol::Http => server.clients.request_plain(check).await,
                ExternalAuthorizationProtocol::Grpc => server.clients.request_h2(check).await,
            }
        })
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                if provider.fail_open {
                    warn!(provider = %provider.provider, "external authorization failed open");
                    continue;
                }
                return Err(error);
            }
            Err(_) if provider.fail_open => {
                warn!(provider = %provider.provider, "external authorization timed out and failed open");
                continue;
            }
            Err(_) => {
                return Err((
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("external authorization {} timed out", provider.provider),
                ));
            }
        };
        let allowed = response.status().is_success();
        if provider.shadow {
            info!(provider = %provider.provider, allowed, "dry-run external authorization completed");
            continue;
        }
        if !allowed {
            server.state.record_policy_denied();
            return Err((
                StatusCode::FORBIDDEN,
                format!(
                    "external authorization {} denied request",
                    provider.provider
                ),
            ));
        }
        for name in &provider.headers_to_upstream_on_allow {
            let Some(value) = response.headers().get(name).cloned() else {
                continue;
            };
            if let Ok(name) = HeaderName::from_str(name) {
                request.headers_mut().insert(name, value);
            }
        }
    }
    Ok(())
}

fn rule_matches(rule: &AuthorizationRule, context: &RequestContext) -> bool {
    (rule.sources.is_empty()
        || rule
            .sources
            .iter()
            .any(|source| source_matches(source, context)))
        && (rule.operations.is_empty()
            || rule
                .operations
                .iter()
                .any(|operation| operation_matches(operation, context)))
        && rule
            .when
            .iter()
            .all(|condition| condition_matches(condition, context))
}

fn source_matches(source: &AuthorizationSource, context: &RequestContext) -> bool {
    matches_attribute(
        &context.source_principal,
        &source.principals,
        &source.not_principals,
    ) && matches_attribute(
        &context.auth.request_principal,
        &source.request_principals,
        &source.not_request_principals,
    ) && matches_attribute(
        &context.namespace,
        &source.namespaces,
        &source.not_namespaces,
    ) && matches_attribute(
        &context.service_account,
        &source.service_accounts,
        &source.not_service_accounts,
    ) && matches_ip(context.source_ip, &source.ip_blocks, &source.not_ip_blocks)
        && matches_ip(
            context.remote_ip,
            &source.remote_ip_blocks,
            &source.not_remote_ip_blocks,
        )
}

fn operation_matches(operation: &AuthorizationOperation, context: &RequestContext) -> bool {
    matches_attribute(&context.host, &operation.hosts, &operation.not_hosts)
        && matches_attribute(
            &context.port.to_string(),
            &operation.ports,
            &operation.not_ports,
        )
        && matches_attribute(&context.method, &operation.methods, &operation.not_methods)
        && matches_attribute(&context.path, &operation.paths, &operation.not_paths)
}

fn condition_matches(condition: &AuthorizationCondition, context: &RequestContext) -> bool {
    if condition.key.starts_with("request.auth.claims[") && condition.key.ends_with(']') {
        let claim = &condition.key["request.auth.claims[".len()..condition.key.len() - 1];
        return claim_value(&context.auth.claims, claim)
            .map(|value| claim_matches(value, &condition.values, &condition.not_values))
            .unwrap_or(condition.values.is_empty());
    }
    let value = match condition.key.as_str() {
        "source.principal" => Some(context.source_principal.to_string()),
        "source.namespace" => Some(context.namespace.to_string()),
        "source.serviceAccount" => Some(context.service_account.to_string()),
        "source.ip" => context.source_ip.map(|value| value.to_string()),
        "remote.ip" => context.remote_ip.map(|value| value.to_string()),
        "destination.port" => Some(context.port.to_string()),
        key if key.starts_with("request.headers[") && key.ends_with(']') => {
            let header = &key["request.headers[".len()..key.len() - 1];
            context
                .headers
                .get(header)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        }
        _ => None,
    }
    .unwrap_or_default();
    matches_attribute(&value, &condition.values, &condition.not_values)
}

fn claim_matches(value: &Value, positive: &[String], negative: &[String]) -> bool {
    match value {
        Value::Array(values) => {
            let values: Vec<String> = values.iter().map(value_string).collect();
            (positive.is_empty()
                || positive
                    .iter()
                    .any(|pattern| values.iter().any(|value| wildcard_match(pattern, value))))
                && !negative
                    .iter()
                    .any(|pattern| values.iter().any(|value| wildcard_match(pattern, value)))
        }
        _ => matches_attribute(&value_string(value), positive, negative),
    }
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

fn matches_attribute(value: &str, positive: &[String], negative: &[String]) -> bool {
    (positive.is_empty()
        || positive
            .iter()
            .any(|pattern| wildcard_match(pattern, value)))
        && !negative
            .iter()
            .any(|pattern| wildcard_match(pattern, value))
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
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

fn matches_ip(address: Option<IpAddr>, positive: &[String], negative: &[String]) -> bool {
    (positive.is_empty() || positive.iter().any(|block| ip_in_block(address, block)))
        && !negative.iter().any(|block| ip_in_block(address, block))
}

fn ip_in_block(address: Option<IpAddr>, block: &str) -> bool {
    let Some(address) = address else {
        return false;
    };
    if let Ok(candidate) = block.parse::<IpAddr>() {
        return candidate == address;
    }
    let Some((network, prefix)) = block.split_once('/') else {
        return false;
    };
    let Ok(network) = network.parse::<IpAddr>() else {
        return false;
    };
    let Ok(prefix) = prefix.parse::<u8>() else {
        return false;
    };
    match (address, network) {
        (IpAddr::V4(address), IpAddr::V4(network)) if prefix <= 32 => {
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            u32::from(address) & mask == u32::from(network) & mask
        }
        (IpAddr::V6(address), IpAddr::V6(network)) if prefix <= 128 => {
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            u128::from(address) & mask == u128::from(network) & mask
        }
        _ => false,
    }
}

fn forwarded_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .and_then(|value| value.trim().parse().ok())
}

fn identity_parts(principal: &str) -> (String, String) {
    let identity = principal.strip_prefix("spiffe://").unwrap_or(principal);
    let parts: Vec<_> = identity.split('/').collect();
    if parts.len() != 5 || parts[1] != "ns" || parts[3] != "sa" {
        return (String::new(), String::new());
    }
    (parts[2].to_string(), format!("{}/{}", parts[2], parts[4]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dxgate_core::{AuthorizationCondition, AuthorizationOperation, AuthorizationSource};

    fn context() -> RequestContext {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", HeaderValue::from_static("blue"));
        RequestContext {
            method: "GET".into(),
            host: "orders.example.com".into(),
            path: "/orders/42".into(),
            headers,
            port: 8080,
            source_ip: Some("10.1.2.3".parse().unwrap()),
            remote_ip: Some("203.0.113.10".parse().unwrap()),
            source_principal: "spiffe://cluster.local/ns/app/sa/client".into(),
            namespace: "app".into(),
            service_account: "app/client".into(),
            auth: AuthContext {
                request_principal: "https://issuer.example/alice".into(),
                claims: json!({"groups":["orders"],"nested":{"tenant":"blue"}}),
            },
        }
    }

    #[test]
    fn rbac_rule_matches_http_jwt_and_ingress_ip_attributes() {
        let rule = AuthorizationRule {
            sources: vec![AuthorizationSource {
                request_principals: vec!["https://issuer.example/*".into()],
                remote_ip_blocks: vec!["203.0.113.0/24".into()],
                ..AuthorizationSource::default()
            }],
            operations: vec![AuthorizationOperation {
                hosts: vec!["*.example.com".into()],
                ports: vec!["8080".into()],
                methods: vec!["GET".into()],
                paths: vec!["/orders/*".into()],
                ..AuthorizationOperation::default()
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
                AuthorizationCondition {
                    key: "request.headers[x-tenant]".into(),
                    values: vec!["blue".into()],
                    not_values: vec![],
                },
            ],
        };
        assert!(rule_matches(&rule, &context()));
    }

    #[test]
    fn claim_to_header_supports_nested_values() {
        let provider = JwtProvider {
            issuer: "https://issuer.example".into(),
            audiences: vec![],
            jwks_uri: String::new(),
            jwks: String::new(),
            from_headers: vec![],
            from_params: vec![],
            from_cookies: vec![],
            forward_original_token: false,
            output_payload_to_header: "x-jwt-payload".into(),
            output_claim_to_headers: vec![dxgate_core::ClaimToHeader {
                claim: "nested.tenant".into(),
                header: "x-jwt-tenant".into(),
            }],
        };
        let claims = json!({"nested":{"tenant":"blue"}});
        let mut headers = HeaderMap::new();
        apply_claim_headers(&provider, &mut headers, &claims).unwrap();
        assert_eq!(headers["x-jwt-tenant"], "blue");
        assert!(headers.contains_key("x-jwt-payload"));
    }

    #[test]
    fn token_locations_cover_header_query_and_cookie() {
        let mut request = Request::builder()
            .uri("/?token=query-token")
            .header("authorization", "Bearer header-token")
            .header("cookie", "session=cookie-token")
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
            from_cookies: vec!["session".into()],
            forward_original_token: false,
            output_payload_to_header: String::new(),
            output_claim_to_headers: vec![],
        };
        assert_eq!(
            token_from_request(&provider, &request).unwrap().0,
            "header-token"
        );
        request.headers_mut().remove("authorization");
        provider.from_headers.clear();
        assert_eq!(
            token_from_request(&provider, &request).unwrap().0,
            "query-token"
        );
        provider.from_params.clear();
        assert_eq!(
            token_from_request(&provider, &request).unwrap().0,
            "cookie-token"
        );
    }
}
