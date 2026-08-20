//! Access-log configuration, request filtering, and text/JSON rendering.
//!
//! The deployment controller resolves Telemetry and passes the effective
//! settings via environment variables. This keeps configuration scoped to the
//! managed gateway deployment while the request path still evaluates filtering
//! and tags locally for every completed request.

use std::collections::BTreeMap;
use std::env;

#[derive(Clone)]
pub(super) struct AccessLogConfig {
    pub(super) enabled: bool,
    pub(super) format: AccessLogFormat,
    mode: AccessLogMode,
    filter: AccessLogFilter,
    pub(super) tags: BTreeMap<String, String>,
    pub(super) otlp_endpoint: Option<String>,
    warnings: Vec<String>,
}

impl AccessLogConfig {
    pub(super) fn from_env() -> Self {
        let enabled = env::var("DXGATE_ACCESS_LOG").ok();
        let format = env::var("DXGATE_ACCESS_LOG_FORMAT").ok();
        let mode = env::var("DXGATE_ACCESS_LOG_MODE").ok();
        let filter = env::var("DXGATE_ACCESS_LOG_FILTER").ok();
        let tags = env::var("DXGATE_ACCESS_LOG_TAGS").ok();
        // A dedicated logs endpoint wins. Existing deployments that only set
        // DXGATE_OTEL_ENDPOINT retain the established collector fallback.
        let otlp_endpoint = env::var("DXGATE_OTEL_LOGS_ENDPOINT")
            .ok()
            .or_else(|| env::var("DXGATE_OTEL_ENDPOINT").ok());
        Self::from_options(
            enabled.as_deref(),
            format.as_deref(),
            mode.as_deref(),
            filter.as_deref(),
            tags.as_deref(),
            otlp_endpoint.as_deref(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_options(
        enabled: Option<&str>,
        format: Option<&str>,
        mode: Option<&str>,
        filter: Option<&str>,
        tags: Option<&str>,
        otlp_endpoint: Option<&str>,
    ) -> Self {
        let mut warnings = Vec::new();
        let (filter, filter_warning) = AccessLogFilter::parse(filter);
        if let Some(warning) = filter_warning {
            warnings.push(warning);
        }
        let (tags, tag_warning) = parse_access_log_tags(tags);
        if let Some(warning) = tag_warning {
            warnings.push(warning);
        }
        let (mode, mode_warning) = parse_access_log_mode(mode);
        if let Some(warning) = mode_warning {
            warnings.push(warning);
        }
        Self {
            enabled: parse_access_log_enabled(enabled),
            format: parse_access_log_format(format),
            mode,
            filter,
            tags,
            otlp_endpoint: otlp_endpoint
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string),
            warnings,
        }
    }

    pub(super) fn allows(&self, event: &AccessLogEvent<'_>) -> bool {
        self.enabled && self.mode.includes_server() && self.filter.matches(event)
    }

    pub(super) fn warnings(&self) -> &[String] {
        &self.warnings
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AccessLogFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AccessLogMode {
    Client,
    Server,
    ClientAndServer,
}

impl AccessLogMode {
    fn includes_server(self) -> bool {
        matches!(self, Self::Server | Self::ClientAndServer)
    }
}

fn parse_access_log_mode(value: Option<&str>) -> (AccessLogMode, Option<String>) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return (AccessLogMode::ClientAndServer, None);
    };
    if value.eq_ignore_ascii_case("server") {
        return (AccessLogMode::Server, None);
    }
    if value.eq_ignore_ascii_case("client") {
        return (AccessLogMode::Client, None);
    }
    if value.eq_ignore_ascii_case("client_and_server")
        || value.eq_ignore_ascii_case("client-and-server")
        || value.eq_ignore_ascii_case("both")
    {
        return (AccessLogMode::ClientAndServer, None);
    }
    (
        AccessLogMode::ClientAndServer,
        Some(format!(
            "invalid DXGATE_ACCESS_LOG_MODE {value:?}; using CLIENT_AND_SERVER"
        )),
    )
}

fn parse_access_log_enabled(value: Option<&str>) -> bool {
    !matches!(
        value.map(str::trim).filter(|value| !value.is_empty()),
        Some(value)
            if value.eq_ignore_ascii_case("false")
                || value.eq_ignore_ascii_case("0")
                || value.eq_ignore_ascii_case("no")
                || value.eq_ignore_ascii_case("off")
    )
}

fn parse_access_log_format(value: Option<&str>) -> AccessLogFormat {
    match value.map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case("json") => AccessLogFormat::Json,
        _ => AccessLogFormat::Text,
    }
}

fn parse_access_log_tags(value: Option<&str>) -> (BTreeMap<String, String>, Option<String>) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return (BTreeMap::new(), None);
    };
    if let Ok(tags) = serde_json::from_str::<BTreeMap<String, String>>(value) {
        return (
            tags.into_iter()
                .filter(|(key, _)| !key.trim().is_empty())
                .collect(),
            None,
        );
    }
    let mut tags = BTreeMap::new();
    for pair in value.split(',') {
        let Some((key, tag_value)) = pair.split_once('=') else {
            return (
                BTreeMap::new(),
                Some(format!(
                    "invalid DXGATE_ACCESS_LOG_TAGS {value:?}; expected JSON object or key=value pairs"
                )),
            );
        };
        let key = key.trim();
        if key.is_empty() {
            return (
                BTreeMap::new(),
                Some(format!(
                    "invalid DXGATE_ACCESS_LOG_TAGS {value:?}; tag keys cannot be empty"
                )),
            );
        }
        tags.insert(key.to_string(), tag_value.trim().to_string());
    }
    (tags, None)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AccessLogFilter {
    All,
    Expression(FilterExpression),
    Never,
}

impl AccessLogFilter {
    fn parse(value: Option<&str>) -> (Self, Option<String>) {
        let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            return (Self::All, None);
        };
        match FilterExpression::parse(value) {
            Ok(expression) => (Self::Expression(expression), None),
            Err(err) => (
                Self::Never,
                Some(format!(
                    "invalid DXGATE_ACCESS_LOG_FILTER {value:?}; access logging disabled for safety: {err}"
                )),
            ),
        }
    }

    fn matches(&self, event: &AccessLogEvent<'_>) -> bool {
        match self {
            Self::All => true,
            Self::Expression(expression) => expression.matches(event),
            Self::Never => false,
        }
    }
}

/// Deliberately small CEL-compatible subset needed for gateway access logs.
/// Unsupported expressions disable only access logging rather than silently
/// broadening a user-specified filter.
#[derive(Clone, Debug, PartialEq, Eq)]
enum FilterExpression {
    True,
    False,
    Or(Vec<FilterExpression>),
    And(Vec<FilterExpression>),
    Not(Box<FilterExpression>),
    Has(FilterField),
    StartsWith(FilterField, String),
    StringCompare(FilterField, Comparison, String),
    StatusCompare(Comparison, u16),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FilterField {
    ResponseCode,
    RequestMethod,
    RequestHost,
    RequestPath,
    ClusterName,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Comparison {
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

impl FilterExpression {
    fn parse(raw: &str) -> Result<Self, String> {
        let raw = strip_outer_parentheses(raw.trim());
        if raw.eq_ignore_ascii_case("true") {
            return Ok(Self::True);
        }
        if raw.eq_ignore_ascii_case("false") {
            return Ok(Self::False);
        }
        let or_parts = split_top_level(raw, "||");
        if or_parts.len() > 1 {
            return or_parts
                .into_iter()
                .map(Self::parse)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Or);
        }
        let and_parts = split_top_level(raw, "&&");
        if and_parts.len() > 1 {
            return and_parts
                .into_iter()
                .map(Self::parse)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::And);
        }
        if let Some(value) = raw.strip_prefix('!') {
            return Self::parse(value).map(|value| Self::Not(Box::new(value)));
        }
        if let Some(field) = raw
            .strip_prefix("has(")
            .and_then(|value| value.strip_suffix(')'))
            .and_then(parse_filter_field)
        {
            return Ok(Self::Has(field));
        }
        if let Some((field, value)) = parse_starts_with(raw)? {
            return Ok(Self::StartsWith(field, value));
        }
        for (token, comparison) in [
            (">=", Comparison::GreaterOrEqual),
            ("<=", Comparison::LessOrEqual),
            ("!=", Comparison::NotEqual),
            ("==", Comparison::Equal),
            (">", Comparison::Greater),
            ("<", Comparison::Less),
        ] {
            if let Some((left, right)) = split_operator(raw, token) {
                let field = parse_filter_field(left.trim())
                    .ok_or_else(|| format!("unsupported field {left:?}"))?;
                if field == FilterField::ResponseCode {
                    let value = right.trim().parse::<u16>().map_err(|_| {
                        format!("response.code requires an HTTP status, got {right:?}")
                    })?;
                    return Ok(Self::StatusCompare(comparison, value));
                }
                if !matches!(comparison, Comparison::Equal | Comparison::NotEqual) {
                    return Err(format!("only == and != are valid for {left}"));
                }
                return Ok(Self::StringCompare(
                    field,
                    comparison,
                    parse_string_literal(right)?,
                ));
            }
        }
        Err("supported forms: response.code comparisons, request method/host/path equality, xds.cluster_name equality, has(...), startsWith(...), !, &&, ||".to_string())
    }

    fn matches(&self, event: &AccessLogEvent<'_>) -> bool {
        match self {
            Self::True => true,
            Self::False => false,
            Self::Or(values) => values.iter().any(|value| value.matches(event)),
            Self::And(values) => values.iter().all(|value| value.matches(event)),
            Self::Not(value) => !value.matches(event),
            Self::Has(FilterField::ResponseCode) => true,
            Self::Has(field) => !field.value(event).is_empty(),
            Self::StartsWith(field, prefix) => field.value(event).starts_with(prefix),
            Self::StringCompare(field, comparison, value) => {
                compare_strings(field.value(event), comparison, value)
            }
            Self::StatusCompare(comparison, value) => {
                compare_status(event.status_code, comparison, *value)
            }
        }
    }
}

impl FilterField {
    fn value<'a>(self, event: &'a AccessLogEvent<'_>) -> &'a str {
        match self {
            Self::ResponseCode => "",
            Self::RequestMethod => event.method,
            Self::RequestHost => event.host,
            Self::RequestPath => event.path,
            Self::ClusterName => event.cluster,
        }
    }
}

fn parse_filter_field(value: &str) -> Option<FilterField> {
    match value.trim() {
        "response.code" => Some(FilterField::ResponseCode),
        "request.method" => Some(FilterField::RequestMethod),
        "request.host" => Some(FilterField::RequestHost),
        "request.path" | "request.url_path" => Some(FilterField::RequestPath),
        "xds.cluster_name" => Some(FilterField::ClusterName),
        _ => None,
    }
}

fn parse_starts_with(raw: &str) -> Result<Option<(FilterField, String)>, String> {
    let Some((field, rest)) = raw.split_once(".startsWith(") else {
        return Ok(None);
    };
    let Some(value) = rest.strip_suffix(')') else {
        return Err("startsWith requires a closing ')'".to_string());
    };
    let field = parse_filter_field(field).ok_or_else(|| format!("unsupported field {field:?}"))?;
    if field == FilterField::ResponseCode {
        return Err("response.code does not support startsWith".to_string());
    }
    Ok(Some((field, parse_string_literal(value)?)))
}

fn parse_string_literal(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.len() >= 2 {
        let first = value.as_bytes()[0];
        let last = value.as_bytes()[value.len() - 1];
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            return Ok(value[1..value.len() - 1].to_string());
        }
    }
    if value.chars().any(char::is_whitespace) {
        return Err(format!("string literal {value:?} must be quoted"));
    }
    Ok(value.to_string())
}

fn compare_strings(left: &str, comparison: &Comparison, right: &str) -> bool {
    match comparison {
        Comparison::Equal => left == right,
        Comparison::NotEqual => left != right,
        _ => false,
    }
}

fn compare_status(left: u16, comparison: &Comparison, right: u16) -> bool {
    match comparison {
        Comparison::Equal => left == right,
        Comparison::NotEqual => left != right,
        Comparison::Greater => left > right,
        Comparison::GreaterOrEqual => left >= right,
        Comparison::Less => left < right,
        Comparison::LessOrEqual => left <= right,
    }
}

fn strip_outer_parentheses(mut value: &str) -> &str {
    loop {
        let trimmed = value.trim();
        if !trimmed.starts_with('(') || !trimmed.ends_with(')') || !outer_parentheses_match(trimmed)
        {
            return trimmed;
        }
        value = &trimmed[1..trimmed.len() - 1];
    }
}

fn outer_parentheses_match(value: &str) -> bool {
    let mut depth = 0_i32;
    let mut quote = None;
    for (index, character) in value.char_indices() {
        if let Some(current) = quote {
            if character == current {
                quote = None;
            }
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character);
            continue;
        }
        match character {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 && index + character.len_utf8() != value.len() {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0 && quote.is_none()
}

fn split_top_level<'a>(value: &'a str, token: &str) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut index = 0;
    let mut depth = 0_i32;
    let mut quote = None;
    while index < value.len() {
        let character = value[index..].chars().next().expect("valid UTF-8");
        if let Some(current) = quote {
            if character == current {
                quote = None;
            }
            index += character.len_utf8();
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character);
            index += character.len_utf8();
            continue;
        }
        match character {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && value[index..].starts_with(token) {
            parts.push(value[start..index].trim());
            index += token.len();
            start = index;
            continue;
        }
        index += character.len_utf8();
    }
    if parts.is_empty() {
        vec![value]
    } else {
        parts.push(value[start..].trim());
        parts
    }
}

fn split_operator<'a>(value: &'a str, token: &str) -> Option<(&'a str, &'a str)> {
    let parts = split_top_level(value, token);
    if parts.len() == 2 {
        Some((parts[0], parts[1]))
    } else {
        None
    }
}

pub(super) struct AccessLogEvent<'a> {
    pub(super) namespace: &'a str,
    pub(super) gateway: &'a str,
    pub(super) route: &'a str,
    pub(super) cluster: &'a str,
    pub(super) protocol: &'a str,
    pub(super) backend: &'a str,
    pub(super) method: &'a str,
    pub(super) host: &'a str,
    pub(super) path: &'a str,
    pub(super) status_code: u16,
    pub(super) latency_ms: u64,
    pub(super) upstream: &'a str,
    pub(super) trace_id: &'a str,
    pub(super) span_id: &'a str,
}

pub(super) fn access_log_line(
    format: AccessLogFormat,
    event: &AccessLogEvent<'_>,
    tags: &BTreeMap<String, String>,
) -> String {
    match format {
        AccessLogFormat::Text => format!(
            "namespace={} gateway={} route={} cluster={} protocol={} backend={} method={} host={} path={} status_code={} latency_ms={} upstream={} trace_id={} span_id={} tags={}",
            event.namespace,
            event.gateway,
            event.route,
            event.cluster,
            event.protocol,
            event.backend,
            event.method,
            event.host,
            event.path,
            event.status_code,
            event.latency_ms,
            event.upstream,
            event.trace_id,
            event.span_id,
            serde_json::to_string(tags).unwrap_or_else(|_| "{}".to_string())
        ),
        AccessLogFormat::Json => serde_json::json!({
            "namespace": event.namespace,
            "gateway": event.gateway,
            "route": event.route,
            "cluster": event.cluster,
            "protocol": event.protocol,
            "backend": event.backend,
            "method": event.method,
            "host": event.host,
            "path": event.path,
            "status_code": event.status_code,
            "latency_ms": event.latency_ms,
            "upstream": event.upstream,
            "trace_id": event.trace_id,
            "span_id": event.span_id,
            "tags": tags,
        })
        .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(status_code: u16) -> AccessLogEvent<'static> {
        AccessLogEvent {
            namespace: "default",
            gateway: "edge",
            route: "httpbin",
            cluster: "httpbin-v1",
            protocol: "http",
            backend: "httpbin-v1",
            method: "GET",
            host: "httpbin.example",
            path: "/status",
            status_code,
            latency_ms: 17,
            upstream: "httpbin:8080",
            trace_id: "",
            span_id: "",
        }
    }

    #[test]
    fn telemetry_values_filter_server_access_logs_and_keep_tags() {
        let config = AccessLogConfig::from_options(
            Some("true"),
            Some("json"),
            Some("SERVER"),
            Some("response.code >= 500 && request.path.startsWith('/status')"),
            Some(r#"{"tenant":"payments","region":"us-east"}"#),
            Some("http://otel-collector:4317"),
        );
        assert!(!config.allows(&event(200)));
        assert!(config.allows(&event(503)));
        assert_eq!(config.tags["tenant"], "payments");
        assert_eq!(
            config.otlp_endpoint.as_deref(),
            Some("http://otel-collector:4317")
        );
    }

    #[test]
    fn client_mode_does_not_emit_gateway_server_access_logs() {
        let config =
            AccessLogConfig::from_options(Some("true"), None, Some("CLIENT"), None, None, None);
        assert!(!config.allows(&event(200)));
    }

    #[test]
    fn invalid_filter_fails_closed_without_rejecting_proxy_startup() {
        let config = AccessLogConfig::from_options(
            Some("true"),
            None,
            None,
            Some("request.header['x-tenant'] == 'a'"),
            None,
            None,
        );
        assert!(!config.allows(&event(200)));
        assert_eq!(config.warnings.len(), 1);
    }

    #[test]
    fn text_and_json_lines_contain_custom_tags() {
        let tags = BTreeMap::from([(String::from("tenant"), String::from("edge"))]);
        let text = access_log_line(AccessLogFormat::Text, &event(502), &tags);
        assert!(text.contains("tags={\"tenant\":\"edge\"}"));
        let json = access_log_line(AccessLogFormat::Json, &event(502), &tags);
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["tags"]["tenant"], "edge");
        assert_eq!(value["protocol"], "http");
    }
}
