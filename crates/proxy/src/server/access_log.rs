//! Access-log configuration and line formatting. The glue that reads live
//! request state (`emit_http_access_log`) lives in the parent module; this
//! module holds the pure config parsing and text/JSON rendering.

use std::env;

#[derive(Clone)]
pub(super) struct AccessLogConfig {
    pub(super) enabled: bool,
    pub(super) format: AccessLogFormat,
}

impl AccessLogConfig {
    pub(super) fn from_env() -> Self {
        let enabled = env::var("DXGATE_ACCESS_LOG").ok();
        let format = env::var("DXGATE_ACCESS_LOG_FORMAT").ok();
        Self::from_values(enabled.as_deref(), format.as_deref())
    }

    pub(super) fn from_values(enabled: Option<&str>, format: Option<&str>) -> Self {
        Self {
            enabled: parse_access_log_enabled(enabled),
            format: parse_access_log_format(format),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AccessLogFormat {
    Text,
    Json,
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

pub(super) struct AccessLogEvent<'a> {
    pub(super) namespace: &'a str,
    pub(super) gateway: &'a str,
    pub(super) route: &'a str,
    pub(super) cluster: &'a str,
    pub(super) method: &'a str,
    pub(super) host: &'a str,
    pub(super) path: &'a str,
    pub(super) status_code: u16,
    pub(super) latency_ms: u64,
    pub(super) upstream: &'a str,
    pub(super) trace_id: &'a str,
    pub(super) span_id: &'a str,
}

pub(super) fn access_log_line(format: AccessLogFormat, event: &AccessLogEvent<'_>) -> String {
    match format {
        AccessLogFormat::Text => format!(
            "namespace={} gateway={} route={} cluster={} method={} host={} path={} status_code={} latency_ms={} upstream={} trace_id={} span_id={}",
            event.namespace,
            event.gateway,
            event.route,
            event.cluster,
            event.method,
            event.host,
            event.path,
            event.status_code,
            event.latency_ms,
            event.upstream,
            event.trace_id,
            event.span_id
        ),
        AccessLogFormat::Json => serde_json::json!({
            "namespace": event.namespace,
            "gateway": event.gateway,
            "route": event.route,
            "cluster": event.cluster,
            "method": event.method,
            "host": event.host,
            "path": event.path,
            "status_code": event.status_code,
            "latency_ms": event.latency_ms,
            "upstream": event.upstream,
            "trace_id": event.trace_id,
            "span_id": event.span_id,
        })
        .to_string(),
    }
}
