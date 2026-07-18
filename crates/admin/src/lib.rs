use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use dxgate_proxy::{
    A2aMethodMetric, HttpRouteMetric, LlmUsageMetric, McpToolMetric, ProxyMetrics, ProxyState,
    Readiness, RouteMetric,
};
use serde::Serialize;
use std::net::SocketAddr;

const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Debug, Clone, Serialize)]
pub struct BuildInfo {
    pub name: &'static str,
    pub version: &'static str,
}

#[derive(Clone)]
pub struct AdminServer {
    state: ProxyState,
    build: BuildInfo,
    proxy_port: u16,
}

impl AdminServer {
    pub fn new(state: ProxyState, proxy_addr: SocketAddr) -> Self {
        Self {
            state,
            build: BuildInfo {
                name: "dxgate",
                version: env!("CARGO_PKG_VERSION"),
            },
            proxy_port: proxy_addr.port(),
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> std::io::Result<()> {
        let app = Router::new()
            .route("/", get(admin_ui))
            .route("/ui", get(admin_ui))
            .route("/assets/dxgate-logo.svg", get(logo_svg))
            .route("/healthz", get(healthz))
            .route("/readyz", get(readyz))
            .route("/metrics", get(metrics))
            .route("/debug/config", get(debug_config))
            .route("/debug/routes", get(debug_routes))
            .route("/debug/clusters", get(debug_clusters))
            .route("/debug/backends", get(debug_backends))
            .route("/debug/policies", get(debug_policies))
            .with_state(self);

        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .map_err(std::io::Error::other)
    }
}

async fn admin_ui(State(admin): State<AdminServer>) -> Html<String> {
    Html(admin_html(admin.proxy_port))
}

async fn logo_svg() -> Response {
    (
        [(header::CONTENT_TYPE, "image/svg+xml; charset=utf-8")],
        include_str!("../../../logo/dxgate-logo.svg"),
    )
        .into_response()
}

async fn healthz(State(admin): State<AdminServer>) -> Json<BuildInfo> {
    Json(admin.build)
}

async fn readyz(State(admin): State<AdminServer>) -> Response {
    let readiness = admin.state.readiness().await;
    let status = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(readiness)).into_response()
}

async fn metrics(State(admin): State<AdminServer>) -> Response {
    let readiness = admin.state.readiness().await;
    let proxy = admin.state.metrics();
    (
        [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
        prometheus_metrics(readiness, proxy),
    )
        .into_response()
}

fn prometheus_metrics(readiness: Readiness, proxy: ProxyMetrics) -> String {
    let mut out = format!(
        "# HELP dxgate_ready Whether dxgate has accepted runtime config\n# TYPE dxgate_ready gauge\ndxgate_ready {}\n# HELP dxgate_config_conflicts Current rejected config conflicts\n# TYPE dxgate_config_conflicts gauge\ndxgate_config_conflicts {}\n",
        if readiness.ready { 1 } else { 0 },
        readiness.conflicts.len()
    );
    out.push_str("# HELP dxgate_requests_total Total requests observed by dxgate\n# TYPE dxgate_requests_total counter\n");
    out.push_str(&format!("dxgate_requests_total {}\n", proxy.total_requests));
    out.push_str("# HELP dxgate_agent_requests_total Agent protocol requests observed by dxgate\n# TYPE dxgate_agent_requests_total counter\n");
    out.push_str(&format!(
        "dxgate_agent_requests_total {}\n",
        proxy.agent_requests
    ));
    out.push_str("# HELP dxgate_policy_denied_total Requests denied by dxgate policy\n# TYPE dxgate_policy_denied_total counter\n");
    out.push_str(&format!(
        "dxgate_policy_denied_total {}\n",
        proxy.policy_denied
    ));
    out.push_str("# HELP dxgate_upstream_failures_total Upstream failures observed by dxgate\n# TYPE dxgate_upstream_failures_total counter\n");
    out.push_str(&format!(
        "dxgate_upstream_failures_total {}\n",
        proxy.upstream_failures
    ));
    out.push_str("# HELP dxgate_http_route_requests_total HTTP gateway requests observed by route and cluster\n# TYPE dxgate_http_route_requests_total counter\n");
    for route in &proxy.http_routes {
        let labels = http_route_labels(route);
        out.push_str(&format!(
            "dxgate_http_route_requests_total{{{labels}}} {}\n",
            route.requests
        ));
    }
    out.push_str("# HELP dxgate_http_route_failures_total HTTP gateway upstream failures observed by route and cluster\n# TYPE dxgate_http_route_failures_total counter\n");
    for route in &proxy.http_routes {
        let labels = http_route_labels(route);
        out.push_str(&format!(
            "dxgate_http_route_failures_total{{{labels}}} {}\n",
            route.failures
        ));
    }
    out.push_str("# HELP dxgate_http_route_latency_ms HTTP gateway upstream latency in milliseconds\n# TYPE dxgate_http_route_latency_ms histogram\n");
    for route in &proxy.http_routes {
        let labels = http_route_labels(route);
        out.push_str(&format!(
            "dxgate_http_route_latency_ms_sum{{{labels}}} {}\n",
            route.latency_ms_sum
        ));
        for bucket in &route.latency_ms_buckets {
            out.push_str(&format!(
                "dxgate_http_route_latency_ms_bucket{{{labels},le=\"{}\"}} {}\n",
                bucket.le, bucket.count
            ));
        }
        out.push_str(&format!(
            "dxgate_http_route_latency_ms_bucket{{{labels},le=\"+Inf\"}} {}\n",
            route.requests
        ));
        out.push_str(&format!(
            "dxgate_http_route_latency_ms_count{{{labels}}} {}\n",
            route.requests
        ));
    }
    out.push_str("# HELP dxgate_agent_route_requests_total Agent protocol requests observed by route and backend\n# TYPE dxgate_agent_route_requests_total counter\n");
    for route in &proxy.routes {
        let labels = agent_route_labels(route);
        out.push_str(&format!(
            "dxgate_agent_route_requests_total{{{labels}}} {}\n",
            route.requests
        ));
    }
    out.push_str("# HELP dxgate_agent_route_failures_total Agent protocol upstream failures observed by route and backend\n# TYPE dxgate_agent_route_failures_total counter\n");
    for route in &proxy.routes {
        let labels = agent_route_labels(route);
        out.push_str(&format!(
            "dxgate_agent_route_failures_total{{{labels}}} {}\n",
            route.failures
        ));
    }
    out.push_str("# HELP dxgate_agent_route_latency_ms Agent protocol upstream latency in milliseconds\n# TYPE dxgate_agent_route_latency_ms histogram\n");
    for route in &proxy.routes {
        let labels = agent_route_labels(route);
        out.push_str(&format!(
            "dxgate_agent_route_latency_ms_sum{{{labels}}} {}\n",
            route.latency_ms_sum
        ));
        for bucket in &route.latency_ms_buckets {
            out.push_str(&format!(
                "dxgate_agent_route_latency_ms_bucket{{{labels},le=\"{}\"}} {}\n",
                bucket.le, bucket.count
            ));
        }
        out.push_str(&format!(
            "dxgate_agent_route_latency_ms_bucket{{{labels},le=\"+Inf\"}} {}\n",
            route.requests
        ));
        out.push_str(&format!(
            "dxgate_agent_route_latency_ms_count{{{labels}}} {}\n",
            route.requests
        ));
    }
    out.push_str("# HELP dxgate_llm_requests_total LLM requests with recorded token usage\n# TYPE dxgate_llm_requests_total counter\n");
    for usage in &proxy.llm_usage {
        let labels = llm_usage_labels(usage);
        out.push_str(&format!(
            "dxgate_llm_requests_total{{{labels}}} {}\n",
            usage.requests
        ));
    }
    out.push_str("# HELP dxgate_llm_tokens_total LLM tokens observed by route, backend, and model\n# TYPE dxgate_llm_tokens_total counter\n");
    for usage in &proxy.llm_usage {
        let labels = llm_usage_labels(usage);
        out.push_str(&format!(
            "dxgate_llm_tokens_total{{{labels},type=\"prompt\"}} {}\n",
            usage.prompt_tokens
        ));
        out.push_str(&format!(
            "dxgate_llm_tokens_total{{{labels},type=\"completion\"}} {}\n",
            usage.completion_tokens
        ));
    }
    out.push_str("# HELP dxgate_mcp_tool_calls_total MCP tools/call requests by route, backend, and tool\n# TYPE dxgate_mcp_tool_calls_total counter\n");
    for tool in &proxy.mcp_tools {
        let labels = mcp_tool_labels(tool);
        out.push_str(&format!(
            "dxgate_mcp_tool_calls_total{{{labels}}} {}\n",
            tool.calls
        ));
    }
    out.push_str("# HELP dxgate_mcp_tool_failures_total MCP tools/call requests that did not return a success status\n# TYPE dxgate_mcp_tool_failures_total counter\n");
    for tool in &proxy.mcp_tools {
        let labels = mcp_tool_labels(tool);
        out.push_str(&format!(
            "dxgate_mcp_tool_failures_total{{{labels}}} {}\n",
            tool.failures
        ));
    }
    out.push_str("# HELP dxgate_a2a_method_calls_total A2A JSON-RPC requests by route, backend, and method\n# TYPE dxgate_a2a_method_calls_total counter\n");
    for method in &proxy.a2a_methods {
        let labels = a2a_method_labels(method);
        out.push_str(&format!(
            "dxgate_a2a_method_calls_total{{{labels}}} {}\n",
            method.calls
        ));
    }
    out.push_str("# HELP dxgate_a2a_method_failures_total A2A JSON-RPC requests that did not return a success status\n# TYPE dxgate_a2a_method_failures_total counter\n");
    for method in &proxy.a2a_methods {
        let labels = a2a_method_labels(method);
        out.push_str(&format!(
            "dxgate_a2a_method_failures_total{{{labels}}} {}\n",
            method.failures
        ));
    }
    out
}

fn llm_usage_labels(usage: &LlmUsageMetric) -> String {
    prometheus_labels(&[
        ("route", usage.route.as_str()),
        ("backend", usage.backend.as_str()),
        ("model", usage.model.as_str()),
    ])
}

fn mcp_tool_labels(tool: &McpToolMetric) -> String {
    prometheus_labels(&[
        ("route", tool.route.as_str()),
        ("backend", tool.backend.as_str()),
        ("tool", tool.tool.as_str()),
    ])
}

fn a2a_method_labels(method: &A2aMethodMetric) -> String {
    prometheus_labels(&[
        ("route", method.route.as_str()),
        ("backend", method.backend.as_str()),
        ("method", method.method.as_str()),
    ])
}

fn http_route_labels(route: &HttpRouteMetric) -> String {
    let status_code = route.status_code.to_string();
    prometheus_labels(&[
        ("namespace", route.namespace.as_str()),
        ("gateway", route.gateway.as_str()),
        ("route", route.route.as_str()),
        ("cluster", route.cluster.as_str()),
        ("method", route.method.as_str()),
        ("status_code", status_code.as_str()),
    ])
}

fn agent_route_labels(route: &RouteMetric) -> String {
    prometheus_labels(&[
        ("protocol", route.protocol.as_str()),
        ("route", route.route.as_str()),
        ("backend", route.backend.as_str()),
    ])
}

fn prometheus_labels(labels: &[(&str, &str)]) -> String {
    labels
        .iter()
        .map(|(name, value)| format!("{name}=\"{}\"", prometheus_label_value(value)))
        .collect::<Vec<_>>()
        .join(",")
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

async fn debug_config(State(admin): State<AdminServer>) -> Json<dxgate_core::RuntimeConfig> {
    Json(admin.state.config().await)
}

async fn debug_routes(State(admin): State<AdminServer>) -> Json<serde_json::Value> {
    let cfg = admin.state.config().await;
    let routes: Vec<_> = cfg
        .listeners
        .iter()
        .flat_map(|listener| {
            listener.virtual_hosts.iter().flat_map(move |host| {
                host.routes.iter().map(move |route| {
                    serde_json::json!({
                        "listener": listener.name,
                        "virtualHost": host.name,
                        "route": route.name,
                        "domains": host.domains,
                        "weightedClusters": route.weighted_clusters,
                    })
                })
            })
        })
        .collect();
    Json(serde_json::json!(routes))
}

async fn debug_clusters(State(admin): State<AdminServer>) -> Json<serde_json::Value> {
    let cfg = admin.state.config().await;
    Json(serde_json::json!(cfg.clusters))
}

async fn debug_backends(State(admin): State<AdminServer>) -> Json<serde_json::Value> {
    let cfg = admin.state.config().await;
    Json(serde_json::json!({
        "providers": cfg.providers,
        "backends": cfg.backends,
        "routes": cfg.routes,
    }))
}

async fn debug_policies(State(admin): State<AdminServer>) -> Json<serde_json::Value> {
    let cfg = admin.state.config().await;
    Json(serde_json::json!(cfg.policies))
}

fn admin_html(proxy_port: u16) -> String {
    ADMIN_HTML.replace("__DXGATE_PROXY_PORT__", &proxy_port.to_string())
}

const ADMIN_HTML: &str = include_str!("../../../ui/admin.html");

#[cfg(test)]
mod tests {
    use super::{admin_html, prometheus_metrics};
    use dxgate_proxy::{
        A2aMethodMetric, HttpRouteMetric, LatencyBucket, LlmUsageMetric, McpToolMetric,
        ProxyMetrics, Readiness,
    };

    #[test]
    fn admin_ui_contains_runtime_panels() {
        let html = admin_html(18080);

        assert!(html.contains("Overview"));
        assert!(html.contains("/debug/config"));
        assert!(html.contains("/metrics"));
        assert!(html.contains("const proxyPort = 18080;"));
        assert!(html.contains("MCP request"));
        assert!(html.contains("/assets/dxgate-logo.svg"));
        assert!(html.contains("id=\"metric-requests\""));
        assert!(html.contains("id=\"metric-failures\""));
        assert!(html.contains("dxgate_requests_total"));
        assert!(html.contains("dxgate_upstream_failures_total"));
        assert!(html.contains("Dubbo clusters"));
        assert!(html.contains("id=\"clusters-table\""));
        assert!(html.contains("cfgList('clusters')"));
        assert!(!html.contains("class=\"mark\""));
        assert!(!html.contains("<strong>dxgate</strong>"));
        assert!(!html.contains("<span>admin</span>"));
        assert!(!html.contains("id=\"statusline\""));
        assert!(!html.contains("id=\"source-line\""));
        assert!(!html.contains("class=\"pill"));
        assert!(!html.contains("loading runtime data"));
        assert!(!html.contains("id=\"copy-config\""));
        assert!(!html.contains("id=\"refresh\""));
        assert!(!html.contains("Copy config"));
        assert!(!html.contains(">Refresh</button>"));
        assert!(!html.contains("getJson('/debug/backends')"));
        assert!(!html.contains("getJson('/debug/policies')"));
        assert!(!html.contains("getJson('/debug/routes')"));
        assert!(!html.contains("metric-routes\">0"));
        assert!(!html.contains("value=\"/mcp\""));
        assert!(!html.contains("mcp-result\">{}"));
    }

    #[test]
    fn prometheus_metrics_escape_labels_and_include_http_dimensions() {
        let text = prometheus_metrics(
            Readiness {
                ready: true,
                version: "test".into(),
                conflicts: vec![],
            },
            ProxyMetrics {
                total_requests: 1,
                agent_requests: 0,
                policy_denied: 0,
                upstream_failures: 1,
                http_routes: vec![HttpRouteMetric {
                    namespace: "app\nns".into(),
                    gateway: "public\"gw".into(),
                    route: "default\\route".into(),
                    cluster: "reviews".into(),
                    method: "GET".into(),
                    status_code: 502,
                    requests: 1,
                    failures: 1,
                    latency_ms_sum: 25,
                    latency_ms_buckets: vec![
                        LatencyBucket { le: 5, count: 0 },
                        LatencyBucket { le: 25, count: 1 },
                    ],
                }],
                routes: vec![],
                llm_usage: vec![LlmUsageMetric {
                    route: "llm".into(),
                    backend: "claude".into(),
                    model: "claude-3".into(),
                    requests: 2,
                    prompt_tokens: 30,
                    completion_tokens: 12,
                }],
                mcp_tools: vec![McpToolMetric {
                    route: "mcp".into(),
                    backend: "mcp-a".into(),
                    tool: "search".into(),
                    calls: 3,
                    failures: 1,
                }],
                a2a_methods: vec![A2aMethodMetric {
                    route: "a2a".into(),
                    backend: "planner".into(),
                    method: "message/send".into(),
                    calls: 4,
                    failures: 2,
                }],
            },
        );

        assert!(text.contains("namespace=\"app\\nns\""));
        assert!(text.contains("gateway=\"public\\\"gw\""));
        assert!(text.contains("route=\"default\\\\route\""));
        assert!(text.contains("method=\"GET\""));
        assert!(text.contains("status_code=\"502\""));
        assert!(text.contains("dxgate_http_route_latency_ms_sum{"));
        assert!(text.contains("dxgate_http_route_latency_ms_count{"));
        assert!(text.contains("le=\"+Inf\""));
        assert!(text.contains(
            "dxgate_mcp_tool_calls_total{route=\"mcp\",backend=\"mcp-a\",tool=\"search\"} 3"
        ));
        assert!(text.contains(
            "dxgate_mcp_tool_failures_total{route=\"mcp\",backend=\"mcp-a\",tool=\"search\"} 1"
        ));
        assert!(text.contains(
            "dxgate_a2a_method_calls_total{route=\"a2a\",backend=\"planner\",method=\"message/send\"} 4"
        ));
        assert!(text.contains(
            "dxgate_a2a_method_failures_total{route=\"a2a\",backend=\"planner\",method=\"message/send\"} 2"
        ));
    }
}
