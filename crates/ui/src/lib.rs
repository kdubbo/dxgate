use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use dxgate_proxy::{
    A2aMethodMetric, HttpRouteConcurrencyMetric, HttpRouteMetric, LlmUsageMetric, McpToolMetric,
    ProxyMetrics, ProxyState, Readiness, RouteMetric,
};
use serde::Serialize;
use std::future::Future;
use std::net::SocketAddr;

const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Debug, Clone, Serialize)]
pub struct BuildInfo {
    pub name: &'static str,
    pub version: &'static str,
}

#[derive(Clone)]
pub struct UiServer {
    state: ProxyState,
    build: BuildInfo,
    proxy_port: u16,
}

impl UiServer {
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
        self.serve_with_shutdown(addr, std::future::pending::<()>())
            .await
    }

    /// Serves until `shutdown` resolves, then stops accepting and lets in-flight
    /// requests finish before returning.
    pub async fn serve_with_shutdown(
        self,
        addr: SocketAddr,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        let app = Router::new()
            .route("/", get(ui_page))
            .route("/ui", get(ui_page))
            .route("/assets/dxgate-logo.svg", get(logo_svg))
            .route("/healthz", get(healthz))
            .route("/readyz", get(readyz))
            .route("/metrics", get(metrics))
            .route("/debug/config", get(debug_config))
            .route("/debug/routes", get(debug_routes))
            .route("/debug/clusters", get(debug_clusters))
            .route("/debug/backends", get(debug_backends))
            .route("/debug/policies", get(debug_policies))
            .route("/debug/sources", get(debug_sources))
            .with_state(self);

        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(std::io::Error::other)
    }
}

async fn ui_page(State(ui): State<UiServer>) -> Html<String> {
    Html(ui_html(ui.proxy_port))
}

async fn logo_svg() -> Response {
    (
        [(header::CONTENT_TYPE, "image/svg+xml; charset=utf-8")],
        include_str!("../../../logo/dxgate-logo.svg"),
    )
        .into_response()
}

async fn healthz(State(ui): State<UiServer>) -> Json<BuildInfo> {
    Json(ui.build)
}

async fn readyz(State(ui): State<UiServer>) -> Response {
    let readiness = ui.state.readiness();
    let status = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(readiness)).into_response()
}

async fn metrics(State(ui): State<UiServer>) -> Response {
    let readiness = ui.state.readiness();
    let proxy = ui.state.metrics();
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
    out.push_str("# HELP dxgate_requests_in_flight Requests being handled right now\n# TYPE dxgate_requests_in_flight gauge\n");
    out.push_str(&format!(
        "dxgate_requests_in_flight {}\n",
        proxy.concurrency.in_flight
    ));
    // Counted separately from in-flight requests: these are waiting on a
    // scale-up, not on an upstream, so folding them together would read as the
    // gateway having gone slow.
    out.push_str("# HELP dxgate_activation_requests_held Requests waiting for a scaled-to-zero target to come up\n# TYPE dxgate_activation_requests_held gauge\n");
    out.push_str(&format!(
        "dxgate_activation_requests_held {}\n",
        proxy.held_activation_requests
    ));
    // Scale on rate() of this rather than on the gauge above: the gauge is a
    // single instant and misses every burst that lands between two scrapes.
    out.push_str("# HELP dxgate_request_seconds_total Accumulated request time; rate() gives average concurrency\n# TYPE dxgate_request_seconds_total counter\n");
    out.push_str(&format!(
        "dxgate_request_seconds_total {}\n",
        proxy.concurrency.seconds_total
    ));
    out.push_str("# HELP dxgate_http_route_requests_in_flight Requests in flight by route and cluster\n# TYPE dxgate_http_route_requests_in_flight gauge\n");
    for route in &proxy.http_route_concurrency {
        let labels = http_route_concurrency_labels(route);
        out.push_str(&format!(
            "dxgate_http_route_requests_in_flight{{{labels}}} {}\n",
            route.concurrency.in_flight
        ));
    }
    out.push_str("# HELP dxgate_http_route_request_seconds_total Accumulated request time by route and cluster\n# TYPE dxgate_http_route_request_seconds_total counter\n");
    for route in &proxy.http_route_concurrency {
        let labels = http_route_concurrency_labels(route);
        out.push_str(&format!(
            "dxgate_http_route_request_seconds_total{{{labels}}} {}\n",
            route.concurrency.seconds_total
        ));
    }
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

fn http_route_concurrency_labels(route: &HttpRouteConcurrencyMetric) -> String {
    prometheus_labels(&[
        ("namespace", route.namespace.as_str()),
        ("gateway", route.gateway.as_str()),
        ("route", route.route.as_str()),
        ("cluster", route.cluster.as_str()),
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

async fn debug_config(State(ui): State<UiServer>) -> Json<dxgate_core::RuntimeConfig> {
    Json(ui.state.snapshot().to_runtime_config())
}

async fn debug_routes(State(ui): State<UiServer>) -> Json<serde_json::Value> {
    let snapshot = ui.state.snapshot();
    let routes: Vec<_> = snapshot
        .route_table()
        .into_iter()
        .map(|entry| {
            serde_json::json!({
                "listener": entry.listener,
                "virtualHost": entry.virtual_host,
                "route": entry.route.name,
                "weightedClusters": entry.route.weighted_clusters,
            })
        })
        .collect();
    Json(serde_json::json!(routes))
}

async fn debug_clusters(State(ui): State<UiServer>) -> Json<serde_json::Value> {
    Json(serde_json::json!(
        ui.state.snapshot().to_runtime_config().clusters
    ))
}

async fn debug_backends(State(ui): State<UiServer>) -> Json<serde_json::Value> {
    let cfg = ui.state.snapshot().to_runtime_config();
    Json(serde_json::json!({
        "providers": cfg.providers,
        "backends": cfg.backends,
        "routes": cfg.routes,
    }))
}

async fn debug_policies(State(ui): State<UiServer>) -> Json<serde_json::Value> {
    let snapshot = ui.state.snapshot();
    let policies: Vec<_> = snapshot
        .to_runtime_config()
        .policies
        .into_iter()
        .map(|policy| {
            let attached: Vec<String> = snapshot
                .policy_refs(&policy.name)
                .iter()
                .map(ToString::to_string)
                .collect();
            serde_json::json!({ "policy": policy, "attachedTo": attached })
        })
        .collect();
    Json(serde_json::json!(policies))
}

/// Which source owns which resource, and the version each source last reported.
/// Configuration is merged from several sources, so "where did this come from"
/// is the first question when a resource is missing or unexpected.
async fn debug_sources(State(ui): State<UiServer>) -> Json<serde_json::Value> {
    let snapshot = ui.state.snapshot();
    let owners: Vec<_> = snapshot
        .owners()
        .iter()
        .map(|(key, source)| {
            serde_json::json!({
                "kind": key.kind.as_str(),
                "name": key.name,
                "source": source.as_str(),
            })
        })
        .collect();
    Json(serde_json::json!({
        "revision": snapshot.revision(),
        "sourceVersions": snapshot.source_versions(),
        "resources": owners,
    }))
}

fn ui_html(proxy_port: u16) -> String {
    UI_HTML.replace("__DXGATE_PROXY_PORT__", &proxy_port.to_string())
}

const UI_HTML: &str = include_str!("../../../ui/ui.html");

#[cfg(test)]
mod tests {
    use super::{prometheus_metrics, ui_html};
    use dxgate_proxy::{
        A2aMethodMetric, ConcurrencyMetric, HttpRouteConcurrencyMetric, HttpRouteMetric,
        LatencyBucket, LlmUsageMetric, McpToolMetric, ProxyMetrics, Readiness,
    };

    fn ready() -> Readiness {
        Readiness {
            ready: true,
            revision: 1,
            version: "static=test".into(),
            source_versions: Default::default(),
            conflicts: vec![],
        }
    }

    fn empty_metrics() -> ProxyMetrics {
        ProxyMetrics {
            held_activation_requests: 0,
            total_requests: 0,
            agent_requests: 0,
            policy_denied: 0,
            upstream_failures: 0,
            concurrency: ConcurrencyMetric::default(),
            http_route_concurrency: vec![],
            http_routes: vec![],
            routes: vec![],
            llm_usage: vec![],
            mcp_tools: vec![],
            a2a_methods: vec![],
        }
    }

    #[test]
    fn ui_page_contains_runtime_panels() {
        let html = ui_html(18080);

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
        assert!(!html.contains("<span>ui</span>"));
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
    fn prometheus_metrics_expose_concurrency_for_autoscaling() {
        let mut proxy = empty_metrics();
        proxy.concurrency = ConcurrencyMetric {
            in_flight: 7,
            seconds_total: 12.5,
        };
        proxy.http_route_concurrency = vec![HttpRouteConcurrencyMetric {
            namespace: "app".into(),
            gateway: "public".into(),
            route: "orders".into(),
            cluster: "orders-v1".into(),
            concurrency: ConcurrencyMetric {
                in_flight: 3,
                seconds_total: 4.25,
            },
        }];

        let text = prometheus_metrics(ready(), proxy);

        assert!(text.contains("# TYPE dxgate_requests_in_flight gauge"));
        assert!(text.contains("dxgate_requests_in_flight 7"));
        // A counter, so rate() over it is average concurrency regardless of
        // when the scrape lands.
        assert!(text.contains("# TYPE dxgate_request_seconds_total counter"));
        assert!(text.contains("dxgate_request_seconds_total 12.5"));
        assert!(text.contains(
            "dxgate_http_route_requests_in_flight{namespace=\"app\",gateway=\"public\",route=\"orders\",cluster=\"orders-v1\"} 3"
        ));
        assert!(text.contains(
            "dxgate_http_route_request_seconds_total{namespace=\"app\",gateway=\"public\",route=\"orders\",cluster=\"orders-v1\"} 4.25"
        ));
        // Per-route concurrency cannot carry method or status: neither is known
        // while the request is still in flight.
        let in_flight_line = text
            .lines()
            .find(|line| line.starts_with("dxgate_http_route_requests_in_flight{"))
            .expect("in-flight series");
        assert!(!in_flight_line.contains("method="));
        assert!(!in_flight_line.contains("status_code="));
    }

    #[test]
    fn prometheus_metrics_escape_labels_and_include_http_dimensions() {
        let text = prometheus_metrics(
            Readiness {
                ready: true,
                revision: 1,
                version: "static=test".into(),
                source_versions: Default::default(),
                conflicts: vec![],
            },
            ProxyMetrics {
                held_activation_requests: 0,
                total_requests: 1,
                agent_requests: 0,
                policy_denied: 0,
                upstream_failures: 1,
                concurrency: ConcurrencyMetric::default(),
                http_route_concurrency: vec![],
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
