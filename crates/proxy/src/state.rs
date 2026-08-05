use dxgate_core::{
    Cluster, ConfigConflict, DxgateError, Endpoint, OutlierDetectionConfig, RateLimitPolicy,
    Result, RuntimeConfig, TokenLimitPolicy, WeightedBackend, WeightedCluster,
};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

const LATENCY_BUCKETS_MS: [u64; 7] = [5, 10, 25, 50, 100, 250, 1000];

#[derive(Clone)]
pub struct ProxyState {
    inner: Arc<Inner>,
}

struct Inner {
    config: RwLock<RuntimeConfig>,
    conflicts: RwLock<Vec<ConfigConflict>>,
    ready: AtomicBool,
    // Round-robin cursors, one per selection domain: routes rotate over their
    // weighted clusters/backends, clusters rotate over their endpoints. A single
    // shared counter made each domain's sequence depend on unrelated traffic, so
    // whenever domains had different sizes the distribution skewed away from the
    // configured weights.
    route_pickers: Mutex<HashMap<String, u64>>,
    endpoint_pickers: Mutex<HashMap<String, u64>>,
    rate_limits: Mutex<HashMap<String, RateLimitBucket>>,
    token_usage: Mutex<HashMap<String, TokenBucket>>,
    circuit_breakers: Mutex<HashMap<String, CircuitBreakerBucket>>,
    outliers: Mutex<HashMap<String, OutlierBucket>>,
    mcp_sessions: Mutex<BindingMap>,
    a2a_tasks: Mutex<BindingMap>,
    metrics: Mutex<MetricsStore>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Readiness {
    pub ready: bool,
    pub version: String,
    pub conflicts: Vec<ConfigConflict>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProxyMetrics {
    pub total_requests: u64,
    pub agent_requests: u64,
    pub policy_denied: u64,
    pub upstream_failures: u64,
    pub http_routes: Vec<HttpRouteMetric>,
    pub routes: Vec<RouteMetric>,
    pub llm_usage: Vec<LlmUsageMetric>,
    pub mcp_tools: Vec<McpToolMetric>,
    pub a2a_methods: Vec<A2aMethodMetric>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LlmUsageMetric {
    pub route: String,
    pub backend: String,
    pub model: String,
    pub requests: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct McpToolMetric {
    pub route: String,
    pub backend: String,
    pub tool: String,
    pub calls: u64,
    pub failures: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct A2aMethodMetric {
    pub route: String,
    pub backend: String,
    pub method: String,
    pub calls: u64,
    pub failures: u64,
}

// Affinity bindings (MCP session -> backend, A2A task -> backend) are bounded
// so abandoned keys cannot grow proxy memory without limit; last_used
// refreshes on every routed request.
const BINDING_TTL: Duration = Duration::from_secs(60 * 60);
const BINDING_CAP: usize = 10_000;

#[derive(Debug)]
struct Binding {
    backend: String,
    last_used: Instant,
}

#[derive(Debug, Default)]
struct BindingMap {
    entries: HashMap<String, Binding>,
}

impl BindingMap {
    fn bind(&mut self, key: String, backend: String) {
        // The O(n) sweeps only run once the map is actually full.
        if self.entries.len() >= BINDING_CAP {
            self.entries
                .retain(|_, binding| binding.last_used.elapsed() < BINDING_TTL);
        }
        if self.entries.len() >= BINDING_CAP {
            // Bindings are an affinity optimization; dropping the idlest one
            // only costs that key its stickiness, not correctness.
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, binding)| binding.last_used)
                .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(
            key,
            Binding {
                backend,
                last_used: Instant::now(),
            },
        );
    }

    fn lookup(&mut self, key: &str) -> Option<String> {
        let binding = self.entries.get_mut(key)?;
        if binding.last_used.elapsed() >= BINDING_TTL {
            self.entries.remove(key);
            return None;
        }
        binding.last_used = Instant::now();
        Some(binding.backend.clone())
    }

    fn remove(&mut self, key: &str) {
        self.entries.remove(key);
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpRouteMetric {
    pub namespace: String,
    pub gateway: String,
    pub route: String,
    pub cluster: String,
    pub method: String,
    pub status_code: u16,
    pub requests: u64,
    pub failures: u64,
    pub latency_ms_sum: u64,
    pub latency_ms_buckets: Vec<LatencyBucket>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RouteMetric {
    pub protocol: String,
    pub route: String,
    pub backend: String,
    pub requests: u64,
    pub failures: u64,
    pub latency_ms_sum: u64,
    pub latency_ms_buckets: Vec<LatencyBucket>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyBucket {
    pub le: u64,
    pub count: u64,
}

#[derive(Debug, Default)]
struct MetricsStore {
    total_requests: u64,
    agent_requests: u64,
    policy_denied: u64,
    upstream_failures: u64,
    http_routes: HashMap<String, HttpRouteMetricCounter>,
    routes: HashMap<String, RouteMetricCounter>,
    llm_usage: HashMap<String, LlmUsageMetric>,
    mcp_tools: HashMap<String, McpToolMetric>,
    a2a_methods: HashMap<String, A2aMethodMetric>,
}

#[derive(Debug, Default)]
struct HttpRouteMetricCounter {
    namespace: String,
    gateway: String,
    route: String,
    cluster: String,
    method: String,
    status_code: u16,
    requests: u64,
    failures: u64,
    latency_ms_sum: u64,
    latency_ms_buckets: [u64; LATENCY_BUCKETS_MS.len()],
}

#[derive(Debug, Default)]
struct RouteMetricCounter {
    protocol: String,
    route: String,
    backend: String,
    requests: u64,
    failures: u64,
    latency_ms_sum: u64,
    latency_ms_buckets: [u64; LATENCY_BUCKETS_MS.len()],
}

#[derive(Debug)]
struct RateLimitBucket {
    window_started: Instant,
    used: u32,
}

#[derive(Debug)]
struct TokenBucket {
    window_started: Instant,
    window: Duration,
    used: u64,
}

impl TokenBucket {
    fn roll_window(&mut self) {
        if self.window_started.elapsed() >= self.window {
            self.window_started = Instant::now();
            self.used = 0;
        }
    }
}

#[derive(Debug, Default)]
struct CircuitBreakerBucket {
    active: u32,
}

// Envoy's defaults, applied when the control plane leaves a field unset.
const DEFAULT_CONSECUTIVE_5XX: u32 = 5;
const DEFAULT_BASE_EJECTION_TIME: Duration = Duration::from_secs(30);
const DEFAULT_MAX_EJECTION_PERCENT: u32 = 10;

#[derive(Debug, Default)]
struct OutlierBucket {
    consecutive_failures: u32,
    // Grows the ejection window for repeat offenders, as Envoy does.
    ejections: u32,
    ejected_until: Option<Instant>,
}

fn outlier_key(cluster: &str, endpoint: &Endpoint) -> String {
    format!("{cluster}|{}:{}", endpoint.address, endpoint.port)
}

/// How many of `total` endpoints may be ejected at once, per `max_ejection_percent`
/// and `min_health_percent`. Ejecting a whole cluster into unavailability would turn
/// a partial outage into a total one, so the caps are honored even when every
/// endpoint is failing.
fn ejection_allowance(total: usize, cfg: &OutlierDetectionConfig) -> usize {
    let max_pct = cfg
        .max_ejection_percent
        .unwrap_or(DEFAULT_MAX_EJECTION_PERCENT)
        .min(100) as usize;
    let by_max = total * max_pct / 100;
    let by_min_health = match cfg.min_health_percent {
        Some(pct) => total.saturating_sub((total * pct.min(100) as usize).div_ceil(100)),
        None => total,
    };
    by_max.min(by_min_health)
}

/// Parses the duration forms the xDS client emits (`"30s"`, `"0.500000000s"`) and the
/// `"500ms"` form static YAML tends to use. Unparseable values fall back to `default`.
fn parse_duration(raw: Option<&String>, default: Duration) -> Duration {
    let Some(raw) = raw.map(|value| value.trim()) else {
        return default;
    };
    let seconds = if let Some(ms) = raw.strip_suffix("ms") {
        ms.parse::<f64>().ok().map(|value| value / 1000.0)
    } else {
        raw.strip_suffix('s').and_then(|s| s.parse::<f64>().ok())
    };
    // from_secs_f64 panics on negative or non-finite input.
    seconds
        .filter(|value| value.is_finite() && *value > 0.0)
        .map(Duration::from_secs_f64)
        .unwrap_or(default)
}

/// Advances `key`'s round-robin cursor and returns its position within `modulus`.
/// Only the first sighting of a key allocates; the key set is bounded by config.
fn next_cursor(pickers: &Mutex<HashMap<String, u64>>, key: &str, modulus: u64) -> u64 {
    if modulus == 0 {
        return 0;
    }
    let mut pickers = pickers.lock().unwrap();
    let cursor = match pickers.get_mut(key) {
        Some(cursor) => cursor,
        None => pickers.entry(key.to_string()).or_insert(0),
    };
    let current = *cursor;
    *cursor = cursor.wrapping_add(1);
    current % modulus
}

pub struct CircuitBreakerPermit {
    state: ProxyState,
    cluster: String,
}

impl Drop for CircuitBreakerPermit {
    fn drop(&mut self) {
        self.state.release_circuit_breaker(&self.cluster);
    }
}

impl ProxyState {
    pub fn new(initial: RuntimeConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                config: RwLock::new(initial),
                conflicts: RwLock::new(Vec::new()),
                ready: AtomicBool::new(false),
                route_pickers: Mutex::new(HashMap::new()),
                endpoint_pickers: Mutex::new(HashMap::new()),
                rate_limits: Mutex::new(HashMap::new()),
                token_usage: Mutex::new(HashMap::new()),
                circuit_breakers: Mutex::new(HashMap::new()),
                outliers: Mutex::new(HashMap::new()),
                mcp_sessions: Mutex::new(BindingMap::default()),
                a2a_tasks: Mutex::new(BindingMap::default()),
                metrics: Mutex::new(MetricsStore::default()),
            }),
        }
    }

    pub async fn apply_config(
        &self,
        cfg: RuntimeConfig,
    ) -> std::result::Result<(), Vec<ConfigConflict>> {
        match cfg.validate() {
            Ok(()) => {
                let cluster_names: std::collections::HashSet<String> = cfg
                    .clusters
                    .iter()
                    .map(|cluster| cluster.name.clone())
                    .collect();
                self.inner
                    .circuit_breakers
                    .lock()
                    .unwrap()
                    .retain(|name, bucket| cluster_names.contains(name) || bucket.active > 0);
                // Cursors for routes/clusters the new config dropped would otherwise
                // linger for the process lifetime.
                self.inner
                    .endpoint_pickers
                    .lock()
                    .unwrap()
                    .retain(|name, _| cluster_names.contains(name));
                // Outlier keys are "{cluster}|{addr}:{port}"; drop those whose cluster
                // is gone, and those whose endpoint no longer appears in it.
                let endpoint_keys: std::collections::HashSet<String> = cfg
                    .clusters
                    .iter()
                    .flat_map(|cluster| {
                        cluster
                            .endpoints
                            .iter()
                            .map(|endpoint| outlier_key(&cluster.name, endpoint))
                    })
                    .collect();
                self.inner
                    .outliers
                    .lock()
                    .unwrap()
                    .retain(|key, _| endpoint_keys.contains(key));
                let route_names: std::collections::HashSet<&str> = cfg
                    .listeners
                    .iter()
                    .flat_map(|listener| &listener.virtual_hosts)
                    .flat_map(|host| &host.routes)
                    .map(|route| route.name.as_str())
                    .chain(cfg.routes.iter().map(|route| route.name.as_str()))
                    .collect();
                self.inner
                    .route_pickers
                    .lock()
                    .unwrap()
                    .retain(|name, _| route_names.contains(name.as_str()));
                *self.inner.config.write().await = cfg;
                self.inner.conflicts.write().await.clear();
                self.inner.ready.store(true, Ordering::SeqCst);
                Ok(())
            }
            Err(conflicts) => {
                *self.inner.conflicts.write().await = conflicts.clone();
                self.inner.ready.store(false, Ordering::SeqCst);
                Err(conflicts)
            }
        }
    }

    pub async fn config(&self) -> RuntimeConfig {
        self.inner.config.read().await.clone()
    }

    pub async fn readiness(&self) -> Readiness {
        Readiness {
            ready: self.inner.ready.load(Ordering::SeqCst),
            version: self.inner.config.read().await.version.clone(),
            conflicts: self.inner.conflicts.read().await.clone(),
        }
    }

    pub async fn pick_cluster<'a>(
        &self,
        route: &str,
        clusters: &'a [WeightedCluster],
    ) -> Option<&'a WeightedCluster> {
        let total: u32 = clusters.iter().map(|c| c.weight).sum();
        if total == 0 {
            return clusters.first();
        }
        let next = next_cursor(&self.inner.route_pickers, route, u64::from(total)) as u32;
        let mut cursor = 0;
        clusters.iter().find(|cluster| {
            cursor += cluster.weight;
            next < cursor
        })
    }

    pub async fn pick_backend<'a>(
        &self,
        route: &str,
        backends: &'a [WeightedBackend],
    ) -> Option<&'a WeightedBackend> {
        let total: u32 = backends.iter().map(|b| b.weight).sum();
        if total == 0 {
            return backends.first();
        }
        let next = next_cursor(&self.inner.route_pickers, route, u64::from(total)) as u32;
        let mut cursor = 0;
        backends.iter().find(|backend| {
            cursor += backend.weight;
            next < cursor
        })
    }

    pub async fn pick_endpoint<'a>(&self, cluster: &'a Cluster) -> Result<&'a Endpoint> {
        let healthy: Vec<&Endpoint> = cluster.endpoints.iter().filter(|ep| ep.healthy).collect();
        if healthy.is_empty() {
            return Err(DxgateError::NoHealthyEndpoints(cluster.name.clone()));
        }
        let candidates = self.admissible_endpoints(cluster, &healthy);
        let idx = next_cursor(
            &self.inner.endpoint_pickers,
            &cluster.name,
            candidates.len() as u64,
        ) as usize;
        Ok(candidates[idx])
    }

    /// `healthy` minus the endpoints outlier detection has currently ejected, capped
    /// so ejection can never empty the cluster. Never returns an empty vec.
    fn admissible_endpoints<'a>(
        &self,
        cluster: &Cluster,
        healthy: &[&'a Endpoint],
    ) -> Vec<&'a Endpoint> {
        let Some(outlier) = cluster.outlier_detection.as_ref() else {
            return healthy.to_vec();
        };
        let allowance = ejection_allowance(healthy.len(), outlier);
        if allowance == 0 {
            return healthy.to_vec();
        }
        let now = Instant::now();
        let buckets = self.inner.outliers.lock().unwrap();
        let mut ejected: Vec<(usize, Instant)> = healthy
            .iter()
            .enumerate()
            .filter_map(|(idx, endpoint)| {
                buckets
                    .get(&outlier_key(&cluster.name, endpoint))
                    .and_then(|bucket| bucket.ejected_until)
                    .filter(|until| *until > now)
                    .map(|until| (idx, until))
            })
            .collect();
        drop(buckets);
        if ejected.is_empty() {
            return healthy.to_vec();
        }
        // Over the cap: keep out the endpoints furthest from recovering and re-admit
        // the rest, so the cluster keeps serving from its least-bad members.
        if ejected.len() > allowance {
            ejected.sort_by_key(|(_, until)| std::cmp::Reverse(*until));
            ejected.truncate(allowance);
        }
        let excluded: std::collections::HashSet<usize> =
            ejected.into_iter().map(|(idx, _)| idx).collect();
        healthy
            .iter()
            .enumerate()
            .filter(|(idx, _)| !excluded.contains(idx))
            .map(|(_, endpoint)| *endpoint)
            .collect()
    }

    /// Feeds one upstream result into the cluster's outlier detector. A run of
    /// `consecutive_5xx_errors` failures ejects the endpoint for a window that grows
    /// with each repeat ejection; any success clears the run.
    pub fn record_endpoint_result(&self, cluster: &Cluster, endpoint: &Endpoint, status: u16) {
        let Some(outlier) = cluster.outlier_detection.as_ref() else {
            return;
        };
        let threshold = outlier
            .consecutive_5xx_errors
            .unwrap_or(DEFAULT_CONSECUTIVE_5XX);
        if threshold == 0 {
            return;
        }
        let key = outlier_key(&cluster.name, endpoint);
        let mut buckets = self.inner.outliers.lock().unwrap();
        if status < 500 {
            if let Some(bucket) = buckets.get_mut(&key) {
                bucket.consecutive_failures = 0;
            }
            return;
        }
        let base = parse_duration(
            outlier.base_ejection_time.as_ref(),
            DEFAULT_BASE_EJECTION_TIME,
        );
        let bucket = buckets.entry(key).or_default();
        bucket.consecutive_failures += 1;
        if bucket.consecutive_failures < threshold {
            return;
        }
        bucket.consecutive_failures = 0;
        bucket.ejections = bucket.ejections.saturating_add(1);
        // saturating_mul: a long-dead endpoint must not overflow its way to a panic.
        bucket.ejected_until = Some(Instant::now() + base.saturating_mul(bucket.ejections));
    }

    pub fn check_rate_limit(&self, key: String, limit: &RateLimitPolicy) -> bool {
        let mut buckets = self.inner.rate_limits.lock().unwrap();
        let bucket = buckets.entry(key).or_insert_with(|| RateLimitBucket {
            window_started: Instant::now(),
            used: 0,
        });
        let window = Duration::from_secs(limit.window_seconds.max(1));
        if bucket.window_started.elapsed() >= window {
            bucket.window_started = Instant::now();
            bucket.used = 0;
        }
        if bucket.used >= limit.requests {
            return false;
        }
        bucket.used += 1;
        true
    }

    // Token accounting is post-hoc: admission checks the window's recorded
    // usage, and each response's usage is added once the provider reports it.
    pub fn check_token_limit(&self, key: &str, limit: &TokenLimitPolicy) -> bool {
        let mut buckets = self.inner.token_usage.lock().unwrap();
        let Some(bucket) = buckets.get_mut(key) else {
            return true;
        };
        bucket.roll_window();
        bucket.used < limit.tokens
    }

    pub fn add_token_usage(&self, key: &str, window_seconds: u64, tokens: u64) {
        // Tracks the policy's current window even if its config changed.
        let window = Duration::from_secs(window_seconds.max(1));
        let mut buckets = self.inner.token_usage.lock().unwrap();
        let bucket = buckets
            .entry(key.to_string())
            .or_insert_with(|| TokenBucket {
                window_started: Instant::now(),
                window,
                used: 0,
            });
        bucket.window = window;
        bucket.roll_window();
        bucket.used = bucket.used.saturating_add(tokens);
    }

    // Err(()) means the breaker is open; there is no failure detail to carry.
    #[allow(clippy::result_unit_err)]
    pub fn try_acquire_circuit_breaker(
        &self,
        cluster: &Cluster,
    ) -> std::result::Result<Option<CircuitBreakerPermit>, ()> {
        let Some(limit) = cluster
            .circuit_breaker
            .as_ref()
            .and_then(|breaker| breaker.concurrent_request_limit())
        else {
            return Ok(None);
        };
        let mut buckets = self.inner.circuit_breakers.lock().unwrap();
        let bucket = buckets.entry(cluster.name.clone()).or_default();
        if bucket.active >= limit {
            return Err(());
        }
        bucket.active += 1;
        Ok(Some(CircuitBreakerPermit {
            state: self.clone(),
            cluster: cluster.name.clone(),
        }))
    }

    fn release_circuit_breaker(&self, cluster: &str) {
        if let Some(bucket) = self.inner.circuit_breakers.lock().unwrap().get_mut(cluster) {
            bucket.active = bucket.active.saturating_sub(1);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_http_request(
        &self,
        namespace: &str,
        gateway: &str,
        route: &str,
        cluster: &str,
        method: &str,
        status_code: u16,
        latency_ms: u64,
    ) {
        let mut metrics = self.inner.metrics.lock().unwrap();
        metrics.total_requests += 1;
        if status_code >= 500 {
            metrics.upstream_failures += 1;
        }
        let key = format!("{namespace}|{gateway}|{route}|{cluster}|{method}|{status_code}");
        let route_metric =
            metrics
                .http_routes
                .entry(key)
                .or_insert_with(|| HttpRouteMetricCounter {
                    namespace: namespace.to_string(),
                    gateway: gateway.to_string(),
                    route: route.to_string(),
                    cluster: cluster.to_string(),
                    method: method.to_string(),
                    status_code,
                    ..HttpRouteMetricCounter::default()
                });
        route_metric.requests += 1;
        if status_code >= 500 {
            route_metric.failures += 1;
        }
        route_metric.latency_ms_sum += latency_ms;
        for (idx, bucket) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if latency_ms <= *bucket {
                route_metric.latency_ms_buckets[idx] += 1;
            }
        }
    }

    pub fn record_agent_request(
        &self,
        protocol: &str,
        route: &str,
        backend: &str,
        status: u16,
        latency_ms: u64,
    ) {
        let mut metrics = self.inner.metrics.lock().unwrap();
        metrics.total_requests += 1;
        metrics.agent_requests += 1;
        if status >= 500 {
            metrics.upstream_failures += 1;
        }
        let key = format!("{protocol}|{route}|{backend}");
        let route_metric = metrics
            .routes
            .entry(key)
            .or_insert_with(|| RouteMetricCounter {
                protocol: protocol.to_string(),
                route: route.to_string(),
                backend: backend.to_string(),
                ..RouteMetricCounter::default()
            });
        route_metric.requests += 1;
        if status >= 500 {
            route_metric.failures += 1;
        }
        route_metric.latency_ms_sum += latency_ms;
        for (idx, bucket) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if latency_ms <= *bucket {
                route_metric.latency_ms_buckets[idx] += 1;
            }
        }
    }

    pub fn record_llm_usage(
        &self,
        route: &str,
        backend: &str,
        model: &str,
        prompt_tokens: u64,
        completion_tokens: u64,
    ) {
        let mut metrics = self.inner.metrics.lock().unwrap();
        let key = format!("{route}|{backend}|{model}");
        let counter = metrics
            .llm_usage
            .entry(key)
            .or_insert_with(|| LlmUsageMetric {
                route: route.to_string(),
                backend: backend.to_string(),
                model: model.to_string(),
                ..LlmUsageMetric::default()
            });
        counter.requests += 1;
        counter.prompt_tokens += prompt_tokens;
        counter.completion_tokens += completion_tokens;
    }

    pub fn record_policy_denied(&self) {
        let mut metrics = self.inner.metrics.lock().unwrap();
        metrics.total_requests += 1;
        metrics.policy_denied += 1;
    }

    pub fn bind_mcp_session(&self, session_id: impl Into<String>, backend: impl Into<String>) {
        self.inner
            .mcp_sessions
            .lock()
            .unwrap()
            .bind(session_id.into(), backend.into());
    }

    pub fn mcp_session_backend(&self, session_id: &str) -> Option<String> {
        self.inner.mcp_sessions.lock().unwrap().lookup(session_id)
    }

    pub fn remove_mcp_session(&self, session_id: &str) {
        self.inner.mcp_sessions.lock().unwrap().remove(session_id);
    }

    pub fn bind_a2a_task(&self, task_id: impl Into<String>, backend: impl Into<String>) {
        self.inner
            .a2a_tasks
            .lock()
            .unwrap()
            .bind(task_id.into(), backend.into());
    }

    pub fn a2a_task_backend(&self, task_id: &str) -> Option<String> {
        self.inner.a2a_tasks.lock().unwrap().lookup(task_id)
    }

    pub fn record_a2a_method_call(&self, route: &str, backend: &str, method: &str, success: bool) {
        let mut metrics = self.inner.metrics.lock().unwrap();
        let key = format!("{route}|{backend}|{method}");
        let counter = metrics
            .a2a_methods
            .entry(key)
            .or_insert_with(|| A2aMethodMetric {
                route: route.to_string(),
                backend: backend.to_string(),
                method: method.to_string(),
                ..A2aMethodMetric::default()
            });
        counter.calls += 1;
        if !success {
            counter.failures += 1;
        }
    }

    pub fn record_mcp_tool_call(&self, route: &str, backend: &str, tool: &str, success: bool) {
        let mut metrics = self.inner.metrics.lock().unwrap();
        let key = format!("{route}|{backend}|{tool}");
        let counter = metrics
            .mcp_tools
            .entry(key)
            .or_insert_with(|| McpToolMetric {
                route: route.to_string(),
                backend: backend.to_string(),
                tool: tool.to_string(),
                ..McpToolMetric::default()
            });
        counter.calls += 1;
        if !success {
            counter.failures += 1;
        }
    }

    pub fn metrics(&self) -> ProxyMetrics {
        let metrics = self.inner.metrics.lock().unwrap();
        let mut http_routes = metrics
            .http_routes
            .values()
            .map(|route| HttpRouteMetric {
                namespace: route.namespace.clone(),
                gateway: route.gateway.clone(),
                route: route.route.clone(),
                cluster: route.cluster.clone(),
                method: route.method.clone(),
                status_code: route.status_code,
                requests: route.requests,
                failures: route.failures,
                latency_ms_sum: route.latency_ms_sum,
                latency_ms_buckets: LATENCY_BUCKETS_MS
                    .iter()
                    .zip(route.latency_ms_buckets.iter())
                    .map(|(le, count)| LatencyBucket {
                        le: *le,
                        count: *count,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        http_routes.sort_by(|a, b| {
            a.namespace
                .cmp(&b.namespace)
                .then_with(|| a.gateway.cmp(&b.gateway))
                .then_with(|| a.route.cmp(&b.route))
                .then_with(|| a.cluster.cmp(&b.cluster))
                .then_with(|| a.method.cmp(&b.method))
                .then_with(|| a.status_code.cmp(&b.status_code))
        });
        let mut routes = metrics
            .routes
            .values()
            .map(|route| RouteMetric {
                protocol: route.protocol.clone(),
                route: route.route.clone(),
                backend: route.backend.clone(),
                requests: route.requests,
                failures: route.failures,
                latency_ms_sum: route.latency_ms_sum,
                latency_ms_buckets: LATENCY_BUCKETS_MS
                    .iter()
                    .zip(route.latency_ms_buckets.iter())
                    .map(|(le, count)| LatencyBucket {
                        le: *le,
                        count: *count,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        routes.sort_by(|a, b| {
            a.protocol
                .cmp(&b.protocol)
                .then_with(|| a.route.cmp(&b.route))
                .then_with(|| a.backend.cmp(&b.backend))
        });
        let mut llm_usage = metrics.llm_usage.values().cloned().collect::<Vec<_>>();
        llm_usage.sort_by(|a, b| {
            a.route
                .cmp(&b.route)
                .then_with(|| a.backend.cmp(&b.backend))
                .then_with(|| a.model.cmp(&b.model))
        });
        let mut mcp_tools = metrics.mcp_tools.values().cloned().collect::<Vec<_>>();
        mcp_tools.sort_by(|a, b| {
            a.route
                .cmp(&b.route)
                .then_with(|| a.backend.cmp(&b.backend))
                .then_with(|| a.tool.cmp(&b.tool))
        });
        let mut a2a_methods = metrics.a2a_methods.values().cloned().collect::<Vec<_>>();
        a2a_methods.sort_by(|a, b| {
            a.route
                .cmp(&b.route)
                .then_with(|| a.backend.cmp(&b.backend))
                .then_with(|| a.method.cmp(&b.method))
        });
        ProxyMetrics {
            total_requests: metrics.total_requests,
            agent_requests: metrics.agent_requests,
            policy_denied: metrics.policy_denied,
            upstream_failures: metrics.upstream_failures,
            http_routes,
            routes,
            llm_usage,
            mcp_tools,
            a2a_methods,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dxgate_core::{
        Cluster, Listener, ListenerProtocol, PathMatch, Route, RouteMatch, VirtualHost,
    };

    fn test_cluster(
        name: &str,
        endpoints: Vec<Endpoint>,
        outlier_detection: Option<OutlierDetectionConfig>,
    ) -> Cluster {
        Cluster {
            name: name.into(),
            endpoints,
            http2: false,
            tls: None,
            circuit_breaker: None,
            outlier_detection,
        }
    }

    fn endpoint(address: &str) -> Endpoint {
        Endpoint {
            address: address.into(),
            port: 8080,
            healthy: true,
            node_name: None,
        }
    }

    fn valid_config(version: &str) -> RuntimeConfig {
        RuntimeConfig {
            version: version.into(),
            listeners: vec![Listener {
                name: "http".into(),
                bind: "0.0.0.0:80".parse().unwrap(),
                protocol: ListenerProtocol::Http,
                virtual_hosts: vec![VirtualHost {
                    name: "wildcard".into(),
                    domains: vec!["*".into()],
                    routes: vec![Route {
                        name: "default".into(),
                        matches: vec![RouteMatch {
                            path: PathMatch::Prefix("/".into()),
                            headers: vec![],
                        }],
                        weighted_clusters: vec![WeightedCluster {
                            name: "backend".into(),
                            weight: 100,
                        }],
                    }],
                }],
                tls_secret: None,
            }],
            clusters: vec![Cluster {
                name: "backend".into(),
                endpoints: vec![Endpoint {
                    address: "127.0.0.1".into(),
                    port: 8080,
                    healthy: true,
                    node_name: None,
                }],
                http2: false,
                tls: None,
                circuit_breaker: None,
                outlier_detection: None,
            }],
            secrets: vec![],
            providers: vec![],
            backends: vec![],
            routes: vec![],
            policies: vec![],
        }
    }

    #[test]
    fn mcp_session_bindings_are_capped() {
        let state = ProxyState::new(RuntimeConfig::empty("bootstrap"));
        for i in 0..=BINDING_CAP {
            state.bind_mcp_session(format!("session-{i}"), "backend");
        }
        // The idlest binding (the first inserted) was evicted to stay at cap.
        assert_eq!(state.mcp_session_backend("session-0"), None);
        assert_eq!(
            state.mcp_session_backend(&format!("session-{BINDING_CAP}")),
            Some("backend".to_string())
        );
    }

    #[test]
    fn a2a_task_bindings_round_trip() {
        let state = ProxyState::new(RuntimeConfig::empty("bootstrap"));
        state.bind_a2a_task("task-1", "planner");
        assert_eq!(
            state.a2a_task_backend("task-1"),
            Some("planner".to_string())
        );
        assert_eq!(state.a2a_task_backend("task-2"), None);
    }

    #[test]
    fn mcp_tool_calls_aggregate_per_tool() {
        let state = ProxyState::new(RuntimeConfig::empty("bootstrap"));
        state.record_mcp_tool_call("mcp", "mcp-a", "search", true);
        state.record_mcp_tool_call("mcp", "mcp-a", "search", false);
        state.record_mcp_tool_call("mcp", "mcp-b", "calendar", true);

        let metrics = state.metrics();
        assert_eq!(metrics.mcp_tools.len(), 2);
        let search = metrics
            .mcp_tools
            .iter()
            .find(|tool| tool.tool == "search")
            .unwrap();
        assert_eq!(search.calls, 2);
        assert_eq!(search.failures, 1);
    }

    #[tokio::test]
    async fn apply_config_updates_readiness_and_conflicts() {
        let state = ProxyState::new(RuntimeConfig::empty("bootstrap"));
        state.apply_config(valid_config("ok")).await.unwrap();

        let readiness = state.readiness().await;
        assert!(readiness.ready);
        assert_eq!(readiness.version, "ok");
        assert!(readiness.conflicts.is_empty());

        let mut invalid = valid_config("bad");
        invalid.clusters.clear();
        let conflicts = state.apply_config(invalid).await.unwrap_err();

        let readiness = state.readiness().await;
        assert!(!readiness.ready);
        assert_eq!(readiness.conflicts, conflicts);
        assert_eq!(readiness.conflicts[0].kind, "missing-cluster");
    }

    #[tokio::test]
    async fn weighted_cluster_picker_is_deterministic() {
        let state = ProxyState::new(RuntimeConfig::empty("test"));
        let clusters = vec![
            WeightedCluster {
                name: "a".into(),
                weight: 2,
            },
            WeightedCluster {
                name: "b".into(),
                weight: 1,
            },
        ];
        let mut names = Vec::new();

        for _ in 0..6 {
            names.push(
                state
                    .pick_cluster("default", &clusters)
                    .await
                    .unwrap()
                    .name
                    .clone(),
            );
        }

        assert_eq!(names, ["a", "a", "b", "a", "a", "b"]);
    }

    #[test]
    fn circuit_breaker_enforces_concurrent_limit() {
        let state = ProxyState::new(RuntimeConfig::empty("test"));
        let cluster = Cluster {
            name: "backend".into(),
            endpoints: vec![],
            http2: false,
            tls: None,
            circuit_breaker: Some(dxgate_core::CircuitBreakerConfig {
                max_connections: None,
                http1_max_pending_requests: None,
                http2_max_requests: Some(1),
                max_requests_per_connection: None,
                max_retries: None,
            }),
            outlier_detection: None,
        };

        let permit = state
            .try_acquire_circuit_breaker(&cluster)
            .expect("first request should pass")
            .expect("configured circuit breaker should return a permit");
        assert!(state.try_acquire_circuit_breaker(&cluster).is_err());
        drop(permit);
        assert!(state
            .try_acquire_circuit_breaker(&cluster)
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn endpoint_picker_skips_unhealthy_endpoints() {
        let state = ProxyState::new(RuntimeConfig::empty("test"));
        let endpoints = vec![
            Endpoint {
                address: "10.0.0.1".into(),
                port: 8080,
                healthy: false,
                node_name: None,
            },
            Endpoint {
                address: "10.0.0.2".into(),
                port: 8080,
                healthy: true,
                node_name: None,
            },
        ];

        let cluster = test_cluster("backend", endpoints, None);
        let endpoint = state.pick_endpoint(&cluster).await.unwrap();
        assert_eq!(endpoint.address, "10.0.0.2");

        let unhealthy = test_cluster(
            "backend",
            vec![Endpoint {
                address: "10.0.0.3".into(),
                port: 8080,
                healthy: false,
                node_name: None,
            }],
            None,
        );
        assert!(state.pick_endpoint(&unhealthy).await.is_err());
    }

    #[tokio::test]
    async fn round_robin_cursors_are_per_cluster() {
        let state = ProxyState::new(RuntimeConfig::empty("test"));
        let a = test_cluster("a", vec![endpoint("10.0.0.1"), endpoint("10.0.0.2")], None);
        let b = test_cluster("b", vec![endpoint("10.1.0.1"), endpoint("10.1.0.2")], None);

        // Interleaving two clusters must not advance the other's cursor: each still
        // alternates over its own endpoints. A shared counter made `a` return
        // 10.0.0.1 every time here.
        let mut picked = Vec::new();
        for _ in 0..4 {
            picked.push(state.pick_endpoint(&a).await.unwrap().address.clone());
            let _ = state.pick_endpoint(&b).await.unwrap();
        }

        assert_eq!(picked, ["10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.2"]);
    }

    #[tokio::test]
    async fn outlier_detection_ejects_after_consecutive_failures() {
        let state = ProxyState::new(RuntimeConfig::empty("test"));
        let cluster = test_cluster(
            "backend",
            vec![endpoint("10.0.0.1"), endpoint("10.0.0.2")],
            Some(OutlierDetectionConfig {
                consecutive_5xx_errors: Some(2),
                interval: None,
                base_ejection_time: Some("30s".into()),
                // 50% of two endpoints: exactly one may be ejected.
                max_ejection_percent: Some(50),
                min_health_percent: None,
            }),
        );
        let bad = &cluster.endpoints[0];

        state.record_endpoint_result(&cluster, bad, 503);
        // One failure is below the threshold, so the endpoint still serves.
        assert!(state
            .admissible_endpoints(&cluster, &cluster.endpoints.iter().collect::<Vec<_>>())
            .iter()
            .any(|ep| ep.address == "10.0.0.1"));

        state.record_endpoint_result(&cluster, bad, 503);
        let admissible =
            state.admissible_endpoints(&cluster, &cluster.endpoints.iter().collect::<Vec<_>>());
        assert_eq!(admissible.len(), 1);
        assert_eq!(admissible[0].address, "10.0.0.2");

        // Every pick now avoids the ejected endpoint.
        for _ in 0..4 {
            assert_eq!(
                state.pick_endpoint(&cluster).await.unwrap().address,
                "10.0.0.2"
            );
        }
    }

    #[test]
    fn outlier_detection_success_clears_the_failure_run() {
        let state = ProxyState::new(RuntimeConfig::empty("test"));
        let cluster = test_cluster(
            "backend",
            vec![endpoint("10.0.0.1"), endpoint("10.0.0.2")],
            Some(OutlierDetectionConfig {
                consecutive_5xx_errors: Some(2),
                interval: None,
                base_ejection_time: None,
                max_ejection_percent: Some(50),
                min_health_percent: None,
            }),
        );
        let flaky = &cluster.endpoints[0];

        state.record_endpoint_result(&cluster, flaky, 503);
        state.record_endpoint_result(&cluster, flaky, 200);
        state.record_endpoint_result(&cluster, flaky, 503);

        // Failures were not consecutive, so nothing is ejected.
        assert_eq!(
            state
                .admissible_endpoints(&cluster, &cluster.endpoints.iter().collect::<Vec<_>>())
                .len(),
            2
        );
    }

    #[test]
    fn ejection_never_empties_a_cluster() {
        let cfg = OutlierDetectionConfig {
            consecutive_5xx_errors: Some(1),
            interval: None,
            base_ejection_time: None,
            max_ejection_percent: Some(100),
            min_health_percent: Some(50),
        };
        // min_health_percent caps the allowance below max_ejection_percent.
        assert_eq!(ejection_allowance(4, &cfg), 2);
        // Envoy's 10% default cannot eject either of two endpoints.
        assert_eq!(
            ejection_allowance(
                2,
                &OutlierDetectionConfig {
                    consecutive_5xx_errors: None,
                    interval: None,
                    base_ejection_time: None,
                    max_ejection_percent: None,
                    min_health_percent: None,
                }
            ),
            0
        );
    }

    #[test]
    fn outlier_durations_parse_xds_and_yaml_forms() {
        let fallback = Duration::from_secs(30);
        assert_eq!(
            parse_duration(Some(&"10s".to_string()), fallback),
            Duration::from_secs(10)
        );
        assert_eq!(
            parse_duration(Some(&"0.500000000s".to_string()), fallback),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_duration(Some(&"250ms".to_string()), fallback),
            Duration::from_millis(250)
        );
        // Unparseable, zero, and absent values all fall back rather than disabling
        // ejection with a zero-length window.
        assert_eq!(
            parse_duration(Some(&"soon".to_string()), fallback),
            fallback
        );
        assert_eq!(parse_duration(Some(&"0s".to_string()), fallback), fallback);
        assert_eq!(parse_duration(None, fallback), fallback);
    }

    #[test]
    fn records_http_route_metrics() {
        let state = ProxyState::new(RuntimeConfig::empty("test"));

        state.record_http_request("app", "public", "default", "reviews", "GET", 200, 12);
        state.record_http_request("app", "public", "default", "reviews", "GET", 502, 260);

        let metrics = state.metrics();
        assert_eq!(metrics.total_requests, 2);
        assert_eq!(metrics.upstream_failures, 1);
        assert_eq!(metrics.http_routes.len(), 2);
        let route = metrics
            .http_routes
            .iter()
            .find(|metric| metric.status_code == 502)
            .expect("missing 502 route metric");
        assert_eq!(route.namespace, "app");
        assert_eq!(route.gateway, "public");
        assert_eq!(route.route, "default");
        assert_eq!(route.cluster, "reviews");
        assert_eq!(route.method, "GET");
        assert_eq!(route.requests, 1);
        assert_eq!(route.failures, 1);
        assert_eq!(route.latency_ms_sum, 260);
        assert_eq!(route.latency_ms_buckets[6].count, 1);
    }
}
