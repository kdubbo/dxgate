use dxgate_core::{
    Cluster, ConfigConflict, DxgateError, Endpoint, RateLimitPolicy, Result, RuntimeConfig,
    WeightedBackend, WeightedCluster,
};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    picker_counter: AtomicU64,
    rate_limits: Mutex<HashMap<String, RateLimitBucket>>,
    circuit_breakers: Mutex<HashMap<String, CircuitBreakerBucket>>,
    mcp_sessions: Mutex<HashMap<String, String>>,
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
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpRouteMetric {
    pub route: String,
    pub cluster: String,
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
}

#[derive(Debug, Default)]
struct HttpRouteMetricCounter {
    route: String,
    cluster: String,
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

#[derive(Debug, Default)]
struct CircuitBreakerBucket {
    active: u32,
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
                picker_counter: AtomicU64::new(0),
                rate_limits: Mutex::new(HashMap::new()),
                circuit_breakers: Mutex::new(HashMap::new()),
                mcp_sessions: Mutex::new(HashMap::new()),
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
        clusters: &'a [WeightedCluster],
    ) -> Option<&'a WeightedCluster> {
        let total: u32 = clusters.iter().map(|c| c.weight).sum();
        if total == 0 {
            return clusters.first();
        }
        let next = self.inner.picker_counter.fetch_add(1, Ordering::Relaxed) as u32 % total;
        let mut cursor = 0;
        clusters.iter().find(|cluster| {
            cursor += cluster.weight;
            next < cursor
        })
    }

    pub async fn pick_backend<'a>(
        &self,
        backends: &'a [WeightedBackend],
    ) -> Option<&'a WeightedBackend> {
        let total: u32 = backends.iter().map(|b| b.weight).sum();
        if total == 0 {
            return backends.first();
        }
        let next = self.inner.picker_counter.fetch_add(1, Ordering::Relaxed) as u32 % total;
        let mut cursor = 0;
        backends.iter().find(|backend| {
            cursor += backend.weight;
            next < cursor
        })
    }

    pub async fn pick_endpoint<'a>(
        &self,
        cluster_name: &str,
        endpoints: &'a [Endpoint],
    ) -> Result<&'a Endpoint> {
        let healthy: Vec<&Endpoint> = endpoints.iter().filter(|ep| ep.healthy).collect();
        if healthy.is_empty() {
            return Err(DxgateError::NoHealthyEndpoints(cluster_name.to_string()));
        }
        let idx =
            self.inner.picker_counter.fetch_add(1, Ordering::Relaxed) as usize % healthy.len();
        Ok(healthy[idx])
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

    pub fn record_http_request(&self, route: &str, cluster: &str, status: u16, latency_ms: u64) {
        let mut metrics = self.inner.metrics.lock().unwrap();
        metrics.total_requests += 1;
        if status >= 500 {
            metrics.upstream_failures += 1;
        }
        let key = format!("{route}|{cluster}");
        let route_metric =
            metrics
                .http_routes
                .entry(key)
                .or_insert_with(|| HttpRouteMetricCounter {
                    route: route.to_string(),
                    cluster: cluster.to_string(),
                    ..HttpRouteMetricCounter::default()
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
            .insert(session_id.into(), backend.into());
    }

    pub fn mcp_session_backend(&self, session_id: &str) -> Option<String> {
        self.inner
            .mcp_sessions
            .lock()
            .unwrap()
            .get(session_id)
            .cloned()
    }

    pub fn remove_mcp_session(&self, session_id: &str) {
        self.inner.mcp_sessions.lock().unwrap().remove(session_id);
    }

    pub fn metrics(&self) -> ProxyMetrics {
        let metrics = self.inner.metrics.lock().unwrap();
        let mut http_routes = metrics
            .http_routes
            .values()
            .map(|route| HttpRouteMetric {
                route: route.route.clone(),
                cluster: route.cluster.clone(),
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
            a.route
                .cmp(&b.route)
                .then_with(|| a.cluster.cmp(&b.cluster))
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
        ProxyMetrics {
            total_requests: metrics.total_requests,
            agent_requests: metrics.agent_requests,
            policy_denied: metrics.policy_denied,
            upstream_failures: metrics.upstream_failures,
            http_routes,
            routes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dxgate_core::{
        Cluster, Listener, ListenerProtocol, PathMatch, Route, RouteMatch, VirtualHost,
    };

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
            names.push(state.pick_cluster(&clusters).await.unwrap().name.clone());
        }

        assert_eq!(names, ["a", "a", "b", "a", "a", "b"]);
    }

    #[test]
    fn circuit_breaker_enforces_concurrent_limit() {
        let state = ProxyState::new(RuntimeConfig::empty("test"));
        let cluster = Cluster {
            name: "backend".into(),
            endpoints: vec![],
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

        let endpoint = state.pick_endpoint("backend", &endpoints).await.unwrap();
        assert_eq!(endpoint.address, "10.0.0.2");

        let unhealthy = vec![Endpoint {
            address: "10.0.0.3".into(),
            port: 8080,
            healthy: false,
            node_name: None,
        }];
        assert!(state.pick_endpoint("backend", &unhealthy).await.is_err());
    }

    #[test]
    fn records_http_route_metrics() {
        let state = ProxyState::new(RuntimeConfig::empty("test"));

        state.record_http_request("default", "reviews", 200, 12);
        state.record_http_request("default", "reviews", 502, 260);

        let metrics = state.metrics();
        assert_eq!(metrics.total_requests, 2);
        assert_eq!(metrics.upstream_failures, 1);
        assert_eq!(metrics.http_routes.len(), 1);
        let route = &metrics.http_routes[0];
        assert_eq!(route.route, "default");
        assert_eq!(route.cluster, "reviews");
        assert_eq!(route.requests, 2);
        assert_eq!(route.failures, 1);
        assert_eq!(route.latency_ms_sum, 272);
        assert_eq!(route.latency_ms_buckets[2].count, 1);
        assert_eq!(route.latency_ms_buckets[6].count, 2);
    }
}
