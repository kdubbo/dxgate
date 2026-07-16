use crate::{ConfigConflict, DxgateError, MatchInput, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

pub const HTTP_LISTENER_PORT: u16 = 80;
pub const HTTPS_LISTENER_PORT: u16 = 443;
pub const ADMIN_PORT: u16 = 15021;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub version: String,
    #[serde(default)]
    pub listeners: Vec<Listener>,
    #[serde(default)]
    pub clusters: Vec<Cluster>,
    #[serde(default)]
    pub secrets: Vec<TlsSecret>,
    #[serde(default)]
    pub providers: Vec<Provider>,
    #[serde(default)]
    pub backends: Vec<Backend>,
    #[serde(default)]
    pub routes: Vec<AgentRoute>,
    #[serde(default)]
    pub policies: Vec<Policy>,
}

impl RuntimeConfig {
    pub fn empty(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            listeners: Vec::new(),
            clusters: Vec::new(),
            secrets: Vec::new(),
            providers: Vec::new(),
            backends: Vec::new(),
            routes: Vec::new(),
            policies: Vec::new(),
        }
    }

    pub fn validate(&self) -> std::result::Result<(), Vec<ConfigConflict>> {
        let mut conflicts = Vec::new();
        let mut listener_names = BTreeSet::new();
        let mut binds: BTreeMap<SocketAddr, (&str, ListenerProtocol, bool)> = BTreeMap::new();

        for listener in &self.listeners {
            if !listener_names.insert(listener.name.as_str()) {
                conflicts.push(ConfigConflict::new(
                    "duplicate-listener",
                    format!("listener {} is defined more than once", listener.name),
                ));
            }

            let tls_enabled = listener.tls_secret.is_some();
            if let Some((existing_name, existing_protocol, existing_tls)) = binds.insert(
                listener.bind,
                (&listener.name, listener.protocol, tls_enabled),
            ) {
                if existing_protocol != listener.protocol || existing_tls != tls_enabled {
                    conflicts.push(ConfigConflict::new(
                        "listener-bind-conflict",
                        format!(
                            "listeners {} and {} both bind {} with incompatible protocol or TLS mode",
                            existing_name, listener.name, listener.bind
                        ),
                    ));
                }
            }
        }

        let mut clusters = BTreeSet::new();
        for cluster in &self.clusters {
            if !clusters.insert(cluster.name.as_str()) {
                conflicts.push(ConfigConflict::new(
                    "duplicate-cluster",
                    format!("cluster {} is defined more than once", cluster.name),
                ));
            }
        }

        let mut providers = BTreeSet::new();
        for provider in &self.providers {
            if !providers.insert(provider.name.as_str()) {
                conflicts.push(ConfigConflict::new(
                    "duplicate-provider",
                    format!("provider {} is defined more than once", provider.name),
                ));
            }
        }

        let mut backends = BTreeSet::new();
        for backend in &self.backends {
            if !backends.insert(backend.name.as_str()) {
                conflicts.push(ConfigConflict::new(
                    "duplicate-backend",
                    format!("backend {} is defined more than once", backend.name),
                ));
            }
            if let BackendKind::Llm { provider, .. } = &backend.kind {
                if !providers.contains(provider.as_str()) {
                    conflicts.push(ConfigConflict::new(
                        "missing-provider",
                        format!(
                            "backend {} references missing provider {}",
                            backend.name, provider
                        ),
                    ));
                }
            }
            for policy in &backend.policies {
                if !self.policies.iter().any(|p| p.name == *policy) {
                    conflicts.push(ConfigConflict::new(
                        "missing-policy",
                        format!(
                            "backend {} references missing policy {}",
                            backend.name, policy
                        ),
                    ));
                }
            }
        }

        let mut policies = BTreeSet::new();
        for policy in &self.policies {
            if !policies.insert(policy.name.as_str()) {
                conflicts.push(ConfigConflict::new(
                    "duplicate-policy",
                    format!("policy {} is defined more than once", policy.name),
                ));
            }
        }

        let mut routes = BTreeSet::new();
        for route in &self.routes {
            if !routes.insert(route.name.as_str()) {
                conflicts.push(ConfigConflict::new(
                    "duplicate-agent-route",
                    format!("agent route {} is defined more than once", route.name),
                ));
            }
            if route.weighted_backends.is_empty() {
                conflicts.push(ConfigConflict::new(
                    "empty-agent-route-destination",
                    format!("agent route {} has no weighted backends", route.name),
                ));
            }
            for dst in &route.weighted_backends {
                if !backends.contains(dst.name.as_str()) {
                    conflicts.push(ConfigConflict::new(
                        "missing-backend",
                        format!(
                            "agent route {} references missing backend {}",
                            route.name, dst.name
                        ),
                    ));
                }
            }
            for policy in &route.policies {
                if !policies.contains(policy.as_str()) {
                    conflicts.push(ConfigConflict::new(
                        "missing-policy",
                        format!(
                            "agent route {} references missing policy {}",
                            route.name, policy
                        ),
                    ));
                }
            }
        }

        for listener in &self.listeners {
            for host in &listener.virtual_hosts {
                for route in &host.routes {
                    if route.weighted_clusters.is_empty() {
                        conflicts.push(ConfigConflict::new(
                            "empty-route-destination",
                            format!(
                                "route {} on listener {} has no weighted clusters",
                                route.name, listener.name
                            ),
                        ));
                    }
                    for dst in &route.weighted_clusters {
                        if !clusters.contains(dst.name.as_str()) {
                            conflicts.push(ConfigConflict::new(
                                "missing-cluster",
                                format!(
                                    "route {} references missing cluster {}",
                                    route.name, dst.name
                                ),
                            ));
                        }
                    }
                }
            }
        }

        if conflicts.is_empty() {
            Ok(())
        } else {
            Err(conflicts)
        }
    }

    pub fn listener_by_port(&self, port: u16) -> Option<&Listener> {
        self.listeners.iter().find(|l| l.bind.port() == port)
    }

    pub fn cluster(&self, name: &str) -> Option<&Cluster> {
        self.clusters.iter().find(|c| c.name == name)
    }

    pub fn provider(&self, name: &str) -> Option<&Provider> {
        self.providers.iter().find(|p| p.name == name)
    }

    pub fn backend(&self, name: &str) -> Option<&Backend> {
        self.backends.iter().find(|b| b.name == name)
    }

    pub fn policy(&self, name: &str) -> Option<&Policy> {
        self.policies.iter().find(|p| p.name == name)
    }

    pub fn agent_route_for<'a>(&'a self, input: &AgentMatchInput<'_>) -> Option<&'a AgentRoute> {
        self.routes
            .iter()
            .filter(|route| route.protocol == input.protocol)
            .find(|route| route.matches(input))
    }

    pub fn route_for<'a>(&'a self, port: u16, input: &MatchInput<'_>) -> Result<&'a Route> {
        let listener = self
            .listener_by_port(port)
            .ok_or_else(|| DxgateError::RouteNotFound {
                host: input.host.to_string(),
                path: input.path.to_string(),
            })?;

        listener
            .virtual_hosts
            .iter()
            .filter(|vh| vh.matches_host(input.host))
            .flat_map(|vh| vh.routes.iter())
            .find(|route| route.matches(input))
            .ok_or_else(|| DxgateError::RouteNotFound {
                host: input.host.to_string(),
                path: input.path.to_string(),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listener {
    pub name: String,
    pub bind: SocketAddr,
    pub protocol: ListenerProtocol,
    #[serde(default)]
    pub virtual_hosts: Vec<VirtualHost>,
    #[serde(default)]
    pub tls_secret: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListenerProtocol {
    Http,
    Https,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualHost {
    pub name: String,
    #[serde(default)]
    pub domains: Vec<String>,
    #[serde(default)]
    pub routes: Vec<Route>,
}

impl VirtualHost {
    pub fn matches_host(&self, host: &str) -> bool {
        self.domains.iter().any(|domain| {
            domain == "*"
                || domain.eq_ignore_ascii_case(host)
                || domain
                    .strip_prefix("*.")
                    .map(|suffix| host.ends_with(suffix))
                    .unwrap_or(false)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Route {
    pub name: String,
    #[serde(default)]
    pub matches: Vec<RouteMatch>,
    #[serde(default)]
    pub weighted_clusters: Vec<WeightedCluster>,
}

impl Route {
    pub fn matches(&self, input: &MatchInput<'_>) -> bool {
        self.matches.is_empty() || self.matches.iter().any(|m| m.matches(input))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteMatch {
    #[serde(default)]
    pub path: PathMatch,
    #[serde(default)]
    pub headers: Vec<HeaderMatch>,
}

impl RouteMatch {
    pub fn matches(&self, input: &MatchInput<'_>) -> bool {
        self.path.matches(input.path) && self.headers.iter().all(|h| h.matches(input))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "lowercase")]
pub enum PathMatch {
    Prefix(String),
    Exact(String),
}

impl Default for PathMatch {
    fn default() -> Self {
        Self::Prefix("/".to_string())
    }
}

impl PathMatch {
    pub fn matches(&self, path: &str) -> bool {
        match self {
            Self::Prefix(prefix) => path.starts_with(prefix),
            Self::Exact(exact) => path == exact,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderMatch {
    pub name: String,
    pub value: String,
}

impl HeaderMatch {
    pub fn matches(&self, input: &MatchInput<'_>) -> bool {
        input
            .headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case(&self.name) && value == &self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeightedCluster {
    pub name: String,
    pub weight: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cluster {
    pub name: String,
    #[serde(default)]
    pub endpoints: Vec<Endpoint>,
    // Forces HTTP/2 to this cluster (h2c on plaintext, ALPN h2 over TLS) even when
    // the request is not gRPC/Triple; gRPC and Triple requests always use HTTP/2.
    #[serde(default, skip_serializing_if = "is_false")]
    pub http2: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<UpstreamTls>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outlier_detection: Option<OutlierDetectionConfig>,
}

impl Cluster {
    pub fn healthy_endpoints(&self) -> impl Iterator<Item = &Endpoint> {
        self.endpoints.iter().filter(|ep| ep.healthy)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http1_max_pending_requests: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http2_max_requests: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_requests_per_connection: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
}

impl CircuitBreakerConfig {
    pub fn concurrent_request_limit(&self) -> Option<u32> {
        self.http2_max_requests
            .or(self.max_connections)
            .or(self.http1_max_pending_requests)
            .filter(|limit| *limit > 0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutlierDetectionConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consecutive_5xx_errors: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ejection_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ejection_percent: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_health_percent: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamTls {
    #[serde(default, skip_serializing_if = "is_default_tls_mode")]
    pub mode: UpstreamTlsMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sni: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub alpn_protocols: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamTlsMode {
    Simple,
    #[default]
    DubboMutual,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_default_tls_mode(mode: &UpstreamTlsMode) -> bool {
    *mode == UpstreamTlsMode::default()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    pub address: String,
    pub port: u16,
    #[serde(default = "default_true")]
    pub healthy: bool,
    #[serde(default)]
    pub node_name: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsSecret {
    pub name: String,
    pub certificate_chain_pem: String,
    pub private_key_pem: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provider {
    pub name: String,
    #[serde(default = "default_openai_compatible")]
    pub kind: ProviderKind,
    pub base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub request_headers: Vec<HeaderValue>,
}

fn default_openai_compatible() -> ProviderKind {
    ProviderKind::OpenAiCompatible
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderKind {
    OpenAiCompatible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Backend {
    pub name: String,
    #[serde(flatten)]
    pub kind: BackendKind,
    #[serde(default)]
    pub policies: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BackendKind {
    Http {
        endpoint: String,
    },
    Llm {
        provider: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        models: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
    },
    Mcp {
        endpoint: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tools: Vec<String>,
    },
    A2a {
        endpoint: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent: Option<String>,
    },
}

impl Backend {
    pub fn endpoint<'a>(&'a self, provider: Option<&'a Provider>) -> Option<&'a str> {
        match &self.kind {
            BackendKind::Http { endpoint }
            | BackendKind::Mcp { endpoint, .. }
            | BackendKind::A2a { endpoint, .. } => Some(endpoint),
            BackendKind::Llm { endpoint, .. } => endpoint
                .as_deref()
                .or_else(|| provider.map(|provider| provider.base_url.as_str())),
        }
    }

    // A request that names no model/tool (e.g. MCP initialize or tools/list)
    // is not constrained, so backends with declared capabilities stay eligible.
    pub fn supports_model(&self, model: Option<&str>) -> bool {
        match &self.kind {
            BackendKind::Llm { models, .. } => {
                models.is_empty() || model.map(|m| models.iter().any(|v| v == m)).unwrap_or(true)
            }
            _ => true,
        }
    }

    pub fn supports_tool(&self, tool: Option<&str>) -> bool {
        match &self.kind {
            BackendKind::Mcp { tools, .. } => {
                tools.is_empty() || tool.map(|t| tools.iter().any(|v| v == t)).unwrap_or(true)
            }
            _ => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRoute {
    pub name: String,
    pub protocol: AgentProtocol,
    #[serde(default)]
    pub matches: Vec<AgentRouteMatch>,
    #[serde(default)]
    pub weighted_backends: Vec<WeightedBackend>,
    #[serde(default)]
    pub policies: Vec<String>,
}

impl AgentRoute {
    pub fn matches(&self, input: &AgentMatchInput<'_>) -> bool {
        self.matches.is_empty() || self.matches.iter().any(|m| m.matches(input))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentProtocol {
    Http,
    Llm,
    Mcp,
    A2a,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRouteMatch {
    #[serde(default)]
    pub path: PathMatch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default)]
    pub headers: Vec<HeaderMatch>,
}

impl AgentRouteMatch {
    pub fn matches(&self, input: &AgentMatchInput<'_>) -> bool {
        self.path.matches(input.path)
            && optional_matches(self.host.as_deref(), Some(input.host), host_matches)
            && optional_matches(self.method.as_deref(), Some(input.method), |a, b| {
                a.eq_ignore_ascii_case(b)
            })
            && optional_matches(self.model.as_deref(), input.model, str_matches)
            && optional_matches(self.tool.as_deref(), input.tool, str_matches)
            && optional_matches(self.agent.as_deref(), input.agent, str_matches)
            && self
                .headers
                .iter()
                .all(|h| h.matches_headers(input.headers))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeightedBackend {
    pub name: String,
    pub weight: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct AgentMatchInput<'a> {
    pub protocol: AgentProtocol,
    pub host: &'a str,
    pub path: &'a str,
    pub method: &'a str,
    pub model: Option<&'a str>,
    pub tool: Option<&'a str>,
    pub agent: Option<&'a str>,
    pub headers: &'a [(String, String)],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    pub name: String,
    #[serde(default)]
    pub action: PolicyAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matches: Option<PolicyMatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateLimitPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_body_bytes: Option<usize>,
    #[serde(default)]
    pub request_headers: HeaderTransform,
    #[serde(default)]
    pub response_headers: HeaderTransform,
}

impl Policy {
    pub fn applies_to(&self, input: &AgentMatchInput<'_>) -> bool {
        self.matches
            .as_ref()
            .map(|m| m.matches(input))
            .unwrap_or(true)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicyAction {
    #[default]
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyMatch {
    #[serde(default)]
    pub protocols: Vec<AgentProtocol>,
    #[serde(default)]
    pub paths: Vec<PathMatch>,
    #[serde(default)]
    pub methods: Vec<String>,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default)]
    pub headers: Vec<HeaderMatch>,
}

impl PolicyMatch {
    pub fn matches(&self, input: &AgentMatchInput<'_>) -> bool {
        matches_any(&self.protocols, input.protocol)
            && path_matches_any(&self.paths, input.path)
            && string_matches_any(&self.methods, Some(input.method), |a, b| {
                a.eq_ignore_ascii_case(b)
            })
            && string_matches_any(&self.models, input.model, str_matches)
            && string_matches_any(&self.tools, input.tool, str_matches)
            && string_matches_any(&self.agents, input.agent, str_matches)
            && self
                .headers
                .iter()
                .all(|h| h.matches_headers(input.headers))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum AuthPolicy {
    ApiKey {
        #[serde(default = "default_authorization_header")]
        header: String,
        #[serde(default)]
        values: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_env: Option<String>,
    },
    Jwt {
        #[serde(default = "default_authorization_header")]
        header: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hmac_secret_env: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        issuer: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        audiences: Vec<String>,
    },
}

fn default_authorization_header() -> String {
    "authorization".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitPolicy {
    pub requests: u32,
    pub window_seconds: u64,
    #[serde(default)]
    pub key: RateLimitKey,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RateLimitKey {
    #[default]
    Route,
    Backend,
    Header,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub attempts: u32,
    #[serde(default = "default_retry_statuses")]
    pub statuses: Vec<u16>,
}

fn default_retry_statuses() -> Vec<u16> {
    vec![502, 503, 504]
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderTransform {
    #[serde(default)]
    pub add: Vec<HeaderValue>,
    #[serde(default)]
    pub remove: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderValue {
    pub name: String,
    pub value: String,
}

impl HeaderMatch {
    pub fn matches_headers(&self, headers: &[(String, String)]) -> bool {
        headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case(&self.name) && value == &self.value)
    }
}

fn optional_matches(
    expected: Option<&str>,
    actual: Option<&str>,
    cmp: impl Fn(&str, &str) -> bool,
) -> bool {
    expected
        .map(|expected| actual.map(|actual| cmp(expected, actual)).unwrap_or(false))
        .unwrap_or(true)
}

fn str_matches(expected: &str, actual: &str) -> bool {
    expected == "*" || expected == actual
}

fn host_matches(expected: &str, actual: &str) -> bool {
    expected == "*"
        || expected.eq_ignore_ascii_case(actual)
        || expected
            .strip_prefix("*.")
            .map(|suffix| actual.ends_with(suffix))
            .unwrap_or(false)
}

fn matches_any<T: PartialEq>(expected: &[T], actual: T) -> bool {
    expected.is_empty() || expected.iter().any(|value| value == &actual)
}

fn path_matches_any(expected: &[PathMatch], actual: &str) -> bool {
    expected.is_empty() || expected.iter().any(|path| path.matches(actual))
}

fn string_matches_any(
    expected: &[String],
    actual: Option<&str>,
    cmp: impl Fn(&str, &str) -> bool,
) -> bool {
    expected.is_empty()
        || actual
            .map(|actual| expected.iter().any(|value| cmp(value, actual)))
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_listener_conflict() {
        let bind = "0.0.0.0:80".parse().unwrap();
        let cfg = RuntimeConfig {
            version: "test".into(),
            listeners: vec![
                Listener {
                    name: "http".into(),
                    bind,
                    protocol: ListenerProtocol::Http,
                    virtual_hosts: vec![],
                    tls_secret: None,
                },
                Listener {
                    name: "https".into(),
                    bind,
                    protocol: ListenerProtocol::Https,
                    virtual_hosts: vec![],
                    tls_secret: Some("secret".into()),
                },
            ],
            clusters: vec![],
            secrets: vec![],
            providers: vec![],
            backends: vec![],
            routes: vec![],
            policies: vec![],
        };

        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validates_missing_weighted_cluster_references() {
        let cfg = RuntimeConfig {
            version: "test".into(),
            listeners: vec![Listener {
                name: "http".into(),
                bind: "0.0.0.0:80".parse().unwrap(),
                protocol: ListenerProtocol::Http,
                virtual_hosts: vec![VirtualHost {
                    name: "wildcard".into(),
                    domains: vec!["*".into()],
                    routes: vec![Route {
                        name: "missing".into(),
                        matches: vec![RouteMatch {
                            path: PathMatch::Prefix("/".into()),
                            headers: vec![],
                        }],
                        weighted_clusters: vec![WeightedCluster {
                            name: "missing-cluster".into(),
                            weight: 100,
                        }],
                    }],
                }],
                tls_secret: None,
            }],
            clusters: vec![],
            secrets: vec![],
            providers: vec![],
            backends: vec![],
            routes: vec![],
            policies: vec![],
        };

        let conflicts = cfg.validate().unwrap_err();
        assert_eq!(conflicts[0].kind, "missing-cluster");
    }

    #[test]
    fn routes_by_host_path_and_header_match() {
        let cfg = RuntimeConfig {
            version: "test".into(),
            listeners: vec![Listener {
                name: "http".into(),
                bind: "0.0.0.0:80".parse().unwrap(),
                protocol: ListenerProtocol::Http,
                virtual_hosts: vec![VirtualHost {
                    name: "example".into(),
                    domains: vec!["*.example.com".into()],
                    routes: vec![
                        Route {
                            name: "admin".into(),
                            matches: vec![RouteMatch {
                                path: PathMatch::Exact("/admin".into()),
                                headers: vec![HeaderMatch {
                                    name: "x-env".into(),
                                    value: "prod".into(),
                                }],
                            }],
                            weighted_clusters: vec![WeightedCluster {
                                name: "admin".into(),
                                weight: 100,
                            }],
                        },
                        Route {
                            name: "default".into(),
                            matches: vec![RouteMatch {
                                path: PathMatch::Prefix("/".into()),
                                headers: vec![],
                            }],
                            weighted_clusters: vec![WeightedCluster {
                                name: "default".into(),
                                weight: 100,
                            }],
                        },
                    ],
                }],
                tls_secret: None,
            }],
            clusters: vec![
                Cluster {
                    name: "admin".into(),
                    endpoints: vec![],
                    http2: false,
                    tls: None,
                    circuit_breaker: None,
                    outlier_detection: None,
                },
                Cluster {
                    name: "default".into(),
                    endpoints: vec![],
                    http2: false,
                    tls: None,
                    circuit_breaker: None,
                    outlier_detection: None,
                },
            ],
            secrets: vec![],
            providers: vec![],
            backends: vec![],
            routes: vec![],
            policies: vec![],
        };

        let headers = vec![("x-env".to_string(), "prod".to_string())];
        let route = cfg
            .route_for(
                HTTP_LISTENER_PORT,
                &MatchInput {
                    host: "api.example.com",
                    path: "/admin",
                    headers: &headers,
                },
            )
            .unwrap();
        assert_eq!(route.name, "admin");

        let route = cfg
            .route_for(
                HTTP_LISTENER_PORT,
                &MatchInput {
                    host: "api.example.com",
                    path: "/users",
                    headers: &[],
                },
            )
            .unwrap();
        assert_eq!(route.name, "default");

        assert!(cfg
            .route_for(
                HTTP_LISTENER_PORT,
                &MatchInput {
                    host: "api.other.test",
                    path: "/users",
                    headers: &[],
                },
            )
            .is_err());
    }

    #[test]
    fn validates_agent_route_backend_and_policy_references() {
        let cfg = RuntimeConfig {
            version: "agent".into(),
            listeners: vec![],
            clusters: vec![],
            secrets: vec![],
            providers: vec![Provider {
                name: "openai".into(),
                kind: ProviderKind::OpenAiCompatible,
                base_url: "https://api.openai.com".into(),
                api_key_env: Some("OPENAI_API_KEY".into()),
                request_headers: vec![],
            }],
            backends: vec![Backend {
                name: "gpt".into(),
                kind: BackendKind::Llm {
                    provider: "openai".into(),
                    models: vec!["gpt-4o-mini".into()],
                    endpoint: None,
                },
                policies: vec!["auth".into()],
            }],
            routes: vec![AgentRoute {
                name: "chat".into(),
                protocol: AgentProtocol::Llm,
                matches: vec![AgentRouteMatch {
                    path: PathMatch::Exact("/v1/chat/completions".into()),
                    host: None,
                    method: Some("POST".into()),
                    model: Some("gpt-4o-mini".into()),
                    tool: None,
                    agent: None,
                    headers: vec![],
                }],
                weighted_backends: vec![WeightedBackend {
                    name: "gpt".into(),
                    weight: 100,
                }],
                policies: vec!["auth".into()],
            }],
            policies: vec![Policy {
                name: "auth".into(),
                action: PolicyAction::Allow,
                matches: None,
                auth: Some(AuthPolicy::ApiKey {
                    header: "authorization".into(),
                    values: vec!["Bearer local".into()],
                    value_env: None,
                }),
                rate_limit: None,
                timeout_ms: None,
                retry: None,
                max_body_bytes: None,
                request_headers: HeaderTransform::default(),
                response_headers: HeaderTransform::default(),
            }],
        };

        cfg.validate().unwrap();
        let input = AgentMatchInput {
            protocol: AgentProtocol::Llm,
            host: "api.example.com",
            path: "/v1/chat/completions",
            method: "post",
            model: Some("gpt-4o-mini"),
            tool: None,
            agent: None,
            headers: &[],
        };

        assert_eq!(cfg.agent_route_for(&input).unwrap().name, "chat");
        assert!(cfg
            .backend("gpt")
            .unwrap()
            .supports_model(Some("gpt-4o-mini")));
        assert!(!cfg.backend("gpt").unwrap().supports_model(Some("other")));
        assert!(cfg.backend("gpt").unwrap().supports_model(None));
    }

    #[test]
    fn unconstrained_requests_keep_capability_scoped_backends_eligible() {
        let backend = Backend {
            name: "tools".into(),
            kind: BackendKind::Mcp {
                endpoint: "http://tools.svc:8080".into(),
                tools: vec!["search".into()],
            },
            policies: vec![],
        };

        assert!(backend.supports_tool(None));
        assert!(backend.supports_tool(Some("search")));
        assert!(!backend.supports_tool(Some("calendar")));
    }

    #[test]
    fn policy_match_filters_protocol_path_and_model() {
        let policy = Policy {
            name: "llm-only".into(),
            action: PolicyAction::Deny,
            matches: Some(PolicyMatch {
                protocols: vec![AgentProtocol::Llm],
                paths: vec![PathMatch::Prefix("/v1/".into())],
                methods: vec!["POST".into()],
                models: vec!["blocked".into()],
                tools: vec![],
                agents: vec![],
                headers: vec![],
            }),
            auth: None,
            rate_limit: None,
            timeout_ms: None,
            retry: None,
            max_body_bytes: None,
            request_headers: HeaderTransform::default(),
            response_headers: HeaderTransform::default(),
        };
        let input = AgentMatchInput {
            protocol: AgentProtocol::Llm,
            host: "api.example.com",
            path: "/v1/chat/completions",
            method: "post",
            model: Some("blocked"),
            tool: None,
            agent: None,
            headers: &[],
        };

        assert!(policy.applies_to(&input));
    }
}
