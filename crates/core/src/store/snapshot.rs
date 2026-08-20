//! The immutable, indexed read view of the store.
//!
//! A snapshot is rebuilt on every committed delta and handed to readers as an
//! `Arc`. It owns the request-path indexes so routing does not rescan the whole
//! configuration:
//!
//! * listeners are collapsed into a per-port index, and each port carries the
//!   virtual hosts bound to it with their domain matchers pre-parsed;
//! * clusters, secrets, providers, backends, and policies are hash maps;
//! * agent routes are bucketed by protocol;
//! * `policy_refs` is the reverse index from a policy to everything attached to
//!   it, used by referential validation and by the admin API.
//!
//! Route order within a port stays as declared. dxgate is an xDS data plane and
//! xDS route matching is first-match-wins over the order the control plane sent,
//! so the index narrows candidates by port and host but must not reorder them.

use super::{ResourceKey, ResourceKind, SourceId};
use crate::{
    AgentMatchInput, AgentProtocol, AgentRoute, Backend, BackendKind, Cluster, ConfigConflict,
    DxgateError, Listener, MatchInput, Policy, Provider, Result, Route, RuntimeConfig, TlsSecret,
};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

/// What [`ConfigSnapshot::route_for`] resolves to.
pub type RouteMatch<'a> = &'a Arc<Route>;

/// Everything [`ConfigSnapshot::build`] needs, already ordered by the store.
pub(super) struct SnapshotInput {
    pub(super) revision: u64,
    pub(super) applied: bool,
    pub(super) source_versions: BTreeMap<SourceId, String>,
    pub(super) owners: BTreeMap<ResourceKey, SourceId>,
    pub(super) listeners: Vec<Arc<Listener>>,
    pub(super) clusters: Vec<(String, Arc<Cluster>)>,
    pub(super) secrets: Vec<(String, Arc<TlsSecret>)>,
    pub(super) providers: Vec<(String, Arc<Provider>)>,
    pub(super) backends: Vec<(String, Arc<Backend>)>,
    pub(super) agent_routes: Vec<Arc<AgentRoute>>,
    pub(super) policies: Vec<(String, Arc<Policy>)>,
    pub(super) rejections: Vec<ConfigConflict>,
}

#[derive(Debug)]
pub struct ConfigSnapshot {
    revision: u64,
    ready: bool,
    version: String,
    source_versions: BTreeMap<SourceId, String>,
    owners: BTreeMap<ResourceKey, SourceId>,
    conflicts: Vec<ConfigConflict>,

    listeners: Vec<Arc<Listener>>,
    ports: BTreeMap<u16, PortIndex>,
    clusters: HashMap<String, Arc<Cluster>>,
    secrets: HashMap<String, Arc<TlsSecret>>,
    providers: HashMap<String, Arc<Provider>>,
    backends: HashMap<String, Arc<Backend>>,
    policies: HashMap<String, Arc<Policy>>,
    agent_routes: Vec<Arc<AgentRoute>>,
    agent_routes_by_protocol: HashMap<AgentProtocol, Vec<Arc<AgentRoute>>>,
    policy_refs: BTreeMap<String, Vec<ResourceKey>>,
    has_http_agent_route: bool,
    has_agent_routes: bool,
}

/// The virtual hosts bound to one listener port, in declaration order.
#[derive(Debug, Default)]
struct PortIndex {
    hosts: Vec<VirtualHostIndex>,
}

#[derive(Debug)]
struct VirtualHostIndex {
    listener: String,
    name: String,
    domains: Vec<DomainMatch>,
    routes: Vec<Arc<Route>>,
}

impl VirtualHostIndex {
    fn matches_host(&self, host: &str) -> bool {
        self.domains.iter().any(|domain| domain.matches(host))
    }
}

/// A virtual-host domain with its wildcard form resolved once at build time
/// instead of re-parsed on every request.
#[derive(Debug)]
enum DomainMatch {
    /// `*`
    Any,
    /// A literal domain, compared case-insensitively.
    Exact(String),
    /// `*.example.com`, stored as the `example.com` suffix.
    Suffix(String),
}

impl DomainMatch {
    fn parse(domain: &str) -> Self {
        if domain == "*" {
            DomainMatch::Any
        } else if let Some(suffix) = domain.strip_prefix("*.") {
            DomainMatch::Suffix(suffix.to_string())
        } else {
            DomainMatch::Exact(domain.to_string())
        }
    }

    fn matches(&self, host: &str) -> bool {
        match self {
            DomainMatch::Any => true,
            DomainMatch::Exact(domain) => domain.eq_ignore_ascii_case(host),
            DomainMatch::Suffix(suffix) => host.ends_with(suffix.as_str()),
        }
    }
}

impl ConfigSnapshot {
    /// The snapshot a store publishes before any source has applied a delta.
    pub(super) fn empty() -> Self {
        Self {
            revision: 0,
            ready: false,
            version: String::new(),
            source_versions: BTreeMap::new(),
            owners: BTreeMap::new(),
            conflicts: Vec::new(),
            listeners: Vec::new(),
            ports: BTreeMap::new(),
            clusters: HashMap::new(),
            secrets: HashMap::new(),
            providers: HashMap::new(),
            backends: HashMap::new(),
            policies: HashMap::new(),
            agent_routes: Vec::new(),
            agent_routes_by_protocol: HashMap::new(),
            policy_refs: BTreeMap::new(),
            has_http_agent_route: false,
            has_agent_routes: false,
        }
    }

    pub(super) fn build(input: SnapshotInput) -> Self {
        let SnapshotInput {
            revision,
            applied,
            source_versions,
            owners,
            listeners,
            clusters,
            secrets,
            providers,
            backends,
            agent_routes,
            policies,
            rejections,
        } = input;

        let clusters: HashMap<String, Arc<Cluster>> = clusters.into_iter().collect();
        let secrets: HashMap<String, Arc<TlsSecret>> = secrets.into_iter().collect();
        let providers: HashMap<String, Arc<Provider>> = providers.into_iter().collect();
        let backends: HashMap<String, Arc<Backend>> = backends.into_iter().collect();
        let policies: HashMap<String, Arc<Policy>> = policies.into_iter().collect();

        let mut ports: BTreeMap<u16, PortIndex> = BTreeMap::new();
        for listener in &listeners {
            let index = ports.entry(listener.bind.port()).or_default();
            for host in &listener.virtual_hosts {
                index.hosts.push(VirtualHostIndex {
                    listener: listener.name.clone(),
                    name: host.name.clone(),
                    domains: host.domains.iter().map(|d| DomainMatch::parse(d)).collect(),
                    routes: host.routes.iter().cloned().map(Arc::new).collect(),
                });
            }
        }

        let mut agent_routes_by_protocol: HashMap<AgentProtocol, Vec<Arc<AgentRoute>>> =
            HashMap::new();
        for route in &agent_routes {
            agent_routes_by_protocol
                .entry(route.protocol)
                .or_default()
                .push(route.clone());
        }
        let has_http_agent_route = agent_routes_by_protocol.contains_key(&AgentProtocol::Http);
        let has_agent_routes = !agent_routes.is_empty();

        let policy_refs = build_policy_refs(&backends, &agent_routes);

        let mut conflicts = rejections;
        conflicts.extend(validate_references(
            &listeners,
            &clusters,
            &providers,
            &backends,
            &agent_routes,
            &policies,
        ));

        let version = source_versions
            .iter()
            .map(|(source, version)| format!("{source}={version}"))
            .collect::<Vec<_>>()
            .join(",");

        Self {
            revision,
            ready: applied && conflicts.is_empty(),
            version,
            source_versions,
            owners,
            conflicts,
            listeners,
            ports,
            clusters,
            secrets,
            providers,
            backends,
            policies,
            agent_routes,
            agent_routes_by_protocol,
            policy_refs,
            has_http_agent_route,
            has_agent_routes,
        }
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn ready(&self) -> bool {
        self.ready
    }

    /// Human-readable version label: `source=version` pairs joined by commas.
    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn source_versions(&self) -> &BTreeMap<SourceId, String> {
        &self.source_versions
    }

    pub fn conflicts(&self) -> &[ConfigConflict] {
        &self.conflicts
    }

    pub fn owner(&self, key: &ResourceKey) -> Option<SourceId> {
        self.owners.get(key).copied()
    }

    pub fn owners(&self) -> &BTreeMap<ResourceKey, SourceId> {
        &self.owners
    }

    pub fn listeners(&self) -> &[Arc<Listener>] {
        &self.listeners
    }

    pub fn cluster(&self, name: &str) -> Option<&Arc<Cluster>> {
        self.clusters.get(name)
    }

    /// Every cluster, in no particular order. For bookkeeping that has to walk
    /// the whole set without copying it.
    pub fn clusters(&self) -> impl Iterator<Item = &Arc<Cluster>> {
        self.clusters.values()
    }

    /// Every route name in the configuration, Gateway API routes and agent
    /// routes alike. Used to retire per-route state when a route disappears.
    pub fn route_names(&self) -> impl Iterator<Item = &str> {
        self.ports
            .values()
            .flat_map(|index| index.hosts.iter())
            .flat_map(|host| host.routes.iter())
            .map(|route| route.name.as_str())
            .chain(self.agent_routes.iter().map(|route| route.name.as_str()))
    }

    pub fn secret(&self, name: &str) -> Option<&Arc<TlsSecret>> {
        self.secrets.get(name)
    }

    pub fn provider(&self, name: &str) -> Option<&Arc<Provider>> {
        self.providers.get(name)
    }

    pub fn providers(&self) -> impl Iterator<Item = &Arc<Provider>> {
        self.providers.values()
    }

    pub fn backend(&self, name: &str) -> Option<&Arc<Backend>> {
        self.backends.get(name)
    }

    pub fn policy(&self, name: &str) -> Option<&Arc<Policy>> {
        self.policies.get(name)
    }

    pub fn policies(&self) -> impl Iterator<Item = &Arc<Policy>> {
        self.policies.values()
    }

    pub fn agent_routes(&self) -> &[Arc<AgentRoute>] {
        &self.agent_routes
    }

    /// Whether any agent route claims plain HTTP. The proxy uses this to decide
    /// if a request that matched no agent-protocol path should still be offered
    /// to the agent router.
    pub fn has_http_agent_route(&self) -> bool {
        self.has_http_agent_route
    }

    pub fn has_agent_routes(&self) -> bool {
        self.has_agent_routes
    }

    /// Reverse index: policy name to the resources that attach it.
    pub fn policy_refs(&self, policy: &str) -> &[ResourceKey] {
        self.policy_refs
            .get(policy)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub fn all_policy_refs(&self) -> &BTreeMap<String, Vec<ResourceKey>> {
        &self.policy_refs
    }

    /// Resolves a Gateway API route: port index, then virtual hosts whose
    /// domains match, then the first route whose matchers accept the request.
    pub fn route_for(&self, port: u16, input: &MatchInput<'_>) -> Result<RouteMatch<'_>> {
        let not_found = || DxgateError::RouteNotFound {
            host: input.host.to_string(),
            path: input.path.to_string(),
        };
        let index = self.ports.get(&port).ok_or_else(not_found)?;
        route_in_port(index, input).ok_or_else(not_found)
    }

    /// Resolves a route when a local proxy hop has hidden the original
    /// listener port. The match must belong to exactly one xDS listener port;
    /// ambiguity is rejected instead of routing a request across listeners.
    pub fn route_for_unique_port(&self, input: &MatchInput<'_>) -> Result<RouteMatch<'_>> {
        let not_found = || DxgateError::RouteNotFound {
            host: input.host.to_string(),
            path: input.path.to_string(),
        };
        let mut matches = self
            .ports
            .values()
            .filter_map(|index| route_in_port(index, input));
        let route = matches.next().ok_or_else(not_found)?;
        if matches.next().is_some() {
            return Err(not_found());
        }
        Ok(route)
    }

    /// Resolves an agent route within the request's protocol bucket.
    pub fn agent_route_for(&self, input: &AgentMatchInput<'_>) -> Option<&Arc<AgentRoute>> {
        self.agent_routes_by_protocol
            .get(&input.protocol)?
            .iter()
            .find(|route| route.matches(input))
    }

    /// Rebuilds the flat configuration document, for `/debug/config` and tests.
    /// Not on the request path.
    pub fn to_runtime_config(&self) -> RuntimeConfig {
        let mut clusters: Vec<_> = self.clusters.values().map(|c| (**c).clone()).collect();
        clusters.sort_by(|a, b| a.name.cmp(&b.name));
        let mut secrets: Vec<_> = self.secrets.values().map(|s| (**s).clone()).collect();
        secrets.sort_by(|a, b| a.name.cmp(&b.name));
        let mut providers: Vec<_> = self.providers.values().map(|p| (**p).clone()).collect();
        providers.sort_by(|a, b| a.name.cmp(&b.name));
        let mut backends: Vec<_> = self.backends.values().map(|b| (**b).clone()).collect();
        backends.sort_by(|a, b| a.name.cmp(&b.name));
        let mut policies: Vec<_> = self.policies.values().map(|p| (**p).clone()).collect();
        policies.sort_by(|a, b| a.name.cmp(&b.name));

        RuntimeConfig {
            version: self.version.clone(),
            listeners: self.listeners.iter().map(|l| (**l).clone()).collect(),
            clusters,
            secrets,
            providers,
            backends,
            routes: self.agent_routes.iter().map(|r| (**r).clone()).collect(),
            policies,
        }
    }

    /// A diagnostic configuration view that preserves resource topology while
    /// removing TLS material. xDS/SDS delivers a workload private key into this
    /// snapshot, so returning [`Self::to_runtime_config`] from an unauthenticated
    /// debug endpoint would disclose credentials.
    pub fn to_redacted_runtime_config(&self) -> RuntimeConfig {
        let mut config = self.to_runtime_config();
        for secret in &mut config.secrets {
            secret.certificate_chain_pem.clear();
            secret.private_key_pem.clear();
            secret.trusted_ca_pem = None;
        }
        config
    }

    /// Flattened `(listener, virtual host, domains, route)` view for the admin
    /// API. Not on the request path.
    pub fn route_table(&self) -> Vec<RouteTableEntry<'_>> {
        self.ports
            .values()
            .flat_map(|index| index.hosts.iter())
            .flat_map(|host| {
                host.routes.iter().map(move |route| RouteTableEntry {
                    listener: host.listener.as_str(),
                    virtual_host: host.name.as_str(),
                    route,
                })
            })
            .collect()
    }
}

fn route_in_port<'a>(index: &'a PortIndex, input: &MatchInput<'_>) -> Option<&'a Arc<Route>> {
    index
        .hosts
        .iter()
        .filter(|host| host.matches_host(input.host))
        .flat_map(|host| host.routes.iter())
        .find(|route| route.matches(input))
}

/// One row of [`ConfigSnapshot::route_table`].
#[derive(Debug, Clone)]
pub struct RouteTableEntry<'a> {
    pub listener: &'a str,
    pub virtual_host: &'a str,
    pub route: &'a Arc<Route>,
}

fn build_policy_refs(
    backends: &HashMap<String, Arc<Backend>>,
    agent_routes: &[Arc<AgentRoute>],
) -> BTreeMap<String, Vec<ResourceKey>> {
    let mut refs: BTreeMap<String, Vec<ResourceKey>> = BTreeMap::new();
    let mut backend_names: Vec<&String> = backends.keys().collect();
    backend_names.sort();
    for name in backend_names {
        for policy in &backends[name].policies {
            refs.entry(policy.clone())
                .or_default()
                .push(ResourceKey::new(ResourceKind::Backend, name.clone()));
        }
    }
    for route in agent_routes {
        for policy in &route.policies {
            refs.entry(policy.clone())
                .or_default()
                .push(ResourceKey::new(
                    ResourceKind::AgentRoute,
                    route.name.clone(),
                ));
        }
    }
    refs
}

/// Cross-resource checks over the merged store.
///
/// These are deliberately not fatal. With several sources feeding one store,
/// a dangling reference is the normal steady state while the other source
/// catches up — an ADS stream can deliver listeners before clusters or an
/// agent route before its backend. Rejecting the
/// update would deadlock convergence, so the store commits it and reports the
/// gap through readiness instead; requests that hit the gap fail with 503.
fn validate_references(
    listeners: &[Arc<Listener>],
    clusters: &HashMap<String, Arc<Cluster>>,
    providers: &HashMap<String, Arc<Provider>>,
    backends: &HashMap<String, Arc<Backend>>,
    agent_routes: &[Arc<AgentRoute>],
    policies: &HashMap<String, Arc<Policy>>,
) -> Vec<ConfigConflict> {
    let mut conflicts = Vec::new();
    let mut binds: BTreeMap<SocketAddr, (&str, crate::ListenerProtocol, bool)> = BTreeMap::new();

    for listener in listeners {
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
                for destination in &route.weighted_clusters {
                    if !clusters.contains_key(&destination.name) {
                        conflicts.push(ConfigConflict::new(
                            "missing-cluster",
                            format!(
                                "route {} references missing cluster {}",
                                route.name, destination.name
                            ),
                        ));
                    }
                }
            }
        }
    }

    let mut backend_names: Vec<&String> = backends.keys().collect();
    backend_names.sort();
    for name in backend_names {
        let backend = &backends[name];
        if let BackendKind::Llm { provider, .. } = &backend.kind {
            if !providers.contains_key(provider) {
                conflicts.push(ConfigConflict::new(
                    "missing-provider",
                    format!("backend {} references missing provider {provider}", name),
                ));
            }
        }
        for policy in &backend.policies {
            if !policies.contains_key(policy) {
                conflicts.push(ConfigConflict::new(
                    "missing-policy",
                    format!("backend {name} references missing policy {policy}"),
                ));
            }
        }
    }

    for route in agent_routes {
        if route.weighted_backends.is_empty() {
            conflicts.push(ConfigConflict::new(
                "empty-agent-route-destination",
                format!("agent route {} has no weighted backends", route.name),
            ));
        }
        for destination in &route.weighted_backends {
            if !backends.contains_key(&destination.name) {
                conflicts.push(ConfigConflict::new(
                    "missing-backend",
                    format!(
                        "agent route {} references missing backend {}",
                        route.name, destination.name
                    ),
                ));
            }
        }
        for policy in &route.policies {
            if !policies.contains_key(policy) {
                conflicts.push(ConfigConflict::new(
                    "missing-policy",
                    format!(
                        "agent route {} references missing policy {policy}",
                        route.name
                    ),
                ));
            }
        }
    }

    conflicts
}
