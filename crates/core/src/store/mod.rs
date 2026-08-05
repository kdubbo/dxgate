//! Owner-tracked configuration store.
//!
//! dxgate takes configuration from several sources — xDS from `dubbod`, a static
//! file, and Kubernetes CRDs — and each source owns a disjoint slice of the
//! runtime config. A whole-value [`RuntimeConfig`] snapshot cannot express that:
//! whoever publishes last wins and silently erases everything the other sources
//! contributed. The store instead keys every resource by `(kind, name)`, records
//! the [`SourceId`] that owns it, and applies per-source deltas. An upsert only
//! replaces a resource the same source already owns, and a removal only removes
//! resources that source owns, so xDS can own listeners and clusters while the
//! Kubernetes controller owns backends, routes, and policies.
//!
//! Sources that can only produce a full list — a static file, a Kubernetes
//! re-list after a watch restart — drive the same delta path through
//! [`SourceState::reconcile`], which diffs the new list against the keys the
//! source published last time and turns each disappearance into an explicit
//! removal.
//!
//! Every committed write republishes an immutable [`ConfigSnapshot`] carrying
//! the request-path indexes. Readers clone one `Arc`; no configuration is cloned
//! per request.

mod delta;
mod snapshot;

pub use delta::{ConfigDelta, SourceState};
pub use snapshot::{ConfigSnapshot, RouteMatch as SnapshotRouteMatch};

use crate::{AgentRoute, Backend, Cluster, ConfigConflict, Listener, Policy, Provider, TlsSecret};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};

/// Which configuration source owns a resource.
///
/// Ownership is what makes multi-source configuration composable: a delta from
/// one source never touches another source's resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceId {
    /// The ADS stream from `dubbod`.
    Xds,
    /// A static YAML/JSON file, optionally watched.
    Static,
    /// The Kubernetes CRD controller.
    Kubernetes,
}

impl SourceId {
    pub const ALL: [SourceId; 3] = [SourceId::Xds, SourceId::Static, SourceId::Kubernetes];

    pub fn as_str(&self) -> &'static str {
        match self {
            SourceId::Xds => "xds",
            SourceId::Static => "static",
            SourceId::Kubernetes => "kubernetes",
        }
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The resource types the store tracks. One variant per top-level collection in
/// [`RuntimeConfig`](crate::RuntimeConfig).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResourceKind {
    Listener,
    Cluster,
    Secret,
    Provider,
    Backend,
    AgentRoute,
    Policy,
}

impl ResourceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResourceKind::Listener => "listener",
            ResourceKind::Cluster => "cluster",
            ResourceKind::Secret => "secret",
            ResourceKind::Provider => "provider",
            ResourceKind::Backend => "backend",
            ResourceKind::AgentRoute => "agent-route",
            ResourceKind::Policy => "policy",
        }
    }
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Identifies one resource in the store. `Ord` puts keys in kind-then-name order
/// so snapshots and debug dumps are stable across runs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ResourceKey {
    pub kind: ResourceKind,
    pub name: String,
}

impl ResourceKey {
    pub fn new(kind: ResourceKind, name: impl Into<String>) -> Self {
        Self {
            kind,
            name: name.into(),
        }
    }
}

impl fmt::Display for ResourceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.kind, self.name)
    }
}

/// A resource plus the source that owns it and its position in that source's
/// last publication.
///
/// `order` exists because route matching is first-match-wins over declaration
/// order, and a name-keyed map has no declaration order of its own. A
/// state-of-the-world source assigns each resource its index in the list it
/// published, so re-publishing the same list reproduces the same order.
#[derive(Debug, Clone)]
struct Owned<T> {
    source: SourceId,
    order: u32,
    value: Arc<T>,
}

/// The mutable half of the store: owner-tracked maps, one per resource kind.
#[derive(Debug, Default)]
struct Resources {
    listeners: BTreeMap<String, Owned<Listener>>,
    clusters: BTreeMap<String, Owned<Cluster>>,
    secrets: BTreeMap<String, Owned<TlsSecret>>,
    providers: BTreeMap<String, Owned<Provider>>,
    backends: BTreeMap<String, Owned<Backend>>,
    agent_routes: BTreeMap<String, Owned<AgentRoute>>,
    policies: BTreeMap<String, Owned<Policy>>,
    /// Version label last reported by each source, surfaced on `/readyz`.
    source_versions: BTreeMap<SourceId, String>,
    /// Upserts refused on each source's most recent apply, kept until that
    /// source applies again so an ownership clash stays visible on `/readyz`
    /// instead of vanishing with the next unrelated update.
    rejections: BTreeMap<SourceId, Vec<ConfigConflict>>,
    /// Monotonic revision, bumped once per committed delta.
    revision: u64,
    /// Whether any source has ever applied a delta. Readiness stays false until
    /// then so a freshly started proxy does not advertise an empty config.
    applied: bool,
}

/// Result of applying one delta.
#[derive(Debug, Clone, Default)]
pub struct ApplyOutcome {
    /// Store revision after the apply.
    pub revision: u64,
    /// Whether the merged config is free of referential conflicts.
    pub ready: bool,
    /// Whether the delta actually changed anything. False means the source
    /// re-published what the store already held and no snapshot was rebuilt.
    pub changed: bool,
    /// Upserts refused because another source owns the key.
    pub rejected: Vec<ConfigConflict>,
    /// Referential problems in the merged config (dangling references, listener
    /// bind conflicts). Not fatal: the delta is committed either way, and the
    /// affected requests fail with 503 at routing time. Surfaced on `/readyz`.
    pub conflicts: Vec<ConfigConflict>,
}

impl ApplyOutcome {
    /// All problems reported by this apply, rejections first.
    pub fn problems(&self) -> Vec<ConfigConflict> {
        let mut out = self.rejected.clone();
        out.extend(self.conflicts.iter().cloned());
        out
    }
}

/// The store: serialized writes over owner-tracked maps, plus an immutable
/// published snapshot for the request path.
///
/// Writes take the write mutex, rebuild the snapshot, and swap it in. Reads take
/// a short read lock only to clone the published `Arc`, so the request path
/// never holds a lock while doing work and never clones configuration.
#[derive(Debug)]
pub struct ConfigStore {
    write: Mutex<Resources>,
    published: RwLock<Arc<ConfigSnapshot>>,
}

impl Default for ConfigStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigStore {
    pub fn new() -> Self {
        Self {
            write: Mutex::new(Resources::default()),
            published: RwLock::new(Arc::new(ConfigSnapshot::empty())),
        }
    }

    /// The current published snapshot. One atomic refcount bump; no copying.
    pub fn snapshot(&self) -> Arc<ConfigSnapshot> {
        self.published
            .read()
            .expect("config snapshot lock poisoned")
            .clone()
    }

    /// Applies `delta` on behalf of `source`, then republishes the snapshot.
    ///
    /// Upserts of keys owned by a different source are refused and reported in
    /// [`ApplyOutcome::rejected`] rather than silently stealing ownership, so a
    /// misconfiguration where two sources claim the same resource is visible on
    /// `/readyz` instead of producing config that flaps between two writers.
    pub fn apply(&self, source: SourceId, delta: ConfigDelta) -> ApplyOutcome {
        let mut resources = self.write.lock().expect("config store lock poisoned");
        let mut rejected = Vec::new();

        let mut changed = false;
        for key in &delta.removes {
            changed |= remove_owned(&mut resources, source, key);
        }

        let ConfigDelta {
            version,
            listeners,
            clusters,
            secrets,
            providers,
            backends,
            agent_routes,
            policies,
            removes: _,
        } = delta;

        changed |= upsert_all(
            &mut resources.listeners,
            source,
            ResourceKind::Listener,
            listeners,
            |value| value.name.clone(),
            &mut rejected,
        );
        changed |= upsert_all(
            &mut resources.clusters,
            source,
            ResourceKind::Cluster,
            clusters,
            |value| value.name.clone(),
            &mut rejected,
        );
        changed |= upsert_all(
            &mut resources.secrets,
            source,
            ResourceKind::Secret,
            secrets,
            |value| value.name.clone(),
            &mut rejected,
        );
        changed |= upsert_all(
            &mut resources.providers,
            source,
            ResourceKind::Provider,
            providers,
            |value| value.name.clone(),
            &mut rejected,
        );
        changed |= upsert_all(
            &mut resources.backends,
            source,
            ResourceKind::Backend,
            backends,
            |value| value.name.clone(),
            &mut rejected,
        );
        changed |= upsert_all(
            &mut resources.agent_routes,
            source,
            ResourceKind::AgentRoute,
            agent_routes,
            |value| value.name.clone(),
            &mut rejected,
        );
        changed |= upsert_all(
            &mut resources.policies,
            source,
            ResourceKind::Policy,
            policies,
            |value| value.name.clone(),
            &mut rejected,
        );

        if let Some(version) = version {
            let previous = resources.source_versions.insert(source, version);
            changed |= previous.as_ref() != resources.source_versions.get(&source);
        }
        let previous_rejections = if rejected.is_empty() {
            resources.rejections.remove(&source)
        } else {
            resources.rejections.insert(source, rejected.clone())
        };
        changed |= previous_rejections.unwrap_or_default() != rejected;
        let first_apply = !resources.applied;
        resources.applied = true;

        // A source that can only publish its whole slice re-sends the same
        // resources on every update. Republishing an identical snapshot would
        // rebuild every index and invalidate readers for nothing.
        if !changed && !first_apply {
            drop(resources);
            let snapshot = self.snapshot();
            return ApplyOutcome {
                revision: snapshot.revision(),
                ready: snapshot.ready(),
                changed: false,
                rejected,
                conflicts: snapshot.conflicts().to_vec(),
            };
        }
        resources.revision += 1;

        let snapshot = Arc::new(build_snapshot(&resources));
        let outcome = ApplyOutcome {
            revision: snapshot.revision(),
            ready: snapshot.ready(),
            changed: true,
            rejected,
            conflicts: snapshot.conflicts().to_vec(),
        };
        *self
            .published
            .write()
            .expect("config snapshot lock poisoned") = snapshot;
        outcome
    }

    /// Drops every resource owned by `source`. Used when a source shuts down or
    /// its stream is torn down permanently.
    pub fn evict_source(&self, source: SourceId) -> ApplyOutcome {
        let mut resources = self.write.lock().expect("config store lock poisoned");
        resources.listeners.retain(|_, o| o.source != source);
        resources.clusters.retain(|_, o| o.source != source);
        resources.secrets.retain(|_, o| o.source != source);
        resources.providers.retain(|_, o| o.source != source);
        resources.backends.retain(|_, o| o.source != source);
        resources.agent_routes.retain(|_, o| o.source != source);
        resources.policies.retain(|_, o| o.source != source);
        resources.source_versions.remove(&source);
        resources.rejections.remove(&source);
        resources.revision += 1;

        let snapshot = Arc::new(build_snapshot(&resources));
        let outcome = ApplyOutcome {
            revision: snapshot.revision(),
            ready: snapshot.ready(),
            changed: true,
            rejected: Vec::new(),
            conflicts: snapshot.conflicts().to_vec(),
        };
        *self
            .published
            .write()
            .expect("config snapshot lock poisoned") = snapshot;
        outcome
    }
}

/// Upserts one kind's worth of resources, assigning each its position in the
/// delta as declaration order. Returns whether anything actually changed.
///
/// A source that can only publish its whole slice re-sends unchanged resources
/// on every update; recognising them keeps the store from bumping its revision
/// and rebuilding every index for nothing.
fn upsert_all<T: PartialEq>(
    map: &mut BTreeMap<String, Owned<T>>,
    source: SourceId,
    kind: ResourceKind,
    values: Vec<T>,
    name_of: impl Fn(&T) -> String,
    rejected: &mut Vec<ConfigConflict>,
) -> bool {
    let mut changed = false;
    for (order, value) in values.into_iter().enumerate() {
        let order = order as u32;
        let name = name_of(&value);
        if let Some(existing) = map.get(&name) {
            if existing.source != source {
                rejected.push(ConfigConflict::new(
                    "ownership-conflict",
                    format!(
                        "source {source} cannot publish {kind} {name}: it is owned by source {}",
                        existing.source
                    ),
                ));
                continue;
            }
            if existing.order == order && *existing.value == value {
                continue;
            }
        }
        changed = true;
        map.insert(
            name,
            Owned {
                source,
                order,
                value: Arc::new(value),
            },
        );
    }
    changed
}

/// Flattens an owner-tracked map into declaration order: position within the
/// owning source first, then source, then name. Cross-source ties cannot be
/// ordered meaningfully, so they fall back to something deterministic.
fn ordered<T>(map: &BTreeMap<String, Owned<T>>) -> Vec<Arc<T>> {
    let mut entries: Vec<(&String, &Owned<T>)> = map.iter().collect();
    entries.sort_by(|(left_name, left), (right_name, right)| {
        left.order
            .cmp(&right.order)
            .then_with(|| left.source.cmp(&right.source))
            .then_with(|| left_name.cmp(right_name))
    });
    entries
        .into_iter()
        .map(|(_, owned)| owned.value.clone())
        .collect()
}

fn by_name<T>(map: &BTreeMap<String, Owned<T>>) -> Vec<(String, Arc<T>)> {
    map.iter()
        .map(|(name, owned)| (name.clone(), owned.value.clone()))
        .collect()
}

/// Removes `key` if `source` owns it. Returns whether anything was removed.
fn remove_owned(resources: &mut Resources, source: SourceId, key: &ResourceKey) -> bool {
    fn take<T>(map: &mut BTreeMap<String, Owned<T>>, source: SourceId, name: &str) -> bool {
        if map.get(name).is_some_and(|owned| owned.source == source) {
            return map.remove(name).is_some();
        }
        false
    }
    match key.kind {
        ResourceKind::Listener => take(&mut resources.listeners, source, &key.name),
        ResourceKind::Cluster => take(&mut resources.clusters, source, &key.name),
        ResourceKind::Secret => take(&mut resources.secrets, source, &key.name),
        ResourceKind::Provider => take(&mut resources.providers, source, &key.name),
        ResourceKind::Backend => take(&mut resources.backends, source, &key.name),
        ResourceKind::AgentRoute => take(&mut resources.agent_routes, source, &key.name),
        ResourceKind::Policy => take(&mut resources.policies, source, &key.name),
    }
}

/// Rebuilds the published snapshot from the current resource maps.
///
/// The rebuild is a full pass over every resource rather than incremental index
/// maintenance: deltas arrive orders of magnitude less often than requests, and
/// an index that can only be wrong after a rebuild is far easier to trust than
/// one that can drift.
fn build_snapshot(resources: &Resources) -> ConfigSnapshot {
    ConfigSnapshot::build(snapshot::SnapshotInput {
        revision: resources.revision,
        applied: resources.applied,
        source_versions: resources.source_versions.clone(),
        owners: resource_owners(resources),
        listeners: ordered(&resources.listeners),
        clusters: by_name(&resources.clusters),
        secrets: by_name(&resources.secrets),
        providers: by_name(&resources.providers),
        backends: by_name(&resources.backends),
        agent_routes: ordered(&resources.agent_routes),
        policies: by_name(&resources.policies),
        rejections: resources.rejections.values().flatten().cloned().collect(),
    })
}

fn resource_owners(resources: &Resources) -> BTreeMap<ResourceKey, SourceId> {
    let mut owners = BTreeMap::new();
    fn collect<T>(
        owners: &mut BTreeMap<ResourceKey, SourceId>,
        kind: ResourceKind,
        map: &BTreeMap<String, Owned<T>>,
    ) {
        for (name, owned) in map {
            owners.insert(ResourceKey::new(kind, name.clone()), owned.source);
        }
    }
    collect(&mut owners, ResourceKind::Listener, &resources.listeners);
    collect(&mut owners, ResourceKind::Cluster, &resources.clusters);
    collect(&mut owners, ResourceKind::Secret, &resources.secrets);
    collect(&mut owners, ResourceKind::Provider, &resources.providers);
    collect(&mut owners, ResourceKind::Backend, &resources.backends);
    collect(
        &mut owners,
        ResourceKind::AgentRoute,
        &resources.agent_routes,
    );
    collect(&mut owners, ResourceKind::Policy, &resources.policies);
    owners
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentProtocol, BackendKind, Endpoint, ListenerProtocol, PathMatch, ProviderKind, Route,
        RouteMatch, RuntimeConfig, VirtualHost, WeightedBackend, WeightedCluster,
    };

    fn cluster(name: &str) -> Cluster {
        Cluster {
            name: name.into(),
            endpoints: vec![Endpoint {
                address: "10.0.0.1".into(),
                port: 8080,
                healthy: true,
                node_name: None,
            }],
            http2: false,
            tls: None,
            circuit_breaker: None,
            outlier_detection: None,
        }
    }

    fn listener(name: &str, port: u16, cluster_name: &str) -> Listener {
        Listener {
            name: name.into(),
            bind: format!("0.0.0.0:{port}").parse().unwrap(),
            protocol: ListenerProtocol::Http,
            virtual_hosts: vec![VirtualHost {
                name: "wildcard".into(),
                domains: vec!["*".into()],
                routes: vec![Route {
                    name: format!("{name}-default"),
                    matches: vec![RouteMatch {
                        path: PathMatch::Prefix("/".into()),
                        headers: vec![],
                    }],
                    weighted_clusters: vec![WeightedCluster {
                        name: cluster_name.into(),
                        weight: 100,
                    }],
                }],
            }],
            tls_secret: None,
        }
    }

    fn llm_backend(name: &str, provider: &str) -> Backend {
        Backend {
            name: name.into(),
            kind: BackendKind::Llm {
                provider: provider.into(),
                models: vec![],
                endpoint: None,
                model_rewrites: Default::default(),
            },
            policies: vec![],
        }
    }

    fn agent_route(name: &str, backend: &str) -> AgentRoute {
        AgentRoute {
            name: name.into(),
            protocol: AgentProtocol::Llm,
            matches: vec![],
            weighted_backends: vec![WeightedBackend {
                name: backend.into(),
                weight: 100,
            }],
            policies: vec![],
        }
    }

    fn provider(name: &str) -> Provider {
        Provider {
            name: name.into(),
            kind: ProviderKind::OpenAi,
            base_url: String::new(),
            api_key_env: None,
            request_headers: vec![],
        }
    }

    #[test]
    fn sources_own_disjoint_slices_and_do_not_erase_each_other() {
        let store = ConfigStore::new();

        // xDS owns the Gateway API slice.
        let xds = store.apply(
            SourceId::Xds,
            ConfigDelta::default()
                .with_version("ads-1")
                .with_listeners(vec![listener("http", 80, "reviews")])
                .with_clusters(vec![cluster("reviews")]),
        );
        assert!(xds.rejected.is_empty());

        // Kubernetes owns the agent slice. This used to clobber the xDS slice.
        let kube = store.apply(
            SourceId::Kubernetes,
            ConfigDelta::default()
                .with_version("7")
                .with_providers(vec![provider("openai")])
                .with_backends(vec![llm_backend("gpt", "openai")])
                .with_agent_routes(vec![agent_route("chat", "gpt")]),
        );
        assert!(kube.rejected.is_empty(), "{:?}", kube.rejected);
        assert!(kube.conflicts.is_empty(), "{:?}", kube.conflicts);

        let snapshot = store.snapshot();
        assert!(snapshot.ready());
        assert_eq!(snapshot.listeners().len(), 1);
        assert_eq!(snapshot.agent_routes().len(), 1);
        assert!(snapshot.cluster("reviews").is_some());
        assert!(snapshot.backend("gpt").is_some());
        assert_eq!(
            snapshot.owner(&ResourceKey::new(ResourceKind::Listener, "http")),
            Some(SourceId::Xds)
        );
        assert_eq!(
            snapshot.owner(&ResourceKey::new(ResourceKind::Backend, "gpt")),
            Some(SourceId::Kubernetes)
        );
    }

    #[test]
    fn a_source_removal_only_touches_its_own_resources() {
        let store = ConfigStore::new();
        store.apply(
            SourceId::Xds,
            ConfigDelta::default().with_clusters(vec![cluster("reviews")]),
        );
        store.apply(
            SourceId::Kubernetes,
            ConfigDelta::default().with_providers(vec![provider("openai")]),
        );

        // Kubernetes tries to remove a cluster it does not own: ignored.
        store.apply(
            SourceId::Kubernetes,
            ConfigDelta::default()
                .with_removes(vec![ResourceKey::new(ResourceKind::Cluster, "reviews")]),
        );
        assert!(store.snapshot().cluster("reviews").is_some());

        // The owner can remove it.
        store.apply(
            SourceId::Xds,
            ConfigDelta::default()
                .with_removes(vec![ResourceKey::new(ResourceKind::Cluster, "reviews")]),
        );
        assert!(store.snapshot().cluster("reviews").is_none());
        assert!(store.snapshot().provider("openai").is_some());
    }

    #[test]
    fn upserting_a_resource_owned_by_another_source_is_rejected() {
        let store = ConfigStore::new();
        store.apply(
            SourceId::Xds,
            ConfigDelta::default().with_clusters(vec![cluster("reviews")]),
        );
        let outcome = store.apply(
            SourceId::Static,
            ConfigDelta::default().with_clusters(vec![cluster("reviews")]),
        );

        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].kind, "ownership-conflict");
        assert_eq!(
            store
                .snapshot()
                .owner(&ResourceKey::new(ResourceKind::Cluster, "reviews")),
            Some(SourceId::Xds)
        );
    }

    #[test]
    fn dangling_references_are_reported_without_dropping_the_delta() {
        let store = ConfigStore::new();
        let outcome = store.apply(
            SourceId::Xds,
            ConfigDelta::default().with_listeners(vec![listener("http", 80, "missing")]),
        );

        assert!(!outcome.ready);
        assert_eq!(outcome.conflicts[0].kind, "missing-cluster");
        // The listener is still there; only readiness reflects the problem. A
        // cluster arriving in a later ADS response resolves it.
        assert_eq!(store.snapshot().listeners().len(), 1);

        let outcome = store.apply(
            SourceId::Xds,
            ConfigDelta::default().with_clusters(vec![cluster("missing")]),
        );
        assert!(outcome.ready);
        assert!(outcome.conflicts.is_empty());
    }

    #[test]
    fn evicting_a_source_leaves_other_sources_intact() {
        let store = ConfigStore::new();
        store.apply(
            SourceId::Xds,
            ConfigDelta::default().with_clusters(vec![cluster("reviews")]),
        );
        store.apply(
            SourceId::Kubernetes,
            ConfigDelta::default().with_providers(vec![provider("openai")]),
        );

        store.evict_source(SourceId::Xds);
        let snapshot = store.snapshot();
        assert!(snapshot.cluster("reviews").is_none());
        assert!(snapshot.provider("openai").is_some());
    }

    #[test]
    fn republishing_an_identical_slice_is_a_no_op() {
        let store = ConfigStore::new();
        let delta = || {
            ConfigDelta::default()
                .with_version("ads-1")
                .with_clusters(vec![cluster("reviews")])
        };

        let first = store.apply(SourceId::Xds, delta());
        assert!(first.changed);

        let second = store.apply(SourceId::Xds, delta());
        assert!(!second.changed);
        assert_eq!(second.revision, first.revision);

        // A real change still gets through.
        let third = store.apply(
            SourceId::Xds,
            ConfigDelta::default()
                .with_version("ads-2")
                .with_clusters(vec![cluster("reviews"), cluster("ratings")]),
        );
        assert!(third.changed);
        assert_eq!(third.revision, first.revision + 1);
    }

    #[test]
    fn readiness_is_false_until_a_source_applies() {
        let store = ConfigStore::new();
        assert!(!store.snapshot().ready());
        store.apply(SourceId::Static, ConfigDelta::default());
        assert!(store.snapshot().ready());
    }

    #[test]
    fn runtime_config_converts_into_an_upsert_only_delta() {
        let cfg = RuntimeConfig {
            version: "file-1".into(),
            listeners: vec![listener("http", 80, "reviews")],
            clusters: vec![cluster("reviews")],
            secrets: vec![],
            providers: vec![],
            backends: vec![],
            routes: vec![],
            policies: vec![],
        };
        let delta = ConfigDelta::from(cfg);

        assert_eq!(delta.version.as_deref(), Some("file-1"));
        assert!(delta.removes.is_empty());
        assert_eq!(delta.listeners.len(), 1);
    }
}
