//! Deltas, and the bridge that lets a state-of-the-world source drive them.

use super::{ResourceKey, ResourceKind};
use crate::{AgentRoute, Backend, Cluster, Listener, Policy, Provider, RuntimeConfig, TlsSecret};
use std::collections::BTreeSet;

/// One source's incremental update: resources to upsert, plus the keys that
/// source is retiring.
///
/// Building a delta directly is the incremental path (delta xDS, a Kubernetes
/// watch event). Sources that can only produce a full list go through
/// [`SourceState::reconcile`], which fills in `removes` for them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigDelta {
    /// Version label the source reports for this update, surfaced on `/readyz`.
    pub version: Option<String>,
    pub listeners: Vec<Listener>,
    pub clusters: Vec<Cluster>,
    pub secrets: Vec<TlsSecret>,
    pub providers: Vec<Provider>,
    pub backends: Vec<Backend>,
    pub agent_routes: Vec<AgentRoute>,
    pub policies: Vec<Policy>,
    pub removes: Vec<ResourceKey>,
}

impl ConfigDelta {
    pub fn is_empty(&self) -> bool {
        self.listeners.is_empty()
            && self.clusters.is_empty()
            && self.secrets.is_empty()
            && self.providers.is_empty()
            && self.backends.is_empty()
            && self.agent_routes.is_empty()
            && self.policies.is_empty()
            && self.removes.is_empty()
    }

    /// The keys this delta upserts, in store order.
    pub fn upserted_keys(&self) -> BTreeSet<ResourceKey> {
        let mut keys = BTreeSet::new();
        for value in &self.listeners {
            keys.insert(ResourceKey::new(ResourceKind::Listener, value.name.clone()));
        }
        for value in &self.clusters {
            keys.insert(ResourceKey::new(ResourceKind::Cluster, value.name.clone()));
        }
        for value in &self.secrets {
            keys.insert(ResourceKey::new(ResourceKind::Secret, value.name.clone()));
        }
        for value in &self.providers {
            keys.insert(ResourceKey::new(ResourceKind::Provider, value.name.clone()));
        }
        for value in &self.backends {
            keys.insert(ResourceKey::new(ResourceKind::Backend, value.name.clone()));
        }
        for value in &self.agent_routes {
            keys.insert(ResourceKey::new(
                ResourceKind::AgentRoute,
                value.name.clone(),
            ));
        }
        for value in &self.policies {
            keys.insert(ResourceKey::new(ResourceKind::Policy, value.name.clone()));
        }
        keys
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn with_listeners(mut self, listeners: Vec<Listener>) -> Self {
        self.listeners = listeners;
        self
    }

    pub fn with_clusters(mut self, clusters: Vec<Cluster>) -> Self {
        self.clusters = clusters;
        self
    }

    pub fn with_secrets(mut self, secrets: Vec<TlsSecret>) -> Self {
        self.secrets = secrets;
        self
    }

    pub fn with_providers(mut self, providers: Vec<Provider>) -> Self {
        self.providers = providers;
        self
    }

    pub fn with_backends(mut self, backends: Vec<Backend>) -> Self {
        self.backends = backends;
        self
    }

    pub fn with_agent_routes(mut self, agent_routes: Vec<AgentRoute>) -> Self {
        self.agent_routes = agent_routes;
        self
    }

    pub fn with_policies(mut self, policies: Vec<Policy>) -> Self {
        self.policies = policies;
        self
    }

    pub fn with_removes(mut self, removes: Vec<ResourceKey>) -> Self {
        self.removes = removes;
        self
    }
}

impl From<RuntimeConfig> for ConfigDelta {
    /// Upserts only. A `RuntimeConfig` on its own does not say what disappeared
    /// since the last one — that is what [`SourceState::reconcile`] adds.
    fn from(cfg: RuntimeConfig) -> Self {
        Self {
            version: Some(cfg.version),
            listeners: cfg.listeners,
            clusters: cfg.clusters,
            secrets: cfg.secrets,
            providers: cfg.providers,
            backends: cfg.backends,
            agent_routes: cfg.routes,
            policies: cfg.policies,
            removes: Vec::new(),
        }
    }
}

/// Remembers the keys a state-of-the-world source published last time, so a
/// full-list source can drive the delta store.
///
/// A static file, a Kubernetes re-list, or a legacy SotW ADS response all say
/// "here is everything I have" and say nothing about what they dropped.
/// `reconcile` diffs the new list against the previous key set and emits the
/// difference as explicit removals, scoped to that one source.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceState {
    published: BTreeSet<ResourceKey>,
}

impl SourceState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Keys currently attributed to this source.
    pub fn keys(&self) -> &BTreeSet<ResourceKey> {
        &self.published
    }

    pub fn is_empty(&self) -> bool {
        self.published.is_empty()
    }

    /// Turns a full-list update into a delta and records the new key set.
    pub fn reconcile(&mut self, config: RuntimeConfig) -> ConfigDelta {
        self.reconcile_delta(ConfigDelta::from(config))
    }

    /// Same as [`SourceState::reconcile`] for callers that already built the
    /// upsert side of the delta. Any `removes` already on `delta` are kept.
    pub fn reconcile_delta(&mut self, mut delta: ConfigDelta) -> ConfigDelta {
        let next = delta.upserted_keys();
        let mut removes: Vec<ResourceKey> = self.published.difference(&next).cloned().collect();
        removes.append(&mut delta.removes);
        delta.removes = removes;
        self.published = next;
        delta
    }

    /// Marks every key as retired, producing the delta that empties the source's
    /// slice of the store.
    pub fn drain(&mut self) -> ConfigDelta {
        let removes = std::mem::take(&mut self.published).into_iter().collect();
        ConfigDelta::default().with_removes(removes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cluster, Endpoint, Provider, ProviderKind, RuntimeConfig};

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

    fn config(version: &str, clusters: Vec<Cluster>, providers: Vec<Provider>) -> RuntimeConfig {
        RuntimeConfig {
            version: version.into(),
            listeners: vec![],
            clusters,
            secrets: vec![],
            providers,
            backends: vec![],
            routes: vec![],
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
    fn reconcile_turns_a_disappearance_into_a_removal() {
        let mut state = SourceState::new();

        let first = state.reconcile(config("1", vec![cluster("a"), cluster("b")], vec![]));
        assert!(first.removes.is_empty());
        assert_eq!(first.clusters.len(), 2);

        let second = state.reconcile(config("2", vec![cluster("a")], vec![]));
        assert_eq!(second.clusters.len(), 1);
        assert_eq!(
            second.removes,
            vec![ResourceKey::new(ResourceKind::Cluster, "b")]
        );
    }

    #[test]
    fn reconcile_does_not_remove_keys_of_other_kinds_with_the_same_name() {
        let mut state = SourceState::new();
        state.reconcile(config(
            "1",
            vec![cluster("shared")],
            vec![provider("shared")],
        ));

        let next = state.reconcile(config("2", vec![cluster("shared")], vec![]));
        assert_eq!(
            next.removes,
            vec![ResourceKey::new(ResourceKind::Provider, "shared")]
        );
    }

    #[test]
    fn drain_retires_everything_the_source_published() {
        let mut state = SourceState::new();
        state.reconcile(config("1", vec![cluster("a")], vec![provider("p")]));

        let drained = state.drain();
        assert_eq!(drained.removes.len(), 2);
        assert!(state.is_empty());
    }
}
