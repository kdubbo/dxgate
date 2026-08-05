//! Kubernetes informers for the dxgate CRDs.
//!
//! The controller used to treat a watch event as a bell: any event triggered a
//! full `LIST` of all four kinds and a rebuild of the whole configuration. That
//! throws away exactly the information a watch carries — which object changed,
//! and whether it was added, modified, or deleted.
//!
//! Here each kind gets a `kube` watcher feeding a local [`Cache`], the four
//! streams are merged, and events are consumed in batches. A `Restarted` event
//! means the watch lost its place and the accompanying list is the new truth, so
//! the cache is replaced wholesale — the one case where a re-list is correct.
//!
//! The caches are then projected onto dxgate's configuration model and diffed by
//! [`SourceState`], so the store receives real removals rather than a
//! whole-config overwrite.

use crate::{
    runtime_config_from_resources, ControllerError, Dxgate, DxgateBackend, DxgateCondition,
    DxgatePolicy, DxgateRoute, DxgateStatus,
};
use dxgate_core::{ConfigStore, SourceId, SourceState};
use futures_util::stream::{self, Stream, StreamExt};
use kube::api::{Patch, PatchParams};
use kube::core::NamespaceResourceScope;
use kube::runtime::watcher::{self, watcher, Config as WatcherConfig};
use kube::runtime::WatchStreamExt;
use kube::{Api, Client, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// How many already-available watch events are folded into one reconcile. A
/// rollout touching many resources produces a burst; projecting once per burst
/// keeps the store from rebuilding its indexes for every individual object.
const EVENT_BATCH: usize = 64;

/// Namespace and name — the identity Kubernetes gives a namespaced object.
type ObjectKey = (String, String);

fn object_key<K: Resource>(object: &K) -> ObjectKey {
    (object.namespace().unwrap_or_default(), object.name_any())
}

/// A local cache of one CRD kind. Ordered by `(namespace, name)` so the
/// projection is deterministic regardless of the order events arrived in.
#[derive(Debug)]
struct Cache<K> {
    objects: BTreeMap<ObjectKey, K>,
}

impl<K> Default for Cache<K> {
    fn default() -> Self {
        Self {
            objects: BTreeMap::new(),
        }
    }
}

impl<K: Resource + Clone> Cache<K> {
    fn apply(&mut self, event: watcher::Event<K>) {
        match event {
            watcher::Event::Applied(object) => {
                self.objects.insert(object_key(&object), object);
            }
            watcher::Event::Deleted(object) => {
                self.objects.remove(&object_key(&object));
            }
            // The watch lost its place, so deletions may have been missed and
            // the accompanying list is authoritative.
            watcher::Event::Restarted(objects) => {
                self.objects = objects
                    .into_iter()
                    .map(|object| (object_key(&object), object))
                    .collect();
            }
        }
    }

    fn values(&self) -> Vec<K> {
        self.objects.values().cloned().collect()
    }

    fn iter(&self) -> impl Iterator<Item = (&ObjectKey, &K)> {
        self.objects.iter()
    }
}

#[derive(Debug, Default)]
struct Caches {
    dxgates: Cache<Dxgate>,
    backends: Cache<DxgateBackend>,
    routes: Cache<DxgateRoute>,
    policies: Cache<DxgatePolicy>,
}

/// One event from any of the four watched kinds.
enum CrdEvent {
    Dxgate(watcher::Event<Dxgate>),
    Backend(watcher::Event<DxgateBackend>),
    Route(watcher::Event<DxgateRoute>),
    Policy(watcher::Event<DxgatePolicy>),
}

impl CrdEvent {
    fn kind(&self) -> &'static str {
        match self {
            CrdEvent::Dxgate(_) => "Dxgate",
            CrdEvent::Backend(_) => "DxgateBackend",
            CrdEvent::Route(_) => "DxgateRoute",
            CrdEvent::Policy(_) => "DxgatePolicy",
        }
    }
}

/// Merges the four watch streams into one. Each watcher retries with backoff on
/// its own, so an error is reported and the stream keeps running.
fn crd_events(client: Client) -> impl Stream<Item = Result<CrdEvent, watcher::Error>> {
    let config = WatcherConfig::default();
    let dxgates = watcher(Api::<Dxgate>::all(client.clone()), config.clone())
        .default_backoff()
        .map(|event| event.map(CrdEvent::Dxgate));
    let backends = watcher(Api::<DxgateBackend>::all(client.clone()), config.clone())
        .default_backoff()
        .map(|event| event.map(CrdEvent::Backend));
    let routes = watcher(Api::<DxgateRoute>::all(client.clone()), config.clone())
        .default_backoff()
        .map(|event| event.map(CrdEvent::Route));
    let policies = watcher(Api::<DxgatePolicy>::all(client), config)
        .default_backoff()
        .map(|event| event.map(CrdEvent::Policy));

    stream::select(
        stream::select(dxgates, backends),
        stream::select(routes, policies),
    )
}

/// Watches the dxgate CRDs and keeps the [`SourceId::Kubernetes`] slice of the
/// store in sync with them. Runs until the event stream ends.
pub async fn run_controller(store: Arc<ConfigStore>) -> Result<(), ControllerError> {
    let client = Client::try_default().await?;
    info!("started dxgate Kubernetes controller");

    let mut events = Box::pin(crd_events(client.clone()).ready_chunks(EVENT_BATCH));
    let mut caches = Caches::default();
    let mut source = SourceState::new();
    let mut statuses = StatusTracker::default();

    while let Some(batch) = events.next().await {
        let mut observed = 0usize;
        for event in batch {
            match event {
                Ok(event) => {
                    debug!(
                        kind = event.kind(),
                        "observed Kubernetes dxgate resource event"
                    );
                    observed += 1;
                    match event {
                        CrdEvent::Dxgate(event) => caches.dxgates.apply(event),
                        CrdEvent::Backend(event) => caches.backends.apply(event),
                        CrdEvent::Route(event) => caches.routes.apply(event),
                        CrdEvent::Policy(event) => caches.policies.apply(event),
                    }
                }
                Err(err) => warn!(%err, "Kubernetes watch error"),
            }
        }
        if observed == 0 {
            continue;
        }
        if let Err(err) = reconcile(
            &client,
            &store,
            &caches,
            &mut source,
            &mut statuses,
            observed,
        )
        .await
        {
            warn!(%err, "failed reconciling Kubernetes dxgate config");
        }
    }

    Ok(())
}

async fn reconcile(
    client: &Client,
    store: &ConfigStore,
    caches: &Caches,
    source: &mut SourceState,
    statuses: &mut StatusTracker,
    observed: usize,
) -> Result<(), ControllerError> {
    let config = match runtime_config_from_resources(
        &caches.dxgates.values(),
        &caches.backends.values(),
        &caches.routes.values(),
        &caches.policies.values(),
    ) {
        Ok(config) => config,
        Err(err) => {
            // The specs did not parse, so there is nothing meaningful to
            // publish. Leave the store holding the last good configuration and
            // say so on the resources.
            let message = format!("RuntimeConfig rejected: {err}");
            patch_statuses(client, caches, statuses, false, "Rejected", &message).await?;
            return Err(err);
        }
    };

    let version = config.version.clone();
    let delta = source.reconcile(config);
    let removes = delta.removes.len();
    let outcome = store.apply(SourceId::Kubernetes, delta);

    for rejected in &outcome.rejected {
        warn!(kind = %rejected.kind, message = %rejected.message, "Kubernetes resource rejected");
    }

    if outcome.changed {
        info!(
            events = observed,
            revision = outcome.revision,
            version = %version,
            removes,
            ready = outcome.ready,
            "applied Kubernetes dxgate runtime config"
        );
    }

    // The status reports the merged store, not just this source: a route whose
    // backend lives in another source is only truly accepted once both are in.
    let problems = outcome.problems();
    let (reason, message) = if problems.is_empty() {
        ("Accepted", "RuntimeConfig accepted".to_string())
    } else {
        (
            "Rejected",
            format!(
                "RuntimeConfig has unresolved references: {}",
                problems
                    .iter()
                    .map(|conflict| conflict.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
        )
    };
    patch_statuses(
        client,
        caches,
        statuses,
        problems.is_empty(),
        reason,
        &message,
    )
    .await
}

/// Remembers the status last written to each object so an unchanged status is
/// not re-patched on every reconcile.
#[derive(Debug, Default)]
struct StatusTracker {
    written: BTreeMap<(&'static str, ObjectKey), (Option<i64>, bool, String)>,
}

impl StatusTracker {
    fn needs_write(
        &self,
        kind: &'static str,
        key: &ObjectKey,
        generation: Option<i64>,
        ready: bool,
        message: &str,
    ) -> bool {
        match self.written.get(&(kind, key.clone())) {
            Some((seen_generation, seen_ready, seen_message)) => {
                *seen_generation != generation || *seen_ready != ready || seen_message != message
            }
            None => true,
        }
    }

    fn record(
        &mut self,
        kind: &'static str,
        key: ObjectKey,
        generation: Option<i64>,
        ready: bool,
        message: &str,
    ) {
        self.written
            .insert((kind, key), (generation, ready, message.to_string()));
    }
}

async fn patch_statuses(
    client: &Client,
    caches: &Caches,
    statuses: &mut StatusTracker,
    ready: bool,
    reason: &str,
    message: &str,
) -> Result<(), ControllerError> {
    patch_kind(
        client,
        "Dxgate",
        &caches.dxgates,
        statuses,
        ready,
        reason,
        message,
    )
    .await?;
    patch_kind(
        client,
        "DxgateBackend",
        &caches.backends,
        statuses,
        ready,
        reason,
        message,
    )
    .await?;
    patch_kind(
        client,
        "DxgateRoute",
        &caches.routes,
        statuses,
        ready,
        reason,
        message,
    )
    .await?;
    patch_kind(
        client,
        "DxgatePolicy",
        &caches.policies,
        statuses,
        ready,
        reason,
        message,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn patch_kind<K>(
    client: &Client,
    kind: &'static str,
    cache: &Cache<K>,
    statuses: &mut StatusTracker,
    ready: bool,
    reason: &str,
    message: &str,
) -> Result<(), ControllerError>
where
    K: Clone
        + Debug
        + DeserializeOwned
        + Serialize
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Send
        + Sync
        + 'static,
{
    for (key, object) in cache.iter() {
        let generation = object.meta().generation;
        if !statuses.needs_write(kind, key, generation, ready, message) {
            continue;
        }
        let Some(namespace) = object.namespace() else {
            continue;
        };
        let api = Api::<K>::namespaced(client.clone(), &namespace);
        let status = ready_status(object, ready, reason, message);
        let patch = Patch::Merge(json!({ "status": status }));
        api.patch_status(&object.name_any(), &PatchParams::default(), &patch)
            .await?;
        statuses.record(kind, key.clone(), generation, ready, message);
    }
    Ok(())
}

pub(crate) fn ready_status<K>(
    resource: &K,
    ready: bool,
    reason: &str,
    message: &str,
) -> DxgateStatus
where
    K: Resource,
{
    DxgateStatus {
        ready,
        message: Some(message.to_string()),
        observed_generation: resource.meta().generation,
        conditions: vec![DxgateCondition {
            type_: "Ready".to_string(),
            status: if ready { "True" } else { "False" }.to_string(),
            reason: reason.to_string(),
            message: message.to_string(),
            observed_generation: resource.meta().generation,
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DxgateBackendSpec;

    fn backend(namespace: &str, name: &str) -> DxgateBackend {
        let mut object = DxgateBackend::new(
            name,
            DxgateBackendSpec {
                backend: json!({ "name": name, "type": "http", "endpoint": "http://x" }),
            },
        );
        object.metadata.namespace = Some(namespace.to_string());
        object
    }

    #[test]
    fn applied_and_deleted_events_update_one_entry() {
        let mut cache = Cache::<DxgateBackend>::default();

        cache.apply(watcher::Event::Applied(backend("app", "a")));
        cache.apply(watcher::Event::Applied(backend("app", "b")));
        assert_eq!(cache.values().len(), 2);

        cache.apply(watcher::Event::Deleted(backend("app", "a")));
        let names: Vec<String> = cache.values().iter().map(|o| o.name_any()).collect();
        assert_eq!(names, ["b"]);
    }

    #[test]
    fn a_restart_replaces_the_cache_because_deletes_may_have_been_missed() {
        let mut cache = Cache::<DxgateBackend>::default();
        cache.apply(watcher::Event::Applied(backend("app", "stale")));

        cache.apply(watcher::Event::Restarted(vec![
            backend("app", "b"),
            backend("app", "a"),
        ]));

        // Ordered by (namespace, name), not by arrival order.
        let names: Vec<String> = cache.values().iter().map(|o| o.name_any()).collect();
        assert_eq!(names, ["a", "b"]);
    }

    #[test]
    fn objects_of_the_same_name_in_different_namespaces_are_distinct() {
        let mut cache = Cache::<DxgateBackend>::default();
        cache.apply(watcher::Event::Applied(backend("app", "shared")));
        cache.apply(watcher::Event::Applied(backend("other", "shared")));

        assert_eq!(cache.values().len(), 2);

        cache.apply(watcher::Event::Deleted(backend("app", "shared")));
        assert_eq!(cache.values().len(), 1);
        assert_eq!(cache.values()[0].namespace().as_deref(), Some("other"));
    }

    #[test]
    fn status_writes_are_skipped_until_something_changes() {
        let mut statuses = StatusTracker::default();
        let key = ("app".to_string(), "a".to_string());

        assert!(statuses.needs_write("DxgateBackend", &key, Some(1), true, "ok"));
        statuses.record("DxgateBackend", key.clone(), Some(1), true, "ok");
        assert!(!statuses.needs_write("DxgateBackend", &key, Some(1), true, "ok"));

        // A new generation, a flipped readiness, or a new message all re-patch.
        assert!(statuses.needs_write("DxgateBackend", &key, Some(2), true, "ok"));
        assert!(statuses.needs_write("DxgateBackend", &key, Some(1), false, "ok"));
        assert!(statuses.needs_write("DxgateBackend", &key, Some(1), true, "nope"));
    }
}
