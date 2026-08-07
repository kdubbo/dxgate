// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! On-demand activation: hold a request whose target is scaled to zero, tell
//! the control plane the target is wanted, and release the request once
//! endpoints appear.
//!
//! The gateway is the only component on the request path that can do this. The
//! mesh is proxyless, so a caller's own gRPC xDS client dials endpoints
//! directly; when there are none it fails immediately and nothing survives long
//! enough to ask for a scale-up. A request that reaches the gateway, by
//! contrast, is already parked in a task the gateway owns, so it can wait.
//!
//! Waiting is all this does. It never scales anything itself: it reports
//! pending counts, KEDA reads them through the control plane's external scaler,
//! and the autoscaler owns the replica count. Keeping that boundary means a
//! gateway restart cannot strand a workload at a size nobody asked for.

use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dxgate_core::Cluster;
use dxgate_xds::activation::activation_demand_client::ActivationDemandClient;
use dxgate_xds::activation::{DemandSnapshot, TargetDemand};
use tokio::sync::Notify;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, warn};

use crate::state::ProxyState;

/// Control-plane address. Must resolve to one record per control-plane replica,
/// not to a load-balanced VIP: KEDA polls whichever replica it lands on, so a
/// report delivered to only one of them would leave the request waiting on a
/// scale-up that the polled replica never learns about.
const CONTROL_PLANE_ENV: &str = "DXGATE_ACTIVATION_CONTROL_PLANE";
/// Identity of this gateway in reports. Two gateways sharing it would overwrite
/// each other's counts, so it must be the pod name, not the Deployment name.
const REPORTER_ENV: &str = "POD_NAME";
const HOLD_TIMEOUT_ENV: &str = "DXGATE_ACTIVATION_HOLD_TIMEOUT";
const MAX_PENDING_ENV: &str = "DXGATE_ACTIVATION_MAX_PENDING_REQUESTS";

/// How long a request waits for its target to come up before giving up. Cold
/// start of a JVM service routinely exceeds 30s, but a caller that has already
/// given up is not helped by a longer wait here.
const DEFAULT_HOLD_TIMEOUT: Duration = Duration::from_secs(30);
/// Cap on requests held at once, across all targets. Held requests occupy a
/// task and a connection each; without a cap, one unreachable target would
/// consume the whole gateway's capacity.
const DEFAULT_MAX_PENDING: usize = 1024;
/// How often a held request re-reads the snapshot. Cold start is measured in
/// seconds, so this only decides how much of that is spent already-ready but
/// not yet noticed.
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Resend interval when demand has not changed. Must stay well under the
/// control plane's 30s reporter TTL, or a steady-state backlog would age out
/// and the target would be scaled back down underneath it.
const HEARTBEAT: Duration = Duration::from_secs(10);
/// How often the control-plane name is re-resolved to pick up replicas that
/// rolled. Independent of the heartbeat: this covers set membership, that one
/// covers liveness of an established stream.
const RESOLVE_INTERVAL: Duration = Duration::from_secs(15);

/// A Service the control plane can activate, as named in a report.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Target {
    pub namespace: String,
    pub name: String,
}

impl Target {
    /// Parses the Service out of an xDS cluster name, which is
    /// `direction|port|subset|authority` with authority a cluster-local DNS
    /// name.
    ///
    /// Returns `None` for anything that is not a Service in this cluster —
    /// external hosts and ServiceEntry-backed clusters have no Deployment to
    /// scale, so holding a request for one would only add latency to a failure.
    pub fn from_cluster_name(cluster: &str) -> Option<Self> {
        let authority = cluster.split('|').nth(3).filter(|a| !a.is_empty())?;
        let authority = authority.split(':').next().unwrap_or(authority);
        let mut parts = authority.split('.');
        let name = parts.next().filter(|p| !p.is_empty())?;
        let namespace = parts.next().filter(|p| !p.is_empty())?;
        // `svc` in the third position is what distinguishes a Service from a
        // pod address or an arbitrary external host that happens to have dots.
        if parts.next() != Some("svc") {
            return None;
        }
        Some(Self {
            namespace: namespace.to_string(),
            name: name.to_string(),
        })
    }
}

/// Holds requests for scaled-to-zero targets and reports the resulting demand.
///
/// Cloning shares one registry; the gateway holds a single logical activator
/// however many times the server is cloned across connections.
#[derive(Clone, Default)]
pub struct Activator {
    inner: Option<Arc<Inner>>,
}

struct Inner {
    reporter: String,
    hold_timeout: Duration,
    max_pending: usize,
    /// Absolute pending count per target. The control plane treats a report as
    /// the whole truth for this reporter, so a target must be *present with a
    /// count* while held and *absent* once released — there is no decrement
    /// message to lose.
    demand: Mutex<HashMap<Target, u64>>,
    /// Raised whenever `demand` changes, so a scale-up is asked for on the
    /// first held request rather than at the next heartbeat.
    changed: Notify,
    held: AtomicU64,
}

impl Activator {
    /// Reads configuration from the environment. Absent
    /// `DXGATE_ACTIVATION_CONTROL_PLANE` disables activation entirely, which is
    /// the state of every gateway that has no scaled-to-zero backends: the
    /// request path then behaves exactly as it did before this existed.
    pub fn from_env() -> Self {
        let Some(control_plane) = env::var(CONTROL_PLANE_ENV)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
        else {
            return Self { inner: None };
        };
        let reporter = env::var(REPORTER_ENV)
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| {
                warn!(
                    "{REPORTER_ENV} is unset; activation reports from this gateway cannot be \
                     told apart from another replica's and will overwrite each other"
                );
                "unknown".to_string()
            });
        let inner = Arc::new(Inner {
            reporter,
            hold_timeout: env_duration(HOLD_TIMEOUT_ENV, DEFAULT_HOLD_TIMEOUT),
            max_pending: env_usize(MAX_PENDING_ENV, DEFAULT_MAX_PENDING),
            demand: Mutex::new(HashMap::new()),
            changed: Notify::new(),
            held: AtomicU64::new(0),
        });
        // The server is constructible outside a runtime, so spawn through an
        // explicit handle rather than `tokio::spawn`. Without a runtime there
        // is nothing to report to: hold requests anyway, but say so, because
        // holding without reporting is a wait that can only ever time out.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(report_loop(Arc::clone(&inner), control_plane));
            }
            Err(_) => warn!(
                "activation configured but no tokio runtime is available; \
                 demand will not be reported"
            ),
        }
        Self { inner: Some(inner) }
    }

    /// Activator with no control plane: holds nothing, reports nothing.
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    /// Activator that holds requests but reports to nobody.
    ///
    /// For tests, and for the one production case that is not a mistake: a
    /// gateway whose targets are scaled by something other than this control
    /// plane still benefits from waiting out the cold start, it just has no
    /// demand to contribute. Configuration comes from arguments rather than the
    /// environment so tests can run in parallel without sharing process state.
    pub fn holding(hold_timeout: Duration, max_pending: usize) -> Self {
        Self {
            inner: Some(Arc::new(Inner {
                reporter: String::new(),
                hold_timeout,
                max_pending,
                demand: Mutex::new(HashMap::new()),
                changed: Notify::new(),
                held: AtomicU64::new(0),
            })),
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Requests currently held. Exposed for the metrics endpoint, where it is
    /// the signal that distinguishes "slow" from "waiting on a cold start".
    pub fn held_requests(&self) -> u64 {
        self.inner
            .as_ref()
            .map_or(0, |inner| inner.held.load(Ordering::Relaxed))
    }

    /// Holds the caller until `cluster` has a healthy endpoint, returning the
    /// refreshed cluster.
    ///
    /// `None` means the caller should fail the request as it would have without
    /// activation: the target is not activatable, the gateway is already
    /// holding its limit, or the wait timed out. It is deliberately not an
    /// error type — every one of those cases ends in the same 503 the
    /// no-endpoint path already produces, and giving them separate errors would
    /// invite the request path to treat a cold start as a new kind of failure.
    pub async fn activate(&self, state: &ProxyState, cluster_name: &str) -> Option<Arc<Cluster>> {
        let inner = self.inner.as_ref()?;
        let target = Target::from_cluster_name(cluster_name)?;

        let _hold = Hold::acquire(Arc::clone(inner), target.clone())?;
        debug!(
            namespace = %target.namespace,
            service = %target.name,
            "holding request for scaled-to-zero target"
        );

        let deadline = tokio::time::Instant::now() + inner.hold_timeout;
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            if let Some(cluster) = ready_cluster(state, cluster_name) {
                debug!(
                    namespace = %target.namespace,
                    service = %target.name,
                    "target activated; releasing held request"
                );
                return Some(cluster);
            }
            if tokio::time::Instant::now() >= deadline {
                warn!(
                    namespace = %target.namespace,
                    service = %target.name,
                    timeout_ms = inner.hold_timeout.as_millis(),
                    "activation timed out; no endpoint appeared"
                );
                return None;
            }
        }
    }
}

fn ready_cluster(state: &ProxyState, cluster_name: &str) -> Option<Arc<Cluster>> {
    let snapshot = state.snapshot();
    let cluster = snapshot.cluster(cluster_name)?;
    // An endpoint that exists but is unhealthy is a pod still starting; it is
    // the healthy count, not the endpoint count, that says the request can go.
    let ready = cluster.healthy_endpoints().next().is_some();
    ready.then(|| Arc::clone(cluster))
}

/// One held request's entry in the demand registry. Registers on acquire and
/// deregisters on drop, so a cancelled request — a client that hung up mid-wait
/// — cannot leave demand behind and hold a workload up indefinitely.
struct Hold {
    inner: Arc<Inner>,
    target: Target,
}

impl Hold {
    fn acquire(inner: Arc<Inner>, target: Target) -> Option<Self> {
        // Reserve capacity before touching the registry, so a rejected request
        // never appears in a report.
        let admitted = inner
            .held
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                (held < inner.max_pending as u64).then_some(held + 1)
            })
            .is_ok();
        if !admitted {
            warn!(
                namespace = %target.namespace,
                service = %target.name,
                max_pending = inner.max_pending,
                "refusing to hold request; activation backlog is full"
            );
            return None;
        }
        *inner
            .demand
            .lock()
            .expect("activation demand mutex poisoned")
            .entry(target.clone())
            .or_insert(0) += 1;
        inner.changed.notify_one();
        Some(Self { inner, target })
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        {
            let mut demand = self
                .inner
                .demand
                .lock()
                .expect("activation demand mutex poisoned");
            if let Some(count) = demand.get_mut(&self.target) {
                *count -= 1;
                // Remove rather than report zero: the control plane reads an
                // absent target as no demand, and leaving zeroes behind would
                // grow the map by one entry per target ever activated.
                if *count == 0 {
                    demand.remove(&self.target);
                }
            }
        }
        self.inner.held.fetch_sub(1, Ordering::AcqRel);
        self.inner.changed.notify_one();
    }
}

impl Inner {
    fn snapshot(&self) -> DemandSnapshot {
        let demand = self
            .demand
            .lock()
            .expect("activation demand mutex poisoned");
        let mut targets: Vec<TargetDemand> = demand
            .iter()
            .map(|(target, pending)| TargetDemand {
                namespace: target.namespace.clone(),
                service: target.name.clone(),
                pending: *pending as i64,
            })
            .collect();
        // Stable order so consecutive snapshots of unchanged demand are
        // byte-identical, which keeps them readable in a packet capture.
        targets.sort_by(|a, b| (&a.namespace, &a.service).cmp(&(&b.namespace, &b.service)));
        DemandSnapshot {
            reporter: self.reporter.clone(),
            targets,
        }
    }
}

/// Keeps one report stream open to every control-plane replica.
async fn report_loop(inner: Arc<Inner>, control_plane: String) {
    let mut live: HashMap<String, Arc<Notify>> = HashMap::new();
    loop {
        match resolve(&control_plane).await {
            Ok(addresses) => {
                // Stop streams to replicas that are gone. The control plane
                // also drops a reporter when its stream ends, so this is what
                // keeps a rolled replica from being counted twice.
                live.retain(|address, stop| {
                    let keep = addresses.contains(address);
                    if !keep {
                        stop.notify_waiters();
                    }
                    keep
                });
                for address in addresses {
                    if live.contains_key(&address) {
                        continue;
                    }
                    let stop = Arc::new(Notify::new());
                    live.insert(address.clone(), Arc::clone(&stop));
                    tokio::spawn(report_to(Arc::clone(&inner), address, stop));
                }
            }
            Err(err) => {
                warn!(
                    control_plane = %control_plane,
                    error = %err,
                    "cannot resolve activation control plane; \
                     scaled-to-zero targets will not be activated"
                );
            }
        }
        tokio::time::sleep(RESOLVE_INTERVAL).await;
    }
}

async fn resolve(control_plane: &str) -> std::io::Result<Vec<String>> {
    let mut addresses: Vec<String> = tokio::net::lookup_host(control_plane)
        .await?
        .map(|addr| addr.to_string())
        .collect();
    addresses.sort();
    addresses.dedup();
    Ok(addresses)
}

/// Streams demand to one replica until `stop` fires, reconnecting on failure.
///
/// Reconnecting is not optional: the control plane treats the end of a stream
/// as the reporter going away and drops its demand immediately, so a dropped
/// connection would silently release every request this gateway is holding.
async fn report_to(inner: Arc<Inner>, address: String, stop: Arc<Notify>) {
    let endpoint = format!("http://{address}");
    let mut backoff = Duration::from_millis(200);
    loop {
        tokio::select! {
            biased;
            _ = stop.notified() => return,
            outcome = stream_once(&inner, &endpoint) => match outcome {
                Ok(()) => backoff = Duration::from_millis(200),
                Err(err) => {
                    debug!(%address, error = %err, "activation report stream ended");
                }
            },
        }
        tokio::select! {
            biased;
            _ = stop.notified() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

async fn stream_once(inner: &Arc<Inner>, endpoint: &str) -> Result<(), tonic::Status> {
    let mut client = ActivationDemandClient::connect(endpoint.to_string())
        .await
        .map_err(|err| tonic::Status::unavailable(err.to_string()))?;

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let pump = {
        let inner = Arc::clone(inner);
        tokio::spawn(async move {
            loop {
                if tx.send(inner.snapshot()).await.is_err() {
                    return;
                }
                // Either edge is enough on its own: `changed` makes a new hold
                // visible immediately, the heartbeat keeps an unchanging
                // backlog from ageing out of the control plane's registry.
                tokio::select! {
                    _ = inner.changed.notified() => {}
                    _ = tokio::time::sleep(HEARTBEAT) => {}
                }
            }
        })
    };

    let result = client.report(ReceiverStream::new(rx)).await;
    pump.abort();
    result.map(|_| ())
}

fn env_duration(key: &str, default: Duration) -> Duration {
    env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map_or(default, Duration::from_secs)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_service_out_of_cluster_name() {
        let target =
            Target::from_cluster_name("outbound|8080||payment.default.svc.cluster.local").unwrap();
        assert_eq!(target.namespace, "default");
        assert_eq!(target.name, "payment");
    }

    #[test]
    fn parses_cluster_name_with_subset() {
        let target =
            Target::from_cluster_name("outbound|8080|v1|payment.prod.svc.cluster.local").unwrap();
        assert_eq!(target.namespace, "prod");
        assert_eq!(target.name, "payment");
    }

    #[test]
    fn rejects_targets_with_nothing_to_scale() {
        // External host: no Deployment behind it.
        assert!(Target::from_cluster_name("outbound|443||api.openai.com").is_none());
        // Pod address rather than a Service.
        assert!(
            Target::from_cluster_name("outbound|8080||10-1-2-3.default.pod.cluster.local")
                .is_none()
        );
        // Not an xDS cluster name at all.
        assert!(Target::from_cluster_name("payment").is_none());
        assert!(Target::from_cluster_name("outbound|8080||").is_none());
    }

    #[test]
    fn strips_port_from_authority() {
        let target =
            Target::from_cluster_name("outbound|8080||payment.default.svc.cluster.local:8080")
                .unwrap();
        assert_eq!(target.name, "payment");
        assert_eq!(target.namespace, "default");
    }

    fn test_inner(max_pending: usize) -> Arc<Inner> {
        Arc::new(Inner {
            reporter: "dxgate-0".to_string(),
            hold_timeout: Duration::from_millis(50),
            max_pending,
            demand: Mutex::new(HashMap::new()),
            changed: Notify::new(),
            held: AtomicU64::new(0),
        })
    }

    fn target(name: &str) -> Target {
        Target {
            namespace: "default".to_string(),
            name: name.to_string(),
        }
    }

    #[test]
    fn snapshot_reports_absolute_counts() {
        let inner = test_inner(8);
        let _a = Hold::acquire(Arc::clone(&inner), target("payment")).unwrap();
        let _b = Hold::acquire(Arc::clone(&inner), target("payment")).unwrap();
        let _c = Hold::acquire(Arc::clone(&inner), target("orders")).unwrap();

        let snapshot = inner.snapshot();
        assert_eq!(snapshot.reporter, "dxgate-0");
        assert_eq!(snapshot.targets.len(), 2);
        assert_eq!(snapshot.targets[0].service, "orders");
        assert_eq!(snapshot.targets[0].pending, 1);
        assert_eq!(snapshot.targets[1].service, "payment");
        assert_eq!(snapshot.targets[1].pending, 2);
    }

    #[test]
    fn releasing_a_hold_drops_the_target_rather_than_reporting_zero() {
        let inner = test_inner(8);
        let first = Hold::acquire(Arc::clone(&inner), target("payment")).unwrap();
        let second = Hold::acquire(Arc::clone(&inner), target("payment")).unwrap();

        drop(first);
        assert_eq!(inner.snapshot().targets[0].pending, 1);

        drop(second);
        assert!(
            inner.snapshot().targets.is_empty(),
            "a released target must be absent, not present with zero"
        );
        assert_eq!(inner.held.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn backlog_limit_refuses_further_holds() {
        let inner = test_inner(1);
        let _held = Hold::acquire(Arc::clone(&inner), target("payment")).unwrap();
        assert!(
            Hold::acquire(Arc::clone(&inner), target("orders")).is_none(),
            "the cap is on requests held at once, not per target"
        );
        // A rejected hold must not appear in a report.
        assert_eq!(inner.snapshot().targets.len(), 1);
    }

    #[test]
    fn capacity_returns_after_a_rejected_hold_is_released() {
        let inner = test_inner(1);
        let held = Hold::acquire(Arc::clone(&inner), target("payment")).unwrap();
        assert!(Hold::acquire(Arc::clone(&inner), target("payment")).is_none());
        drop(held);
        assert!(Hold::acquire(Arc::clone(&inner), target("payment")).is_some());
    }

    #[tokio::test]
    async fn disabled_activator_holds_nothing() {
        let activator = Activator::disabled();
        assert!(!activator.enabled());
        assert_eq!(activator.held_requests(), 0);
    }
}
