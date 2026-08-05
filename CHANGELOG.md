# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project aims
to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- Upstream mTLS now pins the peer's identity. `SpiffeCompatibleVerifier` tolerated
  webpki's `NotValidForName` (SPIFFE leaf certs carry a URI SAN and no DNS SAN) but
  never re-asserted the identity, so any workload holding a certificate from the
  trust domain's CA was accepted for any cluster. `UpstreamTls` gained
  `subject_alt_names`, populated from the xDS validation context's
  `match_subject_alt_names`; when set, the peer certificate must present a matching
  URI or DNS SAN or the handshake fails. When unset the previous behavior is kept
  (chain verified, identity not) and the verifier warns once per client that the
  peer is unverified.

### Added

- An owner-tracked configuration store (`dxgate_core::store`). Every resource is
  keyed by `(kind, name)` and carries the `SourceId` that published it, so a
  source's update only ever touches resources that source owns. Upserting a
  resource another source owns is refused and reported instead of silently
  stealing it, and removals are scoped the same way. This is what lets xDS own
  listeners and clusters while the Kubernetes controller owns backends, routes,
  and policies — previously the three sources fed one whole-value channel and
  whichever published last erased the others.
- `SourceState`, the bridge that lets a state-of-the-world source drive the delta
  store: it diffs each full list against the keys that source published last time
  and turns every disappearance into an explicit removal. Used by the static-file
  source, the Kubernetes informer's re-list path, and the xDS client's derived
  resources.
- Delta ADS (`DeltaAggregatedResources`). The client now prefers the incremental
  protocol, tracks per-resource versions, and replays them as
  `initial_resource_versions` after a reconnect so the control plane only resends
  what changed. Subscriptions are maintained as subscribe/unsubscribe diffs
  rather than full name lists. A control plane that answers `UNIMPLEMENTED` falls
  back to state-of-the-world ADS for the rest of the process lifetime; both
  flavours feed the same resource cache.
- `/debug/sources` on the admin port, listing which source owns each resource and
  the version each source last reported, plus `revision` and `source_versions` on
  `/readyz`. `/debug/policies` now reports what each policy is attached to.
- Outlier detection is enforced. `OutlierDetectionConfig` was parsed from xDS and
  stored on the cluster but never consumed, so a persistently failing endpoint kept
  receiving its share of traffic. A run of `consecutive_5xx_errors` now ejects the
  endpoint for `base_ejection_time`, growing with each repeat ejection, and
  `max_ejection_percent` / `min_health_percent` bound how much of a cluster may be
  ejected at once so ejection cannot turn a partial outage into a total one.
- Graceful shutdown. Both listeners stop accepting on SIGTERM (what Kubernetes
  sends; only SIGINT was handled before) and in-flight requests are drained before
  exit, bounded by `--drain-timeout-seconds` / `DXGATE_DRAIN_TIMEOUT_SECONDS`
  (default 30). `examples/dxgate.yaml` sets a matching
  `terminationGracePeriodSeconds`.
- CI `performance` job running the previously manual-only `performance` and `sla`
  tests with deliberately loose, env-tuned thresholds, gating against
  order-of-magnitude throughput and latency regressions.
- `SECURITY.md` describing the private vulnerability reporting process.
- `CONTRIBUTING.md` with local setup and required-checks documentation.
- `deny.toml` and a CI `supply-chain` job running `cargo deny` (license,
  advisory, and source gating).
- CI jobs verifying the pinned MSRV (Rust 1.85) and building the Docker image.
- xDS conversion tests for `match_subject_alt_names`, circuit-breaker thresholds,
  outlier-detection duration round-tripping, endpoint health-status filtering, and
  route-match regex rejection.

### Fixed

- Configuration sources no longer overwrite each other. xDS, the static file, and
  the Kubernetes controller each pushed a whole `RuntimeConfig` into one
  `watch` channel, so running more than one source meant the last writer won.
  They now write owner-scoped deltas into a shared store, and the xDS client is
  no longer limited to listeners and clusters because it can no longer blank the
  agent slice by publishing.
- The Kubernetes controller consumes watch events instead of using them as a
  bell. It previously re-`LIST`ed all four CRD kinds and rebuilt the whole
  configuration on any event; it now keeps a per-kind informer cache fed by
  `kube::runtime::watcher`, applying `Applied` / `Deleted` per object and
  replacing the cache only on `Restarted` (where a re-list is the correct
  response to a lost watch). Events are batched per reconcile and resource
  statuses are only patched when they actually change.
- Referential validation moved to the store, which is the only component that
  sees every source. The controller used to reject a CRD route whose backend
  came from another source; unresolved references are now reported through
  readiness while the configuration still converges.
- Round-robin selection is per selection domain. One shared `picker_counter` drove
  cluster, backend, and endpoint choice, so each domain's sequence depended on
  unrelated traffic and distribution skewed away from the configured weights
  whenever domains had different sizes. Cursors are now keyed by route (for
  weighted clusters/backends) and by cluster (for endpoints), and are pruned when a
  config drops the route or cluster.

### Changed

- The request path reads an immutable, indexed snapshot instead of cloning the
  configuration. Each request used to clone the entire `RuntimeConfig` and then
  scan listeners, virtual hosts, and routes linearly. A snapshot is now rebuilt
  once per applied delta and handed to readers as an `Arc`: listeners are
  collapsed into a per-port index with pre-parsed domain matchers, clusters,
  providers, backends, and policies are hash maps, agent routes are bucketed by
  protocol, and policies carry a reverse index of what attaches them. Route order
  within a port is unchanged — xDS route matching is first-match-wins over the
  order the control plane sent, so the index narrows candidates without
  reordering them.
- Re-publishing an identical slice is a no-op: the store recognises it, skips the
  revision bump and the index rebuild, and reports `changed: false`. Sources that
  can only emit their whole slice no longer invalidate readers for nothing.
- Split the `proxy` crate's monolithic `server.rs` (2908 lines) into a directory
  module (`server/mod.rs`, now ~2259 lines) by extracting cohesive, low-coupling
  concerns into submodules: `server/upstream.rs` (HTTP clients + data-plane
  mTLS/cert loading), `server/trace.rs` (W3C trace-context propagation),
  `server/access_log.rs` (access-log config + line formatting), `server/headers.rs`
  (hop-by-hop/policy/provider header transforms), `server/routing.rs` (pure
  backend/upstream routing helpers), `server/detect.rs` (request
  classification: stream/gRPC detection and agent-protocol routing by path),
  `server/auth.rs` (API-key / HMAC-JWT authentication enforcement), and
  `server/context.rs` (the parsed per-request agent context and SSE
  anti-buffering headers), `server/policy.rs` (policy-chain evaluation
  into a `PolicyRuntime`: deny/body-size/auth/rate/token enforcement), and
  `server/llm_flow.rs` (OpenAI-compatible LLM exchange building, dialect
  translation, and token-usage metering). `server/mod.rs` is now ~1615
  lines (from 2908). No behavioral change; all tests pass.
