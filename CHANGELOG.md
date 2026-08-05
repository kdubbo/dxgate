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

- Round-robin selection is per selection domain. One shared `picker_counter` drove
  cluster, backend, and endpoint choice, so each domain's sequence depended on
  unrelated traffic and distribution skewed away from the configured weights
  whenever domains had different sizes. Cursors are now keyed by route (for
  weighted clusters/backends) and by cluster (for endpoints), and are pruned when a
  config drops the route or cluster.

### Changed

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
