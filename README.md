<p align="center">
  <img src="./logo/dxgate-logo.svg" alt="dxgate logo" width="260">
</p>

<p align="center">
  <a href="https://github.com/kdubbo/dxgate/actions/workflows/ci.yml"><img src="https://github.com/kdubbo/dxgate/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/license-Apache--2.0-green.svg" alt="license">
  <img src="https://img.shields.io/badge/Rust-1.76%2B-orange.svg?style=flat" alt="Rust: 1.76+">
</p>

dxgate is the delegated gateway for Dubbo Gateway API traffic and the external data-plane proxy of the [Apache Dubbo Kubernetes](https://github.com/apache/dubbo-kubernetes) project. It consumes control-plane configuration from `dubbod` as a router xDS client.

It also ships an additional proxy gateway runtime for OpenAI-compatible LLM routing, MCP tool routing and federation, A2A forwarding, reusable policies, richer metrics, and Kubernetes CRD-driven configuration. The original Dubbo Gateway API/xDS path remains the default path.

## Features

- **Dubbo Gateway API data plane** — HTTP routing by host, path, and header with weighted clusters, driven by `dubbod` over xDS (listeners, clusters, endpoints, TLS secrets).
- **Upstream TLS** — plaintext, simple TLS, and Dubbo mutual TLS (certificates via xDS SDS or file-watcher bootstrap).
- **LLM routing** — OpenAI-compatible `/v1/*` routing with model-aware backend selection and streaming (SSE) pass-through.
- **MCP routing and federation** — `mcp-session-id` session-to-backend binding and federated `tools/list` across multiple MCP backends.
- **A2A forwarding** — agent-card and A2A endpoint routing.
- **Reusable policies** — API-key / JWT auth, rate limiting, retries, timeouts, request/response header transforms, body-size limits, allow/deny.
- **Resilience** — per-cluster circuit breakers and retry with failover across weighted backends.
- **Observability** — Prometheus metrics, W3C trace propagation with OTLP export, structured access logs, and a built-in admin UI.
- **Three config sources** — xDS from `dubbod` (default), static YAML file (optionally watched), and Kubernetes CRDs (`Dxgate`, `DxgateBackend`, `DxgateRoute`, `DxgatePolicy`).

## Quick start

### Local static config mode

```bash
cargo run --bin dxgate -- \
  --http-addr 127.0.0.1:18080 \
  --admin-addr 127.0.0.1:18081 \
  --static-config examples/agent-runtime.yaml
```

`examples/agent-runtime.yaml` shows the full static schema: providers, LLM/MCP/A2A backends, agent routes, and policies. Add `--config-watch` to hot-reload the file on change.

When `--static-config` is used without listener names, dxgate does not start the xDS client by default. Set `--xds-enabled true` or `DXGATE_XDS_ENABLED=true` to run static config and xDS together.

### Kubernetes controller mode

```bash
cargo run --bin dxgate -- --print-crds | kubectl apply -f -
kubectl apply -f examples/rbac.yaml
kubectl apply -f examples/dxgate.yaml
kubectl apply -f examples/crds.yaml
DXGATE_MODE=all DXGATE_OTEL_ENDPOINT=http://otel-collector:4317 cargo run --bin dxgate
```

`DXGATE_MODE` selects `proxy` (default), `controller`, or `all` (proxy + CRD controller in one process).

## Admin endpoints

The admin server (default `0.0.0.0:15021`) serves a web UI at `/` with overview metrics, Dubbo/agent routes, backends, clusters, policies, an MCP playground, and the effective runtime config.

| Path | Purpose |
| --- | --- |
| `/healthz` | Liveness with build info |
| `/readyz` | Readiness (config accepted, conflicts listed) |
| `/metrics` | Prometheus metrics |
| `/debug/config` | Effective runtime config as JSON |
| `/debug/routes`, `/debug/clusters`, `/debug/backends`, `/debug/policies` | Per-resource debug views |

## Configuration

All flags are also available as environment variables (`--help` for the full list). Common runtime knobs:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DXGATE_MODE` | `proxy` | `proxy`, `controller`, or `all` |
| `DXGATE_XDS_ADDRESS` | `http://dubbod.dubbo-system.svc:15012` | dubbod xDS endpoint |
| `DXGATE_HTTP_ADDR` | `0.0.0.0:80` | Gateway listener |
| `DXGATE_ADMIN_ADDR` | `0.0.0.0:15021` | Admin server |
| `DXGATE_STATIC_CONFIG` | – | Static YAML config path |
| `DXGATE_BOOTSTRAP` | – | Bootstrap JSON (xDS address, mTLS cert providers, …) |
| `DXGATE_POLICY_DEFAULT` | `allow` | `deny` rejects agent requests with no matching policy |
| `DXGATE_MAX_BODY_BYTES` | `10485760` | Max buffered request body for agent routes (413 above it) |
| `DXGATE_ACCESS_LOG` | on | `false`/`0`/`no`/`off` disables access logs |
| `DXGATE_ACCESS_LOG_FORMAT` | `text` | `json` for structured access logs |
| `DXGATE_OTEL_ENDPOINT` | – | OTLP gRPC endpoint; enables trace export |
| `DXGATE_OTEL_SAMPLING_PERCENTAGE` | `100` | Parent-based trace sampling ratio |
| `DXGATE_OTEL_TAGS` | – | JSON object of static span tags, for example `{"foo":"bar"}` |

Note: rate-limit buckets are in-process; with multiple replicas each replica enforces its own budget.

## Architecture

| Crate | Role |
| --- | --- |
| `crates/app` | `dxgate` binary: CLI, bootstrap, mode wiring, OTel init |
| `crates/proxy` | Data plane: HTTP/LLM/MCP/A2A forwarding, policies, circuit breakers, upstream TLS |
| `crates/xds` | xDS client for dubbod (listeners, clusters, endpoints, secrets) and static file source |
| `crates/controller` | Kubernetes controller for the `Dxgate*` CRDs |
| `crates/admin` | Admin server: UI, health/readiness, Prometheus metrics, debug endpoints |
| `crates/core` | Shared runtime config model, matchers, validation |

## Development

```bash
cargo build --workspace
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace           # unit + end-to-end tests
cargo test -p dxgate-app --test e2e   # e2e only: routing, policies, body limits, SSE streaming
```

CI runs the same checks on every push and pull request.

## License

[Apache-2.0](./LICENSE)
