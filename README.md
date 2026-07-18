<p align="center">
  <img src="./logo/dxgate-logo.svg" alt="dxgate logo" width="260">
</p>

<p align="center">
  <a href="https://github.com/kdubbo/dxgate/actions/workflows/ci.yml"><img src="https://github.com/kdubbo/dxgate/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/license-Apache--2.0-green.svg" alt="license">
  <img src="https://img.shields.io/badge/Rust-1.85%2B-orange.svg?style=flat" alt="Rust: 1.85+">
</p>

dxgate is the delegated gateway for Dubbo Gateway API traffic and the external data-plane proxy of the [Apache Dubbo Kubernetes](https://github.com/apache/dubbo-kubernetes) project. It consumes control-plane configuration from `dubbod` as a router xDS client.

It also ships an additional proxy gateway runtime for OpenAI-compatible LLM routing, MCP tool routing and federation, A2A forwarding, reusable policies, richer metrics, and Kubernetes CRD-driven configuration. The original Dubbo Gateway API/xDS path remains the default path.

## Features

- **Dubbo Gateway API data plane** — HTTP routing by host, path, and header with weighted clusters, driven by `dubbod` over xDS (listeners, clusters, endpoints, TLS secrets).
- **gRPC / Dubbo Triple / HTTP/2** — end-to-end HTTP/2 pass-through with streaming bodies and trailer propagation; gRPC and Triple requests (detected by content-type) are proxied over HTTP/2 automatically, and `http2: true` on a cluster forces h2c/ALPN h2 for plain HTTP upstreams.
- **Upstream TLS** — plaintext, simple TLS, and Dubbo mutual TLS (certificates via xDS SDS or file-watcher bootstrap).
- **LLM routing** — OpenAI-compatible `/v1/*` routing with model-aware backend selection and streaming (SSE) pass-through.
- **MCP routing and federation** — `mcp-session-id` session-to-backend binding and federated `tools/list` across multiple MCP backends.
- **A2A forwarding** — agent-card and A2A endpoint routing.
- **Reusable policies** — API-key / JWT auth, rate limiting, retries, timeouts, request/response header transforms, body-size limits, allow/deny.
- **Resilience** — per-cluster circuit breakers and retry with failover across weighted backends.
- **Observability** — Prometheus metrics, W3C trace propagation with OTLP export, structured access logs, and a built-in admin UI.
- **Three config sources** — xDS from `dubbod` (default), static YAML file (optionally watched), and Kubernetes CRDs (`Dxgate`, `DxgateBackend`, `DxgateRoute`, `DxgatePolicy`).

## License

Apache License 2.0, see [LICENSE](https://github.com/kdubbo/dxgate/blob/main/LICENSE).
