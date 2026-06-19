<p align="center">
  <img src="./logo/dxgate-logo.svg" alt="dxgate logo" width="260">
</p>

<p align="center">
  <img src="https://img.shields.io/badge/license-Apache--2.0-green.svg" alt="license">
  <img src="https://img.shields.io/badge/Rust-1.76%2B-orange.svg?style=flat" alt="Rust: 1.76+">
</p>

dxgate is the delegated gateway for Dubbo Gateway API traffic. It serves as dubbod external data-plane proxy and consumes control-plane configuration as a router xDS client.

It also includes an additional proxy gateway runtime for OpenAI compatibility with LLM routing, MCP tool routing and federation, A2A forwarding, reusable policies, richer metrics, and Kubernetes CRD driver configuration. The original Dubbo gateway API/xDS path remains the default path.

Local static config mode:

```bash
cargo run --bin dxgate -- \
  --http-addr 127.0.0.1:18080 \
  --admin-addr 127.0.0.1:18081 \
  --static-config examples/agent-runtime.yaml
```

When `--static-config` is used without listener names, dxgate does not start the xDS client by default. Set `--xds-enabled true` or `DXGATE_XDS_ENABLED=true` to run static config and xDS together.

Kubernetes controller mode:

```bash
cargo run --bin dxgate -- --print-crds | kubectl apply -f -
kubectl apply -f examples/rbac.yaml
kubectl apply -f examples/dxgate.yaml
kubectl apply -f examples/crds.yaml
DXGATE_MODE=all DXGATE_OTEL_ENDPOINT=http://otel-collector:4317 cargo run --bin dxgate
```
