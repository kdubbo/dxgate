# Contributing to dxgate

Thanks for your interest in contributing. This document covers the local setup
and the checks your change must pass before it can be merged.

## Prerequisites

- Rust `1.76+` (the pinned toolchain is in `toolchain.toml`; `rustup` picks it up
  automatically).
- `protobuf-compiler` (`protoc`) — required to build the `xds` crate.

```bash
# Debian/Ubuntu
sudo apt-get install -y protobuf-compiler libprotobuf-dev
# macOS
brew install protobuf
```

## Build & Test

```bash
cargo build --workspace
cargo test --workspace
```

## Required Checks

CI runs the following; run them locally before opening a PR to get fast feedback:

```bash
cargo fmt --all --check          # formatting
cargo clippy --workspace --all-targets -- -D warnings   # lints (warnings are errors)
cargo test --workspace           # unit + integration tests
cargo deny check                 # license + advisory + source gating (cargo install cargo-deny)
```

## Pull Requests

- Keep changes focused; one logical change per PR.
- Add or update tests for behavioral changes. Integration tests live in
  `crates/proxy/tests/` (e.g. `grpc.rs`, `llm.rs`, `mcp.rs`, `a2a.rs`).
- Update `CHANGELOG.md` under the `[Unreleased]` section.
- Ensure `cargo fmt` and `cargo clippy` are clean — CI rejects any warnings.

## Commit Messages

Use short, imperative summaries (e.g. "Add circuit breaker for weighted
clusters"). Reference issues where relevant.

## Project Layout

| Crate | Responsibility |
|-------|----------------|
| `core` | Shared config model, identity, errors |
| `xds` | xDS client and generated protobuf types |
| `proxy` | Data-plane: routing, TLS, LLM/MCP/A2A, policies |
| `controller` | Kubernetes CRD controller |
| `admin` | Admin UI / API |
| `app` | Binary entrypoint wiring the runtime together |
