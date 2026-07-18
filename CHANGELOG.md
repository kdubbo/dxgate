# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project aims
to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `SECURITY.md` describing the private vulnerability reporting process.
- `CONTRIBUTING.md` with local setup and required-checks documentation.
- `deny.toml` and a CI `supply-chain` job running `cargo deny` (license,
  advisory, and source gating).
- CI jobs verifying the pinned MSRV (Rust 1.76) and building the Docker image.

### Changed

- Split the `proxy` crate's monolithic `server.rs` (2908 lines) into a directory
  module: extracted `server/upstream.rs` (HTTP clients + data-plane mTLS/cert
  loading), `server/trace.rs` (W3C trace-context propagation), and
  `server/access_log.rs` (access-log config + line formatting). No behavioral
  change; all tests pass.
