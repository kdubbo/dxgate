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
