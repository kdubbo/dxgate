# Security Policy

dxgate is a network-facing data-plane proxy that terminates TLS and enforces
authentication, so we take security reports seriously.

## Supported Versions

Security fixes are applied to the `main` branch. Until a formal release cadence
is established, users should track `main` for the latest fixes.

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please report privately via GitHub Security Advisories:

1. Go to the [Security tab](https://github.com/kdubbo/dxgate/security/advisories/new).
2. Provide a description, affected versions, reproduction steps, and impact.

We aim to acknowledge reports within **72 hours** and to provide a remediation
timeline after triage. Please allow us reasonable time to release a fix before
any public disclosure.

## Scope

In scope:

- The `dxgate` data-plane proxy (routing, TLS termination, auth, rate limiting).
- The xDS client and Kubernetes controller.
- The admin interface.

Out of scope:

- Vulnerabilities in upstream dependencies already tracked by RustSec (report to
  the upstream project; we pick these up via `cargo deny` / `cargo audit` in CI).
- Denial-of-service via unrealistic resource limits on self-hosted deployments.
