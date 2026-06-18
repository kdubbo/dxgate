<p align="center">
  <img src="./logo/dxgate-logo.svg" alt="dxgate logo" width="260">
</p>

![license](https://img.shields.io/badge/license-Apache--2.0-green.svg)
![Rust: 1.76+](https://img.shields.io/badge/Rust-1.76%2B-orange.svg?style=flat)

dxgate is the delegated gateway for Dubbo Gateway API traffic. It serves as dubbod external data-plane proxy and consumes control-plane configuration as a router xDS client.

It also includes an additional proxy gateway runtime for OpenAI compatibility with LLM routing, MCP tool routing and federation, A2A forwarding, reusable policies, richer metrics, and Kubernetes CRD driver configuration. The original Dubbo gateway API/xDS path remains the default path.