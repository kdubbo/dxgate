// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! On-demand activation through the real request path.
//!
//! The unit tests cover the demand bookkeeping; what these cover is the part
//! that only shows up end to end — that a request for an empty cluster is
//! parked rather than failed, that it resumes on the endpoints an xDS update
//! brings in, and that nothing about this changes what a genuinely dead backend
//! does.

use axum::http::Uri;
use axum::routing::any;
use axum::Router;
use dxgate_core::{
    Cluster, ConfigStore, Endpoint, Listener, ListenerProtocol, PathMatch, Route, RouteMatch,
    RuntimeConfig, VirtualHost, WeightedCluster,
};
use dxgate_proxy::{Activator, ProxyServer, ProxyState};
use hyper::{body, Client};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The cluster name carries the Service identity, so it has to look like a real
/// one: `direction|port|subset|authority`.
const COLD_CLUSTER: &str = "outbound|8080||payment.default.svc.cluster.local";

#[tokio::test]
async fn request_for_a_scaled_to_zero_target_resumes_once_endpoints_arrive() {
    let backend_addr = unused_addr();
    let proxy_addr = unused_addr();
    let backend = spawn_backend(backend_addr);

    let state = ProxyState::with_activator(
        Arc::new(ConfigStore::new()),
        Activator::holding(Duration::from_secs(5), 16),
    );
    // Scaled to zero: the cluster exists and is routable, it just has nowhere to
    // send anything yet. This is exactly what dubbod publishes for a Deployment
    // KEDA has taken to zero replicas.
    state.apply_config(config(vec![])).unwrap();

    let proxy = tokio::spawn({
        let state = state.clone();
        async move {
            ProxyServer::new(state).serve(proxy_addr).await.unwrap();
        }
    });
    wait_until_listening(proxy_addr).await;

    let started = Instant::now();
    let request = tokio::spawn(async move { get(proxy_addr, "/orders").await });

    // The request must still be in flight: if activation were not holding it,
    // it would already have come back 503.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !request.is_finished(),
        "request was failed instead of being held for the cold target"
    );

    // The scale-up lands as an ordinary config update — the activator has no
    // special channel, it is watching the same snapshot the request path reads.
    state
        .apply_config(config(vec![endpoint(backend_addr)]))
        .unwrap();

    let (status, body) = request.await.unwrap();
    assert_eq!(status, 200, "held request should resume, got body: {body}");
    assert!(body.contains("path=/orders"), "unexpected body: {body}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "request resumed only at the hold timeout, so it was not woken by the update"
    );

    proxy.abort();
    backend.abort();
}

#[tokio::test]
async fn request_fails_when_the_target_never_comes_up() {
    let proxy_addr = unused_addr();

    let state = ProxyState::with_activator(
        Arc::new(ConfigStore::new()),
        Activator::holding(Duration::from_millis(400), 16),
    );
    state.apply_config(config(vec![])).unwrap();

    let proxy = tokio::spawn({
        let state = state.clone();
        async move {
            ProxyServer::new(state).serve(proxy_addr).await.unwrap();
        }
    });
    wait_until_listening(proxy_addr).await;

    let started = Instant::now();
    let (status, _) = get(proxy_addr, "/orders").await;
    assert_eq!(
        status, 503,
        "a target that never activates must end in the same 503 as before"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "the hold timeout was not waited out"
    );

    proxy.abort();
}

#[tokio::test]
async fn activation_backlog_rejects_excess_without_dropping_the_held_request() {
    let backend_addr = unused_addr();
    let proxy_addr = unused_addr();
    let backend = spawn_backend(backend_addr);

    let state = ProxyState::with_activator(
        Arc::new(ConfigStore::new()),
        Activator::holding(Duration::from_secs(5), 1),
    );
    state.apply_config(config(vec![])).unwrap();

    let proxy = tokio::spawn({
        let state = state.clone();
        async move {
            ProxyServer::new(state).serve(proxy_addr).await.unwrap();
        }
    });
    wait_until_listening(proxy_addr).await;

    let held = tokio::spawn(async move { get(proxy_addr, "/first").await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!held.is_finished());

    let rejected_at = Instant::now();
    let (status, _) = get(proxy_addr, "/second").await;
    assert_eq!(status, 503);
    assert!(
        rejected_at.elapsed() < Duration::from_secs(1),
        "backlog overflow waited instead of failing fast"
    );

    state
        .apply_config(config(vec![endpoint(backend_addr)]))
        .unwrap();
    let (status, body) = held.await.unwrap();
    assert_eq!(status, 200);
    assert!(body.contains("path=/first"));

    proxy.abort();
    backend.abort();
}

#[tokio::test]
async fn unactivatable_targets_fail_immediately() {
    let proxy_addr = unused_addr();

    let state = ProxyState::with_activator(
        Arc::new(ConfigStore::new()),
        // A hold timeout long enough that waiting it out would be unmistakable
        // in the elapsed time below.
        Activator::holding(Duration::from_secs(30), 16),
    );
    // An external host has no Deployment to scale, so holding a request for it
    // would only add 30s to a failure that is already certain.
    let mut cfg = config(vec![]);
    cfg.clusters[0].name = "outbound|443||api.openai.com".into();
    cfg.listeners[0].virtual_hosts[0].routes[0].weighted_clusters[0].name =
        "outbound|443||api.openai.com".into();
    state.apply_config(cfg).unwrap();

    let proxy = tokio::spawn({
        let state = state.clone();
        async move {
            ProxyServer::new(state).serve(proxy_addr).await.unwrap();
        }
    });
    wait_until_listening(proxy_addr).await;

    let started = Instant::now();
    let (status, _) = get(proxy_addr, "/v1/chat/completions").await;
    assert_eq!(status, 503);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "an unactivatable target was held rather than failed outright"
    );

    proxy.abort();
}

#[tokio::test]
async fn activation_disabled_fails_without_waiting() {
    let proxy_addr = unused_addr();

    let state = ProxyState::with_activator(Arc::new(ConfigStore::new()), Activator::disabled());
    state.apply_config(config(vec![])).unwrap();

    let proxy = tokio::spawn({
        let state = state.clone();
        async move {
            ProxyServer::new(state).serve(proxy_addr).await.unwrap();
        }
    });
    wait_until_listening(proxy_addr).await;

    let started = Instant::now();
    let (status, _) = get(proxy_addr, "/orders").await;
    assert_eq!(status, 503);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "a gateway with activation off must behave exactly as it did before"
    );

    proxy.abort();
}

fn config(endpoints: Vec<Endpoint>) -> RuntimeConfig {
    RuntimeConfig {
        version: "activation".into(),
        listeners: vec![Listener {
            name: "http-80".into(),
            bind: "0.0.0.0:80".parse().unwrap(),
            protocol: ListenerProtocol::Http,
            virtual_hosts: vec![VirtualHost {
                name: "wildcard".into(),
                domains: vec!["*".into()],
                routes: vec![Route {
                    name: "default".into(),
                    matches: vec![RouteMatch {
                        path: PathMatch::Prefix("/".into()),
                        headers: vec![],
                    }],
                    weighted_clusters: vec![WeightedCluster {
                        name: COLD_CLUSTER.into(),
                        weight: 100,
                    }],
                }],
            }],
            tls_secret: None,
            security: Default::default(),
        }],
        clusters: vec![Cluster {
            name: COLD_CLUSTER.into(),
            endpoints,
            http2: false,
            tls: None,
            circuit_breaker: None,
            outlier_detection: None,
        }],
        secrets: vec![],
        providers: vec![],
        backends: vec![],
        routes: vec![],
        policies: vec![],
    }
}

fn endpoint(addr: SocketAddr) -> Endpoint {
    Endpoint {
        address: addr.ip().to_string(),
        port: addr.port(),
        healthy: true,
        node_name: None,
    }
}

async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    let uri: Uri = format!("http://{addr}{path}").parse().unwrap();
    let response = Client::new().get(uri).await.unwrap();
    let status = response.status().as_u16();
    let bytes = body::to_bytes(response.into_body()).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

fn spawn_backend(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
    let app = Router::new().fallback(any(|uri: Uri| async move {
        format!("dxgate example backend path={}", uri.path())
    }));
    tokio::spawn(async move {
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    })
}

/// Waits for the proxy socket to accept, without asserting on the response:
/// every test here starts with a cluster that has no endpoints, so the first
/// successful connection still answers 503.
async fn wait_until_listening(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "proxy {addr} never started");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn unused_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}
