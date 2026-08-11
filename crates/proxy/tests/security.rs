use axum::http::{Request, StatusCode};
use axum::routing::any;
use axum::Router;
use dxgate_core::{
    AuthorizationAction, AuthorizationCondition, AuthorizationPolicy, AuthorizationRule,
    AuthorizationSource, Cluster, Endpoint, JwtHeader, JwtProvider, Listener, ListenerProtocol,
    ListenerSecurity, PathMatch, Route, RouteMatch, RuntimeConfig, VirtualHost, WeightedCluster,
};
use dxgate_proxy::{ProxyServer, ProxyState};
use hyper::body::{self, Body};
use hyper::Client;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::json;
use std::net::{SocketAddr, TcpListener};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

const JWT_SECRET: &[u8] = b"01234567890123456789012345678901";

#[tokio::test]
async fn jwt_identity_and_claim_authorization_are_enforced() {
    let backend_addr = unused_addr();
    let backend = tokio::spawn(async move {
        let app = Router::new().fallback(any(|| async { "ok" }));
        axum::Server::bind(&backend_addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    let proxy_addr = unused_addr();
    let state = ProxyState::new();
    state.apply_config(config(backend_addr)).unwrap();
    let proxy = tokio::spawn(ProxyServer::new(state).serve(proxy_addr));
    wait_until_accepting(proxy_addr).await;

    assert_eq!(request(proxy_addr, None).await.0, StatusCode::FORBIDDEN);
    assert_eq!(
        request(proxy_addr, Some("not-a-jwt")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(proxy_addr, Some(&token("alice", "orders"))).await,
        (StatusCode::OK, "ok".into())
    );
    assert_eq!(
        request(proxy_addr, Some(&token("alice", "other"))).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(proxy_addr, Some(&token("bob", "orders"))).await.0,
        StatusCode::FORBIDDEN
    );

    proxy.abort();
    backend.abort();
}

fn token(subject: &str, group: &str) -> String {
    encode(
        &Header {
            alg: Algorithm::HS256,
            kid: Some("test".into()),
            ..Header::default()
        },
        &json!({
            "iss":"https://issuer.example",
            "sub":subject,
            "aud":"dxgate",
            "exp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600,
            "groups":[group]
        }),
        &EncodingKey::from_secret(JWT_SECRET),
    )
    .unwrap()
}

async fn request(addr: SocketAddr, token: Option<&str>) -> (StatusCode, String) {
    let mut builder = Request::builder()
        .method("GET")
        .uri(format!("http://{addr}/orders/42"));
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let response = Client::new()
        .request(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = body::to_bytes(response.into_body()).await.unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

fn config(backend: SocketAddr) -> RuntimeConfig {
    RuntimeConfig {
        version: "security-e2e".into(),
        listeners: vec![Listener {
            name: "http".into(),
            bind: "0.0.0.0:80".parse().unwrap(),
            protocol: ListenerProtocol::Http,
            virtual_hosts: vec![VirtualHost {
                name: "wildcard".into(),
                domains: vec!["*".into()],
                routes: vec![Route {
                    name: "orders".into(),
                    matches: vec![RouteMatch {
                        path: PathMatch::Prefix("/".into()),
                        headers: vec![],
                    }],
                    weighted_clusters: vec![WeightedCluster {
                        name: "backend".into(),
                        weight: 100,
                    }],
                }],
            }],
            tls_secret: None,
            security: ListenerSecurity {
                jwt_providers: vec![JwtProvider {
                    issuer: "https://issuer.example".into(),
                    audiences: vec!["dxgate".into()],
                    jwks_uri: String::new(),
                    jwks: r#"{"keys":[{"kty":"oct","alg":"HS256","kid":"test","k":"MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE"}]}"#.into(),
                    from_headers: vec![JwtHeader {
                        name: "authorization".into(),
                        prefix: "Bearer ".into(),
                    }],
                    from_params: vec![],
                }],
                authorization: vec![AuthorizationPolicy {
                    action: AuthorizationAction::Allow,
                    rules: vec![AuthorizationRule {
                        sources: vec![AuthorizationSource {
                            request_principals: vec![
                                "https://issuer.example/alice".into(),
                            ],
                            principals: vec![],
                        }],
                        when: vec![AuthorizationCondition {
                            key: "request.auth.claims[groups]".into(),
                            values: vec!["orders".into()],
                            not_values: vec![],
                        }],
                    }],
                }],
            },
        }],
        clusters: vec![Cluster {
            name: "backend".into(),
            endpoints: vec![Endpoint {
                address: backend.ip().to_string(),
                port: backend.port(),
                healthy: true,
                node_name: None,
            }],
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

fn unused_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

async fn wait_until_accepting(addr: SocketAddr) {
    for _ in 0..120 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("proxy did not start on {addr}");
}
