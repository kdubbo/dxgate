use axum::body::Body;
use axum::http::{Request, StatusCode, Uri, Version};
use axum::routing::any;
use axum::Router;
use dxgate_core::{
    Cluster, Endpoint, Listener, ListenerProtocol, PathMatch, Route, RouteMatch, RuntimeConfig,
    VirtualHost, WeightedCluster,
};
use dxgate_proxy::{ProxyServer, ProxyState};
use hyper::body;
use hyper::Client;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

struct TestServer {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_unary_and_streaming_pass_through_the_proxy() {
    let upstream = spawn_grpc_health_server().await;
    let proxy = spawn_proxy(upstream.addr, false).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", proxy.addr))
        .unwrap()
        .connect()
        .await
        .expect("gRPC client connects through the proxy");
    let mut client = HealthClient::new(channel);

    let response = client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("unary gRPC call succeeds through the proxy");
    assert_eq!(response.into_inner().status(), ServingStatus::Serving);

    let mut stream = client
        .watch(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("server-streaming gRPC call succeeds through the proxy")
        .into_inner();
    let first = stream
        .message()
        .await
        .expect("streamed message arrives through the proxy")
        .expect("stream yields at least one message");
    assert_eq!(first.status(), ServingStatus::Serving);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_error_status_propagates_in_trailers() {
    let upstream = spawn_grpc_health_server().await;
    let proxy = spawn_proxy(upstream.addr, false).await;

    let channel = tonic::transport::Channel::from_shared(format!("http://{}", proxy.addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = HealthClient::new(channel);

    let err = client
        .check(HealthCheckRequest {
            service: "missing-service".into(),
        })
        .await
        .expect_err("unknown service must surface a gRPC error status");
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_http2_flag_forces_h2_upstream_for_plain_http() {
    let upstream = spawn_version_echo_server().await;
    let proxy = spawn_proxy(upstream.addr, true).await;

    let uri: Uri = format!("http://{}/version", proxy.addr).parse().unwrap();
    let response = Client::new().get(uri).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = body::to_bytes(response.into_body()).await.unwrap();
    assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "HTTP/2.0");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2c_downstream_is_normalized_to_http1_upstream() {
    let upstream = spawn_version_echo_server().await;
    let proxy = spawn_proxy(upstream.addr, false).await;

    let client = Client::builder().http2_only(true).build_http::<Body>();
    let request = Request::builder()
        .uri(format!("http://{}/version", proxy.addr))
        .body(Body::empty())
        .unwrap();
    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.version(), Version::HTTP_2);
    let bytes = body::to_bytes(response.into_body()).await.unwrap();
    assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "HTTP/1.1");
}

async fn spawn_grpc_health_server() -> TestServer {
    let listener = TokioTcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (mut reporter, service) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    wait_until_accepting(addr).await;
    TestServer { addr, task }
}

async fn spawn_version_echo_server() -> TestServer {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().fallback(any(|req: Request<Body>| async move {
        format!("{:?}", req.version())
    }));
    let task = tokio::spawn(async move {
        axum::Server::from_tcp(listener)
            .unwrap()
            .serve(app.into_make_service())
            .await
            .unwrap();
    });
    wait_until_accepting(addr).await;
    TestServer { addr, task }
}

async fn spawn_proxy(upstream: SocketAddr, http2: bool) -> TestServer {
    let state = ProxyState::new(RuntimeConfig::empty("bootstrap"));
    state
        .apply_config(runtime_config(upstream, http2))
        .await
        .unwrap();
    let addr = unused_addr();
    let task = tokio::spawn(async move {
        ProxyServer::new(state).serve(addr).await.unwrap();
    });
    wait_until_accepting(addr).await;
    TestServer { addr, task }
}

fn runtime_config(upstream: SocketAddr, http2: bool) -> RuntimeConfig {
    RuntimeConfig {
        version: "grpc-e2e".into(),
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
                        name: "backend".into(),
                        weight: 100,
                    }],
                }],
            }],
            tls_secret: None,
        }],
        clusters: vec![Cluster {
            name: "backend".into(),
            endpoints: vec![Endpoint {
                address: upstream.ip().to_string(),
                port: upstream.port(),
                healthy: true,
                node_name: None,
            }],
            http2,
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
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn wait_until_accepting(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "server at {addr} never started accepting connections"
        );
        sleep(Duration::from_millis(25)).await;
    }
}
