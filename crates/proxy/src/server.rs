use crate::ProxyState;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Request, Response, StatusCode, Uri};
use axum::routing::any;
use axum::Router;
use hyper::client::HttpConnector;
use hyper::Client;
use kgate_core::{MatchInput, HTTP_LISTENER_PORT};
use std::net::SocketAddr;
use tracing::{debug, warn};

#[derive(Clone)]
pub struct ProxyServer {
    state: ProxyState,
    client: Client<HttpConnector, Body>,
}

impl ProxyServer {
    pub fn new(state: ProxyState) -> Self {
        Self {
            state,
            client: Client::new(),
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> std::io::Result<()> {
        let app = Router::new().fallback(any(proxy_request)).with_state(self);
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .map_err(std::io::Error::other)
    }
}

async fn proxy_request(State(server): State<ProxyServer>, req: Request<Body>) -> Response<Body> {
    match forward(server, req).await {
        Ok(resp) => resp,
        Err((status, message)) => {
            warn!(status = status.as_u16(), %message, "request failed");
            Response::builder()
                .status(status)
                .body(Body::from(message))
                .unwrap_or_else(|_| Response::new(Body::from("proxy error")))
        }
    }
}

async fn forward(
    server: ProxyServer,
    mut req: Request<Body>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let cfg = server.state.config().await;
    let host = host_header(req.headers()).unwrap_or("*");
    let path = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/")
        .to_string();
    let headers = header_pairs(req.headers());
    let input = MatchInput {
        host,
        path: &path,
        headers: &headers,
    };

    let weighted_clusters = cfg
        .route_for(HTTP_LISTENER_PORT, &input)
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?
        .weighted_clusters
        .clone();

    let weighted = server
        .state
        .pick_cluster(&weighted_clusters)
        .await
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "route has no clusters".to_string(),
            )
        })?;

    let cluster = cfg
        .cluster(&weighted.name)
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("cluster {} not found", weighted.name),
            )
        })?
        .clone();

    let endpoint = server
        .state
        .pick_endpoint(&cluster.name, &cluster.endpoints)
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;

    let upstream_uri = format!("http://{}:{}{}", endpoint.address, endpoint.port, path)
        .parse::<Uri>()
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("invalid upstream uri: {e}"),
            )
        })?;

    debug!(
        cluster = %cluster.name,
        endpoint = %endpoint.address,
        "forwarding request"
    );

    *req.uri_mut() = upstream_uri;
    req.headers_mut().remove(http::header::HOST);

    server
        .client
        .request(req)
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
}

fn host_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(|host| host.split(':').next().unwrap_or(host))
}

fn header_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect()
}
