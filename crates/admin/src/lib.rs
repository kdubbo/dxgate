use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use kgate_proxy::ProxyState;
use serde::Serialize;
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize)]
pub struct BuildInfo {
    pub name: &'static str,
    pub version: &'static str,
}

#[derive(Clone)]
pub struct AdminServer {
    state: ProxyState,
    build: BuildInfo,
}

impl AdminServer {
    pub fn new(state: ProxyState) -> Self {
        Self {
            state,
            build: BuildInfo {
                name: "kgate",
                version: env!("CARGO_PKG_VERSION"),
            },
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> std::io::Result<()> {
        let app = Router::new()
            .route("/healthz", get(healthz))
            .route("/readyz", get(readyz))
            .route("/metrics", get(metrics))
            .route("/debug/config", get(debug_config))
            .route("/debug/routes", get(debug_routes))
            .route("/debug/clusters", get(debug_clusters))
            .with_state(self);

        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .map_err(std::io::Error::other)
    }
}

async fn healthz(State(admin): State<AdminServer>) -> Json<BuildInfo> {
    Json(admin.build)
}

async fn readyz(State(admin): State<AdminServer>) -> Response {
    let readiness = admin.state.readiness().await;
    let status = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(readiness)).into_response()
}

async fn metrics(State(admin): State<AdminServer>) -> String {
    let readiness = admin.state.readiness().await;
    format!(
        "# HELP kgate_ready Whether kgate has accepted runtime config\n# TYPE kgate_ready gauge\nkgate_ready {}\n# HELP kgate_config_conflicts Current rejected config conflicts\n# TYPE kgate_config_conflicts gauge\nkgate_config_conflicts {}\n",
        if readiness.ready { 1 } else { 0 },
        readiness.conflicts.len()
    )
}

async fn debug_config(State(admin): State<AdminServer>) -> Json<kgate_core::RuntimeConfig> {
    Json(admin.state.config().await)
}

async fn debug_routes(State(admin): State<AdminServer>) -> Json<serde_json::Value> {
    let cfg = admin.state.config().await;
    let routes: Vec<_> = cfg
        .listeners
        .iter()
        .flat_map(|listener| {
            listener.virtual_hosts.iter().flat_map(move |host| {
                host.routes.iter().map(move |route| {
                    serde_json::json!({
                        "listener": listener.name,
                        "virtualHost": host.name,
                        "route": route.name,
                        "domains": host.domains,
                        "weightedClusters": route.weighted_clusters,
                    })
                })
            })
        })
        .collect();
    Json(serde_json::json!(routes))
}

async fn debug_clusters(State(admin): State<AdminServer>) -> Json<serde_json::Value> {
    let cfg = admin.state.config().await;
    Json(serde_json::json!(cfg.clusters))
}
