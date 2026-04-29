use thiserror::Error;

#[derive(Debug, Error)]
pub enum DxgateError {
    #[error("invalid runtime config: {0}")]
    InvalidConfig(String),

    #[error("route not found for host={host} path={path}")]
    RouteNotFound { host: String, path: String },

    #[error("cluster {0} not found")]
    ClusterNotFound(String),

    #[error("cluster {0} has no healthy endpoints")]
    NoHealthyEndpoints(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, DxgateError>;
