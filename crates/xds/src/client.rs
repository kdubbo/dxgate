use dxgate_core::{RouterIdentity, RuntimeConfig};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use tokio::time;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info};

#[derive(Debug, Error)]
pub enum XdsError {
    #[error("invalid xDS endpoint {endpoint}: {source}")]
    InvalidEndpoint {
        endpoint: String,
        source: tonic::transport::Error,
    },

    #[error("failed connecting to xDS endpoint {endpoint}: {source}")]
    Connect {
        endpoint: String,
        source: tonic::transport::Error,
    },
}

#[derive(Debug, Clone)]
pub struct XdsClientConfig {
    pub endpoint: String,
    pub identity: RouterIdentity,
    pub reconnect_delay: Duration,
}

pub struct XdsClient {
    cfg: XdsClientConfig,
}

impl XdsClient {
    pub fn new(cfg: XdsClientConfig) -> Self {
        Self { cfg }
    }

    pub async fn connect_channel(&self) -> Result<Channel, XdsError> {
        let endpoint = Endpoint::from_shared(self.cfg.endpoint.clone()).map_err(|source| {
            XdsError::InvalidEndpoint {
                endpoint: self.cfg.endpoint.clone(),
                source,
            }
        })?;

        endpoint
            .connect()
            .await
            .map_err(|source| XdsError::Connect {
                endpoint: self.cfg.endpoint.clone(),
                source,
            })
    }

    pub async fn run(self, config_tx: watch::Sender<RuntimeConfig>) -> Result<(), XdsError> {
        loop {
            let channel = self.connect_channel().await?;
            info!(
                node_id = %self.cfg.identity.node_id(),
                endpoint = %self.cfg.endpoint,
                "connected dxgate router to dubbod xDS endpoint"
            );

            // The concrete ADS generated client is intentionally isolated here. The next
            // step is wiring kdubbo xDS protobufs without allowing Kubernetes controller
            // code into dxgate.
            drop(channel);
            debug!(version = %config_tx.borrow().version, "retaining current runtime config");
            time::sleep(self.cfg.reconnect_delay).await;
        }
    }
}
