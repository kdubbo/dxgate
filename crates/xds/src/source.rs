use async_trait::async_trait;
use dxgate_core::{Result, RuntimeConfig};
use std::path::PathBuf;
use tokio::fs;

#[async_trait]
pub trait RuntimeConfigSource: Send + Sync {
    async fn load(&self) -> Result<Option<RuntimeConfig>>;
}

#[derive(Debug, Clone)]
pub struct StaticConfigFile {
    path: PathBuf,
}

impl StaticConfigFile {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl RuntimeConfigSource for StaticConfigFile {
    async fn load(&self) -> Result<Option<RuntimeConfig>> {
        let raw = fs::read_to_string(&self.path).await?;
        let cfg = if self.path.extension().and_then(|e| e.to_str()) == Some("json") {
            serde_json::from_str(&raw)
                .map_err(|e| dxgate_core::DxgateError::InvalidConfig(e.to_string()))?
        } else {
            serde_yaml::from_str(&raw)
                .map_err(|e| dxgate_core::DxgateError::InvalidConfig(e.to_string()))?
        };
        Ok(Some(cfg))
    }
}
