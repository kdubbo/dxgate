use async_trait::async_trait;
use dxgate_core::{DxgateError, Result, RuntimeConfig};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapConfig {
    #[serde(default)]
    pub xds_address: Option<String>,
    #[serde(default)]
    pub http_addr: Option<SocketAddr>,
    #[serde(default)]
    pub admin_addr: Option<SocketAddr>,
    #[serde(default)]
    pub listener_names: Vec<String>,
    #[serde(default)]
    pub pod_name: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub pod_ip: Option<String>,
    #[serde(default)]
    pub node_name: Option<String>,
    #[serde(default)]
    pub cluster_id: Option<String>,
    #[serde(default)]
    pub dns_domain: Option<String>,
}

impl BootstrapConfig {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let raw = fs::read_to_string(&path).await?;
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            serde_json::from_str(&raw).map_err(|e| DxgateError::InvalidConfig(e.to_string()))
        } else {
            serde_yaml::from_str(&raw).map_err(|e| DxgateError::InvalidConfig(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BootstrapConfig;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::fs;

    fn temp_file(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("dxgate-{name}-{}-{nanos}", std::process::id()))
    }

    #[tokio::test]
    async fn bootstrap_config_loads_json() {
        let path = temp_file("bootstrap.json");
        fs::write(
            &path,
            r#"{
  "xds_address": "http://dubbod.dubbo-system.svc:15012",
  "listener_names": ["public-dubbo.app.svc.cluster.local:80"],
  "cluster_id": "Kubernetes",
  "dns_domain": "cluster.local"
}"#,
        )
        .await
        .unwrap();

        let cfg = BootstrapConfig::load(&path).await.unwrap();
        fs::remove_file(&path).await.unwrap();

        assert_eq!(
            cfg.xds_address.as_deref(),
            Some("http://dubbod.dubbo-system.svc:15012")
        );
        assert_eq!(
            cfg.listener_names,
            ["public-dubbo.app.svc.cluster.local:80"]
        );
        assert_eq!(cfg.cluster_id.as_deref(), Some("Kubernetes"));
        assert_eq!(cfg.dns_domain.as_deref(), Some("cluster.local"));
    }

    #[tokio::test]
    async fn bootstrap_config_loads_yaml() {
        let path = temp_file("bootstrap.yaml");
        fs::write(
            &path,
            "xds_address: http://dubbod.dubbo-system.svc:15012\nhttp_addr: 0.0.0.0:8080\n",
        )
        .await
        .unwrap();

        let cfg = BootstrapConfig::load(&path).await.unwrap();
        fs::remove_file(&path).await.unwrap();

        assert_eq!(
            cfg.xds_address.as_deref(),
            Some("http://dubbod.dubbo-system.svc:15012")
        );
        assert_eq!(cfg.http_addr.unwrap().port(), 8080);
    }
}
