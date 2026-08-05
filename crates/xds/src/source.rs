use async_trait::async_trait;
use dxgate_core::{ConfigStore, DxgateError, Result, RuntimeConfig, SourceId, SourceState};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::time;
use tracing::{error, info, warn};

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

    pub fn path(&self) -> &std::path::Path {
        &self.path
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

/// How often the file watcher restats the configuration file.
const CONFIG_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// The static-file configuration source.
///
/// A file is a state-of-the-world source: it says what exists, never what was
/// removed. [`SourceState`] closes that gap by diffing each load against the
/// previous one, so a resource deleted from the file is retired from the store
/// instead of lingering forever. Only the [`SourceId::Static`] slice is touched;
/// resources owned by xDS or the Kubernetes controller are left alone.
pub struct StaticConfigSource {
    file: StaticConfigFile,
    store: Arc<ConfigStore>,
    published: SourceState,
    last_modified: Option<SystemTime>,
}

impl StaticConfigSource {
    pub fn new(path: impl Into<PathBuf>, store: Arc<ConfigStore>) -> Self {
        Self {
            file: StaticConfigFile::new(path),
            store,
            published: SourceState::new(),
            last_modified: None,
        }
    }

    /// Loads the file and applies it. Returns whether the store changed.
    pub async fn reload(&mut self) -> Result<bool> {
        let Some(config) = self.file.load().await? else {
            return Ok(false);
        };
        let version = config.version.clone();
        let delta = self.published.reconcile(config);
        let removes = delta.removes.len();
        let outcome = self.store.apply(SourceId::Static, delta);
        for rejected in &outcome.rejected {
            warn!(kind = %rejected.kind, message = %rejected.message, "static config resource rejected");
        }
        if outcome.changed {
            info!(
                path = %self.file.path().display(),
                revision = outcome.revision,
                version = %version,
                removes,
                ready = outcome.ready,
                "applied static config"
            );
        }
        Ok(outcome.changed)
    }

    /// Polls the file's modification time and reloads it when it moves. Runs
    /// until the task is dropped.
    pub async fn watch(mut self) {
        loop {
            match fs::metadata(self.file.path())
                .await
                .and_then(|meta| meta.modified())
            {
                Ok(modified) => {
                    if self.last_modified != Some(modified) {
                        self.last_modified = Some(modified);
                        if let Err(err) = self.reload().await {
                            error!(%err, path = %self.file.path().display(), "failed reloading static config");
                        }
                    }
                }
                Err(err) => {
                    error!(%err, path = %self.file.path().display(), "failed checking static config")
                }
            }
            time::sleep(CONFIG_POLL_INTERVAL).await;
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapConfig {
    #[serde(default)]
    pub xds_address: Option<String>,
    #[serde(default)]
    pub http_addr: Option<SocketAddr>,
    #[serde(default)]
    pub ui_addr: Option<SocketAddr>,
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
    use super::{BootstrapConfig, RuntimeConfigSource, StaticConfigFile};
    use std::path::PathBuf;
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

    #[tokio::test]
    async fn static_config_loads_config_example() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/config.yaml");
        let cfg = StaticConfigFile::new(path).load().await.unwrap().unwrap();

        cfg.validate().unwrap();
        assert_eq!(cfg.providers.len(), 1);
        assert_eq!(cfg.backends.len(), 4);
        assert_eq!(cfg.routes.len(), 3);
        assert_eq!(cfg.policies.len(), 1);
    }
}
