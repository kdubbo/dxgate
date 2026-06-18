use dxgate_core::{Backend, ConfigConflict, Policy, RuntimeConfig};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::ListParams;
use kube::{Api, Client, CustomResource, CustomResourceExt};
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::watch;
use tokio::time::{self, Duration};
use tracing::{info, warn};

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "gateway.dxgate.io",
    version = "v1alpha1",
    kind = "Dxgate",
    plural = "dxgates",
    namespaced,
    status = "DxgateStatus"
)]
pub struct DxgateSpec {
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub listeners: Vec<Value>,
    #[serde(default)]
    pub clusters: Vec<Value>,
    #[serde(default)]
    pub secrets: Vec<Value>,
    #[serde(default)]
    pub providers: Vec<Value>,
    #[serde(default)]
    pub poll_seconds: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct DxgateStatus {
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "gateway.dxgate.io",
    version = "v1alpha1",
    kind = "DxgateBackend",
    plural = "dxgatebackends",
    namespaced,
    status = "DxgateStatus"
)]
pub struct DxgateBackendSpec {
    pub backend: Value,
}

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "gateway.dxgate.io",
    version = "v1alpha1",
    kind = "DxgateRoute",
    plural = "dxgateroutes",
    namespaced,
    status = "DxgateStatus"
)]
pub struct DxgateRouteSpec {
    pub route: Value,
}

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "gateway.dxgate.io",
    version = "v1alpha1",
    kind = "DxgatePolicy",
    plural = "dxgatepolicies",
    namespaced,
    status = "DxgateStatus"
)]
pub struct DxgatePolicySpec {
    pub policy: Value,
}

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("kubernetes client error: {0}")]
    Kube(#[from] kube::Error),

    #[error("runtime config conversion failed in {field}: {source}")]
    Convert {
        field: &'static str,
        source: serde_json::Error,
    },

    #[error("runtime config rejected: {0:?}")]
    InvalidConfig(Vec<ConfigConflict>),

    #[error("runtime config watcher is closed")]
    RuntimeConfigClosed,
}

pub fn crds() -> Vec<CustomResourceDefinition> {
    vec![
        Dxgate::crd(),
        DxgateBackend::crd(),
        DxgateRoute::crd(),
        DxgatePolicy::crd(),
    ]
}

pub async fn run_controller(
    config_tx: watch::Sender<RuntimeConfig>,
) -> Result<(), ControllerError> {
    let client = Client::try_default().await?;
    info!("started dxgate Kubernetes controller");

    loop {
        match load_runtime_config(client.clone()).await {
            Ok(cfg) => {
                let version = cfg.version.clone();
                if cfg != *config_tx.borrow() {
                    config_tx
                        .send(cfg)
                        .map_err(|_| ControllerError::RuntimeConfigClosed)?;
                    info!(%version, "applied Kubernetes dxgate runtime config");
                }
            }
            Err(err) => warn!(%err, "failed reconciling Kubernetes dxgate config"),
        }
        time::sleep(Duration::from_secs(10)).await;
    }
}

pub async fn load_runtime_config(client: Client) -> Result<RuntimeConfig, ControllerError> {
    let params = ListParams::default();
    let dxgates = Api::<Dxgate>::all(client.clone())
        .list(&params)
        .await?
        .items;
    let backends = Api::<DxgateBackend>::all(client.clone())
        .list(&params)
        .await?
        .items;
    let routes = Api::<DxgateRoute>::all(client.clone())
        .list(&params)
        .await?
        .items;
    let policies = Api::<DxgatePolicy>::all(client).list(&params).await?.items;

    runtime_config_from_resources(&dxgates, &backends, &routes, &policies)
}

pub fn runtime_config_from_resources(
    dxgates: &[Dxgate],
    backend_resources: &[DxgateBackend],
    route_resources: &[DxgateRoute],
    policy_resources: &[DxgatePolicy],
) -> Result<RuntimeConfig, ControllerError> {
    let base = dxgates.first().map(|resource| &resource.spec);
    let mut cfg = RuntimeConfig {
        version: base
            .and_then(|spec| spec.version.clone())
            .unwrap_or_else(|| "kubernetes".to_string()),
        listeners: parse_values(
            base.map(|spec| spec.listeners.as_slice()).unwrap_or(&[]),
            "listeners",
        )?,
        clusters: parse_values(
            base.map(|spec| spec.clusters.as_slice()).unwrap_or(&[]),
            "clusters",
        )?,
        secrets: parse_values(
            base.map(|spec| spec.secrets.as_slice()).unwrap_or(&[]),
            "secrets",
        )?,
        providers: parse_values(
            base.map(|spec| spec.providers.as_slice()).unwrap_or(&[]),
            "providers",
        )?,
        backends: backend_resources
            .iter()
            .map(|resource| parse_value::<Backend>(&resource.spec.backend, "backends"))
            .collect::<Result<Vec<_>, _>>()?,
        routes: route_resources
            .iter()
            .map(|resource| parse_value::<dxgate_core::AgentRoute>(&resource.spec.route, "routes"))
            .collect::<Result<Vec<_>, _>>()?,
        policies: policy_resources
            .iter()
            .map(|resource| parse_value::<Policy>(&resource.spec.policy, "policies"))
            .collect::<Result<Vec<_>, _>>()?,
    };
    cfg.listeners.sort_by(|a, b| a.name.cmp(&b.name));
    cfg.clusters.sort_by(|a, b| a.name.cmp(&b.name));
    cfg.providers.sort_by(|a, b| a.name.cmp(&b.name));
    cfg.backends.sort_by(|a, b| a.name.cmp(&b.name));
    cfg.routes.sort_by(|a, b| a.name.cmp(&b.name));
    cfg.policies.sort_by(|a, b| a.name.cmp(&b.name));
    cfg.validate().map_err(ControllerError::InvalidConfig)?;
    Ok(cfg)
}

fn parse_values<T: DeserializeOwned>(
    values: &[Value],
    field: &'static str,
) -> Result<Vec<T>, ControllerError> {
    values
        .iter()
        .map(|value| parse_value(value, field))
        .collect::<Result<Vec<_>, _>>()
}

fn parse_value<T: DeserializeOwned>(
    value: &Value,
    field: &'static str,
) -> Result<T, ControllerError> {
    serde_json::from_value(value.clone())
        .map_err(|source| ControllerError::Convert { field, source })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dxgate_core::{
        AgentProtocol, AgentRoute, AgentRouteMatch, BackendKind, PathMatch, PolicyAction,
        ProviderKind, WeightedBackend,
    };
    use serde_json::json;

    #[test]
    fn converts_crd_resources_to_runtime_config() {
        let dxgate = Dxgate::new(
            "default",
            DxgateSpec {
                version: Some("crd-test".into()),
                listeners: vec![],
                clusters: vec![],
                secrets: vec![],
                providers: vec![json!({
                    "name": "openai",
                    "kind": "open-ai-compatible",
                    "base_url": "http://llm.local",
                    "api_key_env": "OPENAI_API_KEY"
                })],
                poll_seconds: None,
            },
        );
        let backend = DxgateBackend::new(
            "gpt",
            DxgateBackendSpec {
                backend: serde_json::to_value(Backend {
                    name: "gpt".into(),
                    kind: BackendKind::Llm {
                        provider: "openai".into(),
                        models: vec!["gpt-test".into()],
                        endpoint: None,
                    },
                    policies: vec![],
                })
                .unwrap(),
            },
        );
        let route = DxgateRoute::new(
            "chat",
            DxgateRouteSpec {
                route: serde_json::to_value(AgentRoute {
                    name: "chat".into(),
                    protocol: AgentProtocol::Llm,
                    matches: vec![AgentRouteMatch {
                        path: PathMatch::Exact("/v1/chat/completions".into()),
                        host: None,
                        method: Some("POST".into()),
                        model: Some("gpt-test".into()),
                        tool: None,
                        agent: None,
                        headers: vec![],
                    }],
                    weighted_backends: vec![WeightedBackend {
                        name: "gpt".into(),
                        weight: 100,
                    }],
                    policies: vec![],
                })
                .unwrap(),
            },
        );
        let policy = DxgatePolicy::new(
            "noop",
            DxgatePolicySpec {
                policy: json!({
                    "name": "noop",
                    "action": "allow",
                    "request_headers": { "add": [], "remove": [] },
                    "response_headers": { "add": [], "remove": [] }
                }),
            },
        );

        let cfg =
            runtime_config_from_resources(&[dxgate], &[backend], &[route], &[policy]).unwrap();

        assert_eq!(cfg.version, "crd-test");
        assert_eq!(cfg.providers[0].kind, ProviderKind::OpenAiCompatible);
        assert_eq!(cfg.backends[0].name, "gpt");
        assert_eq!(cfg.routes[0].name, "chat");
        assert_eq!(cfg.policies[0].action, PolicyAction::Allow);
    }

    #[test]
    fn rejects_invalid_crd_runtime_config() {
        let route = DxgateRoute::new(
            "broken",
            DxgateRouteSpec {
                route: json!({
                    "name": "broken",
                    "protocol": "llm",
                    "weighted_backends": [{ "name": "missing", "weight": 100 }]
                }),
            },
        );

        let err = runtime_config_from_resources(&[], &[], &[route], &[]).unwrap_err();

        assert!(matches!(err, ControllerError::InvalidConfig(_)));
    }

    #[test]
    fn exposes_all_crd_definitions() {
        let crds = crds();

        assert_eq!(crds.len(), 4);
        assert!(crds
            .iter()
            .any(|crd| crd.metadata.name.as_deref() == Some("dxgates.gateway.dxgate.io")));
    }
}
