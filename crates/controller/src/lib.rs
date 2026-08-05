//! The Kubernetes CRD source: custom resource definitions, the projection from
//! those resources onto dxgate's configuration model, and the informer that
//! keeps the store in sync with them.

mod informer;

pub use informer::run_controller;

use dxgate_core::{Backend, Policy, RuntimeConfig};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::ListParams;
use kube::{Api, Client, CustomResource, CustomResourceExt};
use schemars::r#gen::SchemaGenerator;
use schemars::schema::{ArrayValidation, InstanceType, Schema, SchemaObject, SingleOrVec};
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(CustomResource, Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    derive = "PartialEq",
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
    #[schemars(schema_with = "raw_object_vec_schema")]
    pub listeners: Vec<Value>,
    #[serde(default)]
    #[schemars(schema_with = "raw_object_vec_schema")]
    pub clusters: Vec<Value>,
    #[serde(default)]
    #[schemars(schema_with = "raw_object_vec_schema")]
    pub secrets: Vec<Value>,
    #[serde(default)]
    #[schemars(schema_with = "raw_object_vec_schema")]
    pub providers: Vec<Value>,
    #[serde(default)]
    pub poll_seconds: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DxgateStatus {
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<DxgateCondition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DxgateCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    pub reason: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

#[derive(CustomResource, Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    derive = "PartialEq",
    group = "gateway.dxgate.io",
    version = "v1alpha1",
    kind = "DxgateBackend",
    plural = "dxgatebackends",
    namespaced,
    status = "DxgateStatus"
)]
pub struct DxgateBackendSpec {
    #[schemars(schema_with = "raw_object_schema")]
    pub backend: Value,
}

#[derive(CustomResource, Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    derive = "PartialEq",
    group = "gateway.dxgate.io",
    version = "v1alpha1",
    kind = "DxgateRoute",
    plural = "dxgateroutes",
    namespaced,
    status = "DxgateStatus"
)]
pub struct DxgateRouteSpec {
    #[schemars(schema_with = "raw_object_schema")]
    pub route: Value,
}

#[derive(CustomResource, Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    derive = "PartialEq",
    group = "gateway.dxgate.io",
    version = "v1alpha1",
    kind = "DxgatePolicy",
    plural = "dxgatepolicies",
    namespaced,
    status = "DxgateStatus"
)]
pub struct DxgatePolicySpec {
    #[schemars(schema_with = "raw_object_schema")]
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
}

fn raw_object_schema(_: &mut SchemaGenerator) -> Schema {
    arbitrary_object_schema()
}

fn raw_object_vec_schema(_: &mut SchemaGenerator) -> Schema {
    let mut schema = SchemaObject {
        instance_type: Some(InstanceType::Array.into()),
        ..SchemaObject::default()
    };
    schema.array = Some(Box::new(ArrayValidation {
        items: Some(SingleOrVec::Single(Box::new(arbitrary_object_schema()))),
        ..ArrayValidation::default()
    }));
    Schema::Object(schema)
}

fn arbitrary_object_schema() -> Schema {
    let mut schema = SchemaObject {
        instance_type: Some(InstanceType::Object.into()),
        ..SchemaObject::default()
    };
    schema.extensions.insert(
        "x-kubernetes-preserve-unknown-fields".to_string(),
        Value::Bool(true),
    );
    Schema::Object(schema)
}

pub fn crds() -> Vec<CustomResourceDefinition> {
    vec![
        Dxgate::crd(),
        DxgateBackend::crd(),
        DxgateRoute::crd(),
        DxgatePolicy::crd(),
    ]
}

/// Reads the dxgate CRDs once and projects them onto a configuration document.
/// Useful for tooling; the running controller uses the informer instead.
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
    // Cross-resource references are deliberately not checked here. A CRD route
    // may legitimately point at a cluster that arrives over xDS, so only the
    // store — which sees every source — can tell a dangling reference from one
    // another source has not delivered yet.
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
                        model_rewrites: Default::default(),
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
    fn rejects_specs_that_do_not_parse() {
        let route = DxgateRoute::new(
            "broken",
            DxgateRouteSpec {
                route: json!({ "name": "broken", "protocol": "carrier-pigeon" }),
            },
        );

        let err = runtime_config_from_resources(&[], &[], &[route], &[]).unwrap_err();

        assert!(matches!(
            err,
            ControllerError::Convert {
                field: "routes",
                ..
            }
        ));
    }

    #[test]
    fn dangling_references_are_left_for_the_store_to_judge() {
        // A route may legitimately name a backend that another source owns, so
        // the projection accepts it and the merged store decides.
        let route = DxgateRoute::new(
            "chat",
            DxgateRouteSpec {
                route: json!({
                    "name": "chat",
                    "protocol": "llm",
                    "weighted_backends": [{ "name": "elsewhere", "weight": 100 }]
                }),
            },
        );

        let cfg = runtime_config_from_resources(&[], &[], &[route], &[]).unwrap();
        assert_eq!(cfg.routes[0].name, "chat");

        let store = dxgate_core::ConfigStore::new();
        let outcome = store.apply(dxgate_core::SourceId::Kubernetes, cfg.into());
        assert!(!outcome.ready);
        assert_eq!(outcome.conflicts[0].kind, "missing-backend");
    }

    #[test]
    fn exposes_all_crd_definitions() {
        let crds = crds();

        assert_eq!(crds.len(), 4);
        assert!(crds
            .iter()
            .any(|crd| crd.metadata.name.as_deref() == Some("dxgates.gateway.dxgate.io")));
    }

    #[test]
    fn builds_ready_status_condition() {
        let dxgate = Dxgate::new(
            "default",
            DxgateSpec {
                version: Some("crd-test".into()),
                listeners: vec![],
                clusters: vec![],
                secrets: vec![],
                providers: vec![],
                poll_seconds: None,
            },
        );

        let status = informer::ready_status(&dxgate, true, "Accepted", "RuntimeConfig accepted");

        assert!(status.ready);
        assert_eq!(status.message.as_deref(), Some("RuntimeConfig accepted"));
        assert_eq!(status.conditions[0].type_, "Ready");
        assert_eq!(status.conditions[0].status, "True");
        assert_eq!(status.conditions[0].reason, "Accepted");
    }
}
