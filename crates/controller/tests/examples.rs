//! The shipped example manifests are parsed by the same code that parses what a
//! cluster sends.
//!
//! Examples rot silently: a field renamed in `dxgate_core` leaves the YAML in
//! `examples/` syntactically fine and semantically dead, and nobody finds out
//! until someone follows the README. Running them through the real projection —
//! and then through the real store — turns that into a build failure.

use dxgate_controller::{
    runtime_config_from_resources, Dxgate, DxgateBackend, DxgatePolicy, DxgateRoute,
};
use dxgate_core::{ConfigDelta, ConfigStore, SourceId};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Default)]
struct Manifests {
    dxgates: Vec<Dxgate>,
    backends: Vec<DxgateBackend>,
    routes: Vec<DxgateRoute>,
    policies: Vec<DxgatePolicy>,
}

fn load(names: &[&str]) -> Manifests {
    let mut manifests = Manifests::default();
    for name in names {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples")
            .join(name);
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));

        for document in serde_yaml::Deserializer::from_str(&raw) {
            let value = serde_yaml::Value::deserialize(document)
                .unwrap_or_else(|err| panic!("parse {}: {err}", path.display()));
            let Some(kind) = value.get("kind").and_then(serde_yaml::Value::as_str) else {
                // The comment-only leading document.
                continue;
            };
            match kind {
                "Dxgate" => manifests.dxgates.push(from_value(&path, kind, value)),
                "DxgateBackend" => manifests.backends.push(from_value(&path, kind, value)),
                "DxgateRoute" => manifests.routes.push(from_value(&path, kind, value)),
                "DxgatePolicy" => manifests.policies.push(from_value(&path, kind, value)),
                other => panic!("{} declares unknown kind {other}", path.display()),
            }
        }
    }
    manifests
}

fn from_value<T: serde::de::DeserializeOwned>(path: &Path, kind: &str, value: serde_yaml::Value) -> T {
    serde_yaml::from_value(value)
        .unwrap_or_else(|err| panic!("{} {kind} does not deserialize: {err}", path.display()))
}

/// Every spec in the examples must survive the projection onto dxgate's
/// configuration model — this is where a renamed or retyped field shows up.
#[test]
fn example_manifests_project_onto_the_configuration_model() {
    let manifests = load(&["agent-route.yaml", "llm-route.yaml"]);

    let config = runtime_config_from_resources(
        &manifests.dxgates,
        &manifests.backends,
        &manifests.routes,
        &manifests.policies,
    )
    .expect("example manifests should project cleanly");

    assert_eq!(config.providers.len(), 1, "one provider is declared");
    assert_eq!(config.backends.len(), 3, "http, llm, and mcp backends");
    assert_eq!(config.routes.len(), 3, "http, llm, and mcp routes");
    assert_eq!(config.policies.len(), 2);
}

/// And the projection must be self-consistent: every backend, provider, and
/// policy an example route names has to exist, or the store reports it and the
/// proxy answers 503.
#[test]
fn example_manifests_leave_no_dangling_references() {
    let manifests = load(&["agent-route.yaml", "llm-route.yaml"]);
    let config = runtime_config_from_resources(
        &manifests.dxgates,
        &manifests.backends,
        &manifests.routes,
        &manifests.policies,
    )
    .expect("example manifests should project cleanly");

    let store = ConfigStore::new();
    let outcome = store.apply(SourceId::Kubernetes, ConfigDelta::from(config));

    assert!(outcome.rejected.is_empty(), "{:?}", outcome.rejected);
    assert!(outcome.conflicts.is_empty(), "{:?}", outcome.conflicts);
    assert!(outcome.ready);
}

/// The HTTP example is the one meant to be runnable end to end, so it must be
/// complete on its own: applying it with `backend.yaml` should route.
#[test]
fn the_http_example_is_self_contained() {
    let manifests = load(&["agent-route.yaml"]);
    let config = runtime_config_from_resources(
        &manifests.dxgates,
        &manifests.backends,
        &manifests.routes,
        &manifests.policies,
    )
    .expect("agent-route.yaml should project cleanly");

    let store = ConfigStore::new();
    let outcome = store.apply(SourceId::Kubernetes, ConfigDelta::from(config));
    assert!(outcome.ready, "{:?}", outcome.problems());

    let snapshot = store.snapshot();
    let route = snapshot
        .agent_routes()
        .iter()
        .find(|route| route.name == "example-http")
        .expect("the example route is published");
    let backend_name = &route.weighted_backends[0].name;
    let backend = snapshot
        .backend(backend_name)
        .expect("the route's backend is published");
    assert_eq!(
        backend.endpoint(None),
        Some("http://dxgate-example-backend.dubbo-system.svc:8080"),
        "the route points at the Service in backend.yaml"
    );
}
