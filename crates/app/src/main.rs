use clap::Parser;
use dxgate_core::{
    AuthPolicy, ConfigStore, RouterIdentity, SecretKeyReference, DEFAULT_CLUSTER_ID,
    DEFAULT_DNS_DOMAIN,
};
use dxgate_proxy::{ProxyServer, ProxyState};
use dxgate_ui::UiServer;
use dxgate_xds::{BootstrapConfig, XdsClient, XdsClientConfig};
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::Sampler;
use opentelemetry_sdk::Resource;
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time;
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Debug, Parser)]
#[command(name = "dxgate")]
#[command(about = "Pure Rust north-south proxy for Dubbo Gateway API traffic")]
struct Args {
    #[arg(
        long,
        env = "DXGATE_XDS_ADDRESS",
        default_value = "http://dubbod.dubbo-system.svc:15012"
    )]
    xds_address: String,

    #[arg(long, env = "DXGATE_XDS_ENABLED")]
    xds_enabled: Option<bool>,

    #[arg(long, env = "DXGATE_HTTP_ADDR", default_value = "0.0.0.0:80")]
    http_addr: SocketAddr,

    #[arg(long, env = "DXGATE_UI_ADDR", default_value = "0.0.0.0:15021")]
    ui_addr: SocketAddr,

    #[arg(long, env = "DXGATE_METRICS_ENABLED", default_value_t = true)]
    metrics_enabled: bool,

    // Should be <= the pod's terminationGracePeriodSeconds, or Kubernetes SIGKILLs
    // the process mid-drain and the graceful shutdown buys nothing.
    #[arg(long, env = "DXGATE_DRAIN_TIMEOUT_SECONDS", default_value_t = 30)]
    drain_timeout_seconds: u64,

    #[arg(long, env = "DXGATE_BOOTSTRAP")]
    bootstrap: Option<PathBuf>,

    #[arg(long, env = "DXGATE_OTEL_ENDPOINT")]
    otel_endpoint: Option<String>,

    #[arg(long, env = "DXGATE_OTEL_SERVICE_NAME", default_value = "dxgate")]
    otel_service_name: String,

    #[arg(long, env = "DXGATE_OTEL_SAMPLING_PERCENTAGE", default_value_t = 100.0)]
    otel_sampling_percentage: f64,

    #[arg(long, env = "DXGATE_OTEL_TAGS")]
    otel_tags: Option<String>,

    #[arg(long, env = "DXGATE_LISTENER_NAMES", value_delimiter = ',')]
    listener_names: Vec<String>,

    #[arg(long, env = "POD_NAME", default_value = "dxgate")]
    pod_name: String,

    #[arg(long, env = "POD_NAMESPACE", default_value = "dubbo-system")]
    namespace: String,

    #[arg(long, env = "INSTANCE_IP", default_value = "127.0.0.1")]
    pod_ip: String,

    #[arg(long, env = "KUBE_NODE_NAME")]
    node_name: Option<String>,

    #[arg(long, env = "DUBBO_META_CLUSTER_ID", default_value = DEFAULT_CLUSTER_ID)]
    cluster_id: String,

    #[arg(long, env = "DOMAIN_SUFFIX", default_value = DEFAULT_DNS_DOMAIN)]
    dns_domain: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // kube's rustls stack disables a default crypto backend; install one
    // before the Secret resolver creates its first Kubernetes TLS client.
    let _ = rustls_kube::crypto::ring::default_provider().install_default();

    let mut args = Args::parse();
    if let Some(path) = args.bootstrap.clone() {
        let bootstrap = BootstrapConfig::load(path)
            .await
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
        apply_bootstrap(&mut args, bootstrap);
    }
    let otel_enabled = init_tracing(
        args.otel_endpoint.as_deref(),
        &args.otel_service_name,
        args.otel_sampling_percentage,
        args.otel_tags.as_deref(),
    )?;

    let run_xds = should_run_xds(&args);
    let identity = RouterIdentity {
        pod_name: args.pod_name,
        namespace: args.namespace.clone(),
        pod_ip: args.pod_ip,
        node_name: args.node_name,
        cluster_id: args.cluster_id,
        dns_domain: args.dns_domain,
    };

    info!(node_id = %identity.node_id(), "starting dxgate router proxy");

    // dubbod is the sole configuration source. Kubernetes access is limited to
    // resolving Secret values referenced by the xDS configuration.
    let store = Arc::new(ConfigStore::new());
    let state = ProxyState::with_store(store.clone());

    if run_xds {
        let xds = XdsClient::new(XdsClientConfig {
            endpoint: args.xds_address,
            identity,
            listener_names: args.listener_names,
            reconnect_delay: Duration::from_secs(10),
        });
        let xds_store = store.clone();
        tokio::spawn(async move {
            if let Err(err) = xds.run(xds_store).await {
                error!(%err, "xDS client exited");
            }
        });
    } else {
        info!("xDS client disabled");
    }

    tokio::spawn(sync_referenced_secrets(
        state.clone(),
        args.namespace.clone(),
    ));

    let proxy = ProxyServer::new(state.clone());
    let access_log_proxy = proxy.clone();
    let ui = UiServer::new(state, args.http_addr, args.metrics_enabled);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut proxy_task = tokio::spawn(
        proxy.serve_with_shutdown(args.http_addr, shutdown_requested(shutdown_rx.clone())),
    );
    let mut ui_task =
        tokio::spawn(ui.serve_with_shutdown(args.ui_addr, shutdown_requested(shutdown_rx)));

    tokio::select! {
        result = &mut proxy_task => result.unwrap_or_else(|err| Err(std::io::Error::other(err)))?,
        result = &mut ui_task => result.unwrap_or_else(|err| Err(std::io::Error::other(err)))?,
        _ = termination_signal() => {
            let drain = Duration::from_secs(args.drain_timeout_seconds);
            info!(drain_timeout = ?drain, "received shutdown signal, draining in-flight requests");
            // Both listeners stop accepting; in-flight requests get until the drain
            // timeout to finish, after which the process exits regardless so a stuck
            // upstream cannot outlive the pod's termination grace period.
            let _ = shutdown_tx.send(true);
            if time::timeout(drain, async {
                let _ = tokio::join!(&mut proxy_task, &mut ui_task);
            })
            .await
            .is_err()
            {
                warn!("drain timeout elapsed with requests still in flight");
            }
        }
    }

    access_log_proxy.shutdown_access_logs().await;

    if otel_enabled {
        opentelemetry::global::shutdown_tracer_provider();
    }

    Ok(())
}

/// Resolves on SIGTERM (what Kubernetes sends first) or SIGINT (Ctrl-C).
async fn termination_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(err) => {
                error!(%err, "failed installing SIGTERM handler; falling back to SIGINT only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn shutdown_requested(mut rx: watch::Receiver<bool>) {
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

async fn sync_referenced_secrets(state: ProxyState, namespace: String) {
    let mut client: Option<Client> = None;
    let mut ticker = time::interval(Duration::from_secs(5));
    loop {
        ticker.tick().await;
        let references = referenced_secrets(&state);
        if references.is_empty() {
            state.replace_credentials(HashMap::new());
            continue;
        }
        let kube = match &client {
            Some(client) => client.clone(),
            None => match Client::try_default().await {
                Ok(value) => {
                    client = Some(value.clone());
                    value
                }
                Err(err) => {
                    warn!(%err, "cannot initialize Kubernetes Secret resolver");
                    continue;
                }
            },
        };
        let mut values = HashMap::new();
        for reference in references {
            if reference.namespace != namespace {
                warn!(
                    secret_namespace = %reference.namespace,
                    secret_name = %reference.name,
                    gateway_namespace = %namespace,
                    "cross-namespace credential reference rejected"
                );
                continue;
            }
            let api = Api::<Secret>::namespaced(kube.clone(), &namespace);
            match api.get(&reference.name).await {
                Ok(secret) => {
                    if let Some(value) = secret
                        .data
                        .as_ref()
                        .and_then(|data| data.get(&reference.key))
                        .and_then(|value| String::from_utf8(value.0.clone()).ok())
                    {
                        values.insert(reference, value);
                    } else {
                        warn!(secret = %reference.name, key = %reference.key, "credential key missing");
                    }
                }
                Err(err) => warn!(secret = %reference.name, %err, "credential Secret unavailable"),
            }
        }
        state.replace_credentials(values);
    }
}

fn referenced_secrets(state: &ProxyState) -> BTreeSet<SecretKeyReference> {
    let snapshot = state.snapshot();
    let mut references = BTreeSet::new();
    for provider in snapshot.providers() {
        if let Some(reference) = &provider.credential_ref {
            references.insert(reference.clone());
        }
    }
    for policy in snapshot.policies() {
        if let Some(AuthPolicy::ApiKey {
            secret_ref: Some(reference),
            ..
        }) = &policy.auth
        {
            references.insert(reference.clone());
        }
    }
    references
}

fn init_tracing(
    otel_endpoint: Option<&str>,
    otel_service_name: &str,
    otel_sampling_percentage: f64,
    otel_tags: Option<&str>,
) -> std::io::Result<bool> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();
    let registry = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer);

    if let Some(endpoint) = otel_endpoint {
        let sampling_percentage = if otel_sampling_percentage.is_finite() {
            otel_sampling_percentage.clamp(0.0, 100.0)
        } else {
            100.0
        };
        let sampling_ratio = sampling_percentage / 100.0;
        let mut resource_attributes =
            vec![KeyValue::new("service.name", otel_service_name.to_string())];
        resource_attributes.extend(parse_otel_tags(otel_tags)?);
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(endpoint.to_string()),
            )
            .with_trace_config(
                opentelemetry_sdk::trace::config()
                    .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                        sampling_ratio,
                    ))))
                    .with_resource(Resource::new(resource_attributes)),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)
            .map_err(|err| std::io::Error::other(format!("initialize OTEL tracing: {err}")))?;
        registry
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .init();
        info!(
            otel_endpoint = %endpoint,
            otel_service_name = %otel_service_name,
            otel_sampling_percentage = sampling_percentage,
            "OpenTelemetry tracing enabled"
        );
        Ok(true)
    } else {
        registry.init();
        Ok(false)
    }
}

fn parse_otel_tags(raw: Option<&str>) -> std::io::Result<Vec<KeyValue>> {
    let Some(raw) = raw.filter(|value| !value.trim().is_empty()) else {
        return Ok(Vec::new());
    };
    let tags: std::collections::BTreeMap<String, String> = serde_json::from_str(raw)
        .map_err(|err| std::io::Error::other(format!("parse DXGATE_OTEL_TAGS: {err}")))?;
    Ok(tags
        .into_iter()
        .map(|(name, value)| KeyValue::new(name, value))
        .collect())
}

fn apply_bootstrap(args: &mut Args, bootstrap: BootstrapConfig) {
    if let Some(value) = bootstrap.xds_address {
        args.xds_address = value;
    }
    if let Some(value) = bootstrap.http_addr {
        args.http_addr = value;
    }
    if let Some(value) = bootstrap.ui_addr {
        args.ui_addr = value;
    }
    if !bootstrap.listener_names.is_empty() {
        args.listener_names = bootstrap.listener_names;
    }
    if let Some(value) = bootstrap.pod_name {
        args.pod_name = value;
    }
    if let Some(value) = bootstrap.namespace {
        args.namespace = value;
    }
    if let Some(value) = bootstrap.pod_ip {
        args.pod_ip = value;
    }
    if let Some(value) = bootstrap.node_name {
        args.node_name = Some(value);
    }
    if let Some(value) = bootstrap.cluster_id {
        args.cluster_id = value;
    }
    if let Some(value) = bootstrap.dns_domain {
        args.dns_domain = value;
    }
}

/// Whether to open an ADS stream. dxgate is a delegated data plane, so the xDS
/// client is on unless explicitly disabled — a proxy without its control plane
/// has nothing to route.
fn should_run_xds(args: &Args) -> bool {
    args.xds_enabled.unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::{apply_bootstrap, parse_otel_tags, should_run_xds, Args};
    use dxgate_xds::BootstrapConfig;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    fn base_args() -> Args {
        Args {
            xds_address: "http://old:15012".to_string(),
            xds_enabled: None,
            http_addr: "0.0.0.0:80".parse().unwrap(),
            ui_addr: "0.0.0.0:15021".parse().unwrap(),
            metrics_enabled: true,
            drain_timeout_seconds: 30,
            bootstrap: Some(PathBuf::from("/etc/dxgate/bootstrap.json")),
            otel_endpoint: None,
            otel_service_name: "dxgate".to_string(),
            otel_sampling_percentage: 100.0,
            otel_tags: None,
            listener_names: Vec::new(),
            pod_name: "dxgate".to_string(),
            namespace: "dubbo-system".to_string(),
            pod_ip: "127.0.0.1".to_string(),
            node_name: None,
            cluster_id: "old-cluster".to_string(),
            dns_domain: "cluster.local".to_string(),
        }
    }

    #[test]
    fn parses_otel_tags_json() {
        let tags = parse_otel_tags(Some(r#"{"foo":"bar","userId":"unknown"}"#)).unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].key.as_str(), "foo");
        assert_eq!(tags[0].value.as_str(), "bar");
        assert_eq!(tags[1].key.as_str(), "userId");
        assert_eq!(tags[1].value.as_str(), "unknown");
    }

    #[test]
    fn bootstrap_overrides_control_plane_fields() {
        let mut args = base_args();

        apply_bootstrap(
            &mut args,
            BootstrapConfig {
                xds_address: Some("http://dubbod.dubbo-system.svc:15012".to_string()),
                http_addr: Some("0.0.0.0:8080".parse::<SocketAddr>().unwrap()),
                listener_names: vec!["public-dubbo.app.svc.cluster.local:80".to_string()],
                cluster_id: Some("Kubernetes".to_string()),
                dns_domain: Some("svc.local".to_string()),
                ..BootstrapConfig::default()
            },
        );

        assert_eq!(args.xds_address, "http://dubbod.dubbo-system.svc:15012");
        assert_eq!(args.http_addr.port(), 8080);
        assert_eq!(args.cluster_id, "Kubernetes");
        assert_eq!(args.dns_domain, "svc.local");
        assert_eq!(args.pod_name, "dxgate");
        assert_eq!(
            args.listener_names,
            ["public-dubbo.app.svc.cluster.local:80"]
        );
    }

    #[test]
    fn xds_runs_unless_explicitly_disabled() {
        let mut args = base_args();
        assert!(should_run_xds(&args));

        args.xds_enabled = Some(false);
        assert!(!should_run_xds(&args));

        args.xds_enabled = Some(true);
        assert!(should_run_xds(&args));
    }
}
