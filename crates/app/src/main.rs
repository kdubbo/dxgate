use clap::{Parser, ValueEnum};
use dxgate_controller::{crds, run_controller};
use dxgate_core::{ConfigStore, RouterIdentity, DEFAULT_CLUSTER_ID, DEFAULT_DNS_DOMAIN};
use dxgate_proxy::{ProxyServer, ProxyState};
use dxgate_ui::UiServer;
use dxgate_xds::{BootstrapConfig, StaticConfigSource, XdsClient, XdsClientConfig};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::Sampler;
use opentelemetry_sdk::Resource;
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
    #[arg(long, env = "DXGATE_MODE", default_value = "proxy")]
    mode: DxgateMode,

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

    // Should be <= the pod's terminationGracePeriodSeconds, or Kubernetes SIGKILLs
    // the process mid-drain and the graceful shutdown buys nothing.
    #[arg(long, env = "DXGATE_DRAIN_TIMEOUT_SECONDS", default_value_t = 30)]
    drain_timeout_seconds: u64,

    #[arg(long, env = "DXGATE_STATIC_CONFIG")]
    static_config: Option<PathBuf>,

    #[arg(long, env = "DXGATE_CONFIG_WATCH", default_value_t = false)]
    config_watch: bool,

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

    #[arg(long)]
    print_crds: bool,

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DxgateMode {
    Proxy,
    Controller,
    All,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut args = Args::parse();
    if args.print_crds {
        print_crds()?;
        return Ok(());
    }
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
        namespace: args.namespace,
        pod_ip: args.pod_ip,
        node_name: args.node_name,
        cluster_id: args.cluster_id,
        dns_domain: args.dns_domain,
    };

    info!(node_id = %identity.node_id(), "starting dxgate router proxy");

    // Every configuration source writes into this one store, each owning a
    // disjoint slice of it. There is no fan-in channel and no last-writer-wins:
    // a source's update only ever touches the resources that source published.
    let store = Arc::new(ConfigStore::new());
    let state = ProxyState::with_store(store.clone());

    if let Some(path) = args.static_config.clone() {
        let mut source = StaticConfigSource::new(path, store.clone());
        if let Err(err) = source.reload().await {
            error!(%err, "failed loading static config");
        }
        if args.config_watch {
            tokio::spawn(source.watch());
        }
    }

    if matches!(args.mode, DxgateMode::Proxy | DxgateMode::All) && run_xds {
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
    } else if matches!(args.mode, DxgateMode::Proxy | DxgateMode::All) {
        info!("xDS client disabled");
    }

    if matches!(args.mode, DxgateMode::Controller | DxgateMode::All) {
        let controller_store = store.clone();
        tokio::spawn(async move {
            if let Err(err) = run_controller(controller_store).await {
                error!(%err, "Kubernetes controller exited");
            }
        });
    }

    if matches!(args.mode, DxgateMode::Controller) {
        termination_signal().await;
        info!("received shutdown signal");
        return Ok(());
    }

    let proxy = ProxyServer::new(state.clone());
    let ui = UiServer::new(state, args.http_addr);
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

fn print_crds() -> std::io::Result<()> {
    for (idx, crd) in crds().into_iter().enumerate() {
        if idx > 0 {
            println!("---");
        }
        let yaml = serde_yaml::to_string(&crd)
            .map_err(|err| std::io::Error::other(format!("serialize CRD: {err}")))?;
        print!("{yaml}");
    }
    Ok(())
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

fn should_run_xds(args: &Args) -> bool {
    args.xds_enabled
        .unwrap_or_else(|| args.static_config.is_none() || !args.listener_names.is_empty())
}

#[cfg(test)]
mod tests {
    use super::{apply_bootstrap, parse_otel_tags, should_run_xds, Args, DxgateMode};
    use dxgate_xds::BootstrapConfig;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    fn base_args() -> Args {
        Args {
            mode: DxgateMode::Proxy,
            xds_address: "http://old:15012".to_string(),
            xds_enabled: None,
            http_addr: "0.0.0.0:80".parse().unwrap(),
            ui_addr: "0.0.0.0:15021".parse().unwrap(),
            drain_timeout_seconds: 30,
            static_config: None,
            config_watch: false,
            bootstrap: Some(PathBuf::from("/etc/dxgate/bootstrap.json")),
            otel_endpoint: None,
            otel_service_name: "dxgate".to_string(),
            otel_sampling_percentage: 100.0,
            otel_tags: None,
            print_crds: false,
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
    fn static_only_run_disables_xds_by_default() {
        let mut args = base_args();
        args.static_config = Some(PathBuf::from("examples/config.yaml"));

        assert!(!should_run_xds(&args));

        args.xds_enabled = Some(true);
        assert!(should_run_xds(&args));
    }

    #[test]
    fn xds_runs_by_default_without_static_config_or_with_listener_names() {
        let mut args = base_args();
        assert!(should_run_xds(&args));

        args.static_config = Some(PathBuf::from("examples/config.yaml"));
        args.listener_names = vec!["public.example:80".to_string()];
        assert!(should_run_xds(&args));
    }
}
