use clap::{Parser, ValueEnum};
use dxgate_admin::AdminServer;
use dxgate_controller::{crds, run_controller};
use dxgate_core::{RouterIdentity, RuntimeConfig, DEFAULT_CLUSTER_ID, DEFAULT_DNS_DOMAIN};
use dxgate_proxy::{ProxyServer, ProxyState};
use dxgate_xds::{
    BootstrapConfig, RuntimeConfigSource, StaticConfigFile, XdsClient, XdsClientConfig,
};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::Resource;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time;
use tracing::{error, info};
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

    #[arg(long, env = "DXGATE_ADMIN_ADDR", default_value = "0.0.0.0:15021")]
    admin_addr: SocketAddr,

    #[arg(long, env = "DXGATE_STATIC_CONFIG")]
    static_config: Option<PathBuf>,

    #[arg(long, env = "DXGATE_CONFIG_WATCH", default_value_t = false)]
    config_watch: bool,

    #[arg(long, env = "DXGATE_BOOTSTRAP")]
    bootstrap: Option<PathBuf>,

    #[arg(long, env = "DXGATE_OTEL_ENDPOINT")]
    otel_endpoint: Option<String>,

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
    let otel_enabled = init_tracing(args.otel_endpoint.as_deref())?;

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

    let initial = RuntimeConfig::empty("bootstrap");
    let state = ProxyState::new(initial.clone());
    let (config_tx, mut config_rx) = watch::channel(initial);

    let apply_state = state.clone();
    tokio::spawn(async move {
        while config_rx.changed().await.is_ok() {
            let cfg = config_rx.borrow().clone();
            if let Err(conflicts) = apply_state.apply_config(cfg).await {
                error!(?conflicts, "runtime config rejected");
            }
        }
    });

    if let Some(path) = args.static_config.clone() {
        let source = StaticConfigFile::new(path);
        match source.load().await {
            Ok(Some(cfg)) => {
                if let Err(conflicts) = state.apply_config(cfg.clone()).await {
                    error!(?conflicts, "static config rejected");
                } else if config_tx.send(cfg).is_err() {
                    error!("runtime config watcher is closed");
                }
            }
            Ok(None) => {}
            Err(err) => error!(%err, "failed loading static config"),
        }
        if args.config_watch {
            let path = args.static_config.clone().unwrap();
            let config_tx = config_tx.clone();
            tokio::spawn(async move {
                watch_static_config(path, config_tx).await;
            });
        }
    }

    if matches!(args.mode, DxgateMode::Proxy | DxgateMode::All) && run_xds {
        let xds = XdsClient::new(XdsClientConfig {
            endpoint: args.xds_address,
            identity,
            listener_names: args.listener_names,
            reconnect_delay: Duration::from_secs(10),
        });
        let xds_tx = config_tx.clone();
        tokio::spawn(async move {
            if let Err(err) = xds.run(xds_tx).await {
                error!(%err, "xDS client exited");
            }
        });
    } else if matches!(args.mode, DxgateMode::Proxy | DxgateMode::All) {
        info!("xDS client disabled");
    }

    if matches!(args.mode, DxgateMode::Controller | DxgateMode::All) {
        let controller_tx = config_tx.clone();
        tokio::spawn(async move {
            if let Err(err) = run_controller(controller_tx).await {
                error!(%err, "Kubernetes controller exited");
            }
        });
    }

    if matches!(args.mode, DxgateMode::Controller) {
        tokio::signal::ctrl_c().await?;
        info!("received shutdown signal");
        return Ok(());
    }

    let proxy = ProxyServer::new(state.clone());
    let admin = AdminServer::new(state, args.http_addr);
    let proxy_task = tokio::spawn(proxy.serve(args.http_addr));
    let admin_task = tokio::spawn(admin.serve(args.admin_addr));

    tokio::select! {
        result = proxy_task => result.unwrap_or_else(|err| Err(std::io::Error::other(err)))?,
        result = admin_task => result.unwrap_or_else(|err| Err(std::io::Error::other(err)))?,
        _ = tokio::signal::ctrl_c() => info!("received shutdown signal"),
    }

    if otel_enabled {
        opentelemetry::global::shutdown_tracer_provider();
    }

    Ok(())
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

fn init_tracing(otel_endpoint: Option<&str>) -> std::io::Result<bool> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let env_filter = tracing_subscriber::EnvFilter::from_default_env();
    let fmt_layer = tracing_subscriber::fmt::layer();
    let registry = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer);

    if let Some(endpoint) = otel_endpoint {
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(endpoint.to_string()),
            )
            .with_trace_config(
                opentelemetry_sdk::trace::config()
                    .with_resource(Resource::new(vec![KeyValue::new("service.name", "dxgate")])),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)
            .map_err(|err| std::io::Error::other(format!("initialize OTEL tracing: {err}")))?;
        registry
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .init();
        info!(otel_endpoint = %endpoint, "OpenTelemetry tracing enabled");
        Ok(true)
    } else {
        registry.init();
        Ok(false)
    }
}

async fn watch_static_config(path: PathBuf, config_tx: watch::Sender<RuntimeConfig>) {
    let mut last = None;
    loop {
        match tokio::fs::metadata(&path)
            .await
            .and_then(|meta| meta.modified())
        {
            Ok(modified) if last.map(|seen| seen != modified).unwrap_or(true) => {
                last = Some(modified);
                let source = StaticConfigFile::new(path.clone());
                match source.load().await {
                    Ok(Some(cfg)) => {
                        if let Err(err) = config_tx.send(cfg) {
                            error!(%err, "static config watcher channel closed");
                            return;
                        }
                        info!(path = %path.display(), "reloaded static config");
                    }
                    Ok(None) => {}
                    Err(err) => {
                        error!(%err, path = %path.display(), "failed reloading static config")
                    }
                }
            }
            Ok(_) => {}
            Err(err) => error!(%err, path = %path.display(), "failed checking static config"),
        }
        time::sleep(Duration::from_secs(2)).await;
    }
}

fn apply_bootstrap(args: &mut Args, bootstrap: BootstrapConfig) {
    if let Some(value) = bootstrap.xds_address {
        args.xds_address = value;
    }
    if let Some(value) = bootstrap.http_addr {
        args.http_addr = value;
    }
    if let Some(value) = bootstrap.admin_addr {
        args.admin_addr = value;
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
    use super::{apply_bootstrap, should_run_xds, Args, DxgateMode};
    use dxgate_xds::BootstrapConfig;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    fn base_args() -> Args {
        Args {
            mode: DxgateMode::Proxy,
            xds_address: "http://old:15012".to_string(),
            xds_enabled: None,
            http_addr: "0.0.0.0:80".parse().unwrap(),
            admin_addr: "0.0.0.0:15021".parse().unwrap(),
            static_config: None,
            config_watch: false,
            bootstrap: Some(PathBuf::from("/etc/dxgate/bootstrap.json")),
            otel_endpoint: None,
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
        args.static_config = Some(PathBuf::from("examples/agent-runtime.yaml"));

        assert!(!should_run_xds(&args));

        args.xds_enabled = Some(true);
        assert!(should_run_xds(&args));
    }

    #[test]
    fn xds_runs_by_default_without_static_config_or_with_listener_names() {
        let mut args = base_args();
        assert!(should_run_xds(&args));

        args.static_config = Some(PathBuf::from("examples/agent-runtime.yaml"));
        args.listener_names = vec!["public.example:80".to_string()];
        assert!(should_run_xds(&args));
    }
}
