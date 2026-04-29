use clap::Parser;
use kgate_admin::AdminServer;
use kgate_core::{RouterIdentity, RuntimeConfig, DEFAULT_CLUSTER_ID, DEFAULT_DNS_DOMAIN};
use kgate_proxy::{ProxyServer, ProxyState};
use kgate_xds::{RuntimeConfigSource, StaticConfigFile, XdsClient, XdsClientConfig};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(name = "kgate")]
#[command(about = "Pure Rust north-south proxy for Dubbo Gateway API traffic")]
struct Args {
    #[arg(
        long,
        env = "KGATE_XDS_ADDRESS",
        default_value = "http://dubbod.dubbo-system.svc:15012"
    )]
    xds_address: String,

    #[arg(long, env = "KGATE_HTTP_ADDR", default_value = "0.0.0.0:80")]
    http_addr: SocketAddr,

    #[arg(long, env = "KGATE_ADMIN_ADDR", default_value = "0.0.0.0:15021")]
    admin_addr: SocketAddr,

    #[arg(long, env = "KGATE_STATIC_CONFIG")]
    static_config: Option<PathBuf>,

    #[arg(long, env = "POD_NAME", default_value = "kgate")]
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
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let identity = RouterIdentity {
        pod_name: args.pod_name,
        namespace: args.namespace,
        pod_ip: args.pod_ip,
        node_name: args.node_name,
        cluster_id: args.cluster_id,
        dns_domain: args.dns_domain,
    };

    info!(node_id = %identity.node_id(), "starting kgate router proxy");

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

    if let Some(path) = args.static_config {
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
    }

    let xds = XdsClient::new(XdsClientConfig {
        endpoint: args.xds_address,
        identity,
        reconnect_delay: Duration::from_secs(10),
    });
    tokio::spawn(async move {
        if let Err(err) = xds.run(config_tx).await {
            error!(%err, "xDS client exited");
        }
    });

    let proxy = ProxyServer::new(state.clone());
    let admin = AdminServer::new(state);

    let proxy_task = tokio::spawn(proxy.serve(args.http_addr));
    let admin_task = tokio::spawn(admin.serve(args.admin_addr));

    tokio::select! {
        result = proxy_task => result.unwrap_or_else(|err| Err(std::io::Error::other(err)))?,
        result = admin_task => result.unwrap_or_else(|err| Err(std::io::Error::other(err)))?,
        _ = tokio::signal::ctrl_c() => info!("received shutdown signal"),
    }

    Ok(())
}
