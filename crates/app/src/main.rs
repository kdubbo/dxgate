use clap::Parser;
use dxgate_admin::AdminServer;
use dxgate_core::{RouterIdentity, RuntimeConfig, DEFAULT_CLUSTER_ID, DEFAULT_DNS_DOMAIN};
use dxgate_proxy::{ProxyServer, ProxyState};
use dxgate_xds::{
    BootstrapConfig, RuntimeConfigSource, StaticConfigFile, XdsClient, XdsClientConfig,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info};

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

    #[arg(long, env = "DXGATE_HTTP_ADDR", default_value = "0.0.0.0:80")]
    http_addr: SocketAddr,

    #[arg(long, env = "DXGATE_ADMIN_ADDR", default_value = "0.0.0.0:15021")]
    admin_addr: SocketAddr,

    #[arg(long, env = "DXGATE_STATIC_CONFIG")]
    static_config: Option<PathBuf>,

    #[arg(long, env = "DXGATE_BOOTSTRAP")]
    bootstrap: Option<PathBuf>,

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
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let mut args = Args::parse();
    if let Some(path) = args.bootstrap.clone() {
        let bootstrap = BootstrapConfig::load(path)
            .await
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;
        apply_bootstrap(&mut args, bootstrap);
    }

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
        listener_names: args.listener_names,
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

#[cfg(test)]
mod tests {
    use super::{apply_bootstrap, Args};
    use dxgate_xds::BootstrapConfig;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    fn base_args() -> Args {
        Args {
            xds_address: "http://old:15012".to_string(),
            http_addr: "0.0.0.0:80".parse().unwrap(),
            admin_addr: "0.0.0.0:15021".parse().unwrap(),
            static_config: None,
            bootstrap: Some(PathBuf::from("/etc/dxgate/bootstrap.json")),
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
}
