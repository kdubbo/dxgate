//! Upstream HTTP clients and data-plane mTLS: client pooling, certificate
//! loading, and the SPIFFE-compatible server certificate verifier.

use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use dxgate_core::{Cluster, UpstreamTls};
use hyper::client::HttpConnector;
use hyper::Client;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use rustls::client::{ServerCertVerified, ServerCertVerifier, WebPkiVerifier};
use rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore};
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tracing::{info, warn};

type PlainClient = Client<HttpConnector, Body>;
type WebClient = Client<HttpsConnector<HttpConnector>, Body>;
type MtlsClient = Client<HttpsConnector<HttpConnector>, Body>;

#[derive(Clone)]
pub(super) struct UpstreamClients {
    plaintext: PlainClient,
    web: WebClient,
    // HTTP/2-only client: h2c prior knowledge on http:// and ALPN h2 on https://.
    h2: WebClient,
    mtls: MtlsSupport,
}

impl UpstreamClients {
    pub(super) fn from_env() -> Self {
        let mtls = match env::var("GRPC_XDS_BOOTSTRAP") {
            Ok(path) if !path.is_empty() => match MtlsClientPool::from_bootstrap(&path) {
                Ok(pool) => {
                    info!(bootstrap = %path, "loaded dxgate upstream mTLS bootstrap");
                    MtlsSupport::Available(Arc::new(pool))
                }
                Err(err) => {
                    warn!(bootstrap = %path, %err, "failed loading dxgate upstream mTLS bootstrap");
                    MtlsSupport::Error(Arc::from(err))
                }
            },
            _ => MtlsSupport::Disabled,
        };
        let web_connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        let h2_connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http2()
            .build();
        Self {
            plaintext: Client::new(),
            web: Client::builder().build::<_, Body>(web_connector),
            h2: Client::builder()
                .http2_only(true)
                .build::<_, Body>(h2_connector),
            mtls,
        }
    }

    pub(super) async fn request_h2(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, (StatusCode, String)> {
        self.h2
            .request(req)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
    }

    pub(super) async fn request_plain(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, (StatusCode, String)> {
        self.plaintext
            .request(req)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
    }

    pub(super) async fn request_web(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, (StatusCode, String)> {
        self.web
            .request(req)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
    }

    pub(super) async fn request_mtls(
        &self,
        cluster: &Cluster,
        tls: &UpstreamTls,
        req: Request<Body>,
        h2: bool,
    ) -> Result<Response<Body>, (StatusCode, String)> {
        let client = match &self.mtls {
            MtlsSupport::Available(pool) => pool.client_for(tls, h2).map_err(|err| {
                (
                    StatusCode::BAD_GATEWAY,
                    format!("cluster {} mTLS setup failed: {err}", cluster.name),
                )
            })?,
            MtlsSupport::Disabled => {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "cluster {} requires mTLS but GRPC_XDS_BOOTSTRAP is not configured",
                        cluster.name
                    ),
                ));
            }
            MtlsSupport::Error(err) => {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "cluster {} requires mTLS but bootstrap loading failed: {err}",
                        cluster.name
                    ),
                ));
            }
        };
        client
            .request(req)
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
    }
}

#[derive(Clone)]
enum MtlsSupport {
    Disabled,
    Available(Arc<MtlsClientPool>),
    Error(Arc<str>),
}

pub(super) struct MtlsClientPool {
    bootstrap: GrpcBootstrap,
    clients: Mutex<HashMap<String, MtlsClient>>,
}

impl MtlsClientPool {
    pub(super) fn from_bootstrap(path: &str) -> Result<Self, String> {
        let file =
            File::open(path).map_err(|e| format!("open gRPC xDS bootstrap {}: {e}", path))?;
        let bootstrap: GrpcBootstrap = serde_json::from_reader(file)
            .map_err(|e| format!("parse gRPC xDS bootstrap {}: {e}", path))?;
        Ok(Self {
            bootstrap,
            clients: Mutex::new(HashMap::new()),
        })
    }

    pub(super) fn client_for(&self, tls: &UpstreamTls, h2: bool) -> Result<MtlsClient, String> {
        let key = format!("{}|h2={h2}", mtls_cache_key(tls));
        let mut clients = self
            .clients
            .lock()
            .map_err(|_| "mTLS client cache lock poisoned".to_string())?;
        if let Some(client) = clients.get(&key) {
            return Ok(client.clone());
        }

        let config = self.tls_config(tls)?;
        let builder = HttpsConnectorBuilder::new()
            .with_tls_config(config)
            .https_only();
        let builder = match &tls.sni {
            Some(sni) if !sni.is_empty() => builder.with_server_name(sni.clone()),
            _ => builder,
        };
        let client = if h2 {
            let connector = builder.enable_http2().build();
            Client::builder()
                .http2_only(true)
                .build::<_, Body>(connector)
        } else {
            let connector = builder.enable_http1().build();
            Client::builder().build::<_, Body>(connector)
        };
        clients.insert(key, client.clone());
        Ok(client)
    }

    fn tls_config(&self, tls: &UpstreamTls) -> Result<ClientConfig, String> {
        let cert_provider = tls.certificate_provider.as_deref().unwrap_or("default");
        let root_provider = tls.validation_provider.as_deref().unwrap_or("default");
        let cert_config = self.bootstrap.provider(cert_provider)?;
        let root_config = self.bootstrap.provider(root_provider)?;
        let cert_file = cert_config.required_path("certificate_file", cert_provider)?;
        let key_file = cert_config.required_path("private_key_file", cert_provider)?;
        let ca_file = root_config.required_path("ca_certificate_file", root_provider)?;

        let certs = load_certs(cert_file, "data-plane client certificate")?;
        let key = load_private_key(key_file)?;
        let roots = load_roots(ca_file)?;
        let verifier = Arc::new(SpiffeCompatibleVerifier {
            inner: WebPkiVerifier::new(roots, None),
        });
        ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(certs, key)
            .map_err(|e| format!("build data-plane mTLS client config: {e}"))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct GrpcBootstrap {
    #[serde(default)]
    certificate_providers: HashMap<String, CertificateProvider>,
}

impl GrpcBootstrap {
    pub(super) fn provider(&self, name: &str) -> Result<&FileWatcherConfig, String> {
        self.certificate_providers
            .get(name)
            .map(|provider| &provider.config)
            .ok_or_else(|| format!("certificate_providers[{name:?}] not found"))
    }
}

#[derive(Debug, Deserialize)]
struct CertificateProvider {
    config: FileWatcherConfig,
}

#[derive(Debug, Deserialize)]
pub(super) struct FileWatcherConfig {
    certificate_file: Option<PathBuf>,
    private_key_file: Option<PathBuf>,
    ca_certificate_file: Option<PathBuf>,
}

impl FileWatcherConfig {
    pub(super) fn required_path(&self, field: &str, provider: &str) -> Result<&Path, String> {
        let path = match field {
            "certificate_file" => &self.certificate_file,
            "private_key_file" => &self.private_key_file,
            "ca_certificate_file" => &self.ca_certificate_file,
            _ => return Err(format!("unknown file watcher field {field}")),
        };
        path.as_deref().ok_or_else(|| {
            format!("certificate_providers[{provider:?}].config.{field} is required")
        })
    }
}

pub(super) fn mtls_cache_key(tls: &UpstreamTls) -> String {
    format!(
        "{}|{}|{}|{}",
        tls.sni.as_deref().unwrap_or_default(),
        tls.certificate_provider.as_deref().unwrap_or("default"),
        tls.validation_provider.as_deref().unwrap_or("default"),
        tls.alpn_protocols.join(",")
    )
}

fn load_certs(path: &Path, label: &str) -> Result<Vec<Certificate>, String> {
    let file = File::open(path).map_err(|e| format!("open {label} {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .map_err(|e| format!("parse {label} {}: {e}", path.display()))?
        .into_iter()
        .map(Certificate)
        .collect::<Vec<_>>();
    if certs.is_empty() {
        return Err(format!(
            "parse {label} {}: no certificates found",
            path.display()
        ));
    }
    Ok(certs)
}

fn load_roots(path: &Path) -> Result<RootCertStore, String> {
    let certs = load_certs(path, "data-plane CA certificate")?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots
            .add(&cert)
            .map_err(|e| format!("add data-plane CA certificate {}: {e}", path.display()))?;
    }
    Ok(roots)
}

fn load_private_key(path: &Path) -> Result<PrivateKey, String> {
    if let Some(key) = load_private_keys(path, KeyFormat::Pkcs8)?
        .into_iter()
        .next()
    {
        return Ok(PrivateKey(key));
    }
    if let Some(key) = load_private_keys(path, KeyFormat::Rsa)?.into_iter().next() {
        return Ok(PrivateKey(key));
    }
    Err(format!(
        "parse data-plane client private key {}: no PKCS8 or RSA keys found",
        path.display()
    ))
}

enum KeyFormat {
    Pkcs8,
    Rsa,
}

fn load_private_keys(path: &Path, format: KeyFormat) -> Result<Vec<Vec<u8>>, String> {
    let file = File::open(path)
        .map_err(|e| format!("open data-plane client private key {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    match format {
        KeyFormat::Pkcs8 => rustls_pemfile::pkcs8_private_keys(&mut reader),
        KeyFormat::Rsa => rustls_pemfile::rsa_private_keys(&mut reader),
    }
    .map_err(|e| {
        format!(
            "parse data-plane client private key {}: {e}",
            path.display()
        )
    })
}

struct SpiffeCompatibleVerifier {
    inner: WebPkiVerifier,
}

impl std::fmt::Debug for SpiffeCompatibleVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpiffeCompatibleVerifier").finish()
    }
}

impl ServerCertVerifier for SpiffeCompatibleVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        server_name: &rustls::ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: SystemTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            scts,
            ocsp_response,
            now,
        ) {
            Ok(verified) => Ok(verified),
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                Ok(ServerCertVerified::assertion())
            }
            Err(err) => Err(err),
        }
    }
}
