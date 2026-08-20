//! Upstream HTTP clients and data-plane mTLS: client pooling, certificate
//! loading, and the SPIFFE-compatible server certificate verifier.

use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use dxgate_core::{Cluster, ConfigSnapshot, TlsSecret, UpstreamTls};
use hyper::client::HttpConnector;
use hyper::Client;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use rustls::client::{ServerCertVerified, ServerCertVerifier, WebPkiVerifier};
use rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore};
use serde::Deserialize;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::BufReader;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Once};
use std::time::SystemTime;
use tracing::{info, warn};
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer, X509Certificate};

type PlainClient = Client<HttpConnector, Body>;
type WebClient = Client<HttpsConnector<HttpConnector>, Body>;
type MtlsClient = Client<HttpsConnector<HttpConnector>, Body>;

#[derive(Clone)]
pub(super) struct UpstreamClients {
    plaintext: PlainClient,
    web: WebClient,
    // HTTP/2-only client: h2c prior knowledge on http:// and ALPN h2 on https://.
    h2: WebClient,
    // ADS/SDS material wins when the cluster names concrete Secret resources.
    // Its cache is invalidated on every newly published config snapshot, so a
    // certificate rotation cannot reuse a client holding the prior keypair.
    dynamic_mtls: Arc<Mutex<DynamicMtlsClientPool>>,
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
            dynamic_mtls: Arc::new(Mutex::new(DynamicMtlsClientPool::default())),
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
        snapshot: &ConfigSnapshot,
        req: Request<Body>,
        h2: bool,
    ) -> Result<Response<Body>, (StatusCode, String)> {
        if tls.certificate_secret.is_some() || tls.validation_secret.is_some() {
            let client = self
                .dynamic_mtls
                .lock()
                .map_err(|_| {
                    (
                        StatusCode::BAD_GATEWAY,
                        format!("cluster {} dynamic mTLS cache lock poisoned", cluster.name),
                    )
                })?
                .client_for(snapshot, tls, h2)
                .map_err(|err| {
                    (
                        StatusCode::BAD_GATEWAY,
                        format!("cluster {} dynamic mTLS setup failed: {err}", cluster.name),
                    )
                })?;
            return client
                .request(req)
                .await
                .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()));
        }

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

#[derive(Default)]
pub(super) struct DynamicMtlsClientPool {
    snapshot_revision: u64,
    clients: HashMap<String, MtlsClient>,
}

impl DynamicMtlsClientPool {
    pub(super) fn client_for(
        &mut self,
        snapshot: &ConfigSnapshot,
        tls: &UpstreamTls,
        h2: bool,
    ) -> Result<MtlsClient, String> {
        // Config snapshots are immutable and atomically swapped by the xDS
        // client. Dropping this cache at a new revision releases the former
        // private key promptly while in-flight requests keep their Arc-owned
        // client until they naturally finish.
        if self.snapshot_revision != snapshot.revision() {
            self.clients.clear();
            self.snapshot_revision = snapshot.revision();
        }

        let certificate_name = tls
            .certificate_secret
            .as_deref()
            .ok_or_else(|| "xDS TLS context has no certificate SDS resource name".to_string())?;
        let validation_name = tls
            .validation_secret
            .as_deref()
            .ok_or_else(|| "xDS TLS context has no validation SDS resource name".to_string())?;
        let certificate = snapshot
            .secret(certificate_name)
            .ok_or_else(|| format!("SDS certificate {certificate_name:?} is not available"))?;
        let validation = snapshot.secret(validation_name).ok_or_else(|| {
            format!("SDS validation context {validation_name:?} is not available")
        })?;
        let key = dynamic_mtls_cache_key(tls, certificate, validation, h2);
        if let Some(client) = self.clients.get(&key) {
            return Ok(client.clone());
        }

        let config = dynamic_tls_config(tls, certificate, validation)?;
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
        self.clients.insert(key, client.clone());
        Ok(client)
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
            allowed_sans: tls.subject_alt_names.clone(),
            warned_unpinned: Once::new(),
        });
        ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(certs, key)
            .map_err(|e| format!("build data-plane mTLS client config: {e}"))
    }
}

fn dynamic_tls_config(
    tls: &UpstreamTls,
    certificate: &TlsSecret,
    validation: &TlsSecret,
) -> Result<ClientConfig, String> {
    if certificate.certificate_chain_pem.is_empty() || certificate.private_key_pem.is_empty() {
        return Err(format!(
            "SDS certificate {:?} has no certificate chain and private key",
            certificate.name
        ));
    }
    let Some(trusted_ca_pem) = validation.trusted_ca_pem.as_deref() else {
        return Err(format!(
            "SDS validation context {:?} has no trusted CA bundle",
            validation.name
        ));
    };
    let certs = pem_certs(
        certificate.certificate_chain_pem.as_bytes(),
        "SDS client certificate",
    )?;
    let key = pem_private_key(
        certificate.private_key_pem.as_bytes(),
        "SDS client private key",
    )?;
    let roots = pem_roots(trusted_ca_pem.as_bytes(), "SDS CA certificate")?;
    let verifier = Arc::new(SpiffeCompatibleVerifier {
        inner: WebPkiVerifier::new(roots, None),
        allowed_sans: tls.subject_alt_names.clone(),
        warned_unpinned: Once::new(),
    });
    ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(certs, key)
        .map_err(|error| format!("build SDS data-plane mTLS client config: {error}"))
}

fn dynamic_mtls_cache_key(
    tls: &UpstreamTls,
    certificate: &TlsSecret,
    validation: &TlsSecret,
    h2: bool,
) -> String {
    // Never use PEM directly as a cache key. Besides needless memory pressure,
    // it would make an accidental debug dump expose private key material.
    let mut hasher = DefaultHasher::new();
    mtls_cache_key(tls).hash(&mut hasher);
    certificate.name.hash(&mut hasher);
    certificate.certificate_chain_pem.hash(&mut hasher);
    certificate.private_key_pem.hash(&mut hasher);
    validation.name.hash(&mut hasher);
    validation.trusted_ca_pem.hash(&mut hasher);
    h2.hash(&mut hasher);
    format!("sds-{:016x}", hasher.finish())
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
    // subject_alt_names is part of the key: it selects the verifier baked into the
    // cached client, so clusters pinning different peer identities must not share one.
    format!(
        "{}|{}|{}|{}|{}",
        tls.sni.as_deref().unwrap_or_default(),
        tls.certificate_provider.as_deref().unwrap_or("default"),
        tls.validation_provider.as_deref().unwrap_or("default"),
        tls.alpn_protocols.join(","),
        tls.subject_alt_names.join(",")
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

fn pem_certs(bytes: &[u8], label: &str) -> Result<Vec<Certificate>, String> {
    let mut reader = Cursor::new(bytes);
    let certs = rustls_pemfile::certs(&mut reader)
        .map_err(|error| format!("parse {label}: {error}"))?
        .into_iter()
        .map(Certificate)
        .collect::<Vec<_>>();
    if certs.is_empty() {
        return Err(format!("parse {label}: no certificates found"));
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

fn pem_roots(bytes: &[u8], label: &str) -> Result<RootCertStore, String> {
    let certs = pem_certs(bytes, label)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots
            .add(&cert)
            .map_err(|error| format!("add {label}: {error}"))?;
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

fn pem_private_key(bytes: &[u8], label: &str) -> Result<PrivateKey, String> {
    let mut pkcs8_reader = Cursor::new(bytes);
    if let Some(key) = rustls_pemfile::pkcs8_private_keys(&mut pkcs8_reader)
        .map_err(|error| format!("parse {label}: {error}"))?
        .into_iter()
        .next()
    {
        return Ok(PrivateKey(key));
    }
    let mut rsa_reader = Cursor::new(bytes);
    if let Some(key) = rustls_pemfile::rsa_private_keys(&mut rsa_reader)
        .map_err(|error| format!("parse {label}: {error}"))?
        .into_iter()
        .next()
    {
        return Ok(PrivateKey(key));
    }
    Err(format!("parse {label}: no PKCS8 or RSA private keys found"))
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
    // Accepted peer identities from the cluster's validation context. Empty disables
    // identity pinning: the chain is still verified, but any workload holding a cert
    // from the same trust domain is accepted.
    allowed_sans: Vec<String>,
    warned_unpinned: Once,
}

impl std::fmt::Debug for SpiffeCompatibleVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpiffeCompatibleVerifier")
            .field("allowed_sans", &self.allowed_sans)
            .finish()
    }
}

impl SpiffeCompatibleVerifier {
    fn verify_peer_identity(&self, end_entity: &Certificate) -> Result<(), rustls::Error> {
        let presented = peer_identities(&end_entity.0).map_err(|err| {
            rustls::Error::General(format!("parse upstream certificate identities: {err}"))
        })?;
        if presented
            .iter()
            .any(|name| self.allowed_sans.iter().any(|allowed| allowed == name))
        {
            return Ok(());
        }
        warn!(
            expected = ?self.allowed_sans,
            presented = ?presented,
            "upstream certificate identity rejected"
        );
        Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::ApplicationVerificationFailure,
        ))
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
            Ok(_) => {}
            // SPIFFE leaf certs identify the workload with a URI SAN and carry no DNS
            // SAN, so webpki's SNI match always fails. Tolerating that is only sound
            // because `allowed_sans` re-asserts the identity below; without it the
            // trust domain's CA is the only thing standing between us and any peer.
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                if self.allowed_sans.is_empty() {
                    self.warned_unpinned.call_once(|| {
                        warn!(
                            "upstream certificate name does not match SNI and no subject_alt_names \
                             are configured: peer identity is unverified, any certificate issued \
                             by the trusted CA is accepted"
                        );
                    });
                }
            }
            Err(err) => return Err(err),
        }
        if !self.allowed_sans.is_empty() {
            self.verify_peer_identity(end_entity)?;
        }
        Ok(ServerCertVerified::assertion())
    }
}

/// URI and DNS subject alternative names presented by a peer certificate.
pub(super) fn peer_identities(der: &[u8]) -> Result<Vec<String>, String> {
    let (_, cert) = X509Certificate::from_der(der).map_err(|e| e.to_string())?;
    let Some(san) = cert.subject_alternative_name().map_err(|e| e.to_string())? else {
        return Ok(Vec::new());
    };
    Ok(san
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some((*uri).to_string()),
            GeneralName::DNSName(dns) => Some((*dns).to_string()),
            _ => None,
        })
        .collect())
}
