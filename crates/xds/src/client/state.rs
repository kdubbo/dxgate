//! The xDS resource cache and its projection onto dxgate's configuration model.
//!
//! A dxgate `Cluster` is a join of an xDS `Cluster` with its
//! `ClusterLoadAssignment`, and a dxgate `Listener` is a join of an xDS
//! `Listener` with the `RouteConfiguration`s it names over RDS. Neither join can
//! be computed from a single incoming resource, so the client keeps the raw xDS
//! resources here and re-projects them after every update.
//!
//! [`AdsState`] accepts both wire flavours: `apply_sotw` replaces a type's whole
//! set (state-of-the-world ADS), `apply_delta` upserts and removes individual
//! resources (delta ADS). Everything downstream of that is shared.

use super::XdsError;
use crate::proto::cluster::v1 as xds_cluster;
use crate::proto::core::v1 as xds_core;
use crate::proto::endpoint::v1 as xds_endpoint;
use crate::proto::extensions::filters::network::http_connection_manager::v1 as xds_hcm;
use crate::proto::extensions::transport_sockets::tls::v1 as xds_tls;
use crate::proto::listener::v1 as xds_listener;
use crate::proto::route::v1 as xds_route;
use crate::proto::service::discovery::v1::DiscoveryResponse;
use dxgate_core::{
    AgentProtocol, AgentRoute, AgentRouteMatch, AuthPolicy, Backend, BackendKind,
    CircuitBreakerConfig, Cluster, Collection, ConfigDelta, Endpoint as RuntimeEndpoint,
    HeaderMatch, HeaderTransform, HeaderValue, Listener, ListenerProtocol, OutlierDetectionConfig,
    PathMatch, Policy, PolicyAction, Provider, ProviderKind, RateLimitKey, RateLimitPolicy,
    RetryPolicy, Route, RouteMatch, SecretKeyReference, SourceState, TokenLimitPolicy, UpstreamTls,
    UpstreamTlsMode, VirtualHost, WeightedBackend, WeightedCluster,
};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub(super) const CLUSTER_TYPE: &str = "type.googleapis.com/cluster.v1.Cluster";
pub(super) const ENDPOINT_TYPE: &str = "type.googleapis.com/endpoint.v1.ClusterLoadAssignment";
pub(super) const LISTENER_TYPE: &str = "type.googleapis.com/listener.v1.Listener";
pub(super) const ROUTE_TYPE: &str = "type.googleapis.com/route.v1.RouteConfiguration";

/// A change to what the client is subscribed to for one resource type.
///
/// Delta ADS carries the change itself, so both halves are sent as-is.
/// State-of-the-world ADS carries the full desired set, so `desired` is what
/// goes on the wire there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SubscriptionChange {
    pub(super) type_url: &'static str,
    pub(super) desired: Vec<String>,
    pub(super) subscribe: Vec<String>,
    pub(super) unsubscribe: Vec<String>,
}

impl SubscriptionChange {
    fn is_empty(&self) -> bool {
        self.subscribe.is_empty() && self.unsubscribe.is_empty()
    }
}

#[derive(Default)]
pub(super) struct AdsState {
    subscriptions: BTreeMap<String, BTreeSet<String>>,
    listeners: Collection<String, ListenerSnapshot>,
    routes: Collection<String, RouteSnapshot>,
    clusters: Collection<String, ClusterSnapshot>,
    endpoints: Collection<String, Vec<RuntimeEndpoint>>,
    /// Per-type resource versions, replayed as a delta stream's
    /// `initial_resource_versions` after a reconnect so the control plane only
    /// resends what actually changed while the stream was down.
    versions: BTreeMap<String, BTreeMap<String, String>>,
    /// Derived resource keys last published to the store, so removals can be
    /// computed even though a projection cannot say what it stopped producing.
    published: SourceState,
}

impl AdsState {
    /// Prepares the cache for a freshly opened stream and returns the listener
    /// names to request first. An empty set means the legacy wildcard
    /// subscription, which is how the client asks `dubbod` for every listener it
    /// is entitled to.
    ///
    /// A new stream carries no subscription state on the server side, so the
    /// record of what RDS/CDS/EDS resources were already subscribed to is
    /// cleared: the desired sets have not changed across the reconnect, and
    /// without this the diff would come out empty and those types would never be
    /// re-requested. Cached resources and their versions are kept, so a delta
    /// stream can still replay `initial_resource_versions`.
    pub(super) fn begin_stream(&mut self, listener_names: Vec<String>) -> Vec<String> {
        for type_url in [ROUTE_TYPE, CLUSTER_TYPE, ENDPOINT_TYPE] {
            self.subscriptions.remove(type_url);
        }
        let names: BTreeSet<String> = listener_names
            .into_iter()
            .filter(|name| !name.is_empty())
            .collect();
        let desired = names.iter().cloned().collect();
        self.subscriptions.insert(LISTENER_TYPE.to_string(), names);
        desired
    }

    pub(super) fn subscription(&self, type_url: &str) -> Vec<String> {
        self.subscriptions
            .get(type_url)
            .map(|names| names.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Versions to replay on reconnect for one resource type.
    pub(super) fn initial_resource_versions(&self, type_url: &str) -> BTreeMap<String, String> {
        self.versions.get(type_url).cloned().unwrap_or_default()
    }

    /// Applies a state-of-the-world response: the payload is the complete set
    /// for that type, so anything the client still holds and the server did not
    /// send is gone.
    pub(super) fn apply_sotw(&mut self, resp: &DiscoveryResponse) -> Result<(), XdsError> {
        // The payload is the complete set for its type, so it replaces what the
        // client holds outright: whatever the server did not send is gone. With
        // an explicit subscription the response only covers the requested names,
        // so resources outside it are carried over.
        let requested = self.subscription(&resp.type_url);
        match resp.type_url.as_str() {
            LISTENER_TYPE => {
                let received = decode_listeners(&resp.resources)?;
                replace_within_subscription(&mut self.listeners, received, &requested);
            }
            ROUTE_TYPE => {
                let received = decode_routes(&resp.resources)?;
                replace_within_subscription(&mut self.routes, received, &requested);
            }
            CLUSTER_TYPE => {
                let received = self.decode_clusters(&resp.resources)?;
                replace_within_subscription(&mut self.clusters, received, &requested);
            }
            ENDPOINT_TYPE => {
                let received = decode_endpoints(&resp.resources)?;
                replace_within_subscription(&mut self.endpoints, received, &requested);
            }
            _ => {}
        }
        Ok(())
    }

    /// Applies a delta response: only the named resources changed, and
    /// `removed` names are gone.
    pub(super) fn apply_delta(
        &mut self,
        type_url: &str,
        resources: &[(String, String, prost_types::Any)],
        removed: &[String],
    ) -> Result<(), XdsError> {
        let payload: Vec<prost_types::Any> = resources
            .iter()
            .map(|(_, _, any)| any.clone())
            .collect::<Vec<_>>();
        match type_url {
            LISTENER_TYPE => {
                upsert_each(&mut self.listeners, decode_listeners(&payload)?);
                remove_each(&mut self.listeners, removed);
            }
            ROUTE_TYPE => {
                upsert_each(&mut self.routes, decode_routes(&payload)?);
                remove_each(&mut self.routes, removed);
            }
            CLUSTER_TYPE => {
                let clusters = self.decode_clusters(&payload)?;
                upsert_each(&mut self.clusters, clusters);
                remove_each(&mut self.clusters, removed);
                self.prune_orphan_endpoints();
            }
            ENDPOINT_TYPE => {
                upsert_each(&mut self.endpoints, decode_endpoints(&payload)?);
                remove_each(&mut self.endpoints, removed);
            }
            _ => return Ok(()),
        }

        let versions = self.versions.entry(type_url.to_string()).or_default();
        for (name, version, _) in resources {
            versions.insert(name.clone(), version.clone());
        }
        for name in removed {
            versions.remove(name);
        }
        Ok(())
    }

    /// Recomputes what the client wants to be subscribed to now that its
    /// resources changed, and reports the difference.
    pub(super) fn refresh_subscriptions(&mut self) -> Vec<SubscriptionChange> {
        [
            (ROUTE_TYPE, self.route_names()),
            (CLUSTER_TYPE, self.cluster_names()),
            (ENDPOINT_TYPE, self.eds_names()),
        ]
        .into_iter()
        .filter_map(|(type_url, desired)| self.set_subscription(type_url, desired))
        .collect()
    }

    fn set_subscription(
        &mut self,
        type_url: &'static str,
        desired: BTreeSet<String>,
    ) -> Option<SubscriptionChange> {
        let current = self.subscriptions.entry(type_url.to_string()).or_default();
        let change = SubscriptionChange {
            type_url,
            desired: desired.iter().cloned().collect(),
            subscribe: desired.difference(current).cloned().collect(),
            unsubscribe: current.difference(&desired).cloned().collect(),
        };
        if change.is_empty() {
            return None;
        }
        *current = desired;
        // A resource we unsubscribed from will never be refreshed again, so its
        // cached version must not be replayed on the next reconnect.
        if let Some(versions) = self.versions.get_mut(type_url) {
            for name in &change.unsubscribe {
                versions.remove(name);
            }
        }
        Some(change)
    }

    /// Projects the cached xDS resources onto dxgate's configuration model and
    /// diffs the result against what was published last, so the store receives
    /// removals for resources this source stopped producing.
    pub(super) fn config_delta(&mut self, version: &str) -> ConfigDelta {
        let listeners = self
            .listeners
            .values()
            .map(|snapshot| {
                let mut listener = snapshot.listener.clone();
                listener.virtual_hosts = snapshot.inline_virtual_hosts.clone();
                for route_name in &snapshot.route_names {
                    if let Some(route) = self.routes.get(route_name) {
                        listener.virtual_hosts.extend(route.virtual_hosts.clone());
                    }
                }
                listener
            })
            .collect();

        let clusters = self
            .clusters
            .values()
            .map(|cluster| Cluster {
                name: cluster.name.clone(),
                endpoints: self
                    .endpoints
                    .get(&cluster.eds_service_name)
                    .cloned()
                    .unwrap_or_default(),
                http2: false,
                tls: cluster.tls.clone(),
                circuit_breaker: cluster.circuit_breaker.clone(),
                outlier_detection: cluster.outlier_detection.clone(),
            })
            .collect();

        let mut providers = BTreeMap::new();
        let mut backends = BTreeMap::new();
        let mut agent_routes = BTreeMap::new();
        let mut policies = BTreeMap::new();
        for route in self.routes.values() {
            for provider in &route.providers {
                providers.insert(provider.name.clone(), provider.clone());
            }
            for backend in &route.backends {
                backends.insert(backend.name.clone(), backend.clone());
            }
            for agent_route in &route.agent_routes {
                agent_routes.insert(agent_route.name.clone(), agent_route.clone());
            }
            for policy in &route.policies {
                policies.insert(policy.name.clone(), policy.clone());
            }
        }

        let delta = ConfigDelta::default()
            .with_version(if version.is_empty() { "ads" } else { version })
            .with_listeners(listeners)
            .with_clusters(clusters)
            .with_providers(providers.into_values().collect())
            .with_backends(backends.into_values().collect())
            .with_agent_routes(agent_routes.into_values().collect())
            .with_policies(policies.into_values().collect());
        self.published.reconcile_delta(delta)
    }

    /// Decodes CDS resources. Clusters are the one type that can carry another
    /// type's payload — an inline `load_assignment` is the endpoint assignment —
    /// so this writes those through as it goes.
    fn decode_clusters(
        &mut self,
        resources: &[prost_types::Any],
    ) -> Result<Vec<(String, ClusterSnapshot)>, XdsError> {
        let mut decoded = Vec::with_capacity(resources.len());
        for resource in resources {
            let cluster = decode_resource::<xds_cluster::Cluster>(CLUSTER_TYPE, resource)?;
            if cluster.name.is_empty() {
                continue;
            }
            let eds_service_name = cluster
                .eds_cluster_config
                .as_ref()
                .filter(|eds| !eds.service_name.is_empty())
                .map(|eds| eds.service_name.clone())
                .unwrap_or_else(|| cluster.name.clone());

            if let Some(load_assignment) = cluster.load_assignment.as_ref() {
                self.endpoints.upsert(
                    eds_service_name.clone(),
                    endpoints_from_assignment(load_assignment),
                );
            }

            decoded.push((
                cluster.name.clone(),
                ClusterSnapshot {
                    tls: upstream_tls_from_cluster(&cluster),
                    circuit_breaker: circuit_breaker_from_cluster(&cluster),
                    outlier_detection: outlier_detection_from_cluster(&cluster),
                    name: cluster.name,
                    eds_service_name,
                },
            ));
        }
        Ok(decoded)
    }

    /// Drops endpoint assignments no remaining cluster points at. Only delta
    /// needs this: the state-of-the-world path replaces the whole set anyway.
    fn prune_orphan_endpoints(&mut self) {
        let live: BTreeSet<String> = self
            .clusters
            .values()
            .map(|cluster| cluster.eds_service_name.clone())
            .collect();
        self.endpoints.retain(|name, _| live.contains(name));
    }

    fn route_names(&self) -> BTreeSet<String> {
        self.listeners
            .values()
            .flat_map(|listener| listener.route_names.iter().cloned())
            .collect()
    }

    fn cluster_names(&self) -> BTreeSet<String> {
        self.routes
            .values()
            .flat_map(|vhosts| {
                vhosts.virtual_hosts.iter().flat_map(|vh| {
                    vh.routes.iter().flat_map(|route| {
                        route
                            .weighted_clusters
                            .iter()
                            .map(|cluster| cluster.name.clone())
                    })
                })
            })
            .collect()
    }

    fn eds_names(&self) -> BTreeSet<String> {
        self.clusters
            .values()
            .map(|cluster| cluster.eds_service_name.clone())
            .collect()
    }
}

fn decode_listeners(
    resources: &[prost_types::Any],
) -> Result<Vec<(String, ListenerSnapshot)>, XdsError> {
    resources
        .iter()
        .map(|resource| {
            let listener = decode_resource::<xds_listener::Listener>(LISTENER_TYPE, resource)?;
            let snapshot = listener_snapshot(listener)?;
            Ok((snapshot.listener.name.clone(), snapshot))
        })
        .collect()
}

fn decode_routes(resources: &[prost_types::Any]) -> Result<Vec<(String, RouteSnapshot)>, XdsError> {
    resources
        .iter()
        .map(|resource| {
            let route = decode_resource::<xds_route::RouteConfiguration>(ROUTE_TYPE, resource)?;
            let snapshot = route_snapshot(&route);
            Ok((route.name, snapshot))
        })
        .collect()
}

fn decode_endpoints(
    resources: &[prost_types::Any],
) -> Result<Vec<(String, Vec<RuntimeEndpoint>)>, XdsError> {
    let mut decoded = Vec::with_capacity(resources.len());
    for resource in resources {
        let assignment =
            decode_resource::<xds_endpoint::ClusterLoadAssignment>(ENDPOINT_TYPE, resource)?;
        if assignment.cluster_name.is_empty() {
            continue;
        }
        decoded.push((
            assignment.cluster_name.clone(),
            endpoints_from_assignment(&assignment),
        ));
    }
    Ok(decoded)
}

fn upsert_each<V: PartialEq>(collection: &mut Collection<String, V>, decoded: Vec<(String, V)>) {
    for (name, value) in decoded {
        collection.upsert(name, value);
    }
}

fn remove_each<V: PartialEq>(collection: &mut Collection<String, V>, removed: &[String]) {
    for name in removed {
        collection.remove(name);
    }
}

/// Applies a state-of-the-world payload.
///
/// A wildcard subscription (no requested names) means the response is the whole
/// world, so it replaces the collection outright. An explicit subscription means
/// the response only speaks for the names asked about, so resources outside the
/// subscription are carried over and the rest is replaced.
fn replace_within_subscription<V: PartialEq>(
    collection: &mut Collection<String, V>,
    received: Vec<(String, V)>,
    requested: &[String],
) {
    if requested.is_empty() {
        collection.replace_all(received);
        return;
    }
    let outside: Vec<String> = collection
        .keys()
        .filter(|name| !requested.contains(name))
        .cloned()
        .collect();
    let mut next: Vec<(String, V)> = received;
    for name in outside {
        if let Some(value) = collection.take(&name) {
            next.push((name, value));
        }
    }
    collection.replace_all(next);
}

#[derive(Debug, Clone, PartialEq)]
struct ListenerSnapshot {
    listener: Listener,
    route_names: Vec<String>,
    inline_virtual_hosts: Vec<VirtualHost>,
}

#[derive(Debug, Clone, PartialEq)]
struct ClusterSnapshot {
    name: String,
    eds_service_name: String,
    tls: Option<UpstreamTls>,
    circuit_breaker: Option<CircuitBreakerConfig>,
    outlier_detection: Option<OutlierDetectionConfig>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RouteSnapshot {
    virtual_hosts: Vec<VirtualHost>,
    providers: Vec<Provider>,
    backends: Vec<Backend>,
    agent_routes: Vec<AgentRoute>,
    policies: Vec<Policy>,
}

fn route_snapshot(route: &xds_route::RouteConfiguration) -> RouteSnapshot {
    let mut snapshot = RouteSnapshot {
        virtual_hosts: convert_virtual_hosts(&route.virtual_hosts),
        ..RouteSnapshot::default()
    };
    let Some(agent) = route.agent_config.as_ref() else {
        return snapshot;
    };
    snapshot.providers = agent
        .providers
        .iter()
        .filter_map(convert_provider)
        .collect();
    snapshot.backends = agent.backends.iter().filter_map(convert_backend).collect();
    snapshot.agent_routes = agent
        .agent_routes
        .iter()
        .filter_map(convert_agent_route)
        .collect();
    snapshot.policies = agent.policies.iter().map(convert_policy).collect();
    snapshot
}

fn convert_provider(provider: &xds_route::AgentProvider) -> Option<Provider> {
    let kind = match xds_route::AgentProviderKind::try_from(provider.kind).ok()? {
        xds_route::AgentProviderKind::Openai => ProviderKind::OpenAi,
        xds_route::AgentProviderKind::Anthropic => ProviderKind::Anthropic,
        xds_route::AgentProviderKind::Unspecified => return None,
    };
    Some(Provider {
        name: provider.name.clone(),
        kind,
        base_url: provider.base_url.clone(),
        api_key_env: None,
        credential_ref: provider.credential.as_ref().map(convert_secret_ref),
        request_headers: provider
            .request_headers
            .iter()
            .map(convert_header_value)
            .collect(),
    })
}

fn convert_backend(backend: &xds_route::AgentBackend) -> Option<Backend> {
    let kind = match backend.backend.as_ref()? {
        xds_route::agent_backend::Backend::Http(http) => BackendKind::Http {
            endpoint: http.endpoint.clone(),
        },
        xds_route::agent_backend::Backend::Llm(llm) => BackendKind::Llm {
            provider: llm.provider.clone(),
            models: llm.models.clone(),
            endpoint: (!llm.endpoint.is_empty()).then(|| llm.endpoint.clone()),
            model_rewrites: llm.model_rewrites.clone().into_iter().collect(),
        },
        xds_route::agent_backend::Backend::Mcp(mcp) => BackendKind::Mcp {
            endpoint: mcp.endpoint.clone(),
            tools: mcp.tools.clone(),
        },
        xds_route::agent_backend::Backend::A2a(a2a) => BackendKind::A2a {
            endpoint: a2a.endpoint.clone(),
            agent: (!a2a.agent.is_empty()).then(|| a2a.agent.clone()),
        },
    };
    Some(Backend {
        name: backend.name.clone(),
        kind,
        policies: backend.policies.clone(),
    })
}

fn convert_agent_route(route: &xds_route::AgentRoute) -> Option<AgentRoute> {
    let protocol = match xds_route::AgentProtocol::try_from(route.protocol).ok()? {
        xds_route::AgentProtocol::Http => AgentProtocol::Http,
        xds_route::AgentProtocol::Llm => AgentProtocol::Llm,
        xds_route::AgentProtocol::Mcp => AgentProtocol::Mcp,
        xds_route::AgentProtocol::A2a => AgentProtocol::A2a,
        xds_route::AgentProtocol::Unspecified => return None,
    };
    Some(AgentRoute {
        name: route.name.clone(),
        protocol,
        matches: route
            .matches
            .iter()
            .filter_map(convert_agent_match)
            .collect(),
        weighted_backends: route
            .weighted_backends
            .iter()
            .map(|backend| WeightedBackend {
                name: backend.name.clone(),
                weight: backend.weight,
            })
            .collect(),
        policies: route.policies.clone(),
        replace_prefix_match: route
            .rewrite
            .as_ref()
            .map(|rewrite| rewrite.replace_prefix_match.clone())
            .filter(|value| !value.is_empty()),
    })
}

fn convert_agent_match(value: &xds_route::AgentRouteMatch) -> Option<AgentRouteMatch> {
    let path = match value.path.as_ref()?.r#match.as_ref()? {
        xds_route::agent_path_match::Match::Prefix(prefix) => PathMatch::Prefix(prefix.clone()),
        xds_route::agent_path_match::Match::Exact(exact) => PathMatch::Exact(exact.clone()),
    };
    Some(AgentRouteMatch {
        path,
        host: optional_string(&value.host),
        method: optional_string(&value.method),
        model: optional_string(&value.model),
        tool: optional_string(&value.tool),
        agent: optional_string(&value.agent),
        headers: value
            .headers
            .iter()
            .map(|header| HeaderMatch {
                name: header.name.clone(),
                value: header.value.clone(),
            })
            .collect(),
    })
}

fn convert_policy(policy: &xds_route::AgentPolicy) -> Policy {
    Policy {
        name: policy.name.clone(),
        action: PolicyAction::Allow,
        matches: None,
        auth: policy.auth.as_ref().map(|auth| AuthPolicy::ApiKey {
            header: if auth.header.is_empty() {
                "authorization".to_string()
            } else {
                auth.header.clone()
            },
            values: Vec::new(),
            value_env: None,
            secret_ref: auth.secret_ref.as_ref().map(convert_secret_ref),
        }),
        rate_limit: policy.rate_limit.as_ref().map(|limit| RateLimitPolicy {
            requests: limit.requests,
            window_seconds: duration_seconds(limit.window.as_ref()).max(1),
            key: convert_rate_limit_key(limit.key),
        }),
        token_limit: policy.token_limit.as_ref().map(|limit| TokenLimitPolicy {
            tokens: limit.tokens,
            window_seconds: duration_seconds(limit.window.as_ref()).max(1),
            key: convert_rate_limit_key(limit.key),
        }),
        timeout_ms: policy.timeout.as_ref().map(duration_millis),
        retry: policy.retry.as_ref().map(|retry| RetryPolicy {
            attempts: retry.attempts,
            statuses: retry
                .status_codes
                .iter()
                .filter_map(|status| u16::try_from(*status).ok())
                .collect(),
        }),
        max_body_bytes: usize::try_from(policy.max_body_bytes)
            .ok()
            .filter(|limit| *limit > 0),
        request_headers: policy
            .request_headers
            .as_ref()
            .map(convert_header_transform)
            .unwrap_or_default(),
        response_headers: policy
            .response_headers
            .as_ref()
            .map(convert_header_transform)
            .unwrap_or_default(),
    }
}

fn convert_secret_ref(reference: &xds_route::SecretKeyReference) -> SecretKeyReference {
    SecretKeyReference {
        namespace: reference.namespace.clone(),
        name: reference.name.clone(),
        key: reference.key.clone(),
    }
}

fn convert_header_transform(transform: &xds_route::AgentHeaderTransform) -> HeaderTransform {
    HeaderTransform {
        add: transform.add.iter().map(convert_header_value).collect(),
        remove: transform.remove.clone(),
    }
}

fn convert_header_value(value: &xds_route::AgentHeaderValue) -> HeaderValue {
    HeaderValue {
        name: value.name.clone(),
        value: value.value.clone(),
    }
}

fn convert_rate_limit_key(value: i32) -> RateLimitKey {
    match xds_route::AgentRateLimitKey::try_from(value)
        .unwrap_or(xds_route::AgentRateLimitKey::Route)
    {
        xds_route::AgentRateLimitKey::Backend => RateLimitKey::Backend,
        xds_route::AgentRateLimitKey::Header => RateLimitKey::Header,
        xds_route::AgentRateLimitKey::Unspecified | xds_route::AgentRateLimitKey::Route => {
            RateLimitKey::Route
        }
    }
}

fn duration_seconds(duration: Option<&prost_types::Duration>) -> u64 {
    duration
        .map(|value| {
            u64::try_from(value.seconds.max(0)).unwrap_or_default() + u64::from(value.nanos > 0)
        })
        .unwrap_or_default()
}

fn duration_millis(duration: &prost_types::Duration) -> u64 {
    u64::try_from(duration.seconds.max(0))
        .unwrap_or_default()
        .saturating_mul(1000)
        .saturating_add(u64::try_from(duration.nanos.max(0)).unwrap_or_default() / 1_000_000)
}

fn optional_string(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

fn upstream_tls_from_cluster(cluster: &xds_cluster::Cluster) -> Option<UpstreamTls> {
    let xds_core::transport_socket::ConfigType::TypedConfig(typed_config) =
        cluster.transport_socket.as_ref()?.config_type.as_ref()?;
    if !typed_config
        .type_url
        .ends_with("extensions.transport_sockets.tls.v1.UpstreamTlsContext")
    {
        return None;
    }
    let tls = xds_tls::UpstreamTlsContext::decode(typed_config.value.as_slice()).ok()?;
    let common = tls.common_tls_context.as_ref();
    let certificate_provider = common.and_then(certificate_provider_name);
    let validation_provider = common.and_then(validation_provider_name);
    let mode = if certificate_provider.is_some() {
        UpstreamTlsMode::DubboMutual
    } else {
        UpstreamTlsMode::Simple
    };
    Some(UpstreamTls {
        mode,
        sni: first_non_empty(tls.sni, cluster_authority(&cluster.name)),
        certificate_provider,
        validation_provider,
        alpn_protocols: common
            .map(|common| common.alpn_protocols.clone())
            .unwrap_or_default(),
        subject_alt_names: common.map(match_subject_alt_names).unwrap_or_default(),
    })
}

fn match_subject_alt_names(common: &xds_tls::CommonTlsContext) -> Vec<String> {
    let Some(xds_tls::common_tls_context::ValidationContextType::CombinedValidationContext(
        combined,
    )) = common.validation_context_type.as_ref()
    else {
        return Vec::new();
    };
    combined
        .default_validation_context
        .as_ref()
        .map(|ctx| {
            ctx.match_subject_alt_names
                .iter()
                .filter(|san| !san.is_empty())
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

fn circuit_breaker_from_cluster(cluster: &xds_cluster::Cluster) -> Option<CircuitBreakerConfig> {
    let threshold = cluster.circuit_breakers.as_ref()?.thresholds.first()?;
    Some(CircuitBreakerConfig {
        max_connections: threshold.max_connections,
        http1_max_pending_requests: threshold.max_pending_requests,
        http2_max_requests: threshold.max_requests,
        max_requests_per_connection: cluster.max_requests_per_connection.as_ref().copied(),
        max_retries: threshold.max_retries,
    })
}

fn outlier_detection_from_cluster(
    cluster: &xds_cluster::Cluster,
) -> Option<OutlierDetectionConfig> {
    let outlier = cluster.outlier_detection.as_ref()?;
    Some(OutlierDetectionConfig {
        consecutive_5xx_errors: outlier.consecutive_5xx,
        interval: outlier.interval.as_ref().map(duration_to_string),
        base_ejection_time: outlier.base_ejection_time.as_ref().map(duration_to_string),
        max_ejection_percent: outlier.max_ejection_percent,
        min_health_percent: outlier.min_health_percent,
    })
}

fn duration_to_string(duration: &prost_types::Duration) -> String {
    if duration.nanos == 0 {
        format!("{}s", duration.seconds)
    } else {
        format!("{}.{:09}s", duration.seconds, duration.nanos)
    }
}

fn certificate_provider_name(common: &xds_tls::CommonTlsContext) -> Option<String> {
    common
        .tls_certificate_certificate_provider_instance
        .as_ref()
        .and_then(instance_name)
}

fn validation_provider_name(common: &xds_tls::CommonTlsContext) -> Option<String> {
    let xds_tls::common_tls_context::ValidationContextType::CombinedValidationContext(combined) =
        common.validation_context_type.as_ref()?;
    combined
        .validation_context_certificate_provider_instance
        .as_ref()
        .and_then(instance_name)
}

fn instance_name(
    instance: &xds_tls::common_tls_context::CertificateProviderInstance,
) -> Option<String> {
    if instance.instance_name.is_empty() {
        None
    } else {
        Some(instance.instance_name.clone())
    }
}

fn cluster_authority(name: &str) -> Option<String> {
    name.split('|')
        .nth(3)
        .filter(|authority| !authority.is_empty())
        .map(ToString::to_string)
}

fn first_non_empty(value: String, fallback: Option<String>) -> Option<String> {
    if value.is_empty() {
        fallback
    } else {
        Some(value)
    }
}

fn listener_snapshot(listener: xds_listener::Listener) -> Result<ListenerSnapshot, XdsError> {
    let port = listener_port(&listener).unwrap_or(80);
    let mut route_names = Vec::new();
    let mut inline_virtual_hosts = Vec::new();

    for hcm in http_connection_managers(&listener)? {
        match hcm.route_specifier {
            Some(xds_hcm::http_connection_manager::RouteSpecifier::Rds(rds))
                if !rds.route_config_name.is_empty() =>
            {
                route_names.push(rds.route_config_name);
            }
            Some(xds_hcm::http_connection_manager::RouteSpecifier::RouteConfig(route_config)) => {
                inline_virtual_hosts.extend(convert_virtual_hosts(&route_config.virtual_hosts));
            }
            _ => {}
        }
    }

    Ok(ListenerSnapshot {
        listener: Listener {
            name: listener.name,
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port),
            protocol: if port == 443 {
                ListenerProtocol::Https
            } else {
                ListenerProtocol::Http
            },
            virtual_hosts: Vec::new(),
            tls_secret: None,
        },
        route_names: sorted_unique(route_names),
        inline_virtual_hosts,
    })
}

fn http_connection_managers(
    listener: &xds_listener::Listener,
) -> Result<Vec<xds_hcm::HttpConnectionManager>, XdsError> {
    let mut managers = Vec::new();

    if let Some(api_listener) = listener.api_listener.as_ref() {
        if let Some(any) = api_listener.api_listener.as_ref() {
            managers.push(decode_hcm(any)?);
        }
    }

    for chain in &listener.filter_chains {
        for filter in &chain.filters {
            let Some(xds_listener::filter::ConfigType::TypedConfig(any)) =
                filter.config_type.as_ref()
            else {
                continue;
            };
            if is_http_connection_manager(&filter.name, any) {
                managers.push(decode_hcm(any)?);
            }
        }
    }

    Ok(managers)
}

fn is_http_connection_manager(name: &str, any: &prost_types::Any) -> bool {
    name.contains("http_connection_manager")
        || any.type_url.ends_with(
            "extensions.filters.network.http_connection_manager.v1.HttpConnectionManager",
        )
}

fn decode_hcm(any: &prost_types::Any) -> Result<xds_hcm::HttpConnectionManager, XdsError> {
    decode_resource("type.googleapis.com/extensions.filters.network.http_connection_manager.v1.HttpConnectionManager", any)
}

fn convert_virtual_hosts(vhosts: &[xds_route::VirtualHost]) -> Vec<VirtualHost> {
    vhosts
        .iter()
        .map(|vh| VirtualHost {
            name: vh.name.clone(),
            domains: vh.domains.clone(),
            routes: vh.routes.iter().filter_map(convert_route).collect(),
        })
        .collect()
}

fn convert_route(route: &xds_route::Route) -> Option<Route> {
    let weighted_clusters = match route.action.as_ref()? {
        xds_route::route::Action::Route(action) => convert_route_action(action),
        xds_route::route::Action::NonForwardingAction(_) => Vec::new(),
    };
    if weighted_clusters.is_empty() {
        return None;
    }

    let matches = match route.r#match.as_ref() {
        Some(route_match) => vec![convert_route_match(route_match)?],
        None => Vec::new(),
    };

    Some(Route {
        name: route.name.clone(),
        matches,
        weighted_clusters,
    })
}

fn convert_route_action(action: &xds_route::RouteAction) -> Vec<WeightedCluster> {
    match action.cluster_specifier.as_ref() {
        Some(xds_route::route_action::ClusterSpecifier::Cluster(name)) if !name.is_empty() => {
            vec![WeightedCluster {
                name: name.clone(),
                weight: 100,
            }]
        }
        Some(xds_route::route_action::ClusterSpecifier::WeightedClusters(weighted)) => weighted
            .clusters
            .iter()
            .filter(|cluster| !cluster.name.is_empty())
            .map(|cluster| WeightedCluster {
                name: cluster.name.clone(),
                weight: cluster.weight.unwrap_or(1),
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn convert_route_match(route_match: &xds_route::RouteMatch) -> Option<RouteMatch> {
    let path = match route_match.path_specifier.as_ref() {
        Some(xds_route::route_match::PathSpecifier::Prefix(prefix)) => {
            PathMatch::Prefix(prefix.clone())
        }
        Some(xds_route::route_match::PathSpecifier::Path(path)) => PathMatch::Exact(path.clone()),
        Some(xds_route::route_match::PathSpecifier::SafeRegex(_)) => return None,
        None => PathMatch::Prefix("/".to_string()),
    };

    let mut headers = Vec::new();
    for header in &route_match.headers {
        match header.header_match_specifier.as_ref() {
            Some(xds_route::header_matcher::HeaderMatchSpecifier::ExactMatch(value)) => {
                headers.push(HeaderMatch {
                    name: header.name.clone(),
                    value: value.clone(),
                });
            }
            Some(xds_route::header_matcher::HeaderMatchSpecifier::SafeRegexMatch(_)) => {
                return None;
            }
            None => {}
        }
    }

    Some(RouteMatch { path, headers })
}

fn endpoints_from_assignment(
    assignment: &xds_endpoint::ClusterLoadAssignment,
) -> Vec<RuntimeEndpoint> {
    let mut endpoints = Vec::new();
    for locality in &assignment.endpoints {
        for lb_endpoint in &locality.lb_endpoints {
            let Some(xds_endpoint::lb_endpoint::HostIdentifier::Endpoint(endpoint)) =
                lb_endpoint.host_identifier.as_ref()
            else {
                continue;
            };
            let Some((address, port)) = socket_address(endpoint.address.as_ref()) else {
                continue;
            };
            endpoints.push(RuntimeEndpoint {
                address,
                port,
                healthy: endpoint_is_healthy(lb_endpoint.health_status),
                node_name: None,
            });
        }
    }
    endpoints.sort_by(|a, b| a.address.cmp(&b.address).then_with(|| a.port.cmp(&b.port)));
    endpoints
}

fn endpoint_is_healthy(status: i32) -> bool {
    !matches!(
        xds_core::HealthStatus::try_from(status).unwrap_or(xds_core::HealthStatus::Unknown),
        xds_core::HealthStatus::Unhealthy
            | xds_core::HealthStatus::Draining
            | xds_core::HealthStatus::Timeout
    )
}

fn listener_port(listener: &xds_listener::Listener) -> Option<u16> {
    socket_address(listener.address.as_ref())
        .map(|(_, port)| port)
        .or_else(|| {
            listener
                .name
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>().ok())
        })
}

fn socket_address(address: Option<&xds_core::Address>) -> Option<(String, u16)> {
    let Some(xds_core::address::Address::SocketAddress(socket)) =
        address.and_then(|address| address.address.as_ref())
    else {
        return None;
    };
    let Some(xds_core::socket_address::PortSpecifier::PortValue(port)) = socket.port_specifier
    else {
        return None;
    };
    Some((socket.address.clone(), u16::try_from(port).ok()?))
}

fn decode_resource<T: Message + Default>(
    type_url: &str,
    resource: &prost_types::Any,
) -> Result<T, XdsError> {
    T::decode(resource.value.as_slice()).map_err(|source| XdsError::Decode {
        type_url: type_url.to_string(),
        source,
    })
}

fn sorted_unique(names: impl IntoIterator<Item = String>) -> Vec<String> {
    names
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::core::v1::{address, socket_address, Address, SocketAddress};
    use dxgate_core::{ResourceKey, ResourceKind, RouterIdentity};
    use prost_types::Any;
    use std::collections::HashMap;

    #[test]
    fn identity_metadata_selects_dubbod_grpc_generator() {
        let identity = RouterIdentity {
            pod_name: "dxgate-abc".into(),
            namespace: "app".into(),
            pod_ip: "10.0.0.10".into(),
            node_name: Some("node-a".into()),
            cluster_id: "Kubernetes".into(),
            dns_domain: "svc.cluster.local".into(),
        };

        assert_eq!(identity.metadata().generator, "grpc");
    }

    #[test]
    fn ads_state_builds_runtime_config_from_lds_rds_cds_and_eds() {
        let route_name = "outbound|80||orders.app.svc.cluster.local";
        let cluster_name = "outbound|8080||orders.app.svc.cluster.local";
        let listener = xds_listener::Listener {
            name: "dxgate.app.svc.cluster.local:80".into(),
            address: Some(socket("10.96.0.10", 80)),
            api_listener: Some(xds_listener::ApiListener {
                api_listener: Some(any(
                    "type.googleapis.com/extensions.filters.network.http_connection_manager.v1.HttpConnectionManager",
                    xds_hcm::HttpConnectionManager {
                        route_specifier: Some(
                            xds_hcm::http_connection_manager::RouteSpecifier::Rds(xds_hcm::Rds {
                                route_config_name: route_name.into(),
                                config_source: None,
                            }),
                        ),
                        ..xds_hcm::HttpConnectionManager::default()
                    },
                )),
            }),
            ..xds_listener::Listener::default()
        };
        let route = xds_route::RouteConfiguration {
            name: route_name.into(),
            virtual_hosts: vec![xds_route::VirtualHost {
                name: "orders".into(),
                domains: vec!["orders.example.com".into()],
                routes: vec![xds_route::Route {
                    name: "orders-default".into(),
                    r#match: Some(xds_route::RouteMatch {
                        path_specifier: Some(xds_route::route_match::PathSpecifier::Prefix(
                            "/".into(),
                        )),
                        headers: Vec::new(),
                    }),
                    action: Some(xds_route::route::Action::Route(xds_route::RouteAction {
                        cluster_specifier: Some(
                            xds_route::route_action::ClusterSpecifier::Cluster(cluster_name.into()),
                        ),
                    })),
                }],
            }],
            agent_config: None,
        };
        let cluster = xds_cluster::Cluster {
            name: cluster_name.into(),
            eds_cluster_config: Some(xds_cluster::cluster::EdsClusterConfig {
                service_name: cluster_name.into(),
                eds_config: None,
            }),
            transport_socket: Some(xds_core::TransportSocket {
                name: "envoy.transport_sockets.tls".into(),
                config_type: Some(xds_core::transport_socket::ConfigType::TypedConfig(any(
                    "type.googleapis.com/extensions.transport_sockets.tls.v1.UpstreamTlsContext",
                    xds_tls::UpstreamTlsContext {
                        sni: "orders.app.svc.cluster.local".into(),
                        common_tls_context: Some(xds_tls::CommonTlsContext {
                            tls_certificate_certificate_provider_instance: Some(
                                xds_tls::common_tls_context::CertificateProviderInstance {
                                    instance_name: "workload".into(),
                                    certificate_name: "default".into(),
                                },
                            ),
                            alpn_protocols: vec!["h2".into()],
                            validation_context_type: Some(
                                xds_tls::common_tls_context::ValidationContextType::CombinedValidationContext(
                                    xds_tls::common_tls_context::CombinedCertificateValidationContext {
                                        validation_context_certificate_provider_instance: Some(
                                            xds_tls::common_tls_context::CertificateProviderInstance {
                                                instance_name: "roots".into(),
                                                certificate_name: "ROOTCA".into(),
                                            },
                                        ),
                                        default_validation_context: None,
                                    },
                                ),
                            ),
                        }),
                    },
                ))),
            }),
            ..xds_cluster::Cluster::default()
        };
        let assignment = xds_endpoint::ClusterLoadAssignment {
            cluster_name: cluster_name.into(),
            endpoints: vec![xds_endpoint::LocalityLbEndpoints {
                lb_endpoints: vec![xds_endpoint::LbEndpoint {
                    host_identifier: Some(xds_endpoint::lb_endpoint::HostIdentifier::Endpoint(
                        xds_endpoint::Endpoint {
                            address: Some(socket("10.244.0.20", 8080)),
                        },
                    )),
                    health_status: xds_core::HealthStatus::Healthy as i32,
                    ..xds_endpoint::LbEndpoint::default()
                }],
                ..xds_endpoint::LocalityLbEndpoints::default()
            }],
        };

        let mut state = AdsState::default();
        state.begin_stream(vec!["dxgate.app.svc.cluster.local:80".into()]);

        state
            .apply_sotw(&response(LISTENER_TYPE, vec![any(LISTENER_TYPE, listener)]))
            .unwrap();
        let changes = state.refresh_subscriptions();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].type_url, ROUTE_TYPE);
        assert_eq!(changes[0].subscribe, [route_name]);

        state
            .apply_sotw(&response(ROUTE_TYPE, vec![any(ROUTE_TYPE, route)]))
            .unwrap();
        let changes = state.refresh_subscriptions();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].type_url, CLUSTER_TYPE);
        assert_eq!(changes[0].subscribe, [cluster_name]);

        state
            .apply_sotw(&response(CLUSTER_TYPE, vec![any(CLUSTER_TYPE, cluster)]))
            .unwrap();
        let changes = state.refresh_subscriptions();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].type_url, ENDPOINT_TYPE);
        assert_eq!(changes[0].subscribe, [cluster_name]);

        state
            .apply_sotw(&response(
                ENDPOINT_TYPE,
                vec![any(ENDPOINT_TYPE, assignment)],
            ))
            .unwrap();
        assert!(state.refresh_subscriptions().is_empty());

        let delta = state.config_delta("v1");
        assert_eq!(delta.version.as_deref(), Some("v1"));
        assert!(delta.removes.is_empty());
        assert_eq!(delta.listeners[0].bind, "0.0.0.0:80".parse().unwrap());
        assert_eq!(
            delta.listeners[0].virtual_hosts[0].domains,
            ["orders.example.com"]
        );
        assert_eq!(delta.clusters[0].endpoints[0].address, "10.244.0.20");
        let tls = delta.clusters[0].tls.as_ref().unwrap();
        assert_eq!(tls.sni.as_deref(), Some("orders.app.svc.cluster.local"));
        assert_eq!(tls.mode, UpstreamTlsMode::DubboMutual);
        assert_eq!(tls.certificate_provider.as_deref(), Some("workload"));
        assert_eq!(tls.validation_provider.as_deref(), Some("roots"));
        assert_eq!(tls.alpn_protocols, ["h2"]);

        // Everything the projection produced is now attributed to this source,
        // which is what lets the next update express a removal.
        let republished = state.config_delta("v1");
        assert!(republished.removes.is_empty());
        assert_eq!(republished.listeners.len(), 1);
    }

    #[test]
    fn rds_agent_config_projects_mesh_ai_resources() {
        let route = xds_route::RouteConfiguration {
            name: "gateway.app:80".into(),
            virtual_hosts: Vec::new(),
            agent_config: Some(xds_route::AgentConfig {
                providers: vec![xds_route::AgentProvider {
                    name: "chat.app.provider".into(),
                    kind: xds_route::AgentProviderKind::Anthropic as i32,
                    base_url: "http://mock-ai.app.svc:8080".into(),
                    credential: Some(xds_route::SecretKeyReference {
                        namespace: "app".into(),
                        name: "ai-key".into(),
                        key: "token".into(),
                    }),
                    request_headers: vec![xds_route::AgentHeaderValue {
                        name: "x-tenant".into(),
                        value: "mesh".into(),
                    }],
                }],
                backends: vec![xds_route::AgentBackend {
                    name: "chat.app".into(),
                    policies: vec!["chat.app.policy".into()],
                    backend: Some(xds_route::agent_backend::Backend::Llm(
                        xds_route::LlmBackend {
                            provider: "chat.app.provider".into(),
                            models: vec!["claude-mock".into()],
                            endpoint: String::new(),
                            model_rewrites: HashMap::from([("chat".into(), "claude-mock".into())]),
                        },
                    )),
                }],
                agent_routes: vec![xds_route::AgentRoute {
                    name: "app/chat/0".into(),
                    protocol: xds_route::AgentProtocol::Llm as i32,
                    matches: vec![xds_route::AgentRouteMatch {
                        path: Some(xds_route::AgentPathMatch {
                            r#match: Some(xds_route::agent_path_match::Match::Prefix(
                                "/anthropic".into(),
                            )),
                        }),
                        host: "ai.example.test".into(),
                        method: "POST".into(),
                        ..xds_route::AgentRouteMatch::default()
                    }],
                    weighted_backends: vec![xds_route::WeightedBackend {
                        name: "chat.app".into(),
                        weight: 100,
                    }],
                    policies: vec!["chat.app.policy".into()],
                    rewrite: Some(xds_route::PathRewrite {
                        replace_prefix_match: "/v1".into(),
                    }),
                }],
                policies: vec![xds_route::AgentPolicy {
                    name: "chat.app.policy".into(),
                    auth: Some(xds_route::ClientAuthPolicy {
                        header: "x-api-key".into(),
                        secret_ref: Some(xds_route::SecretKeyReference {
                            namespace: "app".into(),
                            name: "client-key".into(),
                            key: "token".into(),
                        }),
                    }),
                    timeout: Some(prost_types::Duration {
                        seconds: 2,
                        nanos: 500_000_000,
                    }),
                    retry: Some(xds_route::AgentRetryPolicy {
                        attempts: 2,
                        status_codes: vec![502, 503],
                    }),
                    max_body_bytes: 4096,
                    ..xds_route::AgentPolicy::default()
                }],
            }),
        };

        let snapshot = route_snapshot(&route);
        assert_eq!(snapshot.providers[0].kind, ProviderKind::Anthropic);
        assert_eq!(
            snapshot.providers[0].credential_ref,
            Some(SecretKeyReference {
                namespace: "app".into(),
                name: "ai-key".into(),
                key: "token".into(),
            })
        );
        assert_eq!(snapshot.backends[0].name, "chat.app");
        assert_eq!(snapshot.agent_routes[0].protocol, AgentProtocol::Llm);
        assert_eq!(
            snapshot.agent_routes[0].replace_prefix_match.as_deref(),
            Some("/v1")
        );
        assert_eq!(snapshot.policies[0].timeout_ms, Some(2500));
        assert_eq!(snapshot.policies[0].max_body_bytes, Some(4096));
    }

    #[test]
    fn a_reconnect_re_requests_the_derived_subscriptions() {
        let mut state = AdsState::default();
        state.begin_stream(vec![]);
        state
            .apply_delta(
                CLUSTER_TYPE,
                &[(
                    "orders".to_string(),
                    "1".to_string(),
                    any(
                        CLUSTER_TYPE,
                        xds_cluster::Cluster {
                            name: "orders".into(),
                            ..xds_cluster::Cluster::default()
                        },
                    ),
                )],
                &[],
            )
            .unwrap();
        let changes = state.refresh_subscriptions();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].type_url, ENDPOINT_TYPE);
        // Steady state: nothing to change.
        assert!(state.refresh_subscriptions().is_empty());

        // The stream drops and comes back. The desired set is identical, but the
        // server has no memory of it, so it must be sent again.
        state.begin_stream(vec![]);
        let changes = state.refresh_subscriptions();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].type_url, ENDPOINT_TYPE);
        assert_eq!(changes[0].subscribe, ["orders"]);
        // Cached versions survive so the delta stream can skip unchanged resources.
        assert_eq!(
            state.initial_resource_versions(CLUSTER_TYPE),
            BTreeMap::from([("orders".to_string(), "1".to_string())])
        );
    }

    #[test]
    fn delta_removals_retire_the_derived_resources() {
        let mut state = AdsState::default();
        let cluster = |name: &str| xds_cluster::Cluster {
            name: name.into(),
            ..xds_cluster::Cluster::default()
        };

        state
            .apply_delta(
                CLUSTER_TYPE,
                &[
                    (
                        "orders".to_string(),
                        "1".to_string(),
                        any(CLUSTER_TYPE, cluster("orders")),
                    ),
                    (
                        "ratings".to_string(),
                        "1".to_string(),
                        any(CLUSTER_TYPE, cluster("ratings")),
                    ),
                ],
                &[],
            )
            .unwrap();
        let delta = state.config_delta("v1");
        assert_eq!(delta.clusters.len(), 2);
        assert!(delta.removes.is_empty());

        // A reconnect replays exactly the versions the server acknowledged.
        assert_eq!(
            state.initial_resource_versions(CLUSTER_TYPE),
            BTreeMap::from([
                ("orders".to_string(), "1".to_string()),
                ("ratings".to_string(), "1".to_string()),
            ])
        );

        state
            .apply_delta(CLUSTER_TYPE, &[], &["ratings".to_string()])
            .unwrap();
        let delta = state.config_delta("v2");
        assert_eq!(delta.clusters.len(), 1);
        assert_eq!(delta.clusters[0].name, "orders");
        assert_eq!(
            delta.removes,
            vec![ResourceKey::new(ResourceKind::Cluster, "ratings")]
        );
        assert!(!state
            .initial_resource_versions(CLUSTER_TYPE)
            .contains_key("ratings"));
    }

    #[test]
    fn delta_endpoint_updates_do_not_disturb_other_clusters() {
        let mut state = AdsState::default();
        for name in ["orders", "ratings"] {
            state
                .apply_delta(
                    CLUSTER_TYPE,
                    &[(
                        name.to_string(),
                        "1".to_string(),
                        any(
                            CLUSTER_TYPE,
                            xds_cluster::Cluster {
                                name: name.into(),
                                ..xds_cluster::Cluster::default()
                            },
                        ),
                    )],
                    &[],
                )
                .unwrap();
        }
        state
            .apply_delta(
                ENDPOINT_TYPE,
                &[(
                    "orders".to_string(),
                    "1".to_string(),
                    any(
                        ENDPOINT_TYPE,
                        xds_endpoint::ClusterLoadAssignment {
                            cluster_name: "orders".into(),
                            endpoints: vec![xds_endpoint::LocalityLbEndpoints {
                                lb_endpoints: vec![lb_endpoint(
                                    "10.244.0.20",
                                    8080,
                                    xds_core::HealthStatus::Healthy,
                                )],
                                ..xds_endpoint::LocalityLbEndpoints::default()
                            }],
                        },
                    ),
                )],
                &[],
            )
            .unwrap();

        let delta = state.config_delta("v1");
        let orders = delta
            .clusters
            .iter()
            .find(|cluster| cluster.name == "orders")
            .unwrap();
        let ratings = delta
            .clusters
            .iter()
            .find(|cluster| cluster.name == "ratings")
            .unwrap();
        assert_eq!(orders.endpoints.len(), 1);
        assert!(ratings.endpoints.is_empty());
    }

    #[test]
    fn upstream_tls_without_workload_certificate_provider_is_simple() {
        let cluster = xds_cluster::Cluster {
            name: "outbound|443||httpbin-egress.app.svc.cluster.local".into(),
            transport_socket: Some(xds_core::TransportSocket {
                name: "envoy.transport_sockets.tls".into(),
                config_type: Some(xds_core::transport_socket::ConfigType::TypedConfig(any(
                    "type.googleapis.com/extensions.transport_sockets.tls.v1.UpstreamTlsContext",
                    xds_tls::UpstreamTlsContext {
                        sni: "httpbin.org".into(),
                        common_tls_context: Some(xds_tls::CommonTlsContext::default()),
                    },
                ))),
            }),
            ..xds_cluster::Cluster::default()
        };

        let tls = upstream_tls_from_cluster(&cluster).expect("expected TLS config");

        assert_eq!(tls.mode, UpstreamTlsMode::Simple);
        assert_eq!(tls.sni.as_deref(), Some("httpbin.org"));
        assert_eq!(tls.certificate_provider, None);
        assert_eq!(tls.validation_provider, None);
    }

    #[test]
    fn upstream_tls_carries_match_subject_alt_names() {
        let cluster = tls_cluster(xds_tls::CommonTlsContext {
            tls_certificate_certificate_provider_instance: Some(
                xds_tls::common_tls_context::CertificateProviderInstance {
                    instance_name: "workload".into(),
                    certificate_name: "default".into(),
                },
            ),
            alpn_protocols: vec![],
            validation_context_type: Some(
                xds_tls::common_tls_context::ValidationContextType::CombinedValidationContext(
                    xds_tls::common_tls_context::CombinedCertificateValidationContext {
                        validation_context_certificate_provider_instance: Some(
                            xds_tls::common_tls_context::CertificateProviderInstance {
                                instance_name: "roots".into(),
                                certificate_name: "ROOTCA".into(),
                            },
                        ),
                        default_validation_context: Some(xds_tls::CertificateValidationContext {
                            match_subject_alt_names: vec![
                                "spiffe://cluster.local/ns/app/sa/orders".into(),
                                // Empty entries would pin an identity nothing can
                                // present, silently failing every handshake.
                                String::new(),
                            ],
                            trusted_ca: None,
                        }),
                    },
                ),
            ),
        });

        let tls = upstream_tls_from_cluster(&cluster).expect("expected TLS config");

        assert_eq!(
            tls.subject_alt_names,
            ["spiffe://cluster.local/ns/app/sa/orders"]
        );
    }

    #[test]
    fn upstream_tls_without_validation_context_has_no_subject_alt_names() {
        let cluster = tls_cluster(xds_tls::CommonTlsContext::default());

        let tls = upstream_tls_from_cluster(&cluster).expect("expected TLS config");

        assert!(tls.subject_alt_names.is_empty());
    }

    #[test]
    fn circuit_breaker_reads_the_first_threshold() {
        let cluster = xds_cluster::Cluster {
            name: "outbound|8080||orders.app.svc.cluster.local".into(),
            circuit_breakers: Some(xds_cluster::cluster::CircuitBreakers {
                thresholds: vec![xds_cluster::cluster::circuit_breakers::Thresholds {
                    max_connections: Some(32),
                    max_pending_requests: Some(64),
                    max_requests: Some(128),
                    max_retries: Some(3),
                    track_remaining: false,
                }],
            }),
            max_requests_per_connection: Some(100),
            ..xds_cluster::Cluster::default()
        };

        let breaker = circuit_breaker_from_cluster(&cluster).expect("expected circuit breaker");

        assert_eq!(breaker.max_connections, Some(32));
        assert_eq!(breaker.http1_max_pending_requests, Some(64));
        assert_eq!(breaker.http2_max_requests, Some(128));
        assert_eq!(breaker.max_requests_per_connection, Some(100));
        assert_eq!(breaker.max_retries, Some(3));
        // http2_max_requests wins the concurrency limit the proxy actually enforces.
        assert_eq!(breaker.concurrent_request_limit(), Some(128));
    }

    #[test]
    fn circuit_breaker_is_absent_without_thresholds() {
        let cluster = xds_cluster::Cluster {
            circuit_breakers: Some(xds_cluster::cluster::CircuitBreakers { thresholds: vec![] }),
            ..xds_cluster::Cluster::default()
        };

        assert!(circuit_breaker_from_cluster(&cluster).is_none());
    }

    #[test]
    fn outlier_detection_converts_durations() {
        let cluster = xds_cluster::Cluster {
            outlier_detection: Some(xds_cluster::cluster::OutlierDetection {
                consecutive_5xx: Some(3),
                interval: Some(prost_types::Duration {
                    seconds: 10,
                    nanos: 0,
                }),
                base_ejection_time: Some(prost_types::Duration {
                    seconds: 0,
                    nanos: 500_000_000,
                }),
                max_ejection_percent: Some(50),
                min_health_percent: Some(40),
                ..xds_cluster::cluster::OutlierDetection::default()
            }),
            ..xds_cluster::Cluster::default()
        };

        let outlier = outlier_detection_from_cluster(&cluster).expect("expected outlier detection");

        assert_eq!(outlier.consecutive_5xx_errors, Some(3));
        assert_eq!(outlier.interval.as_deref(), Some("10s"));
        // Sub-second windows must survive the round-trip through the string form the
        // proxy parses back; truncating to "0s" would disable ejection entirely.
        assert_eq!(outlier.base_ejection_time.as_deref(), Some("0.500000000s"));
        assert_eq!(outlier.max_ejection_percent, Some(50));
        assert_eq!(outlier.min_health_percent, Some(40));
    }

    #[test]
    fn unhealthy_and_draining_endpoints_are_marked_unhealthy() {
        let assignment = xds_endpoint::ClusterLoadAssignment {
            cluster_name: "outbound|8080||orders.app.svc.cluster.local".into(),
            endpoints: vec![xds_endpoint::LocalityLbEndpoints {
                lb_endpoints: vec![
                    lb_endpoint("10.0.0.1", 8080, xds_core::HealthStatus::Healthy),
                    lb_endpoint("10.0.0.2", 8080, xds_core::HealthStatus::Unhealthy),
                    lb_endpoint("10.0.0.3", 8080, xds_core::HealthStatus::Draining),
                    lb_endpoint("10.0.0.4", 8080, xds_core::HealthStatus::Timeout),
                    // Dubbod omits the status for endpoints it considers healthy.
                    lb_endpoint("10.0.0.5", 8080, xds_core::HealthStatus::Unknown),
                ],
                ..xds_endpoint::LocalityLbEndpoints::default()
            }],
        };

        let endpoints = endpoints_from_assignment(&assignment);

        let healthy: Vec<_> = endpoints
            .iter()
            .filter(|endpoint| endpoint.healthy)
            .map(|endpoint| endpoint.address.as_str())
            .collect();
        assert_eq!(healthy, ["10.0.0.1", "10.0.0.5"]);
        // Unhealthy endpoints stay in the config so readiness and debug endpoints can
        // show them; the picker is what filters them out.
        assert_eq!(endpoints.len(), 5);
    }

    #[test]
    fn route_match_rejects_regex_and_defaults_a_missing_path() {
        let exact = convert_route_match(&xds_route::RouteMatch {
            path_specifier: Some(xds_route::route_match::PathSpecifier::Path(
                "/orders".into(),
            )),
            headers: vec![xds_route::HeaderMatcher {
                name: "x-env".into(),
                header_match_specifier: Some(
                    xds_route::header_matcher::HeaderMatchSpecifier::ExactMatch("prod".into()),
                ),
            }],
        })
        .expect("exact path should convert");
        assert_eq!(exact.path, PathMatch::Exact("/orders".into()));
        assert_eq!(exact.headers[0].name, "x-env");
        assert_eq!(exact.headers[0].value, "prod");

        let defaulted = convert_route_match(&xds_route::RouteMatch {
            path_specifier: None,
            headers: vec![],
        })
        .expect("missing path specifier should default to a prefix match");
        assert_eq!(defaulted.path, PathMatch::Prefix("/".into()));

        // dxgate has no regex matcher, so a regex route must be dropped rather than
        // silently widened into a prefix that matches more traffic than intended.
        assert!(convert_route_match(&xds_route::RouteMatch {
            path_specifier: Some(xds_route::route_match::PathSpecifier::SafeRegex(
                Default::default()
            )),
            headers: vec![],
        })
        .is_none());
    }

    fn tls_cluster(common: xds_tls::CommonTlsContext) -> xds_cluster::Cluster {
        xds_cluster::Cluster {
            name: "outbound|8080||orders.app.svc.cluster.local".into(),
            transport_socket: Some(xds_core::TransportSocket {
                name: "envoy.transport_sockets.tls".into(),
                config_type: Some(xds_core::transport_socket::ConfigType::TypedConfig(any(
                    "type.googleapis.com/extensions.transport_sockets.tls.v1.UpstreamTlsContext",
                    xds_tls::UpstreamTlsContext {
                        sni: "orders.app.svc.cluster.local".into(),
                        common_tls_context: Some(common),
                    },
                ))),
            }),
            ..xds_cluster::Cluster::default()
        }
    }

    fn lb_endpoint(
        address: &str,
        port: u32,
        status: xds_core::HealthStatus,
    ) -> xds_endpoint::LbEndpoint {
        xds_endpoint::LbEndpoint {
            host_identifier: Some(xds_endpoint::lb_endpoint::HostIdentifier::Endpoint(
                xds_endpoint::Endpoint {
                    address: Some(socket(address, port)),
                },
            )),
            health_status: status as i32,
            ..xds_endpoint::LbEndpoint::default()
        }
    }

    fn response(type_url: &str, resources: Vec<Any>) -> DiscoveryResponse {
        DiscoveryResponse {
            version_info: "v1".into(),
            resources,
            canary: false,
            type_url: type_url.into(),
            nonce: "nonce".into(),
            control_plane: None,
        }
    }

    fn any<T: Message>(type_url: &str, message: T) -> Any {
        Any {
            type_url: type_url.into(),
            value: message.encode_to_vec(),
        }
    }

    fn socket(address_value: &str, port: u32) -> Address {
        Address {
            address: Some(address::Address::SocketAddress(SocketAddress {
                address: address_value.into(),
                port_specifier: Some(socket_address::PortSpecifier::PortValue(port)),
            })),
        }
    }
}
