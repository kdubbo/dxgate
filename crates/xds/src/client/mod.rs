//! The ADS client.
//!
//! dxgate prefers the incremental protocol (`DeltaAggregatedResources`): the
//! control plane sends only the resources that changed plus the names it
//! retired, and a reconnecting client replays `initial_resource_versions` so the
//! server can skip everything it already has. Not every control plane implements
//! it, so a stream that comes back `UNIMPLEMENTED` falls back to
//! state-of-the-world (`StreamAggregatedResources`) for the rest of the process
//! lifetime.
//!
//! Both flavours feed the same [`AdsState`], which projects the raw xDS
//! resources onto dxgate's configuration model and writes the result into the
//! shared [`ConfigStore`] as the [`SourceId::Xds`] slice. The client never
//! touches resources owned by another source.

mod state;

use crate::proto::core::v1 as xds_core;
use crate::proto::service::discovery::v1::aggregated_discovery_service_client::AggregatedDiscoveryServiceClient;
use crate::proto::service::discovery::v1::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest,
};
use dxgate_core::{ConfigStore, RouterIdentity, SourceId};
use prost_types::{value::Kind, Struct, Value};
use state::{AdsState, LISTENER_TYPE};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;
use tracing::{debug, info, warn};

const MAX_DECODING_MESSAGE_SIZE: usize = 32 * 1024 * 1024;
const REQUEST_CHANNEL_CAPACITY: usize = 32;

#[derive(Debug, Error)]
pub enum XdsError {
    #[error("invalid xDS endpoint {endpoint}: {source}")]
    InvalidEndpoint {
        endpoint: String,
        source: tonic::transport::Error,
    },

    #[error("failed connecting to xDS endpoint {endpoint}: {source}")]
    Connect {
        endpoint: String,
        source: tonic::transport::Error,
    },

    #[error("failed opening ADS stream: {0}")]
    StreamOpen(Box<tonic::Status>),

    #[error("ADS stream receive failed: {0}")]
    StreamReceive(Box<tonic::Status>),

    #[error("ADS request channel is closed")]
    RequestChannelClosed,

    #[error("failed decoding {type_url} resource: {source}")]
    Decode {
        type_url: String,
        source: prost::DecodeError,
    },

    /// The control plane does not implement `DeltaAggregatedResources`. Handled
    /// internally by falling back to state-of-the-world ADS.
    #[error("control plane does not implement delta ADS")]
    DeltaUnsupported,
}

#[derive(Debug, Clone)]
pub struct XdsClientConfig {
    pub endpoint: String,
    pub identity: RouterIdentity,
    pub listener_names: Vec<String>,
    pub reconnect_delay: Duration,
}

/// Which ADS flavour the client is speaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamMode {
    Delta,
    StateOfTheWorld,
}

pub struct XdsClient {
    cfg: XdsClientConfig,
}

impl XdsClient {
    pub fn new(cfg: XdsClientConfig) -> Self {
        Self { cfg }
    }

    pub async fn connect_channel(&self) -> Result<Channel, XdsError> {
        let endpoint = Endpoint::from_shared(self.cfg.endpoint.clone()).map_err(|source| {
            XdsError::InvalidEndpoint {
                endpoint: self.cfg.endpoint.clone(),
                source,
            }
        })?;

        endpoint
            .connect()
            .await
            .map_err(|source| XdsError::Connect {
                endpoint: self.cfg.endpoint.clone(),
                source,
            })
    }

    /// Runs until the endpoint itself is unusable, reconnecting with a fixed
    /// delay in between. The resource cache survives reconnects so a control
    /// plane blip neither blanks the data plane nor forces a full resync.
    pub async fn run(self, store: Arc<ConfigStore>) -> Result<(), XdsError> {
        let mut state = AdsState::default();
        let mut mode = StreamMode::Delta;

        loop {
            let result = match mode {
                StreamMode::Delta => self.run_delta(&store, &mut state).await,
                StreamMode::StateOfTheWorld => self.run_sotw(&store, &mut state).await,
            };
            match result {
                Ok(()) => warn!(?mode, "ADS stream ended"),
                Err(err @ XdsError::InvalidEndpoint { .. }) => return Err(err),
                Err(XdsError::DeltaUnsupported) => {
                    warn!(
                        "control plane does not implement delta ADS; \
                         falling back to state-of-the-world ADS"
                    );
                    mode = StreamMode::StateOfTheWorld;
                    continue;
                }
                Err(err) => warn!(%err, ?mode, "ADS stream failed"),
            }
            time::sleep(self.cfg.reconnect_delay).await;
        }
    }

    async fn run_delta(&self, store: &ConfigStore, state: &mut AdsState) -> Result<(), XdsError> {
        let channel = self.connect_channel().await?;
        let node = self.node();
        let mut ads = AggregatedDiscoveryServiceClient::new(channel)
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE);
        let (request_tx, request_rx) = mpsc::channel(REQUEST_CHANNEL_CAPACITY);
        let mut stream_state = DeltaStream::new(request_tx, node);

        let listeners = state.begin_stream(self.cfg.listener_names.clone());
        stream_state
            .subscribe(
                LISTENER_TYPE,
                listeners,
                Vec::new(),
                state.initial_resource_versions(LISTENER_TYPE),
            )
            .await?;
        // Re-request everything the cache already wants: the new stream has no
        // memory of the previous one's subscriptions.
        for change in state.refresh_subscriptions() {
            let versions = state.initial_resource_versions(change.type_url);
            stream_state
                .subscribe(
                    change.type_url,
                    change.subscribe,
                    change.unsubscribe,
                    versions,
                )
                .await?;
        }

        let response = ads
            .delta_aggregated_resources(ReceiverStream::new(request_rx))
            .await
            .map_err(delta_stream_error)?;
        let mut stream = response.into_inner();

        info!(
            node_id = %self.cfg.identity.node_id(),
            endpoint = %self.cfg.endpoint,
            listeners = ?self.cfg.listener_names,
            "connected dxgate router to dubbod delta ADS endpoint"
        );

        while let Some(resp) = stream.message().await.map_err(delta_stream_error)? {
            self.handle_delta_response(store, state, &mut stream_state, resp)
                .await?;
        }

        Ok(())
    }

    async fn handle_delta_response(
        &self,
        store: &ConfigStore,
        state: &mut AdsState,
        stream_state: &mut DeltaStream,
        resp: DeltaDiscoveryResponse,
    ) -> Result<(), XdsError> {
        let resources: Vec<(String, String, prost_types::Any)> = resp
            .resources
            .into_iter()
            .filter_map(|resource| {
                resource
                    .resource
                    .map(|any| (resource.name, resource.version, any))
            })
            .collect();

        debug!(
            type_url = %resp.type_url,
            updates = resources.len(),
            removes = resp.removed_resources.len(),
            "received delta ADS response"
        );
        state.apply_delta(&resp.type_url, &resources, &resp.removed_resources)?;

        // ACK: the nonce alone, no subscription change and no version replay.
        stream_state.ack(&resp.type_url, &resp.nonce).await?;

        for change in state.refresh_subscriptions() {
            let versions = state.initial_resource_versions(change.type_url);
            stream_state
                .subscribe(
                    change.type_url,
                    change.subscribe,
                    change.unsubscribe,
                    versions,
                )
                .await?;
        }

        publish(store, state, &resp.system_version_info);
        Ok(())
    }

    async fn run_sotw(&self, store: &ConfigStore, state: &mut AdsState) -> Result<(), XdsError> {
        let channel = self.connect_channel().await?;
        let node = self.node();
        let mut ads = AggregatedDiscoveryServiceClient::new(channel)
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE);
        let (request_tx, request_rx) = mpsc::channel(REQUEST_CHANNEL_CAPACITY);

        let listeners = state.begin_stream(self.cfg.listener_names.clone());
        send_discovery_request(&request_tx, &node, LISTENER_TYPE, listeners, "", "").await?;
        for change in state.refresh_subscriptions() {
            send_discovery_request(&request_tx, &node, change.type_url, change.desired, "", "")
                .await?;
        }

        let response = ads
            .stream_aggregated_resources(ReceiverStream::new(request_rx))
            .await
            .map_err(|status| XdsError::StreamOpen(Box::new(status)))?;
        let mut stream = response.into_inner();

        info!(
            node_id = %self.cfg.identity.node_id(),
            endpoint = %self.cfg.endpoint,
            listeners = ?self.cfg.listener_names,
            "connected dxgate router to dubbod ADS endpoint"
        );

        while let Some(resp) = stream
            .message()
            .await
            .map_err(|status| XdsError::StreamReceive(Box::new(status)))?
        {
            state.apply_sotw(&resp)?;
            send_discovery_request(
                &request_tx,
                &node,
                &resp.type_url,
                state.subscription(&resp.type_url),
                &resp.version_info,
                &resp.nonce,
            )
            .await?;

            for change in state.refresh_subscriptions() {
                send_discovery_request(&request_tx, &node, change.type_url, change.desired, "", "")
                    .await?;
            }

            publish(store, state, &resp.version_info);
        }

        Ok(())
    }

    fn node(&self) -> xds_core::Node {
        let metadata = self.cfg.identity.metadata();
        let mut fields = BTreeMap::new();
        fields.insert("GENERATOR".to_string(), string_value(metadata.generator));
        fields.insert("CLUSTER_ID".to_string(), string_value(metadata.cluster_id));
        fields.insert("NAMESPACE".to_string(), string_value(metadata.namespace));
        if let Some(node_name) = metadata.node_name {
            fields.insert("KUBE_NODE_NAME".to_string(), string_value(node_name));
        }

        xds_core::Node {
            id: self.cfg.identity.node_id(),
            cluster: self.cfg.identity.cluster_id.clone(),
            metadata: Some(Struct { fields }),
            locality: None,
        }
    }
}

/// Projects the resource cache into the store as the xDS slice.
fn publish(store: &ConfigStore, state: &mut AdsState, version: &str) {
    let delta = state.config_delta(version);
    let removes = delta.removes.len();
    let outcome = store.apply(SourceId::Xds, delta);
    if !outcome.changed {
        return;
    }
    for rejected in &outcome.rejected {
        warn!(kind = %rejected.kind, message = %rejected.message, "xDS resource rejected");
    }
    if outcome.ready {
        info!(
            revision = outcome.revision,
            version = %version,
            removes,
            "applied xDS runtime config"
        );
    } else {
        // Normal while a stream is still filling in: an ADS server sends
        // listeners before the clusters they reference. Requests that hit the
        // gap fail with 503 and readiness reports it.
        debug!(
            revision = outcome.revision,
            conflicts = ?outcome.conflicts,
            "xDS runtime config is not complete yet"
        );
    }
}

/// Per-stream delta bookkeeping: the request sink, the node identity every
/// request repeats, and which resource types have already sent their first
/// request on this stream.
struct DeltaStream {
    request_tx: mpsc::Sender<DeltaDiscoveryRequest>,
    node: xds_core::Node,
    initialized: BTreeSet<String>,
}

impl DeltaStream {
    fn new(request_tx: mpsc::Sender<DeltaDiscoveryRequest>, node: xds_core::Node) -> Self {
        Self {
            request_tx,
            node,
            initialized: BTreeSet::new(),
        }
    }

    /// Sends a subscription change. `initial_resource_versions` is only valid on
    /// the first request for a type on a stream, so later changes send it empty.
    async fn subscribe(
        &mut self,
        type_url: &str,
        subscribe: Vec<String>,
        unsubscribe: Vec<String>,
        versions: BTreeMap<String, String>,
    ) -> Result<(), XdsError> {
        let first = self.initialized.insert(type_url.to_string());
        let initial_resource_versions = if first {
            versions.into_iter().collect()
        } else {
            HashMap::new()
        };
        self.send(DeltaDiscoveryRequest {
            node: Some(self.node.clone()),
            type_url: type_url.to_string(),
            resource_names_subscribe: subscribe,
            resource_names_unsubscribe: unsubscribe,
            initial_resource_versions,
            response_nonce: String::new(),
            error_detail: None,
        })
        .await
    }

    async fn ack(&mut self, type_url: &str, nonce: &str) -> Result<(), XdsError> {
        self.send(DeltaDiscoveryRequest {
            node: Some(self.node.clone()),
            type_url: type_url.to_string(),
            resource_names_subscribe: Vec::new(),
            resource_names_unsubscribe: Vec::new(),
            initial_resource_versions: HashMap::new(),
            response_nonce: nonce.to_string(),
            error_detail: None,
        })
        .await
    }

    async fn send(&self, request: DeltaDiscoveryRequest) -> Result<(), XdsError> {
        self.request_tx
            .send(request)
            .await
            .map_err(|_| XdsError::RequestChannelClosed)
    }
}

/// `UNIMPLEMENTED` on a delta stream means the control plane only speaks
/// state-of-the-world ADS; every other status is a normal stream failure.
fn delta_stream_error(status: tonic::Status) -> XdsError {
    if status.code() == Code::Unimplemented {
        XdsError::DeltaUnsupported
    } else {
        XdsError::StreamReceive(Box::new(status))
    }
}

async fn send_discovery_request(
    request_tx: &mpsc::Sender<DiscoveryRequest>,
    node: &xds_core::Node,
    type_url: &str,
    resource_names: Vec<String>,
    version_info: &str,
    response_nonce: &str,
) -> Result<(), XdsError> {
    request_tx
        .send(DiscoveryRequest {
            version_info: version_info.to_string(),
            node: Some(node.clone()),
            resource_names,
            type_url: type_url.to_string(),
            response_nonce: response_nonce.to_string(),
            error_detail: None,
        })
        .await
        .map_err(|_| XdsError::RequestChannelClosed)
}

fn string_value(value: String) -> Value {
    Value {
        kind: Some(Kind::StringValue(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use state::CLUSTER_TYPE;

    #[test]
    fn unimplemented_delta_streams_trigger_the_sotw_fallback() {
        let status = tonic::Status::new(Code::Unimplemented, "no delta here");
        assert!(matches!(
            delta_stream_error(status),
            XdsError::DeltaUnsupported
        ));

        let status = tonic::Status::new(Code::Unavailable, "try later");
        assert!(matches!(
            delta_stream_error(status),
            XdsError::StreamReceive(_)
        ));
    }

    #[tokio::test]
    async fn only_the_first_delta_request_per_type_replays_resource_versions() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut stream = DeltaStream::new(tx, xds_core::Node::default());
        let versions = BTreeMap::from([("listener-a".to_string(), "1".to_string())]);

        stream
            .subscribe(
                LISTENER_TYPE,
                vec!["listener-a".into()],
                vec![],
                versions.clone(),
            )
            .await
            .unwrap();
        stream
            .subscribe(LISTENER_TYPE, vec!["listener-b".into()], vec![], versions)
            .await
            .unwrap();

        let first = rx.recv().await.unwrap();
        assert_eq!(first.initial_resource_versions.len(), 1);
        assert_eq!(first.resource_names_subscribe, ["listener-a"]);

        let second = rx.recv().await.unwrap();
        assert!(second.initial_resource_versions.is_empty());
        assert_eq!(second.resource_names_subscribe, ["listener-b"]);
    }

    #[tokio::test]
    async fn acks_carry_the_nonce_and_no_subscription_change() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut stream = DeltaStream::new(tx, xds_core::Node::default());

        stream.ack(CLUSTER_TYPE, "nonce-7").await.unwrap();

        let ack = rx.recv().await.unwrap();
        assert_eq!(ack.type_url, CLUSTER_TYPE);
        assert_eq!(ack.response_nonce, "nonce-7");
        assert!(ack.resource_names_subscribe.is_empty());
        assert!(ack.resource_names_unsubscribe.is_empty());
    }
}
