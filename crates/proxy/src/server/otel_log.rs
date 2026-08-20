//! OTLP log-signal export for access-log events.
//!
//! Tracing export is configured by the application crate. Access logs are a
//! separate OpenTelemetry signal, so this module owns a log provider instead
//! of routing structured access events through the tracing pipeline.

use super::access_log::{access_log_line, AccessLogEvent, AccessLogFormat};
use opentelemetry::{
    logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{logs::LoggerProvider, Resource};
use std::collections::BTreeMap;
use std::env;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::warn;

#[derive(Clone)]
pub(super) struct OtelAccessLogExporter {
    provider: LoggerProvider,
    logger: Arc<opentelemetry_sdk::logs::Logger>,
}

impl OtelAccessLogExporter {
    pub(super) fn new(endpoint: &str) -> Result<Self, opentelemetry::logs::LogError> {
        let service_name =
            env::var("DXGATE_OTEL_SERVICE_NAME").unwrap_or_else(|_| "dxgate".to_string());
        let mut resource_attributes = vec![
            KeyValue::new("service.name", service_name),
            KeyValue::new("service.component", "dxgate.access"),
        ];
        if let Ok(namespace) = env::var("POD_NAMESPACE") {
            if !namespace.trim().is_empty() {
                resource_attributes.push(KeyValue::new("service.namespace", namespace));
            }
        }
        if let Ok(gateway) = env::var("DXGATE_GATEWAY_NAME") {
            if !gateway.trim().is_empty() {
                resource_attributes.push(KeyValue::new("gateway.name", gateway));
            }
        }

        let provider = opentelemetry_otlp::new_pipeline()
            .logging()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(endpoint.to_string()),
            )
            .with_log_config(
                opentelemetry_sdk::logs::Config::default()
                    .with_resource(Resource::new(resource_attributes)),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;
        let logger = Arc::new(provider.logger("dxgate.access"));
        Ok(Self { provider, logger })
    }

    pub(super) fn emit(&self, event: &AccessLogEvent<'_>, tags: &BTreeMap<String, String>) {
        let mut record = self.logger.create_log_record();
        record.set_event_name("dxgate.access");
        record.set_timestamp(SystemTime::now());
        record.set_severity_number(Severity::Info);
        record.set_severity_text("INFO".into());
        record.set_body(AnyValue::from(access_log_line(
            AccessLogFormat::Json,
            event,
            tags,
        )));
        record.add_attributes(access_log_attributes(event, tags));
        self.logger.emit(record);
    }

    pub(super) fn shutdown(&self) {
        for result in self.provider.force_flush() {
            if let Err(err) = result {
                warn!(%err, "failed flushing OTLP access logs");
            }
        }
        if let Err(err) = self.provider.shutdown() {
            warn!(%err, "failed shutting down OTLP access-log exporter");
        }
    }
}

fn access_log_attributes(
    event: &AccessLogEvent<'_>,
    tags: &BTreeMap<String, String>,
) -> Vec<(String, AnyValue)> {
    let mut attributes = vec![
        ("event.name".to_string(), AnyValue::from("dxgate.access")),
        (
            "http.request.method".to_string(),
            AnyValue::from(event.method.to_string()),
        ),
        (
            "url.path".to_string(),
            AnyValue::from(event.path.to_string()),
        ),
        (
            "http.response.status_code".to_string(),
            AnyValue::from(i64::from(event.status_code)),
        ),
        (
            "http.server.request.duration_ms".to_string(),
            AnyValue::from(i64::try_from(event.latency_ms).unwrap_or(i64::MAX)),
        ),
        (
            "server.address".to_string(),
            AnyValue::from(event.host.to_string()),
        ),
        (
            "upstream.address".to_string(),
            AnyValue::from(event.upstream.to_string()),
        ),
        (
            "gateway.namespace".to_string(),
            AnyValue::from(event.namespace.to_string()),
        ),
        (
            "gateway.name".to_string(),
            AnyValue::from(event.gateway.to_string()),
        ),
        (
            "http.route".to_string(),
            AnyValue::from(event.route.to_string()),
        ),
        (
            "dxgate.cluster".to_string(),
            AnyValue::from(event.cluster.to_string()),
        ),
        (
            "dxgate.protocol".to_string(),
            AnyValue::from(event.protocol.to_string()),
        ),
        (
            "dxgate.backend".to_string(),
            AnyValue::from(event.backend.to_string()),
        ),
    ];
    if !event.trace_id.is_empty() {
        attributes.push((
            "trace_id".to_string(),
            AnyValue::from(event.trace_id.to_string()),
        ));
    }
    if !event.span_id.is_empty() {
        attributes.push((
            "span_id".to_string(),
            AnyValue::from(event.span_id.to_string()),
        ));
    }
    attributes.extend(
        tags.iter()
            .map(|(key, value)| (key.clone(), AnyValue::from(value.clone()))),
    );
    attributes
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::logs::v1::{
        logs_service_server::{LogsService, LogsServiceServer},
        ExportLogsServiceRequest, ExportLogsServiceResponse,
    };
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    #[derive(Default)]
    struct Receiver {
        request: Mutex<Option<oneshot::Sender<ExportLogsServiceRequest>>>,
    }

    #[tonic::async_trait]
    impl LogsService for Receiver {
        async fn export(
            &self,
            request: Request<ExportLogsServiceRequest>,
        ) -> Result<Response<ExportLogsServiceResponse>, Status> {
            if let Some(tx) = self.request.lock().unwrap().take() {
                let _ = tx.send(request.into_inner());
            }
            Ok(Response::new(ExportLogsServiceResponse::default()))
        }
    }

    #[tokio::test]
    async fn exports_access_log_to_otlp_logs_service() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (request_tx, request_rx) = oneshot::channel();
        let receiver = Receiver {
            request: Mutex::new(Some(request_tx)),
        };
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(LogsServiceServer::new(receiver))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let exporter = OtelAccessLogExporter::new(&endpoint).unwrap();
        let tags = BTreeMap::from([(String::from("tenant"), String::from("edge"))]);
        exporter.emit(
            &AccessLogEvent {
                namespace: "app",
                gateway: "public",
                route: "checkout",
                cluster: "payments",
                protocol: "http",
                backend: "payments",
                method: "GET",
                host: "api.example.test",
                path: "/checkout",
                status_code: 503,
                latency_ms: 17,
                upstream: "10.0.0.2:8080",
                trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
                span_id: "00f067aa0ba902b7",
            },
            &tags,
        );
        let flush = tokio::task::spawn_blocking(move || exporter.shutdown());

        let request = tokio::time::timeout(Duration::from_secs(5), request_rx)
            .await
            .expect("OTLP collector did not receive access log")
            .expect("OTLP collector sender dropped");
        let record = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(record.severity_text, "INFO");
        let attributes = record
            .attributes
            .iter()
            .filter_map(
                |attribute| match attribute.value.as_ref()?.value.as_ref()? {
                    Value::StringValue(value) => Some((attribute.key.as_str(), value.as_str())),
                    _ => None,
                },
            )
            .collect::<BTreeMap<_, _>>();
        assert_eq!(attributes["http.route"], "checkout");
        assert_eq!(attributes["tenant"], "edge");
        assert_eq!(attributes["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");

        tokio::time::timeout(Duration::from_secs(5), flush)
            .await
            .expect("OTLP exporter did not flush access log")
            .expect("OTLP exporter shutdown task failed");

        // The tonic exporter owns a pooled HTTP/2 connection, so a graceful
        // collector shutdown can wait on it. This is a test-only listener.
        server.abort();
        let _ = server.await;
    }
}
