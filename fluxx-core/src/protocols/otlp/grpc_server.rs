use crate::models::event::{Event, EventSeverity};
use crate::models::metric::Metric;
use crate::models::log_entry::LogEntry;
use crate::storage::event_store::EventStore;
use crate::storage::metric_store::MetricStore;
use crate::storage::log_store::LogStore;
use crate::analysis::error_tracking::ErrorTracker;
use crate::protocols::otlp::convert;
use std::sync::Arc;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};
use tracing::{info, error};

use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::{TraceService, TraceServiceServer},
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueValue;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_server::{MetricsService, MetricsServiceServer},
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    logs_service_server::{LogsService, LogsServiceServer},
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};

/// OTLP Trace gRPC service.
pub struct OtlpTraceService {
    event_store: Arc<dyn EventStore>,
    event_bus: broadcast::Sender<Arc<Event>>,
    error_tracker: Option<Arc<ErrorTracker>>,
}

impl OtlpTraceService {
    pub fn new(event_store: Arc<dyn EventStore>, event_bus: broadcast::Sender<Arc<Event>>) -> Self {
        Self { event_store, event_bus, error_tracker: None }
    }

    pub fn with_error_tracker(mut self, tracker: Arc<ErrorTracker>) -> Self {
        self.error_tracker = Some(tracker);
        self
    }
}

#[tonic::async_trait]
impl TraceService for OtlpTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let req = request.into_inner();
        // Per-batch release tag from the OTel resource. Picks the first
        // `service.version` we see across all resource_spans. Real-world
        // OTLP batches are almost always single-service, so this is a
        // safe approximation; a future revision could carry per-span
        // resource context through `convert::otlp_spans_to_events`.
        let release = extract_release_from_resource_spans(&req.resource_spans);
        let events = convert::otlp_spans_to_events(&req.resource_spans);
        let count = events.len();

        for event in events {
            // Auto-track span errors
            if let Some(ref tracker) = self.error_tracker {
                if matches!(event.severity, EventSeverity::High | EventSeverity::Critical) {
                    let span_name = event.metadata.extra.get("span_name").cloned().unwrap_or_default();
                    let service = event.service_id.clone().unwrap_or_else(|| "unknown".to_string());
                    let mut ctx = crate::analysis::error_tracking::TrackContext::default();
                    ctx.release = release.clone();
                    tracker.track_with("SpanError", &span_name, &service, None, &ctx);
                }
            }

            let event_arc = Arc::new(event.clone());
            let _ = self.event_bus.send(event_arc);
            if let Err(e) = self.event_store.insert(event).await {
                error!("OTLP trace store error: {}", e);
            }
        }

        info!("OTLP: Ingested {} spans via gRPC", count);
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

/// Extract the `service.version` resource attribute as the release tag.
/// Returns the first non-empty value found across all resource_spans;
/// the empty string when no resource declares it.
fn extract_release_from_resource_spans(spans: &[ResourceSpans]) -> String {
    for rs in spans {
        let Some(resource) = rs.resource.as_ref() else { continue };
        for kv in &resource.attributes {
            if kv.key != "service.version" {
                continue;
            }
            let Some(any) = kv.value.as_ref() else { continue };
            if let Some(AnyValueValue::StringValue(s)) = &any.value {
                if !s.is_empty() {
                    return s.clone();
                }
            }
        }
    }
    String::new()
}

/// OTLP Metrics gRPC service.
pub struct OtlpMetricsService {
    metric_store: Arc<dyn MetricStore>,
    metric_bus: broadcast::Sender<Arc<Metric>>,
}

impl OtlpMetricsService {
    pub fn new(metric_store: Arc<dyn MetricStore>, metric_bus: broadcast::Sender<Arc<Metric>>) -> Self {
        Self { metric_store, metric_bus }
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req = request.into_inner();
        let metrics = convert::otlp_metrics_to_metrics(&req.resource_metrics);
        let count = metrics.len();

        for metric in metrics {
            let metric_arc = Arc::new(metric.clone());
            let _ = self.metric_bus.send(metric_arc);
            if let Err(e) = self.metric_store.insert(metric).await {
                error!("OTLP metric store error: {}", e);
            }
        }

        info!("OTLP: Ingested {} metric data points via gRPC", count);
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

/// OTLP Logs gRPC service.
pub struct OtlpLogsService {
    log_store: Arc<dyn LogStore>,
    #[allow(dead_code)]
    log_bus: broadcast::Sender<Arc<LogEntry>>,
}

impl OtlpLogsService {
    pub fn new(log_store: Arc<dyn LogStore>, log_bus: broadcast::Sender<Arc<LogEntry>>) -> Self {
        Self { log_store, log_bus }
    }
}

#[tonic::async_trait]
impl LogsService for OtlpLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let req = request.into_inner();
        let entries = convert::otlp_logs_to_entries(&req.resource_logs);
        let count = entries.len();

        for entry in entries {
            if let Err(e) = self.log_store.insert(entry).await {
                error!("OTLP log store error: {}", e);
            }
        }

        info!("OTLP: Ingested {} log records via gRPC", count);
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

/// Start the OTLP gRPC server on the given port.
pub async fn start_otlp_grpc_server(
    port: u16,
    event_store: Arc<dyn EventStore>,
    event_bus: broadcast::Sender<Arc<Event>>,
    metric_store: Arc<dyn MetricStore>,
    metric_bus: broadcast::Sender<Arc<Metric>>,
    log_store: Arc<dyn LogStore>,
    log_bus: broadcast::Sender<Arc<LogEntry>>,
    error_tracker: Option<Arc<ErrorTracker>>,
) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{}", port).parse()?;

    let mut trace_svc = OtlpTraceService::new(event_store, event_bus);
    if let Some(tracker) = error_tracker {
        trace_svc = trace_svc.with_error_tracker(tracker);
    }
    let metrics_svc = OtlpMetricsService::new(metric_store, metric_bus);
    let logs_svc = OtlpLogsService::new(log_store, log_bus);

    info!("OTLP gRPC: Listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(TraceServiceServer::new(trace_svc))
        .add_service(MetricsServiceServer::new(metrics_svc))
        .add_service(LogsServiceServer::new(logs_svc))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn rs_with_attrs(attrs: Vec<(&str, &str)>) -> ResourceSpans {
        ResourceSpans {
            resource: Some(Resource {
                attributes: attrs
                    .into_iter()
                    .map(|(k, v)| KeyValue {
                        key: k.to_string(),
                        value: Some(AnyValue {
                            value: Some(AnyValueValue::StringValue(v.to_string())),
                        }),
                    })
                    .collect(),
                dropped_attributes_count: 0,
                ..Default::default()
            }),
            scope_spans: vec![],
            schema_url: String::new(),
        }
    }

    #[test]
    fn extract_release_returns_service_version_when_present() {
        let spans = vec![rs_with_attrs(vec![
            ("service.name", "checkout"),
            ("service.version", "1.4.2"),
        ])];
        assert_eq!(extract_release_from_resource_spans(&spans), "1.4.2");
    }

    #[test]
    fn extract_release_returns_empty_when_attr_missing() {
        let spans = vec![rs_with_attrs(vec![("service.name", "checkout")])];
        assert_eq!(extract_release_from_resource_spans(&spans), "");
    }

    #[test]
    fn extract_release_returns_empty_for_no_resources() {
        let spans: Vec<ResourceSpans> = vec![];
        assert_eq!(extract_release_from_resource_spans(&spans), "");
    }

    #[test]
    fn extract_release_picks_first_non_empty_across_resources() {
        let spans = vec![
            rs_with_attrs(vec![("service.version", "")]),
            rs_with_attrs(vec![("service.version", "2.0.0")]),
            rs_with_attrs(vec![("service.version", "3.0.0")]),
        ];
        assert_eq!(extract_release_from_resource_spans(&spans), "2.0.0");
    }
}
