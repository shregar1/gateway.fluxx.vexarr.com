use crate::api::{ApiState, StandardResponse};
use crate::protocols::otlp::convert;
use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use std::sync::Arc;
use tracing::{info, error};

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

/// POST /v1/otlp/traces — Ingest OTLP traces via HTTP/JSON.
pub async fn ingest_traces(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<ExportTraceServiceRequest>,
) -> impl IntoResponse {
    let events = convert::otlp_spans_to_events(&request.resource_spans);
    let count = events.len();

    for event in events {
        let event_arc = Arc::new(event.clone());
        let _ = state.event_bus.send(event_arc);
        if let Err(e) = state.event_store.insert(event).await {
            error!("OTLP HTTP trace store error: {}", e);
        }
    }

    info!("OTLP HTTP: Ingested {} spans", count);
    StandardResponse::ok(
        serde_json::json!({ "accepted_spans": count }),
        "Traces ingested successfully.",
    )
}

/// POST /v1/otlp/metrics — Ingest OTLP metrics via HTTP/JSON.
pub async fn ingest_metrics(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<ExportMetricsServiceRequest>,
) -> impl IntoResponse {
    let metrics = convert::otlp_metrics_to_metrics(&request.resource_metrics);
    let count = metrics.len();

    for metric in metrics {
        let metric_arc = Arc::new(metric.clone());
        let _ = state.metric_bus.send(metric_arc);
        if let Err(e) = state.metric_store.insert(metric).await {
            error!("OTLP HTTP metric store error: {}", e);
        }
    }

    info!("OTLP HTTP: Ingested {} metric data points", count);
    StandardResponse::ok(
        serde_json::json!({ "accepted_metrics": count }),
        "Metrics ingested successfully.",
    )
}

/// POST /v1/otlp/logs — Ingest OTLP logs via HTTP/JSON.
pub async fn ingest_logs(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<ExportLogsServiceRequest>,
) -> impl IntoResponse {
    let entries = convert::otlp_logs_to_entries(&request.resource_logs);
    let count = entries.len();

    for entry in entries {
        if let Err(e) = state.log_store.insert(entry).await {
            error!("OTLP HTTP log store error: {}", e);
        }
    }

    info!("OTLP HTTP: Ingested {} log records", count);
    StandardResponse::ok(
        serde_json::json!({ "accepted_logs": count }),
        "Logs ingested successfully.",
    )
}
