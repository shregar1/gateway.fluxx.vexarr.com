//! OTLP HTTP/JSON handlers.
//!
//! These accept the standard OpenTelemetry HTTP/JSON wire format. The
//! canonical user-facing routes are mounted at `/v1/traces`,
//! `/v1/metrics`, `/v1/logs` — the same paths every OTel SDK ships
//! configured to send to. The legacy `/v1/otlp/*` paths are kept as
//! aliases for backward compat.
//!
//! Each request type has two entry points:
//!   * a `pub async fn write_*` inner helper that takes the parsed
//!     request and the shared `ApiState`. The content-negotiating
//!     dispatcher in `ingest.rs` calls these directly.
//!   * a `pub async fn ingest_*` handler that the axum router mounts —
//!     extracts `State` + `Json` and delegates to the inner helper.

use crate::{ApiState, StandardResponse};
use fluxx_core::protocols::otlp::convert;
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

/// Write a parsed OTLP trace request into the event store / bus.
/// Returns the number of spans accepted.
pub async fn write_otlp_traces(state: &Arc<ApiState>, request: ExportTraceServiceRequest) -> usize {
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
    count
}

/// Write a parsed OTLP metric request into the metric store / bus.
/// Returns the number of metric data points accepted.
pub async fn write_otlp_metrics(state: &Arc<ApiState>, request: ExportMetricsServiceRequest) -> usize {
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
    count
}

/// Write a parsed OTLP log request into the log store.
/// Returns the number of log records accepted.
pub async fn write_otlp_logs(state: &Arc<ApiState>, request: ExportLogsServiceRequest) -> usize {
    let entries = convert::otlp_logs_to_entries(&request.resource_logs);
    let count = entries.len();

    for entry in entries {
        if let Err(e) = state.log_store.insert(entry).await {
            error!("OTLP HTTP log store error: {}", e);
        }
    }

    info!("OTLP HTTP: Ingested {} log records", count);
    count
}

/// POST /v1/traces and POST /v1/otlp/traces — OTLP HTTP/JSON traces.
pub async fn ingest_traces(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<ExportTraceServiceRequest>,
) -> impl IntoResponse {
    let count = write_otlp_traces(&state, request).await;
    StandardResponse::ok(
        serde_json::json!({ "accepted_spans": count }),
        "Traces ingested successfully.",
    )
}

/// POST /v1/metrics and POST /v1/otlp/metrics — OTLP HTTP/JSON metrics.
pub async fn ingest_metrics(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<ExportMetricsServiceRequest>,
) -> impl IntoResponse {
    let count = write_otlp_metrics(&state, request).await;
    StandardResponse::ok(
        serde_json::json!({ "accepted_metrics": count }),
        "Metrics ingested successfully.",
    )
}

/// POST /v1/otlp/logs — OTLP HTTP/JSON logs (alias).
/// Canonical OTLP traffic on `/v1/logs` goes through the dispatcher in
/// `ingest.rs` so we can also accept the fluxx-native shape on the same URL.
pub async fn ingest_logs(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<ExportLogsServiceRequest>,
) -> impl IntoResponse {
    let count = write_otlp_logs(&state, request).await;
    StandardResponse::ok(
        serde_json::json!({ "accepted_logs": count }),
        "Logs ingested successfully.",
    )
}
