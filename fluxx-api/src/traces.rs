//! Trace Detail API
//!
//! Dedicated endpoints for querying distributed traces:
//! - GET /v1/traces/:trace_id — fetch all spans belonging to a trace
//! - GET /v1/traces — list recent traces with filtering

use crate::api::{ApiState, StandardResponse};
use crate::models::event::{Event, ProtocolType};
use axum::{
    extract::{Path, State, Query},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// A trace is a collection of spans sharing a trace_id.
#[derive(Debug, Serialize, Deserialize)]
pub struct TraceDetail {
    pub trace_id: String,
    pub spans: Vec<SpanDetail>,
    pub service_count: usize,
    pub total_duration_us: u64,
    pub root_span: Option<String>,
    pub has_errors: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SpanDetail {
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub span_name: String,
    pub service: String,
    pub start_us: u64,
    pub duration_us: u64,
    pub severity: String,
    pub attributes: HashMap<String, String>,
    pub has_error: bool,
}

/// A summary entry for trace listing.
#[derive(Debug, Serialize, Deserialize)]
pub struct TraceSummary {
    pub trace_id: String,
    pub root_service: String,
    pub root_span_name: String,
    pub span_count: usize,
    pub duration_us: u64,
    pub has_errors: bool,
    pub timestamp_us: u64,
}

/// GET /v1/traces/:trace_id — Fetch all spans for a specific trace.
pub async fn get_trace(
    Path(trace_id): Path<String>,
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    // Query all OTLP events
    let events = state.event_store.list_events(
        None, Some(ProtocolType::Otlp), None, None,
    ).await;

    // Filter to spans matching this trace_id
    let matching: Vec<&Event> = events.iter()
        .filter(|e| {
            e.metadata.extra.get("trace_id").map(|t| t == &trace_id).unwrap_or(false)
        })
        .collect();

    if matching.is_empty() {
        return StandardResponse::error(
            serde_json::json!({}),
            &format!("Trace {} not found", trace_id),
            "NOT_FOUND",
            vec![],
        );
    }

    let spans: Vec<SpanDetail> = matching.iter().map(|e| {
        let meta = &e.metadata.extra;
        let is_error = matches!(e.severity, crate::models::event::EventSeverity::High | crate::models::event::EventSeverity::Critical);
        SpanDetail {
            span_id: e.id.clone(),
            parent_span_id: meta.get("parent_span_id").cloned().filter(|s| !s.is_empty()),
            span_name: meta.get("span_name").cloned().unwrap_or_else(|| "unknown".to_string()),
            service: e.service_id.clone().unwrap_or_else(|| meta.get("service.name").cloned().unwrap_or_else(|| "unknown".to_string())),
            start_us: e.timestamp_us,
            duration_us: e.latency_us,
            severity: format!("{}", e.severity),
            attributes: meta.clone(),
            has_error: is_error,
        }
    }).collect();

    // Find root span (no parent)
    let root_span = spans.iter()
        .find(|s| s.parent_span_id.is_none())
        .map(|s| s.span_id.clone());

    // Compute total duration from earliest start to latest end
    let min_start = spans.iter().map(|s| s.start_us).min().unwrap_or(0);
    let max_end = spans.iter().map(|s| s.start_us + s.duration_us).max().unwrap_or(0);

    let services: std::collections::HashSet<&str> = spans.iter().map(|s| s.service.as_str()).collect();
    let has_errors = spans.iter().any(|s| s.has_error);

    let detail = TraceDetail {
        trace_id,
        service_count: services.len(),
        total_duration_us: max_end.saturating_sub(min_start),
        root_span,
        has_errors,
        spans,
    };

    let data = serde_json::to_value(&detail).unwrap_or_default();
    StandardResponse::ok(data, "Trace detail.")
}

/// GET /v1/traces — List recent traces with optional filtering.
pub async fn list_traces(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let service_filter = params.get("service").cloned();
    let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(50);

    // Query all OTLP events
    let events = state.event_store.list_events(
        None, Some(ProtocolType::Otlp), None, service_filter.clone(),
    ).await;

    // Group by trace_id
    let mut traces: HashMap<String, Vec<&Event>> = HashMap::new();
    for event in &events {
        if let Some(tid) = event.metadata.extra.get("trace_id") {
            traces.entry(tid.clone()).or_default().push(event);
        }
    }

    // Build summaries
    let mut summaries: Vec<TraceSummary> = traces.into_iter().map(|(trace_id, spans)| {
        // Find root span
        let root = spans.iter()
            .find(|s| s.metadata.extra.get("parent_span_id").map_or(true, |p| p.is_empty()))
            .or(spans.first());

        let root_service = root
            .and_then(|r| r.service_id.clone().or_else(|| r.metadata.extra.get("service.name").cloned()))
            .unwrap_or_else(|| "unknown".to_string());

        let root_span_name = root
            .and_then(|r| r.metadata.extra.get("span_name").cloned())
            .unwrap_or_else(|| "unknown".to_string());

        let min_start = spans.iter().map(|s| s.timestamp_us).min().unwrap_or(0);
        let max_end = spans.iter().map(|s| s.timestamp_us + s.latency_us).max().unwrap_or(0);
        let has_errors = spans.iter().any(|s| matches!(s.severity,
            crate::models::event::EventSeverity::High | crate::models::event::EventSeverity::Critical));

        TraceSummary {
            trace_id,
            root_service,
            root_span_name,
            span_count: spans.len(),
            duration_us: max_end.saturating_sub(min_start),
            has_errors,
            timestamp_us: min_start,
        }
    }).collect();

    // Sort by newest first, limit
    summaries.sort_by(|a, b| b.timestamp_us.cmp(&a.timestamp_us));
    summaries.truncate(limit);

    let data = serde_json::to_value(&summaries).unwrap_or_default();
    StandardResponse::ok(data, "Traces list.")
}
