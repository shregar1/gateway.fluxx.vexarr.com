//! Correlation Engine
//!
//! Auto-correlates metrics, logs, traces, errors, and deploys within a time window.
//! Answers: "What happened around timestamp X?" by querying all signal stores
//! and returning a unified timeline.
//!
//! This is Fluxx's key differentiator — Datadog correlates poorly across signals.

use crate::api::{ApiState, StandardResponse};
use crate::models::event::ProtocolType;
use crate::storage::event_store::TimeWindow;
use crate::storage::log_store::LogQuery;
use axum::{
    extract::{State, Query},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

/// A correlated timeline entry from any signal type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelatedEvent {
    pub timestamp_us: u64,
    pub signal_type: String,
    pub severity: String,
    pub title: String,
    pub description: String,
    pub service: Option<String>,
    pub trace_id: Option<String>,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
}

/// Full correlation result across all signals.
#[derive(Debug, Serialize, Deserialize)]
pub struct CorrelationResult {
    pub window_start_us: u64,
    pub window_end_us: u64,
    pub total_events: usize,
    pub timeline: Vec<CorrelatedEvent>,
    pub summary: CorrelationSummary,
    pub likely_cause: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CorrelationSummary {
    pub metric_anomalies: usize,
    pub error_logs: usize,
    pub error_traces: usize,
    pub deploys: usize,
    pub incidents: usize,
    pub services_affected: Vec<String>,
}

/// GET /v1/correlate — Find correlated events around a time window.
///
/// Query params:
/// - `center_us` — center timestamp in microseconds (default: now)
/// - `window_secs` — window size in seconds (default: 300 = 5 minutes)
/// - `service` — optional service filter
pub async fn correlate(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let now_us = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;

    let center_us: u64 = params.get("center_us")
        .and_then(|v| v.parse().ok())
        .unwrap_or(now_us);

    let window_secs: u64 = params.get("window_secs")
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    let service_filter = params.get("service").cloned();
    let window_us = window_secs * 1_000_000;
    let from = center_us.saturating_sub(window_us / 2);
    let to = center_us + window_us / 2;
    let window = TimeWindow::new(from, to);

    let mut timeline: Vec<CorrelatedEvent> = Vec::new();
    let mut services: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut metric_anomalies = 0usize;
    let mut error_logs = 0usize;
    let mut error_traces = 0usize;
    let mut deploy_count = 0usize;

    // 1. Query error events (traces with errors)
    let events = state.event_store.list_events(
        None, Some(ProtocolType::Otlp), None, service_filter.clone(),
    ).await;

    for event in &events {
        if event.timestamp_us < from || event.timestamp_us > to { continue; }
        let is_error = matches!(event.severity,
            crate::models::event::EventSeverity::High | crate::models::event::EventSeverity::Critical);
        if !is_error { continue; }

        let meta = &event.metadata.extra;
        let svc = event.service_id.clone().unwrap_or_else(|| "unknown".to_string());
        services.insert(svc.clone());
        error_traces += 1;

        timeline.push(CorrelatedEvent {
            timestamp_us: event.timestamp_us,
            signal_type: "trace_error".to_string(),
            severity: format!("{}", event.severity),
            title: meta.get("span_name").cloned().unwrap_or_else(|| "Span error".to_string()),
            description: format!("Trace error in {} (latency: {}µs)", svc, event.latency_us),
            service: Some(svc),
            trace_id: meta.get("trace_id").cloned(),
            attributes: meta.clone(),
        });
    }

    // 2. Query error logs
    let log_query = LogQuery {
        level: Some(crate::models::log_entry::LogLevel::Error),
        from_us: Some(from),
        to_us: Some(to),
        service: service_filter.clone(),
        limit: 50,
        ..Default::default()
    };

    if let Ok(logs) = state.log_store.query(log_query).await {
        for log in &logs {
            let svc = log.service.clone().unwrap_or_else(|| "unknown".to_string());
            services.insert(svc.clone());
            error_logs += 1;

            timeline.push(CorrelatedEvent {
                timestamp_us: log.timestamp_us,
                signal_type: "log_error".to_string(),
                severity: format!("{}", log.level),
                title: log.message.chars().take(120).collect(),
                description: format!("[{}] {}", log.level, log.message),
                service: Some(svc),
                trace_id: log.trace_id.clone(),
                attributes: HashMap::new(),
            });
        }
    }

    // 3. Query deploy annotations
    let annotations = state.annotation_store.list(Some(from), Some(to), service_filter.as_deref());
    for ann in &annotations {
        deploy_count += 1;
        timeline.push(CorrelatedEvent {
            timestamp_us: ann.timestamp_us,
            signal_type: "deploy".to_string(),
            severity: "info".to_string(),
            title: ann.title.clone(),
            description: ann.description.clone().unwrap_or_default(),
            service: ann.service.clone(),
            trace_id: None,
            attributes: ann.tags.clone(),
        });
    }

    // 4. Query metric anomalies (recent high-value metrics as proxy)
    if let Ok(names) = state.metric_store.get_names().await {
        for name in names.iter().take(20) {
            if let Ok(metrics) = state.metric_store.query(Some(name), &HashMap::new(), window).await {
                // Detect simple spike: if any value is > 2x the mean
                if metrics.len() > 2 {
                    let mean = metrics.iter().map(|m| m.value).sum::<f64>() / metrics.len() as f64;
                    for m in &metrics {
                        if m.value > mean * 2.0 && m.timestamp_us >= from && m.timestamp_us <= to {
                            metric_anomalies += 1;
                            if let Some(svc) = &m.service_id { services.insert(svc.clone()); }
                            timeline.push(CorrelatedEvent {
                                timestamp_us: m.timestamp_us,
                                signal_type: "metric_spike".to_string(),
                                severity: "warning".to_string(),
                                title: format!("{} spiked to {:.1}", name, m.value),
                                description: format!("{} = {:.2} (mean: {:.2}, {:.0}x)", name, m.value, mean, m.value / mean),
                                service: m.service_id.clone(),
                                trace_id: None,
                                attributes: m.tags.clone(),
                            });
                            break; // One spike per metric
                        }
                    }
                }
            }
        }
    }

    // Sort timeline by timestamp
    timeline.sort_by_key(|e| e.timestamp_us);

    // Determine likely cause (heuristic: earliest deploy before errors)
    let likely_cause = if deploy_count > 0 && (error_logs > 0 || error_traces > 0) {
        let first_deploy = timeline.iter().find(|e| e.signal_type == "deploy");
        first_deploy.map(|d| format!(
            "Likely caused by deployment '{}' at {}",
            d.title,
            format_timestamp(d.timestamp_us),
        ))
    } else if metric_anomalies > 0 && error_logs > 0 {
        Some(format!(
            "Metric anomalies ({}) correlated with {} error logs — possible resource exhaustion",
            metric_anomalies, error_logs
        ))
    } else {
        None
    };

    let total = timeline.len();
    let result = CorrelationResult {
        window_start_us: from,
        window_end_us: to,
        total_events: total,
        timeline,
        summary: CorrelationSummary {
            metric_anomalies,
            error_logs,
            error_traces,
            deploys: deploy_count,
            incidents: 0,
            services_affected: services.into_iter().collect(),
        },
        likely_cause,
    };

    let data = serde_json::to_value(&result).unwrap_or_default();
    StandardResponse::ok(data, "Correlation analysis complete.")
}

fn format_timestamp(us: u64) -> String {
    let secs = us / 1_000_000;
    let dt = chrono::DateTime::from_timestamp(secs as i64, 0)
        .map(|d| d.format("%H:%M:%S").to_string())
        .unwrap_or_else(|| "unknown".to_string());
    dt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_timestamp_epoch_zero() {
        assert_eq!(format_timestamp(0), "00:00:00");
    }

    #[test]
    fn format_timestamp_known_value() {
        // 3600 seconds = 01:00:00 UTC
        assert_eq!(format_timestamp(3600 * 1_000_000), "01:00:00");
    }

    #[test]
    fn format_timestamp_midday() {
        // 12:34:56 UTC
        let secs = 12 * 3600 + 34 * 60 + 56;
        assert_eq!(format_timestamp(secs * 1_000_000), "12:34:56");
    }

    #[test]
    fn format_timestamp_returns_8_chars() {
        let out = format_timestamp(1_700_000_000_000_000);
        assert_eq!(out.len(), 8);
        assert!(out.chars().nth(2) == Some(':'));
    }

    #[test]
    fn format_timestamp_truncates_subsecond() {
        // Microseconds within the same second produce the same output.
        let a = format_timestamp(1_700_000_000_000_000);
        let b = format_timestamp(1_700_000_000_999_999);
        assert_eq!(a, b);
    }

    #[test]
    fn format_timestamp_never_panics_on_max() {
        // Must not panic for extreme values
        let _ = format_timestamp(u64::MAX);
    }
}
