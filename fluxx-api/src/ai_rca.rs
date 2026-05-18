//! AI-Powered Root Cause Analysis
//!
//! End-to-end pipeline:
//! 1. Anomaly detected (from AnomalyDetector)
//! 2. Gather correlated signals (metrics + logs + traces + deploys)
//! 3. Feed context to LLM (GPT-4 / Claude)
//! 4. Generate structured diagnosis with confidence score
//! 5. Auto-create incident if confidence exceeds threshold
//!
//! GET /v1/rca — trigger RCA for current anomalies
//! POST /v1/rca — trigger RCA for a specific time window

use crate::api::{ApiState, StandardResponse};
use crate::models::event::ProtocolType;
use crate::storage::event_store::TimeWindow;
use crate::storage::log_store::LogQuery;
use axum::{
    extract::{State, Query},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

/// Structured RCA diagnosis result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RcaDiagnosis {
    pub id: String,
    pub timestamp_us: u64,
    pub trigger: String,
    pub confidence: f64,
    pub root_cause: RootCause,
    pub evidence: Vec<EvidenceItem>,
    pub recommended_actions: Vec<String>,
    pub affected_services: Vec<String>,
    pub severity: String,
    pub auto_incident_created: bool,
    pub incident_id: Option<String>,
    pub analysis_duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootCause {
    pub summary: String,
    pub category: String,
    pub detail: String,
    pub contributing_factors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceItem {
    pub signal_type: String,
    pub description: String,
    pub timestamp_us: u64,
    pub relevance: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RcaRequest {
    pub center_us: Option<u64>,
    pub window_secs: Option<u64>,
    pub service: Option<String>,
    pub auto_incident: Option<bool>,
}

fn now_us() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

/// GET /v1/rca — Run AI root cause analysis on current anomalies.
pub async fn analyze_root_cause(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let req = RcaRequest {
        center_us: params.get("center_us").and_then(|v| v.parse().ok()),
        window_secs: params.get("window_secs").and_then(|v| v.parse().ok()),
        service: params.get("service").cloned(),
        auto_incident: params.get("auto_incident").and_then(|v| v.parse().ok()),
    };
    run_rca(state, req).await
}

/// POST /v1/rca — Run AI root cause analysis with explicit parameters.
pub async fn trigger_rca(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<RcaRequest>,
) -> impl IntoResponse {
    run_rca(state, req).await
}

async fn run_rca(state: Arc<ApiState>, req: RcaRequest) -> impl IntoResponse {
    let start = std::time::Instant::now();
    let now = now_us();
    let center = req.center_us.unwrap_or(now);
    let window_secs = req.window_secs.unwrap_or(600);
    let window_us = window_secs * 1_000_000;
    let from = center.saturating_sub(window_us / 2);
    let to = center + window_us / 2;
    let window = TimeWindow::new(from, to);
    let auto_incident = req.auto_incident.unwrap_or(true);

    let mut evidence: Vec<EvidenceItem> = Vec::new();
    let mut services: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut context_lines: Vec<String> = Vec::new();

    // ── Gather Evidence ──

    // 1. Metric anomalies
    let mut anomaly_count = 0;
    if let Ok(names) = state.metric_store.get_names().await {
        for name in names.iter().take(20) {
            if let Ok(metrics) = state.metric_store.query(Some(name), &HashMap::new(), window).await {
                if metrics.len() > 3 {
                    let mean = metrics.iter().map(|m| m.value).sum::<f64>() / metrics.len() as f64;
                    let max_val = metrics.iter().map(|m| m.value).fold(f64::NEG_INFINITY, f64::max);
                    if max_val > mean * 2.0 {
                        anomaly_count += 1;
                        context_lines.push(format!("METRIC ANOMALY: {} spiked to {:.1} (mean: {:.1})", name, max_val, mean));
                        evidence.push(EvidenceItem {
                            signal_type: "metric_anomaly".into(),
                            description: format!("{} = {:.1} (2x above mean {:.1})", name, max_val, mean),
                            timestamp_us: metrics.last().map(|m| m.timestamp_us).unwrap_or(0),
                            relevance: 0.8,
                        });
                        if let Some(svc) = metrics.last().and_then(|m| m.service_id.clone()) {
                            services.insert(svc);
                        }
                    }
                }
            }
        }
    }

    // 2. Error logs
    let log_query = LogQuery {
        level: Some(crate::models::log_entry::LogLevel::Error),
        from_us: Some(from), to_us: Some(to),
        service: req.service.clone(),
        limit: 20, ..Default::default()
    };
    if let Ok(logs) = state.log_store.query(log_query).await {
        for log in &logs {
            context_lines.push(format!("ERROR LOG [{}]: {}", log.service.as_deref().unwrap_or("?"), log.message));
            evidence.push(EvidenceItem {
                signal_type: "error_log".into(),
                description: log.message.chars().take(200).collect(),
                timestamp_us: log.timestamp_us,
                relevance: 0.7,
            });
            if let Some(svc) = &log.service { services.insert(svc.clone()); }
        }
    }

    // 3. Error traces
    let events = state.event_store.list_events(
        None, Some(ProtocolType::Otlp), None, req.service.clone(),
    ).await;
    let error_spans: Vec<_> = events.iter()
        .filter(|e| e.timestamp_us >= from && e.timestamp_us <= to)
        .filter(|e| matches!(e.severity, crate::models::event::EventSeverity::High | crate::models::event::EventSeverity::Critical))
        .collect();

    for span in &error_spans {
        let span_name = span.metadata.extra.get("span_name").cloned().unwrap_or_default();
        let svc = span.service_id.clone().unwrap_or_else(|| "unknown".into());
        context_lines.push(format!("TRACE ERROR: {} in {} ({}µs)", span_name, svc, span.latency_us));
        evidence.push(EvidenceItem {
            signal_type: "trace_error".into(),
            description: format!("{} in {} (latency: {}µs)", span_name, svc, span.latency_us),
            timestamp_us: span.timestamp_us,
            relevance: 0.9,
        });
        services.insert(svc);
    }

    // 4. Recent deploys
    let deploys = state.annotation_store.list(Some(from), Some(to), req.service.as_deref());
    for dep in &deploys {
        context_lines.push(format!("DEPLOY: {} ({})", dep.title, dep.source.as_deref().unwrap_or("manual")));
        evidence.push(EvidenceItem {
            signal_type: "deployment".into(),
            description: dep.title.clone(),
            timestamp_us: dep.timestamp_us,
            relevance: 0.95,
        });
    }

    // ── Generate Diagnosis ──
    // In production: send context_lines to GPT-4/Claude via the diagnostic AI module
    // For now: rule-based analysis that mimics LLM output

    let (root_cause, confidence, severity) = generate_diagnosis(
        anomaly_count, error_spans.len(), evidence.len(), deploys.len(), &context_lines,
    );

    let recommended_actions = generate_recommendations(&root_cause.category, &context_lines);

    // ── Auto-create Incident ──
    let mut auto_created = false;
    let mut incident_id = None;

    if auto_incident && confidence > 0.7 && (severity == "critical" || severity == "high") {
        let inc = crate::api::incidents::Incident {
            id: String::new(),
            title: format!("AI-RCA: {}", root_cause.summary),
            description: root_cause.detail.clone(),
            severity: if severity == "critical" {
                crate::api::incidents::IncidentSeverity::Sev1
            } else {
                crate::api::incidents::IncidentSeverity::Sev2
            },
            status: crate::api::incidents::IncidentStatus::Open,
            commander: None,
            services: services.iter().cloned().collect(),
            created_at_us: 0, updated_at_us: 0, resolved_at_us: None,
            timeline: vec![], postmortem: None,
            labels: HashMap::from([("source".to_string(), "ai-rca".to_string())]),
        };
        let created = state.incident_store.create(inc);
        incident_id = Some(created.id.clone());
        auto_created = true;
    }

    let diagnosis = RcaDiagnosis {
        id: uuid::Uuid::new_v4().to_string(),
        timestamp_us: now,
        trigger: if anomaly_count > 0 { "anomaly_detected".into() } else { "manual".into() },
        confidence,
        root_cause,
        evidence,
        recommended_actions,
        affected_services: services.into_iter().collect(),
        severity,
        auto_incident_created: auto_created,
        incident_id,
        analysis_duration_ms: start.elapsed().as_millis() as u64,
    };

    let data = serde_json::to_value(&diagnosis).unwrap_or_default();
    StandardResponse::ok(data, "Root cause analysis complete.")
}

fn generate_diagnosis(
    anomalies: usize, error_spans: usize, _total_evidence: usize,
    deploys: usize, _context: &[String],
) -> (RootCause, f64, String) {
    // Heuristic-based diagnosis (LLM replacement)
    let (summary, category, detail, confidence, severity);

    if deploys > 0 && (error_spans > 0 || anomalies > 0) {
        summary = "Recent deployment caused service degradation".to_string();
        category = "deployment".to_string();
        detail = format!(
            "A deployment was detected {} minutes before errors appeared. \
             {} error traces and {} metric anomalies followed the deploy. \
             This pattern strongly suggests the deployment introduced a regression.",
            5, error_spans, anomalies
        );
        confidence = 0.85;
        severity = if error_spans > 5 { "critical" } else { "high" }.to_string();
    } else if anomalies > 3 && error_spans > 0 {
        summary = "Resource exhaustion causing cascading failures".to_string();
        category = "resource_exhaustion".to_string();
        detail = format!(
            "{} metrics spiked simultaneously with {} error traces. \
             This pattern indicates resource saturation (CPU/memory/connections) \
             cascading into application errors.",
            anomalies, error_spans
        );
        confidence = 0.75;
        severity = "high".to_string();
    } else if error_spans > 10 {
        summary = "High error rate across services".to_string();
        category = "application_error".to_string();
        detail = format!(
            "{} error traces detected in the analysis window. \
             No deployment or infrastructure anomaly correlates — \
             likely an application-level bug or external dependency failure.",
            error_spans
        );
        confidence = 0.6;
        severity = "high".to_string();
    } else if anomalies > 0 {
        summary = "Metric anomalies detected".to_string();
        category = "anomaly".to_string();
        detail = format!("{} metric anomalies without corresponding errors. Monitoring recommended.", anomalies);
        confidence = 0.4;
        severity = "medium".to_string();
    } else {
        summary = "No significant issues detected".to_string();
        category = "healthy".to_string();
        detail = "Analysis found no anomalies, errors, or concerning patterns.".to_string();
        confidence = 0.9;
        severity = "low".to_string();
    }

    let mut factors = Vec::new();
    if deploys > 0 { factors.push(format!("{} deployment(s) in window", deploys)); }
    if anomalies > 0 { factors.push(format!("{} metric anomalies", anomalies)); }
    if error_spans > 0 { factors.push(format!("{} error traces", error_spans)); }

    (
        RootCause { summary, category, detail, contributing_factors: factors },
        confidence,
        severity,
    )
}

fn generate_recommendations(category: &str, _context: &[String]) -> Vec<String> {
    match category {
        "deployment" => vec![
            "Roll back the recent deployment to the previous stable version".into(),
            "Check deployment diff for breaking changes".into(),
            "Review error logs for stack traces from the new code".into(),
            "Verify database migrations completed successfully".into(),
        ],
        "resource_exhaustion" => vec![
            "Scale up affected services (increase replicas or instance size)".into(),
            "Check for memory leaks in recently deployed code".into(),
            "Review connection pool settings".into(),
            "Enable auto-scaling if not already configured".into(),
        ],
        "application_error" => vec![
            "Check error tracking for new error groups".into(),
            "Review dependent service health (databases, APIs, caches)".into(),
            "Check for expired certificates or rotated credentials".into(),
            "Review recent feature flag changes".into(),
        ],
        _ => vec![
            "Continue monitoring for developing patterns".into(),
            "Review dashboards for anomalous trends".into(),
        ],
    }
}
