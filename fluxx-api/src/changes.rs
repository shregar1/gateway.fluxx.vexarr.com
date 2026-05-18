//! Change Intelligence
//!
//! Tracks config/infra changes and correlates them with performance regressions.
//! "Response time increased 40% — likely cause: deployment abc123 12 min ago."

use crate::api::{ApiState, StandardResponse};
use axum::{
    extract::{State, Query},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use parking_lot::RwLock;

/// A tracked change event (deploy, config change, feature flag, scaling event).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub id: String,
    pub change_type: ChangeType,
    pub title: String,
    pub description: Option<String>,
    pub service: Option<String>,
    pub environment: Option<String>,
    pub author: Option<String>,
    pub reference: Option<String>,
    pub timestamp_us: u64,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    Deployment,
    ConfigChange,
    FeatureFlag,
    ScalingEvent,
    InfraChange,
    DatabaseMigration,
    SecurityPatch,
    Rollback,
}

/// Regression detected and correlated with a change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionAnalysis {
    pub metric_name: String,
    pub before_value: f64,
    pub after_value: f64,
    pub change_percent: f64,
    pub direction: String,
    pub likely_change: Option<ChangeEvent>,
    pub confidence: f64,
    pub detected_at_us: u64,
}

fn now_us() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

#[derive(Debug, Default)]
pub struct ChangeStore {
    changes: RwLock<Vec<ChangeEvent>>,
}

impl ChangeStore {
    pub fn new() -> Self { Self::default() }

    pub fn record(&self, mut change: ChangeEvent) {
        if change.id.is_empty() { change.id = uuid::Uuid::new_v4().to_string(); }
        if change.timestamp_us == 0 { change.timestamp_us = now_us(); }
        self.changes.write().push(change);
    }

    pub fn list(&self, service: Option<&str>, change_type: Option<&str>, limit: usize) -> Vec<ChangeEvent> {
        self.changes.read().iter().rev()
            .filter(|c| service.map_or(true, |s| c.service.as_deref() == Some(s)))
            .filter(|c| change_type.map_or(true, |t| format!("{:?}", c.change_type).to_lowercase().contains(t)))
            .take(limit).cloned().collect()
    }

    /// Find the most recent change before a given timestamp for a service.
    pub fn find_preceding_change(&self, service: Option<&str>, before_us: u64) -> Option<ChangeEvent> {
        self.changes.read().iter().rev()
            .filter(|c| c.timestamp_us < before_us)
            .filter(|c| service.map_or(true, |s| c.service.as_deref() == Some(s) || c.service.is_none()))
            .next().cloned()
    }
}

/// POST /v1/changes — Record a change event.
pub async fn record_change(
    State(state): State<Arc<ApiState>>,
    Json(change): Json<ChangeEvent>,
) -> impl IntoResponse {
    state.change_store.record(change);
    StandardResponse::ok(serde_json::json!({"recorded": true}), "Change recorded.")
}

/// GET /v1/changes — List change events.
pub async fn list_changes(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let service = params.get("service").map(|s| s.as_str());
    let change_type = params.get("type").map(|s| s.as_str());
    let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(50);
    let changes = state.change_store.list(service, change_type, limit);
    StandardResponse::ok(serde_json::to_value(&changes).unwrap_or_default(), "Changes.")
}

/// GET /v1/changes/regressions — Detect regressions correlated with recent changes.
pub async fn detect_regressions(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let service = params.get("service").map(|s| s.as_str());
    let lookback_secs: u64 = params.get("lookback_secs").and_then(|v| v.parse().ok()).unwrap_or(3600);
    let now = now_us();
    let lookback_us = lookback_secs * 1_000_000;

    let mut regressions = Vec::new();

    // Check each metric for significant changes
    if let Ok(names) = state.metric_store.get_names().await {
        let before_window = crate::storage::event_store::TimeWindow::new(
            now.saturating_sub(lookback_us * 2), now.saturating_sub(lookback_us),
        );
        let after_window = crate::storage::event_store::TimeWindow::new(
            now.saturating_sub(lookback_us), now,
        );

        for name in names.iter().take(30) {
            let before = state.metric_store.get_aggregated(name, before_window, lookback_secs).await;
            let after = state.metric_store.get_aggregated(name, after_window, lookback_secs).await;

            if let (Ok(b), Ok(a)) = (before, after) {
                if let (Some(bv), Some(av)) = (b.last(), a.last()) {
                    if bv.avg > 0.0 {
                        let change_pct = ((av.avg - bv.avg) / bv.avg) * 100.0;
                        if change_pct.abs() > 20.0 {
                            let direction = if change_pct > 0.0 { "increased" } else { "decreased" };
                            let likely_change = state.change_store.find_preceding_change(
                                service, now.saturating_sub(lookback_us),
                            );
                            let confidence = if likely_change.is_some() { 0.75 } else { 0.3 };

                            regressions.push(RegressionAnalysis {
                                metric_name: name.clone(),
                                before_value: bv.avg,
                                after_value: av.avg,
                                change_percent: change_pct,
                                direction: direction.to_string(),
                                likely_change,
                                confidence,
                                detected_at_us: now,
                            });
                        }
                    }
                }
            }
        }
    }

    regressions.sort_by(|a, b| b.change_percent.abs().partial_cmp(&a.change_percent.abs()).unwrap_or(std::cmp::Ordering::Equal));
    StandardResponse::ok(serde_json::to_value(&regressions).unwrap_or_default(), "Regression analysis.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_change_store() {
        let store = ChangeStore::new();
        store.record(ChangeEvent {
            id: String::new(), change_type: ChangeType::Deployment,
            title: "Deploy v1.2".into(), description: None,
            service: Some("api".into()), environment: Some("prod".into()),
            author: Some("alice".into()), reference: Some("abc123".into()),
            timestamp_us: 1000, metadata: HashMap::new(),
        });
        assert_eq!(store.list(None, None, 10).len(), 1);
        assert!(store.find_preceding_change(Some("api"), 2000).is_some());
        assert!(store.find_preceding_change(Some("api"), 500).is_none());
    }
}
