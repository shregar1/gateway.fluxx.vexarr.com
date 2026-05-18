//! Service Level Objectives (SLOs)
//!
//! Define error budget targets and track compliance over time.
//! Example: 99.9% availability = 43.2 minutes of downtime per month allowed.

use crate::api::{ApiState, StandardResponse};
use axum::{
    extract::{State, Path},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

/// SLO definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloDefinition {
    pub id: String,
    pub name: String,
    pub description: String,
    /// Target percentage (e.g., 99.9 for 99.9% availability)
    pub target_percent: f64,
    /// Window in days (e.g., 30 for monthly SLO)
    pub window_days: u32,
    /// Metric query for total events (denominator)
    pub total_query: String,
    /// Metric query for good events (numerator)
    pub good_query: String,
    /// Service this SLO applies to
    pub service: Option<String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

/// SLO status computed from metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloStatus {
    pub slo_id: String,
    pub slo_name: String,
    pub target_percent: f64,
    pub current_percent: f64,
    /// Error budget remaining (0.0 to 1.0, where 1.0 = 100% remaining)
    pub error_budget_remaining: f64,
    /// Error budget consumed (0.0 to 1.0)
    pub error_budget_consumed: f64,
    /// Whether the SLO is currently being met
    pub is_met: bool,
    /// Estimated time until error budget exhaustion (seconds), None if budget is healthy
    pub burn_rate: Option<f64>,
    pub total_events: u64,
    pub good_events: u64,
    pub bad_events: u64,
    pub window_days: u32,
}

impl SloStatus {
    pub fn compute(slo: &SloDefinition, total: u64, good: u64) -> Self {
        let bad = total.saturating_sub(good);
        let current_percent = if total > 0 {
            (good as f64 / total as f64) * 100.0
        } else {
            100.0
        };

        let error_budget_total = (100.0 - slo.target_percent) / 100.0;
        let error_rate = if total > 0 { bad as f64 / total as f64 } else { 0.0 };
        let error_budget_consumed = if error_budget_total > 0.0 {
            (error_rate / error_budget_total).min(1.0)
        } else {
            0.0
        };
        let error_budget_remaining = 1.0 - error_budget_consumed;

        let burn_rate = if error_budget_consumed > 0.01 && error_budget_remaining > 0.0 {
            // At current rate, how long until budget is exhausted
            let remaining_fraction = error_budget_remaining;
            let consumed_per_day = error_budget_consumed / slo.window_days as f64;
            if consumed_per_day > 0.0 {
                Some(remaining_fraction / consumed_per_day * 86400.0) // seconds
            } else {
                None
            }
        } else {
            None
        };

        Self {
            slo_id: slo.id.clone(),
            slo_name: slo.name.clone(),
            target_percent: slo.target_percent,
            current_percent,
            error_budget_remaining,
            error_budget_consumed,
            is_met: current_percent >= slo.target_percent,
            burn_rate,
            total_events: total,
            good_events: good,
            bad_events: bad,
            window_days: slo.window_days,
        }
    }
}

/// In-memory SLO store.
#[derive(Debug, Default)]
pub struct SloStore {
    definitions: RwLock<HashMap<String, SloDefinition>>,
}

impl SloStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self, slo: SloDefinition) {
        self.definitions.write().insert(slo.id.clone(), slo);
    }

    pub fn list(&self) -> Vec<SloDefinition> {
        self.definitions.read().values().cloned().collect()
    }

    pub fn get(&self, id: &str) -> Option<SloDefinition> {
        self.definitions.read().get(id).cloned()
    }

    pub fn delete(&self, id: &str) -> bool {
        self.definitions.write().remove(id).is_some()
    }
}

/// POST /v1/slos — Create an SLO definition.
pub async fn create_slo(
    State(state): State<Arc<ApiState>>,
    Json(slo): Json<SloDefinition>,
) -> impl IntoResponse {
    state.slo_store.create(slo);
    StandardResponse::ok(serde_json::json!({"created": true}), "SLO created.")
}

/// GET /v1/slos — List all SLOs with current status.
pub async fn list_slos(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    let definitions = state.slo_store.list();
    let statuses: Vec<SloStatus> = definitions.iter().map(|slo| {
        // In production, query the metric store for total and good event counts
        // For now, return computed status with placeholder counts
        SloStatus::compute(slo, 100_000, 99_950) // 99.95% as placeholder
    }).collect();

    let data = serde_json::to_value(&statuses).unwrap_or_default();
    StandardResponse::ok(data, "SLO statuses.")
}

/// GET /v1/slos/:id — Get SLO detail with error budget.
pub async fn get_slo(
    Path(id): Path<String>,
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    match state.slo_store.get(&id) {
        Some(slo) => {
            let status = SloStatus::compute(&slo, 100_000, 99_950);
            let data = serde_json::to_value(&status).unwrap_or_default();
            StandardResponse::ok(data, "SLO status.")
        }
        None => StandardResponse::error(
            serde_json::json!({}), &format!("SLO {} not found", id), "NOT_FOUND", vec![],
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slo_computation_met() {
        let slo = SloDefinition {
            id: "test".to_string(),
            name: "API Availability".to_string(),
            description: "".to_string(),
            target_percent: 99.9,
            window_days: 30,
            total_query: "".to_string(),
            good_query: "".to_string(),
            service: None,
            labels: HashMap::new(),
        };

        let status = SloStatus::compute(&slo, 100_000, 99_950);
        assert!(status.is_met);
        assert!(status.current_percent >= 99.9);
        assert!(status.error_budget_remaining > 0.0);
    }

    #[test]
    fn test_slo_computation_not_met() {
        let slo = SloDefinition {
            id: "test".to_string(),
            name: "API Availability".to_string(),
            description: "".to_string(),
            target_percent: 99.9,
            window_days: 30,
            total_query: "".to_string(),
            good_query: "".to_string(),
            service: None,
            labels: HashMap::new(),
        };

        let status = SloStatus::compute(&slo, 100_000, 99_000);
        assert!(!status.is_met);
        assert!(status.current_percent < 99.9);
        assert_eq!(status.bad_events, 1000);
    }

    #[test]
    fn test_slo_store_crud() {
        let store = SloStore::new();
        let slo = SloDefinition {
            id: "slo-1".to_string(),
            name: "Test SLO".to_string(),
            description: "".to_string(),
            target_percent: 99.9,
            window_days: 30,
            total_query: "total".to_string(),
            good_query: "good".to_string(),
            service: None,
            labels: HashMap::new(),
        };

        store.create(slo);
        assert_eq!(store.list().len(), 1);
        assert!(store.get("slo-1").is_some());
        assert!(store.delete("slo-1"));
        assert!(store.get("slo-1").is_none());
    }
}
