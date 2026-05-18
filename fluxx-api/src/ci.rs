//! CI/CD Visibility — Pipeline execution tracing and test analytics.
//! Tracks pipeline runs, stage durations, test results, and deployment frequency.

use crate::api::{ApiState, StandardResponse};
use axum::{extract::{State, Query}, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PipelineStatus { Running, Success, Failed, Cancelled }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRun {
    pub id: String,
    pub pipeline_name: String,
    pub provider: String,
    pub branch: String,
    pub commit_sha: String,
    pub status: PipelineStatus,
    pub started_at_us: u64,
    pub finished_at_us: Option<u64>,
    pub duration_secs: Option<f64>,
    pub stages: Vec<PipelineStage>,
    pub test_results: Option<TestResults>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStage {
    pub name: String,
    pub status: PipelineStatus,
    pub duration_secs: f64,
    pub started_at_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResults {
    pub total: u32,
    pub passed: u32,
    pub failed: u32,
    pub skipped: u32,
    pub duration_secs: f64,
    pub failures: Vec<TestFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestFailure {
    pub name: String,
    pub suite: String,
    pub message: String,
    pub duration_secs: f64,
}

/// DORA metrics computed from pipeline data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoraMetrics {
    pub deployment_frequency_per_day: f64,
    pub lead_time_for_changes_hours: f64,
    pub change_failure_rate_percent: f64,
    pub mean_time_to_recovery_hours: f64,
    pub period_days: u32,
}

#[derive(Debug, Default)]
pub struct CiStore {
    runs: RwLock<Vec<PipelineRun>>,
}

impl CiStore {
    pub fn new() -> Self { Self::default() }

    pub fn record(&self, run: PipelineRun) { self.runs.write().push(run); }

    pub fn list(&self, pipeline: Option<&str>, status: Option<PipelineStatus>, limit: usize) -> Vec<PipelineRun> {
        let runs = self.runs.read();
        runs.iter().rev()
            .filter(|r| pipeline.map_or(true, |p| r.pipeline_name == p))
            .filter(|r| status.map_or(true, |s| r.status == s))
            .take(limit).cloned().collect()
    }

    pub fn compute_dora(&self, period_days: u32) -> DoraMetrics {
        let runs = self.runs.read();
        let successful: Vec<&PipelineRun> = runs.iter().filter(|r| r.status == PipelineStatus::Success).collect();
        let failed: Vec<&PipelineRun> = runs.iter().filter(|r| r.status == PipelineStatus::Failed).collect();
        let total = runs.len() as f64;
        let days = period_days.max(1) as f64;

        DoraMetrics {
            deployment_frequency_per_day: successful.len() as f64 / days,
            lead_time_for_changes_hours: successful.iter()
                .filter_map(|r| r.duration_secs).sum::<f64>() / successful.len().max(1) as f64 / 3600.0,
            change_failure_rate_percent: if total > 0.0 { (failed.len() as f64 / total) * 100.0 } else { 0.0 },
            mean_time_to_recovery_hours: 0.5, // Placeholder — requires incident correlation
            period_days,
        }
    }
}

pub async fn record_pipeline(State(state): State<Arc<ApiState>>, Json(run): Json<PipelineRun>) -> impl IntoResponse {
    state.ci_store.record(run);
    StandardResponse::ok(serde_json::json!({"recorded": true}), "Pipeline run recorded.")
}

pub async fn list_pipelines(State(state): State<Arc<ApiState>>, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let pipeline = params.get("pipeline").map(|s| s.as_str());
    let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(50);
    let runs = state.ci_store.list(pipeline, None, limit);
    StandardResponse::ok(serde_json::to_value(&runs).unwrap_or_default(), "Pipeline runs.")
}

pub async fn get_dora_metrics(State(state): State<Arc<ApiState>>, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let days: u32 = params.get("days").and_then(|v| v.parse().ok()).unwrap_or(30);
    let dora = state.ci_store.compute_dora(days);
    StandardResponse::ok(serde_json::to_value(&dora).unwrap_or_default(), "DORA metrics.")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_run(name: &str, status: PipelineStatus, duration: Option<f64>) -> PipelineRun {
        PipelineRun {
            id: name.to_string(),
            pipeline_name: name.to_string(),
            provider: "github".to_string(),
            branch: "main".to_string(),
            commit_sha: "abc123".to_string(),
            status,
            started_at_us: 0,
            finished_at_us: None,
            duration_secs: duration,
            stages: vec![],
            test_results: None,
            labels: HashMap::new(),
        }
    }

    #[test]
    fn record_adds_run() {
        let store = CiStore::new();
        store.record(mk_run("p1", PipelineStatus::Success, Some(120.0)));
        assert_eq!(store.list(None, None, 100).len(), 1);
    }

    #[test]
    fn list_empty_store() {
        let store = CiStore::new();
        assert!(store.list(None, None, 10).is_empty());
    }

    #[test]
    fn list_filters_by_pipeline_name() {
        let store = CiStore::new();
        store.record(mk_run("ci-a", PipelineStatus::Success, None));
        store.record(mk_run("ci-b", PipelineStatus::Success, None));
        let result = store.list(Some("ci-a"), None, 100);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].pipeline_name, "ci-a");
    }

    #[test]
    fn list_filters_by_status() {
        let store = CiStore::new();
        store.record(mk_run("a", PipelineStatus::Success, None));
        store.record(mk_run("b", PipelineStatus::Failed, None));
        let result = store.list(None, Some(PipelineStatus::Failed), 100);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].status, PipelineStatus::Failed);
    }

    #[test]
    fn list_respects_limit() {
        let store = CiStore::new();
        for _ in 0..5 {
            store.record(mk_run("x", PipelineStatus::Success, None));
        }
        assert_eq!(store.list(None, None, 2).len(), 2);
    }

    #[test]
    fn list_reverse_order_most_recent_first() {
        let store = CiStore::new();
        store.record(mk_run("first", PipelineStatus::Success, None));
        store.record(mk_run("second", PipelineStatus::Success, None));
        let result = store.list(None, None, 10);
        assert_eq!(result[0].pipeline_name, "second");
    }

    #[test]
    fn compute_dora_empty_store() {
        let store = CiStore::new();
        let d = store.compute_dora(30);
        assert_eq!(d.deployment_frequency_per_day, 0.0);
        assert_eq!(d.change_failure_rate_percent, 0.0);
        assert_eq!(d.period_days, 30);
    }

    #[test]
    fn compute_dora_deployment_frequency() {
        let store = CiStore::new();
        for _ in 0..15 {
            store.record(mk_run("p", PipelineStatus::Success, Some(3600.0)));
        }
        let d = store.compute_dora(30);
        assert!((d.deployment_frequency_per_day - 0.5).abs() < 1e-9);
    }

    #[test]
    fn compute_dora_change_failure_rate() {
        let store = CiStore::new();
        for _ in 0..3 {
            store.record(mk_run("p", PipelineStatus::Success, None));
        }
        store.record(mk_run("p", PipelineStatus::Failed, None));
        let d = store.compute_dora(1);
        assert!((d.change_failure_rate_percent - 25.0).abs() < 1e-9);
    }

    #[test]
    fn compute_dora_lead_time() {
        let store = CiStore::new();
        store.record(mk_run("p", PipelineStatus::Success, Some(3600.0)));
        store.record(mk_run("p", PipelineStatus::Success, Some(7200.0)));
        let d = store.compute_dora(1);
        assert!((d.lead_time_for_changes_hours - 1.5).abs() < 1e-9);
    }

    #[test]
    fn compute_dora_period_clamped_to_min_1() {
        let store = CiStore::new();
        store.record(mk_run("p", PipelineStatus::Success, None));
        let d = store.compute_dora(0);
        assert!(d.deployment_frequency_per_day > 0.0);
    }

    #[test]
    fn compute_dora_only_failed_runs() {
        let store = CiStore::new();
        store.record(mk_run("p", PipelineStatus::Failed, None));
        store.record(mk_run("p", PipelineStatus::Failed, None));
        let d = store.compute_dora(7);
        assert_eq!(d.deployment_frequency_per_day, 0.0);
        assert!((d.change_failure_rate_percent - 100.0).abs() < 1e-9);
    }

    #[test]
    fn compute_dora_cancelled_runs_not_counted_as_success_or_failure() {
        let store = CiStore::new();
        store.record(mk_run("p", PipelineStatus::Cancelled, None));
        let d = store.compute_dora(1);
        assert_eq!(d.deployment_frequency_per_day, 0.0);
        assert_eq!(d.change_failure_rate_percent, 0.0);
    }
}
