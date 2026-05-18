//! API Rate Limiting & Quotas
//!
//! Per-project/team ingestion quotas with configurable limits.
//! Prevents runaway costs and noisy-neighbor problems in multi-tenant deployments.

use crate::api::{ApiState, StandardResponse};
use axum::{
    extract::{State, Path},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use parking_lot::RwLock;

/// Quota definition for a project or team.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaDefinition {
    pub project_id: String,
    pub name: String,
    /// Max events per minute
    pub events_per_minute: u64,
    /// Max logs per minute
    pub logs_per_minute: u64,
    /// Max metrics per minute
    pub metrics_per_minute: u64,
    /// Max spans per minute
    pub spans_per_minute: u64,
    /// Max total bytes per minute
    pub bytes_per_minute: u64,
    /// Action when quota exceeded: "drop", "throttle", "warn"
    pub exceeded_action: String,
    pub enabled: bool,
}

/// Current usage tracked against quotas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaUsage {
    pub project_id: String,
    pub window_start: u64,
    pub events_count: u64,
    pub logs_count: u64,
    pub metrics_count: u64,
    pub spans_count: u64,
    pub bytes_count: u64,
    pub throttled_count: u64,
    pub dropped_count: u64,
}

/// Rate limiter state per project.
#[derive(Debug)]
struct RateLimiterState {
    events: u64,
    logs: u64,
    metrics: u64,
    spans: u64,
    bytes: u64,
    throttled: u64,
    dropped: u64,
    window_start: Instant,
}

impl RateLimiterState {
    fn new() -> Self {
        Self {
            events: 0, logs: 0, metrics: 0, spans: 0, bytes: 0,
            throttled: 0, dropped: 0, window_start: Instant::now(),
        }
    }

    fn reset_if_expired(&mut self) {
        if self.window_start.elapsed() > Duration::from_secs(60) {
            self.events = 0; self.logs = 0; self.metrics = 0;
            self.spans = 0; self.bytes = 0; self.throttled = 0;
            self.dropped = 0; self.window_start = Instant::now();
        }
    }
}

/// Rate limiter result.
#[derive(Debug, Clone, PartialEq)]
pub enum RateLimitResult {
    Allowed,
    Throttled,
    Dropped,
}

/// Quota Manager: tracks usage and enforces limits.
#[derive(Debug)]
pub struct QuotaManager {
    quotas: RwLock<HashMap<String, QuotaDefinition>>,
    usage: RwLock<HashMap<String, RateLimiterState>>,
}

impl QuotaManager {
    pub fn new() -> Self {
        Self {
            quotas: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
        }
    }

    pub fn set_quota(&self, quota: QuotaDefinition) {
        self.quotas.write().insert(quota.project_id.clone(), quota);
    }

    pub fn get_quota(&self, project_id: &str) -> Option<QuotaDefinition> {
        self.quotas.read().get(project_id).cloned()
    }

    pub fn list_quotas(&self) -> Vec<QuotaDefinition> {
        self.quotas.read().values().cloned().collect()
    }

    pub fn delete_quota(&self, project_id: &str) -> bool {
        self.quotas.write().remove(project_id).is_some()
    }

    /// Check if an ingestion request is allowed under the project's quota.
    pub fn check_rate(
        &self,
        project_id: &str,
        signal: &str,
        count: u64,
        bytes: u64,
    ) -> RateLimitResult {
        let quotas = self.quotas.read();
        let quota = match quotas.get(project_id) {
            Some(q) if q.enabled => q,
            _ => return RateLimitResult::Allowed, // No quota = unlimited
        };

        let mut usage = self.usage.write();
        let state = usage.entry(project_id.to_string()).or_insert_with(RateLimiterState::new);
        state.reset_if_expired();

        // Update usage
        state.bytes += bytes;
        match signal {
            "events" => state.events += count,
            "logs" => state.logs += count,
            "metrics" => state.metrics += count,
            "spans" => state.spans += count,
            _ => state.events += count,
        }

        // Check limits
        let over_limit = state.events > quota.events_per_minute
            || state.logs > quota.logs_per_minute
            || state.metrics > quota.metrics_per_minute
            || state.spans > quota.spans_per_minute
            || state.bytes > quota.bytes_per_minute;

        if over_limit {
            match quota.exceeded_action.as_str() {
                "drop" => { state.dropped += count; RateLimitResult::Dropped }
                "throttle" => { state.throttled += count; RateLimitResult::Throttled }
                _ => { state.throttled += count; RateLimitResult::Allowed } // "warn" = log but allow
            }
        } else {
            RateLimitResult::Allowed
        }
    }

    /// Get current usage stats for a project.
    pub fn get_usage(&self, project_id: &str) -> QuotaUsage {
        let usage = self.usage.read();
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_micros() as u64;

        match usage.get(project_id) {
            Some(state) => QuotaUsage {
                project_id: project_id.to_string(),
                window_start: now.saturating_sub(state.window_start.elapsed().as_micros() as u64),
                events_count: state.events, logs_count: state.logs,
                metrics_count: state.metrics, spans_count: state.spans,
                bytes_count: state.bytes, throttled_count: state.throttled,
                dropped_count: state.dropped,
            },
            None => QuotaUsage {
                project_id: project_id.to_string(), window_start: now,
                events_count: 0, logs_count: 0, metrics_count: 0, spans_count: 0,
                bytes_count: 0, throttled_count: 0, dropped_count: 0,
            },
        }
    }
}

impl Default for QuotaManager {
    fn default() -> Self { Self::new() }
}

/// POST /v1/quotas — Set a project quota.
pub async fn set_quota(State(state): State<Arc<ApiState>>, Json(quota): Json<QuotaDefinition>) -> impl IntoResponse {
    state.quota_manager.set_quota(quota);
    StandardResponse::ok(serde_json::json!({"set": true}), "Quota set.")
}

/// GET /v1/quotas — List all quotas.
pub async fn list_quotas(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    StandardResponse::ok(serde_json::to_value(&state.quota_manager.list_quotas()).unwrap_or_default(), "Quotas.")
}

/// GET /v1/quotas/:project_id/usage — Get current usage vs quota.
pub async fn get_usage(Path(project_id): Path<String>, State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let usage = state.quota_manager.get_usage(&project_id);
    let quota = state.quota_manager.get_quota(&project_id);
    StandardResponse::ok(serde_json::json!({ "usage": usage, "quota": quota }), "Quota usage.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiting() {
        let mgr = QuotaManager::new();
        mgr.set_quota(QuotaDefinition {
            project_id: "proj-1".into(), name: "Test".into(),
            events_per_minute: 100, logs_per_minute: 100,
            metrics_per_minute: 100, spans_per_minute: 100,
            bytes_per_minute: 1_000_000, exceeded_action: "drop".into(), enabled: true,
        });

        // Under limit
        assert_eq!(mgr.check_rate("proj-1", "events", 50, 1000), RateLimitResult::Allowed);
        // Still under
        assert_eq!(mgr.check_rate("proj-1", "events", 40, 1000), RateLimitResult::Allowed);
        // Over limit
        assert_eq!(mgr.check_rate("proj-1", "events", 20, 1000), RateLimitResult::Dropped);
    }

    #[test]
    fn test_no_quota_allows_all() {
        let mgr = QuotaManager::new();
        assert_eq!(mgr.check_rate("no-quota", "events", 1_000_000, 0), RateLimitResult::Allowed);
    }

    #[test]
    fn test_usage_tracking() {
        let mgr = QuotaManager::new();
        mgr.set_quota(QuotaDefinition {
            project_id: "proj-1".into(), name: "Test".into(),
            events_per_minute: 1000, logs_per_minute: 1000,
            metrics_per_minute: 1000, spans_per_minute: 1000,
            bytes_per_minute: 10_000_000, exceeded_action: "warn".into(), enabled: true,
        });

        mgr.check_rate("proj-1", "events", 50, 5000);
        mgr.check_rate("proj-1", "logs", 30, 3000);

        let usage = mgr.get_usage("proj-1");
        assert_eq!(usage.events_count, 50);
        assert_eq!(usage.logs_count, 30);
        assert_eq!(usage.bytes_count, 8000);
    }

    fn quota(action: &str) -> QuotaDefinition {
        QuotaDefinition {
            project_id: "p".into(), name: "n".into(),
            events_per_minute: 100, logs_per_minute: 100,
            metrics_per_minute: 100, spans_per_minute: 100,
            bytes_per_minute: 1_000_000, exceeded_action: action.into(), enabled: true,
        }
    }

    #[test]
    fn test_signal_type_logs() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        assert_eq!(mgr.check_rate("p", "logs", 50, 0), RateLimitResult::Allowed);
        assert_eq!(mgr.check_rate("p", "logs", 60, 0), RateLimitResult::Dropped);
    }

    #[test]
    fn test_signal_type_metrics() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        assert_eq!(mgr.check_rate("p", "metrics", 50, 0), RateLimitResult::Allowed);
        let u = mgr.get_usage("p");
        assert_eq!(u.metrics_count, 50);
    }

    #[test]
    fn test_signal_type_spans() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        assert_eq!(mgr.check_rate("p", "spans", 50, 0), RateLimitResult::Allowed);
        let u = mgr.get_usage("p");
        assert_eq!(u.spans_count, 50);
    }

    #[test]
    fn test_signal_type_unknown_defaults_to_events() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        mgr.check_rate("p", "unknown-signal", 10, 0);
        let u = mgr.get_usage("p");
        assert_eq!(u.events_count, 10);
    }

    #[test]
    fn test_throttle_action() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("throttle"));
        mgr.check_rate("p", "events", 100, 0);
        assert_eq!(mgr.check_rate("p", "events", 10, 0), RateLimitResult::Throttled);
    }

    #[test]
    fn test_warn_action_allows() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("warn"));
        mgr.check_rate("p", "events", 100, 0);
        assert_eq!(mgr.check_rate("p", "events", 10, 0), RateLimitResult::Allowed);
    }

    #[test]
    fn test_disabled_quota_allows() {
        let mgr = QuotaManager::new();
        let mut q = quota("drop");
        q.enabled = false;
        mgr.set_quota(q);
        assert_eq!(mgr.check_rate("p", "events", 1_000_000, 0), RateLimitResult::Allowed);
    }

    #[test]
    fn test_bytes_limit_triggers() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        assert_eq!(mgr.check_rate("p", "events", 1, 2_000_000), RateLimitResult::Dropped);
    }

    #[test]
    fn test_delete_quota_returns_true() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        assert!(mgr.delete_quota("p"));
        assert!(mgr.get_quota("p").is_none());
    }

    #[test]
    fn test_delete_quota_nonexistent() {
        let mgr = QuotaManager::new();
        assert!(!mgr.delete_quota("nope"));
    }

    #[test]
    fn test_list_quotas_empty() {
        let mgr = QuotaManager::new();
        assert!(mgr.list_quotas().is_empty());
    }

    #[test]
    fn test_list_quotas_multiple() {
        let mgr = QuotaManager::new();
        let mut q1 = quota("drop"); q1.project_id = "a".into();
        let mut q2 = quota("drop"); q2.project_id = "b".into();
        mgr.set_quota(q1);
        mgr.set_quota(q2);
        assert_eq!(mgr.list_quotas().len(), 2);
    }

    #[test]
    fn test_get_usage_no_activity() {
        let mgr = QuotaManager::new();
        let u = mgr.get_usage("fresh");
        assert_eq!(u.events_count, 0);
        assert_eq!(u.bytes_count, 0);
    }

    #[test]
    fn test_dropped_count_tracked() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        mgr.check_rate("p", "events", 100, 0);
        mgr.check_rate("p", "events", 50, 0); // over limit
        let u = mgr.get_usage("p");
        assert_eq!(u.dropped_count, 50);
    }

    #[test]
    fn test_throttled_count_tracked() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("throttle"));
        mgr.check_rate("p", "events", 100, 0);
        mgr.check_rate("p", "events", 25, 0);
        let u = mgr.get_usage("p");
        assert_eq!(u.throttled_count, 25);
    }

    #[test]
    fn test_set_quota_overwrites() {
        let mgr = QuotaManager::new();
        let mut q1 = quota("drop");
        q1.events_per_minute = 50;
        mgr.set_quota(q1);
        let mut q2 = quota("drop");
        q2.events_per_minute = 999;
        mgr.set_quota(q2);
        assert_eq!(mgr.get_quota("p").unwrap().events_per_minute, 999);
    }

    #[test]
    fn test_rate_limit_result_variants() {
        assert_ne!(RateLimitResult::Allowed, RateLimitResult::Throttled);
        assert_ne!(RateLimitResult::Throttled, RateLimitResult::Dropped);
        assert_eq!(RateLimitResult::Allowed, RateLimitResult::Allowed);
    }
}
