//! API Rate Limiting & Quotas
//!
//! Per-project/team ingestion quotas with configurable limits.
//! Prevents runaway costs and noisy-neighbor problems in multi-tenant deployments.
//!
//! The per-key + per-signal counting + window reset + drop/throttle/warn
//! policy lives in `utilx::ratelimit::multi_dim`. This file owns the
//! fluxx-specific shape: the named `QuotaDefinition` fields the dashboard
//! posts, the named `QuotaUsage` fields the dashboard reads, and the
//! axum handlers wired into StandardResponse.

use crate::{ApiState, StandardResponse};
use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use utilx::ratelimit::multi_dim::{
    CheckResult, ExceededAction, MultiDimConfig, MultiDimLimiter, UsageSnapshot,
};

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

impl QuotaDefinition {
    /// Project the fluxx-shaped quota into the generic utilx multi-dim
    /// config. "warn" is the safe fallback for unrecognised actions —
    /// matches the historical fluxx behaviour where an unknown action
    /// admitted the traffic.
    fn to_multi_dim(&self) -> MultiDimConfig {
        let mut signal_limits = HashMap::new();
        signal_limits.insert("events".to_string(), self.events_per_minute);
        signal_limits.insert("logs".to_string(), self.logs_per_minute);
        signal_limits.insert("metrics".to_string(), self.metrics_per_minute);
        signal_limits.insert("spans".to_string(), self.spans_per_minute);
        let action = ExceededAction::parse(&self.exceeded_action).unwrap_or(ExceededAction::Warn);
        MultiDimConfig {
            window: Duration::from_secs(60),
            signal_limits,
            // Unknown signals fall under "events" semantics in the
            // historical fluxx behaviour — apply the events ceiling.
            default_signal_limit: self.events_per_minute,
            bytes_limit: self.bytes_per_minute,
            action,
            enabled: self.enabled,
        }
    }
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

impl QuotaUsage {
    fn from_snapshot(project_id: &str, snap: &UsageSnapshot) -> Self {
        let get = |k: &str| snap.signal_counts.get(k).copied().unwrap_or(0);
        Self {
            project_id: project_id.to_string(),
            window_start: snap.window_start_us,
            events_count: get("events"),
            logs_count: get("logs"),
            metrics_count: get("metrics"),
            spans_count: get("spans"),
            bytes_count: snap.bytes_count,
            throttled_count: snap.throttled_count,
            dropped_count: snap.dropped_count,
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

impl From<CheckResult> for RateLimitResult {
    fn from(r: CheckResult) -> Self {
        match r {
            // `Warned` still admits the traffic — historical fluxx behaviour
            // for "warn" reported `Allowed` to callers, with the counter
            // moving and surfacing later in QuotaUsage. Preserve that.
            CheckResult::Allowed | CheckResult::Warned => Self::Allowed,
            CheckResult::Throttled => Self::Throttled,
            CheckResult::Dropped => Self::Dropped,
        }
    }
}

/// Quota Manager: tracks usage and enforces limits.
///
/// Owns the source-of-truth `QuotaDefinition` map (fluxx-shaped, what the
/// dashboard reads/writes) and projects it into the generic
/// `utilx::ratelimit::multi_dim::MultiDimLimiter` for the actual counting.
#[derive(Debug)]
pub struct QuotaManager {
    quotas: parking_lot::RwLock<HashMap<String, QuotaDefinition>>,
    limiter: MultiDimLimiter,
}

impl QuotaManager {
    pub fn new() -> Self {
        Self {
            quotas: parking_lot::RwLock::new(HashMap::new()),
            limiter: MultiDimLimiter::new(),
        }
    }

    pub fn set_quota(&self, quota: QuotaDefinition) {
        let project_id = quota.project_id.clone();
        let cfg = quota.to_multi_dim();
        self.quotas.write().insert(project_id.clone(), quota);
        self.limiter.set_config(project_id, cfg);
    }

    pub fn get_quota(&self, project_id: &str) -> Option<QuotaDefinition> {
        self.quotas.read().get(project_id).cloned()
    }

    pub fn list_quotas(&self) -> Vec<QuotaDefinition> {
        self.quotas.read().values().cloned().collect()
    }

    pub fn delete_quota(&self, project_id: &str) -> bool {
        let removed = self.quotas.write().remove(project_id).is_some();
        if removed {
            self.limiter.remove_config(project_id);
        }
        removed
    }

    /// Check if an ingestion request is allowed under the project's quota.
    pub fn check_rate(
        &self,
        project_id: &str,
        signal: &str,
        count: u64,
        bytes: u64,
    ) -> RateLimitResult {
        // Historical fluxx contract: any unrecognised signal name is
        // lumped into the `events` bucket for both counting and limit
        // enforcement. Preserve that so the dashboard's per-signal
        // breakdown stays consistent.
        let effective_signal = match signal {
            "events" | "logs" | "metrics" | "spans" => signal,
            _ => "events",
        };
        self.limiter
            .check(project_id, effective_signal, count, bytes)
            .into()
    }

    /// Get current usage stats for a project.
    pub fn get_usage(&self, project_id: &str) -> QuotaUsage {
        QuotaUsage::from_snapshot(project_id, &self.limiter.usage(project_id))
    }
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

/// POST /v1/quotas — Set a project quota.
pub async fn set_quota(
    State(state): State<Arc<ApiState>>,
    Json(quota): Json<QuotaDefinition>,
) -> impl IntoResponse {
    state.quota_manager.set_quota(quota);
    StandardResponse::ok(serde_json::json!({"set": true}), "Quota set.")
}

/// GET /v1/quotas — List all quotas.
pub async fn list_quotas(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    StandardResponse::ok(
        serde_json::to_value(state.quota_manager.list_quotas()).unwrap_or_default(),
        "Quotas.",
    )
}

/// GET /v1/quotas/:project_id/usage — Get current usage vs quota.
pub async fn get_usage(
    Path(project_id): Path<String>,
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    let usage = state.quota_manager.get_usage(&project_id);
    let quota = state.quota_manager.get_quota(&project_id);
    StandardResponse::ok(
        serde_json::json!({ "usage": usage, "quota": quota }),
        "Quota usage.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiting() {
        let mgr = QuotaManager::new();
        mgr.set_quota(QuotaDefinition {
            project_id: "proj-1".into(),
            name: "Test".into(),
            events_per_minute: 100,
            logs_per_minute: 100,
            metrics_per_minute: 100,
            spans_per_minute: 100,
            bytes_per_minute: 1_000_000,
            exceeded_action: "drop".into(),
            enabled: true,
        });

        // Under limit
        assert_eq!(
            mgr.check_rate("proj-1", "events", 50, 1000),
            RateLimitResult::Allowed
        );
        // Still under
        assert_eq!(
            mgr.check_rate("proj-1", "events", 40, 1000),
            RateLimitResult::Allowed
        );
        // Over limit
        assert_eq!(
            mgr.check_rate("proj-1", "events", 20, 1000),
            RateLimitResult::Dropped
        );
    }

    #[test]
    fn test_no_quota_allows_all() {
        let mgr = QuotaManager::new();
        assert_eq!(
            mgr.check_rate("no-quota", "events", 1_000_000, 0),
            RateLimitResult::Allowed
        );
    }

    #[test]
    fn test_usage_tracking() {
        let mgr = QuotaManager::new();
        mgr.set_quota(QuotaDefinition {
            project_id: "proj-1".into(),
            name: "Test".into(),
            events_per_minute: 1000,
            logs_per_minute: 1000,
            metrics_per_minute: 1000,
            spans_per_minute: 1000,
            bytes_per_minute: 10_000_000,
            exceeded_action: "warn".into(),
            enabled: true,
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
            project_id: "p".into(),
            name: "n".into(),
            events_per_minute: 100,
            logs_per_minute: 100,
            metrics_per_minute: 100,
            spans_per_minute: 100,
            bytes_per_minute: 1_000_000,
            exceeded_action: action.into(),
            enabled: true,
        }
    }

    #[test]
    fn test_signal_type_logs() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        assert_eq!(
            mgr.check_rate("p", "logs", 50, 0),
            RateLimitResult::Allowed
        );
        assert_eq!(
            mgr.check_rate("p", "logs", 60, 0),
            RateLimitResult::Dropped
        );
    }

    #[test]
    fn test_signal_type_metrics() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        assert_eq!(
            mgr.check_rate("p", "metrics", 50, 0),
            RateLimitResult::Allowed
        );
        let u = mgr.get_usage("p");
        assert_eq!(u.metrics_count, 50);
    }

    #[test]
    fn test_signal_type_spans() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        assert_eq!(
            mgr.check_rate("p", "spans", 50, 0),
            RateLimitResult::Allowed
        );
        let u = mgr.get_usage("p");
        assert_eq!(u.spans_count, 50);
    }

    #[test]
    fn test_signal_type_unknown_defaults_to_events() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        mgr.check_rate("p", "unknown-signal", 10, 0);
        let u = mgr.get_usage("p");
        // Historical fluxx behaviour: unknown signals lump into the
        // `events` bucket. Preserved through `check_rate`'s rewrite.
        assert_eq!(u.events_count, 10);
    }

    #[test]
    fn test_throttle_action() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("throttle"));
        mgr.check_rate("p", "events", 100, 0);
        assert_eq!(
            mgr.check_rate("p", "events", 10, 0),
            RateLimitResult::Throttled
        );
    }

    #[test]
    fn test_warn_action_allows() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("warn"));
        mgr.check_rate("p", "events", 100, 0);
        // `Warned` maps back to `Allowed` per the historical contract.
        assert_eq!(
            mgr.check_rate("p", "events", 10, 0),
            RateLimitResult::Allowed
        );
    }

    #[test]
    fn test_disabled_quota_allows() {
        let mgr = QuotaManager::new();
        let mut q = quota("drop");
        q.enabled = false;
        mgr.set_quota(q);
        assert_eq!(
            mgr.check_rate("p", "events", 1_000_000, 0),
            RateLimitResult::Allowed
        );
    }

    #[test]
    fn test_bytes_limit_triggers() {
        let mgr = QuotaManager::new();
        mgr.set_quota(quota("drop"));
        assert_eq!(
            mgr.check_rate("p", "events", 1, 2_000_000),
            RateLimitResult::Dropped
        );
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
        let mut q1 = quota("drop");
        q1.project_id = "a".into();
        let mut q2 = quota("drop");
        q2.project_id = "b".into();
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
