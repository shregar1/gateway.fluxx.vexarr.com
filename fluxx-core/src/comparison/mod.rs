use crate::models::diff::{MetricPoint, EnvDiff};
use crate::models::event::Event;
use crate::storage::EventStore;
use crate::storage::event_store::TimeWindow;
use crate::core::error::Result;
use std::sync::Arc;
use tracing::info;

/// ComparisonEngine: Analyzes historical event data to produce high-fidelity 
/// performance comparisons between deployment environments.
#[derive(Debug)]
pub struct ComparisonEngine {
    store: Arc<dyn EventStore>,
}

impl ComparisonEngine {
    pub fn new(store: Arc<dyn EventStore>) -> Self {
        Self { store }
    }

    /// Aggregates metrics for a specific service within an environment over the window.
    pub async fn aggregate_metrics(&self, env_id: &str, service_id: &str, window: TimeWindow) -> Result<MetricPoint> {
        let events = self.store.query(window).await?;
        
        let filtered: Vec<&Event> = events.iter()
            .filter(|e| e.environment_id.as_deref() == Some(env_id) && e.service_id.as_deref() == Some(service_id))
            .collect();
        
        if filtered.is_empty() {
            return Ok(MetricPoint::default());
        }

        let total_latency: u64 = filtered.iter().map(|e| e.latency_us).sum();
        let error_count: usize = filtered.iter().filter(|e| e.metadata.extra.get("status").map(|s| s != "200" && s != "OK").unwrap_or(false)).count();
        let count = filtered.len() as u64;

        Ok(MetricPoint {
            avg_latency_us: total_latency / count,
            error_rate: (error_count as f64) / (count as f64),
            throughput_rps: count / (window.end_us - window.start_us).max(1_000_000) * 1_000_000,
        })
    }

    /// Compares two environments for the same service.
    pub async fn compare_envs(&self, source_env: &str, target_env: &str, service_id: &str, window: TimeWindow) -> Result<EnvDiff> {
        info!("ComparisonEngine: Performing diff for '{}' vs '{}' on service '{}'.", source_env, target_env, service_id);
        
        let source_metrics = self.aggregate_metrics(source_env, service_id, window.clone()).await?;
        let target_metrics = self.aggregate_metrics(target_env, service_id, window).await?;
        
        Ok(EnvDiff::calculate(&source_metrics, &target_metrics, source_env, target_env))
    }
}
