use crate::models::metric::{Metric, AggregatedMetric};
use crate::storage::event_store::TimeWindow;
use crate::core::error::Result;
use async_trait::async_trait;
use std::collections::HashMap;

/// MetricStore: Interface for high-performance metric persistence and aggregation.
#[async_trait]
pub trait MetricStore: Send + Sync + std::fmt::Debug {
    /// Inserts a metric data point.
    async fn insert(&self, metric: Metric) -> Result<()>;

    /// Queries raw metrics with optional filters.
    async fn query(
        &self,
        name: Option<&str>,
        tags: &HashMap<String, String>,
        window: TimeWindow,
    ) -> Result<Vec<Metric>>;

    /// Returns aggregated metrics for a given name over a time window.
    async fn get_aggregated(
        &self,
        name: &str,
        window: TimeWindow,
        interval_secs: u64,
    ) -> Result<Vec<AggregatedMetric>>;

    /// Returns all known metric names.
    async fn get_names(&self) -> Result<Vec<String>>;

    /// Returns the total number of stored metric data points.
    async fn len(&self) -> Result<usize>;
}
