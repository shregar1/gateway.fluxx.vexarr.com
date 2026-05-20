//! Write-through MetricStore: generic over any hot + cold pair.

use crate::core::error::Result;
use crate::models::metric::{AggregatedMetric, Metric};
use crate::storage::event_store::TimeWindow;
use crate::storage::metric_store::MetricStore;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Debug)]
pub struct WriteThroughMetricStore {
    hot: Arc<dyn MetricStore>,
    cold: Arc<dyn MetricStore>,
    cold_tx: mpsc::Sender<Metric>,
}

impl WriteThroughMetricStore {
    pub fn new(
        hot: Arc<dyn MetricStore>,
        cold: Arc<dyn MetricStore>,
        channel_capacity: usize,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Metric>(channel_capacity);
        let cold_drain = cold.clone();
        tokio::spawn(async move {
            while let Some(metric) = rx.recv().await {
                if let Err(e) = cold_drain.insert(metric).await {
                    warn!("WriteThroughMetricStore: cold insert failed: {e}");
                }
            }
        });
        Self { hot, cold, cold_tx: tx }
    }
}

#[async_trait]
impl MetricStore for WriteThroughMetricStore {
    async fn insert(&self, metric: Metric) -> Result<()> {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.cold_tx.try_send(metric.clone()) {
            warn!("WriteThroughMetricStore: cold channel saturated, dropping durable copy");
        }
        self.hot.insert(metric).await
    }

    async fn query(
        &self,
        name: Option<&str>,
        tags: &HashMap<String, String>,
        window: TimeWindow,
    ) -> Result<Vec<Metric>> {
        let hot = self.hot.query(name, tags, window).await?;
        if !hot.is_empty() {
            return Ok(hot);
        }
        self.cold.query(name, tags, window).await
    }

    async fn get_aggregated(
        &self,
        name: &str,
        window: TimeWindow,
        interval_secs: u64,
    ) -> Result<Vec<AggregatedMetric>> {
        let hot = self.hot.get_aggregated(name, window, interval_secs).await?;
        if !hot.is_empty() {
            return Ok(hot);
        }
        self.cold.get_aggregated(name, window, interval_secs).await
    }

    async fn get_names(&self) -> Result<Vec<String>> {
        let mut names = self.hot.get_names().await.unwrap_or_default();
        let cold_names = self.cold.get_names().await.unwrap_or_default();
        names.extend(cold_names);
        names.sort();
        names.dedup();
        Ok(names)
    }

    async fn len(&self) -> Result<usize> {
        self.cold.len().await
    }
}
