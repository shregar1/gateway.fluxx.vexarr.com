use crate::models::metric::{Metric, AggregatedMetric};
use crate::storage::event_store::TimeWindow;
use crate::storage::metric_store::MetricStore;
use crate::core::error::Result;
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio::sync::RwLock;

/// In-memory metric store using BTreeMap for efficient time-range queries.
#[derive(Debug)]
pub struct InMemoryMetricStore {
    data: RwLock<BTreeMap<u64, Vec<Metric>>>,
    names: RwLock<HashSet<String>>,
}

impl InMemoryMetricStore {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
            names: RwLock::new(HashSet::new()),
        }
    }
}

#[async_trait]
impl MetricStore for InMemoryMetricStore {
    async fn insert(&self, metric: Metric) -> Result<()> {
        {
            let mut names = self.names.write().await;
            names.insert(metric.name.clone());
        }
        let mut data = self.data.write().await;
        data.entry(metric.timestamp_us).or_default().push(metric);
        Ok(())
    }

    async fn query(
        &self,
        name: Option<&str>,
        tags: &HashMap<String, String>,
        window: TimeWindow,
    ) -> Result<Vec<Metric>> {
        let data = self.data.read().await;
        let mut results = Vec::new();

        for (_ts, metrics) in data.range(window.start_us..=window.end_us) {
            for m in metrics {
                if let Some(n) = name {
                    if m.name != n {
                        continue;
                    }
                }
                if !tags.is_empty() {
                    let matches = tags.iter().all(|(k, v)| m.tags.get(k) == Some(v));
                    if !matches {
                        continue;
                    }
                }
                results.push(m.clone());
            }
        }

        Ok(results)
    }

    async fn get_aggregated(
        &self,
        name: &str,
        window: TimeWindow,
        interval_secs: u64,
    ) -> Result<Vec<AggregatedMetric>> {
        let data = self.data.read().await;
        let interval_us = interval_secs * 1_000_000;

        // Bucket values by interval
        let mut buckets: BTreeMap<u64, Vec<f64>> = BTreeMap::new();

        for (_ts, metrics) in data.range(window.start_us..=window.end_us) {
            for m in metrics {
                if m.name != name {
                    continue;
                }
                let bucket_key = (m.timestamp_us / interval_us) * interval_us;
                buckets.entry(bucket_key).or_default().push(m.value);
            }
        }

        let results = buckets
            .into_iter()
            .map(|(ts, mut values)| AggregatedMetric::from_values(name, ts, &mut values))
            .collect();

        Ok(results)
    }

    async fn get_names(&self) -> Result<Vec<String>> {
        let names = self.names.read().await;
        let mut result: Vec<String> = names.iter().cloned().collect();
        result.sort();
        Ok(result)
    }

    async fn len(&self) -> Result<usize> {
        let data = self.data.read().await;
        Ok(data.values().map(|v| v.len()).sum())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::metric::MetricType;

    #[tokio::test]
    async fn test_insert_and_query() {
        let store = InMemoryMetricStore::new();
        let m = Metric::new("cpu.usage".to_string(), 75.0, MetricType::Gauge)
            .with_timestamp(1000);
        store.insert(m).await.unwrap();

        let results = store
            .query(Some("cpu.usage"), &HashMap::new(), TimeWindow::new(0, 2000))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, 75.0);
    }

    #[tokio::test]
    async fn test_get_names() {
        let store = InMemoryMetricStore::new();
        store.insert(Metric::new("a".to_string(), 1.0, MetricType::Counter)).await.unwrap();
        store.insert(Metric::new("b".to_string(), 2.0, MetricType::Gauge)).await.unwrap();

        let names = store.get_names().await.unwrap();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn test_aggregation() {
        let store = InMemoryMetricStore::new();
        for i in 0..10 {
            let m = Metric::new("latency".to_string(), i as f64, MetricType::Histogram)
                .with_timestamp(1_000_000 + i * 100);
            store.insert(m).await.unwrap();
        }

        let agg = store
            .get_aggregated("latency", TimeWindow::new(0, 10_000_000), 60)
            .await
            .unwrap();
        assert!(!agg.is_empty());
        assert_eq!(agg[0].count, 10);
    }
}
