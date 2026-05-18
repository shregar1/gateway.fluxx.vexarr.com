use crate::models::metric::{Metric, MetricType, AggregatedMetric};
use crate::storage::metric_store::MetricStore;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{info, error};

/// Aggregates StatsD metrics over configurable flush intervals.
pub struct MetricAggregator {
    counters: RwLock<HashMap<String, f64>>,
    gauges: RwLock<HashMap<String, f64>>,
    histograms: RwLock<HashMap<String, Vec<f64>>>,
    sets: RwLock<HashMap<String, HashSet<String>>>,
    /// Tags associated with each metric name (last seen tags)
    metric_tags: RwLock<HashMap<String, HashMap<String, String>>>,
    flush_interval: Duration,
    metric_store: Arc<dyn MetricStore>,
    metric_bus: broadcast::Sender<Arc<Metric>>,
}

impl MetricAggregator {
    pub fn new(
        flush_interval_secs: u64,
        metric_store: Arc<dyn MetricStore>,
        metric_bus: broadcast::Sender<Arc<Metric>>,
    ) -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
            sets: RwLock::new(HashMap::new()),
            metric_tags: RwLock::new(HashMap::new()),
            flush_interval: Duration::from_secs(flush_interval_secs),
            metric_store,
            metric_bus,
        }
    }

    /// Record a parsed metric into the appropriate aggregation bucket.
    pub fn record(&self, metric: &Metric) {
        let key = metric_key(&metric.name, &metric.tags);

        // Store tags for this metric
        {
            let mut tags = self.metric_tags.write();
            tags.insert(key.clone(), metric.tags.clone());
        }

        // Apply sample rate correction for counters
        let adjusted_value = if metric.sample_rate > 0.0 && metric.sample_rate < 1.0 {
            metric.value / metric.sample_rate
        } else {
            metric.value
        };

        match metric.metric_type {
            MetricType::Counter => {
                let mut counters = self.counters.write();
                *counters.entry(key).or_insert(0.0) += adjusted_value;
            }
            MetricType::Gauge => {
                let mut gauges = self.gauges.write();
                gauges.insert(key, metric.value);
            }
            MetricType::Histogram | MetricType::Timing | MetricType::Distribution => {
                let mut histograms = self.histograms.write();
                histograms.entry(key).or_default().push(metric.value);
            }
            MetricType::Set => {
                let mut sets = self.sets.write();
                sets.entry(key).or_default().insert(format!("{}", metric.value));
            }
            _ => {}
        }
    }

    /// Flush all accumulated metrics: compute aggregates, emit to store and bus, reset.
    /// Collects all metrics under the lock, then emits after releasing it (Send-safe).
    pub async fn flush(&self) {
        let mut to_emit: Vec<Metric> = Vec::new();

        // Collect counter metrics
        {
            let mut counters = self.counters.write();
            let tags_map = self.metric_tags.read();
            for (key, value) in counters.drain() {
                let (name, tags) = resolve_name_tags(&key, &tags_map);
                to_emit.push(Metric::new(name, value, MetricType::Counter).with_tags(tags));
            }
        }

        // Collect gauge metrics (don't reset — gauges persist their last value)
        {
            let gauges = self.gauges.read();
            let tags_map = self.metric_tags.read();
            for (key, value) in gauges.iter() {
                let (name, tags) = resolve_name_tags(key, &tags_map);
                to_emit.push(Metric::new(name, *value, MetricType::Gauge).with_tags(tags));
            }
        }

        // Collect histogram/timing metrics
        {
            let mut histograms = self.histograms.write();
            let tags_map = self.metric_tags.read();
            for (key, mut values) in histograms.drain() {
                if values.is_empty() {
                    continue;
                }
                let (name, tags) = resolve_name_tags(&key, &tags_map);
                let agg = AggregatedMetric::from_values(&name, 0, &mut values);
                to_emit.push(Metric::new(name, agg.sum, MetricType::Histogram).with_tags(tags));
            }
        }

        // Collect set metrics (emit cardinality)
        {
            let mut sets = self.sets.write();
            let tags_map = self.metric_tags.read();
            for (key, unique_values) in sets.drain() {
                let (name, tags) = resolve_name_tags(&key, &tags_map);
                to_emit.push(Metric::new(name, unique_values.len() as f64, MetricType::Set).with_tags(tags));
            }
        }

        // Emit all collected metrics (no locks held)
        let flushed = to_emit.len();
        for metric in to_emit {
            self.emit(metric).await;
        }

        if flushed > 0 {
            info!("StatsD aggregator: Flushed {} metrics", flushed);
        }
    }

    async fn emit(&self, metric: Metric) {
        let metric_arc = Arc::new(metric.clone());
        let _ = self.metric_bus.send(metric_arc);
        if let Err(e) = self.metric_store.insert(metric).await {
            error!("StatsD aggregator store error: {}", e);
        }
    }

    /// Run the flush loop on an interval timer.
    pub async fn run_flush_loop(&self) {
        let mut interval = tokio::time::interval(self.flush_interval);
        loop {
            interval.tick().await;
            self.flush().await;
        }
    }
}

/// Create a unique key combining metric name and sorted tags.
fn metric_key(name: &str, tags: &HashMap<String, String>) -> String {
    if tags.is_empty() {
        return name.to_string();
    }
    let mut sorted_tags: Vec<(&String, &String)> = tags.iter().collect();
    sorted_tags.sort_by_key(|(k, _)| k.as_str());
    let tag_str: Vec<String> = sorted_tags.iter().map(|(k, v)| format!("{}:{}", k, v)).collect();
    format!("{}|{}", name, tag_str.join(","))
}

/// Resolve the metric name and tags from a key.
fn resolve_name_tags(
    key: &str,
    tags_map: &HashMap<String, HashMap<String, String>>,
) -> (String, HashMap<String, String>) {
    let name = key.split('|').next().unwrap_or(key).to_string();
    let tags = tags_map.get(key).cloned().unwrap_or_default();
    (name, tags)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::in_memory_metric::InMemoryMetricStore;

    #[test]
    fn test_metric_key_no_tags() {
        let key = metric_key("cpu.usage", &HashMap::new());
        assert_eq!(key, "cpu.usage");
    }

    #[test]
    fn test_metric_key_with_tags() {
        let tags = HashMap::from([
            ("env".to_string(), "prod".to_string()),
            ("host".to_string(), "web1".to_string()),
        ]);
        let key = metric_key("cpu.usage", &tags);
        assert!(key.starts_with("cpu.usage|"));
        assert!(key.contains("env:prod"));
        assert!(key.contains("host:web1"));
    }

    #[tokio::test]
    async fn test_counter_aggregation() {
        let store = Arc::new(InMemoryMetricStore::new());
        let (tx, _) = broadcast::channel(64);
        let agg = MetricAggregator::new(10, store.clone(), tx);

        let m1 = Metric::new("req.count".to_string(), 1.0, MetricType::Counter);
        let m2 = Metric::new("req.count".to_string(), 3.0, MetricType::Counter);
        agg.record(&m1);
        agg.record(&m2);
        agg.flush().await;

        let names = store.get_names().await.unwrap();
        assert!(names.contains(&"req.count".to_string()));
    }

    #[tokio::test]
    async fn test_gauge_persistence() {
        let store = Arc::new(InMemoryMetricStore::new());
        let (tx, _) = broadcast::channel(64);
        let agg = MetricAggregator::new(10, store.clone(), tx);

        let m = Metric::new("temp".to_string(), 22.5, MetricType::Gauge);
        agg.record(&m);
        agg.flush().await;

        // Gauge should still be present after flush
        let gauges = agg.gauges.read();
        assert!(gauges.contains_key("temp"));
    }

    #[test]
    fn test_sample_rate_correction() {
        let store = Arc::new(InMemoryMetricStore::new());
        let (tx, _) = broadcast::channel(64);
        let agg = MetricAggregator::new(10, store, tx);

        let m = Metric::new("req.count".to_string(), 1.0, MetricType::Counter)
            .with_sample_rate(0.5);
        agg.record(&m);

        let counters = agg.counters.read();
        // 1.0 / 0.5 = 2.0
        assert_eq!(*counters.get("req.count").unwrap(), 2.0);
    }
}
