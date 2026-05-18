use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// Metric types supported by Fluxx (compatible with StatsD/DogStatsD and OpenTelemetry).
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Set,
    Timing,
    Distribution,
    Summary,
}

impl std::fmt::Display for MetricType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricType::Counter => write!(f, "counter"),
            MetricType::Gauge => write!(f, "gauge"),
            MetricType::Histogram => write!(f, "histogram"),
            MetricType::Set => write!(f, "set"),
            MetricType::Timing => write!(f, "timing"),
            MetricType::Distribution => write!(f, "distribution"),
            MetricType::Summary => write!(f, "summary"),
        }
    }
}

/// A single metric data point, compatible with StatsD, DogStatsD, and OpenTelemetry metrics.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Metric {
    pub id: String,
    pub name: String,
    pub value: f64,
    pub metric_type: MetricType,
    pub tags: HashMap<String, String>,
    pub timestamp_us: u64,
    pub sample_rate: f64,
    pub service_id: Option<String>,
    pub project_id: Option<String>,
    pub environment_id: Option<String>,
}

impl Metric {
    pub fn new(name: String, value: f64, metric_type: MetricType) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name,
            value,
            metric_type,
            tags: HashMap::new(),
            timestamp_us: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
            sample_rate: 1.0,
            service_id: None,
            project_id: None,
            environment_id: None,
        }
    }

    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = tags;
        self
    }

    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.sample_rate = rate;
        self
    }

    pub fn with_timestamp(mut self, timestamp_us: u64) -> Self {
        self.timestamp_us = timestamp_us;
        self
    }
}

/// Aggregated metric result for time-series queries.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AggregatedMetric {
    pub name: String,
    pub timestamp_us: u64,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub avg: f64,
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
}

impl AggregatedMetric {
    pub fn from_values(name: &str, timestamp_us: u64, values: &mut Vec<f64>) -> Self {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let count = values.len() as u64;
        let sum: f64 = values.iter().sum();
        let min = values.first().copied().unwrap_or(0.0);
        let max = values.last().copied().unwrap_or(0.0);
        let avg = if count > 0 { sum / count as f64 } else { 0.0 };

        Self {
            name: name.to_string(),
            timestamp_us,
            count,
            sum,
            min,
            max,
            avg,
            p50: percentile(values, 50.0),
            p90: percentile(values, 90.0),
            p95: percentile(values, 95.0),
            p99: percentile(values, 99.0),
        }
    }
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (pct / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_new() {
        let m = Metric::new("cpu.usage".to_string(), 75.5, MetricType::Gauge);
        assert_eq!(m.name, "cpu.usage");
        assert_eq!(m.value, 75.5);
        assert_eq!(m.metric_type, MetricType::Gauge);
        assert_eq!(m.sample_rate, 1.0);
        assert!(m.timestamp_us > 0);
    }

    #[test]
    fn test_metric_with_tags() {
        let tags = HashMap::from([("env".to_string(), "prod".to_string())]);
        let m = Metric::new("req.count".to_string(), 1.0, MetricType::Counter)
            .with_tags(tags.clone());
        assert_eq!(m.tags, tags);
    }

    #[test]
    fn test_metric_serialization() {
        let m = Metric::new("test".to_string(), 42.0, MetricType::Counter);
        let json = serde_json::to_string(&m).unwrap();
        assert!(json.contains("\"test\""));
        assert!(json.contains("counter"));
    }

    #[test]
    fn test_aggregated_metric() {
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let agg = AggregatedMetric::from_values("test", 1000, &mut values);
        assert_eq!(agg.count, 10);
        assert_eq!(agg.sum, 55.0);
        assert_eq!(agg.min, 1.0);
        assert_eq!(agg.max, 10.0);
        assert_eq!(agg.avg, 5.5);
    }

    #[test]
    fn test_aggregated_metric_empty() {
        let mut values = vec![];
        let agg = AggregatedMetric::from_values("empty", 0, &mut values);
        assert_eq!(agg.count, 0);
        assert_eq!(agg.avg, 0.0);
    }

    #[test]
    fn test_metric_type_display() {
        assert_eq!(MetricType::Counter.to_string(), "counter");
        assert_eq!(MetricType::Gauge.to_string(), "gauge");
        assert_eq!(MetricType::Histogram.to_string(), "histogram");
    }

    #[test]
    fn test_metric_type_display_all_variants() {
        assert_eq!(MetricType::Set.to_string(), "set");
        assert_eq!(MetricType::Timing.to_string(), "timing");
        assert_eq!(MetricType::Distribution.to_string(), "distribution");
        assert_eq!(MetricType::Summary.to_string(), "summary");
    }

    #[test]
    fn test_metric_with_sample_rate() {
        let m = Metric::new("x".into(), 1.0, MetricType::Counter)
            .with_sample_rate(0.25);
        assert_eq!(m.sample_rate, 0.25);
    }

    #[test]
    fn test_metric_with_timestamp() {
        let m = Metric::new("x".into(), 1.0, MetricType::Gauge)
            .with_timestamp(12345);
        assert_eq!(m.timestamp_us, 12345);
    }

    #[test]
    fn test_metric_has_unique_ids() {
        let a = Metric::new("x".into(), 1.0, MetricType::Counter);
        let b = Metric::new("x".into(), 1.0, MetricType::Counter);
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn test_metric_defaults_none() {
        let m = Metric::new("x".into(), 1.0, MetricType::Gauge);
        assert!(m.service_id.is_none());
        assert!(m.project_id.is_none());
        assert!(m.environment_id.is_none());
    }

    #[test]
    fn test_aggregated_single_value() {
        let mut v = vec![42.0];
        let agg = AggregatedMetric::from_values("x", 1, &mut v);
        assert_eq!(agg.count, 1);
        assert_eq!(agg.min, 42.0);
        assert_eq!(agg.max, 42.0);
        assert_eq!(agg.avg, 42.0);
        assert_eq!(agg.p50, 42.0);
    }

    #[test]
    fn test_aggregated_unsorted_input() {
        let mut v = vec![5.0, 1.0, 3.0, 2.0, 4.0];
        let agg = AggregatedMetric::from_values("x", 0, &mut v);
        assert_eq!(agg.min, 1.0);
        assert_eq!(agg.max, 5.0);
        assert_eq!(agg.avg, 3.0);
    }

    #[test]
    fn test_aggregated_percentiles_large() {
        let mut v: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let agg = AggregatedMetric::from_values("x", 0, &mut v);
        assert!(agg.p99 >= 95.0);
        assert!(agg.p50 >= 45.0 && agg.p50 <= 55.0);
    }

    #[test]
    fn test_metric_type_equality() {
        assert_eq!(MetricType::Counter, MetricType::Counter);
        assert_ne!(MetricType::Counter, MetricType::Gauge);
    }

    #[test]
    fn test_metric_type_deserialize() {
        let t: MetricType = serde_json::from_str("\"gauge\"").unwrap();
        assert_eq!(t, MetricType::Gauge);
    }

    #[test]
    fn test_aggregated_negative_values() {
        let mut v = vec![-5.0, -1.0, 0.0, 1.0, 5.0];
        let agg = AggregatedMetric::from_values("x", 0, &mut v);
        assert_eq!(agg.sum, 0.0);
        assert_eq!(agg.min, -5.0);
        assert_eq!(agg.max, 5.0);
    }
}
