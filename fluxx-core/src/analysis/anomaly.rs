//! ML Anomaly Detection Engine
//!
//! Detects anomalies in metric time-series using statistical methods:
//! - Exponential Weighted Moving Average (EWMA) for baseline tracking
//! - Z-score based deviation detection
//! - Seasonal decomposition (hourly patterns)
//! - No manual thresholds required — learns from data automatically

use crate::storage::metric_store::MetricStore;
use crate::storage::event_store::TimeWindow;
use crate::analysis::insights::{Insight, InsightLevel};
use crate::models::event::ProtocolType;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use parking_lot::RwLock;
use tracing::{info, warn};

/// Anomaly detection result for a single metric.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Anomaly {
    pub metric_name: String,
    pub current_value: f64,
    pub expected_value: f64,
    pub deviation: f64,
    pub z_score: f64,
    pub severity: AnomalySeverity,
    pub timestamp_us: u64,
}

#[derive(Debug, Clone, Copy, serde::Serialize, PartialEq)]
pub enum AnomalySeverity {
    Low,       // z-score 2-3
    Medium,    // z-score 3-4
    High,      // z-score 4-5
    Critical,  // z-score > 5
}

impl AnomalySeverity {
    fn from_zscore(z: f64) -> Self {
        let abs = z.abs();
        if abs > 5.0 { Self::Critical }
        else if abs > 4.0 { Self::High }
        else if abs > 3.0 { Self::Medium }
        else { Self::Low }
    }
}

/// Per-metric baseline state tracked by the EWMA algorithm.
#[derive(Debug, Clone)]
struct MetricBaseline {
    ewma: f64,
    ewma_variance: f64,
    sample_count: u64,
    // Seasonal buckets: 24 hourly averages for daily patterns
    hourly_means: [f64; 24],
    hourly_counts: [u64; 24],
}

impl MetricBaseline {
    fn new() -> Self {
        Self {
            ewma: 0.0,
            ewma_variance: 0.0,
            sample_count: 0,
            hourly_means: [0.0; 24],
            hourly_counts: [0; 24],
        }
    }

    /// Update the baseline with a new observation.
    fn update(&mut self, value: f64, hour: usize, alpha: f64) {
        if self.sample_count == 0 {
            self.ewma = value;
            self.ewma_variance = 0.0;
        } else {
            let diff = value - self.ewma;
            self.ewma += alpha * diff;
            self.ewma_variance = (1.0 - alpha) * (self.ewma_variance + alpha * diff * diff);
        }
        self.sample_count += 1;

        // Update seasonal component
        let h = hour % 24;
        let count = self.hourly_counts[h] as f64;
        self.hourly_means[h] = (self.hourly_means[h] * count + value) / (count + 1.0);
        self.hourly_counts[h] += 1;
    }

    /// Get the expected value (EWMA + seasonal adjustment).
    fn expected(&self, hour: usize) -> f64 {
        let h = hour % 24;
        if self.hourly_counts[h] > 10 {
            // Use seasonal mean if we have enough data
            self.hourly_means[h]
        } else {
            self.ewma
        }
    }

    /// Get the standard deviation estimate.
    fn stddev(&self) -> f64 {
        if self.sample_count < 3 {
            return f64::MAX; // Not enough data
        }
        self.ewma_variance.sqrt().max(0.001) // Avoid division by zero
    }

    /// Compute z-score for a value.
    fn z_score(&self, value: f64, hour: usize) -> f64 {
        let expected = self.expected(hour);
        let stddev = self.stddev();
        (value - expected) / stddev
    }
}

/// Anomaly Detection Engine
/// Learns baselines per metric and detects deviations automatically.
pub struct AnomalyDetector {
    baselines: RwLock<HashMap<String, MetricBaseline>>,
    metric_store: Arc<dyn MetricStore>,
    /// EWMA smoothing factor (0-1). Lower = more smoothing.
    alpha: f64,
    /// Z-score threshold for anomaly detection.
    z_threshold: f64,
    /// Minimum samples before detection activates per metric.
    min_samples: u64,
    /// Check interval.
    check_interval: Duration,
}

impl AnomalyDetector {
    pub fn new(
        metric_store: Arc<dyn MetricStore>,
        check_interval_secs: u64,
    ) -> Self {
        Self {
            baselines: RwLock::new(HashMap::new()),
            metric_store,
            alpha: 0.1,       // 10% weight to new values
            z_threshold: 2.5,  // 2.5 standard deviations
            min_samples: 20,   // Need 20+ data points
            check_interval: Duration::from_secs(check_interval_secs),
        }
    }

    /// Run the detection loop.
    pub async fn run(&self) {
        info!("AnomalyDetector: Starting (alpha={}, z_threshold={}, min_samples={})",
            self.alpha, self.z_threshold, self.min_samples);

        let mut interval = tokio::time::interval(self.check_interval);
        loop {
            interval.tick().await;
            match self.detect().await {
                Ok(anomalies) => {
                    if !anomalies.is_empty() {
                        info!("AnomalyDetector: Found {} anomalies", anomalies.len());
                    }
                }
                Err(e) => warn!("AnomalyDetector: Detection failed: {}", e),
            }
        }
    }

    /// Single detection pass: query recent metrics, update baselines, detect anomalies.
    pub async fn detect(&self) -> anyhow::Result<Vec<Anomaly>> {
        let now_us = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // Look back one check interval
        let lookback_us = self.check_interval.as_micros() as u64;
        let window = TimeWindow::new(now_us.saturating_sub(lookback_us * 2), now_us);

        let names = self.metric_store.get_names().await?;
        let mut anomalies = Vec::new();

        // Current hour for seasonal decomposition
        let hour = (now_us / 1_000_000 / 3600 % 24) as usize;

        for name in &names {
            let metrics = self.metric_store
                .query(Some(name), &HashMap::new(), window)
                .await?;

            if metrics.is_empty() {
                continue;
            }

            let mut baselines = self.baselines.write();
            let baseline = baselines.entry(name.clone()).or_insert_with(MetricBaseline::new);

            for m in &metrics {
                baseline.update(m.value, hour, self.alpha);
            }

            // Only detect anomalies if we have enough data
            if baseline.sample_count < self.min_samples {
                continue;
            }

            // Check the most recent value
            if let Some(latest) = metrics.last() {
                let z = baseline.z_score(latest.value, hour);

                if z.abs() > self.z_threshold {
                    anomalies.push(Anomaly {
                        metric_name: name.clone(),
                        current_value: latest.value,
                        expected_value: baseline.expected(hour),
                        deviation: latest.value - baseline.expected(hour),
                        z_score: z,
                        severity: AnomalySeverity::from_zscore(z),
                        timestamp_us: latest.timestamp_us,
                    });
                }
            }
        }

        Ok(anomalies)
    }

    /// Convert anomalies to Insights for the alerting pipeline.
    pub fn anomalies_to_insights(anomalies: &[Anomaly]) -> Vec<Insight> {
        anomalies.iter().map(|a| {
            let level = match a.severity {
                AnomalySeverity::Critical => InsightLevel::Critical,
                AnomalySeverity::High => InsightLevel::Critical,
                AnomalySeverity::Medium => InsightLevel::Anomaly,
                AnomalySeverity::Low => InsightLevel::Warning,
            };
            let direction = if a.deviation > 0.0 { "above" } else { "below" };
            Insight {
                protocol: ProtocolType::System,
                level,
                message: format!(
                    "Anomaly: {} is {:.2} ({} expected {:.2}, z-score: {:.1})",
                    a.metric_name, a.current_value, direction, a.expected_value, a.z_score
                ),
                metadata: HashMap::from([
                    ("metric".to_string(), a.metric_name.clone()),
                    ("z_score".to_string(), format!("{:.2}", a.z_score)),
                    ("severity".to_string(), format!("{:?}", a.severity)),
                ]),
            }
        }).collect()
    }

    /// Get current baselines for diagnostics.
    pub fn get_baselines(&self) -> HashMap<String, (f64, f64, u64)> {
        let baselines = self.baselines.read();
        baselines.iter().map(|(name, b)| {
            (name.clone(), (b.ewma, b.stddev(), b.sample_count))
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::metric::{Metric, MetricType};
    use crate::storage::in_memory_metric::InMemoryMetricStore;

    #[test]
    fn test_baseline_learning() {
        let mut baseline = MetricBaseline::new();
        // Feed stable values
        for _ in 0..50 {
            baseline.update(100.0, 12, 0.1);
        }
        assert!((baseline.ewma - 100.0).abs() < 1.0);
        assert!(baseline.stddev() < 1.0);
    }

    #[test]
    fn test_anomaly_detection_spike() {
        let mut baseline = MetricBaseline::new();
        // Build stable baseline
        for _ in 0..100 {
            baseline.update(50.0, 12, 0.1);
        }
        // A massive spike should have high z-score
        let z = baseline.z_score(500.0, 12);
        assert!(z.abs() > 2.5, "z-score {} should exceed threshold", z);
    }

    #[test]
    fn test_anomaly_severity_levels() {
        assert_eq!(AnomalySeverity::from_zscore(2.5), AnomalySeverity::Low);
        assert_eq!(AnomalySeverity::from_zscore(3.5), AnomalySeverity::Medium);
        assert_eq!(AnomalySeverity::from_zscore(4.5), AnomalySeverity::High);
        assert_eq!(AnomalySeverity::from_zscore(6.0), AnomalySeverity::Critical);
    }

    #[test]
    fn test_seasonal_decomposition() {
        let mut baseline = MetricBaseline::new();
        // Morning traffic pattern: high at hour 9
        for _ in 0..50 {
            baseline.update(200.0, 9, 0.1);
        }
        // Night traffic: low at hour 3
        for _ in 0..50 {
            baseline.update(10.0, 3, 0.1);
        }
        // Expected at hour 9 should be ~200, at hour 3 should be ~10
        assert!((baseline.expected(9) - 200.0).abs() < 10.0);
        assert!((baseline.expected(3) - 10.0).abs() < 5.0);
    }

    #[tokio::test]
    async fn test_detector_with_store() {
        let store = Arc::new(InMemoryMetricStore::new());
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        // Insert stable metrics
        for i in 0..30 {
            let m = Metric::new("cpu".to_string(), 50.0, MetricType::Gauge)
                .with_timestamp(now - (30 - i) * 1_000_000);
            store.insert(m).await.unwrap();
        }
        // Insert anomalous value
        let spike = Metric::new("cpu".to_string(), 500.0, MetricType::Gauge)
            .with_timestamp(now);
        store.insert(spike).await.unwrap();

        let detector = AnomalyDetector::new(store, 60);
        let anomalies = detector.detect().await.unwrap();
        // Should detect the spike
        assert!(!anomalies.is_empty(), "Should detect CPU spike anomaly");
        assert_eq!(anomalies[0].metric_name, "cpu");
    }
}
