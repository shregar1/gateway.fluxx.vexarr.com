//! Metric Alert Evaluator
//!
//! Periodically queries metric thresholds and fires alerts when conditions are met.
//! Bridges the MetricStore with the AlertManager via the RuleEngine.

use crate::alerts::config::AlertRuleConfig;
use crate::alerts::rules::{RuleEngine, MetricValue};
use crate::alerts::manager::AlertManager;
use crate::storage::metric_store::MetricStore;
use crate::storage::event_store::TimeWindow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{info, error};

/// Periodically evaluates metric-based alert rules against the MetricStore.
pub struct MetricAlertEvaluator {
    metric_store: Arc<dyn MetricStore>,
    alert_manager: Arc<AlertManager>,
    rule_engine: RuleEngine,
    check_interval: Duration,
    lookback_secs: u64,
}

impl MetricAlertEvaluator {
    pub fn new(
        metric_store: Arc<dyn MetricStore>,
        alert_manager: Arc<AlertManager>,
        rules: Vec<AlertRuleConfig>,
        check_interval_secs: u64,
        lookback_secs: u64,
    ) -> Self {
        Self {
            metric_store,
            alert_manager,
            rule_engine: RuleEngine::new(rules),
            check_interval: Duration::from_secs(check_interval_secs),
            lookback_secs,
        }
    }

    /// Run the evaluation loop. Checks metric thresholds on every tick.
    pub async fn run(&self) {
        info!(
            "MetricAlertEvaluator: Starting evaluation loop (interval={}s, lookback={}s)",
            self.check_interval.as_secs(),
            self.lookback_secs,
        );

        let mut interval = tokio::time::interval(self.check_interval);
        loop {
            interval.tick().await;
            if let Err(e) = self.evaluate().await {
                error!("MetricAlertEvaluator: Evaluation failed: {}", e);
            }
        }
    }

    /// Single evaluation pass: query recent metrics, run rules, fire alerts.
    async fn evaluate(&self) -> anyhow::Result<()> {
        let now_us = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let lookback_us = self.lookback_secs * 1_000_000;
        let from_us = now_us.saturating_sub(lookback_us);
        let window = TimeWindow::new(from_us, now_us);

        // Get all known metric names
        let names = self.metric_store.get_names().await?;
        if names.is_empty() {
            return Ok(());
        }

        // Build a metric values map: metric_name -> latest aggregated value
        let mut metric_values: HashMap<String, MetricValue> = HashMap::new();

        for name in &names {
            // Get aggregated stats over the lookback window
            let aggregated = self.metric_store
                .get_aggregated(name, window, self.lookback_secs)
                .await?;

            if let Some(latest) = aggregated.last() {
                // Expose multiple facets of each metric for rule matching
                metric_values.insert(name.clone(), MetricValue::Float(latest.avg));
                metric_values.insert(format!("{}.avg", name), MetricValue::Float(latest.avg));
                metric_values.insert(format!("{}.min", name), MetricValue::Float(latest.min));
                metric_values.insert(format!("{}.max", name), MetricValue::Float(latest.max));
                metric_values.insert(format!("{}.p50", name), MetricValue::Float(latest.p50));
                metric_values.insert(format!("{}.p90", name), MetricValue::Float(latest.p90));
                metric_values.insert(format!("{}.p95", name), MetricValue::Float(latest.p95));
                metric_values.insert(format!("{}.p99", name), MetricValue::Float(latest.p99));
                metric_values.insert(format!("{}.count", name), MetricValue::Float(latest.count as f64));
                metric_values.insert(format!("{}.sum", name), MetricValue::Float(latest.sum));
            }
        }

        if metric_values.is_empty() {
            return Ok(());
        }

        // Evaluate all rules against current metric values
        let triggered = self.rule_engine.evaluate(&metric_values);

        for (rule, alert) in triggered {
            info!(
                "MetricAlertEvaluator: Rule '{}' triggered: {}",
                rule.name, alert.message
            );
            let _ = self.alert_manager.process_alert(alert, &rule).await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::in_memory_metric::InMemoryMetricStore;
    use crate::models::metric::{Metric, MetricType};
    use crate::alerts::config::{AlertConfig, AlertSeverity};
    use crate::alerts::manager::AlertManager;
    use std::time::SystemTime;

    fn now_us() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    #[tokio::test]
    async fn test_evaluator_fires_on_threshold() {
        let store = Arc::new(InMemoryMetricStore::new());

        // Insert a high CPU metric
        let metric = Metric::new("cpu.usage".to_string(), 95.0, MetricType::Gauge)
            .with_timestamp(now_us());
        store.insert(metric).await.unwrap();

        let config = AlertConfig::default();
        let manager = Arc::new(AlertManager::new(config).await.unwrap());

        let rules = vec![AlertRuleConfig {
            name: "high-cpu".to_string(),
            description: "CPU usage above 80%".to_string(),
            condition: "cpu.usage > 80".to_string(),
            severity: AlertSeverity::Warning,
            channels: vec![],
            labels: HashMap::new(),
            cooldown_seconds: None,
        }];

        let evaluator = MetricAlertEvaluator::new(
            store, manager.clone(), rules, 10, 60,
        );

        evaluator.evaluate().await.unwrap();

        // Check that an alert was created
        let alerts = manager.get_alerts(None).await;
        assert!(!alerts.is_empty(), "Expected at least one alert to fire");
    }

    #[tokio::test]
    async fn test_evaluator_no_fire_below_threshold() {
        let store = Arc::new(InMemoryMetricStore::new());

        let metric = Metric::new("cpu.usage".to_string(), 50.0, MetricType::Gauge)
            .with_timestamp(now_us());
        store.insert(metric).await.unwrap();

        let config = AlertConfig::default();
        let manager = Arc::new(AlertManager::new(config).await.unwrap());

        let rules = vec![AlertRuleConfig {
            name: "high-cpu".to_string(),
            description: "CPU usage above 80%".to_string(),
            condition: "cpu.usage > 80".to_string(),
            severity: AlertSeverity::Warning,
            channels: vec![],
            labels: HashMap::new(),
            cooldown_seconds: None,
        }];

        let evaluator = MetricAlertEvaluator::new(
            store, manager.clone(), rules, 10, 60,
        );

        evaluator.evaluate().await.unwrap();

        let alerts = manager.get_alerts(None).await;
        assert!(alerts.is_empty(), "Expected no alerts to fire");
    }
}
