//! Recording Rules
//!
//! Pre-compute and store aggregated metric values on a schedule.
//! Recording rules evaluate PromQL expressions periodically and store the results
//! as new metrics, enabling fast dashboard queries.
//!
//! Config example:
//! ```yaml
//! recording_rules:
//!   - name: "job:http_requests:rate5m"
//!     expr: "rate(http_requests_total[5m])"
//!     interval_secs: 60
//!     labels:
//!       aggregation: "recording_rule"
//! ```

use crate::models::metric::{Metric, MetricType};
use crate::storage::metric_store::MetricStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::broadcast;
use tracing::{info, warn, error};

/// A recording rule definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingRule {
    pub name: String,
    pub expr: String,
    pub interval_secs: u64,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

/// Evaluates recording rules on a schedule and stores results as metrics.
pub struct RecordingRuleEvaluator {
    rules: Vec<RecordingRule>,
    metric_store: Arc<dyn MetricStore>,
    metric_bus: broadcast::Sender<Arc<Metric>>,
}

impl RecordingRuleEvaluator {
    pub fn new(
        rules: Vec<RecordingRule>,
        metric_store: Arc<dyn MetricStore>,
        metric_bus: broadcast::Sender<Arc<Metric>>,
    ) -> Self {
        Self { rules, metric_store, metric_bus }
    }

    pub async fn run(&self) {
        if self.rules.is_empty() {
            info!("RecordingRules: No rules configured");
            return;
        }
        info!("RecordingRules: Evaluating {} rules", self.rules.len());

        // Each rule runs on its own interval
        let mut handles = Vec::new();
        for rule in &self.rules {
            let rule = rule.clone();
            let store = self.metric_store.clone();
            let bus = self.metric_bus.clone();

            handles.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(rule.interval_secs));
                loop {
                    interval.tick().await;
                    evaluate_rule(&rule, &store, &bus).await;
                }
            }));
        }

        for h in handles {
            let _ = h.await;
        }
    }
}

async fn evaluate_rule(
    rule: &RecordingRule,
    store: &Arc<dyn MetricStore>,
    bus: &broadcast::Sender<Arc<Metric>>,
) {
    // Simple evaluation: extract metric name from expr, compute aggregate
    // In production this would call the full PromQL evaluator
    let now_us = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;

    // Extract the metric name from the expression (simplified)
    let metric_name = extract_metric_name(&rule.expr);
    let lookback_us = 300 * 1_000_000u64; // 5 minutes

    let window = crate::storage::event_store::TimeWindow::new(
        now_us.saturating_sub(lookback_us), now_us,
    );

    match store.get_aggregated(&metric_name, window, 300).await {
        Ok(agg) => {
            if let Some(latest) = agg.last() {
                let mut tags = rule.labels.clone();
                tags.insert("__recording_rule__".to_string(), "true".to_string());

                let metric = Metric::new(rule.name.clone(), latest.avg, MetricType::Gauge)
                    .with_tags(tags)
                    .with_timestamp(now_us);

                let m_arc = Arc::new(metric.clone());
                let _ = bus.send(m_arc);
                if let Err(e) = store.insert(metric).await {
                    error!("RecordingRule '{}': store error: {}", rule.name, e);
                }
            }
        }
        Err(e) => {
            warn!("RecordingRule '{}': evaluation error: {}", rule.name, e);
        }
    }
}

/// Extract the inner metric name from a PromQL expression (simplified).
fn extract_metric_name(expr: &str) -> String {
    let expr = expr.trim();
    // Strip function wrappers: rate(X[5m]) → X, avg(X) → X
    let inner = if let Some(paren) = expr.find('(') {
        let end = expr.rfind(')').unwrap_or(expr.len());
        &expr[paren + 1..end]
    } else {
        expr
    };

    // Strip range selector: X[5m] → X
    let name = if let Some(bracket) = inner.find('[') {
        &inner[..bracket]
    } else {
        inner
    };

    // Strip label selectors: X{label="val"} → X
    let name = if let Some(brace) = name.find('{') {
        &name[..brace]
    } else {
        name
    };

    name.trim().replace('_', ".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_metric_name() {
        assert_eq!(extract_metric_name("rate(http_requests_total[5m])"), "http.requests.total");
        assert_eq!(extract_metric_name("avg(cpu_usage)"), "cpu.usage");
        assert_eq!(extract_metric_name("http_requests_total"), "http.requests.total");
        assert_eq!(extract_metric_name(r#"sum(errors{service="api"})"#), "errors");
    }
}
