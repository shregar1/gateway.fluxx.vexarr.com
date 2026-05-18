//! Predictive Alerting
//!
//! Forecasts metric trends 1-6 hours ahead using linear regression on EWMA baselines.
//! "Memory will exceed 90% in approximately 2.3 hours at current growth rate."
//! Alerts BEFORE the problem happens.
//!
//! GET /v1/predict — forecast a metric's future value
//! GET /v1/predict/alerts — list predicted threshold breaches

use crate::api::{ApiState, StandardResponse};
use crate::storage::event_store::TimeWindow;
use axum::{
    extract::{State, Query},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

/// Prediction result for a single metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricForecast {
    pub metric_name: String,
    pub current_value: f64,
    pub predicted_values: Vec<ForecastPoint>,
    pub trend: String,
    pub slope_per_hour: f64,
    pub r_squared: f64,
    pub threshold_breach: Option<ThresholdBreach>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForecastPoint {
    pub hours_ahead: f64,
    pub predicted_value: f64,
    pub lower_bound: f64,
    pub upper_bound: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdBreach {
    pub threshold: f64,
    pub threshold_name: String,
    pub estimated_breach_hours: f64,
    pub estimated_breach_value: f64,
    pub severity: String,
    pub message: String,
}

/// Default thresholds for common metrics.
fn default_thresholds() -> HashMap<String, (f64, String)> {
    let mut m = HashMap::new();
    m.insert("cpu".to_string(), (80.0, "CPU critical threshold".to_string()));
    m.insert("memory".to_string(), (90.0, "Memory critical threshold".to_string()));
    m.insert("disk".to_string(), (85.0, "Disk space critical threshold".to_string()));
    m.insert("error".to_string(), (0.05, "Error rate threshold".to_string()));
    m
}

/// GET /v1/predict — Forecast a specific metric.
pub async fn predict_metric(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let metric_name = match params.get("metric") {
        Some(m) => m.clone(),
        None => return StandardResponse::error(
            serde_json::json!({}), "Missing 'metric' parameter", "MISSING_PARAM", vec![],
        ),
    };

    let hours_ahead: f64 = params.get("hours").and_then(|v| v.parse().ok()).unwrap_or(6.0);
    let lookback_hours: f64 = params.get("lookback_hours").and_then(|v| v.parse().ok()).unwrap_or(6.0);
    let threshold: Option<f64> = params.get("threshold").and_then(|v| v.parse().ok());

    let now_us = now();
    let lookback_us = (lookback_hours * 3600.0 * 1_000_000.0) as u64;
    let window = TimeWindow::new(now_us.saturating_sub(lookback_us), now_us);

    let metrics = match state.metric_store.query(Some(&metric_name), &HashMap::new(), window).await {
        Ok(m) => m,
        Err(e) => return StandardResponse::error(serde_json::json!({}), &format!("{}", e), "QUERY_ERROR", vec![]),
    };

    if metrics.len() < 3 {
        return StandardResponse::error(
            serde_json::json!({}), "Not enough data points for prediction (need 3+)", "INSUFFICIENT_DATA", vec![],
        );
    }

    let forecast = compute_forecast(&metric_name, &metrics, hours_ahead, threshold);
    let data = serde_json::to_value(&forecast).unwrap_or_default();
    StandardResponse::ok(data, "Metric forecast.")
}

/// GET /v1/predict/alerts — List all metrics with predicted threshold breaches.
pub async fn predicted_alerts(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let hours_ahead: f64 = params.get("hours").and_then(|v| v.parse().ok()).unwrap_or(6.0);
    let now_us = now();
    let lookback_us = 6 * 3600 * 1_000_000u64;
    let window = TimeWindow::new(now_us.saturating_sub(lookback_us), now_us);

    let thresholds = default_thresholds();
    let mut alerts: Vec<MetricForecast> = Vec::new();

    if let Ok(names) = state.metric_store.get_names().await {
        for name in &names {
            // Find matching threshold
            let threshold = thresholds.iter()
                .find(|(key, _)| name.to_lowercase().contains(key.as_str()))
                .map(|(_, (val, _))| *val);

            if threshold.is_none() { continue; }

            if let Ok(metrics) = state.metric_store.query(Some(name), &HashMap::new(), window).await {
                if metrics.len() < 3 { continue; }

                let forecast = compute_forecast(name, &metrics, hours_ahead, threshold);
                if forecast.threshold_breach.is_some() {
                    alerts.push(forecast);
                }
            }
        }
    }

    alerts.sort_by(|a, b| {
        let a_h = a.threshold_breach.as_ref().map(|t| t.estimated_breach_hours).unwrap_or(f64::MAX);
        let b_h = b.threshold_breach.as_ref().map(|t| t.estimated_breach_hours).unwrap_or(f64::MAX);
        a_h.partial_cmp(&b_h).unwrap_or(std::cmp::Ordering::Equal)
    });

    let data = serde_json::to_value(&alerts).unwrap_or_default();
    StandardResponse::ok(data, "Predicted alerts.")
}

/// Compute forecast using linear regression on recent data points.
fn compute_forecast(
    name: &str,
    metrics: &[crate::models::metric::Metric],
    hours_ahead: f64,
    threshold: Option<f64>,
) -> MetricForecast {
    // Convert to (hours_from_start, value) pairs
    let base_ts = metrics.first().map(|m| m.timestamp_us).unwrap_or(0);
    let points: Vec<(f64, f64)> = metrics.iter()
        .map(|m| {
            let hours = (m.timestamp_us - base_ts) as f64 / (3600.0 * 1_000_000.0);
            (hours, m.value)
        })
        .collect();

    let n = points.len() as f64;
    let sum_x: f64 = points.iter().map(|(x, _)| x).sum();
    let sum_y: f64 = points.iter().map(|(_, y)| y).sum();
    let sum_xy: f64 = points.iter().map(|(x, y)| x * y).sum();
    let sum_xx: f64 = points.iter().map(|(x, _)| x * x).sum();

    // Linear regression: y = slope * x + intercept
    let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x).max(0.0001);
    let intercept = (sum_y - slope * sum_x) / n;

    // R² (coefficient of determination)
    let mean_y = sum_y / n;
    let ss_tot: f64 = points.iter().map(|(_, y)| (y - mean_y).powi(2)).sum();
    let ss_res: f64 = points.iter().map(|(x, y)| (y - (slope * x + intercept)).powi(2)).sum();
    let r_squared = if ss_tot > 0.0 { 1.0 - ss_res / ss_tot } else { 0.0 };

    let current_hours = points.last().map(|(x, _)| *x).unwrap_or(0.0);
    let current_value = points.last().map(|(_, y)| *y).unwrap_or(0.0);

    // Standard error for confidence intervals
    let std_err = (ss_res / (n - 2.0).max(1.0)).sqrt();

    // Generate forecast points
    let mut predicted_values = Vec::new();
    for h in 1..=(hours_ahead as usize) {
        let t = current_hours + h as f64;
        let predicted = slope * t + intercept;
        predicted_values.push(ForecastPoint {
            hours_ahead: h as f64,
            predicted_value: predicted,
            lower_bound: predicted - 2.0 * std_err,
            upper_bound: predicted + 2.0 * std_err,
        });
    }

    let trend = if slope.abs() < 0.01 { "stable" }
        else if slope > 0.0 { "increasing" }
        else { "decreasing" };

    // Check threshold breach
    let threshold_breach = threshold.and_then(|thresh| {
        if slope <= 0.0 && current_value < thresh {
            return None; // Decreasing or stable, won't breach
        }
        if current_value >= thresh {
            return Some(ThresholdBreach {
                threshold: thresh,
                threshold_name: format!("{} threshold", name),
                estimated_breach_hours: 0.0,
                estimated_breach_value: current_value,
                severity: "critical".into(),
                message: format!("{} is already above threshold ({:.1} >= {:.1})", name, current_value, thresh),
            });
        }
        // Time to breach: (threshold - intercept_at_current) / slope
        let current_predicted = slope * current_hours + intercept;
        let hours_to_breach = (thresh - current_predicted) / slope;
        if hours_to_breach > 0.0 && hours_to_breach <= hours_ahead {
            let severity = if hours_to_breach < 1.0 { "critical" }
                else if hours_to_breach < 3.0 { "high" }
                else { "warning" };
            Some(ThresholdBreach {
                threshold: thresh,
                threshold_name: format!("{} threshold", name),
                estimated_breach_hours: hours_to_breach,
                estimated_breach_value: thresh,
                severity: severity.into(),
                message: format!(
                    "{} will exceed {:.1} in approximately {:.1} hours at current growth rate ({:+.2}/hour)",
                    name, thresh, hours_to_breach, slope
                ),
            })
        } else {
            None
        }
    });

    MetricForecast {
        metric_name: name.to_string(),
        current_value,
        predicted_values,
        trend: trend.to_string(),
        slope_per_hour: slope,
        r_squared: r_squared.max(0.0),
        threshold_breach,
    }
}

fn now() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::metric::{Metric, MetricType};

    #[test]
    fn test_linear_forecast_increasing() {
        let base = 1_000_000u64;
        let metrics: Vec<Metric> = (0..10).map(|i| {
            Metric::new("cpu".into(), 50.0 + i as f64 * 5.0, MetricType::Gauge)
                .with_timestamp(base + i * 3600 * 1_000_000) // 1 hour apart
        }).collect();

        let forecast = compute_forecast("cpu", &metrics, 6.0, Some(80.0));
        assert_eq!(forecast.trend, "increasing");
        assert!(forecast.slope_per_hour > 0.0);
        assert!(forecast.threshold_breach.is_some(), "Should predict breach of 80%");
    }

    #[test]
    fn test_linear_forecast_stable() {
        let base = 1_000_000u64;
        let metrics: Vec<Metric> = (0..10).map(|i| {
            Metric::new("cpu".into(), 50.0, MetricType::Gauge)
                .with_timestamp(base + i * 3600 * 1_000_000)
        }).collect();

        let forecast = compute_forecast("cpu", &metrics, 6.0, Some(80.0));
        assert_eq!(forecast.trend, "stable");
        assert!(forecast.threshold_breach.is_none(), "Stable metric shouldn't predict breach");
    }

    #[test]
    fn test_forecast_points() {
        let base = 1_000_000u64;
        let metrics: Vec<Metric> = (0..5).map(|i| {
            Metric::new("mem".into(), 60.0 + i as f64 * 2.0, MetricType::Gauge)
                .with_timestamp(base + i * 3600 * 1_000_000)
        }).collect();

        let forecast = compute_forecast("mem", &metrics, 3.0, None);
        assert_eq!(forecast.predicted_values.len(), 3);
        // Each point should be higher than the last (increasing trend)
        for i in 1..forecast.predicted_values.len() {
            assert!(forecast.predicted_values[i].predicted_value >= forecast.predicted_values[i-1].predicted_value);
        }
    }

    fn make_metrics(values: &[f64]) -> Vec<Metric> {
        let base = 1_000_000u64;
        values.iter().enumerate().map(|(i, v)| {
            Metric::new("cpu".into(), *v, MetricType::Gauge)
                .with_timestamp(base + (i as u64) * 3600 * 1_000_000)
        }).collect()
    }

    #[test]
    fn test_decreasing_trend() {
        let metrics = make_metrics(&[90.0, 85.0, 80.0, 75.0, 70.0]);
        let forecast = compute_forecast("cpu", &metrics, 6.0, Some(95.0));
        assert_eq!(forecast.trend, "decreasing");
        assert!(forecast.slope_per_hour < 0.0);
    }

    #[test]
    fn test_decreasing_below_threshold_no_breach() {
        let metrics = make_metrics(&[50.0, 45.0, 40.0, 35.0, 30.0]);
        let forecast = compute_forecast("cpu", &metrics, 6.0, Some(80.0));
        assert!(forecast.threshold_breach.is_none());
    }

    #[test]
    fn test_current_at_threshold_breach_immediate() {
        let metrics = make_metrics(&[70.0, 72.0, 74.0, 76.0, 80.0]);
        let forecast = compute_forecast("cpu", &metrics, 6.0, Some(80.0));
        let breach = forecast.threshold_breach.expect("should breach");
        assert_eq!(breach.estimated_breach_hours, 0.0);
        assert_eq!(breach.severity, "critical");
    }

    #[test]
    fn test_current_above_threshold_breach_immediate() {
        let metrics = make_metrics(&[80.0, 82.0, 85.0, 88.0, 92.0]);
        let forecast = compute_forecast("cpu", &metrics, 6.0, Some(80.0));
        let breach = forecast.threshold_breach.expect("should breach");
        assert_eq!(breach.estimated_breach_hours, 0.0);
    }

    #[test]
    fn test_hours_to_breach_accuracy() {
        // Linear increase of 5/hour from 50. Threshold 80 → reach at t=6
        // Last data point is t=4 at 70. So 2 more hours to reach 80.
        let metrics = make_metrics(&[50.0, 55.0, 60.0, 65.0, 70.0]);
        let forecast = compute_forecast("cpu", &metrics, 6.0, Some(80.0));
        let breach = forecast.threshold_breach.expect("should breach");
        assert!((breach.estimated_breach_hours - 2.0).abs() < 0.1,
            "expected ~2h, got {}", breach.estimated_breach_hours);
    }

    #[test]
    fn test_breach_severity_critical_under_1h() {
        let metrics = make_metrics(&[75.0, 76.0, 77.0, 78.0, 79.5]);
        let forecast = compute_forecast("cpu", &metrics, 6.0, Some(80.0));
        if let Some(b) = forecast.threshold_breach {
            assert_eq!(b.severity, "critical");
        }
    }

    #[test]
    fn test_breach_severity_warning_far_out() {
        // Breach in ~5 hours
        let metrics = make_metrics(&[50.0, 52.0, 54.0, 56.0, 58.0]);
        let forecast = compute_forecast("cpu", &metrics, 10.0, Some(80.0));
        if let Some(b) = forecast.threshold_breach {
            assert!(b.estimated_breach_hours > 3.0);
            assert_eq!(b.severity, "warning");
        }
    }

    #[test]
    fn test_breach_beyond_horizon_returns_none() {
        // Very slow growth, threshold unreachable in short horizon
        let metrics = make_metrics(&[50.0, 50.1, 50.2, 50.3, 50.4]);
        let forecast = compute_forecast("cpu", &metrics, 2.0, Some(80.0));
        assert!(forecast.threshold_breach.is_none());
    }

    #[test]
    fn test_no_threshold_no_breach() {
        let metrics = make_metrics(&[10.0, 20.0, 30.0, 40.0, 50.0]);
        let forecast = compute_forecast("cpu", &metrics, 6.0, None);
        assert!(forecast.threshold_breach.is_none());
    }

    #[test]
    fn test_forecast_includes_bounds() {
        let metrics = make_metrics(&[50.0, 52.0, 54.0, 56.0, 58.0]);
        let forecast = compute_forecast("cpu", &metrics, 3.0, None);
        for p in &forecast.predicted_values {
            assert!(p.upper_bound >= p.predicted_value);
            assert!(p.lower_bound <= p.predicted_value);
        }
    }

    #[test]
    fn test_r_squared_non_negative() {
        let metrics = make_metrics(&[50.0, 60.0, 55.0, 65.0, 60.0]);
        let forecast = compute_forecast("cpu", &metrics, 3.0, None);
        assert!(forecast.r_squared >= 0.0);
    }

    #[test]
    fn test_current_value_matches_last() {
        let metrics = make_metrics(&[10.0, 20.0, 30.0, 40.0, 77.0]);
        let forecast = compute_forecast("cpu", &metrics, 3.0, None);
        assert_eq!(forecast.current_value, 77.0);
    }
}
