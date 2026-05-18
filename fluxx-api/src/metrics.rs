use crate::api::{ApiState, StandardResponse};
use crate::storage::event_store::TimeWindow;
use axum::{
    extract::{State, Query},
    response::IntoResponse,
};
use std::sync::Arc;
use std::collections::HashMap;

/// GET /v1/metrics — Query metrics with optional filtering.
pub async fn query_metrics(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let name = params.get("name").map(|s| s.as_str());
    let from = params.get("from").and_then(|v| v.parse().ok()).unwrap_or(0u64);
    let to = params.get("to").and_then(|v| v.parse().ok()).unwrap_or(u64::MAX);
    let interval = params.get("interval").and_then(|v| v.parse().ok());

    // Extract tag filters (params like tag.env=prod)
    let tags: HashMap<String, String> = params
        .iter()
        .filter_map(|(k, v)| k.strip_prefix("tag.").map(|tag| (tag.to_string(), v.clone())))
        .collect();

    let window = TimeWindow::new(from, to);

    // If interval is provided, return aggregated metrics
    if let (Some(metric_name), Some(interval_secs)) = (name, interval) {
        match state.metric_store.get_aggregated(metric_name, window, interval_secs).await {
            Ok(agg) => {
                let data = serde_json::to_value(&agg).unwrap_or_default();
                return StandardResponse::ok(data, "Aggregated metrics.");
            }
            Err(e) => return StandardResponse::error(
                serde_json::json!({}),
                &format!("Metric aggregation failed: {}", e),
                "QUERY_ERROR",
                vec![],
            ),
        }
    }

    match state.metric_store.query(name, &tags, window).await {
        Ok(metrics) => {
            let data = serde_json::to_value(&metrics).unwrap_or_default();
            StandardResponse::ok(data, "Metric query results.")
        }
        Err(e) => StandardResponse::error(
            serde_json::json!({}),
            &format!("Metric query failed: {}", e),
            "QUERY_ERROR",
            vec![],
        ),
    }
}

/// GET /v1/metrics/names — List all known metric names.
pub async fn list_metric_names(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    match state.metric_store.get_names().await {
        Ok(names) => {
            let data = serde_json::to_value(&names).unwrap_or_default();
            StandardResponse::ok(data, "Known metric names.")
        }
        Err(e) => StandardResponse::error(
            serde_json::json!({}),
            &format!("Failed to list metric names: {}", e),
            "QUERY_ERROR",
            vec![],
        ),
    }
}

/// GET /metrics — Prometheus exposition format endpoint.
pub async fn prometheus_endpoint(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    let names = match state.metric_store.get_names().await {
        Ok(n) => n,
        Err(_) => return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "# Error fetching metrics\n".to_string(),
        ),
    };

    let window = TimeWindow::new(0, u64::MAX);
    let mut output = String::new();

    for name in &names {
        let metrics = match state.metric_store.query(Some(name), &HashMap::new(), window).await {
            Ok(m) => m,
            Err(_) => continue,
        };

        if metrics.is_empty() {
            continue;
        }

        // Use the most recent metric's type for the TYPE line
        let prom_type = match metrics.last().map(|m| m.metric_type) {
            Some(crate::models::metric::MetricType::Counter) => "counter",
            Some(crate::models::metric::MetricType::Gauge) => "gauge",
            Some(crate::models::metric::MetricType::Histogram) => "histogram",
            Some(crate::models::metric::MetricType::Summary) => "summary",
            _ => "gauge",
        };

        // Sanitize metric name for Prometheus (replace . and - with _)
        let prom_name: String = name.chars().map(|c| if c == '.' || c == '-' { '_' } else { c }).collect();

        output.push_str(&format!("# TYPE {} {}\n", prom_name, prom_type));

        for m in &metrics {
            let labels = if m.tags.is_empty() {
                String::new()
            } else {
                let pairs: Vec<String> = m.tags.iter()
                    .map(|(k, v)| format!("{}=\"{}\"", k, v.replace('\\', "\\\\").replace('"', "\\\"")))
                    .collect();
                format!("{{{}}}", pairs.join(","))
            };

            let ts_ms = m.timestamp_us / 1000;
            output.push_str(&format!("{}{} {} {}\n", prom_name, labels, m.value, ts_ms));
        }
    }

    (axum::http::StatusCode::OK, output)
}
