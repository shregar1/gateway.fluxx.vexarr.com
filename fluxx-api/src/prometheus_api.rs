//! Prometheus-canonical HTTP API surface.
//!
//! Wraps the existing PromQL evaluator (`crate::promql`) with the
//! exact wire format Grafana's Prometheus datasource expects.
//! Path-compatible with `/api/v1/query` / `/api/v1/query_range` so
//! a user can point Grafana at fluxx with **zero config beyond
//! the URL** and existing PromQL dashboards work unchanged.
//!
//! The trick is the envelope. `crate::promql::query` returns
//! fluxx's `StandardResponse` envelope — fine for first-party
//! callers, useless for Grafana. The handlers here return:
//!
//! ```json
//! {
//!   "status": "success",
//!   "data": {
//!     "resultType": "vector",
//!     "result": [
//!       { "metric": {"__name__": "x", "label": "v"}, "value": [1730000000, "1.5"] }
//!     ]
//!   }
//! }
//! ```
//!
//! ## Implemented (MVP)
//!
//! - `GET /api/v1/query?query=<promql>&time=<rfc3339|unix>`
//! - `GET /api/v1/query_range?query=<promql>&start=<t>&end=<t>&step=<dur>`
//! - `GET /api/v1/labels` — stub returning `[]` (full impl is a follow-up)
//! - `GET /api/v1/label/{name}/values` — stub returning `[]`
//!
//! ## Not implemented yet
//!
//! - `/api/v1/series` (label matchers → series list)
//! - `/api/v1/metadata` (metric metadata)
//! - `/api/v1/rules`, `/api/v1/alerts`, `/api/v1/targets` — operator
//!   surfaces fluxx doesn't expose
//! - Range query downsampling — `query_range` currently evaluates
//!   the instant query once per step (correct but inefficient)

use crate::ApiState;
use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Json},
};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Debug, Serialize)]
struct PromResponse<T: Serialize> {
    status: &'static str,
    data: T,
}

#[derive(Debug, Serialize)]
struct PromError {
    status: &'static str,
    #[serde(rename = "errorType")]
    error_type: &'static str,
    error: String,
}

fn ok<T: Serialize>(data: T) -> Json<JsonValue> {
    Json(serde_json::to_value(PromResponse { status: "success", data })
        .unwrap_or(JsonValue::Null))
}

fn bad_data(msg: impl Into<String>) -> Json<JsonValue> {
    Json(serde_json::to_value(PromError {
        status: "error",
        error_type: "bad_data",
        error: msg.into(),
    }).unwrap_or(JsonValue::Null))
}

fn execution(msg: impl Into<String>) -> Json<JsonValue> {
    Json(serde_json::to_value(PromError {
        status: "error",
        error_type: "execution",
        error: msg.into(),
    }).unwrap_or(JsonValue::Null))
}

/// Parse a Prometheus `time` parameter: unix seconds (float) or
/// rfc3339. Defaults to now if missing/unparseable.
fn parse_time_us(s: Option<&String>) -> u64 {
    if let Some(s) = s {
        if let Ok(secs) = s.parse::<f64>() {
            return (secs * 1_000_000.0) as u64;
        }
        if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(s) {
            return (ts.timestamp() as u64) * 1_000_000
                + (ts.timestamp_subsec_micros() as u64);
        }
    }
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Parse a Prometheus `step` duration: "15s", "1m", "1h", or a bare number (seconds).
fn parse_step_secs(s: Option<&String>) -> u64 {
    let Some(s) = s else { return 60 };
    if let Ok(n) = s.parse::<u64>() {
        return n.max(1);
    }
    if s.len() < 2 {
        return 60;
    }
    let (n_str, unit) = s.split_at(s.len() - 1);
    let n: u64 = n_str.parse().unwrap_or(60);
    match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86400,
        _ => 60,
    }
    .max(1)
}

/// Translate a `QueryResult` from the internal evaluator into the
/// Prometheus `{"resultType": ..., "result": ...}` JSON shape.
fn to_prom_data(r: crate::promql::QueryResult) -> JsonValue {
    match r {
        crate::promql::QueryResult::Vector { result } => {
            let samples: Vec<JsonValue> = result
                .into_iter()
                .map(|s| {
                    // Prom format: [unix_seconds, "stringified value"]
                    let ts_secs = (s.value.0 as f64) / 1_000_000.0;
                    serde_json::json!({
                        "metric": s.metric,
                        "value": [ts_secs, s.value.1.to_string()]
                    })
                })
                .collect();
            serde_json::json!({"resultType": "vector", "result": samples})
        }
        crate::promql::QueryResult::Scalar { result } => {
            let ts_secs = result.0 / 1_000_000.0;
            serde_json::json!({
                "resultType": "scalar",
                "result": [ts_secs, result.1.to_string()]
            })
        }
    }
}

/// `GET /api/v1/query?query=<promql>&time=<t>` — instant query.
pub async fn api_v1_query(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let q = match params.get("query").or(params.get("q")) {
        Some(q) if !q.is_empty() => q.clone(),
        _ => return bad_data("missing 'query' parameter"),
    };
    let now_us = parse_time_us(params.get("time"));
    let lookback_us = params
        .get("lookback")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(300)
        * 1_000_000;
    match crate::promql::evaluate_promql(&state, &q, now_us, lookback_us).await {
        Ok(result) => ok(to_prom_data(result)),
        Err(e) => execution(e),
    }
}

/// `GET /api/v1/query_range?query=<q>&start=<t>&end=<t>&step=<d>` — range query.
/// Returns a matrix of (metric, samples[]) for each step in
/// [start, end]. MVP: evaluates the instant query once per step;
/// future optimization is to push the step loop into the evaluator.
pub async fn api_v1_query_range(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let q = match params.get("query").or(params.get("q")) {
        Some(q) if !q.is_empty() => q.clone(),
        _ => return bad_data("missing 'query' parameter"),
    };
    let start_us = parse_time_us(params.get("start"));
    let end_us = parse_time_us(params.get("end"));
    if end_us <= start_us {
        return bad_data("end must be > start");
    }
    let step_secs = parse_step_secs(params.get("step"));
    let step_us = step_secs * 1_000_000;
    let lookback_us = step_us; // each step looks back one step's worth

    // Cap step count so a malicious / accidental wide range with
    // 1s step doesn't fan out into millions of evaluations.
    const MAX_STEPS: u64 = 11_000;
    let n_steps = ((end_us - start_us) / step_us) + 1;
    if n_steps > MAX_STEPS {
        return bad_data(format!(
            "step count {n_steps} exceeds cap {MAX_STEPS} — increase step or shrink range"
        ));
    }

    // metric_label_set → Vec<[ts_secs, "value"]>
    let mut series: HashMap<String, (HashMap<String, String>, Vec<JsonValue>)> = HashMap::new();
    let mut scalar: Option<Vec<JsonValue>> = None;

    let mut t_us = start_us;
    while t_us <= end_us {
        match crate::promql::evaluate_promql(&state, &q, t_us, lookback_us).await {
            Ok(crate::promql::QueryResult::Vector { result }) => {
                let ts_secs = (t_us as f64) / 1_000_000.0;
                for s in result {
                    // Stable key for the series — sorted labels.
                    let mut k: Vec<(&String, &String)> = s.metric.iter().collect();
                    k.sort_by(|a, b| a.0.cmp(b.0));
                    let key = k.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(",");
                    let entry = series.entry(key).or_insert_with(|| (s.metric.clone(), Vec::new()));
                    entry.1.push(serde_json::json!([ts_secs, s.value.1.to_string()]));
                }
            }
            Ok(crate::promql::QueryResult::Scalar { result }) => {
                let ts_secs = (t_us as f64) / 1_000_000.0;
                scalar.get_or_insert_with(Vec::new)
                    .push(serde_json::json!([ts_secs, result.1.to_string()]));
            }
            Err(e) => return execution(e),
        }
        t_us = t_us.saturating_add(step_us);
        if t_us == 0 { break; }
    }

    if let Some(samples) = scalar {
        return ok(serde_json::json!({"resultType": "matrix", "result": [
            {"metric": {}, "values": samples}
        ]}));
    }
    let result: Vec<JsonValue> = series
        .into_iter()
        .map(|(_, (metric, values))| serde_json::json!({"metric": metric, "values": values}))
        .collect();
    ok(serde_json::json!({"resultType": "matrix", "result": result}))
}

/// `GET /api/v1/labels` — stub returning [].
/// Real impl would aggregate distinct label names from the
/// active series. Future PR.
pub async fn api_v1_labels(State(_state): State<Arc<ApiState>>) -> impl IntoResponse {
    ok(JsonValue::Array(Vec::new()))
}

/// `GET /api/v1/label/{name}/values` — stub returning [].
pub async fn api_v1_label_values(
    State(_state): State<Arc<ApiState>>,
    Path(_name): Path<String>,
) -> impl IntoResponse {
    ok(JsonValue::Array(Vec::new()))
}
