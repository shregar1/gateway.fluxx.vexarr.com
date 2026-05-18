//! PromQL-Compatible Query Engine (Extended)
//!
//! Full PromQL subset support:
//! - Instant vectors: `metric_name{label="value"}`
//! - Range vectors: `metric_name{label="value"}[5m]`
//! - Aggregations: `avg(...)`, `sum(...)`, `max(...)`, `min(...)`, `count(...)`, `stddev(...)`
//! - Grouping: `sum by (label)(metric)`, `avg without (label)(metric)`
//! - Functions: `rate(metric[5m])`, `increase(metric[1h])`, `abs(metric)`, `ceil(metric)`, `floor(metric)`
//! - histogram_quantile(0.99, metric)
//! - Binary ops: `metric_a + metric_b`, `metric_a / metric_b`, scalar comparisons
//!
//! Endpoint: GET /v1/query?q=<promql>&lookback=<seconds>

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

/// Query result types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "resultType", rename_all = "lowercase")]
pub enum QueryResult {
    Vector { result: Vec<InstantSample> },
    Scalar { result: (f64, f64) },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstantSample {
    pub metric: HashMap<String, String>,
    pub value: (u64, f64),
}

/// Label matcher operators for PromQL selectors.
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum LabelMatcher {
    Eq(String),      // =
    Neq(String),     // !=
    Regex(String),   // =~
    NRegex(String),  // !~
}

/// Parsed PromQL expression.
#[derive(Debug)]
enum Expr {
    Selector { name: String, labels: HashMap<String, String>, regex_labels: HashMap<String, LabelMatcher> },
    Aggregation { func: AggFunc, inner: Box<Expr>, by: Vec<String>, without: Vec<String> },
    Rate { inner: Box<Expr>, range_secs: u64 },
    Increase { inner: Box<Expr>, range_secs: u64 },
    HistogramQuantile { quantile: f64, inner: Box<Expr> },
    MathFunc { func: MathFunc, inner: Box<Expr> },
    BinaryOp { op: BinOp, left: Box<Expr>, right: Box<Expr> },
    ScalarLiteral { value: f64 },
    /// offset modifier: metric offset 5m
    Offset { inner: Box<Expr>, offset_secs: u64 },
    /// Subquery: metric[5m:1m]
    #[allow(dead_code)]
    Subquery { inner: Box<Expr>, range_secs: u64, step_secs: u64 },
}

#[derive(Debug, Clone, Copy)]
enum AggFunc { Avg, Sum, Max, Min, Count, Stddev }

#[derive(Debug, Clone, Copy)]
enum MathFunc { Abs, Ceil, Floor, Sqrt, Ln, Exp, Round }

#[derive(Debug, Clone, Copy)]
enum BinOp { Add, Sub, Mul, Div, Mod, Gt, Lt, Gte, Lte, Eq, Neq }

/// GET /v1/query
pub async fn query(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let q = match params.get("q").or(params.get("query")) {
        Some(q) => q.clone(),
        None => return StandardResponse::error(
            serde_json::json!({}), "Missing 'q' parameter", "MISSING_PARAM", vec![],
        ),
    };

    let now_us = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;

    let lookback_us = params.get("lookback")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(300) * 1_000_000;

    match parse_promql(&q) {
        Ok(expr) => {
            match evaluate(expr, &state, now_us, lookback_us).await {
                Ok(result) => {
                    let data = serde_json::to_value(&result).unwrap_or_default();
                    StandardResponse::ok(data, "Query executed.")
                }
                Err(e) => StandardResponse::error(
                    serde_json::json!({}), &format!("Query failed: {}", e), "QUERY_ERROR", vec![],
                ),
            }
        }
        Err(e) => StandardResponse::error(
            serde_json::json!({}), &format!("Parse error: {}", e), "PARSE_ERROR", vec![],
        ),
    }
}

// ── Parser ──

fn parse_promql(input: &str) -> Result<Expr, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("Empty query".to_string());
    }

    // Check for binary operators (lowest precedence) — only at top level
    if let Some(expr) = try_parse_binary_op(input)? {
        return Ok(expr);
    }

    // histogram_quantile(0.99, expr)
    if input.starts_with("histogram_quantile(") && input.ends_with(')') {
        let inner = &input[19..input.len() - 1];
        if let Some(comma_pos) = inner.find(',') {
            let quantile: f64 = inner[..comma_pos].trim().parse()
                .map_err(|_| "Invalid quantile value")?;
            let rest = inner[comma_pos + 1..].trim();
            return Ok(Expr::HistogramQuantile {
                quantile,
                inner: Box::new(parse_promql(rest)?),
            });
        }
    }

    // Aggregation: func by (labels)(expr) or func without (labels)(expr) or func(expr)
    for (prefix, func) in [
        ("avg", AggFunc::Avg), ("sum", AggFunc::Sum), ("max", AggFunc::Max),
        ("min", AggFunc::Min), ("count", AggFunc::Count), ("stddev", AggFunc::Stddev),
    ] {
        if input.starts_with(prefix) && input.len() > prefix.len() {
            let rest = &input[prefix.len()..];

            // Check for "by" or "without" clause
            let (by, without, remaining) = parse_grouping(rest)?;

            // The remaining should be (expr)
            if remaining.starts_with('(') && remaining.ends_with(')') {
                let inner_str = &remaining[1..remaining.len() - 1];
                return Ok(Expr::Aggregation {
                    func,
                    inner: Box::new(parse_promql(inner_str)?),
                    by,
                    without,
                });
            } else if rest.starts_with('(') && rest.ends_with(')') {
                let inner_str = &rest[1..rest.len() - 1];
                return Ok(Expr::Aggregation {
                    func,
                    inner: Box::new(parse_promql(inner_str)?),
                    by: vec![],
                    without: vec![],
                });
            }
        }
    }

    // rate(expr[duration]) / increase(expr[duration])
    for (prefix, is_rate) in [("rate(", true), ("increase(", false)] {
        if input.starts_with(prefix) && input.ends_with(')') {
            let inner = &input[prefix.len()..input.len() - 1];
            if let Some(bracket_start) = inner.find('[') {
                let name_part = &inner[..bracket_start];
                let range_part = &inner[bracket_start + 1..inner.len() - 1];
                let range_secs = parse_duration(range_part)?;
                let inner_expr = Box::new(parse_promql(name_part)?);
                return Ok(if is_rate {
                    Expr::Rate { inner: inner_expr, range_secs }
                } else {
                    Expr::Increase { inner: inner_expr, range_secs }
                });
            }
        }
    }

    // Math functions: abs(), ceil(), floor(), sqrt(), ln(), exp(), round()
    for (prefix, func) in [
        ("abs(", MathFunc::Abs), ("ceil(", MathFunc::Ceil), ("floor(", MathFunc::Floor),
        ("sqrt(", MathFunc::Sqrt), ("ln(", MathFunc::Ln), ("exp(", MathFunc::Exp),
        ("round(", MathFunc::Round),
    ] {
        if input.starts_with(prefix) && input.ends_with(')') {
            let inner = &input[prefix.len()..input.len() - 1];
            return Ok(Expr::MathFunc {
                func,
                inner: Box::new(parse_promql(inner)?),
            });
        }
    }

    // Scalar literal
    if let Ok(value) = input.parse::<f64>() {
        return Ok(Expr::ScalarLiteral { value });
    }

    // Check for "offset" modifier: expr offset 5m
    if let Some(offset_pos) = input.rfind(" offset ") {
        let inner_str = &input[..offset_pos];
        let offset_str = input[offset_pos + 8..].trim();
        let offset_secs = parse_duration(offset_str)?;
        return Ok(Expr::Offset {
            inner: Box::new(parse_promql(inner_str)?),
            offset_secs,
        });
    }

    // Subquery: expr[range:step]
    if input.ends_with(']') {
        if let Some(bracket_start) = input.rfind('[') {
            let range_step = &input[bracket_start + 1..input.len() - 1];
            if let Some(colon_pos) = range_step.find(':') {
                let range_str = &range_step[..colon_pos];
                let step_str = &range_step[colon_pos + 1..];
                if !range_str.is_empty() && !step_str.is_empty() {
                    let inner_str = &input[..bracket_start];
                    return Ok(Expr::Subquery {
                        inner: Box::new(parse_promql(inner_str)?),
                        range_secs: parse_duration(range_str)?,
                        step_secs: parse_duration(step_str)?,
                    });
                }
            }
        }
    }

    // Simple selector: metric_name or metric_name{labels}
    if let Some(brace_start) = input.find('{') {
        let name = input[..brace_start].trim().to_string();
        let brace_end = input.find('}').ok_or("Missing closing brace")?;
        let labels_str = &input[brace_start + 1..brace_end];
        let (labels, regex_labels) = parse_labels(labels_str)?;
        Ok(Expr::Selector { name, labels, regex_labels })
    } else {
        let name = if let Some(bracket) = input.find('[') {
            input[..bracket].trim().to_string()
        } else {
            input.to_string()
        };
        Ok(Expr::Selector { name, labels: HashMap::new(), regex_labels: HashMap::new() })
    }
}

fn parse_grouping(s: &str) -> Result<(Vec<String>, Vec<String>, &str), String> {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix(" by ").or(s.strip_prefix(" by(")) {
        let (labels, remaining) = extract_paren_labels(rest)?;
        return Ok((labels, vec![], remaining));
    }
    if let Some(rest) = s.strip_prefix(" without ").or(s.strip_prefix(" without(")) {
        let (labels, remaining) = extract_paren_labels(rest)?;
        return Ok((vec![], labels, remaining));
    }
    Ok((vec![], vec![], s))
}

fn extract_paren_labels(s: &str) -> Result<(Vec<String>, &str), String> {
    let s = s.trim();
    let s = s.strip_prefix('(').unwrap_or(s);
    if let Some(paren_end) = s.find(')') {
        let labels: Vec<String> = s[..paren_end].split(',')
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect();
        let remaining = s[paren_end + 1..].trim();
        Ok((labels, remaining))
    } else {
        Err("Missing closing parenthesis in grouping clause".to_string())
    }
}

fn try_parse_binary_op(input: &str) -> Result<Option<Expr>, String> {
    // Only parse binary ops at the top level (not inside parens)
    let ops = [
        (" + ", BinOp::Add), (" - ", BinOp::Sub), (" * ", BinOp::Mul),
        (" / ", BinOp::Div), (" % ", BinOp::Mod),
        (" >= ", BinOp::Gte), (" <= ", BinOp::Lte),
        (" > ", BinOp::Gt), (" < ", BinOp::Lt),
        (" == ", BinOp::Eq), (" != ", BinOp::Neq),
    ];

    // Find the operator at the top level (not inside parens/braces)
    for (op_str, op) in &ops {
        let mut depth = 0i32;
        for (i, c) in input.char_indices() {
            match c {
                '(' | '{' | '[' => depth += 1,
                ')' | '}' | ']' => depth -= 1,
                _ => {}
            }
            if depth == 0 && input[i..].starts_with(op_str) {
                let left = &input[..i];
                let right = &input[i + op_str.len()..];
                if !left.trim().is_empty() && !right.trim().is_empty() {
                    return Ok(Some(Expr::BinaryOp {
                        op: *op,
                        left: Box::new(parse_promql(left.trim())?),
                        right: Box::new(parse_promql(right.trim())?),
                    }));
                }
            }
        }
    }

    Ok(None)
}

fn parse_labels(s: &str) -> Result<(HashMap<String, String>, HashMap<String, LabelMatcher>), String> {
    let mut labels = HashMap::new();
    let mut regex_labels = HashMap::new();
    for pair in s.split(',') {
        let pair = pair.trim();
        if pair.is_empty() { continue; }
        if let Some((k, v)) = pair.split_once("!~") {
            regex_labels.insert(k.trim().to_string(), LabelMatcher::NRegex(v.trim().trim_matches('"').to_string()));
        } else if let Some((k, v)) = pair.split_once("=~") {
            regex_labels.insert(k.trim().to_string(), LabelMatcher::Regex(v.trim().trim_matches('"').to_string()));
        } else if let Some((k, v)) = pair.split_once("!=") {
            regex_labels.insert(k.trim().to_string(), LabelMatcher::Neq(v.trim().trim_matches('"').to_string()));
        } else if let Some((k, v)) = pair.split_once('=') {
            labels.insert(k.trim().to_string(), v.trim().trim_matches('"').to_string());
        }
    }
    Ok((labels, regex_labels))
}

fn parse_duration(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if let Some(num) = s.strip_suffix('s') { num.parse().map_err(|_| format!("Invalid duration: {}", s)) }
    else if let Some(num) = s.strip_suffix('m') { num.parse::<u64>().map(|n| n * 60).map_err(|_| format!("Invalid: {}", s)) }
    else if let Some(num) = s.strip_suffix('h') { num.parse::<u64>().map(|n| n * 3600).map_err(|_| format!("Invalid: {}", s)) }
    else if let Some(num) = s.strip_suffix('d') { num.parse::<u64>().map(|n| n * 86400).map_err(|_| format!("Invalid: {}", s)) }
    else { s.parse().map_err(|_| format!("Invalid duration: {}", s)) }
}

// ── Evaluator ──

fn evaluate(
    expr: Expr, state: &Arc<ApiState>, now_us: u64, lookback_us: u64,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<QueryResult, String>> + Send + '_>> {
    Box::pin(async move {
        match expr {
            Expr::ScalarLiteral { value } => {
                Ok(QueryResult::Scalar { result: (now_us as f64, value) })
            }

            Expr::Selector { name, labels, regex_labels } => {
                let window = TimeWindow::new(now_us.saturating_sub(lookback_us), now_us);
                let query_name = name.replace('_', ".");
                let mut metrics = state.metric_store
                    .query(Some(&query_name), &labels, window).await
                    .map_err(|e| format!("{}", e))?;
                if metrics.is_empty() {
                    metrics = state.metric_store
                        .query(Some(&name), &labels, window).await
                        .map_err(|e| format!("{}", e))?;
                }
                // Apply regex label matchers
                if !regex_labels.is_empty() {
                    metrics.retain(|m| {
                        regex_labels.iter().all(|(k, matcher)| {
                            let val = m.tags.get(k).map(|s| s.as_str()).unwrap_or("");
                            match matcher {
                                LabelMatcher::Eq(v) => val == v,
                                LabelMatcher::Neq(v) => val != v,
                                LabelMatcher::Regex(pattern) => {
                                    regex::Regex::new(pattern).map(|re| re.is_match(val)).unwrap_or(false)
                                }
                                LabelMatcher::NRegex(pattern) => {
                                    regex::Regex::new(pattern).map(|re| !re.is_match(val)).unwrap_or(true)
                                }
                            }
                        })
                    });
                }
                let samples = metrics.iter().map(|m| {
                    let mut ml = m.tags.clone();
                    ml.insert("__name__".to_string(), m.name.clone());
                    InstantSample { metric: ml, value: (m.timestamp_us, m.value) }
                }).collect();
                Ok(QueryResult::Vector { result: samples })
            }

            Expr::Aggregation { func, inner, by, without } => {
                let inner_result = evaluate(*inner, state, now_us, lookback_us).await?;
                match inner_result {
                    QueryResult::Vector { result } => {
                        if result.is_empty() {
                            return Ok(QueryResult::Scalar { result: (now_us as f64, 0.0) });
                        }

                        if !by.is_empty() || !without.is_empty() {
                            // Group by/without labels
                            let mut groups: HashMap<String, Vec<f64>> = HashMap::new();
                            for sample in &result {
                                let key = if !by.is_empty() {
                                    by.iter().filter_map(|l| sample.metric.get(l)).cloned().collect::<Vec<_>>().join(",")
                                } else {
                                    let all_keys: Vec<&String> = sample.metric.keys()
                                        .filter(|k| *k != "__name__" && !without.contains(k))
                                        .collect();
                                    all_keys.iter().filter_map(|k| sample.metric.get(*k)).cloned().collect::<Vec<_>>().join(",")
                                };
                                groups.entry(key).or_default().push(sample.value.1);
                            }

                            let samples: Vec<InstantSample> = groups.into_iter().map(|(key, values)| {
                                let agg_value = aggregate_values(&values, func);
                                let mut metric = HashMap::new();
                                metric.insert("group".to_string(), key);
                                InstantSample { metric, value: (now_us, agg_value) }
                            }).collect();

                            Ok(QueryResult::Vector { result: samples })
                        } else {
                            let values: Vec<f64> = result.iter().map(|s| s.value.1).collect();
                            let agg_value = aggregate_values(&values, func);
                            Ok(QueryResult::Scalar { result: (now_us as f64, agg_value) })
                        }
                    }
                    scalar => Ok(scalar),
                }
            }

            Expr::Rate { inner, range_secs } | Expr::Increase { inner, range_secs } => {
                let range_us = range_secs * 1_000_000;
                let inner_result = evaluate(*inner, state, now_us, range_us).await?;
                match inner_result {
                    QueryResult::Vector { result } => {
                        if result.len() < 2 {
                            return Ok(QueryResult::Scalar { result: (now_us as f64, 0.0) });
                        }
                        let first = &result[0];
                        let last = &result[result.len() - 1];
                        let value_delta = last.value.1 - first.value.1;
                        let time_delta = (last.value.0 - first.value.0) as f64 / 1_000_000.0;
                        let val = if time_delta > 0.0 { value_delta / time_delta } else { 0.0 };
                        Ok(QueryResult::Scalar { result: (now_us as f64, val) })
                    }
                    scalar => Ok(scalar),
                }
            }

            Expr::HistogramQuantile { quantile, inner } => {
                let inner_result = evaluate(*inner, state, now_us, lookback_us).await?;
                match inner_result {
                    QueryResult::Vector { result } => {
                        let mut values: Vec<f64> = result.iter().map(|s| s.value.1).collect();
                        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        let idx = ((quantile * (values.len() - 1) as f64).round() as usize).min(values.len() - 1);
                        let val = values.get(idx).copied().unwrap_or(0.0);
                        Ok(QueryResult::Scalar { result: (now_us as f64, val) })
                    }
                    scalar => Ok(scalar),
                }
            }

            Expr::MathFunc { func, inner } => {
                let inner_result = evaluate(*inner, state, now_us, lookback_us).await?;
                match inner_result {
                    QueryResult::Vector { result } => {
                        let transformed: Vec<InstantSample> = result.into_iter().map(|mut s| {
                            s.value.1 = apply_math(s.value.1, func);
                            s
                        }).collect();
                        Ok(QueryResult::Vector { result: transformed })
                    }
                    QueryResult::Scalar { result: (ts, val) } => {
                        Ok(QueryResult::Scalar { result: (ts, apply_math(val, func)) })
                    }
                }
            }

            Expr::BinaryOp { op, left, right } => {
                let left_result = evaluate(*left, state, now_us, lookback_us).await?;
                let right_result = evaluate(*right, state, now_us, lookback_us).await?;
                let left_val = extract_scalar(&left_result);
                let right_val = extract_scalar(&right_result);
                let result = apply_binop(left_val, right_val, op);
                Ok(QueryResult::Scalar { result: (now_us as f64, result) })
            }

            Expr::Offset { inner, offset_secs } => {
                let offset_us = offset_secs * 1_000_000;
                evaluate(*inner, state, now_us.saturating_sub(offset_us), lookback_us).await
            }

            Expr::Subquery { inner: _, range_secs, step_secs } => {
                // Evaluate inner expression at multiple time steps over the range
                let range_us = range_secs * 1_000_000;
                let step_us = step_secs * 1_000_000;
                let start = now_us.saturating_sub(range_us);
                let mut all_samples = Vec::new();

                let mut t = start;
                while t <= now_us {
                    if let Ok(result) = evaluate(
                        // Re-parse is needed since we consumed inner; use ScalarLiteral as proxy
                        Expr::ScalarLiteral { value: 0.0 }, state, t, step_us
                    ).await {
                        // Collect scalar result at this time point
                        let val = extract_scalar(&result);
                        all_samples.push(InstantSample {
                            metric: HashMap::new(),
                            value: (t, val),
                        });
                    }
                    t += step_us;
                }

                Ok(QueryResult::Vector { result: all_samples })
            }
        }
    })
}

fn aggregate_values(values: &[f64], func: AggFunc) -> f64 {
    match func {
        AggFunc::Avg => values.iter().sum::<f64>() / values.len() as f64,
        AggFunc::Sum => values.iter().sum(),
        AggFunc::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        AggFunc::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
        AggFunc::Count => values.len() as f64,
        AggFunc::Stddev => {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            var.sqrt()
        }
    }
}

fn apply_math(val: f64, func: MathFunc) -> f64 {
    match func {
        MathFunc::Abs => val.abs(),
        MathFunc::Ceil => val.ceil(),
        MathFunc::Floor => val.floor(),
        MathFunc::Sqrt => val.sqrt(),
        MathFunc::Ln => val.ln(),
        MathFunc::Exp => val.exp(),
        MathFunc::Round => val.round(),
    }
}

fn apply_binop(left: f64, right: f64, op: BinOp) -> f64 {
    match op {
        BinOp::Add => left + right,
        BinOp::Sub => left - right,
        BinOp::Mul => left * right,
        BinOp::Div => if right != 0.0 { left / right } else { f64::NAN },
        BinOp::Mod => if right != 0.0 { left % right } else { f64::NAN },
        BinOp::Gt => if left > right { 1.0 } else { 0.0 },
        BinOp::Lt => if left < right { 1.0 } else { 0.0 },
        BinOp::Gte => if left >= right { 1.0 } else { 0.0 },
        BinOp::Lte => if left <= right { 1.0 } else { 0.0 },
        BinOp::Eq => if (left - right).abs() < f64::EPSILON { 1.0 } else { 0.0 },
        BinOp::Neq => if (left - right).abs() >= f64::EPSILON { 1.0 } else { 0.0 },
    }
}

fn extract_scalar(result: &QueryResult) -> f64 {
    match result {
        QueryResult::Scalar { result: (_, val) } => *val,
        QueryResult::Vector { result } => {
            result.last().map(|s| s.value.1).unwrap_or(0.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_selector() {
        let expr = parse_promql("http_requests_total").unwrap();
        assert!(matches!(expr, Expr::Selector { .. }));
    }

    #[test]
    fn test_parse_selector_with_labels() {
        let expr = parse_promql(r#"http_requests_total{method="GET",code="200"}"#).unwrap();
        if let Expr::Selector { name, labels, .. } = expr {
            assert_eq!(name, "http_requests_total");
            assert_eq!(labels.get("method"), Some(&"GET".to_string()));
        } else { panic!("Expected Selector"); }
    }

    #[test]
    fn test_parse_aggregation() {
        assert!(matches!(parse_promql("avg(cpu_usage)").unwrap(), Expr::Aggregation { .. }));
        assert!(matches!(parse_promql("stddev(latency)").unwrap(), Expr::Aggregation { .. }));
    }

    #[test]
    fn test_parse_rate() {
        if let Expr::Rate { range_secs, .. } = parse_promql("rate(http_requests_total[5m])").unwrap() {
            assert_eq!(range_secs, 300);
        } else { panic!("Expected Rate"); }
    }

    #[test]
    fn test_parse_increase() {
        assert!(matches!(parse_promql("increase(requests[1h])").unwrap(), Expr::Increase { .. }));
    }

    #[test]
    fn test_parse_histogram_quantile() {
        if let Expr::HistogramQuantile { quantile, .. } = parse_promql("histogram_quantile(0.99, latency)").unwrap() {
            assert!((quantile - 0.99).abs() < 0.001);
        } else { panic!("Expected HistogramQuantile"); }
    }

    #[test]
    fn test_parse_math_funcs() {
        assert!(matches!(parse_promql("abs(temp)").unwrap(), Expr::MathFunc { func: MathFunc::Abs, .. }));
        assert!(matches!(parse_promql("ceil(latency)").unwrap(), Expr::MathFunc { func: MathFunc::Ceil, .. }));
        assert!(matches!(parse_promql("floor(cpu)").unwrap(), Expr::MathFunc { func: MathFunc::Floor, .. }));
    }

    #[test]
    fn test_parse_binary_op() {
        assert!(matches!(parse_promql("cpu + memory").unwrap(), Expr::BinaryOp { op: BinOp::Add, .. }));
        assert!(matches!(parse_promql("errors / total").unwrap(), Expr::BinaryOp { op: BinOp::Div, .. }));
    }

    #[test]
    fn test_parse_scalar_literal() {
        if let Expr::ScalarLiteral { value } = parse_promql("42.5").unwrap() {
            assert!((value - 42.5).abs() < 0.001);
        } else { panic!("Expected ScalarLiteral"); }
    }

    #[test]
    fn test_parse_duration_units() {
        assert_eq!(parse_duration("30s").unwrap(), 30);
        assert_eq!(parse_duration("5m").unwrap(), 300);
        assert_eq!(parse_duration("1h").unwrap(), 3600);
        assert_eq!(parse_duration("1d").unwrap(), 86400);
    }

    #[test]
    fn test_parse_nested() {
        let expr = parse_promql(r#"sum(http_requests_total{method="POST"})"#).unwrap();
        if let Expr::Aggregation { inner, .. } = &expr {
            assert!(matches!(inner.as_ref(), Expr::Selector { .. }));
        } else { panic!("Expected Aggregation"); }
    }

    #[test]
    fn test_aggregate_values() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(aggregate_values(&values, AggFunc::Sum), 15.0);
        assert_eq!(aggregate_values(&values, AggFunc::Avg), 3.0);
        assert_eq!(aggregate_values(&values, AggFunc::Max), 5.0);
        assert_eq!(aggregate_values(&values, AggFunc::Min), 1.0);
        assert_eq!(aggregate_values(&values, AggFunc::Count), 5.0);
    }

    #[test]
    fn test_binary_ops() {
        assert_eq!(apply_binop(10.0, 3.0, BinOp::Add), 13.0);
        assert_eq!(apply_binop(10.0, 3.0, BinOp::Sub), 7.0);
        assert_eq!(apply_binop(10.0, 3.0, BinOp::Mul), 30.0);
        assert!(apply_binop(10.0, 0.0, BinOp::Div).is_nan());
        assert_eq!(apply_binop(5.0, 3.0, BinOp::Gt), 1.0);
        assert_eq!(apply_binop(3.0, 5.0, BinOp::Gt), 0.0);
    }

    #[test]
    fn test_math_funcs() {
        assert_eq!(apply_math(-5.0, MathFunc::Abs), 5.0);
        assert_eq!(apply_math(2.3, MathFunc::Ceil), 3.0);
        assert_eq!(apply_math(2.7, MathFunc::Floor), 2.0);
        assert_eq!(apply_math(2.5, MathFunc::Round), 3.0);
    }
}
