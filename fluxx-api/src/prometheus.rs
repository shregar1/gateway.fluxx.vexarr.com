//! Prometheus Remote Write Receiver
//!
//! Accepts Prometheus remote_write HTTP requests and converts them into Fluxx metrics.
//! This allows existing Prometheus setups to forward metrics to Fluxx with zero code change.
//!
//! ## Usage
//! Configure Prometheus to remote_write to `http://fluxx-host:7007/v1/prom/write`

use crate::api::{ApiState, StandardResponse};
use crate::models::metric::{Metric, MetricType};
use axum::{
    extract::State,
    response::IntoResponse,
    body::Bytes,
};
use std::sync::Arc;
use tracing::{info, error, warn};

/// POST /v1/prom/write — Accept Prometheus remote_write requests.
///
/// Prometheus sends snappy-compressed protobuf by default.
/// We also support plain-text line protocol as a simpler alternative.
///
/// For full protobuf support, we'd need the prometheus remote_write proto.
/// This implementation supports the text exposition format as a pragmatic first step,
/// plus raw metric lines that tools like Telegraf/Vector can send.
pub async fn remote_write(
    State(state): State<Arc<ApiState>>,
    body: Bytes,
) -> impl IntoResponse {
    // Try snappy decompression first (Prometheus default), then plain text
    let text = match std::str::from_utf8(&body) {
        Ok(t) if t.contains('\n') || t.contains('{') || t.starts_with('#') => t.to_string(),
        _ => {
            // Binary body — try snappy decompression, then interpret as text lines
            match snap::raw::Decoder::new().decompress_vec(&body) {
                Ok(decompressed) => {
                    // Decompressed — try to parse as text (Prometheus sends
                    // snappy-compressed text exposition format in some configs)
                    match String::from_utf8(decompressed) {
                        Ok(text) => text,
                        Err(_) => {
                            // Binary protobuf — not yet supported
                            return StandardResponse::error(
                                serde_json::json!({}),
                                "Snappy-compressed protobuf remote_write not yet supported. Use text exposition format.",
                                "UNSUPPORTED_FORMAT",
                                vec![],
                            ).into_response();
                        }
                    }
                }
                Err(_) => {
                    // Not snappy — try as raw text
                    match String::from_utf8(body.to_vec()) {
                        Ok(text) => text,
                        Err(_) => return StandardResponse::error(
                            serde_json::json!({}),
                            "Unable to parse body as text or snappy-compressed data",
                            "PARSE_ERROR",
                            vec![],
                        ).into_response(),
                    }
                }
            }
        }
    };

    let mut ingested = 0u64;
    let mut errors = 0u64;

    for line in text.lines() {
        let line = line.trim();
        // Skip comments and empty lines
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        match parse_prom_line(line) {
            Ok(metric) => {
                let metric_arc = Arc::new(metric.clone());
                let _ = state.metric_bus.send(metric_arc);
                if let Err(e) = state.metric_store.insert(metric).await {
                    error!("Prom remote_write store error: {}", e);
                    errors += 1;
                } else {
                    ingested += 1;
                }
            }
            Err(e) => {
                warn!("Prom remote_write parse error: {} (line: {})", e, &line[..line.len().min(80)]);
                errors += 1;
            }
        }
    }

    if ingested > 0 {
        info!("Prometheus remote_write: Ingested {} metrics ({} errors)", ingested, errors);
    }

    StandardResponse::ok(
        serde_json::json!({ "ingested": ingested, "errors": errors }),
        "Prometheus metrics ingested.",
    ).into_response()
}

/// Parse a Prometheus text exposition line into a Fluxx Metric.
///
/// Format: `metric_name{label1="val1",label2="val2"} value [timestamp_ms]`
fn parse_prom_line(line: &str) -> Result<Metric, String> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err("Empty or comment line".to_string());
    }

    let (name, labels_str, value_rest) = parse_name_labels_value(line)?;

    // Parse labels into tags
    let mut tags = std::collections::HashMap::new();
    if !labels_str.is_empty() {
        for pair in labels_str.split(',') {
            let pair = pair.trim();
            if let Some((k, v)) = pair.split_once('=') {
                let v = v.trim_matches('"');
                tags.insert(k.trim().to_string(), v.to_string());
            }
        }
    }

    // Parse value and optional timestamp
    let parts: Vec<&str> = value_rest.split_whitespace().collect();
    if parts.is_empty() {
        return Err("Missing value".to_string());
    }

    let value: f64 = parts[0].parse().map_err(|_| format!("Invalid value: '{}'", parts[0]))?;

    let timestamp_us = if parts.len() > 1 {
        // Prometheus sends millisecond timestamps
        let ts_ms: u64 = parts[1].parse().unwrap_or(0);
        ts_ms * 1000
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64
    };

    // Convert metric name: replace _ with . for Fluxx convention
    let fluxx_name = name.replace('_', ".");

    let metric = Metric::new(fluxx_name, value, MetricType::Gauge)
        .with_tags(tags)
        .with_timestamp(timestamp_us);

    Ok(metric)
}

/// Parse the name, labels, and value portion of a Prometheus text line.
fn parse_name_labels_value(line: &str) -> Result<(&str, &str, &str), String> {
    if let Some(brace_start) = line.find('{') {
        let brace_end = line.find('}').ok_or("Missing closing brace")?;
        let name = &line[..brace_start];
        let labels_str = &line[brace_start + 1..brace_end];
        let rest = line[brace_end + 1..].trim();
        Ok((name, labels_str, rest))
    } else {
        let space_pos = line.find(' ').ok_or("Missing value")?;
        let name = &line[..space_pos];
        let rest = &line[space_pos + 1..];
        Ok((name, "", rest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_metric() {
        let m = parse_prom_line("http_requests_total 1027").unwrap();
        assert_eq!(m.name, "http.requests.total");
        assert_eq!(m.value, 1027.0);
        assert!(m.tags.is_empty());
    }

    #[test]
    fn test_parse_metric_with_labels() {
        let m = parse_prom_line(r#"http_requests_total{method="post",code="200"} 1027"#).unwrap();
        assert_eq!(m.name, "http.requests.total");
        assert_eq!(m.value, 1027.0);
        assert_eq!(m.tags.get("method"), Some(&"post".to_string()));
        assert_eq!(m.tags.get("code"), Some(&"200".to_string()));
    }

    #[test]
    fn test_parse_metric_with_timestamp() {
        let m = parse_prom_line("http_requests_total 1027 1395066363000").unwrap();
        assert_eq!(m.value, 1027.0);
        assert_eq!(m.timestamp_us, 1395066363000 * 1000);
    }

    #[test]
    fn test_parse_float_value() {
        let m = parse_prom_line("node_cpu_seconds_total 72345.12").unwrap();
        assert_eq!(m.value, 72345.12);
    }

    #[test]
    fn test_parse_empty_line() {
        assert!(parse_prom_line("").is_err());
    }

    #[test]
    fn test_parse_comment_skipped() {
        // Comments should be filtered before calling parse_prom_line
        assert!(parse_prom_line("# HELP http_requests_total").is_err());
    }

    #[test]
    fn test_parse_negative_value() {
        let m = parse_prom_line("temperature_celsius -40.5").unwrap();
        assert_eq!(m.value, -40.5);
    }
}
