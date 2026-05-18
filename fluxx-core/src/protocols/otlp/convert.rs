use crate::models::event::{Event, EventSeverity, Metadata, ProtocolType};
use crate::models::metric::{Metric, MetricType};
use crate::models::log_entry::{LogEntry, LogLevel};
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::trace::v1::status::StatusCode as OtelStatusCode;

/// Convert OTLP ResourceSpans into Fluxx Events.
pub fn otlp_spans_to_events(resource_spans: &[ResourceSpans]) -> Vec<Event> {
    let mut events = Vec::new();

    for rs in resource_spans {
        let service_name = rs.resource.as_ref().and_then(|r| {
            r.attributes.iter().find(|a| a.key == "service.name").and_then(|a| {
                a.value.as_ref().and_then(|v| v.value.as_ref()).map(|val| format_any_value(val))
            })
        });

        let resource_attrs: Vec<(&str, String)> = rs.resource.as_ref().map(|r| {
            r.attributes.iter().map(|a| {
                let val = a.value.as_ref()
                    .and_then(|v| v.value.as_ref())
                    .map(|val| format_any_value(val))
                    .unwrap_or_default();
                (a.key.as_str(), val)
            }).collect()
        }).unwrap_or_default();

        for scope_spans in &rs.scope_spans {
            for span in &scope_spans.spans {
                let trace_id = hex::encode(&span.trace_id);
                let span_id = hex::encode(&span.span_id);
                let parent_span_id = if span.parent_span_id.is_empty() {
                    None
                } else {
                    Some(hex::encode(&span.parent_span_id))
                };

                let start_us = span.start_time_unix_nano / 1000;
                let end_us = span.end_time_unix_nano / 1000;
                let latency_us = end_us.saturating_sub(start_us);

                let severity = match span.status.as_ref().map(|s| s.code()) {
                    Some(OtelStatusCode::Error) => EventSeverity::High,
                    Some(OtelStatusCode::Ok) => EventSeverity::Info,
                    _ => EventSeverity::Low,
                };

                let mut metadata = Metadata::new();
                metadata.add("trace_id", &trace_id);
                metadata.add("span_id", &span_id);
                if let Some(ref parent) = parent_span_id {
                    metadata.add("parent_span_id", parent);
                }
                metadata.add("span_name", &span.name);
                metadata.add("span_kind", &format!("{}", span.kind));

                // Add resource attributes
                for (k, v) in &resource_attrs {
                    metadata.add(k, v);
                }

                // Add span attributes
                for attr in &span.attributes {
                    let val = attr.value.as_ref()
                        .and_then(|v| v.value.as_ref())
                        .map(|val| format_any_value(val))
                        .unwrap_or_default();
                    metadata.add(&attr.key, &val);
                }

                let mut event = Event::new(span_id.clone(), ProtocolType::Otlp);
                event.timestamp_us = start_us as u64;
                event.latency_us = latency_us as u64;
                event.severity = severity;
                event.service_id = service_name.clone();
                event.metadata = metadata;

                // Serialize span events/links as payload if present
                if !span.events.is_empty() || !span.links.is_empty() {
                    let payload = serde_json::json!({
                        "events": span.events.iter().map(|e| {
                            serde_json::json!({
                                "name": e.name,
                                "timestamp_ns": e.time_unix_nano,
                            })
                        }).collect::<Vec<_>>(),
                        "links": span.links.len(),
                    });
                    event.payload = Some(payload.to_string());
                }

                events.push(event);
            }
        }
    }

    events
}

/// Convert OTLP ResourceMetrics into Fluxx Metrics.
pub fn otlp_metrics_to_metrics(resource_metrics: &[ResourceMetrics]) -> Vec<Metric> {
    let mut metrics = Vec::new();

    for rm in resource_metrics {
        let service_name = rm.resource.as_ref().and_then(|r| {
            r.attributes.iter().find(|a| a.key == "service.name").and_then(|a| {
                a.value.as_ref().and_then(|v| v.value.as_ref()).map(|val| format_any_value(val))
            })
        });

        for scope_metrics in &rm.scope_metrics {
            for otel_metric in &scope_metrics.metrics {
                let name = &otel_metric.name;

                if let Some(ref data) = otel_metric.data {
                    use opentelemetry_proto::tonic::metrics::v1::metric::Data;

                    match data {
                        Data::Gauge(gauge) => {
                            for dp in &gauge.data_points {
                                let (value, ts) = extract_number_dp(dp);
                                let mut m = Metric::new(name.clone(), value, MetricType::Gauge)
                                    .with_timestamp(ts);
                                m.service_id = service_name.clone();
                                add_dp_attributes(&mut m, &dp.attributes);
                                metrics.push(m);
                            }
                        }
                        Data::Sum(sum) => {
                            for dp in &sum.data_points {
                                let (value, ts) = extract_number_dp(dp);
                                let mut m = Metric::new(name.clone(), value, MetricType::Counter)
                                    .with_timestamp(ts);
                                m.service_id = service_name.clone();
                                add_dp_attributes(&mut m, &dp.attributes);
                                metrics.push(m);
                            }
                        }
                        Data::Histogram(hist) => {
                            for dp in &hist.data_points {
                                let ts = (dp.time_unix_nano / 1000) as u64;
                                let value = dp.sum.unwrap_or(0.0);
                                let mut m = Metric::new(name.clone(), value, MetricType::Histogram)
                                    .with_timestamp(ts);
                                m.service_id = service_name.clone();
                                m.tags.insert("count".to_string(), dp.count.to_string());
                                add_dp_attributes(&mut m, &dp.attributes);
                                metrics.push(m);
                            }
                        }
                        Data::Summary(summary) => {
                            for dp in &summary.data_points {
                                let ts = (dp.time_unix_nano / 1000) as u64;
                                let value = dp.sum;
                                let mut m = Metric::new(name.clone(), value, MetricType::Summary)
                                    .with_timestamp(ts);
                                m.service_id = service_name.clone();
                                m.tags.insert("count".to_string(), dp.count.to_string());
                                add_dp_attributes(&mut m, &dp.attributes);
                                metrics.push(m);
                            }
                        }
                        Data::ExponentialHistogram(eh) => {
                            for dp in &eh.data_points {
                                let ts = (dp.time_unix_nano / 1000) as u64;
                                let value = dp.sum.unwrap_or(0.0);
                                let mut m = Metric::new(name.clone(), value, MetricType::Histogram)
                                    .with_timestamp(ts);
                                m.service_id = service_name.clone();
                                m.tags.insert("count".to_string(), dp.count.to_string());
                                add_dp_attributes(&mut m, &dp.attributes);
                                metrics.push(m);
                            }
                        }
                    }
                }
            }
        }
    }

    metrics
}

/// Convert OTLP ResourceLogs into Fluxx LogEntries.
pub fn otlp_logs_to_entries(resource_logs: &[ResourceLogs]) -> Vec<LogEntry> {
    let mut entries = Vec::new();

    for rl in resource_logs {
        let service_name = rl.resource.as_ref().and_then(|r| {
            r.attributes.iter().find(|a| a.key == "service.name").and_then(|a| {
                a.value.as_ref().and_then(|v| v.value.as_ref()).map(|val| format_any_value(val))
            })
        });

        let resource_attrs: std::collections::HashMap<String, String> = rl.resource.as_ref()
            .map(|r| {
                r.attributes.iter().map(|a| {
                    let val = a.value.as_ref()
                        .and_then(|v| v.value.as_ref())
                        .map(|val| format_any_value(val))
                        .unwrap_or_default();
                    (a.key.clone(), val)
                }).collect()
            })
            .unwrap_or_default();

        for scope_logs in &rl.scope_logs {
            for log_record in &scope_logs.log_records {
                let level = LogLevel::from_otel_severity(log_record.severity_number);

                let message = log_record.body.as_ref()
                    .and_then(|v| v.value.as_ref())
                    .map(|val| format_any_value(val))
                    .unwrap_or_default();

                let trace_id = if log_record.trace_id.is_empty() {
                    None
                } else {
                    Some(hex::encode(&log_record.trace_id))
                };

                let span_id = if log_record.span_id.is_empty() {
                    None
                } else {
                    Some(hex::encode(&log_record.span_id))
                };

                let mut attributes = std::collections::HashMap::new();
                for attr in &log_record.attributes {
                    let val = attr.value.as_ref()
                        .and_then(|v| v.value.as_ref())
                        .map(|val| serde_json::Value::String(format_any_value(val)))
                        .unwrap_or(serde_json::Value::Null);
                    attributes.insert(attr.key.clone(), val);
                }

                let ts_us = (log_record.time_unix_nano / 1000) as u64;

                let mut entry = LogEntry::new(level, message);
                if ts_us > 0 {
                    entry.timestamp_us = ts_us;
                }
                entry.service = service_name.clone();
                entry.trace_id = trace_id;
                entry.span_id = span_id;
                entry.attributes = attributes;
                entry.resource = resource_attrs.clone();

                entries.push(entry);
            }
        }
    }

    entries
}

fn extract_number_dp(dp: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint) -> (f64, u64) {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    let value = match dp.value {
        Some(Value::AsDouble(v)) => v,
        Some(Value::AsInt(v)) => v as f64,
        None => 0.0,
    };
    let ts = (dp.time_unix_nano / 1000) as u64;
    (value, ts)
}

fn add_dp_attributes(
    metric: &mut Metric,
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) {
    for attr in attrs {
        let val = attr.value.as_ref()
            .and_then(|v| v.value.as_ref())
            .map(|val| format_any_value(val))
            .unwrap_or_default();
        metric.tags.insert(attr.key.clone(), val);
    }
}

fn format_any_value(value: &opentelemetry_proto::tonic::common::v1::any_value::Value) -> String {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    match value {
        Value::StringValue(s) => s.clone(),
        Value::BoolValue(b) => b.to_string(),
        Value::IntValue(i) => i.to_string(),
        Value::DoubleValue(d) => d.to_string(),
        Value::ArrayValue(arr) => {
            let items: Vec<String> = arr.values.iter()
                .filter_map(|v| v.value.as_ref().map(format_any_value))
                .collect();
            format!("[{}]", items.join(","))
        }
        Value::KvlistValue(kvlist) => {
            let pairs: Vec<String> = kvlist.values.iter()
                .map(|kv| {
                    let val = kv.value.as_ref()
                        .and_then(|v| v.value.as_ref())
                        .map(format_any_value)
                        .unwrap_or_default();
                    format!("{}={}", kv.key, val)
                })
                .collect();
            format!("{{{}}}", pairs.join(","))
        }
        Value::BytesValue(b) => hex::encode(b),
    }
}

// hex encoding for trace/span IDs
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}
