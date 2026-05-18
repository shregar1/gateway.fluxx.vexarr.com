//! Self-monitoring helpers — let the Fluxx agent surface its own
//! lifecycle events in its own log store, so the embedded dashboard
//! at /v1/logs (and dashboard.fluxx.dev via the cloud forwarder)
//! can show "agent booted", "license validated", "OTLP server up", etc.
//! alongside customer telemetry.
//!
//! The agent's tracing! macros already write to stderr/journald. This
//! module is the structured-log counterpart that makes those same
//! events queryable through the same surface customers use to find
//! their own logs.
//!
//! Usage (from the runtime, after log_store is built):
//!
//!     self_telemetry::emit(
//!         &log_store,
//!         self_telemetry::Severity::Info,
//!         "agent.boot.complete",
//!         hashmap! { "version".into() => env!("CARGO_PKG_VERSION").into() },
//!     ).await;

use crate::models::log_entry::{LogEntry, LogLevel};
use crate::storage::log_store::LogStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Severity matching OTel SeverityNumber buckets, but we lower it to
/// the agent's own LogLevel enum (which is closer to syslog).
#[derive(Debug, Clone, Copy)]
pub enum Severity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Critical,
}

impl Severity {
    fn to_log_level(self) -> LogLevel {
        match self {
            Severity::Trace => LogLevel::Trace,
            Severity::Debug => LogLevel::Debug,
            Severity::Info => LogLevel::Info,
            Severity::Warn => LogLevel::Warn,
            Severity::Error => LogLevel::Error,
            Severity::Critical => LogLevel::Fatal,
        }
    }
}

/// Best-effort write of a self-monitoring log entry. Failures are
/// swallowed — we never want a log-store hiccup to take down the
/// thing that's logging about itself.
pub async fn emit(
    log_store: &Arc<dyn LogStore>,
    severity: Severity,
    message: &str,
    attrs: HashMap<String, serde_json::Value>,
) {
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);

    let entry = LogEntry {
        id: Uuid::new_v4().to_string(),
        timestamp_us: now_us,
        level: severity.to_log_level(),
        message: message.to_string(),
        // Tag every self-telemetry record with the same service name
        // so the dashboard can group/filter them as "agent internals".
        service: Some("fluxx-agent".to_string()),
        trace_id: None,
        span_id: None,
        attributes: attrs,
        resource: HashMap::new(),
        project_id: None,
        environment_id: None,
    };

    if let Err(e) = log_store.insert(entry).await {
        // Don't recurse via tracing — that would call back into the
        // logging system. eprintln is the loudest non-recursive option.
        eprintln!(
            "[self_telemetry] failed to record `{}`: {}",
            message, e
        );
    }
}
