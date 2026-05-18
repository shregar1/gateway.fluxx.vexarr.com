use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// Log severity levels, compatible with OpenTelemetry severity numbers.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "trace"),
            LogLevel::Debug => write!(f, "debug"),
            LogLevel::Info => write!(f, "info"),
            LogLevel::Warn => write!(f, "warn"),
            LogLevel::Error => write!(f, "error"),
            LogLevel::Fatal => write!(f, "fatal"),
        }
    }
}

impl LogLevel {
    /// Convert from OpenTelemetry severity number (1-24).
    pub fn from_otel_severity(num: i32) -> Self {
        match num {
            1..=4 => LogLevel::Trace,
            5..=8 => LogLevel::Debug,
            9..=12 => LogLevel::Info,
            13..=16 => LogLevel::Warn,
            17..=20 => LogLevel::Error,
            21..=24 => LogLevel::Fatal,
            _ => LogLevel::Info,
        }
    }
}

/// A structured log entry supporting OpenTelemetry log data model.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogEntry {
    pub id: String,
    pub timestamp_us: u64,
    pub level: LogLevel,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(default)]
    pub attributes: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub resource: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment_id: Option<String>,
}

impl LogEntry {
    pub fn new(level: LogLevel, message: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp_us: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
            level,
            message,
            service: None,
            trace_id: None,
            span_id: None,
            attributes: HashMap::new(),
            resource: HashMap::new(),
            project_id: None,
            environment_id: None,
        }
    }

    pub fn with_service(mut self, service: String) -> Self {
        self.service = Some(service);
        self
    }

    pub fn with_trace_context(mut self, trace_id: String, span_id: String) -> Self {
        self.trace_id = Some(trace_id);
        self.span_id = Some(span_id);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_entry_new() {
        let entry = LogEntry::new(LogLevel::Info, "test message".to_string());
        assert_eq!(entry.level, LogLevel::Info);
        assert_eq!(entry.message, "test message");
        assert!(entry.timestamp_us > 0);
        assert!(entry.trace_id.is_none());
    }

    #[test]
    fn test_log_entry_with_trace_context() {
        let entry = LogEntry::new(LogLevel::Error, "error".to_string())
            .with_trace_context("trace-123".to_string(), "span-456".to_string());
        assert_eq!(entry.trace_id, Some("trace-123".to_string()));
        assert_eq!(entry.span_id, Some("span-456".to_string()));
    }

    #[test]
    fn test_log_entry_serialization() {
        let entry = LogEntry::new(LogLevel::Warn, "warning".to_string());
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("warn"));
        assert!(json.contains("warning"));
    }

    #[test]
    fn test_log_entry_deserialization() {
        let json = r#"{"id":"test","timestamp_us":12345,"level":"error","message":"fail"}"#;
        let entry: LogEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.level, LogLevel::Error);
        assert_eq!(entry.message, "fail");
    }

    #[test]
    fn test_log_level_from_otel() {
        assert_eq!(LogLevel::from_otel_severity(1), LogLevel::Trace);
        assert_eq!(LogLevel::from_otel_severity(9), LogLevel::Info);
        assert_eq!(LogLevel::from_otel_severity(17), LogLevel::Error);
        assert_eq!(LogLevel::from_otel_severity(21), LogLevel::Fatal);
    }

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
        assert!(LogLevel::Error < LogLevel::Fatal);
    }

    #[test]
    fn test_log_level_display() {
        assert_eq!(LogLevel::Info.to_string(), "info");
        assert_eq!(LogLevel::Error.to_string(), "error");
    }

    #[test]
    fn test_log_level_display_all() {
        assert_eq!(LogLevel::Trace.to_string(), "trace");
        assert_eq!(LogLevel::Debug.to_string(), "debug");
        assert_eq!(LogLevel::Warn.to_string(), "warn");
        assert_eq!(LogLevel::Fatal.to_string(), "fatal");
    }

    #[test]
    fn test_log_level_from_otel_boundaries() {
        assert_eq!(LogLevel::from_otel_severity(4), LogLevel::Trace);
        assert_eq!(LogLevel::from_otel_severity(5), LogLevel::Debug);
        assert_eq!(LogLevel::from_otel_severity(8), LogLevel::Debug);
        assert_eq!(LogLevel::from_otel_severity(12), LogLevel::Info);
        assert_eq!(LogLevel::from_otel_severity(13), LogLevel::Warn);
        assert_eq!(LogLevel::from_otel_severity(16), LogLevel::Warn);
        assert_eq!(LogLevel::from_otel_severity(20), LogLevel::Error);
        assert_eq!(LogLevel::from_otel_severity(24), LogLevel::Fatal);
    }

    #[test]
    fn test_log_level_from_otel_out_of_range() {
        assert_eq!(LogLevel::from_otel_severity(0), LogLevel::Info);
        assert_eq!(LogLevel::from_otel_severity(-1), LogLevel::Info);
        assert_eq!(LogLevel::from_otel_severity(100), LogLevel::Info);
    }

    #[test]
    fn test_log_entry_with_service() {
        let e = LogEntry::new(LogLevel::Info, "m".into()).with_service("svc-1".into());
        assert_eq!(e.service, Some("svc-1".to_string()));
    }

    #[test]
    fn test_log_entry_unique_ids() {
        let a = LogEntry::new(LogLevel::Info, "a".into());
        let b = LogEntry::new(LogLevel::Info, "a".into());
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn test_log_entry_defaults() {
        let e = LogEntry::new(LogLevel::Info, "m".into());
        assert!(e.service.is_none());
        assert!(e.trace_id.is_none());
        assert!(e.span_id.is_none());
        assert!(e.project_id.is_none());
        assert!(e.attributes.is_empty());
        assert!(e.resource.is_empty());
    }

    #[test]
    fn test_log_entry_all_levels_serialize() {
        for lvl in &[LogLevel::Trace, LogLevel::Debug, LogLevel::Info,
                     LogLevel::Warn, LogLevel::Error, LogLevel::Fatal] {
            let e = LogEntry::new(*lvl, "x".into());
            let _json = serde_json::to_string(&e).unwrap();
        }
    }

    #[test]
    fn test_log_entry_with_attributes_roundtrip() {
        let mut e = LogEntry::new(LogLevel::Info, "m".into());
        e.attributes.insert("k".to_string(), serde_json::json!(42));
        let json = serde_json::to_string(&e).unwrap();
        let parsed: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.attributes.get("k"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn test_log_level_equality() {
        assert_eq!(LogLevel::Info, LogLevel::Info);
        assert_ne!(LogLevel::Info, LogLevel::Warn);
    }

    #[test]
    fn test_log_entry_empty_message() {
        let e = LogEntry::new(LogLevel::Info, String::new());
        assert_eq!(e.message, "");
    }
}
