//! Integration Framework
//!
//! Core traits and types for infrastructure integrations.

use async_trait::async_trait;
use std::collections::HashMap;
use std::time::SystemTime;

/// Core trait for all integrations
#[async_trait]
pub trait Integration: Send + Sync {
    /// Integration name
    fn name(&self) -> &str;

    /// Integration version
    fn version(&self) -> &str {
        "1.0.0"
    }

    /// Collect metrics from this integration
    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError>;

    /// Collect events from this integration
    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError>;

    /// Health check
    async fn health_check(&self) -> Result<(), IntegrationError>;

    /// Get integration configuration
    fn config(&self) -> &IntegrationConfig;
}

/// Integration configuration
#[derive(Debug, Clone, Default)]
pub struct IntegrationConfig {
    /// Name of the integration
    pub name: String,
    /// Collection interval in seconds
    pub interval_secs: u64,
    /// Custom tags
    pub tags: HashMap<String, String>,
    /// Timeout for operations
    pub timeout_secs: u64,
}

/// Metric collected from an integration
#[derive(Debug, Clone)]
pub struct Metric {
    /// Metric name
    pub name: String,
    /// Metric value
    pub value: MetricValue,
    /// Timestamp
    pub timestamp: SystemTime,
    /// Tags/labels
    pub tags: HashMap<String, String>,
    /// Source integration
    pub source: String,
    /// Metric type
    pub metric_type: MetricType,
    /// Unit (optional)
    pub unit: Option<String>,
    /// Description (optional)
    pub description: Option<String>,
}

/// Metric value types
#[derive(Debug, Clone)]
pub enum MetricValue {
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// String value (for state metrics)
    String(String),
    /// Boolean value
    Boolean(bool),
    /// Histogram data
    Histogram(Vec<f64>),
    /// Counter value
    Counter(u64),
    /// Gauge value
    Gauge(f64),
}

/// Metric types
#[derive(Debug, Clone, Copy)]
pub enum MetricType {
    /// Counter - cumulative metric
    Counter,
    /// Gauge - point-in-time value
    Gauge,
    /// Histogram - distribution of values
    Histogram,
    /// Summary - similar to histogram
    Summary,
}

/// Event from an integration
#[derive(Debug, Clone)]
pub struct Event {
    /// Event title
    pub title: String,
    /// Event description
    pub description: String,
    /// Severity
    pub severity: EventSeverity,
    /// Timestamp
    pub timestamp: SystemTime,
    /// Source integration
    pub source: String,
    /// Event type
    pub event_type: String,
    /// Additional data
    pub metadata: HashMap<String, String>,
}

/// Event severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl std::fmt::Display for EventSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventSeverity::Info => write!(f, "info"),
            EventSeverity::Warning => write!(f, "warning"),
            EventSeverity::Error => write!(f, "error"),
            EventSeverity::Critical => write!(f, "critical"),
        }
    }
}

/// Integration error types
#[derive(Debug, thiserror::Error)]
pub enum IntegrationError {
    #[error("Connection failed: {0}")]
    Connection(String),
    
    #[error("Authentication failed: {0}")]
    Authentication(String),
    
    #[error("Query failed: {0}")]
    Query(String),
    
    #[error("Timeout")]
    Timeout,
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Not supported: {0}")]
    NotSupported(String),
    
    #[error("Other: {0}")]
    Other(String),
}

/// Database connection info
#[derive(Debug, Clone)]
pub struct DatabaseConnectionInfo {
    /// Hostname
    pub host: String,
    /// Port
    pub port: u16,
    /// Database name
    pub database: String,
    /// Username
    pub username: String,
    /// SSL mode
    pub ssl_mode: SslMode,
}

/// SSL mode for database connections
#[derive(Debug, Clone, Copy)]
pub enum SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

/// Query performance metrics
#[derive(Debug, Clone, Default)]
pub struct QueryMetrics {
    /// Query string (normalized)
    pub query: String,
    /// Total execution time (ms)
    pub total_time_ms: f64,
    /// Number of calls
    pub calls: u64,
    /// Mean time (ms)
    pub mean_time_ms: f64,
    /// Standard deviation
    pub stddev_time_ms: f64,
    /// Rows returned
    pub rows: u64,
    /// Shared blocks hit
    pub shared_blks_hit: u64,
    /// Shared blocks read
    pub shared_blks_read: u64,
}

/// Database connection metrics
#[derive(Debug, Clone, Default)]
pub struct ConnectionMetrics {
    /// Active connections
    pub active: u32,
    /// Idle connections
    pub idle: u32,
    /// Total connections
    pub total: u32,
    /// Max connections allowed
    pub max: u32,
    /// Waiting connections
    pub waiting: u32,
}

/// Network flow information
#[derive(Debug, Clone)]
pub struct NetworkFlow {
    /// Source address
    pub src_addr: String,
    /// Source port
    pub src_port: u16,
    /// Destination address
    pub dst_addr: String,
    /// Destination port
    pub dst_port: u16,
    /// Protocol (TCP/UDP)
    pub protocol: String,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Bytes received
    pub bytes_recv: u64,
    /// Packets sent
    pub packets_sent: u64,
    /// Packets received
    pub packets_recv: u64,
    /// Connection state
    pub state: String,
    /// Process ID (if available)
    pub pid: Option<u32>,
    /// Process name (if available)
    pub process_name: Option<String>,
}

/// CI/CD pipeline information
#[derive(Debug, Clone)]
pub struct PipelineInfo {
    /// Pipeline ID
    pub id: String,
    /// Pipeline name
    pub name: String,
    /// Status
    pub status: PipelineStatus,
    /// Branch
    pub branch: String,
    /// Commit SHA
    pub commit_sha: String,
    /// Author
    pub author: String,
    /// Start time
    pub started_at: SystemTime,
    /// Duration (seconds)
    pub duration_secs: u64,
    /// URL
    pub url: String,
    /// Stages
    pub stages: Vec<PipelineStage>,
}

/// Pipeline status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    Pending,
    Running,
    Success,
    Failed,
    Cancelled,
    Skipped,
}

impl std::fmt::Display for PipelineStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineStatus::Pending => write!(f, "pending"),
            PipelineStatus::Running => write!(f, "running"),
            PipelineStatus::Success => write!(f, "success"),
            PipelineStatus::Failed => write!(f, "failed"),
            PipelineStatus::Cancelled => write!(f, "cancelled"),
            PipelineStatus::Skipped => write!(f, "skipped"),
        }
    }
}

/// Pipeline stage
#[derive(Debug, Clone)]
pub struct PipelineStage {
    /// Stage name
    pub name: String,
    /// Status
    pub status: PipelineStatus,
    /// Duration (seconds)
    pub duration_secs: u64,
    /// Jobs in this stage
    pub jobs: Vec<JobInfo>,
}

/// Job information
#[derive(Debug, Clone)]
pub struct JobInfo {
    /// Job name
    pub name: String,
    /// Status
    pub status: PipelineStatus,
    /// Duration (seconds)
    pub duration_secs: u64,
    /// Log URL
    pub log_url: Option<String>,
}

impl Metric {
    /// Create a new gauge metric
    pub fn gauge(
        name: impl Into<String>,
        value: f64,
        source: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            value: MetricValue::Gauge(value),
            timestamp: SystemTime::now(),
            tags: HashMap::new(),
            source: source.into(),
            metric_type: MetricType::Gauge,
            unit: None,
            description: None,
        }
    }

    /// Create a new counter metric
    pub fn counter(
        name: impl Into<String>,
        value: u64,
        source: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            value: MetricValue::Counter(value),
            timestamp: SystemTime::now(),
            tags: HashMap::new(),
            source: source.into(),
            metric_type: MetricType::Counter,
            unit: None,
            description: None,
        }
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Add multiple tags
    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags.extend(tags);
        self
    }

    /// Set unit
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    /// Set description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }
}

impl Event {
    /// Create a new event
    pub fn new(
        title: impl Into<String>,
        description: impl Into<String>,
        severity: EventSeverity,
        source: impl Into<String>,
        event_type: impl Into<String>,
    ) -> Self {
        Self {
            title: title.into(),
            description: description.into(),
            severity,
            timestamp: SystemTime::now(),
            source: source.into(),
            event_type: event_type.into(),
            metadata: HashMap::new(),
        }
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_creation() {
        let metric = Metric::gauge("cpu_usage", 75.5, "system")
            .with_tag("host", "server-1")
            .with_unit("percent");

        assert_eq!(metric.name, "cpu_usage");
        assert_eq!(metric.source, "system");
        assert_eq!(metric.tags.get("host"), Some(&"server-1".to_string()));
        assert_eq!(metric.unit, Some("percent".to_string()));
    }

    #[test]
    fn test_event_creation() {
        let event = Event::new(
            "High CPU",
            "CPU usage above 90%",
            EventSeverity::Warning,
            "monitor",
            "threshold_breach",
        )
        .with_metadata("host", "server-1")
        .with_metadata("value", "95");

        assert_eq!(event.title, "High CPU");
        assert_eq!(event.severity, EventSeverity::Warning);
        assert_eq!(event.metadata.get("host"), Some(&"server-1".to_string()));
    }
}
