use crate::models::log_entry::{LogEntry, LogLevel};
use crate::core::error::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Query parameters for log search.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogQuery {
    pub q: Option<String>,
    pub service: Option<String>,
    pub level: Option<LogLevel>,
    pub trace_id: Option<String>,
    pub from_us: Option<u64>,
    pub to_us: Option<u64>,
    pub limit: usize,
    pub offset: usize,
}

/// LogStore: Interface for structured log persistence and retrieval.
#[async_trait]
pub trait LogStore: Send + Sync + std::fmt::Debug {
    /// Inserts a log entry.
    async fn insert(&self, entry: LogEntry) -> Result<()>;

    /// Queries log entries with filtering.
    async fn query(&self, params: LogQuery) -> Result<Vec<LogEntry>>;

    /// Returns the total number of stored log entries.
    async fn len(&self) -> Result<usize>;
}
