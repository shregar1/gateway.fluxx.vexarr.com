//! ClickHouse-backed LogStore. Bulk `JSONEachRow` inserts via a
//! background batcher (single-row INSERTs are 100× slower against
//! ClickHouse than bulk). Queries hit the HTTP interface directly.
//!
//! Durability semantics: insert is **fire-and-forget**. The entry
//! is queued onto a bounded channel and the background drainer
//! flushes on a `flush_batch_size` count or `flush_interval` timer
//! — whichever fires first. If ClickHouse is unreachable, retries
//! happen on the next flush; the queue caps at `queue_capacity`
//! and saturation drops the entry with a warn (in-memory upstream
//! still has it if WriteThrough is layered on top).

use crate::core::error::Result;
use crate::models::log_entry::{LogEntry, LogLevel};
use crate::storage::clickhouse_client::ClickHouseClient;
use crate::storage::log_store::{LogQuery, LogStore};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS {db}.logs (
    id              String,
    timestamp_us    UInt64,
    level           LowCardinality(String),
    message         String,
    service         LowCardinality(Nullable(String)),
    trace_id        Nullable(String),
    span_id         Nullable(String),
    project_id      Nullable(String),
    environment_id  Nullable(String),
    attributes      Map(String, String),
    resource        Map(String, String)
) ENGINE = MergeTree() ORDER BY (timestamp_us)
"#;

#[derive(Debug, Serialize, Deserialize)]
struct LogRow {
    id: String,
    timestamp_us: u64,
    level: String,
    message: String,
    service: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    attributes: HashMap<String, String>,
    resource: HashMap<String, String>,
}

impl From<&LogEntry> for LogRow {
    fn from(e: &LogEntry) -> Self {
        Self {
            id: e.id.clone(),
            timestamp_us: e.timestamp_us,
            level: level_to_str(e.level).to_string(),
            message: e.message.clone(),
            service: e.service.clone(),
            trace_id: e.trace_id.clone(),
            span_id: e.span_id.clone(),
            project_id: e.project_id.clone(),
            environment_id: e.environment_id.clone(),
            // Map<String, String> on the ClickHouse side; serialize
            // attribute values as their JSON `to_string` for stable
            // round-trip across mixed-type AnyValue inputs.
            attributes: e
                .attributes
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect(),
            resource: e.resource.clone(),
        }
    }
}

impl LogRow {
    fn into_entry(self) -> LogEntry {
        LogEntry {
            id: self.id,
            timestamp_us: self.timestamp_us,
            level: str_to_level(&self.level),
            message: self.message,
            service: self.service,
            trace_id: self.trace_id,
            span_id: self.span_id,
            attributes: self
                .attributes
                .into_iter()
                .map(|(k, v)| (k, serde_json::Value::String(v)))
                .collect(),
            resource: self.resource,
            project_id: self.project_id,
            environment_id: self.environment_id,
        }
    }
}

fn level_to_str(l: LogLevel) -> &'static str {
    match l {
        LogLevel::Trace => "trace",
        LogLevel::Debug => "debug",
        LogLevel::Info => "info",
        LogLevel::Warn => "warn",
        LogLevel::Error => "error",
        LogLevel::Fatal => "fatal",
    }
}

fn str_to_level(s: &str) -> LogLevel {
    match s {
        "trace" => LogLevel::Trace,
        "debug" => LogLevel::Debug,
        "warn" => LogLevel::Warn,
        "error" => LogLevel::Error,
        "fatal" => LogLevel::Fatal,
        _ => LogLevel::Info,
    }
}

#[derive(Debug)]
pub struct ClickHouseLogStore {
    client: Arc<ClickHouseClient>,
    tx: mpsc::Sender<LogEntry>,
}

impl ClickHouseLogStore {
    pub async fn new(
        client: Arc<ClickHouseClient>,
        queue_capacity: usize,
        flush_batch_size: usize,
        flush_interval: Duration,
    ) -> Result<Self> {
        let schema_sql = SCHEMA.replace("{db}", client.database());
        client.execute(&schema_sql).await?;
        info!("ClickHouseLogStore: schema ensured ({}.logs)", client.database());

        let (tx, rx) = mpsc::channel::<LogEntry>(queue_capacity);
        spawn_drainer(client.clone(), rx, flush_batch_size, flush_interval);
        Ok(Self { client, tx })
    }
}

fn spawn_drainer(
    client: Arc<ClickHouseClient>,
    mut rx: mpsc::Receiver<LogEntry>,
    batch_size: usize,
    interval: Duration,
) {
    tokio::spawn(async move {
        let mut buf: Vec<LogRow> = Vec::with_capacity(batch_size);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                maybe_entry = rx.recv() => {
                    match maybe_entry {
                        Some(e) => {
                            buf.push(LogRow::from(&e));
                            if buf.len() >= batch_size {
                                flush(&client, &mut buf).await;
                            }
                        }
                        None => {
                            // sender dropped — flush and exit
                            flush(&client, &mut buf).await;
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    flush(&client, &mut buf).await;
                }
            }
        }
    });
}

async fn flush(client: &ClickHouseClient, buf: &mut Vec<LogRow>) {
    if buf.is_empty() {
        return;
    }
    let to_send: Vec<LogRow> = std::mem::take(buf);
    if let Err(e) = client.insert_json("logs", &to_send).await {
        warn!("ClickHouseLogStore: flush failed ({} rows dropped): {e}", to_send.len());
    }
}

#[async_trait]
impl LogStore for ClickHouseLogStore {
    async fn insert(&self, entry: LogEntry) -> Result<()> {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(entry) {
            warn!("ClickHouseLogStore: queue saturated, dropping entry");
        }
        Ok(())
    }

    async fn query(&self, params: LogQuery) -> Result<Vec<LogEntry>> {
        let limit = if params.limit == 0 { 100 } else { params.limit };
        let from = params.from_us.unwrap_or(0);
        let to = params.to_us.unwrap_or(u64::MAX);
        let mut sql = format!(
            "SELECT id, timestamp_us, level, message, service, trace_id, span_id, project_id, environment_id, attributes, resource \
             FROM {}.logs WHERE timestamp_us BETWEEN {from} AND {to}",
            self.client.database()
        );
        if let Some(svc) = params.service.as_ref() {
            sql.push_str(&format!(" AND service = '{}'", svc.replace('\'', "''")));
        }
        if let Some(level) = params.level {
            sql.push_str(&format!(" AND level = '{}'", level_to_str(level)));
        }
        if let Some(tid) = params.trace_id.as_ref() {
            sql.push_str(&format!(" AND trace_id = '{}'", tid.replace('\'', "''")));
        }
        if let Some(q) = params.q.as_ref() {
            sql.push_str(&format!(
                " AND positionCaseInsensitive(message, '{}') > 0",
                q.replace('\'', "''")
            ));
        }
        sql.push_str(&format!(
            " ORDER BY timestamp_us DESC LIMIT {limit} OFFSET {}",
            params.offset
        ));
        let rows: Vec<LogRow> = self.client.query_json(&sql).await?;
        Ok(rows.into_iter().map(|r| r.into_entry()).collect())
    }

    async fn len(&self) -> Result<usize> {
        let sql = format!("SELECT count() FROM {}.logs", self.client.database());
        let n = self.client.query_count(&sql).await?;
        Ok(n as usize)
    }
}
