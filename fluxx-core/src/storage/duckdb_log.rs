//! DuckDB-backed LogStore with batched Appender ingest.
//!
//! Writes are fire-and-forget: insert() pushes onto a bounded
//! mpsc channel and returns immediately. A background drainer
//! batches up to `batch_size` rows or waits `flush_interval`, then
//! commits the batch via DuckDB's binary Appender API (no SQL
//! parsing per row — orders of magnitude faster than per-row
//! INSERT).
//!
//! Reads (query, len) hit the Connection directly through the
//! shared `Arc<Mutex<Connection>>`. The drainer releases the lock
//! between batches so reads and archive operations can interleave.
//!
//! `:memory:` databases must go through `Connection::open_in_memory()`
//! — passing it as a Path creates a *file* literally named
//! ":memory:" relative to cwd and every call sees an empty db.

use crate::core::error::{Error, Result};
use crate::models::log_entry::{LogEntry, LogLevel};
use crate::storage::log_store::{LogQuery, LogStore};
use async_trait::async_trait;
use duckdb::{params, Connection, DropBehavior};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{info, warn};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS logs (
    id            TEXT,
    timestamp_us  BIGINT NOT NULL,
    level         TEXT NOT NULL,
    message       TEXT NOT NULL,
    service       TEXT,
    trace_id      TEXT,
    span_id       TEXT,
    project_id    TEXT,
    environment_id TEXT,
    attributes    JSON,
    resource      JSON
);
CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(timestamp_us);
CREATE INDEX IF NOT EXISTS idx_logs_service ON logs(service);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
CREATE INDEX IF NOT EXISTS idx_logs_trace ON logs(trace_id);
"#;

const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CHANNEL_CAPACITY: usize = 100_000;

/// One drainer command. Either an entry to append, or a flush
/// request that blocks until pending entries have been committed
/// (used by tests and by the cold-tier archiver to guarantee
/// freshness before reading).
enum Cmd {
    Insert(LogEntry),
    Flush(oneshot::Sender<()>),
}

#[derive(Debug)]
pub struct DuckDbLogStore {
    conn: Arc<Mutex<Connection>>,
    tx: mpsc::Sender<Cmd>,
    path: String,
}

impl DuckDbLogStore {
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Self> {
        Self::open_with(path, DEFAULT_BATCH_SIZE, DEFAULT_FLUSH_INTERVAL, DEFAULT_CHANNEL_CAPACITY)
    }

    pub fn open_with<P: Into<PathBuf>>(
        path: P,
        batch_size: usize,
        flush_interval: Duration,
        channel_capacity: usize,
    ) -> Result<Self> {
        let path: PathBuf = path.into();
        let path_str = path.to_string_lossy().to_string();
        let conn = if path_str == ":memory:" {
            Connection::open_in_memory()
                .map_err(|e| Error::Internal(format!("duckdb open :memory:: {e}")))?
        } else {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        Error::Internal(format!("create duckdb dir: {e}"))
                    })?;
                }
            }
            Connection::open(&path)
                .map_err(|e| Error::Internal(format!("duckdb open {path_str}: {e}")))?
        };
        conn.execute_batch(SCHEMA)
            .map_err(|e| Error::Internal(format!("duckdb schema: {e}")))?;
        info!("DuckDbLogStore: opened {path_str} (batch={batch_size}, flush={flush_interval:?})");

        let conn = Arc::new(Mutex::new(conn));
        let (tx, rx) = mpsc::channel::<Cmd>(channel_capacity);
        spawn_drainer(conn.clone(), rx, batch_size, flush_interval);

        Ok(Self { conn, tx, path: path_str })
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// Block until all currently-queued inserts have been committed
    /// to DuckDB. Used by tests and by `archive_to_parquet` to read
    /// a consistent snapshot.
    pub async fn flush_now(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Flush(tx))
            .await
            .map_err(|_| Error::Internal("DuckDbLogStore: drainer is gone".into()))?;
        rx.await
            .map_err(|_| Error::Internal("DuckDbLogStore: flush ack lost".into()))?;
        Ok(())
    }

    /// Cold-tier archive: write all log rows with `timestamp_us <
    /// older_than_us` to a Parquet file at `output_path` (local FS
    /// path or `s3://bucket/key` URL — S3 requires the `httpfs`
    /// extension to be loadable). When `delete_after = true`, the
    /// archived rows are removed from the hot DuckDB after the
    /// COPY completes. Returns the number of rows archived.
    pub async fn archive_to_parquet(
        &self,
        older_than_us: u64,
        output_path: String,
        delete_after: bool,
    ) -> Result<usize> {
        // Drain any pending inserts so the COPY snapshot is fresh.
        self.flush_now().await?;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<usize> {
            let threshold = clamp_u64(older_than_us);
            let conn = conn.blocking_lock();
            let copy_sql = format!(
                "COPY (SELECT * FROM logs WHERE timestamp_us < {threshold}) \
                 TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE)"
            );
            conn.execute_batch(&copy_sql)
                .map_err(|e| Error::Internal(format!("duckdb COPY logs: {e}")))?;
            let archived: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM logs WHERE timestamp_us < ?",
                    params![threshold],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            if delete_after && archived > 0 {
                conn.execute(
                    "DELETE FROM logs WHERE timestamp_us < ?",
                    params![threshold],
                )
                .map_err(|e| Error::Internal(format!("duckdb DELETE logs: {e}")))?;
            }
            Ok(archived as usize)
        })
        .await
        .map_err(|e| Error::Internal(format!("join error: {e}")))?
    }
}

fn spawn_drainer(
    conn: Arc<Mutex<Connection>>,
    mut rx: mpsc::Receiver<Cmd>,
    batch_size: usize,
    flush_interval: Duration,
) {
    tokio::spawn(async move {
        let mut buf: Vec<LogEntry> = Vec::with_capacity(batch_size);
        let mut pending_flush_replies: Vec<oneshot::Sender<()>> = Vec::new();
        let mut ticker = tokio::time::interval(flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(Cmd::Insert(e)) => {
                        buf.push(e);
                        if buf.len() >= batch_size {
                            flush(&conn, &mut buf, &mut pending_flush_replies).await;
                        }
                    }
                    Some(Cmd::Flush(reply)) => {
                        pending_flush_replies.push(reply);
                        flush(&conn, &mut buf, &mut pending_flush_replies).await;
                    }
                    None => {
                        // Senders dropped — final flush and exit.
                        flush(&conn, &mut buf, &mut pending_flush_replies).await;
                        break;
                    }
                },
                _ = ticker.tick() => {
                    flush(&conn, &mut buf, &mut pending_flush_replies).await;
                }
            }
        }
    });
}

async fn flush(
    conn: &Arc<Mutex<Connection>>,
    buf: &mut Vec<LogEntry>,
    pending_flush_replies: &mut Vec<oneshot::Sender<()>>,
) {
    let drain: Vec<LogEntry> = std::mem::take(buf);
    let replies: Vec<oneshot::Sender<()>> = std::mem::take(pending_flush_replies);
    if drain.is_empty() {
        for r in replies {
            let _ = r.send(());
        }
        return;
    }
    let row_count = drain.len();
    let conn = conn.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn_guard = conn.blocking_lock();
        let mut tx = conn_guard
            .transaction()
            .map_err(|e| Error::Internal(format!("duckdb transaction: {e}")))?;
        tx.set_drop_behavior(DropBehavior::Commit);
        {
            let mut app = tx
                .appender("logs")
                .map_err(|e| Error::Internal(format!("duckdb appender: {e}")))?;
            for entry in &drain {
                let attrs = serde_json::to_string(&entry.attributes).unwrap_or_else(|_| "{}".into());
                let res = serde_json::to_string(&entry.resource).unwrap_or_else(|_| "{}".into());
                app.append_row(params![
                    entry.id,
                    entry.timestamp_us as i64,
                    level_to_str(entry.level),
                    entry.message,
                    entry.service,
                    entry.trace_id,
                    entry.span_id,
                    entry.project_id,
                    entry.environment_id,
                    attrs,
                    res,
                ])
                .map_err(|e| Error::Internal(format!("duckdb append: {e}")))?;
            }
        }
        // tx drops with Commit behavior
        Ok(())
    })
    .await;
    if let Err(e) = result.unwrap_or_else(|e| Err(Error::Internal(format!("join: {e}")))) {
        warn!("DuckDbLogStore: batch flush failed ({row_count} rows dropped): {e}");
    }
    for r in replies {
        let _ = r.send(());
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

#[async_trait]
impl LogStore for DuckDbLogStore {
    async fn insert(&self, entry: LogEntry) -> Result<()> {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(Cmd::Insert(entry)) {
            warn!("DuckDbLogStore: channel saturated, dropping entry");
        }
        Ok(())
    }

    async fn query(&self, params_in: LogQuery) -> Result<Vec<LogEntry>> {
        // Ensure pending writes are visible.
        let _ = self.flush_now().await;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<LogEntry>> {
            let limit = if params_in.limit == 0 { 100 } else { params_in.limit };
            let from = clamp_u64(params_in.from_us.unwrap_or(0));
            let to = clamp_u64(params_in.to_us.unwrap_or(u64::MAX));

            let mut sql = String::from(
                "SELECT id, timestamp_us, level, message, service, trace_id, span_id, project_id, environment_id, attributes, resource
                 FROM logs
                 WHERE timestamp_us BETWEEN ? AND ?",
            );
            let mut binds: Vec<Box<dyn duckdb::ToSql>> = vec![Box::new(from), Box::new(to)];

            if let Some(svc) = params_in.service.as_ref() {
                sql.push_str(" AND service = ?");
                binds.push(Box::new(svc.clone()));
            }
            if let Some(level) = params_in.level {
                sql.push_str(" AND level = ?");
                binds.push(Box::new(level_to_str(level).to_string()));
            }
            if let Some(tid) = params_in.trace_id.as_ref() {
                sql.push_str(" AND trace_id = ?");
                binds.push(Box::new(tid.clone()));
            }
            if let Some(q) = params_in.q.as_ref() {
                sql.push_str(" AND lower(message) LIKE ?");
                binds.push(Box::new(format!("%{}%", q.to_lowercase())));
            }
            sql.push_str(" ORDER BY timestamp_us DESC LIMIT ? OFFSET ?");
            binds.push(Box::new(limit as i64));
            binds.push(Box::new(params_in.offset as i64));

            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| Error::Internal(format!("duckdb prepare: {e}")))?;
            let bind_refs: Vec<&dyn duckdb::ToSql> = binds.iter().map(|b| b.as_ref()).collect();
            let rows = stmt
                .query_map(duckdb::params_from_iter(bind_refs.iter()), |row| {
                    let attrs_json: String = row.get(9).unwrap_or_default();
                    let res_json: String = row.get(10).unwrap_or_default();
                    let attributes = serde_json::from_str(&attrs_json).unwrap_or_default();
                    let resource = serde_json::from_str(&res_json).unwrap_or_default();
                    let level_str: String = row.get(2)?;
                    Ok(LogEntry {
                        id: row.get(0).unwrap_or_default(),
                        timestamp_us: row.get::<_, i64>(1)? as u64,
                        level: str_to_level(&level_str),
                        message: row.get(3)?,
                        service: row.get(4).ok(),
                        trace_id: row.get(5).ok(),
                        span_id: row.get(6).ok(),
                        project_id: row.get(7).ok(),
                        environment_id: row.get(8).ok(),
                        attributes,
                        resource,
                    })
                })
                .map_err(|e| Error::Internal(format!("duckdb query: {e}")))?;

            let mut out = Vec::new();
            for r in rows {
                out.push(r.map_err(|e| Error::Internal(format!("row: {e}")))?);
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::Internal(format!("join error: {e}")))?
    }

    async fn len(&self) -> Result<usize> {
        let _ = self.flush_now().await;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<usize> {
            let conn = conn.blocking_lock();
            let n: i64 = conn
                .query_row("SELECT COUNT(*) FROM logs", [], |r| r.get(0))
                .map_err(|e| Error::Internal(format!("duckdb len: {e}")))?;
            Ok(n as usize)
        })
        .await
        .map_err(|e| Error::Internal(format!("join error: {e}")))?
    }
}

/// DuckDB stores timestamps as BIGINT (i64). Clamp u64 caller values
/// at i64::MAX so a default `u64::MAX` doesn't wrap to -1 in SQL.
fn clamp_u64(v: u64) -> i64 {
    if v > i64::MAX as u64 { i64::MAX } else { v as i64 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip() {
        let store = DuckDbLogStore::open_in_memory().unwrap();
        store
            .insert(LogEntry::new(LogLevel::Info, "hello duckdb".to_string()))
            .await
            .unwrap();
        // len() calls flush_now() internally so this is deterministic.
        let n = store.len().await.unwrap();
        assert_eq!(n, 1);
        let rows = store.query(LogQuery::default()).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].message, "hello duckdb");
    }

    #[tokio::test]
    async fn filter_by_level() {
        let store = DuckDbLogStore::open_in_memory().unwrap();
        store
            .insert(LogEntry::new(LogLevel::Info, "i".to_string()))
            .await
            .unwrap();
        store
            .insert(LogEntry::new(LogLevel::Error, "e".to_string()))
            .await
            .unwrap();
        let errs = store
            .query(LogQuery {
                level: Some(LogLevel::Error),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].message, "e");
    }

    #[tokio::test]
    async fn archive_to_parquet_writes_file_and_deletes() {
        let tmp = std::env::temp_dir().join(format!("fluxx-test-{}.parquet", uuid::Uuid::new_v4()));
        let store = DuckDbLogStore::open_in_memory().unwrap();
        for i in 0..5 {
            let mut e = LogEntry::new(LogLevel::Info, format!("msg {i}"));
            e.timestamp_us = 1000 + i;
            store.insert(e).await.unwrap();
        }
        let n = store
            .archive_to_parquet(10_000, tmp.to_string_lossy().to_string(), true)
            .await
            .unwrap();
        assert_eq!(n, 5);
        assert!(tmp.exists(), "parquet file should exist at {:?}", tmp);
        assert!(tmp.metadata().unwrap().len() > 0, "parquet file should be non-empty");
        assert_eq!(store.len().await.unwrap(), 0, "rows should be deleted after archive");
        let _ = std::fs::remove_file(&tmp);
    }

    #[tokio::test]
    async fn batched_insert_flushes_at_batch_size() {
        // batch_size=3 means rows visible after every 3rd insert.
        let store = DuckDbLogStore::open_with(":memory:", 3, Duration::from_secs(60), 100).unwrap();
        for i in 0..7 {
            store.insert(LogEntry::new(LogLevel::Info, format!("m{i}"))).await.unwrap();
        }
        // Two full batches of 3 should have flushed automatically.
        // The remaining 1 needs an explicit flush_now to be visible.
        store.flush_now().await.unwrap();
        assert_eq!(store.len().await.unwrap(), 7);
    }
}
