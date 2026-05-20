//! DuckDB-backed EventStore (traces) with batched Appender ingest.
//! Same shape as `duckdb_log` — see that module's header.

use crate::core::error::{Error, Result};
use crate::models::event::{Event, EventSeverity, Metadata, ProtocolType};
use crate::storage::event_store::{EventStore, TimeWindow};
use async_trait::async_trait;
use duckdb::{params, Connection, DropBehavior};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{info, warn};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS events (
    id              TEXT,
    session_id      TEXT,
    project_id      TEXT,
    environment_id  TEXT,
    service_id      TEXT,
    timestamp_us    BIGINT NOT NULL,
    protocol        TEXT NOT NULL,
    latency_us      BIGINT NOT NULL,
    severity        TEXT NOT NULL,
    payload         TEXT,
    metadata        JSON
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp_us);
CREATE INDEX IF NOT EXISTS idx_events_protocol ON events(protocol);
CREATE INDEX IF NOT EXISTS idx_events_severity ON events(severity);
CREATE INDEX IF NOT EXISTS idx_events_service ON events(service_id);
"#;

const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CHANNEL_CAPACITY: usize = 100_000;

enum Cmd {
    Insert(Event),
    Flush(oneshot::Sender<()>),
}

#[derive(Debug)]
pub struct DuckDbEventStore {
    conn: Arc<Mutex<Connection>>,
    tx: mpsc::Sender<Cmd>,
    path: String,
}

impl DuckDbEventStore {
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
        info!("DuckDbEventStore: opened {path_str} (batch={batch_size}, flush={flush_interval:?})");

        let conn = Arc::new(Mutex::new(conn));
        let (tx, rx) = mpsc::channel::<Cmd>(channel_capacity);
        spawn_drainer(conn.clone(), rx, batch_size, flush_interval);
        Ok(Self { conn, tx, path: path_str })
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    pub async fn flush_now(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Flush(tx))
            .await
            .map_err(|_| Error::Internal("DuckDbEventStore: drainer is gone".into()))?;
        rx.await
            .map_err(|_| Error::Internal("DuckDbEventStore: flush ack lost".into()))?;
        Ok(())
    }

    /// Cold-tier archive — see `DuckDbLogStore::archive_to_parquet`.
    pub async fn archive_to_parquet(
        &self,
        older_than_us: u64,
        output_path: String,
        delete_after: bool,
    ) -> Result<usize> {
        self.flush_now().await?;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<usize> {
            let threshold = clamp_u64(older_than_us);
            let conn = conn.blocking_lock();
            let copy_sql = format!(
                "COPY (SELECT * FROM events WHERE timestamp_us < {threshold}) \
                 TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE)"
            );
            conn.execute_batch(&copy_sql)
                .map_err(|e| Error::Internal(format!("duckdb COPY events: {e}")))?;
            let archived: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE timestamp_us < ?",
                    params![threshold],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            if delete_after && archived > 0 {
                conn.execute(
                    "DELETE FROM events WHERE timestamp_us < ?",
                    params![threshold],
                )
                .map_err(|e| Error::Internal(format!("duckdb DELETE events: {e}")))?;
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
        let mut buf: Vec<Event> = Vec::with_capacity(batch_size);
        let mut pending: Vec<oneshot::Sender<()>> = Vec::new();
        let mut ticker = tokio::time::interval(flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(Cmd::Insert(e)) => {
                        buf.push(e);
                        if buf.len() >= batch_size {
                            flush(&conn, &mut buf, &mut pending).await;
                        }
                    }
                    Some(Cmd::Flush(reply)) => {
                        pending.push(reply);
                        flush(&conn, &mut buf, &mut pending).await;
                    }
                    None => { flush(&conn, &mut buf, &mut pending).await; break; }
                },
                _ = ticker.tick() => flush(&conn, &mut buf, &mut pending).await,
            }
        }
    });
}

async fn flush(
    conn: &Arc<Mutex<Connection>>,
    buf: &mut Vec<Event>,
    pending: &mut Vec<oneshot::Sender<()>>,
) {
    let drain: Vec<Event> = std::mem::take(buf);
    let replies: Vec<oneshot::Sender<()>> = std::mem::take(pending);
    if drain.is_empty() {
        for r in replies { let _ = r.send(()); }
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
                .appender("events")
                .map_err(|e| Error::Internal(format!("duckdb appender: {e}")))?;
            for event in &drain {
                let metadata = serde_json::to_string(&event.metadata).unwrap_or_else(|_| "{}".into());
                app.append_row(params![
                    event.id,
                    event.session_id,
                    event.project_id,
                    event.environment_id,
                    event.service_id,
                    clamp_u64(event.timestamp_us),
                    protocol_to_str(event.protocol),
                    clamp_u64(event.latency_us),
                    severity_to_str(event.severity),
                    event.payload,
                    metadata,
                ])
                .map_err(|e| Error::Internal(format!("duckdb append: {e}")))?;
            }
        }
        Ok(())
    })
    .await;
    if let Err(e) = result.unwrap_or_else(|e| Err(Error::Internal(format!("join: {e}")))) {
        warn!("DuckDbEventStore: batch flush failed ({row_count} rows dropped): {e}");
    }
    for r in replies { let _ = r.send(()); }
}

fn protocol_to_str(p: ProtocolType) -> &'static str {
    match p {
        ProtocolType::Http => "http",
        ProtocolType::Ws => "ws",
        ProtocolType::Tcp => "tcp",
        ProtocolType::Udp => "udp",
        ProtocolType::Grpc => "grpc",
        ProtocolType::Quic => "quic",
        ProtocolType::Ebpf => "ebpf",
        ProtocolType::Mqtt => "mqtt",
        ProtocolType::Modbus => "modbus",
        ProtocolType::System => "system",
        ProtocolType::Otlp => "otlp",
        ProtocolType::StatsD => "statsd",
        ProtocolType::Log => "log",
        ProtocolType::Other => "other",
    }
}

fn str_to_protocol(s: &str) -> ProtocolType {
    match s {
        "http" => ProtocolType::Http,
        "ws" => ProtocolType::Ws,
        "tcp" => ProtocolType::Tcp,
        "udp" => ProtocolType::Udp,
        "grpc" => ProtocolType::Grpc,
        "quic" => ProtocolType::Quic,
        "ebpf" => ProtocolType::Ebpf,
        "mqtt" => ProtocolType::Mqtt,
        "modbus" => ProtocolType::Modbus,
        "system" => ProtocolType::System,
        "otlp" => ProtocolType::Otlp,
        "statsd" => ProtocolType::StatsD,
        "log" => ProtocolType::Log,
        _ => ProtocolType::Other,
    }
}

fn severity_to_str(s: EventSeverity) -> &'static str {
    match s {
        EventSeverity::Info => "info",
        EventSeverity::Low => "low",
        EventSeverity::Medium => "medium",
        EventSeverity::High => "high",
        EventSeverity::Critical => "critical",
    }
}

fn str_to_severity(s: &str) -> EventSeverity {
    match s {
        "low" => EventSeverity::Low,
        "medium" => EventSeverity::Medium,
        "high" => EventSeverity::High,
        "critical" => EventSeverity::Critical,
        _ => EventSeverity::Info,
    }
}

fn row_to_event(row: &duckdb::Row<'_>) -> duckdb::Result<Event> {
    let protocol_s: String = row.get(6)?;
    let severity_s: String = row.get(8)?;
    let metadata_json: String = row.get(10).unwrap_or_default();
    let metadata: Metadata = serde_json::from_str(&metadata_json).unwrap_or_default();
    Ok(Event {
        id: row.get(0).unwrap_or_default(),
        session_id: row.get(1).ok(),
        project_id: row.get(2).ok(),
        environment_id: row.get(3).ok(),
        service_id: row.get(4).ok(),
        timestamp_us: row.get::<_, i64>(5)? as u64,
        protocol: str_to_protocol(&protocol_s),
        latency_us: row.get::<_, i64>(7)? as u64,
        severity: str_to_severity(&severity_s),
        payload: row.get(9).ok(),
        metadata,
    })
}

/// Clamp u64 → i64 so default `u64::MAX` doesn't wrap to -1 in SQL BIGINT.
fn clamp_u64(v: u64) -> i64 {
    if v > i64::MAX as u64 { i64::MAX } else { v as i64 }
}

#[async_trait]
impl EventStore for DuckDbEventStore {
    async fn insert(&self, event: Event) -> Result<()> {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(Cmd::Insert(event)) {
            warn!("DuckDbEventStore: channel saturated, dropping event");
        }
        Ok(())
    }

    async fn query(&self, window: TimeWindow) -> Result<Vec<Event>> {
        let _ = self.flush_now().await;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<Event>> {
            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare(
                    "SELECT id, session_id, project_id, environment_id, service_id, timestamp_us, protocol, latency_us, severity, payload, metadata
                     FROM events WHERE timestamp_us BETWEEN ? AND ? ORDER BY timestamp_us",
                )
                .map_err(|e| Error::Internal(format!("duckdb prepare: {e}")))?;
            let rows = stmt
                .query_map(params![clamp_u64(window.start_us), clamp_u64(window.end_us)], row_to_event)
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
                .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
                .map_err(|e| Error::Internal(format!("duckdb len: {e}")))?;
            Ok(n as usize)
        })
        .await
        .map_err(|e| Error::Internal(format!("join error: {e}")))?
    }

    async fn list_events(
        &self,
        project_id: Option<String>,
        protocol: Option<ProtocolType>,
        severity: Option<EventSeverity>,
        service_id: Option<String>,
    ) -> Vec<Event> {
        let _ = self.flush_now().await;
        let conn = self.conn.clone();
        let res: Result<Vec<Event>> = tokio::task::spawn_blocking(move || -> Result<Vec<Event>> {
            let mut sql = String::from(
                "SELECT id, session_id, project_id, environment_id, service_id, timestamp_us, protocol, latency_us, severity, payload, metadata
                 FROM events WHERE 1=1",
            );
            let mut binds: Vec<Box<dyn duckdb::ToSql>> = vec![];
            if let Some(p) = project_id {
                sql.push_str(" AND project_id = ?");
                binds.push(Box::new(p));
            }
            if let Some(p) = protocol {
                sql.push_str(" AND protocol = ?");
                binds.push(Box::new(protocol_to_str(p).to_string()));
            }
            if let Some(s) = severity {
                sql.push_str(" AND severity = ?");
                binds.push(Box::new(severity_to_str(s).to_string()));
            }
            if let Some(s) = service_id {
                sql.push_str(" AND service_id = ?");
                binds.push(Box::new(s));
            }
            sql.push_str(" ORDER BY timestamp_us DESC");

            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| Error::Internal(format!("duckdb prepare: {e}")))?;
            let bind_refs: Vec<&dyn duckdb::ToSql> = binds.iter().map(|b| b.as_ref()).collect();
            let rows = stmt
                .query_map(duckdb::params_from_iter(bind_refs.iter()), row_to_event)
                .map_err(|e| Error::Internal(format!("duckdb query: {e}")))?;
            let mut out = Vec::new();
            for r in rows {
                out.push(r.map_err(|e| Error::Internal(format!("row: {e}")))?);
            }
            Ok(out)
        })
        .await
        .unwrap_or_else(|_| Ok(Vec::new()));
        res.unwrap_or_default()
    }

    async fn list_events_since(&self, since_us: u64) -> Vec<Event> {
        let _ = self.flush_now().await;
        let conn = self.conn.clone();
        let res: Result<Vec<Event>> = tokio::task::spawn_blocking(move || -> Result<Vec<Event>> {
            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare(
                    "SELECT id, session_id, project_id, environment_id, service_id, timestamp_us, protocol, latency_us, severity, payload, metadata
                     FROM events WHERE timestamp_us > ? ORDER BY timestamp_us",
                )
                .map_err(|e| Error::Internal(format!("duckdb prepare: {e}")))?;
            let rows = stmt
                .query_map(params![clamp_u64(since_us)], row_to_event)
                .map_err(|e| Error::Internal(format!("duckdb query: {e}")))?;
            let mut out = Vec::new();
            for r in rows {
                out.push(r.map_err(|e| Error::Internal(format!("row: {e}")))?);
            }
            Ok(out)
        })
        .await
        .unwrap_or_else(|_| Ok(Vec::new()));
        res.unwrap_or_default()
    }

    async fn get_stats(&self) -> serde_json::Value {
        let _ = self.flush_now().await;
        let conn = self.conn.clone();
        let stats: Result<serde_json::Value> = tokio::task::spawn_blocking(move || -> Result<serde_json::Value> {
            let conn = conn.blocking_lock();
            let total: i64 = conn
                .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
                .unwrap_or(0);
            let mut counts = serde_json::Map::new();
            let mut stmt = conn
                .prepare("SELECT protocol, COUNT(*) FROM events GROUP BY protocol")
                .map_err(|e| Error::Internal(format!("duckdb prepare: {e}")))?;
            let rows = stmt
                .query_map([], |r| {
                    let p: String = r.get(0)?;
                    let c: i64 = r.get(1)?;
                    Ok((p, c))
                })
                .map_err(|e| Error::Internal(format!("duckdb query: {e}")))?;
            for r in rows.flatten() {
                counts.insert(r.0, serde_json::Value::from(r.1));
            }
            Ok(serde_json::json!({
                "total_events": total,
                "backend": "duckdb",
                "protocols": serde_json::Value::Object(counts),
            }))
        })
        .await
        .unwrap_or_else(|_| Ok(serde_json::json!({"total_events": 0})));
        stats.unwrap_or_else(|_| serde_json::json!({"total_events": 0}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip() {
        let store = DuckDbEventStore::open_in_memory().unwrap();
        let ev = Event::new("e1".to_string(), ProtocolType::Otlp);
        store.insert(ev).await.unwrap();
        assert_eq!(store.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn filter_by_protocol() {
        let store = DuckDbEventStore::open_in_memory().unwrap();
        store
            .insert(Event::new("a".to_string(), ProtocolType::Http))
            .await
            .unwrap();
        store
            .insert(Event::new("b".to_string(), ProtocolType::Otlp))
            .await
            .unwrap();
        let http = store
            .list_events(None, Some(ProtocolType::Http), None, None)
            .await;
        assert_eq!(http.len(), 1);
        assert_eq!(http[0].id, "a");
    }
}
