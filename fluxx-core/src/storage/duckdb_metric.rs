//! DuckDB-backed MetricStore with batched Appender ingest.
//! Same shape as `duckdb_log` — see that module's header.

use crate::core::error::{Error, Result};
use crate::models::metric::{AggregatedMetric, Metric, MetricType};
use crate::storage::event_store::TimeWindow;
use crate::storage::metric_store::MetricStore;
use async_trait::async_trait;
use duckdb::{params, Connection, DropBehavior};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{info, warn};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS metrics (
    id              TEXT,
    name            TEXT NOT NULL,
    value           DOUBLE NOT NULL,
    metric_type     TEXT NOT NULL,
    tags            JSON,
    timestamp_us    BIGINT NOT NULL,
    sample_rate     DOUBLE,
    service_id      TEXT,
    project_id      TEXT,
    environment_id  TEXT
);
CREATE INDEX IF NOT EXISTS idx_metrics_ts ON metrics(timestamp_us);
CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(name);
CREATE INDEX IF NOT EXISTS idx_metrics_service ON metrics(service_id);
"#;

const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_CHANNEL_CAPACITY: usize = 100_000;

enum Cmd {
    Insert(Metric),
    Flush(oneshot::Sender<()>),
}

#[derive(Debug)]
pub struct DuckDbMetricStore {
    conn: Arc<Mutex<Connection>>,
    tx: mpsc::Sender<Cmd>,
    path: String,
}

impl DuckDbMetricStore {
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
        info!("DuckDbMetricStore: opened {path_str} (batch={batch_size}, flush={flush_interval:?})");

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
            .map_err(|_| Error::Internal("DuckDbMetricStore: drainer is gone".into()))?;
        rx.await
            .map_err(|_| Error::Internal("DuckDbMetricStore: flush ack lost".into()))?;
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
                "COPY (SELECT * FROM metrics WHERE timestamp_us < {threshold}) \
                 TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE)"
            );
            conn.execute_batch(&copy_sql)
                .map_err(|e| Error::Internal(format!("duckdb COPY metrics: {e}")))?;
            let archived: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM metrics WHERE timestamp_us < ?",
                    params![threshold],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            if delete_after && archived > 0 {
                conn.execute(
                    "DELETE FROM metrics WHERE timestamp_us < ?",
                    params![threshold],
                )
                .map_err(|e| Error::Internal(format!("duckdb DELETE metrics: {e}")))?;
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
        let mut buf: Vec<Metric> = Vec::with_capacity(batch_size);
        let mut pending: Vec<oneshot::Sender<()>> = Vec::new();
        let mut ticker = tokio::time::interval(flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(Cmd::Insert(m)) => {
                        buf.push(m);
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
    buf: &mut Vec<Metric>,
    pending: &mut Vec<oneshot::Sender<()>>,
) {
    let drain: Vec<Metric> = std::mem::take(buf);
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
                .appender("metrics")
                .map_err(|e| Error::Internal(format!("duckdb appender: {e}")))?;
            for metric in &drain {
                let tags_json = serde_json::to_string(&metric.tags).unwrap_or_else(|_| "{}".into());
                app.append_row(params![
                    metric.id,
                    metric.name,
                    metric.value,
                    metric_type_to_str(metric.metric_type),
                    tags_json,
                    clamp_u64(metric.timestamp_us),
                    metric.sample_rate,
                    metric.service_id,
                    metric.project_id,
                    metric.environment_id,
                ])
                .map_err(|e| Error::Internal(format!("duckdb append: {e}")))?;
            }
        }
        Ok(())
    })
    .await;
    if let Err(e) = result.unwrap_or_else(|e| Err(Error::Internal(format!("join: {e}")))) {
        warn!("DuckDbMetricStore: batch flush failed ({row_count} rows dropped): {e}");
    }
    for r in replies { let _ = r.send(()); }
}

fn metric_type_to_str(t: MetricType) -> &'static str {
    match t {
        MetricType::Counter => "counter",
        MetricType::Gauge => "gauge",
        MetricType::Histogram => "histogram",
        MetricType::Set => "set",
        MetricType::Timing => "timing",
        MetricType::Distribution => "distribution",
        MetricType::Summary => "summary",
    }
}

fn str_to_metric_type(s: &str) -> MetricType {
    match s {
        "counter" => MetricType::Counter,
        "histogram" => MetricType::Histogram,
        "set" => MetricType::Set,
        "timing" => MetricType::Timing,
        "distribution" => MetricType::Distribution,
        "summary" => MetricType::Summary,
        _ => MetricType::Gauge,
    }
}

fn row_to_metric(row: &duckdb::Row<'_>) -> duckdb::Result<Metric> {
    let mt_s: String = row.get(3)?;
    let tags_json: String = row.get(4).unwrap_or_default();
    let tags: HashMap<String, String> = serde_json::from_str(&tags_json).unwrap_or_default();
    Ok(Metric {
        id: row.get(0).unwrap_or_default(),
        name: row.get(1)?,
        value: row.get(2)?,
        metric_type: str_to_metric_type(&mt_s),
        tags,
        timestamp_us: row.get::<_, i64>(5)? as u64,
        sample_rate: row.get(6).unwrap_or(1.0),
        service_id: row.get(7).ok(),
        project_id: row.get(8).ok(),
        environment_id: row.get(9).ok(),
    })
}

/// Clamp u64 → i64 so default `u64::MAX` doesn't wrap to -1 in SQL BIGINT.
fn clamp_u64(v: u64) -> i64 {
    if v > i64::MAX as u64 { i64::MAX } else { v as i64 }
}

#[async_trait]
impl MetricStore for DuckDbMetricStore {
    async fn insert(&self, metric: Metric) -> Result<()> {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(Cmd::Insert(metric)) {
            warn!("DuckDbMetricStore: channel saturated, dropping metric");
        }
        Ok(())
    }

    async fn query(
        &self,
        name: Option<&str>,
        tags: &HashMap<String, String>,
        window: TimeWindow,
    ) -> Result<Vec<Metric>> {
        let _ = self.flush_now().await;
        let conn = self.conn.clone();
        let name_owned: Option<String> = name.map(|s| s.to_string());
        let tags_owned = tags.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<Metric>> {
            let conn = conn.blocking_lock();
            let mut sql = String::from(
                "SELECT id, name, value, metric_type, tags, timestamp_us, sample_rate, service_id, project_id, environment_id
                 FROM metrics WHERE timestamp_us BETWEEN ? AND ?",
            );
            let mut binds: Vec<Box<dyn duckdb::ToSql>> =
                vec![Box::new(clamp_u64(window.start_us)), Box::new(clamp_u64(window.end_us))];
            if let Some(n) = name_owned {
                sql.push_str(" AND name = ?");
                binds.push(Box::new(n));
            }
            sql.push_str(" ORDER BY timestamp_us");

            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| Error::Internal(format!("duckdb prepare: {e}")))?;
            let bind_refs: Vec<&dyn duckdb::ToSql> = binds.iter().map(|b| b.as_ref()).collect();
            let rows = stmt
                .query_map(duckdb::params_from_iter(bind_refs.iter()), row_to_metric)
                .map_err(|e| Error::Internal(format!("duckdb query: {e}")))?;
            let mut out = Vec::new();
            for r in rows {
                let m = r.map_err(|e| Error::Internal(format!("row: {e}")))?;
                if !tags_owned.is_empty()
                    && !tags_owned.iter().all(|(k, v)| m.tags.get(k) == Some(v))
                {
                    continue;
                }
                out.push(m);
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::Internal(format!("join error: {e}")))?
    }

    async fn get_aggregated(
        &self,
        name: &str,
        window: TimeWindow,
        interval_secs: u64,
    ) -> Result<Vec<AggregatedMetric>> {
        let _ = self.flush_now().await;
        let conn = self.conn.clone();
        let name_owned = name.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<AggregatedMetric>> {
            let interval_us = (interval_secs * 1_000_000) as i64;
            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare(
                    "SELECT timestamp_us, value FROM metrics
                     WHERE name = ? AND timestamp_us BETWEEN ? AND ?",
                )
                .map_err(|e| Error::Internal(format!("duckdb prepare: {e}")))?;
            let rows = stmt
                .query_map(
                    params![&name_owned, clamp_u64(window.start_us), clamp_u64(window.end_us)],
                    |r| {
                        let ts: i64 = r.get(0)?;
                        let v: f64 = r.get(1)?;
                        Ok((ts, v))
                    },
                )
                .map_err(|e| Error::Internal(format!("duckdb query: {e}")))?;

            let mut buckets: std::collections::BTreeMap<u64, Vec<f64>> = Default::default();
            for r in rows.flatten() {
                let key = ((r.0 / interval_us) * interval_us) as u64;
                buckets.entry(key).or_default().push(r.1);
            }
            Ok(buckets
                .into_iter()
                .map(|(ts, mut v)| AggregatedMetric::from_values(&name_owned, ts, &mut v))
                .collect())
        })
        .await
        .map_err(|e| Error::Internal(format!("join error: {e}")))?
    }

    async fn get_names(&self) -> Result<Vec<String>> {
        let _ = self.flush_now().await;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare("SELECT DISTINCT name FROM metrics ORDER BY name")
                .map_err(|e| Error::Internal(format!("duckdb prepare: {e}")))?;
            let rows = stmt
                .query_map([], |r| r.get::<_, String>(0))
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
                .query_row("SELECT COUNT(*) FROM metrics", [], |r| r.get(0))
                .map_err(|e| Error::Internal(format!("duckdb len: {e}")))?;
            Ok(n as usize)
        })
        .await
        .map_err(|e| Error::Internal(format!("join error: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip() {
        let store = DuckDbMetricStore::open_in_memory().unwrap();
        store
            .insert(
                Metric::new("cpu".to_string(), 50.0, MetricType::Gauge).with_timestamp(1000),
            )
            .await
            .unwrap();
        assert_eq!(store.len().await.unwrap(), 1);
        let names = store.get_names().await.unwrap();
        assert_eq!(names, vec!["cpu"]);
    }

    #[tokio::test]
    async fn agg_buckets() {
        let store = DuckDbMetricStore::open_in_memory().unwrap();
        for i in 0..5 {
            store
                .insert(
                    Metric::new("lat".to_string(), i as f64, MetricType::Histogram)
                        .with_timestamp(1_000_000 + i * 100_000),
                )
                .await
                .unwrap();
        }
        let agg = store
            .get_aggregated("lat", TimeWindow::new(0, 10_000_000), 1)
            .await
            .unwrap();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].count, 5);
    }
}
