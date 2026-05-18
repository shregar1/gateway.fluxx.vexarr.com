//! ClickHouse-backed persistence for error events.
//!
//! Acts as a write-behind sink: `enqueue()` is non-blocking and pushes
//! into a bounded mpsc channel; a background task drains the channel,
//! batches up to `flush_batch_size` rows (or `flush_interval`, whichever
//! first), and INSERTs via the ClickHouse HTTP interface in
//! `JSONEachRow` format.
//!
//! Failure mode is fail-safe, not fail-loud: if ClickHouse is
//! unreachable, the queue fills until capacity; new events are then
//! dropped with a `warn!` and a counter increment rather than blocking
//! the ingest hot path. Real durability comes from an upstream WAL in
//! a later milestone (see ARCHITECTURE.md §M1.1).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

/// A per-group rollup from the last N hours. Computed in ClickHouse by
/// `fetch_recent_groups`, consumed at agent startup to warm the
/// in-memory `ErrorTracker.groups` map. Stack trace is intentionally
/// omitted — it lives on the per-event rows and isn't worth replaying
/// when the next live ingest will repopulate it.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GroupSnapshot {
    pub fingerprint: String,
    pub error_type: String,
    pub message: String,
    /// `toUnixTimestamp64Micro(min(ts))` in the query — microseconds since epoch.
    /// Comes through as a JSON number; serde handles the conversion.
    pub first_seen_us: u64,
    pub last_seen_us: u64,
    pub count: u64,
    pub services: Vec<String>,
    /// Release of the chronologically-first event (`argMin(release, ts)`
    /// in CH; sticky-first in the in-memory fallback). Empty when no
    /// event in the window carried a release tag.
    #[serde(default)]
    pub first_release: String,
    /// Release of the chronologically-last event.
    #[serde(default)]
    pub last_release: String,
}

/// One row to insert into `fluxx.error_events`. Serializes to the
/// `JSONEachRow` line format ClickHouse expects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEventRow {
    pub event_id: String,
    pub org_id: String,
    pub project_id: String,
    pub service: String,
    /// ClickHouse parses DateTime64 from RFC3339 with sub-second precision.
    pub ts: DateTime<Utc>,
    pub fingerprint: String,
    pub error_type: String,
    pub message: String,
    pub release: String,
    pub environment: String,
    pub stack_raw: String,
    pub frames_function: Vec<String>,
    pub frames_file: Vec<String>,
    pub frames_line: Vec<u32>,
    pub frames_column: Vec<u32>,
    /// 0 = minified/raw, 1 = symbolicated (set by sourcemap pass).
    pub frames_resolved: u8,
    /// One source-snippet per frame (positional). When sourcesContent
    /// in the .map is present, this is the failing line plus ±2 lines
    /// joined with `\n`. Empty string when context isn't available.
    #[serde(default)]
    pub frames_context: Vec<String>,
    /// Breadcrumb ring buffer as a JSON-encoded array of
    /// `{ts, category, message, data}`. Empty when the SDK didn't
    /// emit any. Promoted out of `tags` in schema 002 so receivers
    /// don't have to know to look there.
    #[serde(default)]
    pub breadcrumbs: String,
    pub session_id: String,
    pub user_id: String,
    pub url: String,
    pub user_agent: String,
    pub tags: HashMap<String, String>,
}

/// Sink for error events. ErrorTracker depends on this trait, not the
/// concrete ClickHouse sink, so unit tests can use a `RecordingSink`.
#[async_trait]
pub trait ErrorEventSink: Send + Sync + std::fmt::Debug {
    /// Non-blocking enqueue. Drops with a warn if the queue is full or
    /// the drain task has exited.
    fn enqueue(&self, row: ErrorEventRow);

    /// Force-flush pending events. Returns the number flushed in this
    /// call. Intended for graceful shutdown and tests.
    async fn flush(&self) -> anyhow::Result<usize>;

    /// Operator-visible counters.
    fn stats(&self) -> SinkStats;
}

#[derive(Debug, Clone, Serialize)]
pub struct SinkStats {
    pub enqueued: u64,
    pub flushed: u64,
    pub dropped: u64,
    pub flush_errors: u64,
    pub queue_depth: u64,
}

/// Configuration for the ClickHouse error-event sink.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub flush_interval: Duration,
    pub flush_batch_size: usize,
    pub queue_capacity: usize,
    pub http_timeout: Duration,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 8123,
            database: "fluxx".to_string(),
            username: None,
            password: None,
            flush_interval: Duration::from_secs(5),
            flush_batch_size: 1000,
            queue_capacity: 100_000,
            http_timeout: Duration::from_secs(30),
        }
    }
}

/// HTTP-backed ClickHouse sink. Holds an mpsc producer; the consuming
/// drain task runs on the tokio runtime until the sink is dropped.
#[derive(Debug)]
pub struct ClickHouseErrorEventSink {
    tx: mpsc::Sender<ErrorEventRow>,
    flush_tx: mpsc::Sender<FlushRequest>,
    enqueued: Arc<AtomicU64>,
    flushed: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    flush_errors: Arc<AtomicU64>,
    queue_capacity: usize,
}

#[derive(Debug)]
struct FlushRequest {
    reply: oneshot::Sender<anyhow::Result<usize>>,
}

impl ClickHouseErrorEventSink {
    /// Connect to ClickHouse, apply migrations, and start the drain
    /// task. Returns once the sink is ready to accept enqueues.
    pub async fn open(config: ClickHouseConfig) -> anyhow::Result<Arc<Self>> {
        let client = HttpClient::new(config.clone());
        client.ping().await?;
        client.run_migrations().await?;

        let (tx, rx) = mpsc::channel(config.queue_capacity);
        let (flush_tx, flush_rx) = mpsc::channel(8);

        let enqueued = Arc::new(AtomicU64::new(0));
        let flushed = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let flush_errors = Arc::new(AtomicU64::new(0));

        let sink = Arc::new(Self {
            tx,
            flush_tx,
            enqueued: enqueued.clone(),
            flushed: flushed.clone(),
            dropped: dropped.clone(),
            flush_errors: flush_errors.clone(),
            queue_capacity: config.queue_capacity,
        });

        let drain_cfg = config.clone();
        tokio::spawn(drain_loop(
            rx,
            flush_rx,
            client,
            drain_cfg,
            flushed,
            flush_errors,
        ));

        info!(
            host = %config.host,
            db = %config.database,
            batch_size = config.flush_batch_size,
            interval_secs = config.flush_interval.as_secs(),
            "ClickHouse error sink opened"
        );
        Ok(sink)
    }

    fn queue_depth(&self) -> usize {
        self.queue_capacity.saturating_sub(self.tx.capacity())
    }
}

#[async_trait]
impl ErrorEventSink for ClickHouseErrorEventSink {
    fn enqueue(&self, row: ErrorEventRow) {
        match self.tx.try_send(row) {
            Ok(()) => {
                self.enqueued.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                let n = self.dropped.fetch_add(1, Ordering::Relaxed);
                if n.is_power_of_two() {
                    warn!(dropped_total = n + 1, "ClickHouse error queue full; dropping events");
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn flush(&self) -> anyhow::Result<usize> {
        let (reply, rx) = oneshot::channel();
        self.flush_tx
            .send(FlushRequest { reply })
            .await
            .map_err(|_| anyhow::anyhow!("drain task is gone; cannot flush"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("flush reply dropped"))?
    }

    fn stats(&self) -> SinkStats {
        SinkStats {
            enqueued: self.enqueued.load(Ordering::Relaxed),
            flushed: self.flushed.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            flush_errors: self.flush_errors.load(Ordering::Relaxed),
            queue_depth: self.queue_depth() as u64,
        }
    }
}

/// Background drain task: pulls from the queue, batches, and INSERTs.
async fn drain_loop(
    mut rx: mpsc::Receiver<ErrorEventRow>,
    mut flush_rx: mpsc::Receiver<FlushRequest>,
    client: HttpClient,
    config: ClickHouseConfig,
    flushed: Arc<AtomicU64>,
    flush_errors: Arc<AtomicU64>,
) {
    let mut buf: Vec<ErrorEventRow> = Vec::with_capacity(config.flush_batch_size);
    let mut ticker = tokio::time::interval(config.flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            row = rx.recv() => match row {
                Some(row) => {
                    buf.push(row);
                    if buf.len() >= config.flush_batch_size {
                        do_flush(&client, &mut buf, &flushed, &flush_errors).await;
                    }
                }
                None => {
                    // Producer side dropped — drain whatever remains, then exit.
                    do_flush(&client, &mut buf, &flushed, &flush_errors).await;
                    info!("ClickHouse drain loop exiting cleanly");
                    return;
                }
            },

            _ = ticker.tick() => {
                if !buf.is_empty() {
                    do_flush(&client, &mut buf, &flushed, &flush_errors).await;
                }
            }

            sig = flush_rx.recv() => {
                if let Some(req) = sig {
                    while let Ok(row) = rx.try_recv() {
                        buf.push(row);
                    }
                    let n = buf.len();
                    let result = if n == 0 {
                        Ok(0)
                    } else {
                        match client.insert_batch(&buf).await {
                            Ok(()) => {
                                flushed.fetch_add(n as u64, Ordering::Relaxed);
                                Ok(n)
                            }
                            Err(e) => {
                                flush_errors.fetch_add(1, Ordering::Relaxed);
                                Err(e)
                            }
                        }
                    };
                    buf.clear();
                    let _ = req.reply.send(result);
                }
            }
        }
    }
}

async fn do_flush(
    client: &HttpClient,
    buf: &mut Vec<ErrorEventRow>,
    flushed: &Arc<AtomicU64>,
    flush_errors: &Arc<AtomicU64>,
) {
    if buf.is_empty() {
        return;
    }
    let n = buf.len();
    match client.insert_batch(buf).await {
        Ok(()) => {
            flushed.fetch_add(n as u64, Ordering::Relaxed);
            debug!(n, "ClickHouse: batch flushed");
        }
        Err(e) => {
            flush_errors.fetch_add(1, Ordering::Relaxed);
            error!(error = %e, n, "ClickHouse: flush failed; batch dropped");
        }
    }
    buf.clear();
}

/// Minimal HTTP wire client for ClickHouse. Separated from the sink so
/// the drain loop can hold it by value and the sink stays clone-light.
#[derive(Debug, Clone)]
struct HttpClient {
    config: ClickHouseConfig,
    http: reqwest::Client,
}

impl HttpClient {
    fn new(config: ClickHouseConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(config.http_timeout)
            .build()
            .expect("reqwest client should build with default settings");
        Self { config, http }
    }

    fn base_url(&self) -> String {
        format!(
            "http://{}:{}/?database={}",
            self.config.host, self.config.port, self.config.database
        )
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match (&self.config.username, &self.config.password) {
            (Some(u), Some(p)) => req.basic_auth(u, Some(p)),
            (Some(u), None) => req.basic_auth(u, Option::<&str>::None),
            _ => req,
        }
    }

    async fn ping(&self) -> anyhow::Result<()> {
        let url = format!(
            "{}&query={}",
            self.base_url(),
            urlencoding::encode("SELECT 1")
        );
        let resp = self
            .auth(self.http.get(url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("ClickHouse ping failed: {}", e))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("ClickHouse ping returned {}: {}", status, body);
        }
        Ok(())
    }

    async fn run_migrations(&self) -> anyhow::Result<()> {
        // All known DDL files, in order. Each is idempotent
        // (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS) so re-running is
        // safe; appending new files at the bottom is the only thing
        // needed to evolve the schema.
        let migrations: &[(&str, &str)] = &[
            ("001_error_events", include_str!("clickhouse_schema/001_error_events.sql")),
            (
                "002_breadcrumbs_and_source_context",
                include_str!("clickhouse_schema/002_breadcrumbs_and_source_context.sql"),
            ),
        ];
        for (name, ddl) in migrations {
            for stmt in split_sql_statements(ddl) {
                let stmt = stmt.trim();
                if stmt.is_empty() || stmt.starts_with("--") {
                    continue;
                }
                // Sent without `database=` since some statements
                // create the database itself; queries inside the DDL
                // qualify with `fluxx.<table>`.
                let url = format!(
                    "http://{}:{}/?query={}",
                    self.config.host,
                    self.config.port,
                    urlencoding::encode(stmt)
                );
                let resp = self
                    .auth(self.http.post(url))
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!("CH migration {} POST: {}", name, e))?;
                if !resp.status().is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("ClickHouse migration {} failed: {}", name, body);
                }
            }
            debug!(migration = name, "ClickHouse migration applied");
        }
        info!("ClickHouse migrations applied ({} file(s))", migrations.len());
        Ok(())
    }

    async fn insert_batch(&self, rows: &[ErrorEventRow]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = String::with_capacity(rows.len() * 512);
        for row in rows {
            let line = serde_json::to_string(row)
                .map_err(|e| anyhow::anyhow!("serialize error event: {}", e))?;
            body.push_str(&line);
            body.push('\n');
        }
        let query = "INSERT INTO fluxx.error_events FORMAT JSONEachRow";
        let url = format!(
            "{}&query={}",
            self.base_url(),
            urlencoding::encode(query)
        );
        let resp = self
            .auth(self.http.post(url).body(body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("CH insert failed: {}", e))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("ClickHouse insert {} returned {}: {}", rows.len(), status, body);
        }
        Ok(())
    }
}

/// Fetch per-fingerprint group rollups for the last `since_hours`. Used
/// at agent startup to repopulate the in-memory tracker.
///
/// Queries `fluxx.error_events` directly (not the `error_groups`
/// materialized view) — both are correct, but the events table doesn't
/// require `-Merge` functions and keeps the query trivially readable.
pub async fn fetch_recent_groups(
    config: &ClickHouseConfig,
    since_hours: u32,
) -> anyhow::Result<Vec<GroupSnapshot>> {
    let client = HttpClient::new(config.clone());
    client.fetch_recent_groups(since_hours).await
}

impl HttpClient {
    async fn fetch_recent_groups(
        &self,
        since_hours: u32,
    ) -> anyhow::Result<Vec<GroupSnapshot>> {
        let query = format!(
            "SELECT \
               fingerprint, \
               any(error_type)                  AS error_type, \
               any(message)                     AS message, \
               toUnixTimestamp64Micro(min(ts))  AS first_seen_us, \
               toUnixTimestamp64Micro(max(ts))  AS last_seen_us, \
               count()                          AS count, \
               groupUniqArray(service)          AS services, \
               argMin(release, ts)              AS first_release, \
               argMax(release, ts)              AS last_release \
             FROM fluxx.error_events \
             WHERE ts > now() - INTERVAL {} HOUR \
             GROUP BY fingerprint \
             FORMAT JSONEachRow",
            since_hours
        );
        let url = format!("{}&query={}", self.base_url(), urlencoding::encode(&query));
        let resp = self
            .auth(self.http.get(url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("CH replay query failed: {}", e))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("CH replay query returned {}: {}", status, body);
        }
        let body = resp
            .text()
            .await
            .map_err(|e| anyhow::anyhow!("read CH replay body: {}", e))?;

        let mut out = Vec::new();
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<GroupSnapshot>(line) {
                Ok(snap) => out.push(snap),
                Err(e) => warn!(error = %e, line, "skipping malformed replay row"),
            }
        }
        Ok(out)
    }
}

/// Naive SQL splitter: splits on `;` outside single-quoted strings.
/// Adequate for our committed DDL since we control the input. Not a
/// general-purpose parser.
fn split_sql_statements(sql: &str) -> Vec<&str> {
    let mut stmts = Vec::new();
    let mut start = 0;
    let mut in_string = false;
    for (i, b) in sql.bytes().enumerate() {
        match b {
            b'\'' => in_string = !in_string,
            b';' if !in_string => {
                stmts.push(&sql[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < sql.len() {
        stmts.push(&sql[start..]);
    }
    stmts
}

// ============================================================================
// Read side — M2: ClickHouse as the canonical query source.
// ============================================================================

/// Aggregate counters surfaced to consumers (dashboard footer, /diagnostics).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReadStoreStats {
    pub total_groups: u64,
    pub total_occurrences: u64,
}

/// Read-side dual of `ErrorEventSink`. The dashboard and alerting layer
/// call into this; an in-memory fallback is provided by `ErrorTracker`
/// when no store is configured.
///
/// All methods take `since_hours` so callers can scope to a window
/// (the dashboard's default is 24h, "all-time" views use the table TTL).
/// Returning `anyhow::Result` lets callers see CH errors and degrade
/// gracefully — the trait does not promise a result, only an attempt.
#[async_trait]
pub trait ErrorReadStore: Send + Sync + std::fmt::Debug {
    async fn list_groups(&self, since_hours: u32) -> anyhow::Result<Vec<GroupSnapshot>>;
    async fn get_group(
        &self,
        fingerprint: &str,
        since_hours: u32,
    ) -> anyhow::Result<Option<GroupSnapshot>>;
    async fn top_groups(
        &self,
        n: usize,
        since_hours: u32,
    ) -> anyhow::Result<Vec<GroupSnapshot>>;
    /// 24-element hour-of-day histogram. `out[hour]` = count of events
    /// whose `toHour(ts)` equals `hour`, summed over the window.
    async fn trend(&self, since_hours: u32) -> anyhow::Result<[u64; 24]>;
    async fn stats(&self) -> anyhow::Result<ReadStoreStats>;
    /// Most recent event matching the fingerprint, with full frame
    /// arrays + breadcrumbs in tags. Used by the dashboard's error
    /// detail panel to render the frame walker.
    async fn latest_event(
        &self,
        fingerprint: &str,
        since_hours: u32,
    ) -> anyhow::Result<Option<LatestEvent>>;
    /// Page of recent events for a fingerprint, newest first. The
    /// dashboard's "Recent" tab pages through these so an operator can
    /// see how the same fingerprint manifested across users / URLs.
    async fn recent_events(
        &self,
        fingerprint: &str,
        since_hours: u32,
        limit: u32,
    ) -> anyhow::Result<Vec<LatestEvent>>;
}

/// One concrete error event as it appears in `fluxx.error_events`,
/// projected for the dashboard's detail panel. The shape mirrors
/// `ErrorEventRow` minus the bookkeeping fields the UI doesn't care
/// about (event_id, org_id, environment).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LatestEvent {
    pub fingerprint: String,
    pub error_type: String,
    pub message: String,
    pub service: String,
    pub release: String,
    /// `toUnixTimestamp64Micro(ts)` — microseconds since epoch.
    pub ts_us: u64,
    pub stack_raw: String,
    #[serde(default)]
    pub frames_function: Vec<String>,
    #[serde(default)]
    pub frames_file: Vec<String>,
    #[serde(default)]
    pub frames_line: Vec<u32>,
    #[serde(default)]
    pub frames_column: Vec<u32>,
    #[serde(default)]
    pub frames_resolved: u8,
    #[serde(default)]
    pub frames_context: Vec<String>,
    #[serde(default)]
    pub breadcrumbs: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub user_agent: String,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

/// ClickHouse-backed read store. Queries `fluxx.error_events` directly
/// (and `fluxx.error_groups` for aggregate counters) via the same HTTP
/// interface as the sink. Owns its own `reqwest::Client`; reqwest pools
/// connections internally, so concurrent sink + store usage is fine.
#[derive(Debug)]
pub struct ClickHouseErrorReadStore {
    client: HttpClient,
}

impl ClickHouseErrorReadStore {
    /// Constructs a store. Does not ping or run migrations — assumes
    /// the sink has already done both. (If you want a standalone
    /// reader, call `HttpClient::ping` via the sink-open path first.)
    pub fn new(config: ClickHouseConfig) -> Arc<Self> {
        Arc::new(Self {
            client: HttpClient::new(config),
        })
    }
}

/// True iff `s` is a valid 16-character hex fingerprint. Used to guard
/// `WHERE fingerprint = '...'` against SQL injection — fingerprints we
/// hand out are always 16 ASCII hex chars (see `compute_fingerprint`).
fn is_valid_fingerprint(s: &str) -> bool {
    s.len() == 16 && s.chars().all(|c| c.is_ascii_hexdigit())
}

#[async_trait]
impl ErrorReadStore for ClickHouseErrorReadStore {
    async fn list_groups(&self, since_hours: u32) -> anyhow::Result<Vec<GroupSnapshot>> {
        // Reuses the replay query — same shape, same projection.
        self.client.fetch_recent_groups(since_hours).await
    }

    async fn get_group(
        &self,
        fingerprint: &str,
        since_hours: u32,
    ) -> anyhow::Result<Option<GroupSnapshot>> {
        if !is_valid_fingerprint(fingerprint) {
            return Ok(None);
        }
        let query = format!(
            "SELECT \
               fingerprint, \
               any(error_type)                  AS error_type, \
               any(message)                     AS message, \
               toUnixTimestamp64Micro(min(ts))  AS first_seen_us, \
               toUnixTimestamp64Micro(max(ts))  AS last_seen_us, \
               count()                          AS count, \
               groupUniqArray(service)          AS services, \
               argMin(release, ts)              AS first_release, \
               argMax(release, ts)              AS last_release \
             FROM fluxx.error_events \
             WHERE fingerprint = '{}' \
               AND ts > now() - INTERVAL {} HOUR \
             GROUP BY fingerprint \
             FORMAT JSONEachRow",
            fingerprint, since_hours
        );
        let body = self.client.run_select(&query).await?;
        let snap = body
            .lines()
            .filter_map(|l| {
                let l = l.trim();
                if l.is_empty() {
                    None
                } else {
                    serde_json::from_str::<GroupSnapshot>(l).ok()
                }
            })
            .next();
        Ok(snap)
    }

    async fn top_groups(
        &self,
        n: usize,
        since_hours: u32,
    ) -> anyhow::Result<Vec<GroupSnapshot>> {
        let query = format!(
            "SELECT \
               fingerprint, \
               any(error_type)                  AS error_type, \
               any(message)                     AS message, \
               toUnixTimestamp64Micro(min(ts))  AS first_seen_us, \
               toUnixTimestamp64Micro(max(ts))  AS last_seen_us, \
               count()                          AS count, \
               groupUniqArray(service)          AS services, \
               argMin(release, ts)              AS first_release, \
               argMax(release, ts)              AS last_release \
             FROM fluxx.error_events \
             WHERE ts > now() - INTERVAL {} HOUR \
             GROUP BY fingerprint \
             ORDER BY count DESC \
             LIMIT {} \
             FORMAT JSONEachRow",
            since_hours, n
        );
        let body = self.client.run_select(&query).await?;
        let mut out = Vec::new();
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<GroupSnapshot>(line) {
                Ok(g) => out.push(g),
                Err(e) => warn!(error = %e, line, "skipping malformed top_groups row"),
            }
        }
        Ok(out)
    }

    async fn trend(&self, since_hours: u32) -> anyhow::Result<[u64; 24]> {
        #[derive(Deserialize)]
        struct Row {
            hour: u8,
            count: u64,
        }
        let query = format!(
            "SELECT toHour(ts) AS hour, count() AS count \
             FROM fluxx.error_events \
             WHERE ts > now() - INTERVAL {} HOUR \
             GROUP BY hour \
             ORDER BY hour \
             FORMAT JSONEachRow",
            since_hours
        );
        let body = self.client.run_select(&query).await?;
        let mut trend = [0u64; 24];
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<Row>(line) {
                Ok(r) if (r.hour as usize) < 24 => trend[r.hour as usize] = r.count,
                Ok(_) => {} // out-of-range, ignore
                Err(e) => warn!(error = %e, line, "skipping malformed trend row"),
            }
        }
        Ok(trend)
    }

    async fn stats(&self) -> anyhow::Result<ReadStoreStats> {
        #[derive(Deserialize)]
        struct Row {
            total_groups: u64,
            total_occurrences: u64,
        }
        // Reads from the materialized rollup so the query stays cheap
        // even at high event counts. `sum(count)` over
        // SimpleAggregateFunction(sum, ...) handles unmerged parts.
        let query = "SELECT \
                       uniqExact(fingerprint) AS total_groups, \
                       sum(count)             AS total_occurrences \
                     FROM fluxx.error_groups \
                     FORMAT JSONEachRow";
        let body = self.client.run_select(query).await?;
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(r) = serde_json::from_str::<Row>(line) {
                return Ok(ReadStoreStats {
                    total_groups: r.total_groups,
                    total_occurrences: r.total_occurrences,
                });
            }
        }
        Ok(ReadStoreStats::default())
    }

    async fn latest_event(
        &self,
        fingerprint: &str,
        since_hours: u32,
    ) -> anyhow::Result<Option<LatestEvent>> {
        if !is_valid_fingerprint(fingerprint) {
            return Ok(None);
        }
        // ClickHouse's Map(LowCardinality(String), String) round-trips
        // through JSONEachRow as a regular object; serde deserialises
        // it directly into `HashMap<String, String>`. `argMax`/`any`
        // on individual columns is the cheap way to pick the row for
        // the most recent timestamp without an ORDER BY + LIMIT 1.
        let body = self
            .client
            .run_select(&events_query(fingerprint, since_hours, 1))
            .await?;
        let line = body.lines().map(str::trim).find(|l| !l.is_empty());
        let Some(line) = line else {
            return Ok(None);
        };
        match serde_json::from_str::<LatestEvent>(line) {
            Ok(e) => Ok(Some(e)),
            Err(e) => {
                warn!(error = %e, "latest_event: row parse failed");
                Ok(None)
            }
        }
    }

    /// Page of recent events for a fingerprint, ordered by `ts DESC`.
    /// Used by the "Recent" tab in the dashboard's error detail sheet.
    /// `limit` is clamped to [1, 200].
    async fn recent_events(
        &self,
        fingerprint: &str,
        since_hours: u32,
        limit: u32,
    ) -> anyhow::Result<Vec<LatestEvent>> {
        if !is_valid_fingerprint(fingerprint) {
            return Ok(Vec::new());
        }
        let limit = limit.clamp(1, 200);
        let body = self
            .client
            .run_select(&events_query(fingerprint, since_hours, limit))
            .await?;
        let mut out = Vec::new();
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<LatestEvent>(line) {
                Ok(e) => out.push(e),
                Err(e) => warn!(error = %e, line, "recent_events: row parse failed"),
            }
        }
        Ok(out)
    }
}

/// Shared SELECT for the per-event detail endpoints. Projecting the
/// breadcrumbs + frames_context columns added in schema 002 alongside
/// the original columns so a single deserializer covers both.
fn events_query(fingerprint: &str, since_hours: u32, limit: u32) -> String {
    format!(
        "SELECT \
           fingerprint, \
           error_type, \
           message, \
           service, \
           release, \
           toUnixTimestamp64Micro(ts) AS ts_us, \
           stack_raw, \
           frames_function, \
           frames_file, \
           frames_line, \
           frames_column, \
           frames_resolved, \
           frames_context, \
           breadcrumbs, \
           session_id, \
           user_id, \
           url, \
           user_agent, \
           tags \
         FROM fluxx.error_events \
         WHERE fingerprint = '{}' \
           AND ts > now() - INTERVAL {} HOUR \
         ORDER BY ts DESC \
         LIMIT {} \
         FORMAT JSONEachRow",
        fingerprint, since_hours, limit
    )
}

impl HttpClient {
    /// Shared GET-with-query helper for the read-side queries. Returns
    /// the raw response body; callers parse JSONEachRow line-by-line.
    async fn run_select(&self, query: &str) -> anyhow::Result<String> {
        let url = format!(
            "{}&query={}",
            self.base_url(),
            urlencoding::encode(query)
        );
        let resp = self
            .auth(self.http.get(url))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("CH SELECT failed: {}", e))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("CH SELECT returned {}: {}", status, body);
        }
        resp.text()
            .await
            .map_err(|e| anyhow::anyhow!("read CH SELECT body: {}", e))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Mutex;

    /// In-memory sink for tests — records every enqueue.
    #[derive(Debug, Default)]
    pub struct RecordingSink {
        rows: Mutex<Vec<ErrorEventRow>>,
        enqueued: AtomicU64,
    }

    impl RecordingSink {
        pub fn snapshot(&self) -> Vec<ErrorEventRow> {
            self.rows.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ErrorEventSink for RecordingSink {
        fn enqueue(&self, row: ErrorEventRow) {
            self.rows.lock().unwrap().push(row);
            self.enqueued.fetch_add(1, Ordering::Relaxed);
        }
        async fn flush(&self) -> anyhow::Result<usize> {
            Ok(self.rows.lock().unwrap().len())
        }
        fn stats(&self) -> SinkStats {
            SinkStats {
                enqueued: self.enqueued.load(Ordering::Relaxed),
                flushed: 0,
                dropped: 0,
                flush_errors: 0,
                queue_depth: 0,
            }
        }
    }

    fn sample_row() -> ErrorEventRow {
        ErrorEventRow {
            event_id: uuid::Uuid::new_v4().to_string(),
            org_id: "org_x".into(),
            project_id: "proj_y".into(),
            service: "api".into(),
            ts: Utc::now(),
            fingerprint: "abcd1234abcd1234".into(),
            error_type: "TypeError".into(),
            message: "boom".into(),
            release: "1.0.0".into(),
            environment: "production".into(),
            stack_raw: "Error: boom\n  at f (a.js:1:2)".into(),
            frames_function: vec!["f".into()],
            frames_file: vec!["a.js".into()],
            frames_line: vec![1],
            frames_column: vec![2],
            frames_resolved: 0,
            frames_context: vec![],
            breadcrumbs: String::new(),
            session_id: "sess1".into(),
            user_id: String::new(),
            url: "https://app/".into(),
            user_agent: "test".into(),
            tags: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn recording_sink_records_enqueues() {
        let sink = RecordingSink::default();
        sink.enqueue(sample_row());
        sink.enqueue(sample_row());
        assert_eq!(sink.snapshot().len(), 2);
        assert_eq!(sink.stats().enqueued, 2);
    }

    #[test]
    fn split_sql_one_statement() {
        let stmts = split_sql_statements("CREATE TABLE a (x Int)");
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("TABLE a"));
    }

    #[test]
    fn split_sql_multiple_statements() {
        let s = "CREATE TABLE a (x Int);\nCREATE TABLE b (y String);";
        let stmts = split_sql_statements(s);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("TABLE a"));
        assert!(stmts[1].contains("TABLE b"));
    }

    #[test]
    fn split_sql_ignores_semicolons_in_strings() {
        let s = "INSERT INTO t VALUES ('a;b');CREATE TABLE c (x Int);";
        let stmts = split_sql_statements(s);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn split_sql_handles_trailing_whitespace() {
        let s = "CREATE TABLE a (x Int);\n   \n";
        let stmts = split_sql_statements(s);
        // Empty trailing segment is included; the runner trims and skips it.
        let non_empty: Vec<_> = stmts.iter().filter(|s| !s.trim().is_empty()).collect();
        assert_eq!(non_empty.len(), 1);
    }

    #[test]
    fn error_event_row_roundtrips_through_json() {
        let row = sample_row();
        let json = serde_json::to_string(&row).unwrap();
        let back: ErrorEventRow = serde_json::from_str(&json).unwrap();
        assert_eq!(back.fingerprint, row.fingerprint);
        assert_eq!(back.release, row.release);
        assert_eq!(back.frames_line, row.frames_line);
    }

    #[test]
    fn json_each_row_format_is_one_line_per_row() {
        let rows = vec![sample_row(), sample_row()];
        let mut body = String::new();
        for r in &rows {
            body.push_str(&serde_json::to_string(r).unwrap());
            body.push('\n');
        }
        assert_eq!(body.matches('\n').count(), 2);
        // Each line must parse independently — ClickHouse parses JSONEachRow line-by-line.
        for line in body.lines() {
            let _: ErrorEventRow = serde_json::from_str(line).expect("each line must parse");
        }
    }

    #[test]
    fn migration_ddl_includes_expected_tables() {
        let ddl = include_str!("clickhouse_schema/001_error_events.sql");
        assert!(ddl.contains("fluxx.error_events"));
        assert!(ddl.contains("fluxx.error_groups"));
        assert!(ddl.contains("fluxx.error_group_by_release"));
        assert!(ddl.contains("MATERIALIZED VIEW"));
    }

    #[test]
    fn config_defaults_are_reasonable() {
        let c = ClickHouseConfig::default();
        assert_eq!(c.port, 8123);
        assert_eq!(c.database, "fluxx");
        assert_eq!(c.flush_batch_size, 1000);
        assert_eq!(c.queue_capacity, 100_000);
        assert!(c.flush_interval.as_secs() <= 10);
    }

    #[test]
    fn fingerprint_validator_accepts_16_hex_chars() {
        assert!(is_valid_fingerprint("abcd1234abcd1234"));
        assert!(is_valid_fingerprint("0000000000000000"));
        assert!(is_valid_fingerprint("FFFFFFFFFFFFFFFF"));
    }

    #[test]
    fn fingerprint_validator_rejects_invalid_input() {
        assert!(!is_valid_fingerprint(""));
        assert!(!is_valid_fingerprint("short"));
        assert!(!is_valid_fingerprint("toolongtoolongtoolong"));
        // SQL injection attempt:
        assert!(!is_valid_fingerprint("'; DROP TABLE--"));
        assert!(!is_valid_fingerprint("abcd1234abcd123g")); // 'g' is not hex
        assert!(!is_valid_fingerprint("abcd1234abcd123 ")); // trailing space
    }

    #[test]
    fn read_store_stats_default_is_zero() {
        let s = ReadStoreStats::default();
        assert_eq!(s.total_groups, 0);
        assert_eq!(s.total_occurrences, 0);
    }

    /// Mock for downstream tests (in error_tracking) — records calls
    /// and returns canned responses. Lives here so `pub(crate)`-visible
    /// across the crate without needing a separate test_support module.
    #[derive(Debug, Default)]
    pub(crate) struct RecordingReadStore {
        pub groups: parking_lot::Mutex<Vec<GroupSnapshot>>,
        pub trend_data: parking_lot::Mutex<[u64; 24]>,
        pub stats_data: parking_lot::Mutex<ReadStoreStats>,
    }

    impl RecordingReadStore {
        pub fn with_groups(snapshots: Vec<GroupSnapshot>) -> Self {
            Self {
                groups: parking_lot::Mutex::new(snapshots),
                trend_data: parking_lot::Mutex::new([0u64; 24]),
                stats_data: parking_lot::Mutex::new(ReadStoreStats::default()),
            }
        }
    }

    #[async_trait]
    impl ErrorReadStore for RecordingReadStore {
        async fn list_groups(&self, _h: u32) -> anyhow::Result<Vec<GroupSnapshot>> {
            Ok(self.groups.lock().clone())
        }
        async fn get_group(
            &self,
            fingerprint: &str,
            _h: u32,
        ) -> anyhow::Result<Option<GroupSnapshot>> {
            Ok(self
                .groups
                .lock()
                .iter()
                .find(|g| g.fingerprint == fingerprint)
                .cloned())
        }
        async fn top_groups(&self, n: usize, _h: u32) -> anyhow::Result<Vec<GroupSnapshot>> {
            let mut g = self.groups.lock().clone();
            g.sort_by(|a, b| b.count.cmp(&a.count));
            g.truncate(n);
            Ok(g)
        }
        async fn trend(&self, _h: u32) -> anyhow::Result<[u64; 24]> {
            Ok(*self.trend_data.lock())
        }
        async fn stats(&self) -> anyhow::Result<ReadStoreStats> {
            Ok(self.stats_data.lock().clone())
        }
        async fn latest_event(
            &self,
            _fp: &str,
            _h: u32,
        ) -> anyhow::Result<Option<LatestEvent>> {
            // Recording store has no per-event detail; tests that need
            // it construct a separate store fixture.
            Ok(None)
        }
        async fn recent_events(
            &self,
            _fp: &str,
            _h: u32,
            _limit: u32,
        ) -> anyhow::Result<Vec<LatestEvent>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn recording_read_store_returns_canned_snapshots() {
        let snaps = vec![
            GroupSnapshot {
                fingerprint: "aaaaaaaaaaaaaaaa".into(),
                error_type: "A".into(),
                message: "boom".into(),
                first_seen_us: 1,
                last_seen_us: 2,
                count: 5,
                services: vec!["api".into()],
                first_release: String::new(),
                last_release: String::new(),
            },
            GroupSnapshot {
                fingerprint: "bbbbbbbbbbbbbbbb".into(),
                error_type: "B".into(),
                message: "crash".into(),
                first_seen_us: 1,
                last_seen_us: 2,
                count: 12,
                services: vec!["worker".into()],
                first_release: String::new(),
                last_release: String::new(),
            },
        ];
        let store = RecordingReadStore::with_groups(snaps);

        let all = store.list_groups(24).await.unwrap();
        assert_eq!(all.len(), 2);

        let one = store
            .get_group("aaaaaaaaaaaaaaaa", 24)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(one.error_type, "A");

        let none = store.get_group("zzzzzzzzzzzzzzzz", 24).await.unwrap();
        assert!(none.is_none());

        let top = store.top_groups(1, 24).await.unwrap();
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].fingerprint, "bbbbbbbbbbbbbbbb"); // 12 > 5
    }

    #[tokio::test]
    async fn sink_drops_when_drain_exited() {
        // We can't easily exercise the real sink without a CH server,
        // but we can prove the trait-level contract: enqueueing into
        // a sink whose channel is closed simply bumps `dropped`.
        let (tx, rx) = mpsc::channel::<ErrorEventRow>(1);
        drop(rx); // close the consumer side immediately
        let (flush_tx, _flush_rx) = mpsc::channel(1);
        let sink = ClickHouseErrorEventSink {
            tx,
            flush_tx,
            enqueued: Arc::new(AtomicU64::new(0)),
            flushed: Arc::new(AtomicU64::new(0)),
            dropped: Arc::new(AtomicU64::new(0)),
            flush_errors: Arc::new(AtomicU64::new(0)),
            queue_capacity: 1,
        };
        sink.enqueue(sample_row());
        assert_eq!(sink.stats().dropped, 1);
        assert_eq!(sink.stats().enqueued, 0);
    }
}
