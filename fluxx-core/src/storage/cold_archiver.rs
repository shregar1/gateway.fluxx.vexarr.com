//! Cold-tier archiver.
//!
//! Periodically exports DuckDB rows older than a threshold into
//! Parquet files (local FS or S3). Optionally deletes the archived
//! rows from the hot DuckDB after a successful COPY so the hot tier
//! stays small.
//!
//! Design: one background tokio task per fluxx process, ticking on
//! a configurable interval. Each tick:
//!   1. Picks `now - older_than_secs` as the cutoff
//!   2. For each signal (logs/events/metrics): runs DuckDB
//!      `COPY (SELECT * FROM <table> WHERE timestamp_us < cutoff)
//!       TO '<dir>/<signal>-<bucket>.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)`
//!   3. If `delete_after_archive`, runs `DELETE FROM <table> WHERE timestamp_us < cutoff`
//!
//! S3 destinations (`s3://bucket/prefix`) work when DuckDB's
//! `httpfs` extension is available and credentials are set via
//! `s3_region` / `s3_access_key_id` / `s3_secret_access_key` env
//! vars. We attempt `INSTALL httpfs; LOAD httpfs` on archiver
//! start; if it fails we log a warning and S3 paths will error
//! at COPY time.
//!
//! Gated on DuckDB being the active persistent backend — without
//! DuckDB there's no SQL table to COPY from. The in-memory store
//! has no cold-tier path (yet); users who want cold archive
//! should enable DuckDB.

use crate::storage::{DuckDbEventStore, DuckDbLogStore, DuckDbMetricStore};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct ColdArchiverOptions {
    /// Output base. Local FS path (`/var/lib/fluxx/cold`) or `s3://`
    /// URL prefix. Files are named `<signal>-<bucket_us>.parquet`
    /// where `bucket_us` is the cutoff timestamp.
    pub output_base: String,
    /// How often the archiver fires.
    pub interval: Duration,
    /// Per-signal retention. Each signal has its own cutoff
    /// (`now - older_than_*`) so logs and metrics can roll to
    /// cold on different schedules.
    pub older_than_logs: Duration,
    pub older_than_traces: Duration,
    pub older_than_metrics: Duration,
    /// If true, archived rows are DELETEd from DuckDB after the
    /// COPY succeeds. If false, archives accumulate but the hot
    /// DuckDB keeps all data.
    pub delete_after_archive: bool,
}

pub struct ColdArchiver {
    _shutdown_tx: oneshot::Sender<()>,
}

impl ColdArchiver {
    /// Start the background archiver task. Returns a handle that
    /// shuts the task down when dropped.
    pub fn start(
        opts: ColdArchiverOptions,
        logs: Arc<DuckDbLogStore>,
        events: Arc<DuckDbEventStore>,
        metrics: Arc<DuckDbMetricStore>,
    ) -> Self {
        info!(
            "ColdArchiver: starting (output_base={}, interval={:?}, older_than[logs={:?} traces={:?} metrics={:?}], delete={})",
            opts.output_base,
            opts.interval,
            opts.older_than_logs,
            opts.older_than_traces,
            opts.older_than_metrics,
            opts.delete_after_archive,
        );

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(opts.interval);
            // Skip the immediate tick — wait the full interval
            // before the first archive sweep.
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = run_sweep(&opts, &logs, &events, &metrics).await {
                            warn!("ColdArchiver: sweep failed: {e}");
                        }
                    }
                    _ = &mut shutdown_rx => {
                        info!("ColdArchiver: shutdown signal received, exiting");
                        break;
                    }
                }
            }
        });

        Self { _shutdown_tx: shutdown_tx }
    }
}

async fn run_sweep(
    opts: &ColdArchiverOptions,
    logs: &DuckDbLogStore,
    events: &DuckDbEventStore,
    metrics: &DuckDbMetricStore,
) -> Result<(), String> {
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    let cutoff_logs = now_us.saturating_sub(opts.older_than_logs.as_micros() as u64);
    let cutoff_traces = now_us.saturating_sub(opts.older_than_traces.as_micros() as u64);
    let cutoff_metrics = now_us.saturating_sub(opts.older_than_metrics.as_micros() as u64);

    let base = opts.output_base.trim_end_matches('/');
    let logs_path = format!("{base}/logs-{cutoff_logs}.parquet");
    let events_path = format!("{base}/events-{cutoff_traces}.parquet");
    let metrics_path = format!("{base}/metrics-{cutoff_metrics}.parquet");

    // Run all three concurrently — they share DuckDB's process but
    // hit different files and different store mutexes.
    let (l, e, m) = tokio::join!(
        logs.archive_to_parquet(cutoff_logs, logs_path.clone(), opts.delete_after_archive),
        events.archive_to_parquet(cutoff_traces, events_path.clone(), opts.delete_after_archive),
        metrics.archive_to_parquet(cutoff_metrics, metrics_path.clone(), opts.delete_after_archive),
    );

    let n_logs = l.map_err(|e| format!("logs: {e}"))?;
    let n_events = e.map_err(|e| format!("events: {e}"))?;
    let n_metrics = m.map_err(|e| format!("metrics: {e}"))?;

    info!(
        "ColdArchiver: sweep done logs={n_logs}@{cutoff_logs} events={n_events}@{cutoff_traces} metrics={n_metrics}@{cutoff_metrics}",
    );
    Ok(())
}
