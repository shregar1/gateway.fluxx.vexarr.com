//! Write-through LogStore: any hot LogStore + any cold LogStore.
//!
//! Writes hit `hot` synchronously (return immediately) and are
//! queued onto a bounded async channel for `cold` persistence.
//! Queries hit `hot` first; if empty, fall through to `cold`.
//!
//! Generic over the underlying store types — pair InMemory with
//! DuckDB, InMemory with ClickHouse, DuckDB with ClickHouse, or
//! any combination. The trait abstraction is the contract.
//!
//! Backpressure: when the channel saturates, new entries to the
//! cold sink are dropped with a warn. The hot copy is preserved
//! for live tail / recent queries — durability for the dropped
//! entries is lost. Tune `channel_capacity` if your write rate
//! sustains above the cold store's per-insert ceiling.

use crate::core::error::Result;
use crate::models::log_entry::LogEntry;
use crate::storage::log_store::{LogQuery, LogStore};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Debug)]
pub struct WriteThroughLogStore {
    hot: Arc<dyn LogStore>,
    cold: Arc<dyn LogStore>,
    cold_tx: mpsc::Sender<LogEntry>,
}

impl WriteThroughLogStore {
    pub fn new(
        hot: Arc<dyn LogStore>,
        cold: Arc<dyn LogStore>,
        channel_capacity: usize,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<LogEntry>(channel_capacity);
        let cold_drain = cold.clone();
        tokio::spawn(async move {
            while let Some(entry) = rx.recv().await {
                if let Err(e) = cold_drain.insert(entry).await {
                    warn!("WriteThroughLogStore: cold insert failed: {e}");
                }
            }
        });
        Self { hot, cold, cold_tx: tx }
    }
}

#[async_trait]
impl LogStore for WriteThroughLogStore {
    async fn insert(&self, entry: LogEntry) -> Result<()> {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.cold_tx.try_send(entry.clone()) {
            warn!("WriteThroughLogStore: cold channel saturated, dropping durable copy");
        }
        self.hot.insert(entry).await
    }

    async fn query(&self, params: LogQuery) -> Result<Vec<LogEntry>> {
        let hot = self.hot.query(params.clone()).await?;
        if !hot.is_empty() {
            return Ok(hot);
        }
        self.cold.query(params).await
    }

    async fn len(&self) -> Result<usize> {
        // Cold is authoritative for total durable count.
        self.cold.len().await
    }
}
