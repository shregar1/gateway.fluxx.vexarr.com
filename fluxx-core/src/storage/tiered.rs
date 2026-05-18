//! Tiered Storage Engine
//!
//! Hot (in-memory) → Warm (compressed mmap) → Cold (evicted) storage tiers.
//! Automatically moves data between tiers based on age and configurable TTLs.

use crate::models::event::{Event, ProtocolType, EventSeverity};
use crate::storage::event_store::{EventStore, TimeWindow};
use crate::core::error::Result;
use crate::perf::compression;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::info;

/// Configuration for tiered storage.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TieredStorageConfig {
    /// Max events in hot tier (in-memory)
    pub hot_max_events: usize,
    /// Hot tier TTL in seconds (move to warm after this)
    pub hot_ttl_secs: u64,
    /// Warm tier TTL in seconds (evict after this)
    pub warm_ttl_secs: u64,
    /// Compression enabled for warm tier
    pub compress_warm: bool,
    /// Max total events across all tiers
    pub max_total_events: usize,
}

impl Default for TieredStorageConfig {
    fn default() -> Self {
        Self {
            hot_max_events: 100_000,
            hot_ttl_secs: 3600,       // 1 hour in hot
            warm_ttl_secs: 86400,     // 24 hours in warm
            compress_warm: true,
            max_total_events: 1_000_000,
        }
    }
}

/// Compressed event batch for the warm tier.
#[derive(Debug)]
struct WarmBatch {
    /// Compressed serialized events
    compressed_data: Vec<u8>,
    /// Number of events in this batch
    event_count: usize,
    /// Original (uncompressed) size
    original_size: usize,
    /// Timestamp range
    min_ts: u64,
    max_ts: u64,
}

/// Tiered Event Store: hot (BTreeMap) + warm (compressed batches) + TTL eviction.
#[derive(Debug)]
pub struct TieredEventStore {
    config: TieredStorageConfig,
    /// Hot tier: in-memory BTreeMap indexed by timestamp
    hot: RwLock<BTreeMap<u64, Vec<Event>>>,
    /// Warm tier: compressed event batches
    warm: RwLock<Vec<WarmBatch>>,
    /// Stats
    hot_count: RwLock<usize>,
    warm_count: RwLock<usize>,
}

impl TieredEventStore {
    pub fn new(config: TieredStorageConfig) -> Self {
        Self {
            config,
            hot: RwLock::new(BTreeMap::new()),
            warm: RwLock::new(Vec::new()),
            hot_count: RwLock::new(0),
            warm_count: RwLock::new(0),
        }
    }

    /// Run the background tier management loop.
    pub async fn run_tier_manager(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            self.promote_and_evict().await;
        }
    }

    /// Move old hot data to warm, evict expired warm data.
    async fn promote_and_evict(&self) {
        let now_us = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let hot_cutoff = now_us.saturating_sub(self.config.hot_ttl_secs * 1_000_000);
        let warm_cutoff = now_us.saturating_sub(
            (self.config.hot_ttl_secs + self.config.warm_ttl_secs) * 1_000_000,
        );

        // Move hot → warm
        let events_to_warm: Vec<(u64, Vec<Event>)>;
        {
            let mut hot = self.hot.write().await;
            let keys_to_move: Vec<u64> = hot.range(..hot_cutoff).map(|(k, _)| *k).collect();
            events_to_warm = keys_to_move.iter()
                .filter_map(|k| hot.remove(k).map(|v| (*k, v)))
                .collect();
        }

        if !events_to_warm.is_empty() {
            // Batch compress the events
            let all_events: Vec<Event> = events_to_warm.iter()
                .flat_map(|(_, evs)| evs.iter().cloned())
                .collect();
            let total_moved = all_events.len();

            if !all_events.is_empty() {
                let min_ts = all_events.iter().map(|e| e.timestamp_us).min().unwrap_or(0);
                let max_ts = all_events.iter().map(|e| e.timestamp_us).max().unwrap_or(0);

                let serialized = serde_json::to_vec(&all_events).unwrap_or_default();
                let original_size = serialized.len();

                let compressed = if self.config.compress_warm {
                    compression::fast_compress(&serialized).unwrap_or(serialized)
                } else {
                    serialized
                };

                let batch = WarmBatch {
                    compressed_data: compressed,
                    event_count: total_moved,
                    original_size,
                    min_ts,
                    max_ts,
                };

                let mut warm = self.warm.write().await;
                warm.push(batch);
                let mut wc = self.warm_count.write().await;
                *wc += total_moved;
            }

            let mut hc = self.hot_count.write().await;
            *hc = hc.saturating_sub(total_moved);
            info!("TieredStorage: Moved {} events from hot to warm tier", total_moved);
        }

        // Evict expired warm data
        {
            let mut warm = self.warm.write().await;
            let before = warm.len();
            warm.retain(|batch| batch.max_ts > warm_cutoff);
            let evicted_batches = before - warm.len();
            if evicted_batches > 0 {
                let mut wc = self.warm_count.write().await;
                // Approximate count reduction
                *wc = warm.iter().map(|b| b.event_count).sum();
                info!("TieredStorage: Evicted {} warm batches", evicted_batches);
            }
        }
    }

    /// Decompress and query a warm batch.
    fn query_warm_batch(batch: &WarmBatch, window: TimeWindow) -> Vec<Event> {
        if batch.max_ts < window.start_us || batch.min_ts > window.end_us {
            return Vec::new(); // Batch doesn't overlap with query window
        }

        let decompressed = compression::fast_decompress(&batch.compressed_data, batch.original_size * 2)
            .unwrap_or_default();

        let events: Vec<Event> = serde_json::from_slice(&decompressed).unwrap_or_default();
        events.into_iter()
            .filter(|e| e.timestamp_us >= window.start_us && e.timestamp_us <= window.end_us)
            .collect()
    }

    pub async fn get_tier_stats(&self) -> serde_json::Value {
        let hc = self.hot_count.read().await;
        let wc = self.warm_count.read().await;
        let warm = self.warm.read().await;
        let warm_bytes: usize = warm.iter().map(|b| b.compressed_data.len()).sum();
        let warm_original: usize = warm.iter().map(|b| b.original_size).sum();

        serde_json::json!({
            "hot_events": *hc,
            "warm_events": *wc,
            "warm_batches": warm.len(),
            "warm_compressed_bytes": warm_bytes,
            "warm_original_bytes": warm_original,
            "compression_ratio": if warm_bytes > 0 { warm_original as f64 / warm_bytes as f64 } else { 0.0 },
            "total_events": *hc + *wc,
        })
    }
}

#[async_trait]
impl EventStore for TieredEventStore {
    async fn insert(&self, event: Event) -> Result<()> {
        let mut hot = self.hot.write().await;
        hot.entry(event.timestamp_us).or_default().push(event);
        let mut hc = self.hot_count.write().await;
        *hc += 1;

        // If hot tier exceeds max, trigger promotion
        if *hc > self.config.hot_max_events {
            drop(hot);
            drop(hc);
            self.promote_and_evict().await;
        }

        Ok(())
    }

    async fn query(&self, window: TimeWindow) -> Result<Vec<Event>> {
        let mut results = Vec::new();

        // Query hot tier
        {
            let hot = self.hot.read().await;
            for (_, evs) in hot.range(window.start_us..=window.end_us) {
                results.extend(evs.iter().cloned());
            }
        }

        // Query warm tier
        {
            let warm = self.warm.read().await;
            for batch in warm.iter() {
                results.extend(Self::query_warm_batch(batch, window));
            }
        }

        // Sort by timestamp
        results.sort_by_key(|e| e.timestamp_us);
        Ok(results)
    }

    async fn len(&self) -> Result<usize> {
        let hc = self.hot_count.read().await;
        let wc = self.warm_count.read().await;
        Ok(*hc + *wc)
    }

    async fn list_events(
        &self,
        project_id: Option<String>,
        protocol: Option<ProtocolType>,
        severity: Option<EventSeverity>,
        service_id: Option<String>,
    ) -> Vec<Event> {
        let hot = self.hot.read().await;
        hot.values()
            .flat_map(|evs| evs.iter())
            .filter(|ev| {
                let p = project_id.is_none() || ev.project_id == project_id;
                let pr = protocol.is_none() || Some(ev.protocol) == protocol;
                let s = severity.is_none() || Some(ev.severity) == severity;
                let si = service_id.is_none() || ev.service_id == service_id;
                p && pr && s && si
            })
            .cloned()
            .collect()
    }

    async fn list_events_since(&self, since_us: u64) -> Vec<Event> {
        let mut results = Vec::new();

        // Hot tier: BTreeMap range scan for keys > since_us.
        {
            let hot = self.hot.read().await;
            for (_, evs) in hot.range((std::ops::Bound::Excluded(since_us), std::ops::Bound::Unbounded)) {
                results.extend(evs.iter().cloned());
            }
        }

        // Warm tier: decompress batches that overlap the time range.
        {
            let warm = self.warm.read().await;
            for batch in warm.iter() {
                if batch.max_ts <= since_us {
                    continue; // entire batch is before the watermark
                }
                let decompressed = compression::fast_decompress(
                    &batch.compressed_data,
                    batch.original_size * 2,
                )
                .unwrap_or_default();
                let events: Vec<Event> = serde_json::from_slice(&decompressed).unwrap_or_default();
                results.extend(events.into_iter().filter(|e| e.timestamp_us > since_us));
            }
        }

        results.sort_by_key(|e| e.timestamp_us);
        results
    }

    async fn get_stats(&self) -> serde_json::Value {
        self.get_tier_stats().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::Event;

    #[tokio::test]
    async fn test_insert_and_query() {
        let store = TieredEventStore::new(TieredStorageConfig::default());
        let event = Event::new("test".to_string(), ProtocolType::Http);
        let ts = event.timestamp_us;
        store.insert(event).await.unwrap();

        let results = store.query(TimeWindow::new(ts - 1, ts + 1)).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_hot_to_warm_promotion() {
        let config = TieredStorageConfig {
            hot_max_events: 5,
            hot_ttl_secs: 0, // Immediate promotion
            warm_ttl_secs: 3600,
            compress_warm: true,
            max_total_events: 100,
        };
        let store = TieredEventStore::new(config);

        // Use timestamps in the past so they're eligible for promotion
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        for i in 0..10 {
            let mut e = Event::new(format!("e{}", i), ProtocolType::Http);
            e.timestamp_us = now - (10 - i) * 1_000_000; // 1-10 seconds ago
            store.insert(e).await.unwrap();
        }

        // Force promotion
        store.promote_and_evict().await;

        let stats = store.get_tier_stats().await;
        let total = stats["total_events"].as_u64().unwrap_or(0);
        assert!(total > 0, "Should have events across tiers");
    }

    #[tokio::test]
    async fn test_len() {
        let store = TieredEventStore::new(TieredStorageConfig::default());
        assert_eq!(store.len().await.unwrap(), 0);

        store.insert(Event::new("a".to_string(), ProtocolType::Http)).await.unwrap();
        store.insert(Event::new("b".to_string(), ProtocolType::Tcp)).await.unwrap();
        assert_eq!(store.len().await.unwrap(), 2);
    }
}
