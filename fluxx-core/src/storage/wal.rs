//! Write-Ahead Log (WAL) Persistent Event Store
//!
//! Persists events to disk using append-only WAL files with zstd compression.
//! Survives restarts. Uses in-memory BTreeMap index for fast queries,
//! rebuilt from WAL on startup.

use crate::models::event::{Event, ProtocolType, EventSeverity};
use crate::storage::event_store::{EventStore, TimeWindow};
use crate::core::error::Result;
use crate::perf::compression;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write as IoWrite};
use std::path::{Path, PathBuf};
use parking_lot::RwLock;
use tracing::{info, warn};

/// WAL-backed persistent event store.
#[derive(Debug)]
pub struct WalEventStore {
    /// In-memory index for fast queries (rebuilt from WAL on start)
    index: RwLock<BTreeMap<u64, Vec<Event>>>,
    /// WAL directory
    wal_dir: PathBuf,
    /// Current WAL file writer
    writer: RwLock<Option<std::io::BufWriter<std::fs::File>>>,
    /// Max events per WAL segment before rotation
    segment_max_events: usize,
    /// Current segment event count
    segment_count: RwLock<usize>,
    /// Total event count
    total_count: RwLock<usize>,
    /// Max total events (TTL eviction)
    max_events: usize,
}

impl WalEventStore {
    /// Open or create a WAL-backed store at the given directory.
    pub fn open(wal_dir: &str, max_events: usize) -> std::io::Result<Self> {
        let path = PathBuf::from(wal_dir);
        fs::create_dir_all(&path)?;

        let store = Self {
            index: RwLock::new(BTreeMap::new()),
            wal_dir: path.clone(),
            writer: RwLock::new(None),
            segment_max_events: 50_000,
            segment_count: RwLock::new(0),
            total_count: RwLock::new(0),
            max_events,
        };

        // Replay existing WAL segments
        store.replay_wal()?;

        // Open current segment for writing
        store.rotate_segment()?;

        info!("WAL store opened at {:?} ({} events recovered)", path, *store.total_count.read());
        Ok(store)
    }

    /// Replay all WAL segment files to rebuild the in-memory index.
    fn replay_wal(&self) -> std::io::Result<()> {
        let mut segments: Vec<PathBuf> = fs::read_dir(&self.wal_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |ext| ext == "wal"))
            .collect();
        segments.sort();

        let mut total = 0usize;
        for seg_path in &segments {
            match self.replay_segment(seg_path) {
                Ok(count) => {
                    total += count;
                }
                Err(e) => {
                    warn!("WAL: Failed to replay {:?}: {}", seg_path, e);
                }
            }
        }

        *self.total_count.write() = total;
        Ok(())
    }

    /// Replay a single WAL segment file.
    fn replay_segment(&self, path: &Path) -> std::io::Result<usize> {
        let file = fs::File::open(path)?;
        let reader = BufReader::new(file);
        let mut count = 0;

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() { continue; }

            // Each line is a compressed+base64 encoded event, or plain JSON
            let event: Event = if line.starts_with('{') {
                // Plain JSON (for human-readable segments)
                match serde_json::from_str(&line) {
                    Ok(e) => e,
                    Err(_) => continue,
                }
            } else {
                // Base64-encoded compressed event
                match decode_event(&line) {
                    Ok(e) => e,
                    Err(_) => continue,
                }
            };

            let mut idx = self.index.write();
            idx.entry(event.timestamp_us).or_default().push(event);
            count += 1;
        }

        Ok(count)
    }

    /// Rotate to a new WAL segment file.
    fn rotate_segment(&self) -> std::io::Result<()> {
        // Flush current writer
        if let Some(ref mut w) = *self.writer.write() {
            w.flush()?;
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let seg_name = format!("{:016}.wal", timestamp);
        let seg_path = self.wal_dir.join(seg_name);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&seg_path)?;

        *self.writer.write() = Some(std::io::BufWriter::new(file));
        *self.segment_count.write() = 0;

        Ok(())
    }

    /// Write an event to the WAL.
    fn write_event(&self, event: &Event) -> std::io::Result<()> {
        let encoded = encode_event(event)?;
        let mut writer_guard = self.writer.write();
        if let Some(ref mut w) = *writer_guard {
            w.write_all(encoded.as_bytes())?;
            w.write_all(b"\n")?;
        }

        let mut sc = self.segment_count.write();
        *sc += 1;

        // Rotate if segment is full
        if *sc >= self.segment_max_events {
            drop(writer_guard);
            self.rotate_segment()?;
        }

        Ok(())
    }

    /// Evict oldest events if over max capacity.
    fn evict_if_needed(&self) {
        let total = *self.total_count.read();
        if total <= self.max_events {
            return;
        }

        let excess = total - self.max_events;
        let mut idx = self.index.write();
        let mut removed = 0;

        let keys: Vec<u64> = idx.keys().copied().collect();
        for key in keys {
            if removed >= excess { break; }
            if let Some(events) = idx.remove(&key) {
                removed += events.len();
            }
        }

        *self.total_count.write() = total - removed;
    }

    /// Flush the WAL to disk.
    pub fn sync(&self) -> std::io::Result<()> {
        if let Some(ref mut w) = *self.writer.write() {
            w.flush()?;
        }
        Ok(())
    }
}

#[async_trait]
impl EventStore for WalEventStore {
    async fn insert(&self, event: Event) -> Result<()> {
        // Write to WAL first (durability). Propagate failures so callers
        // (and operators) know the event was NOT persisted to disk.
        if let Err(e) = self.write_event(&event) {
            warn!(event_id = %event.id, error = %e, "WAL write failed — event not persisted to disk");
            return Err(e.into());
        }

        // Update in-memory index
        let mut idx = self.index.write();
        idx.entry(event.timestamp_us).or_default().push(event);
        drop(idx);

        let mut tc = self.total_count.write();
        *tc += 1;
        drop(tc);

        self.evict_if_needed();
        Ok(())
    }

    async fn query(&self, window: TimeWindow) -> Result<Vec<Event>> {
        let idx = self.index.read();
        let events = idx.range(window.start_us..=window.end_us)
            .flat_map(|(_, evs)| evs.clone())
            .collect();
        Ok(events)
    }

    async fn len(&self) -> Result<usize> {
        Ok(*self.total_count.read())
    }

    async fn list_events(
        &self,
        project_id: Option<String>,
        protocol: Option<ProtocolType>,
        severity: Option<EventSeverity>,
        service_id: Option<String>,
    ) -> Vec<Event> {
        let idx = self.index.read();
        idx.values()
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
        let idx = self.index.read();
        // BTreeMap range scan: only visit keys > since_us.
        idx.range((std::ops::Bound::Excluded(since_us), std::ops::Bound::Unbounded))
            .flat_map(|(_, evs)| evs.iter().cloned())
            .collect()
    }

    async fn get_stats(&self) -> serde_json::Value {
        let idx = self.index.read();
        let total: usize = idx.values().map(|v| v.len()).sum();
        let mut protocol_counts = std::collections::HashMap::new();
        for evs in idx.values() {
            for ev in evs {
                *protocol_counts.entry(format!("{:?}", ev.protocol)).or_insert(0) += 1;
            }
        }

        let wal_files = fs::read_dir(&self.wal_dir)
            .map(|entries| entries.filter_map(|e| e.ok()).count())
            .unwrap_or(0);

        serde_json::json!({
            "total_events": total,
            "unique_time_buckets": idx.len(),
            "protocols": protocol_counts,
            "wal_segments": wal_files,
            "storage": "wal",
            "persistent": true,
        })
    }
}

/// Encode an event as compressed base64 for WAL storage.
fn encode_event(event: &Event) -> std::io::Result<String> {
    let json = serde_json::to_vec(event)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let compressed = compression::fast_compress(&json)?;
    Ok(base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &compressed))
}

/// Decode a compressed base64 event from WAL storage.
fn decode_event(encoded: &str) -> std::io::Result<Event> {
    let compressed = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let json = compression::fast_decompress(&compressed, 1024 * 1024)?;
    serde_json::from_slice(&json)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_insert_and_query() {
        let dir = tempdir().unwrap();
        let store = WalEventStore::open(dir.path().to_str().unwrap(), 10000).unwrap();

        let event = Event::new("test-1".to_string(), ProtocolType::Http);
        let ts = event.timestamp_us;
        store.insert(event).await.unwrap();

        let results = store.query(TimeWindow::new(ts - 1, ts + 1)).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_wal_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        // Write events
        {
            let store = WalEventStore::open(&path, 10000).unwrap();
            for i in 0..5 {
                let mut e = Event::new(format!("e{}", i), ProtocolType::Http);
                e.timestamp_us = 1000 + i;
                store.insert(e).await.unwrap();
            }
            store.sync().unwrap();
        }

        // Reopen and verify
        {
            let store = WalEventStore::open(&path, 10000).unwrap();
            let len = store.len().await.unwrap();
            assert_eq!(len, 5, "Should recover 5 events from WAL");
        }
    }

    #[tokio::test]
    async fn test_wal_eviction() {
        let dir = tempdir().unwrap();
        let store = WalEventStore::open(dir.path().to_str().unwrap(), 10).unwrap();

        for i in 0..20 {
            let mut e = Event::new(format!("e{}", i), ProtocolType::Http);
            e.timestamp_us = i;
            store.insert(e).await.unwrap();
        }

        let len = store.len().await.unwrap();
        assert!(len <= 11, "Should evict old events, got {}", len);
    }
}
