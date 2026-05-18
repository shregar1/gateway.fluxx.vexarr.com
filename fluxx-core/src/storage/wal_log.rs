//! WAL-backed Persistent Log Store
//!
//! Combines in-memory BTreeMap index for fast queries with WAL persistence
//! for crash safety. Replays WAL segments on startup to rebuild index.

use crate::models::log_entry::LogEntry;
use crate::storage::log_store::{LogStore, LogQuery};
use crate::core::error::Result;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write as IoWrite};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use parking_lot::RwLock;
use tracing::{info, error};

/// WAL-backed log store configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalLogConfig {
    /// WAL directory path.
    pub wal_dir: String,
    /// Max entries in memory before oldest are evicted.
    pub max_entries: usize,
    /// Enable compression for WAL entries.
    pub compress: bool,
}

impl Default for WalLogConfig {
    fn default() -> Self {
        Self {
            wal_dir: "/var/lib/fluxx/log-wal".to_string(),
            max_entries: 500_000,
            compress: false,
        }
    }
}

/// WAL-backed persistent log store.
#[derive(Debug)]
pub struct WalLogStore {
    config: WalLogConfig,
    index: RwLock<BTreeMap<u64, Vec<LogEntry>>>,
    writer: parking_lot::RwLock<Option<std::io::BufWriter<std::fs::File>>>,
    log_bus: broadcast::Sender<Arc<LogEntry>>,
    entry_count: RwLock<usize>,
}

impl WalLogStore {
    pub fn open(config: WalLogConfig) -> std::io::Result<Self> {
        fs::create_dir_all(&config.wal_dir)?;
        let (tx, _) = broadcast::channel(4096);

        let store = Self {
            config: config.clone(),
            index: RwLock::new(BTreeMap::new()),
            writer: parking_lot::RwLock::new(None),
            log_bus: tx,
            entry_count: RwLock::new(0),
        };

        // Replay existing WAL
        let replayed = store.replay_wal_sync()?;

        // Open current WAL for writing
        let wal_path = PathBuf::from(&config.wal_dir).join("current.wal");
        let file = OpenOptions::new().create(true).append(true).open(&wal_path)?;
        *store.writer.write() = Some(std::io::BufWriter::new(file));

        info!("WalLogStore: Opened at {:?} ({} entries recovered)", config.wal_dir, replayed);
        Ok(store)
    }

    fn replay_wal_sync(&self) -> std::io::Result<usize> {
        let dir = PathBuf::from(&self.config.wal_dir);
        let mut total = 0;

        let mut files: Vec<PathBuf> = fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
            .map(|e| e.path())
            .collect();
        files.sort();

        let index = self.index.write();
        let mut index = index;

        for path in files {
            if let Ok(file) = fs::File::open(&path) {
                let reader = BufReader::new(file);
                for line in reader.lines() {
                    let line = match line { Ok(l) => l, Err(_) => continue };
                    if line.is_empty() { continue; }
                    let entry: LogEntry = match serde_json::from_str(&line) {
                        Ok(e) => e,
                        Err(_) => continue,
                    };
                    index.entry(entry.timestamp_us).or_default().push(entry);
                    total += 1;
                }
            }
        }

        drop(index);
        let mut count = self.entry_count.write();
        *count = total;

        Ok(total)
    }

    fn write_entry_sync(&self, entry: &LogEntry) -> std::io::Result<()> {
        let line = serde_json::to_string(entry)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let mut writer = self.writer.write();
        if let Some(ref mut w) = *writer {
            w.write_all(line.as_bytes())?;
            w.write_all(b"\n")?;
        }
        Ok(())
    }

    fn evict_oldest(&self) {
        let count = *self.entry_count.read();
        if count <= self.config.max_entries {
            return;
        }

        let excess = count - self.config.max_entries;
        let mut index = self.index.write();
        let mut removed = 0;
        let keys: Vec<u64> = index.keys().copied().collect();
        for key in keys {
            if removed >= excess { break; }
            if let Some(entries) = index.remove(&key) {
                removed += entries.len();
            }
        }
        drop(index);

        let mut c = self.entry_count.write();
        *c = c.saturating_sub(removed);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<LogEntry>> {
        self.log_bus.subscribe()
    }

    pub fn sender(&self) -> broadcast::Sender<Arc<LogEntry>> {
        self.log_bus.clone()
    }

    pub fn sync(&self) -> std::io::Result<()> {
        let mut writer = self.writer.write();
        if let Some(ref mut w) = *writer {
            w.flush()?;
        }
        Ok(())
    }
}

#[async_trait]
impl LogStore for WalLogStore {
    async fn insert(&self, entry: LogEntry) -> Result<()> {
        // Broadcast for live tail
        let entry_arc = Arc::new(entry.clone());
        let _ = self.log_bus.send(entry_arc);

        // Write to WAL
        if let Err(e) = self.write_entry_sync(&entry) {
            error!("WalLogStore: WAL write failed: {}", e);
        }

        // Update in-memory index
        let mut index = self.index.write();
        index.entry(entry.timestamp_us).or_default().push(entry);
        drop(index);

        let mut count = self.entry_count.write();
        *count += 1;
        drop(count);

        self.evict_oldest();
        Ok(())
    }

    async fn query(&self, params: LogQuery) -> Result<Vec<LogEntry>> {
        let index = self.index.read();
        let limit = if params.limit == 0 { 100 } else { params.limit };
        let from = params.from_us.unwrap_or(0);
        let to = params.to_us.unwrap_or(u64::MAX);

        let mut results = Vec::new();
        let mut skipped = 0;

        for (_, entries) in index.range(from..=to).rev() {
            for entry in entries.iter().rev() {
                if let Some(ref svc) = params.service {
                    if entry.service.as_ref() != Some(svc) { continue; }
                }
                if let Some(ref level) = params.level {
                    if entry.level != *level { continue; }
                }
                if let Some(ref tid) = params.trace_id {
                    if entry.trace_id.as_ref() != Some(tid) { continue; }
                }
                if let Some(ref q) = params.q {
                    if !entry.message.to_lowercase().contains(&q.to_lowercase()) { continue; }
                }
                if skipped < params.offset { skipped += 1; continue; }
                results.push(entry.clone());
                if results.len() >= limit { return Ok(results); }
            }
        }

        Ok(results)
    }

    async fn len(&self) -> Result<usize> {
        Ok(*self.entry_count.read())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::log_entry::LogLevel;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_log_insert_and_query() {
        let dir = tempdir().unwrap();
        let config = WalLogConfig {
            wal_dir: dir.path().to_str().unwrap().to_string(),
            max_entries: 10000,
            compress: false,
        };
        let store = WalLogStore::open(config).unwrap();

        store.insert(LogEntry::new(LogLevel::Info, "hello wal".to_string())).await.unwrap();

        let results = store.query(LogQuery::default()).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message, "hello wal");
    }

    #[tokio::test]
    async fn test_wal_log_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        // Write
        {
            let config = WalLogConfig { wal_dir: path.clone(), max_entries: 10000, compress: false };
            let store = WalLogStore::open(config).unwrap();
            for i in 0..5 {
                store.insert(LogEntry::new(LogLevel::Info, format!("msg {}", i))).await.unwrap();
            }
            store.sync().unwrap();
        }

        // Re-open and verify
        {
            let config = WalLogConfig { wal_dir: path, max_entries: 10000, compress: false };
            let store = WalLogStore::open(config).unwrap();
            let count = store.len().await.unwrap();
            assert_eq!(count, 5, "Should recover 5 entries from WAL");
        }
    }

    #[tokio::test]
    async fn test_wal_log_eviction() {
        let dir = tempdir().unwrap();
        let config = WalLogConfig {
            wal_dir: dir.path().to_str().unwrap().to_string(),
            max_entries: 5,
            compress: false,
        };
        let store = WalLogStore::open(config).unwrap();

        for i in 0..20 {
            let mut entry = LogEntry::new(LogLevel::Info, format!("msg {}", i));
            entry.timestamp_us = i as u64 * 1000;
            store.insert(entry).await.unwrap();
        }

        let len = store.len().await.unwrap();
        assert!(len <= 6, "Should evict old entries, got {}", len);
    }
}
