//! File-backed Log Store
//!
//! Stores log entries as JSON Lines (one JSON object per line) in append-only files.
//! Supports rotation by size and time-based retention.
//! Queries scan files and filter in-memory for flexibility.

use crate::models::log_entry::LogEntry;
use crate::storage::log_store::{LogStore, LogQuery};
use crate::core::error::Result;
use async_trait::async_trait;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write as IoWrite};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tracing::{info, error};

/// File-backed log store configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileLogConfig {
    /// Directory for log files.
    pub directory: String,
    /// Max file size in bytes before rotation.
    pub max_file_bytes: u64,
    /// Max number of rotated files to keep.
    pub max_files: usize,
    /// File name prefix.
    pub prefix: String,
}

impl Default for FileLogConfig {
    fn default() -> Self {
        Self {
            directory: "/var/lib/fluxx/logs".to_string(),
            max_file_bytes: 100 * 1024 * 1024, // 100 MB
            max_files: 10,
            prefix: "fluxx-logs".to_string(),
        }
    }
}

/// File-backed log store using JSON Lines format.
#[derive(Debug)]
pub struct FileLogStore {
    config: FileLogConfig,
    writer: RwLock<Option<std::io::BufWriter<std::fs::File>>>,
    current_file: RwLock<PathBuf>,
    current_size: RwLock<u64>,
    log_bus: broadcast::Sender<Arc<LogEntry>>,
    entry_count: RwLock<usize>,
}

impl FileLogStore {
    pub fn new(config: FileLogConfig) -> std::io::Result<Self> {
        fs::create_dir_all(&config.directory)?;

        let (tx, _) = broadcast::channel(4096);
        let file_path = Self::current_file_path(&config);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        let size = file.metadata()?.len();

        info!("FileLogStore: Opened {:?} ({} bytes)", file_path, size);

        Ok(Self {
            config,
            writer: RwLock::new(Some(std::io::BufWriter::new(file))),
            current_file: RwLock::new(file_path),
            current_size: RwLock::new(size),
            log_bus: tx,
            entry_count: RwLock::new(0),
        })
    }

    fn current_file_path(config: &FileLogConfig) -> PathBuf {
        PathBuf::from(&config.directory).join(format!("{}.jsonl", config.prefix))
    }

    /// Rotate the current log file.
    async fn rotate(&self) -> std::io::Result<()> {
        // Flush current writer
        {
            let mut writer = self.writer.write().await;
            if let Some(ref mut w) = *writer {
                w.flush()?;
            }
            *writer = None;
        }

        let dir = PathBuf::from(&self.config.directory);
        let current = self.current_file.read().await.clone();

        // Rename current → timestamped archive
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let archive_name = format!("{}.{}.jsonl", self.config.prefix, ts);
        let archive_path = dir.join(&archive_name);
        fs::rename(&current, &archive_path)?;

        // Prune old files
        self.prune_old_files().await?;

        // Open new file
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current)?;

        let mut writer = self.writer.write().await;
        *writer = Some(std::io::BufWriter::new(new_file));
        let mut size = self.current_size.write().await;
        *size = 0;

        info!("FileLogStore: Rotated to {:?}", archive_path);
        Ok(())
    }

    async fn prune_old_files(&self) -> std::io::Result<()> {
        let dir = PathBuf::from(&self.config.directory);
        let mut archives: Vec<(PathBuf, std::time::SystemTime)> = fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.starts_with(&self.config.prefix) && name.contains('.') && name.ends_with(".jsonl")
                    && name != format!("{}.jsonl", self.config.prefix)
            })
            .filter_map(|e| {
                let modified = e.metadata().ok()?.modified().ok()?;
                Some((e.path(), modified))
            })
            .collect();

        archives.sort_by(|a, b| b.1.cmp(&a.1)); // newest first

        if archives.len() > self.config.max_files {
            for (path, _) in archives.drain(self.config.max_files..) {
                fs::remove_file(&path)?;
                info!("FileLogStore: Pruned {:?}", path);
            }
        }

        Ok(())
    }

    /// Read log entries from all files matching a query.
    async fn scan_files(&self, params: &LogQuery) -> Vec<LogEntry> {
        let dir = PathBuf::from(&self.config.directory);
        let mut results = Vec::new();

        // Scan current file + archived files
        let mut files: Vec<PathBuf> = fs::read_dir(&dir)
            .ok()
            .map(|entries| {
                entries.filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".jsonl"))
                    .map(|e| e.path())
                    .collect()
            })
            .unwrap_or_default();

        files.sort(); // oldest first

        for file_path in files {
            if let Ok(file) = fs::File::open(&file_path) {
                let reader = BufReader::new(file);
                for line in reader.lines() {
                    let line = match line {
                        Ok(l) => l,
                        Err(_) => continue,
                    };
                    if line.is_empty() { continue; }

                    let entry: LogEntry = match serde_json::from_str(&line) {
                        Ok(e) => e,
                        Err(_) => continue,
                    };

                    if !matches_query(&entry, params) {
                        continue;
                    }

                    results.push(entry);
                    if results.len() >= params.limit.max(100) {
                        return results;
                    }
                }
            }
        }

        results
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<LogEntry>> {
        self.log_bus.subscribe()
    }

    pub fn sender(&self) -> broadcast::Sender<Arc<LogEntry>> {
        self.log_bus.clone()
    }
}

#[async_trait]
impl LogStore for FileLogStore {
    async fn insert(&self, entry: LogEntry) -> Result<()> {
        // Broadcast for live tail
        let entry_arc = Arc::new(entry.clone());
        let _ = self.log_bus.send(entry_arc);

        // Serialize to JSON line
        let line = serde_json::to_string(&entry)
            .map_err(|e| crate::core::error::Error::Internal(e.to_string()))?;
        let line_bytes = line.len() as u64 + 1; // +1 for newline

        // Write to file
        {
            let mut writer_guard = self.writer.write().await;
            if let Some(ref mut w) = *writer_guard {
                w.write_all(line.as_bytes())
                    .map_err(|e| crate::core::error::Error::Internal(e.to_string()))?;
                w.write_all(b"\n")
                    .map_err(|e| crate::core::error::Error::Internal(e.to_string()))?;
            }
        }

        // Track size
        let mut size = self.current_size.write().await;
        *size += line_bytes;
        let should_rotate = *size >= self.config.max_file_bytes;
        drop(size);

        let mut count = self.entry_count.write().await;
        *count += 1;

        // Rotate if needed
        if should_rotate {
            drop(count);
            if let Err(e) = self.rotate().await {
                error!("FileLogStore: Rotation failed: {}", e);
            }
        }

        Ok(())
    }

    async fn query(&self, params: LogQuery) -> Result<Vec<LogEntry>> {
        // Flush writer to ensure all data is on disk before scanning
        {
            let mut writer_guard = self.writer.write().await;
            if let Some(ref mut w) = *writer_guard {
                let _ = w.flush();
            }
        }
        Ok(self.scan_files(&params).await)
    }

    async fn len(&self) -> Result<usize> {
        Ok(*self.entry_count.read().await)
    }
}

/// Check if a log entry matches a query's filters.
fn matches_query(entry: &LogEntry, params: &LogQuery) -> bool {
    if let Some(ref svc) = params.service {
        if entry.service.as_ref() != Some(svc) { return false; }
    }
    if let Some(ref level) = params.level {
        if entry.level != *level { return false; }
    }
    if let Some(ref tid) = params.trace_id {
        if entry.trace_id.as_ref() != Some(tid) { return false; }
    }
    if let Some(ref q) = params.q {
        if !entry.message.to_lowercase().contains(&q.to_lowercase()) { return false; }
    }
    if let Some(from) = params.from_us {
        if entry.timestamp_us < from { return false; }
    }
    if let Some(to) = params.to_us {
        if entry.timestamp_us > to { return false; }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::log_entry::LogLevel;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_file_log_insert_and_query() {
        let dir = tempdir().unwrap();
        let config = FileLogConfig {
            directory: dir.path().to_str().unwrap().to_string(),
            max_file_bytes: 1024 * 1024,
            max_files: 5,
            prefix: "test".to_string(),
        };
        let store = FileLogStore::new(config).unwrap();

        let entry = LogEntry::new(LogLevel::Info, "hello world".to_string());
        store.insert(entry).await.unwrap();

        let results = store.query(LogQuery::default()).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message, "hello world");
    }

    #[tokio::test]
    async fn test_file_log_rotation() {
        let dir = tempdir().unwrap();
        let config = FileLogConfig {
            directory: dir.path().to_str().unwrap().to_string(),
            max_file_bytes: 100, // Small file size to trigger rotation
            max_files: 3,
            prefix: "rotate-test".to_string(),
        };
        let store = FileLogStore::new(config).unwrap();

        // Write enough entries to trigger rotation
        for i in 0..20 {
            let entry = LogEntry::new(LogLevel::Info, format!("log message {}", i));
            store.insert(entry).await.unwrap();
        }

        // Check that files were created
        let files: Vec<_> = fs::read_dir(dir.path()).unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".jsonl"))
            .collect();
        assert!(files.len() > 1, "Should have rotated files, got {}", files.len());
    }

    #[tokio::test]
    async fn test_file_log_query_by_level() {
        let dir = tempdir().unwrap();
        let config = FileLogConfig {
            directory: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };
        let store = FileLogStore::new(config).unwrap();

        store.insert(LogEntry::new(LogLevel::Info, "info msg".to_string())).await.unwrap();
        store.insert(LogEntry::new(LogLevel::Error, "error msg".to_string())).await.unwrap();

        let results = store.query(LogQuery {
            level: Some(LogLevel::Error),
            ..Default::default()
        }).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message, "error msg");
    }

    #[tokio::test]
    async fn test_file_log_live_tail() {
        let dir = tempdir().unwrap();
        let config = FileLogConfig {
            directory: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };
        let store = FileLogStore::new(config).unwrap();
        let mut rx = store.subscribe();

        store.insert(LogEntry::new(LogLevel::Info, "live".to_string())).await.unwrap();

        let received = rx.try_recv().unwrap();
        assert_eq!(received.message, "live");
    }
}
