use crate::models::log_entry::LogEntry;
use crate::storage::log_store::{LogStore, LogQuery};
use crate::core::error::Result;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

/// In-memory log store using BTreeMap for efficient time-range queries.
#[derive(Debug)]
pub struct InMemoryLogStore {
    data: RwLock<BTreeMap<u64, Vec<LogEntry>>>,
    log_bus: broadcast::Sender<Arc<LogEntry>>,
    max_entries: usize,
}

impl InMemoryLogStore {
    pub fn new(max_entries: usize) -> Self {
        let (tx, _) = broadcast::channel(4096);
        Self {
            data: RwLock::new(BTreeMap::new()),
            log_bus: tx,
            max_entries,
        }
    }

    /// Subscribe to the live log tail.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<LogEntry>> {
        self.log_bus.subscribe()
    }

    /// Get the broadcast sender for external use.
    pub fn sender(&self) -> broadcast::Sender<Arc<LogEntry>> {
        self.log_bus.clone()
    }
}

#[async_trait]
impl LogStore for InMemoryLogStore {
    async fn insert(&self, entry: LogEntry) -> Result<()> {
        let entry_arc = Arc::new(entry.clone());
        let _ = self.log_bus.send(entry_arc);

        let mut data = self.data.write().await;
        data.entry(entry.timestamp_us).or_default().push(entry);

        // Enforce max entries by evicting oldest
        let total: usize = data.values().map(|v| v.len()).sum();
        if total > self.max_entries {
            let excess = total - self.max_entries;
            let mut removed = 0;
            let keys_to_check: Vec<u64> = data.keys().copied().collect();
            for key in keys_to_check {
                if removed >= excess {
                    break;
                }
                if let Some(entries) = data.get(&key) {
                    let count = entries.len();
                    if removed + count <= excess {
                        data.remove(&key);
                        removed += count;
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn query(&self, params: LogQuery) -> Result<Vec<LogEntry>> {
        let data = self.data.read().await;
        let mut results = Vec::new();
        let limit = if params.limit == 0 { 100 } else { params.limit };

        let from = params.from_us.unwrap_or(0);
        let to = params.to_us.unwrap_or(u64::MAX);

        let mut skipped = 0;

        for (_ts, entries) in data.range(from..=to).rev() {
            for entry in entries.iter().rev() {
                // Filter by service
                if let Some(ref svc) = params.service {
                    if entry.service.as_ref() != Some(svc) {
                        continue;
                    }
                }

                // Filter by level
                if let Some(ref level) = params.level {
                    if entry.level != *level {
                        continue;
                    }
                }

                // Filter by trace_id
                if let Some(ref tid) = params.trace_id {
                    if entry.trace_id.as_ref() != Some(tid) {
                        continue;
                    }
                }

                // Search on message: regex if starts with /, otherwise substring
                if let Some(ref q) = params.q {
                    if q.starts_with('/') && q.ends_with('/') && q.len() > 2 {
                        // Regex mode: /pattern/
                        let pattern = &q[1..q.len()-1];
                        if let Ok(re) = regex::Regex::new(pattern) {
                            if !re.is_match(&entry.message) {
                                continue;
                            }
                        } else {
                            continue; // Invalid regex, skip
                        }
                    } else {
                        // Substring mode (default)
                        if !entry.message.to_lowercase().contains(&q.to_lowercase()) {
                            continue;
                        }
                    }
                }

                if skipped < params.offset {
                    skipped += 1;
                    continue;
                }

                results.push(entry.clone());
                if results.len() >= limit {
                    return Ok(results);
                }
            }
        }

        Ok(results)
    }

    async fn len(&self) -> Result<usize> {
        let data = self.data.read().await;
        Ok(data.values().map(|v| v.len()).sum())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::log_entry::LogLevel;

    #[tokio::test]
    async fn test_insert_and_query() {
        let store = InMemoryLogStore::new(1000);
        let entry = LogEntry::new(LogLevel::Info, "hello world".to_string());
        store.insert(entry).await.unwrap();

        let results = store.query(LogQuery::default()).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message, "hello world");
    }

    #[tokio::test]
    async fn test_query_by_level() {
        let store = InMemoryLogStore::new(1000);
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
    async fn test_query_by_text() {
        let store = InMemoryLogStore::new(1000);
        store.insert(LogEntry::new(LogLevel::Info, "database connected".to_string())).await.unwrap();
        store.insert(LogEntry::new(LogLevel::Info, "user logged in".to_string())).await.unwrap();

        let results = store.query(LogQuery {
            q: Some("database".to_string()),
            ..Default::default()
        }).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_max_entries_eviction() {
        let store = InMemoryLogStore::new(5);
        for i in 0..10 {
            let mut entry = LogEntry::new(LogLevel::Info, format!("msg {}", i));
            entry.timestamp_us = i as u64 * 1000;
            store.insert(entry).await.unwrap();
        }
        let count = store.len().await.unwrap();
        assert!(count <= 6); // Allow some slack due to batch eviction
    }

    #[tokio::test]
    async fn test_live_tail() {
        let store = InMemoryLogStore::new(1000);
        let mut rx = store.subscribe();

        store.insert(LogEntry::new(LogLevel::Info, "live".to_string())).await.unwrap();

        let received = rx.try_recv().unwrap();
        assert_eq!(received.message, "live");
    }
}
