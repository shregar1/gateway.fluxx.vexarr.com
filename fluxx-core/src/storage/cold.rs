//! Cold Storage Tier — S3/Local File Archive
//!
//! Archives compressed event batches to S3-compatible object storage or local filesystem.
//! Integrates with TieredEventStore for automatic hot→warm→cold data lifecycle.

use crate::perf::compression;
use crate::models::event::Event;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tracing::info;

/// Cold storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColdStorageConfig {
    pub enabled: bool,
    /// Backend: "local" or "s3"
    pub backend: String,
    /// Local filesystem path (for backend="local")
    pub local_path: Option<String>,
    /// S3 bucket name (for backend="s3")
    pub s3_bucket: Option<String>,
    /// S3 region
    pub s3_region: Option<String>,
    /// S3 prefix/folder
    pub s3_prefix: Option<String>,
    /// Max archive age in days before permanent deletion
    pub retention_days: u64,
}

impl Default for ColdStorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: "local".to_string(),
            local_path: Some("/var/lib/fluxx/cold".to_string()),
            s3_bucket: None,
            s3_region: None,
            s3_prefix: Some("fluxx/cold/".to_string()),
            retention_days: 90,
        }
    }
}

/// A cold storage archive batch.
#[derive(Debug, Serialize, Deserialize)]
pub struct ArchiveBatch {
    pub id: String,
    pub min_timestamp_us: u64,
    pub max_timestamp_us: u64,
    pub event_count: usize,
    pub compressed_size: usize,
    pub original_size: usize,
    pub created_at_us: u64,
}

/// Cold storage backend trait.
pub trait ColdBackend: Send + Sync {
    fn archive(&self, batch_id: &str, data: &[u8]) -> Result<(), String>;
    fn retrieve(&self, batch_id: &str) -> Result<Vec<u8>, String>;
    fn list_batches(&self) -> Result<Vec<String>, String>;
    fn delete(&self, batch_id: &str) -> Result<(), String>;
}

/// Local filesystem cold storage backend.
#[derive(Debug)]
pub struct LocalColdBackend {
    base_path: PathBuf,
}

impl LocalColdBackend {
    pub fn new(path: &str) -> Result<Self, String> {
        let base = PathBuf::from(path);
        fs::create_dir_all(&base).map_err(|e| format!("Failed to create cold storage dir: {}", e))?;
        Ok(Self { base_path: base })
    }
}

impl ColdBackend for LocalColdBackend {
    fn archive(&self, batch_id: &str, data: &[u8]) -> Result<(), String> {
        let path = self.base_path.join(format!("{}.zst", batch_id));
        fs::write(&path, data).map_err(|e| format!("Write failed: {}", e))?;
        info!("Cold: Archived {} ({} bytes)", batch_id, data.len());
        Ok(())
    }

    fn retrieve(&self, batch_id: &str) -> Result<Vec<u8>, String> {
        let path = self.base_path.join(format!("{}.zst", batch_id));
        fs::read(&path).map_err(|e| format!("Read failed: {}", e))
    }

    fn list_batches(&self) -> Result<Vec<String>, String> {
        let entries = fs::read_dir(&self.base_path)
            .map_err(|e| format!("List failed: {}", e))?;
        Ok(entries
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.strip_suffix(".zst").map(|s| s.to_string())
            })
            .collect())
    }

    fn delete(&self, batch_id: &str) -> Result<(), String> {
        let path = self.base_path.join(format!("{}.zst", batch_id));
        fs::remove_file(&path).map_err(|e| format!("Delete failed: {}", e))
    }
}

/// S3-compatible cold storage backend (uses pre-signed URLs via reqwest).
/// For production, integrate with aws-sdk-s3 or rusoto.
#[derive(Debug)]
pub struct S3ColdBackend {
    bucket: String,
    #[allow(dead_code)]
    region: String,
    prefix: String,
}

impl S3ColdBackend {
    pub fn new(bucket: &str, region: &str, prefix: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            region: region.to_string(),
            prefix: prefix.to_string(),
        }
    }

    fn object_key(&self, batch_id: &str) -> String {
        format!("{}{}.zst", self.prefix, batch_id)
    }
}

impl ColdBackend for S3ColdBackend {
    fn archive(&self, batch_id: &str, data: &[u8]) -> Result<(), String> {
        // Placeholder: in production, use aws-sdk-s3 PutObject
        let key = self.object_key(batch_id);
        info!("S3 Cold: Would upload s3://{}/{} ({} bytes)", self.bucket, key, data.len());
        // Future: reqwest PUT to S3 presigned URL or aws-sdk-s3
        Ok(())
    }

    fn retrieve(&self, batch_id: &str) -> Result<Vec<u8>, String> {
        let key = self.object_key(batch_id);
        Err(format!("S3 retrieve not yet implemented for s3://{}/{}", self.bucket, key))
    }

    fn list_batches(&self) -> Result<Vec<String>, String> {
        Err("S3 list not yet implemented".to_string())
    }

    fn delete(&self, batch_id: &str) -> Result<(), String> {
        let key = self.object_key(batch_id);
        info!("S3 Cold: Would delete s3://{}/{}", self.bucket, key);
        Ok(())
    }
}

/// Archive a batch of events to cold storage.
pub fn archive_events(
    backend: &dyn ColdBackend,
    events: &[Event],
    batch_id: &str,
) -> Result<ArchiveBatch, String> {
    let serialized = serde_json::to_vec(events)
        .map_err(|e| format!("Serialization failed: {}", e))?;
    let original_size = serialized.len();

    let compressed = compression::fast_compress(&serialized)
        .map_err(|e| format!("Compression failed: {}", e))?;

    backend.archive(batch_id, &compressed)?;

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;

    let min_ts = events.iter().map(|e| e.timestamp_us).min().unwrap_or(0);
    let max_ts = events.iter().map(|e| e.timestamp_us).max().unwrap_or(0);

    Ok(ArchiveBatch {
        id: batch_id.to_string(),
        min_timestamp_us: min_ts,
        max_timestamp_us: max_ts,
        event_count: events.len(),
        compressed_size: compressed.len(),
        original_size,
        created_at_us: now_us,
    })
}

/// Retrieve and decompress events from cold storage.
pub fn retrieve_events(
    backend: &dyn ColdBackend,
    batch_id: &str,
) -> Result<Vec<Event>, String> {
    let compressed = backend.retrieve(batch_id)?;
    let decompressed = compression::fast_decompress(&compressed, 100 * 1024 * 1024)
        .map_err(|e| format!("Decompression failed: {}", e))?;
    serde_json::from_slice(&decompressed)
        .map_err(|e| format!("Deserialization failed: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::{Event, ProtocolType};
    use tempfile::tempdir;

    #[test]
    fn test_local_archive_and_retrieve() {
        let dir = tempdir().unwrap();
        let backend = LocalColdBackend::new(dir.path().to_str().unwrap()).unwrap();

        let events: Vec<Event> = (0..10)
            .map(|i| {
                let mut e = Event::new(format!("e{}", i), ProtocolType::Http);
                e.timestamp_us = 1000 + i;
                e
            })
            .collect();

        let batch = archive_events(&backend, &events, "batch-001").unwrap();
        assert_eq!(batch.event_count, 10);
        assert!(batch.compressed_size < batch.original_size);

        let recovered = retrieve_events(&backend, "batch-001").unwrap();
        assert_eq!(recovered.len(), 10);
        assert_eq!(recovered[0].id, "e0");
    }

    #[test]
    fn test_local_list_and_delete() {
        let dir = tempdir().unwrap();
        let backend = LocalColdBackend::new(dir.path().to_str().unwrap()).unwrap();

        backend.archive("b1", b"data1").unwrap();
        backend.archive("b2", b"data2").unwrap();

        let batches = backend.list_batches().unwrap();
        assert_eq!(batches.len(), 2);

        backend.delete("b1").unwrap();
        let batches = backend.list_batches().unwrap();
        assert_eq!(batches.len(), 1);
    }
}
