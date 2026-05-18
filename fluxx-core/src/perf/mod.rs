//! Ultra-high-performance optimizations for fluxx-agent
//! Targets: $1M/year infrastructure cost (100× more efficient than baseline)

pub mod fast_json;
pub mod compression;
pub mod mmap_storage;
pub mod binary_protocol;

use std::sync::atomic::{AtomicU64, Ordering};

/// Cache-line padded counter to prevent false sharing
#[repr(align(64))]
pub struct PaddedCounter {
    value: AtomicU64,
    _padding: [u8; 56],
}

impl PaddedCounter {
    pub fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
            _padding: [0; 56],
        }
    }
    
    #[inline]
    pub fn add(&self, amount: u64) {
        self.value.fetch_add(amount, Ordering::Relaxed);
    }
    
    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Batch size optimized for L1 cache (64KB / ~200 bytes per event = ~300 events)
pub const OPTIMAL_BATCH_SIZE: usize = 256;

/// Channel size for backpressure management
pub const CHANNEL_CAPACITY: usize = 10_000;

/// Size of memory-mapped chunks (4MB)
pub const MMAP_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Compression level (1-22, 3 = speed optimized, 15 = balanced, 22 = max compression)
pub const ZSTD_COMPRESSION_LEVEL: i32 = 3;
