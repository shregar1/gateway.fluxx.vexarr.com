//! Memory-mapped storage for zero-copy persistence
//! 
//! Benefits:
//! - No userspace/kernel copy for reads/writes
//! - OS handles caching automatically
//! - Crash-safe with proper sync

use memmap2::{Mmap, MmapMut};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Memory-mapped append-only log
#[allow(dead_code)]
pub struct MmapLog {
    file: File,
    mmap: MmapMut,
    write_offset: AtomicU64,
    capacity: u64,
}

impl MmapLog {
    /// Create or open a memory-mapped log file
    pub fn open<P: AsRef<Path>>(path: P, size_mb: usize) -> Result<Self, std::io::Error> {
        let size = size_mb * 1024 * 1024;
        
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        
        // Pre-allocate file
        file.set_len(size as u64)?;
        
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        
        Ok(Self {
            file,
            mmap,
            write_offset: AtomicU64::new(0),
            capacity: size as u64,
        })
    }
    
    /// Append data to the log
    #[inline]
    pub fn append(&mut self, data: &[u8]) -> Result<u64, std::io::Error> {
        let offset = self.write_offset.fetch_add(data.len() as u64, Ordering::SeqCst);
        
        if offset + data.len() as u64 > self.capacity {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "Log full",
            ));
        }
        
        let end = (offset as usize) + data.len();
        self.mmap[offset as usize..end].copy_from_slice(data);
        
        Ok(offset)
    }
    
    /// Read data at specific offset
    #[inline]
    pub fn read_at(&self, offset: u64, len: usize) -> Result<&[u8], std::io::Error> {
        if offset + len as u64 > self.capacity {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Read past end",
            ));
        }
        
        Ok(&self.mmap[offset as usize..(offset as usize + len)])
    }
    
    /// Sync to disk (crash safety)
    #[inline]
    pub fn sync(&self) -> Result<(), std::io::Error> {
        self.mmap.flush()
    }
    
    /// Current write offset
    #[inline]
    pub fn offset(&self) -> u64 {
        self.write_offset.load(Ordering::SeqCst)
    }
    
    /// Remaining capacity
    #[inline]
    pub fn remaining(&self) -> u64 {
        self.capacity - self.offset()
    }
}

/// Read-only memory-mapped file
pub struct MmapReader {
    _file: File,
    mmap: Mmap,
}

impl MmapReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        
        Ok(Self {
            _file: file,
            mmap,
        })
    }
    
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap
    }
    
    #[inline]
    pub fn len(&self) -> usize {
        self.mmap.len()
    }
    
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }
}

/// Ring buffer using memory-mapped files
pub struct MmapRingBuffer {
    log: MmapLog,
    read_offset: AtomicU64,
}

impl MmapRingBuffer {
    pub fn open<P: AsRef<Path>>(path: P, size_mb: usize) -> Result<Self, std::io::Error> {
        Ok(Self {
            log: MmapLog::open(path, size_mb)?,
            read_offset: AtomicU64::new(0),
        })
    }
    
    pub fn write(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        self.log.append(data)?;
        Ok(())
    }
    
    pub fn read(&self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let read_pos = self.read_offset.load(Ordering::SeqCst);
        let write_pos = self.log.offset();
        
        if read_pos >= write_pos {
            return Ok(0); // Empty
        }
        
        let available = (write_pos - read_pos) as usize;
        let to_read = buf.len().min(available);
        
        let data = self.log.read_at(read_pos, to_read)?;
        buf[..to_read].copy_from_slice(data);
        
        self.read_offset.fetch_add(to_read as u64, Ordering::SeqCst);
        
        Ok(to_read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_mmap_log() {
        let mut temp_path = std::env::temp_dir();
        temp_path.push("test_mmap_log.bin");
        
        // Clean up
        let _ = std::fs::remove_file(&temp_path);
        
        let mut log = MmapLog::open(&temp_path, 1).unwrap();
        
        let data = b"Hello, memory-mapped world!";
        let offset = log.append(data).unwrap();
        
        assert_eq!(offset, 0);
        assert_eq!(log.offset(), data.len() as u64);
        
        let read = log.read_at(offset, data.len()).unwrap();
        assert_eq!(read, data);
        
        // Cleanup
        let _ = std::fs::remove_file(temp_path);
    }

    #[test]
    fn test_mmap_performance() {
        let mut temp_path = std::env::temp_dir();
        temp_path.push("test_mmap_perf.bin");
        let _ = std::fs::remove_file(&temp_path);
        
        let mut log = MmapLog::open(&temp_path, 100).unwrap();
        
        let data = vec![0u8; 1024]; // 1KB chunks
        let count = 10_000;
        
        let start = std::time::Instant::now();
        for _ in 0..count {
            log.append(&data).unwrap();
        }
        let elapsed = start.elapsed();
        
        let mb_written = (count * 1024) as f64 / 1_048_576.0;
        let seconds = elapsed.as_secs_f64();
        
        println!("Wrote {} MB in {:?}", mb_written, elapsed);
        println!("Throughput: {:.0} MB/s", mb_written / seconds);
        
        // Cleanup
        let _ = std::fs::remove_file(&temp_path);
    }
}
