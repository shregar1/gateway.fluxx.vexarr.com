#![cfg(target_os = "linux")]
use serde::{Deserialize, Serialize};

/// EbpfEvent: Shared memory structure mapping the BPF kernel-side event.
/// Aligned strictly for high-performance memory copy from BPF perf/ring buffers.
#[repr(C)]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct EbpfEvent {
    pub timestamp_us: u64,
    pub src_ip: u32,
    pub dst_ip: u32,
    pub src_port: u16,
    pub dst_port: u16,
    pub payload_size: u32,
    pub protocol: u8, // 6 for TCP, 17 for UDP
}

impl EbpfEvent {
    /// Helper to convert raw BE IP to human-readable string.
    pub fn src_ip_str(&self) -> String {
        let octets = self.src_ip.to_be_bytes();
        format!("{}.{}.{}.{}", octets[0], octets[1], octets[2], octets[3])
    }

    pub fn dst_ip_str(&self) -> String {
        let octets = self.dst_ip.to_be_bytes();
        format!("{}.{}.{}.{}", octets[0], octets[1], octets[2], octets[3])
    }
}
