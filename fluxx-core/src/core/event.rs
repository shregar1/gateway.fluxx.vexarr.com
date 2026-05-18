use serde::{Deserialize, Serialize};
use crate::core::metadata::Metadata;
use std::time::SystemTime;

/// Strong typed protocol identifiers for all Fluxx network traffic.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProtocolType {
    Http,
    Ws,
    Tcp,
    Udp,
    Grpc,
    Quic,
    Ebpf,
    Other,
}

/// The centralized, high-performance event definition for Fluxx.
/// All protocol implementations (HTTP, TCP, etc.) MUST use this structure.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    /// Unique identifier for tracing across layers
    pub id: String,
    
    /// Microsecond-precision timestamp (Unix epoch)
    pub timestamp_us: u64,
    
    /// The protocol that originated the event
    pub protocol: ProtocolType,
    
    /// High-precision latency tracking across the pipeline
    pub latency_us: u64,
    
    /// Byte payload associated with the event
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    
    /// Centralized, structured metadata
    pub metadata: Metadata,
}

impl Event {
    /// Creates a new Fluxx event with the correct timestamp and protocol type.
    pub fn new(id: String, protocol: ProtocolType) -> Self {
        Self {
            id,
            timestamp_us: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
            protocol,
            latency_us: 0,
            payload: None,
            metadata: Metadata::new(),
        }
    }

    /// Helper to efficiently add binary payload to the event.
    pub fn with_payload(mut self, data: Vec<u8>) -> Self {
        self.payload = Some(data);
        self
    }
}
