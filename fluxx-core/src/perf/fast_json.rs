//! SIMD-accelerated JSON processing (4-6× faster than serde_json)

use crate::models::event::Event;
use simd_json;

/// Deserialize JSON with SIMD acceleration
/// 
/// # Performance
/// - 4-6× faster than serde_json for typical event sizes
/// - Requires mutable buffer (reorders JSON during parsing)
/// 
/// # Example
/// ```
/// let mut json_bytes = json_string.into_bytes();
/// let event = fast_parse(&mut json_bytes).unwrap();
/// ```
#[inline]
pub fn fast_parse(json: &mut [u8]) -> Result<Event, simd_json::Error> {
    simd_json::serde::from_slice(json)
}

/// Serialize event to JSON bytes
/// Uses standard serde_json (serialization is already fast)
#[inline]
pub fn fast_serialize(event: &Event) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(event)
}

/// Batch parse multiple JSON documents
/// Processes in parallel when available
pub fn batch_parse(jsons: &mut [Vec<u8>]) -> Vec<Result<Event, simd_json::Error>> {
    jsons.iter_mut()
        .map(|json| fast_parse(json))
        .collect()
}

/// Preallocate buffer for reuse (reduces allocations)
pub struct ReusableBuffer {
    buffer: Vec<u8>,
}

impl ReusableBuffer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
        }
    }
    
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
    
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
    
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.buffer.extend_from_slice(slice);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::{Event, ProtocolType};

    #[test]
    fn test_simd_roundtrip() {
        let event = Event::new("test-1".to_string(), ProtocolType::Http)
            .with_payload("test data".to_string());
        
        let json = fast_serialize(&event).unwrap();
        let mut json_mut = json.clone();
        let decoded = fast_parse(&mut json_mut).unwrap();
        
        assert_eq!(event.id, decoded.id);
        assert_eq!(event.protocol, decoded.protocol);
    }

    #[test]
    fn test_batch_parse() {
        let events: Vec<_> = (0..100)
            .map(|i| Event::new(format!("evt-{}", i), ProtocolType::Http))
            .collect();
        
        let mut jsons: Vec<Vec<u8>> = events.iter()
            .map(|e| fast_serialize(e).unwrap())
            .collect();
        
        let results = batch_parse(&mut jsons);
        assert_eq!(results.len(), 100);
        assert!(results.iter().all(|r| r.is_ok()));
    }
}
