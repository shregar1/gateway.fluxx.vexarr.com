use crate::models::event::Event;
use serde_json;
use anyhow::Result;

/// Unified Interface for event serialization.
/// Supports multiple formats (JSON, Proto, etc.) efficiently.
pub trait Serializer: Send + Sync {
    fn serialize(&self, event: &Event) -> Result<Vec<u8>>;
    fn format_name(&self) -> &str;
}

/// Standard JSON serializer for Fluxx events.
pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    fn serialize(&self, event: &Event) -> Result<Vec<u8>> {
        serde_json::to_vec(event).map_err(|e| e.into())
    }

    fn format_name(&self) -> &str {
        "json"
    }
}
