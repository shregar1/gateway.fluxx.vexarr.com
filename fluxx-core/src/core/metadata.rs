use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Common metadata fields for all Fluxx events.
/// This reduces duplication and ensures consistent naming across protocols.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Metadata {
    pub source_ip: Option<String>,
    pub destination_ip: Option<String>,
    pub request_id: Option<String>,
    pub user_agent: Option<String>,
    
    /// Any protocol-specific extra metadata.
    #[serde(flatten)]
    pub extra: HashMap<String, String>,
}

impl Metadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, key: &str, value: &str) {
        self.extra.insert(key.to_string(), value.to_string());
    }
}
