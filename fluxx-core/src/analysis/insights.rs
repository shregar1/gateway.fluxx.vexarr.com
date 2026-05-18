use serde::{Deserialize, Serialize};
use crate::models::event::ProtocolType;

/// Insight: A high-level finding emitted by the Protocol Intelligence system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Insight {
    pub protocol: ProtocolType,
    pub level: InsightLevel,
    pub message: String,
    pub metadata: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum InsightLevel {
    Info,
    Warning,
    Critical,
    Anomaly,
}

impl Insight {
    pub fn new(protocol: ProtocolType, level: InsightLevel, message: &str) -> Self {
        Self {
            protocol,
            level,
            message: message.to_string(),
            metadata: std::collections::HashMap::new(),
        }
    }
}
