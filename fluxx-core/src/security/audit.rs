//! Audit Trail
//! Immutable log of all platform actions for compliance.
//! Records: who did what, when, on which resource.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use parking_lot::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub id: String,
    pub timestamp_us: u64,
    pub actor: String,
    pub action: String,
    pub resource_type: String,
    pub resource_id: String,
    pub details: HashMap<String, String>,
    pub ip_address: Option<String>,
    pub success: bool,
}

#[derive(Debug, Default)]
pub struct AuditLog {
    entries: RwLock<Vec<AuditEntry>>,
}

impl AuditLog {
    pub fn new() -> Self { Self::default() }

    pub fn record(&self, actor: &str, action: &str, resource_type: &str, resource_id: &str, success: bool, details: HashMap<String, String>) {
        let entry = AuditEntry {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp_us: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_micros() as u64,
            actor: actor.to_string(), action: action.to_string(),
            resource_type: resource_type.to_string(), resource_id: resource_id.to_string(),
            details, ip_address: None, success,
        };
        self.entries.write().push(entry);
    }

    pub fn query(&self, actor: Option<&str>, action: Option<&str>, resource_type: Option<&str>, limit: usize) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        entries.iter().rev()
            .filter(|e| actor.map_or(true, |a| e.actor == a))
            .filter(|e| action.map_or(true, |a| e.action == a))
            .filter(|e| resource_type.map_or(true, |r| e.resource_type == r))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize { self.entries.read().len() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_audit_log() {
        let log = AuditLog::new();
        log.record("admin", "create_slo", "slo", "slo-1", true, HashMap::new());
        log.record("admin", "delete_rule", "alert_rule", "rule-1", true, HashMap::new());
        assert_eq!(log.len(), 2);
        let results = log.query(Some("admin"), None, None, 10);
        assert_eq!(results.len(), 2);
    }
}
