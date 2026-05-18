//! Alert Models

use serde::{Deserialize, Serialize};
use crate::alerts::config::AlertSeverity;

/// Alert: A notification triggered by threshold breaches or anomalies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: String,
    pub severity: AlertSeverity,
    pub message: String,
    pub source: String,
    pub timestamp_us: u64,
}

impl Alert {
    /// Create a new alert
    pub fn new(
        severity: AlertSeverity,
        message: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            severity,
            message: message.into(),
            source: source.into(),
            timestamp_us: chrono::Utc::now().timestamp_micros() as u64,
        }
    }

    /// Get alert digest for deduplication
    pub fn digest(&self) -> String {
        format!("{}:{}:{}", self.source, self.severity as u8, self.message)
    }

    /// Get short ID (first 8 chars)
    pub fn short_id(&self) -> &str {
        &self.id[..8.min(self.id.len())]
    }
}

/// Alert state for tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertState {
    /// Alert is firing
    Firing,
    /// Alert has been acknowledged
    Acknowledged,
    /// Alert has been resolved
    Resolved,
    /// Alert was suppressed
    Suppressed,
}

/// Tracked alert with state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackedAlert {
    pub alert: Alert,
    pub state: AlertState,
    pub created_at: u64,
    pub updated_at: u64,
    pub acknowledged_by: Option<String>,
    pub resolved_at: Option<u64>,
    pub notification_count: u32,
}

impl TrackedAlert {
    pub fn new(alert: Alert) -> Self {
        let now = chrono::Utc::now().timestamp_micros() as u64;
        Self {
            alert,
            state: AlertState::Firing,
            created_at: now,
            updated_at: now,
            acknowledged_by: None,
            resolved_at: None,
            notification_count: 0,
        }
    }

    pub fn acknowledge(&mut self, by: impl Into<String>) {
        self.state = AlertState::Acknowledged;
        self.acknowledged_by = Some(by.into());
        self.updated_at = chrono::Utc::now().timestamp_micros() as u64;
    }

    pub fn resolve(&mut self) {
        self.state = AlertState::Resolved;
        self.resolved_at = Some(chrono::Utc::now().timestamp_micros() as u64);
        self.updated_at = self.resolved_at.unwrap();
    }

    pub fn increment_notification(&mut self) {
        self.notification_count += 1;
    }
}

/// Alert rule result
#[derive(Debug, Clone)]
pub struct AlertRuleResult {
    pub triggered: bool,
    pub severity: Option<AlertSeverity>,
    pub message: Option<String>,
}

impl AlertRuleResult {
    pub fn no_match() -> Self {
        Self {
            triggered: false,
            severity: None,
            message: None,
        }
    }

    pub fn trigger(severity: AlertSeverity, message: impl Into<String>) -> Self {
        Self {
            triggered: true,
            severity: Some(severity),
            message: Some(message.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alert_creation() {
        let alert = Alert::new(
            AlertSeverity::Critical,
            "Test message",
            "test_source",
        );
        
        assert_eq!(alert.severity, AlertSeverity::Critical);
        assert_eq!(alert.message, "Test message");
        assert_eq!(alert.source, "test_source");
        assert!(!alert.id.is_empty());
    }

    #[test]
    fn test_alert_digest() {
        let alert1 = Alert::new(AlertSeverity::Warning, "Same message", "source1");
        let alert2 = Alert::new(AlertSeverity::Warning, "Same message", "source1");
        let alert3 = Alert::new(AlertSeverity::Critical, "Same message", "source1");
        
        assert_eq!(alert1.digest(), alert2.digest());
        assert_ne!(alert1.digest(), alert3.digest());
    }

    #[test]
    fn test_tracked_alert_state() {
        let alert = Alert::new(AlertSeverity::Warning, "Test", "source");
        let mut tracked = TrackedAlert::new(alert);
        
        assert_eq!(tracked.state, AlertState::Firing);
        assert_eq!(tracked.notification_count, 0);
        
        tracked.increment_notification();
        assert_eq!(tracked.notification_count, 1);
        
        tracked.acknowledge("admin");
        assert_eq!(tracked.state, AlertState::Acknowledged);
        assert_eq!(tracked.acknowledged_by, Some("admin".to_string()));
        
        tracked.resolve();
        assert_eq!(tracked.state, AlertState::Resolved);
        assert!(tracked.resolved_at.is_some());
    }
}
