//! Fluxx Agent Alert System
//!
//! A comprehensive alerting system with multiple notification channels:
//! - Slack
//! - Email
//! - Webhook
//! - PagerDuty
//! - Discord
//! - Microsoft Teams
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use fluxx_agent::alerts::{AlertManager, AlertManagerBuilder};
//! use fluxx_agent::alerts::config::AlertSeverity;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Build with channels
//!     let manager = AlertManagerBuilder::new()
//!         .add_slack("slack", "https://hooks.slack.com/...")
//!         .add_webhook("webhook", "https://api.example.com/alerts")
//!         .build().await?;
//!     
//!     // Trigger an alert
//!     manager.trigger(
//!         AlertSeverity::Critical,
//!         "Database connection failed",
//!         "api-server-1",
//!         vec!["slack".to_string(), "webhook".to_string()]
//!     ).await;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Configuration File
//!
//! ```yaml
//! alerts:
//!   enabled: true
//!   cooldown_seconds: 300
//!   max_alerts_per_hour: 100
//!   
//!   rules:
//!     - name: high_error_rate
//!       condition: "error_rate > 0.05"
//!       severity: critical
//!       channels: [slack, email]
//!   
//!   channels:
//!     slack:
//!       type: slack
//!       webhook_url: "${SLACK_WEBHOOK_URL}"
//!       channel: "#alerts"
//! ```

pub mod channels;
pub mod config;
pub mod manager;
pub mod metric_evaluator;
pub mod models;
pub mod rules;
pub mod webhook_sink;

// Re-exports
pub use channels::{AlertChannel, ChannelError, ChannelType};
pub use config::{load_config, from_env, AlertConfig, AlertRuleConfig, AlertSeverity};
pub use manager::{AlertManager, AlertManagerBuilder};
pub use models::{Alert, AlertState, TrackedAlert};

use crate::analysis::insights::{Insight, InsightLevel};

/// AlertEngine: Transforms insights into alerts based on rules
#[derive(Debug, Default)]
pub struct AlertEngine {
    rules: Vec<AlertRule>,
}

/// AlertRule for insight-based alerting
#[derive(Debug, Clone)]
pub struct AlertRule {
    pub name: String,
    pub target_protocol: Option<crate::models::event::ProtocolType>,
    pub min_severity: InsightLevel,
}

impl AlertRule {
    pub fn matches(&self, insight: &Insight) -> bool {
        // Check protocol match
        if let Some(proto) = self.target_protocol {
            if proto != insight.protocol {
                return false;
            }
        }
        
        // Check severity threshold
        insight.level == self.min_severity || insight.level == InsightLevel::Critical
    }
}

impl AlertEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Default alert rules for common scenarios
    pub fn default_rules() -> Self {
        let mut engine = Self::new();
        
        engine.add_rule(AlertRule {
            name: "CriticalReconnects".to_string(),
            target_protocol: Some(crate::models::event::ProtocolType::Ws),
            min_severity: InsightLevel::Critical,
        });
        
        engine.add_rule(AlertRule {
            name: "GenericAnomaly".to_string(),
            target_protocol: None,
            min_severity: InsightLevel::Anomaly,
        });
        
        engine
    }

    pub fn add_rule(&mut self, rule: AlertRule) {
        self.rules.push(rule);
    }

    /// Evaluates an insight against rules and returns an alert if triggered
    pub fn process_insight(&self, insight: Insight) -> Option<Alert> {
        for rule in &self.rules {
            if rule.matches(&insight) {
                let severity = match insight.level {
                    InsightLevel::Critical => AlertSeverity::Critical,
                    _ => AlertSeverity::Warning,
                };

                let alert = Alert::new(
                    severity,
                    format!("[{}] {}", rule.name, insight.message),
                    format!("{:?}", insight.protocol),
                );

                tracing::info!(
                    alert_id = %alert.id,
                    msg = %alert.message,
                    "AlertEngine: Triggered alert"
                );
                
                return Some(alert);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::insights::InsightLevel;

    #[test]
    fn test_alert_rule_matches() {
        let rule = AlertRule {
            name: "Test".to_string(),
            target_protocol: Some(crate::models::event::ProtocolType::Http),
            min_severity: InsightLevel::Warning,
        };

        let matching_insight = Insight {
            protocol: crate::models::event::ProtocolType::Http,
            level: InsightLevel::Warning,
            message: "Test".to_string(),
            metadata: Default::default(),
        };

        assert!(rule.matches(&matching_insight));
    }

    #[tokio::test]
    async fn test_alert_manager_builder() {
        let manager = AlertManagerBuilder::new()
            .add_webhook("test", "https://example.com/webhook")
            .build().await;
        
        assert!(manager.is_ok());
    }
}
