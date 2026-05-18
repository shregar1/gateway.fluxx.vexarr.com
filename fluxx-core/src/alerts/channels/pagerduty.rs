//! PagerDuty Alert Channel
//!
//! Send alerts to PagerDuty for incident management.
//!
//! ## Setup
//!
//! 1. Create a new integration in PagerDuty
//! 2. Copy the integration key
//! 3. Configure in alerts.yaml
//!
//! ## Configuration
//!
//! ```yaml
//! channels:
//!   pagerduty:
//!     type: pagerduty
//!     integration_key: "${PAGERDUTY_KEY}"
//! ```

use super::{AlertChannel, ChannelError, ChannelResult, ChannelType};
use crate::alerts::config::ChannelConfig;
use crate::alerts::models::Alert;
use async_trait::async_trait;
use serde_json::json;

/// PagerDuty Events API v2 channel
pub struct PagerDutyChannel {
    name: String,
    integration_key: String,
    api_url: String,
    client: reqwest::Client,
}

impl PagerDutyChannel {
    /// Create new PagerDuty channel from configuration
    pub fn new(name: &str, config: &ChannelConfig) -> ChannelResult<Self> {
        match config {
            ChannelConfig::PagerDuty {
                integration_key,
                api_url,
            } => {
                if integration_key.is_empty() {
                    return Err(ChannelError::Config(
                        "PagerDuty integration key is required".to_string()
                    ));
                }

                Ok(Self {
                    name: name.to_string(),
                    integration_key: integration_key.clone(),
                    api_url: api_url.clone(),
                    client: reqwest::Client::builder()
                        .timeout(std::time::Duration::from_secs(30))
                        .build()?,
                })
            }
            _ => Err(ChannelError::Config(
                "Invalid config for PagerDuty channel".to_string()
            )),
        }
    }

    /// Build PagerDuty event payload
    ///
    /// Uses PagerDuty Events API v2 format:
    /// https://developer.pagerduty.com/docs/events-api-v2/trigger-events/
    fn build_payload(&self, alert: &Alert) -> serde_json::Value {
        // Map severity to PagerDuty severity
        let pd_severity = match alert.severity {
            crate::alerts::config::AlertSeverity::Info => "info",
            crate::alerts::config::AlertSeverity::Warning => "warning",
            crate::alerts::config::AlertSeverity::Critical => "critical",
        };

        // Build dedup key from alert properties
        // This allows PagerDuty to correlate related alerts
        let dedup_key = format!("fluxx-{}-{}", alert.source, alert.severity.priority());

        json!({
            "routing_key": self.integration_key,
            "event_action": "trigger",
            "dedup_key": dedup_key,
            "payload": {
                "summary": alert.message.lines().next().unwrap_or(&alert.message),
                "severity": pd_severity,
                "source": alert.source,
                "timestamp": chrono::DateTime::from_timestamp_micros(alert.timestamp_us as i64)
                    .map(|d| d.to_rfc3339())
                    .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
                "component": "Fluxx Agent",
                "group": "observability",
                "class": format!("{:?}", alert.severity),
                "custom_details": {
                    "alert_id": alert.id,
                    "full_message": alert.message,
                    "severity": alert.severity,
                    "severity_emoji": alert.severity.emoji(),
                }
            }
        })
    }

    /// Build resolve payload (for when alert clears)
    #[allow(dead_code)]
    fn build_resolve_payload(&self, dedup_key: &str) -> serde_json::Value {
        json!({
            "routing_key": self.integration_key,
            "event_action": "resolve",
            "dedup_key": dedup_key,
        })
    }

    /// Build acknowledge payload
    #[allow(dead_code)]
    fn build_acknowledge_payload(&self, dedup_key: &str) -> serde_json::Value {
        json!({
            "routing_key": self.integration_key,
            "event_action": "acknowledge",
            "dedup_key": dedup_key,
        })
    }
}

#[async_trait]
impl AlertChannel for PagerDutyChannel {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, alert: &Alert) -> ChannelResult<()> {
        // Skip info-level alerts for PagerDuty
        if alert.severity == crate::alerts::config::AlertSeverity::Info {
            tracing::debug!(
                channel = %self.name,
                alert_id = %alert.id,
                "Skipping info-level alert for PagerDuty"
            );
            return Ok(());
        }

        let payload = self.build_payload(alert);

        let response = self.client
            .post(&self.api_url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;

        if status.is_success() {
            tracing::info!(
                channel = %self.name,
                alert_id = %alert.id,
                dedup_key = %payload["dedup_key"].as_str().unwrap_or("unknown"),
                "PagerDuty incident triggered"
            );
            Ok(())
        } else {
            Err(ChannelError::Api {
                status: status.as_u16(),
                message: format!("PagerDuty API error: {}", body),
            })
        }
    }

    async fn health_check(&self) -> ChannelResult<()> {
        // PagerDuty doesn't have a health check endpoint
        // We'll validate by checking the integration key format
        if self.integration_key.len() < 20 {
            return Err(ChannelError::Config(
                "PagerDuty integration key looks invalid".to_string()
            ));
        }

        tracing::info!(
            channel = %self.name,
            "PagerDuty channel configured (integration key validated)"
        );

        Ok(())
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::PagerDuty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerts::config::AlertSeverity;

    fn create_test_alert(severity: AlertSeverity) -> Alert {
        Alert {
            id: "test-123".to_string(),
            severity,
            message: "Test PagerDuty alert\nWith details".to_string(),
            source: "api-server-1".to_string(),
            timestamp_us: 1704067200000000,
        }
    }

    fn create_test_config() -> ChannelConfig {
        ChannelConfig::PagerDuty {
            integration_key: "r+xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string(),
            api_url: "https://events.pagerduty.com/v2/enqueue".to_string(),
        }
    }

    #[test]
    fn test_build_payload() {
        let config = create_test_config();
        let channel = PagerDutyChannel::new("pagerduty", &config).unwrap();
        let alert = create_test_alert(AlertSeverity::Critical);
        let payload = channel.build_payload(&alert);

        assert_eq!(payload["routing_key"], "r+xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        assert_eq!(payload["event_action"], "trigger");
        assert!(payload["dedup_key"].as_str().unwrap().contains("api-server-1"));
        assert_eq!(payload["payload"]["severity"], "critical");
        assert_eq!(payload["payload"]["source"], "api-server-1");
    }

    #[test]
    fn test_severity_mapping() {
        let config = create_test_config();
        let channel = PagerDutyChannel::new("pagerduty", &config).unwrap();

        let info_alert = create_test_alert(AlertSeverity::Info);
        let warning_alert = create_test_alert(AlertSeverity::Warning);
        let critical_alert = create_test_alert(AlertSeverity::Critical);

        assert_eq!(
            channel.build_payload(&info_alert)["payload"]["severity"],
            "info"
        );
        assert_eq!(
            channel.build_payload(&warning_alert)["payload"]["severity"],
            "warning"
        );
        assert_eq!(
            channel.build_payload(&critical_alert)["payload"]["severity"],
            "critical"
        );
    }

    #[test]
    fn test_dedup_key_format() {
        let config = create_test_config();
        let channel = PagerDutyChannel::new("pagerduty", &config).unwrap();
        let alert = create_test_alert(AlertSeverity::Warning);
        let payload = channel.build_payload(&alert);
        
        let dedup_key = payload["dedup_key"].as_str().unwrap();
        assert!(dedup_key.starts_with("fluxx-"));
        assert!(dedup_key.contains("api-server-1"));
    }
}
