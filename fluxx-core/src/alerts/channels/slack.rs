//! Slack Alert Channel
//!
//! Send alerts to Slack via incoming webhooks.
//!
//! ## Setup
//!
//! 1. Create a Slack app at https://api.slack.com/apps
//! 2. Enable "Incoming Webhooks"
//! 3. Copy the webhook URL
//!
//! ## Configuration
//!
//! ```yaml
//! channels:
//!   slack:
//!     type: slack
//!     webhook_url: "https://hooks.slack.com/services/T000/B000/XXXX"
//!     channel: "#alerts"
//!     username: "Fluxx Agent"
//!     icon_emoji: ":warning:"
//! ```

use super::{AlertChannel, ChannelError, ChannelResult, ChannelType};
use crate::alerts::config::ChannelConfig;
use crate::alerts::models::Alert;
use async_trait::async_trait;
use serde_json::json;

/// Slack webhook channel
pub struct SlackChannel {
    name: String,
    webhook_url: String,
    channel: Option<String>,
    username: String,
    icon_emoji: String,
    client: reqwest::Client,
}

impl SlackChannel {
    /// Create new Slack channel from configuration
    pub fn new(name: &str, config: &ChannelConfig) -> ChannelResult<Self> {
        match config {
            ChannelConfig::Slack {
                webhook_url,
                channel,
                username,
                icon_emoji,
            } => {
                if webhook_url.is_empty() {
                    return Err(ChannelError::Config(
                        "Slack webhook URL is required".to_string()
                    ));
                }

                Ok(Self {
                    name: name.to_string(),
                    webhook_url: webhook_url.clone(),
                    channel: channel.clone(),
                    username: username.clone(),
                    icon_emoji: icon_emoji.clone(),
                    client: reqwest::Client::builder()
                        .timeout(std::time::Duration::from_secs(30))
                        .build()?,
                })
            }
            _ => Err(ChannelError::Config(
                "Invalid config for Slack channel".to_string()
            )),
        }
    }

    /// Build Slack message payload from alert
    fn build_payload(&self, alert: &Alert) -> serde_json::Value {
        let color = match alert.severity {
            crate::alerts::config::AlertSeverity::Info => "#36a64f",
            crate::alerts::config::AlertSeverity::Warning => "#ff9900",
            crate::alerts::config::AlertSeverity::Critical => "#ff0000",
        };

        let timestamp = chrono::DateTime::from_timestamp_micros(alert.timestamp_us as i64)
            .map(|d| d.to_rfc3339())
            .unwrap_or_else(|| "Unknown".to_string());

        let mut payload = json!({
            "username": self.username,
            "icon_emoji": self.icon_emoji,
            "attachments": [{
                "color": color,
                "title": format!("{} {}", alert.severity.emoji(), alert.message.lines().next().unwrap_or(&alert.message)),
                "text": alert.message,
                "fields": [
                    {
                        "title": "Severity",
                        "value": format!("{:?}", alert.severity),
                        "short": true
                    },
                    {
                        "title": "Alert ID",
                        "value": alert.id.clone(),
                        "short": true
                    },
                    {
                        "title": "Source",
                        "value": alert.source.clone(),
                        "short": true
                    },
                    {
                        "title": "Time",
                        "value": timestamp,
                        "short": true
                    }
                ],
                "footer": "Fluxx Agent",
                "ts": alert.timestamp_us / 1_000_000
            }]
        });

        // Add channel if specified
        if let Some(channel) = &self.channel {
            payload["channel"] = json!(channel);
        }

        payload
    }

    /// Build simple text message for less important alerts
    fn build_simple_payload(&self, alert: &Alert) -> serde_json::Value {
        let mut payload = json!({
            "username": self.username,
            "icon_emoji": self.icon_emoji,
            "text": format!("{} {}: {}", 
                alert.severity.emoji(),
                alert.severity.to_string().to_uppercase(),
                alert.message.lines().next().unwrap_or(&alert.message)
            ),
        });

        if let Some(channel) = &self.channel {
            payload["channel"] = json!(channel);
        }

        payload
    }
}

#[async_trait]
impl AlertChannel for SlackChannel {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, alert: &Alert) -> ChannelResult<()> {
        // Use rich attachments for warnings and critical
        let payload = if alert.severity == crate::alerts::config::AlertSeverity::Info {
            self.build_simple_payload(alert)
        } else {
            self.build_payload(alert)
        };

        let response = self.client
            .post(&self.webhook_url)
            .json(&payload)
            .send()
            .await?;

        // Slack webhooks return "ok" on success
        let status = response.status();
        let body = response.text().await?;

        if status.is_success() && body == "ok" {
            tracing::info!(
                channel = %self.name,
                alert_id = %alert.id,
                "Alert sent to Slack successfully"
            );
            Ok(())
        } else {
            Err(ChannelError::Api {
                status: status.as_u16(),
                message: format!("Slack API error: {}", body),
            })
        }
    }

    async fn health_check(&self) -> ChannelResult<()> {
        // Send a test message
        let test_payload = json!({
            "username": self.username,
            "icon_emoji": ":white_check_mark:",
            "text": "🔔 Fluxx Agent alert channel test - Configuration is working!"
        });

        let response = self.client
            .post(&self.webhook_url)
            .json(&test_payload)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;

        if status.is_success() && body == "ok" {
            Ok(())
        } else {
            Err(ChannelError::Config(
                format!("Slack health check failed: {} - {}", status, body)
            ))
        }
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::Slack
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerts::config::AlertSeverity;

    fn create_test_alert(severity: AlertSeverity) -> Alert {
        Alert {
            id: "test-alert-123".to_string(),
            severity,
            message: "Test alert message\nWith multiple lines".to_string(),
            source: "test_source".to_string(),
            timestamp_us: 1704067200000000, // 2024-01-01 00:00:00 UTC
        }
    }

    #[test]
    fn test_build_payload() {
        let config = ChannelConfig::Slack {
            webhook_url: "https://hooks.slack.com/test".to_string(),
            channel: Some("#alerts".to_string()),
            username: "Test Bot".to_string(),
            icon_emoji: ":warning:".to_string(),
        };

        let channel = SlackChannel::new("test", &config).unwrap();
        let alert = create_test_alert(AlertSeverity::Critical);
        let payload = channel.build_payload(&alert);

        assert_eq!(payload["username"], "Test Bot");
        assert_eq!(payload["channel"], "#alerts");
        assert!(payload["attachments"][0]["color"] == "#ff0000");
    }

    #[test]
    fn test_severity_colors() {
        let config = ChannelConfig::Slack {
            webhook_url: "https://hooks.slack.com/test".to_string(),
            channel: None,
            username: "Test".to_string(),
            icon_emoji: ":test:".to_string(),
        };

        let channel = SlackChannel::new("test", &config).unwrap();

        let info_alert = create_test_alert(AlertSeverity::Info);
        let warning_alert = create_test_alert(AlertSeverity::Warning);
        let critical_alert = create_test_alert(AlertSeverity::Critical);

        assert_eq!(
            channel.build_payload(&info_alert)["attachments"][0]["color"],
            "#36a64f"
        );
        assert_eq!(
            channel.build_payload(&warning_alert)["attachments"][0]["color"],
            "#ff9900"
        );
        assert_eq!(
            channel.build_payload(&critical_alert)["attachments"][0]["color"],
            "#ff0000"
        );
    }
}
