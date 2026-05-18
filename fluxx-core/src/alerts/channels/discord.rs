//! Discord Alert Channel
//!
//! Send alerts to Discord via incoming webhooks.
//!
//! ## Setup
//!
//! 1. In Discord, go to Server Settings → Integrations → Webhooks
//! 2. Create a new webhook
//! 3. Copy the webhook URL
//!
//! ## Configuration
//!
//! ```yaml
//! channels:
//!   discord:
//!     type: discord
//!     webhook_url: "https://discord.com/api/webhooks/..."
//!     username: "Fluxx Alerts"
//! ```

use super::{AlertChannel, ChannelError, ChannelResult, ChannelType};
use crate::alerts::config::ChannelConfig;
use crate::alerts::models::Alert;
use async_trait::async_trait;
use serde_json::json;

/// Discord webhook channel
pub struct DiscordChannel {
    name: String,
    webhook_url: String,
    username: String,
    client: reqwest::Client,
}

impl DiscordChannel {
    /// Create new Discord channel from configuration
    pub fn new(name: &str, config: &ChannelConfig) -> ChannelResult<Self> {
        match config {
            ChannelConfig::Discord {
                webhook_url,
                username,
            } => {
                if webhook_url.is_empty() {
                    return Err(ChannelError::Config(
                        "Discord webhook URL is required".to_string()
                    ));
                }

                Ok(Self {
                    name: name.to_string(),
                    webhook_url: webhook_url.clone(),
                    username: username.clone(),
                    client: reqwest::Client::builder()
                        .timeout(std::time::Duration::from_secs(30))
                        .build()?,
                })
            }
            _ => Err(ChannelError::Config(
                "Invalid config for Discord channel".to_string()
            )),
        }
    }

    /// Build Discord embed payload
    fn build_payload(&self, alert: &Alert) -> serde_json::Value {
        // Discord uses decimal color values
        let color = match alert.severity {
            crate::alerts::config::AlertSeverity::Info => 0x36a64f,      // Green
            crate::alerts::config::AlertSeverity::Warning => 0xff9900,   // Orange
            crate::alerts::config::AlertSeverity::Critical => 0xff0000,  // Red
        };

        let timestamp = chrono::DateTime::from_timestamp_micros(alert.timestamp_us as i64)
            .map(|d| d.to_rfc3339())
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        json!({
            "username": self.username,
            "embeds": [{
                "title": format!("{} {}", alert.severity.emoji(), alert.message.lines().next().unwrap_or(&alert.message)),
                "description": alert.message,
                "color": color,
                "fields": [
                    {
                        "name": "Severity",
                        "value": format!("{:?}", alert.severity),
                        "inline": true
                    },
                    {
                        "name": "Alert ID",
                        "value": alert.id.clone(),
                        "inline": true
                    },
                    {
                        "name": "Source",
                        "value": alert.source.clone(),
                        "inline": true
                    }
                ],
                "timestamp": timestamp,
                "footer": {
                    "text": "Fluxx Agent"
                }
            }]
        })
    }

    /// Build simple message payload for info-level alerts
    fn build_simple_payload(&self, alert: &Alert) -> serde_json::Value {
        json!({
            "username": self.username,
            "content": format!(
                "{} **{}**: {}",
                alert.severity.emoji(),
                alert.severity.to_string().to_uppercase(),
                alert.message.lines().next().unwrap_or(&alert.message)
            ),
        })
    }
}

#[async_trait]
impl AlertChannel for DiscordChannel {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, alert: &Alert) -> ChannelResult<()> {
        // Use simple message for info, embed for warning/critical
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

        let status = response.status();

        // Discord returns 204 No Content on success
        if status.is_success() {
            tracing::info!(
                channel = %self.name,
                alert_id = %alert.id,
                "Alert sent to Discord successfully"
            );
            Ok(())
        } else {
            let body = response.text().await?;
            Err(ChannelError::Api {
                status: status.as_u16(),
                message: format!("Discord API error: {}", body),
            })
        }
    }

    async fn health_check(&self) -> ChannelResult<()> {
        let test_payload = json!({
            "username": self.username,
            "content": "🔔 Fluxx Agent Discord webhook test - Configuration is working!"
        });

        let response = self.client
            .post(&self.webhook_url)
            .json(&test_payload)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let body = response.text().await?;
            Err(ChannelError::Config(
                format!("Discord health check failed: {}", body)
            ))
        }
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::Discord
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
            message: "Test Discord alert".to_string(),
            source: "test".to_string(),
            timestamp_us: 1704067200000000,
        }
    }

    fn create_test_config() -> ChannelConfig {
        ChannelConfig::Discord {
            webhook_url: "https://discord.com/api/webhooks/123456/test".to_string(),
            username: "Test Bot".to_string(),
        }
    }

    #[test]
    fn test_build_payload() {
        let config = create_test_config();
        let channel = DiscordChannel::new("discord", &config).unwrap();
        let alert = create_test_alert(AlertSeverity::Critical);
        let payload = channel.build_payload(&alert);

        assert_eq!(payload["username"], "Test Bot");
        assert!(payload["embeds"][0]["title"].as_str().unwrap().contains("🚨"));
        assert_eq!(payload["embeds"][0]["color"], 0xff0000);
    }

    #[test]
    fn test_severity_colors() {
        let config = create_test_config();
        let channel = DiscordChannel::new("discord", &config).unwrap();

        let info = create_test_alert(AlertSeverity::Info);
        let warning = create_test_alert(AlertSeverity::Warning);
        let critical = create_test_alert(AlertSeverity::Critical);

        assert_eq!(channel.build_payload(&info)["embeds"][0]["color"], 0x36a64f);
        assert_eq!(channel.build_payload(&warning)["embeds"][0]["color"], 0xff9900);
        assert_eq!(channel.build_payload(&critical)["embeds"][0]["color"], 0xff0000);
    }
}
