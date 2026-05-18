//! Microsoft Teams Alert Channel
//!
//! Send alerts to Microsoft Teams via incoming webhooks.
//!
//! ## Setup
//!
//! 1. In Teams, go to the channel where you want alerts
//! 2. Click "..." → Connectors → Incoming Webhook
//! 3. Configure and copy the webhook URL
//!
//! ## Configuration
//!
//! ```yaml
//! channels:
//!   teams:
//!     type: teams
//!     webhook_url: "https://outlook.office.com/webhook/..."
//! ```

use super::{AlertChannel, ChannelError, ChannelResult, ChannelType};
use crate::alerts::config::ChannelConfig;
use crate::alerts::models::Alert;
use async_trait::async_trait;
use serde_json::json;

/// Microsoft Teams webhook channel
pub struct TeamsChannel {
    name: String,
    webhook_url: String,
    client: reqwest::Client,
}

impl TeamsChannel {
    /// Create new Teams channel from configuration
    pub fn new(name: &str, config: &ChannelConfig) -> ChannelResult<Self> {
        match config {
            ChannelConfig::Teams { webhook_url } => {
                if webhook_url.is_empty() {
                    return Err(ChannelError::Config(
                        "Teams webhook URL is required".to_string()
                    ));
                }

                Ok(Self {
                    name: name.to_string(),
                    webhook_url: webhook_url.clone(),
                    client: reqwest::Client::builder()
                        .timeout(std::time::Duration::from_secs(30))
                        .build()?,
                })
            }
            _ => Err(ChannelError::Config(
                "Invalid config for Teams channel".to_string()
            )),
        }
    }

    /// Build Teams message card (MessageCard format)
    fn build_payload(&self, alert: &Alert) -> serde_json::Value {
        // Teams uses hex color values
        let theme_color = match alert.severity {
            crate::alerts::config::AlertSeverity::Info => "36a64f",
            crate::alerts::config::AlertSeverity::Warning => "ff9900",
            crate::alerts::config::AlertSeverity::Critical => "ff0000",
        };

        json!({
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": theme_color,
            "summary": format!("{} Alert: {}", alert.severity.to_string().to_uppercase(), alert.message.lines().next().unwrap_or(&alert.message)),
            "sections": [
                {
                    "activityTitle": format!("{} Fluxx Agent Alert", alert.severity.emoji()),
                    "activitySubtitle": format!("{:?} severity", alert.severity),
                    "activityImage": self.get_severity_image(&alert.severity),
                    "facts": [
                        {
                            "name": "Alert ID:",
                            "value": alert.id.clone()
                        },
                        {
                            "name": "Source:",
                            "value": alert.source.clone()
                        },
                        {
                            "name": "Severity:",
                            "value": format!("{:?}", alert.severity)
                        },
                        {
                            "name": "Time:",
                            "value": chrono::DateTime::from_timestamp_micros(alert.timestamp_us as i64)
                                .map(|d| d.to_rfc3339())
                                .unwrap_or_else(|| "Unknown".to_string())
                        }
                    ],
                    "markdown": true
                },
                {
                    "title": "Alert Details",
                    "text": alert.message
                }
            ],
            "potentialAction": [
                {
                    "@type": "OpenUri",
                    "name": "View in Fluxx",
                    "targets": [
                        {
                            "os": "default",
                            "uri": format!("https://fluxx.dev/alerts/{}", alert.id)
                        }
                    ]
                }
            ]
        })
    }

    /// Get image URL for severity level
    fn get_severity_image(&self, severity: &crate::alerts::config::AlertSeverity) -> &'static str {
        match severity {
            crate::alerts::config::AlertSeverity::Info => {
                "https://raw.githubusercontent.com/fluxx/agent/main/assets/icon-info.png"
            }
            crate::alerts::config::AlertSeverity::Warning => {
                "https://raw.githubusercontent.com/fluxx/agent/main/assets/icon-warning.png"
            }
            crate::alerts::config::AlertSeverity::Critical => {
                "https://raw.githubusercontent.com/fluxx/agent/main/assets/icon-critical.png"
            }
        }
    }

    /// Build simple text payload for info-level alerts
    fn build_simple_payload(&self, alert: &Alert) -> serde_json::Value {
        json!({
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "text": format!(
                "{} **{}**: {}",
                alert.severity.emoji(),
                alert.severity.to_string().to_uppercase(),
                alert.message.lines().next().unwrap_or(&alert.message)
            )
        })
    }
}

#[async_trait]
impl AlertChannel for TeamsChannel {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, alert: &Alert) -> ChannelResult<()> {
        // Use simple message for info, card for warning/critical
        let payload = if alert.severity == crate::alerts::config::AlertSeverity::Info {
            self.build_simple_payload(alert)
        } else {
            self.build_payload(alert)
        };

        let response = self.client
            .post(&self.webhook_url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        let status = response.status();

        // Teams returns 200 on success
        if status.is_success() {
            tracing::info!(
                channel = %self.name,
                alert_id = %alert.id,
                "Alert sent to Teams successfully"
            );
            Ok(())
        } else {
            let body = response.text().await?;
            Err(ChannelError::Api {
                status: status.as_u16(),
                message: format!("Teams API error: {}", body),
            })
        }
    }

    async fn health_check(&self) -> ChannelResult<()> {
        let test_payload = json!({
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "text": "🔔 Fluxx Agent Teams webhook test - Configuration is working!"
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
                format!("Teams health check failed: {}", body)
            ))
        }
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::Teams
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
            message: "Test Teams alert".to_string(),
            source: "test".to_string(),
            timestamp_us: 1704067200000000,
        }
    }

    fn create_test_config() -> ChannelConfig {
        ChannelConfig::Teams {
            webhook_url: "https://outlook.office.com/webhook/test".to_string(),
        }
    }

    #[test]
    fn test_build_payload() {
        let config = create_test_config();
        let channel = TeamsChannel::new("teams", &config).unwrap();
        let alert = create_test_alert(AlertSeverity::Critical);
        let payload = channel.build_payload(&alert);

        assert_eq!(payload["@type"], "MessageCard");
        assert_eq!(payload["themeColor"], "ff0000");
        assert!(payload["sections"][0]["facts"].as_array().unwrap().len() >= 4);
    }

    #[test]
    fn test_severity_colors() {
        let config = create_test_config();
        let channel = TeamsChannel::new("teams", &config).unwrap();

        let info = create_test_alert(AlertSeverity::Info);
        let warning = create_test_alert(AlertSeverity::Warning);
        let critical = create_test_alert(AlertSeverity::Critical);

        assert_eq!(channel.build_payload(&info)["themeColor"], "36a64f");
        assert_eq!(channel.build_payload(&warning)["themeColor"], "ff9900");
        assert_eq!(channel.build_payload(&critical)["themeColor"], "ff0000");
    }
}
