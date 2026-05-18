//! Alert Notification Channels
//!
//! Send alerts to various destinations:
//! - Slack (via webhook)
//! - Email (via SMTP)
//! - Webhook (generic HTTP)
//! - PagerDuty
//! - Discord
//! - Microsoft Teams

use crate::alerts::models::Alert;
use async_trait::async_trait;

pub mod slack;
pub mod email;
pub mod webhook;
pub mod pagerduty;
pub mod discord;
pub mod teams;

pub use slack::SlackChannel;
pub use email::EmailChannel;
pub use webhook::WebhookChannel;
pub use pagerduty::PagerDutyChannel;
pub use discord::DiscordChannel;
pub use teams::TeamsChannel;

/// Result type for channel operations
pub type ChannelResult<T> = Result<T, ChannelError>;

/// Error type for channel operations
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    
    #[error("API error: {status} - {message}")]
    Api { status: u16, message: String },
    
    #[error("SMTP error: {0}")]
    Smtp(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("Rate limited")]
    RateLimited,
    
    #[error("Channel not available: {0}")]
    NotAvailable(String),
}

/// Trait for alert notification channels
#[async_trait]
pub trait AlertChannel: Send + Sync {
    /// Channel name
    fn name(&self) -> &str;
    
    /// Send an alert through this channel
    async fn send(&self, alert: &Alert) -> ChannelResult<()>;
    
    /// Check if channel is healthy/configured correctly
    async fn health_check(&self) -> ChannelResult<()>;
    
    /// Get channel type
    fn channel_type(&self) -> ChannelType;
}

/// Channel types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelType {
    Slack,
    Email,
    Webhook,
    PagerDuty,
    Discord,
    Teams,
}

impl std::fmt::Display for ChannelType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelType::Slack => write!(f, "slack"),
            ChannelType::Email => write!(f, "email"),
            ChannelType::Webhook => write!(f, "webhook"),
            ChannelType::PagerDuty => write!(f, "pagerduty"),
            ChannelType::Discord => write!(f, "discord"),
            ChannelType::Teams => write!(f, "teams"),
        }
    }
}

/// Factory function to create channel from config
pub fn create_channel(
    name: &str,
    config: &crate::alerts::config::ChannelConfig,
) -> ChannelResult<Box<dyn AlertChannel>> {
    match config {
        crate::alerts::config::ChannelConfig::Slack { .. } => {
            Ok(Box::new(SlackChannel::new(name, config)?))
        }
        crate::alerts::config::ChannelConfig::Email { .. } => {
            Ok(Box::new(EmailChannel::new(name, config)?))
        }
        crate::alerts::config::ChannelConfig::Webhook { .. } => {
            Ok(Box::new(WebhookChannel::new(name, config)?))
        }
        crate::alerts::config::ChannelConfig::PagerDuty { .. } => {
            Ok(Box::new(PagerDutyChannel::new(name, config)?))
        }
        crate::alerts::config::ChannelConfig::Discord { .. } => {
            Ok(Box::new(DiscordChannel::new(name, config)?))
        }
        crate::alerts::config::ChannelConfig::Teams { .. } => {
            Ok(Box::new(TeamsChannel::new(name, config)?))
        }
    }
}

/// Format alert for display
pub fn format_alert(alert: &Alert) -> String {
    format!(
        "{} [{}] {}\n\nTime: {}\nSource: {}\nSeverity: {:?}\n\n{}",
        alert.severity.emoji(),
        alert.id[..8].to_string(),
        alert.message.lines().next().unwrap_or(&alert.message),
        chrono::DateTime::from_timestamp_micros(alert.timestamp_us as i64)
            .map(|d| d.to_rfc3339())
            .unwrap_or_else(|| "unknown".to_string()),
        alert.source,
        alert.severity,
        alert.message
    )
}

/// Truncate message to max length
pub fn truncate_message(msg: &str, max_len: usize) -> String {
    if msg.len() <= max_len {
        msg.to_string()
    } else {
        format!("{}... (truncated)", &msg[..max_len - 20])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_message() {
        let short = "Short message";
        assert_eq!(truncate_message(short, 100), short);

        let long = "a".repeat(200);
        let truncated = truncate_message(&long, 100);
        assert!(truncated.len() <= 100);
        assert!(truncated.contains("truncated"));
    }
}
