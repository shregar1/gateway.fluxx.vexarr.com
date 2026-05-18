//! Alert System Configuration
//!
//! Configure alerts via YAML file or environment variables.
//!
//! ## Example Configuration
//!
//! ```yaml
//! alerts:
//!   enabled: true
//!   
//!   # Global settings
//!   cooldown_seconds: 300  # Don't resend same alert for 5 minutes
//!   max_alerts_per_hour: 100  # Rate limiting
//!   
//!   # Alert rules
//!   rules:
//!     - name: high_error_rate
//!       condition: "error_rate > 0.05"
//!       severity: critical
//!       channels: [slack, email]
//!       
//!     - name: disk_space_low
//!       condition: "disk_free < 10GB"
//!       severity: warning
//!       channels: [slack]
//!       
//!   # Notification channels
//!   channels:
//!     slack:
//!       type: slack
//!       webhook_url: "${SLACK_WEBHOOK_URL}"
//!       channel: "#alerts"
//!       username: "Fluxx Agent"
//!       
//!     email:
//!       type: email
//!       smtp_host: "smtp.gmail.com"
//!       smtp_port: 587
//!       username: "${EMAIL_USER}"
//!       password: "${EMAIL_PASS}"
//!       from: "alerts@fluxx.dev"
//!       to: ["ops@company.com", "oncall@company.com"]
//!       
//!     pagerduty:
//!       type: webhook
//!       url: "https://events.pagerduty.com/v2/enqueue"
//!       headers:
//!         Authorization: "Token token=${PAGERDUTY_KEY}"
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Complete alert system configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertConfig {
    /// Enable/disable alert system
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Global cooldown between identical alerts (seconds)
    #[serde(default = "default_cooldown")]
    pub cooldown_seconds: u64,

    /// Maximum alerts per hour (rate limiting)
    #[serde(default = "default_max_alerts")]
    pub max_alerts_per_hour: u32,

    /// Alert rules
    #[serde(default)]
    pub rules: Vec<AlertRuleConfig>,

    /// Notification channels
    #[serde(default)]
    pub channels: HashMap<String, ChannelConfig>,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cooldown_seconds: 300,
            max_alerts_per_hour: 100,
            rules: vec![],
            channels: HashMap::new(),
        }
    }
}

/// Alert rule configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertRuleConfig {
    /// Unique rule name
    pub name: String,

    /// Human-readable description
    #[serde(default)]
    pub description: String,

    /// Condition expression (e.g., "error_rate > 0.05")
    pub condition: String,

    /// Severity level
    #[serde(default)]
    pub severity: AlertSeverity,

    /// Which channels to notify
    #[serde(default)]
    pub channels: Vec<String>,

    /// Additional labels for routing
    #[serde(default)]
    pub labels: HashMap<String, String>,

    /// Cooldown override (uses global if not set)
    pub cooldown_seconds: Option<u64>,
}

/// Alert severity levels
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

impl Default for AlertSeverity {
    fn default() -> Self {
        AlertSeverity::Warning
    }
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertSeverity::Info => write!(f, "info"),
            AlertSeverity::Warning => write!(f, "warning"),
            AlertSeverity::Critical => write!(f, "critical"),
        }
    }
}

impl AlertSeverity {
    /// Get numeric priority (higher = more severe)
    pub fn priority(&self) -> u8 {
        match self {
            AlertSeverity::Info => 1,
            AlertSeverity::Warning => 2,
            AlertSeverity::Critical => 3,
        }
    }

    /// Get emoji representation
    pub fn emoji(&self) -> &'static str {
        match self {
            AlertSeverity::Info => "ℹ️",
            AlertSeverity::Warning => "⚠️",
            AlertSeverity::Critical => "🚨",
        }
    }
}

/// Channel configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChannelConfig {
    /// Slack webhook integration
    Slack {
        /// Webhook URL (can use env var: ${SLACK_WEBHOOK_URL})
        webhook_url: String,
        /// Channel to post to (e.g., "#alerts")
        #[serde(default)]
        channel: Option<String>,
        /// Bot username
        #[serde(default = "default_slack_username")]
        username: String,
        /// Emoji icon
        #[serde(default = "default_slack_icon")]
        icon_emoji: String,
    },

    /// Email notification
    Email {
        /// SMTP server host
        smtp_host: String,
        /// SMTP server port
        #[serde(default = "default_smtp_port")]
        smtp_port: u16,
        /// SMTP username
        username: String,
        /// SMTP password
        password: String,
        /// Use TLS
        #[serde(default = "default_true")]
        use_tls: bool,
        /// From address
        from: String,
        /// To addresses
        to: Vec<String>,
        /// CC addresses
        #[serde(default)]
        cc: Vec<String>,
    },

    /// Generic webhook
    Webhook {
        /// Target URL
        url: String,
        /// HTTP method
        #[serde(default = "default_webhook_method")]
        method: String,
        /// Custom headers
        #[serde(default)]
        headers: HashMap<String, String>,
        /// Request timeout (seconds)
        #[serde(default = "default_webhook_timeout")]
        timeout_seconds: u64,
        /// Retry attempts
        #[serde(default = "default_webhook_retries")]
        retry_count: u32,
    },

    /// PagerDuty integration
    PagerDuty {
        /// Integration key
        integration_key: String,
        /// API endpoint
        #[serde(default = "default_pagerduty_url")]
        api_url: String,
    },

    /// Discord webhook
    Discord {
        /// Webhook URL
        webhook_url: String,
        /// Username to display
        #[serde(default = "default_discord_username")]
        username: String,
    },

    /// Microsoft Teams webhook
    Teams {
        /// Webhook URL
        webhook_url: String,
    },
}

/// Load configuration from file
pub fn load_config(path: &str) -> anyhow::Result<AlertConfig> {
    let content = std::fs::read_to_string(path)?;
    let mut config: AlertConfig = serde_yaml::from_str(&content)?;
    
    // Expand environment variables in channel configs
    expand_env_vars(&mut config);
    
    Ok(config)
}

/// Load from environment variables
pub fn from_env() -> AlertConfig {
    let mut config = AlertConfig::default();
    
    // Check for basic Slack setup
    if let Ok(webhook_url) = std::env::var("SLACK_WEBHOOK_URL") {
        config.channels.insert(
            "slack".to_string(),
            ChannelConfig::Slack {
                webhook_url,
                channel: std::env::var("SLACK_CHANNEL").ok(),
                username: std::env::var("SLACK_USERNAME")
                    .unwrap_or_else(|_| "Fluxx Agent".to_string()),
                icon_emoji: std::env::var("SLACK_ICON")
                    .unwrap_or_else(|_| ":warning:".to_string()),
            },
        );
    }
    
    // Check for email setup
    if let (Ok(smtp_host), Ok(username), Ok(password)) = (
        std::env::var("SMTP_HOST"),
        std::env::var("SMTP_USER"),
        std::env::var("SMTP_PASS"),
    ) {
        let to: Vec<String> = std::env::var("ALERT_EMAIL_TO")
            .map(|s| s.split(',').map(|e| e.trim().to_string()).collect())
            .unwrap_or_default();
        
        if !to.is_empty() {
            config.channels.insert(
                "email".to_string(),
                ChannelConfig::Email {
                    smtp_host,
                    smtp_port: std::env::var("SMTP_PORT")
                        .ok()
                        .and_then(|p| p.parse().ok())
                        .unwrap_or(587),
                    username,
                    password,
                    use_tls: std::env::var("SMTP_USE_TLS")
                        .map(|v| v == "true" || v == "1")
                        .unwrap_or(true),
                    from: std::env::var("ALERT_EMAIL_FROM")
                        .unwrap_or_else(|_| "alerts@fluxx.dev".to_string()),
                    to,
                    cc: vec![],
                },
            );
        }
    }
    
    // Check for webhook
    if let Ok(url) = std::env::var("ALERT_WEBHOOK_URL") {
        config.channels.insert(
            "webhook".to_string(),
            ChannelConfig::Webhook {
                url,
                method: std::env::var("ALERT_WEBHOOK_METHOD")
                    .unwrap_or_else(|_| "POST".to_string()),
                headers: HashMap::new(),
                timeout_seconds: 30,
                retry_count: 3,
            },
        );
    }
    
    config
}

/// Expand environment variables in strings
fn expand_env_vars(config: &mut AlertConfig) {
    for (_, channel) in &mut config.channels {
        match channel {
            ChannelConfig::Slack { webhook_url, .. } => {
                *webhook_url = expand_env(webhook_url);
            }
            ChannelConfig::Email { username, password, .. } => {
                *username = expand_env(username);
                *password = expand_env(password);
            }
            ChannelConfig::Webhook { url, headers, .. } => {
                *url = expand_env(url);
                for (_, value) in headers.iter_mut() {
                    *value = expand_env(value);
                }
            }
            ChannelConfig::PagerDuty { integration_key, .. } => {
                *integration_key = expand_env(integration_key);
            }
            ChannelConfig::Discord { webhook_url, .. } => {
                *webhook_url = expand_env(webhook_url);
            }
            ChannelConfig::Teams { webhook_url } => {
                *webhook_url = expand_env(webhook_url);
            }
        }
    }
}

/// Expand ${VAR} or $VAR patterns
fn expand_env(s: &str) -> String {
    let mut result = s.to_string();
    
    // Handle ${VAR} pattern
    while let Some(start) = result.find("${") {
        if let Some(end) = result[start..].find('}') {
            let var_name = &result[start + 2..start + end];
            let var_value = std::env::var(var_name).unwrap_or_default();
            result.replace_range(start..start + end + 1, &var_value);
        } else {
            break;
        }
    }
    
    result
}

/// Generate sample configuration
pub fn generate_sample_config() -> String {
    concat!(
        "# Fluxx Agent Alert Configuration\n\n",
        "alerts:\n",
        "  enabled: true\n",
        "  cooldown_seconds: 300\n",
        "  max_alerts_per_hour: 100\n\n",
        "  rules:\n",
        "    - name: high_error_rate\n",
        "      condition: error_rate > 0.05\n",
        "      severity: critical\n",
        "      channels: [slack]\n\n",
        "  channels:\n",
        "    slack:\n",
        "      type: slack\n",
        "      webhook_url: ${SLACK_WEBHOOK_URL}\n"
    ).to_string()
}

// Default functions for serde
fn default_true() -> bool {
    true
}

fn default_cooldown() -> u64 {
    300
}

fn default_max_alerts() -> u32 {
    100
}

fn default_slack_username() -> String {
    "Fluxx Agent".to_string()
}

fn default_slack_icon() -> String {
    ":warning:".to_string()
}

fn default_smtp_port() -> u16 {
    587
}

fn default_webhook_method() -> String {
    "POST".to_string()
}

fn default_webhook_timeout() -> u64 {
    30
}

fn default_webhook_retries() -> u32 {
    3
}

fn default_pagerduty_url() -> String {
    "https://events.pagerduty.com/v2/enqueue".to_string()
}

fn default_discord_username() -> String {
    "Fluxx Agent".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AlertConfig::default();
        assert!(config.enabled);
        assert_eq!(config.cooldown_seconds, 300);
        assert_eq!(config.max_alerts_per_hour, 100);
    }

    #[test]
    fn test_severity_priority() {
        assert!(AlertSeverity::Critical.priority() > AlertSeverity::Warning.priority());
        assert!(AlertSeverity::Warning.priority() > AlertSeverity::Info.priority());
    }

    #[derive(Deserialize)]
    struct ConfigWrapper {
        alerts: AlertConfig,
    }

    #[test]
    fn test_yaml_parse() {
        let yaml = r#"
alerts:
  enabled: true
  cooldown_seconds: 600
  rules:
    - name: test_rule
      condition: "error_rate > 0.1"
      severity: critical
      channels: [slack]
  channels:
    slack:
      type: slack
      webhook_url: "https://hooks.slack.com/test"
"#;

        // Parse as wrapped config
        let wrapper: ConfigWrapper = serde_yaml::from_str(yaml).unwrap();
        let config = wrapper.alerts;
        assert_eq!(config.cooldown_seconds, 600);
        assert_eq!(config.rules.len(), 1);
        assert!(config.channels.contains_key("slack"));
    }

    #[test]
    fn test_expand_env() {
        unsafe {
            std::env::set_var("TEST_VAR", "test_value");
        }
        assert_eq!(expand_env("${TEST_VAR}"), "test_value");
        assert_eq!(expand_env("prefix_${TEST_VAR}_suffix"), "prefix_test_value_suffix");
        unsafe {
            std::env::remove_var("TEST_VAR");
        }
    }
}
