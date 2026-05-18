//! Alert Manager
//!
//! Centralized alert management with:
//! - Rate limiting
//! - Deduplication
//! - State tracking
//! - Multi-channel delivery

use crate::alerts::channels::{create_channel, AlertChannel};
use crate::alerts::config::{AlertConfig, AlertRuleConfig, AlertSeverity};
use crate::alerts::models::{Alert, AlertState, TrackedAlert};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Alert Manager handles alert lifecycle
pub struct AlertManager {
    config: AlertConfig,
    channels: Vec<Box<dyn AlertChannel>>,
    
    // State tracking
    tracked_alerts: Arc<RwLock<HashMap<String, TrackedAlert>>>,
    
    // Deduplication
    recent_digests: Arc<RwLock<HashMap<String, Instant>>>,
    
    // Rate limiting
    alert_count: Arc<RwLock<Vec<Instant>>>,
}

impl AlertManager {
    /// Create new alert manager from configuration
    pub async fn new(config: AlertConfig) -> anyhow::Result<Self> {
        if !config.enabled {
            return Ok(Self {
                config,
                channels: vec![],
                tracked_alerts: Arc::new(RwLock::new(HashMap::new())),
                recent_digests: Arc::new(RwLock::new(HashMap::new())),
                alert_count: Arc::new(RwLock::new(vec![])),
            });
        }

        // Initialize channels
        let mut channels: Vec<Box<dyn AlertChannel>> = vec![];
        
        for (name, channel_config) in &config.channels {
            match create_channel(name, channel_config) {
                Ok(channel) => {
                    tracing::info!(channel = %name, "Alert channel initialized");
                    channels.push(channel);
                }
                Err(e) => {
                    tracing::error!(
                        channel = %name,
                        error = %e,
                        "Failed to initialize alert channel"
                    );
                }
            }
        }

        tracing::info!(
            channels = channels.len(),
            rules = config.rules.len(),
            "Alert manager initialized"
        );

        Ok(Self {
            config,
            channels,
            tracked_alerts: Arc::new(RwLock::new(HashMap::new())),
            recent_digests: Arc::new(RwLock::new(HashMap::new())),
            alert_count: Arc::new(RwLock::new(vec![])),
        })
    }

    /// Process an alert through the manager
    pub async fn process_alert(&self, alert: Alert, rule: &AlertRuleConfig) -> bool {
        // Check rate limits
        if !self.check_rate_limit().await {
            tracing::warn!(
                alert_id = %alert.id,
                "Alert dropped due to rate limiting"
            );
            return false;
        }

        // Check deduplication
        let digest = alert.digest();
        if !self.check_cooldown(&digest, rule).await {
            tracing::debug!(
                alert_id = %alert.id,
                digest = %digest,
                "Alert deduplicated (cooldown)"
            );
            return false;
        }

        // Track the alert
        let tracked = TrackedAlert::new(alert.clone());
        let alert_id = alert.id.clone();
        
        {
            let mut alerts = self.tracked_alerts.write().await;
            alerts.insert(alert_id.clone(), tracked);
        }

        // Send to configured channels
        let channels_to_notify = self.get_channels_for_rule(rule);
        let mut sent = false;

        for channel_name in channels_to_notify {
            if let Some(channel) = self.get_channel(&channel_name) {
                match channel.send(&alert).await {
                    Ok(()) => {
                        sent = true;
                        // Update notification count
                        let mut alerts = self.tracked_alerts.write().await;
                        if let Some(tracked) = alerts.get_mut(&alert_id) {
                            tracked.increment_notification();
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            channel = %channel_name,
                            alert_id = %alert.id,
                            error = %e,
                            "Failed to send alert"
                        );
                    }
                }
            } else {
                tracing::warn!(
                    channel = %channel_name,
                    "Channel not found"
                );
            }
        }

        if sent {
            tracing::info!(
                alert_id = %alert.id,
                severity = ?alert.severity,
                "Alert sent successfully"
            );
        }

        sent
    }

    /// Trigger an alert directly (bypass rules)
    pub async fn trigger(
        &self,
        severity: AlertSeverity,
        message: impl Into<String>,
        source: impl Into<String>,
        channel_names: Vec<String>,
    ) -> Option<String> {
        let alert = Alert::new(severity, message, source);
        let alert_id = alert.id.clone();
        
        // Create a temporary rule config for this alert
        let rule = AlertRuleConfig {
            name: "direct".to_string(),
            description: "Direct trigger".to_string(),
            condition: "".to_string(),
            severity,
            channels: channel_names,
            labels: HashMap::new(),
            cooldown_seconds: None,
        };

        if self.process_alert(alert, &rule).await {
            Some(alert_id)
        } else {
            None
        }
    }

    /// Acknowledge an alert
    pub async fn acknowledge(&self, alert_id: &str, by: impl Into<String>) -> bool {
        let mut alerts = self.tracked_alerts.write().await;
        
        if let Some(tracked) = alerts.get_mut(alert_id) {
            tracked.acknowledge(by);
            tracing::info!(alert_id = %alert_id, "Alert acknowledged");
            true
        } else {
            false
        }
    }

    /// Resolve an alert
    pub async fn resolve(&self, alert_id: &str) -> bool {
        let mut alerts = self.tracked_alerts.write().await;
        
        if let Some(tracked) = alerts.get_mut(alert_id) {
            tracked.resolve();
            tracing::info!(alert_id = %alert_id, "Alert resolved");
            true
        } else {
            false
        }
    }

    /// Get all tracked alerts
    pub async fn get_alerts(&self, state: Option<AlertState>) -> Vec<TrackedAlert> {
        let alerts = self.tracked_alerts.read().await;
        
        alerts
            .values()
            .filter(|t| state.map_or(true, |s| t.state == s))
            .cloned()
            .collect()
    }

    /// Get alert by ID
    pub async fn get_alert(&self, alert_id: &str) -> Option<TrackedAlert> {
        let alerts = self.tracked_alerts.read().await;
        alerts.get(alert_id).cloned()
    }

    /// Run health checks on all channels
    pub async fn health_check(&self) -> HashMap<String, bool> {
        let mut results = HashMap::new();
        
        for channel in &self.channels {
            let name = channel.name().to_string();
            let healthy = channel.health_check().await.is_ok();
            results.insert(name, healthy);
        }
        
        results
    }

    /// Check rate limits
    async fn check_rate_limit(&self) -> bool {
        let now = Instant::now();
        let window = Duration::from_secs(3600); // 1 hour
        
        let mut count = self.alert_count.write().await;
        
        // Remove old entries
        count.retain(|t| now.duration_since(*t) < window);
        
        // Check limit
        if count.len() >= self.config.max_alerts_per_hour as usize {
            return false;
        }
        
        // Add current alert
        count.push(now);
        true
    }

    /// Check cooldown for deduplication
    async fn check_cooldown(&self, digest: &str, rule: &AlertRuleConfig) -> bool {
        let cooldown = Duration::from_secs(
            rule.cooldown_seconds.unwrap_or(self.config.cooldown_seconds)
        );
        
        let now = Instant::now();
        let mut digests = self.recent_digests.write().await;
        
        // Clean old entries
        digests.retain(|_, t| now.duration_since(*t) < cooldown * 2);
        
        // Check if we've seen this digest recently
        if let Some(last_seen) = digests.get(digest) {
            if now.duration_since(*last_seen) < cooldown {
                return false; // Still in cooldown
            }
        }
        
        // Update timestamp
        digests.insert(digest.to_string(), now);
        true
    }

    /// Get channel by name
    fn get_channel(&self, name: &str) -> Option<&Box<dyn AlertChannel>> {
        self.channels.iter().find(|c| c.name() == name)
    }

    /// Get channel names for a rule
    fn get_channels_for_rule(&self, rule: &AlertRuleConfig) -> Vec<String> {
        // Filter to only channels that exist
        rule.channels
            .iter()
            .filter(|c| self.channels.iter().any(|ch| ch.name() == *c))
            .cloned()
            .collect()
    }

    /// Clean up old resolved alerts
    pub async fn cleanup(&self, max_age: Duration) -> usize {
        let now = chrono::Utc::now().timestamp_micros() as u64;
        let max_age_us = max_age.as_micros() as u64;
        
        let mut alerts = self.tracked_alerts.write().await;
        let initial_count = alerts.len();
        
        alerts.retain(|_, tracked| {
            let age = now - tracked.created_at;
            // Keep if not resolved and age < max, or if resolved recently
            tracked.state != AlertState::Resolved || age < max_age_us
        });
        
        initial_count - alerts.len()
    }
}

/// Builder for AlertManager
pub struct AlertManagerBuilder {
    config: AlertConfig,
}

impl AlertManagerBuilder {
    pub fn new() -> Self {
        Self {
            config: AlertConfig::default(),
        }
    }

    pub fn with_config(mut self, config: AlertConfig) -> Self {
        self.config = config;
        self
    }

    pub fn add_slack(mut self, name: &str, webhook_url: &str) -> Self {
        self.config.channels.insert(
            name.to_string(),
            crate::alerts::config::ChannelConfig::Slack {
                webhook_url: webhook_url.to_string(),
                channel: None,
                username: "Fluxx Agent".to_string(),
                icon_emoji: ":warning:".to_string(),
            },
        );
        self
    }

    pub fn add_webhook(mut self, name: &str, url: &str) -> Self {
        self.config.channels.insert(
            name.to_string(),
            crate::alerts::config::ChannelConfig::Webhook {
                url: url.to_string(),
                method: "POST".to_string(),
                headers: std::collections::HashMap::new(),
                timeout_seconds: 30,
                retry_count: 3,
            },
        );
        self
    }

    pub async fn build(self) -> anyhow::Result<AlertManager> {
        AlertManager::new(self.config).await
    }
}

impl Default for AlertManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_alert_manager_disabled() {
        let mut config = AlertConfig::default();
        config.enabled = false;
        
        let manager = AlertManager::new(config).await.unwrap();
        assert!(manager.channels.is_empty());
    }

    #[tokio::test]
    async fn test_rate_limiting() {
        let mut config = AlertConfig::default();
        config.enabled = true;
        config.max_alerts_per_hour = 2;
        
        let manager = AlertManager::new(config).await.unwrap();
        
        // First two should pass
        assert!(manager.check_rate_limit().await);
        assert!(manager.check_rate_limit().await);
        // Third should fail
        assert!(!manager.check_rate_limit().await);
    }

    #[tokio::test]
    async fn test_cooldown() {
        let config = AlertConfig::default();
        let manager = AlertManager::new(config).await.unwrap();
        
        let rule = AlertRuleConfig {
            name: "test".to_string(),
            description: "Test rule".to_string(),
            condition: "".to_string(),
            severity: AlertSeverity::Warning,
            channels: vec![],
            labels: HashMap::new(),
            cooldown_seconds: Some(60),
        };
        
        let digest = "test:digest:123";
        
        // First check should pass
        assert!(manager.check_cooldown(digest, &rule).await);
        // Second check should fail (cooldown)
        assert!(!manager.check_cooldown(digest, &rule).await);
    }
}
