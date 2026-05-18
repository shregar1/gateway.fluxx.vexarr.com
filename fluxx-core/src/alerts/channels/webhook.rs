//! Generic Webhook Alert Channel
//!
//! Send alerts to any HTTP endpoint.
//!
//! ## Configuration
//!
//! ```yaml
//! channels:
//!   webhook:
//!     type: webhook
//!     url: "https://api.company.com/alerts"
//!     method: POST
//!     headers:
//!       Authorization: "Bearer ${API_TOKEN}"
//!       Content-Type: "application/json"
//!     timeout_seconds: 30
//!     retry_count: 3
//! ```

use super::{AlertChannel, ChannelError, ChannelResult, ChannelType};
use crate::alerts::config::ChannelConfig;
use crate::alerts::models::Alert;
use async_trait::async_trait;
use serde_json::json;
use std::collections::HashMap;

/// Generic webhook channel
#[allow(dead_code)]
pub struct WebhookChannel {
    name: String,
    url: String,
    method: String,
    headers: HashMap<String, String>,
    timeout_seconds: u64,
    retry_count: u32,
    client: reqwest::Client,
}

impl WebhookChannel {
    /// Create new webhook channel from configuration
    pub fn new(name: &str, config: &ChannelConfig) -> ChannelResult<Self> {
        match config {
            ChannelConfig::Webhook {
                url,
                method,
                headers,
                timeout_seconds,
                retry_count,
            } => {
                if url.is_empty() {
                    return Err(ChannelError::Config(
                        "Webhook URL is required".to_string()
                    ));
                }

                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(*timeout_seconds))
                    .build()?;

                Ok(Self {
                    name: name.to_string(),
                    url: url.clone(),
                    method: method.to_uppercase(),
                    headers: headers.clone(),
                    timeout_seconds: *timeout_seconds,
                    retry_count: *retry_count,
                    client,
                })
            }
            _ => Err(ChannelError::Config(
                "Invalid config for Webhook channel".to_string()
            )),
        }
    }

    /// Build the webhook payload
    fn build_payload(&self, alert: &Alert) -> serde_json::Value {
        let timestamp = chrono::DateTime::from_timestamp_micros(alert.timestamp_us as i64)
            .map(|d| d.to_rfc3339())
            .unwrap_or_else(|| "Unknown".to_string());

        json!({
            "alert_id": alert.id,
            "severity": alert.severity,
            "severity_priority": alert.severity.priority(),
            "message": alert.message,
            "source": alert.source,
            "timestamp_us": alert.timestamp_us,
            "timestamp_iso": timestamp,
            "channel": self.name,
            "emoji": alert.severity.emoji(),
        })
    }

    /// Send request with retries
    async fn send_with_retry(
        &self,
        payload: &serde_json::Value,
    ) -> ChannelResult<()> {
        let mut last_error = None;

        for attempt in 0..=self.retry_count {
            if attempt > 0 {
                tracing::info!(
                    channel = %self.name,
                    attempt = attempt + 1,
                    max_attempts = self.retry_count + 1,
                    "Retrying webhook send"
                );
                
                // Exponential backoff: 1s, 2s, 4s, 8s...
                tokio::time::sleep(std::time::Duration::from_secs(2_u64.pow(attempt - 1))).await;
            }

            match self.send_request(payload).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::warn!(
                        channel = %self.name,
                        attempt = attempt + 1,
                        error = %e,
                        "Webhook send failed"
                    );
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            ChannelError::Config("All retry attempts failed".to_string())
        }))
    }

    /// Send single request
    async fn send_request(&self, payload: &serde_json::Value) -> ChannelResult<()> {
        let mut request = self.client
            .request(
                match self.method.as_str() {
                    "GET" => reqwest::Method::GET,
                    "POST" => reqwest::Method::POST,
                    "PUT" => reqwest::Method::PUT,
                    "PATCH" => reqwest::Method::PATCH,
                    _ => reqwest::Method::POST,
                },
                &self.url,
            );

        // Add headers
        for (key, value) in &self.headers {
            request = request.header(key, value);
        }

        // Add default content-type if not specified
        if !self.headers.contains_key("Content-Type") {
            request = request.header("Content-Type", "application/json");
        }

        // Add body for non-GET requests
        if self.method != "GET" {
            request = request.json(payload);
        }

        let response = request.send().await?;
        let status = response.status();

        if status.is_success() {
            tracing::info!(
                channel = %self.name,
                status = %status,
                "Webhook sent successfully"
            );
            Ok(())
        } else {
            let body = response.text().await?;
            Err(ChannelError::Api {
                status: status.as_u16(),
                message: body,
            })
        }
    }
}

#[async_trait]
impl AlertChannel for WebhookChannel {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, alert: &Alert) -> ChannelResult<()> {
        let payload = self.build_payload(alert);
        self.send_with_retry(&payload).await
    }

    async fn health_check(&self) -> ChannelResult<()> {
        // Send a test payload
        let test_payload = json!({
            "test": true,
            "message": "Fluxx Agent webhook health check",
            "channel": self.name,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        match self.send_request(&test_payload).await {
            Ok(()) => {
                tracing::info!(
                    channel = %self.name,
                    url = %self.url,
                    "Webhook health check passed"
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    channel = %self.name,
                    url = %self.url,
                    error = %e,
                    "Webhook health check failed"
                );
                Err(e)
            }
        }
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::Webhook
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerts::config::AlertSeverity;

    fn create_test_alert() -> Alert {
        Alert {
            id: "test-123".to_string(),
            severity: AlertSeverity::Warning,
            message: "Test webhook alert".to_string(),
            source: "test_source".to_string(),
            timestamp_us: 1704067200000000,
        }
    }

    fn create_test_config() -> ChannelConfig {
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer test-token".to_string());
        
        ChannelConfig::Webhook {
            url: "https://api.example.com/alerts".to_string(),
            method: "POST".to_string(),
            headers,
            timeout_seconds: 30,
            retry_count: 3,
        }
    }

    #[test]
    fn test_build_payload() {
        let config = create_test_config();
        let channel = WebhookChannel::new("webhook", &config).unwrap();
        let alert = create_test_alert();
        let payload = channel.build_payload(&alert);

        assert_eq!(payload["alert_id"], "test-123");
        assert_eq!(payload["severity_priority"], 2);
        assert_eq!(payload["source"], "test_source");
    }

    #[test]
    fn test_method_normalization() {
        let mut headers = HashMap::new();
        
        let config = ChannelConfig::Webhook {
            url: "https://api.example.com/alerts".to_string(),
            method: "post".to_string(), // lowercase
            headers,
            timeout_seconds: 30,
            retry_count: 0,
        };

        let channel = WebhookChannel::new("webhook", &config).unwrap();
        assert_eq!(channel.method, "POST");
    }
}
