//! New Relic Integration
//!
//! Monitor New Relic alerts, applications, and infrastructure.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// New Relic integration
pub struct NewRelicIntegration {
    config: IntegrationConfig,
    api_key: String,
    account_id: String,
}

impl NewRelicIntegration {
    /// Create new New Relic integration
    pub fn new(api_key: &str, account_id: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("observability_type".to_string(), "newrelic".to_string());

        let config = IntegrationConfig {
            name: "newrelic".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            api_key: api_key.to_string(),
            account_id: account_id.to_string(),
        })
    }

    /// Check if New Relic API is accessible
    async fn check_newrelic_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = "https://api.newrelic.com/graphql";

        let query = r#"{ actor { user { name } } }"#;

        let body = serde_json::json!({
            "query": query
        });

        match client
            .post(url)
            .header("API-Key", &self.api_key)
            .json(&body)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Collect alert policy metrics
    async fn collect_alert_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = "https://api.newrelic.com/graphql";

        let query = r#"
        {
            actor {
                account(id: $accountId) {
                    alerts {
                        policiesSearch {
                            policies {
                                id
                                name
                                incidentPreference
                            }
                        }
                    }
                }
            }
        }"#;

        let body = serde_json::json!({
            "query": query,
            "variables": {
                "accountId": self.account_id.parse::<i64>().unwrap_or(0)
            }
        });

        let response = client
            .post(url)
            .header("API-Key", &self.api_key)
            .json(&body)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let data: serde_json::Value = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        if let Some(policies) = data["data"]["actor"]["account"]["alerts"]["policiesSearch"]["policies"].as_array() {
            metrics.push(Metric::gauge(
                "newrelic.alert_policies.total",
                policies.len() as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all New Relic metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_newrelic_access().await {
            all_metrics.push(Metric::gauge(
                "newrelic.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "newrelic.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_alert_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect alert metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect New Relic events
    async fn collect_newrelic_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_newrelic_access().await {
            events.push(
                Event::new(
                    "New Relic API Unavailable",
                    "Cannot connect to New Relic API".to_string(),
                    EventSeverity::Critical,
                    self.name(),
                    "api_unavailable",
                ),
            );
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for NewRelicIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_newrelic_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_newrelic_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "New Relic API key invalid".to_string()
            ))
        }
    }

    fn config(&self) -> &IntegrationConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_newrelic_integration_creation() {
        let newrelic = NewRelicIntegration::new("api_key", "123456");
        assert!(newrelic.is_ok());
    }
}
