//! Atlassian Bamboo Integration
//!
//! Monitor Bamboo builds, deployments, and agents.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Bamboo integration
pub struct BambooIntegration {
    config: IntegrationConfig,
    base_url: String,
    username: String,
    password: String,
}

impl BambooIntegration {
    /// Create new Bamboo integration
    pub fn new(base_url: &str, username: &str, password: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cicd_provider".to_string(), "bamboo".to_string());

        let config = IntegrationConfig {
            name: "bamboo".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            base_url: base_url.trim_end_matches('/').to_string(),
            username: username.to_string(),
            password: password.to_string(),
        })
    }

    /// Check if Bamboo is accessible
    async fn check_bamboo_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/rest/api/latest/server.json", self.base_url);

        match client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Collect build metrics
    async fn collect_build_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/rest/api/latest/result.json?max-result=100", self.base_url);

        let response = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
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

        if let Some(results) = data["results"]["result"].as_array() {
            let mut state_counts: HashMap<String, u64> = HashMap::new();

            for result in results {
                if let Some(state) = result["buildState"].as_str() {
                    *state_counts.entry(state.to_string()).or_insert(0) += 1;
                }
                if let Some(life_cycle) = result["lifeCycleState"].as_str() {
                    if life_cycle == "InProgress" {
                        *state_counts.entry("in_progress".to_string()).or_insert(0) += 1;
                    }
                }
            }

            for (state, count) in &state_counts {
                metrics.push(Metric::gauge(
                    format!("bamboo.builds.{}", state.to_lowercase()),
                    *count as f64,
                    self.name(),
                ));
            }

            metrics.push(Metric::gauge(
                "bamboo.builds.total",
                results.len() as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect agent metrics
    async fn collect_agent_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/rest/api/latest/agent.json", self.base_url);

        let response = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
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

        if let Some(agents) = data["agents"].as_array() {
            let mut enabled = 0u64;
            let mut busy = 0u64;

            for agent in agents {
                if agent["enabled"].as_bool() == Some(true) {
                    enabled += 1;
                }
                if agent["busy"].as_bool() == Some(true) {
                    busy += 1;
                }
            }

            metrics.push(Metric::gauge(
                "bamboo.agents.total",
                agents.len() as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "bamboo.agents.enabled",
                enabled as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "bamboo.agents.busy",
                busy as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all Bamboo metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_bamboo_access().await {
            all_metrics.push(Metric::gauge(
                "bamboo.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "bamboo.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_build_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect build metrics: {}", e),
        }

        match self.collect_agent_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect agent metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Bamboo events
    async fn collect_bamboo_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_bamboo_access().await {
            events.push(
                Event::new(
                    "Bamboo Unavailable",
                    format!("Cannot connect to Bamboo at {}", self.base_url),
                    EventSeverity::Critical,
                    self.name(),
                    "api_unavailable",
                ),
            );
            return Ok(events);
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for BambooIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_bamboo_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_bamboo_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Bamboo at {}",
                self.base_url
            )))
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
    fn test_bamboo_integration_creation() {
        let bamboo = BambooIntegration::new("http://bamboo.example.com", "user", "pass");
        assert!(bamboo.is_ok());
    }
}
