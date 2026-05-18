//! JetBrains TeamCity Integration
//!
//! Monitor TeamCity builds, agents, and projects.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// TeamCity integration
pub struct TeamCityIntegration {
    config: IntegrationConfig,
    base_url: String,
    username: String,
    password: String,
}

impl TeamCityIntegration {
    /// Create new TeamCity integration
    pub fn new(base_url: &str, username: &str, password: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cicd_provider".to_string(), "teamcity".to_string());

        let config = IntegrationConfig {
            name: "teamcity".to_string(),
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

    /// Check if TeamCity is accessible
    async fn check_teamcity_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/app/rest/server", self.base_url);

        match client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .header("Accept", "application/json")
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
        let url = format!("{}/app/rest/builds?count=100", self.base_url);

        let response = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .header("Accept", "application/json")
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

        if let Some(builds) = data["build"].as_array() {
            let mut status_counts: HashMap<String, u64> = HashMap::new();

            for build in builds {
                if let Some(status) = build["status"].as_str() {
                    *status_counts.entry(status.to_string()).or_insert(0) += 1;
                }
            }

            for (status, count) in &status_counts {
                metrics.push(Metric::gauge(
                    format!("teamcity.builds.{}", status.to_lowercase()),
                    *count as f64,
                    self.name(),
                ));
            }

            metrics.push(Metric::gauge(
                "teamcity.builds.total",
                builds.len() as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect agent metrics
    async fn collect_agent_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/app/rest/agents", self.base_url);

        let response = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .header("Accept", "application/json")
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

        if let Some(agents) = data["agent"].as_array() {
            let mut connected = 0u64;
            let mut enabled = 0u64;

            for agent in agents {
                if agent["connected"].as_bool() == Some(true) {
                    connected += 1;
                }
                if agent["enabled"].as_bool() == Some(true) {
                    enabled += 1;
                }
            }

            metrics.push(Metric::gauge(
                "teamcity.agents.total",
                agents.len() as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "teamcity.agents.connected",
                connected as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "teamcity.agents.enabled",
                enabled as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all TeamCity metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_teamcity_access().await {
            all_metrics.push(Metric::gauge(
                "teamcity.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "teamcity.connection.status",
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

    /// Collect TeamCity events
    async fn collect_teamcity_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_teamcity_access().await {
            events.push(
                Event::new(
                    "TeamCity Unavailable",
                    format!("Cannot connect to TeamCity at {}", self.base_url),
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
impl Integration for TeamCityIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_teamcity_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_teamcity_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to TeamCity at {}",
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
    fn test_teamcity_integration_creation() {
        let teamcity = TeamCityIntegration::new("http://teamcity.example.com", "user", "pass");
        assert!(teamcity.is_ok());
    }
}
