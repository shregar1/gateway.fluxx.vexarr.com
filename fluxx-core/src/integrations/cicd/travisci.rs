//! Travis CI Integration
//!
//! Monitor Travis CI builds and repositories.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Travis CI integration
pub struct TravisCiIntegration {
    config: IntegrationConfig,
    api_token: String,
    owner: String,
    repo: Option<String>,
}

impl TravisCiIntegration {
    /// Create new Travis CI integration for a repository
    pub fn new_repo(api_token: &str, owner: &str, repo: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cicd_provider".to_string(), "travisci".to_string());

        let config = IntegrationConfig {
            name: format!("travisci_{}_{}", owner, repo),
            interval_secs: 300,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            api_token: api_token.to_string(),
            owner: owner.to_string(),
            repo: Some(repo.to_string()),
        })
    }

    /// Create new Travis CI integration for an owner
    pub fn new_owner(api_token: &str, owner: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cicd_provider".to_string(), "travisci".to_string());

        let config = IntegrationConfig {
            name: format!("travisci_{}", owner),
            interval_secs: 300,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            api_token: api_token.to_string(),
            owner: owner.to_string(),
            repo: None,
        })
    }

    /// Check if Travis CI API is accessible
    async fn check_travis_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = "https://api.travis-ci.com/user";

        match client
            .get(url)
            .header("Authorization", format!("token {}", self.api_token))
            .header("Travis-API-Version", "3")
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
        let url = if let Some(ref repo) = self.repo {
            format!(
                "https://api.travis-ci.com/repo/{}/{}/builds",
                self.owner, repo
            )
        } else {
            format!("https://api.travis-ci.com/owner/{}/builds", self.owner)
        };

        let response = client
            .get(&url)
            .header("Authorization", format!("token {}", self.api_token))
            .header("Travis-API-Version", "3")
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

        if let Some(builds) = data["builds"].as_array() {
            let mut state_counts: HashMap<String, u64> = HashMap::new();

            for build in builds {
                if let Some(state) = build["state"].as_str() {
                    *state_counts.entry(state.to_string()).or_insert(0) += 1;
                }
            }

            for (state, count) in &state_counts {
                metrics.push(Metric::gauge(
                    format!("travisci.builds.{}", state),
                    *count as f64,
                    self.name(),
                ));
            }

            metrics.push(Metric::gauge(
                "travisci.builds.total",
                builds.len() as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all Travis CI metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_travis_access().await {
            all_metrics.push(Metric::gauge(
                "travisci.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "travisci.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_build_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect build metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Travis CI events
    async fn collect_travis_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_travis_access().await {
            events.push(
                Event::new(
                    "Travis CI Unavailable",
                    "Cannot connect to Travis CI API".to_string(),
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
impl Integration for TravisCiIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_travis_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_travis_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "Travis CI token invalid".to_string()
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
    fn test_travis_ci_integration_creation() {
        let travis = TravisCiIntegration::new_repo("token123", "myorg", "myrepo");
        assert!(travis.is_ok());
    }
}
