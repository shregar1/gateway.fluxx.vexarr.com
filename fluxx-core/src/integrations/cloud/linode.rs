//! Linode (Akamai) Integration
//!
//! Monitor Linode instances, Kubernetes, volumes, and NodeBalancers.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Linode integration
pub struct LinodeIntegration {
    config: IntegrationConfig,
    api_token: String,
}

impl LinodeIntegration {
    /// Create new Linode integration
    pub fn new(api_token: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cloud_provider".to_string(), "linode".to_string());

        let config = IntegrationConfig {
            name: "linode".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            api_token: api_token.to_string(),
        })
    }

    /// Check if Linode API is accessible
    async fn check_linode_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = "https://api.linode.com/v4/account";

        match client
            .get(url)
            .header("Authorization", format!("Bearer {}", self.api_token))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Collect Linode instance metrics
    async fn collect_instance_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = "https://api.linode.com/v4/linode/instances";

        let response = client
            .get(url)
            .header("Authorization", format!("Bearer {}", self.api_token))
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

        if let Some(instances) = data["data"].as_array() {
            let mut running = 0u64;
            let mut offline = 0u64;

            for instance in instances {
                if let Some(status) = instance["status"].as_str() {
                    match status {
                        "running" => running += 1,
                        "offline" => offline += 1,
                        _ => {}
                    }
                }
            }

            metrics.push(Metric::gauge(
                "linode.instances.total",
                instances.len() as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "linode.instances.running",
                running as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "linode.instances.offline",
                offline as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect LKE (Linode Kubernetes Engine) metrics
    async fn collect_lke_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = "https://api.linode.com/v4/lke/clusters";

        let response = client
            .get(url)
            .header("Authorization", format!("Bearer {}", self.api_token))
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

        if let Some(clusters) = data["data"].as_array() {
            let mut ready = 0u64;

            for cluster in clusters {
                if let Some(status) = cluster["status"].as_str() {
                    if status == "ready" {
                        ready += 1;
                    }
                }
            }

            metrics.push(Metric::gauge(
                "linode.lke.clusters.total",
                clusters.len() as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "linode.lke.clusters.ready",
                ready as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all Linode metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_linode_access().await {
            all_metrics.push(Metric::gauge(
                "linode.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "linode.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_instance_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect instance metrics: {}", e),
        }

        match self.collect_lke_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect LKE metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Linode events
    async fn collect_linode_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_linode_access().await {
            events.push(
                Event::new(
                    "Linode API Unavailable",
                    "Cannot connect to Linode API".to_string(),
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
impl Integration for LinodeIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_linode_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_linode_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "Linode API token invalid".to_string()
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
    fn test_linode_integration_creation() {
        let linode = LinodeIntegration::new("token123");
        assert!(linode.is_ok());
    }
}
