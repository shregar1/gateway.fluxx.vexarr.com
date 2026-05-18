//! DigitalOcean Integration
//!
//! Monitor DigitalOcean droplets, Kubernetes, databases, and load balancers.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// DigitalOcean integration
pub struct DigitalOceanIntegration {
    config: IntegrationConfig,
    api_token: String,
}

impl DigitalOceanIntegration {
    /// Create new DigitalOcean integration
    pub fn new(api_token: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cloud_provider".to_string(), "digitalocean".to_string());

        let config = IntegrationConfig {
            name: "digitalocean".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            api_token: api_token.to_string(),
        })
    }

    /// Check if DigitalOcean API is accessible
    async fn check_do_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = "https://api.digitalocean.com/v2/account";

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

    /// Collect droplet metrics
    async fn collect_droplet_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = "https://api.digitalocean.com/v2/droplets";

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

        if let Some(droplets) = data["droplets"].as_array() {
            let mut running = 0u64;
            let mut off = 0u64;

            for droplet in droplets {
                if let Some(status) = droplet["status"].as_str() {
                    match status {
                        "active" => running += 1,
                        "off" => off += 1,
                        _ => {}
                    }
                }
            }

            metrics.push(Metric::gauge(
                "digitalocean.droplets.total",
                droplets.len() as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "digitalocean.droplets.running",
                running as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "digitalocean.droplets.off",
                off as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect Kubernetes cluster metrics
    async fn collect_kubernetes_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = "https://api.digitalocean.com/v2/kubernetes/clusters";

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

        if let Some(clusters) = data["kubernetes_clusters"].as_array() {
            let mut running = 0u64;
            let mut provisioning = 0u64;

            for cluster in clusters {
                if let Some(status) = cluster["status"]["state"].as_str() {
                    match status {
                        "running" => running += 1,
                        "provisioning" => provisioning += 1,
                        _ => {}
                    }
                }
            }

            metrics.push(Metric::gauge(
                "digitalocean.kubernetes.clusters.total",
                clusters.len() as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "digitalocean.kubernetes.clusters.running",
                running as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "digitalocean.kubernetes.clusters.provisioning",
                provisioning as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect database metrics
    async fn collect_database_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = "https://api.digitalocean.com/v2/databases";

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

        if let Some(databases) = data["databases"].as_array() {
            let mut online = 0u64;
            let mut creating = 0u64;

            for db in databases {
                if let Some(status) = db["status"].as_str() {
                    match status {
                        "online" => online += 1,
                        "creating" => creating += 1,
                        _ => {}
                    }
                }
            }

            metrics.push(Metric::gauge(
                "digitalocean.databases.total",
                databases.len() as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "digitalocean.databases.online",
                online as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "digitalocean.databases.creating",
                creating as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect load balancer metrics
    async fn collect_load_balancer_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = "https://api.digitalocean.com/v2/load_balancers";

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

        if let Some(lbs) = data["load_balancers"].as_array() {
            let mut active = 0u64;

            for lb in lbs {
                if let Some(status) = lb["status"].as_str() {
                    if status == "active" {
                        active += 1;
                    }
                }
            }

            metrics.push(Metric::gauge(
                "digitalocean.load_balancers.total",
                lbs.len() as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "digitalocean.load_balancers.active",
                active as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all DigitalOcean metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        // Check connectivity first
        if !self.check_do_access().await {
            all_metrics.push(Metric::gauge(
                "digitalocean.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "digitalocean.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_droplet_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect droplet metrics: {}", e),
        }

        match self.collect_kubernetes_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect kubernetes metrics: {}", e),
        }

        match self.collect_database_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect database metrics: {}", e),
        }

        match self.collect_load_balancer_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect load balancer metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect DigitalOcean events
    async fn collect_digitalocean_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check connectivity
        if !self.check_do_access().await {
            events.push(
                Event::new(
                    "DigitalOcean API Unavailable",
                    "Cannot connect to DigitalOcean API".to_string(),
                    EventSeverity::Critical,
                    self.name(),
                    "api_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check for off droplets (potential cost savings)
        let client = reqwest::Client::new();
        let url = "https://api.digitalocean.com/v2/droplets";

        if let Ok(response) = client
            .get(url)
            .header("Authorization", format!("Bearer {}", self.api_token))
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(data) = response.json::<serde_json::Value>().await {
                    if let Some(droplets) = data["droplets"].as_array() {
                        let off_count = droplets
                            .iter()
                            .filter(|d| d["status"].as_str() == Some("off"))
                            .count();

                        if off_count > 0 {
                            events.push(
                                Event::new(
                                    "DigitalOcean Stopped Droplets",
                                    format!("{} droplets are stopped (incurring storage costs)", off_count),
                                    EventSeverity::Info,
                                    self.name(),
                                    "stopped_droplets",
                                )
                                .with_metadata("count", &off_count.to_string()),
                            );
                        }
                    }
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for DigitalOceanIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_digitalocean_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_do_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "DigitalOcean API token invalid".to_string()
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
    fn test_digitalocean_integration_creation() {
        let do_integration = DigitalOceanIntegration::new("token123");
        assert!(do_integration.is_ok());
    }
}
