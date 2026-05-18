//! Apache Pulsar Integration
//!
//! Monitor Pulsar brokers, topics, subscriptions, and message rates.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Pulsar integration
pub struct PulsarIntegration {
    config: IntegrationConfig,
    admin_url: String,
}

impl PulsarIntegration {
    /// Create new Pulsar integration
    pub fn new(admin_url: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("queue_type".to_string(), "pulsar".to_string());

        let config = IntegrationConfig {
            name: "pulsar".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            admin_url: admin_url.trim_end_matches('/').to_string(),
        })
    }

    /// Check if Pulsar is accessible
    async fn check_pulsar_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/admin/v2/brokers/health", self.admin_url);

        match client
            .get(&url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Collect broker metrics
    async fn collect_broker_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/admin/v2/brokers/metrics", self.admin_url);

        let response = client
            .get(&url)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let data: Vec<serde_json::Value> = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        for metric in data {
            if let (Some(name), Some(value)) = (
                metric["name"].as_str(),
                metric["value"].as_f64(),
            ) {
                metrics.push(Metric::gauge(
                    format!("pulsar.broker.{}", name),
                    value,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect tenant and namespace metrics
    async fn collect_tenant_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();

        // Get tenants
        let url = format!("{}/admin/v2/tenants", self.admin_url);
        let response = client
            .get(&url)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let tenants: Vec<String> = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        metrics.push(Metric::gauge(
            "pulsar.tenants.total",
            tenants.len() as f64,
            self.name(),
        ));

        // Count namespaces
        let mut total_namespaces = 0u64;
        for tenant in tenants.iter().take(10) {
            // Limit to avoid too many requests
            let url = format!("{}/admin/v2/namespaces/{}", self.admin_url, tenant);
            if let Ok(response) = client
                .get(&url)
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await
            {
                if response.status().is_success() {
                    if let Ok(namespaces) = response.json::<Vec<String>>().await {
                        total_namespaces += namespaces.len() as u64;
                    }
                }
            }
        }

        metrics.push(Metric::gauge(
            "pulsar.namespaces.total",
            total_namespaces as f64,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect all Pulsar metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_pulsar_access().await {
            all_metrics.push(Metric::gauge(
                "pulsar.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "pulsar.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_broker_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect broker metrics: {}", e),
        }

        match self.collect_tenant_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect tenant metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Pulsar events
    async fn collect_pulsar_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_pulsar_access().await {
            events.push(
                Event::new(
                    "Pulsar Unavailable",
                    format!("Cannot connect to Pulsar at {}", self.admin_url),
                    EventSeverity::Critical,
                    self.name(),
                    "pulsar_unavailable",
                ),
            );
            return Ok(events);
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for PulsarIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_pulsar_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_pulsar_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Pulsar at {}",
                self.admin_url
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
    fn test_pulsar_integration_creation() {
        let pulsar = PulsarIntegration::new("http://localhost:8080");
        assert!(pulsar.is_ok());
    }
}
