//! Apache Druid Integration
//!
//! Monitor Druid clusters, datasources, and query performance.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Druid integration
pub struct DruidIntegration {
    config: IntegrationConfig,
    router_url: String,
}

impl DruidIntegration {
    /// Create new Druid integration
    pub fn new(router_url: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("db_type".to_string(), "druid".to_string());

        let config = IntegrationConfig {
            name: "druid".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            router_url: router_url.trim_end_matches('/').to_string(),
        })
    }

    /// Check if Druid is accessible
    async fn check_druid_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/status/health", self.router_url);

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

    /// Collect datasource metrics
    async fn collect_datasource_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/druid/coordinator/v1/datasources", self.router_url);

        let response = client
            .get(&url)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let datasources: Vec<String> = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        metrics.push(Metric::gauge(
            "druid.datasources.total",
            datasources.len() as f64,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect all Druid metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_druid_access().await {
            all_metrics.push(Metric::gauge(
                "druid.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "druid.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_datasource_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect datasource metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Druid events
    async fn collect_druid_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_druid_access().await {
            events.push(
                Event::new(
                    "Druid Unavailable",
                    format!("Cannot connect to Druid at {}", self.router_url),
                    EventSeverity::Critical,
                    self.name(),
                    "druid_unavailable",
                ),
            );
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for DruidIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_druid_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_druid_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Druid at {}",
                self.router_url
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
    fn test_druid_integration_creation() {
        let druid = DruidIntegration::new("http://localhost:8888");
        assert!(druid.is_ok());
    }
}
