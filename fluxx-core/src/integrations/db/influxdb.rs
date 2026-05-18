//! InfluxDB Integration
//!
//! Monitor InfluxDB time-series database, databases, and writes.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// InfluxDB integration
pub struct InfluxDbIntegration {
    config: IntegrationConfig,
    base_url: String,
    org: String,
    token: String,
}

impl InfluxDbIntegration {
    /// Create new InfluxDB integration (v2 API)
    pub fn new(base_url: &str, org: &str, token: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("db_type".to_string(), "influxdb".to_string());

        let config = IntegrationConfig {
            name: "influxdb".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            base_url: base_url.trim_end_matches('/').to_string(),
            org: org.to_string(),
            token: token.to_string(),
        })
    }

    /// Check if InfluxDB is accessible
    async fn check_influxdb_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/api/v2/health", self.base_url);

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

    /// Collect health metrics
    async fn collect_health_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/api/v2/health", self.base_url);

        let response = client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if response.status().is_success() {
            metrics.push(Metric::gauge(
                "influxdb.health.status",
                1.0,
                self.name(),
            ));
        } else {
            metrics.push(Metric::gauge(
                "influxdb.health.status",
                0.0,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect bucket metrics
    async fn collect_bucket_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/api/v2/buckets?org={}", self.base_url, self.org);

        let response = client
            .get(&url)
            .header("Authorization", format!("Token {}", self.token))
            .timeout(std::time::Duration::from_secs(10))
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

        if let Some(buckets) = data["buckets"].as_array() {
            metrics.push(Metric::gauge(
                "influxdb.buckets.total",
                buckets.len() as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all InfluxDB metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_influxdb_access().await {
            all_metrics.push(Metric::gauge(
                "influxdb.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "influxdb.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_health_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect health metrics: {}", e),
        }

        match self.collect_bucket_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect bucket metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect InfluxDB events
    async fn collect_influxdb_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_influxdb_access().await {
            events.push(
                Event::new(
                    "InfluxDB Unavailable",
                    format!("Cannot connect to InfluxDB at {}", self.base_url),
                    EventSeverity::Critical,
                    self.name(),
                    "influxdb_unavailable",
                ),
            );
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for InfluxDbIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_influxdb_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_influxdb_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to InfluxDB at {}",
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
    fn test_influxdb_integration_creation() {
        let influxdb = InfluxDbIntegration::new("http://localhost:8086", "myorg", "token123");
        assert!(influxdb.is_ok());
    }
}
