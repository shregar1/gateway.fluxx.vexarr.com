//! Zipkin Integration
//!
//! Monitor Zipkin tracing system and collector.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Zipkin integration
pub struct ZipkinIntegration {
    config: IntegrationConfig,
    base_url: String,
}

impl ZipkinIntegration {
    /// Create new Zipkin integration
    pub fn new(base_url: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("observability_type".to_string(), "zipkin".to_string());

        let config = IntegrationConfig {
            name: "zipkin".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    /// Check if Zipkin is accessible
    async fn check_zipkin_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/api/v2/services", self.base_url);

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

    /// Collect service metrics
    async fn collect_service_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/api/v2/services", self.base_url);

        let response = client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let services: Vec<String> = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        metrics.push(Metric::gauge(
            "zipkin.services.total",
            services.len() as f64,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect all Zipkin metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_zipkin_access().await {
            all_metrics.push(Metric::gauge(
                "zipkin.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "zipkin.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_service_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect service metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Zipkin events
    async fn collect_zipkin_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_zipkin_access().await {
            events.push(
                Event::new(
                    "Zipkin Unavailable",
                    format!("Cannot connect to Zipkin at {}", self.base_url),
                    EventSeverity::Critical,
                    self.name(),
                    "zipkin_unavailable",
                ),
            );
            return Ok(events);
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for ZipkinIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_zipkin_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_zipkin_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Zipkin at {}",
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
    fn test_zipkin_integration_creation() {
        let zipkin = ZipkinIntegration::new("http://localhost:9411");
        assert!(zipkin.is_ok());
    }
}
