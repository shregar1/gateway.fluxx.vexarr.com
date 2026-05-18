//! Jaeger Integration
//!
//! Monitor Jaeger tracing system, collectors, and query service.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Jaeger integration
pub struct JaegerIntegration {
    config: IntegrationConfig,
    query_url: String,
}

impl JaegerIntegration {
    /// Create new Jaeger integration
    pub fn new(query_url: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("observability_type".to_string(), "jaeger".to_string());

        let config = IntegrationConfig {
            name: "jaeger".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            query_url: query_url.trim_end_matches('/').to_string(),
        })
    }

    /// Check if Jaeger is accessible
    async fn check_jaeger_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/api/services", self.query_url);

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
        let url = format!("{}/api/services", self.query_url);

        let response = client
            .get(&url)
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

        if let Some(services) = data["data"].as_array() {
            metrics.push(Metric::gauge(
                "jaeger.services.total",
                services.len() as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all Jaeger metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_jaeger_access().await {
            all_metrics.push(Metric::gauge(
                "jaeger.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "jaeger.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_service_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect service metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Jaeger events
    async fn collect_jaeger_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_jaeger_access().await {
            events.push(
                Event::new(
                    "Jaeger Unavailable",
                    format!("Cannot connect to Jaeger at {}", self.query_url),
                    EventSeverity::Critical,
                    self.name(),
                    "jaeger_unavailable",
                ),
            );
            return Ok(events);
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for JaegerIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_jaeger_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_jaeger_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Jaeger at {}",
                self.query_url
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
    fn test_jaeger_integration_creation() {
        let jaeger = JaegerIntegration::new("http://localhost:16686");
        assert!(jaeger.is_ok());
    }
}
