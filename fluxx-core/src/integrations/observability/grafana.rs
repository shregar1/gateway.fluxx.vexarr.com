//! Grafana Integration
//!
//! Monitor Grafana server, datasources, and health.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Grafana integration
pub struct GrafanaIntegration {
    config: IntegrationConfig,
    base_url: String,
    api_token: String,
}

impl GrafanaIntegration {
    /// Create new Grafana integration
    pub fn new(base_url: &str, api_token: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("observability_type".to_string(), "grafana".to_string());

        let config = IntegrationConfig {
            name: "grafana".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_token: api_token.to_string(),
        })
    }

    /// Check if Grafana is accessible
    async fn check_grafana_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/api/health", self.base_url);

        match client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_token))
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
        let url = format!("{}/api/health", self.base_url);

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_token))
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

        // Database status
        if let Some(db_status) = data["database"].as_str() {
            let db_value = if db_status == "ok" { 1.0 } else { 0.0 };
            metrics.push(Metric::gauge(
                "grafana.health.database",
                db_value,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect datasource metrics
    async fn collect_datasource_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/api/datasources", self.base_url);

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_token))
            .timeout(std::time::Duration::from_secs(10))
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

        metrics.push(Metric::gauge(
            "grafana.datasources.total",
            data.len() as f64,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect all Grafana metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_grafana_access().await {
            all_metrics.push(Metric::gauge(
                "grafana.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "grafana.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_health_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect health metrics: {}", e),
        }

        match self.collect_datasource_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect datasource metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Grafana events
    async fn collect_grafana_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_grafana_access().await {
            events.push(
                Event::new(
                    "Grafana Unavailable",
                    format!("Cannot connect to Grafana at {}", self.base_url),
                    EventSeverity::Critical,
                    self.name(),
                    "grafana_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check database health
        let client = reqwest::Client::new();
        let url = format!("{}/api/health", self.base_url);

        if let Ok(response) = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.api_token))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(data) = response.json::<serde_json::Value>().await {
                    if let Some(db) = data["database"].as_str() {
                        if db != "ok" {
                            events.push(
                                Event::new(
                                    "Grafana Database Issue",
                                    format!("Database status: {}", db),
                                    EventSeverity::Critical,
                                    self.name(),
                                    "database_issue",
                                ),
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
impl Integration for GrafanaIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_grafana_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_grafana_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Grafana at {}",
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
    fn test_grafana_integration_creation() {
        let grafana = GrafanaIntegration::new("http://localhost:3000", "token123");
        assert!(grafana.is_ok());
    }
}
