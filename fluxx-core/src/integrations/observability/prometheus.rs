//! Prometheus Integration
//!
//! Monitor Prometheus server, targets, and query metrics.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Prometheus integration
pub struct PrometheusIntegration {
    config: IntegrationConfig,
    base_url: String,
}

impl PrometheusIntegration {
    /// Create new Prometheus integration
    pub fn new(base_url: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("observability_type".to_string(), "prometheus".to_string());

        let config = IntegrationConfig {
            name: "prometheus".to_string(),
            interval_secs: 30,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    /// Check if Prometheus is accessible
    async fn check_prometheus_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/-/healthy", self.base_url);

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

    /// Collect runtime metrics
    async fn collect_runtime_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/api/v1/status/runtimeinfo", self.base_url);

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

        if let Some(info) = data["data"].as_object() {
            if let Some(v) = info["timeSeriesCount"].as_f64() {
                metrics.push(Metric::gauge(
                    "prometheus.timeseries.total",
                    v,
                    self.name(),
                ));
            }
            if let Some(v) = info["chunkCount"].as_f64() {
                metrics.push(Metric::gauge(
                    "prometheus.chunks.total",
                    v,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect target health metrics
    async fn collect_target_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/api/v1/targets", self.base_url);

        let response = client
            .get(&url)
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

        if let Some(targets) = data["data"]["activeTargets"].as_array() {
            let mut up = 0u64;
            let mut down = 0u64;

            for target in targets {
                if let Some(health) = target["health"].as_str() {
                    match health {
                        "up" => up += 1,
                        "down" => down += 1,
                        _ => {}
                    }
                }
            }

            metrics.push(Metric::gauge(
                "prometheus.targets.total",
                targets.len() as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "prometheus.targets.up",
                up as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "prometheus.targets.down",
                down as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all Prometheus metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_prometheus_access().await {
            all_metrics.push(Metric::gauge(
                "prometheus.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "prometheus.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_runtime_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect runtime metrics: {}", e),
        }

        match self.collect_target_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect target metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Prometheus events
    async fn collect_prometheus_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_prometheus_access().await {
            events.push(
                Event::new(
                    "Prometheus Unavailable",
                    format!("Cannot connect to Prometheus at {}", self.base_url),
                    EventSeverity::Critical,
                    self.name(),
                    "prometheus_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check for down targets
        let client = reqwest::Client::new();
        let url = format!("{}/api/v1/targets", self.base_url);

        if let Ok(response) = client
            .get(&url)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(data) = response.json::<serde_json::Value>().await {
                    if let Some(targets) = data["data"]["activeTargets"].as_array() {
                        for target in targets {
                            if target["health"].as_str() == Some("down") {
                                if let Some(labels) = target["labels"].as_object() {
                                    let instance = labels["instance"]
                                        .as_str()
                                        .unwrap_or("unknown");
                                    let job = labels["job"]
                                        .as_str()
                                        .unwrap_or("unknown");

                                    events.push(
                                        Event::new(
                                            "Prometheus Target Down",
                                            format!("Target {} for job {} is down", instance, job),
                                            EventSeverity::Warning,
                                            self.name(),
                                            "target_down",
                                        )
                                        .with_metadata("instance", instance)
                                        .with_metadata("job", job),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for PrometheusIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_prometheus_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_prometheus_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Prometheus at {}",
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
    fn test_prometheus_integration_creation() {
        let prometheus = PrometheusIntegration::new("http://localhost:9090");
        assert!(prometheus.is_ok());
    }
}
