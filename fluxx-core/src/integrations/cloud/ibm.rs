//! IBM Cloud Integration
//!
//! Monitor IBM Cloud resources via IBM Cloud API.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// IBM Cloud integration
pub struct IbmCloudIntegration {
    config: IntegrationConfig,
    api_key: String,
    region: String,
}

impl IbmCloudIntegration {
    /// Create new IBM Cloud integration
    pub fn new(api_key: &str, region: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cloud_provider".to_string(), "ibm".to_string());
        tags.insert("region".to_string(), region.to_string());

        let config = IntegrationConfig {
            name: format!("ibm_{}", region),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            api_key: api_key.to_string(),
            region: region.to_string(),
        })
    }

    /// Check if IBM Cloud CLI is available
    async fn check_ibm_access(&self) -> bool {
        let output = tokio::process::Command::new("ibmcloud")
            .args(["resource", "service-instances", "--output", "JSON"])
            .env("IBMCLOUD_API_KEY", &self.api_key)
            .output()
            .await;

        match output {
            Ok(o) => o.status.success(),
            Err(_) => false,
        }
    }

    /// Collect resource metrics
    async fn collect_resource_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let output = tokio::process::Command::new("ibmcloud")
            .args(["resource", "service-instances", "--output", "JSON"])
            .env("IBMCLOUD_API_KEY", &self.api_key)
            .env("IBMCLOUD_REGION", &self.region)
            .output()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !output.status.success() {
            return Ok(metrics);
        }

        let data: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout)
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        metrics.push(Metric::gauge(
            "ibmcloud.service_instances.total",
            data.len() as f64,
            self.name(),
        ));

        // Count by state
        let mut active_count = 0u64;
        for instance in &data {
            if let Some(state) = instance["state"].as_str() {
                if state == "active" {
                    active_count += 1;
                }
            }
        }

        metrics.push(Metric::gauge(
            "ibmcloud.service_instances.active",
            active_count as f64,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect all IBM Cloud metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_ibm_access().await {
            all_metrics.push(Metric::gauge(
                "ibmcloud.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "ibmcloud.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_resource_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect resource metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect IBM Cloud events
    async fn collect_ibm_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_ibm_access().await {
            events.push(
                Event::new(
                    "IBM Cloud Unavailable",
                    "Cannot connect to IBM Cloud".to_string(),
                    EventSeverity::Critical,
                    self.name(),
                    "api_unavailable",
                ),
            );
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for IbmCloudIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_ibm_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_ibm_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "IBM Cloud API key invalid".to_string()
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
    fn test_ibm_cloud_integration_creation() {
        let ibm = IbmCloudIntegration::new("api_key", "us-south");
        assert!(ibm.is_ok());
    }
}
