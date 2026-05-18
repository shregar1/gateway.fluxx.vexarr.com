//! Oracle Cloud Infrastructure (OCI) Integration
//!
//! Monitor Oracle Cloud compute, databases, and resources.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Oracle Cloud integration
pub struct OracleCloudIntegration {
    config: IntegrationConfig,
    region: String,
    tenancy_ocid: String,
}

impl OracleCloudIntegration {
    /// Create new Oracle Cloud integration
    pub fn new(region: &str, tenancy_ocid: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cloud_provider".to_string(), "oracle".to_string());
        tags.insert("region".to_string(), region.to_string());

        let config = IntegrationConfig {
            name: format!("oracle_{}", region),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            region: region.to_string(),
            tenancy_ocid: tenancy_ocid.to_string(),
        })
    }

    /// Check if OCI CLI is available and configured
    async fn check_oci_access(&self) -> bool {
        let output = tokio::process::Command::new("oci")
            .args([
                "compute",
                "instance",
                "list",
                "--region",
                &self.region,
                "--compartment-id",
                &self.tenancy_ocid,
                "--limit",
                "1",
            ])
            .output()
            .await;

        match output {
            Ok(o) => o.status.success(),
            Err(_) => false,
        }
    }

    /// Collect compute instance metrics
    async fn collect_compute_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let output = tokio::process::Command::new("oci")
            .args([
                "compute",
                "instance",
                "list",
                "--region",
                &self.region,
                "--compartment-id",
                &self.tenancy_ocid,
                "--output",
                "json",
            ])
            .output()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !output.status.success() {
            return Ok(metrics);
        }

        let data: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        if let Some(instances) = data["data"].as_array() {
            let mut running = 0u64;
            let mut stopped = 0u64;

            for instance in instances {
                if let Some(state) = instance["lifecycle-state"].as_str() {
                    match state {
                        "RUNNING" => running += 1,
                        "STOPPED" => stopped += 1,
                        _ => {}
                    }
                }
            }

            metrics.push(Metric::gauge(
                "oracle.compute.instances.total",
                instances.len() as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "oracle.compute.instances.running",
                running as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "oracle.compute.instances.stopped",
                stopped as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all Oracle Cloud metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_oci_access().await {
            all_metrics.push(Metric::gauge(
                "oracle.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "oracle.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_compute_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect compute metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Oracle Cloud events
    async fn collect_oracle_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_oci_access().await {
            events.push(
                Event::new(
                    "Oracle Cloud Unavailable",
                    format!("Cannot connect to Oracle Cloud region {}", self.region),
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
impl Integration for OracleCloudIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_oracle_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_oci_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "Oracle Cloud CLI not configured".to_string()
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
    fn test_oracle_cloud_integration_creation() {
        let oracle = OracleCloudIntegration::new("us-ashburn-1", "ocid1.tenancy.oc1..xxx");
        assert!(oracle.is_ok());
    }
}
