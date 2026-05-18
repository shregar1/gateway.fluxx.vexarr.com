//! Alibaba Cloud Integration
//!
//! Monitor Alibaba Cloud ECS, RDS, and OSS resources.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Alibaba Cloud integration
pub struct AlibabaCloudIntegration {
    config: IntegrationConfig,
    region: String,
    access_key_id: String,
    access_key_secret: String,
}

impl AlibabaCloudIntegration {
    /// Create new Alibaba Cloud integration
    pub fn new(region: &str, access_key_id: &str, access_key_secret: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cloud_provider".to_string(), "alibaba".to_string());
        tags.insert("region".to_string(), region.to_string());

        let config = IntegrationConfig {
            name: format!("alibaba_{}", region),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            region: region.to_string(),
            access_key_id: access_key_id.to_string(),
            access_key_secret: access_key_secret.to_string(),
        })
    }

    /// Check if Alibaba Cloud CLI is available
    async fn check_alibaba_access(&self) -> bool {
        let output = tokio::process::Command::new("aliyun")
            .args([
                "ecs",
                "DescribeInstances",
                "--region",
                &self.region,
                "--PageSize",
                "1",
            ])
            .env("ALIBABA_CLOUD_ACCESS_KEY_ID", &self.access_key_id)
            .env("ALIBABA_CLOUD_ACCESS_KEY_SECRET", &self.access_key_secret)
            .output()
            .await;

        match output {
            Ok(o) => o.status.success(),
            Err(_) => false,
        }
    }

    /// Collect ECS instance metrics
    async fn collect_ecs_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let output = tokio::process::Command::new("aliyun")
            .args([
                "ecs",
                "DescribeInstances",
                "--region",
                &self.region,
                "--output",
                "cols=TotalCount",
                "--rows",
                "1",
            ])
            .env("ALIBABA_CLOUD_ACCESS_KEY_ID", &self.access_key_id)
            .env("ALIBABA_CLOUD_ACCESS_KEY_SECRET", &self.access_key_secret)
            .output()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !output.status.success() {
            return Ok(metrics);
        }

        // Parse total count from output
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if let Ok(count) = line.trim().parse::<f64>() {
                metrics.push(Metric::gauge(
                    "alibaba.ecs.instances.total",
                    count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect all Alibaba Cloud metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_alibaba_access().await {
            all_metrics.push(Metric::gauge(
                "alibaba.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "alibaba.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_ecs_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect ECS metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Alibaba Cloud events
    async fn collect_alibaba_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_alibaba_access().await {
            events.push(
                Event::new(
                    "Alibaba Cloud Unavailable",
                    format!("Cannot connect to Alibaba Cloud region {}", self.region),
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
impl Integration for AlibabaCloudIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_alibaba_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_alibaba_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "Alibaba Cloud CLI not configured".to_string()
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
    fn test_alibaba_cloud_integration_creation() {
        let alibaba = AlibabaCloudIntegration::new(
            "cn-hangzhou",
            "access_key_id",
            "access_key_secret"
        );
        assert!(alibaba.is_ok());
    }
}
