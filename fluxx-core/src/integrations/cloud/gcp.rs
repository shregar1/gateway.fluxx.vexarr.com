//! Google Cloud Platform Monitoring Integration
//!
//! Monitor GCP resources via Cloud Monitoring.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// GCP Cloud Monitoring integration
pub struct GcpIntegration {
    config: IntegrationConfig,
    project_id: String,
}

impl GcpIntegration {
    /// Create new GCP integration
    pub fn new(project_id: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cloud_provider".to_string(), "gcp".to_string());
        tags.insert("project".to_string(), project_id.to_string());

        let config = IntegrationConfig {
            name: format!("gcp_{}", project_id),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            project_id: project_id.to_string(),
        })
    }

    /// Check if gcloud CLI is available and authenticated
    async fn check_gcp_access(&self) -> bool {
        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args(["auth", "list", "--filter=status:ACTIVE", "--format=value(account)"]);

        match cmd.output().await {
            Ok(output) => {
                output.status.success() && !output.stdout.is_empty()
            }
            Err(_) => false,
        }
    }

    /// Collect Compute Engine metrics
    async fn collect_compute_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args([
            "compute", "instances", "list",
            "--project", &self.project_id,
            "--format", "value(name)",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let instance_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "gcp.compute.instances.total",
                    instance_count,
                    self.name(),
                ));
            }
        }

        // Count running instances
        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args([
            "compute", "instances", "list",
            "--project", &self.project_id,
            "--filter", "status:RUNNING",
            "--format", "value(name)",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let running_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "gcp.compute.instances.running",
                    running_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect GKE (Kubernetes) metrics
    async fn collect_gke_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args([
            "container", "clusters", "list",
            "--project", &self.project_id,
            "--format", "value(name)",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let cluster_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "gcp.gke.clusters.total",
                    cluster_count,
                    self.name(),
                ));
            }
        }

        // Get node count across all clusters
        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args([
            "container", "clusters", "list",
            "--project", &self.project_id,
            "--format", "value(currentNodeCount)",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let total_nodes: u64 = stdout
                    .lines()
                    .filter(|l| !l.is_empty())
                    .filter_map(|l| l.parse::<u64>().ok())
                    .sum();
                
                metrics.push(Metric::gauge(
                    "gcp.gke.nodes.total",
                    total_nodes as f64,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect Cloud SQL metrics
    async fn collect_sql_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args([
            "sql", "instances", "list",
            "--project", &self.project_id,
            "--format", "value(name)",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let instance_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "gcp.cloudsql.instances.total",
                    instance_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect Cloud Storage metrics
    async fn collect_storage_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args([
            "storage", "buckets", "list",
            "--project", &self.project_id,
            "--format", "value(name)",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let bucket_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "gcp.storage.buckets.total",
                    bucket_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect Cloud Functions metrics
    async fn collect_functions_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args([
            "functions", "list",
            "--project", &self.project_id,
            "--format", "value(name)",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let function_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "gcp.cloudfunctions.total",
                    function_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect Load Balancer metrics
    async fn collect_load_balancer_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Backend services
        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args([
            "compute", "backend-services", "list",
            "--project", &self.project_id,
            "--format", "value(name)",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let backend_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "gcp.compute.backend_services.total",
                    backend_count,
                    self.name(),
                ));
            }
        }

        // URL maps (for HTTP(S) load balancers)
        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args([
            "compute", "url-maps", "list",
            "--project", &self.project_id,
            "--format", "value(name)",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let url_map_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "gcp.compute.url_maps.total",
                    url_map_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect all GCP metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        match self.collect_compute_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect compute metrics: {}", e),
        }

        match self.collect_gke_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect GKE metrics: {}", e),
        }

        match self.collect_sql_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect Cloud SQL metrics: {}", e),
        }

        match self.collect_storage_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect storage metrics: {}", e),
        }

        match self.collect_functions_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect Cloud Functions metrics: {}", e),
        }

        match self.collect_load_balancer_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect load balancer metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect GCP events
    async fn collect_gcp_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check GCP access
        if !self.check_gcp_access().await {
            events.push(
                Event::new(
                    "GCP Access Failed",
                    format!("gcloud CLI not authenticated for project {}", self.project_id),
                    EventSeverity::Critical,
                    self.name(),
                    "gcp_access_failed",
                ),
            );
            return Ok(events);
        }

        // Check for stopped instances (potential cost issue)
        let mut cmd = tokio::process::Command::new("gcloud");
        cmd.args([
            "compute", "instances", "list",
            "--project", &self.project_id,
            "--filter", "status:TERMINATED",
            "--format", "value(name)",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let terminated_count = stdout.lines().filter(|l| !l.is_empty()).count();
                
                if terminated_count > 0 {
                    events.push(
                        Event::new(
                            "Terminated GCE Instances",
                            format!("{} instances are terminated (incurring disk costs)", terminated_count),
                            EventSeverity::Info,
                            self.name(),
                            "terminated_instances",
                        )
                        .with_metadata("count", &terminated_count.to_string()),
                    );
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for GcpIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_gcp_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_gcp_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "gcloud CLI not authenticated".to_string()
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
    fn test_gcp_integration_creation() {
        let gcp = GcpIntegration::new("my-project-123");
        assert!(gcp.is_ok());
        assert_eq!(gcp.unwrap().name(), "gcp_my-project-123");
    }
}
