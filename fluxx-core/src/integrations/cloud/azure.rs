//! Azure Monitor Integration
//!
//! Monitor Azure resources via Azure Monitor.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Azure Monitor integration
pub struct AzureIntegration {
    config: IntegrationConfig,
    subscription_id: String,
}

impl AzureIntegration {
    /// Create new Azure integration
    pub fn new(subscription_id: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cloud_provider".to_string(), "azure".to_string());
        tags.insert("subscription".to_string(), subscription_id.to_string());

        let config = IntegrationConfig {
            name: format!("azure_{}", &subscription_id[..8.min(subscription_id.len())]),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            subscription_id: subscription_id.to_string(),
        })
    }

    /// Check if Azure CLI is available and logged in
    async fn check_azure_access(&self) -> bool {
        let mut cmd = tokio::process::Command::new("az");
        cmd.args(["account", "show"]);

        match cmd.output().await {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    /// Collect VM metrics
    async fn collect_vm_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("az");
        cmd.args([
            "vm", "list",
            "--subscription", &self.subscription_id,
            "--query", "[].id",
            "--output", "tsv",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let vm_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "azure.vm.total",
                    vm_count,
                    self.name(),
                ));
            }
        }

        // Count running VMs
        let mut cmd = tokio::process::Command::new("az");
        cmd.args([
            "vm", "list",
            "--subscription", &self.subscription_id,
            "--query", "[?powerState=='VM running'].id",
            "--output", "tsv",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let running_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "azure.vm.running",
                    running_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect AKS (Kubernetes) metrics
    async fn collect_aks_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("az");
        cmd.args([
            "aks", "list",
            "--subscription", &self.subscription_id,
            "--query", "[].name",
            "--output", "tsv",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let cluster_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "azure.aks.clusters.total",
                    cluster_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect SQL Database metrics
    async fn collect_sql_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("az");
        cmd.args([
            "sql", "server", "list",
            "--subscription", &self.subscription_id,
            "--query", "[].name",
            "--output", "tsv",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let server_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "azure.sql.servers.total",
                    server_count,
                    self.name(),
                ));
            }
        }

        // Count databases
        let mut cmd = tokio::process::Command::new("az");
        cmd.args([
            "sql", "db", "list",
            "--server", "$(az sql server list --subscription", &self.subscription_id, 
            "--query", "[0].name", "--output", "tsv)",
            "--subscription", &self.subscription_id,
            "--query", "[].name",
            "--output", "tsv",
        ]);

        // This requires a server name - skipping for now
        // In production, would iterate through all servers

        Ok(metrics)
    }

    /// Collect Storage Account metrics
    async fn collect_storage_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("az");
        cmd.args([
            "storage", "account", "list",
            "--subscription", &self.subscription_id,
            "--query", "[].name",
            "--output", "tsv",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let account_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "azure.storage.accounts.total",
                    account_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect Function App metrics
    async fn collect_function_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("az");
        cmd.args([
            "functionapp", "list",
            "--subscription", &self.subscription_id,
            "--query", "[].name",
            "--output", "tsv",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let app_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "azure.functionapp.total",
                    app_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect all Azure metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        match self.collect_vm_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect VM metrics: {}", e),
        }

        match self.collect_aks_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect AKS metrics: {}", e),
        }

        match self.collect_sql_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect SQL metrics: {}", e),
        }

        match self.collect_storage_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect storage metrics: {}", e),
        }

        match self.collect_function_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect function app metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Azure events
    async fn collect_azure_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check Azure access
        if !self.check_azure_access().await {
            events.push(
                Event::new(
                    "Azure Access Failed",
                    "Azure CLI not logged in or not installed".to_string(),
                    EventSeverity::Critical,
                    self.name(),
                    "azure_access_failed",
                ),
            );
            return Ok(events);
        }

        // Check for deallocated VMs (potential cost savings)
        let mut cmd = tokio::process::Command::new("az");
        cmd.args([
            "vm", "list",
            "--subscription", &self.subscription_id,
            "--query", "[?powerState=='VM deallocated'].id",
            "--output", "tsv",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let deallocated_count = stdout.lines().filter(|l| !l.is_empty()).count();
                
                if deallocated_count > 0 {
                    events.push(
                        Event::new(
                            "Deallocated VMs",
                            format!("{} VMs are deallocated (not incurring compute costs)", deallocated_count),
                            EventSeverity::Info,
                            self.name(),
                            "deallocated_vms",
                        )
                        .with_metadata("count", &deallocated_count.to_string()),
                    );
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for AzureIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_azure_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_azure_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "Azure CLI not logged in".to_string()
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
    fn test_azure_integration_creation() {
        let azure = AzureIntegration::new("12345678-1234-1234-1234-123456789012");
        assert!(azure.is_ok());
    }
}
