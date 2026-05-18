//! Kubernetes Integration
//!
//! Monitor Kubernetes pods, nodes, services, and cluster health.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Kubernetes integration
pub struct KubernetesIntegration {
    config: IntegrationConfig,
    kubeconfig_path: Option<String>,
    namespace: String,
}

impl KubernetesIntegration {
    /// Create new Kubernetes integration
    pub fn new() -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("orchestrator".to_string(), "kubernetes".to_string());

        let config = IntegrationConfig {
            name: "kubernetes".to_string(),
            interval_secs: 30,
            tags,
            timeout_secs: 15,
        };

        Ok(Self {
            config,
            kubeconfig_path: None,
            namespace: "default".to_string(),
        })
    }

    /// Create with kubeconfig path
    pub fn with_kubeconfig(kubeconfig: &str) -> Result<Self, IntegrationError> {
        let mut this = Self::new()?;
        this.kubeconfig_path = Some(kubeconfig.to_string());
        Ok(this)
    }

    /// Set namespace to monitor
    pub fn with_namespace(mut self, namespace: &str) -> Self {
        self.namespace = namespace.to_string();
        self
    }

    /// Check if kubectl is available and cluster is accessible
    async fn check_cluster_access(&self) -> bool {
        let mut cmd = tokio::process::Command::new("kubectl");
        cmd.arg("version");
        
        if let Some(ref kubeconfig) = self.kubeconfig_path {
            cmd.env("KUBECONFIG", kubeconfig);
        }

        match cmd.output().await {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    /// Collect node metrics
    async fn collect_node_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("kubectl");
        cmd.args(["get", "nodes", "-o", "json"]);
        
        if let Some(ref kubeconfig) = self.kubeconfig_path {
            cmd.env("KUBECONFIG", kubeconfig);
        }

        let output = cmd.output().await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        if !output.status.success() {
            return Ok(metrics);
        }

        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        if let Some(items) = json["items"].as_array() {
            let node_count = items.len() as f64;
            metrics.push(Metric::gauge(
                "kubernetes.nodes.total",
                node_count,
                self.name(),
            ));

            let mut ready_count = 0.0;
            for node in items {
                if let Some(conditions) = node["status"]["conditions"].as_array() {
                    for condition in conditions {
                        if condition["type"].as_str() == Some("Ready") {
                            if condition["status"].as_str() == Some("True") {
                                ready_count += 1.0;
                            }
                        }
                    }
                }

                // Node capacity
                if let Some(capacity) = node["status"]["capacity"].as_object() {
                    if let Some(cpu) = capacity["cpu"].as_str() {
                        if let Ok(cpu_count) = cpu.parse::<f64>() {
                            metrics.push(Metric::gauge(
                                "kubernetes.node.cpu_capacity",
                                cpu_count,
                                self.name(),
                            ));
                        }
                    }
                }
            }

            metrics.push(Metric::gauge(
                "kubernetes.nodes.ready",
                ready_count,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "kubernetes.nodes.not_ready",
                node_count - ready_count,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect pod metrics
    async fn collect_pod_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("kubectl");
        cmd.args([
            "get", "pods", "--all-namespaces",
            "-o", "json",
        ]);
        
        if let Some(ref kubeconfig) = self.kubeconfig_path {
            cmd.env("KUBECONFIG", kubeconfig);
        }

        let output = cmd.output().await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        if !output.status.success() {
            return Ok(metrics);
        }

        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        if let Some(items) = json["items"].as_array() {
            let mut status_counts: HashMap<String, u64> = HashMap::new();
            
            for pod in items {
                if let Some(phase) = pod["status"]["phase"].as_str() {
                    *status_counts.entry(phase.to_string()).or_insert(0) += 1;
                }
            }

            for (status, count) in status_counts {
                metrics.push(Metric::gauge(
                    format!("kubernetes.pods.{}", status.to_lowercase()),
                    count as f64,
                    self.name(),
                ));
            }

            metrics.push(Metric::gauge(
                "kubernetes.pods.total",
                items.len() as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect deployment metrics
    async fn collect_deployment_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("kubectl");
        cmd.args([
            "get", "deployments", "--all-namespaces",
            "-o", "json",
        ]);
        
        if let Some(ref kubeconfig) = self.kubeconfig_path {
            cmd.env("KUBECONFIG", kubeconfig);
        }

        let output = cmd.output().await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        if !output.status.success() {
            return Ok(metrics);
        }

        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        if let Some(items) = json["items"].as_array() {
            metrics.push(Metric::gauge(
                "kubernetes.deployments.total",
                items.len() as f64,
                self.name(),
            ));

            let mut available = 0.0;
            for deployment in items {
                if let Some(status) = deployment["status"].as_object() {
                    if let Some(available_replicas) = status["availableReplicas"].as_u64() {
                        available += available_replicas as f64;
                    }
                }
            }

            metrics.push(Metric::gauge(
                "kubernetes.deployments.available_replicas",
                available,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all Kubernetes metrics
    async fn collect_k8s_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        match self.collect_node_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect node metrics: {}", e),
        }

        match self.collect_pod_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect pod metrics: {}", e),
        }

        match self.collect_deployment_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect deployment metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect events
    async fn collect_k8s_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check cluster access
        if !self.check_cluster_access().await {
            events.push(
                Event::new(
                    "Kubernetes Cluster Unavailable",
                    "Cannot connect to Kubernetes cluster".to_string(),
                    EventSeverity::Critical,
                    self.name(),
                    "cluster_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check for node issues
        let mut cmd = tokio::process::Command::new("kubectl");
        cmd.args(["get", "nodes", "-o", "json"]);
        
        if let Some(ref kubeconfig) = self.kubeconfig_path {
            cmd.env("KUBECONFIG", kubeconfig);
        }

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                    if let Some(items) = json["items"].as_array() {
                        for node in items {
                            if let Some(conditions) = node["status"]["conditions"].as_array() {
                                for condition in conditions {
                                    if let (Some(cond_type), Some(status)) = (
                                        condition["type"].as_str(),
                                        condition["status"].as_str(),
                                    ) {
                                        if cond_type == "Ready" && status != "True" {
                                            if let Some(name) = node["metadata"]["name"].as_str() {
                                                events.push(
                                                    Event::new(
                                                        "Node Not Ready",
                                                        format!("Node {} is not ready", name),
                                                        EventSeverity::Warning,
                                                        self.name(),
                                                        "node_not_ready",
                                                    )
                                                    .with_metadata("node", name),
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Check for pods in CrashLoopBackOff
        let mut cmd = tokio::process::Command::new("kubectl");
        cmd.args([
            "get", "pods", "--all-namespaces",
            "--field-selector", "status.phase!=Running,status.phase!=Succeeded",
            "-o", "json",
        ]);
        
        if let Some(ref kubeconfig) = self.kubeconfig_path {
            cmd.env("KUBECONFIG", kubeconfig);
        }

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                    if let Some(items) = json["items"].as_array() {
                        for pod in items {
                            if let Some(name) = pod["metadata"]["name"].as_str() {
                                if let Some(ns) = pod["metadata"]["namespace"].as_str() {
                                    events.push(
                                        Event::new(
                                            "Pod Not Running",
                                            format!("Pod {}/{} is not in Running state", ns, name),
                                            EventSeverity::Warning,
                                            self.name(),
                                            "pod_not_running",
                                        )
                                        .with_metadata("pod", name)
                                        .with_metadata("namespace", ns),
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
impl Integration for KubernetesIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_k8s_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_k8s_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_cluster_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(
                "Cannot access Kubernetes cluster".to_string()
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
    fn test_kubernetes_integration_creation() {
        let k8s = KubernetesIntegration::new();
        assert!(k8s.is_ok());
    }
}
