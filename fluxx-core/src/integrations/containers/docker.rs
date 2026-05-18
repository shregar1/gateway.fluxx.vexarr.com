//! Docker Integration
//!
//! Monitor Docker containers, images, volumes, and system resources.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Docker integration
pub struct DockerIntegration {
    config: IntegrationConfig,
    socket_path: String,
}

impl DockerIntegration {
    /// Create new Docker integration
    pub fn new() -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("container_runtime".to_string(), "docker".to_string());

        let config = IntegrationConfig {
            name: "docker".to_string(),
            interval_secs: 30,
            tags,
            timeout_secs: 10,
        };

        // Default Docker socket path
        let socket_path = "/var/run/docker.sock".to_string();

        Ok(Self {
            config,
            socket_path,
        })
    }

    /// Create with custom socket path
    pub fn with_socket(socket_path: &str) -> Result<Self, IntegrationError> {
        let mut this = Self::new()?;
        this.socket_path = socket_path.to_string();
        Ok(this)
    }

    /// Check if Docker daemon is accessible
    async fn check_docker_daemon(&self) -> bool {
        // Check if socket exists
        tokio::fs::metadata(&self.socket_path).await.is_ok()
    }

    /// Collect container stats by reading cgroup info
    async fn collect_container_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Parse /sys/fs/cgroup for container metrics
        let cgroup_path = "/sys/fs/cgroup";
        
        // Check for cgroup v2 (unified) vs v1
        let unified_path = format!("{}/cgroup.controllers", cgroup_path);
        let is_cgroup_v2 = tokio::fs::metadata(&unified_path).await.is_ok();

        if is_cgroup_v2 {
            metrics.extend(self.collect_cgroup_v2_metrics(cgroup_path).await?);
        } else {
            metrics.extend(self.collect_cgroup_v1_metrics(cgroup_path).await?);
        }

        // Count running containers from /sys/fs/cgroup/system.slice/docker-
        let container_count = self.count_containers().await;
        metrics.push(Metric::gauge(
            "docker.containers.total",
            container_count as f64,
            self.name(),
        ));

        // Try to get system info from docker command
        if let Ok(output) = tokio::process::Command::new("docker")
            .args(["system", "df", "--format", "{{.Type}}:{{.Size}}:{{.Count}}"])
            .output()
            .await
        {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    let parts: Vec<&str> = line.split(':').collect();
                    if parts.len() >= 3 {
                        let resource_type = parts[0].to_lowercase();
                        if let Ok(count) = parts[2].parse::<f64>() {
                            metrics.push(Metric::gauge(
                                format!("docker.{}.count", resource_type),
                                count,
                                self.name(),
                            ));
                        }
                    }
                }
            }
        }

        Ok(metrics)
    }

    /// Collect cgroup v2 (unified) metrics
    async fn collect_cgroup_v2_metrics(&self, _cgroup_path: &str) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Read memory.stat for memory metrics
        let memory_stat = "/sys/fs/cgroup/memory.stat";
        if let Ok(content) = tokio::fs::read_to_string(memory_stat).await {
            for line in content.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() == 2 {
                    let key = parts[0];
                    if let Ok(value) = parts[1].parse::<f64>() {
                        metrics.push(Metric::gauge(
                            format!("docker.memory.{}", key.replace('.', "_")),
                            value,
                            self.name(),
                        ));
                    }
                }
            }
        }

        // Read memory.current
        let memory_current = "/sys/fs/cgroup/memory.current";
        if let Ok(content) = tokio::fs::read_to_string(memory_current).await {
            if let Ok(value) = content.trim().parse::<f64>() {
                metrics.push(Metric::gauge(
                    "docker.memory.current_bytes",
                    value,
                    self.name(),
                ));
            }
        }

        // Read cpu.stat
        let cpu_stat = "/sys/fs/cgroup/cpu.stat";
        if let Ok(content) = tokio::fs::read_to_string(cpu_stat).await {
            for line in content.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() == 2 {
                    let key = parts[0];
                    if let Ok(value) = parts[1].parse::<f64>() {
                        metrics.push(Metric::gauge(
                            format!("docker.cpu.{}", key),
                            value,
                            self.name(),
                        ));
                    }
                }
            }
        }

        Ok(metrics)
    }

    /// Collect cgroup v1 metrics
    async fn collect_cgroup_v1_metrics(&self, cgroup_path: &str) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Memory usage
        let memory_usage = format!("{}/memory/memory.usage_in_bytes", cgroup_path);
        if let Ok(content) = tokio::fs::read_to_string(&memory_usage).await {
            if let Ok(value) = content.trim().parse::<f64>() {
                metrics.push(Metric::gauge(
                    "docker.memory.usage_bytes",
                    value,
                    self.name(),
                ));
            }
        }

        // Memory limit
        let memory_limit = format!("{}/memory/memory.limit_in_bytes", cgroup_path);
        if let Ok(content) = tokio::fs::read_to_string(&memory_limit).await {
            if let Ok(value) = content.trim().parse::<f64>() {
                metrics.push(Metric::gauge(
                    "docker.memory.limit_bytes",
                    value,
                    self.name(),
                ));
            }
        }

        // CPU stats
        let cpu_stat = format!("{}/cpu/cpu.stat", cgroup_path);
        if let Ok(content) = tokio::fs::read_to_string(&cpu_stat).await {
            for line in content.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() == 2 {
                    if let Ok(value) = parts[1].parse::<f64>() {
                        metrics.push(Metric::gauge(
                            format!("docker.cpu.{}", parts[0]),
                            value,
                            self.name(),
                        ));
                    }
                }
            }
        }

        Ok(metrics)
    }

    /// Count running containers
    async fn count_containers(&self) -> u64 {
        // Try to count docker containers
        if let Ok(output) = tokio::process::Command::new("docker")
            .args(["ps", "-q"])
            .output()
            .await
        {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                return stdout.lines().count() as u64;
            }
        }
        0
    }

    /// Collect container events
    async fn collect_container_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check if docker daemon is running
        if !self.check_docker_daemon().await {
            events.push(
                Event::new(
                    "Docker Daemon Unavailable",
                    format!("Docker socket not found at {}", self.socket_path),
                    EventSeverity::Warning,
                    self.name(),
                    "daemon_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check container count
        let container_count = self.count_containers().await;
        if container_count > 100 {
            events.push(
                Event::new(
                    "High Container Count",
                    format!("{} containers running, consider cleanup", container_count),
                    EventSeverity::Info,
                    self.name(),
                    "high_container_count",
                )
                .with_metadata("count", &container_count.to_string()),
            );
        }

        // Check disk usage (warn if docker system df shows high usage)
        if let Ok(output) = tokio::process::Command::new("docker")
            .args(["system", "df", "--format", "{{.Type}}:{{.Size}}"])
            .output()
            .await
        {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    if line.starts_with("Images:") {
                        // Could parse size and warn if too large
                        break;
                    }
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for DockerIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_container_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_container_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_docker_daemon().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Docker socket not found at {}",
                self.socket_path
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
    fn test_docker_integration_creation() {
        let docker = DockerIntegration::new();
        assert!(docker.is_ok());
    }
}
