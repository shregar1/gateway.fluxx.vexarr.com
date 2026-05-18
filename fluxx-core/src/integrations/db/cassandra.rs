//! Apache Cassandra Integration
//!
//! Monitor Cassandra clusters, nodes, keyspaces, and query performance.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Cassandra integration
pub struct CassandraIntegration {
    config: IntegrationConfig,
    hosts: Vec<String>,
    port: u16,
}

impl CassandraIntegration {
    /// Create new Cassandra integration
    pub fn new(hosts: &[&str], port: u16) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("db_type".to_string(), "cassandra".to_string());

        let config = IntegrationConfig {
            name: "cassandra".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            hosts: hosts.iter().map(|h| h.to_string()).collect(),
            port,
        })
    }

    /// Check if Cassandra is reachable (via nodetool or CQL port)
    async fn check_cassandra_access(&self) -> bool {
        // Try nodetool first
        let output = tokio::process::Command::new("nodetool")
            .arg("status")
            .output()
            .await;

        if let Ok(output) = output {
            if output.status.success() {
                return true;
            }
        }

        // Fallback to TCP connection check
        for host in &self.hosts {
            let timeout = tokio::time::Duration::from_secs(5);
            if tokio::time::timeout(
                timeout,
                tokio::net::TcpStream::connect((host.as_str(), self.port)),
            )
            .await
            .is_ok_and(|r| r.is_ok())
            {
                return true;
            }
        }

        false
    }

    /// Collect node status via nodetool
    async fn collect_node_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Try to get cluster status
        let output = tokio::process::Command::new("nodetool")
            .arg("status")
            .output()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !output.status.success() {
            return Ok(metrics);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut total_nodes = 0u64;
        let mut up_nodes = 0u64;
        let mut down_nodes = 0u64;

        for line in stdout.lines() {
            if line.starts_with("UN") {
                // Up Normal
                up_nodes += 1;
                total_nodes += 1;
            } else if line.starts_with("DN") {
                // Down Normal
                down_nodes += 1;
                total_nodes += 1;
            } else if line.starts_with("UJ") || line.starts_with("DJ") || line.starts_with("UM")
            {
                // Joining/Moving nodes
                total_nodes += 1;
            }
        }

        metrics.push(Metric::gauge(
            "cassandra.nodes.total",
            total_nodes as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "cassandra.nodes.up",
            up_nodes as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "cassandra.nodes.down",
            down_nodes as f64,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect tpstats (thread pool statistics)
    async fn collect_tpstats(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let output = tokio::process::Command::new("nodetool")
            .arg("tpstats")
            .output()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !output.status.success() {
            return Ok(metrics);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse thread pool stats
        for line in stdout.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                let pool_name = parts[0];
                if let (Ok(active), Ok(pending), Ok(completed)) = (
                    parts[1].parse::<f64>(),
                    parts[2].parse::<f64>(),
                    parts[3].parse::<f64>(),
                ) {
                    metrics.push(Metric::gauge(
                        format!("cassandra.threadpool.{}.active", pool_name),
                        active,
                        self.name(),
                    ));
                    metrics.push(Metric::gauge(
                        format!("cassandra.threadpool.{}.pending", pool_name),
                        pending,
                        self.name(),
                    ));
                    metrics.push(Metric::gauge(
                        format!("cassandra.threadpool.{}.completed", pool_name),
                        completed,
                        self.name(),
                    ));
                }
            }
        }

        Ok(metrics)
    }

    /// Collect compaction stats
    async fn collect_compaction_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let output = tokio::process::Command::new("nodetool")
            .arg("compactionstats")
            .output()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !output.status.success() {
            return Ok(metrics);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse "pending tasks: N"
        for line in stdout.lines() {
            if line.starts_with("pending tasks:") {
                if let Some(num_str) = line.split(':').nth(1) {
                    if let Ok(tasks) = num_str.trim().parse::<f64>() {
                        metrics.push(Metric::gauge(
                            "cassandra.compaction.pending_tasks",
                            tasks,
                            self.name(),
                        ));
                    }
                }
            }
        }

        Ok(metrics)
    }

    /// Collect all Cassandra metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        // Check connectivity first
        if !self.check_cassandra_access().await {
            all_metrics.push(Metric::gauge(
                "cassandra.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "cassandra.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_node_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect node metrics: {}", e),
        }

        match self.collect_tpstats().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect tpstats: {}", e),
        }

        match self.collect_compaction_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect compaction metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Cassandra events
    async fn collect_cassandra_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check connectivity
        if !self.check_cassandra_access().await {
            events.push(
                Event::new(
                    "Cassandra Unavailable",
                    format!("Cannot connect to Cassandra at {:?}", self.hosts),
                    EventSeverity::Critical,
                    self.name(),
                    "cassandra_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check for down nodes
        let output = tokio::process::Command::new("nodetool")
            .arg("status")
            .output()
            .await;

        if let Ok(output) = output {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    if line.starts_with("DN") {
                        // Down Node
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 2 {
                            let ip = parts[1];
                            events.push(
                                Event::new(
                                    "Cassandra Node Down",
                                    format!("Node at {} is down", ip),
                                    EventSeverity::Critical,
                                    self.name(),
                                    "node_down",
                                )
                                .with_metadata("node_ip", ip),
                            );
                        }
                    }
                }
            }
        }

        // Check for pending compactions
        let output = tokio::process::Command::new("nodetool")
            .arg("compactionstats")
            .output()
            .await;

        if let Ok(output) = output {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    if line.starts_with("pending tasks:") {
                        if let Some(num_str) = line.split(':').nth(1) {
                            if let Ok(tasks) = num_str.trim().parse::<f64>() {
                                if tasks > 20.0 {
                                    events.push(
                                        Event::new(
                                            "High Compaction Tasks",
                                            format!("{} pending compaction tasks", tasks),
                                            EventSeverity::Warning,
                                            self.name(),
                                            "high_compactions",
                                        )
                                        .with_metadata("pending_tasks", &tasks.to_string()),
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
impl Integration for CassandraIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_cassandra_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_cassandra_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Cassandra at {:?}",
                self.hosts
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
    fn test_cassandra_integration_creation() {
        let cassandra = CassandraIntegration::new(&["localhost"], 9042);
        assert!(cassandra.is_ok());
    }
}
