//! MySQL Integration

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// MySQL integration
pub struct MySqlIntegration {
    config: IntegrationConfig,
    connection_string: String,
}

impl MySqlIntegration {
    /// Create new MySQL integration
    pub fn new(connection_string: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("db_type".to_string(), "mysql".to_string());

        let config = IntegrationConfig {
            name: "mysql".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            connection_string: connection_string.to_string(),
        })
    }

    /// Parse connection string to extract host and port
    fn parse_host_port(&self) -> (String, u16) {
        let default_host = "localhost".to_string();
        let default_port = 3306u16;

        // Simple parsing: "user:pass@tcp(host:port)/dbname" or "host:port"
        if let Some(tcp_start) = self.connection_string.find("@tcp(") {
            if let Some(tcp_end) = self.connection_string[tcp_start..].find(')') {
                let host_port = &self.connection_string[tcp_start + 5..tcp_start + tcp_end];
                if let Some(colon_idx) = host_port.find(':') {
                    let host = host_port[..colon_idx].to_string();
                    let port = host_port[colon_idx + 1..].parse().unwrap_or(default_port);
                    return (host, port);
                }
                return (host_port.to_string(), default_port);
            }
        }

        (default_host, default_port)
    }

    /// Check if MySQL port is reachable
    async fn check_connectivity(&self) -> bool {
        let (host, port) = self.parse_host_port();
        let timeout = tokio::time::Duration::from_secs(5);
        
        tokio::time::timeout(
            timeout,
            tokio::net::TcpStream::connect((host.as_str(), port)),
        )
        .await
        .is_ok_and(|r| r.is_ok())
    }
}

#[async_trait]
impl Integration for MySqlIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];
        
        let is_connected = self.check_connectivity().await;
        
        metrics.push(Metric::gauge(
            "mysql.connection.status",
            if is_connected { 1.0 } else { 0.0 },
            self.name(),
        ).with_tag("host", &self.parse_host_port().0));

        if is_connected {
            // Placeholder for actual MySQL metrics
            metrics.push(Metric::gauge(
                "mysql.threads_connected",
                0.0, // SHOW STATUS LIKE 'Threads_connected'
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "mysql.threads_running",
                0.0, // SHOW STATUS LIKE 'Threads_running'
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "mysql.questions",
                0.0, // SHOW STATUS LIKE 'Questions'
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "mysql.slow_queries",
                0.0, // SHOW STATUS LIKE 'Slow_queries'
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "mysql.innodb_buffer_pool_reads",
                0.0,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "mysql.innodb_buffer_pool_read_requests",
                0.0,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_connectivity().await {
            events.push(
                Event::new(
                    "MySQL Connection Failed",
                    format!("Cannot connect to MySQL at {}", self.connection_string),
                    EventSeverity::Critical,
                    self.name(),
                    "connection_failed",
                ),
            );
        }

        Ok(events)
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_connectivity().await {
            Ok(())
        } else {
            Err(IntegrationError::Other(format!(
                "Cannot connect to MySQL at {}",
                self.connection_string
            )))
        }
    }

    fn config(&self) -> &IntegrationConfig {
        &self.config
    }
}
