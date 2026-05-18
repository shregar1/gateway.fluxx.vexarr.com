//! PostgreSQL Integration

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// PostgreSQL integration
pub struct PostgresIntegration {
    config: IntegrationConfig,
    connection_string: String,
}

impl PostgresIntegration {
    /// Create new PostgreSQL integration
    pub fn new(connection_string: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("db_type".to_string(), "postgresql".to_string());

        let config = IntegrationConfig {
            name: "postgresql".to_string(),
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
        // Simple parsing: "postgres://host:port/database" or "host=host port=port"
        let default_host = "localhost".to_string();
        let default_port = 5432u16;

        if let Some(host_part) = self.connection_string.strip_prefix("postgres://") {
            if let Some(at_idx) = host_part.rfind('@') {
                let after_at = &host_part[at_idx + 1..];
                if let Some(slash_idx) = after_at.find('/') {
                    let host_port = &after_at[..slash_idx];
                    if let Some(colon_idx) = host_port.find(':') {
                        let host = host_port[..colon_idx].to_string();
                        let port = host_port[colon_idx + 1..].parse().unwrap_or(default_port);
                        return (host, port);
                    }
                    return (host_port.to_string(), default_port);
                }
            } else if let Some(slash_idx) = host_part.find('/') {
                let host_port = &host_part[..slash_idx];
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

    /// Check if PostgreSQL port is reachable
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
impl Integration for PostgresIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];
        
        // Check connectivity
        let is_connected = self.check_connectivity().await;
        
        metrics.push(Metric::gauge(
            "postgresql.connection.status",
            if is_connected { 1.0 } else { 0.0 },
            self.name(),
        ).with_tag("host", &self.parse_host_port().0));

        if is_connected {
            // Placeholder for actual metrics collection
            // In production, would query pg_stat_activity, pg_stat_database
            metrics.push(Metric::gauge(
                "postgresql.active_connections",
                0.0, // Would query: SELECT count(*) FROM pg_stat_activity WHERE state = 'active'
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "postgresql.idle_connections",
                0.0, // Would query: SELECT count(*) FROM pg_stat_activity WHERE state = 'idle'
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "postgresql.transactions.commited",
                0.0,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "postgresql.transactions.rolled_back",
                0.0,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "postgresql.blocks.read",
                0.0,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "postgresql.blocks.hit",
                0.0,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check connectivity
        if !self.check_connectivity().await {
            events.push(
                Event::new(
                    "PostgreSQL Connection Failed",
                    format!("Cannot connect to PostgreSQL at {}", self.connection_string),
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
                "Cannot connect to PostgreSQL at {}",
                self.connection_string
            )))
        }
    }

    fn config(&self) -> &IntegrationConfig {
        &self.config
    }
}
