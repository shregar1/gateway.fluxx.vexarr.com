//! Redis Integration

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Redis integration
pub struct RedisIntegration {
    config: IntegrationConfig,
    host: String,
    port: u16,
}

impl RedisIntegration {
    /// Create new Redis integration
    pub fn new(host: &str, port: u16) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("db_type".to_string(), "redis".to_string());

        let config = IntegrationConfig {
            name: "redis".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            host: host.to_string(),
            port,
        })
    }

    /// Check if Redis port is reachable
    async fn check_connectivity(&self) -> bool {
        let timeout = tokio::time::Duration::from_secs(5);
        
        tokio::time::timeout(
            timeout,
            tokio::net::TcpStream::connect((self.host.as_str(), self.port)),
        )
        .await
        .is_ok_and(|r| r.is_ok())
    }
}

#[async_trait]
impl Integration for RedisIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];
        
        // Check connectivity
        let is_connected = self.check_connectivity().await;
        
        metrics.push(Metric::gauge(
            "redis.connection.status",
            if is_connected { 1.0 } else { 0.0 },
            self.name(),
        ).with_tag("host", &self.host));

        if is_connected {
            // Placeholder for actual Redis metrics
            // In production, would use redis-rs or raw TCP protocol
            metrics.push(Metric::gauge(
                "redis.connected_clients",
                0.0, // INFO clients
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "redis.used_memory",
                0.0, // INFO memory
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "redis.total_commands_processed",
                0.0, // INFO stats
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "redis.keyspace_hits",
                0.0, // INFO stats
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "redis.keyspace_misses",
                0.0, // INFO stats
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "redis.uptime_in_seconds",
                0.0, // INFO server
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
                    "Redis Connection Failed",
                    format!("Cannot connect to Redis at {}:{}", self.host, self.port),
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
                "Cannot connect to Redis at {}:{}",
                self.host, self.port
            )))
        }
    }

    fn config(&self) -> &IntegrationConfig {
        &self.config
    }
}
