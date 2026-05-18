//! NATS Integration
//!
//! Monitor NATS servers, connections, subscriptions, and message rates.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// NATS integration
pub struct NatsIntegration {
    config: IntegrationConfig,
    server_url: String,
}

impl NatsIntegration {
    /// Create new NATS integration
    pub fn new(server_url: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("queue_type".to_string(), "nats".to_string());

        let config = IntegrationConfig {
            name: "nats".to_string(),
            interval_secs: 30,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            server_url: server_url.to_string(),
        })
    }

    /// Check if NATS is accessible
    async fn check_nats_access(&self) -> bool {
        let timeout = tokio::time::Duration::from_secs(5);

        // Parse host and port from URL (e.g., "nats://localhost:4222")
        let url = self.server_url.replace("nats://", "");
        let parts: Vec<&str> = url.split(':').collect();
        if parts.len() != 2 {
            return false;
        }

        let host = parts[0];
        if let Ok(port) = parts[1].parse::<u16>() {
            return tokio::time::timeout(
                timeout,
                tokio::net::TcpStream::connect((host, port)),
            )
            .await
            .is_ok_and(|r| r.is_ok());
        }

        false
    }

    /// Get NATS metrics via HTTP monitoring endpoint
    async fn get_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // NATS monitoring endpoint is typically on port 8222
        let url = self.server_url.replace("4222", "8222") + "/varz";

        let client = reqwest::Client::new();
        let response = client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let data: serde_json::Value = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        // Server info
        if let Some(v) = data["server_id"].as_str() {
            metrics.push(Metric::gauge(
                "nats.server.info",
                1.0,
                self.name(),
            ).with_tag("server_id", v));
        }

        // Connections
        if let Some(v) = data["connections"].as_f64() {
            metrics.push(Metric::gauge(
                "nats.connections",
                v,
                self.name(),
            ));
        }

        // Total connections (including closed)
        if let Some(v) = data["total_connections"].as_f64() {
            metrics.push(Metric::gauge(
                "nats.connections.total",
                v,
                self.name(),
            ));
        }

        // Routes (cluster connections)
        if let Some(v) = data["routes"].as_f64() {
            metrics.push(Metric::gauge(
                "nats.routes",
                v,
                self.name(),
            ));
        }

        // Remotes (gateway connections)
        if let Some(v) = data["remotes"].as_f64() {
            metrics.push(Metric::gauge(
                "nats.remotes",
                v,
                self.name(),
            ));
        }

        // Subscriptions
        if let Some(v) = data["subscriptions"].as_f64() {
            metrics.push(Metric::gauge(
                "nats.subscriptions",
                v,
                self.name(),
            ));
        }

        // Memory
        if let Some(v) = data["mem"].as_f64() {
            metrics.push(Metric::gauge(
                "nats.memory.bytes",
                v,
                self.name(),
            ));
        }

        // Slow consumers
        if let Some(v) = data["slow_consumers"].as_f64() {
            metrics.push(Metric::gauge(
                "nats.slow_consumers",
                v,
                self.name(),
            ));
        }

        // Messages
        if let Some(in_msgs) = data["in_msgs"].as_f64() {
            metrics.push(Metric::gauge(
                "nats.messages.in",
                in_msgs,
                self.name(),
            ));
        }
        if let Some(out_msgs) = data["out_msgs"].as_f64() {
            metrics.push(Metric::gauge(
                "nats.messages.out",
                out_msgs,
                self.name(),
            ));
        }

        // Bytes
        if let Some(in_bytes) = data["in_bytes"].as_f64() {
            metrics.push(Metric::gauge(
                "nats.bytes.in",
                in_bytes,
                self.name(),
            ));
        }
        if let Some(out_bytes) = data["out_bytes"].as_f64() {
            metrics.push(Metric::gauge(
                "nats.bytes.out",
                out_bytes,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all NATS metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_nats_access().await {
            all_metrics.push(Metric::gauge(
                "nats.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "nats.connection.status",
            1.0,
            self.name(),
        ));

        match self.get_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect NATS metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect NATS events
    async fn collect_nats_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_nats_access().await {
            events.push(
                Event::new(
                    "NATS Unavailable",
                    format!("Cannot connect to NATS at {}", self.server_url),
                    EventSeverity::Critical,
                    self.name(),
                    "nats_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check for slow consumers
        let url = self.server_url.replace("4222", "8222") + "/varz";
        let client = reqwest::Client::new();

        if let Ok(response) = client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(data) = response.json::<serde_json::Value>().await {
                    if let Some(slow) = data["slow_consumers"].as_f64() {
                        if slow > 0.0 {
                            events.push(
                                Event::new(
                                    "NATS Slow Consumers",
                                    format!("{} slow consumers detected", slow),
                                    EventSeverity::Warning,
                                    self.name(),
                                    "slow_consumers",
                                )
                                .with_metadata("count", &slow.to_string()),
                            );
                        }
                    }
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for NatsIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_nats_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_nats_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to NATS at {}",
                self.server_url
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
    fn test_nats_integration_creation() {
        let nats = NatsIntegration::new("nats://localhost:4222");
        assert!(nats.is_ok());
    }
}
