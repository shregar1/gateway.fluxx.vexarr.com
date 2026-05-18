//! RabbitMQ Integration
//!
//! Monitor RabbitMQ queues, exchanges, connections, and node health.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// RabbitMQ integration
pub struct RabbitMqIntegration {
    config: IntegrationConfig,
    base_url: String,
    username: String,
    password: String,
}

impl RabbitMqIntegration {
    /// Create new RabbitMQ integration
    pub fn new(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
    ) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("queue_type".to_string(), "rabbitmq".to_string());

        let config = IntegrationConfig {
            name: "rabbitmq".to_string(),
            interval_secs: 30,
            tags,
            timeout_secs: 10,
        };

        let base_url = format!("http://{}:{}/api", host, port);

        Ok(Self {
            config,
            base_url,
            username: username.to_string(),
            password: password.to_string(),
        })
    }

    /// Check if RabbitMQ Management API is accessible
    async fn check_rabbitmq_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/overview", self.base_url);
        
        match client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Collect overview metrics
    async fn collect_overview_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/overview", self.base_url);
        
        let response = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
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

        // Object totals
        if let Some(object_totals) = data["object_totals"].as_object() {
            if let Some(connections) = object_totals["connections"].as_f64() {
                metrics.push(Metric::gauge(
                    "rabbitmq.connections.total",
                    connections,
                    self.name(),
                ));
            }
            if let Some(channels) = object_totals["channels"].as_f64() {
                metrics.push(Metric::gauge(
                    "rabbitmq.channels.total",
                    channels,
                    self.name(),
                ));
            }
            if let Some(queues) = object_totals["queues"].as_f64() {
                metrics.push(Metric::gauge(
                    "rabbitmq.queues.total",
                    queues,
                    self.name(),
                ));
            }
            if let Some(consumers) = object_totals["consumers"].as_f64() {
                metrics.push(Metric::gauge(
                    "rabbitmq.consumers.total",
                    consumers,
                    self.name(),
                ));
            }
        }

        // Message stats
        if let Some(msg_stats) = data["message_stats"].as_object() {
            if let Some(publish) = msg_stats["publish"].as_f64() {
                metrics.push(Metric::gauge(
                    "rabbitmq.messages.published",
                    publish,
                    self.name(),
                ));
            }
            if let Some(deliver) = msg_stats["deliver"].as_f64() {
                metrics.push(Metric::gauge(
                    "rabbitmq.messages.delivered",
                    deliver,
                    self.name(),
                ));
            }
            if let Some(ack) = msg_stats["ack"].as_f64() {
                metrics.push(Metric::gauge(
                    "rabbitmq.messages.acked",
                    ack,
                    self.name(),
                ));
            }
        }

        // Queue totals
        if let Some(queue_totals) = data["queue_totals"].as_object() {
            if let Some(messages) = queue_totals["messages"].as_f64() {
                metrics.push(Metric::gauge(
                    "rabbitmq.messages.total",
                    messages,
                    self.name(),
                ));
            }
            if let Some(ready) = queue_totals["messages_ready"].as_f64() {
                metrics.push(Metric::gauge(
                    "rabbitmq.messages.ready",
                    ready,
                    self.name(),
                ));
            }
            if let Some(unack) = queue_totals["messages_unacknowledged"].as_f64() {
                metrics.push(Metric::gauge(
                    "rabbitmq.messages.unacknowledged",
                    unack,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect node metrics
    async fn collect_node_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/nodes", self.base_url);
        
        let response = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let nodes: Vec<serde_json::Value> = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        for node in nodes {
            if let Some(name) = node["name"].as_str() {
                // Memory usage
                if let Some(mem_used) = node["mem_used"].as_f64() {
                    metrics.push(Metric::gauge(
                        "rabbitmq.node.memory_used_bytes",
                        mem_used,
                        self.name(),
                    ).with_tag("node", name));
                }
                if let Some(mem_limit) = node["mem_limit"].as_f64() {
                    metrics.push(Metric::gauge(
                        "rabbitmq.node.memory_limit_bytes",
                        mem_limit,
                        self.name(),
                    ).with_tag("node", name));
                }

                // Disk usage
                if let Some(disk_free) = node["disk_free"].as_f64() {
                    metrics.push(Metric::gauge(
                        "rabbitmq.node.disk_free_bytes",
                        disk_free,
                        self.name(),
                    ).with_tag("node", name));
                }

                // File descriptors
                if let Some(fd_used) = node["fd_used"].as_f64() {
                    metrics.push(Metric::gauge(
                        "rabbitmq.node.fd_used",
                        fd_used,
                        self.name(),
                    ).with_tag("node", name));
                }
                if let Some(fd_total) = node["fd_total"].as_f64() {
                    metrics.push(Metric::gauge(
                        "rabbitmq.node.fd_total",
                        fd_total,
                        self.name(),
                    ).with_tag("node", name));
                }

                // Sockets
                if let Some(sockets_used) = node["sockets_used"].as_f64() {
                    metrics.push(Metric::gauge(
                        "rabbitmq.node.sockets_used",
                        sockets_used,
                        self.name(),
                    ).with_tag("node", name));
                }
            }
        }

        Ok(metrics)
    }

    /// Collect queue details
    async fn collect_queue_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/queues", self.base_url);
        
        let response = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let queues: Vec<serde_json::Value> = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        for queue in queues {
            if let (Some(name), Some(vhost)) = (
                queue["name"].as_str(),
                queue["vhost"].as_str(),
            ) {
                let queue_tag = format!("{}/{}", vhost, name);

                if let Some(messages) = queue["messages"].as_f64() {
                    metrics.push(Metric::gauge(
                        "rabbitmq.queue.messages",
                        messages,
                        self.name(),
                    ).with_tag("queue", &queue_tag));
                }
                if let Some(ready) = queue["messages_ready"].as_f64() {
                    metrics.push(Metric::gauge(
                        "rabbitmq.queue.messages_ready",
                        ready,
                        self.name(),
                    ).with_tag("queue", &queue_tag));
                }
                if let Some(unack) = queue["messages_unacknowledged"].as_f64() {
                    metrics.push(Metric::gauge(
                        "rabbitmq.queue.messages_unacknowledged",
                        unack,
                        self.name(),
                    ).with_tag("queue", &queue_tag));
                }
                if let Some(consumers) = queue["consumers"].as_f64() {
                    metrics.push(Metric::gauge(
                        "rabbitmq.queue.consumers",
                        consumers,
                        self.name(),
                    ).with_tag("queue", &queue_tag));
                }
            }
        }

        Ok(metrics)
    }

    /// Collect all RabbitMQ metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        // Check connectivity first
        if !self.check_rabbitmq_access().await {
            all_metrics.push(Metric::gauge(
                "rabbitmq.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "rabbitmq.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_overview_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect overview metrics: {}", e),
        }

        match self.collect_node_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect node metrics: {}", e),
        }

        match self.collect_queue_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect queue metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect RabbitMQ events
    async fn collect_rabbitmq_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check connectivity
        if !self.check_rabbitmq_access().await {
            events.push(
                Event::new(
                    "RabbitMQ Unavailable",
                    format!("Cannot connect to RabbitMQ at {}", self.base_url),
                    EventSeverity::Critical,
                    self.name(),
                    "rabbitmq_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check for queues with high message count
        let client = reqwest::Client::new();
        let url = format!("{}/queues", self.base_url);
        
        if let Ok(response) = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(queues) = response.json::<Vec<serde_json::Value>>().await {
                    for queue in queues {
                        if let (Some(name), Some(vhost), Some(messages)) = (
                            queue["name"].as_str(),
                            queue["vhost"].as_str(),
                            queue["messages"].as_f64(),
                        ) {
                            if messages > 10000.0 {
                                events.push(
                                    Event::new(
                                        "High Queue Depth",
                                        format!("Queue {}/{} has {} messages", vhost, name, messages),
                                        EventSeverity::Warning,
                                        self.name(),
                                        "high_queue_depth",
                                    )
                                    .with_metadata("queue", name)
                                    .with_metadata("vhost", vhost)
                                    .with_metadata("messages", &messages.to_string()),
                                );
                            }
                        }
                    }
                }
            }
        }

        // Check for memory alarms
        let url = format!("{}/nodes", self.base_url);
        if let Ok(response) = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(nodes) = response.json::<Vec<serde_json::Value>>().await {
                    for node in nodes {
                        if let (Some(name), Some(mem_alarm)) = (
                            node["name"].as_str(),
                            node["mem_alarm"].as_bool(),
                        ) {
                            if mem_alarm {
                                events.push(
                                    Event::new(
                                        "RabbitMQ Memory Alarm",
                                        format!("Node {} has triggered memory alarm", name),
                                        EventSeverity::Critical,
                                        self.name(),
                                        "memory_alarm",
                                    )
                                    .with_metadata("node", name),
                                );
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
impl Integration for RabbitMqIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_rabbitmq_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_rabbitmq_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to RabbitMQ at {}",
                self.base_url
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
    fn test_rabbitmq_integration_creation() {
        let rabbitmq = RabbitMqIntegration::new("localhost", 15672, "guest", "guest");
        assert!(rabbitmq.is_ok());
    }
}
