//! Apache Kafka Integration
//!
//! Monitor Kafka brokers, topics, consumer groups, and partitions.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Kafka integration
pub struct KafkaIntegration {
    config: IntegrationConfig,
    bootstrap_servers: String,
}

impl KafkaIntegration {
    /// Create new Kafka integration
    pub fn new(bootstrap_servers: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("queue_type".to_string(), "kafka".to_string());

        let config = IntegrationConfig {
            name: "kafka".to_string(),
            interval_secs: 30,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            bootstrap_servers: bootstrap_servers.to_string(),
        })
    }

    /// Check if Kafka is reachable
    async fn check_kafka_access(&self) -> bool {
        // Try to connect to one of the bootstrap servers
        let servers: Vec<&str> = self.bootstrap_servers.split(',').collect();
        
        for server in servers {
            let parts: Vec<&str> = server.split(':').collect();
            if parts.len() != 2 {
                continue;
            }
            
            let host = parts[0];
            if let Ok(port) = parts[1].parse::<u16>() {
                let timeout = tokio::time::Duration::from_secs(5);
                if tokio::time::timeout(
                    timeout,
                    tokio::net::TcpStream::connect((host, port)),
                )
                .await
                .is_ok_and(|r| r.is_ok())
                {
                    return true;
                }
            }
        }
        
        false
    }

    /// Collect broker metrics using kafka-broker-api-versions.sh or similar
    async fn collect_broker_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Check if kafka-topics command is available
        let mut cmd = tokio::process::Command::new("kafka-topics.sh");
        cmd.args([
            "--bootstrap-server", &self.bootstrap_servers,
            "--list",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let topic_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "kafka.topics.total",
                    topic_count,
                    self.name(),
                ));
            }
        }

        // Try kafka-consumer-groups.sh for consumer lag
        let mut cmd = tokio::process::Command::new("kafka-consumer-groups.sh");
        cmd.args([
            "--bootstrap-server", &self.bootstrap_servers,
            "--all-groups",
            "--describe",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let mut total_lag: i64 = 0;
                let mut consumer_groups = 0u64;
                
                for line in stdout.lines() {
                    // Parse lines like: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 5 && parts[0] != "GROUP" {
                        if let Ok(lag) = parts[4].parse::<i64>() {
                            total_lag += lag;
                            consumer_groups += 1;
                        }
                    }
                }
                
                metrics.push(Metric::gauge(
                    "kafka.consumer_lag.total",
                    total_lag as f64,
                    self.name(),
                ));
                metrics.push(Metric::gauge(
                    "kafka.consumer_groups.total",
                    consumer_groups as f64,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect metrics via JMX if available
    async fn collect_jmx_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Try to use JMX tools if available
        // This is a placeholder - actual JMX collection would require
        // proper JMX client setup

        // Common Kafka JMX metrics:
        // kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec
        // kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec
        // kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec
        
        metrics.push(Metric::gauge(
            "kafka.messages_in_per_sec",
            0.0, // Placeholder
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "kafka.bytes_in_per_sec",
            0.0,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "kafka.bytes_out_per_sec",
            0.0,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect all Kafka metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        // Check connectivity first
        if !self.check_kafka_access().await {
            all_metrics.push(Metric::gauge(
                "kafka.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "kafka.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_broker_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect broker metrics: {}", e),
        }

        match self.collect_jmx_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect JMX metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Kafka events
    async fn collect_kafka_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check connectivity
        if !self.check_kafka_access().await {
            events.push(
                Event::new(
                    "Kafka Unavailable",
                    format!("Cannot connect to Kafka at {}", self.bootstrap_servers),
                    EventSeverity::Critical,
                    self.name(),
                    "kafka_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check consumer lag
        let mut cmd = tokio::process::Command::new("kafka-consumer-groups.sh");
        cmd.args([
            "--bootstrap-server", &self.bootstrap_servers,
            "--all-groups",
            "--describe",
        ]);

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                
                for line in stdout.lines() {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 5 && parts[0] != "GROUP" {
                        if let Ok(lag) = parts[4].parse::<i64>() {
                            if lag > 10000 {
                                let group = parts.get(0).copied().unwrap_or("unknown");
                                let topic = parts.get(1).copied().unwrap_or("unknown");
                                events.push(
                                    Event::new(
                                        "High Consumer Lag",
                                        format!("Consumer group {} on topic {} has lag of {}", group, topic, lag),
                                        EventSeverity::Warning,
                                        self.name(),
                                        "high_consumer_lag",
                                    )
                                    .with_metadata("group", group)
                                    .with_metadata("topic", topic)
                                    .with_metadata("lag", &lag.to_string()),
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
impl Integration for KafkaIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_kafka_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_kafka_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Kafka at {}",
                self.bootstrap_servers
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
    fn test_kafka_integration_creation() {
        let kafka = KafkaIntegration::new("localhost:9092");
        assert!(kafka.is_ok());
    }
}
