//! Apache ActiveMQ Integration
//!
//! Monitor ActiveMQ brokers, queues, topics, and connections.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// ActiveMQ integration
pub struct ActiveMqIntegration {
    config: IntegrationConfig,
    admin_url: String,
    username: String,
    password: String,
}

impl ActiveMqIntegration {
    /// Create new ActiveMQ integration
    pub fn new(admin_url: &str, username: &str, password: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("queue_type".to_string(), "activemq".to_string());

        let config = IntegrationConfig {
            name: "activemq".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            admin_url: admin_url.trim_end_matches('/').to_string(),
            username: username.to_string(),
            password: password.to_string(),
        })
    }

    /// Check if ActiveMQ is accessible
    async fn check_activemq_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=localhost",
            self.admin_url);

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

    /// Collect broker metrics via Jolokia API
    async fn collect_broker_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!(
            "{}/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=localhost",
            self.admin_url
        );

        let response = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .timeout(std::time::Duration::from_secs(30))
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

        if let Some(value) = data["value"].as_object() {
            // Memory usage
            if let Some(mem) = value["MemoryPercentUsage"].as_f64() {
                metrics.push(Metric::gauge(
                    "activemq.broker.memory_usage_percent",
                    mem,
                    self.name(),
                ));
            }

            // Store usage
            if let Some(store) = value["StorePercentUsage"].as_f64() {
                metrics.push(Metric::gauge(
                    "activemq.broker.store_usage_percent",
                    store,
                    self.name(),
                ));
            }

            // Temp usage
            if let Some(temp) = value["TempPercentUsage"].as_f64() {
                metrics.push(Metric::gauge(
                    "activemq.broker.temp_usage_percent",
                    temp,
                    self.name(),
                ));
            }

            // Queue count
            if let Some(queues) = value["Queues"].as_array() {
                metrics.push(Metric::gauge(
                    "activemq.queues.total",
                    queues.len() as f64,
                    self.name(),
                ));
            }

            // Topic count
            if let Some(topics) = value["Topics"].as_array() {
                metrics.push(Metric::gauge(
                    "activemq.topics.total",
                    topics.len() as f64,
                    self.name(),
                ));
            }

            // Total message count
            if let Some(count) = value["TotalMessageCount"].as_f64() {
                metrics.push(Metric::gauge(
                    "activemq.messages.total",
                    count,
                    self.name(),
                ));
            }

            // Total consumer count
            if let Some(count) = value["TotalConsumerCount"].as_f64() {
                metrics.push(Metric::gauge(
                    "activemq.consumers.total",
                    count,
                    self.name(),
                ));
            }

            // Total enqueue/dequeue counts
            if let Some(count) = value["TotalEnqueueCount"].as_f64() {
                metrics.push(Metric::gauge(
                    "activemq.messages.enqueued",
                    count,
                    self.name(),
                ));
            }
            if let Some(count) = value["TotalDequeueCount"].as_f64() {
                metrics.push(Metric::gauge(
                    "activemq.messages.dequeued",
                    count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect all ActiveMQ metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_activemq_access().await {
            all_metrics.push(Metric::gauge(
                "activemq.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "activemq.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_broker_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect broker metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect ActiveMQ events
    async fn collect_activemq_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_activemq_access().await {
            events.push(
                Event::new(
                    "ActiveMQ Unavailable",
                    format!("Cannot connect to ActiveMQ at {}", self.admin_url),
                    EventSeverity::Critical,
                    self.name(),
                    "activemq_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check memory usage
        let client = reqwest::Client::new();
        let url = format!(
            "{}/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=localhost",
            self.admin_url
        );

        if let Ok(response) = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(data) = response.json::<serde_json::Value>().await {
                    if let Some(value) = data["value"].as_object() {
                        if let Some(mem) = value["MemoryPercentUsage"].as_f64() {
                            if mem > 90.0 {
                                events.push(
                                    Event::new(
                                        "ActiveMQ High Memory Usage",
                                        format!("Memory usage is {}%", mem),
                                        EventSeverity::Warning,
                                        self.name(),
                                        "high_memory",
                                    )
                                    .with_metadata("memory_percent", &mem.to_string()),
                                );
                            }
                        }

                        if let Some(store) = value["StorePercentUsage"].as_f64() {
                            if store > 90.0 {
                                events.push(
                                    Event::new(
                                        "ActiveMQ High Store Usage",
                                        format!("Store usage is {}%", store),
                                        EventSeverity::Warning,
                                        self.name(),
                                        "high_store",
                                    )
                                    .with_metadata("store_percent", &store.to_string()),
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
impl Integration for ActiveMqIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_activemq_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_activemq_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to ActiveMQ at {}",
                self.admin_url
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
    fn test_activemq_integration_creation() {
        let activemq = ActiveMqIntegration::new("http://localhost:8161", "admin", "admin");
        assert!(activemq.is_ok());
    }
}
