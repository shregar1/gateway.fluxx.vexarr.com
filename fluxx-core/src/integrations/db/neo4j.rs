//! Neo4j Integration
//!
//! Monitor Neo4j graph database nodes, relationships, and query performance.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Neo4j integration
pub struct Neo4jIntegration {
    config: IntegrationConfig,
    host: String,
    port: u16,
    username: String,
    password: String,
}

impl Neo4jIntegration {
    /// Create new Neo4j integration
    pub fn new(host: &str, port: u16, username: &str, password: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("db_type".to_string(), "neo4j".to_string());

        let config = IntegrationConfig {
            name: format!("neo4j_{}", host),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            host: host.to_string(),
            port,
            username: username.to_string(),
            password: password.to_string(),
        })
    }

    /// Check if Neo4j is accessible via HTTP API
    async fn check_neo4j_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("http://{}:{}/db/data/", self.host, self.port);

        match client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => {
                // Try Bolt port connectivity
                let timeout = tokio::time::Duration::from_secs(5);
                tokio::time::timeout(
                    timeout,
                    tokio::net::TcpStream::connect((self.host.as_str(), self.port)),
                )
                .await
                .is_ok_and(|r| r.is_ok())
            }
        }
    }

    /// Collect metrics from Neo4j
    async fn collect_neo4j_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        if !self.check_neo4j_access().await {
            metrics.push(Metric::gauge(
                "neo4j.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(metrics);
        }

        metrics.push(Metric::gauge(
            "neo4j.connection.status",
            1.0,
            self.name(),
        ));

        // Placeholder for Neo4j metrics via JMX or HTTP
        // In production would query:
        // - CALL dbms.queryJmx("org.neo.neo4j:*")
        // - Total node count: MATCH (n) RETURN count(n)
        // - Total relationship count: MATCH ()-[r]->() RETURN count(r)
        // - Store sizes

        Ok(metrics)
    }

    /// Collect Neo4j events
    async fn collect_neo4j_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_neo4j_access().await {
            events.push(
                Event::new(
                    "Neo4j Unavailable",
                    format!("Cannot connect to Neo4j at {}:{}", self.host, self.port),
                    EventSeverity::Critical,
                    self.name(),
                    "neo4j_unavailable",
                ),
            );
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for Neo4jIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_neo4j_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_neo4j_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_neo4j_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Neo4j at {}:{}",
                self.host, self.port
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
    fn test_neo4j_integration_creation() {
        let neo4j = Neo4jIntegration::new("localhost", 7474, "neo4j", "password");
        assert!(neo4j.is_ok());
    }
}
