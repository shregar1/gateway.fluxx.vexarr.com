//! CockroachDB Integration
//!
//! Monitor CockroachDB distributed SQL database.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// CockroachDB integration
pub struct CockroachDbIntegration {
    config: IntegrationConfig,
    host: String,
    port: u16,
}

impl CockroachDbIntegration {
    /// Create new CockroachDB integration
    pub fn new(host: &str, port: u16) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("db_type".to_string(), "cockroachdb".to_string());

        let config = IntegrationConfig {
            name: format!("cockroachdb_{}", host),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            host: host.to_string(),
            port,
        })
    }

    /// Check if CockroachDB is accessible
    async fn check_cockroachdb_access(&self) -> bool {
        // Try HTTP API first
        let client = reqwest::Client::new();
        let url = format!("http://{}:{}/health", self.host, self.port - 1); // HTTP port is SQL port - 1

        match client
            .get(&url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => {
                // Fallback to SQL port
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

    /// Collect all CockroachDB metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        if !self.check_cockroachdb_access().await {
            metrics.push(Metric::gauge(
                "cockroachdb.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(metrics);
        }

        metrics.push(Metric::gauge(
            "cockroachdb.connection.status",
            1.0,
            self.name(),
        ));

        // Placeholder for CockroachDB metrics
        // Would query /_status/vars for Prometheus metrics
        // or use SQL to query crdb_internal tables

        Ok(metrics)
    }

    /// Collect CockroachDB events
    async fn collect_cockroachdb_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_cockroachdb_access().await {
            events.push(
                Event::new(
                    "CockroachDB Unavailable",
                    format!("Cannot connect to CockroachDB at {}:{}", self.host, self.port),
                    EventSeverity::Critical,
                    self.name(),
                    "cockroachdb_unavailable",
                ),
            );
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for CockroachDbIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_cockroachdb_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_cockroachdb_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to CockroachDB at {}:{}",
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
    fn test_cockroachdb_integration_creation() {
        let cockroachdb = CockroachDbIntegration::new("localhost", 26257);
        assert!(cockroachdb.is_ok());
    }
}
