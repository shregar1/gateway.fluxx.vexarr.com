//! TimescaleDB Integration
//!
//! Monitor TimescaleDB hypertables, chunks, and compression.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// TimescaleDB integration
pub struct TimescaleDbIntegration {
    config: IntegrationConfig,
    host: String,
    port: u16,
    database: String,
}

impl TimescaleDbIntegration {
    /// Create new TimescaleDB integration
    pub fn new(host: &str, port: u16, database: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("db_type".to_string(), "timescaledb".to_string());

        let config = IntegrationConfig {
            name: format!("timescaledb_{}", host),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            host: host.to_string(),
            port,
            database: database.to_string(),
        })
    }

    /// Check if TimescaleDB is accessible
    async fn check_timescaledb_access(&self) -> bool {
        let timeout = tokio::time::Duration::from_secs(5);
        tokio::time::timeout(
            timeout,
            tokio::net::TcpStream::connect((self.host.as_str(), self.port)),
        )
        .await
        .is_ok_and(|r| r.is_ok())
    }

    /// Collect hypertable metrics
    async fn collect_hypertable_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Placeholder - would query timescaledb_information.hypertables
        metrics.push(Metric::gauge(
            "timescaledb.hypertables.total",
            0.0,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect all TimescaleDB metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_timescaledb_access().await {
            all_metrics.push(Metric::gauge(
                "timescaledb.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "timescaledb.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_hypertable_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect hypertable metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect TimescaleDB events
    async fn collect_timescaledb_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_timescaledb_access().await {
            events.push(
                Event::new(
                    "TimescaleDB Unavailable",
                    format!("Cannot connect to TimescaleDB at {}:{}/{}", self.host, self.port, self.database),
                    EventSeverity::Critical,
                    self.name(),
                    "timescaledb_unavailable",
                ),
            );
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for TimescaleDbIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_timescaledb_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_timescaledb_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to TimescaleDB at {}:{}/{}",
                self.host, self.port, self.database
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
    fn test_timescaledb_integration_creation() {
        let timescaledb = TimescaleDbIntegration::new("localhost", 5432, "mydb");
        assert!(timescaledb.is_ok());
    }
}
