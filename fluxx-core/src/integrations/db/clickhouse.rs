//! ClickHouse Integration
//!
//! Monitor ClickHouse clusters, tables, queries, and replication.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// ClickHouse integration
pub struct ClickHouseIntegration {
    config: IntegrationConfig,
    host: String,
    port: u16,
    database: String,
    username: Option<String>,
    password: Option<String>,
}

impl ClickHouseIntegration {
    /// Create new ClickHouse integration
    pub fn new(host: &str, port: u16, database: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("db_type".to_string(), "clickhouse".to_string());

        let config = IntegrationConfig {
            name: format!("clickhouse_{}", host),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            host: host.to_string(),
            port,
            database: database.to_string(),
            username: None,
            password: None,
        })
    }

    /// Create with authentication
    pub fn with_auth(mut self, username: &str, password: &str) -> Self {
        self.username = Some(username.to_string());
        self.password = Some(password.to_string());
        self
    }

    /// Build HTTP URL for queries
    fn build_url(&self, query: &str) -> String {
        let base = format!("http://{}:{}", self.host, self.port);
        format!("{}/?database={}&query={}", base, self.database, url_escape(query))
    }

    /// Check if ClickHouse is accessible
    async fn check_clickhouse_access(&self) -> bool {
        let client = reqwest::Client::new();
        let query = "SELECT 1";
        let url = self.build_url(query);

        let mut request = client.get(&url);
        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            request = request.basic_auth(user, Some(pass));
        }

        match request
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Execute query and get result as string
    async fn execute_query(&self, query: &str) -> Result<String, IntegrationError> {
        let client = reqwest::Client::new();
        let url = self.build_url(query);

        let mut request = client.get(&url);
        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            request = request.basic_auth(user, Some(pass));
        }

        let response = request
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Err(IntegrationError::Query(format!(
                "Query failed with status: {}",
                response.status()
            )));
        }

        let text = response
            .text()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        Ok(text)
    }

    /// Collect system metrics
    async fn collect_system_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Get system.events
        let query = "SELECT event, value FROM system.events WHERE value > 0 FORMAT JSONCompact";
        let result = self.execute_query(query).await?;

        // Parse simple format (tab-separated values)
        for line in result.lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 2 {
                if let Ok(value) = parts[1].parse::<f64>() {
                    metrics.push(Metric::gauge(
                        format!("clickhouse.events.{}", parts[0]),
                        value,
                        self.name(),
                    ));
                }
            }
        }

        // Get system.metrics
        let query = "SELECT metric, value FROM system.metrics FORMAT JSONCompact";
        let result = self.execute_query(query).await?;

        for line in result.lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 2 {
                if let Ok(value) = parts[1].parse::<f64>() {
                    metrics.push(Metric::gauge(
                        format!("clickhouse.metrics.{}", parts[0]),
                        value,
                        self.name(),
                    ));
                }
            }
        }

        Ok(metrics)
    }

    /// Collect asynchronous metrics
    async fn collect_async_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let query = "SELECT metric, value FROM system.asynchronous_metrics FORMAT JSONCompact";
        let result = self.execute_query(query).await?;

        for line in result.lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 2 {
                if let Ok(value) = parts[1].parse::<f64>() {
                    metrics.push(Metric::gauge(
                        format!("clickhouse.async.{}", parts[0]),
                        value,
                        self.name(),
                    ));
                }
            }
        }

        Ok(metrics)
    }

    /// Collect table statistics
    async fn collect_table_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let query = r#"
            SELECT 
                database,
                table,
                count() as parts,
                sum(rows) as total_rows,
                sum(bytes) as total_bytes
            FROM system.parts
            WHERE active = 1
            GROUP BY database, table
            FORMAT JSONCompact
        "#;

        let result = self.execute_query(query).await?;
        let mut total_tables = 0u64;
        let mut total_rows: f64 = 0.0;
        let mut total_bytes: f64 = 0.0;

        for line in result.lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 5 {
                total_tables += 1;
                if let Ok(rows) = parts[3].parse::<f64>() {
                    total_rows += rows;
                }
                if let Ok(bytes) = parts[4].parse::<f64>() {
                    total_bytes += bytes;
                }
            }
        }

        metrics.push(Metric::gauge(
            "clickhouse.tables.total",
            total_tables as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "clickhouse.tables.total_rows",
            total_rows,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "clickhouse.tables.total_bytes",
            total_bytes,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect process list (active queries)
    async fn collect_query_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let query = r#"
            SELECT 
                count() as running_queries,
                sum(elapsed) as total_elapsed
            FROM system.processes
            FORMAT JSONCompact
        "#;

        let result = self.execute_query(query).await?;

        for line in result.lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 2 {
                if let Ok(running) = parts[0].parse::<f64>() {
                    metrics.push(Metric::gauge(
                        "clickhouse.queries.running",
                        running,
                        self.name(),
                    ));
                }
                if let Ok(elapsed) = parts[1].parse::<f64>() {
                    metrics.push(Metric::gauge(
                        "clickhouse.queries.total_elapsed_seconds",
                        elapsed,
                        self.name(),
                    ));
                }
            }
        }

        Ok(metrics)
    }

    /// Collect all ClickHouse metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        // Check connectivity first
        if !self.check_clickhouse_access().await {
            all_metrics.push(Metric::gauge(
                "clickhouse.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "clickhouse.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_system_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect system metrics: {}", e),
        }

        match self.collect_async_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect async metrics: {}", e),
        }

        match self.collect_table_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect table metrics: {}", e),
        }

        match self.collect_query_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect query metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect ClickHouse events
    async fn collect_clickhouse_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check connectivity
        if !self.check_clickhouse_access().await {
            events.push(
                Event::new(
                    "ClickHouse Unavailable",
                    format!("Cannot connect to ClickHouse at {}:{}", self.host, self.port),
                    EventSeverity::Critical,
                    self.name(),
                    "clickhouse_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check for long-running queries
        let query = r#"
            SELECT 
                query_id,
                user,
                elapsed,
                query
            FROM system.processes
            WHERE elapsed > 300
            FORMAT JSONCompact
        "#;

        if let Ok(result) = self.execute_query(query).await {
            for line in result.lines() {
                let parts: Vec<&str> = line.split('\t').collect();
                if parts.len() >= 4 {
                    let query_id = parts[0];
                    let user = parts[1];
                    let elapsed = parts[2];
                    events.push(
                        Event::new(
                            "Long Running ClickHouse Query",
                            format!("Query {} has been running for {} seconds", query_id, elapsed),
                            EventSeverity::Warning,
                            self.name(),
                            "long_running_query",
                        )
                        .with_metadata("query_id", query_id)
                        .with_metadata("user", user)
                        .with_metadata("elapsed", elapsed),
                    );
                }
            }
        }

        // Check for failed queries (last 5 minutes)
        let query = r#"
            SELECT 
                count()
            FROM system.query_log
            WHERE event_time > now() - INTERVAL 5 MINUTE
              AND type = 'ExceptionWhileProcessing'
            FORMAT JSONCompact
        "#;

        if let Ok(result) = self.execute_query(query).await {
            if let Ok(failed_count) = result.trim().parse::<f64>() {
                if failed_count > 0.0 {
                    events.push(
                        Event::new(
                            "ClickHouse Query Failures",
                            format!("{} queries failed in last 5 minutes", failed_count),
                            EventSeverity::Warning,
                            self.name(),
                            "query_failures",
                        )
                        .with_metadata("failed_count", &failed_count.to_string()),
                    );
                }
            }
        }

        // Check replica lag (for replicated tables)
        let query = r#"
            SELECT 
                database,
                table,
                absolute_delay
            FROM system.replicas
            WHERE absolute_delay > 60
            FORMAT JSONCompact
        "#;

        if let Ok(result) = self.execute_query(query).await {
            for line in result.lines() {
                let parts: Vec<&str> = line.split('\t').collect();
                if parts.len() >= 3 {
                    let database = parts[0];
                    let table = parts[1];
                    let delay = parts[2];
                    events.push(
                        Event::new(
                            "ClickHouse Replica Lag",
                            format!("Table {}.{} is {} seconds behind", database, table, delay),
                            EventSeverity::Warning,
                            self.name(),
                            "replica_lag",
                        )
                        .with_metadata("database", database)
                        .with_metadata("table", table)
                        .with_metadata("delay_seconds", delay),
                    );
                }
            }
        }

        Ok(events)
    }
}

/// Simple URL escape for query strings
fn url_escape(s: &str) -> String {
    s.replace(' ', "%20")
        .replace('\n', "%0A")
        .replace('\t', "%09")
        .replace('"', "%22")
}

#[async_trait]
impl Integration for ClickHouseIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_clickhouse_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_clickhouse_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to ClickHouse at {}:{}",
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
    fn test_clickhouse_integration_creation() {
        let ch = ClickHouseIntegration::new("localhost", 8123, "default");
        assert!(ch.is_ok());
    }
}
