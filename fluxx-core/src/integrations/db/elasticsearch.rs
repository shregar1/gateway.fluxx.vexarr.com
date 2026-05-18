//! Elasticsearch Integration
//!
//! Monitor Elasticsearch clusters, indices, nodes, and search performance.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Elasticsearch integration
pub struct ElasticsearchIntegration {
    config: IntegrationConfig,
    base_url: String,
    username: Option<String>,
    password: Option<String>,
}

impl ElasticsearchIntegration {
    /// Create new Elasticsearch integration
    pub fn new(base_url: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("db_type".to_string(), "elasticsearch".to_string());

        let config = IntegrationConfig {
            name: "elasticsearch".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            base_url: base_url.trim_end_matches('/').to_string(),
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

    /// Check if Elasticsearch is accessible
    async fn check_elasticsearch_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/_cluster/health", self.base_url);

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

    /// Collect cluster health metrics
    async fn collect_cluster_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/_cluster/health", self.base_url);

        let mut request = client.get(&url);
        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            request = request.basic_auth(user, Some(pass));
        }

        let response = request
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

        // Cluster status (0=green, 1=yellow, 2=red)
        let status_value = match data["status"].as_str() {
            Some("green") => 0.0,
            Some("yellow") => 1.0,
            Some("red") => 2.0,
            _ => 3.0,
        };
        metrics.push(Metric::gauge(
            "elasticsearch.cluster.status",
            status_value,
            self.name(),
        ));

        // Node counts
        if let Some(v) = data["number_of_nodes"].as_f64() {
            metrics.push(Metric::gauge(
                "elasticsearch.cluster.nodes.total",
                v,
                self.name(),
            ));
        }
        if let Some(v) = data["number_of_data_nodes"].as_f64() {
            metrics.push(Metric::gauge(
                "elasticsearch.cluster.nodes.data",
                v,
                self.name(),
            ));
        }

        // Indices
        if let Some(v) = data["active_primary_shards"].as_f64() {
            metrics.push(Metric::gauge(
                "elasticsearch.cluster.shards.active_primary",
                v,
                self.name(),
            ));
        }
        if let Some(v) = data["active_shards"].as_f64() {
            metrics.push(Metric::gauge(
                "elasticsearch.cluster.shards.active",
                v,
                self.name(),
            ));
        }
        if let Some(v) = data["relocating_shards"].as_f64() {
            metrics.push(Metric::gauge(
                "elasticsearch.cluster.shards.relocating",
                v,
                self.name(),
            ));
        }
        if let Some(v) = data["initializing_shards"].as_f64() {
            metrics.push(Metric::gauge(
                "elasticsearch.cluster.shards.initializing",
                v,
                self.name(),
            ));
        }
        if let Some(v) = data["unassigned_shards"].as_f64() {
            metrics.push(Metric::gauge(
                "elasticsearch.cluster.shards.unassigned",
                v,
                self.name(),
            ));
        }
        if let Some(v) = data["delayed_unassigned_shards"].as_f64() {
            metrics.push(Metric::gauge(
                "elasticsearch.cluster.shards.delayed_unassigned",
                v,
                self.name(),
            ));
        }

        // Pending tasks
        if let Some(v) = data["number_of_pending_tasks"].as_f64() {
            metrics.push(Metric::gauge(
                "elasticsearch.cluster.pending_tasks",
                v,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect node stats
    async fn collect_node_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/_nodes/stats", self.base_url);

        let mut request = client.get(&url);
        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            request = request.basic_auth(user, Some(pass));
        }

        let response = request
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

        if let Some(nodes) = data["nodes"].as_object() {
            for (node_id, node_data) in nodes {
                if let Some(jvm) = node_data["jvm"].as_object() {
                    // Heap usage
                    if let Some(heap_used) = jvm["mem"]["heap_used_in_bytes"].as_f64() {
                        metrics.push(Metric::gauge(
                            "elasticsearch.node.jvm.heap_used_bytes",
                            heap_used,
                            self.name(),
                        ).with_tag("node", node_id));
                    }
                    if let Some(heap_max) = jvm["mem"]["heap_max_in_bytes"].as_f64() {
                        metrics.push(Metric::gauge(
                            "elasticsearch.node.jvm.heap_max_bytes",
                            heap_max,
                            self.name(),
                        ).with_tag("node", node_id));
                    }

                    // GC
                    if let Some(gc) = jvm["gc"]["collectors"]["old"]["collection_count"].as_f64() {
                        metrics.push(Metric::gauge(
                            "elasticsearch.node.gc.old_collections",
                            gc,
                            self.name(),
                        ).with_tag("node", node_id));
                    }
                }

                // Process stats
                if let Some(open_file_descriptors) =
                    node_data["process"]["open_file_descriptors"].as_f64()
                {
                    metrics.push(Metric::gauge(
                        "elasticsearch.node.process.open_file_descriptors",
                        open_file_descriptors,
                        self.name(),
                    ).with_tag("node", node_id));
                }

                // Indexing stats
                if let Some(index_total) = node_data["indices"]["indexing"]["index_total"].as_f64()
                {
                    metrics.push(Metric::gauge(
                        "elasticsearch.node.indices.indexing.total",
                        index_total,
                        self.name(),
                    ).with_tag("node", node_id));
                }

                // Search stats
                if let Some(query_total) = node_data["indices"]["search"]["query_total"].as_f64() {
                    metrics.push(Metric::gauge(
                        "elasticsearch.node.indices.search.total",
                        query_total,
                        self.name(),
                    ).with_tag("node", node_id));
                }

                // Document count
                if let Some(docs) = node_data["indices"]["docs"]["count"].as_f64() {
                    metrics.push(Metric::gauge(
                        "elasticsearch.node.indices.docs.count",
                        docs,
                        self.name(),
                    ).with_tag("node", node_id));
                }
            }
        }

        Ok(metrics)
    }

    /// Collect index stats
    async fn collect_index_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/_stats", self.base_url);

        let mut request = client.get(&url);
        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            request = request.basic_auth(user, Some(pass));
        }

        let response = request
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

        if let Some(indices) = data["indices"].as_object() {
            for (index_name, index_data) in indices {
                if let Some(total) = index_data["total"].as_object() {
                    // Document count
                    if let Some(docs) = total["docs"]["count"].as_f64() {
                        metrics.push(Metric::gauge(
                            "elasticsearch.index.docs.count",
                            docs,
                            self.name(),
                        ).with_tag("index", index_name));
                    }

                    // Store size
                    if let Some(size) = total["store"]["size_in_bytes"].as_f64() {
                        metrics.push(Metric::gauge(
                            "elasticsearch.index.store.size_bytes",
                            size,
                            self.name(),
                        ).with_tag("index", index_name));
                    }

                    // Search stats
                    if let Some(query_time) = total["search"]["query_time_in_millis"].as_f64() {
                        metrics.push(Metric::gauge(
                            "elasticsearch.index.search.query_time_ms",
                            query_time,
                            self.name(),
                        ).with_tag("index", index_name));
                    }

                    // Indexing stats
                    if let Some(indexing_time) = total["indexing"]["index_time_in_millis"].as_f64() {
                        metrics.push(Metric::gauge(
                            "elasticsearch.index.indexing.time_ms",
                            indexing_time,
                            self.name(),
                        ).with_tag("index", index_name));
                    }
                }
            }
        }

        Ok(metrics)
    }

    /// Collect all Elasticsearch metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        // Check connectivity first
        if !self.check_elasticsearch_access().await {
            all_metrics.push(Metric::gauge(
                "elasticsearch.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "elasticsearch.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_cluster_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect cluster metrics: {}", e),
        }

        match self.collect_node_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect node metrics: {}", e),
        }

        match self.collect_index_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect index metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Elasticsearch events
    async fn collect_elasticsearch_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check connectivity
        if !self.check_elasticsearch_access().await {
            events.push(
                Event::new(
                    "Elasticsearch Unavailable",
                    format!("Cannot connect to Elasticsearch at {}", self.base_url),
                    EventSeverity::Critical,
                    self.name(),
                    "elasticsearch_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check cluster health
        let client = reqwest::Client::new();
        let url = format!("{}/_cluster/health", self.base_url);

        let mut request = client.get(&url);
        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            request = request.basic_auth(user, Some(pass));
        }

        if let Ok(response) = request
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(data) = response.json::<serde_json::Value>().await {
                    if let Some(status) = data["status"].as_str() {
                        if status == "red" {
                            events.push(
                                Event::new(
                                    "Elasticsearch Cluster Red",
                                    "Cluster health is RED - some primary shards are unassigned",
                                    EventSeverity::Critical,
                                    self.name(),
                                    "cluster_red",
                                ),
                            );
                        } else if status == "yellow" {
                            events.push(
                                Event::new(
                                    "Elasticsearch Cluster Yellow",
                                    "Cluster health is YELLOW - some replica shards are unassigned",
                                    EventSeverity::Warning,
                                    self.name(),
                                    "cluster_yellow",
                                ),
                            );
                        }
                    }

                    // Check unassigned shards
                    if let Some(unassigned) = data["unassigned_shards"].as_f64() {
                        if unassigned > 0.0 {
                            events.push(
                                Event::new(
                                    "Elasticsearch Unassigned Shards",
                                    format!("{} unassigned shards", unassigned),
                                    EventSeverity::Warning,
                                    self.name(),
                                    "unassigned_shards",
                                )
                                .with_metadata("unassigned_shards", &unassigned.to_string()),
                            );
                        }
                    }

                    // Check pending tasks
                    if let Some(pending) = data["number_of_pending_tasks"].as_f64() {
                        if pending > 10.0 {
                            events.push(
                                Event::new(
                                    "Elasticsearch Pending Tasks",
                                    format!("{} pending cluster tasks", pending),
                                    EventSeverity::Warning,
                                    self.name(),
                                    "pending_tasks",
                                )
                                .with_metadata("pending_tasks", &pending.to_string()),
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
impl Integration for ElasticsearchIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_elasticsearch_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_elasticsearch_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Elasticsearch at {}",
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
    fn test_elasticsearch_integration_creation() {
        let es = ElasticsearchIntegration::new("http://localhost:9200");
        assert!(es.is_ok());
    }
}
