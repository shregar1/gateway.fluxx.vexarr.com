//! Jenkins Integration
//!
//! Monitor Jenkins builds, jobs, queue, and executors.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Jenkins integration
pub struct JenkinsIntegration {
    config: IntegrationConfig,
    base_url: String,
    username: String,
    api_token: String,
}

impl JenkinsIntegration {
    /// Create new Jenkins integration
    pub fn new(
        base_url: &str,
        username: &str,
        api_token: &str,
    ) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cicd_provider".to_string(), "jenkins".to_string());

        let config = IntegrationConfig {
            name: "jenkins".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            base_url: base_url.trim_end_matches('/').to_string(),
            username: username.to_string(),
            api_token: api_token.to_string(),
        })
    }

    /// Check if Jenkins is accessible
    async fn check_jenkins_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/api/json", self.base_url);
        
        match client
            .get(&url)
            .basic_auth(&self.username, Some(&self.api_token))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Collect job metrics
    async fn collect_job_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!(
            "{}/api/json?tree=jobs[name,color,lastBuild[number,duration,result,timestamp]]",
            self.base_url
        );

        let response = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.api_token))
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

        let mut status_counts: HashMap<String, u64> = HashMap::new();
        let mut total_duration: f64 = 0.0;
        let mut duration_count: u64 = 0;

        if let Some(jobs) = data["jobs"].as_array() {
            for job in jobs {
                // Parse job color (status indicator)
                if let Some(color) = job["color"].as_str() {
                    let status = if color.starts_with("blue") {
                        "success"
                    } else if color.starts_with("red") {
                        "failed"
                    } else if color.starts_with("yellow") {
                        "unstable"
                    } else if color.starts_with("notbuilt") {
                        "not_built"
                    } else if color.ends_with("_anime") {
                        "running"
                    } else {
                        "unknown"
                    };
                    *status_counts.entry(status.to_string()).or_insert(0) += 1;
                }

                // Parse last build duration
                if let Some(last_build) = job["lastBuild"].as_object() {
                    if let Some(duration) = last_build["duration"].as_f64() {
                        if duration > 0.0 {
                            total_duration += duration / 1000.0; // Convert to seconds
                            duration_count += 1;
                        }
                    }
                }
            }

            metrics.push(Metric::gauge(
                "jenkins.jobs.total",
                jobs.len() as f64,
                self.name(),
            ));
        }

        // Status counts
        for (status, count) in &status_counts {
            metrics.push(Metric::gauge(
                format!("jenkins.jobs.{}", status),
                *count as f64,
                self.name(),
            ));
        }

        // Average duration
        if duration_count > 0 {
            metrics.push(Metric::gauge(
                "jenkins.jobs.avg_duration_seconds",
                total_duration / duration_count as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect executor metrics
    async fn collect_executor_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!(
            "{}/computer/api/json?tree=computer[displayName,idle,offline,numExecutors,loadStatistics]",
            self.base_url
        );

        let response = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.api_token))
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

        let mut total_executors = 0u64;
        let mut busy_executors = 0u64;
        let mut offline_nodes = 0u64;

        if let Some(computers) = data["computer"].as_array() {
            for computer in computers {
                if let Some(num_executors) = computer["numExecutors"].as_u64() {
                    total_executors += num_executors;
                }

                if let Some(idle) = computer["idle"].as_bool() {
                    if !idle {
                        busy_executors += 1;
                    }
                }

                if let Some(offline) = computer["offline"].as_bool() {
                    if offline {
                        offline_nodes += 1;
                    }
                }
            }

            metrics.push(Metric::gauge(
                "jenkins.executors.total",
                total_executors as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "jenkins.executors.busy",
                busy_executors as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "jenkins.executors.idle",
                (total_executors - busy_executors) as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "jenkins.nodes.offline",
                offline_nodes as f64,
                self.name(),
            ));
            metrics.push(Metric::gauge(
                "jenkins.nodes.total",
                computers.len() as f64,
                self.name(),
            ));

            // Executor utilization
            if total_executors > 0 {
                let utilization = (busy_executors as f64 / total_executors as f64) * 100.0;
                metrics.push(Metric::gauge(
                    "jenkins.executors.utilization_percent",
                    utilization,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect queue metrics
    async fn collect_queue_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/queue/api/json?tree=items[id,inQueueSince,why]", self.base_url);

        let response = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.api_token))
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

        if let Some(items) = data["items"].as_array() {
            let queue_size = items.len() as f64;
            metrics.push(Metric::gauge(
                "jenkins.queue.size",
                queue_size,
                self.name(),
            ));

            // Calculate oldest item in queue
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let mut max_wait_ms: i64 = 0;
            for item in items {
                if let Some(in_queue_since) = item["inQueueSince"].as_i64() {
                    let wait_time = now - in_queue_since;
                    if wait_time > max_wait_ms {
                        max_wait_ms = wait_time;
                    }
                }
            }

            if max_wait_ms > 0 {
                metrics.push(Metric::gauge(
                    "jenkins.queue.max_wait_seconds",
                    (max_wait_ms / 1000) as f64,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect all Jenkins metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        // Check connectivity first
        if !self.check_jenkins_access().await {
            all_metrics.push(Metric::gauge(
                "jenkins.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "jenkins.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_job_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect job metrics: {}", e),
        }

        match self.collect_executor_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect executor metrics: {}", e),
        }

        match self.collect_queue_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect queue metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Jenkins events
    async fn collect_jenkins_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check connectivity
        if !self.check_jenkins_access().await {
            events.push(
                Event::new(
                    "Jenkins Unavailable",
                    format!("Cannot connect to Jenkins at {}", self.base_url),
                    EventSeverity::Critical,
                    self.name(),
                    "jenkins_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check for failed jobs
        let client = reqwest::Client::new();
        let url = format!(
            "{}/api/json?tree=jobs[name,color,lastBuild[number,result,timestamp]]",
            self.base_url
        );

        if let Ok(response) = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.api_token))
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(data) = response.json::<serde_json::Value>().await {
                    if let Some(jobs) = data["jobs"].as_array() {
                        for job in jobs {
                            if let (Some(name), Some(color)) = (
                                job["name"].as_str(),
                                job["color"].as_str(),
                            ) {
                                if color.starts_with("red") {
                                    events.push(
                                        Event::new(
                                            "Jenkins Job Failed",
                                            format!("Job {} is failing", name),
                                            EventSeverity::Warning,
                                            self.name(),
                                            "job_failed",
                                        )
                                        .with_metadata("job", name),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        // Check for offline nodes
        let url = format!("{}/computer/api/json", self.base_url);
        if let Ok(response) = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.api_token))
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(data) = response.json::<serde_json::Value>().await {
                    if let Some(computers) = data["computer"].as_array() {
                        for computer in computers {
                            if let (Some(name), Some(offline)) = (
                                computer["displayName"].as_str(),
                                computer["offline"].as_bool(),
                            ) {
                                if offline && name != "master" {
                                    events.push(
                                        Event::new(
                                            "Jenkins Node Offline",
                                            format!("Node {} is offline", name),
                                            EventSeverity::Warning,
                                            self.name(),
                                            "node_offline",
                                        )
                                        .with_metadata("node", name),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        // Check queue depth
        let url = format!("{}/queue/api/json", self.base_url);
        if let Ok(response) = client
            .get(&url)
            .basic_auth(&self.username, Some(&self.api_token))
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(data) = response.json::<serde_json::Value>().await {
                    if let Some(items) = data["items"].as_array() {
                        if items.len() > 50 {
                            events.push(
                                Event::new(
                                    "Jenkins Queue Backlog",
                                    format!("{} items in queue", items.len()),
                                    EventSeverity::Warning,
                                    self.name(),
                                    "queue_backlog",
                                )
                                .with_metadata("queue_size", &items.len().to_string()),
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
impl Integration for JenkinsIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_jenkins_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_jenkins_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Jenkins at {}",
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
    fn test_jenkins_integration_creation() {
        let jenkins = JenkinsIntegration::new(
            "http://jenkins.example.com",
            "admin",
            "api_token_123",
        );
        assert!(jenkins.is_ok());
    }
}
