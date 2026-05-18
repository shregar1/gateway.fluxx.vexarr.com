//! GitLab CI Integration
//!
//! Monitor GitLab CI/CD pipelines, jobs, and runners.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// GitLab CI integration
pub struct GitLabCiIntegration {
    config: IntegrationConfig,
    base_url: String,
    token: String,
    project_id: Option<String>,
}

impl GitLabCiIntegration {
    /// Create new GitLab CI integration for a specific project
    pub fn new_project(
        base_url: &str,
        token: &str,
        project_id: &str,
    ) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cicd_provider".to_string(), "gitlab".to_string());
        tags.insert("project_id".to_string(), project_id.to_string());

        let config = IntegrationConfig {
            name: format!("gitlab_ci_{}", project_id.replace("/", "_")),
            interval_secs: 300,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            base_url: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
            project_id: Some(project_id.to_string()),
        })
    }

    /// Create new GitLab CI integration for instance-wide monitoring
    pub fn new_instance(base_url: &str, token: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cicd_provider".to_string(), "gitlab".to_string());

        let config = IntegrationConfig {
            name: "gitlab_ci_instance".to_string(),
            interval_secs: 300,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            base_url: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
            project_id: None,
        })
    }

    /// Check if GitLab API is accessible
    async fn check_gitlab_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/api/v4/user", self.base_url);
        
        match client
            .get(&url)
            .header("PRIVATE-TOKEN", &self.token)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Collect pipeline metrics for a project
    async fn collect_pipeline_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        
        let url = if let Some(ref project_id) = self.project_id {
            format!(
                "{}/api/v4/projects/{}/pipelines?per_page=100",
                self.base_url,
                project_id.replace("/", "%2F")
            )
        } else {
            format!("{}/api/v4/pipelines?per_page=100", self.base_url)
        };

        let response = client
            .get(&url)
            .header("PRIVATE-TOKEN", &self.token)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let pipelines: Vec<serde_json::Value> = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        let mut status_counts: HashMap<String, u64> = HashMap::new();
        let mut total_duration: f64 = 0.0;
        let mut duration_count: u64 = 0;

        for pipeline in &pipelines {
            if let Some(status) = pipeline["status"].as_str() {
                *status_counts.entry(status.to_string()).or_insert(0) += 1;
            }

            if let Some(duration) = pipeline["duration"].as_f64() {
                total_duration += duration;
                duration_count += 1;
            }
        }

        // Status counts
        for (status, count) in &status_counts {
            metrics.push(Metric::gauge(
                format!("gitlab.pipelines.{}", status),
                *count as f64,
                self.name(),
            ));
        }

        metrics.push(Metric::gauge(
            "gitlab.pipelines.total",
            pipelines.len() as f64,
            self.name(),
        ));

        // Average duration
        if duration_count > 0 {
            metrics.push(Metric::gauge(
                "gitlab.pipelines.avg_duration_seconds",
                total_duration / duration_count as f64,
                self.name(),
            ));
        }

        // Success rate
        let success_count = status_counts.get("success").copied().unwrap_or(0);
        if !pipelines.is_empty() {
            let success_rate = (success_count as f64 / pipelines.len() as f64) * 100.0;
            metrics.push(Metric::gauge(
                "gitlab.pipelines.success_rate",
                success_rate,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect job metrics
    async fn collect_job_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        
        let url = if let Some(ref project_id) = self.project_id {
            format!(
                "{}/api/v4/projects/{}/jobs?per_page=100",
                self.base_url,
                project_id.replace("/", "%2F")
            )
        } else {
            format!("{}/api/v4/jobs?per_page=100", self.base_url)
        };

        let response = client
            .get(&url)
            .header("PRIVATE-TOKEN", &self.token)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let jobs: Vec<serde_json::Value> = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        let mut status_counts: HashMap<String, u64> = HashMap::new();

        for job in &jobs {
            if let Some(status) = job["status"].as_str() {
                *status_counts.entry(status.to_string()).or_insert(0) += 1;
            }
        }

        for (status, count) in &status_counts {
            metrics.push(Metric::gauge(
                format!("gitlab.jobs.{}", status),
                *count as f64,
                self.name(),
            ));
        }

        metrics.push(Metric::gauge(
            "gitlab.jobs.total",
            jobs.len() as f64,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect runner metrics
    async fn collect_runner_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/api/v4/runners/all?per_page=100", self.base_url);

        let response = client
            .get(&url)
            .header("PRIVATE-TOKEN", &self.token)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let runners: Vec<serde_json::Value> = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        let mut active_count = 0u64;
        let mut paused_count = 0u64;
        let mut offline_count = 0u64;

        for runner in &runners {
            if let Some(active) = runner["active"].as_bool() {
                if active {
                    active_count += 1;
                } else {
                    paused_count += 1;
                }
            }
            if let Some(online) = runner["online"].as_bool() {
                if !online {
                    offline_count += 1;
                }
            }
        }

        metrics.push(Metric::gauge(
            "gitlab.runners.total",
            runners.len() as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "gitlab.runners.active",
            active_count as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "gitlab.runners.paused",
            paused_count as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "gitlab.runners.offline",
            offline_count as f64,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect all GitLab metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        // Check connectivity first
        if !self.check_gitlab_access().await {
            all_metrics.push(Metric::gauge(
                "gitlab.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "gitlab.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_pipeline_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect pipeline metrics: {}", e),
        }

        match self.collect_job_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect job metrics: {}", e),
        }

        // Only collect runner metrics for instance-wide or admin token
        if self.project_id.is_none() {
            match self.collect_runner_metrics().await {
                Ok(metrics) => all_metrics.extend(metrics),
                Err(e) => tracing::warn!("Failed to collect runner metrics: {}", e),
            }
        }

        Ok(all_metrics)
    }

    /// Collect GitLab events
    async fn collect_gitlab_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check connectivity
        if !self.check_gitlab_access().await {
            events.push(
                Event::new(
                    "GitLab API Unavailable",
                    format!("Cannot connect to GitLab at {}", self.base_url),
                    EventSeverity::Critical,
                    self.name(),
                    "api_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check for failed pipelines
        let client = reqwest::Client::new();
        
        let url = if let Some(ref project_id) = self.project_id {
            format!(
                "{}/api/v4/projects/{}/pipelines?status=failed&per_page=10",
                self.base_url,
                project_id.replace("/", "%2F")
            )
        } else {
            format!("{}/api/v4/pipelines?status=failed&per_page=10", self.base_url)
        };

        if let Ok(response) = client
            .get(&url)
            .header("PRIVATE-TOKEN", &self.token)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(pipelines) = response.json::<Vec<serde_json::Value>>().await {
                    for pipeline in pipelines {
                        if let (Some(id), Some(ref_val), Some(status)) = (
                            pipeline["id"].as_i64(),
                            pipeline["ref"].as_str(),
                            pipeline["status"].as_str(),
                        ) {
                            if status == "failed" {
                                events.push(
                                    Event::new(
                                        "GitLab Pipeline Failed",
                                        format!("Pipeline {} on branch {} failed", id, ref_val),
                                        EventSeverity::Warning,
                                        self.name(),
                                        "pipeline_failed",
                                    )
                                    .with_metadata("pipeline_id", &id.to_string())
                                    .with_metadata("branch", ref_val),
                                );
                            }
                        }
                    }
                }
            }
        }

        // Check for offline runners
        let url = format!("{}/api/v4/runners/all?per_page=100", self.base_url);
        if let Ok(response) = client
            .get(&url)
            .header("PRIVATE-TOKEN", &self.token)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(runners) = response.json::<Vec<serde_json::Value>>().await {
                    for runner in runners {
                        if let (Some(name), Some(online)) = (
                            runner["description"].as_str(),
                            runner["online"].as_bool(),
                        ) {
                            if !online {
                                events.push(
                                    Event::new(
                                        "GitLab Runner Offline",
                                        format!("Runner {} is offline", name),
                                        EventSeverity::Warning,
                                        self.name(),
                                        "runner_offline",
                                    )
                                    .with_metadata("runner", name),
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
impl Integration for GitLabCiIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_gitlab_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_gitlab_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to GitLab at {}",
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
    fn test_gitlab_project_integration_creation() {
        let gitlab = GitLabCiIntegration::new_project(
            "https://gitlab.com",
            "token123",
            "mygroup/myproject",
        );
        assert!(gitlab.is_ok());
    }

    #[test]
    fn test_gitlab_instance_integration_creation() {
        let gitlab = GitLabCiIntegration::new_instance("https://gitlab.com", "token123");
        assert!(gitlab.is_ok());
    }
}
