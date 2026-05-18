//! CircleCI Integration
//!
//! Monitor CircleCI pipelines, workflows, and jobs.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// CircleCI integration
pub struct CircleCiIntegration {
    config: IntegrationConfig,
    api_token: String,
    org_slug: String,
    project_slug: Option<String>,
}

impl CircleCiIntegration {
    /// Create new CircleCI integration for a specific project
    pub fn new_project(
        api_token: &str,
        org_slug: &str,
        project_slug: &str,
    ) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cicd_provider".to_string(), "circleci".to_string());
        tags.insert("org".to_string(), org_slug.to_string());
        tags.insert("project".to_string(), project_slug.to_string());

        let config = IntegrationConfig {
            name: format!("circleci_{}_{}", org_slug, project_slug),
            interval_secs: 300,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            api_token: api_token.to_string(),
            org_slug: org_slug.to_string(),
            project_slug: Some(project_slug.to_string()),
        })
    }

    /// Create new CircleCI integration for organization-wide monitoring
    pub fn new_org(api_token: &str, org_slug: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cicd_provider".to_string(), "circleci".to_string());
        tags.insert("org".to_string(), org_slug.to_string());

        let config = IntegrationConfig {
            name: format!("circleci_{}", org_slug),
            interval_secs: 300,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            api_token: api_token.to_string(),
            org_slug: org_slug.to_string(),
            project_slug: None,
        })
    }

    /// Check if CircleCI API is accessible
    async fn check_circleci_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = "https://circleci.com/api/v2/me";
        
        match client
            .get(url)
            .header("Circle-Token", &self.api_token)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Collect pipeline metrics
    async fn collect_pipeline_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        
        let url = if let Some(ref project) = self.project_slug {
            format!(
                "https://circleci.com/api/v2/project/gh/{}/{}/pipeline",
                self.org_slug, project
            )
        } else {
            format!(
                "https://circleci.com/api/v2/pipeline?org-slug=gh/{}",
                self.org_slug
            )
        };

        let response = client
            .get(&url)
            .header("Circle-Token", &self.api_token)
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

        if let Some(items) =data["items"].as_array() {
            let mut status_counts: HashMap<String, u64> = HashMap::new();
            let mut trigger_counts: HashMap<String, u64> = HashMap::new();

            for pipeline in items {
                // State counts
                if let Some(state) = pipeline["state"].as_str() {
                    *status_counts.entry(state.to_string()).or_insert(0) += 1;
                }

                // Trigger type counts
                if let Some(trigger) = pipeline["trigger"].as_object() {
                    if let Some(trigger_type) = trigger["type"].as_str() {
                        *trigger_counts.entry(trigger_type.to_string()).or_insert(0) += 1;
                    }
                }
            }

            // Status metrics
            for (status, count) in &status_counts {
                metrics.push(Metric::gauge(
                    format!("circleci.pipelines.{}", status),
                    *count as f64,
                    self.name(),
                ));
            }

            // Trigger type metrics
            for (trigger_type, count) in &trigger_counts {
                metrics.push(Metric::gauge(
                    format!("circleci.pipelines.trigger.{}_count", trigger_type),
                    *count as f64,
                    self.name(),
                ));
            }

            metrics.push(Metric::gauge(
                "circleci.pipelines.total",
                items.len() as f64,
                self.name(),
            ));

            // Success rate
            let success_count = status_counts.get("created").copied().unwrap_or(0)
                + status_counts.get("success").copied().unwrap_or(0);
            if !items.is_empty() {
                let success_rate = (success_count as f64 / items.len() as f64) * 100.0;
                metrics.push(Metric::gauge(
                    "circleci.pipelines.success_rate",
                    success_rate,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect workflow metrics for recent pipelines
    async fn collect_workflow_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        
        // Get recent pipelines first
        let url = if let Some(ref project) = self.project_slug {
            format!(
                "https://circleci.com/api/v2/project/gh/{}/{}/pipeline",
                self.org_slug, project
            )
        } else {
            format!(
                "https://circleci.com/api/v2/pipeline?org-slug=gh/{}",
                self.org_slug
            )
        };

        let response = client
            .get(&url)
            .header("Circle-Token", &self.api_token)
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

        let mut total_workflows = 0u64;
        let mut status_counts: HashMap<String, u64> = HashMap::new();
        let mut total_duration: f64 = 0.0;
        let mut duration_count: u64 = 0;

        if let Some(items) = data["items"].as_array() {
            // Get workflows for each pipeline (limit to first 5 to avoid rate limits)
            for pipeline in items.iter().take(5) {
                if let Some(pipeline_id) = pipeline["id"].as_str() {
                    let workflow_url = format!(
                        "https://circleci.com/api/v2/pipeline/{}/workflow",
                        pipeline_id
                    );

                    if let Ok(workflow_response) = client
                        .get(&workflow_url)
                        .header("Circle-Token", &self.api_token)
                        .timeout(std::time::Duration::from_secs(30))
                        .send()
                        .await
                    {
                        if workflow_response.status().is_success() {
                            if let Ok(workflow_data) = workflow_response.json::<serde_json::Value>().await {
                                if let Some(workflows) = workflow_data["items"].as_array() {
                                    for workflow in workflows {
                                        total_workflows += 1;

                                        if let Some(status) = workflow["status"].as_str() {
                                            *status_counts.entry(status.to_string()).or_insert(0) += 1;
                                        }

                                        if let (Some(created), Some(stopped)) = (
                                            workflow["created_at"].as_str(),
                                            workflow["stopped_at"].as_str(),
                                        ) {
                                            if let (Ok(created_dt), Ok(stopped_dt)) = (
                                                chrono::DateTime::parse_from_rfc3339(created),
                                                chrono::DateTime::parse_from_rfc3339(stopped),
                                            ) {
                                                let duration = (stopped_dt - created_dt).num_seconds() as f64;
                                                if duration > 0.0 {
                                                    total_duration += duration;
                                                    duration_count += 1;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        for (status, count) in &status_counts {
            metrics.push(Metric::gauge(
                format!("circleci.workflows.{}", status),
                *count as f64,
                self.name(),
            ));
        }

        metrics.push(Metric::gauge(
            "circleci.workflows.total",
            total_workflows as f64,
            self.name(),
        ));

        if duration_count > 0 {
            metrics.push(Metric::gauge(
                "circleci.workflows.avg_duration_seconds",
                total_duration / duration_count as f64,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect all CircleCI metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        // Check connectivity first
        if !self.check_circleci_access().await {
            all_metrics.push(Metric::gauge(
                "circleci.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "circleci.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_pipeline_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect pipeline metrics: {}", e),
        }

        match self.collect_workflow_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect workflow metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect CircleCI events
    async fn collect_circleci_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check connectivity
        if !self.check_circleci_access().await {
            events.push(
                Event::new(
                    "CircleCI API Unavailable",
                    "Cannot connect to CircleCI API".to_string(),
                    EventSeverity::Critical,
                    self.name(),
                    "api_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check for failed pipelines
        let client = reqwest::Client::new();
        
        let url = if let Some(ref project) = self.project_slug {
            format!(
                "https://circleci.com/api/v2/project/gh/{}/{}/pipeline",
                self.org_slug, project
            )
        } else {
            format!(
                "https://circleci.com/api/v2/pipeline?org-slug=gh/{}",
                self.org_slug
            )
        };

        if let Ok(response) = client
            .get(&url)
            .header("Circle-Token", &self.api_token)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(data) = response.json::<serde_json::Value>().await {
                    if let Some(items) = data["items"].as_array() {
                        for pipeline in items.iter().take(10) {
                            if let (Some(id), Some(state)) = (
                                pipeline["id"].as_str(),
                                pipeline["state"].as_str(),
                            ) {
                                if state == "failed" || state == "error" {
                                    let vcs = pipeline["vcs"].as_object();
                                    let branch = vcs
                                        .and_then(|v| v["branch"].as_str())
                                        .unwrap_or("unknown");
                                    
                                    events.push(
                                        Event::new(
                                            "CircleCI Pipeline Failed",
                                            format!("Pipeline {} on branch {} failed", id, branch),
                                            EventSeverity::Warning,
                                            self.name(),
                                            "pipeline_failed",
                                        )
                                        .with_metadata("pipeline_id", id)
                                        .with_metadata("branch", branch),
                                    );
                                }
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
impl Integration for CircleCiIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_circleci_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_circleci_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(
                "Cannot connect to CircleCI API".to_string()
            ))
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
    fn test_circleci_project_integration_creation() {
        let circleci = CircleCiIntegration::new_project(
            "token123",
            "myorg",
            "myrepo",
        );
        assert!(circleci.is_ok());
    }

    #[test]
    fn test_circleci_org_integration_creation() {
        let circleci = CircleCiIntegration::new_org("token123", "myorg");
        assert!(circleci.is_ok());
    }
}
