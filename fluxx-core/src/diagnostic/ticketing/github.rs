//! GitHub Integration

use super::{format_github_markdown, TicketingProvider, TicketInfo};
use crate::diagnostic::config::GitHubConfig;
use crate::diagnostic::models::DiagnosticReport;
use async_trait::async_trait;
use serde_json::json;

/// GitHub API client
pub struct GitHubClient {
    config: GitHubConfig,
    client: reqwest::Client,
}

impl GitHubClient {
    /// Create new GitHub client
    pub fn new(config: GitHubConfig) -> anyhow::Result<Self> {
        if !config.enabled {
            return Err(anyhow::anyhow!("GitHub integration is disabled"));
        }

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        Ok(Self { config, client })
    }

    /// Build request headers
    fn headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("token {}", self.config.token)).unwrap(),
        );
        headers.insert(
            reqwest::header::ACCEPT,
            reqwest::header::HeaderValue::from_static("application/vnd.github.v3+json"),
        );
        headers
    }

    /// Get API URL
    fn api_url(&self, path: &str) -> String {
        format!(
            "https://api.github.com/repos/{}/{}{}",
            self.config.owner, self.config.repo, path
        )
    }

    /// Map priority to GitHub labels
    fn map_labels(&self, severity: &str) -> Vec<String> {
        let mut labels = self.config.labels.clone();
        
        // Add severity label
        match severity.to_lowercase().as_str() {
            "critical" => labels.push("priority:critical".to_string()),
            "high" => labels.push("priority:high".to_string()),
            "warning" => labels.push("priority:medium".to_string()),
            "info" => labels.push("priority:low".to_string()),
            _ => {}
        }

        labels.push("diagnostic".to_string());
        labels
    }
}

#[async_trait]
impl TicketingProvider for GitHubClient {
    fn name(&self) -> &str {
        "github"
    }

    async fn create_ticket(&self, report: &DiagnosticReport) -> anyhow::Result<TicketInfo> {
        let title = format!(
            "[{}] {} - {}",
            report.error.severity.to_uppercase(),
            report.error.source,
            report.error.message.lines().next().unwrap_or(&report.error.message)
        );

        let body = format_github_markdown(report);

        let issue_data = json!({
            "title": title,
            "body": body,
            "labels": self.map_labels(&report.error.severity),
        });

        // Add assignees if configured
        let issue_data = if !self.config.assignees.is_empty() {
            let mut data = issue_data;
            data["assignees"] = json!(self.config.assignees);
            data
        } else {
            issue_data
        };

        let response = self.client
            .post(&self.api_url("/issues"))
            .headers(self.headers())
            .json(&issue_data)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(anyhow::anyhow!("GitHub API error: {}", error_text));
        }

        let result: serde_json::Value = response.json().await?;
        let issue_number = result["number"].as_u64()
            .ok_or_else(|| anyhow::anyhow!("Invalid response: missing issue number"))?;

        let issue_url = result["html_url"].as_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid response: missing URL"))?;

        tracing::info!(
            issue_number = issue_number,
            url = %issue_url,
            "Created GitHub issue"
        );

        Ok(TicketInfo {
            id: issue_number.to_string(),
            url: issue_url.to_string(),
            provider: "github".to_string(),
            status: "open".to_string(),
        })
    }

    async fn resolve_ticket(&self, ticket_id: &str, resolution: &str) -> anyhow::Result<()> {
        let comment_data = json!({
            "body": format!("✅ **Resolved**\n\n{}", resolution)
        });

        // Add resolution comment
        let response = self.client
            .post(&self.api_url(&format!("/issues/{}/comments", ticket_id)))
            .headers(self.headers())
            .json(&comment_data)
            .send()
            .await?;

        if !response.status().is_success() {
            let error = response.text().await?;
            return Err(anyhow::anyhow!("Failed to add comment: {}", error));
        }

        // Close the issue
        let close_data = json!({
            "state": "closed"
        });

        let response = self.client
            .patch(&self.api_url(&format!("/issues/{}", ticket_id)))
            .headers(self.headers())
            .json(&close_data)
            .send()
            .await?;

        if response.status().is_success() {
            tracing::info!(ticket_id = %ticket_id, "Closed GitHub issue");
            Ok(())
        } else {
            let error = response.text().await?;
            Err(anyhow::anyhow!("Failed to close issue: {}", error))
        }
    }

    async fn add_comment(&self, ticket_id: &str, comment: &str) -> anyhow::Result<()> {
        let comment_data = json!({
            "body": comment
        });

        let response = self.client
            .post(&self.api_url(&format!("/issues/{}/comments", ticket_id)))
            .headers(self.headers())
            .json(&comment_data)
            .send()
            .await?;

        if response.status().is_success() {
            tracing::info!(ticket_id = %ticket_id, "Added comment to GitHub issue");
            Ok(())
        } else {
            let error = response.text().await?;
            Err(anyhow::anyhow!("Failed to add comment: {}", error))
        }
    }

    async fn health_check(&self) -> anyhow::Result<()> {
        let response = self.client
            .get(&self.api_url(""))
            .headers(self.headers())
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("GitHub health check failed: {}", response.status()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> GitHubConfig {
        GitHubConfig {
            enabled: true,
            token: "test-token".to_string(),
            owner: "test-org".to_string(),
            repo: "test-repo".to_string(),
            auto_create: true,
            labels: vec!["bug".to_string()],
            assignees: vec![],
        }
    }

    #[test]
    fn test_map_labels() {
        let config = create_test_config();
        let client = GitHubClient::new(config).unwrap();

        let labels = client.map_labels("critical");
        assert!(labels.contains(&"priority:critical".to_string()));
        assert!(labels.contains(&"diagnostic".to_string()));
    }

    #[test]
    fn test_api_url() {
        let config = create_test_config();
        let client = GitHubClient::new(config).unwrap();

        assert_eq!(
            client.api_url("/issues"),
            "https://api.github.com/repos/test-org/test-repo/issues"
        );
    }
}
