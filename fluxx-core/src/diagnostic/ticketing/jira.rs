//! Jira Integration

use super::{TicketingProvider, TicketInfo, TicketTemplate};
use crate::diagnostic::config::JiraConfig;
use crate::diagnostic::models::DiagnosticReport;
use async_trait::async_trait;
use base64::Engine;
use serde_json::json;

/// Jira API client
pub struct JiraClient {
    config: JiraConfig,
    client: reqwest::Client,
    auth_header: String,
}

impl JiraClient {
    /// Create new Jira client
    pub fn new(config: JiraConfig) -> anyhow::Result<Self> {
        if !config.enabled {
            return Err(anyhow::anyhow!("Jira integration is disabled"));
        }

        let auth = format!("{}:{}", config.username, config.api_token);
        let auth_header = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(auth));

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        Ok(Self {
            config,
            client,
            auth_header,
        })
    }

    /// Build request headers
    fn headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&self.auth_header).unwrap(),
        );
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        headers
    }

    /// Get API URL
    fn api_url(&self, path: &str) -> String {
        format!("{}/rest/api/2{}", self.config.host, path)
    }

    /// Map priority to Jira priority ID
    fn map_priority(&self, priority: super::TicketPriority) -> String {
        let priority_name = match priority {
            super::TicketPriority::Lowest => "Lowest",
            super::TicketPriority::Low => "Low",
            super::TicketPriority::Medium => "Medium",
            super::TicketPriority::High => "High",
            super::TicketPriority::Highest => "Highest",
        };

        // Check for custom mapping
        self.config.priority_mapping
            .get(priority_name)
            .cloned()
            .unwrap_or_else(|| priority_name.to_string())
    }
}

#[async_trait]
impl TicketingProvider for JiraClient {
    fn name(&self) -> &str {
        "jira"
    }

    async fn create_ticket(&self, report: &DiagnosticReport) -> anyhow::Result<TicketInfo> {
        let template = TicketTemplate::from_report(report);

        let issue_data = json!({
            "fields": {
                "project": {
                    "key": self.config.project_key
                },
                "summary": template.title,
                "description": template.description,
                "issuetype": {
                    "name": self.config.issue_type
                },
                "priority": {
                    "name": self.map_priority(template.priority)
                },
                "labels": template.labels.iter().chain(self.config.labels.iter()).cloned().collect::<Vec<_>>(),
            }
        });

        // Add assignee if configured
        let issue_data = if let Some(assignee) = &self.config.assignee {
            let mut data = issue_data;
            data["fields"]["assignee"] = json!({"name": assignee});
            data
        } else {
            issue_data
        };

        let response = self.client
            .post(&self.api_url("/issue"))
            .headers(self.headers())
            .json(&issue_data)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(anyhow::anyhow!("Jira API error: {}", error_text));
        }

        let result: serde_json::Value = response.json().await?;
        let issue_key = result["key"].as_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid response: missing issue key"))?;

        let issue_url = format!("{}/browse/{}", self.config.host, issue_key);

        tracing::info!(
            issue_key = %issue_key,
            url = %issue_url,
            "Created Jira issue"
        );

        Ok(TicketInfo {
            id: issue_key.to_string(),
            url: issue_url,
            provider: "jira".to_string(),
            status: "Open".to_string(),
        })
    }

    async fn resolve_ticket(&self, ticket_id: &str, resolution: &str) -> anyhow::Result<()> {
        let transition_data = json!({
            "transition": {
                "id": "2"  // "Done" transition (typically)
            },
            "fields": {
                "comment": {
                    "add": {
                        "body": format!("Resolved: {}", resolution)
                    }
                }
            }
        });

        let response = self.client
            .post(&self.api_url(&format!("/issue/{}/transitions", ticket_id)))
            .headers(self.headers())
            .json(&transition_data)
            .send()
            .await?;

        if response.status().is_success() {
            tracing::info!(ticket_id = %ticket_id, "Resolved Jira issue");
            Ok(())
        } else {
            let error = response.text().await?;
            Err(anyhow::anyhow!("Failed to resolve ticket: {}", error))
        }
    }

    async fn add_comment(&self, ticket_id: &str, comment: &str) -> anyhow::Result<()> {
        let comment_data = json!({
            "body": comment
        });

        let response = self.client
            .post(&self.api_url(&format!("/issue/{}/comment", ticket_id)))
            .headers(self.headers())
            .json(&comment_data)
            .send()
            .await?;

        if response.status().is_success() {
            tracing::info!(ticket_id = %ticket_id, "Added comment to Jira issue");
            Ok(())
        } else {
            let error = response.text().await?;
            Err(anyhow::anyhow!("Failed to add comment: {}", error))
        }
    }

    async fn health_check(&self) -> anyhow::Result<()> {
        let response = self.client
            .get(&self.api_url("/myself"))
            .headers(self.headers())
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Jira health check failed: {}", response.status()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic::models::{ErrorContext, RootCauseAnalysis};
    use std::collections::HashMap;

    fn create_test_config() -> JiraConfig {
        JiraConfig {
            enabled: true,
            host: "https://test.atlassian.net".to_string(),
            username: "test@example.com".to_string(),
            api_token: "test-token".to_string(),
            project_key: "TEST".to_string(),
            issue_type: "Bug".to_string(),
            auto_create: true,
            labels: vec![],
            assignee: None,
            priority_mapping: HashMap::new(),
        }
    }

    #[test]
    fn test_map_priority() {
        let config = create_test_config();
        let client = JiraClient::new(config).unwrap();

        assert_eq!(client.map_priority(super::super::TicketPriority::Highest), "Highest");
        assert_eq!(client.map_priority(super::super::TicketPriority::Low), "Low");
    }

    #[test]
    fn test_api_url() {
        let config = create_test_config();
        let client = JiraClient::new(config).unwrap();

        assert_eq!(
            client.api_url("/issue"),
            "https://test.atlassian.net/rest/api/2/issue"
        );
    }
}
