//! Linear Integration

use super::{TicketingProvider, TicketInfo, TicketTemplate};
use crate::diagnostic::config::LinearConfig;
use crate::diagnostic::models::DiagnosticReport;
use async_trait::async_trait;
use serde_json::json;

/// Linear GraphQL API client
pub struct LinearClient {
    config: LinearConfig,
    client: reqwest::Client,
}

impl LinearClient {
    /// Create new Linear client
    pub fn new(config: LinearConfig) -> anyhow::Result<Self> {
        if !config.enabled {
            return Err(anyhow::anyhow!("Linear integration is disabled"));
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
            reqwest::header::HeaderValue::from_str(&self.config.api_key).unwrap(),
        );
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        headers
    }

    /// Execute GraphQL query
    async fn graphql(&self, query: &str, variables: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let body = json!({
            "query": query,
            "variables": variables
        });

        let response = self.client
            .post("https://api.linear.app/graphql")
            .headers(self.headers())
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let error = response.text().await?;
            return Err(anyhow::anyhow!("Linear API error: {}", error));
        }

        let result: serde_json::Value = response.json().await?;
        
        if let Some(errors) = result.get("errors") {
            return Err(anyhow::anyhow!("GraphQL errors: {}", errors));
        }

        Ok(result)
    }

    /// Map severity to Linear priority
    fn map_priority(&self, severity: &str) -> i32 {
        match severity.to_lowercase().as_str() {
            "critical" => 1,  // Urgent
            "high" => 2,      // High
            "warning" => 3,   // Medium
            "info" => 4,      // Low
            _ => 3,
        }
    }
}

#[async_trait]
impl TicketingProvider for LinearClient {
    fn name(&self) -> &str {
        "linear"
    }

    async fn create_ticket(&self, report: &DiagnosticReport) -> anyhow::Result<TicketInfo> {
        let template = TicketTemplate::from_report(report);

        // First, get the team ID
        let team_query = r#"
            query {
                teams {
                    nodes {
                        id
                        name
                    }
                }
            }
        "#;

        let team_result = self.graphql(team_query, json!({})).await?;
        
        // Use configured team ID or first available
        let team_id = if self.config.team_id.is_empty() {
            team_result["data"]["teams"]["nodes"][0]["id"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("No teams found"))?
                .to_string()
        } else {
            self.config.team_id.clone()
        };

        // Create the issue
        let create_mutation = r#"
            mutation CreateIssue($input: IssueCreateInput!) {
                issueCreate(input: $input) {
                    success
                    issue {
                        id
                        identifier
                        url
                    }
                }
            }
        "#;

        let label_ids: Vec<serde_json::Value> = self.config.labels.iter()
            .map(|l| json!(l))
            .collect();

        let variables = json!({
            "input": {
                "title": template.title,
                "description": template.description,
                "teamId": team_id,
                "priority": self.map_priority(&report.error.severity),
                "labelIds": label_ids,
            }
        });

        let result = self.graphql(create_mutation, variables).await?;
        
        let issue = &result["data"]["issueCreate"]["issue"];
        let issue_id = issue["identifier"].as_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid response: missing issue identifier"))?;
        let issue_url = issue["url"].as_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid response: missing issue URL"))?;

        tracing::info!(
            issue_id = %issue_id,
            url = %issue_url,
            "Created Linear issue"
        );

        Ok(TicketInfo {
            id: issue_id.to_string(),
            url: issue_url.to_string(),
            provider: "linear".to_string(),
            status: "Todo".to_string(),
        })
    }

    async fn resolve_ticket(&self, ticket_id: &str, resolution: &str) -> anyhow::Result<()> {
        // Add comment
        let comment_mutation = r#"
            mutation CreateComment($input: CommentCreateInput!) {
                commentCreate(input: $input) {
                    success
                }
            }
        "#;

        let variables = json!({
            "input": {
                "issueId": ticket_id,
                "body": format!("✅ **Resolved**\n\n{}", resolution)
            }
        });

        self.graphql(comment_mutation, variables).await?;

        // Update state to Done (typically state ID for Done)
        let update_mutation = r#"
            mutation UpdateIssue($id: String!, $input: IssueUpdateInput!) {
                issueUpdate(id: $id, input: $input) {
                    success
                }
            }
        "#;

        let variables = json!({
            "id": ticket_id,
            "input": {
                "stateId": "done"  // This is a placeholder - would need actual state ID
            }
        });

        let result = self.graphql(update_mutation, variables).await?;
        
        if result["data"]["issueUpdate"]["success"].as_bool().unwrap_or(false) {
            tracing::info!(ticket_id = %ticket_id, "Resolved Linear issue");
            Ok(())
        } else {
            Err(anyhow::anyhow!("Failed to resolve Linear issue"))
        }
    }

    async fn add_comment(&self, ticket_id: &str, comment: &str) -> anyhow::Result<()> {
        let mutation = r#"
            mutation CreateComment($input: CommentCreateInput!) {
                commentCreate(input: $input) {
                    success
                }
            }
        "#;

        let variables = json!({
            "input": {
                "issueId": ticket_id,
                "body": comment
            }
        });

        let result = self.graphql(mutation, variables).await?;
        
        if result["data"]["commentCreate"]["success"].as_bool().unwrap_or(false) {
            tracing::info!(ticket_id = %ticket_id, "Added comment to Linear issue");
            Ok(())
        } else {
            Err(anyhow::anyhow!("Failed to add comment"))
        }
    }

    async fn health_check(&self) -> anyhow::Result<()> {
        let query = r#"
            query {
                viewer {
                    id
                    name
                }
            }
        "#;

        let result = self.graphql(query, json!({})).await?;
        
        if result["data"]["viewer"].is_object() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Linear health check failed"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> LinearConfig {
        LinearConfig {
            enabled: true,
            api_key: "test-api-key".to_string(),
            team_id: "test-team-id".to_string(),
            auto_create: true,
            labels: vec![],
        }
    }

    #[test]
    fn test_map_priority() {
        let config = create_test_config();
        let client = LinearClient::new(config).unwrap();

        assert_eq!(client.map_priority("critical"), 1);
        assert_eq!(client.map_priority("high"), 2);
        assert_eq!(client.map_priority("warning"), 3);
        assert_eq!(client.map_priority("info"), 4);
    }
}
