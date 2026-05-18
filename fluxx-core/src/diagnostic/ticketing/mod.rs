//! Ticketing integrations for auto-creating issues

pub mod jira;
pub mod github;
pub mod linear;

pub use jira::JiraClient;
pub use github::GitHubClient;
pub use linear::LinearClient;

use crate::diagnostic::models::{DiagnosticReport, TicketInfo};
use async_trait::async_trait;

/// Ticketing provider trait
#[async_trait]
pub trait TicketingProvider: Send + Sync {
    /// Get provider name
    fn name(&self) -> &str;

    /// Create ticket from diagnostic report
    async fn create_ticket(&self, report: &DiagnosticReport) -> anyhow::Result<TicketInfo>;

    /// Update ticket with resolution
    async fn resolve_ticket(&self, ticket_id: &str, resolution: &str) -> anyhow::Result<()>;

    /// Add comment to ticket
    async fn add_comment(&self, ticket_id: &str, comment: &str) -> anyhow::Result<()>;

    /// Health check
    async fn health_check(&self) -> anyhow::Result<()>;
}

/// Ticket priority
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TicketPriority {
    Lowest,
    Low,
    Medium,
    High,
    Highest,
}

impl TicketPriority {
    /// Convert from severity string
    pub fn from_severity(severity: &str) -> Self {
        match severity.to_lowercase().as_str() {
            "critical" => TicketPriority::Highest,
            "high" => TicketPriority::High,
            "warning" => TicketPriority::Medium,
            "info" => TicketPriority::Low,
            _ => TicketPriority::Medium,
        }
    }
}

/// Ticket template for creating issues
pub struct TicketTemplate {
    pub title: String,
    pub description: String,
    pub priority: TicketPriority,
    pub labels: Vec<String>,
}

impl TicketTemplate {
    /// Create from diagnostic report
    pub fn from_report(report: &DiagnosticReport) -> Self {
        let title = format!(
            "[{}] {} - {}",
            report.error.severity.to_uppercase(),
            report.error.source,
            report.error.message.lines().next().unwrap_or(&report.error.message)
        );

        let mut description = format!(
            "h2. Error Details\n\n"
        );

        description.push_str(&format!(
            "*Message:* {}\n*Type:* {}\n*Source:* {}\n*Diagnostic ID:* {}\n\n",
            report.error.message,
            report.error.error_type,
            report.error.source,
            report.id
        ));

        description.push_str("h2. Root Cause Analysis\n\n");
        description.push_str(&format!(
            "*Primary Cause:* {}\n*Explanation:* {}\n\n",
            report.root_cause.primary_cause,
            report.root_cause.explanation
        ));

        if !report.suggested_fixes.is_empty() {
            description.push_str("h2. Suggested Fixes\n\n");
            for (_i, fix) in report.suggested_fixes.iter().enumerate() {
                description.push_str(&format!(
                    "# *{}* (confidence: {:.0}%)\n{}\nSteps:\n",
                    fix.title,
                    fix.confidence * 100.0,
                    fix.description
                ));
                for step in &fix.steps {
                    description.push_str(&format!("#* {}\n", step));
                }
                description.push('\n');
            }
        }

        if !report.code_samples.is_empty() {
            description.push_str("h2. Code Samples\n\n");
            for sample in &report.code_samples {
                description.push_str(&format!(
                    "*{}* ({}):\n{{code:{}}}\n{}\n{{code}}\n\n{}\n\n",
                    sample.title,
                    sample.language,
                    sample.language,
                    sample.code,
                    sample.explanation
                ));
            }
        }

        description.push_str("h2. Context\n\n");
        for (key, value) in &report.error.context {
            description.push_str(&format!("*{}:* {}\n", key, value));
        }

        Self {
            title,
            description,
            priority: TicketPriority::from_severity(&report.error.severity),
            labels: vec!["auto-generated".to_string(), "diagnostic".to_string()],
        }
    }
}

/// Format report for GitHub (Markdown)
pub fn format_github_markdown(report: &DiagnosticReport) -> String {
    let mut md = format!(
        "## Error Details\n\n"
    );

    md.push_str(&format!(
        "- **Message:** {}\n- **Type:** {}\n- **Source:** {}\n- **Diagnostic ID:** {}\n\n",
        report.error.message,
        report.error.error_type,
        report.error.source,
        report.id
    ));

    md.push_str("## Root Cause Analysis\n\n");
    md.push_str(&format!(
        "**Primary Cause:** {}\n\n**Explanation:** {}\n\n",
        report.root_cause.primary_cause,
        report.root_cause.explanation
    ));

    if !report.suggested_fixes.is_empty() {
        md.push_str("## Suggested Fixes\n\n");
        for (i, fix) in report.suggested_fixes.iter().enumerate() {
            md.push_str(&format!(
                "### {}. {} (confidence: {:.0}%)\n\n{}\n\n**Steps:**\n",
                i + 1,
                fix.title,
                fix.confidence * 100.0,
                fix.description
            ));
            for step in &fix.steps {
                md.push_str(&format!("- {}\n", step));
            }
            md.push('\n');
        }
    }

    if !report.code_samples.is_empty() {
        md.push_str("## Code Samples\n\n");
        for sample in &report.code_samples {
            md.push_str(&format!(
                "### {}\n\n_{}_\n\n```{}\n{}\n```\n\n{}\n\n",
                sample.title,
                sample.description,
                sample.language,
                sample.code,
                sample.explanation
            ));
        }
    }

    md
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic::models::{ErrorContext, RootCauseAnalysis};

    fn create_test_report() -> DiagnosticReport {
        DiagnosticReport {
            id: "test-123".to_string(),
            error: ErrorContext {
                message: "Connection refused".to_string(),
                error_type: "ConnectionError".to_string(),
                source: "api-service".to_string(),
                severity: "critical".to_string(),
                ..Default::default()
            },
            root_cause: RootCauseAnalysis {
                primary_cause: "Database down".to_string(),
                explanation: "The PostgreSQL instance is not responding".to_string(),
                ..Default::default()
            },
            suggested_fixes: vec![],
            related_articles: vec![],
            code_samples: vec![],
            remediation_plan: None,
            confidence: 0.95,
            processing_time_ms: 1000,
            timestamp_us: 0,
        }
    }

    #[test]
    fn test_ticket_template_from_report() {
        let report = create_test_report();
        let template = TicketTemplate::from_report(&report);

        assert!(template.title.contains("CRITICAL"));
        assert!(template.title.contains("api-service"));
        assert!(template.description.contains("Database down"));
    }

    #[test]
    fn test_priority_from_severity() {
        assert_eq!(TicketPriority::from_severity("critical"), TicketPriority::Highest);
        assert_eq!(TicketPriority::from_severity("warning"), TicketPriority::Medium);
        assert_eq!(TicketPriority::from_severity("info"), TicketPriority::Low);
    }
}
