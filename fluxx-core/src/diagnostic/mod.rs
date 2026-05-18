//! AI-Powered Diagnostic & Remediation System
//!
//! Automatically analyze errors, suggest fixes, generate code samples,
//! and create tickets in your issue tracking system.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use fluxx_agent::diagnostic::{DiagnosticEngine, DiagnosticRequest};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Initialize diagnostic engine
//!     let engine = DiagnosticEngine::from_env().await?;
//!
//!     // Create a diagnostic request
//!     let request = DiagnosticRequest {
//!         error: ErrorContext {
//!             message: "Connection refused".to_string(),
//!             error_type: "ConnectionError".to_string(),
//!             source: "api-service".to_string(),
//!             ..Default::default()
//!         },
//!         include_remediation: true,
//!         ..Default::default()
//!     };
//!
//!     // Run diagnosis
//!     let response = engine.diagnose(request).await?;
//!
//!     println!("Root cause: {}", response.report.root_cause.primary_cause);
//!     println!("Suggested fixes: {}", response.report.suggested_fixes.len());
//!
//!     Ok(())
//! }
//! ```

pub mod ai;
pub mod config;
pub mod models;
pub mod remediation;
pub mod ticketing;
pub mod api;

use crate::alerts::models::Alert;
use crate::diagnostic::ai::analyzer::AiAnalyzer;
use crate::diagnostic::config::{DiagnosticConfig, from_env, load_config};
use crate::diagnostic::models::*;
use crate::diagnostic::ticketing::{TicketingProvider, JiraClient, GitHubClient, LinearClient};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Main diagnostic engine
pub struct DiagnosticEngine {
    pub config: DiagnosticConfig,
    pub analyzer: Option<AiAnalyzer>,
    pub ticketing_providers: Vec<Box<dyn TicketingProvider>>,
    pub knowledge_base: Arc<RwLock<KnowledgeBase>>,
}

/// Knowledge base for storing and retrieving similar issues
#[derive(Default)]
pub struct KnowledgeBase {
    pub articles: Vec<KnowledgeArticle>,
}

impl KnowledgeBase {
    pub fn new() -> Self {
        Self::default()
    }

    /// Find similar articles
    pub fn find_similar(&self, _error: &ErrorContext, threshold: f64) -> Vec<KnowledgeArticle> {
        // Placeholder - would implement vector similarity search
        self.articles.iter()
            .filter(|a| a.similarity >= threshold)
            .cloned()
            .collect()
    }

    /// Add article
    pub fn add_article(&mut self, article: KnowledgeArticle) {
        self.articles.push(article);
    }
}

impl DiagnosticEngine {
    /// Create new diagnostic engine with configuration
    pub async fn new(config: DiagnosticConfig) -> anyhow::Result<Self> {
        // Initialize AI analyzer if enabled
        let analyzer = if config.enabled && config.ai.enable_root_cause_analysis {
            match AiAnalyzer::new(config.ai.clone()).await {
                Ok(a) => Some(a),
                Err(e) => {
                    tracing::warn!("Failed to initialize AI analyzer: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Initialize ticketing providers
        let mut ticketing_providers: Vec<Box<dyn TicketingProvider>> = vec![];

        if let Some(jira_config) = &config.ticketing.jira {
            if jira_config.enabled {
                match JiraClient::new(jira_config.clone()) {
                    Ok(client) => {
                        tracing::info!("Jira ticketing integration enabled");
                        ticketing_providers.push(Box::new(client));
                    }
                    Err(e) => tracing::warn!("Failed to initialize Jira: {}", e),
                }
            }
        }

        if let Some(github_config) = &config.ticketing.github {
            if github_config.enabled {
                match GitHubClient::new(github_config.clone()) {
                    Ok(client) => {
                        tracing::info!("GitHub ticketing integration enabled");
                        ticketing_providers.push(Box::new(client));
                    }
                    Err(e) => tracing::warn!("Failed to initialize GitHub: {}", e),
                }
            }
        }

        if let Some(linear_config) = &config.ticketing.linear {
            if linear_config.enabled {
                match LinearClient::new(linear_config.clone()) {
                    Ok(client) => {
                        tracing::info!("Linear ticketing integration enabled");
                        ticketing_providers.push(Box::new(client));
                    }
                    Err(e) => tracing::warn!("Failed to initialize Linear: {}", e),
                }
            }
        }

        let knowledge_base = Arc::new(RwLock::new(KnowledgeBase::new()));

        Ok(Self {
            config,
            analyzer,
            ticketing_providers,
            knowledge_base,
        })
    }

    /// Create from environment variables
    pub async fn from_env() -> anyhow::Result<Self> {
        let config = from_env();
        Self::new(config).await
    }

    /// Load from configuration file
    pub async fn from_file(path: &str) -> anyhow::Result<Self> {
        let config = load_config(path)?;
        Self::new(config).await
    }

    /// Run complete diagnosis
    pub async fn diagnose(&self, request: DiagnosticRequest) -> anyhow::Result<DiagnosticResponse> {
        if !self.config.enabled {
            return Err(anyhow::anyhow!("Diagnostic engine is disabled"));
        }

        let mut report = if let Some(analyzer) = &self.analyzer {
            analyzer.diagnose(&request).await?
        } else {
            // Fallback to basic analysis
            let mut r = DiagnosticReport::new(request.error.clone());
            r.root_cause = RootCauseAnalysis {
                primary_cause: "AI analysis not available".to_string(),
                explanation: "Please configure AI provider for automatic analysis".to_string(),
                ..Default::default()
            };
            r
        };

        // Find related knowledge base articles
        let kb = self.knowledge_base.read().await;
        report.related_articles = kb.find_similar(&request.error, self.config.knowledge_base.similarity_threshold);
        drop(kb);

        // Create ticket if enabled
        let ticket = if self.should_create_ticket(&report).await {
            match self.create_ticket(&report).await {
                Ok(t) => Some(t),
                Err(e) => {
                    tracing::error!("Failed to create ticket: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Generate next steps
        let next_steps = self.generate_next_steps(&report);

        Ok(DiagnosticResponse {
            report,
            ticket,
            next_steps,
        })
    }

    /// Diagnose from an alert
    pub async fn diagnose_alert(&self, alert: &Alert) -> anyhow::Result<DiagnosticResponse> {
        let request = DiagnosticRequest {
            error: ErrorContext {
                message: alert.message.clone(),
                error_type: "Alert".to_string(),
                source: alert.source.clone(),
                severity: alert.severity.to_string(),
                ..Default::default()
            },
            include_remediation: true,
            ..Default::default()
        };

        self.diagnose(request).await
    }

    /// Check if should auto-create ticket
    async fn should_create_ticket(&self, report: &DiagnosticReport) -> bool {
        // Check if any provider has auto_create enabled
        let has_auto_create = self.ticketing_providers.iter().any(|_p| {
            // This is a simplified check - in reality we'd need to access config
            // For now, assume auto_create is based on severity
            report.error.severity.to_lowercase() == "critical"
        });

        has_auto_create && report.confidence >= self.config.remediation.confidence_threshold
    }

    /// Create ticket in first available provider
    async fn create_ticket(&self, report: &DiagnosticReport) -> anyhow::Result<TicketInfo> {
        for provider in &self.ticketing_providers {
            match provider.create_ticket(report).await {
                Ok(ticket) => {
                    tracing::info!(
                        provider = provider.name(),
                        ticket_id = %ticket.id,
                        "Created ticket"
                    );
                    return Ok(ticket);
                }
                Err(e) => {
                    tracing::warn!(
                        provider = provider.name(),
                        error = %e,
                        "Failed to create ticket, trying next provider"
                    );
                }
            }
        }

        Err(anyhow::anyhow!("All ticketing providers failed"))
    }

    /// Generate next steps for user
    fn generate_next_steps(&self, report: &DiagnosticReport) -> Vec<String> {
        let mut steps = vec![];

        // Step 1: Review root cause
        steps.push(format!(
            "1. Review the root cause: {}",
            report.root_cause.primary_cause
        ));

        // Step 2: Check suggested fixes
        for (i, fix) in report.suggested_fixes.iter().enumerate() {
            steps.push(format!(
                "{}. {} (confidence: {:.0}%)",
                i + 2,
                fix.description,
                fix.confidence * 100.0
            ));
        }

        // Step 3: Check code samples
        if !report.code_samples.is_empty() {
            steps.push("3. Review the provided code samples for implementation guidance".to_string());
        }

        // Step 4: Follow remediation plan
        if report.remediation_plan.is_some() {
            steps.push("4. Follow the step-by-step remediation plan".to_string());
        }

        // Step 5: Check tickets
        if self.config.ticketing.jira.is_some() || 
           self.config.ticketing.github.is_some() || 
           self.config.ticketing.linear.is_some() {
            steps.push("5. Track progress in the linked ticket".to_string());
        }

        // Step 6: Provide feedback
        if self.config.learning.collect_feedback {
            steps.push("6. Provide feedback on the diagnosis to help improve future recommendations".to_string());
        }

        steps
    }

    /// Submit feedback
    pub async fn submit_feedback(&self, feedback: DiagnosticFeedback) -> anyhow::Result<()> {
        tracing::info!(
            report_id = %feedback.report_id,
            helpful = feedback.helpful,
            "Received diagnostic feedback"
        );

        // Would store feedback for model retraining
        // This is a placeholder

        Ok(())
    }

    /// Health check
    pub async fn health_check(&self) -> anyhow::Result<()> {
        // Check AI provider
        if let Some(analyzer) = &self.analyzer {
            analyzer.health_check().await?;
        }

        // Check ticketing providers
        for provider in &self.ticketing_providers {
            if let Err(e) = provider.health_check().await {
                tracing::warn!(provider = provider.name(), error = %e, "Health check failed");
            }
        }

        Ok(())
    }

    /// Get knowledge base
    pub fn knowledge_base(&self) -> Arc<RwLock<KnowledgeBase>> {
        self.knowledge_base.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_diagnostic_engine_disabled() {
        let mut config = DiagnosticConfig::default();
        config.enabled = false;

        let engine = DiagnosticEngine::new(config).await;
        assert!(engine.is_ok());

        let engine = engine.unwrap();
        let request = DiagnosticRequest {
            error: ErrorContext {
                message: "test".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = engine.diagnose(request).await;
        assert!(result.is_err());
    }
}
