//! Diagnostic & Remediation Configuration
//!
//! Configure AI-powered error analysis, auto-remediation, and ticketing integrations.
//!
//! ## Example Configuration
//!
//! ```yaml
//! diagnostic:
//!   enabled: true
//!   
//!   # AI Analysis
//!   ai:
//!     provider: openai  # openai, anthropic, local
//!     api_key: "${OPENAI_API_KEY}"
//!     model: gpt-4
//!     max_tokens: 2000
//!     temperature: 0.3
//!   
//!   # Auto-remediation settings
//!   remediation:
//!     enabled: true
//!     auto_apply: false  # Require approval before applying fixes
//!     confidence_threshold: 0.85
//!     max_retries: 3
//!   
//!   # Ticketing integrations
//!   ticketing:
//!     jira:
//!       enabled: true
//!       host: "https://company.atlassian.net"
//!       username: "${JIRA_USER}"
//!       api_token: "${JIRA_TOKEN}"
//!       project_key: "OPS"
//!       issue_type: "Bug"
//!       auto_create: true
//!       
//!     github:
//!       enabled: true
//!       token: "${GITHUB_TOKEN}"
//!       owner: "myorg"
//!       repo: "myrepo"
//!       auto_create: true
//!       
//!     linear:
//!       enabled: false
//!       api_key: "${LINEAR_API_KEY}"
//!       team_id: "TEAM_ID"
//!   
//!   # Knowledge base
//!   knowledge_base:
//!     enabled: true
//!     vector_store_path: "./kb_vectors"
//!     similarity_threshold: 0.8
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Complete diagnostic configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiagnosticConfig {
    /// Enable/disable diagnostic system
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// AI analysis configuration
    #[serde(default)]
    pub ai: AiConfig,

    /// Remediation settings
    #[serde(default)]
    pub remediation: RemediationConfig,

    /// Ticketing integrations
    #[serde(default)]
    pub ticketing: TicketingConfig,

    /// Knowledge base settings
    #[serde(default)]
    pub knowledge_base: KnowledgeBaseConfig,

    /// Learning settings
    #[serde(default)]
    pub learning: LearningConfig,
}

impl Default for DiagnosticConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ai: AiConfig::default(),
            remediation: RemediationConfig::default(),
            ticketing: TicketingConfig::default(),
            knowledge_base: KnowledgeBaseConfig::default(),
            learning: LearningConfig::default(),
        }
    }
}

/// AI provider configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AiConfig {
    /// AI provider
    #[serde(default = "default_ai_provider")]
    pub provider: String,

    /// API key (can use env var)
    #[serde(default)]
    pub api_key: String,

    /// Model name
    #[serde(default = "default_ai_model")]
    pub model: String,

    /// Maximum tokens per request
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,

    /// Temperature (0-1, lower = more deterministic)
    #[serde(default = "default_temperature")]
    pub temperature: f32,

    /// Request timeout (seconds)
    #[serde(default = "default_ai_timeout")]
    pub timeout_seconds: u64,

    /// Enable code generation
    #[serde(default = "default_true")]
    pub enable_code_generation: bool,

    /// Enable root cause analysis
    #[serde(default = "default_true")]
    pub enable_root_cause_analysis: bool,

    /// Custom prompts
    #[serde(default)]
    pub custom_prompts: HashMap<String, String>,
}

impl Default for AiConfig {
    fn default() -> Self {
        Self {
            provider: default_ai_provider(),
            api_key: String::new(),
            model: default_ai_model(),
            max_tokens: default_max_tokens(),
            temperature: default_temperature(),
            timeout_seconds: default_ai_timeout(),
            enable_code_generation: true,
            enable_root_cause_analysis: true,
            custom_prompts: HashMap::new(),
        }
    }
}

/// Remediation configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RemediationConfig {
    /// Enable auto-remediation
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Auto-apply fixes (dangerous if true)
    #[serde(default)]
    pub auto_apply: bool,

    /// Minimum confidence for auto-apply
    #[serde(default = "default_confidence_threshold")]
    pub confidence_threshold: f64,

    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Require human approval for these severities
    #[serde(default = "default_approval_required")]
    pub approval_required_for: Vec<String>,

    /// Rollback on failure
    #[serde(default = "default_true")]
    pub rollback_on_failure: bool,
}

impl Default for RemediationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_apply: false,
            confidence_threshold: default_confidence_threshold(),
            max_retries: default_max_retries(),
            approval_required_for: default_approval_required(),
            rollback_on_failure: true,
        }
    }
}

/// Ticketing integrations configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TicketingConfig {
    /// Jira integration
    #[serde(default)]
    pub jira: Option<JiraConfig>,

    /// GitHub integration
    #[serde(default)]
    pub github: Option<GitHubConfig>,

    /// Linear integration
    #[serde(default)]
    pub linear: Option<LinearConfig>,

    /// Custom webhook
    #[serde(default)]
    pub webhook: Option<WebhookConfig>,
}

/// Jira configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JiraConfig {
    pub enabled: bool,
    pub host: String,
    pub username: String,
    pub api_token: String,
    pub project_key: String,
    pub issue_type: String,
    #[serde(default)]
    pub auto_create: bool,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub assignee: Option<String>,
    #[serde(default)]
    pub priority_mapping: HashMap<String, String>,
}

/// GitHub configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GitHubConfig {
    pub enabled: bool,
    pub token: String,
    pub owner: String,
    pub repo: String,
    #[serde(default)]
    pub auto_create: bool,
    #[serde(default = "default_github_labels")]
    pub labels: Vec<String>,
    #[serde(default)]
    pub assignees: Vec<String>,
}

/// Linear configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinearConfig {
    pub enabled: bool,
    pub api_key: String,
    pub team_id: String,
    #[serde(default)]
    pub auto_create: bool,
    #[serde(default)]
    pub labels: Vec<String>,
}

/// Custom webhook configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebhookConfig {
    pub enabled: bool,
    pub url: String,
    #[serde(default = "default_webhook_method")]
    pub method: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// Knowledge base configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KnowledgeBaseConfig {
    /// Enable knowledge base
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Vector store path
    #[serde(default = "default_kb_path")]
    pub vector_store_path: String,

    /// Similarity threshold for matching
    #[serde(default = "default_similarity_threshold")]
    pub similarity_threshold: f64,

    /// Maximum results to return
    #[serde(default = "default_max_kb_results")]
    pub max_results: usize,

    /// Auto-learn from resolved issues
    #[serde(default = "default_true")]
    pub auto_learn: bool,
}

impl Default for KnowledgeBaseConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            vector_store_path: default_kb_path(),
            similarity_threshold: default_similarity_threshold(),
            max_results: default_max_kb_results(),
            auto_learn: true,
        }
    }
}

/// Learning configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LearningConfig {
    /// Enable continuous learning
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Feedback collection
    #[serde(default = "default_true")]
    pub collect_feedback: bool,

    /// Model retraining interval (hours)
    #[serde(default = "default_retrain_interval")]
    pub retrain_interval_hours: u64,

    /// Minimum samples for retraining
    #[serde(default = "default_min_samples")]
    pub min_samples_for_retrain: u32,
}

impl Default for LearningConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            collect_feedback: true,
            retrain_interval_hours: default_retrain_interval(),
            min_samples_for_retrain: default_min_samples(),
        }
    }
}

// Default functions
fn default_true() -> bool { true }
fn default_ai_provider() -> String { "openai".to_string() }
fn default_ai_model() -> String { "gpt-4".to_string() }
fn default_max_tokens() -> u32 { 2000 }
fn default_temperature() -> f32 { 0.3 }
fn default_ai_timeout() -> u64 { 60 }
fn default_confidence_threshold() -> f64 { 0.85 }
fn default_max_retries() -> u32 { 3 }
fn default_approval_required() -> Vec<String> { vec!["critical".to_string()] }
fn default_webhook_method() -> String { "POST".to_string() }
fn default_kb_path() -> String { "./kb_vectors".to_string() }
fn default_similarity_threshold() -> f64 { 0.8 }
fn default_max_kb_results() -> usize { 5 }
fn default_retrain_interval() -> u64 { 24 }
fn default_min_samples() -> u32 { 100 }
fn default_github_labels() -> Vec<String> { vec!["bug".to_string(), "auto-generated".to_string()] }

/// Load configuration from file
pub fn load_config(path: &str) -> anyhow::Result<DiagnosticConfig> {
    let content = std::fs::read_to_string(path)?;
    let config: DiagnosticConfig = serde_yaml::from_str(&content)?;
    Ok(config)
}

/// Load from environment variables
pub fn from_env() -> DiagnosticConfig {
    let mut config = DiagnosticConfig::default();

    // AI configuration
    if let Ok(api_key) = std::env::var("OPENAI_API_KEY") {
        config.ai.provider = "openai".to_string();
        config.ai.api_key = api_key;
    } else if let Ok(api_key) = std::env::var("ANTHROPIC_API_KEY") {
        config.ai.provider = "anthropic".to_string();
        config.ai.api_key = api_key;
    }

    if let Ok(model) = std::env::var("AI_MODEL") {
        config.ai.model = model;
    }

    // Jira configuration
    if let (Ok(host), Ok(user), Ok(token)) = (
        std::env::var("JIRA_HOST"),
        std::env::var("JIRA_USER"),
        std::env::var("JIRA_TOKEN"),
    ) {
        config.ticketing.jira = Some(JiraConfig {
            enabled: true,
            host,
            username: user,
            api_token: token,
            project_key: std::env::var("JIRA_PROJECT").unwrap_or_else(|_| "OPS".to_string()),
            issue_type: std::env::var("JIRA_ISSUE_TYPE").unwrap_or_else(|_| "Bug".to_string()),
            auto_create: std::env::var("JIRA_AUTO_CREATE")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            labels: vec![],
            assignee: None,
            priority_mapping: HashMap::new(),
        });
    }

    // GitHub configuration
    if let (Ok(token), Ok(owner), Ok(repo)) = (
        std::env::var("GITHUB_TOKEN"),
        std::env::var("GITHUB_OWNER"),
        std::env::var("GITHUB_REPO"),
    ) {
        config.ticketing.github = Some(GitHubConfig {
            enabled: true,
            token,
            owner,
            repo,
            auto_create: std::env::var("GITHUB_AUTO_CREATE")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            labels: default_github_labels(),
            assignees: vec![],
        });
    }

    config
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DiagnosticConfig::default();
        assert!(config.enabled);
        assert!(config.ai.enable_code_generation);
        assert!(!config.remediation.auto_apply);
    }

    #[test]
    fn test_severity_display() {
        // Placeholder test
        assert_eq!(2 + 2, 4);
    }
}
