//! Diagnostic Models

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A diagnostic report for an error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticReport {
    /// Unique report ID
    pub id: String,

    /// Original error
    pub error: ErrorContext,

    /// Root cause analysis
    pub root_cause: RootCauseAnalysis,

    /// Suggested fixes
    pub suggested_fixes: Vec<SuggestedFix>,

    /// Related knowledge base articles
    pub related_articles: Vec<KnowledgeArticle>,

    /// Generated code samples
    pub code_samples: Vec<CodeSample>,

    /// Remediation plan
    pub remediation_plan: Option<RemediationPlan>,

    /// Confidence score (0-1)
    pub confidence: f64,

    /// Processing time (ms)
    pub processing_time_ms: u64,

    /// Timestamp
    pub timestamp_us: u64,
}

impl DiagnosticReport {
    pub fn new(error: ErrorContext) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            error,
            root_cause: RootCauseAnalysis::default(),
            suggested_fixes: vec![],
            related_articles: vec![],
            code_samples: vec![],
            remediation_plan: None,
            confidence: 0.0,
            processing_time_ms: 0,
            timestamp_us: chrono::Utc::now().timestamp_micros() as u64,
        }
    }
}

/// Error context
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ErrorContext {
    /// Error message
    pub message: String,

    /// Error type/class
    pub error_type: String,

    /// Stack trace
    pub stack_trace: Option<String>,

    /// Source service/component
    pub source: String,

    /// Severity
    pub severity: String,

    /// Additional context
    pub context: HashMap<String, String>,

    /// Related logs
    pub logs: Vec<String>,

    /// Metrics at time of error
    pub metrics: HashMap<String, f64>,
}

/// Root cause analysis
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RootCauseAnalysis {
    /// Primary cause
    pub primary_cause: String,

    /// Contributing factors
    pub contributing_factors: Vec<String>,

    /// Detailed explanation
    pub explanation: String,

    /// Component at fault
    pub faulty_component: Option<String>,

    /// Timeline of events
    pub event_timeline: Vec<TimelineEvent>,
}

/// Timeline event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub timestamp_us: u64,
    pub description: String,
    pub component: String,
}

/// Suggested fix
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuggestedFix {
    /// Fix ID
    pub id: String,

    /// Title
    pub title: String,

    /// Description
    pub description: String,

    /// Steps to implement
    pub steps: Vec<String>,

    /// Confidence score (0-1)
    pub confidence: f64,

    /// Estimated time to implement
    pub estimated_time_minutes: u32,

    /// Risk level
    pub risk_level: RiskLevel,

    /// Can be auto-applied
    pub auto_applicable: bool,

    /// Required permissions
    pub required_permissions: Vec<String>,

    /// Rollback steps
    pub rollback_steps: Vec<String>,
}

/// Risk level
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

impl RiskLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            RiskLevel::Low => "low",
            RiskLevel::Medium => "medium",
            RiskLevel::High => "high",
            RiskLevel::Critical => "critical",
        }
    }
}

/// Knowledge base article
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeArticle {
    /// Article ID
    pub id: String,

    /// Title
    pub title: String,

    /// Content
    pub content: String,

    /// Similarity score (0-1)
    pub similarity: f64,

    /// Source URL
    pub source_url: Option<String>,

    /// Tags
    pub tags: Vec<String>,
}

/// Code sample
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeSample {
    /// Sample ID
    pub id: String,

    /// Title
    pub title: String,

    /// Description
    pub description: String,

    /// Programming language
    pub language: String,

    /// Code
    pub code: String,

    /// Explanation
    pub explanation: String,

    /// Where to apply
    pub file_path: Option<String>,

    /// Line numbers
    pub line_numbers: Option<(u32, u32)>,
}

/// Remediation plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemediationPlan {
    /// Plan ID
    pub id: String,

    /// Steps to execute
    pub steps: Vec<RemediationStep>,

    /// Total estimated time
    pub total_estimated_time_minutes: u32,

    /// Overall risk
    pub overall_risk: RiskLevel,

    /// Requires approval
    pub requires_approval: bool,

    /// Approval reason
    pub approval_reason: Option<String>,
}

/// Remediation step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemediationStep {
    /// Step number
    pub step_number: u32,

    /// Title
    pub title: String,

    /// Description
    pub description: String,

    /// Action type
    pub action_type: ActionType,

    /// Action details
    pub action: Action,

    /// Verification command
    pub verification: Option<String>,

    /// Rollback action
    pub rollback: Option<Action>,
}

/// Action type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionType {
    Command,
    CodeChange,
    ConfigChange,
    Restart,
    Scale,
    Manual,
}

/// Action details
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Action {
    Command {
        command: String,
        working_dir: Option<String>,
        env_vars: HashMap<String, String>,
    },
    CodeChange {
        file_path: String,
        search_pattern: String,
        replacement: String,
    },
    ConfigChange {
        file_path: String,
        key: String,
        new_value: String,
    },
    Restart {
        service_name: String,
    },
    Scale {
        service_name: String,
        replicas: u32,
    },
    Manual {
        instructions: String,
    },
}

/// Diagnostic request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticRequest {
    /// Error context
    pub error: ErrorContext,

    /// Request ID
    pub request_id: Option<String>,

    /// Additional context
    pub context: HashMap<String, String>,

    /// Preferred language for code samples
    pub preferred_language: Option<String>,

    /// Include remediation plan
    pub include_remediation: bool,
}

impl Default for DiagnosticRequest {
    fn default() -> Self {
        Self {
            error: ErrorContext::default(),
            request_id: None,
            context: HashMap::new(),
            preferred_language: None,
            include_remediation: true,
        }
    }
}

/// Diagnostic response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticResponse {
    /// Report
    pub report: DiagnosticReport,

    /// Ticket created (if ticketing enabled)
    pub ticket: Option<TicketInfo>,

    /// Next steps for user
    pub next_steps: Vec<String>,
}

/// Ticket information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TicketInfo {
    /// Ticket ID
    pub id: String,

    /// Ticket URL
    pub url: String,

    /// Provider
    pub provider: String,

    /// Status
    pub status: String,
}

/// Feedback for learning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticFeedback {
    /// Report ID
    pub report_id: String,

    /// Was the diagnosis helpful?
    pub helpful: bool,

    /// Was the fix successful?
    pub fix_successful: Option<bool>,

    /// Correct root cause (if different)
    pub correct_root_cause: Option<String>,

    /// User comments
    pub comments: Option<String>,

    /// Applied fix ID
    pub applied_fix_id: Option<String>,
}

/// Learning sample
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LearningSample {
    /// Error signature
    pub error_signature: String,

    /// Correct diagnosis
    pub correct_diagnosis: String,

    /// Successful fix
    pub successful_fix: String,

    /// Context
    pub context: HashMap<String, String>,

    /// Timestamp
    pub timestamp_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diagnostic_report_creation() {
        let error = ErrorContext {
            message: "Connection refused".to_string(),
            error_type: "ConnectionError".to_string(),
            source: "api-service".to_string(),
            ..Default::default()
        };

        let report = DiagnosticReport::new(error);
        assert!(!report.id.is_empty());
        assert_eq!(report.confidence, 0.0);
    }

    #[test]
    fn test_risk_level_as_str() {
        assert_eq!(RiskLevel::Low.as_str(), "low");
        assert_eq!(RiskLevel::Critical.as_str(), "critical");
    }
}
