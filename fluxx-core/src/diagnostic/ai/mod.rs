//! AI-powered diagnostic analysis

pub mod analyzer;
pub mod prompts;

pub use analyzer::AiAnalyzer;
pub use prompts::PromptTemplates;

use crate::diagnostic::models::*;
use async_trait::async_trait;

/// AI provider trait
#[async_trait]
pub trait AiProvider: Send + Sync {
    /// Get provider name
    fn name(&self) -> &str;

    /// Analyze error and return root cause
    async fn analyze_error(&self, error: &ErrorContext) -> anyhow::Result<RootCauseAnalysis>;

    /// Generate suggested fixes
    async fn generate_fixes(&self, error: &ErrorContext, root_cause: &RootCauseAnalysis) 
        -> anyhow::Result<Vec<SuggestedFix>>;

    /// Generate code samples
    async fn generate_code_samples(&self, error: &ErrorContext, fix: &SuggestedFix, language: &str)
        -> anyhow::Result<Vec<CodeSample>>;

    /// Create remediation plan
    async fn create_remediation_plan(&self, error: &ErrorContext, fixes: &[SuggestedFix])
        -> anyhow::Result<RemediationPlan>;

    /// Health check
    async fn health_check(&self) -> anyhow::Result<()>;
}

/// AI response parser
pub fn parse_ai_response(response: &str) -> anyhow::Result<ParsedAiResponse> {
    // Try to parse as JSON first
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(response) {
        return Ok(ParsedAiResponse {
            root_cause: json["root_cause"].as_str().map(|s| s.to_string()),
            explanation: json["explanation"].as_str().map(|s| s.to_string()),
            fixes: parse_fixes_from_json(&json),
            code_samples: parse_code_samples_from_json(&json),
            confidence: json["confidence"].as_f64().unwrap_or(0.5),
        });
    }

    // Fall back to text parsing
    Ok(parse_text_response(response))
}

/// Parsed AI response
#[derive(Debug, Default)]
pub struct ParsedAiResponse {
    pub root_cause: Option<String>,
    pub explanation: Option<String>,
    pub fixes: Vec<ParsedFix>,
    pub code_samples: Vec<ParsedCodeSample>,
    pub confidence: f64,
}

/// Parsed fix
#[derive(Debug, Default)]
pub struct ParsedFix {
    pub title: String,
    pub description: String,
    pub steps: Vec<String>,
    pub confidence: f64,
    pub risk_level: String,
}

/// Parsed code sample
#[derive(Debug, Default)]
pub struct ParsedCodeSample {
    pub language: String,
    pub code: String,
    pub explanation: String,
}

fn parse_fixes_from_json(json: &serde_json::Value) -> Vec<ParsedFix> {
    let mut fixes = vec![];
    
    if let Some(fixes_array) = json["fixes"].as_array() {
        for fix in fixes_array {
            fixes.push(ParsedFix {
                title: fix["title"].as_str().unwrap_or("Fix").to_string(),
                description: fix["description"].as_str().unwrap_or("").to_string(),
                steps: fix["steps"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                    .unwrap_or_default(),
                confidence: fix["confidence"].as_f64().unwrap_or(0.5),
                risk_level: fix["risk"].as_str().unwrap_or("medium").to_string(),
            });
        }
    }
    
    fixes
}

fn parse_code_samples_from_json(json: &serde_json::Value) -> Vec<ParsedCodeSample> {
    let mut samples = vec![];
    
    if let Some(samples_array) = json["code_samples"].as_array() {
        for sample in samples_array {
            samples.push(ParsedCodeSample {
                language: sample["language"].as_str().unwrap_or("text").to_string(),
                code: sample["code"].as_str().unwrap_or("").to_string(),
                explanation: sample["explanation"].as_str().unwrap_or("").to_string(),
            });
        }
    }
    
    samples
}

fn parse_text_response(response: &str) -> ParsedAiResponse {
    let mut parsed = ParsedAiResponse::default();
    
    // Simple text parsing heuristics
    for line in response.lines() {
        let line = line.trim();
        
        if line.starts_with("Root cause:") || line.starts_with("## Root Cause") {
            parsed.root_cause = Some(line.split(':').nth(1).unwrap_or(line).trim().to_string());
        } else if line.starts_with("Explanation:") || line.starts_with("## Explanation") {
            parsed.explanation = Some(line.split(':').nth(1).unwrap_or(line).trim().to_string());
        } else if line.starts_with("Confidence:") {
            parsed.confidence = line.split(':')
                .nth(1)
                .and_then(|s| s.trim().parse::<f64>().ok())
                .unwrap_or(0.5);
        }
    }
    
    parsed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_response() {
        let json = r#"{
            "root_cause": "Database connection pool exhausted",
            "explanation": "Too many concurrent connections",
            "confidence": 0.95,
            "fixes": [
                {
                    "title": "Increase connection pool",
                    "description": "Increase max connections",
                    "steps": ["Edit config", "Restart"],
                    "confidence": 0.9,
                    "risk": "low"
                }
            ]
        }"#;

        let parsed = parse_ai_response(json).unwrap();
        assert_eq!(parsed.root_cause, Some("Database connection pool exhausted".to_string()));
        assert_eq!(parsed.confidence, 0.95);
        assert_eq!(parsed.fixes.len(), 1);
    }

    #[test]
    fn test_parse_text_response() {
        let text = r#"
Root cause: Out of memory
Explanation: Memory leak in cache
Confidence: 0.85
        "#;

        let parsed = parse_text_response(text);
        assert_eq!(parsed.root_cause, Some("Out of memory".to_string()));
        assert_eq!(parsed.confidence, 0.85);
    }
}
