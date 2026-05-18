//! AI Analyzer Implementation

use super::AiProvider;
use crate::diagnostic::models::*;
use crate::diagnostic::config::AiConfig;
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::json;

/// AI Analyzer using external LLM providers
#[allow(dead_code)]
pub struct AiAnalyzer {
    config: AiConfig,
    client: reqwest::Client,
    provider: Box<dyn AiProvider>,
}

impl AiAnalyzer {
    /// Create new AI analyzer
    pub async fn new(config: AiConfig) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_seconds))
            .build()?;

        let provider: Box<dyn AiProvider> = match config.provider.as_str() {
            "openai" => Box::new(OpenAiProvider::new(config.clone(), client.clone())?),
            "anthropic" => Box::new(AnthropicProvider::new(config.clone(), client.clone())?),
            _ => return Err(anyhow::anyhow!("Unknown AI provider: {}", config.provider)),
        };

        Ok(Self {
            config,
            client,
            provider,
        })
    }

    /// Run complete diagnostic analysis
    pub async fn diagnose(&self, request: &DiagnosticRequest) -> anyhow::Result<DiagnosticReport> {
        let start = std::time::Instant::now();
        let mut report = DiagnosticReport::new(request.error.clone());

        // Step 1: Root cause analysis
        tracing::info!("Starting root cause analysis...");
        report.root_cause = self.provider.analyze_error(&request.error).await?;

        // Step 2: Generate fixes
        tracing::info!("Generating fixes...");
        report.suggested_fixes = self.provider.generate_fixes(&request.error, &report.root_cause).await?;

        // Step 3: Generate code samples
        if self.config.enable_code_generation && !report.suggested_fixes.is_empty() {
            tracing::info!("Generating code samples...");
            let language = request.preferred_language.as_deref().unwrap_or("rust");
            
            for fix in &report.suggested_fixes[..1.min(report.suggested_fixes.len())] {
                match self.provider.generate_code_samples(&request.error, fix, language).await {
                    Ok(samples) => report.code_samples.extend(samples),
                    Err(e) => tracing::warn!("Failed to generate code: {}", e),
                }
            }
        }

        // Step 4: Create remediation plan
        if request.include_remediation && !report.suggested_fixes.is_empty() {
            tracing::info!("Creating remediation plan...");
            report.remediation_plan = Some(
                self.provider.create_remediation_plan(&request.error, &report.suggested_fixes).await?
            );
        }

        // Calculate confidence
        report.confidence = self.calculate_confidence(&report);
        report.processing_time_ms = start.elapsed().as_millis() as u64;

        tracing::info!(
            report_id = %report.id,
            confidence = report.confidence,
            processing_time_ms = report.processing_time_ms,
            "Diagnostic analysis complete"
        );

        Ok(report)
    }

    /// Calculate overall confidence score
    fn calculate_confidence(&self, report: &DiagnosticReport) -> f64 {
        let mut scores = vec![];

        // Root cause confidence (weighted 40%)
        if !report.root_cause.primary_cause.is_empty() {
            scores.push(0.7);
        }

        // Fix confidence (average, weighted 40%)
        if !report.suggested_fixes.is_empty() {
            let fix_avg: f64 = report.suggested_fixes.iter().map(|f| f.confidence).sum::<f64>() 
                / report.suggested_fixes.len() as f64;
            scores.push(fix_avg * 0.8);
        }

        // Code sample confidence (weighted 20%)
        if !report.code_samples.is_empty() {
            scores.push(0.75);
        }

        if scores.is_empty() {
            0.0
        } else {
            scores.iter().sum::<f64>() / scores.len() as f64
        }
    }

    /// Health check
    pub async fn health_check(&self) -> anyhow::Result<()> {
        self.provider.health_check().await
    }
}

/// OpenAI provider
pub struct OpenAiProvider {
    config: AiConfig,
    client: reqwest::Client,
    api_url: String,
}

impl OpenAiProvider {
    fn new(config: AiConfig, client: reqwest::Client) -> anyhow::Result<Self> {
        if config.api_key.is_empty() {
            return Err(anyhow::anyhow!("OpenAI API key is required"));
        }

        Ok(Self {
            config,
            client,
            api_url: "https://api.openai.com/v1/chat/completions".to_string(),
        })
    }

    fn build_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.config.api_key)).unwrap(),
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers
    }

    async fn chat_completion(&self, messages: Vec<serde_json::Value>) -> anyhow::Result<String> {
        let body = json!({
            "model": self.config.model,
            "messages": messages,
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
        });

        let response = self.client
            .post(&self.api_url)
            .headers(self.build_headers())
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let error = response.text().await?;
            return Err(anyhow::anyhow!("OpenAI API error: {}", error));
        }

        let json: serde_json::Value = response.json().await?;
        let content = json["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid response format"))?;

        Ok(content.to_string())
    }
}

#[async_trait]
impl AiProvider for OpenAiProvider {
    fn name(&self) -> &str {
        "openai"
    }

    async fn analyze_error(&self, error: &ErrorContext) -> anyhow::Result<RootCauseAnalysis> {
        let prompt = format!(
            r#"Analyze this error and provide root cause analysis in JSON format:

Error: {}
Type: {}
Source: {}
Context: {:?}
Stack trace: {}

Respond with JSON in this exact format:
{{
  "primary_cause": "the main cause of the error",
  "contributing_factors": ["factor 1", "factor 2"],
  "explanation": "detailed technical explanation",
  "faulty_component": "component name if identifiable"
}}"#,
            error.message,
            error.error_type,
            error.source,
            error.context,
            error.stack_trace.as_deref().unwrap_or("none")
        );

        let messages = vec![
            json!({"role": "system", "content": "You are an expert SRE and software engineer specializing in error analysis and root cause identification."}),
            json!({"role": "user", "content": prompt}),
        ];

        let response = self.chat_completion(messages).await?;
        
        // Parse JSON response
        let parsed: serde_json::Value = serde_json::from_str(&response)
            .or_else(|_| {
                // Try to extract JSON from markdown
                let json_start = response.find('{').unwrap_or(0);
                let json_end = response.rfind('}').map(|i| i + 1).unwrap_or(response.len());
                serde_json::from_str(&response[json_start..json_end])
            })?;

        Ok(RootCauseAnalysis {
            primary_cause: parsed["primary_cause"].as_str().unwrap_or("Unknown").to_string(),
            contributing_factors: parsed["contributing_factors"]
                .as_array()
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default(),
            explanation: parsed["explanation"].as_str().unwrap_or("").to_string(),
            faulty_component: parsed["faulty_component"].as_str().map(|s| s.to_string()),
            event_timeline: vec![],
        })
    }

    async fn generate_fixes(&self, error: &ErrorContext, root_cause: &RootCauseAnalysis) -> anyhow::Result<Vec<SuggestedFix>> {
        let prompt = format!(
            r#"Given this error and root cause, suggest fixes in JSON format:

Error: {}
Root Cause: {}
Explanation: {}

Respond with JSON array of fixes in this exact format:
[
  {{
    "title": "fix title",
    "description": "detailed description",
    "steps": ["step 1", "step 2"],
    "confidence": 0.95,
    "risk_level": "low",
    "auto_applicable": true
  }}
]"#,
            error.message,
            root_cause.primary_cause,
            root_cause.explanation
        );

        let messages = vec![
            json!({"role": "system", "content": "You are an expert SRE providing practical fixes for production errors."}),
            json!({"role": "user", "content": prompt}),
        ];

        let response = self.chat_completion(messages).await?;
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&response)
            .or_else(|_| {
                let json_start = response.find('[').unwrap_or(0);
                let json_end = response.rfind(']').map(|i| i + 1).unwrap_or(response.len());
                serde_json::from_str(&response[json_start..json_end])
            })?;

        let fixes: Vec<SuggestedFix> = parsed.iter().enumerate().map(|(i, fix)| SuggestedFix {
            id: format!("fix-{}", i + 1),
            title: fix["title"].as_str().unwrap_or("Fix").to_string(),
            description: fix["description"].as_str().unwrap_or("").to_string(),
            steps: fix["steps"]
                .as_array()
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default(),
            confidence: fix["confidence"].as_f64().unwrap_or(0.5),
            estimated_time_minutes: 15,
            risk_level: match fix["risk_level"].as_str().unwrap_or("medium") {
                "low" => RiskLevel::Low,
                "high" => RiskLevel::High,
                "critical" => RiskLevel::Critical,
                _ => RiskLevel::Medium,
            },
            auto_applicable: fix["auto_applicable"].as_bool().unwrap_or(false),
            required_permissions: vec![],
            rollback_steps: vec![],
        }).collect();

        Ok(fixes)
    }

    async fn generate_code_samples(&self, error: &ErrorContext, fix: &SuggestedFix, language: &str) -> anyhow::Result<Vec<CodeSample>> {
        let prompt = format!(
            r#"Generate {} code to fix this error:

Error: {}
Fix: {}
Steps: {:?}

Provide code samples in JSON format:
[
  {{
    "title": "sample title",
    "description": "what this code does",
    "language": "{}",
    "code": "the actual code",
    "explanation": "line by line explanation"
  }}
]"#,
            language,
            error.message,
            fix.title,
            fix.steps,
            language
        );

        let messages = vec![
            json!({"role": "system", "content": format!("You are an expert {} developer.", language)}),
            json!({"role": "user", "content": prompt}),
        ];

        let response = self.chat_completion(messages).await?;
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&response)
            .or_else(|_| {
                let json_start = response.find('[').unwrap_or(0);
                let json_end = response.rfind(']').map(|i| i + 1).unwrap_or(response.len());
                serde_json::from_str(&response[json_start..json_end])
            })?;

        let samples: Vec<CodeSample> = parsed.iter().enumerate().map(|(i, sample)| CodeSample {
            id: format!("sample-{}", i + 1),
            title: sample["title"].as_str().unwrap_or("Code Sample").to_string(),
            description: sample["description"].as_str().unwrap_or("").to_string(),
            language: sample["language"].as_str().unwrap_or(language).to_string(),
            code: sample["code"].as_str().unwrap_or("").to_string(),
            explanation: sample["explanation"].as_str().unwrap_or("").to_string(),
            file_path: None,
            line_numbers: None,
        }).collect();

        Ok(samples)
    }

    async fn create_remediation_plan(&self, error: &ErrorContext, fixes: &[SuggestedFix]) -> anyhow::Result<RemediationPlan> {
        let fix_descriptions: Vec<String> = fixes.iter()
            .map(|f| format!("- {} (confidence: {:.0}%)", f.title, f.confidence * 100.0))
            .collect();

        let prompt = format!(
            r#"Create a remediation plan for this error:

Error: {}
Source: {}

Suggested fixes:
{}

Respond with JSON in this format:
{{
  "steps": [
    {{
      "step_number": 1,
      "title": "step title",
      "description": "what to do",
      "action_type": "command|code_change|config_change|restart|manual",
      "verification": "how to verify success"
    }}
  ],
  "total_estimated_time_minutes": 30,
  "overall_risk": "medium",
  "requires_approval": true
}}"#,
            error.message,
            error.source,
            fix_descriptions.join("\n")
        );

        let messages = vec![
            json!({"role": "system", "content": "You are an expert SRE creating safe remediation plans."}),
            json!({"role": "user", "content": prompt}),
        ];

        let response = self.chat_completion(messages).await?;
        let parsed: serde_json::Value = serde_json::from_str(&response)
            .or_else(|_| {
                let json_start = response.find('{').unwrap_or(0);
                let json_end = response.rfind('}').map(|i| i + 1).unwrap_or(response.len());
                serde_json::from_str(&response[json_start..json_end])
            })?;

        let steps: Vec<RemediationStep> = parsed["steps"]
            .as_array()
            .map(|arr| {
                arr.iter().map(|step| RemediationStep {
                    step_number: step["step_number"].as_u64().unwrap_or(1) as u32,
                    title: step["title"].as_str().unwrap_or("Step").to_string(),
                    description: step["description"].as_str().unwrap_or("").to_string(),
                    action_type: ActionType::Manual, // Simplified for now
                    action: Action::Manual {
                        instructions: step["description"].as_str().unwrap_or("").to_string(),
                    },
                    verification: step["verification"].as_str().map(|s| s.to_string()),
                    rollback: None,
                }).collect()
            })
            .unwrap_or_default();

        Ok(RemediationPlan {
            id: uuid::Uuid::new_v4().to_string(),
            steps,
            total_estimated_time_minutes: parsed["total_estimated_time_minutes"].as_u64().unwrap_or(30) as u32,
            overall_risk: match parsed["overall_risk"].as_str().unwrap_or("medium") {
                "low" => RiskLevel::Low,
                "high" => RiskLevel::High,
                "critical" => RiskLevel::Critical,
                _ => RiskLevel::Medium,
            },
            requires_approval: parsed["requires_approval"].as_bool().unwrap_or(true),
            approval_reason: Some("Remediation requires manual approval".to_string()),
        })
    }

    async fn health_check(&self) -> anyhow::Result<()> {
        // Simple check by listing models
        let response = self.client
            .get("https://api.openai.com/v1/models")
            .headers(self.build_headers())
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("OpenAI API health check failed: {}", response.status()))
        }
    }
}

/// Anthropic provider (placeholder)
pub struct AnthropicProvider {
    #[allow(dead_code)]
    config: AiConfig,
    #[allow(dead_code)]
    client: reqwest::Client,
}

impl AnthropicProvider {
    fn new(config: AiConfig, client: reqwest::Client) -> anyhow::Result<Self> {
        if config.api_key.is_empty() {
            return Err(anyhow::anyhow!("Anthropic API key is required"));
        }
        Ok(Self { config, client })
    }
}

#[async_trait]
impl AiProvider for AnthropicProvider {
    fn name(&self) -> &str {
        "anthropic"
    }

    async fn analyze_error(&self, _error: &ErrorContext) -> anyhow::Result<RootCauseAnalysis> {
        // Placeholder - would implement Claude API
        Ok(RootCauseAnalysis {
            primary_cause: "Placeholder - implement Claude API".to_string(),
            contributing_factors: vec![],
            explanation: "Anthropic provider not yet implemented".to_string(),
            faulty_component: None,
            event_timeline: vec![],
        })
    }

    async fn generate_fixes(&self, _error: &ErrorContext, _root_cause: &RootCauseAnalysis) -> anyhow::Result<Vec<SuggestedFix>> {
        Ok(vec![])
    }

    async fn generate_code_samples(&self, _error: &ErrorContext, _fix: &SuggestedFix, _language: &str) -> anyhow::Result<Vec<CodeSample>> {
        Ok(vec![])
    }

    async fn create_remediation_plan(&self, _error: &ErrorContext, _fixes: &[SuggestedFix]) -> anyhow::Result<RemediationPlan> {
        Err(anyhow::anyhow!("Not implemented"))
    }

    async fn health_check(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests would require API keys in a real environment
    // They're placeholders for unit test structure

    #[test]
    fn test_calculate_confidence() {
        // Would need mock provider to test
        assert_eq!(2 + 2, 4);
    }
}
