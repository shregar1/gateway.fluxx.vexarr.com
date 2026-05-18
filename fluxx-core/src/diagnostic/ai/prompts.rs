//! AI Prompt Templates

/// Prompt for root cause analysis
pub const ROOT_CAUSE_PROMPT: &str = r#"You are an expert Site Reliability Engineer analyzing production errors.

Analyze the following error and provide:
1. Root cause
2. Contributing factors
3. Detailed technical explanation

Error Details:
{error_details}

Respond in JSON format:
{
  "primary_cause": "main cause",
  "contributing_factors": ["factor1", "factor2"],
  "explanation": "detailed explanation",
  "faulty_component": "component name"
}"#;

/// Prompt for generating fixes
pub const FIX_GENERATION_PROMPT: &str = r#"You are an expert software engineer providing production fixes.

Given this error and root cause, suggest practical fixes:

Error: {error_message}
Root Cause: {root_cause}

For each fix, provide:
1. Title
2. Description
3. Step-by-step implementation
4. Confidence level (0-1)
5. Risk level (low/medium/high)

Respond in JSON format:
[
  {
    "title": "fix name",
    "description": "what this fixes",
    "steps": ["step 1", "step 2"],
    "confidence": 0.95,
    "risk": "low"
  }
]"#;

/// Prompt for code generation
pub const CODE_GENERATION_PROMPT: &str = r#"You are an expert {language} developer.

Generate code to fix this error:

Error: {error_message}
Fix: {fix_description}

Provide:
1. Complete, working code
2. Line-by-line explanation
3. Best practices

Respond in JSON format:
{
  "title": "code sample title",
  "description": "what this code does",
  "code": "actual code here",
  "explanation": "line by line explanation"
}"#;

/// Prompt templates struct
pub struct PromptTemplates;

impl PromptTemplates {
    /// Format root cause prompt
    pub fn root_cause(error_details: &str) -> String {
        ROOT_CAUSE_PROMPT.replace("{error_details}", error_details)
    }

    /// Format fix generation prompt
    pub fn fix_generation(error_message: &str, root_cause: &str) -> String {
        FIX_GENERATION_PROMPT
            .replace("{error_message}", error_message)
            .replace("{root_cause}", root_cause)
    }

    /// Format code generation prompt
    pub fn code_generation(language: &str, error_message: &str, fix_description: &str) -> String {
        CODE_GENERATION_PROMPT
            .replace("{language}", language)
            .replace("{error_message}", error_message)
            .replace("{fix_description}", fix_description)
    }
}
