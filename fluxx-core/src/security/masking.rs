use regex::Regex;
use std::sync::Arc;

/// MaskingRule: Defines a specific pattern for PII identification and redaction.
#[derive(Debug, Clone)]
pub struct MaskingRule {
    pub name: String,
    pub pattern: Regex,
    pub replacement: String,
}

/// MaskingEngine: High-performance rules engine for identifying and redacting 
/// sensitive information (PII) from network payloads.
#[derive(Debug, Clone)]
pub struct MaskingEngine {
    rules: Vec<Arc<MaskingRule>>,
}

impl MaskingEngine {
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
        }
    }

    /// Default rules for common PII (Emails, Credit Cards, Auth Headers).
    pub fn default_rules() -> Self {
        let mut engine = Self::new();
        engine.add_rule("Email", r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", "[EMAIL_REDACTED]");
        engine.add_rule("CreditCard", r"\b(?:\d[ -]*?){13,16}\b", "[CC_REDACTED]");
        engine.add_rule("AuthHeader", r"(?i)Authorization: Bearer [A-Za-z0-9._~+/-]+", "Authorization: Bearer [TOKEN_REDACTED]");
        engine
    }

    pub fn add_rule(&mut self, name: &str, pattern: &str, replacement: &str) {
        if let Ok(reg) = Regex::new(pattern) {
             self.rules.push(Arc::new(MaskingRule {
                name: name.to_string(),
                pattern: reg,
                replacement: replacement.to_string(),
            }));
        }
    }

    /// Applies all configured rules to the event payload.
    pub fn apply(&self, payload: String) -> String {
        let mut sanitized = payload;
        for rule in &self.rules {
            sanitized = rule.pattern.replace_all(&sanitized, &rule.replacement).to_string();
        }
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 78: MaskingEngine new
    #[test]
    fn test_masking_engine_new() {
        let engine = MaskingEngine::new();
        assert!(engine.rules.is_empty());
    }

    // Test 79: MaskingEngine default rules
    #[test]
    fn test_masking_engine_default_rules() {
        let engine = MaskingEngine::default_rules();
        assert_eq!(engine.rules.len(), 3);
    }

    // Test 80: Add rule success
    #[test]
    fn test_masking_add_rule() {
        let mut engine = MaskingEngine::new();
        engine.add_rule("Test", r"test\d+", "[REDACTED]");
        assert_eq!(engine.rules.len(), 1);
    }

    // Test 81: Add invalid rule (should not panic)
    #[test]
    fn test_masking_add_invalid_rule() {
        let mut engine = MaskingEngine::new();
        engine.add_rule("Invalid", r"[invalid", "[REDACTED]");
        assert!(engine.rules.is_empty());
    }

    // Test 82: Apply email masking
    #[test]
    fn test_masking_email() {
        let engine = MaskingEngine::default_rules();
        let result = engine.apply("Contact: user@example.com".to_string());
        assert_eq!(result, "Contact: [EMAIL_REDACTED]");
    }

    // Test 83: Apply credit card masking
    #[test]
    fn test_masking_credit_card() {
        let engine = MaskingEngine::default_rules();
        let result = engine.apply("Card: 4111 1111 1111 1111".to_string());
        assert_eq!(result, "Card: [CC_REDACTED]");
    }

    // Test 84: Apply auth header masking
    #[test]
    fn test_masking_auth_header() {
        let engine = MaskingEngine::default_rules();
        let result = engine.apply("Authorization: Bearer abc123xyz".to_string());
        assert_eq!(result, "Authorization: Bearer [TOKEN_REDACTED]");
    }

    // Test 85: Apply multiple masks
    #[test]
    fn test_masking_multiple() {
        let engine = MaskingEngine::default_rules();
        let input = "Email: test@example.com, Card: 4111111111111111".to_string();
        let result = engine.apply(input);
        assert!(result.contains("[EMAIL_REDACTED]"));
        assert!(result.contains("[CC_REDACTED]"));
    }

    // Test 86: No match returns original
    #[test]
    fn test_masking_no_match() {
        let engine = MaskingEngine::default_rules();
        let input = "Hello World".to_string();
        let result = engine.apply(input.clone());
        assert_eq!(result, input);
    }

    // Test 87: Custom rule application
    #[test]
    fn test_masking_custom_rule() {
        let mut engine = MaskingEngine::new();
        engine.add_rule("SSN", r"\d{3}-\d{2}-\d{4}", "[SSN_REDACTED]");
        let result = engine.apply("SSN: 123-45-6789".to_string());
        assert_eq!(result, "SSN: [SSN_REDACTED]");
    }

    // Test 88: Empty payload
    #[test]
    fn test_masking_empty() {
        let engine = MaskingEngine::default_rules();
        let result = engine.apply("".to_string());
        assert_eq!(result, "");
    }

    // Test 89: MaskingRule debug format
    #[test]
    fn test_masking_rule_debug() {
        let engine = MaskingEngine::default_rules();
        let debug_str = format!("{:?}", engine);
        assert!(debug_str.contains("MaskingEngine"));
    }

    // Test 90: Multiple rules order
    #[test]
    fn test_masking_rule_order() {
        let mut engine = MaskingEngine::new();
        engine.add_rule("First", r"foo", "bar");
        engine.add_rule("Second", r"bar", "baz");
        let result = engine.apply("foo".to_string());
        // First rule changes foo->bar, second changes bar->baz
        assert_eq!(result, "baz");
    }
}
