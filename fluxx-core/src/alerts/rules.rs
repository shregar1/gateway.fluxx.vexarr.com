//! Alert Rule Engine
//!
//! Evaluate alert conditions against incoming data.
//!
//! ## Condition Syntax
//!
//! Conditions use simple expression syntax:
//!
//! ```
//! # Comparisons
//! error_rate > 0.05
//! cpu_percent >= 90
//! memory_used < 100MB
//! 
//! # Logical operators
//! error_rate > 0.05 AND cpu_percent > 80
//! status_code == 500 OR status_code == 503
//! 
//! # Functions
//! avg(cpu_percent) > 80
//! p99(latency) > 500
//! count(errors) > 100
//! ```

use crate::alerts::config::{AlertRuleConfig, AlertSeverity};
use crate::alerts::models::Alert;
use std::collections::HashMap;

/// Rule engine for evaluating alert conditions
pub struct RuleEngine {
    rules: Vec<AlertRuleConfig>,
}

/// Metric value types
#[derive(Debug, Clone)]
pub enum MetricValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

impl MetricValue {
    /// Convert to f64 for comparisons
    pub fn as_f64(&self) -> f64 {
        match self {
            MetricValue::Integer(i) => *i as f64,
            MetricValue::Float(f) => *f,
            MetricValue::Boolean(b) => if *b { 1.0 } else { 0.0 },
            MetricValue::String(s) => s.parse().unwrap_or(0.0),
        }
    }

    /// Convert to string
    pub fn as_string(&self) -> String {
        match self {
            MetricValue::Integer(i) => i.to_string(),
            MetricValue::Float(f) => f.to_string(),
            MetricValue::String(s) => s.clone(),
            MetricValue::Boolean(b) => b.to_string(),
        }
    }
}

impl RuleEngine {
    /// Create new rule engine
    pub fn new(rules: Vec<AlertRuleConfig>) -> Self {
        Self { rules }
    }

    /// Evaluate all rules against current metrics
    pub fn evaluate(&self, metrics: &HashMap<String, MetricValue>) -> Vec<(AlertRuleConfig, Alert)> {
        let mut triggered = vec![];

        for rule in &self.rules {
            if let Some(alert) = self.evaluate_rule(rule, metrics) {
                triggered.push((rule.clone(), alert));
            }
        }

        triggered
    }

    /// Evaluate a single rule
    fn evaluate_rule(&self, rule: &AlertRuleConfig, metrics: &HashMap<String, MetricValue>) -> Option<Alert> {
        if self.evaluate_condition(&rule.condition, metrics) {
            let message = format!("{}: Condition '{}' triggered", 
                rule.name, 
                rule.condition
            );
            
            Some(Alert::new(
                rule.severity.clone(),
                message,
                "rule-engine",
            ))
        } else {
            None
        }
    }

    /// Evaluate a condition expression
    fn evaluate_condition(&self, condition: &str, metrics: &HashMap<String, MetricValue>) -> bool {
        // Simple expression parser
        // Handles: metric > value, metric >= value, metric < value, metric <= value, metric == value
        
        let condition = condition.trim();
        
        // Handle AND/OR
        if condition.contains(" AND ") {
            let parts: Vec<&str> = condition.split(" AND ").collect();
            return parts.iter().all(|p| self.evaluate_condition(p, metrics));
        }
        
        if condition.contains(" OR ") {
            let parts: Vec<&str> = condition.split(" OR ").collect();
            return parts.iter().any(|p| self.evaluate_condition(p, metrics));
        }
        
        // Parse comparison
        let operators = [">=", "<=", "==", "!=", ">", "<"];
        
        for op in &operators {
            if let Some(pos) = condition.find(op) {
                let metric_name = condition[..pos].trim();
                let expected_value = condition[pos + op.len()..].trim();
                
                let actual_value = metrics.get(metric_name);
                
                return match actual_value {
                    Some(actual) => self.compare(actual, op, expected_value),
                    None => {
                        tracing::debug!(metric = %metric_name, "Metric not found for condition evaluation");
                        false
                    }
                };
            }
        }
        
        tracing::warn!(condition = %condition, "Could not parse condition");
        false
    }

    /// Compare a metric value against an expected value
    fn compare(&self, actual: &MetricValue, op: &str, expected: &str) -> bool {
        // Parse expected value
        let expected_num = self.parse_number(expected);
        let actual_num = actual.as_f64();
        
        match op {
            ">" => actual_num > expected_num,
            ">=" => actual_num >= expected_num,
            "<" => actual_num < expected_num,
            "<=" => actual_num <= expected_num,
            "==" => {
                // Try numeric comparison first, then string
                if let Ok(exp) = expected.parse::<f64>() {
                    (actual_num - exp).abs() < f64::EPSILON
                } else {
                    actual.as_string() == expected
                }
            }
            "!=" => {
                if let Ok(exp) = expected.parse::<f64>() {
                    (actual_num - exp).abs() >= f64::EPSILON
                } else {
                    actual.as_string() != expected
                }
            }
            _ => false,
        }
    }

    /// Parse a number from string (handles units like MB, GB, etc.)
    fn parse_number(&self, s: &str) -> f64 {
        let s = s.trim();
        
        // Handle units
        if s.ends_with("GB") || s.ends_with("gb") {
            s[..s.len()-2].trim().parse::<f64>().map(|v| v * 1e9).unwrap_or(0.0)
        } else if s.ends_with("MB") || s.ends_with("mb") {
            s[..s.len()-2].trim().parse::<f64>().map(|v| v * 1e6).unwrap_or(0.0)
        } else if s.ends_with("KB") || s.ends_with("kb") {
            s[..s.len()-2].trim().parse::<f64>().map(|v| v * 1e3).unwrap_or(0.0)
        } else if s.ends_with("ms") {
            s[..s.len()-2].trim().parse::<f64>().unwrap_or(0.0)
        } else if s.ends_with("s") && !s.contains('.') {
            // Seconds (but avoid matching scientific notation)
            s[..s.len()-1].trim().parse::<f64>().map(|v| v * 1000.0).unwrap_or(0.0)
        } else {
            s.parse::<f64>().unwrap_or(0.0)
        }
    }
}

/// Predefined rule templates
pub mod templates {
    use super::*;

    /// High error rate rule
    pub fn high_error_rate(threshold: f64, channels: Vec<String>) -> AlertRuleConfig {
        AlertRuleConfig {
            name: "high_error_rate".to_string(),
            description: format!("Error rate exceeds {:.0}%", threshold * 100.0),
            condition: format!("error_rate > {}", threshold),
            severity: AlertSeverity::Critical,
            channels,
            labels: [("category".to_string(), "reliability".to_string())]
                .into_iter()
                .collect(),
            cooldown_seconds: Some(300),
        }
    }

    /// High latency rule
    pub fn high_latency(p99_threshold_ms: f64, channels: Vec<String>) -> AlertRuleConfig {
        AlertRuleConfig {
            name: "high_latency".to_string(),
            description: format!("P99 latency exceeds {:.0}ms", p99_threshold_ms),
            condition: format!("latency_p99 > {}", p99_threshold_ms),
            severity: AlertSeverity::Warning,
            channels,
            labels: [("category".to_string(), "performance".to_string())]
                .into_iter()
                .collect(),
            cooldown_seconds: Some(300),
        }
    }

    /// Low disk space rule
    pub fn low_disk_space(free_percent: f64, channels: Vec<String>) -> AlertRuleConfig {
        AlertRuleConfig {
            name: "low_disk_space".to_string(),
            description: format!("Disk space below {:.0}%", free_percent),
            condition: format!("disk_free_percent < {}", free_percent),
            severity: AlertSeverity::Warning,
            channels,
            labels: [("category".to_string(), "infrastructure".to_string())]
                .into_iter()
                .collect(),
            cooldown_seconds: Some(600),
        }
    }

    /// High CPU usage rule
    pub fn high_cpu(percent: f64, channels: Vec<String>) -> AlertRuleConfig {
        AlertRuleConfig {
            name: "high_cpu".to_string(),
            description: format!("CPU usage above {:.0}%", percent),
            condition: format!("cpu_percent > {}", percent),
            severity: AlertSeverity::Warning,
            channels,
            labels: [("category".to_string(), "infrastructure".to_string())]
                .into_iter()
                .collect(),
            cooldown_seconds: Some(300),
        }
    }

    /// High memory usage rule
    pub fn high_memory(percent: f64, channels: Vec<String>) -> AlertRuleConfig {
        AlertRuleConfig {
            name: "high_memory".to_string(),
            description: format!("Memory usage above {:.0}%", percent),
            condition: format!("memory_percent > {}", percent),
            severity: AlertSeverity::Warning,
            channels,
            labels: [("category".to_string(), "infrastructure".to_string())]
                .into_iter()
                .collect(),
            cooldown_seconds: Some(300),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_value_as_f64() {
        assert_eq!(MetricValue::Integer(42).as_f64(), 42.0);
        assert_eq!(MetricValue::Float(3.14).as_f64(), 3.14);
        assert_eq!(MetricValue::Boolean(true).as_f64(), 1.0);
        assert_eq!(MetricValue::Boolean(false).as_f64(), 0.0);
        assert_eq!(MetricValue::String("100".to_string()).as_f64(), 100.0);
    }

    #[test]
    fn test_simple_comparison() {
        let engine = RuleEngine::new(vec![]);
        let mut metrics = HashMap::new();
        metrics.insert("cpu".to_string(), MetricValue::Integer(85));

        assert!(engine.evaluate_condition("cpu > 80", &metrics));
        assert!(!engine.evaluate_condition("cpu > 90", &metrics));
        assert!(engine.evaluate_condition("cpu >= 85", &metrics));
        assert!(engine.evaluate_condition("cpu < 90", &metrics));
    }

    #[test]
    fn test_logical_operators() {
        let engine = RuleEngine::new(vec![]);
        let mut metrics = HashMap::new();
        metrics.insert("cpu".to_string(), MetricValue::Integer(85));
        metrics.insert("memory".to_string(), MetricValue::Integer(90));

        assert!(engine.evaluate_condition("cpu > 80 AND memory > 80", &metrics));
        assert!(!engine.evaluate_condition("cpu > 90 AND memory > 80", &metrics));
        assert!(engine.evaluate_condition("cpu > 90 OR memory > 80", &metrics));
    }

    #[test]
    fn test_unit_parsing() {
        let engine = RuleEngine::new(vec![]);
        
        assert_eq!(engine.parse_number("100"), 100.0);
        assert_eq!(engine.parse_number("10MB"), 10_000_000.0);
        assert_eq!(engine.parse_number("1GB"), 1_000_000_000.0);
        assert_eq!(engine.parse_number("500ms"), 500.0);
        assert_eq!(engine.parse_number("5s"), 5000.0);
    }

    #[test]
    fn test_rule_evaluation() {
        let rule = AlertRuleConfig {
            name: "test_rule".to_string(),
            description: "Test".to_string(),
            condition: "error_rate > 0.05".to_string(),
            severity: AlertSeverity::Critical,
            channels: vec![],
            labels: HashMap::new(),
            cooldown_seconds: None,
        };

        let engine = RuleEngine::new(vec![rule.clone()]);
        
        // Should trigger
        let mut metrics_high = HashMap::new();
        metrics_high.insert("error_rate".to_string(), MetricValue::Float(0.1));
        
        let results = engine.evaluate(&metrics_high);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.name, "test_rule");

        // Should not trigger
        let mut metrics_low = HashMap::new();
        metrics_low.insert("error_rate".to_string(), MetricValue::Float(0.01));
        
        let results = engine.evaluate(&metrics_low);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_templates() {
        let rule = templates::high_error_rate(0.05, vec!["slack".to_string()]);
        assert_eq!(rule.name, "high_error_rate");
        assert!(rule.condition.contains("0.05"));
        assert_eq!(rule.severity, AlertSeverity::Critical);
    }
}
