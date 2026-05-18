//! Natural Language Query Engine
//!
//! Converts natural language questions into Fluxx Agent queries.

use crate::integrations::{IntegrationManager, Metric, IntegrationError, IntegrationStatus};
use std::sync::Arc;
use tokio::sync::RwLock;

/// NLP Query Engine
pub struct NlpQueryEngine {
    manager: Arc<RwLock<IntegrationManager>>,
}

/// Parsed query from natural language
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub intent: QueryIntent,
    pub integration: Option<String>,
    pub metric_pattern: Option<String>,
    pub filters: Vec<(String, String)>,
    pub time_range: Option<String>,
}

#[derive(Debug, Clone)]
pub enum QueryIntent {
    GetMetrics,
    CheckHealth,
    GetEvents,
    Diagnose,
    List,
    Unknown,
}

impl NlpQueryEngine {
    /// Create new NLP query engine
    pub fn new(manager: Arc<RwLock<IntegrationManager>>) -> Self {
        Self { manager }
    }

    /// Parse natural language question
    pub fn parse(&self, question: &str) -> ParsedQuery {
        let lower = question.to_lowercase();
        
        // Detect intent
        let intent = if lower.contains("health") 
            || lower.contains("status") 
            || lower.contains("up") 
            || lower.contains("down") 
            || lower.contains("healthy") {
            QueryIntent::CheckHealth
        } else if lower.contains("event") 
            || lower.contains("error") 
            || lower.contains("warning") 
            || lower.contains("alert") {
            QueryIntent::GetEvents
        } else if lower.contains("diagnose") 
            || lower.contains("debug") 
            || lower.contains("why") 
            || lower.contains("problem") {
            QueryIntent::Diagnose
        } else if lower.contains("list") 
            || lower.contains("what") 
            || lower.contains("show me") {
            QueryIntent::GetMetrics
        } else {
            QueryIntent::Unknown
        };
        
        // Detect integration
        let integration = self.detect_integration(&lower);
        
        // Detect metric pattern
        let metric_pattern = self.detect_metric_pattern(&lower);
        
        // Detect filters
        let filters = self.detect_filters(&lower);
        
        // Detect time range
        let time_range = self.detect_time_range(&lower);
        
        ParsedQuery {
            intent,
            integration,
            metric_pattern,
            filters,
            time_range,
        }
    }

    /// Execute parsed query
    pub async fn execute(&self, question: &str) -> Result<QueryResult, IntegrationError> {
        let parsed = self.parse(question);
        let manager = self.manager.read().await;
        
        match parsed.intent {
            QueryIntent::CheckHealth => {
                let health = manager.health_check().await;
                let summary = self.format_health_summary(&health);
                Ok(QueryResult {
                    answer: summary,
                    raw_data: self.serialize_health(&health),
                })
            }
            QueryIntent::GetEvents => {
                let events = manager.collect_events().await;
                let summary = self.format_events_summary(events);
                Ok(QueryResult {
                    answer: summary,
                    raw_data: serde_json::json!({}),
                })
            }
            QueryIntent::GetMetrics => {
                let metrics = manager.collect_metrics().await;
                let filtered: Vec<_> = metrics
                    .into_iter()
                    .filter(|m| {
                        if let Some(ref integration) = parsed.integration {
                            m.name.starts_with(integration)
                        } else {
                            true
                        }
                    })
                    .collect();
                let summary = self.format_metrics_summary(filtered, parsed.integration);
                Ok(QueryResult {
                    answer: summary,
                    raw_data: serde_json::json!({}),
                })
            }
            QueryIntent::List => {
                let names = manager.integration_names();
                Ok(QueryResult {
                    answer: format!("Configured integrations: {}", names.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")),
                    raw_data: serde_json::json!(names.iter().map(|s| s.to_string()).collect::<Vec<String>>()),
                })
            }
            _ => Ok(QueryResult {
                answer: "I'm not sure what you're asking. Try asking about health, metrics, events, or specific integrations.".to_string(),
                raw_data: serde_json::json!({}),
            }),
        }
    }

    /// Detect integration from question
    fn detect_integration(&self, question: &str) -> Option<String> {
        let integrations = vec![
            ("postgres", "postgresql"),
            ("mysql", "mysql"),
            ("redis", "redis"),
            ("mongo", "mongodb"),
            ("cassandra", "cassandra"),
            ("elasticsearch", "elasticsearch"),
            ("clickhouse", "clickhouse"),
            ("neo4j", "neo4j"),
            ("influx", "influxdb"),
            ("timescale", "timescaledb"),
            ("cockroach", "cockroachdb"),
            ("aws", "aws"),
            ("azure", "azure"),
            ("gcp", "gcp"),
            ("google cloud", "gcp"),
            ("digitalocean", "digitalocean"),
            ("do", "digitalocean"),
            ("linode", "linode"),
            ("ibm", "ibm"),
            ("oracle", "oracle"),
            ("alibaba", "alibaba"),
            ("docker", "docker"),
            ("kubernetes", "kubernetes"),
            ("k8s", "kubernetes"),
            ("kafka", "kafka"),
            ("rabbitmq", "rabbitmq"),
            ("sqs", "sqs"),
            ("nats", "nats"),
            ("pulsar", "pulsar"),
            ("activemq", "activemq"),
            ("github", "github_actions"),
            ("gitlab", "gitlab_ci"),
            ("jenkins", "jenkins"),
            ("circleci", "circleci"),
            ("travis", "travisci"),
            ("teamcity", "teamcity"),
            ("bamboo", "bamboo"),
            ("memcached", "memcached"),
            ("varnish", "varnish"),
            ("prometheus", "prometheus"),
            ("grafana", "grafana"),
            ("jaeger", "jaeger"),
            ("zipkin", "zipkin"),
            ("datadog", "datadog"),
            ("newrelic", "newrelic"),
            ("vault", "vault"),
            ("ldap", "ldap"),
        ];
        
        for (keyword, integration) in integrations {
            if question.contains(keyword) {
                return Some(integration.to_string());
            }
        }
        None
    }

    /// Detect metric pattern
    fn detect_metric_pattern(&self, question: &str) -> Option<String> {
        let patterns = vec![
            ("connection", "connections"),
            ("memory", "memory"),
            ("cpu", "cpu"),
            ("disk", "disk"),
            ("query", "query"),
            ("latency", "latency"),
            ("error", "error"),
            ("queue", "queue"),
            ("pod", "pod"),
            ("node", "node"),
        ];
        
        for (keyword, pattern) in patterns {
            if question.contains(keyword) {
                return Some(pattern.to_string());
            }
        }
        None
    }

    /// Detect filters
    fn detect_filters(&self, _question: &str) -> Vec<(String, String)> {
        // Simplified - would use more sophisticated NLP in production
        vec![]
    }

    /// Detect time range
    fn detect_time_range(&self, question: &str) -> Option<String> {
        if question.contains("hour") || question.contains("1h") {
            Some("1h".to_string())
        } else if question.contains("day") || question.contains("24h") {
            Some("1d".to_string())
        } else if question.contains("week") {
            Some("1w".to_string())
        } else {
            None
        }
    }

    /// Format health summary
    fn format_health_summary(&self, health: &std::collections::HashMap<String, IntegrationStatus>) -> String {
        let total = health.len();
        let healthy = health.values()
            .filter(|s| format!("{:?}", s).contains("Healthy"))
            .count();
        
        let mut response = format!("📊 **Health Status**: {}/{} integrations healthy\n\n", healthy, total);
        
        for (name, status) in health {
            let emoji = if format!("{:?}", status).contains("Healthy") {
                "✅"
            } else {
                "❌"
            };
            response.push_str(&format!("{} **{}**: {:?}\n", emoji, name, status));
        }
        
        response
    }

    /// Format events summary
    fn format_events_summary(&self, events: Vec<crate::integrations::Event>) -> String {
        if events.is_empty() {
            return "✅ No recent events.".to_string();
        }
        
        let mut response = format!("🚨 **Recent Events** ({} total)\n\n", events.len());
        
        for event in events.iter().take(10) {
            let emoji = match event.severity {
                crate::integrations::EventSeverity::Critical => "🔴",
                crate::integrations::EventSeverity::Error => "❌",
                crate::integrations::EventSeverity::Warning => "⚠️",
                crate::integrations::EventSeverity::Info => "ℹ️",
            };
            response.push_str(&format!("{} **{}**: {}\n", emoji, event.title, event.description));
        }
        
        response
    }

    /// Serialize health data to JSON
    fn serialize_health(&self, health: &std::collections::HashMap<String, IntegrationStatus>) -> serde_json::Value {
        let serialized: std::collections::HashMap<String, String> = health
            .iter()
            .map(|(k, v)| (k.clone(), format!("{:?}", v)))
            .collect();
        serde_json::json!(serialized)
    }

    /// Format metrics summary
    fn format_metrics_summary(&self, metrics: Vec<Metric>, integration: Option<String>) -> String {
        let name = integration.unwrap_or_else(|| "all integrations".to_string());
        
        if metrics.is_empty() {
            return format!("No metrics found for {}", name);
        }
        
        let mut response = format!("📈 **Metrics** for {} ({} total)\n\n", name, metrics.len());
        
        // Group by integration
        let mut by_integration: std::collections::HashMap<String, Vec<&Metric>> = std::collections::HashMap::new();
        for metric in &metrics {
            let integ = metric.name.split('.').next().unwrap_or("unknown").to_string();
            by_integration.entry(integ).or_default().push(metric);
        }
        
        for (integration, metrics) in by_integration.iter().take(5) {
            response.push_str(&format!("**{}**:\n", integration));
            for metric in metrics.iter().take(5) {
                response.push_str(&format!("  - {}: {:?}\n", metric.name, metric.value));
            }
            response.push('\n');
        }
        
        response
    }
}

/// Query result
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub answer: String,
    pub raw_data: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_health_intent() {
        let engine = NlpQueryEngine::new(Arc::new(RwLock::new(IntegrationManager::new())));
        
        let queries = vec![
            ("Is everything healthy?", QueryIntent::CheckHealth),
            ("What's the status?", QueryIntent::CheckHealth),
            ("Is postgres up?", QueryIntent::CheckHealth),
            ("Show me redis metrics", QueryIntent::GetMetrics),
            ("Any errors?", QueryIntent::GetEvents),
        ];
        
        for (question, expected_intent) in queries {
            let parsed = engine.parse(question);
            assert!(matches!(parsed.intent, expected_intent), "Failed for: {}", question);
        }
    }
    
    #[test]
    fn test_detect_integration() {
        let engine = NlpQueryEngine::new(Arc::new(RwLock::new(IntegrationManager::new())));
        
        assert_eq!(engine.detect_integration("postgres status"), Some("postgresql".to_string()));
        assert_eq!(engine.detect_integration("redis memory"), Some("redis".to_string()));
        assert_eq!(engine.detect_integration("aws instances"), Some("aws".to_string()));
        assert_eq!(engine.detect_integration("kubernetes pods"), Some("kubernetes".to_string()));
    }
}
