//! Model Context Protocol (MCP) Server for AI Assistants
//!
//! Implements MCP protocol for Claude Desktop and other MCP-compatible assistants.

use crate::integrations::{IntegrationManager, Metric, Event, EventSeverity};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error};

/// MCP Server configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct McpConfig {
    pub enabled: bool,
    pub max_context_metrics: usize,
    pub max_context_events: usize,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_context_metrics: 100,
            max_context_events: 50,
        }
    }
}

/// MCP Server for Fluxx Agent
pub struct FluxxMcpServer {
    manager: Arc<RwLock<IntegrationManager>>,
    config: McpConfig,
}

/// MCP Tool definition
#[derive(Serialize, Deserialize, Clone)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// Tool call request
#[derive(Serialize, Deserialize)]
pub struct ToolCall {
    pub name: String,
    pub arguments: serde_json::Value,
}

/// Tool call response
#[derive(Serialize, Deserialize)]
pub struct ToolResponse {
    pub content: Vec<ToolContent>,
}

#[derive(Serialize, Deserialize)]
pub struct ToolContent {
    #[serde(rename = "type")]
    pub content_type: String,
    pub text: String,
}

impl FluxxMcpServer {
    /// Create new MCP server
    pub fn new(manager: Arc<RwLock<IntegrationManager>>, config: McpConfig) -> Self {
        Self { manager, config }
    }

    /// Get available tools
    pub fn get_tools(&self) -> Vec<Tool> {
        vec![
            Tool {
                name: "fluxx_query_metrics".to_string(),
                description: "Query infrastructure metrics from Fluxx Agent. Use this when asked about performance, usage, or current state of any service.".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "integration": {
                            "type": "string",
                            "description": "Integration name to filter by (e.g., 'postgresql', 'redis', 'kubernetes', 'aws'). Use 'all' for everything."
                        },
                        "metric_pattern": {
                            "type": "string",
                            "description": "Optional: filter metrics by name pattern (e.g., 'connections', 'memory', 'cpu')"
                        }
                    }
                }),
            },
            Tool {
                name: "fluxx_health_check".to_string(),
                description: "Check health status of integrations. Use this when asked if services are healthy, up, or experiencing issues.".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "integration": {
                            "type": "string",
                            "description": "Optional: specific integration to check. If not provided, checks all integrations."
                        }
                    }
                }),
            },
            Tool {
                name: "fluxx_get_events".to_string(),
                description: "Get recent infrastructure events and alerts. Use this when asked about errors, warnings, or recent issues.".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "severity": {
                            "type": "string",
                            "enum": ["info", "warning", "error", "critical", "all"],
                            "description": "Filter events by severity level. Default is 'all'."
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of events to return (1-50). Default is 10.",
                            "default": 10
                        }
                    }
                }),
            },
            Tool {
                name: "fluxx_list_integrations".to_string(),
                description: "List all configured integrations. Use this to see what services are being monitored.".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {}
                }),
            },
            Tool {
                name: "fluxx_diagnose".to_string(),
                description: "Run diagnostics on a specific service. Use this when investigating problems with a particular service.".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "service": {
                            "type": "string",
                            "description": "Service name to diagnose (e.g., 'postgresql', 'redis', 'kubernetes')"
                        },
                        "symptom": {
                            "type": "string",
                            "description": "Description of the observed issue or symptom"
                        }
                    },
                    "required": ["service"]
                }),
            },
        ]
    }

    /// Execute a tool call
    pub async fn execute_tool(&self, call: ToolCall) -> Result<ToolResponse, String> {
        let result = match call.name.as_str() {
            "fluxx_query_metrics" => self.handle_query_metrics(call.arguments).await,
            "fluxx_health_check" => self.handle_health_check(call.arguments).await,
            "fluxx_get_events" => self.handle_get_events(call.arguments).await,
            "fluxx_list_integrations" => self.handle_list_integrations().await,
            "fluxx_diagnose" => self.handle_diagnose(call.arguments).await,
            _ => Err(format!("Unknown tool: {}", call.name)),
        };

        match result {
            Ok(data) => Ok(ToolResponse {
                content: vec![ToolContent {
                    content_type: "text".to_string(),
                    text: serde_json::to_string_pretty(&data).unwrap_or_default(),
                }],
            }),
            Err(e) => Ok(ToolResponse {
                content: vec![ToolContent {
                    content_type: "text".to_string(),
                    text: format!("Error: {}", e),
                }],
            }),
        }
    }

    /// Handle metrics query
    async fn handle_query_metrics(&self, args: serde_json::Value) -> Result<serde_json::Value, String> {
        let manager = self.manager.read().await;
        
        let integration_filter = args.get("integration").and_then(|v| v.as_str());
        let metric_pattern = args.get("metric_pattern").and_then(|v| v.as_str());
        
        let all_metrics = manager.collect_metrics().await;
        
        let filtered: Vec<Metric> = all_metrics
            .into_iter()
            .filter(|m| {
                if let Some(integration) = integration_filter {
                    if integration != "all" && !m.name.starts_with(integration) {
                        return false;
                    }
                }
                if let Some(pattern) = metric_pattern {
                    if !m.name.contains(pattern) {
                        return false;
                    }
                }
                true
            })
            .take(self.config.max_context_metrics)
            .collect();
        
        // Group by integration for better readability
        let mut by_integration: std::collections::HashMap<String, Vec<serde_json::Value>> = std::collections::HashMap::new();
        for metric in filtered {
            let integration = metric.name.split('.').next().unwrap_or("unknown").to_string();
            let metric_json = serde_json::json!({
                "name": metric.name,
                "value": format!("{:?}", metric.value),
                "timestamp": metric.timestamp.elapsed().ok().map(|d| d.as_secs_f64()).unwrap_or(0.0),
            });
            by_integration.entry(integration).or_default().push(metric_json);
        }
        
        Ok(serde_json::json!({
            "metrics_by_integration": by_integration,
            "total_metrics": by_integration.values().map(|v| v.len()).sum::<usize>(),
            "integrations": by_integration.keys().collect::<Vec<_>>(),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }))
    }

    /// Handle health check
    async fn handle_health_check(&self, args: serde_json::Value) -> Result<serde_json::Value, String> {
        let manager = self.manager.read().await;
        let health = manager.health_check().await;
        
        if let Some(integration) = args.get("integration").and_then(|v| v.as_str()) {
            if let Some(status) = health.get(integration) {
                let status_str = format!("{:?}", status);
                let is_healthy = status_str.contains("Healthy");
                return Ok(serde_json::json!({
                    "integration": integration,
                    "status": status_str,
                    "healthy": is_healthy,
                }));
            } else {
                return Err(format!("Integration '{}' not found. Available: {:?}", 
                    integration, 
                    health.keys().collect::<Vec<_>>()
                ));
            }
        }
        
        let health_summary: Vec<serde_json::Value> = health
            .iter()
            .map(|(name, status)| {
                let status_str = format!("{:?}", status);
                serde_json::json!({
                    "integration": name,
                    "status": status_str,
                    "healthy": status_str.contains("Healthy"),
                })
            })
            .collect();
        
        let healthy_count = health_summary.iter()
            .filter(|v| v.get("healthy").and_then(|v| v.as_bool()).unwrap_or(false))
            .count();
        
        Ok(serde_json::json!({
            "integrations": health_summary,
            "summary": {
                "total": health_summary.len(),
                "healthy": healthy_count,
                "unhealthy": health_summary.len() - healthy_count,
            },
            "overall_status": if healthy_count == health_summary.len() { 
                "✅ All systems healthy" 
            } else { 
                "⚠️ Some systems unhealthy" 
            },
        }))
    }

    /// Handle get events
    async fn handle_get_events(&self, args: serde_json::Value) -> Result<serde_json::Value, String> {
        let manager = self.manager.read().await;
        
        let severity = args.get("severity").and_then(|v| v.as_str()).unwrap_or("all");
        let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
        
        let all_events = manager.collect_events().await;
        
        let filtered: Vec<Event> = all_events
            .into_iter()
            .filter(|e| {
                if severity == "all" {
                    true
                } else {
                    format!("{:?}", e.severity).to_lowercase() == severity.to_lowercase()
                }
            })
            .take(limit.min(self.config.max_context_events))
            .collect();
        
        let events_formatted: Vec<serde_json::Value> = filtered
            .into_iter()
            .map(|e| serde_json::json!({
                "title": e.title,
                "description": e.description,
                "severity": format!("{:?}", e.severity),
                "source": e.source,
                "timestamp": e.timestamp.duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
            }))
            .collect();
        
        let critical_count = events_formatted.iter()
            .filter(|e| e.get("severity").and_then(|s| s.as_str()) == Some("Critical"))
            .count();
        
        Ok(serde_json::json!({
            "events": events_formatted,
            "count": events_formatted.len(),
            "critical_count": critical_count,
            "severity_filter": severity,
        }))
    }

    /// Handle list integrations
    async fn handle_list_integrations(&self) -> Result<serde_json::Value, String> {
        let manager = self.manager.read().await;
        let names: Vec<String> = manager.integration_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        
        Ok(serde_json::json!({
            "integrations": names,
            "count": names.len(),
            "categories": self.categorize_integrations(&names),
        }))
    }

    /// Handle diagnose
    async fn handle_diagnose(&self, args: serde_json::Value) -> Result<serde_json::Value, String> {
        let service = args.get("service")
            .and_then(|v| v.as_str())
            .ok_or("Service name required")?;
        
        let symptom = args.get("symptom").and_then(|v| v.as_str()).unwrap_or("unknown issue");
        
        let manager = self.manager.read().await;
        let health = manager.health_check().await;
        
        let service_health = health.get(service)
            .map(|s| format!("{:?}", s))
            .unwrap_or_else(|| "unknown".to_string());
        
        let events = manager.collect_events().await;
        let service_events: Vec<_> = events
            .into_iter()
            .filter(|e| e.source == service || e.source.contains(service))
            .take(5)
            .collect();
        
        let is_healthy = service_health.contains("Healthy");
        
        Ok(serde_json::json!({
            "service": service,
            "symptom": symptom,
            "health_status": service_health,
            "healthy": is_healthy,
            "recent_events": service_events.iter().map(|e| serde_json::json!({
                "title": e.title,
                "description": e.description,
                "severity": format!("{:?}", e.severity),
            })).collect::<Vec<_>>(),
            "recommendations": self.generate_recommendations(service, is_healthy, &service_events),
        }))
    }

    /// Categorize integrations by type
    fn categorize_integrations(&self, names: &[String]) -> serde_json::Value {
        let mut categories = serde_json::Map::new();
        
        categories.insert("databases".to_string(), 
            names.iter().filter(|n| 
                ["postgres", "mysql", "redis", "mongodb", "cassandra", "elasticsearch", 
                 "clickhouse", "neo4j", "influxdb", "timescaledb", "cockroachdb"]
                .iter().any(|db| n.contains(db))
            ).cloned().collect::<serde_json::Value>()
        );
        
        categories.insert("cloud".to_string(),
            names.iter().filter(|n|
                ["aws", "azure", "gcp", "digitalocean", "linode", "ibm", "oracle", "alibaba"]
                .iter().any(|c| n.contains(c))
            ).cloned().collect::<serde_json::Value>()
        );
        
        categories.insert("containers".to_string(),
            names.iter().filter(|n| n.contains("docker") || n.contains("kubernetes"))
                .cloned().collect::<serde_json::Value>()
        );
        
        categories.insert("queues".to_string(),
            names.iter().filter(|n|
                ["kafka", "rabbitmq", "sqs", "nats", "pulsar", "activemq"]
                .iter().any(|q| n.contains(q))
            ).cloned().collect::<serde_json::Value>()
        );
        
        serde_json::Value::Object(categories)
    }

    /// Generate recommendations
    fn generate_recommendations(&self, service: &str, is_healthy: bool, events: &[Event]) -> Vec<String> {
        let mut recommendations = vec![];
        
        if !is_healthy {
            recommendations.push(format!("🔍 Check {} logs for error messages", service));
            recommendations.push(format!("🔧 Verify {} configuration and credentials", service));
            recommendations.push(format!("🌐 Check network connectivity to {}", service));
            
            if service.contains("postgres") || service.contains("mysql") {
                recommendations.push("💾 Check database connection pool settings".to_string());
            }
            
            if service.contains("redis") || service.contains("memcached") {
                recommendations.push("🧠 Check memory usage and eviction policies".to_string());
            }
        } else {
            recommendations.push(format!("✅ {} appears healthy", service));
        }
        
        // Add event-specific recommendations
        for event in events.iter().take(3) {
            match event.severity {
                EventSeverity::Critical => {
                    recommendations.push(format!("🚨 Critical: {} - {}", event.title, event.description));
                }
                EventSeverity::Error => {
                    recommendations.push(format!("❌ Error: {}", event.title));
                }
                _ => {}
            }
        }
        
        recommendations
    }
}

/// Start MCP server (stdio mode for Claude Desktop)
pub async fn start_mcp_server_stdio(
    manager: Arc<RwLock<IntegrationManager>>,
    config: McpConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !config.enabled {
        tracing::info!("MCP server disabled");
        return Ok(());
    }
    
    let server = FluxxMcpServer::new(manager, config);
    
    // Use tokio::io for async stdio
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin).lines();
    
    eprintln!("Fluxx Agent MCP Server starting (stdio)...");
    
    while let Ok(Some(line)) = reader.next_line().await {
        debug!(line = %line, "Received MCP request");
        
        let request: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "Invalid JSON received");
                continue;
            }
        };
        
        let id = request.get("id").cloned();
        let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let params = request.get("params").cloned().unwrap_or(serde_json::json!({}));
        
        let response = match method {
            "initialize" => {
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": { "listChanged": false }
                        },
                        "serverInfo": {
                            "name": "fluxx-agent",
                            "version": "0.1.0"
                        }
                    }
                })
            },
            "notifications/initialized" => {
                continue; // Notification, no response needed
            },
            "tools/list" => {
                let tools = server.get_tools();
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "tools": tools
                    }
                })
            },
            "tools/call" => {
                let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let arguments = params.get("arguments").cloned().unwrap_or(serde_json::json!({}));
                
                let call = ToolCall {
                    name: name.to_string(),
                    arguments,
                };
                
                let result = server.execute_tool(call).await;
                match result {
                    Ok(resp) => serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": resp
                    }),
                    Err(e) => serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {
                            "code": -32000,
                            "message": e
                        }
                    })
                }
            },
            _ => {
                if let Some(req_id) = id {
                    serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {
                            "code": -32601,
                            "message": format!("Method not found: {}", method)
                        }
                    })
                } else {
                    continue;
                }
            }
        };
        
        let response_line = format!("{}\n", serde_json::to_string(&response).unwrap());
        stdout.write_all(response_line.as_bytes()).await?;
        stdout.flush().await?;
    }
    
    Ok(())
}

/// Print MCP configuration for Claude Desktop
#[allow(dead_code)]
fn print_mcp_config() {
    println!("\n{{");
    println!("  \"mcpServers\": {{");
    println!("    \"fluxx-agent\": {{");
    println!("      \"command\": \"fluxx-agent\",");
    println!("      \"args\": [\"mcp\"],");
    println!("      \"env\": {{");
    println!("        \"FLUXX_CONFIG\": \"/path/to/fluxx.yaml\"");
    println!("      }}");
    println!("    }}");
    println!("  }}");
    println!("}}\n");
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_mcp_server_creation() {
        let manager = Arc::new(RwLock::new(IntegrationManager::new()));
        let config = McpConfig::default();
        let server = FluxxMcpServer::new(manager, config);
        
        let tools = server.get_tools();
        assert_eq!(tools.len(), 5);
        assert!(tools.iter().any(|t| t.name == "fluxx_query_metrics"));
        assert!(tools.iter().any(|t| t.name == "fluxx_health_check"));
    }
}
