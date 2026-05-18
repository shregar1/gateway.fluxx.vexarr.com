use crate::api::{ApiState, StandardResponse};
use crate::models::event::ProtocolType;
use axum::{
    extract::State,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Service map response with nodes as services and edges as call relationships.
#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceMapResponse {
    pub nodes: Vec<ServiceNode>,
    pub edges: Vec<ServiceEdge>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceNode {
    pub id: String,
    pub name: String,
    pub span_count: u64,
    pub avg_latency_us: u64,
    pub error_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceEdge {
    pub source: String,
    pub target: String,
    pub request_count: u64,
    pub avg_latency_us: u64,
    pub error_rate: f64,
}

/// GET /v1/service-map — Build service dependency map from OTLP trace spans.
pub async fn get_service_map(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    // Query all OTLP events
    let events = state.event_store.list_events(
        None,
        Some(ProtocolType::Otlp),
        None,
        None,
    ).await;

    // Aggregate per-service stats
    let mut service_stats: HashMap<String, ServiceStats> = HashMap::new();
    // Collect edges: (caller_service, callee_service) -> EdgeStats
    let mut edge_stats: HashMap<(String, String), EdgeStats> = HashMap::new();
    // Map span_id -> service_name for parent lookups
    let mut span_to_service: HashMap<String, String> = HashMap::new();

    // First pass: collect span_id -> service mapping
    for event in &events {
        let service = event.service_id.clone()
            .or_else(|| event.metadata.extra.get("service.name").cloned())
            .unwrap_or_else(|| "unknown".to_string());
        let span_id = event.id.clone();
        span_to_service.insert(span_id, service);
    }

    // Second pass: build graph
    for event in &events {
        let service = event.service_id.clone()
            .or_else(|| event.metadata.extra.get("service.name").cloned())
            .unwrap_or_else(|| "unknown".to_string());

        let is_error = matches!(
            event.severity,
            crate::models::event::EventSeverity::High | crate::models::event::EventSeverity::Critical
        );

        // Update service node stats
        let stats = service_stats.entry(service.clone()).or_insert_with(ServiceStats::default);
        stats.span_count += 1;
        stats.total_latency_us += event.latency_us;
        if is_error {
            stats.error_count += 1;
        }

        // Derive edge from parent_span_id
        if let Some(parent_span_id) = event.metadata.extra.get("parent_span_id") {
            if !parent_span_id.is_empty() {
                if let Some(parent_service) = span_to_service.get(parent_span_id) {
                    if *parent_service != service {
                        let key = (parent_service.clone(), service.clone());
                        let edge = edge_stats.entry(key).or_insert_with(EdgeStats::default);
                        edge.request_count += 1;
                        edge.total_latency_us += event.latency_us;
                        if is_error {
                            edge.error_count += 1;
                        }
                    }
                }
            }
        }
    }

    // Build response
    let nodes: Vec<ServiceNode> = service_stats.into_iter().map(|(name, stats)| {
        ServiceNode {
            id: name.clone(),
            name,
            span_count: stats.span_count,
            avg_latency_us: if stats.span_count > 0 { stats.total_latency_us / stats.span_count } else { 0 },
            error_count: stats.error_count,
        }
    }).collect();

    let edges: Vec<ServiceEdge> = edge_stats.into_iter().map(|((source, target), stats)| {
        ServiceEdge {
            source,
            target,
            request_count: stats.request_count,
            avg_latency_us: if stats.request_count > 0 { stats.total_latency_us / stats.request_count } else { 0 },
            error_rate: if stats.request_count > 0 { stats.error_count as f64 / stats.request_count as f64 } else { 0.0 },
        }
    }).collect();

    let response = ServiceMapResponse { nodes, edges };
    let data = serde_json::to_value(&response).unwrap_or_default();
    StandardResponse::ok(data, "Service map generated from trace spans.")
}

#[derive(Default)]
struct ServiceStats {
    span_count: u64,
    total_latency_us: u64,
    error_count: u64,
}

#[derive(Default)]
struct EdgeStats {
    request_count: u64,
    total_latency_us: u64,
    error_count: u64,
}
