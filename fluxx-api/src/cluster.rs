//! Cluster Mode API
//!
//! Exposes mesh networking state and enables federated queries
//! across the Fluxx cluster. Leverages the existing gossip protocol
//! and query federation infrastructure.

use crate::api::{ApiState, StandardResponse};
use axum::{
    extract::State,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Cluster status response.
#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub node_id: String,
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub suspect_nodes: usize,
    pub dead_nodes: usize,
    pub regions: Vec<RegionInfo>,
    pub nodes: Vec<NodeSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegionInfo {
    pub name: String,
    pub node_count: usize,
    pub healthy_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeSummary {
    pub id: String,
    pub address: String,
    pub region: String,
    pub zone: String,
    pub status: String,
    pub role: String,
}

/// GET /v1/cluster — Get cluster status and node list.
pub async fn cluster_status(
    State(_state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    // In production, this reads from the GossipProtocol's node registry.
    // For now, return self-node status for single-node mode.
    let hostname = whoami::fallible::hostname().unwrap_or_else(|_| "unknown".to_string());
    let node_id = format!("fluxx-{}", &hostname);

    let status = ClusterStatus {
        node_id: node_id.clone(),
        total_nodes: 1,
        healthy_nodes: 1,
        suspect_nodes: 0,
        dead_nodes: 0,
        regions: vec![RegionInfo {
            name: "local".to_string(),
            node_count: 1,
            healthy_count: 1,
        }],
        nodes: vec![NodeSummary {
            id: node_id,
            address: "0.0.0.0:7007".to_string(),
            region: "local".to_string(),
            zone: "default".to_string(),
            status: "healthy".to_string(),
            role: "primary".to_string(),
        }],
    };

    let data = serde_json::to_value(&status).unwrap_or_default();
    StandardResponse::ok(data, "Cluster status.")
}

/// GET /v1/cluster/topology — Get mesh topology for visualization.
pub async fn cluster_topology(
    State(_state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    let hostname = whoami::fallible::hostname().unwrap_or_else(|_| "unknown".to_string());

    let topology = serde_json::json!({
        "nodes": [{
            "id": format!("fluxx-{}", hostname),
            "label": hostname,
            "type": "primary",
            "status": "healthy",
            "metrics": {
                "events_per_sec": 0,
                "cpu_usage": 0.0,
                "memory_mb": 0,
            }
        }],
        "edges": [],
        "gossip": {
            "interval_ms": 200,
            "fanout": 3,
        }
    });

    StandardResponse::ok(topology, "Mesh topology.")
}
