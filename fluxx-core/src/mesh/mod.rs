//! Mesh Networking for Fluxx Agent
//! 
//! Deploy a distributed mesh of Fluxx Agent nodes with simple configuration.
//! 
//! ## Quick Start
//! 
//! ```yaml
//! # mesh.yaml - Deploy a 3-node mesh
//! mesh:
//!   node_id: node-1
//!   bind: 0.0.0.0:7946
//!   
//!   peers:
//!     - node-2.local:7946
//!     - node-3.local:7946
//!   
//!   federation:
//!     enabled: true
//!     query_timeout: 30s
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

pub mod config;
pub mod gossip;
pub mod query;

/// Mesh node identifier
pub type NodeId = String;

/// Mesh configuration - everything needed to join a mesh
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MeshConfig {
    /// Unique ID for this node
    pub node_id: NodeId,
    
    /// Address to bind for mesh communication
    pub bind: SocketAddr,
    
    /// Peer nodes to connect to
    #[serde(default)]
    pub peers: Vec<String>,
    
    /// Gossip protocol settings
    #[serde(default)]
    pub gossip: GossipConfig,
    
    /// Query federation settings
    #[serde(default)]
    pub federation: FederationConfig,
    
    /// Data replication settings
    #[serde(default)]
    pub replication: ReplicationConfig,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            node_id: generate_node_id(),
            bind: "0.0.0.0:7946".parse().unwrap(),
            peers: vec![],
            gossip: GossipConfig::default(),
            federation: FederationConfig::default(),
            replication: ReplicationConfig::default(),
        }
    }
}

/// Gossip protocol configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GossipConfig {
    /// How often to gossip (milliseconds)
    #[serde(default = "default_gossip_interval")]
    pub interval_ms: u64,
    
    /// How long before considering a node dead (milliseconds)
    #[serde(default = "default_gossip_timeout")]
    pub timeout_ms: u64,
    
    /// Number of nodes to gossip to per round
    #[serde(default = "default_gossip_fanout")]
    pub fanout: usize,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            interval_ms: 200,
            timeout_ms: 1000,
            fanout: 3,
        }
    }
}

/// Federation configuration for distributed queries
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FederationConfig {
    /// Enable federated queries across mesh
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    /// Query timeout
    #[serde(default = "default_query_timeout")]
    pub timeout_secs: u64,
    
    /// Max concurrent queries to other nodes
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout_secs: 30,
            max_concurrent: 10,
        }
    }
}

/// Data replication configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplicationConfig {
    /// Replication factor (how many copies)
    #[serde(default = "default_replication_factor")]
    pub factor: u32,
    
    /// Replicate across regions
    #[serde(default = "default_true")]
    pub cross_region: bool,
    
    /// Async replication (faster, may lose data on crash)
    #[serde(default = "default_true")]
    pub async_replication: bool,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            factor: 2,
            cross_region: true,
            async_replication: true,
        }
    }
}

/// Information about a mesh node
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub address: SocketAddr,
    pub region: String,
    pub zone: String,
    pub status: NodeStatus,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum NodeStatus {
    Healthy,
    Suspect,
    Dead,
}

// Default helpers
fn default_gossip_interval() -> u64 { 200 }
fn default_gossip_timeout() -> u64 { 1000 }
fn default_gossip_fanout() -> usize { 3 }
fn default_query_timeout() -> u64 { 30 }
fn default_max_concurrent() -> usize { 10 }
fn default_replication_factor() -> u32 { 2 }
fn default_true() -> bool { true }

fn generate_node_id() -> String {
    use uuid::Uuid;
    format!("node-{}", Uuid::new_v4().to_string().split('-').next().unwrap())
}

/// Example configurations for common scenarios
pub mod examples {
    use super::*;
    
    /// Minimal 2-node mesh
    pub fn two_node_mesh() -> MeshConfig {
        MeshConfig {
            node_id: "node-1".to_string(),
            bind: "0.0.0.0:7946".parse().unwrap(),
            peers: vec!["node-2.local:7946".to_string()],
            ..Default::default()
        }
    }
    
    /// Multi-region mesh
    pub fn multi_region_mesh() -> Vec<MeshConfig> {
        vec![
            MeshConfig {
                node_id: "us-east-1".to_string(),
                bind: "0.0.0.0:7946".parse().unwrap(),
                peers: vec![
                    "us-west-1.local:7946".to_string(),
                    "eu-west-1.local:7946".to_string(),
                ],
                replication: ReplicationConfig {
                    factor: 3,
                    cross_region: true,
                    async_replication: true,
                },
                ..Default::default()
            },
            // ... other regions
        ]
    }
    
    /// Edge computing mesh (many small nodes)
    pub fn edge_mesh() -> MeshConfig {
        MeshConfig {
            node_id: generate_node_id(),
            bind: "0.0.0.0:7946".parse().unwrap(),
            peers: vec!["edge-gateway.local:7946".to_string()],
            gossip: GossipConfig {
                interval_ms: 500,  // Slower gossip for edge
                timeout_ms: 5000,
                fanout: 2,
            },
            replication: ReplicationConfig {
                factor: 1,  // Minimal replication at edge
                cross_region: false,
                async_replication: true,
            },
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = MeshConfig::default();
        assert!(!config.node_id.is_empty());
        assert_eq!(config.gossip.interval_ms, 200);
        assert!(config.federation.enabled);
    }
    
    #[test]
    fn test_yaml_parse() {
        let yaml = r#"
node_id: my-node
bind: 0.0.0.0:7946
peers:
  - peer1.local:7946
  - peer2.local:7946
gossip:
  interval_ms: 500
federation:
  enabled: true
  timeout_secs: 60
"#;
        
        let config: MeshConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.node_id, "my-node");
        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.gossip.interval_ms, 500);
        assert_eq!(config.federation.timeout_secs, 60);
    }
}
