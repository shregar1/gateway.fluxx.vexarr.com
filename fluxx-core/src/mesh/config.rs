//! Mesh Configuration Loading
//! 
//! Load mesh configuration from YAML files for zero-code deployment.
//!
//! ## Configuration File Locations
//!
//! Configuration is loaded from (in order of precedence):
//! 1. Path specified by `FLUXX_MESH_CONFIG` environment variable
//! 2. `./mesh.yaml` in current directory
//! 3. `/etc/fluxx/mesh.yaml` (system config)
//! 4. Default configuration (single-node mode)

use super::MeshConfig;
use std::path::Path;

/// Load mesh configuration from file or environment
/// 
/// ## Environment Variables
///
/// - `FLUXX_MESH_CONFIG` - Path to config file
/// - `FLUXX_NODE_ID` - Override node ID
/// - `FLUXX_MESH_BIND` - Override bind address
/// - `FLUXX_MESH_PEERS` - Comma-separated list of peer addresses
///
/// ## Example
///
/// ```bash
/// # Use config file
/// FLUXX_MESH_CONFIG=/etc/fluxx/mesh.yaml ./fluxx-agent
///
/// # Or use environment variables only
/// FLUXX_NODE_ID=node-1 FLUXX_MESH_BIND=0.0.0.0:7946 FLUXX_MESH_PEERS=peer1:7946,peer2:7946 ./fluxx-agent
/// ```
pub fn load() -> anyhow::Result<MeshConfig> {
    // Check for config file path
    if let Ok(config_path) = std::env::var("FLUXX_MESH_CONFIG") {
        return load_from_file(&config_path);
    }
    
    // Check standard locations
    let locations = [
        "./mesh.yaml",
        "./config/mesh.yaml",
        "/etc/fluxx/mesh.yaml",
    ];
    
    for location in &locations {
        if Path::new(location).exists() {
            return load_from_file(location);
        }
    }
    
    // Build from environment variables
    Ok(load_from_env())
}

/// Load configuration from a YAML file
pub fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<MeshConfig> {
    let contents = std::fs::read_to_string(path.as_ref())
        .map_err(|e| anyhow::anyhow!("Failed to read config file: {}", e))?;
    
    let mut config: MeshConfig = serde_yaml::from_str(&contents)
        .map_err(|e| anyhow::anyhow!("Failed to parse config: {}", e))?;
    
    // Apply environment variable overrides
    apply_env_overrides(&mut config);
    
    tracing::info!(
        "Loaded mesh configuration from {}",
        path.as_ref().display()
    );
    
    Ok(config)
}

/// Load configuration from environment variables only
fn load_from_env() -> MeshConfig {
    let mut config = MeshConfig::default();
    apply_env_overrides(&mut config);
    config
}

/// Apply environment variable overrides
fn apply_env_overrides(config: &mut MeshConfig) {
    if let Ok(node_id) = std::env::var("FLUXX_NODE_ID") {
        config.node_id = node_id;
    }
    
    if let Ok(bind) = std::env::var("FLUXX_MESH_BIND") {
        if let Ok(addr) = bind.parse() {
            config.bind = addr;
        }
    }
    
    if let Ok(peers) = std::env::var("FLUXX_MESH_PEERS") {
        config.peers = peers.split(',').map(|s| s.trim().to_string()).collect();
    }
    
    if let Ok(enabled) = std::env::var("FLUXX_MESH_FEDERATION") {
        config.federation.enabled = enabled == "true" || enabled == "1";
    }
    
    if let Ok(timeout) = std::env::var("FLUXX_MESH_QUERY_TIMEOUT") {
        if let Ok(secs) = timeout.parse() {
            config.federation.timeout_secs = secs;
        }
    }
}

/// Validate mesh configuration
pub fn validate(config: &MeshConfig) -> anyhow::Result<()> {
    if config.node_id.is_empty() {
        return Err(anyhow::anyhow!("Node ID cannot be empty"));
    }
    
    if config.node_id.contains(' ') {
        return Err(anyhow::anyhow!("Node ID cannot contain spaces"));
    }
    
    // Validate peer addresses
    for peer in &config.peers {
        if peer.parse::<std::net::SocketAddr>().is_err() {
            // Try with :7946 port
            let with_port = format!("{}:7946", peer);
            if with_port.parse::<std::net::SocketAddr>().is_err() {
                return Err(anyhow::anyhow!(
                    "Invalid peer address: {}",
                    peer
                ));
            }
        }
    }
    
    Ok(())
}

/// Generate sample configuration file
pub fn generate_sample_config() -> String {
    r#"# Fluxx Agent Mesh Configuration
# 
# This file configures a node to participate in a distributed mesh.
# Deploy multiple nodes with similar configs to form a cluster.

# Unique identifier for this node
node_id: node-1

# Address to bind for mesh communication
bind: 0.0.0.0:7946

# Peer nodes to connect to (at least one needed for mesh)
peers:
  - node-2.local:7946
  - node-3.local:7946

# Gossip protocol settings
gossip:
  # How often to gossip (milliseconds)
  interval_ms: 200
  
  # How long before considering a node dead
  timeout_ms: 1000
  
  # Number of nodes to gossip to per round
  fanout: 3

# Query federation settings
federation:
  # Enable distributed queries across mesh
  enabled: true
  
  # Query timeout in seconds
  timeout_secs: 30
  
  # Max concurrent queries to other nodes
  max_concurrent: 10

# Data replication settings
replication:
  # Number of copies to maintain
  factor: 2
  
  # Replicate across regions
  cross_region: true
  
  # Use async replication (faster, may lose data on crash)
  async_replication: true
"#.to_string()
}

/// Generate Docker Compose configuration for mesh
pub fn generate_docker_compose(node_count: u32) -> String {
    let mut compose = String::from(
        r#"version: '3.8'

services:
"#
    );
    
    for i in 1..=node_count {
        let peers: Vec<String> = (1..=node_count)
            .filter(|&j| j != i)
            .map(|j| format!("fluxx-node-{}:7946", j))
            .collect();
        
        compose.push_str(&format!(
            r#"  fluxx-node-{i}:
    image: fluxx/agent:latest
    container_name: fluxx-node-{i}
    environment:
      - FLUXX_NODE_ID=node-{i}
      - FLUXX_MESH_BIND=0.0.0.0:7946
      - FLUXX_MESH_PEERS={peers}
    ports:
      - "{port}:7946"
    volumes:
      - ./data/node-{i}:/data
    networks:
      - fluxx-mesh

"#,
            i = i,
            port = 7945 + i,
            peers = peers.join(",")
        ));
    }
    
    compose.push_str(
        r#"networks:
  fluxx-mesh:
    driver: bridge
"#
    );
    
    compose
}

/// Generate Kubernetes StatefulSet for mesh
pub fn generate_kubernetes_manifest(nodes: u32) -> String {
    format!(
        r#"apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: fluxx-agent
  labels:
    app: fluxx-agent
spec:
  serviceName: fluxx-agent
  replicas: {nodes}
  selector:
    matchLabels:
      app: fluxx-agent
  template:
    metadata:
      labels:
        app: fluxx-agent
    spec:
      containers:
      - name: fluxx-agent
        image: fluxx/agent:latest
        env:
        - name: FLUXX_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: FLUXX_MESH_BIND
          value: "0.0.0.0:7946"
        - name: FLUXX_MESH_PEERS
          value: "fluxx-agent-0.fluxx-agent:7946,fluxx-agent-1.fluxx-agent:7946"
        ports:
        - containerPort: 7946
          name: mesh
---
apiVersion: v1
kind: Service
metadata:
  name: fluxx-agent
spec:
  selector:
    app: fluxx-agent
  ports:
  - port: 7946
    name: mesh
  clusterIP: None
"#,
        nodes = nodes
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_load_from_env() {
        // This test would need env var manipulation
        // Skipping for now as it affects global state
    }
    
    #[test]
    fn test_validate_config() {
        let config = MeshConfig {
            node_id: "test-node".to_string(),
            peers: vec!["192.168.1.1:7946".to_string()],
            ..Default::default()
        };
        
        assert!(validate(&config).is_ok());
    }
    
    #[test]
    fn test_validate_empty_node_id() {
        let config = MeshConfig {
            node_id: "".to_string(),
            ..Default::default()
        };
        
        assert!(validate(&config).is_err());
    }
    
    #[test]
    fn test_generate_docker_compose() {
        let compose = generate_docker_compose(3);
        assert!(compose.contains("fluxx-node-1"));
        assert!(compose.contains("fluxx-node-2"));
        assert!(compose.contains("fluxx-node-3"));
    }
    
    #[test]
    fn test_generate_kubernetes() {
        let k8s = generate_kubernetes_manifest(3);
        assert!(k8s.contains("kind: StatefulSet"));
        assert!(k8s.contains("replicas: 3"));
    }
}
