//! Mesh Initialization
//! 
//! Simple, zero-code mesh deployment. Just configure and go.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use fluxx_agent::mesh;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Initialize mesh from config file or environment
//!     let mesh = mesh::init().await?;
//!     
//!     // The mesh is now running and connected
//!     println!("Connected to {} nodes", mesh.size().await);
//!     
//!     // Run until shutdown
//!     mesh.wait_for_shutdown().await;
//!     Ok(())
//! }
//! ```

use super::{config, gossip::GossipProtocol, MeshConfig};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Running mesh instance
pub struct Mesh {
    gossip: Arc<GossipProtocol>,
    config: MeshConfig,
    shutdown: Arc<RwLock<bool>>,
}

impl Mesh {
    /// Get number of nodes in mesh
    pub async fn size(&self) -> usize {
        self.gossip.mesh_size().await
    }
    
    /// Get list of healthy nodes
    pub async fn nodes(&self) -> Vec<super::NodeInfo> {
        self.gossip.nodes().await
    }
    
    /// Check if mesh is connected to other nodes
    pub async fn is_connected(&self) -> bool {
        self.gossip.mesh_size().await > 0
    }
    
    /// Wait for mesh to form (have at least n nodes)
    pub async fn wait_for_mesh(&self, min_nodes: usize, timeout_secs: u64) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        
        loop {
            let size = self.gossip.mesh_size().await;
            if size >= min_nodes {
                return true;
            }
            
            if tokio::time::Instant::now() > deadline {
                return false;
            }
            
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    
    /// Shutdown the mesh
    pub async fn shutdown(&self) {
        let mut shutdown = self.shutdown.write().await;
        *shutdown = true;
    }
    
    /// Wait for shutdown signal
    pub async fn wait_for_shutdown(&self) {
        loop {
            let shutdown = *self.shutdown.read().await;
            if shutdown {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    
    /// Get reference to gossip protocol
    pub fn gossip(&self) -> Arc<GossipProtocol> {
        self.gossip.clone()
    }
}

/// Initialize mesh from configuration
///
/// Loads configuration and starts mesh protocols.
pub async fn init() -> anyhow::Result<Mesh> {
    // Load configuration
    let config = config::load()?;
    init_with_config(config).await
}

/// Initialize mesh with explicit configuration
pub async fn init_with_config(config: MeshConfig) -> anyhow::Result<Mesh> {
    // Validate configuration
    config::validate(&config)?;
    
    tracing::info!("Starting mesh node: {}", config.node_id);
    
    // Initialize gossip protocol
    let gossip = Arc::new(
        GossipProtocol::new(
            config.node_id.clone(),
            config.bind,
            &config.gossip,
        ).await?
    );
    
    // Start gossip
    let gossip_clone = gossip.clone();
    tokio::spawn(async move {
        if let Err(e) = gossip_clone.run().await {
            tracing::error!("Gossip protocol error: {}", e);
        }
    });
    
    // Join mesh if peers are configured
    if !config.peers.is_empty() {
        tracing::info!("Joining mesh with {} peers", config.peers.len());
        gossip.join(&config.peers).await?;
    } else {
        tracing::info!("No peers configured, running as standalone node");
    }
    
    Ok(Mesh {
        gossip,
        config,
        shutdown: Arc::new(RwLock::new(false)),
    })
}

/// Check if mesh configuration exists
pub fn is_configured() -> bool {
    config::load().is_ok()
}

/// Print mesh status
pub async fn print_status(mesh: &Mesh) {
    let nodes = mesh.gossip.nodes().await;
    let healthy = mesh.gossip.healthy_nodes().await;
    
    println!("\n=== Fluxx Mesh Status ===");
    println!("Node ID: {}", mesh.config.node_id);
    println!("Bind Address: {}", mesh.config.bind);
    println!("Total Nodes: {}", nodes.len());
    println!("Healthy Nodes: {}", healthy.len());
    println!("Federation: {}", 
        if mesh.config.federation.enabled { "enabled" } else { "disabled" }
    );
    
    if !nodes.is_empty() {
        println!("\nMesh Members:");
        for node in &nodes {
            let status = match node.status {
                super::NodeStatus::Healthy => "✓",
                super::NodeStatus::Suspect => "?",
                super::NodeStatus::Dead => "✗",
            };
            println!("  {} {} @ {}", status, node.id, node.address);
        }
    }
    
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_mesh_config_validation() {
        let config = MeshConfig::default();
        assert!(config::validate(&config).is_ok());
    }
}
