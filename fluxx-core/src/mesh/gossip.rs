//! Gossip Protocol for Mesh Membership
//! 
//! Simple gossip-based membership protocol. Nodes periodically exchange
//! their view of the mesh to detect failures and disseminate state.

use super::{NodeId, NodeInfo, NodeStatus};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::time::interval;

/// Gossip protocol state
pub struct GossipProtocol {
    /// This node's ID
    node_id: NodeId,
    
    /// Known nodes in the mesh
    nodes: Arc<RwLock<HashMap<NodeId, NodeState>>>,
    
    /// Configuration
    config: GossipConfig,
    
    /// UDP socket for gossip
    socket: Arc<UdpSocket>,
}

#[derive(Debug, Clone)]
struct NodeState {
    info: NodeInfo,
    last_seen: Instant,
    incarnation: u64,
}

#[derive(Debug, Clone)]
struct GossipConfig {
    interval: Duration,
    timeout: Duration,
    fanout: usize,
}

impl From<&super::GossipConfig> for GossipConfig {
    fn from(c: &super::GossipConfig) -> Self {
        Self {
            interval: Duration::from_millis(c.interval_ms),
            timeout: Duration::from_millis(c.timeout_ms),
            fanout: c.fanout,
        }
    }
}

/// Gossip message types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum GossipMessage {
    /// Ping: Are you alive?
    Ping {
        from: NodeId,
        incarnation: u64,
    },
    
    /// Ack: Yes, I'm alive
    Ack {
        from: NodeId,
        incarnation: u64,
    },
    
    /// Full state sync
    StateSync {
        from: NodeId,
        nodes: Vec<NodeInfo>,
    },
    
    /// Node update (join, leave, status change)
    NodeUpdate {
        from: NodeId,
        node: NodeInfo,
        incarnation: u64,
    },
}

impl GossipProtocol {
    /// Initialize gossip protocol
    pub async fn new(
        node_id: NodeId,
        bind_addr: SocketAddr,
        config: &super::GossipConfig,
    ) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        
        Ok(Self {
            node_id,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            config: config.into(),
            socket: Arc::new(socket),
        })
    }
    
    /// Start gossip protocol
    pub async fn run(&self) -> anyhow::Result<()> {
        // Spawn receive task
        let recv_socket = self.socket.clone();
        let recv_nodes = self.nodes.clone();
        let node_id = self.node_id.clone();
        
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                match recv_socket.recv_from(&mut buf).await {
                    Ok((len, from)) => {
                        if let Ok(msg) = simd_json::from_slice::<GossipMessage>(&mut buf[..len]) {
                            handle_gossip_message(msg, &recv_nodes, &node_id, from).await;
                        }
                    }
                    Err(e) => {
                        tracing::error!("Gossip receive error: {}", e);
                    }
                }
            }
        });
        
        // Spawn gossip task
        let gossip_socket = self.socket.clone();
        let gossip_nodes = self.nodes.clone();
        let gossip_config = self.config.clone();
        let gossip_node_id = self.node_id.clone();
        
        tokio::spawn(async move {
            let mut ticker = interval(gossip_config.interval);
            
            loop {
                ticker.tick().await;
                
                // Pick random nodes to gossip to
                let targets = select_gossip_targets(&gossip_nodes, gossip_config.fanout).await;
                
                for target in targets {
                    let msg = GossipMessage::Ping {
                        from: gossip_node_id.clone(),
                        incarnation: 0,
                    };
                    
                    if let Ok(data) = simd_json::to_vec(&msg) {
                        let _ = gossip_socket.send_to(&data, target.address).await;
                    }
                }
            }
        });
        
        // Spawn failure detection task
        let failure_nodes = self.nodes.clone();
        let failure_config = self.config.clone();
        
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(1));
            
            loop {
                ticker.tick().await;
                
                let mut nodes = failure_nodes.write().await;
                let now = Instant::now();
                
                for (_, state) in nodes.iter_mut() {
                    if now.duration_since(state.last_seen) > failure_config.timeout {
                        state.info.status = NodeStatus::Suspect;
                    }
                }
            }
        });
        
        Ok(())
    }
    
    /// Join mesh by connecting to known peers
    pub async fn join(&self, peers: &[String]) -> anyhow::Result<()> {
        for peer in peers {
            // Send state sync request
            let msg = GossipMessage::StateSync {
                from: self.node_id.clone(),
                nodes: vec![], // Request: empty nodes list
            };
            
            if let Ok(data) = simd_json::to_vec(&msg) {
                if let Ok(addr) = peer.parse::<SocketAddr>() {
                    self.socket.send_to(&data, addr).await?;
                }
            }
        }
        
        Ok(())
    }
    
    /// Get all known nodes
    pub async fn nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .filter(|s| s.info.status != NodeStatus::Dead)
            .map(|s| s.info.clone())
            .collect()
    }
    
    /// Get healthy nodes only
    pub async fn healthy_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .filter(|s| s.info.status == NodeStatus::Healthy)
            .map(|s| s.info.clone())
            .collect()
    }
    
    /// Get current mesh size
    pub async fn mesh_size(&self) -> usize {
        let nodes = self.nodes.read().await;
        nodes.values()
            .filter(|s| s.info.status != NodeStatus::Dead)
            .count()
    }
}

async fn handle_gossip_message(
    msg: GossipMessage,
    nodes: &Arc<RwLock<HashMap<NodeId, NodeState>>>,
    our_id: &NodeId,
    from_addr: SocketAddr,
) {
    match msg {
        GossipMessage::Ping { from, incarnation } => {
            // Update or add sender to our view
            let mut nodes_lock = nodes.write().await;
            let now = Instant::now();
            
            if let Some(state) = nodes_lock.get_mut(&from) {
                state.last_seen = now;
                state.info.status = NodeStatus::Healthy;
            } else {
                // New node
                nodes_lock.insert(from.clone(), NodeState {
                    info: NodeInfo {
                        id: from.clone(),
                        address: from_addr,
                        region: "unknown".to_string(),
                        zone: "unknown".to_string(),
                        status: NodeStatus::Healthy,
                        metadata: Default::default(),
                    },
                    last_seen: now,
                    incarnation,
                });
            }
            
            tracing::debug!("Received ping from {}", from);
        }
        
        GossipMessage::Ack { from, .. } => {
            let mut nodes_lock = nodes.write().await;
            if let Some(state) = nodes_lock.get_mut(&from) {
                state.last_seen = Instant::now();
                state.info.status = NodeStatus::Healthy;
            }
        }
        
        GossipMessage::StateSync { from, nodes: remote_nodes } => {
            // Merge remote state with our view
            let mut nodes_lock = nodes.write().await;
            
            for node_info in remote_nodes {
                if node_info.id != *our_id {
                    nodes_lock.entry(node_info.id.clone())
                        .or_insert_with(|| NodeState {
                            info: node_info,
                            last_seen: Instant::now(),
                            incarnation: 0,
                        });
                }
            }
            
            tracing::debug!("Received state sync from {} with {} nodes", from, nodes_lock.len());
        }
        
        GossipMessage::NodeUpdate { from, node, incarnation } => {
            let node_id = node.id.clone();
            let mut nodes_lock = nodes.write().await;
            
            if let Some(state) = nodes_lock.get_mut(&node_id) {
                if incarnation > state.incarnation {
                    state.info = node;
                    state.incarnation = incarnation;
                    state.last_seen = Instant::now();
                }
            } else if node_id != *our_id {
                nodes_lock.insert(node_id.clone(), NodeState {
                    info: node,
                    last_seen: Instant::now(),
                    incarnation,
                });
            }
            
            tracing::debug!("Received node update from {} for {}", from, node_id);
        }
    }
}

async fn select_gossip_targets(
    nodes: &Arc<RwLock<HashMap<NodeId, NodeState>>>,
    fanout: usize,
) -> Vec<NodeInfo> {
    let nodes_lock = nodes.read().await;
    let healthy: Vec<_> = nodes_lock.values()
        .filter(|s| s.info.status == NodeStatus::Healthy)
        .collect();
    
    // Simple random selection
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    Instant::now().hash(&mut hasher);
    let seed = hasher.finish();
    
    let mut selected = Vec::new();
    for i in 0..fanout.min(healthy.len()) {
        let idx = ((seed + i as u64) % healthy.len() as u64) as usize;
        selected.push(healthy[idx].info.clone());
    }
    
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_gossip_basic() {
        let config = super::super::GossipConfig::default();
        let gossip = GossipProtocol::new(
            "test-node".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            &config,
        ).await.unwrap();
        
        // Should start with empty mesh
        assert_eq!(gossip.mesh_size().await, 0);
    }
}
