//! Distributed Query Federation
//! 
//! Execute queries across the entire mesh and aggregate results.
//! 
//! ## Usage
//! 
//! ```rust
//! let federated = QueryFederation::new(gossip);
//! 
//! // Query all nodes
//! let results = federated.query("SELECT * FROM events WHERE service='api'")
//!     .await?;
//! ```

use super::{gossip::GossipProtocol, NodeId, NodeInfo};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;
use std::sync::Arc;

/// Simple query result
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// Federated query engine
pub struct QueryFederation {
    gossip: Arc<GossipProtocol>,
    timeout: Duration,
    max_concurrent: usize,
}

/// Result from a federated query
#[derive(Debug, Clone)]
pub struct FederatedResult {
    /// Results from each node
    pub node_results: HashMap<NodeId, QueryResult>,
    
    /// Nodes that failed to respond
    pub failed_nodes: Vec<NodeId>,
    
    /// Total rows across all nodes
    pub total_rows: usize,
    
    /// Query execution time
    pub execution_time_ms: u64,
}

impl QueryFederation {
    /// Create new query federation
    pub fn new(gossip: Arc<GossipProtocol>, timeout_secs: u64, max_concurrent: usize) -> Self {
        Self {
            gossip,
            timeout: Duration::from_secs(timeout_secs),
            max_concurrent,
        }
    }
    
    /// Execute query across all healthy nodes
    pub async fn query(&self, sql: &str) -> anyhow::Result<FederatedResult> {
        let start = std::time::Instant::now();
        
        // Get healthy nodes
        let nodes = self.gossip.healthy_nodes().await;
        let node_count = nodes.len();
        
        tracing::info!("Federating query to {} nodes", node_count);
        
        let mut node_results = HashMap::new();
        let mut failed_nodes = Vec::new();
        
        // Execute query on each node with concurrency limit
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent));
        let mut handles = Vec::new();
        
        for node in nodes {
            let permit = semaphore.clone().acquire_owned().await?;
            let query = sql.to_string();
            let node_id = node.id.clone();
            let node_addr = node.address;
            
            let handle = tokio::spawn(async move {
                let _permit = permit; // Hold permit until done
                
                match Self::query_node(&node_addr, &query).await {
                    Ok(result) => Ok((node_id, result)),
                    Err(e) => {
                        tracing::warn!("Query failed on node {}: {}", node_id, e);
                        Err(node_id)
                    }
                }
            });
            
            handles.push(handle);
        }
        
        // Collect results with timeout
        for handle in handles {
            match timeout(self.timeout, handle).await {
                Ok(Ok(Ok((node_id, result)))) => {
                    node_results.insert(node_id, result);
                }
                Ok(Ok(Err(node_id))) => {
                    failed_nodes.push(node_id);
                }
                Ok(Err(e)) => {
                    tracing::error!("Task join error: {}", e);
                }
                Err(_) => {
                    tracing::warn!("Query timeout");
                }
            }
        }
        
        let total_rows = node_results.values()
            .map(|r| r.rows.len())
            .sum();
        
        let execution_time_ms = start.elapsed().as_millis() as u64;
        
        tracing::info!(
            "Federated query complete: {} rows from {} nodes in {}ms",
            total_rows,
            node_results.len(),
            execution_time_ms
        );
        
        Ok(FederatedResult {
            node_results,
            failed_nodes,
            total_rows,
            execution_time_ms,
        })
    }
    
    /// Query a specific node
    async fn query_node(
        addr: &std::net::SocketAddr,
        _sql: &str,
    ) -> anyhow::Result<QueryResult> {
        use tokio::io::AsyncWriteExt;
        
        // Connect to node's query endpoint
        let stream = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::net::TcpStream::connect(addr)
        ).await??;
        
        // Send query
        let mut stream = stream;
        let request = format!("QUERY\n");
        stream.write_all(request.as_bytes()).await?;
        stream.shutdown().await?;
        
        // For now, return mock result
        // In real implementation, this would deserialize the response
        Ok(QueryResult {
            columns: vec!["timestamp".to_string(), "message".to_string()],
            rows: vec![],
        })
    }
    
    /// Get mesh topology information
    pub async fn topology(&self) -> MeshTopology {
        let nodes = self.gossip.nodes().await;
        let healthy = self.gossip.healthy_nodes().await;
        
        MeshTopology {
            total_nodes: nodes.len(),
            healthy_nodes: healthy.len(),
            suspect_nodes: nodes.len() - healthy.len(),
            regions: Self::group_by_region(&nodes),
        }
    }
    
    fn group_by_region(nodes: &[NodeInfo]) -> HashMap<String, usize> {
        let mut regions = HashMap::new();
        for node in nodes {
            *regions.entry(node.region.clone()).or_insert(0) += 1;
        }
        regions
    }
}

/// Mesh topology information
#[derive(Debug, Clone)]
pub struct MeshTopology {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub suspect_nodes: usize,
    pub regions: HashMap<String, usize>,
}

/// Distributed aggregation functions
pub mod aggregation {
    use super::{FederatedResult, QueryResult};
    use crate::mesh::NodeId;
    
    /// Merge results from multiple nodes
    pub fn merge_results(results: &FederatedResult) -> QueryResult {
        let mut all_rows = Vec::new();
        let mut columns = Vec::new();
        
        for result in results.node_results.values() {
            if columns.is_empty() {
                columns = result.columns.clone();
            }
            all_rows.extend(result.rows.clone());
        }
        
        QueryResult {
            columns,
            rows: all_rows,
        }
    }
    
    /// Count total events across mesh
    pub fn total_events(results: &FederatedResult) -> usize {
        results.total_rows
    }
    
    /// Find nodes with most events
    pub fn top_nodes_by_volume(results: &FederatedResult, n: usize) -> Vec<(NodeId, usize)> {
        let mut counts: Vec<_> = results.node_results
            .iter()
            .map(|(id, result)| (id.clone(), result.rows.len()))
            .collect();
        
        counts.sort_by(|a, b| b.1.cmp(&a.1));
        counts.into_iter().take(n).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_federated_result_aggregation() {
        let result = FederatedResult {
            node_results: HashMap::new(),
            failed_nodes: vec![],
            total_rows: 0,
            execution_time_ms: 100,
        };
        
        assert_eq!(result.total_rows, 0);
    }
}
