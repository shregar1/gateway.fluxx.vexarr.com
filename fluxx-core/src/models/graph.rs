use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Node: Represents a service or network endpoint in the Fluxx flow graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub name: String,
    pub protocol: String,
}

/// Edge: Represents a transactional request or stream between two nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub source_id: String,
    pub target_id: String,
    pub protocol: String,
    pub count: u64,
    pub avg_latency_us: u64,
}

/// Graph: A real-time representation of service topology and request flow.
/// Designed for high-performance JSON output to frontend visualization layers.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Graph {
    pub nodes: HashMap<String, Node>,
    pub edges: HashMap<String, Edge>, // Key: "source_id:target_id:protocol"
}

impl Graph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, node: Node) {
        self.nodes.entry(node.id.clone()).or_insert(node);
    }

    pub fn update_edge(&mut self, source_id: &str, target_id: &str, protocol: &str, latency_us: u64) {
        let edge_key = format!("{}:{}:{}", source_id, target_id, protocol);
        let edge = self.edges.entry(edge_key).or_insert(Edge {
            source_id: source_id.to_string(),
            target_id: target_id.to_string(),
            protocol: protocol.to_string(),
            count: 0,
            avg_latency_us: 0,
        });

        // Update moving average (simplified)
        let new_count = edge.count + 1;
        edge.avg_latency_us = ((edge.avg_latency_us * edge.count) + latency_us) / new_count;
        edge.count = new_count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 21: Graph new creates empty graph
    #[test]
    fn test_graph_new_empty() {
        let graph = Graph::new();
        assert!(graph.nodes.is_empty());
        assert!(graph.edges.is_empty());
    }

    // Test 22: Graph default creates empty graph
    #[test]
    fn test_graph_default() {
        let graph: Graph = Default::default();
        assert!(graph.nodes.is_empty());
        assert!(graph.edges.is_empty());
    }

    // Test 23: Add single node
    #[test]
    fn test_graph_add_single_node() {
        let mut graph = Graph::new();
        let node = Node {
            id: "node-1".to_string(),
            name: "Test Node".to_string(),
            protocol: "http".to_string(),
        };
        graph.add_node(node);
        assert_eq!(graph.nodes.len(), 1);
        assert!(graph.nodes.contains_key("node-1"));
    }

    // Test 24: Add multiple nodes
    #[test]
    fn test_graph_add_multiple_nodes() {
        let mut graph = Graph::new();
        graph.add_node(Node { id: "n1".to_string(), name: "Node1".to_string(), protocol: "http".to_string() });
        graph.add_node(Node { id: "n2".to_string(), name: "Node2".to_string(), protocol: "ws".to_string() });
        graph.add_node(Node { id: "n3".to_string(), name: "Node3".to_string(), protocol: "tcp".to_string() });
        assert_eq!(graph.nodes.len(), 3);
    }

    // Test 25: Add duplicate node (should not duplicate)
    #[test]
    fn test_graph_add_duplicate_node() {
        let mut graph = Graph::new();
        let node1 = Node { id: "n1".to_string(), name: "Node1".to_string(), protocol: "http".to_string() };
        let node2 = Node { id: "n1".to_string(), name: "Node2".to_string(), protocol: "ws".to_string() };
        graph.add_node(node1);
        graph.add_node(node2);
        assert_eq!(graph.nodes.len(), 1);
    }

    // Test 26: Update edge creates new edge
    #[test]
    fn test_graph_update_edge_new() {
        let mut graph = Graph::new();
        graph.update_edge("src", "dst", "http", 1000);
        assert_eq!(graph.edges.len(), 1);
        let edge = graph.edges.get("src:dst:http").unwrap();
        assert_eq!(edge.count, 1);
        assert_eq!(edge.avg_latency_us, 1000);
    }

    // Test 27: Update edge updates average
    #[test]
    fn test_graph_update_edge_average() {
        let mut graph = Graph::new();
        graph.update_edge("src", "dst", "http", 1000);
        graph.update_edge("src", "dst", "http", 2000);
        graph.update_edge("src", "dst", "http", 3000);
        let edge = graph.edges.get("src:dst:http").unwrap();
        assert_eq!(edge.count, 3);
        assert_eq!(edge.avg_latency_us, 2000);
    }

    // Test 28: Update different edges
    #[test]
    fn test_graph_update_different_edges() {
        let mut graph = Graph::new();
        graph.update_edge("a", "b", "http", 1000);
        graph.update_edge("a", "c", "http", 2000);
        graph.update_edge("b", "c", "ws", 3000);
        assert_eq!(graph.edges.len(), 3);
    }

    // Test 29: Node serialization
    #[test]
    fn test_node_serialization() {
        let node = Node {
            id: "n1".to_string(),
            name: "Test".to_string(),
            protocol: "http".to_string(),
        };
        let json = serde_json::to_string(&node).unwrap();
        assert!(json.contains("n1"));
        assert!(json.contains("Test"));
    }

    // Test 30: Edge serialization
    #[test]
    fn test_edge_serialization() {
        let edge = Edge {
            source_id: "src".to_string(),
            target_id: "dst".to_string(),
            protocol: "http".to_string(),
            count: 10,
            avg_latency_us: 5000,
        };
        let json = serde_json::to_string(&edge).unwrap();
        assert!(json.contains("src"));
        assert!(json.contains("dst"));
        assert!(json.contains("10"));
    }

    // Test 31: Graph serialization
    #[test]
    fn test_graph_serialization() {
        let mut graph = Graph::new();
        graph.add_node(Node { id: "n1".to_string(), name: "Node1".to_string(), protocol: "http".to_string() });
        graph.update_edge("n1", "n2", "http", 1000);
        let json = serde_json::to_string(&graph).unwrap();
        assert!(json.contains("nodes"));
        assert!(json.contains("edges"));
    }

    // Test 32: Graph deserialization
    #[test]
    fn test_graph_deserialization() {
        let json = r#"{"nodes":{"n1":{"id":"n1","name":"Node1","protocol":"http"}},"edges":{}}"#;
        let graph: Graph = serde_json::from_str(json).unwrap();
        assert_eq!(graph.nodes.len(), 1);
    }

    // Test 251: Node creation with all fields
    #[test]
    fn test_node_creation_all_fields() {
        let node = Node {
            id: "node-1".to_string(),
            name: "Test Node".to_string(),
            protocol: "http".to_string(),
        };
        assert_eq!(node.id, "node-1");
        assert_eq!(node.name, "Test Node");
        assert_eq!(node.protocol, "http");
    }

    // Test 252: Node creation with empty strings
    #[test]
    fn test_node_creation_empty_strings() {
        let node = Node {
            id: "".to_string(),
            name: "".to_string(),
            protocol: "".to_string(),
        };
        assert!(node.id.is_empty());
        assert!(node.name.is_empty());
        assert!(node.protocol.is_empty());
    }

    // Test 253: Node clone
    #[test]
    fn test_node_clone() {
        let node = Node {
            id: "n1".to_string(),
            name: "Node".to_string(),
            protocol: "tcp".to_string(),
        };
        let cloned = node.clone();
        assert_eq!(node.id, cloned.id);
        assert_eq!(node.name, cloned.name);
        assert_eq!(node.protocol, cloned.protocol);
    }

    // Test 254: Node debug format
    #[test]
    fn test_node_debug() {
        let node = Node {
            id: "n1".to_string(),
            name: "Node".to_string(),
            protocol: "http".to_string(),
        };
        let debug_str = format!("{:?}", node);
        assert!(debug_str.contains("n1"));
        assert!(debug_str.contains("Node"));
    }

    // Test 255: Node deserialization
    #[test]
    fn test_node_deserialization() {
        let json = r#"{"id":"n1","name":"Test","protocol":"ws"}"#;
        let node: Node = serde_json::from_str(json).unwrap();
        assert_eq!(node.id, "n1");
        assert_eq!(node.name, "Test");
        assert_eq!(node.protocol, "ws");
    }

    // Test 256: Node with special characters in name
    #[test]
    fn test_node_special_chars_name() {
        let node = Node {
            id: "n1".to_string(),
            name: "Node!@#$%^&*()".to_string(),
            protocol: "http".to_string(),
        };
        assert_eq!(node.name, "Node!@#$%^&*()");
    }

    // Test 257: Node with unicode in name
    #[test]
    fn test_node_unicode_name() {
        let node = Node {
            id: "n1".to_string(),
            name: "节点 🖥️".to_string(),
            protocol: "grpc".to_string(),
        };
        assert_eq!(node.name, "节点 🖥️");
    }

    // Test 258: Node round-trip serialization
    #[test]
    fn test_node_round_trip() {
        let original = Node {
            id: "n1".to_string(),
            name: "Test".to_string(),
            protocol: "http".to_string(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Node = serde_json::from_str(&json).unwrap();
        assert_eq!(original.id, deserialized.id);
        assert_eq!(original.name, deserialized.name);
    }

    // Test 259: Edge creation with all fields
    #[test]
    fn test_edge_creation_all_fields() {
        let edge = Edge {
            source_id: "src".to_string(),
            target_id: "dst".to_string(),
            protocol: "http".to_string(),
            count: 100,
            avg_latency_us: 5000,
        };
        assert_eq!(edge.source_id, "src");
        assert_eq!(edge.target_id, "dst");
        assert_eq!(edge.protocol, "http");
        assert_eq!(edge.count, 100);
        assert_eq!(edge.avg_latency_us, 5000);
    }

    // Test 260: Edge with zero count
    #[test]
    fn test_edge_zero_count() {
        let edge = Edge {
            source_id: "src".to_string(),
            target_id: "dst".to_string(),
            protocol: "tcp".to_string(),
            count: 0,
            avg_latency_us: 0,
        };
        assert_eq!(edge.count, 0);
        assert_eq!(edge.avg_latency_us, 0);
    }

    // Test 261: Edge with max values
    #[test]
    fn test_edge_max_values() {
        let edge = Edge {
            source_id: "src".to_string(),
            target_id: "dst".to_string(),
            protocol: "udp".to_string(),
            count: u64::MAX,
            avg_latency_us: u64::MAX,
        };
        assert_eq!(edge.count, u64::MAX);
        assert_eq!(edge.avg_latency_us, u64::MAX);
    }

    // Test 262: Edge clone
    #[test]
    fn test_edge_clone() {
        let edge = Edge {
            source_id: "src".to_string(),
            target_id: "dst".to_string(),
            protocol: "http".to_string(),
            count: 10,
            avg_latency_us: 100,
        };
        let cloned = edge.clone();
        assert_eq!(edge.source_id, cloned.source_id);
        assert_eq!(edge.count, cloned.count);
    }

    // Test 263: Edge debug format
    #[test]
    fn test_edge_debug() {
        let edge = Edge {
            source_id: "src".to_string(),
            target_id: "dst".to_string(),
            protocol: "http".to_string(),
            count: 10,
            avg_latency_us: 100,
        };
        let debug_str = format!("{:?}", edge);
        assert!(debug_str.contains("src"));
        assert!(debug_str.contains("dst"));
    }

    // Test 264: Edge deserialization
    #[test]
    fn test_edge_deserialization() {
        let json = r#"{"source_id":"s","target_id":"t","protocol":"ws","count":5,"avg_latency_us":200}"#;
        let edge: Edge = serde_json::from_str(json).unwrap();
        assert_eq!(edge.source_id, "s");
        assert_eq!(edge.target_id, "t");
        assert_eq!(edge.count, 5);
        assert_eq!(edge.avg_latency_us, 200);
    }

    // Test 265: Edge round-trip serialization
    #[test]
    fn test_edge_round_trip() {
        let original = Edge {
            source_id: "src".to_string(),
            target_id: "dst".to_string(),
            protocol: "grpc".to_string(),
            count: 50,
            avg_latency_us: 1000,
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Edge = serde_json::from_str(&json).unwrap();
        assert_eq!(original.source_id, deserialized.source_id);
        assert_eq!(original.count, deserialized.count);
    }

    // Test 266: Graph with multiple nodes
    #[test]
    fn test_graph_multiple_nodes() {
        let mut graph = Graph::new();
        for i in 0..10 {
            graph.add_node(Node {
                id: format!("node{}", i),
                name: format!("Node {}", i),
                protocol: "http".to_string(),
            });
        }
        assert_eq!(graph.nodes.len(), 10);
    }

    // Test 267: Graph with duplicate node updates
    #[test]
    fn test_graph_duplicate_node_ignores_new() {
        let mut graph = Graph::new();
        let node1 = Node { id: "n1".to_string(), name: "First".to_string(), protocol: "http".to_string() };
        let node2 = Node { id: "n1".to_string(), name: "Second".to_string(), protocol: "ws".to_string() };
        graph.add_node(node1);
        graph.add_node(node2);
        let stored = graph.nodes.get("n1").unwrap();
        assert_eq!(stored.name, "First");
    }

    // Test 268: Graph update edge zero latency
    #[test]
    fn test_graph_update_edge_zero_latency() {
        let mut graph = Graph::new();
        graph.update_edge("a", "b", "http", 0);
        let edge = graph.edges.get("a:b:http").unwrap();
        assert_eq!(edge.avg_latency_us, 0);
    }

    // Test 269: Graph update edge many times
    #[test]
    fn test_graph_update_edge_many_times() {
        let mut graph = Graph::new();
        for i in 1..=100 {
            graph.update_edge("src", "dst", "tcp", i as u64 * 10);
        }
        let edge = graph.edges.get("src:dst:tcp").unwrap();
        assert_eq!(edge.count, 100);
        assert_eq!(edge.avg_latency_us, 505);
    }

    // Test 270: Graph multiple edges same source
    #[test]
    fn test_graph_multiple_edges_same_source() {
        let mut graph = Graph::new();
        graph.update_edge("src", "dst1", "http", 100);
        graph.update_edge("src", "dst2", "http", 200);
        graph.update_edge("src", "dst3", "http", 300);
        assert_eq!(graph.edges.len(), 3);
    }

    // Test 271: Graph multiple edges same target
    #[test]
    fn test_graph_multiple_edges_same_target() {
        let mut graph = Graph::new();
        graph.update_edge("src1", "dst", "http", 100);
        graph.update_edge("src2", "dst", "http", 200);
        graph.update_edge("src3", "dst", "http", 300);
        assert_eq!(graph.edges.len(), 3);
    }

    // Test 272: Graph edges with different protocols
    #[test]
    fn test_graph_edges_different_protocols() {
        let mut graph = Graph::new();
        graph.update_edge("a", "b", "http", 100);
        graph.update_edge("a", "b", "ws", 200);
        graph.update_edge("a", "b", "grpc", 300);
        assert_eq!(graph.edges.len(), 3);
    }

    // Test 273: Graph round-trip serialization
    #[test]
    fn test_graph_round_trip() {
        let mut original = Graph::new();
        original.add_node(Node { id: "n1".to_string(), name: "Node".to_string(), protocol: "http".to_string() });
        original.update_edge("n1", "n2", "http", 500);
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Graph = serde_json::from_str(&json).unwrap();
        assert_eq!(original.nodes.len(), deserialized.nodes.len());
        assert_eq!(original.edges.len(), deserialized.edges.len());
    }

    // Test 274: Graph clone
    #[test]
    fn test_graph_clone() {
        let mut graph = Graph::new();
        graph.add_node(Node { id: "n1".to_string(), name: "Node".to_string(), protocol: "http".to_string() });
        graph.update_edge("a", "b", "tcp", 100);
        let cloned = graph.clone();
        assert_eq!(graph.nodes.len(), cloned.nodes.len());
        assert_eq!(graph.edges.len(), cloned.edges.len());
    }

    // Test 275: Graph debug format
    #[test]
    fn test_graph_debug() {
        let mut graph = Graph::new();
        graph.add_node(Node { id: "n1".to_string(), name: "Node".to_string(), protocol: "http".to_string() });
        let debug_str = format!("{:?}", graph);
        assert!(debug_str.contains("Graph"));
        assert!(debug_str.contains("n1"));
    }

    // Test 276: Graph empty serialization
    #[test]
    fn test_graph_empty_serialization() {
        let graph = Graph::new();
        let json = serde_json::to_string(&graph).unwrap();
        assert!(json.contains("\"nodes\""));
        assert!(json.contains("\"edges\""));
    }

    // Test 277: Graph complex scenario
    #[test]
    fn test_graph_complex_scenario() {
        let mut graph = Graph::new();
        let nodes = vec!["gateway", "auth", "api", "db"];
        for node in &nodes {
            graph.add_node(Node {
                id: node.to_string(),
                name: node.to_string(),
                protocol: "http".to_string(),
            });
        }
        graph.update_edge("gateway", "auth", "http", 50);
        graph.update_edge("auth", "api", "grpc", 100);
        graph.update_edge("api", "db", "tcp", 200);
        assert_eq!(graph.nodes.len(), 4);
        assert_eq!(graph.edges.len(), 3);
    }

    // Test 278: Edge with empty source_target
    #[test]
    fn test_edge_empty_ids() {
        let edge = Edge {
            source_id: "".to_string(),
            target_id: "".to_string(),
            protocol: "".to_string(),
            count: 0,
            avg_latency_us: 0,
        };
        assert!(edge.source_id.is_empty());
    }

    // Test 279: Node partial equality
    #[test]
    fn test_node_partial_eq() {
        let n1 = Node { id: "n1".to_string(), name: "Node".to_string(), protocol: "http".to_string() };
        let n2 = Node { id: "n1".to_string(), name: "Node".to_string(), protocol: "http".to_string() };
        let n3 = Node { id: "n2".to_string(), name: "Node".to_string(), protocol: "http".to_string() };
        assert_eq!(n1.id, n2.id);
        assert_ne!(n1.id, n3.id);
    }

    // Test 280: Edge partial equality
    #[test]
    fn test_edge_partial_eq() {
        let e1 = Edge { source_id: "s".to_string(), target_id: "t".to_string(), protocol: "http".to_string(), count: 1, avg_latency_us: 100 };
        let e2 = Edge { source_id: "s".to_string(), target_id: "t".to_string(), protocol: "http".to_string(), count: 1, avg_latency_us: 100 };
        assert_eq!(e1.source_id, e2.source_id);
        assert_eq!(e1.count, e2.count);
    }

    // Test 281: Graph nodes HashMap behavior
    #[test]
    fn test_graph_nodes_hashmap() {
        let mut graph = Graph::new();
        graph.add_node(Node { id: "key".to_string(), name: "Value".to_string(), protocol: "p".to_string() });
        assert!(graph.nodes.contains_key("key"));
    }

    // Test 282: Graph edges HashMap behavior
    #[test]
    fn test_graph_edges_hashmap() {
        let mut graph = Graph::new();
        graph.update_edge("s", "t", "p", 100);
        assert!(graph.edges.contains_key("s:t:p"));
    }

    // Test 283: Moving average calculation accuracy
    #[test]
    fn test_moving_average_accuracy() {
        let mut graph = Graph::new();
        graph.update_edge("a", "b", "http", 100);
        graph.update_edge("a", "b", "http", 200);
        graph.update_edge("a", "b", "http", 300);
        graph.update_edge("a", "b", "http", 400);
        let edge = graph.edges.get("a:b:http").unwrap();
        assert_eq!(edge.avg_latency_us, 250);
    }

    // Test 284: Graph with node and edge modifications
    #[test]
    fn test_graph_node_edge_interaction() {
        let mut graph = Graph::new();
        graph.add_node(Node { id: "service".to_string(), name: "Service".to_string(), protocol: "grpc".to_string() });
        graph.update_edge("client", "service", "grpc", 150);
        assert_eq!(graph.nodes.len(), 1);
        assert_eq!(graph.edges.len(), 1);
    }

    // Test 285: Node with long strings
    #[test]
    fn test_node_long_strings() {
        let long = "x".repeat(10000);
        let node = Node {
            id: long.clone(),
            name: long.clone(),
            protocol: long.clone(),
        };
        assert_eq!(node.id.len(), 10000);
    }

    // Test 286: Edge with long strings
    #[test]
    fn test_edge_long_strings() {
        let long = "y".repeat(1000);
        let edge = Edge {
            source_id: long.clone(),
            target_id: long.clone(),
            protocol: long.clone(),
            count: 1,
            avg_latency_us: 100,
        };
        assert_eq!(edge.protocol.len(), 1000);
    }

    // Test 287: Graph deserialization with extra fields
    #[test]
    fn test_graph_deserialization_extra_fields() {
        let json = r#"{"nodes":{"n1":{"id":"n1","name":"N","protocol":"http"}},"edges":{},"extra":"ignored"}"#;
        let graph: Graph = serde_json::from_str(json).unwrap();
        assert_eq!(graph.nodes.len(), 1);
    }

    // Test 288: Graph update edge single call
    #[test]
    fn test_graph_update_edge_single() {
        let mut graph = Graph::new();
        graph.update_edge("src", "dst", "http", 1000);
        let edge = graph.edges.get("src:dst:http").unwrap();
        assert_eq!(edge.count, 1);
        assert_eq!(edge.avg_latency_us, 1000);
    }

    // Test 289: Node serialization all protocols
    #[test]
    fn test_node_all_protocols() {
        let protocols = vec!["http", "ws", "tcp", "udp", "grpc", "quic"];
        for p in protocols {
            let node = Node { id: "n".to_string(), name: "N".to_string(), protocol: p.to_string() };
            let json = serde_json::to_string(&node).unwrap();
            assert!(json.contains(p));
        }
    }

    // Test 290: Graph edge key format
    #[test]
    fn test_graph_edge_key_format() {
        let mut graph = Graph::new();
        graph.update_edge("a", "b", "p", 100);
        let keys: Vec<_> = graph.edges.keys().cloned().collect();
        assert_eq!(keys[0], "a:b:p");
    }

    // Test 291: Empty Graph default
    #[test]
    fn test_graph_default_empty() {
        let graph: Graph = Default::default();
        assert!(graph.nodes.is_empty());
        assert!(graph.edges.is_empty());
    }

    // Test 292: Node with various protocols
    #[test]
    fn test_node_various_protocols() {
        let protocols = vec!["HTTP", "WebSocket", "TCP/IP", "UDP"];
        for p in protocols {
            let node = Node {
                id: "n".to_string(),
                name: "N".to_string(),
                protocol: p.to_string(),
            };
            assert_eq!(node.protocol, p);
        }
    }

    // Test 293: Edge count increment
    #[test]
    fn test_edge_count_increment() {
        let mut graph = Graph::new();
        for _ in 0..5 {
            graph.update_edge("s", "t", "p", 100);
        }
        let edge = graph.edges.get("s:t:p").unwrap();
        assert_eq!(edge.count, 5);
    }

    // Test 294: Graph nodes and edges isolation
    #[test]
    fn test_graph_nodes_edges_isolation() {
        let mut graph = Graph::new();
        graph.add_node(Node { id: "only-node".to_string(), name: "N".to_string(), protocol: "http".to_string() });
        assert!(graph.nodes.contains_key("only-node"));
        assert!(graph.edges.is_empty());
    }

    // Test 295: Graph edge without nodes
    #[test]
    fn test_graph_edge_without_nodes() {
        let mut graph = Graph::new();
        graph.update_edge("a", "b", "http", 100);
        assert!(graph.edges.contains_key("a:b:http"));
        assert!(graph.nodes.is_empty());
    }

    // Test 296: Graph serialization with complex nodes
    #[test]
    fn test_graph_complex_nodes_serialization() {
        let mut graph = Graph::new();
        graph.add_node(Node { id: "n1".to_string(), name: "负载均衡器".to_string(), protocol: "http".to_string() });
        graph.add_node(Node { id: "n2".to_string(), name: "数据库 🗄️".to_string(), protocol: "tcp".to_string() });
        let json = serde_json::to_string(&graph).unwrap();
        assert!(json.contains("负载均衡器"));
        assert!(json.contains("数据库"));
    }

    // Test 297: Graph deserialization preserves edge data
    #[test]
    fn test_graph_deserialization_edge_data() {
        let mut original = Graph::new();
        original.update_edge("s", "t", "http", 500);
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Graph = serde_json::from_str(&json).unwrap();
        let edge = deserialized.edges.values().next().unwrap();
        assert_eq!(edge.avg_latency_us, 500);
        assert_eq!(edge.count, 1);
    }

    // Test 298: Graph multiple update edges same pair
    #[test]
    fn test_graph_multiple_updates_same_pair() {
        let mut graph = Graph::new();
        graph.update_edge("a", "b", "http", 100);
        graph.update_edge("a", "b", "http", 200);
        graph.update_edge("a", "b", "http", 300);
        let edge = graph.edges.get("a:b:http").unwrap();
        assert_eq!(edge.count, 3);
        assert_eq!(edge.avg_latency_us, 200);
    }

    // Test 299: Graph is Send + Sync
    #[test]
    fn test_graph_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Graph>();
        assert_send_sync::<Node>();
        assert_send_sync::<Edge>();
    }

    // Test 300: Node and Edge are Send + Sync
    #[test]
    fn test_node_edge_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Node>();
        assert_send_sync::<Edge>();
    }
}
