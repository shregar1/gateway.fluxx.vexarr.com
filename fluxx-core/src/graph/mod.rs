use crate::models::graph::{Graph, Node};
use crate::models::event::Event;
use crate::core::traits::Processor;
use crate::core::error::Result;
use async_trait::async_trait;
use std::sync::{Arc, RwLock};
use tracing::debug;

/// GraphBuilder: A high-performance real-time service that builds the flow graph
/// incrementally from the Event pipeline. Ensures minimal memory overhead and
/// provides direct JSON access for frontend observability dashboard layers.
#[derive(Debug, Default, Clone)]
pub struct GraphBuilder {
    graph: Arc<RwLock<Graph>>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fetches the current architectural topology as a serialized JSON blob.
    pub fn export_json(&self) -> String {
        let graph = self.graph.read().expect("RwLock poisoned");
        serde_json::to_string(&*graph).unwrap_or_else(|_| "{}".to_string())
    }

    /// Internal logic for updating the logical topography based on an event.
    fn update_graph(&self, event: &Event) {
        let mut graph = self.graph.write().expect("RwLock poisoned");
        
        // 1. Identify and link the Source (Endpoint/Address)
        let source_id = event.metadata.source_ip.clone().unwrap_or_else(|| "unknown".to_string());
        let source_node = Node {
            id: source_id.clone(),
            name: source_id.clone(),
            protocol: format!("{:?}", event.protocol),
        };
        graph.nodes.insert(source_id.clone(), source_node);

        // 2. Identify and link the Target (Destination/Host)
        let target_id = event.metadata.destination_ip.clone().unwrap_or_else(|| "internal-engine".to_string());
        let target_node = Node {
            id: target_id.clone(),
            name: target_id.clone(),
            protocol: format!("{:?}", event.protocol),
        };
        graph.nodes.insert(target_id.clone(), target_node);

        // 3. Update the Edge (Request Flow)
        graph.update_edge(&source_id, &target_id, &format!("{:?}", event.protocol), event.latency_us);
    }
}

#[async_trait]
impl Processor for GraphBuilder {
    async fn process(&self, event: Event) -> Result<Option<Event>> {
        // Enforce real-time topological mapping within the event flow.
        // This is a zero-latency operation for the pipeline due to Arc mutation.
        debug!(id = %event.id, "GraphBuilder: Mapping request flow.");
        self.update_graph(&event);

        // Continue the flux-flow (unmodified event stream).
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "GraphBuilder"
    }
}
