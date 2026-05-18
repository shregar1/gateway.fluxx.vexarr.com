use crate::models::explanation::StructuredExplanation;
use crate::models::event::Event;
use crate::analysis::insights::Insight;
use tracing::{info, debug};

/// ExplainEngine: Utilizing high-performance AI-driven interpretation of 
/// complex network telemetry and protocol anomalies.
#[derive(Debug, Default)]
pub struct ExplainEngine {
    // Note: Future feature to integrate with LLM (e.g., Vertex AI / OpenAI)
}

impl ExplainEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Generates a structured interpretation of a specific Event (Request/Block).
    pub async fn explain_event(&self, event: &Event) -> StructuredExplanation {
        info!(id = %event.id, "ExplainEngine: Analyzing event telemetry.");
        debug!(protocol = ?event.protocol, "Explain: Interrogating protocol metadata.");
        
        let message = format!("{:?} captured with {}us latency.", event.protocol, event.latency_us);
        StructuredExplanation::mock(&message)
    }

    /// Generates a structured interpretation of a high-level Anomaly (Insight).
    pub async fn explain_insight(&self, insight: &Insight) -> StructuredExplanation {
        info!(protocol = ?insight.protocol, "ExplainEngine: Interrogating protocol anomaly.");
        
        StructuredExplanation::mock(&insight.message)
    }
}
