use crate::core::error::Result;
use crate::core::traits::{Capturer, Processor, Transporter, Protocol};
use crate::processing::Pipeline;
use crate::core::runtime::Lifecycle;
use std::sync::Arc;
use tracing::{info, error, debug};

/// Agent: The high-level orchestrator for the Fluxx network agent.
pub struct Agent {
    capturers: Vec<Arc<dyn Capturer>>,
    processors: Vec<Arc<dyn Processor>>,
    transporters: Vec<Arc<dyn Transporter>>,
    protocols: Vec<Arc<dyn Protocol>>,
    lifecycle: Lifecycle,
}

impl Agent {
    pub fn new() -> Self {
        Self {
            capturers: Vec::new(),
            processors: Vec::new(),
            transporters: Vec::new(),
            protocols: Vec::new(),
            lifecycle: Lifecycle::new(),
        }
    }

    pub fn add_protocol(&mut self, protocol: Arc<dyn Protocol>) {
        info!("Agent: Registering protocol '{}'", protocol.name());
        self.protocols.push(protocol);
    }

    pub fn add_capturer(&mut self, capturer: Arc<dyn Capturer>) {
        self.capturers.push(capturer);
    }

    pub fn add_processor(&mut self, processor: Arc<dyn Processor>) {
        self.processors.push(processor);
    }

    pub fn add_transporter(&mut self, transporter: Arc<dyn Transporter>) {
        self.transporters.push(transporter);
    }

    pub async fn start(&self) -> Result<()> {
        info!("Agent: Coordinating startup for {} protocols...", self.protocols.len());

        for protocol in &self.protocols {
            info!("Agent: Protocol '{}' starting...", protocol.name());
            if let Err(e) = protocol.start().await {
                error!("Agent: Failed to start protocol '{}': {}", protocol.name(), e);
                return Err(e);
            }
            debug!("Agent: Protocol '{}' is now active.", protocol.name());
        }

        let pipeline = Pipeline::new(
            self.capturers.clone(),
            self.processors.clone(),
            self.transporters.clone(),
        );

        pipeline.run(self.lifecycle.token()).await?;
        self.lifecycle.wait_for_shutdown().await;

        for protocol in &self.protocols {
            info!("Agent: Protocol '{}' stopping...", protocol.name());
            if let Err(e) = protocol.stop().await {
                error!("Agent: Graceful stop failed for '{}': {}", protocol.name(), e);
            }
        }

        info!("Agent: All operations completed.");
        Ok(())
    }

    pub fn shutdown(&self) {
        info!("Agent: Global shutdown initiated.");
        self.lifecycle.shutdown();
    }
}

impl Default for Agent {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 121: Agent new
    #[test]
    fn test_agent_new() {
        let agent = Agent::new();
        // Agent should be empty on creation
        assert!(agent.capturers.is_empty());
        assert!(agent.processors.is_empty());
        assert!(agent.transporters.is_empty());
        assert!(agent.protocols.is_empty());
    }

    // Test 122: Agent default
    #[test]
    fn test_agent_default() {
        let agent: Agent = Default::default();
        assert!(agent.capturers.is_empty());
        assert!(agent.processors.is_empty());
    }

    // Test 123: Agent add_capturer
    #[test]
    fn test_agent_add_capturer() {
        use crate::protocols::common::CaptureChannel;
        let mut agent = Agent::new();
        let capturer = Arc::new(CaptureChannel::new("test", 100));
        agent.add_capturer(capturer);
        assert_eq!(agent.capturers.len(), 1);
    }

    // Test 124: Agent add_processor
    #[test]
    fn test_agent_add_processor() {
        use crate::processing::TopologyProcessor;
        let mut agent = Agent::new();
        let processor = Arc::new(TopologyProcessor::new("proj", "env", "svc"));
        agent.add_processor(processor);
        assert_eq!(agent.processors.len(), 1);
    }

    // Test 125: Agent add_transporter
    #[test]
    fn test_agent_add_transporter() {
        use crate::transport::stdout::StdoutTransporter;
        let mut agent = Agent::new();
        let transporter = Arc::new(StdoutTransporter::new());
        agent.add_transporter(transporter);
        assert_eq!(agent.transporters.len(), 1);
    }

    // Test 126: Agent shutdown
    #[test]
    fn test_agent_shutdown() {
        let agent = Agent::new();
        agent.shutdown();
        assert!(agent.lifecycle.token().is_cancelled());
    }

    // Test 127: Agent multiple capturers
    #[test]
    fn test_agent_multiple_capturers() {
        use crate::protocols::common::CaptureChannel;
        let mut agent = Agent::new();
        agent.add_capturer(Arc::new(CaptureChannel::new("c1", 100)));
        agent.add_capturer(Arc::new(CaptureChannel::new("c2", 100)));
        agent.add_capturer(Arc::new(CaptureChannel::new("c3", 100)));
        assert_eq!(agent.capturers.len(), 3);
    }

    // Test 128: Agent multiple processors
    #[test]
    fn test_agent_multiple_processors() {
        use crate::processing::TopologyProcessor;
        let mut agent = Agent::new();
        agent.add_processor(Arc::new(TopologyProcessor::new("p1", "e1", "s1")));
        agent.add_processor(Arc::new(TopologyProcessor::new("p2", "e2", "s2")));
        assert_eq!(agent.processors.len(), 2);
    }

    // Test 129: Agent multiple transporters
    #[test]
    fn test_agent_multiple_transporters() {
        use crate::transport::stdout::StdoutTransporter;
        let mut agent = Agent::new();
        agent.add_transporter(Arc::new(StdoutTransporter::new()));
        agent.add_transporter(Arc::new(StdoutTransporter::new()));
        assert_eq!(agent.transporters.len(), 2);
    }
}
