use crate::core::error::Result;
use crate::core::traits::{Protocol, Capturer, Processor, Transporter};
use crate::protocols::common::ProtocolRunner;
use crate::models::event::{Event, ProtocolType};
use std::sync::Arc;
use tracing::{info, error};
use tokio::sync::broadcast;
use async_trait::async_trait;

/// A highly modular, SOLID-compliant protocol host that uses composition.
/// It wires together a Capturer, multiple Processors, and multiple Transporters.
/// Following: DRY - Uses ProtocolRunner for common lifecycle logic.
/// Following: DIP - Depends on traits, not concrete implementations.
#[derive(Debug)]
pub struct GenericProtocol {
    name: String,
    protocol_type: ProtocolType,
    capturer: Arc<dyn Capturer>,
    processors: Vec<Arc<dyn Processor>>,
    transporters: Vec<Arc<dyn Transporter>>,
    runner: Option<Arc<ProtocolRunner>>,
}

impl GenericProtocol {
    pub fn new(name: &str, protocol_type: ProtocolType, capturer: Arc<dyn Capturer>) -> Self {
        Self {
            name: name.to_string(),
            protocol_type,
            capturer,
            processors: Vec::new(),
            transporters: Vec::new(),
            runner: None,
        }
    }

    pub fn add_processor(mut self, processor: Arc<dyn Processor>) -> Self {
        self.processors.push(processor);
        self
    }

    pub fn add_transporter(mut self, transporter: Arc<dyn Transporter>) -> Self {
        self.transporters.push(transporter);
        self
    }

    /// Starts the protocol ingestion and processing pipeline.
    /// Following: SRP - Orchestrates components without implementing their logic.
    pub async fn start(&mut self) -> Result<()> {
        info!("Protocol '{}': Starting ingestion pipeline...", self.name);
        
        let (runner, stop_rx) = ProtocolRunner::new(&self.name);
        let runner = Arc::new(runner);
        self.runner = Some(runner.clone());

        let capturer = self.capturer.clone();
        let processors = self.processors.clone();
        let transporters = self.transporters.clone();

        runner.run_loop(capturer, stop_rx, move |mut event| {
            let processors = processors.clone();
            let transporters = transporters.clone();
            
            tokio::spawn(async move {
                // Sequential Processing
                for processor in processors {
                    match processor.process(event).await {
                        Ok(Some(e)) => event = e,
                        Ok(None) => return, // Dropped
                        Err(e) => {
                            error!("Processor '{}' failed: {}", processor.name(), e);
                            return;
                        }
                    }
                }

                // Concurrent Transportation
                let shared_event = Arc::new(event);
                for transporter in transporters {
                    let tx = transporter.clone();
                    let ev = shared_event.clone();
                    tokio::spawn(async move {
                        if let Err(e) = tx.send(ev).await {
                            error!("Transporter '{}' failed: {}", tx.name(), e);
                        }
                    });
                }
            });
        }).await;

        Ok(())
    }

    pub fn stop(&self) {
        if let Some(ref runner) = self.runner {
            runner.stop();
        }
    }
}

impl Protocol for GenericProtocol {
    fn name(&self) -> &str {
        &self.name
    }

    fn protocol_type(&self) -> ProtocolType {
        self.protocol_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::Event;
    
    #[derive(Debug)]
    struct MockCapturer;
    #[async_trait]
    impl Capturer for MockCapturer {
        async fn capture(&self) -> Result<Event> { Ok(Event::default()) }
        fn name(&self) -> &str { "mock-cap" }
    }

    #[derive(Debug)]
    struct MockProcessor;
    #[async_trait]
    impl Processor for MockProcessor {
        async fn process(&self, e: Event) -> Result<Option<Event>> { Ok(Some(e)) }
        fn name(&self) -> &str { "mock-proc" }
    }

    #[derive(Debug)]
    struct MockTransporter;
    #[async_trait]
    impl Transporter for MockTransporter {
        async fn send(&self, _: Arc<Event>) -> Result<()> { Ok(()) }
        fn name(&self) -> &str { "mock-trans" }
    }

    #[tokio::test]
    async fn test_generic_protocol_composition() {
        let protocol = GenericProtocol::new("test", ProtocolType::Http, Arc::new(MockCapturer))
            .add_processor(Arc::new(MockProcessor))
            .add_transporter(Arc::new(MockTransporter));
            
        assert_eq!(protocol.name(), "test");
        assert_eq!(protocol.processors.len(), 1);
        assert_eq!(protocol.transporters.len(), 1);
    }
}
