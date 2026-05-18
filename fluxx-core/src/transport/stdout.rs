use async_trait::async_trait;
use crate::core::error::Result;
use crate::models::event::Event;
use crate::core::traits::Transporter;
use tracing::{info, debug};
use std::sync::Arc;

#[derive(Debug)]
pub struct StdoutTransporter;

#[async_trait]
impl Transporter for StdoutTransporter {
    async fn send(&self, event: Arc<Event>) -> Result<()> {
        info!(
            protocol=?event.protocol, 
            id=%event.id, 
            latency=%event.latency_us,
            "Stdout: Dispatching event."
        );
        
        if let Some(p) = event.payload.as_ref() {
            debug!(size=p.len(), "Payload preview: {}", p);
        }
        
        debug!(
            source=?event.metadata.source_ip, 
            ua=?event.metadata.user_agent, 
            extras=?event.metadata.extra,
            "Metadata details."
        );
        
        Ok(())
    }

    fn name(&self) -> &str {
        "StdoutTransporter"
    }
}

impl StdoutTransporter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for StdoutTransporter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::{Event, ProtocolType};

    // Test 137: StdoutTransporter new
    #[test]
    fn test_stdout_transporter_new() {
        let transporter = StdoutTransporter::new();
        assert_eq!(transporter.name(), "StdoutTransporter");
    }

    // Test 138: StdoutTransporter default
    #[test]
    fn test_stdout_transporter_default() {
        let transporter: StdoutTransporter = Default::default();
        assert_eq!(transporter.name(), "StdoutTransporter");
    }

    // Test 139: StdoutTransporter name
    #[test]
    fn test_stdout_transporter_name() {
        let transporter = StdoutTransporter::new();
        assert_eq!(transporter.name(), "StdoutTransporter");
    }

    // Test 140: StdoutTransporter send with no payload
    #[tokio::test]
    async fn test_stdout_send_no_payload() {
        let transporter = StdoutTransporter::new();
        let event = Arc::new(Event::new("evt-1".to_string(), ProtocolType::Http));
        let result = transporter.send(event).await;
        assert!(result.is_ok());
    }

    // Test 141: StdoutTransporter send with payload
    #[tokio::test]
    async fn test_stdout_send_with_payload() {
        let transporter = StdoutTransporter::new();
        let event = Arc::new(Event::new("evt-1".to_string(), ProtocolType::Http)
            .with_payload("test data".to_string()));
        let result = transporter.send(event).await;
        assert!(result.is_ok());
    }

    // Test 142: StdoutTransporter send with metadata
    #[tokio::test]
    async fn test_stdout_send_with_metadata() {
        let transporter = StdoutTransporter::new();
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.metadata.source_ip = Some("127.0.0.1".to_string());
        event.metadata.user_agent = Some("test-agent".to_string());
        let result = transporter.send(Arc::new(event)).await;
        assert!(result.is_ok());
    }

    // Test 143: StdoutTransporter implements Transporter
    fn assert_transporter_impl<T: Transporter>() {}
    #[test]
    fn test_stdout_implements_transporter() {
        assert_transporter_impl::<StdoutTransporter>();
    }

    // Test 144: StdoutTransporter debug format
    #[test]
    fn test_stdout_debug() {
        let transporter = StdoutTransporter::new();
        let debug_str = format!("{:?}", transporter);
        assert!(debug_str.contains("StdoutTransporter"));
    }
}
