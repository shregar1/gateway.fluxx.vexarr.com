use async_trait::async_trait;
use crate::core::error::Result;
use crate::core::traits::Protocol;

/// A minimal dummy protocol for testing abstraction layers.
/// Focuses purely on protocol-specific setup and cleanup.
#[derive(Debug)]
pub struct DummyProtocol {
    name: String,
}

impl DummyProtocol {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl Protocol for DummyProtocol {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<()> {
        // Unique protocol setup: (e.g., port listener, queue connect)
        // No boilerplate logs (handled by Agent)
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        // Unique protocol cleanup: (e.g., close connections, flush buffers)
        // No boilerplate logs (handled by Agent)
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        Ok(())
    }
}
