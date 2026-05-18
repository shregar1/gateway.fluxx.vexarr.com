use async_trait::async_trait;
use crate::core::error::Result;
use std::fmt::Debug;

/// Abstraction for raw binary delivery to remote network targets.
/// Focuses purely on protocol-specific delivery (HTTP, WS, QUIC, etc.)
#[async_trait]
pub trait NetworkLayer: Send + Sync + Debug {
    async fn write(&self, data: Vec<u8>) -> Result<()>;
    fn protocol_name(&self) -> &str;
    
    // Lifecycle hooks for persistent connections
    async fn start(&self) -> Result<()> { Ok(()) }
    async fn stop(&self) -> Result<()> { Ok(()) }
}
