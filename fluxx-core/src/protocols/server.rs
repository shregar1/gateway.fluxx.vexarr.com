use crate::core::error::Result;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};
use std::sync::Arc;
use async_trait::async_trait;

/// Interface for protocol-specific connection handlers.
/// Implement this to define how raw TCP streams are processed.
#[async_trait]
pub trait StreamHandler: Send + Sync + 'static {
    async fn handle(&self, stream: TcpStream, peer: std::net::SocketAddr) -> Result<()>;
}

/// A reusable TCP server host that handles the standard binding and accept loop.
/// Decouples generic networking from protocol-specific logic.
pub struct TcpServer {
    address: String,
}

impl TcpServer {
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_string(),
        }
    }

    /// Runs the TCP accept loop and spawns individual tasks for each connection.
    pub async fn listen<H: StreamHandler>(&self, handler: Arc<H>) -> Result<()> {
        info!("Server: Binding to {}...", self.address);
        let listener = TcpListener::bind(&self.address).await
            .map_err(|e| crate::core::error::Error::Internal(format!("Failed to bind to {}: {}", self.address, e)))?;

        info!("Server: Listening for connections on {}", self.address);
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let handler_clone = handler.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handler_clone.handle(stream, peer).await {
                            error!("Server: Connection handler error for {}: {}", peer, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Server: Accept error: {}", e);
                }
            }
        }
    }
}
