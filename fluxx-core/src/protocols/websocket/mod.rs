use async_trait::async_trait;
use crate::core::error::{Error, Result};
use crate::models::event::{Event, ProtocolType};
use crate::core::traits::{Capturer, Protocol};
use crate::protocols::common::CaptureChannel;
use crate::protocols::server::StreamHandler;
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio_tungstenite::accept_async;
use tracing::{error, info, debug, warn};
use std::sync::Arc;

/// WebSocket protocol module focused on high-performance stream observation.
/// Utilizes a shared CaptureChannel for bridge-based event delivery.
#[derive(Debug)]
pub struct WebSocketCapturer {
    pub name: String,
    pub address: String,
    channel: Arc<CaptureChannel>,
}

impl WebSocketCapturer {
    pub fn new(name: &str, address: &str) -> Self {
        Self {
            name: name.to_string(),
            address: address.to_string(),
            channel: Arc::new(CaptureChannel::new(name, 2048)),
        }
    }
}

#[async_trait]
impl StreamHandler for WebSocketCapturer {
    async fn handle(&self, stream: TcpStream, peer: std::net::SocketAddr) -> Result<()> {
        debug!(peer=%peer, "WS: Handling incoming handshake.");
        
        let ws_stream = accept_async(stream).await
            .map_err(|e| Error::Internal(format!("WS Handshake failure for {}: {}", peer, e)))?;

        let (_, mut reader) = ws_stream.split();
        let tx = self.channel.sender();

        while let Some(msg) = reader.next().await {
            match msg {
                Ok(message) if message.is_binary() || message.is_text() => {
                    let mut event = Event::new(uuid::Uuid::new_v4().to_string(), ProtocolType::Ws);
                    event.payload = Some(String::from_utf8_lossy(&message.into_data()).to_string());
                    event.metadata.source_ip = Some(peer.ip().to_string());
                    
                    if let Err(_) = tx.send(event).await {
                        warn!(peer=%peer, "WS: Internal buffer full or closed, dropping connection.");
                        break;
                    }
                }
                Ok(_) => debug!("WS: Protocol control message ignored."),
                Err(e) => {
                    error!(error=%e, peer=%peer, "WS: Stream interrupted.");
                    break;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Protocol for WebSocketCapturer {
    fn name(&self) -> &str {
        &self.name
    }

    fn protocol_type(&self) -> crate::models::event::ProtocolType {
        crate::models::event::ProtocolType::Ws
    }

    async fn start(&self) -> Result<()> {
        info!(addr=%self.address, "WS: Initializing listener daemon.");
        // Note: The actual TCP server is orchestrated by the Agent via TcpServer host.
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        info!(name=%self.name, "WS: Finalizing cleanup.");
        Ok(())
    }
}

#[async_trait]
impl Capturer for WebSocketCapturer {
    async fn capture(&self) -> Result<Event> {
        self.channel.capture().await
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_capturer_init() {
        let ws = WebSocketCapturer::new("TestWS", "127.0.0.1:8080");
        assert_eq!(ws.name, "TestWS");
    }
}
