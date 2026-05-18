use async_trait::async_trait;
use crate::core::error::{Error, Result};
use crate::core::event::{Event, ProtocolType};
use crate::core::traits::{Capturer, Protocol};
use crate::protocols::server::StreamHandler;
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::accept_async;
use tracing::{error, info, debug};

/// WebSocket protocol module refactored for minimal coupling.
/// Focuses purely on WebSocket semantics while delegating TCP orchestration.
#[derive(Debug)]
pub struct WebSocketCapturer {
    pub name: String,
    pub address: String,
    tx: mpsc::Sender<Event>,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Event>>>,
}

impl WebSocketCapturer {
    pub fn new(name: &str, address: &str) -> Self {
        let (tx, rx) = mpsc::channel(2048);
        Self {
            name: name.to_string(),
            address: address.to_string(),
            tx,
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
        }
    }
}

#[async_trait]
impl StreamHandler for WebSocketCapturer {
    async fn handle(&self, stream: TcpStream, peer: std::net::SocketAddr) -> Result<()> {
        debug!("WS: Upgrading connection from {}", peer);
        
        let ws_stream = accept_async(stream).await
            .map_err(|e| Error::Internal(format!("WS Handshake with {} failed: {}", peer, e)))?;

        let (_, mut reader) = ws_stream.split();

        while let Some(msg) = reader.next().await {
            match msg {
                Ok(message) => {
                    if message.is_text() || message.is_binary() {
                        let mut event = Event::new(uuid::Uuid::new_v4().to_string(), ProtocolType::Ws);
                        event.payload = Some(message.into_data());
                        event.metadata.source_ip = Some(peer.ip().to_string());
                        
                        self.tx.send(event).await
                            .map_err(|e| Error::Internal(format!("WS task send failed: {}", e)))?;
                    }
                }
                Err(e) => {
                    error!("WS: Connection error from {}: {}", peer, e);
                    break;
                }
            }
        }
        Ok(())
    }
}

// Since Protocol and Capturer both need and take &self, we can implement them normally.
// The Agent will spawn the server loop.
#[async_trait]
impl Protocol for WebSocketCapturer {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<()> {
        info!("WS: Protocol initialized for {} (TCP server managed by Agent)", self.address);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        info!("WS: Cleaning up protocol {}.", self.name);
        Ok(())
    }
}

#[async_trait]
impl Capturer for WebSocketCapturer {
    async fn capture(&self) -> Result<Event> {
        let mut rx = self.rx.lock().await;
        rx.recv().await.ok_or_else(|| Error::Capture("WS channel closed".to_string()))
    }

    fn name(&self) -> &str {
        &self.name
    }
}
