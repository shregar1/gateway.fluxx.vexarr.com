use async_trait::async_trait;
use crate::core::error::{Error, Result};
use crate::transport::network::NetworkLayer;
use futures_util::SinkExt;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, debug};
use url::Url;

#[derive(Debug)]
pub struct WebSocketTransport {
    url: String,
}

impl WebSocketTransport {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
        }
    }
}

#[async_trait]
impl NetworkLayer for WebSocketTransport {
    async fn write(&self, data: Vec<u8>) -> Result<()> {
        debug!("WS Delivery: Sending bytes to {}", self.url);
        
        let url = Url::parse(&self.url)
            .map_err(|e| Error::Transport(format!("Invalid URL: {}", e)))?;

        // Persistent pool could be added here later.
        let (mut ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| Error::Transport(format!("WS Connect failed: {}", e)))?;

        ws_stream.send(Message::Binary(data))
            .await
            .map_err(|e| Error::Transport(format!("WS Send failed: {}", e)))?;

        ws_stream.close(None).await.ok();
        
        Ok(())
    }

    fn protocol_name(&self) -> &str {
        "websocket"
    }

    async fn start(&self) -> Result<()> {
        info!("WebSocket Transport initialized for {}", self.url);
        Ok(())
    }
}
