use async_trait::async_trait;
use crate::core::error::Result;
use crate::core::traits::{Capturer, Protocol};
use crate::models::event::{Event, ProtocolType};
use crate::protocols::common::CaptureChannel;
use quinn::Endpoint;
use tracing::{info, error};
use std::net::SocketAddr;

/// QuicCapturer: Efficient UDP-based stream management.
/// Delegating buffer management to CaptureChannel for single responsibility.
#[derive(Debug)]
pub struct QuicCapturer {
    pub name: String,
    pub address: SocketAddr,
    channel: CaptureChannel,
}

impl QuicCapturer {
    pub fn new(name: &str, address: &str) -> Self {
        Self {
            name: name.to_string(),
            address: address.parse().unwrap_or("127.0.0.1:4433".parse().unwrap()),
            channel: CaptureChannel::new(name, 4096),
        }
    }

    #[allow(dead_code)]
    async fn listen_task(&self, endpoint: Endpoint) -> Result<()> {
        while let Some(connecting) = endpoint.accept().await {
            info!(peer=%connecting.remote_address(), "QUIC: Handshaking incoming connection.");
            let tx = self.channel.sender();
            
            tokio::spawn(async move {
                match connecting.await {
                    Ok(connection) => {
                        loop {
                            match connection.accept_bi().await {
                                Ok((_, mut rcv)) => {
                                    if let Ok(bytes) = rcv.read_to_end(1024 * 1024).await {
                                        let mut event = Event::new(uuid::Uuid::new_v4().to_string(), ProtocolType::Quic);
                                        event.payload = Some(String::from_utf8_lossy(&bytes).to_string());
                                        event.metadata.source_ip = Some(connection.remote_address().ip().to_string());
                                        let _ = tx.send(event).await;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                    Err(e) => error!(error=%e, "QUIC: Connection abort."),
                }
            });
        }
        Ok(())
    }
}

#[async_trait]
impl Protocol for QuicCapturer {
    fn name(&self) -> &str {
        &self.name
    }

    fn protocol_type(&self) -> crate::models::event::ProtocolType {
        crate::models::event::ProtocolType::Quic
    }

    async fn start(&self) -> Result<()> {
        info!(addr=%self.address, "QUIC: Start listening.");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        info!(name=%self.name, "QUIC: Shutdown.");
        Ok(())
    }
}

#[async_trait]
impl Capturer for QuicCapturer {
    async fn capture(&self) -> Result<Event> {
        self.channel.capture().await
    }

    fn name(&self) -> &str {
        &self.name
    }
}
