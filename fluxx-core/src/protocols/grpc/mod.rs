use async_trait::async_trait;
use crate::core::error::Result;
use crate::core::traits::{Capturer, Protocol};
use crate::protocols::common::CaptureChannel;
use crate::models::event::Event;
use tracing::info;
use std::net::SocketAddr;

/// GrpcCapturer: Support for high-performance gRPC interceptors.
#[derive(Debug)]
pub struct GrpcCapturer {
    pub name: String,
    pub address: SocketAddr,
    channel: CaptureChannel,
}

impl GrpcCapturer {
    pub fn new(name: &str, address: &str) -> Self {
        Self {
            name: name.to_string(),
            address: address.parse().unwrap_or("127.0.0.1:50051".parse().unwrap()),
            channel: CaptureChannel::new(name, 2048),
        }
    }
}

#[async_trait]
impl Protocol for GrpcCapturer {
    fn name(&self) -> &str {
        &self.name
    }

    fn protocol_type(&self) -> crate::models::event::ProtocolType {
        crate::models::event::ProtocolType::Grpc
    }

    async fn start(&self) -> Result<()> {
        info!(addr=%self.address, "gRPC: Interceptors active.");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        info!(name=%self.name, "gRPC: Finalized.");
        Ok(())
    }
}

#[async_trait]
impl Capturer for GrpcCapturer {
    async fn capture(&self) -> Result<Event> {
        self.channel.capture().await
    }

    fn name(&self) -> &str {
        &self.name
    }
}
