use async_trait::async_trait;
use crate::core::error::{Result, Error};
use crate::models::event::Event;
use crate::core::traits::Transporter;
use crate::transport::network::NetworkLayer;
use crate::transport::serializer::{Serializer, JsonSerializer};
use std::sync::Arc;
use tokio_retry::{Retry, strategy::ExponentialBackoff};
use tracing::{debug, error};

/// ManagedTransporter: Standardizes event delivery by orchestrating
/// serialization, retries, and protocol implementation.
#[derive(Debug)]
pub struct ManagedTransporter {
    network: Arc<dyn NetworkLayer>,
    serializer: Arc<dyn Serializer>,
    retry_strategy: std::iter::Take<ExponentialBackoff>,
}

impl ManagedTransporter {
    pub fn new(network: Arc<dyn NetworkLayer>) -> Self {
        Self {
            network,
            serializer: Arc::new(JsonSerializer),
            retry_strategy: ExponentialBackoff::from_millis(10).take(3),
        }
    }
}

#[async_trait]
impl Transporter for ManagedTransporter {
    async fn send(&self, event: Arc<Event>) -> Result<()> {
        let name = self.name();
        
        // Serialize using the shared event (no clone of payload)
        let data = self.serializer.serialize(&event)
            .map_err(|e| Error::Transport(format!("Serialization for {} failure: {}", name, e)))?;

        let retry_strat = self.retry_strategy.clone();
        let network = self.network.clone();
        let event_id = event.id.clone();
        let name = self.name().to_string();
        
        let name_for_err = name.clone();
        Retry::spawn(retry_strat, move || {
            let network = network.clone();
            let data = data.clone();
            let current_id = event_id.clone();
            let current_name = name.clone();
            async move {
                debug!(id=%current_id, trans=%current_name, "ManagedTransporter: Delivering bytes...");
                network.write(data).await
            }
        }).await
        .map_err(|e| {
            error!(id=%event.id, transport=%name_for_err, error=%e, "ManagedTransporter: Persistent delivery failure.");
            e
        })?;

        Ok(())
    }

    fn name(&self) -> &str {
        "ManagedTransporter"
    }
}

#[async_trait]
impl crate::core::traits::Protocol for ManagedTransporter {
    fn name(&self) -> &str {
        self.network.protocol_name()
    }

    fn protocol_type(&self) -> crate::models::event::ProtocolType {
        crate::models::event::ProtocolType::Other
    }

    async fn start(&self) -> Result<()> {
        self.network.start().await
    }

    async fn stop(&self) -> Result<()> {
        self.network.stop().await
    }
}

// Support for Debug since traits don't always have it easily
impl std::fmt::Debug for dyn Serializer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Serializer({})", self.format_name())
    }
}
