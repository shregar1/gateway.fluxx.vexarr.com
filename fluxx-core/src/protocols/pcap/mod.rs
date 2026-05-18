use async_trait::async_trait;
use crate::core::error::{Result, Error};
use crate::core::traits::{Capturer, Protocol};
use crate::models::event::{Event, ProtocolType};
use tracing::info;
use pcap::{Capture, Active};
use parking_lot::Mutex;

/// Pcap capture for cross-platform network analysis
pub struct PcapCapture {
    pub name: String,
    pub interface: String,
    cap: Mutex<Option<Capture<Active>>>,
}

impl std::fmt::Debug for PcapCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PcapCapture")
            .field("name", &self.name)
            .field("interface", &self.interface)
            .field("active", &self.cap.lock().is_some())
            .finish()
    }
}

impl PcapCapture {
    pub fn new(name: &str, interface: &str) -> Self {
        Self {
            name: name.to_string(),
            interface: interface.to_string(),
            cap: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Protocol for PcapCapture {
    fn name(&self) -> &str {
        &self.name
    }

    fn protocol_type(&self) -> crate::models::event::ProtocolType {
        crate::models::event::ProtocolType::Other
    }

    async fn start(&self) -> Result<()> {
        info!(interface=%self.interface, "Pcap: Initializing live capture engine...");
        
        let cap = Capture::from_device(self.interface.as_str())
            .map_err(|e| Error::Protocol(self.name.clone(), format!("Device lookup error: {}", e)))?
            .promisc(true)
            .snaplen(65535)
            .timeout(1000)
            .open()
            .map_err(|e| Error::Protocol(self.name.clone(), format!("Failed to open device: {}. (Try running with sudo)", e)))?;

        *self.cap.lock() = Some(cap);
        info!(interface=%self.interface, "Pcap: Live capture engine is now active.");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        *self.cap.lock() = None;
        info!(interface=%self.interface, "Pcap: Capture engine stopped.");
        Ok(())
    }
}

#[async_trait]
impl Capturer for PcapCapture {
    async fn capture(&self) -> Result<Event> {
        // We use a scope here to ensure the MutexGuard is dropped BEFORE any await points.
        // Holding a parking_lot::MutexGuard across an .await makes the future !Send.
        let capture_result = {
            let mut guard = self.cap.lock();
            let cap = guard.as_mut().ok_or_else(|| {
                Error::Protocol(self.name.clone(), "Capture engine not started".into())
            })?;

            match cap.next_packet() {
                Ok(packet) => Ok(packet.header.len),
                Err(e) => Err(e),
            }
        };

        match capture_result {
            Ok(len) => {
                let mut event = Event::new(uuid::Uuid::new_v4().to_string(), ProtocolType::Other);
                event.payload = Some(format!("Captured {} bytes", len));
                event.metadata.add("interface", &self.interface);
                Ok(event)
            }
            Err(pcap::Error::TimeoutExpired) => {
                // Safe to await here as the guard has been dropped
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                Err(Error::Protocol(self.name.clone(), "Capture timeout".into()))
            }
            Err(e) => Err(Error::Protocol(self.name.clone(), format!("Capture error: {}", e))),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}
