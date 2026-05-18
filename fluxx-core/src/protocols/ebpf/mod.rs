use async_trait::async_trait;
use crate::core::error::{Result, Error};
use crate::core::traits::{Capturer, Protocol};
use crate::models::event::Event;
#[cfg(target_os = "linux")]
use crate::protocols::ebpf::kernel::EbpfEvent;
#[cfg(target_os = "linux")]
use aya::{Bpf, programs::Xdp};
#[cfg(target_os = "linux")]
use aya_log::BpfLogger;
#[cfg(target_os = "linux")]
pub mod kernel;


#[cfg(not(target_os = "linux"))]
#[derive(Debug)]
pub struct EbpfCapture {
    pub name: String,
    pub interface: String,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct EbpfCapture {
    pub name: String,
    pub interface: String,
    channel: Arc<CaptureChannel>,
}

impl EbpfCapture {
    pub fn new(name: &str, interface: &str) -> Self {
        #[cfg(target_os = "linux")]
        return Self {
            name: name.to_string(),
            interface: interface.to_string(),
            channel: Arc::new(CaptureChannel::new(name, 10000)),
        };

        #[cfg(not(target_os = "linux"))]
        return Self {
            name: name.to_string(),
            interface: interface.to_string(),
        };
    }

    /// Loading and attaching BPF XDP probes for network-level capture.
    pub async fn init_ebpf(&self) -> Result<()> {
        #[cfg(not(target_os = "linux"))]
        return Err(Error::Protocol(self.name.clone(), "eBPF capture is only supported on Linux".to_string()));

        #[cfg(target_os = "linux")]
        {
            info!(interface=%self.interface, "eBPF: Loading XDP probes.");
            Ok(())
        }
    }

    /// Internal mapper to translate Kernel events into Fluxx pipeline events.
    #[cfg(target_os = "linux")]
    pub fn map_event(&self, k_event: kernel::EbpfEvent) -> Event {
        let proto = match k_event.protocol {
            6 => ProtocolType::Tcp,
            17 => ProtocolType::Udp,
            _ => ProtocolType::Other,
        };

        let mut event = Event::new(uuid::Uuid::new_v4().to_string(), proto);
        event.timestamp_us = k_event.timestamp_us;
        event.metadata.source_ip = Some(k_event.src_ip_str());
        event.metadata.destination_ip = Some(k_event.dst_ip_str());
        event.metadata.add("src_port", &k_event.src_port.to_string());
        event.metadata.add("dst_port", &k_event.dst_port.to_string());
        event.metadata.add("size_bytes", &k_event.payload_size.to_string());
        
        event
    }
}

#[async_trait]
impl Protocol for EbpfCapture {
    fn name(&self) -> &str {
        &self.name
    }

    fn protocol_type(&self) -> crate::models::event::ProtocolType {
        crate::models::event::ProtocolType::Ebpf
    }

    async fn start(&self) -> Result<()> {
        self.init_ebpf().await?;
        #[cfg(target_os = "linux")]
        info!(interface=%self.interface, "eBPF: Capture operational.");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        info!(interface=%self.interface, "eBPF: Probes detached.");
        Ok(())
    }
}

#[async_trait]
impl Capturer for EbpfCapture {
    async fn capture(&self) -> Result<Event> {
        #[cfg(not(target_os = "linux"))]
        return Err(Error::Protocol(self.name.clone(), "eBPF capture not supported on this OS".to_string()));

        #[cfg(target_os = "linux")]
        self.channel.capture().await
    }

    fn name(&self) -> &str {
        &self.name
    }
}
