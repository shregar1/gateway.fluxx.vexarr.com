use tokio::sync::{mpsc, broadcast};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::models::event::Event;
use crate::core::error::{Result, Error};
use async_trait::async_trait;
use crate::core::traits::Capturer;
use tracing::{info, error};

/// Shared component for protocols that use internal buffering (MPSC)
/// to bridge between async collectors and the synchronous pipeline demand.
#[derive(Debug)]
pub struct CaptureChannel {
    pub name: String,
    tx: mpsc::Sender<Event>,
    rx: Arc<Mutex<mpsc::Receiver<Event>>>,
}

impl CaptureChannel {
    pub fn new(name: &str, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            name: name.to_string(),
            tx,
            rx: Arc::new(Mutex::new(rx)),
        }
    }

    pub fn sender(&self) -> mpsc::Sender<Event> {
        self.tx.clone()
    }

    pub async fn recv(&self) -> Result<Event> {
        let mut rx = self.rx.lock().await;
        rx.recv().await.ok_or_else(|| Error::Capture(format!("Channel for {} closed", self.name)))
    }
}

/// Generic implementation allows protocols to easily adhere to the Capturer trait.
#[async_trait]
impl Capturer for CaptureChannel {
    async fn capture(&self) -> Result<Event> {
        self.recv().await
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A reusable component that handles the ingestion loop for any protocol.
/// Following: DRY - Eliminates duplicated lifecycle logic in start/stop methods.
/// Following: SRP - Focuses purely on reliable task execution and signal handling.
pub struct ProtocolRunner {
    pub name: String,
    stop_tx: broadcast::Sender<()>,
}

impl ProtocolRunner {
    pub fn new(name: &str) -> (Self, broadcast::Receiver<()>) {
        let (tx, rx) = broadcast::channel(1);
        (
            Self {
                name: name.to_string(),
                stop_tx: tx,
            },
            rx,
        )
    }

    /// Runs the ingestion loop given a capturer and a way to handle results.
    /// Encapsulates the boilerplate tokio::spawn and loop logic.
    pub async fn run_loop<C, F>(
        &self,
        capturer: Arc<C>,
        mut stop_rx: broadcast::Receiver<()>,
        mut handler: F,
    ) where
        C: Capturer + 'static,
        F: FnMut(Event) + Send + 'static,
    {
        let name = self.name.clone();
        tokio::spawn(async move {
            info!("Runner '{}': Ingestion loop active.", name);
            loop {
                tokio::select! {
                    _ = stop_rx.recv() => {
                        info!("Runner '{}': Shutdown signal received.", name);
                        break;
                    }
                    result = capturer.capture() => {
                        match result {
                            Ok(event) => handler(event),
                            Err(e) => error!("Runner '{}': Capture failed: {}", name, e),
                        }
                    }
                }
            }
        });
    }

    pub fn stop(&self) {
        let _ = self.stop_tx.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::{Event, ProtocolType};

    // Test 111: CaptureChannel new
    #[test]
    fn test_capture_channel_new() {
        let channel = CaptureChannel::new("test-channel", 100);
        assert_eq!(channel.name, "test-channel");
    }

    // Test 112: CaptureChannel sender
    #[test]
    fn test_capture_channel_sender() {
        let channel = CaptureChannel::new("test-channel", 100);
        let _sender = channel.sender();
    }

    // Test 113: CaptureChannel send and recv
    #[tokio::test]
    async fn test_capture_channel_send_recv() {
        let channel = CaptureChannel::new("test-channel", 100);
        let sender = channel.sender();
        let event = Event::new("evt-1".to_string(), ProtocolType::Http);
        
        sender.send(event.clone()).await.unwrap();
        let received = channel.recv().await.unwrap();
        
        assert_eq!(received.id, "evt-1");
    }

    // Test 114: CaptureChannel multiple events
    #[tokio::test]
    async fn test_capture_channel_multiple() {
        let channel = CaptureChannel::new("test-channel", 100);
        let sender = channel.sender();
        
        sender.send(Event::new("evt-1".to_string(), ProtocolType::Http)).await.unwrap();
        sender.send(Event::new("evt-2".to_string(), ProtocolType::Ws)).await.unwrap();
        sender.send(Event::new("evt-3".to_string(), ProtocolType::Grpc)).await.unwrap();
        
        let e1 = channel.recv().await.unwrap();
        let e2 = channel.recv().await.unwrap();
        let e3 = channel.recv().await.unwrap();
        
        assert_eq!(e1.id, "evt-1");
        assert_eq!(e2.id, "evt-2");
        assert_eq!(e3.id, "evt-3");
    }

    // Test 115: CaptureChannel name method
    #[test]
    fn test_capture_channel_name() {
        let channel = CaptureChannel::new("my-channel", 100);
        assert_eq!(channel.name(), "my-channel");
    }

    // Test 116: CaptureChannel capturer trait name
    #[tokio::test]
    async fn test_capture_channel_capturer_name() {
        let channel = CaptureChannel::new("trait-channel", 100);
        assert_eq!(Capturer::name(&channel), "trait-channel");
    }

    // Test 117: CaptureChannel buffer capacity
    #[test]
    fn test_capture_channel_capacity() {
        // Channel is created with specified capacity
        let channel = CaptureChannel::new("test", 10);
        let _ = channel.sender();
    }

    // Test 118: CaptureChannel debug format
    #[test]
    fn test_capture_channel_debug() {
        let channel = CaptureChannel::new("test-channel", 100);
        let debug_str = format!("{:?}", channel);
        assert!(debug_str.contains("test-channel"));
    }

    // Test 119: Empty channel recv times out (sender is held by the channel itself,
    // so the receiver will never see a close — verify it simply blocks).
    #[tokio::test]
    async fn test_capture_channel_empty_recv() {
        let channel = CaptureChannel::new("test", 1);
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            channel.recv(),
        ).await;
        assert!(result.is_err(), "recv should not return while no messages are sent");
    }

    // Test 120: CaptureChannel implements Capturer
    fn assert_capturer_impl<T: Capturer>() {}
    #[test]
    fn test_capture_channel_implements_trait() {
        assert_capturer_impl::<CaptureChannel>();
    }
}
