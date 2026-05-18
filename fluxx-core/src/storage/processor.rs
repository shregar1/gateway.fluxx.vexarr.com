use crate::storage::event_store::EventStore;
use crate::models::event::Event;
use crate::core::traits::Processor;
use crate::core::error::Result;
use async_trait::async_trait;
use tracing::debug;
use std::sync::Arc;

/// StorageProcessor: A middleware component that persists events into an `EventStore` 
/// as they flow through the synchronous processing pipeline.
/// Enables time-travel debugging by providing a historical stream of all handled data.
#[derive(Debug, Clone)]
pub struct StorageProcessor {
    store: Arc<dyn EventStore>,
}

impl StorageProcessor {
    pub fn new(store: Arc<dyn EventStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Processor for StorageProcessor {
    async fn process(&self, event: Event) -> Result<Option<Event>> {
        // Persist the event chronologically for time-travel debugging.
        // This operation happens inside the pipeline, ensuring no data loss.
        debug!(id = %event.id, "StorageProcessor: Persisting event to store.");
        if let Err(e) = self.store.insert(event.clone()).await {
            // We log the error but don't stop the pipeline; persistence shouldn't 
            // break the live delivery flow.
            tracing::error!(error=%e, "StorageProcessor: Failed to persist event.");
        }

        // Return the original event to continue downstream (e.g. to transporters).
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "StorageProcessor"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::{Event, EventSeverity, ProtocolType};
    use crate::storage::in_memory::InMemoryEventStore;

    fn make_event(id: &str) -> Event {
        Event::new(id.to_string(), ProtocolType::Http)
    }

    #[test]
    fn test_processor_name() {
        let store: Arc<dyn EventStore> = Arc::new(InMemoryEventStore::new());
        let p = StorageProcessor::new(store);
        assert_eq!(p.name(), "StorageProcessor");
    }

    #[tokio::test]
    async fn test_process_returns_event() {
        let store: Arc<dyn EventStore> = Arc::new(InMemoryEventStore::new());
        let p = StorageProcessor::new(store);
        let event = make_event("e1");
        let result = p.process(event.clone()).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().id, "e1");
    }

    #[tokio::test]
    async fn test_process_persists_event() {
        let backing = Arc::new(InMemoryEventStore::new());
        let store: Arc<dyn EventStore> = backing.clone();
        let p = StorageProcessor::new(store);
        p.process(make_event("e1")).await.unwrap();
        assert_eq!(backing.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_process_multiple_events() {
        let backing = Arc::new(InMemoryEventStore::new());
        let store: Arc<dyn EventStore> = backing.clone();
        let p = StorageProcessor::new(store);
        for i in 0..5 {
            p.process(make_event(&format!("e{}", i))).await.unwrap();
        }
        assert_eq!(backing.len().await.unwrap(), 5);
    }

    #[tokio::test]
    async fn test_process_preserves_event_fields() {
        let backing = Arc::new(InMemoryEventStore::new());
        let store: Arc<dyn EventStore> = backing.clone();
        let p = StorageProcessor::new(store);
        let mut event = make_event("id-42");
        event.severity = EventSeverity::High;
        event.project_id = Some("p1".to_string());

        let out = p.process(event).await.unwrap().unwrap();
        assert_eq!(out.id, "id-42");
        assert_eq!(out.severity, EventSeverity::High);
        assert_eq!(out.project_id, Some("p1".to_string()));
    }

    #[tokio::test]
    async fn test_process_different_protocols() {
        let backing = Arc::new(InMemoryEventStore::new());
        let store: Arc<dyn EventStore> = backing.clone();
        let p = StorageProcessor::new(store);
        p.process(Event::new("http".into(), ProtocolType::Http)).await.unwrap();
        p.process(Event::new("tcp".into(), ProtocolType::Tcp)).await.unwrap();
        p.process(Event::new("udp".into(), ProtocolType::Udp)).await.unwrap();
        assert_eq!(backing.len().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_processor_clone_shares_store() {
        let backing = Arc::new(InMemoryEventStore::new());
        let store: Arc<dyn EventStore> = backing.clone();
        let p1 = StorageProcessor::new(store);
        let p2 = p1.clone();
        p1.process(make_event("a")).await.unwrap();
        p2.process(make_event("b")).await.unwrap();
        assert_eq!(backing.len().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_process_empty_id() {
        let backing = Arc::new(InMemoryEventStore::new());
        let store: Arc<dyn EventStore> = backing.clone();
        let p = StorageProcessor::new(store);
        let result = p.process(make_event("")).await.unwrap();
        assert!(result.is_some());
        assert_eq!(backing.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_process_returns_some_always() {
        let backing = Arc::new(InMemoryEventStore::new());
        let store: Arc<dyn EventStore> = backing.clone();
        let p = StorageProcessor::new(store);
        for _ in 0..10 {
            let result = p.process(make_event("x")).await.unwrap();
            assert!(result.is_some());
        }
    }
}
