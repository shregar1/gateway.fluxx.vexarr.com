//! Write-through EventStore (traces): generic over any hot + cold
//! pair. Same shape as `write_through_log`.

use crate::core::error::Result;
use crate::models::event::{Event, EventSeverity, ProtocolType};
use crate::storage::event_store::{EventStore, TimeWindow};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Debug)]
pub struct WriteThroughEventStore {
    hot: Arc<dyn EventStore>,
    cold: Arc<dyn EventStore>,
    cold_tx: mpsc::Sender<Event>,
}

impl WriteThroughEventStore {
    pub fn new(
        hot: Arc<dyn EventStore>,
        cold: Arc<dyn EventStore>,
        channel_capacity: usize,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Event>(channel_capacity);
        let cold_drain = cold.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                if let Err(e) = cold_drain.insert(event).await {
                    warn!("WriteThroughEventStore: cold insert failed: {e}");
                }
            }
        });
        Self { hot, cold, cold_tx: tx }
    }
}

#[async_trait]
impl EventStore for WriteThroughEventStore {
    async fn insert(&self, event: Event) -> Result<()> {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.cold_tx.try_send(event.clone()) {
            warn!("WriteThroughEventStore: cold channel saturated, dropping durable copy");
        }
        self.hot.insert(event).await
    }

    async fn query(&self, window: TimeWindow) -> Result<Vec<Event>> {
        let hot = self.hot.query(window).await?;
        if !hot.is_empty() {
            return Ok(hot);
        }
        self.cold.query(window).await
    }

    async fn len(&self) -> Result<usize> {
        self.cold.len().await
    }

    async fn list_events(
        &self,
        project_id: Option<String>,
        protocol: Option<ProtocolType>,
        severity: Option<EventSeverity>,
        service_id: Option<String>,
    ) -> Vec<Event> {
        let hot = self
            .hot
            .list_events(project_id.clone(), protocol, severity, service_id.clone())
            .await;
        if !hot.is_empty() {
            return hot;
        }
        self.cold
            .list_events(project_id, protocol, severity, service_id)
            .await
    }

    async fn list_events_since(&self, since_us: u64) -> Vec<Event> {
        let hot = self.hot.list_events_since(since_us).await;
        if !hot.is_empty() {
            return hot;
        }
        self.cold.list_events_since(since_us).await
    }

    async fn get_stats(&self) -> serde_json::Value {
        self.cold.get_stats().await
    }
}
