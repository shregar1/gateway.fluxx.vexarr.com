use crate::models::event::Event;
use crate::core::error::Result;
use crate::storage::event_store::{EventStore, TimeWindow};
use crate::replay::strategy::{ReplayStrategy, IdentityStrategy};
use std::sync::Arc;
use tracing::{info, debug};

pub mod strategy;

/// ReplayEngine: Coordinates the deterministic streaming of historical traffic.
/// Enforces order and applies mutations according to a pluggable ReplayStrategy.
#[derive(Debug)]
pub struct ReplayEngine {
    store: Arc<dyn EventStore>,
    strategy: Box<dyn ReplayStrategy>,
}

impl ReplayEngine {
    pub fn new(store: Arc<dyn EventStore>) -> Self {
        Self {
            store,
            strategy: Box::new(IdentityStrategy),
        }
    }

    pub fn with_strategy(mut self, strategy: impl ReplayStrategy + 'static) -> Self {
        self.strategy = Box::new(strategy);
        self
    }

    /// Replays historical events for the window into the provided handler.
    /// Ensures chronological order and applies all configured mutations.
    pub async fn replay<F>(&self, window: TimeWindow, mut handler: F) -> Result<()>
    where
        F: FnMut(Event) + Send + Sync + 'static,
    {
        info!(start = window.start_us, end = window.end_us, "ReplayEngine: Initiating session replay.");
        
        // 1. Fetch raw event block from store
        let mut events = self.store.query(window).await?;
        
        // 2. Enforce strict chronological order for deterministic simulation
        events.sort_by_key(|e| e.timestamp_us);
        
        let initial_count = events.len();
        let mut replayed_count = 0;

        // 3. Process and Re-inject
        for event in events {
            if let Some(mutated_event) = self.strategy.transform(event) {
                // If deterministic delay simulation is required, it could be implemented here.
                handler(mutated_event);
                replayed_count += 1;
            }
        }

        debug!(
            replayed = replayed_count, 
            dropped = initial_count - replayed_count, 
            "ReplayEngine: Chronological block completed."
        );
        
        Ok(())
    }
}
