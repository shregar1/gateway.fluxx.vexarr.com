use crate::models::event::Event;
use std::fmt::Debug;

/// ReplayStrategy: Defines how historical events are mutated before re-injection.
/// Supports deterministic delay, filtering, and payload transformations.
pub trait ReplayStrategy: Send + Sync + Debug {
    /// Mutates an event before replay. Returns None to drop the event.
    fn transform(&self, event: Event) -> Option<Event>;
}

/// A standard strategy that preserves the original event properties.
#[derive(Debug, Default, Clone)]
pub struct IdentityStrategy;

impl ReplayStrategy for IdentityStrategy {
    fn transform(&self, event: Event) -> Option<Event> {
        Some(event)
    }
}

/// A sophisticated strategy for chaos engineering and stress testing.
#[derive(Debug, Clone)]
pub struct MutatingStrategy {
    pub delay_ms: u64,
    pub drop_rate: f64, // 0.0 to 1.0
}

impl ReplayStrategy for MutatingStrategy {
    fn transform(&self, mut event: Event) -> Option<Event> {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        // 1. Drop based on percentage
        if rng.gen_bool(self.drop_rate) {
            return None;
        }

        // 2. Add delay to metadata (or simulate in engine)
        if self.delay_ms > 0 {
            event.latency_us += self.delay_ms * 1000;
        }

        Some(event)
    }
}
