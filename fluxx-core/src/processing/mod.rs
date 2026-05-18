use async_trait::async_trait;
use crate::core::error::Result;
use crate::models::event::Event;
use crate::core::traits::Processor;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::debug;

pub mod pipeline;
pub mod topology;

pub use pipeline::Pipeline;
pub use topology::TopologyProcessor;

/// TagProcessor: Enriches events with external metadata tags.
#[derive(Debug, Default, Clone)]
pub struct TagProcessor {
    tags: HashMap<String, String>,
}

impl TagProcessor {
    pub fn new(tags: HashMap<String, String>) -> Self {
        Self { tags }
    }

    pub fn with_tag(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(key.to_string(), value.to_string());
        self
    }
}

#[async_trait]
impl Processor for TagProcessor {
    async fn process(&self, mut event: Event) -> Result<Option<Event>> {
        for (k, v) in &self.tags {
            event.metadata.add(k, v);
        }
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "TagProcessor"
    }
}

/// LatencyProcessor: Computes the internal system latency of the event.
#[derive(Debug, Default, Clone)]
pub struct LatencyProcessor;

impl LatencyProcessor {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Processor for LatencyProcessor {
    async fn process(&self, mut event: Event) -> Result<Option<Event>> {
        let now_us = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        if now_us >= event.timestamp_us {
            event.latency_us = now_us - event.timestamp_us;
        }
        
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "LatencyProcessor"
    }
}

/// LatencyBucketProcessor: Classifies events into human-readable buckets.
#[derive(Debug, Clone)]
pub struct LatencyBucketProcessor {
    buckets_us: Vec<u64>,
}

impl LatencyBucketProcessor {
    pub fn new(buckets_ms: Vec<u64>) -> Self {
        let mut buckets_us: Vec<u64> = buckets_ms.into_iter().map(|ms| ms * 1000).collect();
        buckets_us.sort();
        Self { buckets_us }
    }

    pub fn default_buckets() -> Self {
        Self::new(vec![1, 10, 50, 100, 500, 1000])
    }
}

#[async_trait]
impl Processor for LatencyBucketProcessor {
    async fn process(&self, mut event: Event) -> Result<Option<Event>> {
        let latency = event.latency_us;
        let bucket = self.buckets_us
            .iter()
            .find(|&&b| latency <= b)
            .map(|&b| format!("{}ms", b / 1000))
            .unwrap_or_else(|| "overflow".to_string());

        event.metadata.add("latency_bucket", &bucket);
        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "LatencyBucketProcessor"
    }
}

/// FilterProcessor: Drops events based on a configurable predicate.
pub struct FilterProcessor {
    name: String,
    filter_fn: Arc<dyn Fn(&Event) -> bool + Send + Sync>,
}

impl FilterProcessor {
    pub fn new<F>(name: &str, f: F) -> Self 
    where F: Fn(&Event) -> bool + Send + Sync + 'static {
        Self {
            name: name.to_string(),
            filter_fn: Arc::new(f),
        }
    }
}

impl std::fmt::Debug for FilterProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterProcessor").field("name", &self.name).finish()
    }
}

#[async_trait]
impl Processor for FilterProcessor {
    async fn process(&self, event: Event) -> Result<Option<Event>> {
        if (self.filter_fn)(&event) {
            Ok(Some(event))
        } else {
            debug!(id=%event.id, proc=%self.name, "Processing: Event dropped by filter.");
            Ok(None)
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// ProcessorChain: Enables middleware-style sequential composition.
#[derive(Debug, Default, Clone)]
pub struct ProcessorChain {
    processors: Vec<Arc<dyn Processor>>,
}

impl ProcessorChain {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pipe(mut self, processor: impl Processor + 'static) -> Self {
        self.processors.push(Arc::new(processor));
        self
    }
}

#[async_trait]
impl Processor for ProcessorChain {
    async fn process(&self, event: Event) -> Result<Option<Event>> {
        let mut current = Some(event);
        for proc in &self.processors {
            if let Some(ev) = current {
                current = proc.process(ev).await?;
            } else {
                break;
            }
        }
        Ok(current)
    }

    fn name(&self) -> &str {
        "ProcessorChain"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::ProtocolType;

    #[tokio::test]
    async fn test_processor_logic() {
        let chain = ProcessorChain::new()
            .pipe(TagProcessor::new(HashMap::from([("env".to_string(), "prod".to_string())])))
            .pipe(FilterProcessor::new("PassAll", |_| true));

        let event = Event::new("test-1".to_string(), ProtocolType::Http);
        let res = chain.process(event).await.unwrap().unwrap();
        
        assert_eq!(res.metadata.extra.get("env").unwrap(), "prod");
    }
}
