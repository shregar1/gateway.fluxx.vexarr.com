use async_trait::async_trait;
use crate::core::error::Result;
use crate::core::{Event, Processor};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::debug;

/// Enriches events with external or constant tags for segmentation.
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

/// Calculates the overall latency from creation (timestamp_us) to "now".
/// This decouples latency calculation from protocols.
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

/// Categorizes events into latency buckets (e.g., <10ms, <50ms, etc.)
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
        Self::new(vec![10, 50, 100, 500, 1000, 5000])
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

/// Filters out events based on custom logic.
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
        f.debug_struct("FilterProcessor")
            .field("name", &self.name)
            .finish()
    }
}

#[async_trait]
impl Processor for FilterProcessor {
    async fn process(&self, event: Event) -> Result<Option<Event>> {
        if (self.filter_fn)(&event) {
            Ok(Some(event))
        } else {
            debug!("FilterProcessor '{}' dropped event {}", self.name, event.id);
            Ok(None)
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A composite processor that chains multiple processors together.
#[derive(Debug, Default, Clone)]
pub struct ProcessorChain {
    processors: Vec<Arc<dyn Processor>>,
}

impl ProcessorChain {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn chain(mut self, processor: Arc<dyn Processor>) -> Self {
        self.processors.push(processor);
        self
    }

    pub fn pipe(mut self, processor: impl Processor + 'static) -> Self {
        self.processors.push(Arc::new(processor));
        self
    }
}

#[async_trait]
impl Processor for ProcessorChain {
    async fn process(&self, event: Event) -> Result<Option<Event>> {
        let mut current_event = Some(event);

        for processor in &self.processors {
            if let Some(ev) = current_event {
                current_event = processor.process(ev).await?;
            } else {
                break;
            }
        }

        Ok(current_event)
    }

    fn name(&self) -> &str {
        "ProcessorChain"
    }
}

/// Masks sensitive information (PII) from event metadata and payloads.
#[derive(Debug, Clone)]
pub struct SecurityProcessor {
    sensitive_keys: Vec<String>,
    mask_placeholder: String,
}

impl SecurityProcessor {
    pub fn new(keys: Vec<&str>) -> Self {
        Self {
            sensitive_keys: keys.into_iter().map(|s| s.to_string()).collect(),
            mask_placeholder: "[MASKED]".to_string(),
        }
    }

    pub fn with_placeholder(mut self, placeholder: &str) -> Self {
        self.mask_placeholder = placeholder.to_string();
        self
    }
}

#[async_trait]
impl Processor for SecurityProcessor {
    async fn process(&self, mut event: Event) -> Result<Option<Event>> {
        // 1. Mask metadata
        for key in &self.sensitive_keys {
            if event.metadata.extra.contains_key(key) {
                event.metadata.add(key, &self.mask_placeholder);
            }
        }

        // 2. Simple payload masking (if it's a string, we look for "key": "value" patterns)
        if let Some(ref mut payload) = event.payload {
            for key in &self.sensitive_keys {
                let pattern = format!("\"{}\":", key);
                if payload.contains(&pattern) {
                    // This is a naive implementation; in production, use a JSON parser
                    *payload = payload.replace(key, &format!("{}_masked", key));
                }
            }
        }

        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "SecurityProcessor"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ProtocolType;
    use tokio;

    #[tokio::test]
    async fn test_tag_processor() {
        let tags = HashMap::from([("env".to_string(), "test".to_string())]);
        let processor = TagProcessor::new(tags).with_tag("version", "1.0");
        let event = Event::new("1".to_string(), ProtocolType::Http);
        
        let result = processor.process(event).await.unwrap().unwrap();
        assert_eq!(result.metadata.extra.get("env").unwrap(), "test");
        assert_eq!(result.metadata.extra.get("version").unwrap(), "1.0");
    }

    #[tokio::test]
    async fn test_latency_processor() {
        let processor = LatencyProcessor::new();
        let mut event = Event::new("1".to_string(), ProtocolType::Http);
        // Simulate some time passing by artificially decreasing timestamp
        event.timestamp_us -= 100_000; // -100ms
        
        let result = processor.process(event).await.unwrap().unwrap();
        assert!(result.latency_us >= 100_000);
    }

    #[tokio::test]
    async fn test_latency_bucket_processor() {
        let processor = LatencyBucketProcessor::new(vec![10, 50, 100]);
        
        let mut event = Event::new("1".to_string(), ProtocolType::Http);
        event.latency_us = 25_000; // 25ms
        let result = processor.process(event.clone()).await.unwrap().unwrap();
        assert_eq!(result.metadata.extra.get("latency_bucket").unwrap(), "50ms");

        event.latency_us = 150_000; // 150ms
        let result = processor.process(event).await.unwrap().unwrap();
        assert_eq!(result.metadata.extra.get("latency_bucket").unwrap(), "overflow");
    }

    #[tokio::test]
    async fn test_filter_processor() {
        let processor = FilterProcessor::new("TestFilter", |e| e.id == "keep");
        
        let event1 = Event::new("keep".to_string(), ProtocolType::Http);
        let result1 = processor.process(event1).await.unwrap();
        assert!(result1.is_some());

        let event2 = Event::new("drop".to_string(), ProtocolType::Http);
        let result2 = processor.process(event2).await.unwrap();
        assert!(result2.is_none());
    }

    #[tokio::test]
    async fn test_processor_chain_composition() {
        let chain = ProcessorChain::new()
            .pipe(TagProcessor::new(HashMap::from([("stage".to_string(), "prod".to_string())])))
            .pipe(FilterProcessor::new("Filter", |e| e.metadata.extra.contains_key("stage")));

        let event = Event::new("1".to_string(), ProtocolType::Http);
        let result = chain.process(event).await.unwrap().unwrap();
        
        assert_eq!(result.metadata.extra.get("stage").unwrap(), "prod");
    }

    #[tokio::test]
    async fn test_processor_chain_short_circuit() {
        let chain = ProcessorChain::new()
            .pipe(FilterProcessor::new("DropAll", |_| false))
            .pipe(TagProcessor::new(HashMap::from([("never".to_string(), "reached".to_string())])));

        let event = Event::new("1".to_string(), ProtocolType::Http);
        let result = chain.process(event).await.unwrap();
        
        assert!(result.is_none());
    }
}
