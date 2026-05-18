use crate::models::event::Event;
use crate::storage::event_store::{EventStore, TimeWindow};
use crate::core::error::Result;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// InMemoryEventStore: Chronological persistence using a BTreeMap index.
/// Focused purely on pluggable storage and range retrieval efficiency.
#[derive(Debug, Default)]
pub struct InMemoryEventStore {
    events: Arc<RwLock<BTreeMap<u64, Vec<Event>>>>,
}

impl InMemoryEventStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl EventStore for InMemoryEventStore {
    async fn insert(&self, event: Event) -> Result<()> {
        let mut map = self.events.write().expect("RwLock poisoned");
        map.entry(event.timestamp_us).or_default().push(event);
        Ok(())
    }

    async fn query(&self, window: TimeWindow) -> Result<Vec<Event>> {
        let map = self.events.read().expect("RwLock poisoned");
        
        // BTreeMap provides an efficient O(log N) range scan for time windows.
        let events = map.range(window.start_us..=window.end_us)
            .flat_map(|(_, evs)| evs.clone())
            .collect();
            
        Ok(events)
    }

    async fn len(&self) -> Result<usize> {
        let map = self.events.read().expect("RwLock poisoned");
        Ok(map.values().map(|v| v.len()).sum())
    }

    async fn list_events(&self, project_id: Option<String>, protocol: Option<crate::models::event::ProtocolType>, severity: Option<crate::models::event::EventSeverity>, service_id: Option<String>) -> Vec<Event> {
        let map = self.events.read().expect("RwLock poisoned");
        
        map.values()
            .flat_map(|evs| evs.iter())
            .filter(|ev| {
                let project_match = project_id.is_none() || ev.project_id == project_id;
                let protocol_match = protocol.is_none() || Some(ev.protocol) == protocol;
                let severity_match = severity.is_none() || Some(ev.severity) == severity;
                let service_match = service_id.is_none() || ev.service_id == service_id;
                project_match && protocol_match && severity_match && service_match
            })
            .cloned()
            .collect()
    }

    async fn list_events_since(&self, since_us: u64) -> Vec<Event> {
        let map = self.events.read().expect("RwLock poisoned");
        // BTreeMap range scan: only visit keys > since_us.
        map.range((std::ops::Bound::Excluded(since_us), std::ops::Bound::Unbounded))
            .flat_map(|(_, evs)| evs.iter().cloned())
            .collect()
    }

    async fn get_stats(&self) -> serde_json::Value {
        let map = self.events.read().expect("RwLock poisoned");
        let total = map.values().map(|v| v.len()).sum::<usize>();
        
        let mut protocol_counts = std::collections::HashMap::new();
        for evs in map.values() {
            for ev in evs {
                *protocol_counts.entry(format!("{:?}", ev.protocol)).or_insert(0) += 1;
            }
        }

        serde_json::json!({
            "total_events": total,
            "unique_time_buckets": map.len(),
            "protocols": protocol_counts
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::{Event, EventSeverity, ProtocolType};

    fn make_event(id: &str, ts: u64, protocol: ProtocolType) -> Event {
        let mut e = Event::new(id.to_string(), protocol);
        e.timestamp_us = ts;
        e
    }

    #[tokio::test]
    async fn test_new_store_is_empty() {
        let store = InMemoryEventStore::new();
        assert_eq!(store.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_insert_single_event() {
        let store = InMemoryEventStore::new();
        store.insert(make_event("e1", 100, ProtocolType::Http)).await.unwrap();
        assert_eq!(store.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_insert_multiple_events_same_timestamp() {
        let store = InMemoryEventStore::new();
        store.insert(make_event("a", 500, ProtocolType::Http)).await.unwrap();
        store.insert(make_event("b", 500, ProtocolType::Tcp)).await.unwrap();
        store.insert(make_event("c", 500, ProtocolType::Udp)).await.unwrap();
        assert_eq!(store.len().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_query_empty_window() {
        let store = InMemoryEventStore::new();
        let result = store.query(TimeWindow::new(0, 1000)).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_query_time_window_range() {
        let store = InMemoryEventStore::new();
        store.insert(make_event("a", 100, ProtocolType::Http)).await.unwrap();
        store.insert(make_event("b", 500, ProtocolType::Http)).await.unwrap();
        store.insert(make_event("c", 1500, ProtocolType::Http)).await.unwrap();

        let result = store.query(TimeWindow::new(200, 1000)).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "b");
    }

    #[tokio::test]
    async fn test_query_inclusive_bounds() {
        let store = InMemoryEventStore::new();
        store.insert(make_event("a", 100, ProtocolType::Http)).await.unwrap();
        store.insert(make_event("b", 200, ProtocolType::Http)).await.unwrap();

        let result = store.query(TimeWindow::new(100, 200)).await.unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_query_at_u64_max() {
        let store = InMemoryEventStore::new();
        store.insert(make_event("a", u64::MAX, ProtocolType::Http)).await.unwrap();
        let result = store.query(TimeWindow::new(0, u64::MAX)).await.unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_list_events_no_filters() {
        let store = InMemoryEventStore::new();
        store.insert(make_event("a", 100, ProtocolType::Http)).await.unwrap();
        store.insert(make_event("b", 200, ProtocolType::Tcp)).await.unwrap();

        let all = store.list_events(None, None, None, None).await;
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_list_events_filter_by_protocol() {
        let store = InMemoryEventStore::new();
        store.insert(make_event("a", 100, ProtocolType::Http)).await.unwrap();
        store.insert(make_event("b", 200, ProtocolType::Tcp)).await.unwrap();
        store.insert(make_event("c", 300, ProtocolType::Http)).await.unwrap();

        let http_only = store.list_events(None, Some(ProtocolType::Http), None, None).await;
        assert_eq!(http_only.len(), 2);
    }

    #[tokio::test]
    async fn test_list_events_filter_by_severity() {
        let store = InMemoryEventStore::new();
        let mut e1 = make_event("a", 100, ProtocolType::Http);
        e1.severity = EventSeverity::Critical;
        let mut e2 = make_event("b", 200, ProtocolType::Http);
        e2.severity = EventSeverity::Info;
        store.insert(e1).await.unwrap();
        store.insert(e2).await.unwrap();

        let critical = store.list_events(None, None, Some(EventSeverity::Critical), None).await;
        assert_eq!(critical.len(), 1);
        assert_eq!(critical[0].id, "a");
    }

    #[tokio::test]
    async fn test_list_events_filter_by_project_id() {
        let store = InMemoryEventStore::new();
        let mut e1 = make_event("a", 100, ProtocolType::Http);
        e1.project_id = Some("proj-1".to_string());
        let mut e2 = make_event("b", 200, ProtocolType::Http);
        e2.project_id = Some("proj-2".to_string());
        store.insert(e1).await.unwrap();
        store.insert(e2).await.unwrap();

        let p1 = store.list_events(Some("proj-1".to_string()), None, None, None).await;
        assert_eq!(p1.len(), 1);
        assert_eq!(p1[0].id, "a");
    }

    #[tokio::test]
    async fn test_list_events_filter_by_service_id() {
        let store = InMemoryEventStore::new();
        let mut e1 = make_event("a", 100, ProtocolType::Http);
        e1.service_id = Some("svc-x".to_string());
        let mut e2 = make_event("b", 200, ProtocolType::Http);
        e2.service_id = Some("svc-y".to_string());
        store.insert(e1).await.unwrap();
        store.insert(e2).await.unwrap();

        let sx = store.list_events(None, None, None, Some("svc-x".to_string())).await;
        assert_eq!(sx.len(), 1);
    }

    #[tokio::test]
    async fn test_list_events_all_filters_combined() {
        let store = InMemoryEventStore::new();
        let mut e = make_event("a", 100, ProtocolType::Http);
        e.project_id = Some("p1".to_string());
        e.service_id = Some("s1".to_string());
        e.severity = EventSeverity::High;
        store.insert(e).await.unwrap();

        let matched = store.list_events(
            Some("p1".to_string()),
            Some(ProtocolType::Http),
            Some(EventSeverity::High),
            Some("s1".to_string()),
        ).await;
        assert_eq!(matched.len(), 1);

        let unmatched = store.list_events(
            Some("p1".to_string()),
            Some(ProtocolType::Tcp),
            None, None,
        ).await;
        assert_eq!(unmatched.len(), 0);
    }

    #[tokio::test]
    async fn test_get_stats_empty() {
        let store = InMemoryEventStore::new();
        let stats = store.get_stats().await;
        assert_eq!(stats["total_events"], 0);
        assert_eq!(stats["unique_time_buckets"], 0);
    }

    #[tokio::test]
    async fn test_get_stats_with_events() {
        let store = InMemoryEventStore::new();
        store.insert(make_event("a", 100, ProtocolType::Http)).await.unwrap();
        store.insert(make_event("b", 200, ProtocolType::Http)).await.unwrap();
        store.insert(make_event("c", 300, ProtocolType::Tcp)).await.unwrap();

        let stats = store.get_stats().await;
        assert_eq!(stats["total_events"], 3);
        assert_eq!(stats["unique_time_buckets"], 3);
    }

    #[tokio::test]
    async fn test_query_outside_range_returns_empty() {
        let store = InMemoryEventStore::new();
        store.insert(make_event("a", 5000, ProtocolType::Http)).await.unwrap();
        let result = store.query(TimeWindow::new(0, 100)).await.unwrap();
        assert!(result.is_empty());
    }
}
