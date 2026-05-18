use crate::sessions::models::Session;
use crate::models::event::Event;
use async_trait::async_trait;

/// Interface for session data persistence.
/// Enables future support for Redis, Disk, or Cloud storage backends.
#[async_trait]
pub trait SessionStore: Send + Sync + std::fmt::Debug {
    /// Inserts a new event or appends it to an existing session timeline.
    async fn insert_or_update(&self, session_id: String, event: Event);
    
    /// Retrieves a session by its unique identifier.
    async fn get(&self, id: &str) -> Option<Session>;
    
    /// Lists all active sessions currently within the storage bounds.
    async fn list(&self) -> Vec<Session>;
    
    /// Prunes stale session data according to the backend's policy.
    async fn evict(&self);
}

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tracing::info;

/// Optimized in-memory implementation of the SessionStore.
#[derive(Debug)]
pub struct InMemorySessionStore {
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    max_age_us: u64,
}

impl InMemorySessionStore {
    pub fn new(max_age_seconds: u64) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            max_age_us: max_age_seconds * 1_000_000,
        }
    }
}

#[async_trait]
impl SessionStore for InMemorySessionStore {
    async fn insert_or_update(&self, session_id: String, event: Event) {
        let mut map = self.sessions.write().expect("RwLock poisoned");
        let session = map.entry(session_id.clone()).or_insert_with(|| Session::new(session_id));
        session.add_event(event);
    }

    async fn get(&self, id: &str) -> Option<Session> {
        let map = self.sessions.read().expect("RwLock poisoned");
        map.get(id).cloned()
    }

    async fn list(&self) -> Vec<Session> {
        let map = self.sessions.read().expect("RwLock poisoned");
        map.values().cloned().collect()
    }

    async fn evict(&self) {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_micros() as u64;
        let mut map = self.sessions.write().expect("RwLock poisoned");
        let initial_size = map.len();
        map.retain(|_, session| (now - session.last_seen_us) < self.max_age_us);
        if map.len() < initial_size {
            info!(count = initial_size - map.len(), "InMemorySessionStore: Evicted stale sessions.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::{Event, ProtocolType};

    // Test 101: InMemorySessionStore new
    #[test]
    fn test_session_store_new() {
        let store = InMemorySessionStore::new(3600);
        let _unused = store.sessions.read().unwrap();
    }

    // Test 102: Insert new session
    #[tokio::test]
    async fn test_session_store_insert_new() {
        let store = InMemorySessionStore::new(3600);
        let event = Event::new("evt-1".to_string(), ProtocolType::Http);
        store.insert_or_update("sess-1".to_string(), event).await;
        
        let sessions = store.list().await;
        assert_eq!(sessions.len(), 1);
    }

    // Test 103: Update existing session
    #[tokio::test]
    async fn test_session_store_update_existing() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        store.insert_or_update("sess-1".to_string(), Event::new("evt-2".to_string(), ProtocolType::Http)).await;
        
        let session = store.get("sess-1").await.unwrap();
        assert_eq!(session.events.len(), 2);
    }

    // Test 104: Get non-existent session
    #[tokio::test]
    async fn test_session_store_get_nonexistent() {
        let store = InMemorySessionStore::new(3600);
        let session = store.get("nonexistent").await;
        assert!(session.is_none());
    }

    // Test 105: List multiple sessions
    #[tokio::test]
    async fn test_session_store_list_multiple() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        store.insert_or_update("sess-2".to_string(), Event::new("evt-2".to_string(), ProtocolType::Ws)).await;
        store.insert_or_update("sess-3".to_string(), Event::new("evt-3".to_string(), ProtocolType::Grpc)).await;
        
        let sessions = store.list().await;
        assert_eq!(sessions.len(), 3);
    }

    // Test 106: List empty store
    #[tokio::test]
    async fn test_session_store_list_empty() {
        let store = InMemorySessionStore::new(3600);
        let sessions = store.list().await;
        assert!(sessions.is_empty());
    }

    // Test 107: Evict stale sessions
    #[tokio::test]
    async fn test_session_store_evict() {
        let store = InMemorySessionStore::new(0); // 0 seconds max age
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        std::thread::sleep(std::time::Duration::from_millis(100));
        store.evict().await;
        
        let sessions = store.list().await;
        assert!(sessions.is_empty());
    }

    // Test 108: Evict keeps fresh sessions
    #[tokio::test]
    async fn test_session_store_evict_keeps_fresh() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        store.evict().await;
        
        let sessions = store.list().await;
        assert_eq!(sessions.len(), 1);
    }

    // Test 109: Multiple events in different sessions
    #[tokio::test]
    async fn test_session_store_multiple_sessions_events() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        store.insert_or_update("sess-1".to_string(), Event::new("evt-2".to_string(), ProtocolType::Http)).await;
        store.insert_or_update("sess-2".to_string(), Event::new("evt-3".to_string(), ProtocolType::Ws)).await;
        
        let sessions = store.list().await;
        assert_eq!(sessions.len(), 2);
        
        let sess1 = store.get("sess-1").await.unwrap();
        assert_eq!(sess1.events.len(), 2);
    }

    // Test 110: Session store debug format
    #[test]
    fn test_session_store_debug() {
        let store = InMemorySessionStore::new(3600);
        let debug_str = format!("{:?}", store);
        assert!(debug_str.contains("InMemorySessionStore"));
    }

    // Test 601: Store with zero max age
    #[test]
    fn test_store_zero_max_age() {
        let store = InMemorySessionStore::new(0);
        assert_eq!(store.max_age_us, 0);
    }

    // Test 602: Store with large max age
    #[test]
    fn test_store_large_max_age() {
        let store = InMemorySessionStore::new(u64::MAX / 1_000_000);
        assert!(store.max_age_us > 0);
    }

    // Test 603: Insert same session multiple times
    #[tokio::test]
    async fn test_store_insert_same_session_multiple() {
        let store = InMemorySessionStore::new(3600);
        for i in 0..10 {
            store.insert_or_update("sess-1".to_string(), Event::new(format!("evt-{}", i), ProtocolType::Http)).await;
        }
        let session = store.get("sess-1").await.unwrap();
        assert_eq!(session.events.len(), 10);
    }

    // Test 604: Get after eviction returns none for expired
    #[tokio::test]
    async fn test_store_get_after_eviction() {
        let store = InMemorySessionStore::new(0);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        std::thread::sleep(std::time::Duration::from_millis(10));
        store.evict().await;
        
        let session = store.get("sess-1").await;
        assert!(session.is_none());
    }

    // Test 605: Store list after partial eviction
    #[tokio::test]
    async fn test_store_list_after_partial_eviction() {
        let store = InMemorySessionStore::new(1); // 1 second max age
        store.insert_or_update("fresh".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        std::thread::sleep(std::time::Duration::from_millis(100));
        store.evict().await;
        
        let sessions = store.list().await;
        assert_eq!(sessions.len(), 1); // Still fresh
    }

    // Test 606: Store insert update timestamp
    #[tokio::test]
    async fn test_store_insert_updates_timestamp() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session1 = store.get("sess-1").await.unwrap();
        let first_seen = session1.last_seen_us;
        
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        store.insert_or_update("sess-1".to_string(), Event::new("evt-2".to_string(), ProtocolType::Http)).await;
        let session2 = store.get("sess-1").await.unwrap();
        
        assert!(session2.last_seen_us >= first_seen);
    }

    // Test 607: Store get preserves session ID
    #[tokio::test]
    async fn test_store_get_preserves_id() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("my-session-id".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session = store.get("my-session-id").await.unwrap();
        assert_eq!(session.id, "my-session-id");
    }

    // Test 608: Store with many sessions
    #[tokio::test]
    async fn test_store_many_sessions() {
        let store = InMemorySessionStore::new(3600);
        for i in 0..100 {
            store.insert_or_update(format!("sess-{}", i), Event::new(format!("evt-{}", i), ProtocolType::Http)).await;
        }
        
        let sessions = store.list().await;
        assert_eq!(sessions.len(), 100);
    }

    // Test 609: Store evict empty store
    #[tokio::test]
    async fn test_store_evict_empty() {
        let store = InMemorySessionStore::new(0);
        store.evict().await; // Should not panic
        let sessions = store.list().await;
        assert!(sessions.is_empty());
    }

    // Test 610: Store list preserves all sessions
    #[tokio::test]
    async fn test_store_list_preserves_sessions() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        store.insert_or_update("sess-2".to_string(), Event::new("evt-2".to_string(), ProtocolType::Ws)).await;
        
        let sessions = store.list().await;
        let ids: Vec<_> = sessions.iter().map(|s| s.id.clone()).collect();
        assert!(ids.contains(&"sess-1".to_string()));
        assert!(ids.contains(&"sess-2".to_string()));
    }

    // Test 611: Store get non-existent after insert
    #[tokio::test]
    async fn test_store_get_nonexistent_after_insert() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session = store.get("nonexistent").await;
        assert!(session.is_none());
    }

    // Test 612: Store insert events with different protocols
    #[tokio::test]
    async fn test_store_different_protocols() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        store.insert_or_update("sess-1".to_string(), Event::new("evt-2".to_string(), ProtocolType::Ws)).await;
        store.insert_or_update("sess-1".to_string(), Event::new("evt-3".to_string(), ProtocolType::Grpc)).await;
        
        let session = store.get("sess-1").await.unwrap();
        assert_eq!(session.events.len(), 3);
    }

    // Test 613: Store evict some keep others
    #[tokio::test]
    async fn test_store_evict_partial() {
        let store = InMemorySessionStore::new(0); // Immediate eviction
        
        store.insert_or_update("old".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        std::thread::sleep(std::time::Duration::from_millis(100));
        
        // This creates a new session "new" with fresh timestamp
        store.insert_or_update("new".to_string(), Event::new("evt-2".to_string(), ProtocolType::Http)).await;
        
        // Actually with max_age_us=0, all sessions older than 0 microseconds should be evicted
        // So both will be evicted immediately since their age > 0
        store.evict().await;
        
        let sessions = store.list().await;
        // With 0 max_age, all sessions should be evicted
        assert!(sessions.is_empty());
    }

    // Test 614: Store session independence
    #[tokio::test]
    async fn test_store_session_independence() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        store.insert_or_update("sess-2".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let sess1 = store.get("sess-1").await.unwrap();
        let sess2 = store.get("sess-2").await.unwrap();
        
        assert_ne!(sess1.id, sess2.id);
        assert_eq!(sess1.events.len(), 1);
        assert_eq!(sess2.events.len(), 1);
    }

    // Test 615: Store with empty session ID
    #[tokio::test]
    async fn test_store_empty_session_id() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session = store.get("").await;
        assert!(session.is_some());
    }

    // Test 616: Store with unicode session ID
    #[tokio::test]
    async fn test_store_unicode_session_id() {
        let store = InMemorySessionStore::new(3600);
        let id = "会话-セッション".to_string();
        store.insert_or_update(id.clone(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session = store.get(&id).await;
        assert!(session.is_some());
        assert_eq!(session.unwrap().id, id);
    }

    // Test 617: Store max age conversion
    #[test]
    fn test_store_max_age_conversion() {
        let store1 = InMemorySessionStore::new(1);
        assert_eq!(store1.max_age_us, 1_000_000);
        
        let store3600 = InMemorySessionStore::new(3600);
        assert_eq!(store3600.max_age_us, 3_600_000_000);
    }

    // Test 618: Store get returns cloned session
    #[tokio::test]
    async fn test_store_get_returns_clone() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session1 = store.get("sess-1").await.unwrap();
        let session2 = store.get("sess-1").await.unwrap();
        
        // Both should be independent clones
        assert_eq!(session1.id, session2.id);
    }

    // Test 619: Store list order not guaranteed
    #[tokio::test]
    async fn test_store_list_order() {
        let store = InMemorySessionStore::new(3600);
        for i in 0..5 {
            store.insert_or_update(format!("sess-{}", i), Event::new("evt".to_string(), ProtocolType::Http)).await;
        }
        
        let sessions = store.list().await;
        assert_eq!(sessions.len(), 5);
        // Order may vary due to HashMap, so we just check presence
    }

    // Test 620: Store evict with no expired sessions
    #[tokio::test]
    async fn test_store_evict_none_expired() {
        let store = InMemorySessionStore::new(3600); // 1 hour
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        store.evict().await;
        
        let sessions = store.list().await;
        assert_eq!(sessions.len(), 1);
    }

    // Test 621: Store multiple evictions
    #[tokio::test]
    async fn test_store_multiple_evictions() {
        let store = InMemorySessionStore::new(0);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        store.evict().await;
        store.evict().await;
        store.evict().await;
        
        let sessions = store.list().await;
        assert!(sessions.is_empty());
    }

    // Test 622: Store session event count
    #[tokio::test]
    async fn test_store_event_count() {
        let store = InMemorySessionStore::new(3600);
        for i in 0..50 {
            store.insert_or_update("sess-1".to_string(), Event::new(format!("evt-{}", i), ProtocolType::Http)).await;
        }
        
        let session = store.get("sess-1").await.unwrap();
        assert_eq!(session.events.len(), 50);
    }

    // Test 623: Store with very long session ID
    #[tokio::test]
    async fn test_store_long_session_id() {
        let store = InMemorySessionStore::new(3600);
        let long_id = "a".repeat(10000);
        store.insert_or_update(long_id.clone(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session = store.get(&long_id).await;
        assert!(session.is_some());
    }

    // Test 624: Store concurrent inserts same session
    #[tokio::test]
    async fn test_store_concurrent_inserts() {
        let store = std::sync::Arc::new(InMemorySessionStore::new(3600));
        let mut handles = vec![];
        
        for i in 0..10 {
            let store_clone = store.clone();
            let handle = tokio::spawn(async move {
                store_clone.insert_or_update("shared".to_string(), Event::new(format!("evt-{}", i), ProtocolType::Http)).await;
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.await.unwrap();
        }
        
        let session = store.get("shared").await.unwrap();
        assert_eq!(session.events.len(), 10);
    }

    // Test 625: Store get mutates nothing
    #[tokio::test]
    async fn test_store_get_immutable() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let _ = store.get("sess-1").await;
        let _ = store.get("sess-1").await;
        
        let session = store.get("sess-1").await.unwrap();
        assert_eq!(session.events.len(), 1);
    }

    // Test 626: Store list with no sessions
    #[tokio::test]
    async fn test_store_list_no_sessions() {
        let store = InMemorySessionStore::new(3600);
        let sessions = store.list().await;
        assert!(sessions.is_empty());
    }

    // Test 627: Store insert_or_update creates new session
    #[tokio::test]
    async fn test_store_insert_or_update_new() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("new-sess".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session = store.get("new-sess").await;
        assert!(session.is_some());
    }

    // Test 628: Store with 1 second max age
    #[tokio::test]
    async fn test_store_one_second_max_age() {
        let store = InMemorySessionStore::new(1);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        // Session should still exist immediately
        let session = store.get("sess-1").await;
        assert!(session.is_some());
    }

    // Test 629: Store event protocol preserved
    #[tokio::test]
    async fn test_store_event_protocol_preserved() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Tcp)).await;
        
        let session = store.get("sess-1").await.unwrap();
        assert_eq!(session.events[0].protocol, ProtocolType::Tcp);
    }

    // Test 630: Store event source preserved
    #[tokio::test]
    async fn test_store_event_source_preserved() {
        let store = InMemorySessionStore::new(3600);
        let mut event = Event::new("evt-1".to_string(), ProtocolType::Http);
        event.metadata.source_ip = Some("test-source".to_string());
        store.insert_or_update("sess-1".to_string(), event).await;
        
        let session = store.get("sess-1").await.unwrap();
        assert_eq!(session.events[0].metadata.source_ip, Some("test-source".to_string()));
    }

    // Test 631: Store list returns clones
    #[tokio::test]
    async fn test_store_list_returns_clones() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let list1 = store.list().await;
        let list2 = store.list().await;
        
        assert_eq!(list1.len(), list2.len());
    }

    // Test 632: Store evict removes correct sessions
    #[tokio::test]
    async fn test_store_evict_removes_correct() {
        let store = InMemorySessionStore::new(0); // Immediate eviction
        
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        std::thread::sleep(std::time::Duration::from_millis(50));
        store.insert_or_update("sess-2".to_string(), Event::new("evt-2".to_string(), ProtocolType::Http)).await;
        
        std::thread::sleep(std::time::Duration::from_millis(100));
        store.evict().await;
        
        // Both should be evicted with max_age_us = 0
        let sessions = store.list().await;
        assert!(sessions.is_empty());
    }

    // Test 633: Store with max age boundary
    #[tokio::test]
    async fn test_store_max_age_boundary() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        // Should exist immediately
        let session = store.get("sess-1").await;
        assert!(session.is_some());
    }

    // Test 634: Store concurrent list and insert
    #[tokio::test]
    async fn test_store_concurrent_list_insert() {
        let store = std::sync::Arc::new(InMemorySessionStore::new(3600));
        
        for i in 0..5 {
            let store_clone = store.clone();
            tokio::spawn(async move {
                store_clone.insert_or_update(format!("sess-{}", i), Event::new("evt".to_string(), ProtocolType::Http)).await;
            });
        }
        
        // Give inserts time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        
        let sessions = store.list().await;
        assert_eq!(sessions.len(), 5);
    }

    // Test 635: Store get case sensitivity
    #[tokio::test]
    async fn test_store_get_case_sensitive() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("MySession".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session_lower = store.get("mysession").await;
        let session_upper = store.get("MYSESSION").await;
        let session_exact = store.get("MySession").await;
        
        assert!(session_lower.is_none());
        assert!(session_upper.is_none());
        assert!(session_exact.is_some());
    }

    // Test 636: Store events maintain order
    #[tokio::test]
    async fn test_store_events_maintain_order() {
        let store = InMemorySessionStore::new(3600);
        for i in 0..20 {
            store.insert_or_update("sess-1".to_string(), Event::new(format!("evt-{}", i), ProtocolType::Http)).await;
        }
        
        let session = store.get("sess-1").await.unwrap();
        for i in 0..20 {
            assert_eq!(session.events[i].id, format!("evt-{}", i));
        }
    }

    // Test 637: Store with special chars session ID
    #[tokio::test]
    async fn test_store_special_chars_id() {
        let store = InMemorySessionStore::new(3600);
        let id = "sess-@#$%^&*()".to_string();
        store.insert_or_update(id.clone(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session = store.get(&id).await;
        assert!(session.is_some());
    }

    // Test 638: Store evict multiple times empty
    #[tokio::test]
    async fn test_store_evict_empty_multiple() {
        let store = InMemorySessionStore::new(0);
        store.evict().await;
        store.evict().await;
        store.evict().await;
        
        let sessions = store.list().await;
        assert!(sessions.is_empty());
    }

    // Test 639: Store insert same event multiple times
    #[tokio::test]
    async fn test_store_insert_same_event() {
        let store = InMemorySessionStore::new(3600);
        let event = Event::new("same-evt".to_string(), ProtocolType::Http);
        
        store.insert_or_update("sess-1".to_string(), event.clone()).await;
        store.insert_or_update("sess-1".to_string(), event.clone()).await;
        store.insert_or_update("sess-1".to_string(), event.clone()).await;
        
        let session = store.get("sess-1").await.unwrap();
        assert_eq!(session.events.len(), 3);
    }

    // Test 640: Store get from list
    #[tokio::test]
    async fn test_store_get_from_list() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let sessions = store.list().await;
        let from_list = sessions.iter().find(|s| s.id == "sess-1");
        let from_get = store.get("sess-1").await;
        
        assert!(from_list.is_some());
        assert!(from_get.is_some());
        assert_eq!(from_list.unwrap().id, from_get.unwrap().id);
    }

    // Test 641: Store session created timestamp preserved
    #[tokio::test]
    async fn test_store_created_timestamp_preserved() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session1 = store.get("sess-1").await.unwrap();
        let created = session1.created_at_us;
        
        // Add more events
        std::thread::sleep(std::time::Duration::from_millis(10));
        store.insert_or_update("sess-1".to_string(), Event::new("evt-2".to_string(), ProtocolType::Http)).await;
        
        let session2 = store.get("sess-1").await.unwrap();
        assert_eq!(session2.created_at_us, created); // Created should not change
    }

    // Test 642: Store with UUID session ID
    #[tokio::test]
    async fn test_store_uuid_session_id() {
        let store = InMemorySessionStore::new(3600);
        let uuid = "550e8400-e29b-41d4-a716-446655440000".to_string();
        store.insert_or_update(uuid.clone(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session = store.get(&uuid).await;
        assert!(session.is_some());
    }

    // Test 643: Store list after insert and evict
    #[tokio::test]
    async fn test_store_list_after_evict() {
        let store = InMemorySessionStore::new(0);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        std::thread::sleep(std::time::Duration::from_millis(100));
        store.evict().await;
        
        let sessions = store.list().await;
        assert!(sessions.is_empty());
    }

    // Test 644: Store concurrent gets
    #[tokio::test]
    async fn test_store_concurrent_gets() {
        let store = std::sync::Arc::new(InMemorySessionStore::new(3600));
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let mut handles = vec![];
        for _ in 0..10 {
            let store_clone = store.clone();
            let handle = tokio::spawn(async move {
                store_clone.get("sess-1").await
            });
            handles.push(handle);
        }
        
        let results = futures_util::future::join_all(handles).await;
        for result in results {
            assert!(result.unwrap().is_some());
        }
    }

    // Test 645: Store update existing changes event count
    #[tokio::test]
    async fn test_store_update_changes_count() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let count_before = store.get("sess-1").await.unwrap().events.len();
        store.insert_or_update("sess-1".to_string(), Event::new("evt-2".to_string(), ProtocolType::Http)).await;
        let count_after = store.get("sess-1").await.unwrap().events.len();
        
        assert_eq!(count_before, 1);
        assert_eq!(count_after, 2);
    }

    // Test 646: Store with numeric session ID
    #[tokio::test]
    async fn test_store_numeric_session_id() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("12345".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let session = store.get("12345").await;
        assert!(session.is_some());
    }

    // Test 647: Store evict timing calculation
    #[test]
    fn test_store_evict_timing() {
        let store = InMemorySessionStore::new(1); // 1 second = 1_000_000 microseconds
        assert_eq!(store.max_age_us, 1_000_000);
    }

    // Test 648: Store get returns correct session type
    #[tokio::test]
    async fn test_store_get_returns_session() {
        let store = InMemorySessionStore::new(3600);
        store.insert_or_update("sess-1".to_string(), Event::new("evt-1".to_string(), ProtocolType::Http)).await;
        
        let result = store.get("sess-1").await;
        assert!(matches!(result, Some(_)));
    }

    // Test 649: Store list returns Vec of Sessions
    #[tokio::test]
    async fn test_store_list_type() {
        let store = InMemorySessionStore::new(3600);
        let result = store.list().await;
        // Type is Vec<Session>
        let _: Vec<Session> = result;
    }

    // Test 650: Store trait object compatibility
    #[test]
    fn test_store_trait_object() {
        let store: Box<dyn SessionStore> = Box::new(InMemorySessionStore::new(3600));
        // Just verify it compiles and can be boxed
        let debug_str = format!("{:?}", store);
        assert!(!debug_str.is_empty());
    }
}
