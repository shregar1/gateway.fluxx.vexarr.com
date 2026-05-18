//! Database Monitoring (DBM)
//! Tracks slow queries, explain plans, connection pool stats, and wait events.
//! Receives data from database integrations and provides query analytics.

use crate::api::{ApiState, StandardResponse};
use axum::{extract::{State, Query}, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQuery {
    pub id: String,
    pub database: String,
    pub query_text: String,
    pub normalized_query: String,
    pub duration_ms: f64,
    pub rows_examined: u64,
    pub rows_returned: u64,
    pub timestamp_us: u64,
    pub user: String,
    pub source_service: Option<String>,
    pub explain_plan: Option<String>,
    pub wait_events: Vec<WaitEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitEvent {
    pub event_type: String,
    pub event_name: String,
    pub duration_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
    pub normalized_query: String,
    pub call_count: u64,
    pub avg_duration_ms: f64,
    pub max_duration_ms: f64,
    pub p99_duration_ms: f64,
    pub avg_rows_examined: u64,
    pub total_time_ms: f64,
    pub databases: Vec<String>,
}

#[allow(dead_code)]
fn now_us() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros() as u64 }

#[derive(Debug, Default)]
pub struct DbmStore {
    queries: RwLock<Vec<SlowQuery>>,
}

impl DbmStore {
    pub fn new() -> Self { Self::default() }

    pub fn record(&self, query: SlowQuery) {
        self.queries.write().push(query);
    }

    pub fn get_slow_queries(&self, database: Option<&str>, min_duration_ms: f64, limit: usize) -> Vec<SlowQuery> {
        let queries = self.queries.read();
        queries.iter().rev()
            .filter(|q| database.map_or(true, |db| q.database == db))
            .filter(|q| q.duration_ms >= min_duration_ms)
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn get_query_stats(&self) -> Vec<QueryStats> {
        let queries = self.queries.read();
        let mut stats_map: HashMap<String, Vec<&SlowQuery>> = HashMap::new();
        for q in queries.iter() {
            stats_map.entry(q.normalized_query.clone()).or_default().push(q);
        }

        let mut stats: Vec<QueryStats> = stats_map.into_iter().map(|(norm, qs)| {
            let count = qs.len() as u64;
            let total: f64 = qs.iter().map(|q| q.duration_ms).sum();
            let max = qs.iter().map(|q| q.duration_ms).fold(0.0f64, f64::max);
            let avg_rows: u64 = if count > 0 { qs.iter().map(|q| q.rows_examined).sum::<u64>() / count } else { 0 };
            let dbs: Vec<String> = qs.iter().map(|q| q.database.clone()).collect::<std::collections::HashSet<_>>().into_iter().collect();
            let mut durations: Vec<f64> = qs.iter().map(|q| q.duration_ms).collect();
            durations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let p99 = durations.get((durations.len() as f64 * 0.99) as usize).copied().unwrap_or(max);

            QueryStats { normalized_query: norm, call_count: count, avg_duration_ms: total / count as f64,
                max_duration_ms: max, p99_duration_ms: p99, avg_rows_examined: avg_rows,
                total_time_ms: total, databases: dbs }
        }).collect();
        stats.sort_by(|a, b| b.total_time_ms.partial_cmp(&a.total_time_ms).unwrap_or(std::cmp::Ordering::Equal));
        stats
    }
}

pub async fn ingest_slow_query(State(state): State<Arc<ApiState>>, Json(query): Json<SlowQuery>) -> impl IntoResponse {
    state.dbm_store.record(query);
    StandardResponse::ok(serde_json::json!({"recorded": true}), "Slow query recorded.")
}

pub async fn list_slow_queries(State(state): State<Arc<ApiState>>, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let db = params.get("database").map(|s| s.as_str());
    let min_ms: f64 = params.get("min_duration_ms").and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(50);
    let queries = state.dbm_store.get_slow_queries(db, min_ms, limit);
    StandardResponse::ok(serde_json::to_value(&queries).unwrap_or_default(), "Slow queries.")
}

pub async fn get_query_stats(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let stats = state.dbm_store.get_query_stats();
    StandardResponse::ok(serde_json::to_value(&stats).unwrap_or_default(), "Query stats.")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_dbm_store() {
        let store = DbmStore::new();
        store.record(SlowQuery {
            id: "1".to_string(), database: "postgres".to_string(),
            query_text: "SELECT * FROM users WHERE id = 1".to_string(),
            normalized_query: "SELECT * FROM users WHERE id = ?".to_string(),
            duration_ms: 150.0, rows_examined: 50000, rows_returned: 1,
            timestamp_us: 0, user: "app".to_string(), source_service: None,
            explain_plan: None, wait_events: vec![],
        });
        store.record(SlowQuery {
            id: "2".to_string(), database: "postgres".to_string(),
            query_text: "SELECT * FROM users WHERE id = 2".to_string(),
            normalized_query: "SELECT * FROM users WHERE id = ?".to_string(),
            duration_ms: 200.0, rows_examined: 50000, rows_returned: 1,
            timestamp_us: 0, user: "app".to_string(), source_service: None,
            explain_plan: None, wait_events: vec![],
        });
        let stats = store.get_query_stats();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].call_count, 2);
        assert_eq!(stats[0].avg_duration_ms, 175.0);
    }
}
