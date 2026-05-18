//! Network Performance Monitoring (NPM)
//! Tracks TCP/UDP flows, DNS queries, and network latency between services.
//! Receives flow data from eBPF or agent-collected network stats.

use crate::api::{ApiState, StandardResponse};
use axum::{extract::{State, Query}, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkFlow {
    pub id: String,
    pub source_ip: String,
    pub source_port: u16,
    pub dest_ip: String,
    pub dest_port: u16,
    pub protocol: String,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub packets_sent: u64,
    pub packets_received: u64,
    pub retransmits: u64,
    pub rtt_us: u64,
    pub source_service: Option<String>,
    pub dest_service: Option<String>,
    pub timestamp_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsQuery {
    pub id: String,
    pub query_name: String,
    pub query_type: String,
    pub response_code: String,
    pub latency_us: u64,
    pub source_ip: String,
    pub server_ip: String,
    pub timestamp_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkSummary {
    pub total_flows: u64,
    pub total_bytes: u64,
    pub total_retransmits: u64,
    pub avg_rtt_us: u64,
    pub dns_queries: u64,
    pub dns_failures: u64,
    pub top_talkers: Vec<(String, u64)>,
}

#[derive(Debug, Default)]
pub struct NpmStore {
    flows: RwLock<Vec<NetworkFlow>>,
    dns: RwLock<Vec<DnsQuery>>,
}

impl NpmStore {
    pub fn new() -> Self { Self::default() }

    pub fn record_flow(&self, flow: NetworkFlow) { self.flows.write().push(flow); }
    pub fn record_dns(&self, query: DnsQuery) { self.dns.write().push(query); }

    pub fn get_flows(&self, source: Option<&str>, dest: Option<&str>, limit: usize) -> Vec<NetworkFlow> {
        let flows = self.flows.read();
        flows.iter().rev()
            .filter(|f| source.map_or(true, |s| f.source_service.as_deref() == Some(s) || f.source_ip == s))
            .filter(|f| dest.map_or(true, |d| f.dest_service.as_deref() == Some(d) || f.dest_ip == d))
            .take(limit).cloned().collect()
    }

    pub fn get_dns_queries(&self, name: Option<&str>, limit: usize) -> Vec<DnsQuery> {
        let dns = self.dns.read();
        dns.iter().rev()
            .filter(|d| name.map_or(true, |n| d.query_name.contains(n)))
            .take(limit).cloned().collect()
    }

    pub fn get_summary(&self) -> NetworkSummary {
        let flows = self.flows.read();
        let dns = self.dns.read();
        let total_bytes: u64 = flows.iter().map(|f| f.bytes_sent + f.bytes_received).sum();
        let total_retransmits: u64 = flows.iter().map(|f| f.retransmits).sum();
        let avg_rtt = if flows.is_empty() { 0 } else { flows.iter().map(|f| f.rtt_us).sum::<u64>() / flows.len() as u64 };
        let dns_failures = dns.iter().filter(|d| d.response_code != "NOERROR").count() as u64;

        // Top talkers by bytes
        let mut talkers: HashMap<String, u64> = HashMap::new();
        for f in flows.iter() {
            *talkers.entry(f.source_ip.clone()).or_default() += f.bytes_sent;
        }
        let mut top: Vec<(String, u64)> = talkers.into_iter().collect();
        top.sort_by(|a, b| b.1.cmp(&a.1));
        top.truncate(10);

        NetworkSummary {
            total_flows: flows.len() as u64, total_bytes, total_retransmits,
            avg_rtt_us: avg_rtt, dns_queries: dns.len() as u64, dns_failures, top_talkers: top,
        }
    }
}

pub async fn record_flow(State(state): State<Arc<ApiState>>, Json(flow): Json<NetworkFlow>) -> impl IntoResponse {
    state.npm_store.record_flow(flow);
    StandardResponse::ok(serde_json::json!({"recorded": true}), "Flow recorded.")
}

pub async fn list_flows(State(state): State<Arc<ApiState>>, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let src = params.get("source").map(|s| s.as_str());
    let dst = params.get("dest").map(|s| s.as_str());
    let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(50);
    StandardResponse::ok(serde_json::to_value(&state.npm_store.get_flows(src, dst, limit)).unwrap_or_default(), "Network flows.")
}

pub async fn network_summary(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    StandardResponse::ok(serde_json::to_value(&state.npm_store.get_summary()).unwrap_or_default(), "Network summary.")
}

pub async fn record_dns(State(state): State<Arc<ApiState>>, Json(query): Json<DnsQuery>) -> impl IntoResponse {
    state.npm_store.record_dns(query);
    StandardResponse::ok(serde_json::json!({"recorded": true}), "DNS query recorded.")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_flow(src_ip: &str, dst_ip: &str, src_svc: Option<&str>, dst_svc: Option<&str>, bytes_s: u64, bytes_r: u64, rtt: u64, retx: u64) -> NetworkFlow {
        NetworkFlow {
            id: "f1".to_string(),
            source_ip: src_ip.to_string(),
            source_port: 1234,
            dest_ip: dst_ip.to_string(),
            dest_port: 80,
            protocol: "tcp".to_string(),
            bytes_sent: bytes_s,
            bytes_received: bytes_r,
            packets_sent: 10,
            packets_received: 10,
            retransmits: retx,
            rtt_us: rtt,
            source_service: src_svc.map(|s| s.to_string()),
            dest_service: dst_svc.map(|s| s.to_string()),
            timestamp_us: 1,
        }
    }

    fn mk_dns(name: &str, rc: &str) -> DnsQuery {
        DnsQuery {
            id: "d1".to_string(),
            query_name: name.to_string(),
            query_type: "A".to_string(),
            response_code: rc.to_string(),
            latency_us: 1000,
            source_ip: "10.0.0.1".to_string(),
            server_ip: "8.8.8.8".to_string(),
            timestamp_us: 1,
        }
    }

    #[test]
    fn record_flow_adds_to_store() {
        let store = NpmStore::new();
        store.record_flow(mk_flow("1.1.1.1", "2.2.2.2", None, None, 100, 200, 500, 0));
        assert_eq!(store.get_summary().total_flows, 1);
    }

    #[test]
    fn record_dns_adds_to_store() {
        let store = NpmStore::new();
        store.record_dns(mk_dns("example.com", "NOERROR"));
        assert_eq!(store.get_summary().dns_queries, 1);
    }

    #[test]
    fn get_flows_empty_store() {
        let store = NpmStore::new();
        assert!(store.get_flows(None, None, 50).is_empty());
    }

    #[test]
    fn get_flows_filters_by_source_ip() {
        let store = NpmStore::new();
        store.record_flow(mk_flow("1.1.1.1", "2.2.2.2", None, None, 1, 1, 1, 0));
        store.record_flow(mk_flow("3.3.3.3", "2.2.2.2", None, None, 1, 1, 1, 0));
        let result = store.get_flows(Some("1.1.1.1"), None, 50);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].source_ip, "1.1.1.1");
    }

    #[test]
    fn get_flows_filters_by_source_service() {
        let store = NpmStore::new();
        store.record_flow(mk_flow("1.1.1.1", "2.2.2.2", Some("web"), None, 1, 1, 1, 0));
        store.record_flow(mk_flow("3.3.3.3", "2.2.2.2", Some("api"), None, 1, 1, 1, 0));
        let result = store.get_flows(Some("web"), None, 50);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn get_flows_filters_by_dest() {
        let store = NpmStore::new();
        store.record_flow(mk_flow("1.1.1.1", "2.2.2.2", None, Some("db"), 1, 1, 1, 0));
        store.record_flow(mk_flow("1.1.1.1", "4.4.4.4", None, Some("cache"), 1, 1, 1, 0));
        let result = store.get_flows(None, Some("db"), 50);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn get_flows_respects_limit() {
        let store = NpmStore::new();
        for _ in 0..5 {
            store.record_flow(mk_flow("1.1.1.1", "2.2.2.2", None, None, 1, 1, 1, 0));
        }
        assert_eq!(store.get_flows(None, None, 2).len(), 2);
    }

    #[test]
    fn get_dns_queries_filters_by_name_substring() {
        let store = NpmStore::new();
        store.record_dns(mk_dns("example.com", "NOERROR"));
        store.record_dns(mk_dns("google.com", "NOERROR"));
        let result = store.get_dns_queries(Some("example"), 50);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn get_dns_queries_no_filter_returns_all() {
        let store = NpmStore::new();
        store.record_dns(mk_dns("a.com", "NOERROR"));
        store.record_dns(mk_dns("b.com", "NOERROR"));
        assert_eq!(store.get_dns_queries(None, 50).len(), 2);
    }

    #[test]
    fn get_summary_empty() {
        let store = NpmStore::new();
        let s = store.get_summary();
        assert_eq!(s.total_flows, 0);
        assert_eq!(s.avg_rtt_us, 0);
        assert_eq!(s.dns_queries, 0);
        assert!(s.top_talkers.is_empty());
    }

    #[test]
    fn get_summary_aggregates_bytes_and_retransmits() {
        let store = NpmStore::new();
        store.record_flow(mk_flow("1.1.1.1", "2.2.2.2", None, None, 100, 50, 1000, 3));
        store.record_flow(mk_flow("1.1.1.1", "2.2.2.2", None, None, 200, 100, 2000, 2));
        let s = store.get_summary();
        assert_eq!(s.total_bytes, 450);
        assert_eq!(s.total_retransmits, 5);
        assert_eq!(s.avg_rtt_us, 1500);
    }

    #[test]
    fn get_summary_counts_dns_failures() {
        let store = NpmStore::new();
        store.record_dns(mk_dns("a.com", "NOERROR"));
        store.record_dns(mk_dns("b.com", "NXDOMAIN"));
        store.record_dns(mk_dns("c.com", "SERVFAIL"));
        let s = store.get_summary();
        assert_eq!(s.dns_queries, 3);
        assert_eq!(s.dns_failures, 2);
    }

    #[test]
    fn get_summary_top_talkers_sorted_by_bytes() {
        let store = NpmStore::new();
        store.record_flow(mk_flow("1.1.1.1", "2.2.2.2", None, None, 100, 0, 1, 0));
        store.record_flow(mk_flow("3.3.3.3", "2.2.2.2", None, None, 500, 0, 1, 0));
        store.record_flow(mk_flow("2.2.2.2", "2.2.2.2", None, None, 300, 0, 1, 0));
        let s = store.get_summary();
        assert_eq!(s.top_talkers[0].0, "3.3.3.3");
        assert_eq!(s.top_talkers[0].1, 500);
    }
}
