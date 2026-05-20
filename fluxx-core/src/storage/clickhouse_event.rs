//! ClickHouse-backed EventStore (traces). Same batching pattern as
//! `clickhouse_log` — see that module's header for durability
//! semantics and rationale.

use crate::core::error::Result;
use crate::models::event::{Event, EventSeverity, Metadata, ProtocolType};
use crate::storage::clickhouse_client::ClickHouseClient;
use crate::storage::event_store::{EventStore, TimeWindow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS {db}.events (
    id              String,
    session_id      Nullable(String),
    project_id      Nullable(String),
    environment_id  Nullable(String),
    service_id      Nullable(String),
    timestamp_us    UInt64,
    protocol        LowCardinality(String),
    latency_us      UInt64,
    severity        LowCardinality(String),
    payload         Nullable(String),
    metadata        String
) ENGINE = MergeTree() ORDER BY (timestamp_us)
"#;

#[derive(Debug, Serialize, Deserialize)]
struct EventRow {
    id: String,
    session_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    service_id: Option<String>,
    timestamp_us: u64,
    protocol: String,
    latency_us: u64,
    severity: String,
    payload: Option<String>,
    /// `Metadata` is serialized to a JSON String column rather than
    /// a Map<String,String> because the source struct has typed
    /// fields (`source_ip`, etc.) alongside a flattened `extra` —
    /// preserving fidelity is easier as opaque JSON.
    metadata: String,
}

impl From<&Event> for EventRow {
    fn from(e: &Event) -> Self {
        Self {
            id: e.id.clone(),
            session_id: e.session_id.clone(),
            project_id: e.project_id.clone(),
            environment_id: e.environment_id.clone(),
            service_id: e.service_id.clone(),
            timestamp_us: e.timestamp_us,
            protocol: protocol_to_str(e.protocol).to_string(),
            latency_us: e.latency_us,
            severity: severity_to_str(e.severity).to_string(),
            payload: e.payload.clone(),
            metadata: serde_json::to_string(&e.metadata).unwrap_or_else(|_| "{}".to_string()),
        }
    }
}

impl EventRow {
    fn into_event(self) -> Event {
        let metadata: Metadata = serde_json::from_str(&self.metadata).unwrap_or_default();
        Event {
            id: self.id,
            session_id: self.session_id,
            project_id: self.project_id,
            environment_id: self.environment_id,
            service_id: self.service_id,
            timestamp_us: self.timestamp_us,
            protocol: str_to_protocol(&self.protocol),
            latency_us: self.latency_us,
            severity: str_to_severity(&self.severity),
            payload: self.payload,
            metadata,
        }
    }
}

fn protocol_to_str(p: ProtocolType) -> &'static str {
    match p {
        ProtocolType::Http => "http",
        ProtocolType::Ws => "ws",
        ProtocolType::Tcp => "tcp",
        ProtocolType::Udp => "udp",
        ProtocolType::Grpc => "grpc",
        ProtocolType::Quic => "quic",
        ProtocolType::Ebpf => "ebpf",
        ProtocolType::Mqtt => "mqtt",
        ProtocolType::Modbus => "modbus",
        ProtocolType::System => "system",
        ProtocolType::Otlp => "otlp",
        ProtocolType::StatsD => "statsd",
        ProtocolType::Log => "log",
        ProtocolType::Other => "other",
    }
}

fn str_to_protocol(s: &str) -> ProtocolType {
    match s {
        "http" => ProtocolType::Http,
        "ws" => ProtocolType::Ws,
        "tcp" => ProtocolType::Tcp,
        "udp" => ProtocolType::Udp,
        "grpc" => ProtocolType::Grpc,
        "quic" => ProtocolType::Quic,
        "ebpf" => ProtocolType::Ebpf,
        "mqtt" => ProtocolType::Mqtt,
        "modbus" => ProtocolType::Modbus,
        "system" => ProtocolType::System,
        "otlp" => ProtocolType::Otlp,
        "statsd" => ProtocolType::StatsD,
        "log" => ProtocolType::Log,
        _ => ProtocolType::Other,
    }
}

fn severity_to_str(s: EventSeverity) -> &'static str {
    match s {
        EventSeverity::Info => "info",
        EventSeverity::Low => "low",
        EventSeverity::Medium => "medium",
        EventSeverity::High => "high",
        EventSeverity::Critical => "critical",
    }
}

fn str_to_severity(s: &str) -> EventSeverity {
    match s {
        "low" => EventSeverity::Low,
        "medium" => EventSeverity::Medium,
        "high" => EventSeverity::High,
        "critical" => EventSeverity::Critical,
        _ => EventSeverity::Info,
    }
}

#[derive(Debug)]
pub struct ClickHouseEventStore {
    client: Arc<ClickHouseClient>,
    tx: mpsc::Sender<Event>,
}

impl ClickHouseEventStore {
    pub async fn new(
        client: Arc<ClickHouseClient>,
        queue_capacity: usize,
        flush_batch_size: usize,
        flush_interval: Duration,
    ) -> Result<Self> {
        let schema_sql = SCHEMA.replace("{db}", client.database());
        client.execute(&schema_sql).await?;
        info!("ClickHouseEventStore: schema ensured ({}.events)", client.database());
        let (tx, rx) = mpsc::channel::<Event>(queue_capacity);
        spawn_drainer(client.clone(), rx, flush_batch_size, flush_interval);
        Ok(Self { client, tx })
    }
}

fn spawn_drainer(
    client: Arc<ClickHouseClient>,
    mut rx: mpsc::Receiver<Event>,
    batch_size: usize,
    interval: Duration,
) {
    tokio::spawn(async move {
        let mut buf: Vec<EventRow> = Vec::with_capacity(batch_size);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(e) => {
                        buf.push(EventRow::from(&e));
                        if buf.len() >= batch_size {
                            flush(&client, &mut buf).await;
                        }
                    }
                    None => { flush(&client, &mut buf).await; break; }
                },
                _ = ticker.tick() => flush(&client, &mut buf).await,
            }
        }
    });
}

async fn flush(client: &ClickHouseClient, buf: &mut Vec<EventRow>) {
    if buf.is_empty() { return; }
    let to_send: Vec<EventRow> = std::mem::take(buf);
    if let Err(e) = client.insert_json("events", &to_send).await {
        warn!("ClickHouseEventStore: flush failed ({} rows dropped): {e}", to_send.len());
    }
}

#[async_trait]
impl EventStore for ClickHouseEventStore {
    async fn insert(&self, event: Event) -> Result<()> {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(event) {
            warn!("ClickHouseEventStore: queue saturated, dropping event");
        }
        Ok(())
    }

    async fn query(&self, window: TimeWindow) -> Result<Vec<Event>> {
        let sql = format!(
            "SELECT id, session_id, project_id, environment_id, service_id, timestamp_us, protocol, latency_us, severity, payload, metadata \
             FROM {}.events WHERE timestamp_us BETWEEN {} AND {} ORDER BY timestamp_us",
            self.client.database(), window.start_us, window.end_us
        );
        let rows: Vec<EventRow> = self.client.query_json(&sql).await?;
        Ok(rows.into_iter().map(EventRow::into_event).collect())
    }

    async fn len(&self) -> Result<usize> {
        let sql = format!("SELECT count() FROM {}.events", self.client.database());
        Ok(self.client.query_count(&sql).await? as usize)
    }

    async fn list_events(
        &self,
        project_id: Option<String>,
        protocol: Option<ProtocolType>,
        severity: Option<EventSeverity>,
        service_id: Option<String>,
    ) -> Vec<Event> {
        let mut sql = format!(
            "SELECT id, session_id, project_id, environment_id, service_id, timestamp_us, protocol, latency_us, severity, payload, metadata \
             FROM {}.events WHERE 1=1",
            self.client.database()
        );
        if let Some(p) = project_id {
            sql.push_str(&format!(" AND project_id = '{}'", p.replace('\'', "''")));
        }
        if let Some(p) = protocol {
            sql.push_str(&format!(" AND protocol = '{}'", protocol_to_str(p)));
        }
        if let Some(s) = severity {
            sql.push_str(&format!(" AND severity = '{}'", severity_to_str(s)));
        }
        if let Some(s) = service_id {
            sql.push_str(&format!(" AND service_id = '{}'", s.replace('\'', "''")));
        }
        sql.push_str(" ORDER BY timestamp_us DESC");
        match self.client.query_json::<EventRow>(&sql).await {
            Ok(rows) => rows.into_iter().map(EventRow::into_event).collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn list_events_since(&self, since_us: u64) -> Vec<Event> {
        let sql = format!(
            "SELECT id, session_id, project_id, environment_id, service_id, timestamp_us, protocol, latency_us, severity, payload, metadata \
             FROM {}.events WHERE timestamp_us > {since_us} ORDER BY timestamp_us",
            self.client.database()
        );
        self.client
            .query_json::<EventRow>(&sql)
            .await
            .map(|rs| rs.into_iter().map(EventRow::into_event).collect())
            .unwrap_or_default()
    }

    async fn get_stats(&self) -> serde_json::Value {
        let total: u64 = self
            .client
            .query_count(&format!("SELECT count() FROM {}.events", self.client.database()))
            .await
            .unwrap_or(0);
        let by_proto_sql = format!(
            "SELECT protocol AS p, count() AS c FROM {}.events GROUP BY protocol",
            self.client.database()
        );
        let mut protocols = serde_json::Map::new();
        if let Ok(rows) = self.client.query_json::<serde_json::Value>(&by_proto_sql).await {
            for row in rows {
                if let (Some(p), Some(c)) = (
                    row.get("p").and_then(|v| v.as_str()),
                    row.get("c").and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))),
                ) {
                    protocols.insert(p.to_string(), serde_json::Value::from(c));
                }
            }
        }
        serde_json::json!({
            "total_events": total,
            "backend": "clickhouse",
            "protocols": serde_json::Value::Object(protocols),
        })
    }
}
