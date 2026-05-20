//! ClickHouse-backed MetricStore. Same batching pattern.

use crate::core::error::Result;
use crate::models::metric::{AggregatedMetric, Metric, MetricType};
use crate::storage::clickhouse_client::ClickHouseClient;
use crate::storage::event_store::TimeWindow;
use crate::storage::metric_store::MetricStore;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS {db}.metrics (
    id              String,
    name            LowCardinality(String),
    value           Float64,
    metric_type     LowCardinality(String),
    tags            Map(String, String),
    timestamp_us    UInt64,
    sample_rate     Float64,
    service_id      LowCardinality(Nullable(String)),
    project_id      Nullable(String),
    environment_id  Nullable(String)
) ENGINE = MergeTree() ORDER BY (name, timestamp_us)
"#;

#[derive(Debug, Serialize, Deserialize)]
struct MetricRow {
    id: String,
    name: String,
    value: f64,
    metric_type: String,
    tags: HashMap<String, String>,
    timestamp_us: u64,
    sample_rate: f64,
    service_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
}

impl From<&Metric> for MetricRow {
    fn from(m: &Metric) -> Self {
        Self {
            id: m.id.clone(),
            name: m.name.clone(),
            value: m.value,
            metric_type: metric_type_to_str(m.metric_type).to_string(),
            tags: m.tags.clone(),
            timestamp_us: m.timestamp_us,
            sample_rate: m.sample_rate,
            service_id: m.service_id.clone(),
            project_id: m.project_id.clone(),
            environment_id: m.environment_id.clone(),
        }
    }
}

impl MetricRow {
    fn into_metric(self) -> Metric {
        Metric {
            id: self.id,
            name: self.name,
            value: self.value,
            metric_type: str_to_metric_type(&self.metric_type),
            tags: self.tags,
            timestamp_us: self.timestamp_us,
            sample_rate: self.sample_rate,
            service_id: self.service_id,
            project_id: self.project_id,
            environment_id: self.environment_id,
        }
    }
}

fn metric_type_to_str(t: MetricType) -> &'static str {
    match t {
        MetricType::Counter => "counter",
        MetricType::Gauge => "gauge",
        MetricType::Histogram => "histogram",
        MetricType::Set => "set",
        MetricType::Timing => "timing",
        MetricType::Distribution => "distribution",
        MetricType::Summary => "summary",
    }
}

fn str_to_metric_type(s: &str) -> MetricType {
    match s {
        "counter" => MetricType::Counter,
        "histogram" => MetricType::Histogram,
        "set" => MetricType::Set,
        "timing" => MetricType::Timing,
        "distribution" => MetricType::Distribution,
        "summary" => MetricType::Summary,
        _ => MetricType::Gauge,
    }
}

#[derive(Debug)]
pub struct ClickHouseMetricStore {
    client: Arc<ClickHouseClient>,
    tx: mpsc::Sender<Metric>,
}

impl ClickHouseMetricStore {
    pub async fn new(
        client: Arc<ClickHouseClient>,
        queue_capacity: usize,
        flush_batch_size: usize,
        flush_interval: Duration,
    ) -> Result<Self> {
        let schema_sql = SCHEMA.replace("{db}", client.database());
        client.execute(&schema_sql).await?;
        info!("ClickHouseMetricStore: schema ensured ({}.metrics)", client.database());
        let (tx, rx) = mpsc::channel::<Metric>(queue_capacity);
        spawn_drainer(client.clone(), rx, flush_batch_size, flush_interval);
        Ok(Self { client, tx })
    }
}

fn spawn_drainer(
    client: Arc<ClickHouseClient>,
    mut rx: mpsc::Receiver<Metric>,
    batch_size: usize,
    interval: Duration,
) {
    tokio::spawn(async move {
        let mut buf: Vec<MetricRow> = Vec::with_capacity(batch_size);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(m) => {
                        buf.push(MetricRow::from(&m));
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

async fn flush(client: &ClickHouseClient, buf: &mut Vec<MetricRow>) {
    if buf.is_empty() { return; }
    let to_send: Vec<MetricRow> = std::mem::take(buf);
    if let Err(e) = client.insert_json("metrics", &to_send).await {
        warn!("ClickHouseMetricStore: flush failed ({} rows dropped): {e}", to_send.len());
    }
}

#[async_trait]
impl MetricStore for ClickHouseMetricStore {
    async fn insert(&self, metric: Metric) -> Result<()> {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(metric) {
            warn!("ClickHouseMetricStore: queue saturated, dropping metric");
        }
        Ok(())
    }

    async fn query(
        &self,
        name: Option<&str>,
        tags: &HashMap<String, String>,
        window: TimeWindow,
    ) -> Result<Vec<Metric>> {
        let mut sql = format!(
            "SELECT id, name, value, metric_type, tags, timestamp_us, sample_rate, service_id, project_id, environment_id \
             FROM {}.metrics WHERE timestamp_us BETWEEN {} AND {}",
            self.client.database(), window.start_us, window.end_us
        );
        if let Some(n) = name {
            sql.push_str(&format!(" AND name = '{}'", n.replace('\'', "''")));
        }
        sql.push_str(" ORDER BY timestamp_us");
        let rows: Vec<MetricRow> = self.client.query_json(&sql).await?;
        Ok(rows
            .into_iter()
            .map(MetricRow::into_metric)
            .filter(|m| tags.iter().all(|(k, v)| m.tags.get(k) == Some(v)))
            .collect())
    }

    async fn get_aggregated(
        &self,
        name: &str,
        window: TimeWindow,
        interval_secs: u64,
    ) -> Result<Vec<AggregatedMetric>> {
        let sql = format!(
            "SELECT timestamp_us, value FROM {}.metrics \
             WHERE name = '{}' AND timestamp_us BETWEEN {} AND {}",
            self.client.database(),
            name.replace('\'', "''"),
            window.start_us,
            window.end_us
        );
        #[derive(Deserialize)]
        struct Row { timestamp_us: u64, value: f64 }
        let rows: Vec<Row> = self.client.query_json(&sql).await?;
        let interval_us = (interval_secs * 1_000_000) as u64;
        let mut buckets: BTreeMap<u64, Vec<f64>> = BTreeMap::new();
        for r in rows {
            let key = (r.timestamp_us / interval_us) * interval_us;
            buckets.entry(key).or_default().push(r.value);
        }
        Ok(buckets
            .into_iter()
            .map(|(ts, mut v)| AggregatedMetric::from_values(name, ts, &mut v))
            .collect())
    }

    async fn get_names(&self) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT DISTINCT name FROM {}.metrics ORDER BY name",
            self.client.database()
        );
        #[derive(Deserialize)]
        struct NameRow { name: String }
        let rows: Vec<NameRow> = self.client.query_json(&sql).await?;
        Ok(rows.into_iter().map(|r| r.name).collect())
    }

    async fn len(&self) -> Result<usize> {
        let sql = format!("SELECT count() FROM {}.metrics", self.client.database());
        Ok(self.client.query_count(&sql).await? as usize)
    }
}
