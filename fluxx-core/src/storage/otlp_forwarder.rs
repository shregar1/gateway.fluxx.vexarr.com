//! OTLP HTTP/JSON forwarder.
//!
//! Re-emits every received OTLP payload to a configurable downstream
//! OTLP-receiving endpoint. The universal escape hatch from fluxx's
//! storage to any other observability backend — point it at an OTel
//! Collector and you cover every exporter the Collector supports
//! (Datadog, Honeycomb, Splunk, Tempo, Loki, Mimir, Elasticsearch,
//! Jaeger, Zipkin, the cloud-provider firehoses, etc).
//!
//! Architecture: three bounded `mpsc` channels (one per signal),
//! each drained by a background tokio task that POSTs JSON to
//! `{endpoint}/v1/{traces|metrics|logs}`. Forwarding is fire-and-
//! forget from the ingest handler's perspective — saturation drops
//! the durable copy with a warn, the in-memory + DuckDB paths still
//! run.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct OtlpForwarderOptions {
    pub endpoint: String,
    pub timeout_secs: u64,
    pub channel_capacity: usize,
    /// Optional bearer token; sent as `Authorization: Bearer <token>`.
    pub bearer_token: Option<String>,
}

#[derive(Debug)]
pub struct OtlpForwarder {
    logs_tx: mpsc::Sender<ExportLogsServiceRequest>,
    traces_tx: mpsc::Sender<ExportTraceServiceRequest>,
    metrics_tx: mpsc::Sender<ExportMetricsServiceRequest>,
}

impl OtlpForwarder {
    pub fn new(cfg: OtlpForwarderOptions) -> Arc<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(cfg.timeout_secs.max(1)))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(32)
            .build()
            .expect("reqwest client");
        let endpoint = cfg.endpoint.trim_end_matches('/').to_string();
        let bearer = cfg.bearer_token.clone();

        let (logs_tx, mut logs_rx) = mpsc::channel::<ExportLogsServiceRequest>(cfg.channel_capacity);
        let (traces_tx, mut traces_rx) = mpsc::channel::<ExportTraceServiceRequest>(cfg.channel_capacity);
        let (metrics_tx, mut metrics_rx) = mpsc::channel::<ExportMetricsServiceRequest>(cfg.channel_capacity);

        // Logs sink
        {
            let client = client.clone();
            let url = format!("{endpoint}/v1/logs");
            let bearer = bearer.clone();
            tokio::spawn(async move {
                while let Some(req) = logs_rx.recv().await {
                    post_json(&client, &url, &req, bearer.as_deref(), "logs").await;
                }
            });
        }
        // Traces sink
        {
            let client = client.clone();
            let url = format!("{endpoint}/v1/traces");
            let bearer = bearer.clone();
            tokio::spawn(async move {
                while let Some(req) = traces_rx.recv().await {
                    post_json(&client, &url, &req, bearer.as_deref(), "traces").await;
                }
            });
        }
        // Metrics sink
        {
            let client = client.clone();
            let url = format!("{endpoint}/v1/metrics");
            let bearer = bearer.clone();
            tokio::spawn(async move {
                while let Some(req) = metrics_rx.recv().await {
                    post_json(&client, &url, &req, bearer.as_deref(), "metrics").await;
                }
            });
        }

        info!("OtlpForwarder: enabled → {endpoint}/v1/{{logs,traces,metrics}} (channel={})", cfg.channel_capacity);
        Arc::new(Self { logs_tx, traces_tx, metrics_tx })
    }

    /// Fire-and-forget enqueue. Returns immediately. If the channel
    /// is saturated, drops the copy with a warn and returns Ok —
    /// never blocks the OTLP ingest handler.
    pub fn forward_logs(&self, req: ExportLogsServiceRequest) {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.logs_tx.try_send(req) {
            warn!("OtlpForwarder: logs channel saturated, dropping forward copy");
        }
    }

    pub fn forward_traces(&self, req: ExportTraceServiceRequest) {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.traces_tx.try_send(req) {
            warn!("OtlpForwarder: traces channel saturated, dropping forward copy");
        }
    }

    pub fn forward_metrics(&self, req: ExportMetricsServiceRequest) {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.metrics_tx.try_send(req) {
            warn!("OtlpForwarder: metrics channel saturated, dropping forward copy");
        }
    }
}

async fn post_json<T: serde::Serialize + ?Sized>(
    client: &Client,
    url: &str,
    body: &T,
    bearer: Option<&str>,
    signal: &str,
) {
    let mut req = client.post(url).header("Content-Type", "application/json");
    if let Some(t) = bearer {
        req = req.bearer_auth(t);
    }
    let body_bytes = match serde_json::to_vec(body) {
        Ok(b) => b,
        Err(e) => {
            warn!("OtlpForwarder[{signal}]: serialize failed: {e}");
            return;
        }
    };
    let req = req.body(body_bytes);
    match req.send().await {
        Ok(resp) if resp.status().is_success() => {
            // happy path — most requests
        }
        Ok(resp) => {
            warn!("OtlpForwarder[{signal}]: downstream returned {}", resp.status());
        }
        Err(e) => {
            warn!("OtlpForwarder[{signal}]: send failed: {e}");
        }
    }
}
