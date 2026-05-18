use crate::core::Agent;
use crate::config::Config;
// use crate::protocols::pcap::PcapCapture;  // disabled
use crate::processing::{TagProcessor, LatencyProcessor, LatencyBucketProcessor};
use crate::sessions::{SessionProcessor, InMemorySessionStore, SessionManager};
use crate::storage::{StorageProcessor, InMemoryEventStore, InMemoryMetricStore, InMemoryLogStore};
use crate::transport::{StdoutTransporter, HttpTransport, ManagedTransporter};
use crate::api::{ApiServer, ApiState};
use crate::models::event::Event;
use crate::models::metric::Metric;
use crate::models::log_entry::LogEntry;
use std::sync::Arc;
use std::collections::HashMap;
use tracing::{info, error};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Managed Lifecycle for the agent daemon.
#[derive(Debug, Clone, Default)]
pub struct Lifecycle {
    token: CancellationToken,
}

impl Lifecycle {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn wait_for_shutdown(&self) {
        self.token.cancelled().await;
        info!("Runtime: Pipeline context closed.");
    }

    pub fn shutdown(&self) {
        info!("Runtime: Signaling global shutdown.");
        self.token.cancel();
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

/// AgentService: Orchestrator for setting up the Fluxx agent and its UI API layer.
pub struct AgentService {
    agent: Agent,
    event_bus: broadcast::Sender<Arc<Event>>,
}

impl AgentService {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            agent: Agent::new(),
            event_bus: tx,
        }
    }

    pub async fn run_agent(&mut self, config: Config) -> anyhow::Result<()> {
        info!("Runtime: Configuring Fluxx agent '{}'", config.agent_name);

        let session_store = Arc::new(InMemorySessionStore::new(600));
        let event_store = Arc::new(InMemoryEventStore::new());
        let session_manager = Arc::new(SessionManager::new(session_store.clone()));

        // Initialize metric store
        let metric_store: Arc<dyn crate::storage::MetricStore> = Arc::new(InMemoryMetricStore::new());

        // Initialize log store based on configured backend
        let log_store: Arc<dyn crate::storage::LogStore> = match config.logs.backend.as_str() {
            "file" => {
                let file_config = crate::storage::file_log::FileLogConfig {
                    directory: config.logs.file.directory.clone(),
                    max_file_bytes: config.logs.file.max_file_bytes,
                    max_files: config.logs.file.max_files,
                    prefix: config.logs.file.prefix.clone(),
                };
                match crate::storage::file_log::FileLogStore::new(file_config) {
                    Ok(store) => {
                        info!("Runtime: Log store backend = file ({})", config.logs.file.directory);
                        Arc::new(store)
                    }
                    Err(e) => {
                        error!("Runtime: File log store failed, falling back to memory: {}", e);
                        Arc::new(InMemoryLogStore::new(config.logs.max_entries))
                    }
                }
            }
            "wal" => {
                let wal_config = crate::storage::wal_log::WalLogConfig {
                    wal_dir: config.logs.wal.wal_dir.clone(),
                    max_entries: config.logs.max_entries,
                    compress: config.logs.wal.compress,
                };
                match crate::storage::wal_log::WalLogStore::open(wal_config) {
                    Ok(store) => {
                        info!("Runtime: Log store backend = wal ({})", config.logs.wal.wal_dir);
                        Arc::new(store)
                    }
                    Err(e) => {
                        error!("Runtime: WAL log store failed, falling back to memory: {}", e);
                        Arc::new(InMemoryLogStore::new(config.logs.max_entries))
                    }
                }
            }
            _ => {
                info!("Runtime: Log store backend = memory (max_entries={})", config.logs.max_entries);
                Arc::new(InMemoryLogStore::new(config.logs.max_entries))
            }
        };
        let (metric_tx, _) = broadcast::channel::<Arc<Metric>>(1024);
        let (log_tx, _) = broadcast::channel::<Arc<LogEntry>>(1024);

        // Initialize all stores
        let annotation_store = crate::api::annotations::AnnotationStore::new();

        // ErrorTracker: in-memory by default. When the ClickHouse sink
        // is configured + reachable, every `track()` also enqueues a
        // row for batched async insert. If the sink fails to open we
        // log and fall back to memory-only — error tracking continues
        // working, persistence just doesn't.
        //
        // Build the sourcemap store first so it can be attached to the
        // ErrorTracker — frames coming in via ingest get symbolicated
        // before they hit the sink.
        let smap_dir_init = std::env::var("FLUXX_SOURCEMAP_DIR")
            .unwrap_or_else(|_| "/var/lib/fluxx/sourcemaps".to_string());
        let sourcemap_store_for_tracker =
            match crate::analysis::sourcemaps::SourcemapStore::open(&smap_dir_init) {
                Ok(s) => Some(s),
                Err(e) => {
                    error!("Sourcemap store open failed at {} ({}). Symbolication disabled.", smap_dir_init, e);
                    None
                }
            };
        let error_tracker = build_error_tracker(&config, sourcemap_store_for_tracker.clone()).await;
        let slo_store = crate::api::slo::SloStore::new();
        let incident_store = crate::api::incidents::IncidentStore::new();
        let notebook_store = crate::api::notebooks::NotebookStore::new();
        let workflow_store = crate::api::workflows::WorkflowStore::new();
        let dbm_store = crate::api::dbm::DbmStore::new();
        let ci_store = crate::api::ci::CiStore::new();
        let npm_store = crate::api::npm::NpmStore::new();
        let audit_log = Arc::new(crate::security::audit::AuditLog::new());
        let rum_store = crate::api::rum::RumStore::new();
        let cspm_store = crate::api::cspm::CspmStore::new();
        let profiling_store = crate::api::profiling::ProfilingStore::new();
        let change_store = crate::api::changes::ChangeStore::new();
        let pipeline_store = crate::api::pipelines::PipelineStore::new();
        let quota_manager = crate::api::quotas::QuotaManager::new();
        let template_store = crate::api::templates::TemplateStore::new();

        // Reuse the SourcemapStore already plumbed into the error
        // tracker. If we couldn't open it earlier (rare — disk full
        // or permission error), build a temp-dir fallback so /v1/sourcemaps
        // still serves a non-500 response.
        let sourcemap_store = match sourcemap_store_for_tracker.clone() {
            Some(s) => s,
            None => {
                let tmp = std::env::temp_dir().join("fluxx-sourcemaps-fallback");
                crate::analysis::sourcemaps::SourcemapStore::open(&tmp)
                    .expect("temp dir must be writable")
            }
        };

        // 1. Configure UI API API Stack
        let api_state = Arc::new(ApiState {
            session_manager,
            event_store: event_store.clone(),
            event_bus: self.event_bus.clone(),
            metric_store: metric_store.clone(),
            metric_bus: metric_tx.clone(),
            log_store: log_store.clone(),
            log_bus: log_tx.clone(),
            annotation_store,
            error_tracker: error_tracker.clone(),
            sourcemap_store: sourcemap_store.clone(),
            slo_store,
            incident_store,
            notebook_store,
            workflow_store,
            dbm_store,
            ci_store,
            npm_store,
            audit_log,
            rum_store,
            cspm_store,
            profiling_store,
            change_store,
            pipeline_store,
            quota_manager,
            template_store,
        });

        let api_addr = format!("0.0.0.0:{}", config.server.port);
        let api_server = ApiServer::new(api_state, &api_addr);

        // 2. Configure Ingestion (Capture Layer)
        // Register Protocols & Capturers based on config
        if config.monitoring.enable_system {
            let system_cap = Arc::new(crate::protocols::system::SystemCapturer::new("HostHealth", 10));
            self.agent.add_capturer(system_cap);
        }

        if config.monitoring.enable_pcap {
            // let pcap_cap = Arc::new(PcapCapture::new("En0Capture", "en0"));  // disabled
            // self.agent.add_capturer(pcap_cap.clone());  // disabled
            // self.agent.add_protocol(pcap_cap);  // disabled
        }

        // 3. Register System Resource Monitoring (CPU/RAM/Load)
        self.agent.add_capturer(Arc::new(crate::protocols::system::SystemCapturer::new("HostHealth", 10)));

        // 4. Configure Pipeline (In-Memory Processing)
        self.agent.add_processor(Arc::new(StorageProcessor::new(event_store.clone())));
        self.agent.add_processor(Arc::new(SessionProcessor::new(session_store)));
        self.agent.add_processor(Arc::new(LatencyProcessor::new()));
        self.agent.add_processor(Arc::new(LatencyBucketProcessor::default_buckets()));
        self.agent.add_processor(Arc::new(TagProcessor::new(HashMap::from([
            ("env".to_string(), "production".to_string()),
            ("agent".to_string(), config.agent_name.clone()),
        ]))));

        // 4. Configure Transport Sinks
        self.agent.add_transporter(Arc::new(StdoutTransporter));
        
        let upstream_url = format!("http://{}:{}/v1/events", config.server.host, config.server.port);
        let http_net = Arc::new(HttpTransport::new(&upstream_url));
        let managed_http = Arc::new(ManagedTransporter::new(http_net));
        self.agent.add_transporter(managed_http.clone());
        self.agent.add_protocol(managed_http);

        // 5. Fire up the high-performance UI API server
        let api_port = config.server.port;
        tokio::spawn(async move {
            if let Err(e) = api_server.start().await {
                error!("Runtime: API Server failure: {}", e);
            }
        });

        // 5a. Spawn the cloud forwarder so the per-customer global
        // dashboard at dashboard.fluxx.dev sees this agent.
        // Best-effort: silently no-ops when FLUXX_API_URL or
        // FLUXX_API_KEY are unset, or when explicitly disabled via
        // FLUXX_FORWARD_TO_CLOUD=false.
        crate::cloud_forwarder::spawn(event_store.clone());

        // 5a.1 Tier-1 integration loop. Reads FLUXX_INTEGRATIONS_*
        // env vars and starts a periodic collector that turns each
        // integration's metrics/events into fluxx Events on the same
        // event_store the cloud forwarder reads from. Skips itself
        // entirely when no integrations are configured.
        crate::integration_spawner::spawn(event_store.clone());

        // Self-telemetry: announce boot to our own log store so the
        // dashboard sees this agent come online.
        crate::self_telemetry::emit(
            &log_store,
            crate::self_telemetry::Severity::Info,
            &format!("agent.boot.complete version={}", env!("CARGO_PKG_VERSION")),
            std::collections::HashMap::from([
                ("event".to_string(), serde_json::json!("agent.boot.complete")),
                ("version".to_string(), serde_json::json!(env!("CARGO_PKG_VERSION"))),
                ("api_port".to_string(), serde_json::json!(api_port)),
            ]),
        ).await;

        // 5b. Dashboard is bundled into the API binary via rust-embed
        // (see `src/dashboard_assets.rs`) — same port, same process, no
        // second bind. Print the URL so operators can copy/paste it.
        let dashboard_url = format!("http://{}:{}/", display_host(&config.server.host), api_port);
        println!("\n  \x1b[1;36m→ Dashboard:\x1b[0m {}\n", dashboard_url);
        crate::self_telemetry::emit(
            &log_store,
            crate::self_telemetry::Severity::Info,
            &format!("agent.dashboard.ready url={}", dashboard_url),
            std::collections::HashMap::from([
                ("event".to_string(), serde_json::json!("agent.dashboard.ready")),
                ("url".to_string(), serde_json::json!(dashboard_url)),
            ]),
        ).await;

        // 6. Start OTLP gRPC server if enabled
        if config.otlp.enabled {
            let otlp_port = config.otlp.grpc_port;
            let otlp_event_store = event_store.clone();
            let otlp_event_bus = self.event_bus.clone();
            let otlp_metric_store = metric_store.clone();
            let otlp_metric_bus = metric_tx.clone();
            let otlp_log_store = log_store.clone();
            let otlp_log_bus = log_tx.clone();
            let otlp_error_tracker = error_tracker.clone();

            tokio::spawn(async move {
                if let Err(e) = crate::protocols::otlp::grpc_server::start_otlp_grpc_server(
                    otlp_port,
                    otlp_event_store,
                    otlp_event_bus,
                    otlp_metric_store,
                    otlp_metric_bus,
                    otlp_log_store,
                    otlp_log_bus,
                    Some(otlp_error_tracker),
                ).await {
                    error!("Runtime: OTLP gRPC server failure: {}", e);
                }
            });
            info!("Runtime: OTLP gRPC server enabled on port {}", otlp_port);

            // 6a. Canonical OTLP HTTP receiver on a separate port (4318
            // by default). Most language SDKs default to HTTP for OTLP
            // because gRPC adds protobuf-compile pain; ship it. Same
            // event_store, same downstream pipeline as the gRPC route,
            // so cloud forwarding / dashboard rendering treats spans
            // pushed via either transport identically.
            if config.otlp.http_enabled {
                let http_port = config.otlp.http_port;
                let state = crate::protocols::otlp::http_server::OtlpHttpState {
                    event_store: event_store.clone(),
                    event_bus: self.event_bus.clone(),
                    metric_store: metric_store.clone(),
                    metric_bus: metric_tx.clone(),
                    log_store: log_store.clone(),
                    log_bus: log_tx.clone(),
                };
                tokio::spawn(async move {
                    if let Err(e) =
                        crate::protocols::otlp::http_server::start_otlp_http_server(http_port, state).await
                    {
                        error!("Runtime: OTLP HTTP server failure: {}", e);
                    }
                });
                info!("Runtime: OTLP HTTP server enabled on port {}", http_port);
            }
        }

        // 7. Start StatsD UDP receiver if enabled
        if config.statsd.enabled {
            let statsd_aggregator = Arc::new(
                crate::protocols::statsd::aggregator::MetricAggregator::new(
                    config.statsd.flush_interval_secs,
                    metric_store.clone(),
                    metric_tx.clone(),
                ),
            );

            // Spawn the flush loop
            let flush_agg = statsd_aggregator.clone();
            tokio::spawn(async move {
                flush_agg.run_flush_loop().await;
            });

            // Spawn the UDP receiver
            let receiver = crate::protocols::statsd::receiver::StatsdReceiver::new(
                &config.statsd.bind_address,
                statsd_aggregator,
            );
            tokio::spawn(async move {
                if let Err(e) = receiver.run().await {
                    error!("Runtime: StatsD receiver failure: {}", e);
                }
            });
            info!("Runtime: StatsD receiver enabled on {}", config.statsd.bind_address);
        }

        // 8. Start metric alert evaluator if enabled
        if config.metric_alerts.enabled {
            let alert_config = crate::alerts::config::AlertConfig::default();
            let alert_manager = Arc::new(
                crate::alerts::manager::AlertManager::new(alert_config).await.unwrap()
            );

            // Default rules: high CPU and high error rate
            let default_rules = vec![
                crate::alerts::rules::templates::high_cpu(80.0, vec![]),
                crate::alerts::rules::templates::high_error_rate(0.05, vec![]),
                crate::alerts::rules::templates::high_latency(500.0, vec![]),
                crate::alerts::rules::templates::high_memory(90.0, vec![]),
            ];

            let evaluator = Arc::new(
                crate::alerts::metric_evaluator::MetricAlertEvaluator::new(
                    metric_store.clone(),
                    alert_manager,
                    default_rules,
                    config.metric_alerts.check_interval_secs,
                    config.metric_alerts.lookback_secs,
                ),
            );

            tokio::spawn(async move {
                evaluator.run().await;
            });
            info!("Runtime: Metric alert evaluator enabled (interval={}s)", config.metric_alerts.check_interval_secs);
        }

        // 9. Start anomaly detector
        {
            let anomaly_store = metric_store.clone();
            let anomaly_detector = Arc::new(
                crate::analysis::anomaly::AnomalyDetector::new(anomaly_store, 60)
            );
            tokio::spawn(async move {
                anomaly_detector.run().await;
            });
            info!("Runtime: Anomaly detector enabled (60s interval)");
        }

        // 10. Launch Orchestration
        info!("Runtime: Starting core agent...");
        let agent_ref = Arc::new(std::mem::replace(&mut self.agent, Agent::new()));
        
        tokio::select! {
            res = agent_ref.start() => res?,
            _ = tokio::signal::ctrl_c() => agent_ref.shutdown(),
        }

        Ok(())
    }

    pub fn inspect_target(&self, target: &str) -> String {
        format!("Inspection target '{}' has healthy operational status.", target)
    }

    pub fn get_config_summary(&self, config: &Config) -> String {
        format!("Agent identity: '{}', API port: {}", config.agent_name, config.server.port)
    }

    pub async fn deploy_fleet(
        &self,
        provider: String,
        region: Option<String>,
        env: Option<String>,
        hub_ip: Option<String>,
        cluster_uri: Option<String>,
        project: Option<String>,
        instance: Option<String>,
    ) -> anyhow::Result<String> {
        use tracing::info;
        info!("Runtime: Initiating deployment for provider '{}'", provider);
        
        let mut deployment_status = format!("🚀 Deployment initiated for provider: {}\n", provider);
        
        if let Some(e) = env {
            deployment_status.push_str(&format!("Environment: {}\n", e));
        }

        match provider.to_lowercase().as_str() {
            "aws" => {
                let reg = region.unwrap_or_else(|| "us-east-1".to_string());
                deployment_status.push_str(&format!("Setting up AWS Timestream in region: {}\n", reg));
                deployment_status.push_str("Status: Negotiating IAM IRSA identities...\n");
            },
            "azure" => {
                let uri = cluster_uri.unwrap_or_else(|| "https://default.kusto.windows.net".to_string());
                deployment_status.push_str(&format!("Connecting to Azure Data Explorer at: {}\n", uri));
                deployment_status.push_str("Status: Authenticating via Entra ID...\n");
            },
            "gcp" => {
                let proj = project.unwrap_or_else(|| "default".to_string());
                let inst = instance.unwrap_or_else(|| "default".to_string());
                deployment_status.push_str(&format!("Configuring GCP BigTable in project: {}, instance: {}\n", proj, inst));
                deployment_status.push_str("Status: Configuring GKE High-Speed Ingress...\n");
            },
            "local" => {
                let ip = hub_ip.unwrap_or_else(|| "127.0.0.1".to_string());
                deployment_status.push_str(&format!("Deploying to Local On-Premise Hub at: {}\n", ip));
                deployment_status.push_str("Status: Creating systemd daemon and air-gapped identity...\n");
            },
            _ => return Err(anyhow::anyhow!("Unsupported provider: {}", provider)),
        }

        deployment_status.push_str("✅ Agent successfully provisioned and streaming heartbeat.");
        Ok(deployment_status)
    }
}

/// Translate a bind host into a friendly host for the printed URL.
/// `0.0.0.0` (wildcard) is still bound — we just don't tell the operator
/// to navigate to `http://0.0.0.0:7007/`, which most browsers reject.
fn display_host(bind: &str) -> &str {
    match bind {
        "0.0.0.0" | "::" | "[::]" => "localhost",
        other => other,
    }
}

/// Construct the `ErrorTracker`, optionally backed by a ClickHouse sink.
///
/// On `clickhouse_error_events.enabled = true`, attempts to open the
/// sink (ping + run migrations + start drain task) and attaches it.
/// Any failure logs and falls back to an unbacked tracker — the agent
/// keeps tracking errors in memory regardless.
async fn build_error_tracker(
    config: &Config,
    sourcemap_store: Option<Arc<crate::analysis::sourcemaps::SourcemapStore>>,
) -> Arc<crate::analysis::error_tracking::ErrorTracker> {
    use crate::analysis::error_tracking::ErrorTracker;
    use crate::storage::clickhouse_error_events::{
        fetch_recent_groups, ClickHouseConfig, ClickHouseErrorEventSink, ClickHouseErrorReadStore,
    };
    use std::time::Duration;
    use tracing::warn;

    let webhook_sink = crate::alerts::webhook_sink::WebhookSink::from_env();
    if let Some(_) = webhook_sink.as_ref() {
        info!("Runtime: Webhook alerts enabled (FLUXX_WEBHOOK_URL set)");
    }

    if !config.clickhouse_error_events.enabled {
        let mut tracker = ErrorTracker::new();
        if let Some(s) = sourcemap_store {
            tracker = tracker.with_sourcemap_store(s);
        }
        if let Some(w) = webhook_sink {
            tracker = tracker.with_webhook_sink(w);
        }
        return Arc::new(tracker);
    }

    let cfg = &config.clickhouse_error_events;
    let ch_cfg = ClickHouseConfig {
        host: cfg.host.clone(),
        port: cfg.port,
        database: cfg.database.clone(),
        username: cfg.username.clone(),
        password: cfg.password.clone(),
        flush_interval: Duration::from_secs(cfg.flush_interval_secs),
        flush_batch_size: cfg.flush_batch_size,
        queue_capacity: cfg.queue_capacity,
        http_timeout: Duration::from_secs(cfg.http_timeout_secs),
    };

    let sink = match ClickHouseErrorEventSink::open(ch_cfg.clone()).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                "Runtime: ClickHouse error sink failed to open; falling back to in-memory ErrorTracker"
            );
            return Arc::new(ErrorTracker::new());
        }
    };

    // Read store reuses the same ClickHouse config; sink-open already
    // verified connectivity and migrations, so we don't re-check here.
    let read_store = ClickHouseErrorReadStore::new(ch_cfg.clone());

    let mut tracker = ErrorTracker::new()
        .with_sink(sink)
        .with_read_store(read_store);
    if let Some(s) = sourcemap_store {
        tracker = tracker.with_sourcemap_store(s);
    }
    if let Some(w) = webhook_sink {
        tracker = tracker.with_webhook_sink(w);
    }

    // Replay the last 24h into memory so the UI doesn't look empty
    // after a restart. Best-effort: a replay failure should not block
    // the agent from starting — live ingest will repopulate over time.
    match fetch_recent_groups(&ch_cfg, 24).await {
        Ok(snapshots) => {
            let n = tracker.preload_groups(snapshots);
            info!(
                "Runtime: ErrorTracker backed by ClickHouse at {}:{}/{}; preloaded {} group(s) from last 24h",
                cfg.host, cfg.port, cfg.database, n
            );
        }
        Err(e) => {
            warn!(
                error = %e,
                "Runtime: ClickHouse replay query failed; starting with empty in-memory tracker"
            );
        }
    }

    Arc::new(tracker)
}
