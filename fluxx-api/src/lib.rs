//! fluxx-api — HTTP API surface for the Fluxx agent.
//!
//! The api consumes a `fluxx_core::RuntimeContext` (built by
//! `AgentService::prepare_runtime` in fluxx-core) and adds its own
//! in-memory stores for the read/write endpoints that don't have a
//! persistent backing yet (SLOs, incidents, notebooks, workflows,
//! DBM, CI, NPM, RUM, CSPM, profiling, changes, pipelines, quotas,
//! templates, annotations).
//!
//! The `fluxx` binary in `fluxx-core/src/main.rs` is the only place
//! that depends on both crates — that's where the runtime context
//! and the api server get glued together and joined.

pub mod ai_rca;
pub mod annotations;
pub mod auth;
pub mod changes;
pub mod ci;
pub mod cluster;
pub mod correlate;
pub mod cost;
pub mod cspm;
pub mod dashboard;
pub mod dbm;
pub mod errors;
pub mod incidents;
pub mod ingest;
pub mod logs;
pub mod metrics;
pub mod nl_dashboard;
pub mod notebooks;
pub mod npm;
pub mod otlp_http_handlers;
pub mod pipelines;
pub mod predictive;
pub mod profiling;
pub mod prometheus;
pub mod promql;
pub mod quotas;
pub mod recording_rules;
pub mod response;
pub mod rum;
pub mod server;
pub mod service_map;
pub mod slo;
pub mod sourcemaps;
pub mod templates;
pub mod traces;
pub mod workflows;

use std::sync::Arc;
use tokio::sync::broadcast;

use fluxx_core::analysis::error_tracking::ErrorTracker;
use fluxx_core::analysis::sourcemaps::SourcemapStore;
use fluxx_core::core::RuntimeContext;
use fluxx_core::models::event::Event;
use fluxx_core::models::log_entry::LogEntry;
use fluxx_core::models::metric::Metric;
use fluxx_core::security::audit::AuditLog;
use fluxx_core::sessions::SessionManager;
use fluxx_core::storage::event_store::EventStore;
use fluxx_core::storage::log_store::LogStore;
use fluxx_core::storage::metric_store::MetricStore;

use crate::annotations::AnnotationStore;
use crate::changes::ChangeStore;
use crate::ci::CiStore;
use crate::cspm::CspmStore;
use crate::dbm::DbmStore;
use crate::incidents::IncidentStore;
use crate::notebooks::NotebookStore;
use crate::npm::NpmStore;
use crate::pipelines::PipelineStore;
use crate::profiling::ProfilingStore;
use crate::quotas::QuotaManager;
use crate::rum::RumStore;
use crate::slo::SloStore;
use crate::templates::TemplateStore;
use crate::workflows::WorkflowStore;

/// Shared state every Axum handler in this crate `State<Arc<ApiState>>`s
/// into. The agent-runtime primitives at the top come from
/// `fluxx_core::core::RuntimeContext`; the api-only stores below are
/// built inside `build_api` and live for the lifetime of the server.
pub struct ApiState {
    pub session_manager: Arc<SessionManager>,
    pub event_store: Arc<dyn EventStore>,
    pub event_bus: broadcast::Sender<Arc<Event>>,
    pub metric_store: Arc<dyn MetricStore>,
    pub metric_bus: broadcast::Sender<Arc<Metric>>,
    pub log_store: Arc<dyn LogStore>,
    pub log_bus: broadcast::Sender<Arc<LogEntry>>,
    pub sampler: Arc<fluxx_core::sampling::Sampler>,
    pub annotation_store: AnnotationStore,
    pub error_tracker: Arc<ErrorTracker>,
    pub sourcemap_store: Arc<SourcemapStore>,
    pub slo_store: SloStore,
    pub incident_store: IncidentStore,
    pub notebook_store: NotebookStore,
    pub workflow_store: WorkflowStore,
    pub dbm_store: DbmStore,
    pub ci_store: CiStore,
    pub npm_store: NpmStore,
    pub audit_log: Arc<AuditLog>,
    pub rum_store: RumStore,
    pub cspm_store: CspmStore,
    pub profiling_store: ProfilingStore,
    pub change_store: ChangeStore,
    pub pipeline_store: PipelineStore,
    pub quota_manager: QuotaManager,
    pub template_store: TemplateStore,
}

pub use response::StandardResponse;
pub use server::ApiServer;
// Convenience re-export so downstream callers (the binary, tests)
// don't need to depend on fluxx-core directly to refer to `Insight`.
pub use fluxx_core::analysis::insights::Insight;

/// Build the HTTP API server from a runtime context.
///
/// Consumes the agent-runtime primitives in `ctx` (session manager,
/// stores, buses, error tracker, sourcemap store, audit log) and
/// instantiates every in-memory store the api owns. Returns an
/// `ApiServer` ready to `.start()` against `addr`.
pub fn build_api(ctx: RuntimeContext, addr: &str) -> ApiServer {
    let api_state = Arc::new(ApiState {
        session_manager: ctx.session_manager,
        event_store: ctx.event_store,
        event_bus: ctx.event_bus,
        metric_store: ctx.metric_store,
        metric_bus: ctx.metric_bus,
        log_store: ctx.log_store,
        log_bus: ctx.log_bus,
        error_tracker: ctx.error_tracker,
        sourcemap_store: ctx.sourcemap_store,
        audit_log: ctx.audit_log,
        sampler: ctx.sampler,
        annotation_store: AnnotationStore::new(),
        slo_store: SloStore::new(),
        incident_store: IncidentStore::new(),
        notebook_store: NotebookStore::new(),
        workflow_store: WorkflowStore::new(),
        dbm_store: DbmStore::new(),
        ci_store: CiStore::new(),
        npm_store: NpmStore::new(),
        rum_store: RumStore::new(),
        cspm_store: CspmStore::new(),
        profiling_store: ProfilingStore::new(),
        change_store: ChangeStore::new(),
        pipeline_store: PipelineStore::new(),
        quota_manager: QuotaManager::new(),
        template_store: TemplateStore::new(),
    });
    ApiServer::new(api_state, addr)
}
