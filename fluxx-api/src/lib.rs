pub mod server;
pub mod response;
pub mod logs;
pub mod metrics;
pub mod service_map;
pub mod dashboard;
pub mod annotations;
pub mod promql;
pub mod cluster;
pub mod traces;
pub mod recording_rules;
pub mod slo;
pub mod incidents;
pub mod notebooks;
pub mod workflows;
pub mod dbm;
pub mod ci;
pub mod npm;
pub mod rum;
pub mod errors;
pub mod sourcemaps;
pub mod cspm;
pub mod profiling;
pub mod cost;
pub mod correlate;
pub mod changes;
pub mod pipelines;
pub mod quotas;
pub mod ai_rca;
pub mod nl_dashboard;
pub mod predictive;
pub mod templates;

use crate::sessions::SessionManager;
use crate::storage::event_store::EventStore;
use crate::storage::metric_store::MetricStore;
use crate::storage::log_store::LogStore;
use crate::models::event::Event;
use crate::models::metric::Metric;
use crate::models::log_entry::LogEntry;
use crate::api::annotations::AnnotationStore;
use crate::api::slo::SloStore;
use crate::api::incidents::IncidentStore;
use crate::api::notebooks::NotebookStore;
use crate::api::workflows::WorkflowStore;
use crate::api::dbm::DbmStore;
use crate::api::ci::CiStore;
use crate::api::npm::NpmStore;
use crate::analysis::error_tracking::ErrorTracker;
use crate::analysis::sourcemaps::SourcemapStore;
use crate::security::audit::AuditLog;
use crate::api::rum::RumStore;
use crate::api::changes::ChangeStore;
use crate::api::pipelines::PipelineStore;
use crate::api::quotas::QuotaManager;
use crate::api::cspm::CspmStore;
use crate::api::profiling::ProfilingStore;
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct ApiState {
    pub session_manager: Arc<SessionManager>,
    pub event_store: Arc<dyn EventStore>,
    pub event_bus: broadcast::Sender<Arc<Event>>,
    pub metric_store: Arc<dyn MetricStore>,
    pub metric_bus: broadcast::Sender<Arc<Metric>>,
    pub log_store: Arc<dyn LogStore>,
    pub log_bus: broadcast::Sender<Arc<LogEntry>>,
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
    pub template_store: crate::api::templates::TemplateStore,
}

pub use server::ApiServer;
pub use response::StandardResponse;
pub use crate::analysis::insights::Insight;
