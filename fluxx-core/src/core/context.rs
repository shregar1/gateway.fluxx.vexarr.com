//! `RuntimeContext` — the hand-off between `fluxx-core` and `fluxx-api`.
//!
//! The agent runtime (storage, session manager, event/metric/log
//! bus, error tracker, sourcemap store, audit log) is built inside
//! `AgentService::run_agent` and lives entirely in `fluxx-core`. The
//! HTTP API surface — Axum routes, the API-specific in-memory stores
//! (SLOs, incidents, notebooks, …) and `ApiState` — lives in the
//! sibling `fluxx-api` crate.
//!
//! `RuntimeContext` is the narrow contract between the two: every
//! field is a primitive already constructed by the core runtime,
//! sized to be cheaply clonable. The binary in `src/main.rs` is the
//! only place that knows about both crates; it calls
//! `AgentService::build_runtime` to get the `(AgentService,
//! RuntimeContext)` pair, then hands the context to
//! `fluxx_api::build_api(ctx, addr)`.
//!
//! Keep this struct minimal. Anything api-only (the 20+ store types
//! used only by api handlers) is constructed inside `build_api` and
//! must NOT leak back into the core graph.

use std::sync::Arc;
use tokio::sync::broadcast;

use crate::analysis::error_tracking::ErrorTracker;
use crate::analysis::sourcemaps::SourcemapStore;
use crate::models::event::Event;
use crate::models::log_entry::LogEntry;
use crate::models::metric::Metric;
use crate::security::audit::AuditLog;
use crate::sessions::SessionManager;
use crate::storage::event_store::EventStore;
use crate::storage::log_store::LogStore;
use crate::storage::metric_store::MetricStore;

/// Primitives the runtime hands to the HTTP API layer.
///
/// `Clone` is cheap — every field is either an `Arc<…>`, a
/// `broadcast::Sender` (cloneable), or an `Arc<dyn …>` trait object.
#[derive(Clone)]
pub struct RuntimeContext {
    pub session_manager: Arc<SessionManager>,
    pub event_store: Arc<dyn EventStore>,
    pub event_bus: broadcast::Sender<Arc<Event>>,
    pub metric_store: Arc<dyn MetricStore>,
    pub metric_bus: broadcast::Sender<Arc<Metric>>,
    pub log_store: Arc<dyn LogStore>,
    pub log_bus: broadcast::Sender<Arc<LogEntry>>,
    pub error_tracker: Arc<ErrorTracker>,
    pub sourcemap_store: Arc<SourcemapStore>,
    pub audit_log: Arc<AuditLog>,
}
