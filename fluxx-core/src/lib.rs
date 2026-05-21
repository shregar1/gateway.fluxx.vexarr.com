//! Fluxx Agent: High-performance network agent.

pub mod config;
pub mod core;
pub mod install;
pub mod protocols;
pub mod plugins;
pub mod security;
pub mod graph;
pub mod analysis;
pub mod alerts;
pub mod explain;
pub mod comparison;
// HTTP API surface lives in the sibling `fluxx-api` crate. fluxx-core
// exposes a `core::RuntimeContext` (built by `AgentService::run_agent`)
// that fluxx-api consumes via `fluxx_api::build_api(ctx, addr)`. The
// `fluxx` binary in `src/main.rs` wires them together — that keeps
// fluxx-core itself free of any api dependency and breaks the cycle
// that would otherwise form (api -> core -> api).
pub mod cardinality;
pub mod replay;
pub mod sessions;
pub mod storage;
pub mod transport;
pub mod processing;
pub mod models;
pub mod cli;
pub mod utils;
pub mod cloud_forwarder;
pub mod self_telemetry;
pub mod integration_spawner;

// High-performance optimizations module
pub mod perf;

// Distributed mesh networking
pub mod mesh;

// AI-powered diagnostic and remediation
pub mod diagnostic;

// AI assistant integrations
pub mod ai;

// Synthetic monitoring (HTTP checks, API tests, SSL)
pub mod synthetic;

// Subscription / license validation
pub mod license;

// 500+ integrations framework
pub mod integrations;

// Embedded per-node dashboard. The Vite-built bundle in
// `node.dashboard.fluxx.dev/dist/` is compiled into the binary so a
// self-hosted user only runs one process.
//
// Gated behind the `bundled-dashboard` feature because the dashboard
// is a sibling repo and may not be present on every checkout. The
// HTTP layer (fluxx-api) has its own copy of the embed logic and
// degrades to a 404 stub when the feature is off, so the agent boots
// regardless.
#[cfg(feature = "bundled-dashboard")]
pub mod dashboard_assets;

// Re-exports
pub use core::Agent;
pub use core::traits::{Capturer, Processor, Transporter, Protocol};
pub use models::event::{Event, ProtocolType};
pub use config::Config;
pub use core::context::RuntimeContext;
pub use core::runtime::{AgentService, Lifecycle};
