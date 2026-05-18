//! `fluxx` binary entry point.
//!
//! This is the only crate that takes a dependency on BOTH
//! `fluxx-core` (library) and `fluxx-api`. The split was done to
//! break a previous cycle (`fluxx-api -> fluxx-core -> fluxx-api`).
//! The library half of `fluxx-core` knows nothing about HTTP routes;
//! the api crate knows about core primitives but never calls back
//! into the cli or runtime. This binary is where the two meet.
//!
//! For `fluxx start` we:
//!   1. Build the agent runtime via `AgentService::prepare_runtime`.
//!      That returns a `RuntimeContext` containing the session
//!      manager, event/metric/log stores + buses, error tracker,
//!      sourcemap store, and audit log.
//!   2. Hand the context to `fluxx_api::build_api(ctx, &addr)` to
//!      construct the `ApiServer` (it builds its own api-only
//!      in-memory stores internally).
//!   3. Run the agent loop and the api server concurrently with
//!      `tokio::try_join!` so a failure in either tears the process
//!      down.
//!
//! Other subcommands (inspect, config, deploy, mesh, mcp, openclaw,
//! verify, init, register, errors, release) don't need the api;
//! `fluxx_core::cli::run` handles them itself.

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    fluxx_core::cli::run(start_agent_with_api).await
}

/// `fluxx start` handler. Consumes the `AgentService` constructed
/// inside `cli::run`, prepares the runtime context, builds the api
/// server from it, and joins both futures.
async fn start_agent_with_api(
    mut service: fluxx_core::AgentService,
    cfg: fluxx_core::Config,
) -> Result<()> {
    // 1. Prepare the agent runtime. This wires capturers, processors,
    //    transports, and spawns all background services (OTLP,
    //    StatsD, alerts, anomaly detector, cloud forwarder, etc.).
    //    It does NOT start the agent's blocking event loop yet.
    let ctx = service.prepare_runtime(cfg).await?;

    // 2. The api address is captured during `prepare_runtime` so we
    //    bind to the same port the runtime printed to stdout.
    //    Falls back to 0.0.0.0:7007 if the service didn't record one
    //    (only happens if prepare_runtime errored, in which case the
    //    `?` above already short-circuited).
    let addr = service
        .api_addr()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "0.0.0.0:7007".to_string());

    // 3. Build the api server from the runtime context.
    let api_server = fluxx_api::build_api(ctx, &addr);

    // 4. Join the agent loop and api server. Either future completing
    //    (cleanly or with an error) propagates and tears the process
    //    down — that matches what the old in-line tokio::spawn +
    //    tokio::select! flow did, but without the cycle.
    tokio::try_join!(service.run_agent_loop(), api_server.start())?;

    Ok(())
}
