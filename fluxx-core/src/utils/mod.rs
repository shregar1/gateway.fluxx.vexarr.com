use tracing_subscriber::{fmt, EnvFilter};

/// Initialise the global tracing subscriber.
///
/// Default level is `info` (operator-friendly for self-hosted deploys
/// — you can see "API: Binding…", "ClickHouse migrations applied",
/// etc. without re-running with extra flags). `RUST_LOG` still wins:
/// set `RUST_LOG=warn` to quiet it down or `RUST_LOG=debug` to dig in.
///
/// We do **not** set `with_max_level` on the formatter — when a
/// max-level is configured alongside an env filter, the max-level
/// clamps the filter regardless of what the user asked for, which
/// silently swallows every `info!` and `warn!` call in the codebase.
/// That's a footgun we don't want.
pub fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();
}
