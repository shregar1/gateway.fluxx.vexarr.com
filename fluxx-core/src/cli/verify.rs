//! `fluxx verify <service-name>` — last-mile wiring check.
//!
//! Asks the local agent's HTTP API whether it's seen any OTLP spans
//! with the given `service.name` in the recent past. Designed to be
//! the canonical CI / deploy gate after a producer adds the OTel
//! SDK boot block:
//!
//!     # In a CI step or a post-deploy hook:
//!     fluxx verify taskx-api
//!     # Exit 0 = green (spans flowing), exit 1 = red.
//!
//! The check is intentionally simple: "did at least one span with
//! `service.name=<X>` arrive in the last 5 minutes?" Anything more
//! sophisticated would force us to maintain a spec of "what counts
//! as wired" — and `>= 1 fresh span` is unambiguous.
//!
//! ## Design
//!
//! - `VerifySummary::analyze` is pure and unit-tested. It takes a
//!   parsed slice of agent events plus the current timestamp, and
//!   returns the matched / fresh / newest-age / unique-trace counts.
//! - `run()` is the thin HTTP wrapper that fetches the events and
//!   calls `analyze`. No business logic; tested manually.
//! - Exits with code 0 on green, 1 on red. JSON output (`--json`)
//!   is the machine-readable contract for CI pipelines.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Args;
use serde::{Deserialize, Serialize};

const DEFAULT_AGENT_URL: &str = "http://127.0.0.1:7007";

/// Arguments for `fluxx verify`. Mirrored 1:1 in the CLI parser.
#[derive(Args, Debug)]
pub struct VerifyArgs {
    /// The `service.name` resource attribute to look for in OTLP spans.
    /// Match the value of `OTEL_SERVICE_NAME` set in the producer's env.
    pub service_name: String,

    /// Base URL of the local Fluxx agent's HTTP API. Default is the
    /// loopback address the systemd unit binds to. Override when the
    /// agent runs in a sidecar / different port.
    #[arg(long, default_value = DEFAULT_AGENT_URL)]
    pub agent_url: String,

    /// A span counts as "fresh" if it's newer than now − this many
    /// seconds. The default (300s = 5 min) is generous enough to cover
    /// a typical CI deploy window without being so wide that stale
    /// pre-deploy spans get counted as green.
    #[arg(long, default_value_t = 300)]
    pub freshness_secs: u64,

    /// Max events to inspect from the agent's `/v1/events` response.
    /// 100 is enough to verify wiring; anything larger gives the
    /// agent a bigger query for no extra signal.
    #[arg(long, default_value_t = 100)]
    pub limit: usize,

    /// Emit a JSON summary on stdout instead of the coloured human
    /// format. Use this in CI: `if ! fluxx verify foo --json > out;
    /// then jq ... < out; fi`.
    #[arg(long)]
    pub json: bool,
}

/// Subset of the agent's Event shape we need for verification. Extra
/// fields are ignored by serde's default behaviour, so this stays
/// forward-compatible as the agent grows new attributes.
#[derive(Debug, Deserialize)]
pub(crate) struct AgentEvent {
    service_id: Option<String>,
    timestamp_us: u64,
    #[serde(default)]
    metadata: EventMeta,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct EventMeta {
    trace_id: Option<String>,
    // span_name is captured by serde-flatten in the agent's Metadata
    // struct, but we don't actually need it for the green/red signal.
    // Adding it back is one line if we ever want to summarise span
    // names in `--json` mode.
}

/// What the agent returns for `GET /v1/events`.
#[derive(Debug, Deserialize)]
struct AgentEnvelope {
    #[serde(default)]
    data: Vec<AgentEvent>,
}

/// The summary `fluxx verify` exits on. Constructed by
/// `VerifySummary::analyze` and printed as either text or JSON.
#[derive(Debug, PartialEq, Serialize)]
pub struct VerifySummary {
    /// Total events whose `service_id` matched the queried name.
    pub matched: usize,
    /// Events that matched AND were newer than the freshness cutoff.
    pub fresh: usize,
    /// Seconds since the most-recent matching event. None means no
    /// match at all.
    pub newest_age_secs: Option<f64>,
    /// Distinct `trace_id` values across matching events. A high
    /// number means real production traffic, not just a single
    /// synthetic probe. (Information only — the wiring check does
    /// not require multiple traces.)
    pub unique_traces: usize,
}

impl VerifySummary {
    /// Pure analyzer. Decoupled from HTTP and time so it can be
    /// exhaustively unit-tested.
    pub(crate) fn analyze(
        events: &[AgentEvent],
        service_name: &str,
        now_us: u64,
        freshness_secs: u64,
    ) -> Self {
        let cutoff_us = now_us.saturating_sub(freshness_secs.saturating_mul(1_000_000));

        let mut matched = 0usize;
        let mut fresh = 0usize;
        let mut newest_us: Option<u64> = None;
        let mut traces: HashSet<&str> = HashSet::new();

        for e in events {
            if e.service_id.as_deref() != Some(service_name) {
                continue;
            }
            matched += 1;
            if e.timestamp_us >= cutoff_us {
                fresh += 1;
            }
            match newest_us {
                Some(prev) if e.timestamp_us > prev => newest_us = Some(e.timestamp_us),
                None => newest_us = Some(e.timestamp_us),
                _ => {}
            }
            if let Some(tid) = e.metadata.trace_id.as_deref() {
                if !tid.is_empty() {
                    traces.insert(tid);
                }
            }
        }

        Self {
            matched,
            fresh,
            newest_age_secs: newest_us.map(|n| (now_us.saturating_sub(n) as f64) / 1_000_000.0),
            unique_traces: traces.len(),
        }
    }

    /// `fluxx verify` exit-code contract: green iff at least one
    /// fresh span was seen.
    pub fn is_green(&self) -> bool {
        self.fresh > 0
    }
}

// ─── HTTP wrapper (thin, intentionally untested in unit suite) ─────────────

/// Fetch `/v1/events?service_id=...&limit=...` from the local agent
/// and deserialize into the analyzer-friendly shape. Anything wrong
/// at this layer (agent down, malformed JSON) bubbles up as anyhow
/// errors with operator-readable context.
async fn fetch_events(
    agent_url: &str,
    service_name: &str,
    limit: usize,
) -> anyhow::Result<Vec<AgentEvent>> {
    let url = format!(
        "{}/v1/events?service_id={}&protocol=otlp&limit={}",
        agent_url.trim_end_matches('/'),
        urlencoding::encode(service_name),
        limit
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let resp = client.get(&url).send().await.map_err(|e| {
        anyhow::anyhow!(
            "could not reach the Fluxx agent at {} ({}). Is the agent running? Try: systemctl status fluxx-agent",
            agent_url,
            e
        )
    })?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("agent returned HTTP {}: {}", status, body);
    }
    let env: AgentEnvelope = resp.json().await.map_err(|e| {
        anyhow::anyhow!(
            "agent returned non-JSON or unexpected JSON for /v1/events: {}",
            e
        )
    })?;
    Ok(env.data)
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Entry point called from `Commands::Verify` in `cli::mod`.
pub async fn run(args: VerifyArgs) -> anyhow::Result<()> {
    let events = fetch_events(&args.agent_url, &args.service_name, args.limit).await?;
    let summary =
        VerifySummary::analyze(&events, &args.service_name, now_us(), args.freshness_secs);

    if args.json {
        println!("{}", serde_json::to_string(&summary)?);
    } else {
        print_human(&args.service_name, &summary, args.freshness_secs);
    }

    if summary.is_green() {
        Ok(())
    } else {
        // Non-zero exit so CI catches this as a failure.
        std::process::exit(1);
    }
}

fn print_human(service: &str, s: &VerifySummary, freshness_secs: u64) {
    let green = "\x1b[32m";
    let red = "\x1b[31m";
    let dim = "\x1b[2m";
    let off = "\x1b[0m";

    println!("Fluxx verify  service.name={}", service);
    if s.is_green() {
        let age = s.newest_age_secs.unwrap_or(0.0);
        println!(
            "  {}✓{} {} fresh span(s) in the last {}s (newest {:.1}s ago)",
            green, off, s.fresh, freshness_secs, age
        );
        println!(
            "  {}·{} {} unique trace(s), {} total matched events",
            dim, off, s.unique_traces, s.matched
        );
    } else if s.matched > 0 {
        // We DO have spans for this service, but none in the freshness
        // window — usually means "wiring worked once, but the service
        // has stopped emitting recently." Useful distinction from
        // "never seen this name at all."
        let age = s.newest_age_secs.unwrap_or(0.0);
        println!(
            "  {}✗{} no fresh spans (newest is {:.1}s old, freshness window = {}s)",
            red, off, age, freshness_secs
        );
        println!("  {}·{} hint: is the producer still running and exporting?", dim, off);
    } else {
        println!("  {}✗{} no spans seen for service.name={}", red, off, service);
        println!(
            "  {}·{} hint: check OTEL_SERVICE_NAME and FLUXX_OTEL_ENDPOINT in the producer's env",
            dim, off
        );
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-rolled `AgentEvent` factory so test bodies stay readable.
    fn ev(service: &str, ts_us: u64, trace: Option<&str>) -> AgentEvent {
        AgentEvent {
            service_id: Some(service.to_string()),
            timestamp_us: ts_us,
            metadata: EventMeta {
                trace_id: trace.map(str::to_string),
            },
        }
    }

    fn ev_no_service(ts_us: u64) -> AgentEvent {
        AgentEvent {
            service_id: None,
            timestamp_us: ts_us,
            metadata: EventMeta::default(),
        }
    }

    const NOW: u64 = 1_000_000_000_000; // arbitrary "now" in microseconds

    #[test]
    fn analyze_empty_events_returns_zero_matched() {
        let s = VerifySummary::analyze(&[], "taskx-api", NOW, 300);
        assert_eq!(s.matched, 0);
        assert_eq!(s.fresh, 0);
        assert_eq!(s.newest_age_secs, None);
        assert_eq!(s.unique_traces, 0);
        assert!(!s.is_green());
    }

    #[test]
    fn analyze_filters_by_service_name() {
        // Three events: two for taskx-api, one for fluxx-api. We must
        // only count the two — verify is per-service, not aggregate.
        let events = vec![
            ev("taskx-api", NOW - 1_000_000, Some("t1")),
            ev("taskx-api", NOW - 2_000_000, Some("t2")),
            ev("fluxx-api", NOW - 500_000, Some("f1")),
        ];
        let s = VerifySummary::analyze(&events, "taskx-api", NOW, 300);
        assert_eq!(s.matched, 2);
        assert_eq!(s.fresh, 2);
        assert_eq!(s.unique_traces, 2);
    }

    #[test]
    fn analyze_classifies_fresh_vs_stale() {
        // Two events: one inside the 300s window, one outside.
        let events = vec![
            ev("taskx-api", NOW - 100_000_000, Some("t1")),     // 100s ago, fresh
            ev("taskx-api", NOW - 400_000_000, Some("t2")),     // 400s ago, stale
        ];
        let s = VerifySummary::analyze(&events, "taskx-api", NOW, 300);
        assert_eq!(s.matched, 2);
        assert_eq!(s.fresh, 1, "expected only one fresh span");
        // is_green is true because there IS at least one fresh span.
        assert!(s.is_green());
    }

    #[test]
    fn analyze_is_not_green_when_only_stale_spans_match() {
        // The "service used to be wired but isn't emitting now" case.
        // matched > 0 but fresh == 0 — print a different message in
        // the human path and still exit 1.
        let events = vec![ev("taskx-api", NOW - 1_000_000_000, Some("t1"))];
        let s = VerifySummary::analyze(&events, "taskx-api", NOW, 300);
        assert_eq!(s.matched, 1);
        assert_eq!(s.fresh, 0);
        assert!(!s.is_green());
        assert!(s.newest_age_secs.unwrap() > 100.0);
    }

    #[test]
    fn analyze_picks_newest_timestamp_for_age() {
        // Sequence of three events; newest_age_secs must reflect the
        // most-recent one regardless of input ordering.
        let events = vec![
            ev("taskx-api", NOW - 10_000_000, Some("t1")),
            ev("taskx-api", NOW - 1_000_000, Some("t2")), // newest
            ev("taskx-api", NOW - 5_000_000, Some("t3")),
        ];
        let s = VerifySummary::analyze(&events, "taskx-api", NOW, 300);
        // Newest is 1_000_000us = 1s ago.
        let age = s.newest_age_secs.expect("expected an age");
        assert!((age - 1.0).abs() < 0.01, "expected ~1.0s, got {}", age);
    }

    #[test]
    fn analyze_dedupes_trace_ids() {
        // Five matching events but only two distinct trace_ids — unique_traces=2.
        // Real OTLP traffic has many spans per trace, so this is the
        // common case; a regression that returned 5 would mislead
        // operators about the actual request volume.
        let events = vec![
            ev("taskx-api", NOW - 1_000_000, Some("trace-A")),
            ev("taskx-api", NOW - 2_000_000, Some("trace-A")),
            ev("taskx-api", NOW - 3_000_000, Some("trace-A")),
            ev("taskx-api", NOW - 4_000_000, Some("trace-B")),
            ev("taskx-api", NOW - 5_000_000, Some("trace-B")),
        ];
        let s = VerifySummary::analyze(&events, "taskx-api", NOW, 300);
        assert_eq!(s.matched, 5);
        assert_eq!(s.unique_traces, 2);
    }

    #[test]
    fn analyze_ignores_missing_trace_id_in_unique_count() {
        // Some agent-side events legitimately have no trace_id (system
        // probes, internal lifecycle). They count as matched but must
        // not bump unique_traces.
        let events = vec![
            ev("taskx-api", NOW - 1_000_000, None),
            ev("taskx-api", NOW - 2_000_000, Some("trace-A")),
            ev("taskx-api", NOW - 3_000_000, Some("")), // empty-string trace_id
        ];
        let s = VerifySummary::analyze(&events, "taskx-api", NOW, 300);
        assert_eq!(s.matched, 3);
        assert_eq!(s.unique_traces, 1, "only trace-A counts");
    }

    #[test]
    fn analyze_ignores_events_with_no_service_id() {
        // The agent stores some events with service_id=None (raw system
        // metrics, etc.). They must NOT count toward any service's
        // verification, no matter what name the caller passed.
        let events = vec![
            ev_no_service(NOW - 1_000_000),
            ev_no_service(NOW - 2_000_000),
        ];
        let s = VerifySummary::analyze(&events, "taskx-api", NOW, 300);
        assert_eq!(s.matched, 0);
    }

    #[test]
    fn analyze_handles_event_in_the_future_gracefully() {
        // Clock skew between producer and verifier can put timestamps
        // in the future. `now_us.saturating_sub(...)` keeps age >= 0
        // and the event still counts as fresh (since fresh is defined
        // as "ts >= cutoff", any future timestamp is fresh).
        let events = vec![ev("taskx-api", NOW + 5_000_000, Some("t1"))];
        let s = VerifySummary::analyze(&events, "taskx-api", NOW, 300);
        assert_eq!(s.matched, 1);
        assert_eq!(s.fresh, 1);
        // newest_age_secs is computed via saturating_sub; for a
        // future timestamp the saturating-result is 0, so age == 0.
        assert_eq!(s.newest_age_secs, Some(0.0));
    }

    #[test]
    fn analyze_does_not_overflow_with_huge_freshness_window() {
        // u64 freshness_secs * 1_000_000 microseconds could overflow
        // for absurd inputs (`--freshness-secs $((1<<40))`). The
        // saturating_mul + saturating_sub keep the cutoff bounded at
        // 0, which means everything is fresh and we don't panic.
        let events = vec![ev("taskx-api", NOW - 1_000_000, None)];
        let s = VerifySummary::analyze(&events, "taskx-api", NOW, u64::MAX);
        assert_eq!(s.fresh, 1);
    }

    #[test]
    fn is_green_requires_at_least_one_fresh() {
        let stale = VerifySummary {
            matched: 100,
            fresh: 0,
            newest_age_secs: Some(900.0),
            unique_traces: 50,
        };
        assert!(!stale.is_green());

        let one_fresh = VerifySummary {
            matched: 1,
            fresh: 1,
            newest_age_secs: Some(2.3),
            unique_traces: 1,
        };
        assert!(one_fresh.is_green());
    }
}
