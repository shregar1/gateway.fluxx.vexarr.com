//! Adaptive sampling — the single biggest cost lever for an
//! observability gateway.
//!
//! ## Why
//!
//! At per-ingested-event pricing (Datadog's model), the difference
//! between storing every span and storing the interesting 1% is
//! the difference between a $400k bill and a $4k one. Sampling is
//! how every cost-conscious observability deployment scales.
//!
//! ## Design
//!
//! Head-based for v1 — the sampler decides at ingest time, from
//! the entry's own fields, without seeing the rest of the trace.
//! Always cheap (no per-trace buffer), always deterministic for
//! the same input. Trades fidelity (we can't group a sampled-out
//! child span with its kept parent) for simplicity.
//!
//! Three rules, evaluated in order — first match wins; default
//! is "drop with probability `1 - default_keep_rate`":
//!
//!   1. Always-keep on **error**:
//!      - logs: level >= Warn
//!      - traces: severity >= High, OR has `error=true` /
//!        `exception.*` attr
//!      - metrics: never auto-kept; metrics are dimensional and
//!        per-point sampling is rarely what users want
//!
//!   2. Always-keep on **outlier**: latency > `slow_latency_us`
//!      threshold (traces only)
//!
//!   3. Random drop at `default_keep_rate` for everything else
//!
//! Metrics pass through unsampled by default — users should tune
//! cost via cardinality limits (separate feature, future PR).
//!
//! ## Stats
//!
//! Per-signal `kept` and `dropped` counters live on the `Sampler`
//! struct. Exported via a `stats()` method for the dashboard or
//! a future Prometheus scrape endpoint.

use crate::models::event::{Event, EventSeverity};
use crate::models::log_entry::{LogEntry, LogLevel};
use crate::models::metric::Metric;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Sampling decision for a single entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamplingDecision {
    /// Keep this entry — write to storage.
    Keep,
    /// Drop this entry — never written to storage.
    Drop,
}

#[derive(Debug, Clone)]
pub struct SamplerConfig {
    /// Master switch. When `false`, every decision is `Keep` —
    /// behavior identical to no sampler in the pipeline.
    pub enabled: bool,

    /// Fraction of "boring" entries to keep, in `[0.0, 1.0]`.
    /// `1.0` = keep everything (sampler is a noop for boring entries).
    /// `0.0` = drop everything boring (only errors + outliers survive).
    /// `0.1` = keep 10%.
    pub default_keep_rate: f64,

    /// Trace spans slower than this threshold (microseconds) are
    /// always kept regardless of `default_keep_rate`.
    pub slow_latency_us: u64,

    /// When true, errors are always kept (logs >= Warn, traces
    /// >= High severity or with exception attributes).
    pub always_keep_errors: bool,

    /// When true, metrics are sampled at `default_keep_rate`.
    /// Default false — metrics are dimensional and per-point
    /// sampling is usually the wrong tool (use cardinality limits
    /// instead).
    pub sample_metrics: bool,
}

impl Default for SamplerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_keep_rate: 0.1,
            slow_latency_us: 1_000_000, // 1 second
            always_keep_errors: true,
            sample_metrics: false,
        }
    }
}

#[derive(Debug, Default)]
struct Counters {
    logs_kept: AtomicU64,
    logs_dropped: AtomicU64,
    traces_kept: AtomicU64,
    traces_dropped: AtomicU64,
    metrics_kept: AtomicU64,
    metrics_dropped: AtomicU64,
}

#[derive(Debug)]
pub struct Sampler {
    cfg: SamplerConfig,
    counters: Arc<Counters>,
}

impl Sampler {
    pub fn new(cfg: SamplerConfig) -> Self {
        Self {
            cfg,
            counters: Arc::new(Counters::default()),
        }
    }

    pub fn config(&self) -> &SamplerConfig {
        &self.cfg
    }

    /// Decide a log entry. Always-keep on level >= Warn (when
    /// errors are kept), otherwise random drop at `default_keep_rate`.
    pub fn decide_log(&self, entry: &LogEntry) -> SamplingDecision {
        if !self.cfg.enabled {
            self.counters.logs_kept.fetch_add(1, Ordering::Relaxed);
            return SamplingDecision::Keep;
        }
        if self.cfg.always_keep_errors && log_is_error(entry.level) {
            self.counters.logs_kept.fetch_add(1, Ordering::Relaxed);
            return SamplingDecision::Keep;
        }
        if self.roll_keep() {
            self.counters.logs_kept.fetch_add(1, Ordering::Relaxed);
            SamplingDecision::Keep
        } else {
            self.counters.logs_dropped.fetch_add(1, Ordering::Relaxed);
            SamplingDecision::Drop
        }
    }

    /// Decide a trace event. Always-keep on error (severity >=
    /// High, or `error` / `exception.*` attribute), always-keep on
    /// outlier (latency > threshold), otherwise random drop.
    pub fn decide_trace(&self, event: &Event) -> SamplingDecision {
        if !self.cfg.enabled {
            self.counters.traces_kept.fetch_add(1, Ordering::Relaxed);
            return SamplingDecision::Keep;
        }
        if self.cfg.always_keep_errors && trace_is_error(event) {
            self.counters.traces_kept.fetch_add(1, Ordering::Relaxed);
            return SamplingDecision::Keep;
        }
        if event.latency_us >= self.cfg.slow_latency_us {
            self.counters.traces_kept.fetch_add(1, Ordering::Relaxed);
            return SamplingDecision::Keep;
        }
        if self.roll_keep() {
            self.counters.traces_kept.fetch_add(1, Ordering::Relaxed);
            SamplingDecision::Keep
        } else {
            self.counters.traces_dropped.fetch_add(1, Ordering::Relaxed);
            SamplingDecision::Drop
        }
    }

    /// Decide a metric. By default metrics pass through unsampled
    /// — `sample_metrics = true` opts into random drop at
    /// `default_keep_rate`.
    pub fn decide_metric(&self, _metric: &Metric) -> SamplingDecision {
        if !self.cfg.enabled || !self.cfg.sample_metrics {
            self.counters.metrics_kept.fetch_add(1, Ordering::Relaxed);
            return SamplingDecision::Keep;
        }
        if self.roll_keep() {
            self.counters.metrics_kept.fetch_add(1, Ordering::Relaxed);
            SamplingDecision::Keep
        } else {
            self.counters.metrics_dropped.fetch_add(1, Ordering::Relaxed);
            SamplingDecision::Drop
        }
    }

    fn roll_keep(&self) -> bool {
        // Special-case 1.0 / 0.0 to avoid the RNG call entirely.
        if self.cfg.default_keep_rate >= 1.0 {
            return true;
        }
        if self.cfg.default_keep_rate <= 0.0 {
            return false;
        }
        let mut rng = rand::thread_rng();
        rng.gen::<f64>() < self.cfg.default_keep_rate
    }

    /// Per-signal kept/dropped totals — for observability and a
    /// future `/v1/stats` view.
    pub fn stats(&self) -> SamplerStats {
        SamplerStats {
            logs_kept: self.counters.logs_kept.load(Ordering::Relaxed),
            logs_dropped: self.counters.logs_dropped.load(Ordering::Relaxed),
            traces_kept: self.counters.traces_kept.load(Ordering::Relaxed),
            traces_dropped: self.counters.traces_dropped.load(Ordering::Relaxed),
            metrics_kept: self.counters.metrics_kept.load(Ordering::Relaxed),
            metrics_dropped: self.counters.metrics_dropped.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SamplerStats {
    pub logs_kept: u64,
    pub logs_dropped: u64,
    pub traces_kept: u64,
    pub traces_dropped: u64,
    pub metrics_kept: u64,
    pub metrics_dropped: u64,
}

fn log_is_error(level: LogLevel) -> bool {
    matches!(level, LogLevel::Warn | LogLevel::Error | LogLevel::Fatal)
}

fn trace_is_error(event: &Event) -> bool {
    if matches!(event.severity, EventSeverity::High | EventSeverity::Critical) {
        return true;
    }
    let attrs = &event.metadata.extra;
    if attrs.get("error").map(|v| v == "true").unwrap_or(false) {
        return true;
    }
    if attrs.keys().any(|k| k.starts_with("exception.")) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::event::ProtocolType;
    use crate::models::metric::MetricType;

    #[test]
    fn disabled_keeps_everything() {
        let s = Sampler::new(SamplerConfig {
            enabled: false,
            default_keep_rate: 0.0,
            ..Default::default()
        });
        assert_eq!(
            s.decide_log(&LogEntry::new(LogLevel::Info, "x".into())),
            SamplingDecision::Keep
        );
    }

    #[test]
    fn keeps_errors_even_when_drop_all() {
        let s = Sampler::new(SamplerConfig {
            enabled: true,
            default_keep_rate: 0.0,
            always_keep_errors: true,
            ..Default::default()
        });
        assert_eq!(
            s.decide_log(&LogEntry::new(LogLevel::Error, "boom".into())),
            SamplingDecision::Keep
        );
        assert_eq!(
            s.decide_log(&LogEntry::new(LogLevel::Info, "fine".into())),
            SamplingDecision::Drop
        );
    }

    #[test]
    fn keeps_slow_traces() {
        let s = Sampler::new(SamplerConfig {
            enabled: true,
            default_keep_rate: 0.0,
            slow_latency_us: 1_000_000,
            ..Default::default()
        });
        let mut slow = Event::new("s".into(), ProtocolType::Otlp);
        slow.latency_us = 2_000_000;
        slow.severity = EventSeverity::Info;
        assert_eq!(s.decide_trace(&slow), SamplingDecision::Keep);

        let mut fast = Event::new("f".into(), ProtocolType::Otlp);
        fast.latency_us = 100;
        fast.severity = EventSeverity::Info;
        assert_eq!(s.decide_trace(&fast), SamplingDecision::Drop);
    }

    #[test]
    fn keeps_traces_with_exception_attr() {
        let s = Sampler::new(SamplerConfig {
            enabled: true,
            default_keep_rate: 0.0,
            always_keep_errors: true,
            ..Default::default()
        });
        let mut ev = Event::new("e".into(), ProtocolType::Otlp);
        ev.severity = EventSeverity::Info;
        ev.metadata.add("exception.type", "RuntimeError");
        assert_eq!(s.decide_trace(&ev), SamplingDecision::Keep);
    }

    #[test]
    fn keeps_traces_with_error_true_attr() {
        let s = Sampler::new(SamplerConfig {
            enabled: true,
            default_keep_rate: 0.0,
            always_keep_errors: true,
            ..Default::default()
        });
        let mut ev = Event::new("e".into(), ProtocolType::Otlp);
        ev.severity = EventSeverity::Info;
        ev.metadata.add("error", "true");
        assert_eq!(s.decide_trace(&ev), SamplingDecision::Keep);
    }

    #[test]
    fn metrics_pass_through_by_default() {
        let s = Sampler::new(SamplerConfig {
            enabled: true,
            default_keep_rate: 0.0,
            sample_metrics: false,
            ..Default::default()
        });
        let m = Metric::new("cpu".into(), 1.0, MetricType::Gauge);
        assert_eq!(s.decide_metric(&m), SamplingDecision::Keep);
    }

    #[test]
    fn metrics_sampled_when_opted_in() {
        let s = Sampler::new(SamplerConfig {
            enabled: true,
            default_keep_rate: 0.0,
            sample_metrics: true,
            ..Default::default()
        });
        let m = Metric::new("cpu".into(), 1.0, MetricType::Gauge);
        assert_eq!(s.decide_metric(&m), SamplingDecision::Drop);
    }

    #[test]
    fn stats_increment_correctly() {
        let s = Sampler::new(SamplerConfig {
            enabled: true,
            default_keep_rate: 0.0,
            always_keep_errors: true,
            ..Default::default()
        });
        s.decide_log(&LogEntry::new(LogLevel::Error, "e".into())); // kept
        s.decide_log(&LogEntry::new(LogLevel::Info, "i".into())); // dropped
        s.decide_log(&LogEntry::new(LogLevel::Info, "i".into())); // dropped
        let stats = s.stats();
        assert_eq!(stats.logs_kept, 1);
        assert_eq!(stats.logs_dropped, 2);
    }

    #[test]
    fn keep_rate_1_is_lossless() {
        let s = Sampler::new(SamplerConfig {
            enabled: true,
            default_keep_rate: 1.0,
            always_keep_errors: false,
            ..Default::default()
        });
        for _ in 0..1000 {
            assert_eq!(
                s.decide_log(&LogEntry::new(LogLevel::Info, "x".into())),
                SamplingDecision::Keep
            );
        }
    }

    #[test]
    fn rough_distribution_at_half_keep() {
        // Not a tight statistical test — just ensures we don't
        // accidentally hard-keep or hard-drop at 0.5.
        let s = Sampler::new(SamplerConfig {
            enabled: true,
            default_keep_rate: 0.5,
            always_keep_errors: false,
            ..Default::default()
        });
        let n = 10_000;
        let kept = (0..n)
            .filter(|_| s.decide_log(&LogEntry::new(LogLevel::Info, "x".into())) == SamplingDecision::Keep)
            .count();
        // 5σ on a binomial(n=10000, p=0.5) is ~250 — give generous
        // bounds to keep this stable on CI.
        assert!(kept > 4500 && kept < 5500, "kept = {kept}");
    }
}
