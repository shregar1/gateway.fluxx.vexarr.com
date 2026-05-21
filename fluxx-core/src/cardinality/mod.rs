//! Per-metric cardinality budget.
//!
//! ## Why
//!
//! A single metric with 5 tags each holding 100 distinct values
//! is 10 billion possible time series. Storage and query cost
//! both scale with active series count, so an unchecked
//! high-cardinality tag (`user_id`, `request_id`, `trace_id`,
//! anything per-instance) bankrupts your observability budget.
//!
//! Datadog charges extra to lift cardinality caps — making this
//! free and visible is a direct competitive advantage. A cap
//! that *defaults on* with a *visible counter* is dramatically
//! more useful than one that defaults off.
//!
//! ## Design
//!
//! Per-metric-name set of "series keys" — a series key is the
//! sorted, joined string form of a metric's `tags` HashMap. Two
//! samples with identical tag combinations share one series key.
//!
//! On insert:
//!   1. Compute series_key
//!   2. Look up `metric_name` in the tracker
//!   3. If series_key already exists in that name's set → KEEP
//!   4. Else if name's set is below `max_series_per_metric` AND
//!      global set is below `max_total_series` → record + KEEP
//!   5. Else → DROP, increment counter, log warn at most once per
//!      (metric, throttle window)
//!
//! Memory cost: bounded by the per-metric cap × per-tag-combo
//! string cost. At 10k series × ~100 B average = ~1 MB.
//!
//! ## What it doesn't do (yet)
//!
//! - **Per-tag granular caps** — Datadog lets you set
//!   "max 100 distinct values for tag `customer_id`". Future PR.
//! - **Tag dropping instead of metric dropping** — Prometheus's
//!   `metric_relabel_configs` approach. Future PR.
//! - **TTL on series keys** — once a series is in the set it
//!   counts forever even if no recent samples. Acceptable for
//!   first cut; for long-running gateways add a periodic
//!   `forget_inactive(older_than)`.

use crate::models::metric::Metric;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Cardinality decision for a single metric data point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardinalityDecision {
    /// Series is known or fits within the budget — keep.
    Allow,
    /// Series would push the metric (or global) over its cap — drop.
    Reject,
}

#[derive(Debug, Clone)]
pub struct CardinalityConfig {
    pub enabled: bool,
    /// Max distinct series allowed per metric name. 0 disables
    /// the per-metric cap (only the global cap applies).
    pub max_series_per_metric: usize,
    /// Max distinct series allowed across all metrics. 0 disables
    /// the global cap (only per-metric applies).
    pub max_total_series: usize,
}

impl Default for CardinalityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            // Conservative defaults — operator opt-in to raise.
            max_series_per_metric: 10_000,
            max_total_series: 1_000_000,
        }
    }
}

#[derive(Debug, Default)]
struct Counters {
    kept: AtomicU64,
    rejected: AtomicU64,
}

#[derive(Debug)]
pub struct CardinalityTracker {
    cfg: CardinalityConfig,
    /// per_metric[name] = set of series keys seen for that metric.
    /// Single Mutex is fine for this workload: at steady state the
    /// write rate equals the *new-series* rate which is bounded by
    /// the cap, and reads only happen on insert which is naturally
    /// serialized by storage write paths anyway.
    per_metric: Mutex<HashMap<String, HashSet<String>>>,
    total_series: AtomicU64,
    counters: Arc<Counters>,
}

impl CardinalityTracker {
    pub fn new(cfg: CardinalityConfig) -> Self {
        Self {
            cfg,
            per_metric: Mutex::new(HashMap::new()),
            total_series: AtomicU64::new(0),
            counters: Arc::new(Counters::default()),
        }
    }

    pub fn config(&self) -> &CardinalityConfig {
        &self.cfg
    }

    /// Decide whether to admit a metric data point.
    pub fn admit(&self, metric: &Metric) -> CardinalityDecision {
        if !self.cfg.enabled {
            self.counters.kept.fetch_add(1, Ordering::Relaxed);
            return CardinalityDecision::Allow;
        }
        let series_key = series_key_for(metric);
        let mut map = self.per_metric.lock().expect("cardinality mutex poisoned");

        // Already known series → allow immediately.
        if let Some(set) = map.get(&metric.name) {
            if set.contains(&series_key) {
                self.counters.kept.fetch_add(1, Ordering::Relaxed);
                return CardinalityDecision::Allow;
            }
        }

        // Global cap — count series we'd be adding.
        if self.cfg.max_total_series > 0
            && self.total_series.load(Ordering::Relaxed) as usize >= self.cfg.max_total_series
        {
            self.counters.rejected.fetch_add(1, Ordering::Relaxed);
            return CardinalityDecision::Reject;
        }

        // Per-metric cap.
        let set = map.entry(metric.name.clone()).or_default();
        if self.cfg.max_series_per_metric > 0 && set.len() >= self.cfg.max_series_per_metric {
            self.counters.rejected.fetch_add(1, Ordering::Relaxed);
            return CardinalityDecision::Reject;
        }
        set.insert(series_key);
        self.total_series.fetch_add(1, Ordering::Relaxed);
        self.counters.kept.fetch_add(1, Ordering::Relaxed);
        CardinalityDecision::Allow
    }

    pub fn stats(&self) -> CardinalityStats {
        let map = self.per_metric.lock().expect("cardinality mutex poisoned");
        let per_metric: HashMap<String, usize> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.len()))
            .collect();
        CardinalityStats {
            kept: self.counters.kept.load(Ordering::Relaxed),
            rejected: self.counters.rejected.load(Ordering::Relaxed),
            total_series: self.total_series.load(Ordering::Relaxed) as usize,
            per_metric_series: per_metric,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CardinalityStats {
    pub kept: u64,
    pub rejected: u64,
    pub total_series: usize,
    /// metric_name → distinct series count
    pub per_metric_series: HashMap<String, usize>,
}

/// Stable string form of a metric's tag combination. Tags are
/// sorted by key so `{"a":"1","b":"2"}` and `{"b":"2","a":"1"}`
/// hash to the same key.
fn series_key_for(metric: &Metric) -> String {
    if metric.tags.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(&String, &String)> = metric.tags.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let mut s = String::with_capacity(pairs.len() * 16);
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            s.push('\x1f'); // ASCII unit-separator; tolerates any key/value chars
        }
        s.push_str(k);
        s.push('=');
        s.push_str(v);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::metric::MetricType;

    fn m(name: &str, tags: &[(&str, &str)]) -> Metric {
        let mut metric = Metric::new(name.to_string(), 1.0, MetricType::Gauge);
        for (k, v) in tags {
            metric.tags.insert(k.to_string(), v.to_string());
        }
        metric
    }

    #[test]
    fn disabled_allows_everything() {
        let t = CardinalityTracker::new(CardinalityConfig {
            enabled: false,
            max_series_per_metric: 1,
            max_total_series: 1,
        });
        for i in 0..10 {
            let metric = m("x", &[("k", &i.to_string())]);
            assert_eq!(t.admit(&metric), CardinalityDecision::Allow);
        }
    }

    #[test]
    fn rejects_past_per_metric_cap() {
        let t = CardinalityTracker::new(CardinalityConfig {
            enabled: true,
            max_series_per_metric: 3,
            max_total_series: 0,
        });
        for i in 0..3 {
            assert_eq!(
                t.admit(&m("cpu", &[("host", &i.to_string())])),
                CardinalityDecision::Allow
            );
        }
        // 4th distinct series exceeds the cap
        assert_eq!(
            t.admit(&m("cpu", &[("host", "3")])),
            CardinalityDecision::Reject
        );
        // Reusing an existing series is fine
        assert_eq!(
            t.admit(&m("cpu", &[("host", "0")])),
            CardinalityDecision::Allow
        );
    }

    #[test]
    fn caps_are_per_metric_not_global() {
        let t = CardinalityTracker::new(CardinalityConfig {
            enabled: true,
            max_series_per_metric: 2,
            max_total_series: 0,
        });
        assert_eq!(t.admit(&m("cpu", &[("host", "a")])), CardinalityDecision::Allow);
        assert_eq!(t.admit(&m("cpu", &[("host", "b")])), CardinalityDecision::Allow);
        assert_eq!(t.admit(&m("cpu", &[("host", "c")])), CardinalityDecision::Reject);
        // Different metric — own budget
        assert_eq!(t.admit(&m("mem", &[("host", "a")])), CardinalityDecision::Allow);
        assert_eq!(t.admit(&m("mem", &[("host", "b")])), CardinalityDecision::Allow);
    }

    #[test]
    fn global_cap_independently_enforced() {
        let t = CardinalityTracker::new(CardinalityConfig {
            enabled: true,
            max_series_per_metric: 0,
            max_total_series: 3,
        });
        assert_eq!(t.admit(&m("a", &[("k", "1")])), CardinalityDecision::Allow);
        assert_eq!(t.admit(&m("b", &[("k", "2")])), CardinalityDecision::Allow);
        assert_eq!(t.admit(&m("c", &[("k", "3")])), CardinalityDecision::Allow);
        // 4th series globally — reject regardless of metric
        assert_eq!(t.admit(&m("d", &[("k", "4")])), CardinalityDecision::Reject);
    }

    #[test]
    fn tag_order_does_not_affect_series_key() {
        let t = CardinalityTracker::new(CardinalityConfig {
            enabled: true,
            max_series_per_metric: 1,
            max_total_series: 0,
        });
        // Both create the same series — second call should hit the
        // dedup path, not rejection.
        assert_eq!(
            t.admit(&m("req", &[("a", "1"), ("b", "2")])),
            CardinalityDecision::Allow
        );
        assert_eq!(
            t.admit(&m("req", &[("b", "2"), ("a", "1")])),
            CardinalityDecision::Allow
        );
    }

    #[test]
    fn empty_tags_share_one_series() {
        let t = CardinalityTracker::new(CardinalityConfig {
            enabled: true,
            max_series_per_metric: 1,
            max_total_series: 0,
        });
        assert_eq!(t.admit(&m("untagged", &[])), CardinalityDecision::Allow);
        assert_eq!(t.admit(&m("untagged", &[])), CardinalityDecision::Allow);
    }

    #[test]
    fn stats_track_kept_and_rejected() {
        let t = CardinalityTracker::new(CardinalityConfig {
            enabled: true,
            max_series_per_metric: 2,
            max_total_series: 0,
        });
        t.admit(&m("cpu", &[("h", "1")])); // keep (new)
        t.admit(&m("cpu", &[("h", "2")])); // keep (new)
        t.admit(&m("cpu", &[("h", "3")])); // reject (cap)
        t.admit(&m("cpu", &[("h", "1")])); // keep (dedup)
        let s = t.stats();
        assert_eq!(s.kept, 3);
        assert_eq!(s.rejected, 1);
        assert_eq!(s.total_series, 2);
        assert_eq!(s.per_metric_series.get("cpu").copied(), Some(2));
    }

    #[test]
    fn zero_per_metric_disables_per_metric_check() {
        let t = CardinalityTracker::new(CardinalityConfig {
            enabled: true,
            max_series_per_metric: 0, // disabled
            max_total_series: 5,
        });
        for i in 0..5 {
            assert_eq!(
                t.admit(&m("x", &[("k", &i.to_string())])),
                CardinalityDecision::Allow
            );
        }
        // 6th hits global cap
        assert_eq!(
            t.admit(&m("x", &[("k", "5")])),
            CardinalityDecision::Reject
        );
    }
}
