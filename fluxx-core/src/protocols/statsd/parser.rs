use crate::models::metric::{Metric, MetricType};
use std::collections::HashMap;

/// Parse a single StatsD/DogStatsD line into a Metric.
///
/// Format: `metric_name:value|type|@sample_rate|#tag1:val1,tag2:val2`
///
/// Type codes:
/// - `c`  = Counter
/// - `g`  = Gauge
/// - `h`  = Histogram (DogStatsD)
/// - `ms` = Timing
/// - `s`  = Set
/// - `d`  = Distribution (DogStatsD)
pub fn parse_statsd_line(line: &str) -> Result<Metric, String> {
    let line = line.trim();
    if line.is_empty() {
        return Err("empty line".to_string());
    }

    // Split name:rest
    let colon_pos = line.find(':').ok_or("missing ':' separator")?;
    let name = &line[..colon_pos];
    let rest = &line[colon_pos + 1..];

    if name.is_empty() {
        return Err("empty metric name".to_string());
    }

    // Split remaining parts by '|'
    let parts: Vec<&str> = rest.split('|').collect();
    if parts.len() < 2 {
        return Err("missing type field".to_string());
    }

    // Parse type first to determine how to handle value
    let metric_type = match parts[1] {
        "c" => MetricType::Counter,
        "g" => MetricType::Gauge,
        "h" => MetricType::Histogram,
        "ms" => MetricType::Timing,
        "s" => MetricType::Set,
        "d" => MetricType::Distribution,
        t => return Err(format!("unknown type: '{}'", t)),
    };

    // Parse value — sets use string values (hashed to f64), others are numeric
    let value: f64 = if metric_type == MetricType::Set {
        // For sets, hash the string value to a f64 for storage; the actual unique
        // counting happens in the aggregator using the raw string
        parts[0].parse().unwrap_or_else(|_| {
            // Use a simple hash for non-numeric set values
            let mut hash: u64 = 5381;
            for b in parts[0].bytes() {
                hash = hash.wrapping_mul(33).wrapping_add(b as u64);
            }
            hash as f64
        })
    } else {
        parts[0].parse().map_err(|_| format!("invalid value: '{}'", parts[0]))?
    };

    // Parse optional sample rate and tags
    let mut sample_rate = 1.0;
    let mut tags = HashMap::new();

    for part in &parts[2..] {
        if let Some(rate_str) = part.strip_prefix('@') {
            sample_rate = rate_str.parse().unwrap_or(1.0);
        } else if let Some(tag_str) = part.strip_prefix('#') {
            for tag in tag_str.split(',') {
                let tag = tag.trim();
                if tag.is_empty() {
                    continue;
                }
                if let Some((k, v)) = tag.split_once(':') {
                    tags.insert(k.to_string(), v.to_string());
                } else {
                    tags.insert(tag.to_string(), "true".to_string());
                }
            }
        }
    }

    let metric = Metric::new(name.to_string(), value, metric_type)
        .with_tags(tags)
        .with_sample_rate(sample_rate);

    Ok(metric)
}

/// Parse a StatsD datagram (may contain multiple newline-separated metrics).
pub fn parse_statsd_datagram(data: &str) -> Vec<Result<Metric, String>> {
    data.lines()
        .filter(|line| !line.trim().is_empty())
        .map(parse_statsd_line)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_counter() {
        let m = parse_statsd_line("page.views:1|c").unwrap();
        assert_eq!(m.name, "page.views");
        assert_eq!(m.value, 1.0);
        assert_eq!(m.metric_type, MetricType::Counter);
        assert_eq!(m.sample_rate, 1.0);
    }

    #[test]
    fn test_parse_gauge() {
        let m = parse_statsd_line("cpu.usage:75.5|g").unwrap();
        assert_eq!(m.name, "cpu.usage");
        assert_eq!(m.value, 75.5);
        assert_eq!(m.metric_type, MetricType::Gauge);
    }

    #[test]
    fn test_parse_histogram() {
        let m = parse_statsd_line("request.latency:320|h").unwrap();
        assert_eq!(m.metric_type, MetricType::Histogram);
    }

    #[test]
    fn test_parse_timing() {
        let m = parse_statsd_line("db.query:45|ms").unwrap();
        assert_eq!(m.metric_type, MetricType::Timing);
        assert_eq!(m.value, 45.0);
    }

    #[test]
    fn test_parse_set() {
        let m = parse_statsd_line("users.unique:user123|s").unwrap();
        assert_eq!(m.metric_type, MetricType::Set);
    }

    #[test]
    fn test_parse_with_sample_rate() {
        let m = parse_statsd_line("page.views:1|c|@0.5").unwrap();
        assert_eq!(m.sample_rate, 0.5);
    }

    #[test]
    fn test_parse_with_tags() {
        let m = parse_statsd_line("page.views:1|c|#env:prod,region:us-east").unwrap();
        assert_eq!(m.tags.get("env"), Some(&"prod".to_string()));
        assert_eq!(m.tags.get("region"), Some(&"us-east".to_string()));
    }

    #[test]
    fn test_parse_with_sample_rate_and_tags() {
        let m = parse_statsd_line("page.views:1|c|@0.5|#env:prod").unwrap();
        assert_eq!(m.sample_rate, 0.5);
        assert_eq!(m.tags.get("env"), Some(&"prod".to_string()));
    }

    #[test]
    fn test_parse_distribution() {
        let m = parse_statsd_line("request.size:512|d").unwrap();
        assert_eq!(m.metric_type, MetricType::Distribution);
    }

    #[test]
    fn test_parse_datagram_multiple_lines() {
        let data = "cpu.usage:75|g\nreq.count:1|c\ndb.latency:12|ms\n";
        let results = parse_statsd_datagram(data);
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn test_parse_invalid_missing_type() {
        assert!(parse_statsd_line("foo:1").is_err());
    }

    #[test]
    fn test_parse_invalid_missing_value() {
        assert!(parse_statsd_line("foo:|c").is_err());
    }

    #[test]
    fn test_parse_invalid_unknown_type() {
        assert!(parse_statsd_line("foo:1|x").is_err());
    }

    #[test]
    fn test_parse_empty_line() {
        assert!(parse_statsd_line("").is_err());
    }

    #[test]
    fn test_parse_tag_without_value() {
        let m = parse_statsd_line("foo:1|c|#standalone").unwrap();
        assert_eq!(m.tags.get("standalone"), Some(&"true".to_string()));
    }

    #[test]
    fn test_parse_negative_value() {
        let m = parse_statsd_line("gauge.temp:-5.2|g").unwrap();
        assert_eq!(m.value, -5.2);
    }

    #[test]
    fn test_parse_missing_colon() {
        assert!(parse_statsd_line("noColonHere").is_err());
    }

    #[test]
    fn test_parse_empty_name() {
        assert!(parse_statsd_line(":1|c").is_err());
    }

    #[test]
    fn test_parse_invalid_value_non_numeric() {
        assert!(parse_statsd_line("foo:notanumber|c").is_err());
    }

    #[test]
    fn test_parse_set_with_string_value() {
        let m = parse_statsd_line("unique:some-user-id|s").unwrap();
        assert_eq!(m.metric_type, MetricType::Set);
        // value is the hash; should be non-zero
        assert!(m.value != 0.0);
    }

    #[test]
    fn test_parse_datagram_skips_blank_lines() {
        let data = "cpu:1|g\n\n\nmem:2|g\n";
        let results = parse_statsd_datagram(data);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_parse_datagram_mix_of_valid_invalid() {
        let data = "good:1|c\nbad\nalsogood:2|g";
        let results = parse_statsd_datagram(data);
        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok());
        assert!(results[1].is_err());
        assert!(results[2].is_ok());
    }

    #[test]
    fn test_parse_zero_value() {
        let m = parse_statsd_line("zero:0|c").unwrap();
        assert_eq!(m.value, 0.0);
    }

    #[test]
    fn test_parse_large_value() {
        let m = parse_statsd_line("big:1000000000|c").unwrap();
        assert_eq!(m.value, 1_000_000_000.0);
    }

    #[test]
    fn test_parse_multiple_tags_no_values() {
        let m = parse_statsd_line("foo:1|c|#a,b,c").unwrap();
        assert_eq!(m.tags.len(), 3);
        assert_eq!(m.tags.get("a"), Some(&"true".to_string()));
    }

    #[test]
    fn test_parse_invalid_sample_rate_defaults_to_one() {
        let m = parse_statsd_line("foo:1|c|@abc").unwrap();
        assert_eq!(m.sample_rate, 1.0);
    }

    #[test]
    fn test_parse_datagram_empty_string() {
        let results = parse_statsd_datagram("");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_parse_trims_whitespace() {
        let m = parse_statsd_line("  foo:1|c  ").unwrap();
        assert_eq!(m.name, "foo");
    }
}
