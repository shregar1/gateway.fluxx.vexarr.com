//! `fluxx register` — manually announce a producer service to Fluxx.
//!
//! Pairs with `fluxx init` (declares intent + writes fluxx.toml) and
//! `fluxx verify` (proves the wiring works). The middle step is
//! `register`: it makes the service appear in the dashboard
//! `/service-map` *immediately*, before any real production span flows.
//! Useful when:
//!
//!   • You're setting up a new service and want it visible to ops
//!     before the deploy actually rolls out — pre-warm the dashboard
//!     so the on-call sees the expected node, not "where did this
//!     come from?" when traffic starts.
//!   • The service is low-traffic and you want a baseline ping so it
//!     never disappears from the map during quiet hours.
//!   • A producer team wants to attach metadata (tier, owner, support
//!     contact) as resource attributes without waiting for the SDK
//!     wiring to land.
//!
//! ## Design
//!
//! Rather than introducing a new "service registry" endpoint or a new
//! DB table, `fluxx register` does exactly one thing: it constructs a
//! valid OTLP trace with a single synthetic span and sends it to the
//! local agent. The span:
//!
//!   • Has `service.name = <name>` on its Resource (so it appears in
//!     `/service-map` like any other producer).
//!   • Has `span.name = "fluxx.register"` so you can filter it out of
//!     traffic dashboards if you want ("legitimate spans" vs "the
//!     declaration pings").
//!   • Includes operator-supplied resource attributes (tier, owner,
//!     description, repeated --attr key=value pairs).
//!
//! The wire format is protobuf over HTTP (`/v1/traces` on the agent's
//! OTLP HTTP listener), the same path the JS/Python SDKs use. No new
//! agent code needed.
//!
//! ## Why not phone home to the cloud directly?
//!
//! Two reasons. First, `fluxx register` runs on the *producer's*
//! machine, which already has a local agent we trust on loopback —
//! no auth, no TLS, no network egress. Adding a cloud round-trip
//! means handing the producer a cloud API key, which is a security
//! surface we'd rather not multiply. Second, the local agent's cloud
//! forwarder is already pushing every event to the cloud; this span
//! rides that same pipeline. One transport, one set of tests.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Args;
use prost::Message;
use rand::RngCore;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

/// Span kind for the registration span. SPAN_KIND_INTERNAL (1) is the
/// honest classification — this isn't a server, client, producer, or
/// consumer span. It exists purely to declare presence.
const SPAN_KIND_INTERNAL: i32 = 1;

const DEFAULT_AGENT_URL: &str = "http://127.0.0.1:4318";

#[derive(Args, Debug)]
pub struct RegisterArgs {
    /// Service name to register. If omitted, we look for fluxx.toml
    /// in the current directory and use its `service_name` field.
    pub service_name: Option<String>,

    /// Tier annotation (production / staging / dev / etc). Attached
    /// to the registration span's resource as `fluxx.tier`.
    #[arg(long)]
    pub tier: Option<String>,

    /// Team or person responsible for this service. Attached as
    /// `fluxx.owner`. Free-form — email, GitHub handle, Slack channel.
    #[arg(long)]
    pub owner: Option<String>,

    /// One-line human description. Attached as `fluxx.description`.
    #[arg(long)]
    pub description: Option<String>,

    /// Repeatable arbitrary key=value resource attributes. Use this
    /// for any annotation the curated `--tier` / `--owner` /
    /// `--description` flags don't cover.
    #[arg(long = "attr", value_parser = parse_kv)]
    pub attrs: Vec<(String, String)>,

    /// Base URL of the local Fluxx agent's OTLP HTTP listener. The
    /// default is loopback; override for sidecar deployments.
    #[arg(long, default_value = DEFAULT_AGENT_URL)]
    pub agent_url: String,

    /// Don't actually POST. Just print what would be sent. Useful for
    /// debugging a `fluxx register` invocation in CI before letting it
    /// touch the agent.
    #[arg(long)]
    pub dry_run: bool,
}

fn parse_kv(s: &str) -> std::result::Result<(String, String), String> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| format!("expected key=value, got `{}`", s))?;
    let k = k.trim();
    if k.is_empty() {
        return Err("attribute key must not be empty".to_string());
    }
    Ok((k.to_string(), v.to_string()))
}

// ─── Pure builders (unit-tested) ────────────────────────────────────────────

/// Build a `KeyValue` representing a string attribute. Returns None
/// when the value is empty so we don't emit `fluxx.tier = ""` noise.
fn string_attr(key: &str, value: &str) -> Option<KeyValue> {
    if value.is_empty() {
        return None;
    }
    Some(KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    })
}

/// Assemble the full resource-attribute list for the registration
/// span. `service.name` is always first (and required); the rest are
/// optional. Pulled into its own function so tests can assert the
/// "every named flag becomes a KV" property without going through
/// the whole request builder.
pub(crate) fn build_resource_attributes(
    service_name: &str,
    tier: Option<&str>,
    owner: Option<&str>,
    description: Option<&str>,
    extra: &[(String, String)],
) -> Vec<KeyValue> {
    let mut out: Vec<KeyValue> = Vec::with_capacity(4 + extra.len());

    // service.name is non-optional. Empty input should never reach
    // here (validated upstream), but be defensive.
    out.push(KeyValue {
        key: "service.name".to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(service_name.to_string())),
        }),
    });

    if let Some(v) = tier.and_then(|t| string_attr("fluxx.tier", t)) {
        out.push(v);
    }
    if let Some(v) = owner.and_then(|t| string_attr("fluxx.owner", t)) {
        out.push(v);
    }
    if let Some(v) = description.and_then(|t| string_attr("fluxx.description", t)) {
        out.push(v);
    }
    for (k, v) in extra {
        out.push(KeyValue {
            key: k.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.clone())),
            }),
        });
    }
    out
}

/// Build a complete `ExportTraceServiceRequest` containing one
/// synthetic registration span. Pure: takes a clock argument so tests
/// can pin timestamps deterministically.
pub(crate) fn build_request(
    service_name: &str,
    resource_attrs: Vec<KeyValue>,
    now_ns: u64,
    trace_id: [u8; 16],
    span_id: [u8; 8],
) -> ExportTraceServiceRequest {
    let span = Span {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        trace_state: String::new(),
        parent_span_id: Vec::new(),
        flags: 0,
        name: "fluxx.register".to_string(),
        kind: SPAN_KIND_INTERNAL,
        start_time_unix_nano: now_ns,
        // 1ms after start — gives /service-map a non-zero duration to
        // average over without skewing real-latency numbers.
        end_time_unix_nano: now_ns + 1_000_000,
        attributes: vec![
            // span-level attribute too, so even if a downstream
            // dashboard tab strips resource attrs, the marker survives.
            KeyValue {
                key: "fluxx.event".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(
                        "registration".to_string(),
                    )),
                }),
            },
        ],
        dropped_attributes_count: 0,
        events: Vec::new(),
        dropped_events_count: 0,
        links: Vec::new(),
        dropped_links_count: 0,
        status: None,
    };

    let scope_spans = ScopeSpans {
        scope: None,
        spans: vec![span],
        schema_url: String::new(),
    };

    // Build the Resource via ..default so future-added proto fields
    // (e.g. entity_refs in a newer opentelemetry-proto) get their
    // zero defaults without breaking compilation here.
    let mut resource = Resource::default();
    resource.attributes = resource_attrs;
    resource.dropped_attributes_count = 0;
    let resource_spans = ResourceSpans {
        resource: Some(resource),
        scope_spans: vec![scope_spans],
        schema_url: String::new(),
    };

    let _ = service_name; // already on the resource; arg kept for symmetry
    ExportTraceServiceRequest {
        resource_spans: vec![resource_spans],
    }
}

// ─── fluxx.toml fallback (uses the init module's schema) ────────────────────

fn service_name_from_local_toml() -> Option<String> {
    let p = std::path::Path::new("fluxx.toml");
    if !p.is_file() {
        return None;
    }
    let body = std::fs::read_to_string(p).ok()?;
    let parsed: crate::cli::init::ProducerConfig = toml::from_str(&body).ok()?;
    Some(parsed.producer.service_name)
}

// ─── Runtime ────────────────────────────────────────────────────────────────

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn random_id<const N: usize>() -> [u8; N] {
    let mut buf = [0u8; N];
    rand::thread_rng().fill_bytes(&mut buf);
    buf
}

pub async fn run(args: RegisterArgs) -> Result<()> {
    // Resolve the service name from arg or fluxx.toml.
    let service_name = args
        .service_name
        .clone()
        .or_else(service_name_from_local_toml)
        .context(
            "no service name provided (positional arg) and no fluxx.toml found in cwd.\n\
             Either run `fluxx register <service-name>` explicitly, or `cd` into a \
             repo that has a fluxx.toml.",
        )?;

    let attrs = build_resource_attributes(
        &service_name,
        args.tier.as_deref(),
        args.owner.as_deref(),
        args.description.as_deref(),
        &args.attrs,
    );

    let trace_id = random_id::<16>();
    let span_id = random_id::<8>();
    let req = build_request(&service_name, attrs, now_ns(), trace_id, span_id);
    let body = req.encode_to_vec();

    if args.dry_run {
        println!(
            "dry-run: would POST {} bytes of protobuf to {}/v1/traces",
            body.len(),
            args.agent_url
        );
        println!("service.name = {}", service_name);
        if let Some(t) = &args.tier {
            println!("fluxx.tier = {}", t);
        }
        if let Some(o) = &args.owner {
            println!("fluxx.owner = {}", o);
        }
        if let Some(d) = &args.description {
            println!("fluxx.description = {}", d);
        }
        for (k, v) in &args.attrs {
            println!("{} = {}", k, v);
        }
        return Ok(());
    }

    let url = format!("{}/v1/traces", args.agent_url.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let resp = client
        .post(&url)
        .header("Content-Type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .with_context(|| {
            format!(
                "could not reach the Fluxx agent at {}. Is the agent running? Try: \
                 systemctl status fluxx-agent",
                args.agent_url
            )
        })?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("agent returned HTTP {}: {}", status, body);
    }

    println!(
        "✓ registered service.name={} (trace_id={})",
        service_name,
        hex16(&trace_id)
    );
    println!(
        "  appears in the dashboard /service-map within ~30s. \
         Verify with: fluxx verify {}",
        service_name
    );
    Ok(())
}

fn hex16(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for c in b {
        use std::fmt::Write;
        let _ = write!(&mut s, "{:02x}", c);
    }
    s
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn val_of(kv: &KeyValue) -> &str {
        match &kv.value {
            Some(AnyValue {
                value: Some(any_value::Value::StringValue(s)),
            }) => s.as_str(),
            _ => "",
        }
    }

    fn find<'a>(kvs: &'a [KeyValue], key: &str) -> Option<&'a KeyValue> {
        kvs.iter().find(|kv| kv.key == key)
    }

    // ── parse_kv ─────────────────────────────────────────────────────────

    #[test]
    fn parse_kv_splits_on_first_equals() {
        assert_eq!(
            parse_kv("foo=bar").unwrap(),
            ("foo".to_string(), "bar".to_string())
        );
    }

    #[test]
    fn parse_kv_keeps_extra_equals_in_value() {
        // Values with `=` (base64, URLs with query strings) must survive
        // unmangled. split_once handles this correctly; pin the behaviour.
        assert_eq!(
            parse_kv("url=https://x.example.com/?a=1&b=2").unwrap(),
            (
                "url".to_string(),
                "https://x.example.com/?a=1&b=2".to_string()
            )
        );
    }

    #[test]
    fn parse_kv_rejects_missing_equals() {
        let e = parse_kv("nope").unwrap_err();
        assert!(e.contains("expected key=value"));
    }

    #[test]
    fn parse_kv_rejects_empty_key() {
        // `=value` is meaningless. We reject loudly so a typo doesn't
        // become an attribute with empty name (which OTel collectors
        // may silently drop later).
        let e = parse_kv("=value").unwrap_err();
        assert!(e.contains("must not be empty"));
    }

    #[test]
    fn parse_kv_trims_whitespace_around_key() {
        assert_eq!(
            parse_kv("  foo  =bar").unwrap(),
            ("foo".to_string(), "bar".to_string())
        );
    }

    // ── build_resource_attributes ────────────────────────────────────────

    #[test]
    fn build_attrs_always_includes_service_name() {
        let kvs = build_resource_attributes("taskx-api", None, None, None, &[]);
        let svc = find(&kvs, "service.name").expect("service.name missing");
        assert_eq!(val_of(svc), "taskx-api");
    }

    #[test]
    fn build_attrs_promotes_tier_owner_description() {
        let kvs = build_resource_attributes(
            "x",
            Some("production"),
            Some("ops@example.com"),
            Some("Order processor"),
            &[],
        );
        assert_eq!(val_of(find(&kvs, "fluxx.tier").unwrap()), "production");
        assert_eq!(val_of(find(&kvs, "fluxx.owner").unwrap()), "ops@example.com");
        assert_eq!(
            val_of(find(&kvs, "fluxx.description").unwrap()),
            "Order processor"
        );
    }

    #[test]
    fn build_attrs_omits_empty_strings() {
        // Empty input must NOT produce a `fluxx.tier = ""` entry.
        // Empty resource attributes pollute dashboards and confuse
        // downstream filters.
        let kvs = build_resource_attributes("x", Some(""), Some(""), Some(""), &[]);
        assert_eq!(kvs.len(), 1, "only service.name should remain, got {:?}", kvs);
    }

    #[test]
    fn build_attrs_appends_extra_pairs_in_order() {
        let extra = vec![
            ("region".to_string(), "us-central1".to_string()),
            ("region".to_string(), "us-east1".to_string()), // duplicate key on purpose
        ];
        let kvs = build_resource_attributes("x", None, None, None, &extra);
        let regions: Vec<&str> = kvs
            .iter()
            .filter(|kv| kv.key == "region")
            .map(val_of)
            .collect();
        // We don't dedupe — last-write-wins semantics belong on the
        // server side. Pin so a future "helpful" dedupe in this fn
        // doesn't surprise operators.
        assert_eq!(regions, vec!["us-central1", "us-east1"]);
    }

    #[test]
    fn build_attrs_service_name_is_first() {
        // The OTel spec doesn't require ordering, but downstream
        // dashboards have shown bugs where they pluck the FIRST
        // attribute as the title. Pin service.name at position 0 so
        // we don't regress that surface.
        let kvs = build_resource_attributes(
            "x",
            Some("production"),
            None,
            None,
            &[("zone".to_string(), "us-c1".to_string())],
        );
        assert_eq!(kvs[0].key, "service.name");
    }

    // ── build_request ────────────────────────────────────────────────────

    #[test]
    fn build_request_has_one_resource_one_scope_one_span() {
        let req = build_request(
            "x",
            build_resource_attributes("x", None, None, None, &[]),
            1_000,
            [0xaa; 16],
            [0xbb; 8],
        );
        assert_eq!(req.resource_spans.len(), 1);
        let rs = &req.resource_spans[0];
        assert_eq!(rs.scope_spans.len(), 1);
        assert_eq!(rs.scope_spans[0].spans.len(), 1);
    }

    #[test]
    fn build_request_span_has_fluxx_register_name() {
        let req = build_request(
            "x",
            build_resource_attributes("x", None, None, None, &[]),
            1_000,
            [0; 16],
            [0; 8],
        );
        let span = &req.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.name, "fluxx.register");
        assert_eq!(span.kind, SPAN_KIND_INTERNAL);
    }

    #[test]
    fn build_request_span_has_one_millisecond_duration() {
        // The 1ms duration is intentional — non-zero so /service-map
        // can compute averages, but tiny enough not to skew real
        // latency numbers. Pin against a future regression where
        // someone changes it to e.g. 1s and breaks dashboards.
        let req = build_request(
            "x",
            build_resource_attributes("x", None, None, None, &[]),
            1_000_000_000,
            [0; 16],
            [0; 8],
        );
        let span = &req.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(
            span.end_time_unix_nano - span.start_time_unix_nano,
            1_000_000,
            "registration span duration must be exactly 1ms"
        );
    }

    #[test]
    fn build_request_span_carries_fluxx_event_attribute() {
        // Belt-and-braces: even if a downstream tool drops resource
        // attributes (e.g. some sampling middleware), the span itself
        // tells you "I am a registration ping" via fluxx.event.
        let req = build_request(
            "x",
            build_resource_attributes("x", None, None, None, &[]),
            0,
            [0; 16],
            [0; 8],
        );
        let span = &req.resource_spans[0].scope_spans[0].spans[0];
        let ev = span
            .attributes
            .iter()
            .find(|kv| kv.key == "fluxx.event")
            .expect("fluxx.event span attribute missing");
        assert_eq!(val_of(ev), "registration");
    }

    #[test]
    fn build_request_resource_carries_service_name() {
        let req = build_request(
            "taskx-api",
            build_resource_attributes("taskx-api", None, None, None, &[]),
            0,
            [0; 16],
            [0; 8],
        );
        let resource = req.resource_spans[0].resource.as_ref().unwrap();
        let svc = find(&resource.attributes, "service.name").unwrap();
        assert_eq!(val_of(svc), "taskx-api");
    }

    #[test]
    fn build_request_protobuf_roundtrips() {
        // The whole point of this command is sending a valid OTLP
        // payload. If `prost::encode_to_vec` fails or our types are
        // misshaped, the agent's decoder will reject it. Round-trip
        // through prost to prove the wire format is sane.
        let original = build_request(
            "taskx-api",
            build_resource_attributes("taskx-api", Some("production"), None, None, &[]),
            42,
            [0xaa; 16],
            [0xbb; 8],
        );
        let bytes = original.encode_to_vec();
        let decoded = ExportTraceServiceRequest::decode(bytes.as_slice())
            .expect("encoded request should re-decode");
        assert_eq!(decoded.resource_spans.len(), 1);
        let span = &decoded.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.name, "fluxx.register");
        assert_eq!(span.trace_id.len(), 16);
        assert_eq!(span.span_id.len(), 8);
    }

    // ── hex16 ────────────────────────────────────────────────────────────

    #[test]
    fn hex16_lowercase_hex_no_separators() {
        assert_eq!(hex16(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
        assert_eq!(hex16(&[0x00, 0xff]), "00ff");
    }

    #[test]
    fn hex16_handles_empty_input() {
        assert_eq!(hex16(&[]), "");
    }
}
