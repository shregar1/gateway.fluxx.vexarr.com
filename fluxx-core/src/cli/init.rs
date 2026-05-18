//! `fluxx init` — declare a repo as a Fluxx producer.
//!
//! Writes `fluxx.toml` at the repo root. The file is a marker +
//! configuration: it tells the world (and CI, and other agents,
//! and future humans) that this repo emits telemetry to a local
//! Fluxx agent, what `service.name` it announces itself under,
//! which exports are turned on, and what env var the entrypoint
//! reads. The actual OTel SDK wiring is printed as a next-steps
//! block — we deliberately don't inject code into the repo.
//!
//! Design contract:
//!
//!   • Detection is heuristic but explicit. Each manifest file
//!     (`Cargo.toml`, `go.mod`, `package.json`, etc.) is checked
//!     in priority order; the first match wins. The mapping table
//!     is in `detect_stack`, pinned by tests.
//!
//!   • The schema is frozen at v1. `ProducerConfig` uses
//!     `#[serde(deny_unknown_fields)]` so a typo in a downstream
//!     repo (`servic_name` instead of `service_name`) fails loudly
//!     at `fluxx init --validate`, not silently in production.
//!
//!   • Re-running on a repo that already has `fluxx.toml` is safe:
//!     parses the existing file, complains about any problems,
//!     never overwrites unless `--force` is passed.
//!
//!   • Every output snippet is rendered by a pure function. The
//!     stack-specific next-steps blocks live in `next_steps_for`,
//!     each a string-returning helper that's covered by a unit
//!     test asserting the snippet mentions the right SDK package.
//!
//! Why no code injection: touching a producer repo's source from
//! a CLI is the kind of thing that breaks PR review and silently
//! drifts. The producer team copy-pastes the printed snippet into
//! the entrypoint. Two extra minutes for them, zero risk of us
//! invisibly editing their code.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Args;
use serde::{Deserialize, Serialize};

/// CLI arguments for `fluxx init`.
#[derive(Args, Debug)]
pub struct InitArgs {
    /// Repo root to operate on. Defaults to the current working dir.
    #[arg(long, default_value = ".")]
    pub path: PathBuf,

    /// Override the auto-derived service name (which is the sanitised
    /// directory name). Useful for monorepos where the dir name and
    /// the deploy-time service name diverge.
    #[arg(long)]
    pub name: Option<String>,

    /// Force a specific stack tag instead of detecting it. Mostly
    /// useful for monorepos where multiple manifest files coexist
    /// (a Tauri repo has both Cargo.toml and package.json) and you
    /// want to be explicit.
    #[arg(long)]
    pub stack: Option<String>,

    /// Overwrite an existing fluxx.toml. Without this flag, an
    /// existing file is validated (and we exit non-zero if it's
    /// broken) but never touched.
    #[arg(long)]
    pub force: bool,

    /// Don't write anything — just validate the existing fluxx.toml
    /// if there is one. Exit 0 = valid, exit 1 = missing or broken.
    /// Designed as a CI pre-flight: `fluxx init --validate` in the
    /// repo's PR workflow.
    #[arg(long)]
    pub validate: bool,
}

// ─── Stack detection ────────────────────────────────────────────────────────

/// Every language/framework `fluxx init` knows how to wire up.
/// Adding a new variant requires:
///   1. Update `detect_stack` with the file/dependency signature.
///   2. Update `as_toml_str` + `from_toml_str` for serialisation.
///   3. Add a `next_steps_for` branch with the SDK snippet.
///   4. Add a detection test below.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Stack {
    PythonFastApi,
    PythonFlask,
    PythonDjango,
    PythonGeneric,
    NodeTypescript,
    NodeJavascript,
    Go,
    Rust,
    JvmMaven,
    JvmGradle,
    DotnetCsharp,
    RubyRails,
    RubyGeneric,
    PhpLaravel,
    PhpGeneric,
    Unknown,
}

impl Stack {
    /// Stable string written to fluxx.toml. Never rename without a
    /// migration; downstream repos pin against these values.
    pub fn as_toml_str(&self) -> &'static str {
        match self {
            Stack::PythonFastApi => "python-fastapi",
            Stack::PythonFlask => "python-flask",
            Stack::PythonDjango => "python-django",
            Stack::PythonGeneric => "python",
            Stack::NodeTypescript => "node-typescript",
            Stack::NodeJavascript => "node-javascript",
            Stack::Go => "go",
            Stack::Rust => "rust",
            Stack::JvmMaven => "jvm-maven",
            Stack::JvmGradle => "jvm-gradle",
            Stack::DotnetCsharp => "dotnet-csharp",
            Stack::RubyRails => "ruby-rails",
            Stack::RubyGeneric => "ruby",
            Stack::PhpLaravel => "php-laravel",
            Stack::PhpGeneric => "php",
            Stack::Unknown => "unknown",
        }
    }

    pub fn from_toml_str(s: &str) -> Option<Self> {
        Some(match s {
            "python-fastapi" => Stack::PythonFastApi,
            "python-flask" => Stack::PythonFlask,
            "python-django" => Stack::PythonDjango,
            "python" => Stack::PythonGeneric,
            "node-typescript" => Stack::NodeTypescript,
            "node-javascript" => Stack::NodeJavascript,
            "go" => Stack::Go,
            "rust" => Stack::Rust,
            "jvm-maven" => Stack::JvmMaven,
            "jvm-gradle" => Stack::JvmGradle,
            "dotnet-csharp" => Stack::DotnetCsharp,
            "ruby-rails" => Stack::RubyRails,
            "ruby" => Stack::RubyGeneric,
            "php-laravel" => Stack::PhpLaravel,
            "php" => Stack::PhpGeneric,
            "unknown" => Stack::Unknown,
            _ => return None,
        })
    }
}

/// Detect the stack by inspecting manifest files in the given repo
/// root. Priority order matters — Rust beats Node (Tauri), Python
/// frameworks beat generic Python, and unknown is the fallback.
///
/// This is the single source of truth for the detection rules. All
/// tests target this function directly with `tempfile::TempDir`.
pub fn detect_stack(root: &Path) -> Stack {
    // Strictly-typed manifests first (no false positives).
    if root.join("Cargo.toml").is_file() {
        return Stack::Rust;
    }
    if root.join("go.mod").is_file() {
        return Stack::Go;
    }
    if root.join("pom.xml").is_file() {
        return Stack::JvmMaven;
    }
    if root.join("build.gradle").is_file() || root.join("build.gradle.kts").is_file() {
        return Stack::JvmGradle;
    }
    // .NET — look for any .csproj at the root.
    if any_file_with_extension(root, "csproj") {
        return Stack::DotnetCsharp;
    }

    // Python — sniff for framework deps in pyproject.toml or requirements.txt.
    let py_manifest = first_existing(root, &["pyproject.toml", "requirements.txt", "setup.py"]);
    if let Some(p) = py_manifest {
        let content = fs::read_to_string(&p).unwrap_or_default().to_lowercase();
        if content.contains("fastapi") {
            return Stack::PythonFastApi;
        }
        if content.contains("django") {
            return Stack::PythonDjango;
        }
        if content.contains("flask") {
            return Stack::PythonFlask;
        }
        return Stack::PythonGeneric;
    }

    // Ruby — Gemfile + sniff for Rails.
    let ruby_manifest = first_existing(root, &["Gemfile", "Gemfile.lock"]);
    if let Some(p) = ruby_manifest {
        let content = fs::read_to_string(&p).unwrap_or_default().to_lowercase();
        if content.contains("rails") {
            return Stack::RubyRails;
        }
        return Stack::RubyGeneric;
    }

    // PHP — composer.json + sniff for Laravel.
    if let Ok(s) = fs::read_to_string(root.join("composer.json")) {
        let lower = s.to_lowercase();
        if lower.contains("laravel/framework") || lower.contains("laravel/laravel") {
            return Stack::PhpLaravel;
        }
        return Stack::PhpGeneric;
    }

    // Node — distinguish TS from JS by tsconfig.json presence.
    if root.join("package.json").is_file() {
        if root.join("tsconfig.json").is_file() {
            return Stack::NodeTypescript;
        }
        return Stack::NodeJavascript;
    }

    Stack::Unknown
}

fn first_existing(root: &Path, names: &[&str]) -> Option<PathBuf> {
    for n in names {
        let p = root.join(n);
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

fn any_file_with_extension(root: &Path, ext: &str) -> bool {
    fs::read_dir(root)
        .map(|rd| {
            rd.flatten()
                .any(|e| e.path().extension().is_some_and(|x| x == ext))
        })
        .unwrap_or(false)
}

// ─── Service-name derivation ────────────────────────────────────────────────

/// Sanitise an arbitrary string into a service name fluxx-style:
/// lowercase, ASCII alphanumeric + dashes, no leading/trailing or
/// consecutive dashes. Empty input becomes "unknown" so we never
/// produce an invalid service.name resource attribute.
pub fn sanitize_service_name(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_dash = true; // suppress leading dash
    for c in s.chars() {
        let mapped = if c.is_ascii_alphanumeric() {
            c.to_ascii_lowercase()
        } else {
            '-'
        };
        if mapped == '-' && prev_dash {
            continue;
        }
        out.push(mapped);
        prev_dash = mapped == '-';
    }
    let trimmed = out.trim_end_matches('-').to_string();
    if trimmed.is_empty() {
        return "unknown".to_string();
    }
    trimmed
}

/// Best-effort derive a default service name from the repo path:
/// the final directory segment, run through `sanitize_service_name`.
pub fn derive_service_name(root: &Path) -> String {
    let canon = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let name = canon
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    sanitize_service_name(&name)
}

// ─── TOML schema ────────────────────────────────────────────────────────────

/// `fluxx.toml` v1 schema. Forward-compat: adding new optional fields
/// is a non-breaking change; renaming or removing existing fields is.
/// `deny_unknown_fields` everywhere makes typos fail loudly.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProducerConfig {
    pub producer: Producer,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Producer {
    pub service_name: String,
    pub stack: String,
    pub endpoint: Endpoint,
    pub exports: Exports,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub env_var: String,
    pub prod_value: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Exports {
    pub traces: bool,
    pub metrics: bool,
    pub logs: bool,
}

impl ProducerConfig {
    pub fn default_for(service_name: &str, stack: Stack) -> Self {
        let mut attrs = HashMap::new();
        attrs.insert(
            "deployment.environment".to_string(),
            "production".to_string(),
        );
        Self {
            producer: Producer {
                service_name: service_name.to_string(),
                stack: stack.as_toml_str().to_string(),
                endpoint: Endpoint {
                    env_var: "FLUXX_OTEL_ENDPOINT".to_string(),
                    prod_value: "http://localhost:4318".to_string(),
                },
                exports: Exports {
                    traces: true,
                    metrics: false,
                    logs: false,
                },
                attributes: attrs,
            },
        }
    }
}

/// Render `cfg` to the friendly TOML format we write to disk.
/// Hand-rolled rather than `toml::to_string_pretty` so we can
/// include the documentation comments operators expect.
pub fn render_toml(cfg: &ProducerConfig) -> String {
    let p = &cfg.producer;
    let mut attrs_block = String::new();
    let mut keys: Vec<&String> = p.attributes.keys().collect();
    keys.sort(); // deterministic output for diff-friendly fluxx.toml
    for k in keys {
        let v = &p.attributes[k];
        attrs_block.push_str(&format!(
            "{} = {}\n",
            toml_key(k),
            toml::Value::String(v.clone())
        ));
    }
    format!(
        "# Marker file — this repo produces telemetry to a local Fluxx agent.\n\
         # Generated by `fluxx init`. Safe to hand-edit.\n\
         # Schema v1; keep field names stable.\n\
         \n\
         [producer]\n\
         service_name = {sn}\n\
         stack        = {st}\n\
         \n\
         [producer.endpoint]\n\
         # Env var the entrypoint reads to discover the agent. Leave\n\
         # unset in dev to disable telemetry; set to `prod_value` in\n\
         # deploy config to turn it on.\n\
         env_var      = {ev}\n\
         prod_value   = {pv}\n\
         \n\
         [producer.exports]\n\
         traces  = {t}\n\
         metrics = {m}\n\
         logs    = {l}\n\
         \n\
         [producer.attributes]\n\
         # Resource attributes attached to every span/log emitted by\n\
         # this service. Override per-deploy via OTEL_RESOURCE_ATTRIBUTES.\n\
         {attrs}",
        sn = toml::Value::String(p.service_name.clone()),
        st = toml::Value::String(p.stack.clone()),
        ev = toml::Value::String(p.endpoint.env_var.clone()),
        pv = toml::Value::String(p.endpoint.prod_value.clone()),
        t = p.exports.traces,
        m = p.exports.metrics,
        l = p.exports.logs,
        attrs = attrs_block,
    )
}

/// TOML bare-key when the string is a valid bare key, quoted otherwise.
fn toml_key(s: &str) -> String {
    let bare = !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if bare {
        s.to_string()
    } else {
        toml::Value::String(s.to_string()).to_string()
    }
}

// ─── Next-steps snippets ────────────────────────────────────────────────────

/// Return the post-init guidance string for a given stack + service
/// name. Each branch's snippet is pinned by a test asserting the
/// expected SDK package name appears — guards against the kind of
/// regression where someone edits one snippet and breaks the others.
pub fn next_steps_for(stack: Stack, service_name: &str) -> String {
    let intro = format!(
        "Next steps for `{service_name}` ({stack_tag}):\n\n\
         1. Set these env vars in your prod deploy config:\n\
            OTEL_SERVICE_NAME={service_name}\n\
            FLUXX_OTEL_ENDPOINT=http://localhost:4318\n\
            OTEL_RESOURCE_ATTRIBUTES=deployment.environment=production\n\n",
        stack_tag = stack.as_toml_str()
    );
    let snippet = stack_snippet(stack);
    let verify = format!(
        "3. After deploying, prove it works:\n   fluxx verify {service_name}\n"
    );
    format!("{intro}2. Add the OTel SDK in the entrypoint:\n\n{snippet}\n\n{verify}")
}

fn stack_snippet(stack: Stack) -> String {
    match stack {
        Stack::PythonFastApi => python_fastapi_snippet(),
        Stack::PythonFlask => python_flask_snippet(),
        Stack::PythonDjango => python_django_snippet(),
        Stack::PythonGeneric => python_generic_snippet(),
        Stack::NodeTypescript | Stack::NodeJavascript => node_snippet(),
        Stack::Go => go_snippet(),
        Stack::Rust => rust_snippet(),
        Stack::JvmMaven | Stack::JvmGradle => jvm_snippet(),
        Stack::DotnetCsharp => dotnet_snippet(),
        Stack::RubyRails | Stack::RubyGeneric => ruby_snippet(),
        Stack::PhpLaravel | Stack::PhpGeneric => php_snippet(),
        Stack::Unknown => unknown_snippet(),
    }
}

fn python_fastapi_snippet() -> String {
    "  pip install opentelemetry-distro opentelemetry-exporter-otlp-proto-http \\\n\
    \x20             opentelemetry-instrumentation-fastapi \\\n\
    \x20             opentelemetry-instrumentation-sqlalchemy\n\n\
    \x20 # In the file where `app = FastAPI()` is constructed, BEFORE that line:\n\
    \x20 import os\n\
    \x20 from opentelemetry import trace\n\
    \x20 from opentelemetry.sdk.trace import TracerProvider\n\
    \x20 from opentelemetry.sdk.resources import Resource\n\
    \x20 from opentelemetry.sdk.trace.export import BatchSpanProcessor\n\
    \x20 from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter\n\
    \x20 from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor\n\
    \x20 if os.getenv(\"FLUXX_OTEL_ENDPOINT\"):\n\
    \x20     provider = TracerProvider(resource=Resource.create({\n\
    \x20         \"service.name\": os.getenv(\"OTEL_SERVICE_NAME\", \"unknown\"),\n\
    \x20     }))\n\
    \x20     provider.add_span_processor(BatchSpanProcessor(\n\
    \x20         OTLPSpanExporter(endpoint=f\"{os.getenv('FLUXX_OTEL_ENDPOINT')}/v1/traces\")))\n\
    \x20     trace.set_tracer_provider(provider)\n\n\
    \x20 # AFTER `app = FastAPI(...)`:\n\
    \x20 if os.getenv(\"FLUXX_OTEL_ENDPOINT\"):\n\
    \x20     FastAPIInstrumentor.instrument_app(app)\n".to_string()
}

fn python_flask_snippet() -> String {
    "  pip install opentelemetry-distro opentelemetry-exporter-otlp-proto-http \\\n\
    \x20             opentelemetry-instrumentation-flask\n\n\
    \x20 # In the app factory (typically src/app.py), AFTER `app = Flask(__name__)`:\n\
    \x20 import os\n\
    \x20 from opentelemetry import trace\n\
    \x20 from opentelemetry.sdk.trace import TracerProvider\n\
    \x20 from opentelemetry.sdk.resources import Resource\n\
    \x20 from opentelemetry.sdk.trace.export import BatchSpanProcessor\n\
    \x20 from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter\n\
    \x20 from opentelemetry.instrumentation.flask import FlaskInstrumentor\n\
    \x20 if os.getenv(\"FLUXX_OTEL_ENDPOINT\"):\n\
    \x20     prov = TracerProvider(resource=Resource.create({\n\
    \x20         \"service.name\": os.getenv(\"OTEL_SERVICE_NAME\", \"unknown\")}))\n\
    \x20     prov.add_span_processor(BatchSpanProcessor(\n\
    \x20         OTLPSpanExporter(endpoint=f\"{os.getenv('FLUXX_OTEL_ENDPOINT')}/v1/traces\")))\n\
    \x20     trace.set_tracer_provider(prov)\n\
    \x20     FlaskInstrumentor().instrument_app(app)\n".to_string()
}

fn python_django_snippet() -> String {
    "  pip install opentelemetry-distro opentelemetry-exporter-otlp-proto-http \\\n\
    \x20             opentelemetry-instrumentation-django\n\n\
    \x20 # The easiest path is opentelemetry-instrument auto-loader; in your\n\
    \x20 # Dockerfile / Procfile, prefix your manage.py / gunicorn invocation:\n\n\
    \x20 #   opentelemetry-instrument \\\n\
    \x20 #     --exporter_otlp_endpoint $FLUXX_OTEL_ENDPOINT \\\n\
    \x20 #     --service_name $OTEL_SERVICE_NAME \\\n\
    \x20 #     gunicorn myproject.wsgi\n\n\
    \x20 # Or, for explicit programmatic init, see manage.py + the Django\n\
    \x20 # docs in opentelemetry-instrumentation-django.\n".to_string()
}

fn python_generic_snippet() -> String {
    "  pip install opentelemetry-distro opentelemetry-exporter-otlp-proto-http\n\n\
    \x20 # In your service entrypoint, BEFORE any framework imports:\n\
    \x20 import os\n\
    \x20 from opentelemetry import trace\n\
    \x20 from opentelemetry.sdk.trace import TracerProvider\n\
    \x20 from opentelemetry.sdk.resources import Resource\n\
    \x20 from opentelemetry.sdk.trace.export import BatchSpanProcessor\n\
    \x20 from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter\n\
    \x20 if os.getenv(\"FLUXX_OTEL_ENDPOINT\"):\n\
    \x20     prov = TracerProvider(resource=Resource.create({\n\
    \x20         \"service.name\": os.getenv(\"OTEL_SERVICE_NAME\", \"unknown\")}))\n\
    \x20     prov.add_span_processor(BatchSpanProcessor(\n\
    \x20         OTLPSpanExporter(endpoint=f\"{os.getenv('FLUXX_OTEL_ENDPOINT')}/v1/traces\")))\n\
    \x20     trace.set_tracer_provider(prov)\n".to_string()
}

fn node_snippet() -> String {
    "  npm install --save \\\n\
    \x20   @opentelemetry/sdk-node \\\n\
    \x20   @opentelemetry/auto-instrumentations-node \\\n\
    \x20   @opentelemetry/exporter-trace-otlp-proto \\\n\
    \x20   @opentelemetry/resources \\\n\
    \x20   @opentelemetry/semantic-conventions\n\n\
    \x20 // src/telemetry.ts (or .js) — must be IMPORTED FIRST in the entrypoint.\n\
    \x20 // Use exporter-trace-otlp-proto (NOT -http) — it sends protobuf, which\n\
    \x20 // every receiver speaks. The -http variant sends JSON with proto3's\n\
    \x20 // int64-as-string convention that several agents (including Fluxx)\n\
    \x20 // can't parse out of the box.\n\
    \x20 import { NodeSDK } from \"@opentelemetry/sdk-node\";\n\
    \x20 import { OTLPTraceExporter } from \"@opentelemetry/exporter-trace-otlp-proto\";\n\
    \x20 import { getNodeAutoInstrumentations } from \"@opentelemetry/auto-instrumentations-node\";\n\
    \x20 import { Resource } from \"@opentelemetry/resources\";\n\
    \x20 import { SemanticResourceAttributes } from \"@opentelemetry/semantic-conventions\";\n\
    \x20 const ep = process.env.FLUXX_OTEL_ENDPOINT;\n\
    \x20 if (ep) {\n\
    \x20   new NodeSDK({\n\
    \x20     resource: new Resource({\n\
    \x20       [SemanticResourceAttributes.SERVICE_NAME]: process.env.OTEL_SERVICE_NAME ?? \"unknown\",\n\
    \x20     }),\n\
    \x20     traceExporter: new OTLPTraceExporter({ url: `${ep}/v1/traces` }),\n\
    \x20     instrumentations: [getNodeAutoInstrumentations()],\n\
    \x20   }).start();\n\
    \x20 }\n".to_string()
}

fn go_snippet() -> String {
    "  go get go.opentelemetry.io/otel \\\n\
    \x20        go.opentelemetry.io/otel/sdk \\\n\
    \x20        go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp \\\n\
    \x20        go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp\n\n\
    \x20 // In main.go, before the server starts:\n\
    \x20 if ep := os.Getenv(\"FLUXX_OTEL_ENDPOINT\"); ep != \"\" {\n\
    \x20   exp, _ := otlptracehttp.New(ctx,\n\
    \x20     otlptracehttp.WithEndpoint(strings.TrimPrefix(strings.TrimPrefix(ep, \"http://\"), \"https://\")),\n\
    \x20     otlptracehttp.WithInsecure())\n\
    \x20   tp := sdktrace.NewTracerProvider(\n\
    \x20     sdktrace.WithBatcher(exp),\n\
    \x20     sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL,\n\
    \x20       semconv.ServiceName(getenv(\"OTEL_SERVICE_NAME\", \"unknown\")))))\n\
    \x20   otel.SetTracerProvider(tp); defer tp.Shutdown(context.Background())\n\
    \x20 }\n\
    \x20 // Then wrap your HTTP handler: handler = otelhttp.NewHandler(handler, \"http\")\n".to_string()
}

fn rust_snippet() -> String {
    "  # Cargo.toml additions:\n\
    \x20 # opentelemetry        = { version = \"0.27\", features = [\"trace\"] }\n\
    \x20 # opentelemetry_sdk    = { version = \"0.27\", features = [\"rt-tokio\"] }\n\
    \x20 # opentelemetry-otlp   = { version = \"0.27\", features = [\"http-proto\", \"reqwest-client\"] }\n\
    \x20 # tracing-opentelemetry= \"0.28\"\n\n\
    \x20 // In your init function:\n\
    \x20 if let Ok(ep) = std::env::var(\"FLUXX_OTEL_ENDPOINT\") {\n\
    \x20   let exporter = opentelemetry_otlp::SpanExporter::builder()\n\
    \x20     .with_http()\n\
    \x20     .with_endpoint(format!(\"{}/v1/traces\", ep))\n\
    \x20     .build().expect(\"otlp\");\n\
    \x20   let provider = opentelemetry_sdk::trace::TracerProvider::builder()\n\
    \x20     .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)\n\
    \x20     .with_resource(opentelemetry_sdk::Resource::new([\n\
    \x20       opentelemetry::KeyValue::new(\"service.name\",\n\
    \x20         std::env::var(\"OTEL_SERVICE_NAME\").unwrap_or_else(|_| \"unknown\".into())),\n\
    \x20     ]))\n\
    \x20     .build();\n\
    \x20   opentelemetry::global::set_tracer_provider(provider);\n\
    \x20 }\n".to_string()
}

fn jvm_snippet() -> String {
    "  # Easiest: drop the OpenTelemetry Java agent JAR into your container,\n\
    \x20 # then set these env vars (no code change needed):\n\n\
    \x20 #   JAVA_TOOL_OPTIONS=\"-javaagent:/opt/otel/opentelemetry-javaagent.jar\"\n\
    \x20 #   OTEL_EXPORTER_OTLP_ENDPOINT=$FLUXX_OTEL_ENDPOINT\n\
    \x20 #   OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf\n\
    \x20 #   OTEL_SERVICE_NAME=$OTEL_SERVICE_NAME\n\n\
    \x20 # Download: https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases\n".to_string()
}

fn dotnet_snippet() -> String {
    "  dotnet add package OpenTelemetry.Extensions.Hosting\n\
    \x20 dotnet add package OpenTelemetry.Exporter.OpenTelemetryProtocol\n\
    \x20 dotnet add package OpenTelemetry.Instrumentation.AspNetCore\n\n\
    \x20 // In Program.cs:\n\
    \x20 var ep = Environment.GetEnvironmentVariable(\"FLUXX_OTEL_ENDPOINT\");\n\
    \x20 if (!string.IsNullOrEmpty(ep)) {\n\
    \x20   builder.Services.AddOpenTelemetry()\n\
    \x20     .ConfigureResource(r => r.AddService(\n\
    \x20         Environment.GetEnvironmentVariable(\"OTEL_SERVICE_NAME\") ?? \"unknown\"))\n\
    \x20     .WithTracing(t => t\n\
    \x20       .AddAspNetCoreInstrumentation()\n\
    \x20       .AddHttpClientInstrumentation()\n\
    \x20       .AddOtlpExporter(o => {\n\
    \x20         o.Endpoint = new Uri($\"{ep}/v1/traces\");\n\
    \x20         o.Protocol = OtlpExportProtocol.HttpProtobuf;\n\
    \x20       }));\n\
    \x20 }\n".to_string()
}

fn ruby_snippet() -> String {
    "  # Gemfile:\n\
    \x20 gem \"opentelemetry-sdk\"\n\
    \x20 gem \"opentelemetry-exporter-otlp\"\n\
    \x20 gem \"opentelemetry-instrumentation-all\"\n\n\
    \x20 # In config/application.rb (or your boot file):\n\
    \x20 require \"opentelemetry/sdk\"\n\
    \x20 require \"opentelemetry/exporter/otlp\"\n\
    \x20 require \"opentelemetry/instrumentation/all\"\n\
    \x20 if ENV[\"FLUXX_OTEL_ENDPOINT\"]\n\
    \x20   OpenTelemetry::SDK.configure do |c|\n\
    \x20     c.service_name = ENV[\"OTEL_SERVICE_NAME\"] || \"unknown\"\n\
    \x20     c.use_all\n\
    \x20   end\n\
    \x20 end\n\n\
    \x20 # ENV: OTEL_EXPORTER_OTLP_ENDPOINT=$FLUXX_OTEL_ENDPOINT\n\
    \x20 #      OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf\n".to_string()
}

fn php_snippet() -> String {
    "  composer require open-telemetry/sdk \\\n\
    \x20   open-telemetry/exporter-otlp \\\n\
    \x20   open-telemetry/opentelemetry-auto-laravel  # or your framework's auto-pkg\n\n\
    \x20 # ENV for OpenTelemetry SDK auto-loader:\n\
    \x20 #   OTEL_PHP_AUTOLOAD_ENABLED=true\n\
    \x20 #   OTEL_TRACES_EXPORTER=otlp\n\
    \x20 #   OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf\n\
    \x20 #   OTEL_EXPORTER_OTLP_ENDPOINT=$FLUXX_OTEL_ENDPOINT\n\
    \x20 #   OTEL_SERVICE_NAME=$OTEL_SERVICE_NAME\n".to_string()
}

fn unknown_snippet() -> String {
    "  Stack not auto-detected. Install the OpenTelemetry SDK for your\n\
    \x20 language, configure it to export protobuf over HTTP to:\n\n\
    \x20   $FLUXX_OTEL_ENDPOINT/v1/traces\n\n\
    \x20 Set service.name to the value of $OTEL_SERVICE_NAME.\n".to_string()
}

// ─── File I/O ───────────────────────────────────────────────────────────────

const FLUXX_TOML: &str = "fluxx.toml";

/// Read + parse fluxx.toml from `root`. Returns `Ok(None)` if the
/// file doesn't exist; `Err` for any other failure (permissions,
/// malformed TOML, unknown fields).
pub fn read_existing(root: &Path) -> Result<Option<ProducerConfig>> {
    let p = root.join(FLUXX_TOML);
    if !p.exists() {
        return Ok(None);
    }
    let s = fs::read_to_string(&p)
        .with_context(|| format!("reading {}", p.display()))?;
    let cfg: ProducerConfig = toml::from_str(&s)
        .with_context(|| format!("parsing {} as fluxx.toml v1", p.display()))?;
    Ok(Some(cfg))
}

pub async fn run(args: InitArgs) -> Result<()> {
    let root = args.path.canonicalize().unwrap_or(args.path.clone());

    // Validate-only path: parse the existing file, print result, exit.
    if args.validate {
        match read_existing(&root)? {
            Some(cfg) => {
                println!(
                    "✓ {}/fluxx.toml is valid (service.name={}, stack={})",
                    root.display(),
                    cfg.producer.service_name,
                    cfg.producer.stack
                );
                return Ok(());
            }
            None => {
                bail!("no fluxx.toml in {} (run `fluxx init` to create one)", root.display());
            }
        }
    }

    // Existing fluxx.toml: validate, then refuse to overwrite unless --force.
    if let Some(existing) = read_existing(&root)? {
        if !args.force {
            println!(
                "✓ {}/fluxx.toml already exists and parses (service.name={}). \
                 Re-run with --force to regenerate.",
                root.display(),
                existing.producer.service_name
            );
            return Ok(());
        }
    }

    // Resolve stack: explicit override > detection.
    let stack = match args.stack.as_deref() {
        Some(s) => Stack::from_toml_str(s)
            .with_context(|| format!("unknown --stack value: {}. Use one of: {:?}", s, all_stack_strings()))?,
        None => detect_stack(&root),
    };

    // Resolve service name: explicit override > derived.
    let service_name = args
        .name
        .clone()
        .unwrap_or_else(|| derive_service_name(&root));
    let service_name = sanitize_service_name(&service_name);

    let cfg = ProducerConfig::default_for(&service_name, stack);
    let body = render_toml(&cfg);
    let target = root.join(FLUXX_TOML);
    fs::write(&target, body).with_context(|| format!("writing {}", target.display()))?;

    println!(
        "✓ wrote {} (service.name={}, stack={})",
        target.display(),
        cfg.producer.service_name,
        cfg.producer.stack
    );
    println!();
    println!("{}", next_steps_for(stack, &cfg.producer.service_name));
    Ok(())
}

fn all_stack_strings() -> Vec<&'static str> {
    [
        Stack::PythonFastApi,
        Stack::PythonFlask,
        Stack::PythonDjango,
        Stack::PythonGeneric,
        Stack::NodeTypescript,
        Stack::NodeJavascript,
        Stack::Go,
        Stack::Rust,
        Stack::JvmMaven,
        Stack::JvmGradle,
        Stack::DotnetCsharp,
        Stack::RubyRails,
        Stack::RubyGeneric,
        Stack::PhpLaravel,
        Stack::PhpGeneric,
        Stack::Unknown,
    ]
    .iter()
    .map(|s| s.as_toml_str())
    .collect()
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn touch(dir: &Path, name: &str) {
        fs::File::create(dir.join(name)).unwrap();
    }
    fn write_file(dir: &Path, name: &str, contents: &str) {
        let mut f = fs::File::create(dir.join(name)).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
    }

    // ── detect_stack ─────────────────────────────────────────────────────

    #[test]
    fn detect_unknown_for_empty_dir() {
        let td = TempDir::new().unwrap();
        assert_eq!(detect_stack(td.path()), Stack::Unknown);
    }

    #[test]
    fn detect_rust_from_cargo_toml() {
        let td = TempDir::new().unwrap();
        touch(td.path(), "Cargo.toml");
        assert_eq!(detect_stack(td.path()), Stack::Rust);
    }

    #[test]
    fn detect_rust_wins_over_node_when_both_present() {
        // Tauri / Wasm-pack repos have both — we treat the Rust crate
        // as the canonical entrypoint. Pin so a future re-ordering
        // doesn't silently change the detection outcome.
        let td = TempDir::new().unwrap();
        touch(td.path(), "Cargo.toml");
        touch(td.path(), "package.json");
        assert_eq!(detect_stack(td.path()), Stack::Rust);
    }

    #[test]
    fn detect_go_from_go_mod() {
        let td = TempDir::new().unwrap();
        touch(td.path(), "go.mod");
        assert_eq!(detect_stack(td.path()), Stack::Go);
    }

    #[test]
    fn detect_python_fastapi_from_pyproject_dep_line() {
        let td = TempDir::new().unwrap();
        write_file(
            td.path(),
            "pyproject.toml",
            "[project]\ndependencies = [\"fastapi\"]\n",
        );
        assert_eq!(detect_stack(td.path()), Stack::PythonFastApi);
    }

    #[test]
    fn detect_python_django_from_requirements_txt() {
        let td = TempDir::new().unwrap();
        write_file(td.path(), "requirements.txt", "Django==4.2\npsycopg2\n");
        assert_eq!(detect_stack(td.path()), Stack::PythonDjango);
    }

    #[test]
    fn detect_python_flask_from_requirements_txt() {
        let td = TempDir::new().unwrap();
        write_file(td.path(), "requirements.txt", "flask==3.0\n");
        assert_eq!(detect_stack(td.path()), Stack::PythonFlask);
    }

    #[test]
    fn detect_python_generic_when_no_framework_matches() {
        let td = TempDir::new().unwrap();
        write_file(td.path(), "requirements.txt", "requests==2.0\n");
        assert_eq!(detect_stack(td.path()), Stack::PythonGeneric);
    }

    #[test]
    fn detect_node_typescript_when_tsconfig_present() {
        let td = TempDir::new().unwrap();
        touch(td.path(), "package.json");
        touch(td.path(), "tsconfig.json");
        assert_eq!(detect_stack(td.path()), Stack::NodeTypescript);
    }

    #[test]
    fn detect_node_javascript_when_no_tsconfig() {
        let td = TempDir::new().unwrap();
        touch(td.path(), "package.json");
        assert_eq!(detect_stack(td.path()), Stack::NodeJavascript);
    }

    #[test]
    fn detect_jvm_maven_from_pom() {
        let td = TempDir::new().unwrap();
        touch(td.path(), "pom.xml");
        assert_eq!(detect_stack(td.path()), Stack::JvmMaven);
    }

    #[test]
    fn detect_jvm_gradle_from_build_gradle() {
        let td = TempDir::new().unwrap();
        touch(td.path(), "build.gradle");
        assert_eq!(detect_stack(td.path()), Stack::JvmGradle);
    }

    #[test]
    fn detect_jvm_gradle_from_kts() {
        let td = TempDir::new().unwrap();
        touch(td.path(), "build.gradle.kts");
        assert_eq!(detect_stack(td.path()), Stack::JvmGradle);
    }

    #[test]
    fn detect_dotnet_from_any_csproj() {
        let td = TempDir::new().unwrap();
        touch(td.path(), "MyApp.csproj");
        assert_eq!(detect_stack(td.path()), Stack::DotnetCsharp);
    }

    #[test]
    fn detect_ruby_rails_from_gemfile() {
        let td = TempDir::new().unwrap();
        write_file(td.path(), "Gemfile", "gem 'rails', '~> 7.0'\n");
        assert_eq!(detect_stack(td.path()), Stack::RubyRails);
    }

    #[test]
    fn detect_ruby_generic_without_rails() {
        let td = TempDir::new().unwrap();
        write_file(td.path(), "Gemfile", "gem 'sinatra'\n");
        assert_eq!(detect_stack(td.path()), Stack::RubyGeneric);
    }

    #[test]
    fn detect_php_laravel_from_composer() {
        let td = TempDir::new().unwrap();
        write_file(
            td.path(),
            "composer.json",
            "{ \"require\": { \"laravel/framework\": \"^11.0\" } }",
        );
        assert_eq!(detect_stack(td.path()), Stack::PhpLaravel);
    }

    #[test]
    fn detect_php_generic_when_no_laravel() {
        let td = TempDir::new().unwrap();
        write_file(td.path(), "composer.json", "{}");
        assert_eq!(detect_stack(td.path()), Stack::PhpGeneric);
    }

    // ── sanitize_service_name ────────────────────────────────────────────

    #[test]
    fn sanitize_lowercases() {
        assert_eq!(sanitize_service_name("TaskX-API"), "taskx-api");
    }

    #[test]
    fn sanitize_replaces_non_alphanumeric_with_dash() {
        assert_eq!(sanitize_service_name("api.fluxx.dev"), "api-fluxx-dev");
        assert_eq!(sanitize_service_name("my_service"), "my-service");
        assert_eq!(sanitize_service_name("foo bar"), "foo-bar");
    }

    #[test]
    fn sanitize_collapses_consecutive_dashes() {
        // Naive replace would produce "a--b" for "a__b". Tests pin
        // the collapse behaviour so service.name doesn't end up
        // mangled in dashboards.
        assert_eq!(sanitize_service_name("a__b"), "a-b");
        assert_eq!(sanitize_service_name("a / b / c"), "a-b-c");
    }

    #[test]
    fn sanitize_trims_leading_and_trailing_dashes() {
        assert_eq!(sanitize_service_name(".foo."), "foo");
        assert_eq!(sanitize_service_name("---foo---"), "foo");
    }

    #[test]
    fn sanitize_empty_input_becomes_unknown() {
        assert_eq!(sanitize_service_name(""), "unknown");
        assert_eq!(sanitize_service_name("---"), "unknown");
    }

    // ── ProducerConfig round-trip ────────────────────────────────────────

    #[test]
    fn default_config_has_traces_on_metrics_logs_off() {
        let cfg = ProducerConfig::default_for("foo", Stack::Rust);
        assert_eq!(cfg.producer.service_name, "foo");
        assert_eq!(cfg.producer.stack, "rust");
        assert!(cfg.producer.exports.traces);
        assert!(!cfg.producer.exports.metrics);
        assert!(!cfg.producer.exports.logs);
        assert_eq!(
            cfg.producer.endpoint.env_var,
            "FLUXX_OTEL_ENDPOINT"
        );
        assert_eq!(cfg.producer.endpoint.prod_value, "http://localhost:4318");
    }

    #[test]
    fn render_toml_roundtrips_through_parser() {
        // The canonical TDD guard for a writer + reader pair: write
        // a config, parse the rendered string, assert equality.
        let cfg = ProducerConfig::default_for("taskx-api", Stack::PythonFastApi);
        let s = render_toml(&cfg);
        let back: ProducerConfig =
            toml::from_str(&s).expect("rendered fluxx.toml should re-parse");
        assert_eq!(back, cfg);
    }

    #[test]
    fn parser_rejects_unknown_top_level_keys() {
        // deny_unknown_fields contract: a typo in a producer field
        // must be a parse error, not silently ignored. (Caught a real
        // bug in an early draft where `servic_name` would have been
        // silently dropped.)
        let bad = r#"
[producer]
service_name = "x"
stack = "rust"
servic_name = "typo"
[producer.endpoint]
env_var = "FLUXX_OTEL_ENDPOINT"
prod_value = "http://localhost:4318"
[producer.exports]
traces = true
metrics = false
logs = false
"#;
        let res: Result<ProducerConfig, _> = toml::from_str(bad);
        assert!(res.is_err(), "expected parse failure for unknown field");
    }

    #[test]
    fn parser_rejects_unknown_nested_keys() {
        let bad = r#"
[producer]
service_name = "x"
stack = "rust"
[producer.endpoint]
env_var = "FLUXX_OTEL_ENDPOINT"
prod_value = "http://localhost:4318"
nope = "typo"
[producer.exports]
traces = true
metrics = false
logs = false
"#;
        let res: Result<ProducerConfig, _> = toml::from_str(bad);
        assert!(res.is_err());
    }

    // ── Stack <-> string round-trip ──────────────────────────────────────

    #[test]
    fn stack_string_roundtrips() {
        // Every variant must round-trip through (as_toml_str, from_toml_str).
        // Forces newcomers to add the from_toml_str branch when they
        // add a Stack variant.
        for s in [
            Stack::PythonFastApi,
            Stack::PythonFlask,
            Stack::PythonDjango,
            Stack::PythonGeneric,
            Stack::NodeTypescript,
            Stack::NodeJavascript,
            Stack::Go,
            Stack::Rust,
            Stack::JvmMaven,
            Stack::JvmGradle,
            Stack::DotnetCsharp,
            Stack::RubyRails,
            Stack::RubyGeneric,
            Stack::PhpLaravel,
            Stack::PhpGeneric,
            Stack::Unknown,
        ] {
            let back = Stack::from_toml_str(s.as_toml_str())
                .expect(&format!("missing from_toml_str for {:?}", s));
            assert_eq!(back, s);
        }
    }

    // ── next_steps_for snippet integrity ─────────────────────────────────

    #[test]
    fn next_steps_python_fastapi_mentions_the_right_packages() {
        // Pin the SDK package names per stack. The bug this guards
        // against: someone edits one snippet and accidentally pastes
        // the JS snippet content into the Python branch.
        let s = next_steps_for(Stack::PythonFastApi, "foo");
        assert!(s.contains("opentelemetry-instrumentation-fastapi"));
        assert!(s.contains("OTLPSpanExporter"));
        assert!(s.contains("foo")); // service_name interpolated
        assert!(s.contains("fluxx verify foo")); // verify hint
    }

    #[test]
    fn next_steps_node_uses_proto_exporter_not_http() {
        // The proto vs http distinction is the one that bit us during
        // bring-up. The snippet must recommend -proto.
        let s = next_steps_for(Stack::NodeTypescript, "bar");
        assert!(s.contains("@opentelemetry/exporter-trace-otlp-proto"));
        assert!(!s.contains("exporter-trace-otlp-http\""));
    }

    #[test]
    fn next_steps_go_mentions_otlptracehttp() {
        let s = next_steps_for(Stack::Go, "baz");
        assert!(s.contains("otlptracehttp"));
        assert!(s.contains("otelhttp"));
    }

    #[test]
    fn next_steps_rust_mentions_opentelemetry_otlp() {
        let s = next_steps_for(Stack::Rust, "x");
        assert!(s.contains("opentelemetry-otlp"));
        assert!(s.contains("with_http"));
    }

    #[test]
    fn next_steps_jvm_mentions_javaagent_jar() {
        let s = next_steps_for(Stack::JvmMaven, "x");
        assert!(s.contains("opentelemetry-javaagent.jar"));
        assert!(s.contains("http/protobuf"));
    }

    #[test]
    fn next_steps_dotnet_mentions_aspnet_core() {
        let s = next_steps_for(Stack::DotnetCsharp, "x");
        assert!(s.contains("AspNetCore"));
        assert!(s.contains("OtlpExportProtocol.HttpProtobuf"));
    }

    #[test]
    fn next_steps_unknown_is_actionable() {
        let s = next_steps_for(Stack::Unknown, "x");
        assert!(s.contains("/v1/traces"));
        assert!(s.contains("OTEL_SERVICE_NAME"));
    }

    #[test]
    fn next_steps_always_appends_verify_hint() {
        // Every stack should end with the `fluxx verify <name>` reminder
        // — that's the last-mile success signal we want producers to
        // converge on, no matter their stack.
        for stack in [
            Stack::PythonFastApi,
            Stack::NodeTypescript,
            Stack::Go,
            Stack::Rust,
            Stack::Unknown,
        ] {
            let s = next_steps_for(stack, "svc");
            assert!(s.contains("fluxx verify svc"), "missing verify hint for {:?}", stack);
        }
    }

    // ── read_existing + integration via TempDir ──────────────────────────

    #[test]
    fn read_existing_returns_none_when_no_file() {
        let td = TempDir::new().unwrap();
        let r = read_existing(td.path()).unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn read_existing_parses_a_valid_file() {
        let td = TempDir::new().unwrap();
        let cfg = ProducerConfig::default_for("foo", Stack::Rust);
        write_file(td.path(), "fluxx.toml", &render_toml(&cfg));
        let parsed = read_existing(td.path()).unwrap().unwrap();
        assert_eq!(parsed.producer.service_name, "foo");
        assert_eq!(parsed.producer.stack, "rust");
    }

    #[test]
    fn read_existing_errors_on_malformed_file() {
        // Bad TOML must surface as an Err, not a silent default —
        // the operator needs to know their file is broken.
        let td = TempDir::new().unwrap();
        write_file(td.path(), "fluxx.toml", "this is not toml = = =\n");
        let r = read_existing(td.path());
        assert!(r.is_err(), "expected error for malformed fluxx.toml");
    }
}
