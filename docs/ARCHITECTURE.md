# Architecture

The gateway is a single Rust binary that plays three roles:

1. **Trust broker** between the per-node dashboard and the upstream
   auth server. The dashboard never talks to auth directly.
2. **Ingest agent** that accepts every observability signal the
   industry has standardised on — OTLP traces/metrics/logs at canonical
   paths, plus fluxx-specific signals like RUM and profiling.
3. **Dashboard host** — the React dashboard is embedded into the
   binary and served from the same port.

```
                ┌─────────────────────────────────────────────────────────────┐
                │                       Operator's box                        │
                │                                                             │
                │   ┌──────────────┐         ┌──────────────────────────┐    │
                │   │   Browser    │ ───────▶│  fluxx binary (port 7007) │    │
                │   │ (dashboard)  │  HTTP   │                          │    │
                │   └──────────────┘         │  ┌─────────────────────┐ │    │
                │                            │  │ Auth middleware     │ │    │
                │   ┌──────────────┐         │  │ /v1/auth/verify     │ │    │
                │   │  Your apps   │ ───────▶│  │ /v1/traces …        │ │    │
                │   │  (OTel SDK)  │  OTLP   │  └──────────┬──────────┘ │    │
                │   └──────────────┘         │             │            │    │
                │                            │  ┌──────────▼──────────┐ │    │
                │   ┌──────────────┐         │  │ Storage (events,    │ │    │
                │   │  Embedded    │ ◀───────│  │ logs, metrics, RUM, │ │    │
                │   │  dashboard   │  HTTP   │  │ traces, profiles…)  │ │    │
                │   │  (RustEmbed) │         │  └─────────────────────┘ │    │
                │   └──────────────┘         └────────────┬─────────────┘    │
                │                                         │                  │
                └─────────────────────────────────────────┼──────────────────┘
                                                          │ bearer-auth
                                                          ▼
                                          ┌───────────────────────────────────┐
                                          │      Upstream auth server         │
                                          │   AUTH_URL (compile-time const)   │
                                          │   POST /v1/keys/verify { api_key }│
                                          └───────────────────────────────────┘
```

## Trust anchor: `AUTH_URL` is baked at build time

The upstream auth-server URL is captured at compile time via
`env!("AUTH_URL")`. It is **not** readable from the process env at
runtime. Why:

- An attacker (or a misconfigured ops engineer) who can flip a runtime
  env var could redirect the gateway to a fake auth server that
  returns "valid" for every key, silently bypassing real validation.
- Baking it into the binary means the URL travels with the artifact —
  what CI built is what production calls.
- The build deliberately fails (loudly, with a copy-pasteable
  cargo command) if `AUTH_URL` is unset. There is **no default**.

See [`COMPANY_ENV.md`](./COMPANY_ENV.md).

## One operator secret: `FLUXX_API_KEY`

A single runtime env var does triple duty:

1. **Edge-auth bearer** for `/v1/*` (verified against a persisted
   `sha256(token)` hash in `/var/lib/fluxx/admin.token.sha256`).
2. **Gateway's own bearer** when it forwards `/v1/auth/verify` calls
   to the upstream auth server.
3. **Defense-in-depth check** — the gateway constant-time compares the
   dashboard-submitted key against `FLUXX_API_KEY` **before** forwarding
   to the upstream auth server. This short-circuits obviously-wrong
   keys and removes the footgun of "any key the operator pastes is
   forwarded as-is."

Without `FLUXX_API_KEY`, the gateway runs in **passthrough mode**: any
non-empty key the dashboard pastes is accepted, and a warning is
logged. Useful for offline dev; not for production. See
[`USER_ENV.md`](./USER_ENV.md#required).

## Boot flow

```
cargo build (--release)
  └─ env!("AUTH_URL")             ← required at build time, baked or BUILD FAILS

./fluxx start
  ├─ Config::load()                ← optional config/default.toml + APP_* env overrides
  │
  ├─ init_admin_token()
  │     ├─ FLUXX_API_KEY set?    → use as edge-auth bearer; outcome=FromEnv
  │     ├─ token file exists?    → load persisted hash;     outcome=Loaded
  │     └─ else                  → mint a fresh token, print ONCE, persist its hash
  │
  ├─ Auth middleware mounts on /v1/*
  │     ├─ /k/<KEY>/...           → strip prefix, extract <KEY> as bearer
  │     ├─ Public paths           → through  (/, /v1/status, /v1/auth/verify, assets)
  │     ├─ Token via header/path  → validate (Bearer, Basic, X-API-Key, /k/)
  │     │     ├─ valid            → through
  │     │     └─ invalid          → 401     (never silently fall through on bad token)
  │     └─ No token + ingest path → through unless FLUXX_REQUIRE_INGEST_AUTH set
  │
  ├─ POST /v1/auth/verify  { api_key }     (the dashboard's sign-in call)
  │     ├─ AUTH_URL non-empty AND FLUXX_API_KEY set?
  │     │     ├─ Defense-in-depth: constant-time compare api_key == FLUXX_API_KEY
  │     │     └─ Forward to AUTH_URL with FLUXX_API_KEY as bearer
  │     └─ Else → passthrough (warns, accepts any non-empty key)
  │
  └─ Embedded dashboard mounted (RustEmbed at /)
```

## Wire shape

Every API endpoint returns the same envelope, regardless of success or
failure. Callers branch on `responseKey`, **not** HTTP status, because
the gateway always returns HTTP 200 with the envelope.

```jsonc
{
  "transactionUrn": "urn:fluxx:txn:abc123",
  "status": "SUCCESS" | "FAILED",
  "responseKey": "OK" | "INVALID_API_KEY" | "AUTH_UNAVAILABLE" | …,
  "responseMessage": "API key verified.",
  "errors": [],                       // list of {message, …}; empty on success
  "timestamp": 1700000000000,
  "metadata": {},
  "data": { … },                      // endpoint-specific payload
  "reference_urn": "urn:…"            // ONLY snake_case field; everything else camelCase
}
```

Wire-shape source of truth: `fluxx-api/src/response.rs`.

## Ingest paths

Three groups:

1. **OTel-canonical** — every modern observability tool already knows
   how to talk to these:
   - `POST /v1/traces` (OTLP HTTP/JSON)
   - `POST /v1/metrics` (OTLP HTTP/JSON)
   - `POST /v1/logs` (OTLP HTTP/JSON **or** fluxx-native LogEntry array
     — dispatcher branches on body shape)
2. **Fluxx-native** — for signals OTLP doesn't cover:
   - `/v1/rum`, `/v1/dbm/queries`, `/v1/ci/pipelines`,
     `/v1/network/{flows,dns}`, `/v1/profiling`, `/v1/cspm/findings`,
     `/v1/changes`, `/v1/annotations`, `/v1/incidents`, `/v1/slos`,
     `/v1/notebooks`, `/v1/prom/write`
3. **Legacy aliases** — kept so old deployments keep working:
   - `/v1/otlp/traces`, `/v1/otlp/metrics`, `/v1/otlp/logs`
   - `/v1/logs/raw` (strict native-only, no OTLP branching)

Full table + curl examples: [`INGEST.md`](./INGEST.md).

## URL-based auth

Standard HTTP, no proprietary tokens. Four mechanisms in precedence
order; pick whichever the client supports:

```
/k/<KEY>/v1/traces                            ← path-prefix (URL-only)
Authorization: Bearer <KEY>                   ← OTel SDK default
Authorization: Basic base64(<KEY>:)           ← what URL userinfo becomes
X-API-Key: <KEY>                              ← convenience header
```

The URL forms — `https://<KEY>@host/v1/traces` and `/k/<KEY>/v1/...` —
exist so a tool that can only configure a single static URL string
(embedded devices, browser beacons, simple shell scripts) can still
authenticate. See [`INGEST.md`](./INGEST.md#auth) for trade-offs.

## What runs where

| Component | Code path | Notes |
|---|---|---|
| Auth middleware | `fluxx-api/src/server.rs::auth_middleware` | Mounted on every `/v1/*` route. |
| Auth broker | `fluxx-api/src/auth.rs` | Implements `POST /v1/auth/verify`. |
| Ingest dispatcher | `fluxx-api/src/ingest.rs` | Content-negotiates `/v1/logs` between OTLP and native shapes. |
| OTLP handlers | `fluxx-api/src/otlp_http_handlers.rs` | Inner `write_*` helpers reused by the dispatcher. |
| Edge-auth tokens | `fluxx-core/src/security/bootstrap_token.rs` | sha256-hashed token persistence. |
| Storage | `fluxx-core/src/storage/*` | Event, log, metric, trace stores. |
| Dashboard | `node.dashboard.fluxx.vexarr.com` (separate repo, bundled via RustEmbed) | Talks **only** to the gateway, never the auth server directly. |

## Workspace layout

```
gateway.fluxx.vexarr.com/
├── Cargo.toml              # workspace
├── fluxx-core/             # lib: storage, security, mesh, integrations…
├── fluxx-api/              # lib: HTTP routes, OTLP handlers, dispatcher
├── fluxx-bin/              # bin: composes core + api into the `fluxx` binary
├── docs/                   # this directory
├── .env                    # build-time AUTH_URL
└── .env.user               # runtime-env documentation
```

The three-crate split exists to break a cyclic dep: `fluxx-core` is the
storage/security library, `fluxx-api` depends on `fluxx-core`, and
`fluxx-bin` depends on both to compose them into a binary. Each crate
can be built and tested in isolation.
