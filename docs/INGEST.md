# Ingestion API — the canonical paths

Fluxx exposes the **same paths every OTel-compatible tool already knows
how to send to**. Any SDK that talks `OTEL_EXPORTER_OTLP_ENDPOINT`
works against fluxx with **zero code changes** — just point its
endpoint at your gateway.

| Signal | Canonical path | Wire format |
|---|---|---|
| Traces | `POST /v1/traces` | OTLP HTTP/JSON |
| Metrics | `POST /v1/metrics` | OTLP HTTP/JSON |
| Logs | `POST /v1/logs` | OTLP HTTP/JSON **or** fluxx-native LogEntry array (auto-detected) |

This is the same shape OTel, Datadog Agent's OTLP exporter, Grafana
Alloy, and the Splunk OTel collector all converged on.

## Configure any OTel SDK

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://your-gateway:7007
export OTEL_EXPORTER_OTLP_PROTOCOL=http/json
# (auth) export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer $FLUXX_API_KEY"
```

That's it. The SDK will POST to `/v1/traces`, `/v1/metrics`, `/v1/logs`
automatically.

## Examples

**OTLP traces** (every OTel SDK):

```bash
curl -X POST http://localhost:7007/v1/traces \
  -H "Content-Type: application/json" \
  -d '{
    "resourceSpans": [{
      "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "checkout-api"}}]},
      "scopeSpans": [{
        "scope": {"name": "demo"},
        "spans": [{
          "traceId": "5b8aa5a2d2c872e8321cf37308d69df2",
          "spanId":  "5fb397be34d26b51",
          "name": "GET /cart",
          "kind": 2,
          "startTimeUnixNano": "1700000000000000000",
          "endTimeUnixNano":   "1700000000100000000"
        }]
      }]
    }]
  }'
```

**OTLP logs** (any OTel exporter):

```bash
curl -X POST http://localhost:7007/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [{
      "resource": {"attributes": []},
      "scopeLogs": [{
        "scope": {"name": "demo"},
        "logRecords": [{
          "timeUnixNano": "1700000000000000000",
          "severityNumber": 9,
          "severityText": "INFO",
          "body": {"stringValue": "hello world"}
        }]
      }]
    }]
  }'
```

**Fluxx-native logs** (same URL, just an array):

```bash
curl -X POST http://localhost:7007/v1/logs \
  -H "Content-Type: application/json" \
  -d '[{
    "id": "1", "timestamp_us": 1700000000000000, "level": "info",
    "message": "hello world", "service": "checkout-api"
  }]'
```

The dispatcher branches on body shape — object with `resourceLogs` →
OTLP, top-level array → fluxx-native. Both write through the same log
store and surface in the same dashboard panels.

## Fluxx-native paths (when you want the explicit contract)

These exist for callers that want the strict shape and don't want the
content-sniffing layer:

| Path | Body |
|---|---|
| `POST /v1/logs/raw` | `Vec<LogEntry>` only (no OTLP branching) |
| `POST /v1/otlp/traces` | OTLP traces only — legacy alias for `/v1/traces` |
| `POST /v1/otlp/metrics` | OTLP metrics only — legacy alias for `/v1/metrics` |
| `POST /v1/otlp/logs` | OTLP logs only — legacy alias for `/v1/logs` |

The legacy `/v1/otlp/*` paths are kept so deployments that point at the
old URLs keep working; new deployments should use the canonical paths.

## Beyond OTLP

Endpoints for signals OTLP doesn't cover are fluxx-specific and stay on
their dedicated paths:

| Signal | Path |
|---|---|
| RUM (browser sessions / vitals / actions / errors) | `POST /v1/rum` |
| Slow database queries | `POST /v1/dbm/queries` |
| CI pipeline runs | `POST /v1/ci/pipelines` |
| Network flows / DNS | `POST /v1/network/flows` / `POST /v1/network/dns` |
| Profiling | `POST /v1/profiling` |
| CSPM findings | `POST /v1/cspm/findings` |
| Changes / annotations | `POST /v1/changes` / `POST /v1/annotations` |
| Incidents / SLOs / Notebooks | `POST /v1/incidents` / `POST /v1/slos` / `POST /v1/notebooks` |
| Prometheus remote-write | `POST /v1/prom/write` |

## Auth

The same `FLUXX_API_KEY` that authenticates the dashboard is also the
bearer for every ingest endpoint. The gateway accepts the key through
**four** standard HTTP mechanisms — pick whichever fits your client.

### 1. Bearer header *(recommended)*

```
Authorization: Bearer <FLUXX_API_KEY>
```

This is what every OTel SDK produces when you set:

```bash
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer $FLUXX_API_KEY"
```

### 2. `X-API-Key` header

```
X-API-Key: <FLUXX_API_KEY>
```

A convenience for tools that don't expose the `Authorization` header
explicitly.

### 3. URL userinfo *(URL-only auth)*

For clients that can't set headers but accept a configurable URL.
Standard HTTP `userinfo` syntax — every HTTP library converts it to
`Authorization: Basic base64(<KEY>:)` automatically:

```
https://<FLUXX_API_KEY>@gateway.example.com/v1/traces
```

curl example:

```bash
curl -X POST "https://flx_live_abc123@gateway.example.com/v1/traces" \
  -H "Content-Type: application/json" -d @trace.json
```

Some libraries put secrets in the password slot — that works too:

```
https://:<FLUXX_API_KEY>@gateway.example.com/v1/traces
```

### 4. Path-prefix *(for clients that can't even do userinfo)*

A last-resort escape hatch for embedded devices and shell scripts that
can only configure a static URL with no auth parsing. Embed the key
directly in the path:

```
POST /k/<FLUXX_API_KEY>/v1/traces
POST /k/<FLUXX_API_KEY>/v1/logs?level=error&service=foo
```

The gateway strips `/k/<KEY>/` before routing, so the handlers see
`/v1/traces` and `/v1/logs?...` as normal.

> ⚠️ Path-prefix exposes the key in proxy access logs and browser
> history. Use header or userinfo auth where you can; reach for
> path-prefix only when the client truly cannot do either.

### Precedence

When more than one mechanism is present on the same request, the
gateway picks in this order:

1. `/k/<KEY>/...` path-prefix
2. `Authorization: Bearer <KEY>`
3. `Authorization: Basic base64(<KEY>:)` (userinfo)
4. `X-API-Key: <KEY>`

A request with a **wrong** key always fails 401 — submitting any key at
all is intent to authenticate, so we never silently fall back to the
anonymous-ingest path.

### Ingest paths are unauthenticated by default

`/v1/rum`, `/v1/logs`, `/v1/events`, `/v1/otlp/*`, `/v1/prom/write`
accept anonymous POSTs by default because browser RUM has no
server-side secret. Flip on `FLUXX_REQUIRE_INGEST_AUTH=true` to require
a valid key on every ingest, using any of the four mechanisms above.
