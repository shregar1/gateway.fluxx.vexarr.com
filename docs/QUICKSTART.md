# Quickstart

From clone to first telemetry in five minutes.

## 1. Build

The build needs **one** env var — the full auth-server endpoint URL.
It's baked into the binary as a compile-time constant so the running
process can't be redirected at a fake auth host. See
[`COMPANY_ENV.md`](./COMPANY_ENV.md) for the full rationale.

```bash
git clone <repo> gateway.fluxx.vexarr.com
cd gateway.fluxx.vexarr.com

AUTH_URL=https://auth.vexarr.com/v1/keys/verify cargo build --release
```

Forgot to set `AUTH_URL`? The build fails with a copy-pasteable
`AUTH_URL=… cargo build --release` example.

## 2. Run

Set your fluxx PAT and start the binary. That single env var is the
edge-auth bearer **and** the gateway's bearer to the upstream auth
server — one secret, one place to rotate.

```bash
export FLUXX_API_KEY=flx_live_…
./target/release/fluxx start
```

You should see:

```
🔑 Fluxx admin token (copy now — shown only once):
   flx_live_…
Auth bootstrap: FromEnv
API: Binding Fluxx layer to 0.0.0.0:7007
```

If you don't set `FLUXX_API_KEY`, the gateway runs in **passthrough
mode** (any non-empty key the dashboard pastes is accepted) and warns
loudly — fine for local dev, not for production.

## 3. Verify

The gateway is up:

```bash
curl http://localhost:7007/v1/status
# → {"status":"ok",...}
```

The dashboard is served from the same port:

```
open http://localhost:7007/
```

Paste your `FLUXX_API_KEY` into the sign-in prompt.

## 4. Send your first telemetry

Three ways, pick whichever matches your stack.

### With an OTel SDK (zero config beyond env)

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:7007
export OTEL_EXPORTER_OTLP_PROTOCOL=http/json
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer $FLUXX_API_KEY"

# now any OTel-instrumented app POSTs traces/metrics/logs to fluxx
# automatically — no per-vendor path tweaks
```

### With curl

```bash
curl -X POST http://localhost:7007/v1/traces \
  -H "Authorization: Bearer $FLUXX_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "resourceSpans": [{
      "resource": {"attributes": [{"key":"service.name","value":{"stringValue":"demo"}}]},
      "scopeSpans": [{"scope":{"name":"demo"},"spans":[{
        "traceId":"5b8aa5a2d2c872e8321cf37308d69df2",
        "spanId":"5fb397be34d26b51",
        "name":"GET /","kind":2,
        "startTimeUnixNano":"1700000000000000000",
        "endTimeUnixNano":"1700000000100000000"
      }]}]
    }]
  }'
```

### With the bundled Python test driver

The `testing/` directory in the repo ships a stdlib-only client that
exercises every dashboard panel:

```bash
cd ../testing
python3 -m fluxx.smoke              # one of every endpoint
python3 -m fluxx.load --rate 50     # 50 req/s mixed for 60s
python3 -m fluxx.stress             # 32 threads, no throttle
python3 -m fluxx.soak --duration 28800   # 8h overnight drip
```

## 5. Verify in the dashboard

Open the dashboard, sign in, and the panels should now populate:

- **RUM** — sessions, vitals, errors
- **Traces** — service map + waterfall
- **Logs** — structured + searchable
- **Errors** — fingerprinted aggregates
- **CI / DBM / NPM / Profiling / CSPM** — each on its own page

If a panel stays empty, hit it from the test driver
(`python3 -m fluxx.smoke`) — the smoke probe POSTs one valid payload to
each endpoint and prints `✔` / `✘` so you can tell whether the issue
is at ingest or display.

## Next

- Tuning runtime knobs (CORS, ingest auth, alert sinks, integrations):
  [`USER_ENV.md`](./USER_ENV.md)
- All four URL/header auth mechanisms (`/k/<KEY>/...`, userinfo, Bearer,
  X-API-Key): [`INGEST.md`](./INGEST.md#auth)
- How the gateway brokers between the dashboard and the auth server:
  [`ARCHITECTURE.md`](./ARCHITECTURE.md)
