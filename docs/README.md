# Fluxx Gateway — Documentation

The fluxx gateway is a self-hosted observability agent that

1. Brokers API-key verification between the per-node dashboard and the
   upstream auth server.
2. Accepts OTel-compatible telemetry on the canonical paths
   (`/v1/{traces,metrics,logs}`) plus fluxx-specific signals like RUM,
   profiling, CSPM, and CI pipelines.
3. Serves the embedded React dashboard from the same binary.

## Index

| Doc | Use when… |
|---|---|
| [`QUICKSTART.md`](./QUICKSTART.md) | You want to build the binary, start it, and send the first event. |
| [`ARCHITECTURE.md`](./ARCHITECTURE.md) | You're integrating fluxx into a system and need to understand how it fits — boot flow, trust anchors, what talks to what. |
| [`COMPANY_ENV.md`](./COMPANY_ENV.md) | You're building the binary (CI / release pipeline). One env var. |
| [`USER_ENV.md`](./USER_ENV.md) | You're deploying or running the binary. One env var to set, plus a long list of optional knobs. |
| [`INGEST.md`](./INGEST.md) | You're pointing an SDK or agent at fluxx — covers every ingest path, all four URL/header auth mechanisms, and curl examples. |

## TL;DR

**Build** (`docs/COMPANY_ENV.md`):

```bash
AUTH_URL=https://auth.vexarr.com/v1/keys/verify cargo build --release
```

**Run** (`docs/USER_ENV.md`):

```bash
export FLUXX_API_KEY=flx_live_…
./fluxx start
```

**Ingest** (`docs/INGEST.md`):

```bash
# point any OTel SDK at the gateway, no per-vendor path tweaks
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:7007
export OTEL_EXPORTER_OTLP_PROTOCOL=http/json
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer $FLUXX_API_KEY"
```

**Dashboard:** open `http://localhost:7007/` and paste the same
`FLUXX_API_KEY` into the sign-in prompt.
