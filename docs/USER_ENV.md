# Runtime env vars

These are set when the binary is **run**. Set what you need, leave the
rest alone — every optional var has a default.

> See [`COMPANY_ENV.md`](./COMPANY_ENV.md) for the build-time knob
> (`AUTH_URL`). See [`INGEST.md`](./INGEST.md) for telemetry endpoint
> paths and auth examples. See [`ARCHITECTURE.md`](./ARCHITECTURE.md)
> for the broader picture.

## Required

| Var | Description |
|---|---|
| `FLUXX_API_KEY` | Your auth-server PAT. Does triple duty: edge-auth bearer for `/v1/*`, gateway's bearer when forwarding to the upstream auth server, and defense-in-depth check on dashboard-submitted keys. Without it, the gateway runs in **passthrough mode** and warns on every verify. |

```bash
export FLUXX_API_KEY=flx_live_…
./fluxx start
```

## Optional

### Auth / security

| Var | Default | Description |
|---|---|---|
| `FLUXX_API_KEY_FILE` | `/var/lib/fluxx/admin.token.sha256` | Where the sha256(token) hash is persisted between boots. |
| `FLUXX_REQUIRE_INGEST_AUTH` | unset (anonymous ingest allowed) | If set to any value, ingest endpoints (`/v1/rum`, `/v1/logs`, `/v1/events`, `/v1/otlp/*`, `/v1/prom/write`, the OTel canonical paths) require a valid `FLUXX_API_KEY` via any of the four auth mechanisms (Bearer, Basic/userinfo, X-API-Key, `/k/<KEY>/` path-prefix). See [`INGEST.md`](./INGEST.md#auth). |
| `FLUXX_CORS_ORIGIN` | unset (same-origin only) | CSV of allowed browser-RUM origins. |
| `FLUXX_CORS_ALLOW_ANY` | unset | Must be `true` to allow `FLUXX_CORS_ORIGIN=*`. |

### Server

| Var | Default | Description |
|---|---|---|
| `APP_SERVER__HOST` | `127.0.0.1` | Bind host. Use `0.0.0.0` for external listeners. |
| `APP_SERVER__PORT` | `7007` | Bind port. |

> The server reads `config/default.toml` if present (optional) and
> lets `APP_*` env vars override individual keys (`__` as nested
> separator).

### Agent identity

| Var | Default | Description |
|---|---|---|
| `FLUXX_AGENT_ID` | random UUID at first boot | Stable agent id reported to the legacy cloud forwarder. |
| `HOSTNAME` | `uname()` | Fallback agent id when `FLUXX_AGENT_ID` isn't set. |
| `FLUXX_SOURCEMAP_DIR` | built-in | Where uploaded sourcemaps live on disk. |
| `FLUXX_AGENT_URL` | `http://127.0.0.1:7007` | Used by `fluxx release sourcemaps` / `fluxx errors` CLI subcommands to reach a running agent. |

### Legacy cloud forwarder (only if you're paying for hosted Fluxx)

Off by default. You almost never need to touch these — the gateway
auto-detects self-hosted mode when `AUTH_URL` was baked non-empty or
`FLUXX_API_URL` is unset.

| Var | Default | Description |
|---|---|---|
| `FLUXX_API_URL` | unset → self-hosted | Hosted-Fluxx control plane. |
| `FLUXX_FORWARD_TO_CLOUD` | unset | If `true`, mirror events to the cloud. |
| `FLUXX_SELF_HOSTED` | unset | If `true`/`1`/`yes`/`on`, explicitly skip the legacy license check. |
| `FLUXX_OFFLINE_MODE` | unset | Accept offline license-OK result. |

### Mesh / clustering

All unset by default — mesh is opt-in.

| Var | Default | Description |
|---|---|---|
| `FLUXX_MESH_CONFIG` | unset | Path to mesh config file. |
| `FLUXX_NODE_ID` | unset | Stable node id in the mesh. |
| `FLUXX_MESH_BIND` | unset | UDP gossip bind addr. |
| `FLUXX_MESH_PEERS` | unset | CSV seed peers. |
| `FLUXX_MESH_FEDERATION` | unset | Enable cross-cluster federation. |
| `FLUXX_MESH_QUERY_TIMEOUT` | unset | Per-query timeout (seconds). |

### Integration probes

| Var | Default | Description |
|---|---|---|
| `FLUXX_INTEGRATIONS_POSTGRES` | unset | CSV of `postgres://…` URIs to probe. |
| `FLUXX_INTEGRATIONS_REDIS` | unset | CSV of `host:port`. |
| `FLUXX_INTEGRATIONS_DOCKER` | unset | `true` to probe local Docker. |
| `FLUXX_INTEGRATIONS_HTTP_PROBES` | unset | CSV of URLs. |
| `FLUXX_INTEGRATIONS_INTERVAL_SECS` | `30` | Probe interval. |

### Alert sinks

| Var | Default | Description |
|---|---|---|
| `FLUXX_WEBHOOK_URL` | unset | Outbound webhook sink. |
| `FLUXX_WEBHOOK_SECRET` | unset | HMAC-SHA256 signing key for outbound webhooks. |
| `SLACK_WEBHOOK_URL` | unset | Slack incoming webhook URL. |
| `SLACK_CHANNEL` | unset | Channel override. |
| `SLACK_USERNAME` | `Fluxx` | Bot display name. |
| `SLACK_ICON` | `:warning:` | Bot icon. |
| `SMTP_HOST` / `SMTP_USER` / `SMTP_PASS` | unset | All three required for email alerts. |
| `SMTP_PORT` | `587` | SMTP server port. |
| `SMTP_USE_TLS` | `true` | Use STARTTLS. |
| `ALERT_EMAIL_FROM` | unset | From address. |
| `ALERT_EMAIL_TO` | unset | CSV recipients. |
| `ALERT_WEBHOOK_URL` | unset | Generic alert webhook. |
| `ALERT_WEBHOOK_METHOD` | `POST` | HTTP method. |

### AI-assisted diagnostics

| Var | Default | Description |
|---|---|---|
| `OPENAI_API_KEY` | unset | Enable OpenAI provider. |
| `ANTHROPIC_API_KEY` | unset | Enable Anthropic provider (used if OpenAI not set). |
| `AI_MODEL` | provider default | Model override. |

### Ticketing

| Var | Default | Description |
|---|---|---|
| `JIRA_HOST` / `JIRA_USER` / `JIRA_TOKEN` | unset | All three required for Jira integration. |
| `JIRA_PROJECT` | `OPS` | Project key. |
| `JIRA_ISSUE_TYPE` | `Bug` | Issue type. |
| `JIRA_AUTO_CREATE` | unset | If `true`, auto-file tickets. |
| `GITHUB_TOKEN` / `GITHUB_OWNER` / `GITHUB_REPO` | unset | All three required for GitHub integration. |
| `GITHUB_AUTO_CREATE` | unset | If `true`, auto-file issues. |
