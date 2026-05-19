# Build-time env vars

These are set when the binary is **built** (read at compile time, baked
into the binary). Changing them requires a rebuild.

> Why baked at build time, not read at runtime?
> [`ARCHITECTURE.md`](./ARCHITECTURE.md#trust-anchor-auth_url-is-baked-at-build-time).

## Required

| Var | Description |
|---|---|
| `AUTH_URL` | Full auth-server endpoint URL (including `/v1/keys/verify`) the binary will POST to. No default — build fails if unset. |

```bash
AUTH_URL=https://auth.<env>.vexarr.com/v1/keys/verify cargo build --release
```

## Optional

_None._ The build has exactly one knob.
