# Deploy

Three ways to run fluxx in production-ish environments. Pick one.

| Path | When | What you get |
|---|---|---|
| **k8s/** | You have a cluster | Plain manifests — `kubectl apply -k k8s/` |
| **helm/fluxx-gateway/** | You want values overrides | `helm install fluxx ./helm/fluxx-gateway` |
| **grafana/** | You already have Grafana | A starter dashboard JSON to import |

All three boot the gateway with the in-memory backend by default (the laptop-friendly mode). To turn on DuckDB persistence, the OTel forwarder, ClickHouse, etc., edit the ConfigMap or `values.yaml` — see [`../fluxx-core/src/config/mod.rs`](../fluxx-core/src/config/mod.rs) for the full schema.

## Quickest possible start (kustomize)

```bash
kubectl create namespace fluxx
kubectl apply -k deploy/k8s/
kubectl -n fluxx port-forward svc/fluxx-gateway 4318:4318 7007:7007
```

Then point an OTel SDK at `http://localhost:4318` and the dashboard at `http://localhost:7007`.

## Image

The manifests reference `ghcr.io/shregar1/fluxx-gateway:latest`. Override with `--set image.repository=...` for Helm or edit `deployment.yaml` for kustomize. A `Dockerfile` for the gateway is **not** included in this PR — build pipeline is a follow-up.

## Storage

These manifests use `emptyDir` for `/var/lib/fluxx` — ephemeral, fine for the in-memory backend. For DuckDB persistence flip the volume to a `PersistentVolumeClaim`; the Helm chart has a `persistence.enabled` toggle for this.

## Grafana dashboard

`grafana/fluxx-overview.json` shows ingest rate, error rate, and latency p50/p95/p99 *of the gateway itself* — fluxx scraping its own `/v1/stats`. Import via Grafana UI → Dashboards → New → Import → upload JSON.
