# cdc-connector

A sidecar that subscribes to TiKV Change Data Capture (CDC) via TiCDC's
`logservice/logpuller` and forwards events to SurrealDB's `/cdc/ingest`
endpoint.

This replaces the previous in-process CDC consumer in SurrealDB (forked
`tikv-client` subscriber) which silently dropped `NotLeader` /
`BatchSplit` events during normal TiKV region movement.

See `docs/architecture/cdc-via-ticdc.md` in the evo-design repo for the
full design and rationale.

## Deployment

### Sidecar (recommended)

Two containers in the same Kubernetes pod:

```yaml
spec:
  containers:
    - name: surrealdb
      image: surrealdb/surrealdb:<your-tag>
      env:
        # Enable the cdc_ingest route (optional: allow-http defaults to all).
        - name: SURREAL_CAPS_ALLOW_HTTP
          value: "cdc_ingest,rpc,health,sql,import,export,version"
      # ... your existing config

    - name: cdc-connector
      image: <your-registry>/cdc-connector:<tag>
      env:
        - name: CDC_PD_ENDPOINTS
          value: "pd-0.pd:2379,pd-1.pd:2379,pd-2.pd:2379"
        - name: CDC_SURREAL_INGEST_URL
          value: "http://localhost:8000/cdc/ingest"
        - name: CDC_SURREAL_USERNAME
          valueFrom:
            secretKeyRef:
              name: surrealdb-root
              key: username
        - name: CDC_SURREAL_PASSWORD
          valueFrom:
            secretKeyRef:
              name: surrealdb-root
              key: password
```

The connector uses `localhost` because sidecar containers share a network
namespace.

### Standalone pod or separate deployment

The connector is location-independent: any reachable SurrealDB URL works.
Just change `CDC_SURREAL_INGEST_URL` to the appropriate service URL.

## Configuration

| Env var | Required | Default | Description |
|---|---|---|---|
| `CDC_PD_ENDPOINTS` | yes | — | Comma-separated PD addresses, e.g. `pd-0:2379,pd-1:2379`. |
| `CDC_SURREAL_INGEST_URL` | yes | — | Full URL of SurrealDB's `/cdc/ingest`. |
| `CDC_SURREAL_USERNAME` | yes | — | SurrealDB root username. |
| `CDC_SURREAL_PASSWORD` | yes | — | SurrealDB root password. |
| `CDC_START_KEY` | no | `""` | TiKV key range start. Empty = from beginning. |
| `CDC_END_KEY` | no | `""` | TiKV key range end. Empty = until end. |
| `CDC_HEARTBEAT_INTERVAL` | no | `5s` | Go duration string. |
| `CDC_RECONNECT_INITIAL_BACKOFF` | no | `1s` | Initial backoff on HTTP drop. |
| `CDC_RECONNECT_MAX_BACKOFF` | no | `60s` | Max backoff cap. |
| `CDC_REGION_REQUEST_WORKERS` | no | `1` | TiKV region request workers per store. |

## Auth

The ingest endpoint requires a **root-level** SurrealDB session. The
connector authenticates via HTTP Basic auth on every request.

In production you may prefer a dedicated service account at root level
(e.g. `cdc_connector`) rather than reusing a human root credential. Create
it with SurrealQL:

```sql
DEFINE USER cdc_connector ON ROOT PASSWORD '<random>' ROLES OWNER;
```

Then set `CDC_SURREAL_USERNAME=cdc_connector` and provision the password
via whatever secret-management mechanism the deployment uses.

Non-root sessions are rejected at the handler level — CDC ingest is a
system-level backplane, not a user-facing write path.

## Enabling the route in SurrealDB

The `/cdc/ingest` endpoint is gated by SurrealDB's HTTP route capabilities.
By default all routes are allowed; if you've explicitly restricted routes,
add `cdc_ingest` to your allowlist:

```bash
surreal start --allow-http cdc_ingest,rpc,health,sql,import,export ...
# or
SURREAL_CAPS_ALLOW_HTTP=cdc_ingest,rpc,health,sql,import,export surreal start ...
```

## Build

```bash
docker build -t cdc-connector .
```

Or locally:

```bash
go build -o cdc-connector ./cmd/cdc-connector
```

Requires Go 1.25+ (TiCDC's minimum).

## Operational notes

- **Backpressure**: the connector has a bounded in-memory event channel
  (4096 frames). When SurrealDB is slow the channel fills and events are
  dropped (counted in logs). The logpuller also applies its own
  backpressure upstream.
- **Reconnects**: HTTP stream failures trigger exponential backoff
  (`1s → 60s`). TiKV region transitions (`NotLeader`, `EpochNotMatch`,
  `RegionNotFound`) are handled transparently by the logpuller.
- **Resumption**: the connector does not currently persist its
  `resolved_ts` watermark across restarts. On restart it subscribes from
  the current cluster timestamp, which may cause a brief gap in live query
  notifications during a restart. Persistent resumption is future work.
- **Observability**: connector emits structured JSON logs via `zap`. Each
  container's logs are independent in Argo CD / kubectl.
