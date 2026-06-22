# Debugging & fault injection

Artemis can inject artificial packet loss so you can exercise the loss metrics,
alerting, and the UI without waiting for a real fault. These endpoints are
**dev-mode only** and are disabled by default.

## Enabling dev mode

Start the manager with `--dev` (or `DEV=true`):

```shell
connqc manager --http-addr :8080 --grpc-addr :8443 --dev
```

In dev mode the manager logs `Dev mode enabled — debug/fault-injection endpoints
active` and exposes the debug routes below. Without `--dev` they return 404.

## How it works

The manager sends a `Fault` message down the target agent's gRPC stream. The
agent's **echo listener then silently drops the requested fraction of incoming
probes** for the given duration. Every peer probing that agent therefore observes
packet loss — exactly as if the network were dropping packets — which flows through
the normal pipeline: loss metrics → ClickHouse samples → `packet_loss` /
`blackout` alerts → SSE → the topology graph turns red.

The fault clears automatically after the duration (the agent resets its drop rate
to 0 and logs `Fault injection cleared`).

## Triggering packet loss

### From the UI

1. Open the **Agents** page (`/agents`). In dev mode each agent row gains an
   **inject loss** button.
2. Click it, enter the loss percentage (e.g. `80`) and a duration in seconds.
3. Watch the **Dashboard** — edges *into* that agent go amber/red, and a
   `packet_loss` (or `blackout` at 100%) alert appears on the **Alerts** page.

### From the API

```shell
# Make agent "a2" drop 80% of received probes for 10 seconds.
curl -XPOST localhost:8080/api/debug/inject \
  -d '{"agent":"a2","loss_percent":80,"duration_ms":10000}'
# → {"ok":true}     (404 {"error":"unknown agent"} if the name is wrong)
```

| Field | Meaning |
|---|---|
| `agent` | agent **name** whose echo listener should drop probes |
| `loss_percent` | 0–100, fraction of received probes to drop |
| `duration_ms` | how long to keep dropping (default 30000) |

`GET /api/debug/enabled` returns `200` in dev mode (used by the UI to decide
whether to show the inject controls), `404` otherwise.

### Full demo

```shell
connqc manager --dev --http-addr :8080 --grpc-addr :8443
connqc agent --manager-addr localhost:8443 --agent-name a1 --addr 127.0.0.1:19101
connqc agent --manager-addr localhost:8443 --agent-name a2 --addr 127.0.0.1:19102
# inject loss and watch the mesh react:
curl -XPOST localhost:8080/api/debug/inject -d '{"agent":"a2","loss_percent":80,"duration_ms":15000}'
curl -N localhost:8080/api/stream            # live edge + alert events
curl localhost:8080/api/alerts               # the packet_loss alert
```

## Other debugging aids

- `curl -N localhost:8080/api/stream` — live SSE feed of `{"type":"edge"|"alert"}`.
- `localhost:8080/metrics` — Prometheus gauges incl. `artemis_edge_loss_percent`.
- `localhost:8080/swagger/index.html` — interactive API docs.
- `--log.level debug` on any component for verbose per-probe / per-stream logs
  (the dropping agent logs `Dropping probe (fault injection)` at debug level).
- Clean up stray local test processes with `pkill -x connqc`.
