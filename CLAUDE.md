# Artemis — Claude Code Context

Artemis is a **distributed connection-quality mesh**: a central **manager** (control
plane) orchestrates a fleet of **agents** that probe each other over TCP/UDP, stream
per-edge statistics up in real time, and surface live topology, alerting, and history.

> ⚠️ **HARD RULE:** Do **not** read, grep, or mine `sample-work/uptime-monitor`. It is
> a separate, finished project kept only as a sample. Build artemis from this repo.

---

## 1. Commands (one binary, `cmd/connqc`)

| Command | Role |
|---|---|
| `connqc manager` | Control plane: gRPC AgentLink server, registry, mesh engine, store, alerting, HTTP API + UI + `/metrics` |
| `connqc agent` | Echo listener (so peers can probe it) + probers + AgentLink client |
| `connqc server` | Standalone echo server (unchanged legacy) |
| `connqc client` | Standalone one-shot prober (unchanged legacy) + stats/Prometheus/reporter |
| `connqc hashpw <pw>` | Print a bcrypt hash for `--admin-pass-hash` |

The module path is `github.com/slothnetworks/artemis` (the binary is still `connqc`).

## 2. Layout (`internal/`-centric)

```
cmd/connqc/            # CLI: main.go (flags+commands), client.go, server.go, manager.go, agent.go, hashpw.go
internal/
  probe/               # the probe engine + wire protocol (client.go, server.go, message.go, payload.go)
    tcp/  udp/          #   transport adapters
  mtr/                 # native raw-socket ICMP MTR engine (ported from uptime-monitor with the author's approval)
  buffr/               # buffered reader (UDP single-read)
  stats/               # Collector/Event, RollingCollector (loss/percentiles/jitter), Snapshot, Prometheus, reporter
  manager/             # control-plane engine: manager.go (AgentLink), registry, store, broadcast(SSE), mesh, metrics, interceptor
  api/                 # browser HTTP/SSE/REST + UI rendering + auth (api.go, ui.go)
  agent/               # agent.go (echo + probers + gRPC link + store-and-forward hooks)
  alerting/            # engine.go (rules), silence.go, notifier.go
  store/               # storage abstraction (SampleSink/EventSink/AlertSink + Nop)
  db/postgres/         # pgx EventSink+AlertSink + migrations runner
  db/clickhouse/       # clickhouse-go SampleSink (batched) + migrations runner
  pbconv/              # stats.Snapshot <-> protobuf
  pb/artemis/v1/       # generated gRPC code
proto/artemis/v1/      # agentlink.proto
migrations/            # embed.go + postgres/*.sql + clickhouse/*.sql
web/                   # embedded UI: templates/{layouts,pages}, static/{css,js,img,js/vendor}
docker/                # Dockerfile, docker-compose.yml, prometheus.yml, grafana/
```

## 3. Data flow

```
agent prober (probe.Client) ──events──> stats.RollingCollector (per agent)
        │ flush every --flush-interval
        ▼
  gRPC AgentLink stream  ── Sample / Event / MtrSnapshot ──>  manager
        ▲                                                       │
  Assignment / RunMtr  ◀── mesh engine pushes target lists ─────┤
                                                                ▼
   manager: Store (in-mem ring) ──> ClickHouse (samples, batched)
            event/alert pipeline ──> Postgres (events, alerts)
            alerting.Engine (2s eval) ──> SSE broadcast + Notifier
            Broadcast(SSE) ──> browser /api/stream
   manager HTTP: /api/topology /api/agents /api/mesh /api/alerts /api/silences
                 /api/edges/{a}/{b}/series|diff  /api/stream  /metrics  + UI pages
```

Edges are labeled **agent-name → agent-name** (the agent maps target addr → peer name
from its assignment), so the reciprocal of `a→b` is `b→a`.

## 4. Build / test / run

```
make test              # go test -cover -race ./...
make proto             # buf lint + buf generate  (needs buf, protoc-gen-go, protoc-gen-go-grpc on PATH)
go build ./...
gofmt -l internal cmd web   # must be empty

# Local mesh (no DBs needed; degrades to in-memory):
connqc manager --http-addr :8080 --grpc-addr :8443
connqc agent --manager-addr localhost:8443 --agent-name a1 --addr 127.0.0.1:19101
connqc agent --manager-addr localhost:8443 --agent-name a2 --addr 127.0.0.1:19102
# → http://localhost:8080

# Full stack (DBs + Prometheus + Grafana):
docker compose -f docker/docker-compose.yml up --build
# UI :8080 · Prometheus :9090 · Grafana :3000 (admin/admin)
```

Proto toolchain: `go install github.com/bufbuild/buf/cmd/buf@latest`,
`google.golang.org/protobuf/cmd/protoc-gen-go@latest`,
`google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`.

## 5. Conventions

- Logging: `github.com/hamba/logger/v2` (`log.Info("msg", lctx.Str("k","v"))`); field type is `logger.Field`.
- CLI: `urfave/cli/v2` via `hamba/cmd/v2`; flag constants + `cmd.Flags{...}.Merge(cmd.LogFlags)`.
- Functional options for constructors that gained behavior (e.g. `probe.NewClient(..., probe.WithCollector(...))`).
- Storage degrades gracefully: no DSN ⇒ `Nop*Sink` ⇒ in-memory only.
- Auth is **optional**: empty `--admin-user`/`--admin-pass-hash` ⇒ UI/API open.

## 6. How to extend

- **A probe/test type** → `internal/probe/` (payload generator in `payload.go`, send loop in `client.go`).
- **An alert rule** → add a condition in `internal/alerting/engine.go` `Evaluate`; thresholds in `Config`.
- **An API endpoint** → add a handler + route in `internal/api/api.go`.
- **A UI page** → add `web/templates/pages/<name>.html`, register in `api/ui.go` `pages` map + a route.
- **A gRPC message** → edit `proto/artemis/v1/agentlink.proto`, run `make proto`, update `internal/pbconv` + handlers.
- **A DB migration** → add `migrations/{postgres,clickhouse}/NNN_*.sql` (idempotent `IF NOT EXISTS`); applied on manager start.

## 7. Testing & debugging

- Tests live next to code: `internal/stats/*_test.go`, `internal/probe/*_test.go`, `internal/alerting/engine_test.go`.
- DB-backed code is verified against real Postgres/ClickHouse via docker (see `docker/docker-compose.yml`).
- Manager `/metrics` exposes `artemis_edge_{loss_percent,latency_p95_seconds,jitter_seconds,throughput_bytes}`, `artemis_alerts_active`, `artemis_agents`.
- SSE: `curl -N localhost:8080/api/stream` shows live `{"type":"edge"|"alert",...}` events.
- Fault injection: run the manager with `--dev`, then `POST /api/debug/inject {agent,loss_percent,duration_ms}` (or the "inject loss" button on the Agents page) makes that agent's echo listener drop probes so peers see loss. See `docs/debugging.md`.

## 8. Gotchas

- Background test processes survive across shells — clean up with `pkill -x connqc` (not `pkill -f /tmp/connqc`, which matches your shell).
- ClickHouse queries must target the configured DB (`artemis`), not `default`.
- `NewClient`/`New(...)` keep working via functional options — don't break the positional signatures.
