# Architecture

Artemis has two long-running roles plus two standalone tools, all in one binary.

## Components

- **manager** — the control plane. Terminates each agent's gRPC `AgentLink` stream,
  keeps the live mesh view, computes mesh assignments, runs the alerting engine, and
  serves the HTTP API + web UI + Prometheus `/metrics`.
- **agent** — runs an echo listener (so peers can probe it), a set of probers (one per
  assigned peer), and the gRPC client that enrolls and streams telemetry up.
- **server** / **client** — the original standalone echo server and one-shot prober.

## Streaming

- **Agent ↔ Manager:** a single gRPC **bidirectional stream** (`AgentLink`). The agent
  streams `Enroll`, `Sample`, `Event` (and, if enabled, `MtrSnapshot`) up; the manager
  streams `EnrollAck`, `Assignment`, `RunMtr` down. Auth is an optional shared bearer
  token in gRPC metadata.
- **Manager → Browser:** **SSE** (`/api/stream`) broadcasts live edge + alert updates.

## Storage

- **In-memory** per-edge ring buffer (hot path for the live UI).
- **ClickHouse** `samples` (batched inserts, day-partitioned, 30-day TTL) for history.
- **PostgreSQL** `events` + `alerts` (relational, queryable, upsert-by-id alerts).
- With no DSNs the manager runs **in-memory only** (graceful degradation).

## Mesh

The manager computes each agent's target list from the current registry and a **mesh
mode** (`full`, `intra-group`, `inter-group`, `off`) and pushes `Assignment`s. Agents
apply assignments idempotently (start/stop probers). Edges are labeled
agent-name → agent-name.

## Alerting

The engine evaluates rules every 2s over the live mesh: packet loss, blackout, high
latency (p95), high jitter, agent-down, and size-mismatch (event-driven). Alerts are
stateful (open/resolve), silence-aware, persisted to Postgres, broadcast over SSE, and
delivered to notifiers (log by default; webhook drop-in).

## Data integrity

Each probe payload self-describes its length + CRC32 (`internal/probe/payload.go`).
The source detects round-trip modification (echo ≠ sent → `SizeMismatch`); a
destination-side validating listener can attribute forward-path modification. Path
MTU / effective MSS are read via Linux socket options where available.
