# Deployment

## Full stack via docker-compose

From the repo root:

```
docker compose -f docker/docker-compose.yml up --build
```

Brings up:

| Service | Port | Notes |
|---|---|---|
| manager | 8080 (UI/API/metrics), 8443 (gRPC) | persists to postgres + clickhouse |
| agent-a / agent-b / agent-c | — | enroll with the manager; groups `us`/`eu` |
| postgres | 5432 | events + alerts |
| clickhouse | 8123/9000 | samples (30-day TTL) |
| prometheus | 9090 | scrapes `manager:8080/metrics` |
| grafana | 3000 | admin/admin; datasources + "Artemis Mesh" dashboard auto-provisioned |

Open <http://localhost:8080> for the live topology, and <http://localhost:3000> for
Grafana.

## Scaling agents

Add more `agent-*` services (or run agents on separate hosts). For real deployments,
agents should use **host networking** so their reported IPv4/IPv6 are routable; in that
mode set `--addr` to a reachable `host:port`. The compose demo uses bridged networking
for convenience (agents report their container IPs).

## Configuration

The manager and agents are configured via flags / env vars (see `connqc manager --help`
and `connqc agent --help`). Key manager flags:

- `--http-addr`, `--grpc-addr`
- `--postgres-dsn`, `--clickhouse-dsn` (empty ⇒ in-memory only)
- `--agent-token` (shared bearer token; empty ⇒ open)
- `--admin-user`, `--admin-pass-hash` (UI auth; empty ⇒ open). Generate the hash with
  `connqc hashpw <password>`.

## Retention

ClickHouse `samples` carries a 30-day TTL (see `migrations/clickhouse/001_samples.sql`).
Adjust the TTL there and re-apply on a fresh volume.
