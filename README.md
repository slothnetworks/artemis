# Artemis — Connection Quality Mesh

<p align="center">
    <a href="#license">
        <img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat" />
    </a>
    <a href="https://goreportcard.com/report/github.com/slothnetworks/artemis">
        <img src="https://goreportcard.com/badge/github.com/slothnetworks/artemis" />
    </a>
</p>

Artemis assesses the reliability of network transport **between many hosts at once**.
A central **manager** orchestrates a fleet of **agents** that probe each other over
TCP/UDP, stream per-edge statistics up in real time, and surface a live topology graph,
alerting, and history. It builds on the original `connqc` probe (still available as the
standalone `server`/`client` commands).

Detects: unstable links, intermittent firewall drops, blocked ports, route flaps,
high latency / jitter, packet loss, **in-flight payload modification**, and MTU/MSS
clamping.

## Quickstart

### Local mesh (no databases — in-memory)

```shell
connqc manager --http-addr :8080 --grpc-addr :8443
connqc agent --manager-addr localhost:8443 --agent-name a1 --addr 127.0.0.1:19101
connqc agent --manager-addr localhost:8443 --agent-name a2 --addr 127.0.0.1:19102
# open http://localhost:8080
```

The manager **auto-meshes** enrolled agents (each probes every other). The dashboard
shows a live topology graph (edges colored by loss/latency); click an edge for a
latency/loss chart and a reciprocal A↔B diff.

### Full stack (Postgres + ClickHouse + Prometheus + Grafana)

```shell
docker compose -f docker/docker-compose.yml up --build
# UI :8080 · Prometheus :9090 · Grafana :3000 (admin/admin)
```

See [docs/deployment.md](docs/deployment.md) and [docs/architecture.md](docs/architecture.md).
For development, [CLAUDE.md](CLAUDE.md) is the single source of truth.

## Commands

| Command | Role |
|---|---|
| `connqc manager` | Control plane: gRPC AgentLink, mesh, storage, alerting, UI + API + `/metrics` |
| `connqc agent` | Echo listener + probers + manager client |
| `connqc server` | Standalone echo server |
| `connqc client` | Standalone prober (stats, Prometheus, summaries) |
| `connqc hashpw <pw>` | Bcrypt hash for `--admin-pass-hash` |

### Standalone server / client

```shell
connqc server --addr :8123
connqc client --addr 127.0.0.1:8123 --interval 200ms --payload-size 9000 --burst 5 --metrics-addr :9090
```

The standalone client reports aggregate loss / latency percentiles / jitter, exposes
Prometheus metrics, and supports variable payload sizes (MTU sweep) and bursts. Run
`connqc <command> --help` for all flags.

## License

Copyright 2023 marbis GmbH

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
