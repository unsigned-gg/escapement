# escapement

Agent-orchestration engine — metered, deterministic dispatch for the agent
constellation.

## v1.0.0

### Crates

- `crates/escapement-core` — the engine (Rust, zero dependencies). 13 modules:
  - `registry` — agent registration, heartbeat liveness, capability lookup
  - `dispatch` — priority task queue, capability-matched assignment
  - `plan` — DAG plan types, topological resolver, cycle detection
  - `custody` — content-addressed provenance, custody chain propagation
  - `protocol` — typed wire format v0.1, version negotiation
  - `metering` — concurrency limits, token-bucket rate limiting
  - `lanes` — priority lanes, weighted deficit round-robin
  - `backpressure` — provider capacity + health tracking
  - `persistence` — WAL serialization, crash recovery
  - `resilience` — retry policy, exponential backoff
  - `telemetry` — span/metric/log types, OTLP config
  - `budget` — per-provider token caps
  - `waker` — TCP wake proxy client for scale-to-zero microVMs
  - `merge` — CRDT overlap merge for parallel changesets
  - `presence` — mission-control AgentHub presence bridge

- `crates/blackwall-bridge` — bridge to blackwall's custody API (4 modules):
  - `spawn` — spawn runs into Landlock jail, poll status
  - `receive` — parse RunRecord, update custody chain
  - `settle` — SettleTracker, plan-level settle aggregation
  - `execution` — PlanExecutor: DAG plan execution with custody inheritance

- `crates/escapement-serve` — orchestrator daemon with HTTP API:
  - `Orchestrator` — wires all subsystems (registry, dispatcher, metering, budget, custody, telemetry)
  - `http` — zero-dep HTTP server: `/healthz`, `/dispatch`, `/jobs`, `/version`

- `edge/worker` — Cloudflare Worker edge (TypeScript):
  - `/dispatch` admission endpoint with protocol validation
  - `/jobs` listing and `/jobs/:id/cancel`

### Tests

- 190 total: 137 core + 42 bridge + 11 serve + 5 edge
- `cargo fmt --check && cargo clippy -- -D warnings && cargo test` clean

### Deployment

- Helm chart (`deploy/helm/escapement/`) — Deployment, Service, SA, PVC, PDB, NetworkPolicy
- ArgoCD Application (`deploy/gitops/escapement-app.yaml`)
- Deployed on Cygnus cluster (7 Talos bare-metal nodes)

### License

BUSL-1.1 (provisional proprietary, destined for 0BSD FOSS upon Change Date).

## Development

```bash
cargo test                      # engine tests
cargo clippy --all-targets      # lints (pedantic, warnings deny in CI)
cd edge/worker && pnpm test     # edge tests (vitest)
```

Releases via release-please (Conventional Commits). Main is PR-only.
