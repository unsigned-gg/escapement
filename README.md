# escapement

Agent-orchestration engine — metered dispatch for the agent constellation.

## Layout

- `crates/escapement-core` — the engine (Rust, zero dependencies). v0 surface:
  - `registry` — agent registration, heartbeat liveness, capability lookup.
  - `dispatch` — priority task queue with capability-matched, deterministic
    assignment.
- `edge/worker` — thin Cloudflare Worker edge (TypeScript). Currently a stub
  exposing `/healthz`, `/readyz`, `/version`; deploy wiring deferred.

Time in the core is caller-supplied milliseconds — the engine is deterministic
and host-agnostic (native daemon, tests, or a Durable Object edge can all
drive it).

## Scope

Greenfield. The existing agent-coordination surfaces (AgentHub Durable Object,
agent.unsigned.gg — both homed in `unsigned-gg/mission-control`) are **not**
absorbed by v0; adoption is a later, separately decided step.

## Development

```bash
cargo test                      # engine tests
cargo clippy --all-targets      # lints (pedantic, warnings deny in CI)
cd edge/worker && pnpm test     # edge tests (vitest)
```

Releases via release-please (Conventional Commits). Main is PR-only.
