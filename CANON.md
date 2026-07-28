<!-- lineage
role: node
conforms_to: https://github.com/cerebral-work/terrarium/blob/main/CANON.md
defines: escapement, dispatch-protocol, custody-bridge, metering
consumes: terrarium/CANON.md, terrarium/docs/REGISTRY.md, terrarium/docs/runbooks/adopt-standards.md
depends_on: docs/dispatch-protocol-v0.1.md (the typed wire format), README.md
-->

# escapement — CANON

**Status:** v1.0.0 SHIPPED · updated 2026-07-28 · terrarium **federated / external
node** (adopts the standards, stays its own repo). Registered in
`terrarium/docs/REGISTRY.md` as `escapement | unsigned-gg/escapement | platform
(unsigned) | Cerebral Ops | agent-orchestration engine — metered dispatch`.

`docs/dispatch-protocol-v0.1.md` is the ground-truth wire format; this CANON is the
shaping doctrine. Escapement dispatches **through** blackwall custody (`blackwall-bridge`),
not around it — it is the metered dispatch substrate soma's reflex routes *into*.

## 0. What this is

The agent-orchestration engine for the unsigned constellation: **metered,
deterministic dispatch** for a fleet of persistent function agents. Priority lanes,
capability-matched assignment, concurrency + rate budgeting, content-addressed
custody propagation, and a typed wire protocol that forbids ambiguous job JSON from
crossing the wire. Zero runtime dependencies in the core; the bridge is the only
crate that reaches out (to blackwall).

## 1. Components (workspace + edge)

| Project | Lang | Role |
|---|---|---|
| **escapement-core** | Rust | The engine (13 modules: registry, dispatch, plan, custody, protocol, metering, lanes, backpressure, persistence, resilience, telemetry, budget, waker) + `merge` (CRDT overlap) + `presence` (AgentHub bridge). Zero dependencies. |
| **blackwall-bridge** | Rust | Bridge to blackwall's custody API (`spawn`→Landlock jail + poll, `receive`→RunRecord, `settle`→plan-level aggregation, `execution`→DAG plan execution with custody inheritance). Escapement dispatches *through* blackwall, never around it. |
| **escapement-serve** | Rust | The orchestrator daemon: wires registry/dispatcher/metering/budget/custody/telemetry + zero-dep HTTP server (`/healthz`, `/dispatch`, `/jobs`, `/version`). |
| **edge/worker** | TypeScript | Cloudflare Worker edge: `/dispatch` admission + protocol validation, `/jobs` + `/jobs/:id/cancel`. |

### 1c. Integration contract (the edges)

| Edge | Direction | Wire surface | Spec lives |
|---|---|---|---|
| edge → escapement-serve | admission | `PlanSubmission` (protocol `"0.1"`) → `DispatchReceipt` | `docs/dispatch-protocol-v0.1.md` |
| escapement-serve → blackwall-bridge | dispatch | blackwall CLI/serve JSON; forwards W3C `traceparent` | `crates/blackwall-bridge` |
| blackwall-bridge → escapement-serve | settle | `SettleReceipt` (`run_hash`, `state`, `output` custody ref) | `docs/dispatch-protocol-v0.1.md` |

The protocol is **stabilizing** at `"0.1"`; the 1.0.0 release froze it toward
`"1.0"` (EST-37). An edge without a written spec is a defect (the terrarium
CANON §1c rule, adopted here).

## 2. Standards (adopted from terrarium)

Per `terrarium/docs/runbooks/adopt-standards.md` §1: lefthook pre-commit/pre-push,
release-please + conventional-commits, affected-CI gate ladder, `.claude` settings +
the operator hook set (`guard-main-push`, **operator-installed — this repo declares
the set, the operator lays down the scripts**), signed commits (ED25519, no AI
attribution), and the lineage convention (every doc carries a `lineage` block).

Escapement is a **Cargo workspace** (no moon/proto: raw cargo in CI). The affected-CI
gate therefore runs `cargo` across the workspace, not `moon ci --affected`.

## 3. Custody + observability (dogfood)

- **blackwall custody** — `.blackwall/` (gitignored local) + `blackwall.toml`
  (provider=gateway, model=llm/glm-5.2, verify=cargo fmt/clippy/test, automerge=false,
  OTLP endpoint). Initialized 2026-07-24 at the federation-standards commit.
- **OTLP** — escapement-serve accepts a W3C `traceparent`, creates a child span, and
  propagates context down to the blackwall-bridge gate span. The dispatch edge is a
  child of the caller's trace. Telemetry is *not* a first-class escapement crate —
  emissivity is delegated to blackwall (the soma→escapement→blackwall→reverie-guard
  spine).

## 4. Non-goals

Not a credential custodian (that's reflex). Not a containment layer (that's blackwall).
Not a memory store (that's reverie). Escapement is the **dispatch substrate** — it
decides *when* and *to whom* a unit of agent work goes, *under* the custody +
gate planes it sits between.

## Hard rules

- **Signed conventional commits, no AI attribution.** Main is PR-only.
- **`cargo fmt + clippy (-D warnings) + cargo test`** gate every change (mirrors CI).
- **Release via release-please** — Conventional Commits → CHANGELOG + tag + GitHub
  Release. Never manual `git tag` for a routine release (v1.0.0 predates this rule —
  see the commit-message note on this change).
- **SHA-pin GitHub Actions** — no `@latest` / bare `@vN`; Dependabot keeps pins current.
- **An edge without a written spec is a defect** — protocol changes land in
  `docs/dispatch-protocol-v0.1.md` before code.
