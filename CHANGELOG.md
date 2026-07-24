# Changelog

## [1.0.0] - 2026-07-24

### Added

- **DAG plan types** (`plan.rs`) — `Plan`, `PlanNode`, topological resolver via Kahn's algorithm with deterministic BTreeSet ready set, cycle detection via DFS, missing-needs validation, duplicate-id check. EST-17.
- **Custody provenance** (`custody.rs`) — `CustodyRef` (BLAKE3 hash newtype), `DispatchRecord` (requester, plan node, parent custody, output, chain depth), `CustodyChain` (trace, propagate). EST-19.
- **Typed dispatch protocol** (`protocol.rs`) — v0.1 wire format: `PlanSubmission`, `DispatchReceipt` (Admitted/Deferred/Rejected), `SettleReceipt`. Version negotiation via `check_version()`. EST-20.
- **Concurrency limits** (`metering.rs`) — `ConcurrencyGuard` per-capability max-parallel tracking, `TokenBucket` rate limiter with caller-supplied millisecond time. EST-22.
- **Priority lanes** (`lanes.rs`) — `LaneScheduler` with weighted deficit round-robin, `PriorityLane` named lanes with configurable weights, FIFO within lanes. EST-23.
- **Backpressure** (`backpressure.rs`) — `ProviderRegistry` per-provider capacity + health tracking, `BackpressureDecision` (Accept/Queue/ProviderDown), `ProviderHealth` (Healthy/Degraded/Down). EST-24.
- **Waker integration** (`waker.rs`) — `WakerTarget`, `wake_and_wait()`, `WakeResult`. Zero-dep TCP client for scale-to-zero microVM activation. EST-34.
- **CRDT overlap merge** (`merge.rs`) — `Changeset`, `merge_overlapping()`, `merge_all()`, `has_overlap()`, `overlapping_paths()`. Last-write-wins by blob hash. EST-38.
- **Mission-control presence** (`presence.rs`) — `PresenceBridge`, `Presence`, `RosterEntry`. Syncs agent presence from AgentHub, drives dispatch decisions. EST-35.
- **Persistence** (`persistence.rs`) — WAL serialization (`WalEntry`, `serialize_entry`, `deserialize_entry`), replay, `RecoveredState` reconstruction, crash recovery. EST-29.
- **Retry policy** (`resilience.rs`) — `RetryPolicy` with exponential backoff, `RetryReason` (Timeout/ProviderError/AgentDeath). EST-30.
- **Telemetry** (`telemetry.rs`) — `TelemetrySpan`, `TelemetryMetric`, `TelemetryLog`, `DispatchMetrics`, `OtlpConfig`. Zero-dep span/metric/log types. EST-33.
- **Budget enforcement** (`budget.rs`) — `BudgetTracker` per-provider token caps, `BudgetDecision` (WithinBudget/BudgetExceeded). EST-39.
- **Helm chart** — `deploy/helm/escapement/` with Deployment, Service, SA, PVC, PDB, NetworkPolicy. EST-36.
- **ArgoCD Application** — `deploy/gitops/escapement-app.yaml`. EST-36.
- **Protocol documentation** — `docs/dispatch-protocol-v0.1.md`. EST-37.

### Changed

- **License** — BUSL 1.1 (provisional proprietary, destined for 0BSD FOSS upon Change Date). PR #3 merged.
- **Workspace** — 3 crates: `escapement-core`, `blackwall-bridge`, `escapement-serve`.
- **Blackwall bridge** — aligned with real blackwall v1.0.0 JSON contract (PR #5). EST-70-74.
- **Edge worker** — v1.0.0, `/dispatch` admission endpoint, full validation. EST-21.

### Test count

- 179 Rust tests (137 core + 42 bridge) across 12 core modules + 4 bridge modules
- 5 TypeScript tests (edge worker router)
- All passing under `cargo fmt --check && cargo clippy -- -D warnings && cargo test`
