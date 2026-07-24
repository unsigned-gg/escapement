# Dispatch Protocol v0.1

The typed wire format for communication between escapement-core,
escapement-edge, and blackwall-bridge. No ambiguous job JSON crosses the wire.

## Version

Protocol version: `"0.1"` (see `PROTOCOL_VERSION` in `protocol.rs`)

Version negotiation: incompatible protocol versions are rejected via
`check_version()`. The 1.0.0 release will freeze this at `"1.0"`.

## Message Types

### 1. PlanSubmission

Submit a plan or one-shot task for dispatch.

```jsonc
{
  "protocol": "0.1",
  "nodes": [
    {
      "task": {
        "id": "t1",
        "required_capability": "build",
        "priority": 5
      },
      "needs": []
    }
  ],
  "custody_refs": ["blake3hash_of_parent_output"]
}
```

- `nodes` — array of `PlanNodeSpec`, each containing a `task` (id, capability,
  priority) and `needs` (set of task ids this node depends on).
- `custody_refs` — optional, custody references from predecessor outputs
  (for plan nodes that inherit custody from an external parent).

### 2. DispatchReceipt

Admission decision returned when a plan/task is accepted.

```jsonc
{
  "protocol": "0.1",
  "job_id": "t1",
  "queue_position": 0,
  "decision": "admitted"  // admitted | deferred | rejected
}
```

- `job_id` — the task id of the first node in the submission.
- `queue_position` — 0-indexed position in the dispatch queue.
- `decision`:
  - `admitted` — accepted and queued for dispatch.
  - `deferred` — accepted but waiting for capacity (reason in body).
  - `rejected` — rejected (over quota, invalid plan, etc.).

### 3. SettleReceipt

Notification that a run has settled.

```jsonc
{
  "protocol": "0.1",
  "job_id": "t1",
  "run_hash": "blake3hash",
  "state": "Completed(AgentId(\"a1\"))",
  "output": "blake3hash_of_output"
}
```

- `run_hash` — BLAKE3 content-addressed hash of the blackwall run record.
- `state` — the final `TaskState` (Completed, Failed, Cancelled, TimedOut).
- `output` — optional, the output custody reference (set when the run
  produces a changeset).

## Version Negotiation

When a client submits a `PlanSubmission`, the protocol version is checked:

```rust
pub fn check_version(version: &str) -> Result<(), ProtocolError>
```

- Matching version → accepted.
- Mismatching version → `ProtocolError::VersionMismatch`.

## ProtobufMessage Enum

All three message types are wrapped in a `ProtocolMessage` enum:

```rust
pub enum ProtocolMessage {
    PlanSubmission(PlanSubmission),
    DispatchReceipt(DispatchReceipt),
    SettleReceipt(SettleReceipt),
}
```

## Future: Serialization

The current protocol uses zero-dependency manual JSON serialization. A future
`escapement-protocol` crate with serde + bincode will provide production wire
serialization without changing the typed contract.

## Stability

The protocol is **stabilizing** during pre-1.0 development. The 1.0.0 release
(EST-37) will freeze the protocol at `"1.0"`. Breaking changes before 1.0
require a minor version bump (0.1 → 0.2).

After 1.0, breaking changes require a major version bump (1.0 → 2.0).
