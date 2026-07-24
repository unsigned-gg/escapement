//! escapement-core — agent-orchestration engine primitives.
//!
//! v0 surface: an agent [`registry`] (registration, liveness, capability
//! lookup), a [`dispatch`] layer (priority task queue, capability-matched
//! assignment, full lifecycle state machine), a [`plan`] layer (DAG plan,
//! topological resolution), a [`custody`] layer (content-addressed
//! provenance, chain propagation), a [`protocol`] layer (typed wire
//! format for dispatch communication), a [`metering`] layer (concurrency
//! limits, token-bucket rate limiting), a [`lanes`] layer (priority lanes,
//! weighted fair queueing), a [`backpressure`] layer (provider capacity
//! tracking), a [`persistence`] layer (WAL serialization, crash recovery),
//! a [`resilience`] layer (retry policy), a [`telemetry`] layer (span/metric/log
//! types), and a [`budget`] layer (per-provider token caps). Time is
//! caller-supplied milliseconds so the engine stays deterministic and
//! host-agnostic.

pub mod backpressure;
pub mod budget;
pub mod custody;
pub mod dispatch;
pub mod lanes;
pub mod merge;
pub mod metering;
pub mod persistence;
pub mod plan;
pub mod presence;
pub mod protocol;
pub mod registry;
pub mod resilience;
pub mod telemetry;
pub mod tracing;
pub mod waker;

pub use backpressure::{
    BackpressureDecision, ProviderCapacity, ProviderHealth, ProviderRegistry, UnknownProvider,
};
pub use budget::{BudgetDecision, BudgetTracker, UnknownBudgetProvider};
pub use custody::{CustodyChain, CustodyError, CustodyRef, DispatchRecord};
pub use dispatch::{DispatchError, Dispatcher, Task, TaskId, TaskState};
pub use lanes::{LaneError, LaneScheduler, PriorityLane};
pub use merge::{has_overlap, merge_all, merge_overlapping, overlapping_paths, Changeset};
pub use metering::{ConcurrencyExceeded, ConcurrencyGuard, RateLimited, TokenBucket};
pub use persistence::{
    deserialize_entry, reconstruct, replay, serialize_entry, write_entry, PersistenceError,
    RecoveredState, WalEntry, WalOp,
};
pub use plan::{Plan, PlanError, PlanNode, PlanNodeId};
pub use presence::{Presence, PresenceBridge, RosterEntry};
pub use protocol::{
    AdmissionDecision, DispatchReceipt, PlanNodeSpec, PlanSubmission, ProtocolError,
    ProtocolMessage, SettleReceipt, PROTOCOL_VERSION,
};
pub use registry::{AgentId, AgentRecord, AgentState, Registry, RegistryError};
pub use resilience::{RetryPolicy, RetryReason};
pub use telemetry::{
    DispatchEvent, DispatchMetrics, OtlpConfig, TelemetryLog, TelemetryMetric, TelemetrySpan,
};
pub use waker::{wake_and_wait, WakeResult, WakerTarget};

pub use tracing::{OtlpExporter, Span, SpanBatch, TraceContext};
