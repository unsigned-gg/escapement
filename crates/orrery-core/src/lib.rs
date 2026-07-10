//! orrery-core — agent-orchestration engine primitives.
//!
//! v0 surface: an agent [`registry`] (registration, liveness, capability
//! lookup) and a [`dispatch`] layer (priority task queue, capability-matched
//! assignment). Time is caller-supplied milliseconds so the engine stays
//! deterministic and host-agnostic (native daemon, tests, or a Durable
//! Object edge can all drive it).

pub mod dispatch;
pub mod registry;

pub use dispatch::{DispatchError, Dispatcher, Task, TaskId, TaskState};
pub use registry::{AgentId, AgentRecord, AgentState, Registry, RegistryError};
