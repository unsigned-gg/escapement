//! Dispatch protocol v0: the typed wire format for communication between
//! escapement-core, escapement-edge, and blackwall-bridge.
//!
//! Three message types:
//! - [`PlanSubmission`] — plan nodes, task specs, dependency edges, custody refs
//! - [`DispatchReceipt`] — job ID, queue position, admission decision
//! - [`SettleReceipt`] — run hash, settle state, output location
//!
//! All messages carry a protocol version. The format is manually serialized
//! (zero-dependency JSON via `std::fmt`) to keep `escapement-core` dep-free.
//! A future `escapement-protocol` crate with serde can wrap these for
//! production wire serialization.

use std::collections::BTreeSet;
use std::fmt;

use crate::custody::CustodyRef;
use crate::dispatch::{Task, TaskId, TaskState};
use crate::plan::PlanNode;

/// Current protocol version.
pub const PROTOCOL_VERSION: &str = "0.1";

/// A message in the dispatch protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolMessage {
    PlanSubmission(PlanSubmission),
    DispatchReceipt(DispatchReceipt),
    SettleReceipt(SettleReceipt),
}

/// Submit a plan or one-shot task for dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanSubmission {
    pub protocol: String,
    pub nodes: Vec<PlanNodeSpec>,
    /// Custody references for root nodes (predecessor outputs).
    pub custody_refs: Vec<CustodyRef>,
}

/// A task spec within a plan submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanNodeSpec {
    pub task: Task,
    pub needs: BTreeSet<TaskId>,
}

impl From<&PlanNode> for PlanNodeSpec {
    fn from(node: &PlanNode) -> Self {
        Self {
            task: node.task.clone(),
            needs: node.needs.clone(),
        }
    }
}

impl PlanSubmission {
    #[must_use]
    pub fn new(nodes: Vec<PlanNodeSpec>) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.into(),
            nodes,
            custody_refs: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_custody(mut self, refs: Vec<CustodyRef>) -> Self {
        self.custody_refs = refs;
        self
    }
}

/// Admission receipt returned when a plan/task is accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchReceipt {
    pub protocol: String,
    pub job_id: TaskId,
    /// Position in the queue (0 = next to dispatch).
    pub queue_position: usize,
    pub decision: AdmissionDecision,
}

/// The admission decision for a submitted plan/task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// Accepted and queued.
    Admitted,
    /// Accepted but deferred (waiting for capacity).
    Deferred { reason: String },
    /// Rejected (over quota, invalid plan, etc.).
    Rejected { reason: String },
}

impl DispatchReceipt {
    #[must_use]
    pub fn admitted(job_id: TaskId, queue_position: usize) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.into(),
            job_id,
            queue_position,
            decision: AdmissionDecision::Admitted,
        }
    }

    #[must_use]
    pub fn deferred(job_id: TaskId, queue_position: usize, reason: impl Into<String>) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.into(),
            job_id,
            queue_position,
            decision: AdmissionDecision::Deferred {
                reason: reason.into(),
            },
        }
    }

    #[must_use]
    pub fn rejected(job_id: TaskId, reason: impl Into<String>) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.into(),
            job_id,
            queue_position: 0,
            decision: AdmissionDecision::Rejected {
                reason: reason.into(),
            },
        }
    }
}

/// Receipt returned when a run settles (completes, fails, or is discarded).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettleReceipt {
    pub protocol: String,
    pub job_id: TaskId,
    /// The BLAKE3 hash of the blackwall run record.
    pub run_hash: CustodyRef,
    /// The final task state.
    pub state: TaskState,
    /// Where the output landed.
    pub output: Option<CustodyRef>,
}

impl SettleReceipt {
    #[must_use]
    pub fn new(
        job_id: TaskId,
        run_hash: CustodyRef,
        state: TaskState,
        output: Option<CustodyRef>,
    ) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.into(),
            job_id,
            run_hash,
            state,
            output,
        }
    }
}

/// Protocol-level errors (version mismatch, malformed messages).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// The message's protocol version is incompatible.
    VersionMismatch { expected: String, found: String },
    /// The message is malformed.
    Malformed(String),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VersionMismatch { expected, found } => {
                write!(
                    f,
                    "protocol version mismatch: expected {expected}, found {found}"
                )
            }
            Self::Malformed(msg) => write!(f, "malformed protocol message: {msg}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Check that a message's protocol version is compatible.
///
/// # Errors
/// Returns [`ProtocolError::VersionMismatch`] if the version doesn't match.
pub fn check_version(version: &str) -> Result<(), ProtocolError> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::VersionMismatch {
            expected: PROTOCOL_VERSION.into(),
            found: version.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: &str, capability: &str, priority: u8) -> Task {
        Task {
            id: TaskId::new(id).unwrap(),
            required_capability: capability.into(),
            priority,
        }
    }

    #[test]
    fn plan_submission_carries_protocol_version() {
        let submission = PlanSubmission::new(vec![]);
        assert_eq!(submission.protocol, "0.1");
    }

    #[test]
    fn plan_submission_with_nodes_and_custody() {
        let node = PlanNodeSpec {
            task: task("t1", "build", 5),
            needs: BTreeSet::new(),
        };
        let submission =
            PlanSubmission::new(vec![node]).with_custody(vec![CustodyRef::new("hash123").unwrap()]);
        assert_eq!(submission.nodes.len(), 1);
        assert_eq!(submission.custody_refs.len(), 1);
    }

    #[test]
    fn dispatch_receipt_admitted() {
        let receipt = DispatchReceipt::admitted(TaskId::new("j1").unwrap(), 0);
        assert_eq!(receipt.decision, AdmissionDecision::Admitted);
        assert_eq!(receipt.queue_position, 0);
        assert_eq!(receipt.protocol, "0.1");
    }

    #[test]
    fn dispatch_receipt_deferred() {
        let receipt = DispatchReceipt::deferred(TaskId::new("j1").unwrap(), 3, "capacity");
        assert!(matches!(
            receipt.decision,
            AdmissionDecision::Deferred { .. }
        ));
        assert_eq!(receipt.queue_position, 3);
    }

    #[test]
    fn dispatch_receipt_rejected() {
        let receipt = DispatchReceipt::rejected(TaskId::new("j1").unwrap(), "over quota");
        assert!(matches!(
            receipt.decision,
            AdmissionDecision::Rejected { .. }
        ));
        assert_eq!(receipt.queue_position, 0);
    }

    #[test]
    fn settle_receipt_carries_run_hash_and_state() {
        use crate::registry::AgentId;
        let receipt = SettleReceipt::new(
            TaskId::new("j1").unwrap(),
            CustodyRef::new("blake3hash").unwrap(),
            TaskState::Completed(AgentId::new("a1").unwrap()),
            Some(CustodyRef::new("output_hash").unwrap()),
        );
        assert_eq!(receipt.run_hash.as_str(), "blake3hash");
        assert!(matches!(receipt.state, TaskState::Completed(_)));
        assert!(receipt.output.is_some());
    }

    #[test]
    fn check_version_matches() {
        assert!(check_version("0.1").is_ok());
    }

    #[test]
    fn check_version_mismatch() {
        let err = check_version("2.0").unwrap_err();
        assert!(matches!(err, ProtocolError::VersionMismatch { .. }));
    }

    #[test]
    fn plan_node_spec_from_plan_node() {
        let node = PlanNode::new(task("t1", "build", 5));
        let spec = PlanNodeSpec::from(&node);
        assert_eq!(spec.task, node.task);
        assert!(spec.needs.is_empty());
    }

    #[test]
    fn protocol_message_variants() {
        let submission = ProtocolMessage::PlanSubmission(PlanSubmission::new(vec![]));
        let receipt = ProtocolMessage::DispatchReceipt(DispatchReceipt::admitted(
            TaskId::new("j1").unwrap(),
            0,
        ));
        let settle = ProtocolMessage::SettleReceipt(SettleReceipt::new(
            TaskId::new("j1").unwrap(),
            CustodyRef::new("hash").unwrap(),
            TaskState::Queued,
            None,
        ));
        assert!(matches!(submission, ProtocolMessage::PlanSubmission(_)));
        assert!(matches!(receipt, ProtocolMessage::DispatchReceipt(_)));
        assert!(matches!(settle, ProtocolMessage::SettleReceipt(_)));
    }

    #[test]
    fn deferred_decision_carries_reason() {
        let receipt = DispatchReceipt::deferred(TaskId::new("j1").unwrap(), 1, "backpressure");
        if let AdmissionDecision::Deferred { reason } = receipt.decision {
            assert_eq!(reason, "backpressure");
        } else {
            panic!("expected Deferred");
        }
    }
}
