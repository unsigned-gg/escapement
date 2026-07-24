//! Settle state propagation: track blackwall's settle state machine
//! (`select`/`release`/`discard`/`apply`) through the escapement dispatch
//! lifecycle.
//!
//! A dispatched job's lifecycle doesn't end at `Completed` — it ends when
//! its changeset is settled. DAG plan nodes block: a plan doesn't complete
//! until ALL nodes settle. Settle state is tracked per-node and propagated
//! to the plan level.
//!
//! Plan-level settle semantics:
//! - all nodes `Applied` → plan complete
//! - any node `Discarded` → plan partial
//! - any node `Released` → plan dropped

use std::collections::BTreeMap;
use std::fmt;

use escapement_core::dispatch::TaskId;
use escapement_core::plan::Plan;

use crate::receive::SettleState;

/// Per-task settle tracking, mapped from blackwall's settle state machine.
#[derive(Debug, Default, Clone)]
pub struct SettleTracker {
    /// Task id → settle state.
    states: BTreeMap<TaskId, SettleState>,
    /// Track whether each state was set (to distinguish "not settled" from
    /// "settled to Pending").
    known: BTreeMap<TaskId, bool>,
}

/// Plan-level settle status, aggregated from per-node settle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanSettleStatus {
    /// No nodes have been settled yet.
    NotStarted,
    /// Some nodes settled, others still pending.
    InProgress,
    /// All nodes applied — plan complete.
    Complete,
    /// Some nodes discarded — plan partial.
    Partial,
    /// Some nodes released — plan dropped.
    Dropped,
}

impl fmt::Display for PlanSettleStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotStarted => write!(f, "not_started"),
            Self::InProgress => write!(f, "in_progress"),
            Self::Complete => write!(f, "complete"),
            Self::Partial => write!(f, "partial"),
            Self::Dropped => write!(f, "dropped"),
        }
    }
}

impl SettleTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a task for settle tracking (initial state: not started).
    pub fn register(&mut self, task_id: TaskId) {
        self.states.insert(task_id.clone(), SettleState::Pending);
        self.known.insert(task_id, false);
    }

    /// Update a task's settle state.
    ///
    /// # Errors
    /// Returns [`SettleError::UnregisteredTask`] if the task was never
    /// registered via [`Self::register`].
    pub fn set_state(&mut self, task_id: &TaskId, state: SettleState) -> Result<(), SettleError> {
        if !self.known.contains_key(task_id) {
            return Err(SettleError::UnregisteredTask(task_id.clone()));
        }
        self.states.insert(task_id.clone(), state);
        self.known.insert(task_id.clone(), true);
        Ok(())
    }

    /// Get a task's settle state.
    #[must_use]
    pub fn state(&self, task_id: &TaskId) -> Option<SettleState> {
        if *self.known.get(task_id)? {
            self.states.get(task_id).copied()
        } else {
            None
        }
    }

    /// Aggregate per-node settle states into a plan-level status.
    ///
    /// - all nodes `Applied` → [`PlanSettleStatus::Complete`]
    /// - any node `Discarded` → [`PlanSettleStatus::Partial`]
    /// - any node `Released` → [`PlanSettleStatus::Dropped`]
    /// - some pending → [`PlanSettleStatus::InProgress`]
    /// - none settled → [`PlanSettleStatus::NotStarted`]
    ///
    /// Returns `NotStarted` if the plan is empty or no tasks are registered.
    #[must_use]
    pub fn plan_status(&self, plan: &Plan) -> PlanSettleStatus {
        if plan.is_empty() {
            return PlanSettleStatus::NotStarted;
        }

        let mut any_settled = false;
        let mut any_pending = false;
        let mut any_discarded = false;
        let mut any_released = false;

        for node in plan.iter() {
            match self.state(&node.task.id) {
                Some(SettleState::Applied) => any_settled = true,
                Some(SettleState::Discarded) => {
                    any_settled = true;
                    any_discarded = true;
                }
                Some(SettleState::Released) => {
                    any_settled = true;
                    any_released = true;
                }
                Some(SettleState::Selected | SettleState::Pending) => {
                    any_settled = true;
                    any_pending = true;
                }
                None => {
                    // Not started for this node.
                    any_pending = true;
                }
            }
        }

        if !any_settled {
            PlanSettleStatus::NotStarted
        } else if any_released {
            PlanSettleStatus::Dropped
        } else if any_discarded {
            PlanSettleStatus::Partial
        } else if any_pending {
            PlanSettleStatus::InProgress
        } else {
            PlanSettleStatus::Complete
        }
    }

    /// Whether a task has reached a terminal settle state.
    #[must_use]
    pub fn is_terminal(&self, task_id: &TaskId) -> bool {
        self.state(task_id).is_some_and(|s| s.is_terminal())
    }
}

/// Errors from settle tracking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettleError {
    /// The task was never registered for settle tracking.
    UnregisteredTask(TaskId),
}

impl fmt::Display for SettleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnregisteredTask(id) => write!(f, "unregistered task: {id}"),
        }
    }
}

impl std::error::Error for SettleError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receive::SettleState;
    use escapement_core::dispatch::{Task, TaskId};
    use escapement_core::plan::PlanNode;

    fn task(id: &str, capability: &str, priority: u8) -> Task {
        Task {
            id: TaskId::new(id).unwrap(),
            required_capability: capability.into(),
            priority,
        }
    }

    #[test]
    fn register_and_set_state() {
        let mut tracker = SettleTracker::new();
        let tid = TaskId::new("t1").unwrap();
        tracker.register(tid.clone());

        assert_eq!(tracker.state(&tid), None); // registered but not settled

        tracker.set_state(&tid, SettleState::Applied).unwrap();
        assert_eq!(tracker.state(&tid), Some(SettleState::Applied));
    }

    #[test]
    fn set_state_unregistered_errors() {
        let mut tracker = SettleTracker::new();
        let tid = TaskId::new("ghost").unwrap();
        let err = tracker.set_state(&tid, SettleState::Applied).unwrap_err();
        assert_eq!(err, SettleError::UnregisteredTask(tid));
    }

    #[test]
    fn plan_status_not_started() {
        let plan = Plan::from_nodes([PlanNode::new(task("t1", "build", 5))]).unwrap();
        let tracker = SettleTracker::new();
        // No tasks registered → not started
        assert_eq!(tracker.plan_status(&plan), PlanSettleStatus::NotStarted);
    }

    #[test]
    fn plan_status_in_progress() {
        let plan = Plan::from_nodes([
            PlanNode::new(task("t1", "build", 5)),
            PlanNode::new(task("t2", "build", 5)),
        ])
        .unwrap();
        let mut tracker = SettleTracker::new();
        tracker.register(TaskId::new("t1").unwrap());
        tracker.register(TaskId::new("t2").unwrap());

        // One settled, one pending.
        tracker
            .set_state(&TaskId::new("t1").unwrap(), SettleState::Applied)
            .unwrap();

        assert_eq!(tracker.plan_status(&plan), PlanSettleStatus::InProgress);
    }

    #[test]
    fn plan_status_all_applied_complete() {
        let plan = Plan::from_nodes([
            PlanNode::new(task("t1", "build", 5)),
            PlanNode::new(task("t2", "build", 5)),
        ])
        .unwrap();
        let mut tracker = SettleTracker::new();
        tracker.register(TaskId::new("t1").unwrap());
        tracker.register(TaskId::new("t2").unwrap());
        tracker
            .set_state(&TaskId::new("t1").unwrap(), SettleState::Applied)
            .unwrap();
        tracker
            .set_state(&TaskId::new("t2").unwrap(), SettleState::Applied)
            .unwrap();

        assert_eq!(tracker.plan_status(&plan), PlanSettleStatus::Complete);
    }

    #[test]
    fn plan_status_mixed_partial() {
        let plan = Plan::from_nodes([
            PlanNode::new(task("t1", "build", 5)),
            PlanNode::new(task("t2", "build", 5)),
        ])
        .unwrap();
        let mut tracker = SettleTracker::new();
        tracker.register(TaskId::new("t1").unwrap());
        tracker.register(TaskId::new("t2").unwrap());
        tracker
            .set_state(&TaskId::new("t1").unwrap(), SettleState::Applied)
            .unwrap();
        tracker
            .set_state(&TaskId::new("t2").unwrap(), SettleState::Discarded)
            .unwrap();

        assert_eq!(tracker.plan_status(&plan), PlanSettleStatus::Partial);
    }

    #[test]
    fn plan_status_released_dropped() {
        let plan = Plan::from_nodes([
            PlanNode::new(task("t1", "build", 5)),
            PlanNode::new(task("t2", "build", 5)),
        ])
        .unwrap();
        let mut tracker = SettleTracker::new();
        tracker.register(TaskId::new("t1").unwrap());
        tracker.register(TaskId::new("t2").unwrap());
        tracker
            .set_state(&TaskId::new("t1").unwrap(), SettleState::Applied)
            .unwrap();
        tracker
            .set_state(&TaskId::new("t2").unwrap(), SettleState::Released)
            .unwrap();

        assert_eq!(tracker.plan_status(&plan), PlanSettleStatus::Dropped);
    }

    #[test]
    fn plan_status_empty_plan_not_started() {
        let plan = Plan::new();
        let tracker = SettleTracker::new();
        assert_eq!(tracker.plan_status(&plan), PlanSettleStatus::NotStarted);
    }

    #[test]
    fn is_terminal_check() {
        let mut tracker = SettleTracker::new();
        let tid = TaskId::new("t1").unwrap();
        tracker.register(tid.clone());

        tracker.set_state(&tid, SettleState::Applied).unwrap();
        assert!(tracker.is_terminal(&tid));

        tracker.set_state(&tid, SettleState::Pending).unwrap();
        assert!(!tracker.is_terminal(&tid));
    }

    #[test]
    fn plan_status_selected_is_in_progress() {
        let plan = Plan::from_nodes([PlanNode::new(task("t1", "build", 5))]).unwrap();
        let mut tracker = SettleTracker::new();
        tracker.register(TaskId::new("t1").unwrap());
        tracker
            .set_state(&TaskId::new("t1").unwrap(), SettleState::Selected)
            .unwrap();

        assert_eq!(tracker.plan_status(&plan), PlanSettleStatus::InProgress);
    }
}
