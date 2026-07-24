//! DAG plan execution: end-to-end orchestration that ties together plan
//! topological resolution, custody chain propagation, and settle tracking.
//!
//! Executes plan nodes in topological order. Each node inherits custody from
//! its predecessor(s). The execution tracks which nodes are ready (all needs
//! met), which are running, and which are complete. Plan completion requires
//! all leaf nodes to be settled (not just completed).
//!
//! This module does NOT spawn actual blackwall runs — it orchestrates the
//! execution graph. The caller drives the actual dispatch (via
//! [`crate::BlackwallBridge`]) and reports results back through the
//! [`PlanExecutor`] API.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use escapement_core::custody::{CustodyChain, CustodyRef, DispatchRecord};
use escapement_core::dispatch::TaskId;
use escapement_core::plan::{Plan, PlanNode};

use crate::receive::SettleState;
use crate::settle::SettleTracker;

/// An execution node — wraps a plan node with execution state.
#[derive(Debug, Clone)]
struct ExecNode {
    node: PlanNode,
    completed: bool,
    custody_ref: Option<CustodyRef>,
}

/// The status of a plan execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStatus {
    /// No nodes executed yet.
    Pending,
    /// Some nodes executing or completed, others waiting.
    Running,
    /// All nodes completed (but not necessarily settled).
    Completed,
    /// All nodes completed AND settled.
    Settled,
    /// Execution paused (e.g., a node failed and needs intervention).
    Paused,
}

impl fmt::Display for ExecutionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Settled => write!(f, "settled"),
            Self::Paused => write!(f, "paused"),
        }
    }
}

/// Orchestrates DAG plan execution with custody inheritance.
///
/// The caller drives the execution loop:
/// 1. Call [`ready_nodes`](Self::ready_nodes) to get nodes whose dependencies
///    are met.
/// 2. Dispatch each ready node (via `BlackwallBridge` or any executor).
/// 3. Call [`complete_node`](Self::complete_node) with the custody ref when
///    a node's run finishes.
/// 4. Repeat until [`status`](Self::status) returns `Completed`.
/// 5. Settle each node via [`settle_node`](Self::settle_node).
/// 6. Check [`status`](Self::status) for `Settled`.
#[derive(Debug)]
pub struct PlanExecutor {
    plan: Plan,
    nodes: BTreeMap<TaskId, ExecNode>,
    custody: CustodyChain,
    settle: SettleTracker,
    /// Nodes whose needs are all completed.
    ready: BTreeSet<TaskId>,
    /// Nodes currently being executed (dispatched but not completed).
    in_flight: BTreeSet<TaskId>,
    status: ExecutionStatus,
}

impl PlanExecutor {
    /// Create a new plan executor for a validated plan.
    #[must_use]
    pub fn new(plan: &Plan) -> Self {
        let mut nodes = BTreeMap::new();
        let mut ready = BTreeSet::new();
        let mut custody = CustodyChain::new();
        let mut settle = SettleTracker::new();

        for node in plan.iter() {
            let task_id = node.task.id.clone();
            // A node with no needs is immediately ready.
            if node.needs.is_empty() {
                ready.insert(task_id.clone());
            }
            // Register in custody chain as root (no parent yet).
            custody.register(DispatchRecord::root(task_id.clone(), "plan-executor"));
            settle.register(task_id.clone());
            nodes.insert(
                task_id.clone(),
                ExecNode {
                    node: node.clone(),
                    completed: false,
                    custody_ref: None,
                },
            );
        }

        let status = if ready.is_empty() && nodes.is_empty() {
            ExecutionStatus::Completed
        } else {
            ExecutionStatus::Pending
        };

        Self {
            plan: plan.clone(),
            nodes,
            custody,
            settle,
            ready,
            in_flight: BTreeSet::new(),
            status,
        }
    }

    /// Get the task ids of nodes whose dependencies are all completed and
    /// that haven't been dispatched yet.
    #[must_use]
    pub fn ready_nodes(&self) -> Vec<&TaskId> {
        self.ready.iter().collect()
    }

    /// Mark a node as dispatched (in flight). Removes it from the ready set.
    ///
    /// # Errors
    /// Returns [`ExecutionError::NodeNotReady`] if the node isn't in the
    /// ready set.
    pub fn dispatch_node(&mut self, task_id: &TaskId) -> Result<(), ExecutionError> {
        if !self.ready.remove(task_id) {
            return Err(ExecutionError::NodeNotReady(task_id.clone()));
        }
        self.in_flight.insert(task_id.clone());
        if self.status == ExecutionStatus::Pending {
            self.status = ExecutionStatus::Running;
        }
        Ok(())
    }

    /// Mark a node as completed with its custody reference. This propagates
    /// custody to dependent nodes (making them ready if all their needs are
    /// now met).
    ///
    /// # Errors
    /// Returns [`ExecutionError::UnknownNode`] if the node isn't in the plan,
    /// or [`ExecutionError::NodeNotInFlight`] if the node wasn't dispatched.
    pub fn complete_node(
        &mut self,
        task_id: &TaskId,
        custody_ref: &CustodyRef,
    ) -> Result<(), ExecutionError> {
        let node = self
            .nodes
            .get_mut(task_id)
            .ok_or(ExecutionError::UnknownNode(task_id.clone()))?;

        if !self.in_flight.remove(task_id) {
            return Err(ExecutionError::NodeNotInFlight(task_id.clone()));
        }

        node.completed = true;
        node.custody_ref = Some(custody_ref.clone());

        // Update custody chain with the output.
        self.custody.register(DispatchRecord {
            task_id: task_id.clone(),
            requester: "plan-executor".into(),
            plan_node: Some(task_id.clone()),
            parent_custody: None, // Set by propagation below
            output: Some(custody_ref.clone()),
            chain_depth: 0,
        });

        // Mark dependents as ready if all their needs are now completed.
        let dependents: Vec<TaskId> = self
            .nodes
            .values()
            .filter(|n| n.node.needs.contains(task_id) && !n.completed)
            .map(|n| n.node.task.id.clone())
            .collect();

        for dep_id in dependents {
            let dep_node = &self.nodes[&dep_id];
            let all_needs_met = dep_node
                .node
                .needs
                .iter()
                .all(|need| self.nodes.get(need).is_some_and(|n| n.completed));

            if all_needs_met {
                self.ready.insert(dep_id);
            }
        }

        // Check if all nodes are completed.
        if self.nodes.values().all(|n| n.completed) && self.in_flight.is_empty() {
            self.status = ExecutionStatus::Completed;
        }

        Ok(())
    }

    /// Settle a node (update its settle state from blackwall's response).
    ///
    /// # Errors
    /// Returns [`ExecutionError::UnknownNode`] if the node isn't in the plan.
    pub fn settle_node(
        &mut self,
        task_id: &TaskId,
        state: SettleState,
    ) -> Result<(), ExecutionError> {
        if !self.nodes.contains_key(task_id) {
            return Err(ExecutionError::UnknownNode(task_id.clone()));
        }
        self.settle
            .set_state(task_id, state)
            .map_err(|_| ExecutionError::UnknownNode(task_id.clone()))?;

        // Check if all nodes are settled.
        if self.status == ExecutionStatus::Completed {
            let all_settled = self
                .nodes
                .keys()
                .all(|id| self.settle.state(id).is_some_and(|s| s.is_terminal()));
            if all_settled {
                self.status = ExecutionStatus::Settled;
            }
        }

        Ok(())
    }

    /// Current execution status.
    #[must_use]
    pub fn status(&self) -> ExecutionStatus {
        self.status
    }

    /// Get the custody chain.
    #[must_use]
    pub fn custody(&self) -> &CustodyChain {
        &self.custody
    }

    /// Number of nodes in the plan.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the plan is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The underlying plan (for inspection).
    #[must_use]
    pub fn plan(&self) -> &Plan {
        &self.plan
    }
}

/// Errors from plan execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionError {
    /// The node isn't in the plan.
    UnknownNode(TaskId),
    /// The node was not dispatched (not in the in-flight set).
    NodeNotInFlight(TaskId),
    /// The node isn't ready (dependencies not met).
    NodeNotReady(TaskId),
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownNode(id) => write!(f, "unknown node: {id}"),
            Self::NodeNotInFlight(id) => write!(f, "node not in flight: {id}"),
            Self::NodeNotReady(id) => write!(f, "node not ready: {id}"),
        }
    }
}

impl std::error::Error for ExecutionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use escapement_core::dispatch::{Task, TaskId};
    use escapement_core::plan::{Plan, PlanNode};

    fn task(id: &str, capability: &str, priority: u8) -> Task {
        Task {
            id: TaskId::new(id).unwrap(),
            required_capability: capability.into(),
            priority,
        }
    }

    fn custody(hash: &str) -> CustodyRef {
        CustodyRef::new(hash).unwrap()
    }

    #[test]
    fn linear_plan_executes_in_order() {
        // a → b → c
        let plan = Plan::from_nodes([
            PlanNode::new(task("a", "build", 5)),
            PlanNode::new(task("b", "build", 5)).with_needs([TaskId::new("a").unwrap()]),
            PlanNode::new(task("c", "build", 5)).with_needs([TaskId::new("b").unwrap()]),
        ])
        .unwrap();

        let mut exec = PlanExecutor::new(&plan);

        // Only "a" is ready.
        assert_eq!(exec.ready_nodes(), vec![&TaskId::new("a").unwrap()]);
        exec.dispatch_node(&TaskId::new("a").unwrap()).unwrap();
        exec.complete_node(&TaskId::new("a").unwrap(), &custody("hash_a"))
            .unwrap();

        // Now "b" is ready.
        assert_eq!(exec.ready_nodes(), vec![&TaskId::new("b").unwrap()]);
        exec.dispatch_node(&TaskId::new("b").unwrap()).unwrap();
        exec.complete_node(&TaskId::new("b").unwrap(), &custody("hash_b"))
            .unwrap();

        // Now "c" is ready.
        assert_eq!(exec.ready_nodes(), vec![&TaskId::new("c").unwrap()]);
        exec.dispatch_node(&TaskId::new("c").unwrap()).unwrap();
        exec.complete_node(&TaskId::new("c").unwrap(), &custody("hash_c"))
            .unwrap();

        assert_eq!(exec.status(), ExecutionStatus::Completed);
    }

    #[test]
    fn diamond_plan_reads_root_first() {
        //    a
        //   / \
        //  b   c
        //   \ /
        //    d
        let plan = Plan::from_nodes([
            PlanNode::new(task("a", "build", 5)),
            PlanNode::new(task("b", "build", 5)).with_needs([TaskId::new("a").unwrap()]),
            PlanNode::new(task("c", "build", 5)).with_needs([TaskId::new("a").unwrap()]),
            PlanNode::new(task("d", "build", 5))
                .with_needs([TaskId::new("b").unwrap(), TaskId::new("c").unwrap()]),
        ])
        .unwrap();

        let mut exec = PlanExecutor::new(&plan);

        // Root "a" is ready.
        assert_eq!(exec.ready_nodes(), vec![&TaskId::new("a").unwrap()]);

        exec.dispatch_node(&TaskId::new("a").unwrap()).unwrap();
        exec.complete_node(&TaskId::new("a").unwrap(), &custody("hash_a"))
            .unwrap();

        // Now b and c are both ready (sorted by id).
        let ready: Vec<&TaskId> = exec.ready_nodes();
        assert_eq!(ready.len(), 2);
        assert!(ready.contains(&&TaskId::new("b").unwrap()));
        assert!(ready.contains(&&TaskId::new("c").unwrap()));

        // Complete b and c.
        exec.dispatch_node(&TaskId::new("b").unwrap()).unwrap();
        exec.complete_node(&TaskId::new("b").unwrap(), &custody("hash_b"))
            .unwrap();
        exec.dispatch_node(&TaskId::new("c").unwrap()).unwrap();
        exec.complete_node(&TaskId::new("c").unwrap(), &custody("hash_c"))
            .unwrap();

        // Now d is ready.
        assert_eq!(exec.ready_nodes(), vec![&TaskId::new("d").unwrap()]);

        exec.dispatch_node(&TaskId::new("d").unwrap()).unwrap();
        exec.complete_node(&TaskId::new("d").unwrap(), &custody("hash_d"))
            .unwrap();

        assert_eq!(exec.status(), ExecutionStatus::Completed);
    }

    #[test]
    fn settle_transitions_to_settled() {
        let plan = Plan::from_nodes([PlanNode::new(task("a", "build", 5))]).unwrap();
        let mut exec = PlanExecutor::new(&plan);

        exec.dispatch_node(&TaskId::new("a").unwrap()).unwrap();
        exec.complete_node(&TaskId::new("a").unwrap(), &custody("hash_a"))
            .unwrap();
        assert_eq!(exec.status(), ExecutionStatus::Completed);

        exec.settle_node(&TaskId::new("a").unwrap(), SettleState::Applied)
            .unwrap();
        assert_eq!(exec.status(), ExecutionStatus::Settled);
    }

    #[test]
    fn empty_plan_is_completed() {
        let plan = Plan::new();
        let exec = PlanExecutor::new(&plan);
        assert_eq!(exec.status(), ExecutionStatus::Completed);
    }

    #[test]
    fn dispatch_not_ready_errors() {
        let plan = Plan::from_nodes([
            PlanNode::new(task("a", "build", 5)),
            PlanNode::new(task("b", "build", 5)).with_needs([TaskId::new("a").unwrap()]),
        ])
        .unwrap();
        let mut exec = PlanExecutor::new(&plan);

        // "b" is not ready (depends on "a").
        assert!(exec.dispatch_node(&TaskId::new("b").unwrap()).is_err());
    }

    #[test]
    fn complete_not_in_flight_errors() {
        let plan = Plan::from_nodes([PlanNode::new(task("a", "build", 5))]).unwrap();
        let mut exec = PlanExecutor::new(&plan);

        // Not dispatched → can't complete.
        assert!(exec
            .complete_node(&TaskId::new("a").unwrap(), &custody("hash"))
            .is_err());
    }

    #[test]
    fn custody_chain_populated() {
        let plan = Plan::from_nodes([PlanNode::new(task("a", "build", 5))]).unwrap();
        let mut exec = PlanExecutor::new(&plan);

        exec.dispatch_node(&TaskId::new("a").unwrap()).unwrap();
        exec.complete_node(&TaskId::new("a").unwrap(), &custody("hash_a"))
            .unwrap();

        let record = exec.custody().get(&TaskId::new("a").unwrap()).unwrap();
        assert_eq!(
            record.output.as_ref().map(CustodyRef::as_str),
            Some("hash_a")
        );
    }
}
