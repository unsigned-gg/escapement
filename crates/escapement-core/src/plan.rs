//! DAG plan: multi-step task plans with dependency edges and topological
//! resolution.
//!
//! A [`Plan`] is a set of [`PlanNode`]s where each node wraps a [`Task`] and
//! declares which other nodes must complete before it can dispatch. The plan
//! validates for duplicate ids, missing needs, and cycles, then resolves into
//! a deterministic topological execution order (dependencies before dependents,
//! ties broken by task-id order for reproducibility).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::dispatch::Task;

/// Unique plan node identifier. Reuses [`crate::dispatch::TaskId`] since a plan
/// node's identity IS its task identity.
pub type PlanNodeId = crate::dispatch::TaskId;

/// A node in a DAG execution plan: a [`Task`] plus the set of task ids that
/// must complete before this node can dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanNode {
    pub task: Task,
    pub needs: BTreeSet<PlanNodeId>,
}

impl PlanNode {
    /// Create a plan node with no dependencies.
    #[must_use]
    pub fn new(task: Task) -> Self {
        Self {
            task,
            needs: BTreeSet::new(),
        }
    }

    /// Set the dependency set. Returns `self` for chaining.
    #[must_use]
    pub fn with_needs(mut self, needs: impl IntoIterator<Item = PlanNodeId>) -> Self {
        self.needs = needs.into_iter().collect();
        self
    }

    #[must_use]
    pub fn id(&self) -> &PlanNodeId {
        &self.task.id
    }
}

/// A validated DAG plan: nodes with dependency edges, resolvable into a
/// topological execution order.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Plan {
    nodes: BTreeMap<PlanNodeId, PlanNode>,
}

/// Plan validation / construction errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    /// A node id appeared more than once.
    DuplicateNode(PlanNodeId),
    /// A `needs` entry references a node id not in the plan.
    MissingNeed {
        node: PlanNodeId,
        missing: PlanNodeId,
    },
    /// The dependency graph contains a cycle.
    Cycle(PlanNodeId),
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateNode(id) => write!(f, "duplicate plan node: {id}"),
            Self::MissingNeed { node, missing } => {
                write!(f, "node {node} needs unknown node: {missing}")
            }
            Self::Cycle(id) => write!(f, "dependency cycle involving node: {id}"),
        }
    }
}

impl std::error::Error for PlanError {}

impl Plan {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a validated plan from an iterator of plan nodes.
    ///
    /// # Errors
    /// Returns [`PlanError`] for duplicate ids, missing needs, or cycles.
    pub fn from_nodes(nodes: impl IntoIterator<Item = PlanNode>) -> Result<Self, PlanError> {
        let mut plan = Self::new();
        for node in nodes {
            plan.add_node(node)?;
        }
        plan.validate()?;
        Ok(plan)
    }

    /// Add a node to the plan.
    ///
    /// # Errors
    /// Returns [`PlanError::DuplicateNode`] if the task id already exists.
    pub fn add_node(&mut self, node: PlanNode) -> Result<(), PlanError> {
        if self.nodes.contains_key(&node.task.id) {
            return Err(PlanError::DuplicateNode(node.task.id));
        }
        self.nodes.insert(node.task.id.clone(), node);
        Ok(())
    }

    /// Validate the plan: no missing needs, no cycles. Duplicate ids are
    /// prevented at insertion time.
    ///
    /// # Errors
    /// Returns [`PlanError::MissingNeed`] or [`PlanError::Cycle`].
    pub fn validate(&self) -> Result<(), PlanError> {
        // Check all needs reference existing nodes.
        for (id, node) in &self.nodes {
            for need in &node.needs {
                if !self.nodes.contains_key(need) {
                    return Err(PlanError::MissingNeed {
                        node: id.clone(),
                        missing: need.clone(),
                    });
                }
            }
        }

        // Cycle detection via DFS with a recursion stack.
        let mut visited = BTreeSet::new();
        let mut on_stack = BTreeSet::new();
        for id in self.nodes.keys() {
            if !visited.contains(id) {
                self.detect_cycle(id, &mut visited, &mut on_stack)?;
            }
        }
        Ok(())
    }

    fn detect_cycle(
        &self,
        id: &PlanNodeId,
        visited: &mut BTreeSet<PlanNodeId>,
        on_stack: &mut BTreeSet<PlanNodeId>,
    ) -> Result<(), PlanError> {
        visited.insert(id.clone());
        on_stack.insert(id.clone());

        if let Some(node) = self.nodes.get(id) {
            for need in &node.needs {
                if !visited.contains(need) {
                    self.detect_cycle(need, visited, on_stack)?;
                } else if on_stack.contains(need) {
                    return Err(PlanError::Cycle(need.clone()));
                }
            }
        }

        on_stack.remove(id);
        Ok(())
    }

    /// Resolve the plan into topological order: dependencies before
    /// dependents. Deterministic — nodes with no remaining unmet dependencies
    /// are emitted in task-id order (Kahn's algorithm with a sorted ready
    /// set).
    ///
    /// # Panics
    /// Never panics on a validated plan. If the plan was not validated,
    /// nodes in a cycle are silently omitted from the output.
    #[must_use]
    pub fn topological_order(&self) -> Vec<&PlanNode> {
        // Reverse adjacency: node → nodes that depend on it.
        let mut dependents: BTreeMap<&PlanNodeId, Vec<&PlanNodeId>> = BTreeMap::new();
        for (id, node) in &self.nodes {
            for need in &node.needs {
                dependents.entry(need).or_default().push(id);
            }
        }

        // In-degree = number of unmet needs per node.
        let mut in_degree: BTreeMap<&PlanNodeId, usize> =
            self.nodes.keys().map(|id| (id, 0)).collect();
        for node in self.nodes.values() {
            *in_degree.get_mut(&node.task.id).unwrap() = node.needs.len();
        }

        let mut ready: BTreeSet<&PlanNodeId> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(id, _)| *id)
            .collect();

        let mut result = Vec::new();

        while let Some(id) = ready.iter().next().copied() {
            ready.remove(&id);

            if let Some(deps) = dependents.get(&id) {
                for &dep_id in deps {
                    let deg = in_degree.get_mut(dep_id).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        ready.insert(dep_id);
                    }
                }
            }

            result.push(&self.nodes[id]);
        }

        result
    }

    /// Iterate over all nodes in the plan (id-sorted order).
    pub fn iter(&self) -> impl Iterator<Item = &PlanNode> {
        self.nodes.values()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    #[must_use]
    pub fn get(&self, id: &PlanNodeId) -> Option<&PlanNode> {
        self.nodes.get(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::TaskId;

    fn task(id: &str, capability: &str, priority: u8) -> Task {
        Task {
            id: TaskId::new(id).unwrap(),
            required_capability: capability.into(),
            priority,
        }
    }

    fn node(id: &str, capability: &str, priority: u8) -> PlanNode {
        PlanNode::new(task(id, capability, priority))
    }

    #[test]
    fn rejects_duplicate_node() {
        let result = Plan::from_nodes([node("a", "build", 5), node("a", "build", 3)]);
        assert_eq!(
            result,
            Err(PlanError::DuplicateNode(TaskId::new("a").unwrap()))
        );
    }

    #[test]
    fn rejects_missing_need() {
        let result = Plan::from_nodes([
            PlanNode::new(task("a", "build", 5)),
            PlanNode::new(task("b", "build", 5)).with_needs([TaskId::new("ghost").unwrap()]),
        ]);
        assert_eq!(
            result,
            Err(PlanError::MissingNeed {
                node: TaskId::new("b").unwrap(),
                missing: TaskId::new("ghost").unwrap()
            })
        );
    }

    #[test]
    fn rejects_cycle_two_nodes() {
        // a needs b, b needs a → cycle
        let result = Plan::from_nodes([
            PlanNode::new(task("a", "build", 5)).with_needs([TaskId::new("b").unwrap()]),
            PlanNode::new(task("b", "build", 5)).with_needs([TaskId::new("a").unwrap()]),
        ]);
        assert!(matches!(result, Err(PlanError::Cycle(_))));
    }

    #[test]
    fn rejects_cycle_three_nodes() {
        // a → b → c → a
        let result = Plan::from_nodes([
            PlanNode::new(task("a", "build", 5)).with_needs([TaskId::new("b").unwrap()]),
            PlanNode::new(task("b", "build", 5)).with_needs([TaskId::new("c").unwrap()]),
            PlanNode::new(task("c", "build", 5)).with_needs([TaskId::new("a").unwrap()]),
        ]);
        assert!(matches!(result, Err(PlanError::Cycle(_))));
    }

    #[test]
    fn valid_plan_resolves_topologically() {
        // a (no deps), b needs a, c needs a, d needs b+c
        let plan = Plan::from_nodes([
            node("a", "build", 5),
            PlanNode::new(task("b", "build", 5)).with_needs([TaskId::new("a").unwrap()]),
            PlanNode::new(task("c", "build", 5)).with_needs([TaskId::new("a").unwrap()]),
            PlanNode::new(task("d", "build", 5))
                .with_needs([TaskId::new("b").unwrap(), TaskId::new("c").unwrap()]),
        ])
        .unwrap();

        let order: Vec<&str> = plan
            .topological_order()
            .iter()
            .map(|n| n.task.id.as_str())
            .collect();

        // a must come before b and c; b and c before d.
        let pos = |s: &str| order.iter().position(|&x| x == s).unwrap();
        assert!(pos("a") < pos("b"));
        assert!(pos("a") < pos("c"));
        assert!(pos("b") < pos("d"));
        assert!(pos("c") < pos("d"));
        assert_eq!(order.len(), 4);
    }

    #[test]
    fn topological_order_is_deterministic() {
        // Two independent nodes: "b" and "a" with no deps.
        // Stable output should be ["a", "b"] (task-id order).
        let plan = Plan::from_nodes([node("b", "build", 9), node("a", "build", 1)]).unwrap();

        let order: Vec<&str> = plan
            .topological_order()
            .iter()
            .map(|n| n.task.id.as_str())
            .collect();

        assert_eq!(order, vec!["a", "b"]);
    }

    #[test]
    fn single_node_plan_validates_and_resolves() {
        let plan = Plan::from_nodes([node("solo", "build", 5)]).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan.topological_order().len(), 1);
    }

    #[test]
    fn empty_plan_validates_and_resolves_empty() {
        let plan = Plan::new();
        plan.validate().unwrap();
        assert!(plan.topological_order().is_empty());
        assert!(plan.is_empty());
    }

    #[test]
    fn self_cycle_rejected() {
        let result = Plan::from_nodes([
            PlanNode::new(task("a", "build", 5)).with_needs([TaskId::new("a").unwrap()])
        ]);
        assert!(matches!(result, Err(PlanError::Cycle(_))));
    }

    #[test]
    fn diamond_dependency_resolves() {
        //    a
        //   / \
        //  b   c
        //   \ /
        //    d
        let plan = Plan::from_nodes([
            node("a", "build", 5),
            PlanNode::new(task("b", "build", 5)).with_needs([TaskId::new("a").unwrap()]),
            PlanNode::new(task("c", "build", 5)).with_needs([TaskId::new("a").unwrap()]),
            PlanNode::new(task("d", "build", 5))
                .with_needs([TaskId::new("b").unwrap(), TaskId::new("c").unwrap()]),
        ])
        .unwrap();

        let order: Vec<&str> = plan
            .topological_order()
            .iter()
            .map(|n| n.task.id.as_str())
            .collect();

        let pos = |s: &str| order.iter().position(|&x| x == s).unwrap();
        assert_eq!(pos("a"), 0); // root first
        assert!(pos("b") < pos("d"));
        assert!(pos("c") < pos("d"));
        assert_eq!(pos("d"), 3); // leaf last
    }
}
