//! Custody provenance: content-addressed references linking dispatch records
//! to blackwall run records, and chain propagation through DAG plan nodes.
//!
//! Every dispatch carries a [`DispatchRecord`] that traces back to its origin:
//! who requested it, which plan node spawned it, its parent custody (the
//! predecessor's output), and where the output landed. This is the audit
//! spine — every job is traceable back to its root.

use std::collections::BTreeMap;
use std::fmt;

use crate::dispatch::TaskId;
use crate::plan::PlanNodeId;

/// A content-addressed reference to a blackwall run record, identified by its
/// BLAKE3 hash. The hash is a string (caller-supplied — escapement-core does
/// NOT compute hashes; that's blackwall's job).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CustodyRef(String);

impl CustodyRef {
    /// Create a custody reference from a BLAKE3 hash string.
    ///
    /// # Errors
    /// Returns [`CustodyError::InvalidRef`] if the hash is empty or
    /// whitespace-only.
    pub fn new(hash: impl Into<String>) -> Result<Self, CustodyError> {
        let hash = hash.into();
        if hash.trim().is_empty() {
            return Err(CustodyError::InvalidRef);
        }
        Ok(Self(hash))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CustodyRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The full dispatch provenance record, carried through the dispatch lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchRecord {
    /// The task this record belongs to.
    pub task_id: TaskId,
    /// Who or what requested this dispatch.
    pub requester: String,
    /// The plan node that spawned this task (if part of a plan).
    pub plan_node: Option<PlanNodeId>,
    /// The parent custody reference (the predecessor's output).
    /// `None` for a root task with no predecessor.
    pub parent_custody: Option<CustodyRef>,
    /// Where the output landed (a blackwall run hash or output path).
    pub output: Option<CustodyRef>,
    /// Depth in the custody chain (0 for root, 1 for first child, etc.).
    pub chain_depth: u32,
}

impl DispatchRecord {
    /// Create a root dispatch record (no parent, depth 0).
    #[must_use]
    pub fn root(task_id: TaskId, requester: impl Into<String>) -> Self {
        Self {
            task_id,
            requester: requester.into(),
            plan_node: None,
            parent_custody: None,
            output: None,
            chain_depth: 0,
        }
    }

    /// Create a child dispatch record inheriting from a parent's custody.
    #[must_use]
    pub fn child(
        task_id: TaskId,
        requester: impl Into<String>,
        plan_node: PlanNodeId,
        parent: &DispatchRecord,
    ) -> Self {
        Self {
            task_id,
            requester: requester.into(),
            plan_node: Some(plan_node),
            parent_custody: parent.output.clone(),
            output: None,
            chain_depth: parent.chain_depth + 1,
        }
    }

    /// Attach an output custody reference (e.g., the BLAKE3 hash of the
    /// completed blackwall run).
    pub fn set_output(&mut self, output: CustodyRef) {
        self.output = Some(output);
    }

    /// Trace the custody chain: walks parent references back to the root.
    /// Returns the chain as a vec of `(depth, custody_ref)` pairs, root first.
    ///
    /// This requires access to a [`CustodyChain`] that stores all records.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.parent_custody.is_none()
    }
}

/// Tracks the custody chain across all dispatched tasks in a plan.
/// Maps task ids to their dispatch records, enabling chain traversal.
#[derive(Debug, Default, Clone)]
pub struct CustodyChain {
    records: BTreeMap<TaskId, DispatchRecord>,
}

/// Custody construction / validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CustodyError {
    /// Empty or whitespace-only custody hash.
    InvalidRef,
    /// A parent custody reference doesn't match any known record.
    UnknownParent(CustodyRef),
}

impl fmt::Display for CustodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRef => write!(f, "custody ref must be non-empty"),
            Self::UnknownParent(r) => write!(f, "unknown parent custody: {r}"),
        }
    }
}

impl std::error::Error for CustodyError {}

impl CustodyChain {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a dispatch record. The task id must be unique.
    pub fn register(&mut self, record: DispatchRecord) {
        self.records.insert(record.task_id.clone(), record);
    }

    /// Get the dispatch record for a task.
    #[must_use]
    pub fn get(&self, task_id: &TaskId) -> Option<&DispatchRecord> {
        self.records.get(task_id)
    }

    /// Trace the custody chain for a task: walks parent references back to the
    /// root. Returns records root-first (depth 0, 1, 2, ...).
    ///
    /// # Errors
    /// Returns [`CustodyError::UnknownParent`] if a parent custody ref
    /// doesn't resolve to any registered record.
    pub fn trace(&self, task_id: &TaskId) -> Result<Vec<&DispatchRecord>, CustodyError> {
        let mut chain = Vec::new();
        let mut current = self.records.get(task_id);

        while let Some(record) = current {
            chain.push(record);
            if let Some(parent_ref) = &record.parent_custody {
                // Find the parent by its output custody ref.
                current = self
                    .records
                    .values()
                    .find(|r| r.output.as_ref() == Some(parent_ref));
                if current.is_none() {
                    return Err(CustodyError::UnknownParent(parent_ref.clone()));
                }
            } else {
                break; // root
            }
        }

        // Reverse so root is first.
        chain.reverse();
        Ok(chain)
    }

    /// Propagate custody from a parent task to a child. The child record
    /// inherits the parent's output as its `parent_custody`, and its `chain_depth`
    /// is set to `parent.chain_depth + 1`.
    ///
    /// # Errors
    /// Returns [`CustodyError::UnknownParent`] if the parent task has no
    /// registered record or no output set.
    pub fn propagate(
        &mut self,
        child_task_id: TaskId,
        requester: impl Into<String>,
        plan_node: PlanNodeId,
        parent_task_id: &TaskId,
    ) -> Result<(), CustodyError> {
        let parent = self
            .records
            .get(parent_task_id)
            .ok_or_else(|| CustodyError::UnknownParent(parent_task_id.to_string().into()))?;

        let parent_output = parent
            .output
            .clone()
            .ok_or_else(|| CustodyError::UnknownParent(parent.task_id.to_string().into()))?;

        let record = DispatchRecord {
            task_id: child_task_id.clone(),
            requester: requester.into(),
            plan_node: Some(plan_node),
            parent_custody: Some(parent_output),
            output: None,
            chain_depth: parent.chain_depth + 1,
        };

        self.records.insert(child_task_id, record);
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// Convenience: convert a string to a `CustodyRef` (for error paths).
impl From<String> for CustodyRef {
    fn from(s: String) -> Self {
        // Panics on empty — only used in error-construction paths where the
        // input is known non-empty (a task id that exists).
        Self::new(s).expect("custody ref from non-empty task id")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::TaskId;

    fn custody(hash: &str) -> CustodyRef {
        CustodyRef::new(hash).unwrap()
    }

    #[test]
    fn rejects_empty_custody_ref() {
        assert_eq!(CustodyRef::new(""), Err(CustodyError::InvalidRef));
        assert_eq!(CustodyRef::new("  "), Err(CustodyError::InvalidRef));
    }

    #[test]
    fn custody_ref_display() {
        let r = custody("abc123");
        assert_eq!(r.to_string(), "abc123");
        assert_eq!(r.as_str(), "abc123");
    }

    #[test]
    fn root_record_has_no_parent() {
        let record = DispatchRecord::root(TaskId::new("t1").unwrap(), "operator");
        assert!(record.is_root());
        assert!(record.parent_custody.is_none());
        assert_eq!(record.chain_depth, 0);
    }

    #[test]
    fn child_record_inherits_parent_output() {
        let parent = DispatchRecord::root(TaskId::new("t1").unwrap(), "operator");
        let child = DispatchRecord::child(
            TaskId::new("t2").unwrap(),
            "operator",
            TaskId::new("t1").unwrap(),
            &parent,
        );
        // Parent has no output yet → child's parent_custody is None.
        // A child with no inherited custody is effectively a root.
        assert!(child.is_root());
        assert_eq!(child.parent_custody, None);
        assert_eq!(child.chain_depth, 1);
        assert_eq!(child.plan_node, Some(TaskId::new("t1").unwrap()));
    }

    #[test]
    fn child_inherits_parent_output_when_set() {
        let mut parent = DispatchRecord::root(TaskId::new("t1").unwrap(), "operator");
        parent.set_output(custody("blake3hash"));

        let child = DispatchRecord::child(
            TaskId::new("t2").unwrap(),
            "operator",
            TaskId::new("t1").unwrap(),
            &parent,
        );
        assert_eq!(child.parent_custody, Some(custody("blake3hash")));
    }

    #[test]
    fn chain_trace_depth_n() {
        let mut chain = CustodyChain::new();

        // Root task with output.
        let mut root = DispatchRecord::root(TaskId::new("t1").unwrap(), "operator");
        root.set_output(custody("hash1"));
        chain.register(root);

        // Child task.
        let mut child = DispatchRecord::root(TaskId::new("t2").unwrap(), "operator");
        child.parent_custody = Some(custody("hash1"));
        child.chain_depth = 1;
        child.set_output(custody("hash2"));
        chain.register(child);

        // Grandchild.
        let mut grandchild = DispatchRecord::root(TaskId::new("t3").unwrap(), "operator");
        grandchild.parent_custody = Some(custody("hash2"));
        grandchild.chain_depth = 2;
        chain.register(grandchild);

        let traced = chain.trace(&TaskId::new("t3").unwrap()).unwrap();
        assert_eq!(traced.len(), 3);
        assert_eq!(traced[0].chain_depth, 0); // root
        assert_eq!(traced[1].chain_depth, 1); // child
        assert_eq!(traced[2].chain_depth, 2); // grandchild
    }

    #[test]
    fn chain_trace_orphan_root() {
        let mut chain = CustodyChain::new();
        let record = DispatchRecord::root(TaskId::new("t1").unwrap(), "operator");
        chain.register(record);

        let traced = chain.trace(&TaskId::new("t1").unwrap()).unwrap();
        assert_eq!(traced.len(), 1);
        assert!(traced[0].is_root());
    }

    #[test]
    fn propagate_creates_child_with_parent_output() {
        let mut chain = CustodyChain::new();

        // Register parent with output.
        let mut parent = DispatchRecord::root(TaskId::new("t1").unwrap(), "operator");
        parent.set_output(custody("parent_hash"));
        chain.register(parent);

        // Propagate to child.
        chain
            .propagate(
                TaskId::new("t2").unwrap(),
                "operator",
                TaskId::new("t1").unwrap(),
                &TaskId::new("t1").unwrap(),
            )
            .unwrap();

        let child = chain.get(&TaskId::new("t2").unwrap()).unwrap();
        assert_eq!(child.parent_custody, Some(custody("parent_hash")));
        assert_eq!(child.chain_depth, 1);
        assert_eq!(child.plan_node, Some(TaskId::new("t1").unwrap()));
    }

    #[test]
    fn propagate_fails_without_parent_output() {
        let mut chain = CustodyChain::new();
        let parent = DispatchRecord::root(TaskId::new("t1").unwrap(), "operator");
        chain.register(parent);

        let result = chain.propagate(
            TaskId::new("t2").unwrap(),
            "operator",
            TaskId::new("t1").unwrap(),
            &TaskId::new("t1").unwrap(),
        );
        assert!(matches!(result, Err(CustodyError::UnknownParent(_))));
    }

    #[test]
    fn propagate_fails_without_parent_registered() {
        let mut chain = CustodyChain::new();
        let result = chain.propagate(
            TaskId::new("t2").unwrap(),
            "operator",
            TaskId::new("t1").unwrap(),
            &TaskId::new("ghost").unwrap(),
        );
        assert!(matches!(result, Err(CustodyError::UnknownParent(_))));
    }

    #[test]
    fn chain_trace_missing_parent_errors() {
        let mut chain = CustodyChain::new();
        let mut record = DispatchRecord::root(TaskId::new("t1").unwrap(), "operator");
        record.parent_custody = Some(custody("nonexistent_hash"));
        chain.register(record);

        let result = chain.trace(&TaskId::new("t1").unwrap());
        assert!(matches!(result, Err(CustodyError::UnknownParent(_))));
    }
}
