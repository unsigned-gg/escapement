//! Task dispatch: priority queue + capability-matched assignment against the
//! registry.

use std::collections::BTreeMap;
use std::fmt;

use crate::registry::{AgentId, AgentState, Registry, RegistryError};

/// Unique task identifier. Non-empty, caller-assigned.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(String);

impl TaskId {
    /// # Errors
    /// Returns [`DispatchError::InvalidTaskId`] if `id` is empty or
    /// whitespace-only.
    pub fn new(id: impl Into<String>) -> Result<Self, DispatchError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(DispatchError::InvalidTaskId);
        }
        Ok(Self(id))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub id: TaskId,
    pub required_capability: String,
    /// Higher dispatches first. Ties break on enqueue order (FIFO).
    pub priority: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    Queued,
    Assigned(AgentId),
    Completed(AgentId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchError {
    InvalidTaskId,
    DuplicateTask(TaskId),
    UnknownTask(TaskId),
    TaskNotAssigned(TaskId),
    Registry(RegistryError),
}

impl fmt::Display for DispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTaskId => write!(f, "task id must be non-empty"),
            Self::DuplicateTask(id) => write!(f, "task already submitted: {id}"),
            Self::UnknownTask(id) => write!(f, "unknown task: {id}"),
            Self::TaskNotAssigned(id) => write!(f, "task not assigned: {id}"),
            Self::Registry(e) => write!(f, "registry error: {e}"),
        }
    }
}

impl std::error::Error for DispatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Registry(e) => Some(e),
            _ => None,
        }
    }
}

impl From<RegistryError> for DispatchError {
    fn from(e: RegistryError) -> Self {
        Self::Registry(e)
    }
}

#[derive(Debug)]
struct QueuedTask {
    task: Task,
    seq: u64,
}

/// Dispatcher over a priority queue of tasks. Assignment is deterministic:
/// highest priority first, FIFO within a priority, lowest agent id among
/// idle capable agents.
#[derive(Debug, Default)]
pub struct Dispatcher {
    queue: Vec<QueuedTask>,
    states: BTreeMap<TaskId, TaskState>,
    next_seq: u64,
}

impl Dispatcher {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enqueue a task.
    ///
    /// # Errors
    /// Returns [`DispatchError::DuplicateTask`] if the task id was already
    /// submitted (queued, assigned, or completed).
    pub fn submit(&mut self, task: Task) -> Result<(), DispatchError> {
        if self.states.contains_key(&task.id) {
            return Err(DispatchError::DuplicateTask(task.id));
        }
        self.states.insert(task.id.clone(), TaskState::Queued);
        self.queue.push(QueuedTask {
            task,
            seq: self.next_seq,
        });
        self.next_seq += 1;
        Ok(())
    }

    /// Assign as many queued tasks as possible to idle capable agents.
    /// Returns the `(task, agent)` pairs assigned this call. Tasks with no
    /// eligible agent remain queued.
    pub fn assign(&mut self, registry: &mut Registry) -> Vec<(TaskId, AgentId)> {
        // Highest priority first; FIFO within a priority.
        self.queue.sort_by(|a, b| {
            b.task
                .priority
                .cmp(&a.task.priority)
                .then(a.seq.cmp(&b.seq))
        });

        let mut assigned = Vec::new();
        let mut remaining = Vec::new();

        for queued in self.queue.drain(..) {
            match registry.claim_idle_with_capability(&queued.task.required_capability) {
                Some(agent_id) => {
                    self.states.insert(
                        queued.task.id.clone(),
                        TaskState::Assigned(agent_id.clone()),
                    );
                    assigned.push((queued.task.id, agent_id));
                }
                None => remaining.push(queued),
            }
        }

        self.queue = remaining;
        assigned
    }

    /// Mark an assigned task completed and return its agent to `Idle`.
    ///
    /// # Errors
    /// Returns [`DispatchError::UnknownTask`] for a never-submitted id and
    /// [`DispatchError::TaskNotAssigned`] for a queued or already-completed
    /// task.
    pub fn complete(
        &mut self,
        registry: &mut Registry,
        task_id: &TaskId,
    ) -> Result<AgentId, DispatchError> {
        let state = self
            .states
            .get(task_id)
            .ok_or_else(|| DispatchError::UnknownTask(task_id.clone()))?;
        let TaskState::Assigned(agent_id) = state.clone() else {
            return Err(DispatchError::TaskNotAssigned(task_id.clone()));
        };
        registry.set_state(&agent_id, AgentState::Idle)?;
        self.states
            .insert(task_id.clone(), TaskState::Completed(agent_id.clone()));
        Ok(agent_id)
    }

    #[must_use]
    pub fn state(&self, task_id: &TaskId) -> Option<&TaskState> {
        self.states.get(task_id)
    }

    #[must_use]
    pub fn queued_len(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::AgentId;

    fn agent(s: &str) -> AgentId {
        AgentId::new(s).unwrap()
    }

    fn task(id: &str, capability: &str, priority: u8) -> Task {
        Task {
            id: TaskId::new(id).unwrap(),
            required_capability: capability.into(),
            priority,
        }
    }

    fn registry_with(agents: &[(&str, &str)]) -> Registry {
        let mut reg = Registry::new();
        for (id, capability) in agents {
            reg.register(agent(id), [(*capability).into()], 0).unwrap();
        }
        reg
    }

    #[test]
    fn rejects_duplicate_task() {
        let mut d = Dispatcher::new();
        d.submit(task("t1", "build", 0)).unwrap();
        assert_eq!(
            d.submit(task("t1", "build", 5)),
            Err(DispatchError::DuplicateTask(TaskId::new("t1").unwrap()))
        );
    }

    #[test]
    fn assigns_highest_priority_first() {
        let mut reg = registry_with(&[("a", "build")]);
        let mut d = Dispatcher::new();
        d.submit(task("low", "build", 1)).unwrap();
        d.submit(task("high", "build", 9)).unwrap();

        let assigned = d.assign(&mut reg);
        assert_eq!(assigned, vec![(TaskId::new("high").unwrap(), agent("a"))]);
        assert_eq!(d.queued_len(), 1); // "low" waits — only one agent.
    }

    #[test]
    fn fifo_within_same_priority() {
        let mut reg = registry_with(&[("a", "build")]);
        let mut d = Dispatcher::new();
        d.submit(task("first", "build", 5)).unwrap();
        d.submit(task("second", "build", 5)).unwrap();

        let assigned = d.assign(&mut reg);
        assert_eq!(assigned, vec![(TaskId::new("first").unwrap(), agent("a"))]);
    }

    #[test]
    fn no_capable_agent_leaves_task_queued() {
        let mut reg = registry_with(&[("a", "review")]);
        let mut d = Dispatcher::new();
        d.submit(task("t1", "build", 5)).unwrap();

        assert!(d.assign(&mut reg).is_empty());
        assert_eq!(d.queued_len(), 1);
        assert_eq!(
            d.state(&TaskId::new("t1").unwrap()),
            Some(&TaskState::Queued)
        );
    }

    #[test]
    fn completion_frees_agent_for_next_task() {
        let mut reg = registry_with(&[("a", "build")]);
        let mut d = Dispatcher::new();
        d.submit(task("t1", "build", 5)).unwrap();
        d.submit(task("t2", "build", 5)).unwrap();

        let first = d.assign(&mut reg);
        assert_eq!(first.len(), 1);
        assert!(d.assign(&mut reg).is_empty()); // agent busy

        let freed = d.complete(&mut reg, &TaskId::new("t1").unwrap()).unwrap();
        assert_eq!(freed, agent("a"));

        let second = d.assign(&mut reg);
        assert_eq!(second, vec![(TaskId::new("t2").unwrap(), agent("a"))]);
    }

    #[test]
    fn complete_on_queued_task_errors() {
        let mut reg = registry_with(&[]);
        let mut d = Dispatcher::new();
        d.submit(task("t1", "build", 5)).unwrap();
        assert_eq!(
            d.complete(&mut reg, &TaskId::new("t1").unwrap()),
            Err(DispatchError::TaskNotAssigned(TaskId::new("t1").unwrap()))
        );
    }

    #[test]
    fn deterministic_agent_tie_break_by_id() {
        let mut reg = registry_with(&[("b", "build"), ("a", "build")]);
        let mut d = Dispatcher::new();
        d.submit(task("t1", "build", 5)).unwrap();

        let assigned = d.assign(&mut reg);
        assert_eq!(assigned, vec![(TaskId::new("t1").unwrap(), agent("a"))]);
    }
}
