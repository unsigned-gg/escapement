//! Agent registry: registration, liveness, and capability lookup.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Unique agent identifier. Non-empty, caller-assigned.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentId(String);

impl AgentId {
    /// # Errors
    /// Returns [`RegistryError::InvalidId`] if `id` is empty or whitespace-only.
    pub fn new(id: impl Into<String>) -> Result<Self, RegistryError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(RegistryError::InvalidId);
        }
        Ok(Self(id))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentState {
    /// Registered and available for assignment.
    Idle,
    /// Currently assigned a task.
    Busy,
    /// Missed its heartbeat window; excluded from assignment until it
    /// heartbeats again.
    Unreachable,
}

#[derive(Debug, Clone)]
pub struct AgentRecord {
    pub id: AgentId,
    pub capabilities: BTreeSet<String>,
    pub state: AgentState,
    /// Last heartbeat, in caller-supplied milliseconds.
    pub last_heartbeat_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    InvalidId,
    DuplicateAgent(AgentId),
    UnknownAgent(AgentId),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidId => write!(f, "agent id must be non-empty"),
            Self::DuplicateAgent(id) => write!(f, "agent already registered: {id}"),
            Self::UnknownAgent(id) => write!(f, "unknown agent: {id}"),
        }
    }
}

impl std::error::Error for RegistryError {}

/// In-memory agent registry. Deterministic iteration order (`BTreeMap`) so
/// assignment tie-breaks are stable across runs.
#[derive(Debug, Default)]
pub struct Registry {
    agents: BTreeMap<AgentId, AgentRecord>,
}

impl Registry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new agent as `Idle`.
    ///
    /// # Errors
    /// Returns [`RegistryError::DuplicateAgent`] if the id is already present.
    pub fn register(
        &mut self,
        id: AgentId,
        capabilities: impl IntoIterator<Item = String>,
        now_ms: u64,
    ) -> Result<(), RegistryError> {
        if self.agents.contains_key(&id) {
            return Err(RegistryError::DuplicateAgent(id));
        }
        self.agents.insert(
            id.clone(),
            AgentRecord {
                id,
                capabilities: capabilities.into_iter().collect(),
                state: AgentState::Idle,
                last_heartbeat_ms: now_ms,
            },
        );
        Ok(())
    }

    /// # Errors
    /// Returns [`RegistryError::UnknownAgent`] if the id is not registered.
    pub fn deregister(&mut self, id: &AgentId) -> Result<AgentRecord, RegistryError> {
        self.agents
            .remove(id)
            .ok_or_else(|| RegistryError::UnknownAgent(id.clone()))
    }

    /// Record a heartbeat. An `Unreachable` agent recovers to `Idle`; a
    /// `Busy` agent stays `Busy`.
    ///
    /// # Errors
    /// Returns [`RegistryError::UnknownAgent`] if the id is not registered.
    pub fn heartbeat(&mut self, id: &AgentId, now_ms: u64) -> Result<(), RegistryError> {
        let record = self
            .agents
            .get_mut(id)
            .ok_or_else(|| RegistryError::UnknownAgent(id.clone()))?;
        record.last_heartbeat_ms = now_ms;
        if record.state == AgentState::Unreachable {
            record.state = AgentState::Idle;
        }
        Ok(())
    }

    /// Mark agents whose last heartbeat is older than `ttl_ms` as
    /// `Unreachable` and return their ids. Already-`Unreachable` agents are
    /// not re-reported.
    pub fn sweep_stale(&mut self, now_ms: u64, ttl_ms: u64) -> Vec<AgentId> {
        let mut newly_stale = Vec::new();
        for record in self.agents.values_mut() {
            if record.state != AgentState::Unreachable
                && now_ms.saturating_sub(record.last_heartbeat_ms) > ttl_ms
            {
                record.state = AgentState::Unreachable;
                newly_stale.push(record.id.clone());
            }
        }
        newly_stale
    }

    /// Idle agents advertising `capability`, in stable id order.
    #[must_use]
    pub fn idle_with_capability(&self, capability: &str) -> Vec<&AgentRecord> {
        self.agents
            .values()
            .filter(|r| r.state == AgentState::Idle && r.capabilities.contains(capability))
            .collect()
    }

    #[must_use]
    pub fn get(&self, id: &AgentId) -> Option<&AgentRecord> {
        self.agents.get(id)
    }

    /// Atomically claim the first idle agent (stable id order) advertising
    /// `capability`, marking it `Busy`.
    pub(crate) fn claim_idle_with_capability(&mut self, capability: &str) -> Option<AgentId> {
        let agent_id = self
            .agents
            .values()
            .find(|r| r.state == AgentState::Idle && r.capabilities.contains(capability))
            .map(|r| r.id.clone())?;
        if let Some(record) = self.agents.get_mut(&agent_id) {
            record.state = AgentState::Busy;
        }
        Some(agent_id)
    }

    pub(crate) fn set_state(
        &mut self,
        id: &AgentId,
        state: AgentState,
    ) -> Result<(), RegistryError> {
        let record = self
            .agents
            .get_mut(id)
            .ok_or_else(|| RegistryError::UnknownAgent(id.clone()))?;
        record.state = state;
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.agents.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.agents.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> AgentId {
        AgentId::new(s).unwrap()
    }

    #[test]
    fn rejects_empty_id() {
        assert_eq!(AgentId::new("  "), Err(RegistryError::InvalidId));
    }

    #[test]
    fn rejects_duplicate_registration() {
        let mut reg = Registry::new();
        reg.register(id("a"), ["build".into()], 0).unwrap();
        assert_eq!(
            reg.register(id("a"), [], 1),
            Err(RegistryError::DuplicateAgent(id("a")))
        );
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn heartbeat_unknown_agent_errors() {
        let mut reg = Registry::new();
        assert_eq!(
            reg.heartbeat(&id("ghost"), 0),
            Err(RegistryError::UnknownAgent(id("ghost")))
        );
    }

    #[test]
    fn sweep_marks_stale_agents_once() {
        let mut reg = Registry::new();
        reg.register(id("a"), [], 0).unwrap();
        reg.register(id("b"), [], 150).unwrap();

        let stale = reg.sweep_stale(200, 100);
        assert_eq!(stale, vec![id("a")]);
        assert_eq!(reg.get(&id("a")).unwrap().state, AgentState::Unreachable);
        assert_eq!(reg.get(&id("b")).unwrap().state, AgentState::Idle);

        // Second sweep does not re-report the same agent.
        assert!(reg.sweep_stale(201, 100).is_empty());
    }

    #[test]
    fn heartbeat_recovers_unreachable_agent() {
        let mut reg = Registry::new();
        reg.register(id("a"), [], 0).unwrap();
        reg.sweep_stale(200, 100);
        assert_eq!(reg.get(&id("a")).unwrap().state, AgentState::Unreachable);

        reg.heartbeat(&id("a"), 250).unwrap();
        assert_eq!(reg.get(&id("a")).unwrap().state, AgentState::Idle);
    }

    #[test]
    fn capability_lookup_excludes_busy_and_unreachable() {
        let mut reg = Registry::new();
        reg.register(id("a"), ["build".into()], 0).unwrap();
        reg.register(id("b"), ["build".into()], 0).unwrap();
        reg.register(id("c"), ["review".into()], 0).unwrap();
        reg.set_state(&id("b"), AgentState::Busy).unwrap();

        let capable = reg.idle_with_capability("build");
        assert_eq!(
            capable.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
            vec![id("a")]
        );
    }
}
