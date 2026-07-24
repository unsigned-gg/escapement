//! Mission-control presence bridge: syncs agent presence from mission-control's
//! `AgentHub` Durable Object into escapement's registry.
//!
//! Escapement's registry is the pure-Rust twin; mission-control's `AgentHub`
//! is the live presence source. This module provides the types and logic
//! to bridge the two: subscribe to roster updates, sync presence state,
//! and route dispatch only to agents with `online` presence.
//!
//! Interop, not absorption — escapement's registry is not replaced by
//! `AgentHub`. The bridge syncs state; it doesn't merge the two systems.

use std::collections::BTreeMap;
use std::fmt;

/// Agent presence states as reported by mission-control's `AgentHub`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    /// Agent is connected (WebSocket) or has a fresh heartbeat.
    Online,
    /// Agent hasn't been seen recently but is within the stale window.
    Stale,
    /// Agent has exceeded the stale window or is explicitly deregistered.
    Offline,
}

impl Presence {
    /// Whether this agent is dispatchable (escapement should assign tasks).
    #[must_use]
    pub fn is_dispatchable(self) -> bool {
        matches!(self, Self::Online | Self::Stale)
    }

    /// Map to escapement's `AgentState` for registry sync.
    #[must_use]
    pub fn to_agent_state(self) -> crate::registry::AgentState {
        use crate::registry::AgentState;
        match self {
            Self::Online | Self::Stale => AgentState::Idle,
            Self::Offline => AgentState::Unreachable,
        }
    }
}

impl fmt::Display for Presence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Online => write!(f, "online"),
            Self::Stale => write!(f, "stale"),
            Self::Offline => write!(f, "offline"),
        }
    }
}

/// A roster entry from mission-control's `AgentHub`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RosterEntry {
    pub agent_id: String,
    pub name: String,
    pub kind: String,
    pub presence: Presence,
}

/// The presence bridge: syncs mission-control roster into escapement's registry.
///
/// In production, the bridge subscribes to mission-control's `/api/agents/roster`
/// endpoint (WebSocket or polling). The synced state drives escapement's
/// dispatch decisions — only `Online`/`Stale` agents receive assignments.
#[derive(Debug, Default)]
pub struct PresenceBridge {
    /// Current roster as last synced from mission-control.
    roster: BTreeMap<String, RosterEntry>,
}

impl PresenceBridge {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Update the roster from a mission-control roster response.
    /// Each entry updates the internal presence state.
    pub fn sync_roster(&mut self, entries: Vec<RosterEntry>) {
        for entry in entries {
            self.roster.insert(entry.agent_id.clone(), entry);
        }
    }

    /// Get the presence state for an agent.
    #[must_use]
    pub fn presence(&self, agent_id: &str) -> Option<Presence> {
        self.roster.get(agent_id).map(|e| e.presence)
    }

    /// Whether an agent is dispatchable (online or stale).
    #[must_use]
    pub fn is_dispatchable(&self, agent_id: &str) -> bool {
        self.presence(agent_id)
            .is_some_and(Presence::is_dispatchable)
    }

    /// List all dispatchable agent IDs.
    #[must_use]
    pub fn dispatchable_agents(&self) -> Vec<&str> {
        self.roster
            .values()
            .filter(|e| e.presence.is_dispatchable())
            .map(|e| e.agent_id.as_str())
            .collect()
    }

    /// Number of agents in the roster.
    #[must_use]
    pub fn len(&self) -> usize {
        self.roster.len()
    }

    /// Whether the roster is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.roster.is_empty()
    }

    /// Get all roster entries (for external sync).
    pub fn entries(&self) -> impl Iterator<Item = &RosterEntry> {
        self.roster.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, presence: Presence) -> RosterEntry {
        RosterEntry {
            agent_id: id.into(),
            name: id.into(),
            kind: "agent".into(),
            presence,
        }
    }

    #[test]
    fn presence_dispatchable() {
        assert!(Presence::Online.is_dispatchable());
        assert!(Presence::Stale.is_dispatchable());
        assert!(!Presence::Offline.is_dispatchable());
    }

    #[test]
    fn presence_to_agent_state() {
        use crate::registry::AgentState;
        assert_eq!(Presence::Online.to_agent_state(), AgentState::Idle);
        assert_eq!(Presence::Offline.to_agent_state(), AgentState::Unreachable);
    }

    #[test]
    fn bridge_sync_and_query() {
        let mut bridge = PresenceBridge::new();
        bridge.sync_roster(vec![
            entry("a1", Presence::Online),
            entry("a2", Presence::Offline),
        ]);
        assert_eq!(bridge.len(), 2);
        assert!(bridge.is_dispatchable("a1"));
        assert!(!bridge.is_dispatchable("a2"));
        assert!(!bridge.is_dispatchable("ghost"));
    }

    #[test]
    fn bridge_dispatchable_list() {
        let mut bridge = PresenceBridge::new();
        bridge.sync_roster(vec![
            entry("a1", Presence::Online),
            entry("a2", Presence::Stale),
            entry("a3", Presence::Offline),
        ]);
        let dispatchable = bridge.dispatchable_agents();
        assert_eq!(dispatchable, vec!["a1", "a2"]);
    }

    #[test]
    fn bridge_empty_state() {
        let bridge = PresenceBridge::new();
        assert!(bridge.is_empty());
        assert_eq!(bridge.len(), 0);
    }

    #[test]
    fn presence_display() {
        assert_eq!(Presence::Online.to_string(), "online");
        assert_eq!(Presence::Stale.to_string(), "stale");
        assert_eq!(Presence::Offline.to_string(), "offline");
    }
}
