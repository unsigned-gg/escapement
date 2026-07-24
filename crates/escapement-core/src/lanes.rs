//! Priority lanes: named scheduling lanes with weighted fair queueing.
//!
//! Instead of a flat `priority: u8`, tasks are assigned to named lanes (e.g.
//! `critical`, `high`, `normal`, `low`, `background`). Lanes have weights
//! that control dispatch throughput: a lane with weight 2 gets 2x the
//! dispatch slots of a lane with weight 1. Within a lane, FIFO ordering is
//! preserved.
//!
//! The [`LaneScheduler`] picks the next lane to dispatch from using a
//! weighted round-robin algorithm (deficit-based), then returns the
//! oldest task in that lane (FIFO).

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

/// A named priority lane with an integer weight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriorityLane {
    pub name: String,
    /// Higher weight = more dispatch slots per round.
    pub weight: u32,
}

impl PriorityLane {
    #[must_use]
    pub fn new(name: impl Into<String>, weight: u32) -> Self {
        Self {
            name: name.into(),
            weight,
        }
    }
}

/// Error returned for lane configuration issues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaneError {
    /// A lane with the same name already exists.
    DuplicateLane(String),
    /// A task references an unknown lane.
    UnknownLane(String),
    /// A lane has weight 0.
    ZeroWeight(String),
}

impl fmt::Display for LaneError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateLane(name) => write!(f, "duplicate lane: {name}"),
            Self::UnknownLane(name) => write!(f, "unknown lane: {name}"),
            Self::ZeroWeight(name) => write!(f, "lane {name} has weight 0"),
        }
    }
}

impl std::error::Error for LaneError {}

/// A queued item in a lane — carries a task id and its enqueue seq for FIFO.
#[derive(Debug, Clone)]
struct QueuedItem {
    task_id: String,
}

/// Per-lane state for the scheduler.
#[derive(Debug, Clone)]
struct LaneState {
    weight: u32,
    queue: VecDeque<QueuedItem>,
    /// Deficit counter for weighted round-robin.
    deficit: u64,
}

/// Priority lane scheduler. Picks which lane to dispatch from next using
/// weighted deficit round-robin, then returns the oldest task in that lane.
#[derive(Debug)]
pub struct LaneScheduler {
    lanes: BTreeMap<String, LaneState>,
    /// Quantum added to each lane's deficit per round.
    quantum: u64,
}

impl Default for LaneScheduler {
    fn default() -> Self {
        Self::new(1000)
    }
}

impl LaneScheduler {
    #[must_use]
    pub fn new(quantum: u64) -> Self {
        Self {
            lanes: BTreeMap::new(),
            quantum,
        }
    }

    /// Register a lane.
    ///
    /// # Errors
    /// Returns [`LaneError::DuplicateLane`] if the name exists,
    /// [`LaneError::ZeroWeight`] if weight is 0.
    pub fn add_lane(&mut self, lane: PriorityLane) -> Result<(), LaneError> {
        if self.lanes.contains_key(&lane.name) {
            return Err(LaneError::DuplicateLane(lane.name));
        }
        if lane.weight == 0 {
            return Err(LaneError::ZeroWeight(lane.name));
        }
        self.lanes.insert(
            lane.name.clone(),
            LaneState {
                weight: lane.weight,
                queue: VecDeque::new(),
                deficit: 0,
            },
        );
        Ok(())
    }

    /// Enqueue a task into a lane.
    ///
    /// # Errors
    /// Returns [`LaneError::UnknownLane`] if the lane doesn't exist.
    pub fn enqueue(&mut self, lane: &str, task_id: impl Into<String>) -> Result<(), LaneError> {
        let state = self
            .lanes
            .get_mut(lane)
            .ok_or_else(|| LaneError::UnknownLane(lane.to_string()))?;
        state.queue.push_back(QueuedItem {
            task_id: task_id.into(),
        });
        Ok(())
    }

    /// Pick the next task to dispatch using weighted deficit round-robin.
    /// Returns the task id, or `None` if all lanes are empty.
    ///
    /// # Panics
    /// Panics if internal invariant is violated (a lane selected for dispatch
    /// has an empty queue, which should be impossible by construction).
    #[must_use]
    pub fn next_task(&mut self) -> Option<String> {
        if self.lanes.values().all(|l| l.queue.is_empty()) {
            return None;
        }

        // If any non-empty lane has deficit >= quantum, dispatch from it.
        // If none do, add quantum to all non-empty lanes' deficits (weighted:
        // each lane gets deficit += quantum * weight), then retry.
        loop {
            // Find the lane with the highest deficit that has tasks.
            let mut best: (Option<String>, u64) = (None, 0);
            for (name, state) in &self.lanes {
                if state.queue.is_empty() {
                    continue;
                }
                if best.0.is_none() || state.deficit > best.1 {
                    best = (Some(name.clone()), state.deficit);
                }
            }

            let (name, deficit) = best;
            let name = name?;

            if deficit >= self.quantum {
                // Dispatch from this lane.
                let state = self.lanes.get_mut(&name).unwrap();
                state.deficit -= self.quantum;
                let item = state.queue.pop_front().unwrap();
                return Some(item.task_id);
            }

            // No lane has enough deficit — add weighted quantum to all non-empty lanes.
            let mut any_non_empty = false;
            for state in self.lanes.values_mut() {
                if !state.queue.is_empty() {
                    state.deficit += self.quantum * u64::from(state.weight);
                    any_non_empty = true;
                }
            }
            if !any_non_empty {
                return None;
            }
            // Loop back — now some lane should have enough deficit.
        }
    }

    /// Number of queued tasks across all lanes.
    #[must_use]
    pub fn queued_len(&self) -> usize {
        self.lanes.values().map(|l| l.queue.len()).sum()
    }

    /// Number of tasks in a specific lane.
    #[must_use]
    pub fn lane_len(&self, lane: &str) -> usize {
        self.lanes.get(lane).map_or(0, |s| s.queue.len())
    }

    /// Check if a lane exists.
    #[must_use]
    pub fn has_lane(&self, name: &str) -> bool {
        self.lanes.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scheduler_with_lanes() -> LaneScheduler {
        let mut s = LaneScheduler::new(1000);
        s.add_lane(PriorityLane::new("high", 2)).unwrap();
        s.add_lane(PriorityLane::new("low", 1)).unwrap();
        s
    }

    #[test]
    fn add_lane() {
        let mut s = LaneScheduler::new(1000);
        s.add_lane(PriorityLane::new("critical", 3)).unwrap();
        assert!(s.has_lane("critical"));
    }

    #[test]
    fn duplicate_lane_errors() {
        let mut s = LaneScheduler::new(1000);
        s.add_lane(PriorityLane::new("high", 1)).unwrap();
        assert_eq!(
            s.add_lane(PriorityLane::new("high", 2)),
            Err(LaneError::DuplicateLane("high".into()))
        );
    }

    #[test]
    fn zero_weight_errors() {
        let mut s = LaneScheduler::new(1000);
        assert_eq!(
            s.add_lane(PriorityLane::new("zero", 0)),
            Err(LaneError::ZeroWeight("zero".into()))
        );
    }

    #[test]
    fn enqueue_to_unknown_lane_errors() {
        let mut s = scheduler_with_lanes();
        assert_eq!(
            s.enqueue("ghost", "t1"),
            Err(LaneError::UnknownLane("ghost".into()))
        );
    }

    #[test]
    fn empty_scheduler_returns_none() {
        let mut s = scheduler_with_lanes();
        assert!(s.next_task().is_none());
    }

    #[test]
    fn next_task_dispatches_from_single_lane() {
        let mut s = scheduler_with_lanes();
        s.enqueue("high", "t1").unwrap();
        assert_eq!(s.next_task(), Some("t1".into()));
        assert_eq!(s.queued_len(), 0);
    }

    #[test]
    fn fifo_within_lane() {
        let mut s = scheduler_with_lanes();
        s.enqueue("high", "first").unwrap();
        s.enqueue("high", "second").unwrap();
        s.enqueue("high", "third").unwrap();

        assert_eq!(s.next_task(), Some("first".into()));
        assert_eq!(s.next_task(), Some("second".into()));
        assert_eq!(s.next_task(), Some("third".into()));
    }

    #[test]
    fn weighted_fairness_2_to_1() {
        // High lane weight=2, low lane weight=1.
        // Over many dispatches, high should get ~2x the throughput.
        let mut s = LaneScheduler::new(1000);
        s.add_lane(PriorityLane::new("high", 2)).unwrap();
        s.add_lane(PriorityLane::new("low", 1)).unwrap();

        // Enqueue 10 tasks in each lane.
        for i in 0..10 {
            s.enqueue("high", format!("h{i}")).unwrap();
            s.enqueue("low", format!("l{i}")).unwrap();
        }

        let mut high_count = 0;
        let mut low_count = 0;
        for _ in 0..15 {
            if let Some(task) = s.next_task() {
                if task.starts_with('h') {
                    high_count += 1;
                } else {
                    low_count += 1;
                }
            } else {
                break;
            }
        }

        // High lane should have gotten more dispatches (weight 2 vs 1).
        assert!(high_count > low_count, "high={high_count}, low={low_count}");
        // Roughly 2:1 ratio — high should be ~10, low ~5 after 15 dispatches.
        assert!(high_count >= 8, "high_count={high_count}");
        assert!(low_count <= 7, "low_count={low_count}");
    }

    #[test]
    fn lane_isolation() {
        let mut s = scheduler_with_lanes();
        s.enqueue("high", "h1").unwrap();
        s.enqueue("low", "l1").unwrap();

        // Should dispatch high first (higher weight → more deficit sooner).
        let first = s.next_task().unwrap();
        assert!(first.starts_with('h'));
    }

    #[test]
    fn queued_len_across_lanes() {
        let mut s = scheduler_with_lanes();
        s.enqueue("high", "t1").unwrap();
        s.enqueue("high", "t2").unwrap();
        s.enqueue("low", "t3").unwrap();
        assert_eq!(s.queued_len(), 3);
        assert_eq!(s.lane_len("high"), 2);
        assert_eq!(s.lane_len("low"), 1);
    }

    #[test]
    fn all_dispatched_returns_none() {
        let mut s = scheduler_with_lanes();
        s.enqueue("high", "t1").unwrap();
        s.enqueue("low", "t1").unwrap();

        let _ = s.next_task();
        let _ = s.next_task();
        assert!(s.next_task().is_none());
    }
}
