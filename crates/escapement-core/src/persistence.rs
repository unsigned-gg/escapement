//! Persistent queue state: a write-ahead log (WAL) for the dispatcher's
//! queue mutations, enabling crash recovery without losing work.
//!
//! The WAL is append-only — every queue mutation (submit, dispatch,
//! complete, fail, cancel, timeout) is logged before it's applied. On
//! restart, the WAL is replayed to reconstruct the dispatcher state.
//!
//! The persistence layer uses a simple line-based format (NDJSON) that
//! can be written to any `Write` and read from any `Read`. The actual
//! storage (file, `SQLite`, blackwall's content-addressed store) is
//! pluggable — this module provides the serialization and replay logic.
//!
//! # Format
//! Each WAL entry is a single line of JSON:
//! ```text
//! {"op":"submit","task_id":"t1","capability":"build","priority":5}
//! {"op":"dispatch","task_id":"t1","agent_id":"a1"}
//! {"op":"complete","task_id":"t1","agent_id":"a1"}
//! ```
//!
//! Time is caller-supplied (no wall clock in the core), so timestamps
//! are not part of the WAL — the caller can correlate via external logs.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, BufRead, Write};

use crate::dispatch::TaskId;
use crate::registry::AgentId;

/// A single WAL entry — a queue mutation to persist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    pub op: WalOp,
    pub task_id: TaskId,
    pub agent_id: Option<AgentId>,
    pub capability: Option<String>,
    pub priority: Option<u8>,
}

/// The type of queue mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOp {
    /// A task was submitted to the queue.
    Submit,
    /// A task was dispatched to an agent.
    Dispatch,
    /// A task completed successfully.
    Complete,
    /// A task failed.
    Fail,
    /// A task was cancelled.
    Cancel,
    /// A task timed out.
    Timeout,
}

impl fmt::Display for WalOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Submit => write!(f, "submit"),
            Self::Dispatch => write!(f, "dispatch"),
            Self::Complete => write!(f, "complete"),
            Self::Fail => write!(f, "fail"),
            Self::Cancel => write!(f, "cancel"),
            Self::Timeout => write!(f, "timeout"),
        }
    }
}

impl WalOp {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "submit" => Some(Self::Submit),
            "dispatch" => Some(Self::Dispatch),
            "complete" => Some(Self::Complete),
            "fail" => Some(Self::Fail),
            "cancel" => Some(Self::Cancel),
            "timeout" => Some(Self::Timeout),
            _ => None,
        }
    }
}

impl WalEntry {
    /// Create a submit entry.
    #[must_use]
    pub fn submit(task_id: TaskId, capability: impl Into<String>, priority: u8) -> Self {
        Self {
            op: WalOp::Submit,
            task_id,
            agent_id: None,
            capability: Some(capability.into()),
            priority: Some(priority),
        }
    }

    /// Create a dispatch entry.
    #[must_use]
    pub fn dispatch(task_id: TaskId, agent_id: AgentId) -> Self {
        Self {
            op: WalOp::Dispatch,
            task_id,
            agent_id: Some(agent_id),
            capability: None,
            priority: None,
        }
    }

    /// Create a complete entry.
    #[must_use]
    pub fn complete(task_id: TaskId, agent_id: AgentId) -> Self {
        Self {
            op: WalOp::Complete,
            task_id,
            agent_id: Some(agent_id),
            capability: None,
            priority: None,
        }
    }
}

/// Serialize a WAL entry to a single JSON line (no trailing newline).
#[must_use]
pub fn serialize_entry(entry: &WalEntry) -> String {
    let mut parts = vec![
        format!(r#""op":"{}""#, entry.op),
        format!(r#""task_id":"{}""#, entry.task_id),
    ];

    if let Some(ref agent_id) = entry.agent_id {
        parts.push(format!(r#""agent_id":"{agent_id}""#));
    }
    if let Some(ref cap) = entry.capability {
        parts.push(format!(r#""capability":"{cap}""#));
    }
    if let Some(pri) = entry.priority {
        parts.push(format!(r#""priority":{pri}"#));
    }

    format!("{{{{{}}}}}", parts.join(","))
}

/// Write a WAL entry to a writer (with trailing newline).
///
/// # Errors
/// Returns [`io::Error`] if the write fails.
pub fn write_entry(writer: &mut impl Write, entry: &WalEntry) -> io::Result<()> {
    writer.write_all(serialize_entry(entry).as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()
}

/// Deserialize a WAL entry from a single JSON line.
///
/// # Errors
/// Returns [`PersistenceError`] if the line cannot be parsed.
pub fn deserialize_entry(line: &str) -> Result<WalEntry, PersistenceError> {
    let line = line.trim();
    if line.is_empty() {
        return Err(PersistenceError::EmptyLine);
    }

    let op_str = parse_json_field(line, "op")
        .ok_or_else(|| PersistenceError::ParseError("missing op field".into()))?;
    let op = WalOp::from_str(&op_str)
        .ok_or_else(|| PersistenceError::ParseError(format!("unknown op: {op_str}")))?;

    let task_id_str = parse_json_field(line, "task_id")
        .ok_or_else(|| PersistenceError::ParseError("missing task_id field".into()))?;
    let task_id = TaskId::new(task_id_str)
        .map_err(|_| PersistenceError::ParseError("invalid task_id".into()))?;

    let agent_id = parse_json_field(line, "agent_id")
        .map(|s| {
            AgentId::new(s).map_err(|_| PersistenceError::ParseError("invalid agent_id".into()))
        })
        .transpose()?;

    let capability = parse_json_field(line, "capability");
    let priority = parse_json_numeric_field(line, "priority");

    Ok(WalEntry {
        op,
        task_id,
        agent_id,
        capability,
        priority,
    })
}

/// Replay a WAL from a reader, collecting entries in order.
///
/// # Errors
/// Returns [`PersistenceError`] if any line cannot be parsed.
pub fn replay(reader: impl BufRead) -> Result<Vec<WalEntry>, PersistenceError> {
    let mut entries = Vec::new();
    for line in reader.lines() {
        let line = line.map_err(|e| PersistenceError::Io(e.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }
        entries.push(deserialize_entry(&line)?);
    }
    Ok(entries)
}

/// Reconstruct the dispatcher state from a replayed WAL.
///
/// Returns:
/// - `task_specs`: the tasks that were submitted (id → (capability, priority))
/// - `completed`: task ids that completed
/// - `dispatched`: task ids currently dispatched (in flight at crash time)
#[must_use]
pub fn reconstruct(entries: &[WalEntry]) -> RecoveredState {
    let mut task_specs: BTreeMap<TaskId, (String, u8)> = BTreeMap::new();
    let mut completed: Vec<TaskId> = Vec::new();
    let mut dispatched: Vec<TaskId> = Vec::new();

    for entry in entries {
        match entry.op {
            WalOp::Submit => {
                if let (Some(cap), Some(pri)) = (&entry.capability, entry.priority) {
                    task_specs.insert(entry.task_id.clone(), (cap.clone(), pri));
                }
            }
            WalOp::Dispatch => {
                dispatched.push(entry.task_id.clone());
            }
            WalOp::Complete => {
                dispatched.retain(|id| id != &entry.task_id);
                completed.push(entry.task_id.clone());
            }
            WalOp::Fail | WalOp::Timeout | WalOp::Cancel => {
                dispatched.retain(|id| id != &entry.task_id);
            }
        }
    }

    RecoveredState {
        task_specs,
        completed,
        in_flight: dispatched,
    }
}

/// The reconstructed dispatcher state after replaying a WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredState {
    /// Tasks that were submitted, with their specs.
    pub task_specs: BTreeMap<TaskId, (String, u8)>,
    /// Tasks that completed before the crash.
    pub completed: Vec<TaskId>,
    /// Tasks that were in-flight (dispatched but not completed) at crash time.
    /// These should be requeued on recovery.
    pub in_flight: Vec<TaskId>,
}

/// Persistence errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistenceError {
    /// An empty line was encountered where a WAL entry was expected.
    EmptyLine,
    /// A WAL entry could not be parsed.
    ParseError(String),
    /// An I/O error occurred.
    Io(String),
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyLine => write!(f, "empty WAL line"),
            Self::ParseError(msg) => write!(f, "WAL parse error: {msg}"),
            Self::Io(msg) => write!(f, "WAL I/O error: {msg}"),
        }
    }
}

impl std::error::Error for PersistenceError {}

/// Extract a string field value from a simple JSON object.
fn parse_json_field(output: &str, field: &str) -> Option<String> {
    let marker = format!("\"{field}\":\"");
    let start = output.find(&marker)?;
    let rest = &output[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Extract a numeric field value from a simple JSON object.
fn parse_json_numeric_field(output: &str, field: &str) -> Option<u8> {
    let marker = format!("\"{field}\":");
    let start = output.find(&marker)?;
    let rest = &output[start + marker.len()..];
    // Read until we hit a non-digit character.
    let end = rest.find(|c: char| !c.is_ascii_digit())?;
    rest[..end].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_submit_entry() {
        let entry = WalEntry::submit(TaskId::new("t1").unwrap(), "build", 5);
        let json = serialize_entry(&entry);
        assert!(json.contains(r#""op":"submit""#));
        assert!(json.contains(r#""task_id":"t1""#));
        assert!(json.contains(r#""capability":"build""#));
        assert!(json.contains(r#""priority":5"#));
    }

    #[test]
    fn serialize_dispatch_entry() {
        let entry = WalEntry::dispatch(TaskId::new("t1").unwrap(), AgentId::new("a1").unwrap());
        let json = serialize_entry(&entry);
        assert!(json.contains(r#""op":"dispatch""#));
        assert!(json.contains(r#""agent_id":"a1""#));
    }

    #[test]
    fn round_trip_serialize_deserialize() {
        let entry = WalEntry::submit(TaskId::new("t1").unwrap(), "build", 5);
        let json = serialize_entry(&entry);
        let parsed = deserialize_entry(&json).unwrap();
        assert_eq!(parsed, entry);
    }

    #[test]
    fn round_trip_dispatch() {
        let entry = WalEntry::dispatch(TaskId::new("t2").unwrap(), AgentId::new("a1").unwrap());
        let json = serialize_entry(&entry);
        let parsed = deserialize_entry(&json).unwrap();
        assert_eq!(parsed, entry);
    }

    #[test]
    fn deserialize_invalid_op_errors() {
        let line = r#"{"op":"unknown","task_id":"t1"}"#;
        assert!(deserialize_entry(line).is_err());
    }

    #[test]
    fn deserialize_missing_op_errors() {
        let line = r#"{"task_id":"t1"}"#;
        assert!(deserialize_entry(line).is_err());
    }

    #[test]
    fn deserialize_empty_line_errors() {
        assert!(deserialize_entry("").is_err());
    }

    #[test]
    fn reconstruct_from_wal() {
        let entries = vec![
            WalEntry::submit(TaskId::new("t1").unwrap(), "build", 5),
            WalEntry::submit(TaskId::new("t2").unwrap(), "review", 3),
            WalEntry::dispatch(TaskId::new("t1").unwrap(), AgentId::new("a1").unwrap()),
            WalEntry::complete(TaskId::new("t1").unwrap(), AgentId::new("a1").unwrap()),
            WalEntry::dispatch(TaskId::new("t2").unwrap(), AgentId::new("a1").unwrap()),
            // t2 is in flight (dispatched, not completed) — crash here.
        ];

        let state = reconstruct(&entries);
        assert_eq!(state.task_specs.len(), 2);
        assert_eq!(state.completed, vec![TaskId::new("t1").unwrap()]);
        assert_eq!(state.in_flight, vec![TaskId::new("t2").unwrap()]);
    }

    #[test]
    fn reconstruct_all_completed() {
        let entries = vec![
            WalEntry::submit(TaskId::new("t1").unwrap(), "build", 5),
            WalEntry::dispatch(TaskId::new("t1").unwrap(), AgentId::new("a1").unwrap()),
            WalEntry::complete(TaskId::new("t1").unwrap(), AgentId::new("a1").unwrap()),
        ];

        let state = reconstruct(&entries);
        assert!(state.in_flight.is_empty());
        assert_eq!(state.completed.len(), 1);
    }

    #[test]
    fn write_and_replay() {
        let mut buf = Vec::new();
        write_entry(
            &mut buf,
            &WalEntry::submit(TaskId::new("t1").unwrap(), "build", 5),
        )
        .unwrap();
        write_entry(
            &mut buf,
            &WalEntry::dispatch(TaskId::new("t1").unwrap(), AgentId::new("a1").unwrap()),
        )
        .unwrap();
        write_entry(
            &mut buf,
            &WalEntry::complete(TaskId::new("t1").unwrap(), AgentId::new("a1").unwrap()),
        )
        .unwrap();

        let cursor = io::Cursor::new(buf);
        let entries = replay(cursor).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].op, WalOp::Submit);
        assert_eq!(entries[1].op, WalOp::Dispatch);
        assert_eq!(entries[2].op, WalOp::Complete);
    }

    #[test]
    fn replay_skips_empty_lines() {
        let mut buf = Vec::new();
        write_entry(
            &mut buf,
            &WalEntry::submit(TaskId::new("t1").unwrap(), "build", 5),
        )
        .unwrap();
        buf.extend_from_slice(b"\n\n"); // empty lines
        write_entry(
            &mut buf,
            &WalEntry::complete(TaskId::new("t1").unwrap(), AgentId::new("a1").unwrap()),
        )
        .unwrap();

        let cursor = io::Cursor::new(buf);
        let entries = replay(cursor).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn wal_op_display() {
        assert_eq!(WalOp::Submit.to_string(), "submit");
        assert_eq!(WalOp::Complete.to_string(), "complete");
        assert_eq!(WalOp::Fail.to_string(), "fail");
    }

    #[test]
    fn idempotent_recovery_double_replay() {
        let entries = vec![
            WalEntry::submit(TaskId::new("t1").unwrap(), "build", 5),
            WalEntry::dispatch(TaskId::new("t1").unwrap(), AgentId::new("a1").unwrap()),
        ];

        let state1 = reconstruct(&entries);
        let state2 = reconstruct(&entries);
        assert_eq!(state1, state2);
    }
}
