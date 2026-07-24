//! Custody record receiving: parse blackwall `RunRecord` output and update
//! escapement `DispatchRecord`s with output location and custody hash.
//!
//! The receive half of the blackwall bridge — processes completed runs:
//! - Parse the `RunRecord` (BLAKE3 hash, changeset ref, settle state)
//! - Update the `DispatchRecord` with output location and custody hash
//! - Propagate custody chain updates (child records carry parent custody ref)

use std::fmt;

use escapement_core::custody::{CustodyChain, CustodyRef};
use escapement_core::dispatch::TaskId;

/// Parsed from blackwall's run record output (JSON, no serde — manual parse).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    /// BLAKE3 content-addressed hash of the run.
    pub hash: CustodyRef,
    /// The task id this run was dispatched for.
    pub task_id: TaskId,
    /// The changeset reference (path-keyed blob hash diffs).
    pub changeset: Option<String>,
    /// The settle state of the run.
    pub settle_state: SettleState,
}

/// The settle state of a blackwall run, mapped from blackwall's settle
/// state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleState {
    /// Run completed but changeset not yet reviewed.
    Pending,
    /// Changeset staged for review.
    Selected,
    /// Changeset released (dropped from staging).
    Released,
    /// Changeset discarded.
    Discarded,
    /// Changeset applied to the world DAG (terminal success).
    Applied,
}

impl SettleState {
    /// Whether this state is terminal (no further transitions).
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Released | Self::Discarded | Self::Applied)
    }

    /// Whether this state represents a successful settle.
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Applied)
    }
}

impl fmt::Display for SettleState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Selected => write!(f, "selected"),
            Self::Released => write!(f, "released"),
            Self::Discarded => write!(f, "discarded"),
            Self::Applied => write!(f, "applied"),
        }
    }
}

impl SettleState {
    /// Parse from a blackwall JSON string.
    fn from_str(s: &str) -> Self {
        match s {
            "selected" => Self::Selected,
            "released" => Self::Released,
            "discarded" => Self::Discarded,
            "applied" => Self::Applied,
            _ => Self::Pending,
        }
    }
}

/// Errors from custody record receiving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiveError {
    /// The blackwall output could not be parsed.
    ParseError(String),
    /// The task id is not registered in the custody chain.
    UnknownTask(TaskId),
    /// The run hash is invalid (empty or whitespace).
    InvalidHash,
}

impl fmt::Display for ReceiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParseError(msg) => write!(f, "parse error: {msg}"),
            Self::UnknownTask(id) => write!(f, "unknown task in custody chain: {id}"),
            Self::InvalidHash => write!(f, "invalid run hash"),
        }
    }
}

impl std::error::Error for ReceiveError {}

/// Parse a `RunRecord` from blackwall's JSON output.
///
/// Expected format (`BLACKWALL_OUTPUT=json` blackwall run show <hash>):
/// ```json
/// {"hash":"abc123","record":{"prompt":"...","model":"...","exit_code":0,...},
///  "context":{"run":"abc123","parent_world":"root","changeset":"","settle_state":{"status":"unsettled"}}}
/// ```
///
/// The `task_id` is NOT present in blackwall's output — the caller must
/// supply it (the task that dispatched this run).
///
/// # Errors
/// Returns [`ReceiveError::ParseError`] if the output cannot be parsed,
/// [`ReceiveError::InvalidHash`] if the hash is empty.
pub fn parse_run_record(output: &str, task_id: &TaskId) -> Result<RunRecord, ReceiveError> {
    let hash = parse_json_string_field(output, "hash")
        .ok_or_else(|| ReceiveError::ParseError("missing hash field".into()))?;
    if hash.is_empty() {
        return Err(ReceiveError::InvalidHash);
    }

    // changeset is nested in context.changeset — the string search finds it
    // anywhere in the output. Empty string → None (no changeset).
    let changeset = parse_json_string_field(output, "changeset").filter(|s| !s.is_empty());

    // settle state status is in context.settle_state.status — search for the
    // "status" field. If absent (context is null), default to Pending.
    let settle_state = parse_json_string_field(output, "status")
        .map_or(SettleState::Pending, |s| SettleState::from_str(&s));

    Ok(RunRecord {
        hash: CustodyRef::new(hash).map_err(|_| ReceiveError::InvalidHash)?,
        task_id: task_id.clone(),
        changeset,
        settle_state,
    })
}

/// Update a `DispatchRecord` in the custody chain with a received `RunRecord`.
///
/// This sets the record's output custody ref and marks it as having a
/// completed run. If the run is settled (Applied), the custody chain is
/// ready for propagation to child tasks.
///
/// # Errors
/// Returns [`ReceiveError::UnknownTask`] if the task is not in the chain.
pub fn update_custody(chain: &mut CustodyChain, record: &RunRecord) -> Result<(), ReceiveError> {
    let dispatch_record = chain
        .get(&record.task_id)
        .ok_or_else(|| ReceiveError::UnknownTask(record.task_id.clone()))?;

    // Clone and update the record with the run's output.
    let mut updated = dispatch_record.clone();
    updated.set_output(record.hash.clone());
    chain.register(updated);

    Ok(())
}

/// Extract a string field value from a simple JSON object (no nesting).
/// Returns `None` if the field is not found.
fn parse_json_string_field(output: &str, field: &str) -> Option<String> {
    let marker = format!("\"{field}\":\"");
    let start = output.find(&marker)?;
    let rest = &output[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use escapement_core::custody::DispatchRecord;
    #[test]
    fn parse_run_record_complete() {
        let output = r#"{"hash":"abc123","record":{"prompt":"...","model":"zai/GLM-5.2","stdout":"...","stderr":"","exit_code":0,"usage":null},"context":{"run":"abc123","parent_world":"root","changeset":"def456","settle_state":{"status":"applied"}}}"#;
        let task_id = TaskId::new("t1").unwrap();
        let record = parse_run_record(output, &task_id).unwrap();
        assert_eq!(record.hash.as_str(), "abc123");
        assert_eq!(record.task_id.as_str(), "t1");
        assert_eq!(record.changeset.as_deref(), Some("def456"));
        assert_eq!(record.settle_state, SettleState::Applied);
    }

    #[test]
    fn parse_run_record_empty_changeset_is_none() {
        let output = r#"{"hash":"abc","record":{"exit_code":0},"context":{"run":"abc","parent_world":"root","changeset":"","settle_state":{"status":"selected"}}}"#;
        let task_id = TaskId::new("t1").unwrap();
        let record = parse_run_record(output, &task_id).unwrap();
        assert!(record.changeset.is_none());
        assert_eq!(record.settle_state, SettleState::Selected);
    }

    #[test]
    fn parse_run_record_null_context_defaults_pending() {
        let output = r#"{"hash":"abc","record":{"exit_code":0},"context":null}"#;
        let task_id = TaskId::new("t1").unwrap();
        let record = parse_run_record(output, &task_id).unwrap();
        assert_eq!(record.settle_state, SettleState::Pending);
        assert!(record.changeset.is_none());
    }

    #[test]
    fn parse_run_record_missing_hash_errors() {
        let output = r#"{"record":{"exit_code":0},"context":null}"#;
        let task_id = TaskId::new("t1").unwrap();
        assert!(parse_run_record(output, &task_id).is_err());
    }

    #[test]
    fn parse_run_record_empty_hash_errors() {
        let output = r#"{"hash":"","record":{"exit_code":0},"context":null}"#;
        let task_id = TaskId::new("t1").unwrap();
        assert!(parse_run_record(output, &task_id).is_err());
    }

    #[test]
    fn update_custody_sets_output() {
        let mut chain = CustodyChain::new();
        let record = DispatchRecord::root(TaskId::new("t1").unwrap(), "operator");
        chain.register(record);

        let run_record = RunRecord {
            hash: CustodyRef::new("blake3hash").unwrap(),
            task_id: TaskId::new("t1").unwrap(),
            changeset: Some("changeset123".into()),
            settle_state: SettleState::Applied,
        };

        update_custody(&mut chain, &run_record).unwrap();

        let updated = chain.get(&TaskId::new("t1").unwrap()).unwrap();
        assert_eq!(
            updated.output.as_ref().map(CustodyRef::as_str),
            Some("blake3hash")
        );
    }

    #[test]
    fn update_custody_unknown_task_errors() {
        let mut chain = CustodyChain::new();
        let run_record = RunRecord {
            hash: CustodyRef::new("hash").unwrap(),
            task_id: TaskId::new("ghost").unwrap(),
            changeset: None,
            settle_state: SettleState::Pending,
        };
        assert!(update_custody(&mut chain, &run_record).is_err());
    }

    #[test]
    fn settle_state_from_str() {
        assert_eq!(SettleState::from_str("pending"), SettleState::Pending);
        assert_eq!(SettleState::from_str("selected"), SettleState::Selected);
        assert_eq!(SettleState::from_str("released"), SettleState::Released);
        assert_eq!(SettleState::from_str("discarded"), SettleState::Discarded);
        assert_eq!(SettleState::from_str("applied"), SettleState::Applied);
        assert_eq!(SettleState::from_str("unknown"), SettleState::Pending);
    }

    #[test]
    fn settle_state_is_terminal() {
        assert!(SettleState::Released.is_terminal());
        assert!(SettleState::Discarded.is_terminal());
        assert!(SettleState::Applied.is_terminal());
        assert!(!SettleState::Pending.is_terminal());
        assert!(!SettleState::Selected.is_terminal());
    }

    #[test]
    fn settle_state_is_success() {
        assert!(SettleState::Applied.is_success());
        assert!(!SettleState::Pending.is_success());
        assert!(!SettleState::Discarded.is_success());
    }

    #[test]
    fn settle_state_display() {
        assert_eq!(SettleState::Applied.to_string(), "applied");
        assert_eq!(SettleState::Pending.to_string(), "pending");
    }
}
