//! blackwall-bridge: the integration layer between escapement-core and
//! blackwall's custody API.
//!
//! Maps an escapement [`Task`] to a blackwall run spec (prompt, provider,
//! model, max-turns, timeout), and maps blackwall's `RunRecord` (BLAKE3 hash)
//! back to an escapement [`CustodyRef`].
//!
//! The bridge uses `std::process::Command` to call the `blackwall` CLI —
//! zero external HTTP dependencies. A serve-API path (`POST /v1/mcp/tools/dispatch`)
//! is available behind the `serve` feature flag.

use std::fmt;
use std::process::Command;

pub mod execution;
pub mod receive;
#[cfg(feature = "serve")]
pub mod serve;
pub mod settle;
#[cfg(feature = "serve")]
pub use serve::{ServeBridge, ServeConfig};

use escapement_core::custody::CustodyRef;
use escapement_core::dispatch::{Task, TaskId};
use escapement_core::protocol::PROTOCOL_VERSION;

/// Configuration for the blackwall bridge.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// Path to the `blackwall` binary (default: "blackwall" from PATH).
    pub binary: String,
    /// Default provider for dispatched runs.
    pub default_provider: String,
    /// Default model for dispatched runs.
    pub default_model: String,
    /// Default max turns per run.
    pub default_max_turns: u32,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            binary: "blackwall".into(),
            default_provider: "gateway".into(),
            default_model: "zai/GLM-5.2".into(),
            default_max_turns: 20,
        }
    }
}

/// A handle to a dispatched blackwall run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunHandle {
    /// The escapement task id that dispatched this run.
    pub task_id: TaskId,
    /// The BLAKE3 hash of the blackwall run record.
    pub run_hash: CustodyRef,
}

impl RunHandle {
    /// Create a custody reference from this run's hash.
    #[must_use]
    pub fn custody_ref(&self) -> CustodyRef {
        self.run_hash.clone()
    }
}

/// The status of a blackwall run, as observed by the bridge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStatus {
    /// Run is still executing.
    Running,
    /// Run completed successfully (BLAKE3 hash available).
    Completed(CustodyRef),
    /// Run failed.
    Failed(String),
    /// Run not found.
    NotFound,
}

/// Errors from the blackwall bridge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeError {
    /// The `blackwall` binary is not on PATH or failed to execute.
    BinaryNotFound(String),
    /// The blackwall command returned a non-zero exit code.
    CommandFailed { code: i32, stderr: String },
    /// The blackwall output could not be parsed.
    ParseError(String),
    /// The task spec is missing required fields for a blackwall dispatch.
    InvalidTaskSpec(String),
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BinaryNotFound(msg) => write!(f, "blackwall binary not found: {msg}"),
            Self::CommandFailed { code, stderr } => {
                write!(f, "blackwall command failed (exit {code}): {stderr}")
            }
            Self::ParseError(msg) => write!(f, "blackwall output parse error: {msg}"),
            Self::InvalidTaskSpec(msg) => write!(f, "invalid task spec: {msg}"),
        }
    }
}

impl std::error::Error for BridgeError {}

/// The bridge between escapement and blackwall's custody API.
#[derive(Debug)]
pub struct BlackwallBridge {
    config: BridgeConfig,
}

impl BlackwallBridge {
    #[must_use]
    pub fn new(config: BridgeConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(BridgeConfig::default())
    }

    /// Build the blackwall run spec for an escapement task.
    ///
    /// # Errors
    /// Returns [`BridgeError::InvalidTaskSpec`] if the task is missing required
    /// metadata for blackwall dispatch.
    pub fn build_run_spec(&self, task: &Task) -> Result<RunSpec, BridgeError> {
        // The task's required_capability maps to the blackwall "task" name.
        let prompt = format!(
            "Dispatched by escapement (protocol {PROTOCOL_VERSION}) — task: {}, capability: {}",
            task.id, task.required_capability
        );

        Ok(RunSpec {
            task_name: task.id.as_str().to_string(),
            prompt,
            provider: self.config.default_provider.clone(),
            model: self.config.default_model.clone(),
            max_turns: self.config.default_max_turns,
        })
    }

    /// Spawn a blackwall run for the given task.
    ///
    /// # Errors
    /// Returns [`BridgeError`] if the binary is missing, the command fails, or
    /// the output cannot be parsed.
    pub fn spawn(&self, task: &Task) -> Result<RunHandle, BridgeError> {
        let spec = self.build_run_spec(task)?;

        let output = Command::new(&self.config.binary)
            .args(["run", "start"])
            .args(["--task", &spec.task_name])
            .args(["--provider", &spec.provider])
            .args(["--model", &spec.model])
            .args(["--max-turns", &spec.max_turns.to_string()])
            .env("BLACKWALL_OUTPUT", "json")
            .output()
            .map_err(|e| BridgeError::BinaryNotFound(e.to_string()))?;

        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(BridgeError::CommandFailed { code, stderr });
        }

        // Parse the BLAKE3 hash from the output.
        // blackwall run start (BLACKWALL_OUTPUT=json) outputs:
        // {"run":"<hash>","changeset":null,"world":"","parent":"root"}
        // We extract the "run" field (simple parsing, no serde dep).
        let stdout = String::from_utf8_lossy(&output.stdout);
        let run_hash = parse_run_hash(&stdout)?;

        Ok(RunHandle {
            task_id: task.id.clone(),
            run_hash: CustodyRef::new(run_hash)
                .map_err(|e| BridgeError::ParseError(e.to_string()))?,
        })
    }

    /// Check the status of a dispatched run.
    ///
    /// # Errors
    /// Returns [`BridgeError`] if the binary is missing or the command fails.
    pub fn poll(&self, handle: &RunHandle) -> Result<RunStatus, BridgeError> {
        let output = Command::new(&self.config.binary)
            .args(["run", "show"])
            .arg(handle.run_hash.as_str())
            .env("BLACKWALL_OUTPUT", "json")
            .output()
            .map_err(|e| BridgeError::BinaryNotFound(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            if stderr.contains("not found") || stderr.contains("No run") {
                return Ok(RunStatus::NotFound);
            }
            let code = output.status.code().unwrap_or(-1);
            return Err(BridgeError::CommandFailed { code, stderr });
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_run_status(&stdout, &handle.run_hash)
    }

    /// Map a blackwall run hash to an escapement custody reference.
    #[must_use]
    pub fn custody_ref(run_hash: impl Into<String>) -> Option<CustodyRef> {
        let hash = run_hash.into();
        if hash.is_empty() {
            return None;
        }
        CustodyRef::new(hash).ok()
    }
}

/// A blackwall run specification built from an escapement task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSpec {
    pub task_name: String,
    pub prompt: String,
    pub provider: String,
    pub model: String,
    pub max_turns: u32,
}

/// Parse the BLAKE3 run hash from blackwall's JSON output.
/// Without serde, we do a simple string search for the hash field.
fn parse_run_hash(output: &str) -> Result<String, BridgeError> {
    // blackwall run start (BLACKWALL_OUTPUT=json) outputs:
    // {"run":"<hash>","changeset":null,"world":"","parent":"root"}
    // Find the "run" field value (fall back to "hash" for run show output).
    let marker = "\"run\":\"";
    if let Some(start) = output.find(marker) {
        let rest = &output[start + marker.len()..];
        if let Some(end) = rest.find('"') {
            return Ok(rest[..end].to_string());
        }
    }
    // Also try "hash" field.
    let marker = "\"hash\":\"";
    if let Some(start) = output.find(marker) {
        let rest = &output[start + marker.len()..];
        if let Some(end) = rest.find('"') {
            return Ok(rest[..end].to_string());
        }
    }
    Err(BridgeError::ParseError(
        "could not find run hash in blackwall output".into(),
    ))
}

/// Parse a run's status from blackwall's `run show` JSON output.
///
/// Expected format (`BLACKWALL_OUTPUT=json` blackwall run show <hash>):
/// ```json
/// {"hash":"...","record":{"exit_code":0,...},"context":{"settle_state":{"status":"applied"}}}
/// ```
///
/// When `context` is null the run completed but has no settle state — treated
/// as completed (completion-only run).
fn parse_run_status(output: &str, custody_ref: &CustodyRef) -> Result<RunStatus, BridgeError> {
    // Extract record.exit_code (integer): 0 = success, !=0 = failed.
    let exit_code = parse_json_int_field(output, "exit_code")
        .ok_or_else(|| BridgeError::ParseError("missing exit_code field".into()))?;

    if exit_code != 0 {
        return Ok(RunStatus::Failed(format!("exit code {exit_code}")));
    }

    // Extract the settle state status from context.settle_state.status.
    // If absent (context is null — completion-only run), treat as completed.
    let settle_status = parse_json_string_field(output, "status");

    match settle_status.as_deref() {
        // exit_code == 0 and not yet settled → run done, custody available.
        // "applied" → settled, custody chain ready for propagation.
        // Both map to Completed.
        Some("applied" | "unsettled" | "selected" | "released" | "discarded") | None => {
            Ok(RunStatus::Completed(custody_ref.clone()))
        }
        Some(other) => Ok(RunStatus::Failed(format!("unknown settle state: {other}"))),
    }
}

/// Extract a string field value from a JSON object (no nesting).
/// Returns `None` if the field is not found.
fn parse_json_string_field(output: &str, field: &str) -> Option<String> {
    let marker = format!("\"{field}\":\"");
    let start = output.find(&marker)?;
    let rest = &output[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Extract an integer field value from a JSON object.
/// Returns `None` if the field is not found or not a valid integer.
fn parse_json_int_field(output: &str, field: &str) -> Option<i64> {
    let marker = format!("\"{field}\":");
    let start = output.find(&marker)?;
    let rest = &output[start + marker.len()..];
    // Skip whitespace and read digits (handle optional leading '-').
    let trimmed = rest.trim_start();
    let bytes = trimmed.as_bytes();
    let mut len = 0;
    if bytes.first() == Some(&b'-') {
        len = 1;
    }
    while len < bytes.len() && bytes[len].is_ascii_digit() {
        len += 1;
    }
    if len == 0 || (len == 1 && bytes[0] == b'-') {
        return None;
    }
    trimmed[..len].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: &str, capability: &str, priority: u8) -> Task {
        Task {
            id: TaskId::new(id).unwrap(),
            required_capability: capability.into(),
            priority,
        }
    }

    #[test]
    fn build_run_spec_from_task() {
        let bridge = BlackwallBridge::with_defaults();
        let task = task("t1", "build", 5);
        let spec = bridge.build_run_spec(&task).unwrap();
        assert_eq!(spec.task_name, "t1");
        assert_eq!(spec.provider, "gateway");
        assert!(spec.prompt.contains("escapement"));
    }

    #[test]
    fn custody_ref_from_hash() {
        let ref_opt = BlackwallBridge::custody_ref("blake3hash123");
        assert!(ref_opt.is_some());
        assert_eq!(ref_opt.unwrap().as_str(), "blake3hash123");
    }

    #[test]
    fn custody_ref_empty_returns_none() {
        assert!(BlackwallBridge::custody_ref("").is_none());
    }

    #[test]
    fn run_handle_custody_ref() {
        let handle = RunHandle {
            task_id: TaskId::new("t1").unwrap(),
            run_hash: CustodyRef::new("hash123").unwrap(),
        };
        assert_eq!(handle.custody_ref().as_str(), "hash123");
    }

    #[test]
    fn parse_run_hash_from_spawn_output() {
        let output = r#"{"run":"abc123def456","changeset":null,"world":"","parent":"root"}"#;
        assert_eq!(parse_run_hash(output).unwrap(), "abc123def456");
    }

    #[test]
    fn parse_run_hash_from_hash_field() {
        let output = r#"{"hash":"xyz789","record":{"exit_code":0},"context":null}"#;
        assert_eq!(parse_run_hash(output).unwrap(), "xyz789");
    }

    #[test]
    fn parse_run_hash_missing_fails() {
        let output = r#"{"changeset":null,"world":""}"#;
        assert!(parse_run_hash(output).is_err());
    }

    #[test]
    fn parse_run_status_completed_unsettled() {
        let output = r#"{"hash":"abc123","record":{"prompt":"...","model":"zai/GLM-5.2","stdout":"...","stderr":"","exit_code":0,"usage":null},"context":{"run":"abc123","parent_world":"root","changeset":"","settle_state":{"status":"unsettled"}}}"#;
        let custody_ref = CustodyRef::new("abc123").unwrap();
        let status = parse_run_status(output, &custody_ref).unwrap();
        assert_eq!(status, RunStatus::Completed(custody_ref.clone()));
    }

    #[test]
    fn parse_run_status_completed_applied() {
        let output = r#"{"hash":"abc123","record":{"exit_code":0},"context":{"run":"abc123","parent_world":"root","changeset":"","settle_state":{"status":"applied","from":"root","to":" commit123"}}}"#;
        let custody_ref = CustodyRef::new("abc123").unwrap();
        let status = parse_run_status(output, &custody_ref).unwrap();
        assert_eq!(status, RunStatus::Completed(custody_ref.clone()));
    }

    #[test]
    fn parse_run_status_failed_nonzero_exit() {
        let output = r#"{"hash":"abc123","record":{"exit_code":1,"stderr":"boom"},"context":{"settle_state":{"status":"unsettled"}}}"#;
        let custody_ref = CustodyRef::new("abc123").unwrap();
        let status = parse_run_status(output, &custody_ref).unwrap();
        assert!(matches!(status, RunStatus::Failed(_)));
    }

    #[test]
    fn parse_run_status_completed_no_context() {
        let output = r#"{"hash":"abc123","record":{"exit_code":0},"context":null}"#;
        let custody_ref = CustodyRef::new("abc123").unwrap();
        let status = parse_run_status(output, &custody_ref).unwrap();
        assert_eq!(status, RunStatus::Completed(custody_ref.clone()));
    }

    #[test]
    fn parse_run_status_missing_exit_code_errors() {
        let output = r#"{"hash":"abc123","record":{}}"#;
        let custody_ref = CustodyRef::new("abc123").unwrap();
        assert!(parse_run_status(output, &custody_ref).is_err());
    }

    #[test]
    fn bridge_config_defaults() {
        let config = BridgeConfig::default();
        assert_eq!(config.binary, "blackwall");
        assert_eq!(config.default_provider, "gateway");
        assert_eq!(config.default_model, "zai/GLM-5.2");
        assert_eq!(config.default_max_turns, 20);
    }

    #[test]
    fn run_spec_fields() {
        let spec = RunSpec {
            task_name: "t1".into(),
            prompt: "test prompt".into(),
            provider: "gateway".into(),
            model: "zai/GLM-5.2".into(),
            max_turns: 10,
        };
        assert_eq!(spec.task_name, "t1");
        assert_eq!(spec.max_turns, 10);
    }
}
