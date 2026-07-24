//! Serve-API bridge: the same spawn/poll/settle interface as the CLI bridge,
//! but against blackwall's HTTP serve API instead of shelling out to the
//! `blackwall` CLI.
//!
//! Behind the `serve` feature flag — adds `reqwest`/`serde`/`serde_json` as
//! optional dependencies. The default build (no features) pulls in zero new
//! dependencies.
//!
//! # Lifecycle model
//!
//! The serve dispatch endpoint (`POST /v1/mcp/tools/dispatch`) is **async**:
//! it enqueues a job and returns a `job_id` + `prompt_hash` immediately — the
//! `job_id` is NOT a blackwall run hash. A `LocalExecutor` must drain the queue
//! to produce the actual BLAKE3 run. Because of this asynchronous split:
//!
//! - [`ServeBridge::spawn`] dispatches the job and returns a [`RunHandle`]
//!   whose `run_hash` carries the **`job_id`** (NOT a BLAKE3 hash). The caller
//!   must drive a `LocalExecutor` separately to drain the queue.
//! - [`ServeBridge::poll`] is a **no-op** that always returns
//!   [`RunStatus::Running`]. The serve API exposes no job-status endpoint, so
//!   the bridge cannot observe queue progress. The caller drives the executor
//!   and then calls [`ServeBridge::receive`] with the resulting real run hash.
//! - [`ServeBridge::receive`] calls `inspect` by run hash and parses the full
//!   [`RunRecord`].
//! - [`ServeBridge::settle`] calls `request_settlement`.
//!
//! [`RunStatus::Running`]: crate::RunStatus::Running
//! [`RunRecord`]: crate::receive::RunRecord

use std::fmt;
use std::time::Duration;

use escapement_core::custody::CustodyRef;
use escapement_core::dispatch::{Task, TaskId};
use escapement_core::protocol::PROTOCOL_VERSION;

use crate::receive::RunRecord;
use crate::receive::SettleState;
use crate::BridgeError;
use crate::RunHandle;
use crate::RunStatus;

/// Default request timeout for serve API calls (30 s).
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Configuration for the serve-API blackwall bridge.
#[derive(Debug, Clone)]
pub struct ServeConfig {
    /// Base URL of the blackwall serve API (e.g. `http://127.0.0.1:7437`).
    pub base_url: String,
    /// Optional Bearer token for the `Authorization` header.
    pub token: Option<String>,
    /// Default project name for dispatch/inspect/settlement calls.
    pub project: String,
    /// Default provider for dispatched runs.
    pub default_provider: String,
    /// Default model for dispatched runs.
    pub default_model: String,
    /// Default max turns per run.
    pub default_max_turns: u32,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:7437".into(),
            token: None,
            project: "default".into(),
            default_provider: "gateway".into(),
            default_model: "zai/GLM-5.2".into(),
            default_max_turns: 20,
        }
    }
}

impl ServeConfig {
    /// Join a relative path onto the configured base URL.
    fn url(&self, path: &str) -> String {
        let base = self.base_url.trim_end_matches('/');
        format!("{base}{path}")
    }
}

/// The serve-API bridge between escapement and blackwall's custody API.
///
/// Wraps a [`reqwest::Client`] and [`ServeConfig`]. See the
/// [module docs](self) for the lifecycle model — notably that `spawn` is
/// async-queued and `poll` is a no-op.
#[derive(Debug, Clone)]
pub struct ServeBridge {
    config: ServeConfig,
    client: reqwest::Client,
}

impl ServeBridge {
    /// Build a bridge from a [`ServeConfig`], constructing a new reqwest
    /// client with the default timeout.
    ///
    /// # Errors
    /// Returns [`ServeError::ClientBuild`] if the reqwest client cannot be
    /// constructed (e.g. invalid TLS backend).
    pub fn new(config: ServeConfig) -> Result<Self, ServeError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .build()
            .map_err(|e| ServeError::ClientBuild(e.to_string()))?;
        Ok(Self { config, client })
    }

    /// Construct a bridge with default config (local serve at :7437).
    ///
    /// # Errors
    /// Returns [`ServeError::ClientBuild`] if the reqwest client cannot be
    /// constructed.
    pub fn with_defaults() -> Result<Self, ServeError> {
        Self::new(ServeConfig::default())
    }

    /// Inject a pre-built reqwest client (primarily for tests).
    #[must_use]
    pub fn with_client(config: ServeConfig, client: reqwest::Client) -> Self {
        Self { config, client }
    }

    /// A reference to the bridge's config.
    #[must_use]
    pub fn config(&self) -> &ServeConfig {
        &self.config
    }

    /// Build the dispatch request body for an escapement task.
    ///
    /// # Errors
    /// Returns [`ServeError::InvalidTask`] if the task id is empty.
    pub fn build_dispatch_request(&self, task: &Task) -> Result<DispatchRequest, ServeError> {
        let prompt = format!(
            "Dispatched by escapement (protocol {PROTOCOL_VERSION}) — task: {}, capability: {}",
            task.id, task.required_capability
        );
        Ok(DispatchRequest {
            project: self.config.project.clone(),
            prompt,
            task: Some(task.id.to_string()),
            provider: self.config.default_provider.clone(),
            model: self.config.default_model.clone(),
            max_tokens: None,
            budget_tokens: None,
            no_jail: false,
            parent: None,
            expected_paths: None,
            var: Vec::new(),
            max_turns: self.config.default_max_turns,
            timeout_secs: None,
        })
    }

    /// Dispatch a blackwall run for the given task via the serve API.
    ///
    /// POSTs to `/v1/mcp/tools/dispatch`. The serve dispatch is **async**:
    /// it returns a `job_id` + `prompt_hash` immediately — a
    /// `LocalExecutor` must drain the queue to produce the real BLAKE3 run
    /// hash. The returned [`RunHandle`] carries the **`job_id`** in its
    /// `run_hash` field (NOT a BLAKE3 hash). The caller drives the executor
    /// separately, then calls [`Self::receive`] with the resulting run hash.
    ///
    /// # Errors
    /// Returns [`ServeError`] on transport failure, non-2xx response, or
    /// response parse failure.
    pub async fn spawn(&self, task: &Task) -> Result<RunHandle, ServeError> {
        let body = self.build_dispatch_request(task)?;
        let resp = self.post_json("/v1/mcp/tools/dispatch", &body).await?;
        let parsed: DispatchResponse = parse_json(&resp).map_err(|e| ServeError::parse(&e))?;

        // The serve dispatch returns a job_id (ULID), not a BLAKE3 run hash.
        // Store the job_id in the handle's run_hash as a placeholder; the
        // caller drives the executor and then calls receive() with the real
        // run hash. Document this clearly.
        let custody = CustodyRef::new(parsed.job_id.clone())
            .map_err(|_| ServeError::invalid_job_id(&parsed.job_id))?;
        Ok(RunHandle {
            task_id: task.id.clone(),
            run_hash: custody,
        })
    }

    /// Check the status of a dispatched run.
    ///
    /// **This is a no-op that always returns [`RunStatus::Running`].** The
    /// serve dispatch is async-queued: the API exposes no job-status endpoint,
    /// so the bridge cannot observe queue progress. The caller must drive a
    /// `LocalExecutor` to drain the queue, then call [`Self::receive`] with
    /// the resulting real run hash.
    ///
    /// The `handle` argument is accepted for interface parity with the CLI
    /// bridge and is intentionally unused here.
    #[allow(unused_variables, clippy::unused_async)]
    pub async fn poll(&self, handle: &RunHandle) -> Result<RunStatus, ServeError> {
        // Async dispatch: the serve API cannot report job progress. The
        // caller drives the executor and then calls receive() with the run
        // hash. Until then, the job is considered Running.
        Ok(RunStatus::Running)
    }

    /// Receive a completed run record by its BLAKE3 run hash.
    ///
    /// POSTs to `/v1/mcp/tools/inspect`. The `run_hash` must be the REAL run
    /// hash produced by the executor (NOT the `job_id` placeholder returned by
    /// [`Self::spawn`]). Parses the response into a [`RunRecord`].
    ///
    /// # Errors
    /// Returns [`ServeError`] on transport failure, non-2xx response, or
    /// response parse failure.
    pub async fn receive(&self, run_hash: &str) -> Result<RunRecord, ServeError> {
        let body = InspectRequest {
            project: self.config.project.clone(),
            run: run_hash.to_string(),
        };
        let resp = self.post_json("/v1/mcp/tools/inspect", &body).await?;
        Self::parse_inspect_response(&resp, run_hash)
    }

    /// Request a settlement action on a run.
    ///
    /// POSTs to `/v1/mcp/tools/request_settlement`. Valid `action`s are
    /// `select`, `release`, `discard` (NOT `apply` — operator-only).
    ///
    /// # Errors
    /// Returns [`ServeError`] on transport failure, non-2xx response, or
    /// response parse failure.
    pub async fn settle(
        &self,
        run_hash: &str,
        action: &str,
    ) -> Result<SettlementResponse, ServeError> {
        let body = SettlementRequest {
            project: self.config.project.clone(),
            run: run_hash.to_string(),
            action: action.to_string(),
        };
        let resp = self
            .post_json("/v1/mcp/tools/request_settlement", &body)
            .await?;
        let parsed: SettlementResponse = parse_json(&resp).map_err(|e| ServeError::parse(&e))?;
        Ok(parsed)
    }

    /// Build an authenticated request builder, setting the Bearer token
    /// header if configured.
    fn request(&self, method: reqwest::Method, url: &str) -> reqwest::RequestBuilder {
        let mut req = self.client.request(method, url);
        if let Some(token) = &self.config.token {
            req = req.bearer_auth(token);
        }
        req
    }

    /// POST a JSON body and return the response text. Errors on non-2xx.
    async fn post_json<T: serde::Serialize>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<String, ServeError> {
        let url = self.config.url(path);
        let resp = self
            .request(reqwest::Method::POST, &url)
            .json(body)
            .send()
            .await
            .map_err(|e| ServeError::transport(path, &e))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(ServeError::status(path, status.as_u16(), text));
        }
        resp.text()
            .await
            .map_err(|e| ServeError::transport(path, &e))
    }

    /// Parse an inspect response body into a [`RunRecord`].
    ///
    /// The serve `inspect` response shape mirrors `blackwall run show --json`:
    /// `{"hash":"...","record":{...},"context":{...}}` (context may be null).
    /// The full record/context is untyped JSON; we extract the fields the
    /// [`RunRecord`] type needs.
    fn parse_inspect_response(body: &str, fallback_hash: &str) -> Result<RunRecord, ServeError> {
        let value: serde_json::Value = parse_json(body).map_err(|e| ServeError::parse(&e))?;

        // Prefer top-level "hash"; fall back to context.run; finally to the
        // caller-supplied fallback (the run hash we inspected by).
        let hash_str = value
            .get("hash")
            .and_then(|v| v.as_str())
            .or_else(|| {
                value
                    .get("context")
                    .and_then(|c| c.get("run"))
                    .and_then(|r| r.as_str())
            })
            .unwrap_or(fallback_hash);

        let hash = CustodyRef::new(hash_str).map_err(|_| ServeError::invalid_hash(hash_str))?;

        // record.prompt holds the original dispatch prompt, which embeds the
        // task id (see build_dispatch_request). Fall back to the hash if the
        // prompt can't be parsed for a task id.
        let task_id_str = value
            .get("record")
            .and_then(|r| r.get("prompt"))
            .and_then(|p| p.as_str())
            .and_then(extract_task_id_from_prompt)
            .map_or_else(|| hash_str.to_string(), str::to_string);
        let task_id = TaskId::new(task_id_str)
            .map_err(|_| ServeError::parse_str("invalid task id in inspect response"))?;

        // changeset lives on the context (may be null / absent / empty).
        let changeset = value
            .get("context")
            .and_then(|c| c.get("changeset"))
            .and_then(|cs| cs.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        // settle_state.status lives on the context (unsettled if absent).
        let settle_state = value
            .get("context")
            .and_then(|c| c.get("settle_state"))
            .and_then(|ss| ss.get("status"))
            .and_then(|s| s.as_str())
            .map_or(SettleState::Pending, settle_state_from_status);

        Ok(RunRecord {
            hash,
            task_id,
            changeset,
            settle_state,
        })
    }
}

/// Extract the escapement task id embedded in a dispatch prompt.
///
/// `build_dispatch_request` writes prompts of the form
/// "Dispatched by escapement (protocol X) — task: <id>, capability: <cap>".
/// Returns the task id segment, or `None` if the prompt doesn't match.
fn extract_task_id_from_prompt(prompt: &str) -> Option<&str> {
    let marker = "task: ";
    let start = prompt.find(marker)? + marker.len();
    let rest = &prompt[start..];
    let end = rest.find(", ").unwrap_or(rest.len());
    let id = &rest[..end];
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

/// Map a blackwall settle-state status string to a [`SettleState`].
///
/// Local copy of the mapping (receive.rs keeps its `from_str` module-private).
/// Unknown statuses map to [`SettleState::Pending`].
fn settle_state_from_status(s: &str) -> SettleState {
    match s {
        "selected" => SettleState::Selected,
        "released" => SettleState::Released,
        "discarded" => SettleState::Discarded,
        "applied" => SettleState::Applied,
        _ => SettleState::Pending,
    }
}

/// POST `/v1/mcp/tools/dispatch` request body.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DispatchRequest {
    /// Project name.
    pub project: String,
    /// The dispatch prompt (embeds the task id; see `build_dispatch_request`).
    pub prompt: String,
    /// The blackwall task name (escapement task id), or null.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,
    /// The provider for the run.
    pub provider: String,
    /// The model for the run.
    pub model: String,
    /// Optional max tokens cap.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    /// Optional budget tokens cap.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_tokens: Option<u64>,
    /// If true, skip the prompt jail.
    pub no_jail: bool,
    /// Optional parent run hash.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// Optional expected output paths.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_paths: Option<Vec<String>>,
    /// Template variables.
    pub var: Vec<String>,
    /// Max turns per run.
    pub max_turns: u32,
    /// Optional timeout in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

/// POST `/v1/mcp/tools/dispatch` response body.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct DispatchResponse {
    /// ULID job id (NOT a BLAKE3 run hash).
    pub job_id: String,
    /// SHA-256 of the dispatch prompt.
    pub prompt_hash: String,
    /// Path where the job was queued.
    pub queued_at: String,
}

/// POST `/v1/mcp/tools/inspect` request body.
#[derive(Debug, Clone, serde::Serialize)]
pub struct InspectRequest {
    /// Project name.
    pub project: String,
    /// Run hash or prefix to inspect.
    pub run: String,
}

/// POST `/v1/mcp/tools/request_settlement` request body.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SettlementRequest {
    /// Project name.
    pub project: String,
    /// Run hash to settle.
    pub run: String,
    /// Settlement action: `select`, `release`, or `discard`.
    pub action: String,
}

/// POST `/v1/mcp/tools/request_settlement` response body.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct SettlementResponse {
    /// The settled run hash.
    pub run_hash: String,
    /// The context hash.
    pub context_hash: String,
    /// The resulting settle state.
    pub settle_state: SettlementStateValue,
}

/// The `settle_state` object within a settlement/inspect response.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct SettlementStateValue {
    /// One of `unsettled`, `selected`, `released`, `discarded`, `applied`.
    pub status: String,
}

/// Errors from the serve-API bridge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServeError {
    /// The reqwest client could not be built.
    ClientBuild(String),
    /// A network/transport error occurred.
    Transport { path: String, msg: String },
    /// The serve API returned a non-2xx status.
    Status {
        path: String,
        code: u16,
        body: String,
    },
    /// The response body could not be parsed.
    ResponseParse(String),
    /// The `job_id` returned by dispatch was empty/invalid.
    InvalidJobId(String),
    /// The run hash was invalid (empty or whitespace).
    InvalidHash(String),
    /// The task spec is missing required fields for a blackwall dispatch.
    InvalidTask(String),
}

impl ServeError {
    fn transport(path: &str, e: &reqwest::Error) -> Self {
        Self::Transport {
            path: path.to_string(),
            msg: e.to_string(),
        }
    }

    fn status(path: &str, code: u16, body: String) -> Self {
        Self::Status {
            path: path.to_string(),
            code,
            body,
        }
    }

    fn parse(e: &serde_json::Error) -> Self {
        Self::ResponseParse(e.to_string())
    }

    fn parse_str(msg: &str) -> Self {
        Self::ResponseParse(msg.to_string())
    }

    fn invalid_job_id(job_id: &str) -> Self {
        Self::InvalidJobId(job_id.to_string())
    }

    fn invalid_hash(hash: &str) -> Self {
        Self::InvalidHash(hash.to_string())
    }
}

impl fmt::Display for ServeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientBuild(msg) => write!(f, "reqwest client build failed: {msg}"),
            Self::Transport { path, msg } => write!(f, "serve transport error ({path}): {msg}"),
            Self::Status { path, code, body } => {
                write!(f, "serve API {path} returned {code}: {body}")
            }
            Self::ResponseParse(msg) => write!(f, "serve response parse error: {msg}"),
            Self::InvalidJobId(id) => {
                write!(f, "serve dispatch returned invalid job_id: {id:?}")
            }
            Self::InvalidHash(hash) => write!(f, "invalid run hash: {hash:?}"),
            Self::InvalidTask(msg) => write!(f, "invalid task spec: {msg}"),
        }
    }
}

impl std::error::Error for ServeError {}

impl From<ServeError> for BridgeError {
    fn from(e: ServeError) -> Self {
        Self::ParseError(e.to_string())
    }
}

/// Parse a JSON string into a deserializable type (no-alloc wrapper).
fn parse_json<T: serde::de::DeserializeOwned>(s: &str) -> Result<T, serde_json::Error> {
    serde_json::from_str(s)
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
    fn serve_config_defaults() {
        let config = ServeConfig::default();
        assert_eq!(config.base_url, "http://127.0.0.1:7437");
        assert!(config.token.is_none());
        assert_eq!(config.project, "default");
        assert_eq!(config.default_provider, "gateway");
        assert_eq!(config.default_model, "zai/GLM-5.2");
        assert_eq!(config.default_max_turns, 20);
    }

    #[test]
    fn serve_config_url_joins_paths() {
        let config = ServeConfig::default();
        assert_eq!(
            config.url("/v1/mcp/tools/dispatch"),
            "http://127.0.0.1:7437/v1/mcp/tools/dispatch"
        );
        // Trailing slash on base is trimmed.
        let config = ServeConfig {
            base_url: "http://127.0.0.1:7437/".into(),
            ..ServeConfig::default()
        };
        assert_eq!(
            config.url("/v1/mcp/tools/inspect"),
            "http://127.0.0.1:7437/v1/mcp/tools/inspect"
        );
    }

    #[test]
    fn build_dispatch_request_serializes_full_body() {
        let bridge = ServeBridge::with_defaults().unwrap();
        let task = task("t1", "build", 5);
        let req = bridge.build_dispatch_request(&task).unwrap();

        assert_eq!(req.project, "default");
        assert_eq!(req.task.as_deref(), Some("t1"));
        assert_eq!(req.provider, "gateway");
        assert_eq!(req.model, "zai/GLM-5.2");
        assert_eq!(req.max_turns, 20);
        assert!(!req.no_jail);
        assert!(req.max_tokens.is_none());
        assert!(req.budget_tokens.is_none());
        assert!(req.parent.is_none());
        assert!(req.expected_paths.is_none());
        assert!(req.timeout_secs.is_none());
        assert!(req.var.is_empty());
        assert!(req.prompt.contains("escapement"));
        assert!(req.prompt.contains("task: t1"));
        assert!(req.prompt.contains("capability: build"));
    }

    #[test]
    fn dispatch_request_json_matches_contract() {
        let bridge = ServeBridge::with_defaults().unwrap();
        let task = task("spawn-1", "review", 3);
        let req = bridge.build_dispatch_request(&task).unwrap();
        let json: serde_json::Value =
            serde_json::to_value(&req).expect("DispatchRequest serializes");

        // Required fields per the serve contract.
        assert_eq!(json["project"], "default");
        assert_eq!(json["prompt"], req.prompt);
        assert_eq!(json["task"], "spawn-1");
        assert_eq!(json["provider"], "gateway");
        assert_eq!(json["model"], "zai/GLM-5.2");
        assert_eq!(json["no_jail"], false);
        assert_eq!(json["var"], serde_json::json!([]));
        assert_eq!(json["max_turns"], 20);
        // Null-valued optional fields are omitted by skip_serializing_if.
        assert!(json.get("max_tokens").is_none());
        assert!(json.get("budget_tokens").is_none());
        assert!(json.get("parent").is_none());
        assert!(json.get("expected_paths").is_none());
        assert!(json.get("timeout_secs").is_none());
    }

    #[test]
    fn dispatch_response_parses_from_mock_json() {
        let mock = serde_json::json!({
            "job_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "prompt_hash": "9c1185a5c5e9fc54612808977ee8f548b2258d31",
            "queued_at": "/var/lib/blackwall/queue/spawn-1"
        });
        let resp: DispatchResponse = serde_json::from_value(mock).expect("DispatchResponse parses");
        assert_eq!(resp.job_id, "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(resp.prompt_hash, "9c1185a5c5e9fc54612808977ee8f548b2258d31");
        assert_eq!(resp.queued_at, "/var/lib/blackwall/queue/spawn-1");
    }

    #[test]
    fn dispatch_response_deserializes_from_json_string() {
        let json = r#"{
            "job_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "prompt_hash": "9c1185a5c5e9fc54612808977ee8f548b2258d31",
            "queued_at": "/var/lib/blackwall/queue/spawn-1"
        }"#;
        let parsed: DispatchResponse = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.job_id, "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(
            parsed.prompt_hash,
            "9c1185a5c5e9fc54612808977ee8f548b2258d31"
        );
        assert_eq!(parsed.queued_at, "/var/lib/blackwall/queue/spawn-1");
    }

    #[test]
    fn inspect_request_serializes() {
        let req = InspectRequest {
            project: "default".into(),
            run: "deadbeef".into(),
        };
        let json: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(json["project"], "default");
        assert_eq!(json["run"], "deadbeef");
    }

    #[test]
    fn settlement_request_serializes() {
        let req = SettlementRequest {
            project: "default".into(),
            run: "deadbeef".into(),
            action: "select".into(),
        };
        let json: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(json["project"], "default");
        assert_eq!(json["run"], "deadbeef");
        assert_eq!(json["action"], "select");
    }

    #[test]
    fn settlement_response_parses_from_mock_json() {
        let mock = serde_json::json!({
            "run_hash": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "context_hash": "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
            "settle_state": {"status": "selected"}
        });
        let resp: SettlementResponse =
            serde_json::from_value(mock).expect("SettlementResponse parses");
        assert_eq!(
            resp.run_hash,
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
        assert_eq!(
            resp.context_hash,
            "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe"
        );
        assert_eq!(resp.settle_state.status, "selected");
    }

    #[test]
    fn parse_inspect_response_full_record_and_context() {
        // Mirrors the verified serve inspect shape from bridge-contract.md.
        let mock = serde_json::json!({
            "hash": "abc123def456abc123def456abc123def456abc123def456abc123def456abcd",
            "record": {
                "prompt": "Dispatched by escapement (protocol 1) — task: t9, capability: review",
                "model": "zai/GLM-5.2",
                "stdout": "done",
                "stderr": "",
                "exit_code": 0,
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
            },
            "context": {
                "run": "abc123def456",
                "parent_world": "root",
                "changeset": "cs-hash-1",
                "settle_state": {"status": "selected"}
            }
        });
        let body = serde_json::to_string(&mock).unwrap();
        let record = ServeBridge::parse_inspect_response(&body, "abc123def456").unwrap();

        assert_eq!(
            record.hash.as_str(),
            "abc123def456abc123def456abc123def456abc123def456abc123def456abcd"
        );
        assert_eq!(record.task_id.as_str(), "t9");
        assert_eq!(record.changeset.as_deref(), Some("cs-hash-1"));
        assert_eq!(record.settle_state, SettleState::Selected);
    }

    #[test]
    fn parse_inspect_response_null_context_is_unsettled() {
        let mock = serde_json::json!({
            "hash": "abc123def456abc123def456abc123def456abc123def456abc123def456abcd",
            "record": {
                "prompt": "Dispatched by escapement (protocol 1) — task: t-init, capability: boot",
                "model": "zai/GLM-5.2",
                "stdout": "",
                "stderr": "",
                "exit_code": 0,
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            },
            "context": null
        });
        let body = serde_json::to_string(&mock).unwrap();
        let record = ServeBridge::parse_inspect_response(&body, "abc123def456").unwrap();

        assert_eq!(record.changeset, None);
        assert_eq!(record.settle_state, SettleState::Pending);
        assert_eq!(record.task_id.as_str(), "t-init");
    }

    #[test]
    fn parse_inspect_response_falls_back_to_context_run_hash() {
        // Top-level "hash" absent → use context.run.
        let mock = serde_json::json!({
            "record": {
                "prompt": "Dispatched by escapement (protocol 1) — task: fallback, capability: x",
                "model": "zai/GLM-5.2",
                "stdout": "",
                "stderr": "",
                "exit_code": 0,
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            },
            "context": {
                "run": "conthash123",
                "parent_world": "root",
                "changeset": "",
                "settle_state": {"status": "unsettled"}
            }
        });
        let body = serde_json::to_string(&mock).unwrap();
        let record = ServeBridge::parse_inspect_response(&body, "ignored").unwrap();

        // Empty changeset string → None.
        assert_eq!(record.changeset, None);
        assert_eq!(record.settle_state, SettleState::Pending);
        assert_eq!(record.task_id.as_str(), "fallback");
        assert_eq!(record.hash.as_str(), "conthash123");
    }

    #[test]
    fn parse_inspect_response_empty_hash_is_error() {
        let mock = serde_json::json!({
            "hash": "   ",
            "record": {
                "prompt": "Dispatched by escapement (protocol 1) — task: t, capability: cap",
                "model": "zai/GLM-5.2",
                "stdout": "",
                "stderr": "",
                "exit_code": 0,
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            },
            "context": null
        });
        let body = serde_json::to_string(&mock).unwrap();
        // Fallback is also whitespace → invalid.
        let err = ServeBridge::parse_inspect_response(&body, "   ").unwrap_err();
        assert!(matches!(err, ServeError::InvalidHash(_)));
    }

    #[test]
    fn parse_inspect_response_missing_task_in_prompt_uses_hash_as_task_id() {
        // A prompt that doesn't match the escapement dispatch format falls back
        // to the run hash as the task id, when its non-empty.
        let mock = serde_json::json!({
            "hash": "validhash",
            "record": {
                "prompt": "not an escapement prompt",
                "model": "zai/GLM-5.2",
                "stdout": "",
                "stderr": "",
                "exit_code": 0,
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            },
            "context": null
        });
        let body = serde_json::to_string(&mock).unwrap();
        let record = ServeBridge::parse_inspect_response(&body, "validhash").unwrap();
        assert_eq!(record.task_id.as_str(), "validhash");
    }

    #[test]
    fn extract_task_id_from_prompt_parses_task_ids() {
        // Standard format: task + capability both present.
        assert_eq!(
            extract_task_id_from_prompt(
                "Dispatched by escapement (protocol 1) — task: t42, capability: build"
            ),
            Some("t42")
        );
        // No capability segment → take the rest of the string.
        assert_eq!(
            extract_task_id_from_prompt("Dispatched by escapement (protocol 1) — task: solo"),
            Some("solo")
        );
        // No task marker.
        assert_eq!(extract_task_id_from_prompt("unrelated prompt"), None);
        // Empty task id after marker.
        assert_eq!(
            extract_task_id_from_prompt(
                "Dispatched by escapement (protocol 1) — task: , capability: build"
            ),
            None
        );
    }

    #[test]
    fn serve_error_display_covers_all_variants() {
        let cases = [
            (ServeError::ClientBuild("tls".into()), "tls"),
            (
                ServeError::Transport {
                    path: "/p".into(),
                    msg: "dnf".into(),
                },
                "dnf",
            ),
            (
                ServeError::Status {
                    path: "/p".into(),
                    code: 500,
                    body: "boom".into(),
                },
                "boom",
            ),
            (ServeError::ResponseParse("bad".into()), "bad"),
            (ServeError::InvalidJobId("".into()), "job_id"),
            (ServeError::InvalidHash("h".into()), "hash"),
            (ServeError::InvalidTask("missing".into()), "missing"),
        ];
        for (err, needle) in cases {
            let s = err.to_string();
            assert!(
                s.contains(needle),
                "display for {err:?} missing {needle:?}: {s}"
            );
        }
    }

    #[test]
    fn serve_error_converts_to_bridge_error() {
        let err: BridgeError = ServeError::Status {
            path: "/x".into(),
            code: 503,
            body: "unavailable".into(),
        }
        .into();
        assert!(matches!(err, BridgeError::ParseError(_)));
        assert!(err.to_string().contains("503"));
    }

    #[test]
    fn serve_bridge_with_client_injects_custom_client() {
        let config = ServeConfig::default();
        let client = reqwest::Client::new();
        let bridge = ServeBridge::with_client(config, client);
        assert_eq!(bridge.config().default_model, "zai/GLM-5.2");
        assert_eq!(bridge.config().project, "default");
    }

    #[test]
    fn settlement_state_value_parses_all_statuses() {
        for status in ["unsettled", "selected", "released", "discarded", "applied"] {
            let json = serde_json::json!({"status": status});
            let v: SettlementStateValue = serde_json::from_value(json).unwrap();
            assert_eq!(v.status, status);
        }
    }
}
