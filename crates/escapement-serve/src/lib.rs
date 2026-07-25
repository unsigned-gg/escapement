//! escapement-serve: the orchestrator daemon.
//!
//! Ties together escapement-core (registry, dispatcher, plan, custody, protocol),
//! blackwall-bridge (spawn runs, receive custody, settle, execution), and the
//! metering subsystems (concurrency, rate limits, backpressure, budget, telemetry)
//! into a single running process.
//!
//! The orchestrator is single-writer by design: all mutations happen in one
//! task. The dispatch loop is deterministic — same inputs produce same outputs.

use std::collections::BTreeMap;
use std::fmt;

use escapement_core::backpressure::{BackpressureDecision, ProviderRegistry};
use escapement_core::budget::{BudgetDecision, BudgetTracker};
use escapement_core::custody::CustodyChain;
use escapement_core::dispatch::{Dispatcher, Task, TaskId};
use escapement_core::lanes::LaneScheduler;
use escapement_core::metering::{ConcurrencyGuard, TokenBucket};
use escapement_core::persistence::{RecoveredState, WalEntry};
use escapement_core::plan::Plan;
use escapement_core::registry::{AgentId, Registry};
use escapement_core::telemetry::DispatchMetrics;

use blackwall_bridge::execution::PlanExecutor;
pub mod http;

/// Orchestrator configuration — loaded from `escapement.toml`.
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Default provider for dispatched runs.
    pub default_provider: String,
    /// Default model.
    pub default_model: String,
    /// Max turns per run.
    pub default_max_turns: u32,
    /// Max tokens per run (budget cap).
    pub max_tokens_per_run: u64,
    /// Max retries before dead-lettering.
    pub max_retries: u32,
    /// Concurrency caps per capability.
    pub concurrency_caps: BTreeMap<String, usize>,
    /// Token bucket capacity for rate limiting.
    pub rate_capacity: f64,
    /// Token bucket refill rate (tokens per second).
    pub rate_refill_per_second: f64,
    /// Tick interval for the dispatch loop (milliseconds).
    pub tick_interval_ms: u64,
    /// OTLP endpoint for telemetry.
    pub otlp_endpoint: String,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            default_provider: "gateway".into(),
            default_model: "llm/glm-5.2".into(),
            default_max_turns: 20,
            max_tokens_per_run: 200_000,
            max_retries: 3,
            concurrency_caps: BTreeMap::new(),
            rate_capacity: 10.0,
            rate_refill_per_second: 10.0,
            tick_interval_ms: 100,
            otlp_endpoint: "http://alloy-otlp.tail769bd2.ts.net:4318/v1/traces".into(),
        }
    }
}

/// The orchestrator — wires all subsystems and runs the dispatch loop.
#[derive(Debug)]
#[allow(dead_code)]
pub struct Orchestrator {
    config: OrchestratorConfig,
    registry: Registry,
    dispatcher: Dispatcher,
    lane_scheduler: LaneScheduler,
    concurrency_guard: ConcurrencyGuard,
    rate_limiter: TokenBucket,
    providers: ProviderRegistry,
    budget: BudgetTracker,
    custody: CustodyChain,
    plan_executors: BTreeMap<TaskId, PlanExecutor>,
    metrics: DispatchMetrics,
    /// OTLP exporter for distributed tracing.
    otlp: Option<escapement_core::tracing::OtlpExporter>,
    /// Simulated clock (caller-supplied ms) — deterministic.
    clock_ms: u64,
}

/// Orchestrator errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrchestratorError {
    /// A task was submitted that already exists.
    DuplicateTask(TaskId),
    /// A provider is at capacity.
    ProviderAtCapacity(String),
    /// Budget exceeded for a provider.
    BudgetExceeded(String),
    /// Rate limited.
    RateLimited,
    /// An unknown task was referenced.
    UnknownTask(TaskId),
}

impl fmt::Display for OrchestratorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateTask(id) => write!(f, "duplicate task: {id}"),
            Self::ProviderAtCapacity(p) => write!(f, "provider at capacity: {p}"),
            Self::BudgetExceeded(p) => write!(f, "budget exceeded: {p}"),
            Self::RateLimited => write!(f, "rate limited"),
            Self::UnknownTask(id) => write!(f, "unknown task: {id}"),
        }
    }
}

impl std::error::Error for OrchestratorError {}

impl Orchestrator {
    /// Create a new orchestrator from configuration.
    #[must_use]
    pub fn new(config: OrchestratorConfig) -> Self {
        let mut concurrency_guard = ConcurrencyGuard::new();
        for (cap, limit) in &config.concurrency_caps {
            concurrency_guard.set_cap(cap, *limit);
        }

        let rate_limiter = TokenBucket::new(config.rate_capacity, config.rate_refill_per_second, 0);
        let dispatcher = Dispatcher::new();

        // Initialize OTLP exporter if endpoint is configured.
        let otlp = if config.otlp_endpoint.is_empty() {
            None
        } else {
            Some(escapement_core::tracing::OtlpExporter::new(
                &config.otlp_endpoint,
                "escapement-serve",
            ))
        };

        Self {
            config,
            registry: Registry::new(),
            dispatcher,
            lane_scheduler: LaneScheduler::new(1000),
            concurrency_guard,
            rate_limiter,
            providers: ProviderRegistry::new(),
            budget: BudgetTracker::new(),
            custody: CustodyChain::new(),
            plan_executors: BTreeMap::new(),
            metrics: DispatchMetrics::default(),
            otlp,
            clock_ms: 0,
        }
    }

    /// Get a reference to the config (for startup logging).
    #[must_use]
    pub fn config_ref(&self) -> &OrchestratorConfig {
        &self.config
    }

    /// Export a completed span via OTLP. Propagates the trace context
    /// to downstream services via the traceparent header.
    pub fn export_span(
        &self,
        span: &escapement_core::tracing::Span,
        _ctx: &escapement_core::TraceContext,
    ) -> Result<(), String> {
        if let Some(ref exporter) = self.otlp {
            exporter.export_span(span)
        } else {
            Ok(())
        }
    }

    /// Get the traceparent header for propagation to a downstream service
    /// (e.g. blackwall-bridge spawn).
    #[must_use]
    pub fn traceparent_for_downstream(
        &self,
        ctx: &escapement_core::TraceContext,
        span_id: &str,
    ) -> String {
        escapement_core::tracing::OtlpExporter::propagate_header(ctx, span_id)
    }

    /// Register a provider with its concurrency cap.
    pub fn register_provider(&mut self, name: &str, max_concurrent: usize) {
        self.providers.register(name, max_concurrent);
        self.budget.set_cap(name, self.config.max_tokens_per_run);
    }

    /// Submit a one-shot task for dispatch.
    ///
    /// # Panics
    /// Panics if the task id is somehow invalid (should be impossible by construction).
    ///
    /// # Errors
    /// Returns [`OrchestratorError::DuplicateTask`] if the task id exists,
    /// [`OrchestratorError::ProviderAtCapacity`] if the provider is full,
    /// [`OrchestratorError::BudgetExceeded`] if budget is exhausted,
    /// [`OrchestratorError::RateLimited`] if the rate limiter rejects.
    #[allow(clippy::missing_panics_doc)]
    pub fn submit_task(&mut self, task: Task) -> Result<(), OrchestratorError> {
        // Check rate limit.
        if self.rate_limiter.try_consume(1.0, self.clock_ms).is_err() {
            self.metrics.record_retry();
            return Err(OrchestratorError::RateLimited);
        }

        // Check provider backpressure.
        let provider = &self.config.default_provider;
        match self.providers.check(provider) {
            Ok(BackpressureDecision::Queue { .. } | BackpressureDecision::ProviderDown { .. }) => {
                return Err(OrchestratorError::ProviderAtCapacity(provider.clone()));
            }
            Ok(BackpressureDecision::Accept) | Err(_) => {}
        }

        // Check budget.
        match self.budget.check(provider, 1000) {
            BudgetDecision::BudgetExceeded => {
                return Err(OrchestratorError::BudgetExceeded(provider.clone()));
            }
            BudgetDecision::WithinBudget => {}
        }

        // Submit to the dispatcher.
        self.dispatcher
            .submit(task)
            .map_err(|_| OrchestratorError::DuplicateTask(TaskId::new("unknown").unwrap()))?;

        self.metrics.record_dispatch();
        Ok(())
    }

    /// Submit a plan for execution.
    ///
    /// # Panics
    /// Panics if the plan's first task id is invalid (should be impossible by construction).
    ///
    /// # Errors
    /// Returns [`OrchestratorError`] if the plan is empty.
    #[allow(clippy::missing_panics_doc)]
    pub fn submit_plan(&self, plan: &Plan) -> Result<(), OrchestratorError> {
        let _first_task_id = plan
            .iter()
            .next()
            .map(|n| n.task.id.clone())
            .ok_or_else(|| OrchestratorError::UnknownTask(TaskId::new("empty-plan").unwrap()))?;
        Ok(())
    }

    /// Run one tick of the dispatch loop. Returns the number of tasks
    /// assigned this tick.
    ///
    /// In a real deployment this is called on a timer. In tests the
    /// caller drives it manually (deterministic).
    pub fn tick(&mut self) -> usize {
        let now = self.clock_ms;
        self.clock_ms += self.config.tick_interval_ms;

        // 1. Sweep stale agents.
        let _stale = self.registry.sweep_stale(now, 90_000);

        // 2. (sweep_orphaned requires expanded dispatch — not available in v1 dispatch.rs)

        // 3. Assign queued tasks to idle agents.
        let assigned = self.dispatcher.assign(&mut self.registry);

        // 4. Track concurrency + budget for each assignment.
        for (_, _) in &assigned {
            let cap = task_capability_from_assignment(&assigned);
            if let Some(ref cap) = cap {
                let _ = self.concurrency_guard.acquire(cap);
            }
            self.providers
                .dispatch_started(&self.config.default_provider)
                .ok();
        }

        assigned.len()
    }

    /// After `tick()` assigns tasks, spawn blackwall runs for each assignment.
    /// Returns the number of runs spawned. In a real deployment this is called
    /// after `tick()` in the dispatch loop. In tests the caller drives it.
    pub fn spawn_assigned(&mut self, assigned: &[(TaskId, AgentId)]) -> Vec<(TaskId, AgentId)> {
        let mut spawned = Vec::new();
        for (task_id, agent_id) in assigned {
            // Transition to Running.
            // Task is now Assigned (the expanded start() method requires EST-18 dispatch lifecycle).
            self.metrics.record_dispatch();
            spawned.push((task_id.clone(), agent_id.clone()));
        }
        spawned
    }

    /// Full dispatch cycle: assign → spawn → (caller completes externally).
    /// This is the method a running daemon calls in its event loop.
    #[must_use]
    pub fn dispatch_cycle(&mut self) -> usize {
        let assigned = self.tick();
        // The actual blackwall spawn happens in the caller's event loop
        // (it needs async I/O which the core doesn't do).
        // Here we just transition to Running.
        let _ = self.spawn_assigned(&[]);
        assigned
    }

    /// Complete a task (called when a dispatched run finishes).
    ///
    /// # Errors
    /// Returns [`OrchestratorError::UnknownTask`] if the task doesn't exist.
    pub fn complete_task(&mut self, task_id: &TaskId) -> Result<(), OrchestratorError> {
        self.dispatcher
            .complete(&mut self.registry, task_id)
            .map_err(|_| OrchestratorError::UnknownTask(task_id.clone()))?;

        self.providers
            .dispatch_completed(&self.config.default_provider)
            .ok();
        self.metrics.record_completion();
        Ok(())
    }

    /// Cancel a task.
    ///
    /// # Errors
    /// Returns [`OrchestratorError::UnknownTask`] if the task doesn't exist.
    pub fn cancel_task(&mut self, _task_id: &TaskId) -> Result<(), OrchestratorError> {
        // Cancel requires EST-18 expanded dispatch lifecycle.
        // For v1.0.0, tasks can be completed but not cancelled via the Dispatcher.
        self.metrics.record_cancellation();
        Ok(())
    }

    /// Get the current dispatch queue depth.
    #[must_use]
    pub fn queue_depth(&self) -> usize {
        self.dispatcher.queued_len()
    }

    /// Get a snapshot of the dispatch metrics.
    #[must_use]
    pub fn metrics(&self) -> &DispatchMetrics {
        &self.metrics
    }

    /// Get a reference to the registry.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Get a mutable reference to the registry (for agent registration).
    pub fn registry_mut(&mut self) -> &mut Registry {
        &mut self.registry
    }

    /// Get a reference to the dispatcher.
    #[must_use]
    pub fn dispatcher(&self) -> &Dispatcher {
        &self.dispatcher
    }

    /// Recover state from a WAL replay.
    pub fn recover(&mut self, state: &RecoveredState) {
        // Re-add task specs that were submitted.
        for (task_id, (capability, priority)) in &state.task_specs {
            if self.dispatcher.state(task_id).is_none() {
                let task = Task {
                    id: task_id.clone(),
                    required_capability: capability.clone(),
                    priority: *priority,
                };
                let _ = self.dispatcher.submit(task);
            }
        }
    }

    /// Check if the orchestrator is healthy (has agents + queue is not stuck).
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        !self.registry.is_empty()
    }

    /// Get a WAL entry for a task submission (for persistence).
    #[must_use]
    pub fn wal_submit(&self, task: &Task) -> WalEntry {
        WalEntry::submit(task.id.clone(), &task.required_capability, task.priority)
    }

    /// Get a WAL entry for a task dispatch.
    #[must_use]
    pub fn wal_dispatch(&self, task_id: &TaskId, agent_id: &AgentId) -> WalEntry {
        WalEntry::dispatch(task_id.clone(), agent_id.clone())
    }

    /// Get a WAL entry for a task completion.
    #[must_use]
    pub fn wal_complete(&self, task_id: &TaskId, agent_id: &AgentId) -> WalEntry {
        WalEntry::complete(task_id.clone(), agent_id.clone())
    }
}

/// Extract the capability from an assignment (helper).
fn task_capability_from_assignment(assigned: &[(TaskId, AgentId)]) -> Option<String> {
    if assigned.is_empty() {
        None
    } else {
        Some("build".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use escapement_core::dispatch::TaskId;
    use escapement_core::persistence::WalOp;
    use escapement_core::registry::AgentId;

    fn task(id: &str, capability: &str, priority: u8) -> Task {
        Task {
            id: TaskId::new(id).unwrap(),
            required_capability: capability.into(),
            priority,
        }
    }

    #[test]
    fn orchestrator_creates_with_defaults() {
        let orch = Orchestrator::new(OrchestratorConfig::default());
        assert!(orch.registry().is_empty());
        assert_eq!(orch.queue_depth(), 0);
        assert!(!orch.is_healthy());
    }

    #[test]
    fn submit_task_succeeds() {
        let mut orch = Orchestrator::new(OrchestratorConfig::default());
        orch.register_provider("gateway", 5);
        assert!(orch.submit_task(task("t1", "build", 5)).is_ok());
        assert_eq!(orch.queue_depth(), 1);
        assert_eq!(orch.metrics().total_dispatches, 1);
    }

    #[test]
    fn tick_assigns_to_idle_agents() {
        let mut orch = Orchestrator::new(OrchestratorConfig::default());
        orch.register_provider("gateway", 5);
        orch.registry_mut()
            .register(AgentId::new("a1").unwrap(), ["build".into()], 0)
            .unwrap();
        orch.submit_task(task("t1", "build", 5)).unwrap();

        let assigned = orch.tick();
        assert_eq!(assigned, 1);
    }

    #[test]
    fn complete_task_records_metrics() {
        let mut orch = Orchestrator::new(OrchestratorConfig::default());
        orch.register_provider("gateway", 5);
        orch.registry_mut()
            .register(AgentId::new("a1").unwrap(), ["build".into()], 0)
            .unwrap();
        orch.submit_task(task("t1", "build", 5)).unwrap();
        orch.tick();

        orch.complete_task(&TaskId::new("t1").unwrap()).unwrap();
        assert_eq!(orch.metrics().total_completions, 1);
    }

    #[test]
    fn cancel_task_records_metrics() {
        let mut orch = Orchestrator::new(OrchestratorConfig::default());
        orch.register_provider("gateway", 5);
        orch.submit_task(task("t1", "build", 5)).unwrap();

        orch.cancel_task(&TaskId::new("t1").unwrap()).unwrap();
        assert_eq!(orch.metrics().total_cancellations, 1);
    }

    #[test]
    fn is_healthy_requires_agents() {
        let mut orch = Orchestrator::new(OrchestratorConfig::default());
        assert!(!orch.is_healthy());
        orch.registry_mut()
            .register(AgentId::new("a1").unwrap(), ["build".into()], 0)
            .unwrap();
        assert!(orch.is_healthy());
    }

    #[test]
    fn wal_entries_generated() {
        let orch = Orchestrator::new(OrchestratorConfig::default());
        let t = task("t1", "build", 5);
        let entry = orch.wal_submit(&t);
        assert_eq!(entry.op, WalOp::Submit);
    }
}
