//! Telemetry types: structured span/event model for OTLP export.
//!
//! Zero-dependency telemetry types that represent the span hierarchy and
//! metrics for dispatch lifecycle events. The actual OTLP exporter (HTTP
//! client) is a separate concern — this module provides the typed data
//! model that gets serialized for export.

use std::collections::BTreeMap;
use std::fmt;

use crate::dispatch::{TaskId, TaskState};

/// A telemetry span — a unit of work in the dispatch lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetrySpan {
    /// Span name (e.g., "dispatch", "plan", "node", "settle").
    pub name: String,
    /// The task id this span is for.
    pub task_id: Option<TaskId>,
    /// The parent span id (if nested).
    pub parent_span: Option<String>,
    /// Span attributes (key-value pairs).
    pub attributes: BTreeMap<String, String>,
}

impl TelemetrySpan {
    /// Create a dispatch span.
    #[must_use]
    pub fn dispatch(task_id: TaskId) -> Self {
        let mut attrs = BTreeMap::new();
        attrs.insert("task_id".into(), task_id.to_string());
        Self {
            name: "dispatch".into(),
            task_id: Some(task_id),
            parent_span: None,
            attributes: attrs,
        }
    }

    /// Create a plan span.
    #[must_use]
    pub fn plan(task_id: TaskId) -> Self {
        let mut attrs = BTreeMap::new();
        attrs.insert("task_id".into(), task_id.to_string());
        Self {
            name: "plan".into(),
            task_id: Some(task_id),
            parent_span: None,
            attributes: attrs,
        }
    }

    /// Set the parent span for nesting.
    #[must_use]
    pub fn with_parent(mut self, parent: impl Into<String>) -> Self {
        self.parent_span = Some(parent.into());
        self
    }

    /// Add an attribute.
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.attributes.insert(key.into(), value.into());
    }
}

/// A telemetry metric — a named measurement for Prometheus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryMetric {
    /// Metric name (e.g., "`queue_depth`", "`dispatch_latency`").
    pub name: String,
    /// Metric value.
    pub value: u64,
    /// Labels (key-value pairs).
    pub labels: BTreeMap<String, String>,
}

/// A structured log event for the dispatch lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryLog {
    /// The task id this log is for.
    pub task_id: TaskId,
    /// The lifecycle event type.
    pub event: DispatchEvent,
    /// The task state at the time of the event.
    pub state: TaskState,
    /// Additional fields.
    pub fields: BTreeMap<String, String>,
}

/// Dispatch lifecycle event types for structured logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchEvent {
    Admitted,
    Assigned,
    Started,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    Retried,
}

impl fmt::Display for DispatchEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admitted => write!(f, "admitted"),
            Self::Assigned => write!(f, "assigned"),
            Self::Started => write!(f, "started"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::TimedOut => write!(f, "timed_out"),
            Self::Retried => write!(f, "retried"),
        }
    }
}

/// The OTLP configuration — where telemetry gets exported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpConfig {
    /// The OTLP endpoint URL.
    pub endpoint: String,
    /// The service name reported in spans.
    pub service_name: String,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:4318/v1/traces".into(),
            service_name: "escapement".into(),
        }
    }
}

/// Metrics that the dispatcher should track.
#[derive(Debug, Clone, Default)]
pub struct DispatchMetrics {
    /// Current queue depth.
    pub queue_depth: u64,
    /// Total dispatches made.
    pub total_dispatches: u64,
    /// Total completions.
    pub total_completions: u64,
    /// Total failures.
    pub total_failures: u64,
    /// Total timeouts.
    pub total_timeouts: u64,
    /// Total retries.
    pub total_retries: u64,
    /// Total cancellations.
    pub total_cancellations: u64,
}

impl DispatchMetrics {
    /// Record a dispatch event.
    pub fn record_dispatch(&mut self) {
        self.total_dispatches += 1;
    }

    /// Record a completion.
    pub fn record_completion(&mut self) {
        self.total_completions += 1;
    }

    /// Record a failure.
    pub fn record_failure(&mut self) {
        self.total_failures += 1;
    }

    /// Record a timeout.
    pub fn record_timeout(&mut self) {
        self.total_timeouts += 1;
    }

    /// Record a retry.
    pub fn record_retry(&mut self) {
        self.total_retries += 1;
    }

    /// Record a cancellation.
    pub fn record_cancellation(&mut self) {
        self.total_cancellations += 1;
    }

    /// Collect all metrics as a list of `TelemetryMetric`.
    #[must_use]
    pub fn collect(&self) -> Vec<TelemetryMetric> {
        vec![
            TelemetryMetric {
                name: "escapement_queue_depth".into(),
                value: self.queue_depth,
                labels: BTreeMap::new(),
            },
            TelemetryMetric {
                name: "escapement_dispatches_total".into(),
                value: self.total_dispatches,
                labels: BTreeMap::new(),
            },
            TelemetryMetric {
                name: "escapement_completions_total".into(),
                value: self.total_completions,
                labels: BTreeMap::new(),
            },
            TelemetryMetric {
                name: "escapement_failures_total".into(),
                value: self.total_failures,
                labels: BTreeMap::new(),
            },
            TelemetryMetric {
                name: "escapement_timeouts_total".into(),
                value: self.total_timeouts,
                labels: BTreeMap::new(),
            },
            TelemetryMetric {
                name: "escapement_retries_total".into(),
                value: self.total_retries,
                labels: BTreeMap::new(),
            },
            TelemetryMetric {
                name: "escapement_cancellations_total".into(),
                value: self.total_cancellations,
                labels: BTreeMap::new(),
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::TaskId;

    #[test]
    fn dispatch_span_has_task_id() {
        let span = TelemetrySpan::dispatch(TaskId::new("t1").unwrap());
        assert_eq!(span.name, "dispatch");
        assert!(span.attributes.contains_key("task_id"));
    }

    #[test]
    fn plan_span_has_parent() {
        let span = TelemetrySpan::plan(TaskId::new("t1").unwrap()).with_parent("parent-span-id");
        assert_eq!(span.parent_span, Some("parent-span-id".into()));
    }

    #[test]
    fn span_set_attribute() {
        let mut span = TelemetrySpan::dispatch(TaskId::new("t1").unwrap());
        span.set_attribute("latency_ms", "42");
        assert_eq!(span.attributes.get("latency_ms"), Some(&"42".to_string()));
    }

    #[test]
    fn dispatch_event_display() {
        assert_eq!(DispatchEvent::Completed.to_string(), "completed");
        assert_eq!(DispatchEvent::TimedOut.to_string(), "timed_out");
        assert_eq!(DispatchEvent::Retried.to_string(), "retried");
    }

    #[test]
    fn metrics_record_and_collect() {
        let mut m = DispatchMetrics::default();
        m.record_dispatch();
        m.record_dispatch();
        m.record_completion();
        m.record_failure();
        m.record_timeout();
        m.record_retry();
        m.record_cancellation();

        let metrics = m.collect();
        assert_eq!(metrics.len(), 7);
        assert_eq!(m.total_dispatches, 2);
        assert_eq!(m.total_completions, 1);
        assert_eq!(m.total_failures, 1);
        assert_eq!(m.total_timeouts, 1);
        assert_eq!(m.total_retries, 1);
        assert_eq!(m.total_cancellations, 1);
    }

    #[test]
    fn otlp_config_default() {
        let config = OtlpConfig::default();
        assert_eq!(config.service_name, "escapement");
        assert!(config.endpoint.contains("alloy-otlp"));
    }

    #[test]
    fn telemetry_metric_has_name_and_value() {
        let metric = TelemetryMetric {
            name: "queue_depth".into(),
            value: 5,
            labels: BTreeMap::new(),
        };
        assert_eq!(metric.name, "queue_depth");
        assert_eq!(metric.value, 5);
    }

    #[test]
    fn telemetry_log_with_fields() {
        use crate::registry::AgentId;
        let log = TelemetryLog {
            task_id: TaskId::new("t1").unwrap(),
            event: DispatchEvent::Completed,
            state: TaskState::Completed(AgentId::new("a1").unwrap()),
            fields: BTreeMap::new(),
        };
        assert_eq!(log.event, DispatchEvent::Completed);
    }
}
