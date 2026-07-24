//! W3C Trace Context + OTLP span propagation.
//!
//! Accepts inbound `traceparent` headers (W3C Trace Context format:
//! `00-{trace-id}-{parent-span-id}-{flags}`), creates child spans for
//! dispatch operations, and prepares the outbound traceparent for
//! propagation to downstream services (e.g. blackwall-bridge spawn).
//!
//! This module is the fix for the estate-wide defect where nobody
//! propagates context (80% of llm traces are orphans). Escapement sits
//! between reverie-guard (decide) and blackwall (custody/execute) —
//! it must propagate context across the seam.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::dispatch::TaskId;

/// W3C Trace Context traceparent header value.
/// Format: `00-{trace-id(32 hex)}-{parent-id(16 hex)}-{flags(2 hex)}`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    /// 32-char hex trace ID.
    pub trace_id: String,
    /// 16-char hex span ID of the parent.
    pub parent_span_id: String,
    /// 2-char hex flags (e.g. `01` = sampled).
    pub flags: String,
}

impl TraceContext {
    /// Parse a `traceparent` header value.
    ///
    /// # Errors
    /// Returns `None` if the header is malformed.
    #[must_use]
    pub fn from_header(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.trim().split('-').collect();
        if parts.len() != 4 || parts[0] != "00" {
            return None;
        }
        let trace_id = parts[1];
        let parent_span_id = parts[2];
        let flags = parts[3];
        // W3C requires: trace-id = 32 hex (not all zeros), span-id = 16 hex (not all zeros)
        if trace_id.len() != 32 || trace_id.chars().all(|c| c == '0') {
            return None;
        }
        if parent_span_id.len() != 16 || parent_span_id.chars().all(|c| c == '0') {
            return None;
        }
        if flags.len() != 2 || !flags.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        Some(Self {
            trace_id: trace_id.to_lowercase(),
            parent_span_id: parent_span_id.to_lowercase(),
            flags: flags.to_lowercase(),
        })
    }

    /// Generate a new span ID (16 hex chars, not all zeros).
    #[must_use]
    pub fn new_span_id() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        // Use the nanosecond timestamp + a counter-like approach.
        // This is deterministic enough for v1 — not cryptographically random.
        format!("{now:016x}")[..16].to_lowercase()
    }

    /// Create a child span under this trace context.
    #[must_use]
    pub fn child_span(&self, name: &str, task_id: &TaskId) -> Span {
        Span {
            trace_id: self.trace_id.clone(),
            span_id: Self::new_span_id(),
            parent_span_id: self.parent_span_id.clone(),
            flags: self.flags.clone(),
            name: name.to_string(),
            task_id: task_id.clone(),
            attributes: BTreeMap::new(),
            start_time_unix_nano: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            end_time_unix_nano: 0,
        }
    }

    /// Create a traceparent header for propagation to a downstream service.
    /// Uses the given span ID as the new parent.
    #[must_use]
    pub fn to_header(&self, span_id: &str) -> String {
        format!("00-{}-{}-{}", self.trace_id, span_id, self.flags)
    }
}

impl fmt::Display for TraceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "00-{}-{}-{}",
            self.trace_id, self.parent_span_id, self.flags
        )
    }
}

/// A span — a unit of work in the dispatch lifecycle.
/// This struct is designed for OTLP export (serializes to the OTLP
/// JSON format for /v1/traces).
#[derive(Debug, Clone)]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub flags: String,
    pub name: String,
    pub task_id: TaskId,
    pub attributes: BTreeMap<String, String>,
    pub start_time_unix_nano: u128,
    pub end_time_unix_nano: u128,
}

impl Span {
    /// Mark the span as ended (set end time).
    pub fn end(&mut self) {
        self.end_time_unix_nano = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
    }

    /// Set an attribute on the span.
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.attributes.insert(key.into(), value.into());
    }

    /// Serialize to OTLP JSON format for /v1/traces export.
    ///
    /// Produces a single `ResourceSpans` > `ScopeSpans` > `Span` entry.
    #[must_use]
    pub fn to_otlp_json(&self, service_name: &str) -> String {
        let attrs: Vec<String> = self
            .attributes
            .iter()
            .map(|(k, v)| format!(r#"{{"key":"{k}","value":{{"stringValue":"{v}"}}}}"#))
            .collect();
        let task_id_attr = format!(
            r#"{{"key":"task.id","value":{{"stringValue":"{}"}}}}"#,
            self.task_id
        );
        let all_attrs = format!("[{},{task_id_attr}]", attrs.join(","));

        format!(
            r#"{{"resourceSpans":[{{"resource":{{"attributes":[{{"key":"service.name","value":{{"stringValue":"{service_name}"}}}}]}},"scopeSpans":[{{"scope":{{"name":"escapement-serve"}},"spans":[{{"traceId":"{}","spanId":"{}","parentSpanId":"{}","flags":{},"name":"{}","kind":"SPAN_KIND_INTERNAL","startTimeUnixNano":"{}","endTimeUnixNano":"{}","attributes":{},"status":{{"code":"STATUS_CODE_OK"}}}}]}}]}}]}}"#,
            self.trace_id,
            self.span_id,
            self.parent_span_id,
            if self.flags == "01" { 256 } else { 0 },
            self.name,
            self.start_time_unix_nano,
            if self.end_time_unix_nano > 0 {
                self.end_time_unix_nano
            } else {
                self.start_time_unix_nano
            },
            all_attrs,
        )
    }
}

/// A batch of spans for OTLP export.
#[derive(Debug, Default)]
pub struct SpanBatch {
    spans: Vec<Span>,
}

impl SpanBatch {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a completed span to the batch.
    pub fn add(&mut self, span: Span) {
        self.spans.push(span);
    }

    /// Number of spans in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.spans.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    /// Serialize the batch to OTLP JSON for /v1/traces export.
    /// Combines all spans into a single `ResourceSpans` entry.
    #[must_use]
    pub fn to_otlp_json(&self, service_name: &str) -> String {
        if self.spans.is_empty() {
            return r#"{"resourceSpans":[]}"#.to_string();
        }
        let span_jsons: Vec<String> = self
            .spans
            .iter()
            .map(|s| {
                let attrs: Vec<String> = s
                    .attributes
                    .iter()
                    .map(|(k, v)| format!(r#"{{"key":"{k}","value":{{"stringValue":"{v}"}}}}"#))
                    .collect();
                let task_id_attr = format!(
                    r#"{{"key":"task.id","value":{{"stringValue":"{}"}}}}"#,
                    s.task_id
                );
                let all_attrs = format!("[{},{task_id_attr}]", attrs.join(","));
                format!(
                    r#"{{"traceId":"{}","spanId":"{}","parentSpanId":"{}","flags":{},"name":"{}","kind":"SPAN_KIND_INTERNAL","startTimeUnixNano":"{}","endTimeUnixNano":"{}","attributes":{},"status":{{"code":"STATUS_CODE_OK"}}}}"#,
                    s.trace_id,
                    s.span_id,
                    s.parent_span_id,
                    if s.flags == "01" { 256 } else { 0 },
                    s.name,
                    s.start_time_unix_nano,
                    if s.end_time_unix_nano > 0 { s.end_time_unix_nano } else { s.start_time_unix_nano },
                    all_attrs,
                )
            })
            .collect();
        format!(
            r#"{{"resourceSpans":[{{"resource":{{"attributes":[{{"key":"service.name","value":{{"stringValue":"{service_name}"}}}}]}},"scopeSpans":[{{"scope":{{"name":"escapement-serve"}},"spans":[{}]}}]}}]}}"#,
            span_jsons.join(",")
        )
    }

    /// Drain all spans (for export-then-clear pattern).
    pub fn drain(&mut self) -> Vec<Span> {
        std::mem::take(&mut self.spans)
    }
}

/// OTLP exporter — sends spans to a collector via HTTP.
///
/// In production this uses reqwest (or similar) to POST to
/// `http://alloy-gateway.monitoring:4318/v1/traces`.
/// In tests the caller can intercept the JSON.
#[derive(Debug)]
pub struct OtlpExporter {
    endpoint: String,
    service_name: String,
}

impl OtlpExporter {
    #[must_use]
    pub fn new(endpoint: impl Into<String>, service_name: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            service_name: service_name.into(),
        }
    }

    /// Export a batch of spans via HTTP POST to the OTLP endpoint.
    ///
    /// # Errors
    /// Returns a string error if the HTTP request fails.
    pub fn export(&self, batch: &SpanBatch) -> Result<(), String> {
        if batch.is_empty() {
            return Ok(());
        }
        let json = batch.to_otlp_json(&self.service_name);
        self.post_json(&json)
    }

    /// Export a single span.
    pub fn export_span(&self, span: &Span) -> Result<(), String> {
        let json = span.to_otlp_json(&self.service_name);
        self.post_json(&json)
    }

    fn post_json(&self, json: &str) -> Result<(), String> {
        use std::io::Read;
        use std::net::TcpStream;
        use std::time::Duration;

        // Parse the endpoint URL (http://host:port/path)
        let url = self.endpoint.trim_start_matches("http://");
        let (host_port, path) = url.split_once('/').unwrap_or((url, "v1/traces"));
        let (host, port_str) = host_port.rsplit_once(':').unwrap_or((host_port, "4318"));
        let port: u16 = port_str.parse().unwrap_or(4318);

        let addr = format!("{host}:{port}");
        let mut stream = TcpStream::connect_timeout(
            &addr
                .parse()
                .map_err(|e: std::net::AddrParseError| e.to_string())?,
            Duration::from_secs(5),
        )
        .map_err(|e| format!("connect to {addr}: {e}"))?;

        let request = format!(
            "POST /{path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json}",
            json.len()
        );
        stream
            .write_all(request.as_bytes())
            .map_err(|e| e.to_string())?;
        stream.flush().map_err(|e| e.to_string())?;

        // Read and discard response.
        let mut buf = [0u8; 1024];
        let _ = stream.read(&mut buf);
        Ok(())
    }

    /// Get the traceparent header for propagation to a downstream service.
    #[must_use]
    pub fn propagate_header(ctx: &TraceContext, span_id: &str) -> String {
        ctx.to_header(span_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::TaskId;

    fn task_id() -> TaskId {
        TaskId::new("t1").unwrap()
    }

    #[test]
    fn parse_valid_traceparent() {
        let ctx =
            TraceContext::from_header("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        assert!(ctx.is_some());
        let ctx = ctx.unwrap();
        assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.parent_span_id, "00f067aa0ba902b7");
        assert_eq!(ctx.flags, "01");
    }

    #[test]
    fn parse_invalid_traceparent_version() {
        assert!(TraceContext::from_header(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        )
        .is_none());
    }

    #[test]
    fn parse_all_zero_trace_id() {
        assert!(TraceContext::from_header(
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01"
        )
        .is_none());
    }

    #[test]
    fn parse_all_zero_span_id() {
        assert!(TraceContext::from_header(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"
        )
        .is_none());
    }

    #[test]
    fn parse_wrong_format() {
        assert!(TraceContext::from_header("not-a-traceparent").is_none());
        assert!(TraceContext::from_header("").is_none());
    }

    #[test]
    fn child_span_inherits_trace_id() {
        let ctx =
            TraceContext::from_header("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .unwrap();
        let span = ctx.child_span("dispatch", &task_id());
        assert_eq!(span.trace_id, ctx.trace_id);
        assert_eq!(span.parent_span_id, ctx.parent_span_id);
        assert_eq!(span.flags, ctx.flags);
        assert_eq!(span.name, "dispatch");
        assert_ne!(span.span_id, ctx.parent_span_id); // new span ID
    }

    #[test]
    fn to_header_for_propagation() {
        let ctx =
            TraceContext::from_header("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .unwrap();
        let header = ctx.to_header("abcdef0123456789");
        assert_eq!(
            header,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-abcdef0123456789-01"
        );
    }

    #[test]
    fn span_to_otlp_json() {
        let ctx =
            TraceContext::from_header("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .unwrap();
        let span = ctx.child_span("dispatch", &task_id());
        let json = span.to_otlp_json("escapement-serve");
        assert!(json.contains("\"traceId\":\"4bf92f3577b34da6a3ce929d0e0e4736\""));
        assert!(json.contains("\"service.name\""));
        assert!(json.contains("\"stringValue\":\"escapement-serve\""));
        assert!(json.contains("\"task.id\""));
    }

    #[test]
    fn batch_to_otlp_json_multiple() {
        let ctx =
            TraceContext::from_header("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .unwrap();
        let mut batch = SpanBatch::new();
        batch.add(ctx.child_span("dispatch", &task_id()));
        batch.add(ctx.child_span("assign", &task_id()));

        let json = batch.to_otlp_json("escapement-serve");
        assert!(json.contains("\"traceId\":\"4bf92f3577b34da6a3ce929d0e0e4736\""));
        // Two spans in the array
        assert_eq!(
            json.matches(r#""name":"dispatch""#).count()
                + json.matches(r#""name":"assign""#).count(),
            2
        );
    }

    #[test]
    fn batch_empty_exports_empty() {
        let batch = SpanBatch::new();
        let json = batch.to_otlp_json("escapement-serve");
        assert_eq!(json, r#"{"resourceSpans":[]}"#);
    }

    #[test]
    fn span_end_sets_end_time() {
        let ctx =
            TraceContext::from_header("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .unwrap();
        let mut span = ctx.child_span("dispatch", &task_id());
        assert_eq!(span.end_time_unix_nano, 0);
        span.end();
        assert!(span.end_time_unix_nano > 0);
    }

    #[test]
    fn span_set_attribute() {
        let ctx =
            TraceContext::from_header("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .unwrap();
        let mut span = ctx.child_span("dispatch", &task_id());
        span.set_attribute("agent", "soma/comms");
        assert_eq!(
            span.attributes.get("agent"),
            Some(&"soma/comms".to_string())
        );
    }

    #[test]
    fn batch_drain_clears() {
        let ctx =
            TraceContext::from_header("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .unwrap();
        let mut batch = SpanBatch::new();
        batch.add(ctx.child_span("dispatch", &task_id()));
        assert_eq!(batch.len(), 1);
        let drained = batch.drain();
        assert_eq!(drained.len(), 1);
        assert!(batch.is_empty());
    }

    #[test]
    fn propagate_header_static() {
        let ctx =
            TraceContext::from_header("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .unwrap();
        let header = OtlpExporter::propagate_header(&ctx, "aabbccddeeff0011");
        assert_eq!(
            header,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-aabbccddeeff0011-01"
        );
    }
}
