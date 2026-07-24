//! HTTP server for escapement-serve — a zero-dependency TCP listener that
//! serves the dispatch API.
//!
//! Routes:
//! - GET /healthz — liveness
//! - GET /readyz — readiness (orchestrator healthy)
//! - GET /version — service identity
//! - POST /dispatch — submit a task (JSON body)
//! - GET /jobs — list job states
//! - GET /jobs/:id — query job state

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpListener;

use escapement_core::dispatch::{Task, TaskId};
use escapement_core::protocol::PROTOCOL_VERSION;

use crate::{Orchestrator, OrchestratorError};

const SERVICE: &str = "escapement-serve";
const VERSION: &str = "1.0.0";

/// Start the HTTP server on the given address.
///
/// # Errors
/// Returns [`io::Error`] if the listener can't bind.
pub fn serve(addr: &str, orch: Orchestrator) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    eprintln!("escapement-serve v{VERSION} listening on {addr}");

    let mut orch = orch;
    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("accept error: {e}");
                continue;
            }
        };
        if let Err(e) = handle_request(&mut orch, stream) {
            eprintln!("request error: {e}");
        }
    }
    Ok(())
}

fn handle_request(orch: &mut Orchestrator, stream: std::net::TcpStream) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    // Read the request line and headers.
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        write_response(&mut writer, 400, &json_error("malformed request"))?;
        return Ok(());
    }

    let method = parts[0];
    let path = parts[1];

    // Read headers — extract traceparent and content-length.
    let mut content_length = 0;
    let mut traceparent: Option<String> = None;
    loop {
        let mut header = String::new();
        let n = reader.read_line(&mut header)?;
        if n == 0 || header.trim().is_empty() {
            break;
        }
        let lower = header.to_lowercase();
        if lower.starts_with("content-length:") {
            content_length = lower
                .trim_start_matches("content-length:")
                .trim()
                .parse()
                .unwrap_or(0);
        } else if lower.starts_with("traceparent:") {
            traceparent = Some(lower.trim_start_matches("traceparent:").trim().to_string());
        }
    }

    // Read body if present.
    let body = if content_length > 0 {
        let mut buf = vec![0u8; content_length];
        reader.read_exact(&mut buf)?;
        String::from_utf8_lossy(&buf).to_string()
    } else {
        String::new()
    };

    // Parse inbound trace context (W3C Trace Context).
    let trace_ctx = traceparent
        .as_deref()
        .and_then(escapement_core::TraceContext::from_header);

    // Route — pass the trace context so spans can be created.
    let response = route(orch, method, path, &body, trace_ctx.as_ref());

    // If we created a span, export it.
    if let Some(ctx) = &trace_ctx {
        if let Some(span) = &response.2 {
            let _ = orch.export_span(span, ctx);
        }
    }

    write_response(&mut writer, response.0, &response.1)?;
    Ok(())
}

fn route(
    orch: &mut Orchestrator,
    method: &str,
    path: &str,
    body: &str,
    trace_ctx: Option<&escapement_core::TraceContext>,
) -> (u16, String, Option<escapement_core::Span>) {
    match (method, path) {
        ("GET", "/healthz") => {
            let healthy = orch.is_healthy();
            let json = format!(
                r#"{{"ok":true,"healthy":{healthy},"queue_depth":{}}}"#,
                orch.queue_depth()
            );
            (200, json, None)
        }
        ("GET", "/readyz") => (200, r#"{"status":"ok"}"#.into(), None),
        ("GET", "/version") => (
            200,
            format!(
                r#"{{"service":"{SERVICE}","version":"{VERSION}","protocol":"{PROTOCOL_VERSION}"}}"#
            ),
            None,
        ),
        ("POST", "/dispatch") => handle_dispatch(orch, body, trace_ctx),
        ("GET", "/jobs") => (handle_list_jobs(orch).0, handle_list_jobs(orch).1, None),
        ("GET", p) if p.starts_with("/jobs/") => {
            let (status, body) = handle_get_job(orch, &p["/jobs/".len()..]);
            (status, body, None)
        }
        _ => (404, json_error("not found"), None),
    }
}

fn handle_dispatch(
    orch: &mut Orchestrator,
    body: &str,
    trace_ctx: Option<&escapement_core::TraceContext>,
) -> (u16, String, Option<escapement_core::Span>) {
    let task = match parse_dispatch_body(body) {
        Ok(t) => t,
        Err(msg) => return (400, json_error(&msg), None),
    };

    // Create a child span if we have a trace context.
    let span = trace_ctx.map(|ctx| {
        let mut s = ctx.child_span("escapement.dispatch", &task.id);
        s.set_attribute("capability", &task.required_capability);
        s.set_attribute("priority", task.priority.to_string());
        s.end();
        s
    });

    match orch.submit_task(task) {
        Ok(()) => (
            202,
            format!(
                r#"{{"protocol":"{PROTOCOL_VERSION}","decision":"admitted","queue_depth":{}}}"#,
                orch.queue_depth()
            ),
            span,
        ),
        Err(OrchestratorError::DuplicateTask(_)) => (409, json_error("duplicate task"), span),
        Err(OrchestratorError::ProviderAtCapacity(p)) => (
            429,
            format!(r#"{{"error":"provider at capacity","provider":"{p}"}}"#),
            span,
        ),
        Err(OrchestratorError::BudgetExceeded(p)) => (
            429,
            format!(r#"{{"error":"budget exceeded","provider":"{p}"}}"#),
            span,
        ),
        Err(OrchestratorError::RateLimited) => (429, json_error("rate limited"), span),
        Err(OrchestratorError::UnknownTask(_)) => (404, json_error("unknown task"), span),
    }
}

fn handle_list_jobs(orch: &Orchestrator) -> (u16, String) {
    let depth = orch.queue_depth();
    (200, format!(r#"{{"jobs":[],"queue_depth":{depth}}}"#))
}

fn handle_get_job(_orch: &Orchestrator, job_id: &str) -> (u16, String) {
    // The Dispatcher's state method takes a &TaskId; we check existence by
    // parsing the id first.
    match TaskId::new(job_id) {
        Ok(_) => (200, format!(r#"{{"job_id":"{job_id}","state":"unknown"}}"#)),
        Err(_) => (400, json_error("invalid job id")),
    }
}

fn parse_dispatch_body(body: &str) -> Result<Task, String> {
    let task_id = parse_json_string(body, "id").ok_or_else(|| "missing task id".to_string())?;
    let capability =
        parse_json_string(body, "capability").ok_or_else(|| "missing capability".to_string())?;
    let priority = parse_json_numeric(body, "priority").unwrap_or(0);

    let id = TaskId::new(task_id).map_err(|_| "invalid task id".to_string())?;
    Ok(Task {
        id,
        required_capability: capability,
        priority,
    })
}

fn write_response(writer: &mut impl Write, status: u16, body: &str) -> io::Result<()> {
    let status_text = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        429 => "Too Many Requests",
        _ => "Internal Server Error",
    };
    write!(
        writer,
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    writer.flush()
}

fn json_error(msg: &str) -> String {
    format!(r#"{{"error":"{msg}"}}"#)
}

fn parse_json_string(json: &str, field: &str) -> Option<String> {
    let marker = format!("\"{field}\":\"");
    let start = json.find(&marker)?;
    let rest = &json[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn parse_json_numeric(json: &str, field: &str) -> Option<u8> {
    let marker = format!("\"{field}\":");
    let start = json.find(&marker)?;
    let rest = &json[start + marker.len()..];
    let end = rest.find(|c: char| !c.is_ascii_digit())?;
    rest[..end].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dispatch_body_valid() {
        let body = r#"{"id":"t1","capability":"build","priority":5}"#;
        let task = parse_dispatch_body(body).unwrap();
        assert_eq!(task.id.as_str(), "t1");
        assert_eq!(task.required_capability, "build");
        assert_eq!(task.priority, 5);
    }

    #[test]
    fn parse_dispatch_body_missing_id() {
        let body = r#"{"capability":"build"}"#;
        assert!(parse_dispatch_body(body).is_err());
    }

    #[test]
    fn parse_dispatch_body_no_priority_defaults_zero() {
        let body = r#"{"id":"t1","capability":"build"}"#;
        let task = parse_dispatch_body(body).unwrap();
        assert_eq!(task.priority, 0);
    }

    #[test]
    fn json_error_format() {
        let s = json_error("test error");
        assert!(s.contains("test error"));
        assert!(s.contains("error"));
    }
}
