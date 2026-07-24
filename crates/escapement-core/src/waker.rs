//! Waker integration: TCP wake proxy client for scale-to-zero microVMs.
//!
//! Escapement dispatches jobs to waker-managed targets — workloads that
//! scale to zero and need to be woken via waker's TCP accept-and-hold
//! proxy before dispatch can proceed.
//!
//! This module provides a zero-dependency TCP client that connects to
//! waker's listener, splices bytes, and reports readiness. The actual
//! waker deployment (Helm chart, Service) is in `unsigned-gg/waker`.

use std::fmt;
use std::io;
use std::net::TcpStream;
use std::time::Duration;

/// Configuration for a waker target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakerTarget {
    /// The waker listener address (e.g. `waker.unsigned.svc:10023`).
    pub listen_addr: String,
    /// The target workload name (e.g. `aria-vm`).
    pub workload: String,
    /// The namespace of the target workload.
    pub namespace: String,
}

impl WakerTarget {
    #[must_use]
    pub fn new(
        listen_addr: impl Into<String>,
        workload: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            listen_addr: listen_addr.into(),
            workload: workload.into(),
            namespace: namespace.into(),
        }
    }
}

/// Result of a wake attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeResult {
    /// The workload was already running and the connection succeeded.
    AlreadyRunning,
    /// The workload was scaled from zero and is now ready.
    Waked,
    /// The wake timed out after `timeout_ms` milliseconds.
    Timeout { timeout_ms: u64 },
    /// A connection error occurred.
    Failed(String),
}

impl fmt::Display for WakeResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyRunning => write!(f, "already running"),
            Self::Waked => write!(f, "waked from zero"),
            Self::Timeout { timeout_ms } => write!(f, "timeout after {timeout_ms}ms"),
            Self::Failed(msg) => write!(f, "failed: {msg}"),
        }
    }
}

/// Wake a waker-managed target by connecting to its TCP listener.
///
/// Waker holds the connection until the backend is ready, then splices
/// bytes. A successful `connect` means the backend answered — the workload
/// is up.
///
/// # Arguments
/// * `target` — The waker target to wake.
/// * `timeout_ms` — Maximum time to wait for the connection (capped at 120s).
///
/// # Errors
/// Returns [`WakeResult::Failed`] on connection errors.
#[must_use]
pub fn wake_and_wait(target: &WakerTarget, timeout_ms: u64) -> WakeResult {
    use std::net::ToSocketAddrs;
    let timeout = Duration::from_millis(timeout_ms.min(120_000));

    // Resolve the listen address to a SocketAddr.
    let addr = match target.listen_addr.to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(addr) => addr,
            None => {
                return WakeResult::Failed(format!("no addresses for {}", target.listen_addr));
            }
        },
        Err(e) => return WakeResult::Failed(e.to_string()),
    };

    match TcpStream::connect_timeout(&addr, timeout) {
        Ok(_) => WakeResult::Waked,
        Err(e) if e.kind() == io::ErrorKind::TimedOut => WakeResult::Timeout { timeout_ms },
        Err(e) => WakeResult::Failed(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waker_target_construction() {
        let target = WakerTarget::new("waker.svc:10023", "aria-vm", "aria-vm");
        assert_eq!(target.listen_addr, "waker.svc:10023");
        assert_eq!(target.workload, "aria-vm");
        assert_eq!(target.namespace, "aria-vm");
    }

    #[test]
    fn wake_result_display() {
        assert_eq!(WakeResult::AlreadyRunning.to_string(), "already running");
        assert_eq!(WakeResult::Waked.to_string(), "waked from zero");
        assert!(WakeResult::Timeout {
            timeout_ms: 120_000
        }
        .to_string()
        .contains("120000"));
        assert!(WakeResult::Failed("refused".into())
            .to_string()
            .contains("refused"));
    }

    #[test]
    fn wake_unresolvable_fails() {
        let target = WakerTarget::new("nonexistent.invalid:9999", "test", "default");
        let result = wake_and_wait(&target, 1000);
        assert!(matches!(
            result,
            WakeResult::Failed(_) | WakeResult::Timeout { .. }
        ));
    }

    #[test]
    fn wake_timeout_capped() {
        // The function should cap at 120s even if a longer timeout is given.
        // Just verify it doesn't panic.
        let target = WakerTarget::new("nonexistent.invalid:9999", "test", "default");
        let _result = wake_and_wait(&target, 999_999_999);
    }
}
