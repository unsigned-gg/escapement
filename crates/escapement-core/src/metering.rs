//! Metered admission: per-capability concurrency limits and token-bucket
//! rate quotas.
//!
//! The [`ConcurrencyGuard`] tracks in-flight dispatches per capability and
//! enforces a configurable max-parallel cap. The [`TokenBucket`] is a
//! per-provider rate limiter using caller-supplied millisecond time.
//!
//! Both are deterministic and host-agnostic (zero I/O, zero deps) — the
//! caller drives the clock.

use std::collections::BTreeMap;
use std::fmt;

/// Tracks in-flight dispatches per capability, enforcing a max-parallel cap.
#[derive(Debug, Default)]
pub struct ConcurrencyGuard {
    /// Configured max-parallel per capability.
    caps: BTreeMap<String, usize>,
    /// Current in-flight count per capability.
    in_flight: BTreeMap<String, usize>,
}

/// Error returned when a capability is at its concurrency cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcurrencyExceeded {
    pub capability: String,
    pub in_flight: usize,
    pub cap: usize,
}

impl fmt::Display for ConcurrencyExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "concurrency cap exceeded for {}: {}/{} in flight",
            self.capability, self.in_flight, self.cap
        )
    }
}

impl std::error::Error for ConcurrencyExceeded {}

impl ConcurrencyGuard {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the max-parallel cap for a capability.
    pub fn set_cap(&mut self, capability: impl Into<String>, cap: usize) {
        self.caps.insert(capability.into(), cap);
    }

    /// Get the configured cap for a capability (default 0 = unlimited).
    #[must_use]
    pub fn cap(&self, capability: &str) -> usize {
        *self.caps.get(capability).unwrap_or(&0)
    }

    /// Current in-flight count for a capability.
    #[must_use]
    pub fn in_flight(&self, capability: &str) -> usize {
        *self.in_flight.get(capability).unwrap_or(&0)
    }

    /// Try to acquire a slot for a capability. Returns the new in-flight
    /// count, or an error if at cap.
    ///
    /// # Errors
    /// Returns [`ConcurrencyExceeded`] if the capability is at its cap.
    pub fn acquire(&mut self, capability: &str) -> Result<usize, ConcurrencyExceeded> {
        let cap = self.cap(capability);
        let current = self.in_flight(capability);
        if cap > 0 && current >= cap {
            return Err(ConcurrencyExceeded {
                capability: capability.into(),
                in_flight: current,
                cap,
            });
        }
        let new = current + 1;
        self.in_flight.insert(capability.into(), new);
        Ok(new)
    }

    /// Release a slot for a capability. Returns the new in-flight count.
    pub fn release(&mut self, capability: &str) -> usize {
        let current = self.in_flight(capability);
        if current == 0 {
            return 0; // underflow guard — idempotent release
        }
        let new = current - 1;
        self.in_flight.insert(capability.into(), new);
        new
    }
}

/// Token-bucket rate limiter using caller-supplied millisecond time.
/// Used for per-provider or per-agent rate limiting.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    /// Maximum tokens the bucket can hold.
    capacity: f64,
    /// Tokens added per millisecond.
    refill_rate_per_ms: f64,
    /// Current token count.
    tokens: f64,
    /// Last refill time (caller-supplied ms).
    last_refill_ms: u64,
}

/// Error returned when the bucket has insufficient tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimited {
    pub retry_after_ms: u64,
}

impl fmt::Display for RateLimited {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rate limited: retry after {}ms", self.retry_after_ms)
    }
}

impl std::error::Error for RateLimited {}

impl TokenBucket {
    /// Create a new token bucket.
    ///
    /// # Arguments
    /// * `capacity` - Maximum tokens the bucket holds.
    /// * `refill_per_second` - Tokens added per second (converted to per-ms internally).
    /// * `now_ms` - Initial time (caller-supplied milliseconds).
    #[must_use]
    pub fn new(capacity: f64, refill_per_second: f64, now_ms: u64) -> Self {
        Self {
            capacity,
            refill_rate_per_ms: refill_per_second / 1000.0,
            tokens: capacity,
            last_refill_ms: now_ms,
        }
    }

    /// Refill the bucket based on elapsed time since the last call.
    #[allow(clippy::cast_precision_loss)]
    fn refill(&mut self, now_ms: u64) {
        let elapsed = now_ms.saturating_sub(self.last_refill_ms);
        if elapsed > 0 {
            self.tokens =
                (self.tokens + elapsed as f64 * self.refill_rate_per_ms).min(self.capacity);
            self.last_refill_ms = now_ms;
        }
    }

    /// Try to consume `cost` tokens. Returns Ok if accepted, Err with
    /// retry-after if insufficient.
    ///
    /// # Errors
    /// Returns [`RateLimited`] with `retry_after_ms` if the bucket doesn't
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn try_consume(&mut self, cost: f64, now_ms: u64) -> Result<(), RateLimited> {
        self.refill(now_ms);
        if self.tokens >= cost {
            self.tokens -= cost;
            Ok(())
        } else {
            let deficit = cost - self.tokens;
            let retry_after_ms = if self.refill_rate_per_ms > 0.0 {
                (deficit / self.refill_rate_per_ms).ceil() as u64
            } else {
                u64::MAX // no refill — never succeeds
            };
            Err(RateLimited { retry_after_ms })
        }
    }

    /// Current token count (after refilling at `now_ms`).
    #[must_use]
    pub fn tokens(&mut self, now_ms: u64) -> f64 {
        self.refill(now_ms);
        self.tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- ConcurrencyGuard ---

    #[test]
    fn acquire_and_release() {
        let mut guard = ConcurrencyGuard::new();
        guard.set_cap("build", 2);

        assert_eq!(guard.acquire("build").unwrap(), 1);
        assert_eq!(guard.acquire("build").unwrap(), 2);
        assert_eq!(guard.in_flight("build"), 2);
    }

    #[test]
    fn acquire_at_cap_errors() {
        let mut guard = ConcurrencyGuard::new();
        guard.set_cap("build", 1);

        guard.acquire("build").unwrap();
        let err = guard.acquire("build").unwrap_err();
        assert_eq!(err.cap, 1);
        assert_eq!(err.in_flight, 1);
    }

    #[test]
    fn release_decrements_count() {
        let mut guard = ConcurrencyGuard::new();
        guard.set_cap("build", 3);

        guard.acquire("build").unwrap();
        guard.acquire("build").unwrap();
        assert_eq!(guard.in_flight("build"), 2);

        guard.release("build");
        assert_eq!(guard.in_flight("build"), 1);
    }

    #[test]
    fn release_below_zero_is_noop() {
        let mut guard = ConcurrencyGuard::new();
        assert_eq!(guard.release("build"), 0);
    }

    #[test]
    fn cap_zero_means_unlimited() {
        let mut guard = ConcurrencyGuard::new();
        // No cap set → cap is 0 → unlimited
        for _ in 0..100 {
            guard.acquire("build").unwrap();
        }
        assert_eq!(guard.in_flight("build"), 100);
    }

    #[test]
    fn separate_capabilities_are_independent() {
        let mut guard = ConcurrencyGuard::new();
        guard.set_cap("build", 1);
        guard.set_cap("review", 1);

        guard.acquire("build").unwrap();
        // "review" is independent — not blocked by "build"
        guard.acquire("review").unwrap();
        assert_eq!(guard.in_flight("build"), 1);
        assert_eq!(guard.in_flight("review"), 1);

        // "build" is at cap now
        assert!(guard.acquire("build").is_err());
        // "review" is also at cap
        assert!(guard.acquire("review").is_err());
    }

    // --- TokenBucket ---

    #[test]
    fn consume_succeeds_with_enough_tokens() {
        let mut bucket = TokenBucket::new(10.0, 1.0, 0);
        assert!(bucket.try_consume(5.0, 0).is_ok());
        assert!((bucket.tokens(0) - 5.0).abs() < 0.001);
    }

    #[test]
    fn consume_fails_with_insufficient_tokens() {
        let mut bucket = TokenBucket::new(5.0, 1.0, 0);
        // Consume all 5
        bucket.try_consume(5.0, 0).unwrap();
        // Next consume fails
        let err = bucket.try_consume(1.0, 0).unwrap_err();
        assert!(err.retry_after_ms > 0);
    }

    #[test]
    fn tokens_refill_over_time() {
        let mut bucket = TokenBucket::new(10.0, 100.0, 0); // 100 tokens/sec = 0.1/ms
        bucket.try_consume(10.0, 0).unwrap();
        assert!((bucket.tokens(0) - 0.0).abs() < 0.001);

        // After 50ms → 5 tokens refilled
        assert!((bucket.tokens(50) - 5.0).abs() < 0.001);

        // After 100ms → 10 tokens (capped at capacity)
        assert!((bucket.tokens(100) - 10.0).abs() < 0.001);
    }

    #[test]
    fn refill_caps_at_capacity() {
        let mut bucket = TokenBucket::new(5.0, 1000.0, 0); // 1000/sec, cap 5
        bucket.try_consume(5.0, 0).unwrap();
        // After a very long time, should still be capped at 5
        assert!((bucket.tokens(999_999) - 5.0).abs() < 0.001);
    }

    #[test]
    fn retry_after_calculation() {
        let mut bucket = TokenBucket::new(0.0, 10.0, 0); // empty, 10 tokens/sec
        let err = bucket.try_consume(10.0, 0).unwrap_err();
        // 10 tokens / 10 per sec = 1 sec = 1000ms
        assert_eq!(err.retry_after_ms, 1000);
    }

    #[test]
    fn recovery_after_token_refill() {
        let mut bucket = TokenBucket::new(1.0, 10.0, 0); // 10 tokens/sec, cap 1
        bucket.try_consume(1.0, 0).unwrap();
        // Failed — no tokens
        assert!(bucket.try_consume(1.0, 0).is_err());
        // After 100ms → 1 token refilled → success
        assert!(bucket.try_consume(1.0, 100).is_ok());
    }
}
