//! Retry policy: exponential backoff for failed dispatches.
//!
//! Computes retry delays using caller-supplied millisecond time. The retry
//! ceiling is tracked by `Dispatcher::fail` (from EST-18); this module
//! provides the delay calculation and a retry policy type.

use std::fmt;

/// Retry policy with exponential backoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum retry attempts before dead-lettering.
    pub max_retries: u32,
    /// Initial delay in milliseconds.
    pub initial_delay_ms: u64,
    /// Backoff multiplier (delay = initial * multiplier^attempt).
    pub backoff_multiplier: u64,
    /// Maximum delay cap in milliseconds.
    pub max_delay_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 1000,
            backoff_multiplier: 2,
            max_delay_ms: 30_000,
        }
    }
}

/// The reason a task is being retried — different reasons may use different
/// policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RetryReason {
    /// Task timed out.
    Timeout,
    /// Provider returned an error.
    #[default]
    ProviderError,
    /// Agent went unreachable mid-task.
    AgentDeath,
}

impl RetryPolicy {
    /// Compute the delay before the next retry attempt.
    /// Returns `None` if the retry ceiling is exceeded.
    ///
    /// # Arguments
    /// * `attempt` — zero-indexed (0 = first failure, 1 = second failure, etc.)
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn delay_ms(&self, attempt: u32) -> Option<u64> {
        if attempt >= self.max_retries {
            return None;
        }
        let exp = self
            .backoff_multiplier
            .checked_pow(attempt)
            .unwrap_or(u64::MAX);
        let delay = self.initial_delay_ms.saturating_mul(exp);
        Some(delay.min(self.max_delay_ms))
    }

    /// Whether the retry ceiling is exceeded for a given attempt count.
    #[must_use]
    pub fn is_exhausted(&self, attempts: u32) -> bool {
        attempts >= self.max_retries
    }

    /// The per-reason max retries. Different reasons may have different
    /// ceilings (e.g., timeout = 3, agent death = 5).
    #[must_use]
    pub fn max_retries_for(&self, reason: RetryReason) -> u32 {
        match reason {
            RetryReason::Timeout | RetryReason::ProviderError => self.max_retries,
            RetryReason::AgentDeath => self.max_retries.saturating_add(2),
        }
    }
}

impl fmt::Display for RetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RetryPolicy(max={}, initial={}ms, mult={}, cap={}ms)",
            self.max_retries, self.initial_delay_ms, self.backoff_multiplier, self.max_delay_ms
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_retries, 3);
        assert_eq!(p.initial_delay_ms, 1000);
        assert_eq!(p.backoff_multiplier, 2);
        assert_eq!(p.max_delay_ms, 30_000);
    }

    #[test]
    fn delay_first_retry() {
        let p = RetryPolicy::default();
        // attempt 0: initial * 2^0 = 1000
        assert_eq!(p.delay_ms(0), Some(1000));
    }

    #[test]
    fn delay_second_retry() {
        let p = RetryPolicy::default();
        // attempt 1: initial * 2^1 = 2000
        assert_eq!(p.delay_ms(1), Some(2000));
    }

    #[test]
    fn delay_third_retry() {
        let p = RetryPolicy::default();
        // attempt 2: initial * 2^2 = 4000
        assert_eq!(p.delay_ms(2), Some(4000));
    }

    #[test]
    fn delay_capped_at_max() {
        let p = RetryPolicy {
            max_retries: 10,
            initial_delay_ms: 1000,
            backoff_multiplier: 2,
            max_delay_ms: 5000,
        };
        // attempt 5: 1000 * 2^5 = 32000 → capped at 5000
        assert_eq!(p.delay_ms(5), Some(5000));
    }

    #[test]
    fn delay_exhausted_returns_none() {
        let p = RetryPolicy::default(); // max_retries = 3
        assert_eq!(p.delay_ms(3), None);
        assert_eq!(p.delay_ms(10), None);
    }

    #[test]
    fn is_exhausted() {
        let p = RetryPolicy::default(); // max_retries = 3
        assert!(!p.is_exhausted(0));
        assert!(!p.is_exhausted(2));
        assert!(p.is_exhausted(3));
        assert!(p.is_exhausted(100));
    }

    #[test]
    fn agent_death_more_retries() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_retries_for(RetryReason::AgentDeath), 5);
        assert_eq!(p.max_retries_for(RetryReason::Timeout), 3);
        assert_eq!(p.max_retries_for(RetryReason::ProviderError), 3);
    }

    #[test]
    fn custom_policy() {
        let p = RetryPolicy {
            max_retries: 5,
            initial_delay_ms: 500,
            backoff_multiplier: 3,
            max_delay_ms: 60_000,
        };
        assert_eq!(p.delay_ms(0), Some(500)); // 500 * 3^0
        assert_eq!(p.delay_ms(1), Some(1500)); // 500 * 3^1
        assert_eq!(p.delay_ms(2), Some(4500)); // 500 * 3^2
        assert_eq!(p.delay_ms(5), None); // exhausted
    }

    #[test]
    fn policy_display() {
        let p = RetryPolicy::default();
        let s = p.to_string();
        assert!(s.contains("max=3"));
        assert!(s.contains("initial=1000ms"));
    }
}
