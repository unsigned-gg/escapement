//! Backpressure protocol: providers (blackwall, waker microVMs, LLM gateway)
//! signal capacity exhaustion; escapement throttles admission in response.
//!
//! The [`ProviderRegistry`] tracks per-provider capacity: max concurrent,
//! current in-flight, and health status. When a provider is at capacity or
//! unhealthy, new jobs for that provider queue rather than dispatch.

use std::collections::BTreeMap;
use std::fmt;

/// Health status of a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProviderHealth {
    /// Provider is healthy and accepting dispatches.
    #[default]
    Healthy,
    /// Provider is degraded — dispatches still accepted but may be slow.
    Degraded,
    /// Provider is down — dispatches queue, not fail.
    Down,
}

impl fmt::Display for ProviderHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Healthy => write!(f, "healthy"),
            Self::Degraded => write!(f, "degraded"),
            Self::Down => write!(f, "down"),
        }
    }
}

/// Per-provider capacity and health state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCapacity {
    pub max_concurrent: usize,
    pub current_in_flight: usize,
    pub health: ProviderHealth,
}

impl ProviderCapacity {
    /// Whether this provider can accept a new dispatch.
    #[must_use]
    pub fn can_accept(&self) -> bool {
        self.health != ProviderHealth::Down && self.current_in_flight < self.max_concurrent
    }

    /// Remaining capacity (slots available).
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.max_concurrent.saturating_sub(self.current_in_flight)
    }
}

/// Tracks capacity and health for all known providers.
#[derive(Debug, Default)]
pub struct ProviderRegistry {
    providers: BTreeMap<String, ProviderCapacity>,
}

/// Error returned when a provider is unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownProvider(pub String);

impl fmt::Display for UnknownProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown provider: {}", self.0)
    }
}

impl std::error::Error for UnknownProvider {}

/// Result of an admission check against a provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackpressureDecision {
    /// Provider has capacity — dispatch proceeds.
    Accept,
    /// Provider is at capacity — job should queue.
    Queue { reason: String },
    /// Provider is down — job should queue (not fail).
    ProviderDown { provider: String },
}

impl ProviderRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a provider with a max concurrency cap.
    pub fn register(&mut self, name: impl Into<String>, max_concurrent: usize) {
        self.providers.insert(
            name.into(),
            ProviderCapacity {
                max_concurrent,
                current_in_flight: 0,
                health: ProviderHealth::default(),
            },
        );
    }

    /// Check if a provider can accept a new dispatch.
    ///
    /// # Errors
    /// Returns [`UnknownProvider`] if the provider is not registered.
    pub fn check(&self, provider: &str) -> Result<BackpressureDecision, UnknownProvider> {
        let cap = self
            .providers
            .get(provider)
            .ok_or_else(|| UnknownProvider(provider.to_string()))?;

        if cap.health == ProviderHealth::Down {
            return Ok(BackpressureDecision::ProviderDown {
                provider: provider.to_string(),
            });
        }

        if cap.can_accept() {
            Ok(BackpressureDecision::Accept)
        } else {
            Ok(BackpressureDecision::Queue {
                reason: format!(
                    "provider {provider} at capacity: {}/{}",
                    cap.current_in_flight, cap.max_concurrent
                ),
            })
        }
    }

    /// Record a dispatch starting (increment in-flight).
    ///
    /// # Errors
    /// Returns [`UnknownProvider`] if the provider is not registered.
    pub fn dispatch_started(&mut self, provider: &str) -> Result<(), UnknownProvider> {
        let cap = self
            .providers
            .get_mut(provider)
            .ok_or_else(|| UnknownProvider(provider.to_string()))?;
        cap.current_in_flight += 1;
        Ok(())
    }

    /// Record a dispatch completing (decrement in-flight).
    ///
    /// # Errors
    /// Returns [`UnknownProvider`] if the provider is not registered.
    pub fn dispatch_completed(&mut self, provider: &str) -> Result<(), UnknownProvider> {
        let cap = self
            .providers
            .get_mut(provider)
            .ok_or_else(|| UnknownProvider(provider.to_string()))?;
        cap.current_in_flight = cap.current_in_flight.saturating_sub(1);
        Ok(())
    }

    /// Update a provider's health status.
    ///
    /// # Errors
    /// Returns [`UnknownProvider`] if the provider is not registered.
    pub fn set_health(
        &mut self,
        provider: &str,
        health: ProviderHealth,
    ) -> Result<(), UnknownProvider> {
        let cap = self
            .providers
            .get_mut(provider)
            .ok_or_else(|| UnknownProvider(provider.to_string()))?;
        cap.health = health;
        Ok(())
    }

    /// Get the capacity state for a provider.
    #[must_use]
    pub fn capacity(&self, provider: &str) -> Option<&ProviderCapacity> {
        self.providers.get(provider)
    }

    /// Check admission and start the dispatch atomically.
    ///
    /// # Errors
    /// Returns [`UnknownProvider`] if the provider is not registered.
    pub fn try_dispatch(
        &mut self,
        provider: &str,
    ) -> Result<BackpressureDecision, UnknownProvider> {
        let decision = self.check(provider)?;
        if matches!(decision, BackpressureDecision::Accept) {
            self.dispatch_started(provider)?;
        }
        Ok(decision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> ProviderRegistry {
        let mut r = ProviderRegistry::new();
        r.register("blackwall", 3);
        r.register("waker", 1);
        r
    }

    #[test]
    fn healthy_provider_accepts() {
        let r = registry();
        assert_eq!(r.check("blackwall").unwrap(), BackpressureDecision::Accept);
    }

    #[test]
    fn at_capacity_queues() {
        let mut r = registry();
        // Fill blackwall to capacity (3).
        r.dispatch_started("blackwall").unwrap();
        r.dispatch_started("blackwall").unwrap();
        r.dispatch_started("blackwall").unwrap();

        let decision = r.check("blackwall").unwrap();
        assert!(matches!(decision, BackpressureDecision::Queue { .. }));
    }

    #[test]
    fn completions_free_capacity() {
        let mut r = registry();
        r.dispatch_started("blackwall").unwrap();
        r.dispatch_started("blackwall").unwrap();
        r.dispatch_started("blackwall").unwrap();

        assert!(matches!(
            r.check("blackwall").unwrap(),
            BackpressureDecision::Queue { .. }
        ));

        r.dispatch_completed("blackwall").unwrap();
        assert_eq!(r.check("blackwall").unwrap(), BackpressureDecision::Accept);
    }

    #[test]
    fn provider_down_queues() {
        let mut r = registry();
        r.set_health("blackwall", ProviderHealth::Down).unwrap();

        let decision = r.check("blackwall").unwrap();
        assert!(matches!(
            decision,
            BackpressureDecision::ProviderDown { .. }
        ));
    }

    #[test]
    fn provider_recovers_resumes_dispatch() {
        let mut r = registry();
        r.set_health("blackwall", ProviderHealth::Down).unwrap();
        assert!(matches!(
            r.check("blackwall").unwrap(),
            BackpressureDecision::ProviderDown { .. }
        ));

        r.set_health("blackwall", ProviderHealth::Healthy).unwrap();
        assert_eq!(r.check("blackwall").unwrap(), BackpressureDecision::Accept);
    }

    #[test]
    fn unknown_provider_errors() {
        let r = registry();
        assert!(r.check("ghost").is_err());
    }

    #[test]
    fn try_dispatch_atomic_check_and_increment() {
        let mut r = registry();
        r.register("test", 1);

        // First dispatch succeeds.
        assert_eq!(
            r.try_dispatch("test").unwrap(),
            BackpressureDecision::Accept
        );
        assert_eq!(r.capacity("test").unwrap().current_in_flight, 1);

        // Second dispatch queues (capacity reached).
        let decision = r.try_dispatch("test").unwrap();
        assert!(matches!(decision, BackpressureDecision::Queue { .. }));
    }

    #[test]
    fn degraded_provider_still_accepts() {
        let mut r = registry();
        r.set_health("blackwall", ProviderHealth::Degraded).unwrap();
        assert_eq!(r.check("blackwall").unwrap(), BackpressureDecision::Accept);
    }

    #[test]
    fn separate_providers_independent() {
        let mut r = registry();
        // Fill waker (cap 1).
        r.dispatch_started("waker").unwrap();

        // Waker is full, blackwall is not.
        assert!(matches!(
            r.check("waker").unwrap(),
            BackpressureDecision::Queue { .. }
        ));
        assert_eq!(r.check("blackwall").unwrap(), BackpressureDecision::Accept);
    }

    #[test]
    fn remaining_capacity() {
        let mut r = registry();
        r.dispatch_started("blackwall").unwrap(); // 1/3
        let cap = r.capacity("blackwall").unwrap();
        assert_eq!(cap.remaining(), 2);
        assert!(cap.can_accept());
    }
}
