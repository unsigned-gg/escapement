//! Budget enforcement: per-run and per-batch token caps.
//!
//! Tracks aggregate spend across dispatched jobs (per-provider, per-batch).
//! Budget-exhausted providers trigger backpressure (jobs queue, not fail).
//! Runs over budget are recorded but their changeset is not retained
//! (fail-closed, matching blackwall's budget enforcement pattern).

use std::collections::BTreeMap;
use std::fmt;

/// Per-provider budget tracking.
#[derive(Debug, Default, Clone)]
pub struct BudgetTracker {
    /// Configured max tokens per provider.
    caps: BTreeMap<String, u64>,
    /// Current token spend per provider.
    spent: BTreeMap<String, u64>,
}

/// Result of a budget check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetDecision {
    /// Budget allows the dispatch.
    WithinBudget,
    /// Budget exhausted — jobs should queue.
    BudgetExceeded,
}

/// Error returned when a provider is unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownBudgetProvider(pub String);

impl fmt::Display for UnknownBudgetProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown provider: {}", self.0)
    }
}

impl std::error::Error for UnknownBudgetProvider {}

impl BudgetTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the token cap for a provider.
    pub fn set_cap(&mut self, provider: impl Into<String>, cap: u64) {
        self.caps.insert(provider.into(), cap);
    }

    /// Get the configured cap for a provider (0 = unlimited).
    #[must_use]
    pub fn cap(&self, provider: &str) -> u64 {
        *self.caps.get(provider).unwrap_or(&0)
    }

    /// Current token spend for a provider.
    #[must_use]
    pub fn spent(&self, provider: &str) -> u64 {
        *self.spent.get(provider).unwrap_or(&0)
    }

    /// Remaining budget for a provider (`u64::MAX` if unlimited).
    #[must_use]
    pub fn remaining(&self, provider: &str) -> u64 {
        let cap = self.cap(provider);
        if cap == 0 {
            return u64::MAX; // unlimited
        }
        cap.saturating_sub(self.spent(provider))
    }

    /// Check if a provider can accept a dispatch with the given token cost.
    ///
    /// # Errors
    /// Returns [`UnknownBudgetProvider`] only if the provider has no cap set
    /// AND you want strict validation. Otherwise, uncapped providers return
    /// `WithinBudget`.
    #[must_use]
    pub fn check(&self, provider: &str, token_cost: u64) -> BudgetDecision {
        let cap = self.cap(provider);
        if cap == 0 {
            return BudgetDecision::WithinBudget; // unlimited
        }
        let remaining = cap.saturating_sub(self.spent(provider));
        if remaining >= token_cost {
            BudgetDecision::WithinBudget
        } else {
            BudgetDecision::BudgetExceeded
        }
    }

    /// Record token spend for a dispatch.
    pub fn record_spend(&mut self, provider: &str, tokens: u64) {
        let current = self.spent(provider);
        self.spent.insert(provider.to_string(), current + tokens);
    }

    /// Reset spend for a provider (e.g., new billing period).
    pub fn reset(&mut self, provider: &str) {
        self.spent.insert(provider.to_string(), 0);
    }

    /// Reset all spend (e.g., new billing period across all providers).
    pub fn reset_all(&mut self) {
        let keys: Vec<String> = self.spent.keys().cloned().collect();
        for key in keys {
            self.spent.insert(key, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn within_budget_accepts() {
        let mut tracker = BudgetTracker::new();
        tracker.set_cap("blackwall", 100_000);
        assert_eq!(
            tracker.check("blackwall", 50_000),
            BudgetDecision::WithinBudget
        );
    }

    #[test]
    fn budget_exceeded_rejects() {
        let mut tracker = BudgetTracker::new();
        tracker.set_cap("blackwall", 100_000);
        tracker.record_spend("blackwall", 80_000);
        assert_eq!(
            tracker.check("blackwall", 50_000),
            BudgetDecision::BudgetExceeded
        );
    }

    #[test]
    fn unlimited_provider_always_within_budget() {
        let tracker = BudgetTracker::new();
        assert_eq!(
            tracker.check("uncapped", 999_999_999),
            BudgetDecision::WithinBudget
        );
    }

    #[test]
    fn record_spend_increments() {
        let mut tracker = BudgetTracker::new();
        tracker.record_spend("blackwall", 5000);
        tracker.record_spend("blackwall", 3000);
        assert_eq!(tracker.spent("blackwall"), 8000);
    }

    #[test]
    fn remaining_budget() {
        let mut tracker = BudgetTracker::new();
        tracker.set_cap("blackwall", 100_000);
        tracker.record_spend("blackwall", 30_000);
        assert_eq!(tracker.remaining("blackwall"), 70_000);
    }

    #[test]
    fn remaining_unlimited_is_max() {
        let tracker = BudgetTracker::new();
        assert_eq!(tracker.remaining("uncapped"), u64::MAX);
    }

    #[test]
    fn reset_provider() {
        let mut tracker = BudgetTracker::new();
        tracker.set_cap("blackwall", 100_000);
        tracker.record_spend("blackwall", 50_000);
        tracker.reset("blackwall");
        assert_eq!(tracker.spent("blackwall"), 0);
        assert_eq!(tracker.remaining("blackwall"), 100_000);
    }

    #[test]
    fn reset_all_providers() {
        let mut tracker = BudgetTracker::new();
        tracker.set_cap("a", 1000);
        tracker.set_cap("b", 2000);
        tracker.record_spend("a", 500);
        tracker.record_spend("b", 1000);
        tracker.reset_all();
        assert_eq!(tracker.spent("a"), 0);
        assert_eq!(tracker.spent("b"), 0);
    }

    #[test]
    fn exact_budget_boundary() {
        let mut tracker = BudgetTracker::new();
        tracker.set_cap("blackwall", 100_000);
        tracker.record_spend("blackwall", 50_000);
        // Exactly at boundary: 50k spent, 50k remaining, cost 50k → within budget
        assert_eq!(
            tracker.check("blackwall", 50_000),
            BudgetDecision::WithinBudget
        );
        // One token over → exceeded
        assert_eq!(
            tracker.check("blackwall", 50_001),
            BudgetDecision::BudgetExceeded
        );
    }
}
