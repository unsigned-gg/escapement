//! CRDT overlap merge: resolves path conflicts between parallel changesets
//! using last-write-wins by blob hash.
//!
//! When parallel plan branches modify the same file paths, their changesets
//! overlap. This module provides a deterministic merge: non-overlapping
//! paths pass through unchanged; overlapping paths are resolved by
//! comparing blob hashes (last-write-wins).
//!
//! The merge is pure and deterministic — same inputs produce same output.

use std::collections::BTreeMap;
use std::fmt;

/// A path-keyed changeset — the output of a blackwall run.
/// Each path maps to a blob hash (content-addressed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Changeset {
    /// Path → blob hash.
    pub entries: BTreeMap<String, String>,
    /// The run hash this changeset belongs to.
    pub run_hash: String,
}

impl Changeset {
    #[must_use]
    pub fn new(run_hash: impl Into<String>) -> Self {
        Self {
            entries: BTreeMap::new(),
            run_hash: run_hash.into(),
        }
    }

    /// Add a path → blob hash mapping.
    pub fn set(&mut self, path: impl Into<String>, blob_hash: impl Into<String>) {
        self.entries.insert(path.into(), blob_hash.into());
    }

    #[must_use]
    pub fn paths(&self) -> Vec<&str> {
        self.entries
            .keys()
            .map(std::string::String::as_str)
            .collect()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Merge two changesets, resolving overlapping paths.
///
/// Non-overlapping paths from both changesets appear in the result.
/// Overlapping paths (same path in both) are resolved by last-write-wins:
/// the `other` changeset's blob hash wins (assuming it's the newer run).
///
/// This is deterministic: same inputs always produce the same merged output.
#[must_use]
pub fn merge_overlapping(base: &Changeset, other: &Changeset) -> Changeset {
    let mut merged = base.entries.clone();

    for (path, blob_hash) in &other.entries {
        merged.insert(path.clone(), blob_hash.clone());
    }

    Changeset {
        entries: merged,
        run_hash: format!("merge:{}+{}", base.run_hash, other.run_hash),
    }
}

/// Merge multiple changesets in order, applying last-write-wins for
/// overlapping paths.
#[must_use]
pub fn merge_all(changesets: &[Changeset]) -> Changeset {
    if changesets.is_empty() {
        return Changeset::new("empty");
    }

    let mut result = changesets[0].clone();
    for cs in &changesets[1..] {
        result = merge_overlapping(&result, cs);
    }
    result
}

/// Check if two changesets have overlapping paths.
#[must_use]
pub fn has_overlap(a: &Changeset, b: &Changeset) -> bool {
    a.entries.keys().any(|k| b.entries.contains_key(k))
}

/// The set of overlapping paths between two changesets.
#[must_use]
pub fn overlapping_paths(a: &Changeset, b: &Changeset) -> Vec<String> {
    a.entries
        .keys()
        .filter(|k| b.entries.contains_key(k.as_str()))
        .cloned()
        .collect()
}

impl fmt::Display for Changeset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Changeset({}, {} entries)", self.run_hash, self.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_non_overlapping() {
        let mut a = Changeset::new("run_a");
        a.set("src/lib.rs", "hash_a1");
        a.set("src/main.rs", "hash_a2");

        let mut b = Changeset::new("run_b");
        b.set("README.md", "hash_b1");

        let merged = merge_overlapping(&a, &b);
        assert_eq!(merged.len(), 3);
        assert_eq!(
            merged.entries.get("src/lib.rs"),
            Some(&"hash_a1".to_string())
        );
        assert_eq!(
            merged.entries.get("README.md"),
            Some(&"hash_b1".to_string())
        );
    }

    #[test]
    fn merge_overlapping_last_write_wins() {
        let mut a = Changeset::new("run_a");
        a.set("src/lib.rs", "hash_old");

        let mut b = Changeset::new("run_b");
        b.set("src/lib.rs", "hash_new");

        let merged = merge_overlapping(&a, &b);
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged.entries.get("src/lib.rs"),
            Some(&"hash_new".to_string())
        );
    }

    #[test]
    fn merge_all_preserves_order() {
        let changesets = vec![
            {
                let mut cs = Changeset::new("run_a");
                cs.set("a.rs", "h1");
                cs
            },
            {
                let mut cs = Changeset::new("run_b");
                cs.set("b.rs", "h2");
                cs
            },
            {
                let mut cs = Changeset::new("run_c");
                cs.set("a.rs", "h3"); // overwrite a.rs
                cs
            },
        ];

        let merged = merge_all(&changesets);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged.entries.get("a.rs"), Some(&"h3".to_string()));
        assert_eq!(merged.entries.get("b.rs"), Some(&"h2".to_string()));
    }

    #[test]
    fn has_overlap_detection() {
        let mut a = Changeset::new("run_a");
        a.set("src/lib.rs", "h1");

        let mut b = Changeset::new("run_b");
        b.set("src/lib.rs", "h2");

        let mut c = Changeset::new("run_c");
        c.set("README.md", "h3");

        assert!(has_overlap(&a, &b));
        assert!(!has_overlap(&a, &c));
    }

    #[test]
    fn overlapping_paths_list() {
        let mut a = Changeset::new("run_a");
        a.set("src/lib.rs", "h1");
        a.set("src/main.rs", "h2");

        let mut b = Changeset::new("run_b");
        b.set("src/lib.rs", "h3");
        b.set("README.md", "h4");

        let overlap = overlapping_paths(&a, &b);
        assert_eq!(overlap, vec!["src/lib.rs".to_string()]);
    }

    #[test]
    fn empty_changesets_merge() {
        let a = Changeset::new("run_a");
        let b = Changeset::new("run_b");
        let merged = merge_overlapping(&a, &b);
        assert!(merged.is_empty());
    }

    #[test]
    fn merge_all_empty_input() {
        let merged = merge_all(&[]);
        assert!(merged.is_empty());
    }

    #[test]
    fn merge_all_single() {
        let mut cs = Changeset::new("run_a");
        cs.set("x.rs", "hash");
        let merged = merge_all(&[cs]);
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn changeset_display() {
        let mut cs = Changeset::new("abc123");
        cs.set("a.rs", "h1");
        cs.set("b.rs", "h2");
        let s = cs.to_string();
        assert!(s.contains("abc123"));
        assert!(s.contains("2 entries"));
    }
}
