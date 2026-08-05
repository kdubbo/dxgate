//! The one keyed collection dxgate applies resource events to.
//!
//! Every component that consumes a stream of resource events needs the same
//! three operations: upsert one key, remove one key, and — when a source can
//! only say "here is everything I have" — replace the whole set and work out
//! what disappeared. Writing that by hand once per component is how controllers
//! acquire their bugs: the mistakes are never in the business logic, they are in
//! "was that an add or an update", "which index still holds the old key", and
//! "what did the source stop sending".
//!
//! dxgate had five hand-written versions of this. It now has one, and the
//! invariants that matter — an add followed by a remove leaves no trace,
//! reordering independent updates converges to the same state, replaying the
//! same state-of-the-world twice is a no-op — are property-tested in one place
//! rather than re-argued at every call site.

use std::collections::btree_map::{Iter, Keys, Values};
use std::collections::{BTreeMap, BTreeSet};

/// What a [`Collection::replace_all`] actually changed.
///
/// `upserted` holds keys whose value is new or different; keys that were
/// re-sent unchanged are deliberately absent, which is what lets callers skip
/// downstream work instead of propagating a no-op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeSet<K> {
    pub upserted: Vec<K>,
    pub removed: Vec<K>,
}

impl<K> Default for ChangeSet<K> {
    fn default() -> Self {
        Self {
            upserted: Vec::new(),
            removed: Vec::new(),
        }
    }
}

impl<K> ChangeSet<K> {
    pub fn is_empty(&self) -> bool {
        self.upserted.is_empty() && self.removed.is_empty()
    }

    /// Whether anything moved. The inverse of [`ChangeSet::is_empty`], spelled
    /// the way call sites read.
    pub fn changed(&self) -> bool {
        !self.is_empty()
    }
}

/// A keyed set of resources with change reporting.
///
/// Ordered by key so every projection built from it is deterministic regardless
/// of the order events arrived in — the difference between a reproducible
/// snapshot and one that depends on watch timing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Collection<K, V> {
    items: BTreeMap<K, V>,
}

impl<K, V> Default for Collection<K, V> {
    fn default() -> Self {
        Self {
            items: BTreeMap::new(),
        }
    }
}

impl<K, V> Collection<K, V> {
    pub fn new() -> Self {
        Self {
            items: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        self.items.iter()
    }

    pub fn keys(&self) -> Keys<'_, K, V> {
        self.items.keys()
    }

    pub fn values(&self) -> Values<'_, K, V> {
        self.items.values()
    }
}

impl<K: Ord, V> Collection<K, V> {
    pub fn get(&self, key: &K) -> Option<&V> {
        self.items.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.items.get_mut(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.items.contains_key(key)
    }

    /// Inserts without comparing. For values that carry no meaningful equality
    /// (or where the caller has already decided something changed).
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.items.insert(key, value)
    }

    /// Removes one key. Returns whether it was there.
    pub fn remove(&mut self, key: &K) -> bool {
        self.items.remove(key).is_some()
    }

    /// Removes one key and hands back its value.
    pub fn take(&mut self, key: &K) -> Option<V> {
        self.items.remove(key)
    }

    /// Drops every entry the predicate rejects, returning the keys dropped.
    pub fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) -> Vec<K>
    where
        K: Clone,
    {
        let removed: Vec<K> = self
            .items
            .iter()
            .filter(|(key, value)| !keep(key, value))
            .map(|(key, _)| key.clone())
            .collect();
        for key in &removed {
            self.items.remove(key);
        }
        removed
    }
}

impl<K: Ord + Clone, V: PartialEq> Collection<K, V> {
    /// Applies one incremental upsert. Returns whether it changed anything, so
    /// a source that re-sends identical resources does not look like an update.
    pub fn upsert(&mut self, key: K, value: V) -> bool {
        if self
            .items
            .get(&key)
            .is_some_and(|existing| *existing == value)
        {
            return false;
        }
        self.items.insert(key, value);
        true
    }

    /// Applies a state-of-the-world update: `items` is the complete set, so
    /// anything currently held and absent from it is retired.
    ///
    /// This is the bridge that lets a full-list source — a file, a Kubernetes
    /// re-list after a lost watch, a legacy SotW xDS response — drive the same
    /// delta-shaped consumers as an incremental one.
    pub fn replace_all(&mut self, items: impl IntoIterator<Item = (K, V)>) -> ChangeSet<K> {
        // Build the next state first so a duplicated key in `items` is resolved
        // (last wins) before anything is compared.
        let next: BTreeMap<K, V> = items.into_iter().collect();
        let upserted = next
            .iter()
            .filter(|(key, value)| self.items.get(key).is_none_or(|old| old != *value))
            .map(|(key, _)| key.clone())
            .collect();
        let removed = self
            .items
            .keys()
            .filter(|key| !next.contains_key(key))
            .cloned()
            .collect();
        self.items = next;
        ChangeSet { upserted, removed }
    }
}

impl<K: Ord + Clone, V> Collection<K, V> {
    /// The key set, for callers that track identity rather than content.
    pub fn key_set(&self) -> BTreeSet<K> {
        self.items.keys().cloned().collect()
    }
}

impl<K: Ord, V> FromIterator<(K, V)> for Collection<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self {
            items: iter.into_iter().collect(),
        }
    }
}

impl<'a, K, V> IntoIterator for &'a Collection<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_reports_only_real_changes() {
        let mut collection = Collection::<&str, u32>::new();

        assert!(collection.upsert("a", 1));
        assert!(!collection.upsert("a", 1));
        assert!(collection.upsert("a", 2));
        assert_eq!(collection.get(&"a"), Some(&2));
    }

    #[test]
    fn replace_all_reports_upserts_and_retirements() {
        let mut collection = Collection::<&str, u32>::new();
        collection.replace_all([("a", 1), ("b", 2)]);

        let change = collection.replace_all([("a", 1), ("c", 3)]);

        // "a" was re-sent unchanged, so it is not an upsert.
        assert_eq!(change.upserted, ["c"]);
        assert_eq!(change.removed, ["b"]);
        assert!(change.changed());
    }

    #[test]
    fn replaying_the_same_state_of_the_world_is_a_no_op() {
        let mut collection = Collection::<&str, u32>::new();
        collection.replace_all([("a", 1), ("b", 2)]);

        let change = collection.replace_all([("a", 1), ("b", 2)]);

        assert!(change.is_empty());
    }

    #[test]
    fn replace_all_resolves_duplicate_keys_before_comparing() {
        let mut collection = Collection::<&str, u32>::new();
        collection.replace_all([("a", 1)]);

        // The last value for a key wins, and the intermediate value must not be
        // mistaken for a change.
        let change = collection.replace_all([("a", 9), ("a", 1)]);

        assert!(change.is_empty());
        assert_eq!(collection.get(&"a"), Some(&1));
    }

    #[test]
    fn retain_reports_what_it_dropped() {
        let mut collection: Collection<&str, u32> =
            [("a", 1), ("b", 2), ("c", 3)].into_iter().collect();

        let dropped = collection.retain(|_, value| *value % 2 == 1);

        assert_eq!(dropped, ["b"]);
        assert_eq!(collection.len(), 2);
    }

    #[test]
    fn iteration_is_key_ordered_regardless_of_insertion_order() {
        let mut collection = Collection::<&str, u32>::new();
        collection.upsert("c", 3);
        collection.upsert("a", 1);
        collection.upsert("b", 2);

        let keys: Vec<_> = collection.keys().copied().collect();
        assert_eq!(keys, ["a", "b", "c"]);
    }
}
