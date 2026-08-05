//! Property tests for the one primitive every dxgate resource cache is built
//! on.
//!
//! These encode the invariants that hand-written state management gets wrong.
//! Each one corresponds to a real failure mode:
//!
//! * *add then remove leaves no trace* — the classic "stale index entry after a
//!   delete", where a resource keeps being routed to after it is gone.
//! * *independent updates converge regardless of order* — watch events arrive in
//!   whatever order the API server and the network produce them; a cache whose
//!   final state depends on that order is a cache that differs between replicas.
//! * *replaying the same state-of-the-world is a no-op* — a resync, a reconnect,
//!   or a periodic re-list must not look like a change, or every reconnect
//!   stampedes the whole downstream pipeline.
//! * *a state-of-the-world replace equals its input* — the property that makes a
//!   re-list a safe recovery action after a lost watch.
//!
//! Testing them here, once, is the point: the alternative is re-arguing them at
//! every call site, which is how dxgate previously shipped an xDS client that
//! never re-subscribed after a reconnect.

use dxgate_core::store::Collection;
use proptest::prelude::*;
use std::collections::BTreeMap;

/// One event against a collection, mirroring what a source can emit.
#[derive(Debug, Clone)]
enum Op {
    Upsert(u8, u8),
    Remove(u8),
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(
        prop_oneof![
            (0u8..8, 0u8..4).prop_map(|(key, value)| Op::Upsert(key, value)),
            (0u8..8).prop_map(Op::Remove),
        ],
        0..24,
    )
}

fn apply(collection: &mut Collection<u8, u8>, ops: &[Op]) {
    for op in ops {
        match op {
            Op::Upsert(key, value) => {
                collection.upsert(*key, *value);
            }
            Op::Remove(key) => {
                collection.remove(key);
            }
        }
    }
}

fn contents(collection: &Collection<u8, u8>) -> BTreeMap<u8, u8> {
    collection.iter().map(|(k, v)| (*k, *v)).collect()
}

proptest! {
    /// Adding a key and then removing it must leave the collection exactly as it
    /// was — no residue in the map, no residue in the key set.
    #[test]
    fn add_then_remove_leaves_no_trace(setup in ops(), key in 0u8..8, value in 0u8..4) {
        let mut collection = Collection::<u8, u8>::new();
        apply(&mut collection, &setup);
        prop_assume!(!collection.contains_key(&key));

        let before = contents(&collection);
        collection.upsert(key, value);
        collection.remove(&key);

        prop_assert_eq!(contents(&collection), before);
    }

    /// Upserts of distinct keys commute: the order watch events arrive in cannot
    /// change where the cache ends up.
    #[test]
    fn upserts_of_distinct_keys_commute(entries in prop::collection::btree_map(0u8..8, 0u8..4, 0..8)) {
        let forward: Vec<(u8, u8)> = entries.iter().map(|(k, v)| (*k, *v)).collect();
        let mut reversed = forward.clone();
        reversed.reverse();

        let mut a = Collection::<u8, u8>::new();
        for (key, value) in forward {
            a.upsert(key, value);
        }
        let mut b = Collection::<u8, u8>::new();
        for (key, value) in reversed {
            b.upsert(key, value);
        }

        prop_assert_eq!(contents(&a), contents(&b));
    }

    /// Whatever sequence of events got the collection here, replacing it with its
    /// own contents must report nothing. A resync is not a change.
    #[test]
    fn replaying_current_contents_reports_no_change(setup in ops()) {
        let mut collection = Collection::<u8, u8>::new();
        apply(&mut collection, &setup);
        let before = contents(&collection);

        let change = collection.replace_all(before.clone());

        prop_assert!(change.is_empty(), "resync reported {:?}", change);
        prop_assert_eq!(contents(&collection), before);
    }

    /// A state-of-the-world replace is authoritative: afterwards the collection
    /// is exactly the input, whatever it held before.
    #[test]
    fn replace_all_makes_the_collection_equal_its_input(
        setup in ops(),
        next in prop::collection::btree_map(0u8..8, 0u8..4, 0..8),
    ) {
        let mut collection = Collection::<u8, u8>::new();
        apply(&mut collection, &setup);

        collection.replace_all(next.clone());

        prop_assert_eq!(contents(&collection), next);
    }

    /// The reported change set must account for the difference exactly: every
    /// retired key was present and is now gone, every upserted key is present
    /// and actually differs, and nothing else moved.
    #[test]
    fn replace_all_change_set_explains_the_difference(
        setup in ops(),
        next in prop::collection::btree_map(0u8..8, 0u8..4, 0..8),
    ) {
        let mut collection = Collection::<u8, u8>::new();
        apply(&mut collection, &setup);
        let before = contents(&collection);

        let change = collection.replace_all(next.clone());

        for key in &change.removed {
            prop_assert!(before.contains_key(key));
            prop_assert!(!next.contains_key(key));
        }
        for key in &change.upserted {
            prop_assert_eq!(before.get(key) != next.get(key), true);
            prop_assert!(next.contains_key(key));
        }
        let expected_removed: Vec<u8> = before
            .keys()
            .filter(|key| !next.contains_key(key))
            .copied()
            .collect();
        let expected_upserted: Vec<u8> = next
            .iter()
            .filter(|(key, value)| before.get(key) != Some(value))
            .map(|(key, _)| *key)
            .collect();
        prop_assert_eq!(change.removed, expected_removed);
        prop_assert_eq!(change.upserted, expected_upserted);
    }

    /// Applying the same event twice is the same as applying it once. Sources
    /// redeliver; that must not be observable.
    #[test]
    fn events_are_idempotent(setup in ops(), op_key in 0u8..8, op_value in 0u8..4, remove in any::<bool>()) {
        let mut once = Collection::<u8, u8>::new();
        apply(&mut once, &setup);
        let mut twice = once.clone();

        if remove {
            once.remove(&op_key);
            twice.remove(&op_key);
            twice.remove(&op_key);
        } else {
            once.upsert(op_key, op_value);
            twice.upsert(op_key, op_value);
            twice.upsert(op_key, op_value);
        }

        prop_assert_eq!(contents(&once), contents(&twice));
    }

    /// A second identical event reports no change, which is what allows a caller
    /// to skip downstream work rather than propagate a no-op.
    #[test]
    fn a_repeated_upsert_reports_no_change(setup in ops(), key in 0u8..8, value in 0u8..4) {
        let mut collection = Collection::<u8, u8>::new();
        apply(&mut collection, &setup);

        collection.upsert(key, value);
        prop_assert!(!collection.upsert(key, value));
    }

    /// The key set stays in step with the contents. Indexes derived from it
    /// cannot go stale behind the map.
    #[test]
    fn key_set_tracks_contents(setup in ops()) {
        let mut collection = Collection::<u8, u8>::new();
        apply(&mut collection, &setup);

        let expected: std::collections::BTreeSet<u8> = contents(&collection).into_keys().collect();
        prop_assert_eq!(collection.key_set(), expected);
    }
}
