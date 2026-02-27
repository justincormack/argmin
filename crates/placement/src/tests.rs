//! Integration tests for the placement crate.
//!
//! Tests the full pipeline: ClusterMap → PlacementConfig → Placer → place().
//! Test numbers refer to the test plan in plans/placement-api.md.

use crate::*;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn node(id: u32, rack: u32, weight: f64) -> NodeInfo {
    NodeInfo {
        id: NodeId::new(id),
        location: TopologyKey::rack(rack),
        weight,
    }
}

fn node_rm(id: u32, rack: u32, machine: u32, weight: f64) -> NodeInfo {
    NodeInfo {
        id: NodeId::new(id),
        location: TopologyKey::rack_machine(rack, machine),
        weight,
    }
}

/// Build a cluster: node_count nodes spread across rack_count racks.
fn build_cluster(node_count: u32, rack_count: u32, weight: f64) -> ClusterMap {
    let nodes: Vec<NodeInfo> = (0..node_count)
        .map(|i| node(i, i % rack_count, weight))
        .collect();
    ClusterMap::new(&nodes).unwrap()
}

fn place_once(placer: &Placer, key: &[u8]) -> Vec<NodeId> {
    let n = placer.config().total_shards as usize;
    let mut out = vec![NodeId::new(0); n];
    placer.place(key, &mut out).unwrap();
    out
}

// ── Test 4: Determinism ───────────────────────────────────────────────────────

#[test]
fn determinism_repeated_calls() {
    let map = build_cluster(12, 3, 1.0);
    let placer = Placer::new(
        PlacementConfig::new(6).unwrap(),
        &map,
        PlacementConstraint::rack_cap(2),
    )
    .unwrap();

    let first = place_once(&placer, b"some-object-key");
    for _ in 0..99 {
        let r = place_once(&placer, b"some-object-key");
        assert_eq!(r, first, "place() must be deterministic");
    }
}

#[test]
fn determinism_two_placers_same_inputs() {
    let map = build_cluster(12, 3, 1.0);
    let constraint = || PlacementConstraint::rack_cap(2);
    let config = PlacementConfig::new(6).unwrap();

    let p1 = Placer::new(config, &map, constraint()).unwrap();
    let p2 = Placer::new(config, &map, constraint()).unwrap();

    let r1 = place_once(&p1, b"key");
    let r2 = place_once(&p2, b"key");
    assert_eq!(r1, r2);
}

#[test]
fn different_keys_produce_different_placement() {
    let map = build_cluster(12, 3, 1.0);
    let placer = Placer::new(
        PlacementConfig::new(6).unwrap(),
        &map,
        PlacementConstraint::rack_cap(2),
    )
    .unwrap();
    let r1 = place_once(&placer, b"key-a");
    let r2 = place_once(&placer, b"key-b");
    // Different keys should produce different placements with overwhelming probability.
    assert_ne!(r1, r2, "different keys should produce different placements");
}

// ── Test 5: No duplicates ─────────────────────────────────────────────────────

#[test]
fn no_duplicate_nodes() {
    let map = build_cluster(12, 3, 1.0);
    let placer = Placer::new(
        PlacementConfig::new(6).unwrap(),
        &map,
        PlacementConstraint::rack_cap(2),
    )
    .unwrap();

    for seed in 0u32..100 {
        let out = place_once(&placer, &seed.to_le_bytes());
        let mut seen = std::collections::BTreeSet::new();
        for &nid in &out {
            assert!(
                seen.insert(nid),
                "duplicate node {nid:?} in placement for seed {seed}"
            );
        }
    }
}

#[test]
fn no_duplicates_when_total_equals_node_count() {
    let node_count = 8u32;
    let map = build_cluster(node_count, 4, 1.0);
    let placer = Placer::new(
        PlacementConfig::new(node_count as u8).unwrap(),
        &map,
        PlacementConstraint::none(),
    )
    .unwrap();

    let out = place_once(&placer, b"stress-key");
    let mut seen = std::collections::BTreeSet::new();
    for &nid in &out {
        assert!(seen.insert(nid));
    }
    assert_eq!(seen.len(), node_count as usize);
}

// ── Test 6: rack_cap / level_cap constraint respected ────────────────────────

#[test]
fn rack_cap_respected() {
    // 12 nodes, 3 racks (4 nodes each), place 6 shards with cap 2 per rack
    let map = build_cluster(12, 3, 1.0);
    let placer = Placer::new(
        PlacementConfig::new(6).unwrap(),
        &map,
        PlacementConstraint::rack_cap(2),
    )
    .unwrap();

    for seed in 0u32..200 {
        let out = place_once(&placer, &seed.to_le_bytes());
        // Count shards per rack
        let mut rack_counts = [0usize; 3];
        for &nid in &out {
            let rack = nid.as_u32() % 3; // nodes are placed round-robin on racks
            rack_counts[rack as usize] += 1;
        }
        for (r, &count) in rack_counts.iter().enumerate() {
            assert!(
                count <= 2,
                "rack {r} has {count} shards (max 2) for seed {seed}"
            );
        }
    }
}

#[test]
fn level_cap_zone_one_per_zone() {
    // 4 nodes, 2 zones of 2 nodes each, place 2 shards with zone cap 1
    let nodes: Vec<NodeInfo> = (0..4u32)
        .map(|i| NodeInfo {
            id: NodeId::new(i),
            location: TopologyKey::new(&[(Level::ZONE, i / 2), (Level::RACK, i)]).unwrap(),
            weight: 1.0,
        })
        .collect();
    let map = ClusterMap::new(&nodes).unwrap();
    let placer = Placer::new(
        PlacementConfig::new(2).unwrap(),
        &map,
        PlacementConstraint::level_cap(Level::ZONE, 1),
    )
    .unwrap();

    for seed in 0u32..100 {
        let out = place_once(&placer, &seed.to_le_bytes());
        // The two selected nodes must be from different zones
        let zone0 = out[0].as_u32() / 2;
        let zone1 = out[1].as_u32() / 2;
        assert_ne!(
            zone0, zone1,
            "two shards landed in same zone for seed {seed}"
        );
    }
}

#[test]
fn level_cap_machine() {
    // 4 racks × 2 machines per rack × 1 node per machine = 8 nodes
    // Place 4 shards with cap 1 per machine
    let nodes: Vec<NodeInfo> = (0..8u32).map(|i| node_rm(i, i / 2, i, 1.0)).collect();
    let map = ClusterMap::new(&nodes).unwrap();
    let placer = Placer::new(
        PlacementConfig::new(4).unwrap(),
        &map,
        PlacementConstraint::level_cap(Level::MACHINE, 1),
    )
    .unwrap();

    for seed in 0u32..100 {
        let out = place_once(&placer, &seed.to_le_bytes());
        let mut machines = std::collections::BTreeSet::new();
        for &nid in &out {
            let machine = nid.as_u32(); // 1 node per machine, machine id == node id
            assert!(
                machines.insert(machine),
                "machine collision for seed {seed}"
            );
        }
    }
}

// ── Test 7: ConstraintUnsatisfiable ──────────────────────────────────────────

#[test]
fn constraint_unsatisfiable_one_rack() {
    // 6 nodes all in rack 0, rack_cap(2), want 6 shards.
    // Placer::new checks active_node_count(6) >= total_shards(6), so it succeeds.
    // The constraint makes placement unsatisfiable at place() time.
    let nodes: Vec<NodeInfo> = (0..6u32).map(|i| node(i, 0, 1.0)).collect();
    let map = ClusterMap::new(&nodes).unwrap();
    let placer = Placer::new(
        PlacementConfig::new(6).unwrap(),
        &map,
        PlacementConstraint::rack_cap(2),
    )
    .unwrap();
    let mut out = vec![NodeId::new(0); 6];
    assert!(matches!(
        placer.place(b"key", &mut out),
        Err(PlacementError::ConstraintUnsatisfiable { shards: 6, .. })
    ));
}

#[test]
fn constraint_satisfiable_three_racks() {
    // 3 racks × 2 nodes, place 6 shards with rack_cap(2) — exactly fills all slots
    let nodes: Vec<NodeInfo> = (0..6u32).map(|i| node(i, i / 2, 1.0)).collect();
    let map = ClusterMap::new(&nodes).unwrap();
    let placer = Placer::new(
        PlacementConfig::new(6).unwrap(),
        &map,
        PlacementConstraint::rack_cap(2),
    )
    .unwrap();
    // Should succeed for any key
    for seed in 0u32..100 {
        assert!(placer
            .place(&seed.to_le_bytes(), &mut [NodeId::new(0); 6])
            .is_ok());
    }
}

#[test]
fn constraint_unsatisfiable_zone_cap() {
    // 2 zones × 3 nodes each, zone_cap(1), total_shards = 3
    // Only 2 zones available but need 3 shards, each zone can hold 1 → unsatisfiable
    let nodes: Vec<NodeInfo> = (0..6u32)
        .map(|i| NodeInfo {
            id: NodeId::new(i),
            location: TopologyKey::new(&[(Level::ZONE, i / 3)]).unwrap(),
            weight: 1.0,
        })
        .collect();
    let map = ClusterMap::new(&nodes).unwrap();
    let result = Placer::new(
        PlacementConfig::new(3).unwrap(),
        &map,
        PlacementConstraint::level_cap(Level::ZONE, 1),
    );
    // TooFewNodes since active_node_count(6) >= total_shards(3), but constraint
    // will make it unsatisfiable at place() time.
    // Actually: Placer::new checks active_node_count >= total_shards, which passes.
    let placer = result.unwrap();
    let mut out = vec![NodeId::new(0); 3];
    assert!(matches!(
        placer.place(b"key", &mut out),
        Err(PlacementError::ConstraintUnsatisfiable { shards: 3, .. })
    ));
}

// ── Test 8: Nodes without the constrained level ───────────────────────────────

#[test]
fn nodes_without_constrained_level_are_global() {
    // 2 nodes with ZONE, 4 nodes without ZONE; zone_cap(1); total_shards = 3
    // The 4 unconstrained nodes compete globally and can fill the remaining slots.
    let mut nodes: Vec<NodeInfo> = Vec::new();
    for i in 0..2u32 {
        nodes.push(NodeInfo {
            id: NodeId::new(i),
            location: TopologyKey::new(&[(Level::ZONE, i)]).unwrap(),
            weight: 1.0,
        });
    }
    for i in 2..6u32 {
        nodes.push(NodeInfo {
            id: NodeId::new(i),
            location: TopologyKey::new(&[]).unwrap(), // no ZONE segment
            weight: 1.0,
        });
    }
    let map = ClusterMap::new(&nodes).unwrap();
    let placer = Placer::new(
        PlacementConfig::new(3).unwrap(),
        &map,
        PlacementConstraint::level_cap(Level::ZONE, 1),
    )
    .unwrap();

    // Should succeed: the 4 unconstrained nodes fill remaining slots.
    let out = place_once(&placer, b"key");
    assert_eq!(out.len(), 3);
}

// ── Test 9: PlacementConstraint::none() ──────────────────────────────────────

#[test]
fn no_constraint_fills_all_slots() {
    let node_count = 8u32;
    let map = build_cluster(node_count, 1, 1.0); // all in same rack
    let placer = Placer::new(
        PlacementConfig::new(node_count as u8).unwrap(),
        &map,
        PlacementConstraint::none(),
    )
    .unwrap();

    let out = place_once(&placer, b"key");
    assert_eq!(out.len(), node_count as usize);
    // No duplicates
    let mut seen = std::collections::BTreeSet::new();
    for &nid in &out {
        assert!(seen.insert(nid));
    }
}

// ── Test 10: Custom constraint ────────────────────────────────────────────────

#[test]
fn custom_partition_matroid_constraint() {
    // Machine cap via closure: at most 2 from any machine.
    let map = build_cluster(12, 6, 1.0); // 12 nodes, 6 "machines" of 2 nodes each
    let machine_cap = 2usize;
    let constraint = PlacementConstraint {
        group_key: std::sync::Arc::new(|node| {
            // Group by rack (which here serves as the machine)
            node.location.level(Level::RACK).unwrap_or(u32::MAX) as u64
        }),
        admit: std::sync::Arc::new(move |same_group_count, _node| {
            if same_group_count < machine_cap {
                Admission::Global
            } else {
                Admission::Constrained
            }
        }),
    };
    let placer = Placer::new(PlacementConfig::new(6).unwrap(), &map, constraint).unwrap();

    for seed in 0u32..100 {
        let out = place_once(&placer, &seed.to_le_bytes());
        // Count per group (rack)
        let mut group_counts = std::collections::BTreeMap::<u32, usize>::new();
        for &nid in &out {
            let rack = nid.as_u32() % 6;
            *group_counts.entry(rack).or_default() += 1;
        }
        for (&rack, &count) in &group_counts {
            assert!(
                count <= machine_cap,
                "rack {rack} has {count} shards (max {machine_cap})"
            );
        }
    }
}

// ── Test 11: Constraint contract violation ────────────────────────────────────

#[test]
fn contract_violation_output_is_valid() {
    // Deliberately violate the partition matroid contract: the admit closure
    // uses shared mutable state (a global call counter) instead of deriving
    // its decision purely from same_group_count and a fixed per-group cap.
    // The effective cap for any given group varies depending on how many other
    // groups have already been processed — not a partition matroid.
    //
    // Per the documented contract, the algorithm gives no optimality or
    // consistency guarantees here. We assert only that the output is
    // structurally valid (no duplicates, correct length) if place() succeeds,
    // or that ConstraintUnsatisfiable is returned.
    use std::sync::atomic::{AtomicUsize, Ordering};

    let call_count = std::sync::Arc::new(AtomicUsize::new(0));
    let cc = call_count.clone();
    let map = build_cluster(12, 3, 1.0);
    let bad_constraint = PlacementConstraint {
        group_key: std::sync::Arc::new(|node| node.location.level(Level::RACK).unwrap_or(0) as u64),
        // VIOLATION: returns Global for the first 6 admit() calls regardless of
        // which group is being evaluated, and Constrained for all subsequent calls.
        // The per-group cap is not fixed — it depends on how many other groups
        // were evaluated first, which the algorithm's correctness proof cannot handle.
        admit: std::sync::Arc::new(move |_same_group, _node| {
            let n = cc.fetch_add(1, Ordering::Relaxed);
            if n < 6 {
                Admission::Global
            } else {
                Admission::Constrained
            }
        }),
    };
    let placer = Placer::new(PlacementConfig::new(6).unwrap(), &map, bad_constraint).unwrap();

    let mut out = vec![NodeId::new(0); 6];
    match placer.place(b"key", &mut out) {
        Ok(()) => {
            // If it succeeds, output must be structurally valid.
            let mut seen = std::collections::BTreeSet::new();
            for &nid in &out {
                assert!(
                    seen.insert(nid),
                    "duplicate node in output from bad constraint"
                );
            }
        }
        Err(PlacementError::ConstraintUnsatisfiable { .. }) => {
            // Also acceptable: contract violation may prevent filling all slots.
        }
        Err(e) => panic!("unexpected error from bad constraint: {e:?}"),
    }
}

// ── Test 17: distinct_count ───────────────────────────────────────────────────

#[test]
fn distinct_count_test() {
    let map = build_cluster(12, 3, 1.0);
    assert_eq!(map.distinct_count(Level::RACK), 3);
    assert_eq!(map.distinct_count(Level::ZONE), 0); // no ZONE segments
}

// ── Test 18: Large cluster ────────────────────────────────────────────────────

#[test]
fn large_cluster_succeeds() {
    let map = build_cluster(1000, 100, 1.0);
    let placer = Placer::new(
        PlacementConfig::new(8).unwrap(),
        &map,
        PlacementConstraint::rack_cap(2),
    )
    .unwrap();

    for seed in 0u32..200 {
        let out = place_once(&placer, &seed.to_le_bytes());
        // No duplicates
        let mut seen = std::collections::BTreeSet::new();
        for &nid in &out {
            assert!(seen.insert(nid));
        }
        // Rack cap respected (2 per rack)
        let mut rack_counts = std::collections::BTreeMap::<u32, usize>::new();
        for &nid in &out {
            let rack = nid.as_u32() % 100;
            *rack_counts.entry(rack).or_default() += 1;
        }
        for (&rack, &count) in &rack_counts {
            assert!(count <= 2, "rack {rack} has {count} shards, seed {seed}");
        }
    }
}

// ── Test 19: Zero heap allocation in hot path (ZONE_HOT compliance) ───────────

#[cfg(test)]
mod alloc_tests {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    // Thread-local counters: only allocations on the current thread are counted.
    // This allows other test threads to allocate freely without affecting the count.
    thread_local! {
        static COUNTING: Cell<bool> = const { Cell::new(false) };
        static ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
    }

    struct CountingAllocator;

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            COUNTING.with(|c| {
                if c.get() {
                    ALLOC_COUNT.with(|a| a.set(a.get() + 1));
                }
            });
            System.alloc(layout)
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            System.dealloc(ptr, layout)
        }
    }

    #[global_allocator]
    static A: CountingAllocator = CountingAllocator;

    fn count_allocs<F: FnOnce()>(f: F) -> usize {
        ALLOC_COUNT.with(|a| a.set(0));
        COUNTING.with(|c| c.set(true));
        f();
        COUNTING.with(|c| c.set(false));
        ALLOC_COUNT.with(|a| a.get())
    }

    #[test]
    fn place_zero_allocs_rack_cap() {
        let map = build_cluster(12, 3, 1.0);
        let placer = Placer::new(
            PlacementConfig::new(6).unwrap(),
            &map,
            PlacementConstraint::rack_cap(2),
        )
        .unwrap();
        let mut out = [NodeId::new(0); 6];

        let allocs = count_allocs(|| {
            placer.place(b"test-key", &mut out).unwrap();
        });
        assert_eq!(
            allocs, 0,
            "place() must not allocate with rack_cap constraint"
        );
    }

    #[test]
    fn place_zero_allocs_none_constraint() {
        let map = build_cluster(8, 4, 1.0);
        let placer = Placer::new(
            PlacementConfig::new(4).unwrap(),
            &map,
            PlacementConstraint::none(),
        )
        .unwrap();
        let mut out = [NodeId::new(0); 4];

        let allocs = count_allocs(|| {
            placer.place(b"test-key", &mut out).unwrap();
        });
        assert_eq!(allocs, 0, "place() must not allocate with none constraint");
    }

    #[test]
    fn place_zero_allocs_large_cluster() {
        let map = build_cluster(1000, 100, 1.0);
        let placer = Placer::new(
            PlacementConfig::new(16).unwrap(),
            &map,
            PlacementConstraint::rack_cap(2),
        )
        .unwrap();
        let mut out = [NodeId::new(0); 16];

        let allocs = count_allocs(|| {
            placer.place(b"test-key", &mut out).unwrap();
        });
        assert_eq!(
            allocs, 0,
            "place() must not allocate even for large clusters"
        );
    }

    #[test]
    fn place_zero_allocs_custom_closure() {
        let map = build_cluster(12, 6, 1.0);
        let constraint = PlacementConstraint {
            group_key: std::sync::Arc::new(|node| {
                node.location.level(Level::RACK).unwrap_or(0) as u64
            }),
            admit: std::sync::Arc::new(|same_group_count, _| {
                if same_group_count < 1 {
                    Admission::Global
                } else {
                    Admission::Constrained
                }
            }),
        };
        let placer = Placer::new(PlacementConfig::new(6).unwrap(), &map, constraint).unwrap();
        let mut out = [NodeId::new(0); 6];

        let allocs = count_allocs(|| {
            placer.place(b"test-key", &mut out).unwrap();
        });
        assert_eq!(
            allocs, 0,
            "place() must not allocate with custom closure constraint"
        );
    }
}

// ── Test 20: Property-based tests ────────────────────────────────────────────

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_no_duplicates_rack_cap(
            node_count in 2u32..=64,
            rack_count in 1u32..=16,
            total_shards in 1u8..=16,
            key in proptest::collection::vec(any::<u8>(), 0..=64),
        ) {
            let rack_count = rack_count.min(node_count);
            let total_shards = total_shards.min(node_count as u8);

            let nodes: Vec<NodeInfo> = (0..node_count)
                .map(|i| node(i, i % rack_count, 1.0))
                .collect();
            let map = ClusterMap::new(&nodes).unwrap();

            // Set cap high enough to always be satisfiable
            let max_per_rack = (total_shards as u32).div_ceil(rack_count) as usize;

            let placer = match Placer::new(
                PlacementConfig::new(total_shards).unwrap(),
                &map,
                PlacementConstraint::rack_cap(max_per_rack),
            ) {
                Ok(p) => p,
                Err(_) => return Ok(()), // skip if can't build placer
            };

            let mut out = vec![NodeId::new(0); total_shards as usize];
            match placer.place(&key, &mut out) {
                Ok(()) => {
                    // No duplicates
                    let mut seen = std::collections::BTreeSet::new();
                    for &nid in &out {
                        prop_assert!(seen.insert(nid), "duplicate node in output");
                    }
                    // Rack cap respected
                    let mut rack_counts = std::collections::BTreeMap::<u32, usize>::new();
                    for &nid in &out {
                        let rack = nid.as_u32() % rack_count;
                        *rack_counts.entry(rack).or_default() += 1;
                    }
                    for &count in rack_counts.values() {
                        prop_assert!(count <= max_per_rack);
                    }
                }
                Err(PlacementError::ConstraintUnsatisfiable { .. }) => {
                    // Acceptable: constraint may not always be satisfiable
                }
                Err(e) => {
                    prop_assert!(false, "unexpected error: {e:?}");
                }
            }
        }
    }
}
