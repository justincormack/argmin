use std::sync::Arc;
use crate::cluster::{ClusterMap, NodeId, NodeInfo};
use crate::config::{PlacementConfig, PlacementError};
use crate::constraint::{Admission, PlacementConstraint};
use crate::hash::score;
use crate::MAX_SHARDS;

/// A single entry in the candidate buffer.
#[derive(Clone, Copy)]
struct Candidate {
    score: f64,
    node_id: NodeId,
    group_key: u64,
}

const EMPTY_CAND: Candidate = Candidate {
    score: f64::INFINITY,
    node_id: NodeId::new(0),
    group_key: 0,
};

/// Stateless placement engine. Constructed once at ZONE_INIT; place() is the hot path.
/// Send + Sync: place() takes &self; all mutable state is stack-local.
pub struct Placer {
    // Note: Arc<dyn Fn> is not Debug; we implement Debug manually below.
    config: PlacementConfig,
    /// Node info paired with precomputed group_key (computed at ZONE_INIT, not hot path).
    nodes: Vec<(NodeInfo, u64)>,
    /// The admit function from the constraint. group_key is not needed after new().
    admit: Arc<dyn Fn(usize, &NodeInfo) -> Admission + Send + Sync>,
}

impl std::fmt::Debug for Placer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Placer")
            .field("config", &self.config)
            .field("node_count", &self.nodes.len())
            .finish_non_exhaustive()
    }
}

impl Placer {
    /// Construct from a config, cluster map, and constraint.
    ///
    /// Precomputes group_key for every node in the map (ZONE_INIT).
    /// Returns Err(TooFewNodes) if active_node_count < total_shards.
    ///
    /// ZONE_INIT: allocates.
    pub fn new(
        config: PlacementConfig,
        map: &ClusterMap,
        constraint: PlacementConstraint,
    ) -> Result<Self, PlacementError> {
        let active = map.active_node_count();
        if active < config.total_shards as usize {
            return Err(PlacementError::TooFewNodes {
                shards: config.total_shards as usize,
                nodes: active,
            });
        }
        let nodes: Vec<(NodeInfo, u64)> = map
            .nodes()
            .iter()
            .map(|n| {
                let gk = (constraint.group_key)(n);
                (n.clone(), gk)
            })
            .collect();
        Ok(Placer { config, nodes, admit: constraint.admit })
    }

    pub fn config(&self) -> PlacementConfig {
        self.config
    }

    /// Assign total_shards nodes to shards for the given key.
    ///
    /// `key`:  arbitrary bytes — object key, PG id, etc. May be empty.
    /// `out`:  caller-allocated; length must equal config.total_shards.
    ///         On success, out[i] is the NodeId for shard i.
    ///
    /// Returns Err(ConstraintUnsatisfiable) if the constraint prevents filling all
    /// slots. Returns Err(OutputLengthMismatch) if out.len() != total_shards.
    ///
    /// ZONE_HOT: no heap allocation. admit must not allocate.
    pub fn place(&self, key: &[u8], out: &mut [NodeId]) -> Result<(), PlacementError> {
        let total = self.config.total_shards as usize;

        if out.len() != total {
            return Err(PlacementError::OutputLengthMismatch {
                got: out.len(),
                expected: total,
            });
        }

        // Stack-allocated candidate buffer; only indices 0..cand_len are valid.
        let mut cands = [EMPTY_CAND; MAX_SHARDS];
        let mut cand_len: usize = 0;

        // Group-count association list: [(group_key, count)]; only 0..gc_len are valid.
        // Invariant: gc_len <= cand_len (one entry per distinct group in cands).
        let mut gc = [(0u64, 0u8); MAX_SHARDS];
        let mut gc_len: usize = 0;

        for &(ref node, gk) in &self.nodes {
            if node.weight == 0.0 {
                continue;
            }

            let s = score(key, node.id, node.weight);
            let same_group = gc_get(&gc, gc_len, gk) as usize;
            let admission = (self.admit)(same_group, node);

            match admission {
                Admission::Excluded => continue,

                Admission::Global => {
                    if cand_len < total {
                        // Buffer has space: append unconditionally.
                        cands[cand_len] =
                            Candidate { score: s, node_id: node.id, group_key: gk };
                        cand_len += 1;
                        gc_inc(&mut gc, &mut gc_len, gk);
                    } else {
                        // Buffer full: replace the worst candidate if this is better.
                        let wi = find_worst(&cands, cand_len);
                        if s < cands[wi].score {
                            let old_gk = cands[wi].group_key;
                            gc_dec(&mut gc, &mut gc_len, old_gk);
                            cands[wi] =
                                Candidate { score: s, node_id: node.id, group_key: gk };
                            gc_inc(&mut gc, &mut gc_len, gk);
                        }
                    }
                }

                Admission::Constrained => {
                    // May only replace the worst candidate from the same group.
                    // If no same-group candidate exists, skip (max = 0 semantics).
                    if let Some(wi) = find_worst_in_group(&cands, cand_len, gk) {
                        if s < cands[wi].score {
                            // Replace within group; group count is unchanged.
                            cands[wi] =
                                Candidate { score: s, node_id: node.id, group_key: gk };
                        }
                    }
                }
            }
        }

        if cand_len < total {
            return Err(PlacementError::ConstraintUnsatisfiable {
                shards: total,
                filled: cand_len,
            });
        }

        for i in 0..total {
            out[i] = cands[i].node_id;
        }
        Ok(())
    }
}

/// Return the count of candidates in the buffer whose group_key matches `gk`.
fn gc_get(gc: &[(u64, u8)], gc_len: usize, gk: u64) -> u8 {
    for i in 0..gc_len {
        if gc[i].0 == gk {
            return gc[i].1;
        }
    }
    0
}

/// Find or insert `gk` in the group-count list and increment its count.
fn gc_inc(gc: &mut [(u64, u8)], gc_len: &mut usize, gk: u64) {
    for i in 0..*gc_len {
        if gc[i].0 == gk {
            gc[i].1 += 1;
            return;
        }
    }
    // New group: insert. gc_len <= cand_len <= total <= MAX_SHARDS so in-bounds.
    gc[*gc_len] = (gk, 1);
    *gc_len += 1;
}

/// Decrement the count for `gk`. Remove the entry if count reaches 0.
fn gc_dec(gc: &mut [(u64, u8)], gc_len: &mut usize, gk: u64) {
    for i in 0..*gc_len {
        if gc[i].0 == gk {
            if gc[i].1 <= 1 {
                // Remove by shifting remaining entries left.
                for j in i..*gc_len - 1 {
                    gc[j] = gc[j + 1];
                }
                *gc_len -= 1;
            } else {
                gc[i].1 -= 1;
            }
            return;
        }
    }
}

/// Return the index of the candidate with the highest score (worst candidate).
/// cand_len must be >= 1.
fn find_worst(cands: &[Candidate], cand_len: usize) -> usize {
    let mut worst_idx = 0;
    let mut worst_score = cands[0].score;
    for i in 1..cand_len {
        if cands[i].score > worst_score {
            worst_score = cands[i].score;
            worst_idx = i;
        }
    }
    worst_idx
}

/// Return the index of the highest-score candidate with group_key == gk,
/// or None if no such candidate exists.
fn find_worst_in_group(cands: &[Candidate], cand_len: usize, gk: u64) -> Option<usize> {
    let mut worst_idx: Option<usize> = None;
    let mut worst_score = f64::NEG_INFINITY;
    for i in 0..cand_len {
        if cands[i].group_key == gk && cands[i].score > worst_score {
            worst_score = cands[i].score;
            worst_idx = Some(i);
        }
    }
    worst_idx
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::topology::TopologyKey;

    fn node(id: u32, rack: u32, weight: f64) -> NodeInfo {
        NodeInfo {
            id: NodeId::new(id),
            location: TopologyKey::rack(rack),
            weight,
        }
    }

    fn build_cluster(node_count: usize, racks: usize, weight: f64) -> ClusterMap {
        let nodes: Vec<NodeInfo> = (0..node_count as u32)
            .map(|i| node(i, i % racks as u32, weight))
            .collect();
        ClusterMap::new(&nodes).unwrap()
    }

    // ── gc helpers ────────────────────────────────────────────────────────────

    #[test]
    fn gc_basic_operations() {
        let mut gc = [(0u64, 0u8); MAX_SHARDS];
        let mut gc_len = 0usize;

        assert_eq!(gc_get(&gc, gc_len, 42), 0);

        gc_inc(&mut gc, &mut gc_len, 42);
        assert_eq!(gc_len, 1);
        assert_eq!(gc_get(&gc, gc_len, 42), 1);

        gc_inc(&mut gc, &mut gc_len, 42);
        assert_eq!(gc_len, 1);
        assert_eq!(gc_get(&gc, gc_len, 42), 2);

        gc_inc(&mut gc, &mut gc_len, 99);
        assert_eq!(gc_len, 2);
        assert_eq!(gc_get(&gc, gc_len, 99), 1);

        gc_dec(&mut gc, &mut gc_len, 42);
        assert_eq!(gc_get(&gc, gc_len, 42), 1);

        gc_dec(&mut gc, &mut gc_len, 42);
        assert_eq!(gc_len, 1, "entry should be removed when count reaches 0");
        assert_eq!(gc_get(&gc, gc_len, 42), 0);
    }

    // ── find_worst / find_worst_in_group ─────────────────────────────────────

    #[test]
    fn find_worst_basic() {
        let cands = [
            Candidate { score: 2.0, node_id: NodeId::new(0), group_key: 0 },
            Candidate { score: 5.0, node_id: NodeId::new(1), group_key: 0 },
            Candidate { score: 1.0, node_id: NodeId::new(2), group_key: 0 },
        ];
        assert_eq!(find_worst(&cands, 3), 1);
    }

    #[test]
    fn find_worst_in_group_basic() {
        let cands = [
            Candidate { score: 2.0, node_id: NodeId::new(0), group_key: 1 },
            Candidate { score: 5.0, node_id: NodeId::new(1), group_key: 2 },
            Candidate { score: 3.0, node_id: NodeId::new(2), group_key: 1 },
        ];
        // Worst in group 1: index 2 (score 3.0 > 2.0)
        assert_eq!(find_worst_in_group(&cands, 3, 1), Some(2));
        // Worst in group 2: index 1
        assert_eq!(find_worst_in_group(&cands, 3, 2), Some(1));
        // No entries for group 3
        assert_eq!(find_worst_in_group(&cands, 3, 3), None);
    }

    // ── output length mismatch ────────────────────────────────────────────────

    #[test]
    fn output_length_mismatch_short() {
        let map = build_cluster(6, 3, 1.0);
        let placer = Placer::new(
            PlacementConfig::new(6).unwrap(),
            &map,
            PlacementConstraint::none(),
        )
        .unwrap();
        let mut out = [NodeId::new(0); 5];
        assert_eq!(
            placer.place(b"key", &mut out),
            Err(PlacementError::OutputLengthMismatch { got: 5, expected: 6 })
        );
    }

    #[test]
    fn output_length_mismatch_long() {
        let map = build_cluster(6, 3, 1.0);
        let placer = Placer::new(
            PlacementConfig::new(6).unwrap(),
            &map,
            PlacementConstraint::none(),
        )
        .unwrap();
        let mut out = [NodeId::new(0); 7];
        assert_eq!(
            placer.place(b"key", &mut out),
            Err(PlacementError::OutputLengthMismatch { got: 7, expected: 6 })
        );
    }

    // ── too few nodes ─────────────────────────────────────────────────────────

    #[test]
    fn too_few_nodes() {
        let map = build_cluster(4, 2, 1.0);
        assert_eq!(
            Placer::new(
                PlacementConfig::new(6).unwrap(),
                &map,
                PlacementConstraint::none()
            )
            .unwrap_err(),
            PlacementError::TooFewNodes { shards: 6, nodes: 4 }
        );
    }

    #[test]
    fn all_zero_weight_is_too_few() {
        let nodes: Vec<NodeInfo> = (0..6).map(|i| node(i, i, 0.0)).collect();
        let map = ClusterMap::new(&nodes).unwrap();
        assert_eq!(
            Placer::new(
                PlacementConfig::new(6).unwrap(),
                &map,
                PlacementConstraint::none()
            )
            .unwrap_err(),
            PlacementError::TooFewNodes { shards: 6, nodes: 0 }
        );
    }
}
