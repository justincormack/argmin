use crate::cluster::NodeInfo;
use crate::topology::Level;
use std::sync::Arc;

pub type GroupKeyFn = dyn Fn(&NodeInfo) -> u64 + Send + Sync;
pub type AdmitFn = dyn Fn(usize, &NodeInfo) -> Admission + Send + Sync;

/// The admission decision returned by a placement constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// No constraint applies; the node competes globally for a slot.
    Global,

    /// The node may only replace a candidate from the same constraint group
    /// (same group key as computed by PlacementConstraint::group_key).
    Constrained,

    /// Skip this node; it cannot be placed under any circumstances.
    Excluded,
}

/// A pluggable placement constraint.
///
/// **Correctness contract**: the greedy streaming selection algorithm is proven
/// correct only for partition matroid constraints. A constraint is valid if:
///
/// - Every node has a fixed group, determined solely by `group_key(node)`.
/// - Each group has a fixed capacity that does not depend on other groups.
/// - `admit(same_group_count, node)` returns `Global` when below cap, `Constrained`
///   when at or above cap.
///
/// Violating this contract may produce suboptimal or inconsistent placement.
///
/// Two responsibilities are kept separate so ZONE_INIT and ZONE_HOT work is cleanly
/// separated:
///
/// **`group_key`** — pure function of a node. Called once per node at `Placer::new`
/// (ZONE_INIT) and stored. In the hot path, same-group candidates are found by
/// comparing stored u64 keys.
///
/// **`admit`** — called once per candidate node during `place()` (ZONE_HOT).
/// Must not allocate. Must implement a partition matroid (see above).
pub struct PlacementConstraint {
    pub group_key: Arc<GroupKeyFn>,
    pub admit: Arc<AdmitFn>,
}

impl PlacementConstraint {
    /// Cap on the number of shards sharing a given topology level value.
    ///
    /// Satisfies the partition matroid contract: group = level value, fixed cap.
    ///
    /// Nodes without a segment for `level` are treated as unconstrained (Global).
    /// Their group_key sentinel (u64::MAX) keeps them isolated from real level values.
    pub fn level_cap(level: Level, max: usize) -> Self {
        PlacementConstraint {
            group_key: Arc::new(move |node| {
                node.location
                    .level(level)
                    .map(|v| v as u64)
                    .unwrap_or(u64::MAX)
            }),
            admit: Arc::new(move |same_group_count, new_node| {
                if new_node.location.level(level).is_none() {
                    return Admission::Global; // node has no segment at this level
                }
                if same_group_count < max {
                    Admission::Global
                } else {
                    Admission::Constrained
                }
            }),
        }
    }

    /// Rack cap. Equivalent to `level_cap(Level::RACK, max)`.
    /// Default for a (k, m) EC scheme: `max = m` (parity count).
    pub fn rack_cap(max: usize) -> Self {
        Self::level_cap(Level::RACK, max)
    }

    /// No constraint: all nodes compete globally.
    pub fn none() -> Self {
        PlacementConstraint {
            group_key: Arc::new(|_| 0),
            admit: Arc::new(|_, _| Admission::Global),
        }
    }
}
