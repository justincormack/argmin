use std::sync::Arc;
use crate::topology::{Level, TopologyKey};
use crate::config::PlacementError;

/// Opaque identifier for a storage node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u32);

impl NodeId {
    pub const fn new(id: u32) -> Self {
        NodeId(id)
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

/// Description of one storage node, supplied at ClusterMap construction.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Application-assigned node ID. Must be unique within the map.
    pub id: NodeId,
    /// Physical location in the topology hierarchy.
    pub location: TopologyKey,
    /// Relative capacity weight. Proportional to storage capacity.
    /// Nodes with weight 0.0 are excluded from placement.
    /// Must be finite and non-negative.
    pub weight: f64,
}

/// Immutable snapshot of the cluster topology.
///
/// Constructed once (ZONE_INIT) when cluster membership changes.
/// Cheap to clone (Arc-wrapped internals). Send + Sync.
#[derive(Clone, Debug)]
pub struct ClusterMap(Arc<Vec<NodeInfo>>);

impl ClusterMap {
    /// Construct from a slice of node descriptions.
    ///
    /// Returns Err if:
    /// - `nodes` is empty
    /// - any weight is negative, NaN, or infinite
    /// - any NodeId is duplicated
    ///
    /// Nodes are stored sorted by NodeId (canonical scan order for place(),
    /// and tie-breaking order for equal scores).
    ///
    /// ZONE_INIT: allocates.
    pub fn new(nodes: &[NodeInfo]) -> Result<Self, PlacementError> {
        if nodes.is_empty() {
            return Err(PlacementError::EmptyCluster);
        }
        for n in nodes {
            if !n.weight.is_finite() || n.weight < 0.0 {
                return Err(PlacementError::InvalidWeight {
                    id: n.id.as_u32(),
                    weight: n.weight,
                });
            }
        }
        let mut sorted: Vec<NodeInfo> = nodes.to_vec();
        sorted.sort_unstable_by_key(|n| n.id);
        for w in sorted.windows(2) {
            if w[0].id == w[1].id {
                return Err(PlacementError::DuplicateNodeId { id: w[0].id.as_u32() });
            }
        }
        Ok(ClusterMap(Arc::new(sorted)))
    }

    /// Number of nodes with weight > 0.0.
    pub fn active_node_count(&self) -> usize {
        self.0.iter().filter(|n| n.weight > 0.0).count()
    }

    /// Number of distinct values at a given topology level among active nodes.
    pub fn distinct_count(&self, level: Level) -> usize {
        // Use a stack-local vec bounded by the node count; allocates but this is
        // a helper called in ZONE_INIT only.
        let mut seen: Vec<u32> = Vec::new();
        for n in self.0.iter().filter(|n| n.weight > 0.0) {
            if let Some(v) = n.location.level(level) {
                if !seen.contains(&v) {
                    seen.push(v);
                }
            }
        }
        seen.len()
    }

    /// Sum of weights across all active nodes.
    pub fn total_weight(&self) -> f64 {
        self.0.iter().filter(|n| n.weight > 0.0).map(|n| n.weight).sum()
    }

    pub(crate) fn nodes(&self) -> &[NodeInfo] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u32, rack: u32, weight: f64) -> NodeInfo {
        NodeInfo {
            id: NodeId::new(id),
            location: TopologyKey::rack(rack),
            weight,
        }
    }

    #[test]
    fn empty_cluster() {
        assert_eq!(ClusterMap::new(&[]).unwrap_err(), PlacementError::EmptyCluster);
    }

    #[test]
    fn negative_weight() {
        let err = ClusterMap::new(&[node(0, 0, -1.0)]).unwrap_err();
        assert!(matches!(err, PlacementError::InvalidWeight { id: 0, .. }));
    }

    #[test]
    fn nan_weight() {
        let err = ClusterMap::new(&[node(0, 0, f64::NAN)]).unwrap_err();
        assert!(matches!(err, PlacementError::InvalidWeight { id: 0, .. }));
    }

    #[test]
    fn infinite_weight() {
        let err = ClusterMap::new(&[node(0, 0, f64::INFINITY)]).unwrap_err();
        assert!(matches!(err, PlacementError::InvalidWeight { id: 0, .. }));
    }

    #[test]
    fn duplicate_node_id() {
        let nodes = vec![node(1, 0, 1.0), node(1, 1, 1.0)];
        assert_eq!(
            ClusterMap::new(&nodes).unwrap_err(),
            PlacementError::DuplicateNodeId { id: 1 }
        );
    }

    #[test]
    fn valid_cluster_sorted_by_id() {
        // Provide nodes out of id order; they should be stored sorted.
        let nodes = vec![node(5, 0, 1.0), node(2, 1, 1.0), node(8, 2, 1.0)];
        let map = ClusterMap::new(&nodes).unwrap();
        let stored = map.nodes();
        assert_eq!(stored[0].id, NodeId::new(2));
        assert_eq!(stored[1].id, NodeId::new(5));
        assert_eq!(stored[2].id, NodeId::new(8));
    }

    #[test]
    fn active_node_count_excludes_zero_weight() {
        let nodes = vec![node(0, 0, 0.0), node(1, 0, 1.0), node(2, 1, 1.0)];
        let map = ClusterMap::new(&nodes).unwrap();
        assert_eq!(map.active_node_count(), 2);
    }

    #[test]
    fn distinct_count() {
        // 12 nodes, 3 racks (4 nodes each)
        let nodes: Vec<NodeInfo> = (0..12).map(|i| node(i, i / 4, 1.0)).collect();
        let map = ClusterMap::new(&nodes).unwrap();
        assert_eq!(map.distinct_count(Level::RACK), 3);
    }

    #[test]
    fn distinct_count_ignores_zero_weight() {
        let nodes = vec![
            node(0, 0, 0.0), // rack 0, zero weight – should not count
            node(1, 1, 1.0),
            node(2, 2, 1.0),
        ];
        let map = ClusterMap::new(&nodes).unwrap();
        assert_eq!(map.distinct_count(Level::RACK), 2);
    }

    #[test]
    fn distinct_count_missing_level() {
        // Nodes with no ZONE segment – should not appear in ZONE distinct_count
        let nodes = vec![
            NodeInfo {
                id: NodeId::new(0),
                location: TopologyKey::rack(0),
                weight: 1.0,
            },
            NodeInfo {
                id: NodeId::new(1),
                location: TopologyKey::rack(1),
                weight: 1.0,
            },
        ];
        let map = ClusterMap::new(&nodes).unwrap();
        assert_eq!(map.distinct_count(Level::ZONE), 0);
        assert_eq!(map.distinct_count(Level::RACK), 2);
    }

    #[test]
    fn total_weight() {
        let nodes = vec![node(0, 0, 1.0), node(1, 0, 2.0), node(2, 1, 0.0)];
        let map = ClusterMap::new(&nodes).unwrap();
        assert_eq!(map.total_weight(), 3.0);
    }

    #[test]
    fn cheap_clone() {
        let nodes = vec![node(0, 0, 1.0)];
        let map = ClusterMap::new(&nodes).unwrap();
        let clone = map.clone();
        assert!(Arc::ptr_eq(&map.0, &clone.0));
    }
}
