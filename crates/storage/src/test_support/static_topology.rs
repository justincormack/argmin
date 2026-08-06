use crate::StaticInitialControlPlaneTopology;

/// Logical observations for a certified static topology.
///
/// The certificate and placement records remain storage-owned. Downstream
/// configuration tests can inspect only the logical values they supplied.
pub trait StaticInitialControlPlaneTopologyTestSupport {
    fn test_topology_generation(&self) -> u64;

    fn test_raft_voters(&self) -> &[u64];

    fn test_logical_acting_sets(&self) -> Vec<Vec<u32>>;
}

impl StaticInitialControlPlaneTopologyTestSupport for StaticInitialControlPlaneTopology {
    fn test_topology_generation(&self) -> u64 {
        StaticInitialControlPlaneTopology::test_topology_generation(self)
    }

    fn test_raft_voters(&self) -> &[u64] {
        StaticInitialControlPlaneTopology::test_raft_voters(self)
    }

    fn test_logical_acting_sets(&self) -> Vec<Vec<u32>> {
        StaticInitialControlPlaneTopology::test_logical_acting_sets(self)
    }
}
