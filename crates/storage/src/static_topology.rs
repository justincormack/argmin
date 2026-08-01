use ec::EcConfig;
use placement::{
    ClusterMap, Level, NodeId, NodeInfo, PlacementConfig, PlacementConstraint, Placer, TopologyKey,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

const INITIAL_PG_PLACEMENT_KEY_DOMAIN: &[u8] = b"argmin-initial-pg-placement-v1";
const MAX_STATIC_STORAGE_PGS: usize = 4_096;

/// Deployment failure domain used to derive the initial storage placement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticStorageFailureDomain {
    None,
    Disk,
    Host,
}

/// Logical deployment identity for one storage node.
///
/// Host and disk labels come from the outer process manifest. Storage owns
/// their conversion into placement-engine topology levels and domain IDs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticStoragePlacementNode {
    node_id: u32,
    host: String,
    disk: String,
}

impl StaticStoragePlacementNode {
    #[must_use]
    pub fn new(node_id: u32, host: impl Into<String>, disk: impl Into<String>) -> Self {
        Self {
            node_id,
            host: host.into(),
            disk: disk.into(),
        }
    }
}

/// Storage-owned result of interpreting a static deployment topology.
#[derive(Clone, Eq, PartialEq)]
pub struct StaticInitialPgPlacement {
    acting_sets: Vec<Vec<u32>>,
}

impl StaticInitialPgPlacement {
    /// Returns the logical node-ID sets needed by the outer manifest codec and
    /// the subsequent storage-owned initial-map certificate builder.
    #[must_use]
    pub fn logical_acting_sets(&self) -> &[Vec<u32>] {
        &self.acting_sets
    }

    #[must_use]
    pub fn into_logical_acting_sets(self) -> Vec<Vec<u32>> {
        self.acting_sets
    }
}

impl fmt::Debug for StaticInitialPgPlacement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticInitialPgPlacement")
            .field("pg_count", &self.acting_sets.len())
            .finish_non_exhaustive()
    }
}

/// Semantic static-placement failure classified and formatted by the storage
/// owner without exposing the placement engine's error type.
pub struct StaticStoragePlacementError {
    message: String,
}

impl StaticStoragePlacementError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Debug for StaticStoragePlacementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticStoragePlacementError")
            .field("message", &self.message)
            .finish()
    }
}

impl fmt::Display for StaticStoragePlacementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StaticStoragePlacementError {}

/// Validate and derive the deterministic initial PG acting sets for a static
/// deployment.
pub fn derive_static_initial_pg_placement(
    pg_count: u32,
    ec_data_shards: u8,
    ec_parity_shards: u8,
    failure_domain: StaticStorageFailureDomain,
    declared_hosts: &[String],
    declared_disks: &[String],
    nodes: &[StaticStoragePlacementNode],
) -> Result<StaticInitialPgPlacement, StaticStoragePlacementError> {
    let pg_count = usize::try_from(pg_count).map_err(|_| {
        StaticStoragePlacementError::new("storage PG count does not fit this platform")
    })?;
    if pg_count > MAX_STATIC_STORAGE_PGS {
        return Err(StaticStoragePlacementError::new(format!(
            "storage PG count exceeds {MAX_STATIC_STORAGE_PGS} entries"
        )));
    }
    if pg_count == 0 {
        return Err(StaticStoragePlacementError::new(
            "storage PG count must be nonzero",
        ));
    }
    let ec = EcConfig::new(ec_data_shards, ec_parity_shards).map_err(|error| {
        StaticStoragePlacementError::new(format!("initial PG EC shape is invalid: {error}"))
    })?;

    let host_domains = stable_domain_ids(declared_hosts.iter().map(String::as_str), "host")?;
    let disk_domains = stable_domain_ids(declared_disks.iter().map(String::as_str), "disk")?;
    let mut ordered_nodes = nodes.iter().collect::<Vec<_>>();
    ordered_nodes.sort_by_key(|node| node.node_id);
    let placement_nodes = ordered_nodes
        .iter()
        .map(|node| {
            let host_domain = host_domains.get(node.host.as_str()).ok_or_else(|| {
                StaticStoragePlacementError::new(format!(
                    "storage node {} references undeclared host domain {:?}",
                    node.node_id, node.host
                ))
            })?;
            let disk_domain = disk_domains.get(node.disk.as_str()).ok_or_else(|| {
                StaticStoragePlacementError::new(format!(
                    "storage node {} references undeclared disk domain {:?}",
                    node.node_id, node.disk
                ))
            })?;
            let location =
                TopologyKey::new(&[(Level::MACHINE, *host_domain), (Level::DISK, *disk_domain)])
                    .expect("static host and disk use distinct built-in topology levels");
            Ok(NodeInfo {
                id: NodeId::new(node.node_id),
                location,
                weight: 1.0,
            })
        })
        .collect::<Result<Vec<_>, StaticStoragePlacementError>>()?;
    let cluster_map = ClusterMap::new(&placement_nodes).map_err(|error| {
        StaticStoragePlacementError::new(format!(
            "initial PG placement cluster map is invalid: {error}"
        ))
    })?;
    let total_shards = ec.total_shards();
    let placement_config = PlacementConfig::new(total_shards).map_err(|error| {
        StaticStoragePlacementError::new(format!("initial PG placement shape is invalid: {error}"))
    })?;
    let constraint = match failure_domain {
        StaticStorageFailureDomain::None => PlacementConstraint::none(),
        StaticStorageFailureDomain::Disk => PlacementConstraint::level_cap(Level::DISK, 1),
        StaticStorageFailureDomain::Host => PlacementConstraint::level_cap(Level::MACHINE, 1),
    };
    let placer = Placer::new(placement_config, &cluster_map, constraint).map_err(|error| {
        StaticStoragePlacementError::new(format!("initial PG placement is impossible: {error}"))
    })?;
    let selected_domains = ordered_nodes
        .iter()
        .map(|node| {
            let domain = match failure_domain {
                StaticStorageFailureDomain::None | StaticStorageFailureDomain::Host => {
                    node.host.as_str()
                }
                StaticStorageFailureDomain::Disk => node.disk.as_str(),
            };
            (node.node_id, domain)
        })
        .collect::<BTreeMap<_, _>>();

    let mut acting_sets = Vec::with_capacity(pg_count);
    let mut placement_key =
        Vec::with_capacity(INITIAL_PG_PLACEMENT_KEY_DOMAIN.len() + std::mem::size_of::<u32>());
    for pg_index in 0..pg_count {
        let pg_id =
            u32::try_from(pg_index).expect("bounded static storage PG count always fits in u32");
        placement_key.clear();
        placement_key.extend_from_slice(INITIAL_PG_PLACEMENT_KEY_DOMAIN);
        placement_key.extend_from_slice(&pg_id.to_be_bytes());
        let mut acting_set = vec![NodeId::new(0); total_shards];
        placer
            .place(&placement_key, &mut acting_set)
            .map_err(|error| {
                StaticStoragePlacementError::new(format!(
                    "initial placement for PG {pg_id} violates deployment policy: {error}"
                ))
            })?;
        if failure_domain != StaticStorageFailureDomain::None {
            let distinct_domains = acting_set
                .iter()
                .map(|node_id| selected_domains[&node_id.as_u32()])
                .collect::<BTreeSet<_>>();
            if distinct_domains.len() != total_shards {
                return Err(StaticStoragePlacementError::new(format!(
                    "initial placement for PG {pg_id} does not occupy {total_shards} distinct failure domains"
                )));
            }
        }
        acting_sets.push(acting_set.into_iter().map(NodeId::as_u32).collect());
    }

    Ok(StaticInitialPgPlacement { acting_sets })
}

fn stable_domain_ids<'a>(
    labels: impl Iterator<Item = &'a str>,
    kind: &str,
) -> Result<BTreeMap<&'a str, u32>, StaticStoragePlacementError> {
    labels
        .collect::<BTreeSet<_>>()
        .into_iter()
        .enumerate()
        .map(|(index, label)| {
            let domain = u32::try_from(index + 1).map_err(|_| {
                StaticStoragePlacementError::new(format!(
                    "static storage {kind} domain count does not fit u32"
                ))
            })?;
            Ok((label, domain))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placement_nodes() -> Vec<StaticStoragePlacementNode> {
        vec![
            StaticStoragePlacementNode::new(1, "host-a", "disk-a"),
            StaticStoragePlacementNode::new(2, "host-b", "disk-b"),
            StaticStoragePlacementNode::new(3, "host-c", "disk-c"),
            StaticStoragePlacementNode::new(4, "host-d", "disk-d"),
        ]
    }

    fn placement_hosts() -> Vec<String> {
        ["host-a", "host-b", "host-c", "host-d"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }

    fn placement_disks() -> Vec<String> {
        ["disk-a", "disk-b", "disk-c", "disk-d"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn static_initial_placement_is_input_order_independent_and_domain_safe() {
        let nodes = placement_nodes();
        let expected = derive_static_initial_pg_placement(
            8,
            2,
            1,
            StaticStorageFailureDomain::Host,
            &placement_hosts(),
            &placement_disks(),
            &nodes,
        )
        .unwrap();
        let mut reversed = nodes;
        reversed.reverse();
        let actual = derive_static_initial_pg_placement(
            8,
            2,
            1,
            StaticStorageFailureDomain::Host,
            &placement_hosts(),
            &placement_disks(),
            &reversed,
        )
        .unwrap();

        assert_eq!(actual, expected);
        assert_eq!(actual.logical_acting_sets().len(), 8);
        for acting_set in actual.logical_acting_sets() {
            assert_eq!(acting_set.len(), 3);
            assert_eq!(acting_set.iter().copied().collect::<BTreeSet<_>>().len(), 3);
        }
        assert_eq!(
            format!("{actual:?}"),
            "StaticInitialPgPlacement { pg_count: 8, .. }"
        );
    }

    #[test]
    fn static_initial_placement_distinguishes_host_and_disk_failure_domains() {
        let nodes = vec![
            StaticStoragePlacementNode::new(1, "host-a", "disk-a"),
            StaticStoragePlacementNode::new(2, "host-a", "disk-b"),
            StaticStoragePlacementNode::new(3, "host-b", "disk-c"),
        ];
        let hosts = ["host-a".to_owned(), "host-b".to_owned()];
        let disks = [
            "disk-a".to_owned(),
            "disk-b".to_owned(),
            "disk-c".to_owned(),
        ];
        let host_error = derive_static_initial_pg_placement(
            1,
            2,
            1,
            StaticStorageFailureDomain::Host,
            &hosts,
            &disks,
            &nodes,
        )
        .unwrap_err();
        assert!(host_error
            .to_string()
            .starts_with("initial placement for PG 0 violates deployment policy:"));

        let disk = derive_static_initial_pg_placement(
            1,
            2,
            1,
            StaticStorageFailureDomain::Disk,
            &hosts,
            &disks,
            &nodes,
        )
        .unwrap();
        assert_eq!(disk.logical_acting_sets()[0].len(), 3);
    }

    #[test]
    fn static_initial_placement_rejects_unbounded_pg_count_before_allocation() {
        let error = derive_static_initial_pg_placement(
            u32::try_from(MAX_STATIC_STORAGE_PGS + 1).unwrap(),
            1,
            0,
            StaticStorageFailureDomain::None,
            &placement_hosts(),
            &placement_disks(),
            &placement_nodes(),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("storage PG count exceeds {MAX_STATIC_STORAGE_PGS} entries")
        );
    }

    #[test]
    fn static_initial_placement_rejects_node_domains_absent_from_declared_topology() {
        let error = derive_static_initial_pg_placement(
            1,
            1,
            0,
            StaticStorageFailureDomain::None,
            &["host-a".to_owned()],
            &["disk-a".to_owned()],
            &[StaticStoragePlacementNode::new(
                1,
                "undeclared-host",
                "disk-a",
            )],
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "storage node 1 references undeclared host domain \"undeclared-host\""
        );
    }

    #[test]
    fn static_initial_placement_rejects_zero_pg_count_and_invalid_ec_shape() {
        let zero_pg_error = derive_static_initial_pg_placement(
            0,
            1,
            0,
            StaticStorageFailureDomain::None,
            &placement_hosts(),
            &placement_disks(),
            &placement_nodes(),
        )
        .unwrap_err();
        assert_eq!(
            zero_pg_error.to_string(),
            "storage PG count must be nonzero"
        );

        let invalid_ec_error = derive_static_initial_pg_placement(
            1,
            0,
            1,
            StaticStorageFailureDomain::None,
            &placement_hosts(),
            &placement_disks(),
            &placement_nodes(),
        )
        .unwrap_err();
        assert!(invalid_ec_error
            .to_string()
            .starts_with("initial PG EC shape is invalid:"));
    }
}
