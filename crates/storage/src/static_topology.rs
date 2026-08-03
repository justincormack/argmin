use crate::control_plane::{
    ClusterControlSnapshot, ControlPlaneError, InitialClusterTopologyCertificate,
};
use crate::control_plane_command::{
    encode_control_plane_command, ControlPlaneCommand, ControlPlaneCommandStateMachine,
};
use crate::control_plane_raft::validate_control_plane_command_replication_size;
use crate::types::PgId;
use ec::EcConfig;
use placement::{
    ClusterMap, Level, NodeId as PlacementNodeId, NodeInfo, PlacementConfig, PlacementConstraint,
    Placer, TopologyKey,
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

/// Logical endpoint for one storage node in a static deployment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticStorageNodeEndpoint {
    node_id: u32,
    endpoint: String,
}

impl StaticStorageNodeEndpoint {
    #[must_use]
    pub fn new(node_id: u32, endpoint: impl Into<String>) -> Self {
        Self {
            node_id,
            endpoint: endpoint.into(),
        }
    }
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
    node_ids: Vec<u32>,
    acting_sets: Vec<Vec<u32>>,
}

impl StaticInitialPgPlacement {
    /// Returns the logical node-ID sets needed by the outer manifest codec and
    /// the subsequent storage-owned initial-map certificate builder.
    #[must_use]
    pub fn logical_acting_sets(&self) -> &[Vec<u32>] {
        &self.acting_sets
    }
}

/// Opaque storage-owned initial topology for a static control plane.
///
/// The value retains the physical PG identities, node identities, acting
/// sets, and topology certificate needed by storage bootstrap. Process code
/// can bind it to Raft and ask storage to bootstrap or validate it, but cannot
/// construct those storage representations itself.
#[derive(Clone)]
pub struct StaticInitialControlPlaneTopology {
    nodes: Vec<(PlacementNodeId, String)>,
    pg_acting_sets: Vec<(PgId, Vec<PlacementNodeId>)>,
    certificate: InitialClusterTopologyCertificate,
}

/// Opaque storage-owned initial topology for the environment-only control
/// plane configuration path.
///
/// Unlike [`StaticInitialControlPlaneTopology`], this legacy configuration has
/// no outer manifest identity from which storage can derive a certificate.
/// Storage still owns conversion of the logical node and PG integers into the
/// private bootstrap command, validates that command before startup, and keeps
/// those representations out of the process layer.
#[derive(Clone)]
pub struct UncertifiedInitialControlPlaneTopology {
    nodes: Vec<(PlacementNodeId, String)>,
    pg_ids: Vec<PgId>,
}

impl UncertifiedInitialControlPlaneTopology {
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    #[must_use]
    pub fn pg_count(&self) -> usize {
        self.pg_ids.len()
    }

    pub(crate) fn bootstrap_command(&self) -> Option<ControlPlaneCommand> {
        if self.nodes.is_empty() {
            return None;
        }
        Some(ControlPlaneCommand::BootstrapInitialClusterMap {
            nodes: self.nodes.clone(),
            pg_ids: self.pg_ids.clone(),
        })
    }

    pub(crate) fn initialized_epoch(&self, snapshot: &ClusterControlSnapshot) -> Option<u64> {
        (snapshot.nodes().next().is_some() || snapshot.pgs().next().is_some())
            .then(|| snapshot.cluster_epoch().get())
    }
}

impl fmt::Debug for UncertifiedInitialControlPlaneTopology {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UncertifiedInitialControlPlaneTopology")
            .field("node_count", &self.nodes.len())
            .field("pg_count", &self.pg_ids.len())
            .finish_non_exhaustive()
    }
}

impl StaticInitialControlPlaneTopology {
    #[must_use]
    pub fn pg_count(&self) -> usize {
        self.pg_acting_sets.len()
    }

    /// Validate whether a control-plane snapshot is either wholly
    /// uninitialized or established with this exact certified topology.
    ///
    /// `Ok(false)` means the snapshot is empty and may be bootstrapped;
    /// `Ok(true)` means the configured topology is already established.
    pub(crate) fn validate_snapshot(
        &self,
        snapshot: &ClusterControlSnapshot,
    ) -> Result<bool, StaticStorageTopologyError> {
        if snapshot.nodes().next().is_none() && snapshot.pgs().next().is_none() {
            return Ok(false);
        }
        if snapshot.initial_topology() != Some(&self.certificate) {
            return Err(StaticStorageTopologyError::new(
                "applied initial topology certificate does not match configured topology",
            ));
        }
        Ok(true)
    }

    /// Validate that this topology's certified bootstrap command fits the
    /// configured control-plane replication envelope.
    pub fn validate_replication_size(&self) -> Result<(), StaticStorageTopologyError> {
        validate_control_plane_command_replication_size(&self.bootstrap_command())
            .map_err(|error| StaticStorageTopologyError::new(error.to_string()))
    }

    pub(crate) fn certificate(&self) -> &InitialClusterTopologyCertificate {
        &self.certificate
    }

    pub(crate) fn bootstrap_command(&self) -> ControlPlaneCommand {
        ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes: self.nodes.clone(),
            pg_acting_sets: self.pg_acting_sets.clone(),
            topology: self.certificate.clone(),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_topology_generation(&self) -> u64 {
        self.certificate.topology_generation()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_raft_voters(&self) -> &[u64] {
        self.certificate.raft_voters()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_logical_acting_sets(&self) -> Vec<Vec<u32>> {
        self.pg_acting_sets
            .iter()
            .map(|(_, acting_set)| {
                acting_set
                    .iter()
                    .copied()
                    .map(PlacementNodeId::as_u32)
                    .collect()
            })
            .collect()
    }
}

impl fmt::Debug for StaticInitialControlPlaneTopology {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticInitialControlPlaneTopology")
            .field("node_count", &self.nodes.len())
            .field("pg_count", &self.pg_acting_sets.len())
            .field("raft_voter_count", &self.certificate.raft_voters().len())
            .finish_non_exhaustive()
    }
}

/// Semantic static-topology failure formatted by storage without exposing
/// control-plane command or certificate representations.
pub struct StaticStorageTopologyError {
    message: String,
}

impl StaticStorageTopologyError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Translate this owner-formatted logical configuration failure into the
    /// control-plane error boundary used by the process-hosted authority.
    #[must_use]
    pub fn into_control_plane_error(self) -> ControlPlaneError {
        ControlPlaneError::InvalidInitialTopology {
            message: self.message,
        }
    }
}

impl fmt::Debug for StaticStorageTopologyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticStorageTopologyError")
            .field("message", &self.message)
            .finish()
    }
}

impl fmt::Display for StaticStorageTopologyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StaticStorageTopologyError {}

/// Bind owner-validated placement, logical endpoints, and the outer manifest
/// identity into one certified initial control-plane topology.
pub fn derive_static_initial_control_plane_topology(
    topology_generation: u64,
    topology_digest_hex: &str,
    raft_voters: &[u64],
    node_endpoints: &[StaticStorageNodeEndpoint],
    placement: StaticInitialPgPlacement,
) -> Result<StaticInitialControlPlaneTopology, StaticStorageTopologyError> {
    let topology_digest = decode_topology_digest(topology_digest_hex)?;
    let mut endpoints = BTreeMap::new();
    for node in node_endpoints {
        if node.endpoint.is_empty() {
            return Err(StaticStorageTopologyError::new(format!(
                "static storage node {} has an empty endpoint",
                node.node_id
            )));
        }
        if endpoints
            .insert(node.node_id, node.endpoint.clone())
            .is_some()
        {
            return Err(StaticStorageTopologyError::new(format!(
                "static storage node {} has duplicate endpoints",
                node.node_id
            )));
        }
    }
    let endpoint_node_ids = endpoints.keys().copied().collect::<Vec<_>>();
    if endpoint_node_ids != placement.node_ids {
        return Err(StaticStorageTopologyError::new(
            "static storage endpoint nodes do not match the validated placement nodes",
        ));
    }

    let nodes = endpoints
        .into_iter()
        .map(|(node_id, endpoint)| (PlacementNodeId::new(node_id), endpoint))
        .collect::<Vec<_>>();
    let pg_acting_sets = placement
        .acting_sets
        .into_iter()
        .enumerate()
        .map(|(pg_index, acting_set)| {
            let pg_id = u32::try_from(pg_index)
                .map(PgId::new)
                .expect("bounded static storage PG count always fits in u32");
            (
                pg_id,
                acting_set.into_iter().map(PlacementNodeId::new).collect(),
            )
        })
        .collect::<Vec<_>>();
    let certificate = InitialClusterTopologyCertificate::new_for_bootstrap_map(
        topology_generation,
        topology_digest,
        raft_voters.to_vec(),
        &nodes,
        &pg_acting_sets,
    )
    .map_err(|error| StaticStorageTopologyError::new(error.to_string()))?;
    let topology = StaticInitialControlPlaneTopology {
        nodes,
        pg_acting_sets,
        certificate,
    };
    encode_control_plane_command(&topology.bootstrap_command())
        .map_err(|error| StaticStorageTopologyError::new(error.to_string()))?;
    ClusterControlSnapshot::empty()
        .apply_control_plane_command(topology.bootstrap_command())
        .map_err(|error| StaticStorageTopologyError::new(error.to_string()))?;
    Ok(topology)
}

/// Validate logical environment configuration and retain its uncertified
/// initial control-plane topology behind an opaque storage-owned value.
pub fn derive_uncertified_initial_control_plane_topology(
    node_endpoints: &[StaticStorageNodeEndpoint],
    pg_ids: &[u32],
) -> Result<UncertifiedInitialControlPlaneTopology, StaticStorageTopologyError> {
    let mut endpoints = BTreeMap::new();
    for node in node_endpoints {
        if node.endpoint.is_empty() {
            return Err(StaticStorageTopologyError::new(format!(
                "environment storage node {} has an empty endpoint",
                node.node_id
            )));
        }
        if endpoints
            .insert(node.node_id, node.endpoint.clone())
            .is_some()
        {
            return Err(StaticStorageTopologyError::new(format!(
                "environment storage node {} has duplicate endpoints",
                node.node_id
            )));
        }
    }
    let topology = UncertifiedInitialControlPlaneTopology {
        nodes: endpoints
            .into_iter()
            .map(|(node_id, endpoint)| (PlacementNodeId::new(node_id), endpoint))
            .collect(),
        pg_ids: pg_ids.iter().copied().map(PgId::new).collect(),
    };
    let Some(command) = topology.bootstrap_command() else {
        // Preserve the existing environment behavior: no configured storage
        // nodes means there is no initial-map bootstrap, irrespective of the
        // default PG list.
        return Ok(topology);
    };
    validate_control_plane_command_replication_size(&command).map_err(|error| {
        StaticStorageTopologyError::new(format!(
            "environment initial control-plane topology exceeds the replication envelope: {error}"
        ))
    })?;
    ClusterControlSnapshot::empty()
        .apply_control_plane_command(command)
        .map_err(|error| {
            StaticStorageTopologyError::new(format!(
                "environment initial control-plane topology is invalid: {error}"
            ))
        })?;
    Ok(topology)
}

fn decode_topology_digest(
    digest: &str,
) -> Result<[u8; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN], StaticStorageTopologyError>
{
    const DIGEST_LEN: usize = crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN;
    if digest.len() != DIGEST_LEN * 2 {
        return Err(StaticStorageTopologyError::new(
            "static topology digest must contain 32 bytes",
        ));
    }
    let mut decoded = [0_u8; DIGEST_LEN];
    for (output, pair) in decoded.iter_mut().zip(digest.as_bytes().chunks_exact(2)) {
        let high = decode_hex_nibble(pair[0]).ok_or_else(|| {
            StaticStorageTopologyError::new("static topology digest contains invalid hex")
        })?;
        let low = decode_hex_nibble(pair[1]).ok_or_else(|| {
            StaticStorageTopologyError::new("static topology digest contains invalid hex")
        })?;
        *output = (high << 4) | low;
    }
    Ok(decoded)
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
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
                id: PlacementNodeId::new(node.node_id),
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
        let mut acting_set = vec![PlacementNodeId::new(0); total_shards];
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
        acting_sets.push(
            acting_set
                .into_iter()
                .map(PlacementNodeId::as_u32)
                .collect(),
        );
    }

    Ok(StaticInitialPgPlacement {
        node_ids: ordered_nodes.iter().map(|node| node.node_id).collect(),
        acting_sets,
    })
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
    use crate::control_plane_command::{ControlPlaneLogId, ReplicatedControlPlaneStateMachine};
    use crate::control_plane_raft::{
        ControlPlaneRaftPeerTransportLimits, ControlPlaneRaftPeerTransportPolicy,
    };

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

    fn initial_topology_inputs() -> (StaticInitialPgPlacement, Vec<StaticStorageNodeEndpoint>) {
        let nodes = vec![
            StaticStoragePlacementNode::new(1, "host-a", "disk-a"),
            StaticStoragePlacementNode::new(2, "host-b", "disk-b"),
        ];
        let placement = derive_static_initial_pg_placement(
            2,
            1,
            0,
            StaticStorageFailureDomain::None,
            &["host-a".to_owned(), "host-b".to_owned()],
            &["disk-a".to_owned(), "disk-b".to_owned()],
            &nodes,
        )
        .unwrap();
        let endpoints = vec![
            StaticStorageNodeEndpoint::new(1, "/tmp/node-1.sock"),
            StaticStorageNodeEndpoint::new(2, "/tmp/node-2.sock"),
        ];
        (placement, endpoints)
    }

    fn initial_topology(generation: u64) -> StaticInitialControlPlaneTopology {
        let (placement, endpoints) = initial_topology_inputs();
        derive_static_initial_control_plane_topology(
            generation,
            &"aa".repeat(32),
            &[101, 102, 103],
            &endpoints,
            placement,
        )
        .unwrap()
    }

    #[test]
    fn uncertified_initial_topology_owns_and_validates_the_legacy_bootstrap_command() {
        let topology = derive_uncertified_initial_control_plane_topology(
            &[
                StaticStorageNodeEndpoint::new(2, "/tmp/node-2.sock"),
                StaticStorageNodeEndpoint::new(1, "/tmp/node-1.sock"),
            ],
            &[7, 3],
        )
        .unwrap();

        assert_eq!(topology.node_count(), 2);
        assert_eq!(topology.pg_count(), 2);
        assert_eq!(
            format!("{topology:?}"),
            "UncertifiedInitialControlPlaneTopology { node_count: 2, pg_count: 2, .. }"
        );
        let command = topology.bootstrap_command().unwrap();
        assert_eq!(
            command,
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (PlacementNodeId::new(1), "/tmp/node-1.sock".to_owned()),
                    (PlacementNodeId::new(2), "/tmp/node-2.sock".to_owned()),
                ],
                pg_ids: vec![PgId::new(7), PgId::new(3)],
            }
        );
        let applied = ClusterControlSnapshot::empty()
            .apply_control_plane_command(command)
            .unwrap();
        assert_eq!(
            topology.initialized_epoch(applied.snapshot()),
            Some(applied.snapshot().cluster_epoch().get())
        );
    }

    #[test]
    fn uncertified_initial_topology_preserves_no_node_noop_and_rejects_invalid_inputs() {
        let empty = derive_uncertified_initial_control_plane_topology(&[], &[1, 2]).unwrap();
        assert_eq!(empty.node_count(), 0);
        assert_eq!(empty.pg_count(), 2);
        assert!(empty.bootstrap_command().is_none());

        let duplicate_endpoint = derive_uncertified_initial_control_plane_topology(
            &[
                StaticStorageNodeEndpoint::new(1, "/tmp/node-1.sock"),
                StaticStorageNodeEndpoint::new(1, "/tmp/other-node-1.sock"),
            ],
            &[1],
        )
        .unwrap_err();
        assert!(duplicate_endpoint
            .to_string()
            .contains("environment storage node 1 has duplicate endpoints"));

        let duplicate_pg = derive_uncertified_initial_control_plane_topology(
            &[StaticStorageNodeEndpoint::new(1, "/tmp/node-1.sock")],
            &[1, 1],
        )
        .unwrap_err();
        assert!(duplicate_pg
            .to_string()
            .contains("control-plane bootstrap repeats PG 1"));
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

    #[test]
    fn static_initial_topology_owns_bootstrap_and_validates_established_certificate() {
        let topology = initial_topology(7);
        assert!(!topology
            .validate_snapshot(&ClusterControlSnapshot::empty())
            .unwrap());
        assert_eq!(
            format!("{topology:?}"),
            "StaticInitialControlPlaneTopology { node_count: 2, pg_count: 2, raft_voter_count: 3, .. }"
        );
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            "static-topology-test",
            [
                (101, "/tmp/raft-101.sock".to_owned()),
                (102, "/tmp/raft-102.sock".to_owned()),
                (103, "/tmp/raft-103.sock".to_owned()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        )
        .with_static_initial_topology(&topology);
        assert_eq!(
            policy.initial_topology_certificate(),
            Some(topology.certificate())
        );
        assert_eq!(
            policy.topology_identity(),
            Some(
                &crate::control_plane_raft::ControlPlaneRaftTopologyIdentity {
                    generation: 7,
                    digest: "aa".repeat(32),
                }
            )
        );

        let mut state_machine = ReplicatedControlPlaneStateMachine::empty();
        state_machine
            .apply_committed_command(
                ControlPlaneLogId::new(1, 1).unwrap(),
                topology.bootstrap_command(),
            )
            .unwrap();
        assert!(topology
            .validate_snapshot(state_machine.snapshot())
            .unwrap());

        let current_primary = state_machine
            .snapshot()
            .pg(PgId::new(0))
            .unwrap()
            .acting_set()[0];
        let replacement = if current_primary == PlacementNodeId::new(1) {
            PlacementNodeId::new(2)
        } else {
            PlacementNodeId::new(1)
        };
        state_machine
            .apply_committed_command(
                ControlPlaneLogId::new(1, 2).unwrap(),
                ControlPlaneCommand::SetPgActingSet {
                    pg_id: PgId::new(0),
                    acting_set: vec![replacement],
                },
            )
            .unwrap();
        assert!(topology
            .validate_snapshot(state_machine.snapshot())
            .unwrap());

        let wrong_generation = initial_topology(8);
        assert_eq!(
            wrong_generation
                .validate_snapshot(state_machine.snapshot())
                .unwrap_err()
                .to_string(),
            "applied initial topology certificate does not match configured topology"
        );
    }

    #[test]
    fn static_initial_topology_rejects_endpoint_identity_and_digest_errors() {
        let (placement, endpoints) = initial_topology_inputs();
        let error = derive_static_initial_control_plane_topology(
            7,
            &"aa".repeat(32),
            &[101],
            &[StaticStorageNodeEndpoint::new(1, "/tmp/node-1.sock")],
            placement.clone(),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "static storage endpoint nodes do not match the validated placement nodes"
        );

        let error = derive_static_initial_control_plane_topology(
            7,
            &"aa".repeat(32),
            &[101],
            &[
                StaticStorageNodeEndpoint::new(1, "/tmp/node-1.sock"),
                StaticStorageNodeEndpoint::new(1, "/tmp/other-node-1.sock"),
                StaticStorageNodeEndpoint::new(2, "/tmp/node-2.sock"),
            ],
            placement.clone(),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "static storage node 1 has duplicate endpoints"
        );

        let error = derive_static_initial_control_plane_topology(
            7,
            &"aa".repeat(32),
            &[101],
            &[
                StaticStorageNodeEndpoint::new(1, ""),
                StaticStorageNodeEndpoint::new(2, "/tmp/node-2.sock"),
            ],
            placement.clone(),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "static storage node 1 has an empty endpoint"
        );

        let error = derive_static_initial_control_plane_topology(
            7,
            "aa",
            &[101],
            &endpoints,
            placement.clone(),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "static topology digest must contain 32 bytes"
        );

        let error = derive_static_initial_control_plane_topology(
            7,
            &"AA".repeat(32),
            &[101],
            &[
                StaticStorageNodeEndpoint::new(1, "/tmp/node-1.sock"),
                StaticStorageNodeEndpoint::new(2, "/tmp/node-2.sock"),
            ],
            placement,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "static topology digest contains invalid hex"
        );

        for (voters, expected) in [
            (
                Vec::new(),
                "initial topology must contain at least one Raft voter",
            ),
            (
                vec![102, 101],
                "initial topology Raft voters must be strictly increasing",
            ),
        ] {
            let (placement, endpoints) = initial_topology_inputs();
            let error = derive_static_initial_control_plane_topology(
                7,
                &"aa".repeat(32),
                &voters,
                &endpoints,
                placement,
            )
            .unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "unexpected voter validation error: {error}"
            );
        }
    }
}
