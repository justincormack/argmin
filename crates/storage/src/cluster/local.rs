use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use placement::{NodeId, PlacementConstraint, PlacementError, TopologyKey};

use super::ShardLocation;
use crate::error::{ClusterBuildError, StoreError};
use crate::{ClusterEpoch, DataPgId, EcShape, ShardIndex, SharedStorageNode};

const PAYLOAD_SHARD_PLACEMENT_KEY_DOMAIN: &[u8] = b"argmin/payload-shard-placement/v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalNodeStoreConfig {
    node_id: NodeId,
    data_dir: PathBuf,
}

impl LocalNodeStoreConfig {
    pub fn new(node_id: NodeId, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            data_dir: data_dir.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

pub struct LocalNodeStore {
    node_id: NodeId,
    data_dir: PathBuf,
    storage_node: Arc<SharedStorageNode>,
}

impl LocalNodeStore {
    fn new(node_id: NodeId, data_dir: PathBuf, storage_node: Arc<SharedStorageNode>) -> Self {
        Self {
            node_id,
            data_dir,
            storage_node,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn storage_node(&self) -> &Arc<SharedStorageNode> {
        &self.storage_node
    }
}

impl std::fmt::Debug for LocalNodeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalNodeStore")
            .field("node_id", &self.node_id)
            .field("data_dir", &self.data_dir)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct LocalClusterMap {
    epoch: ClusterEpoch,
    metadata_primary_node_id: NodeId,
    nodes: BTreeMap<NodeId, LocalNodeStore>,
    placement_map: placement::ClusterMap,
    process_local_registry_key: usize,
}

impl LocalClusterMap {
    pub fn open(
        data_dir: &Path,
        node_ids: &[NodeId],
        pg_ids: &[u32],
        default_ec_shape: EcShape,
    ) -> Result<Self, ClusterBuildError> {
        let configs: Vec<LocalNodeStoreConfig> = node_ids
            .iter()
            .map(|&node_id| {
                LocalNodeStoreConfig::new(
                    node_id,
                    data_dir.join(format!("node-{:04}", node_id.as_u32())),
                )
            })
            .collect();
        Self::open_with_configs(NodeId::new(0), configs, pg_ids, default_ec_shape)
    }

    pub fn open_with_configs(
        metadata_primary_node_id: NodeId,
        configs: impl IntoIterator<Item = LocalNodeStoreConfig>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
    ) -> Result<Self, ClusterBuildError> {
        let configs: Vec<LocalNodeStoreConfig> = configs.into_iter().collect();
        if configs.is_empty() {
            return Err(ClusterBuildError::EmptyCluster);
        }

        let mut node_ids = BTreeSet::<NodeId>::new();
        for config in &configs {
            if !node_ids.insert(config.node_id) {
                return Err(ClusterBuildError::DuplicateNodeId {
                    id: config.node_id.as_u32(),
                });
            }
        }
        if !node_ids.contains(&metadata_primary_node_id) {
            return Err(ClusterBuildError::MetadataPrimaryNotFound {
                id: metadata_primary_node_id.as_u32(),
            });
        }
        let placement_map = build_local_placement_map(node_ids.iter().copied())?;
        validate_local_payload_placement(&placement_map, default_ec_shape)?;

        let mut data_dirs = BTreeMap::<PathBuf, NodeId>::new();
        let mut validated_configs = Vec::with_capacity(configs.len());

        for config in configs {
            let canonical_data_dir = prepare_local_node_data_dir(config.node_id, &config.data_dir)?;
            if let Some(first_node_id) =
                data_dirs.insert(canonical_data_dir.clone(), config.node_id)
            {
                return Err(ClusterBuildError::DuplicateDataDir {
                    first_node_id: first_node_id.as_u32(),
                    duplicate_node_id: config.node_id.as_u32(),
                    data_dir: canonical_data_dir,
                });
            }
            validated_configs.push((config.node_id, canonical_data_dir));
        }

        let mut nodes = BTreeMap::new();
        for (node_id, canonical_data_dir) in validated_configs {
            let storage_node = SharedStorageNode::open_with_default_ec_shape(
                &canonical_data_dir,
                pg_ids,
                default_ec_shape,
            )
            .map_err(|source| ClusterBuildError::OpenLocalNode {
                node_id: node_id.as_u32(),
                source,
            })?;
            nodes.insert(
                node_id,
                LocalNodeStore::new(node_id, canonical_data_dir, Arc::new(storage_node)),
            );
        }

        let metadata_primary = nodes
            .get(&metadata_primary_node_id)
            .expect("validated metadata primary should have been opened");

        Ok(Self {
            epoch: ClusterEpoch::INITIAL,
            metadata_primary_node_id,
            placement_map,
            process_local_registry_key: Arc::as_ptr(metadata_primary.storage_node()) as usize,
            nodes,
        })
    }

    pub fn epoch(&self) -> ClusterEpoch {
        self.epoch
    }

    pub fn metadata_primary_node_id(&self) -> NodeId {
        self.metadata_primary_node_id
    }

    pub fn metadata_primary(&self) -> &LocalNodeStore {
        self.nodes
            .get(&self.metadata_primary_node_id)
            .expect("local cluster map metadata primary must exist")
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes.keys().copied()
    }

    pub fn node(&self, node_id: NodeId) -> Option<&LocalNodeStore> {
        self.nodes.get(&node_id)
    }

    pub fn process_local_registry_key(&self) -> usize {
        self.process_local_registry_key
    }

    pub fn place_payload_shards(
        &self,
        data_pg_id: DataPgId,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
    ) -> Result<Vec<ShardLocation>, ClusterBuildError> {
        let ec_config = ec_config_for_shape(ec_shape)?;
        let total_shards = ec_config.total_shards();
        let placer = local_payload_placer(&self.placement_map, ec_shape)?;
        let placement_key = payload_shard_placement_key(data_pg_id, stable_placement_key);
        let mut node_ids = vec![NodeId::new(0); total_shards];
        placer
            .place(&placement_key, &mut node_ids)
            .map_err(|error| placement_error_for_shape(ec_shape, error))?;

        Ok(node_ids
            .into_iter()
            .enumerate()
            .map(|(shard_index, node_id)| {
                ShardLocation::new(
                    self.epoch,
                    data_pg_id,
                    ShardIndex::new(shard_index as u8),
                    node_id,
                )
            })
            .collect())
    }

    pub fn payload_shard_node(
        &self,
        data_pg_id: DataPgId,
        shard_index: ShardIndex,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
    ) -> Result<NodeId, ClusterBuildError> {
        let locations = self.place_payload_shards(data_pg_id, ec_shape, stable_placement_key)?;
        locations
            .get(usize::from(shard_index.get()))
            .map(ShardLocation::node_id)
            .ok_or(ClusterBuildError::InvalidShardIndex {
                data_shards: ec_shape.k,
                parity_shards: ec_shape.m,
                shard_index: shard_index.get(),
            })
    }
}

fn validate_local_payload_placement(
    placement_map: &placement::ClusterMap,
    default_ec_shape: EcShape,
) -> Result<(), ClusterBuildError> {
    local_payload_placer(placement_map, default_ec_shape)?;
    Ok(())
}

fn local_payload_placer(
    placement_map: &placement::ClusterMap,
    ec_shape: EcShape,
) -> Result<placement::Placer, ClusterBuildError> {
    let ec_config = ec_config_for_shape(ec_shape)?;
    let placement_config = placement::PlacementConfig::new(ec_config.total_shards() as u8)
        .map_err(|error| invalid_ec_shape_error(ec_shape, error))?;
    placement::Placer::new(placement_config, placement_map, PlacementConstraint::none())
        .map_err(|error| placement_error_for_shape(ec_shape, error))
}

fn ec_config_for_shape(ec_shape: EcShape) -> Result<ec::EcConfig, ClusterBuildError> {
    ec::EcConfig::new(ec_shape.k, ec_shape.m)
        .map_err(|error| invalid_ec_shape_error(ec_shape, error))
}

fn invalid_ec_shape_error(ec_shape: EcShape, error: impl std::fmt::Display) -> ClusterBuildError {
    ClusterBuildError::InvalidEcShape {
        data_shards: ec_shape.k,
        parity_shards: ec_shape.m,
        reason: error.to_string(),
    }
}

fn placement_error_for_shape(ec_shape: EcShape, error: PlacementError) -> ClusterBuildError {
    match error {
        PlacementError::TooFewNodes { shards, nodes } => ClusterBuildError::UnplaceableEcShape {
            data_shards: ec_shape.k,
            parity_shards: ec_shape.m,
            required_nodes: shards,
            node_count: nodes,
        },
        other => ClusterBuildError::InvalidLocalPlacement {
            reason: other.to_string(),
        },
    }
}

fn build_local_placement_map(
    node_ids: impl IntoIterator<Item = NodeId>,
) -> Result<placement::ClusterMap, ClusterBuildError> {
    let placement_nodes: Vec<placement::NodeInfo> = node_ids
        .into_iter()
        .map(local_placement_node_info)
        .collect();
    placement::ClusterMap::new(&placement_nodes).map_err(|error| {
        ClusterBuildError::InvalidLocalPlacement {
            reason: error.to_string(),
        }
    })
}

fn payload_shard_placement_key(data_pg_id: DataPgId, stable_placement_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        PAYLOAD_SHARD_PLACEMENT_KEY_DOMAIN.len()
            + std::mem::size_of::<u32>()
            + stable_placement_key.len(),
    );
    key.extend_from_slice(PAYLOAD_SHARD_PLACEMENT_KEY_DOMAIN);
    key.extend_from_slice(&data_pg_id.get().to_be_bytes());
    key.extend_from_slice(stable_placement_key);
    key
}

fn local_placement_node_info(node_id: NodeId) -> placement::NodeInfo {
    placement::NodeInfo {
        id: node_id,
        location: TopologyKey::rack_machine(node_id.as_u32(), node_id.as_u32()),
        weight: 1.0,
    }
}

fn prepare_local_node_data_dir(
    node_id: NodeId,
    data_dir: &Path,
) -> Result<PathBuf, ClusterBuildError> {
    std::fs::create_dir_all(data_dir).map_err(|source| ClusterBuildError::OpenLocalNode {
        node_id: node_id.as_u32(),
        source: StoreError::Io {
            context: "create local node data dir",
            source,
        },
    })?;
    data_dir
        .canonicalize()
        .map_err(|source| ClusterBuildError::OpenLocalNode {
            node_id: node_id.as_u32(),
            source: StoreError::Io {
                context: "canonicalize local node data dir",
                source,
            },
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn opens_distinct_local_node_stores_with_static_epoch() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();

        assert_eq!(map.epoch(), ClusterEpoch::INITIAL);
        assert_eq!(map.metadata_primary_node_id(), NodeId::new(0));
        assert_eq!(map.node_count(), 3);
        assert_eq!(map.node_ids().collect::<Vec<_>>(), node_ids);

        for node_id in node_ids {
            let node = map.node(node_id).unwrap();
            assert_eq!(node.node_id(), node_id);
            assert!(node
                .data_dir()
                .ends_with(format!("node-{:04}", node_id.as_u32())));
            assert!(node.data_dir().join("pg-0000").is_dir());
            assert!(node.data_dir().join("pg-0003").is_dir());
        }
        assert_ne!(
            map.node(NodeId::new(0)).unwrap().data_dir(),
            map.node(NodeId::new(1)).unwrap().data_dir()
        );
    }

    #[test]
    fn storage_cluster_opens_local_node_map() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let cluster = crate::StorageCluster::open_local_nodes(
            tmp.path(),
            &node_ids,
            &[0, 1],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap();

        assert_eq!(cluster.cluster_epoch(), ClusterEpoch::INITIAL);
        assert_eq!(cluster.metadata_node_id(), NodeId::new(0));
        assert_eq!(cluster.local_node_count(), 6);
        assert_eq!(cluster.local_node_ids().collect::<Vec<_>>(), node_ids);
    }

    #[test]
    fn places_payload_shards_deterministically_on_distinct_nodes() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
            NodeId::new(6),
            NodeId::new(7),
        ];
        let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
        let cluster =
            crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape)
                .unwrap();
        let data_pg_id = DataPgId::new(crate::PgId::new(3));

        let first = cluster
            .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
            .unwrap();
        let second = cluster
            .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(first.len(), usize::from(ec_shape.k + ec_shape.m));
        for (expected_index, location) in first.iter().enumerate() {
            assert_eq!(location.cluster_epoch(), ClusterEpoch::INITIAL);
            assert_eq!(location.data_pg_id(), data_pg_id);
            assert_eq!(
                location.shard_index(),
                ShardIndex::new(expected_index as u8)
            );
            assert!(
                cluster
                    .local_node_ids()
                    .any(|node_id| node_id == location.node_id()),
                "placed shard on unknown node {:?}",
                location.node_id()
            );
        }
        let distinct_nodes: BTreeSet<NodeId> = first.iter().map(ShardLocation::node_id).collect();
        assert_eq!(distinct_nodes.len(), first.len());
    }

    #[test]
    fn payload_shard_node_selects_one_placed_shard() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let data_pg_id = DataPgId::new(crate::PgId::new(1));
        let locations = map
            .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
            .unwrap();

        let selected = map
            .payload_shard_node(
                data_pg_id,
                ShardIndex::new(2),
                ec_shape,
                b"stable-payload-key",
            )
            .unwrap();

        assert_eq!(selected, locations[2].node_id());
    }

    #[test]
    fn payload_shard_node_rejects_index_outside_ec_shape() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let err = map
            .payload_shard_node(
                DataPgId::new(crate::PgId::new(0)),
                ShardIndex::new(ec_shape.k + ec_shape.m),
                ec_shape,
                b"stable-payload-key",
            )
            .unwrap_err();

        assert!(matches!(
            err,
            ClusterBuildError::InvalidShardIndex {
                data_shards: 4,
                parity_shards: 2,
                shard_index: 6,
            }
        ));
    }

    #[test]
    fn rejects_too_few_local_nodes_for_default_ec_shape_before_preparing_dirs() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
        ];
        let err = LocalClusterMap::open(
            tmp.path(),
            &node_ids,
            &[0, 1],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ClusterBuildError::UnplaceableEcShape {
                data_shards: 4,
                parity_shards: 2,
                required_nodes: 6,
                node_count: 5,
            }
        ));
        for node_id in node_ids {
            assert!(
                !tmp.path()
                    .join(format!("node-{:04}", node_id.as_u32()))
                    .exists(),
                "placement validation must run before preparing local node directories"
            );
        }
    }

    #[test]
    fn rejects_invalid_ec_shape_before_preparing_dirs() {
        let tmp = test_util::tempdir();
        let node_dir = tmp.path().join("node-0000");
        let err = LocalClusterMap::open_with_configs(
            NodeId::new(0),
            [LocalNodeStoreConfig::new(NodeId::new(0), &node_dir)],
            &[0],
            EcShape { k: 0, m: 2 },
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ClusterBuildError::InvalidEcShape {
                data_shards: 0,
                parity_shards: 2,
                ..
            }
        ));
        assert!(
            !node_dir.exists(),
            "EC shape validation must run before preparing local node directories"
        );
    }

    #[test]
    fn rejects_duplicate_local_node_ids() {
        let tmp = test_util::tempdir();
        let err = LocalClusterMap::open_with_configs(
            NodeId::new(0),
            [
                LocalNodeStoreConfig::new(NodeId::new(0), tmp.path().join("a")),
                LocalNodeStoreConfig::new(NodeId::new(0), tmp.path().join("b")),
            ],
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap_err();

        assert!(matches!(err, ClusterBuildError::DuplicateNodeId { id: 0 }));
    }

    #[test]
    fn rejects_duplicate_local_node_data_dirs() {
        let tmp = test_util::tempdir();
        let shared = tmp.path().join("shared");
        let err = LocalClusterMap::open_with_configs(
            NodeId::new(0),
            [
                LocalNodeStoreConfig::new(NodeId::new(0), &shared),
                LocalNodeStoreConfig::new(NodeId::new(1), &shared),
            ],
            &[0],
            EcShape { k: 1, m: 1 },
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ClusterBuildError::DuplicateDataDir {
                first_node_id: 0,
                duplicate_node_id: 1,
                ..
            }
        ));
        assert!(
            !shared.join("pg-0000").exists(),
            "duplicate directory validation must run before opening PG stores"
        );
    }

    #[test]
    fn rejects_missing_metadata_primary() {
        let tmp = test_util::tempdir();
        let node_dir = tmp.path().join("a");
        let err = LocalClusterMap::open_with_configs(
            NodeId::new(0),
            [LocalNodeStoreConfig::new(NodeId::new(1), &node_dir)],
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ClusterBuildError::MetadataPrimaryNotFound { id: 0 }
        ));
        assert!(
            !node_dir.exists(),
            "metadata primary validation must run before preparing node directories"
        );
    }
}
