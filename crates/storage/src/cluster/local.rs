use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use placement::NodeId;

use crate::error::{ClusterBuildError, StoreError};
use crate::{ClusterEpoch, EcShape, SharedStorageNode};

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
    process_local_registry_key: usize,
}

impl LocalClusterMap {
    pub fn single_node(node_id: NodeId, storage_node: Arc<SharedStorageNode>) -> Self {
        let process_local_registry_key = Arc::as_ptr(&storage_node) as usize;
        let mut nodes = BTreeMap::new();
        nodes.insert(
            node_id,
            LocalNodeStore::new(
                node_id,
                storage_node.data_dir().to_path_buf(),
                Arc::clone(&storage_node),
            ),
        );
        Self {
            epoch: ClusterEpoch::INITIAL,
            metadata_primary_node_id: node_id,
            nodes,
            process_local_registry_key,
        }
    }

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

    #[test]
    fn opens_distinct_local_node_stores_with_static_epoch() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let map = LocalClusterMap::open(
            tmp.path(),
            &node_ids,
            &[0, 1, 2, 3],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap();

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
        let node_ids = [NodeId::new(0), NodeId::new(1)];
        let cluster = crate::StorageCluster::open_local_nodes(
            tmp.path(),
            &node_ids,
            &[0, 1],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap();

        assert_eq!(cluster.cluster_epoch(), ClusterEpoch::INITIAL);
        assert_eq!(cluster.metadata_node_id(), NodeId::new(0));
        assert_eq!(cluster.local_node_count(), 2);
        assert_eq!(cluster.local_node_ids().collect::<Vec<_>>(), node_ids);
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
            SharedStorageNode::DEFAULT_EC_SHAPE,
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
