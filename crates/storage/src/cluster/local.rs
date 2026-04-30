use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use placement::{NodeId, PlacementConstraint, PlacementError, TopologyKey};

use super::ShardLocation;
use crate::error::{ClusterBuildError, ShardIoError, StoreError};
use crate::{
    ClusterEpoch, DataPgId, EcShape, PgId, PgState, ShardIndex, ShardKey, SharedStorageNode,
    WriteAck,
};

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

struct LocalShardNodeClient<'a> {
    node: &'a LocalNodeStore,
    cluster_epoch: ClusterEpoch,
    data_pg_id: DataPgId,
}

impl LocalShardNodeClient<'_> {
    fn write_shard(&self, key: &ShardKey, data: &[u8]) -> Result<WriteAck, ShardIoError> {
        self.node
            .storage_node()
            .write_shard_file(self.data_pg_id.get(), key, data)
            .map_err(|source| self.store_error(source))
    }

    fn read_shard(&self, key: &ShardKey, expected: WriteAck) -> Result<Vec<u8>, ShardIoError> {
        let data = self
            .node
            .storage_node()
            .read_shard_file(self.data_pg_id.get(), key)
            .map_err(|source| self.store_error(source))?;
        self.verify_read_ack(expected, &data)?;
        Ok(data)
    }

    fn read_shard_into(
        &self,
        key: &ShardKey,
        expected: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), ShardIoError> {
        if dst.len() as u64 != expected.stored_size {
            return Err(self.store_error(StoreError::Io {
                context: "read payload shard buffer size mismatch",
                source: std::io::Error::from(std::io::ErrorKind::InvalidData),
            }));
        }
        self.node
            .storage_node()
            .read_shard_file_into(self.data_pg_id.get(), key, dst)
            .map_err(|source| self.store_error(source))?;
        self.verify_read_ack(expected, dst)
    }

    fn delete_shard(&self, key: &ShardKey) -> Result<(), ShardIoError> {
        self.node
            .storage_node()
            .delete_shard_file(self.data_pg_id.get(), key)
            .map_err(|source| self.store_error(source))
    }

    fn store_error(&self, source: StoreError) -> ShardIoError {
        ShardIoError::Store {
            node_id: self.node.node_id().as_u32(),
            pg_id: self.data_pg_id.get(),
            cluster_epoch: self.cluster_epoch,
            source,
        }
    }

    fn verify_read_ack(&self, expected: WriteAck, data: &[u8]) -> Result<(), ShardIoError> {
        if data.len() as u64 != expected.stored_size {
            return Err(self.store_error(StoreError::Io {
                context: "read payload shard size mismatch",
                source: std::io::Error::from(std::io::ErrorKind::InvalidData),
            }));
        }
        let actual = checksum::crc64::checksum(data);
        if actual != expected.crc64 {
            return Err(self.store_error(StoreError::IntegrityError {
                expected: expected.crc64,
                actual,
            }));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPgRoute {
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    primary_node_id: NodeId,
    acting_set: Arc<[NodeId]>,
    state: PgState,
}

impl LocalPgRoute {
    fn active(
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        primary_node_id: NodeId,
        acting_set: Arc<[NodeId]>,
    ) -> Self {
        Self {
            cluster_epoch,
            pg_id,
            primary_node_id,
            acting_set,
            state: PgState::Active,
        }
    }

    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    pub fn primary_node_id(&self) -> NodeId {
        self.primary_node_id
    }

    pub fn acting_set(&self) -> &[NodeId] {
        &self.acting_set
    }

    pub fn state(&self) -> PgState {
        self.state
    }

    fn is_active(&self) -> bool {
        self.state == PgState::Active
    }

    fn contains_node(&self, node_id: NodeId) -> bool {
        self.acting_set.contains(&node_id)
    }
}

#[derive(Debug)]
pub struct LocalClusterMap {
    epoch: ClusterEpoch,
    metadata_primary_node_id: NodeId,
    nodes: BTreeMap<NodeId, LocalNodeStore>,
    pg_routes: BTreeMap<PgId, LocalPgRoute>,
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
        let pg_ids = validate_local_pg_ids(pg_ids)?;
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

        let acting_set = Arc::<[NodeId]>::from(node_ids.iter().copied().collect::<Vec<_>>());
        let pg_routes = build_static_pg_routes(
            ClusterEpoch::INITIAL,
            metadata_primary_node_id,
            Arc::clone(&acting_set),
            &pg_ids,
        );
        let storage_pg_ids: Vec<u32> = pg_ids.iter().map(|pg_id| pg_id.get()).collect();

        let mut nodes = BTreeMap::new();
        for (node_id, canonical_data_dir) in validated_configs {
            let storage_node = SharedStorageNode::open_with_default_ec_shape(
                &canonical_data_dir,
                &storage_pg_ids,
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
            pg_routes,
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

    pub fn pg_route(&self, pg_id: PgId) -> Option<&LocalPgRoute> {
        self.pg_routes.get(&pg_id)
    }

    pub fn pg_routes(&self) -> impl Iterator<Item = &LocalPgRoute> + '_ {
        self.pg_routes.values()
    }

    pub fn process_local_registry_key(&self) -> usize {
        self.process_local_registry_key
    }

    pub fn place_payload_shards(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
    ) -> Result<Vec<ShardLocation>, ClusterBuildError> {
        self.require_current_epoch_for_placement(operation_epoch, data_pg_id.pg_id())?;
        self.require_active_pg_for_placement(data_pg_id.pg_id())?;
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
                    operation_epoch,
                    data_pg_id,
                    ShardIndex::new(shard_index as u8),
                    node_id,
                )
            })
            .collect())
    }

    pub fn payload_shard_node(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        shard_index: ShardIndex,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
    ) -> Result<NodeId, ClusterBuildError> {
        let locations =
            self.place_payload_shards(operation_epoch, data_pg_id, ec_shape, stable_placement_key)?;
        locations
            .get(usize::from(shard_index.get()))
            .map(ShardLocation::node_id)
            .ok_or(ClusterBuildError::InvalidShardIndex {
                data_shards: ec_shape.k,
                parity_shards: ec_shape.m,
                shard_index: shard_index.get(),
            })
    }

    pub fn write_payload_shard(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .write_shard(key, data)
    }

    pub fn read_payload_shard(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<Vec<u8>, ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .read_shard(key, expected)
    }

    pub fn read_payload_shard_into(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .read_shard_into(key, expected, dst)
    }

    pub fn delete_payload_shard(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .delete_shard(key)
    }

    fn shard_node_client(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<LocalShardNodeClient<'_>, ShardIoError> {
        if operation_epoch != self.epoch {
            return Err(ShardIoError::StaleOperationEpoch {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                operation_epoch,
                current_epoch: self.epoch,
            });
        }
        if location.cluster_epoch() != self.epoch {
            return Err(ShardIoError::StaleLocation {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                location_epoch: location.cluster_epoch(),
                current_epoch: self.epoch,
            });
        }
        if location.shard_index() != key.shard_index() {
            return Err(ShardIoError::ShardIndexMismatch {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: self.epoch,
                location_shard_index: location.shard_index().get(),
                key_shard_index: key.shard_index().get(),
            });
        }
        let route =
            self.require_active_pg_for_shard_io(location.data_pg_id().pg_id(), location.node_id())?;
        if !route.contains_node(location.node_id()) {
            return Err(ShardIoError::NodeNotInActingSet {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: self.epoch,
            });
        }

        let node = self
            .nodes
            .get(&location.node_id())
            .ok_or(ShardIoError::NodeNotFound {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: self.epoch,
            })?;
        Ok(LocalShardNodeClient {
            node,
            cluster_epoch: self.epoch,
            data_pg_id: location.data_pg_id(),
        })
    }

    fn require_current_epoch_for_placement(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<(), ClusterBuildError> {
        if operation_epoch != self.epoch {
            return Err(ClusterBuildError::StalePayloadPlacement {
                pg_id: pg_id.get(),
                operation_epoch,
                current_epoch: self.epoch,
            });
        }
        Ok(())
    }

    fn require_active_pg_for_placement(&self, pg_id: PgId) -> Result<(), ClusterBuildError> {
        let route = self
            .pg_routes
            .get(&pg_id)
            .ok_or(ClusterBuildError::PgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
            })?;
        if !route.is_active() {
            return Err(ClusterBuildError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
                state: route.state(),
            });
        }
        Ok(())
    }

    fn require_active_pg_for_shard_io(
        &self,
        pg_id: PgId,
        node_id: NodeId,
    ) -> Result<&LocalPgRoute, ShardIoError> {
        let route = self.pg_routes.get(&pg_id).ok_or(ShardIoError::PgNotFound {
            node_id: node_id.as_u32(),
            pg_id: pg_id.get(),
            cluster_epoch: self.epoch,
        })?;
        if !route.is_active() {
            return Err(ShardIoError::PgNotActive {
                node_id: node_id.as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
                state: route.state(),
            });
        }
        Ok(route)
    }
}

fn build_static_pg_routes(
    cluster_epoch: ClusterEpoch,
    primary_node_id: NodeId,
    acting_set: Arc<[NodeId]>,
    pg_ids: &[PgId],
) -> BTreeMap<PgId, LocalPgRoute> {
    pg_ids
        .iter()
        .copied()
        .map(|pg_id| {
            (
                pg_id,
                LocalPgRoute::active(
                    cluster_epoch,
                    pg_id,
                    primary_node_id,
                    Arc::clone(&acting_set),
                ),
            )
        })
        .collect()
}

fn validate_local_pg_ids(pg_ids: &[u32]) -> Result<Vec<PgId>, ClusterBuildError> {
    if pg_ids.is_empty() {
        return Err(ClusterBuildError::EmptyPgSet);
    }
    let mut seen = BTreeSet::new();
    let mut validated = Vec::with_capacity(pg_ids.len());
    for &raw_pg_id in pg_ids {
        let pg_id = PgId::new(raw_pg_id);
        if !seen.insert(pg_id) {
            return Err(ClusterBuildError::DuplicatePgId { pg_id: raw_pg_id });
        }
        validated.push(pg_id);
    }
    Ok(validated)
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
    use proptest::prelude::*;
    use proptest::test_runner::{TestCaseError, TestCaseResult};
    use std::collections::BTreeSet;
    use std::sync::Arc;

    struct CommittedDirectSegment {
        generation_id: crate::GenerationId,
        segment_okh: [u8; 16],
        payload: Vec<u8>,
        written: crate::DirectPutWrittenSegment,
        locations: Vec<ShardLocation>,
    }

    fn write_committed_direct_segment(
        cluster: &crate::StorageCluster,
        payload: &[u8],
    ) -> CommittedDirectSegment {
        let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
        let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
        let reservation_id = crate::SessionId::try_from("01".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let segment_okh = [41; 16];
        let written = cluster
            .write_direct_put_segment_payload_shards(
                &bucket,
                &key,
                generation_id,
                0,
                &segment_okh,
                payload,
            )
            .unwrap();
        let commit_req = crate::CommitDirectPutObjectReq {
            bucket,
            key,
            generation_reservation_id: reservation_id,
            versioning: crate::BucketVersioningState::Disabled,
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            generation_id,
            size: payload.len() as u64,
            etag_crc64: checksum::crc64::checksum(payload),
            ec: written.ec,
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            object_lock: crate::ObjectLockState::default(),
            encryption: crate::ObjectEncryption::None,
            segment_index: 0,
            segment_crc64: Some(checksum::crc64::checksum(payload)),
            segment_okh,
            segment_vid: generation_id,
            data_pg_id: written.data_pg_id,
        };
        cluster
            .commit_direct_put_object_from_payload_shards(
                &commit_req,
                &written.written_shards,
                |_| Ok::<(), ()>(()),
            )
            .unwrap()
            .unwrap();
        let data_pg_id = DataPgId::new(PgId::new(written.data_pg_id));
        let placement_key =
            super::super::segment_payload_placement_key(&segment_okh, generation_id);
        let locations = cluster
            .place_payload_shards(data_pg_id, written.ec, &placement_key)
            .unwrap();

        CommittedDirectSegment {
            generation_id,
            segment_okh,
            payload: payload.to_vec(),
            written,
            locations,
        }
    }

    #[derive(Debug, Clone)]
    enum LocalClusterTraceOp {
        AdvanceEpoch,
        SetPgState(PgState),
        WriteCurrent(u8),
        WriteStale(u8),
        ReadCurrent,
        ReadStale,
        DeleteCurrent,
        DeleteStale,
        MetadataBridgeStale(u8),
        ZeroSizeStalePayloadRead,
        QueueCurrent(u8),
        QueueStale(u8),
        LeaseReleaseAcrossEpoch(u8),
        RecoverAfterPhysicalShardLoss(u8),
    }

    #[derive(Debug, Clone)]
    struct TraceWrittenShard {
        placement_key: Vec<u8>,
        location: ShardLocation,
        key: ShardKey,
        ack: WriteAck,
        data: Vec<u8>,
    }

    fn local_cluster_trace_strategy() -> impl Strategy<Value = Vec<LocalClusterTraceOp>> {
        prop::collection::vec(
            prop_oneof![
                2 => Just(LocalClusterTraceOp::AdvanceEpoch),
                3 => pg_state_strategy().prop_map(LocalClusterTraceOp::SetPgState),
                5 => any::<u8>().prop_map(LocalClusterTraceOp::WriteCurrent),
                3 => any::<u8>().prop_map(LocalClusterTraceOp::WriteStale),
                4 => Just(LocalClusterTraceOp::ReadCurrent),
                3 => Just(LocalClusterTraceOp::ReadStale),
                3 => Just(LocalClusterTraceOp::DeleteCurrent),
                2 => Just(LocalClusterTraceOp::DeleteStale),
                3 => any::<u8>().prop_map(LocalClusterTraceOp::MetadataBridgeStale),
                3 => Just(LocalClusterTraceOp::ZeroSizeStalePayloadRead),
                2 => any::<u8>().prop_map(LocalClusterTraceOp::QueueCurrent),
                2 => any::<u8>().prop_map(LocalClusterTraceOp::QueueStale),
                2 => any::<u8>().prop_map(LocalClusterTraceOp::LeaseReleaseAcrossEpoch),
                1 => any::<u8>().prop_map(LocalClusterTraceOp::RecoverAfterPhysicalShardLoss),
            ],
            1..=40,
        )
    }

    fn pg_state_strategy() -> impl Strategy<Value = PgState> {
        prop_oneof![
            Just(PgState::Active),
            Just(PgState::Peering),
            Just(PgState::Degraded),
            Just(PgState::Backfilling),
        ]
    }

    fn trace_node_ids() -> [NodeId; 6] {
        [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ]
    }

    fn current_cluster(map: &Arc<LocalClusterMap>) -> Arc<crate::StorageCluster> {
        crate::StorageCluster::from_local_map(Arc::clone(map)).unwrap()
    }

    fn stale_cluster(
        map: &Arc<LocalClusterMap>,
        current_epoch: ClusterEpoch,
    ) -> Arc<crate::StorageCluster> {
        crate::StorageCluster::test_from_local_map_with_epoch(
            Arc::clone(map),
            stale_epoch_for(current_epoch),
        )
        .unwrap()
    }

    fn stale_epoch_for(current_epoch: ClusterEpoch) -> ClusterEpoch {
        if current_epoch == ClusterEpoch::INITIAL {
            ClusterEpoch::new(2).unwrap()
        } else {
            ClusterEpoch::INITIAL
        }
    }

    fn set_trace_epoch(map: &mut Arc<LocalClusterMap>, epoch: ClusterEpoch) {
        let map = Arc::get_mut(map).expect("trace must not retain StorageCluster handles");
        map.epoch = epoch;
        for route in map.pg_routes.values_mut() {
            route.cluster_epoch = epoch;
        }
    }

    fn set_trace_pg_state(map: &mut Arc<LocalClusterMap>, state: PgState) {
        let map = Arc::get_mut(map).expect("trace must not retain StorageCluster handles");
        map.pg_routes.get_mut(&PgId::new(0)).unwrap().state = state;
    }

    fn trace_bucket(seed: u8) -> crate::BucketName {
        crate::BucketName::try_from(format!("trace-bucket-{seed}")).unwrap()
    }

    fn trace_key(seed: u8) -> crate::ObjectKey {
        crate::ObjectKey::try_from(format!("trace-key-{seed}")).unwrap()
    }

    fn trace_session(seed: u8) -> crate::SessionId {
        crate::SessionId::try_from(format!("{:032x}", u128::from(seed) + 1)).unwrap()
    }

    fn trace_generation(seed: u8) -> crate::GenerationId {
        crate::GenerationId::new(u64::from(seed) + 1).unwrap()
    }

    fn trace_shard_key(step: usize, seed: u8) -> ShardKey {
        ShardKey::new(&[seed.wrapping_add(1); 16], 10_000 + step as u64, 0)
    }

    fn trace_placement_key(step: usize, seed: u8) -> Vec<u8> {
        format!("trace-placement-{step}-{seed}").into_bytes()
    }

    fn current_trace_location(
        cluster: &crate::StorageCluster,
        written: &TraceWrittenShard,
        ec_shape: EcShape,
    ) -> ShardLocation {
        cluster
            .place_payload_shards(
                DataPgId::new(PgId::new(0)),
                ec_shape,
                &written.placement_key,
            )
            .unwrap()[usize::from(written.key.shard_index().get())]
    }

    fn written_trace_location_for_epoch(
        written: &TraceWrittenShard,
        current_epoch: ClusterEpoch,
    ) -> ShardLocation {
        ShardLocation::new(
            current_epoch,
            written.location.data_pg_id(),
            written.location.shard_index(),
            written.location.node_id(),
        )
    }

    fn arbitrary_current_location(current_epoch: ClusterEpoch) -> ShardLocation {
        ShardLocation::new(
            current_epoch,
            DataPgId::new(PgId::new(0)),
            ShardIndex::new(0),
            NodeId::new(0),
        )
    }

    fn shard_file_present_on_any_trace_node(map: &LocalClusterMap, key: &ShardKey) -> bool {
        trace_node_ids().into_iter().any(|node_id| {
            map.node(node_id)
                .unwrap()
                .storage_node()
                .read_shard_file(0, key)
                .is_ok()
        })
    }

    fn drain_trace_reclaim_work(cluster: &crate::StorageCluster) {
        while cluster.try_take_reclaim_work().is_some() {}
    }

    fn assert_stale_metadata_bridge_error(
        err: crate::ObjectPgActionError,
        current_epoch: ClusterEpoch,
    ) -> TestCaseResult {
        let expected = matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataPrimaryBridge {
                metadata_node_id: 0,
                operation_epoch,
                current_epoch: err_current_epoch,
            }) if operation_epoch == stale_epoch_for(current_epoch)
                && err_current_epoch == current_epoch
        );
        prop_assert!(expected, "unexpected metadata bridge error: {err:?}");
        Ok(())
    }

    fn run_local_cluster_trace(ops: &[LocalClusterTraceOp]) -> TestCaseResult {
        let tmp = test_util::tempdir();
        let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
        let node_ids = trace_node_ids();
        let mut map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let mut current_epoch = ClusterEpoch::INITIAL;
        let mut pg_state = PgState::Active;
        let mut written = None::<TraceWrittenShard>;

        for (step, op) in ops.iter().enumerate() {
            match op {
                LocalClusterTraceOp::AdvanceEpoch => {
                    current_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
                    set_trace_epoch(&mut map, current_epoch);
                }
                LocalClusterTraceOp::SetPgState(state) => {
                    pg_state = *state;
                    set_trace_pg_state(&mut map, pg_state);
                }
                LocalClusterTraceOp::WriteCurrent(seed) => {
                    let cluster = current_cluster(&map);
                    let placement_key = trace_placement_key(step, *seed);
                    let key = trace_shard_key(step, *seed);
                    let data = format!("trace-payload-{step}-{seed}").into_bytes();
                    match cluster.place_payload_shards(
                        DataPgId::new(PgId::new(0)),
                        ec_shape,
                        &placement_key,
                    ) {
                        Ok(locations) => {
                            prop_assert_eq!(pg_state, PgState::Active);
                            let location = locations[usize::from(key.shard_index().get())];
                            let ack = cluster.write_payload_shard(location, &key, &data).unwrap();
                            let read = cluster.read_payload_shard(location, &key, ack).unwrap();
                            prop_assert_eq!(read.as_slice(), data.as_slice());
                            written = Some(TraceWrittenShard {
                                placement_key,
                                location,
                                key,
                                ack,
                                data,
                            });
                        }
                        Err(ClusterBuildError::PgNotActive { state, .. }) => {
                            prop_assert_eq!(state, pg_state);
                            prop_assert!(!shard_file_present_on_any_trace_node(&map, &key));
                        }
                        Err(err) => return Err(TestCaseError::fail(format!("{err:?}"))),
                    }
                }
                LocalClusterTraceOp::WriteStale(seed) => {
                    let cluster = stale_cluster(&map, current_epoch);
                    let key = trace_shard_key(step, *seed);
                    let err = cluster
                        .write_payload_shard(
                            arbitrary_current_location(current_epoch),
                            &key,
                            b"stale write",
                        )
                        .unwrap_err();
                    let expected = matches!(
                        err,
                        ShardIoError::StaleOperationEpoch {
                            operation_epoch,
                            current_epoch: err_current_epoch,
                            ..
                        } if operation_epoch == stale_epoch_for(current_epoch)
                            && err_current_epoch == current_epoch
                    );
                    prop_assert!(expected, "unexpected stale write error: {err:?}");
                    prop_assert!(!shard_file_present_on_any_trace_node(&map, &key));
                }
                LocalClusterTraceOp::ReadCurrent => {
                    let Some(written) = written.as_ref() else {
                        continue;
                    };
                    let cluster = current_cluster(&map);
                    if pg_state == PgState::Active {
                        let location = current_trace_location(&cluster, written, ec_shape);
                        let read = cluster
                            .read_payload_shard(location, &written.key, written.ack)
                            .unwrap();
                        prop_assert_eq!(read.as_slice(), written.data.as_slice());
                    } else {
                        let location = written_trace_location_for_epoch(written, current_epoch);
                        let err = cluster
                            .read_payload_shard(location, &written.key, written.ack)
                            .unwrap_err();
                        let expected = matches!(
                            err,
                            ShardIoError::PgNotActive { state, .. } if state == pg_state
                        );
                        prop_assert!(expected, "unexpected inactive read error: {err:?}");
                        prop_assert!(shard_file_present_on_any_trace_node(&map, &written.key));
                    }
                }
                LocalClusterTraceOp::ReadStale => {
                    let key = written
                        .as_ref()
                        .map(|written| written.key.clone())
                        .unwrap_or_else(|| trace_shard_key(step, 0));
                    let ack = written
                        .as_ref()
                        .map(|written| written.ack)
                        .unwrap_or(WriteAck {
                            crc64: 0,
                            stored_size: 1,
                        });
                    let cluster = stale_cluster(&map, current_epoch);
                    let err = cluster
                        .read_payload_shard(arbitrary_current_location(current_epoch), &key, ack)
                        .unwrap_err();
                    let expected = matches!(
                        err,
                        ShardIoError::StaleOperationEpoch {
                            operation_epoch,
                            current_epoch: err_current_epoch,
                            ..
                        } if operation_epoch == stale_epoch_for(current_epoch)
                            && err_current_epoch == current_epoch
                    );
                    prop_assert!(expected, "unexpected stale read error: {err:?}");
                }
                LocalClusterTraceOp::DeleteCurrent => {
                    let Some(previous) = written.clone() else {
                        continue;
                    };
                    let cluster = current_cluster(&map);
                    if pg_state == PgState::Active {
                        let location = current_trace_location(&cluster, &previous, ec_shape);
                        cluster
                            .delete_payload_shard(location, &previous.key)
                            .unwrap();
                        prop_assert!(!shard_file_present_on_any_trace_node(&map, &previous.key));
                        written = None;
                    } else {
                        let location = written_trace_location_for_epoch(&previous, current_epoch);
                        let err = cluster
                            .delete_payload_shard(location, &previous.key)
                            .unwrap_err();
                        let expected = matches!(
                            err,
                            ShardIoError::PgNotActive { state, .. } if state == pg_state
                        );
                        prop_assert!(expected, "unexpected inactive delete error: {err:?}");
                        prop_assert!(shard_file_present_on_any_trace_node(&map, &previous.key));
                    }
                }
                LocalClusterTraceOp::DeleteStale => {
                    let Some(previous) = written.as_ref() else {
                        continue;
                    };
                    let cluster = stale_cluster(&map, current_epoch);
                    let err = cluster
                        .delete_payload_shard(
                            arbitrary_current_location(current_epoch),
                            &previous.key,
                        )
                        .unwrap_err();
                    let expected = matches!(
                        err,
                        ShardIoError::StaleOperationEpoch {
                            operation_epoch,
                            current_epoch: err_current_epoch,
                            ..
                        } if operation_epoch == stale_epoch_for(current_epoch)
                            && err_current_epoch == current_epoch
                    );
                    prop_assert!(expected, "unexpected stale delete error: {err:?}");
                    prop_assert!(shard_file_present_on_any_trace_node(&map, &previous.key));
                }
                LocalClusterTraceOp::MetadataBridgeStale(seed) => {
                    let cluster = stale_cluster(&map, current_epoch);
                    let bucket = trace_bucket(*seed);
                    let key = trace_key(*seed);
                    let reservation_id = trace_session(*seed);
                    let err = cluster
                        .reserve_put_object_generation(&bucket, &key, &reservation_id)
                        .unwrap_err();
                    assert_stale_metadata_bridge_error(err, current_epoch)?;
                    let cluster = current_cluster(&map);
                    let err = cluster
                        .test_object_generation_reservation_for(&bucket, &key, &reservation_id)
                        .unwrap_err();
                    let expected = matches!(
                        err,
                        crate::ObjectPgActionError::Metadata(
                            crate::MetadataError::ObjectGenerationReservationNotFound { .. }
                        )
                    );
                    prop_assert!(expected, "unexpected reservation lookup error: {err:?}");
                }
                LocalClusterTraceOp::ZeroSizeStalePayloadRead => {
                    let cluster = stale_cluster(&map, current_epoch);
                    let mut dst = vec![0xAA];
                    let err = cluster
                        .read_segment_payload_stored_bytes_into(
                            crate::SegmentStoredBytesRequest {
                                data_pg_id: 0,
                                segment_okh: [step as u8; 16],
                                segment_vid: crate::GenerationId::MIN,
                                stored_size: 0,
                                segment_crc64: Some(0),
                                ec: ec_shape,
                            },
                            &mut dst,
                        )
                        .unwrap_err();
                    let expected = matches!(
                        err,
                        StoreError::StalePayloadOperation {
                            pg_id: 0,
                            operation_epoch,
                            current_epoch: err_current_epoch,
                        } if operation_epoch == stale_epoch_for(current_epoch)
                            && err_current_epoch == current_epoch
                    );
                    prop_assert!(expected, "unexpected stale zero-size read error: {err:?}");
                    prop_assert_eq!(dst, vec![0xAA]);
                }
                LocalClusterTraceOp::QueueCurrent(seed) => {
                    let cluster = current_cluster(&map);
                    drain_trace_reclaim_work(&cluster);
                    let bucket = trace_bucket(*seed);
                    let key = trace_key(*seed);
                    let generation_id = trace_generation(*seed);
                    cluster.enqueue_object_payload_reclaim(&bucket, &key, generation_id);
                    let work = cluster.try_take_reclaim_work();
                    let expected = matches!(
                        work,
                        Some(crate::ReclaimWorkItem::ObjectPayload((
                            queued_bucket,
                            queued_key,
                            queued_generation_id
                        ))) if queued_bucket == bucket
                            && queued_key == key
                            && queued_generation_id == generation_id
                    );
                    prop_assert!(expected, "unexpected reclaim work item");
                    drain_trace_reclaim_work(&cluster);
                }
                LocalClusterTraceOp::QueueStale(seed) => {
                    let cluster = current_cluster(&map);
                    drain_trace_reclaim_work(&cluster);
                    drop(cluster);
                    let cluster = stale_cluster(&map, current_epoch);
                    cluster.enqueue_object_payload_reclaim(
                        &trace_bucket(*seed),
                        &trace_key(*seed),
                        trace_generation(*seed),
                    );
                    drop(cluster);
                    let cluster = current_cluster(&map);
                    prop_assert!(cluster.try_take_reclaim_work().is_none());
                }
                LocalClusterTraceOp::LeaseReleaseAcrossEpoch(seed) => {
                    let bucket = trace_bucket(*seed);
                    let key = trace_key(*seed);
                    let generation_id = trace_generation(*seed);
                    let cluster = current_cluster(&map);
                    let lease = cluster
                        .acquire_object_payload_lease(&bucket, &key, generation_id)
                        .unwrap();
                    prop_assert_eq!(
                        cluster.object_payload_lease_count(&bucket, &key, generation_id),
                        1
                    );
                    drop(cluster);

                    current_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
                    set_trace_epoch(&mut map, current_epoch);
                    let cluster = current_cluster(&map);
                    prop_assert_eq!(
                        cluster.object_payload_lease_count(&bucket, &key, generation_id),
                        1
                    );
                    let released = lease.release();
                    prop_assert_eq!(released.remaining(), 0);
                    prop_assert_eq!(
                        cluster.object_payload_lease_count(&bucket, &key, generation_id),
                        0
                    );
                }
                LocalClusterTraceOp::RecoverAfterPhysicalShardLoss(seed) => {
                    if pg_state != PgState::Active {
                        continue;
                    }
                    let cluster = current_cluster(&map);
                    let payload = format!("recoverable-physical-shard-loss-{step}-{seed}");
                    let segment = write_committed_direct_segment(&cluster, payload.as_bytes());
                    let shard_path = cluster
                        .test_payload_shard_file_path(
                            segment.written.data_pg_id,
                            segment.written.ec,
                            &segment.segment_okh,
                            segment.generation_id,
                            0,
                        )
                        .unwrap();
                    std::fs::remove_file(shard_path).unwrap();

                    let mut recovered = Vec::new();
                    cluster
                        .read_segment_payload_stored_bytes_into(
                            crate::SegmentStoredBytesRequest {
                                data_pg_id: segment.written.data_pg_id,
                                segment_okh: segment.segment_okh,
                                segment_vid: segment.generation_id,
                                stored_size: segment.payload.len(),
                                segment_crc64: Some(checksum::crc64::checksum(&segment.payload)),
                                ec: segment.written.ec,
                            },
                            &mut recovered,
                        )
                        .unwrap();
                    prop_assert_eq!(recovered, segment.payload);
                }
            }
        }

        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 32,
            .. ProptestConfig::default()
        })]

        #[test]
        fn prop_local_cluster_trace_preserves_epoch_route_and_cleanup_invariants(
            ops in local_cluster_trace_strategy()
        ) {
            run_local_cluster_trace(&ops)?;
        }
    }

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
        assert_eq!(map.pg_routes().count(), 4);
        let route = map.pg_route(PgId::new(2)).unwrap();
        assert_eq!(route.cluster_epoch(), ClusterEpoch::INITIAL);
        assert_eq!(route.pg_id(), PgId::new(2));
        assert_eq!(route.primary_node_id(), NodeId::new(0));
        assert_eq!(route.state(), PgState::Active);
        assert_eq!(route.acting_set(), node_ids);

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
        assert_eq!(cluster.operation_epoch(), ClusterEpoch::INITIAL);
        assert_eq!(cluster.metadata_node_id(), NodeId::new(0));
        assert_eq!(cluster.local_node_count(), 6);
        assert_eq!(cluster.local_node_ids().collect::<Vec<_>>(), node_ids);
        let routes = cluster.local_pg_routes().collect::<Vec<_>>();
        assert_eq!(routes.len(), 2);
        assert_eq!(
            cluster.local_pg_route(PgId::new(1)).unwrap().acting_set(),
            node_ids
        );
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
            .place_payload_shards(
                ClusterEpoch::INITIAL,
                data_pg_id,
                ec_shape,
                b"stable-payload-key",
            )
            .unwrap();

        let selected = map
            .payload_shard_node(
                ClusterEpoch::INITIAL,
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
                ClusterEpoch::INITIAL,
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
    fn place_payload_shards_rejects_unknown_pg() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let map = LocalClusterMap::open(
            tmp.path(),
            &node_ids,
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap();

        let err = map
            .place_payload_shards(
                ClusterEpoch::INITIAL,
                DataPgId::new(PgId::new(99)),
                SharedStorageNode::DEFAULT_EC_SHAPE,
                b"stable-payload-key",
            )
            .unwrap_err();

        assert!(matches!(
            err,
            ClusterBuildError::PgNotFound {
                pg_id: 99,
                cluster_epoch: ClusterEpoch::INITIAL,
            }
        ));
    }

    #[test]
    fn place_payload_shards_rejects_stale_operation_epoch() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let map = LocalClusterMap::open(
            tmp.path(),
            &node_ids,
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap();

        let err = map
            .place_payload_shards(
                ClusterEpoch::new(2).unwrap(),
                DataPgId::new(PgId::new(0)),
                SharedStorageNode::DEFAULT_EC_SHAPE,
                b"stable-payload-key",
            )
            .unwrap_err();

        assert!(matches!(
            err,
            ClusterBuildError::StalePayloadPlacement {
                pg_id: 0,
                operation_epoch,
                current_epoch,
            } if operation_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ));
    }

    #[test]
    fn storage_cluster_dispatches_payload_shard_io_to_placed_local_node() {
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
        let map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let data_pg_id = DataPgId::new(crate::PgId::new(1));
        let location = cluster
            .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
            .unwrap()[3];
        let key = ShardKey::new(&[17; 16], 23, location.shard_index().get());

        let ack = cluster
            .write_payload_shard(location, &key, b"placed shard")
            .unwrap();

        assert_eq!(ack.stored_size, b"placed shard".len() as u64);
        assert_eq!(ack.crc64, checksum::crc64::checksum(b"placed shard"));
        assert_eq!(
            cluster.read_payload_shard(location, &key, ack).unwrap(),
            b"placed shard"
        );
        let mut dst = vec![0; b"placed shard".len()];
        cluster
            .read_payload_shard_into(location, &key, ack, &mut dst)
            .unwrap();
        assert_eq!(dst, b"placed shard");
        assert!(matches!(
            cluster.read_payload_shard(
                location,
                &key,
                WriteAck {
                    crc64: ack.crc64 ^ 1,
                    stored_size: ack.stored_size,
                },
            ),
            Err(ShardIoError::Store {
                source: StoreError::IntegrityError { .. },
                ..
            })
        ));

        let assigned_node = map.node(location.node_id()).unwrap();
        assert_eq!(
            assigned_node
                .storage_node()
                .read_shard_file(data_pg_id.get(), &key)
                .unwrap(),
            b"placed shard"
        );
        for other_node_id in node_ids {
            if other_node_id == location.node_id() {
                continue;
            }
            let other_node = map.node(other_node_id).unwrap();
            assert!(matches!(
                other_node
                    .storage_node()
                    .read_shard_file(data_pg_id.get(), &key),
                Err(StoreError::NotFound)
            ));
        }

        cluster.delete_payload_shard(location, &key).unwrap();
        assert!(matches!(
            cluster.read_payload_shard(location, &key, ack),
            Err(ShardIoError::Store {
                source: StoreError::NotFound,
                ..
            })
        ));
    }

    #[test]
    fn storage_cluster_payload_write_uses_handle_operation_epoch() {
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
        let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
            Arc::clone(&map),
            ClusterEpoch::new(2).unwrap(),
        )
        .unwrap();
        let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
        let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
        let segment_okh = [43; 16];
        let generation_id = crate::GenerationId::MIN;

        let err = stale_cluster
            .write_direct_put_segment_payload_shards(
                &bucket,
                &key,
                generation_id,
                0,
                &segment_okh,
                b"stale epoch payload",
            )
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::StalePayloadOperation {
                pg_id: 0,
                operation_epoch,
                current_epoch,
            } if operation_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ));
        for shard_index in 0..(ec_shape.k + ec_shape.m) {
            assert!(
                !current_cluster
                    .test_payload_shard_file_exists(
                        0,
                        ec_shape,
                        &segment_okh,
                        generation_id,
                        shard_index,
                    )
                    .unwrap(),
                "stale operation epoch wrote shard {shard_index}"
            );
        }
    }

    #[test]
    fn stale_storage_cluster_payload_read_reports_data_pg() {
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
        let mut map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        Arc::get_mut(&mut map).unwrap().epoch = ClusterEpoch::new(2).unwrap();
        let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
            Arc::clone(&map),
            ClusterEpoch::INITIAL,
        )
        .unwrap();
        let mut dst = Vec::new();

        let err = stale_cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: 0,
                    segment_okh: [53; 16],
                    segment_vid: crate::GenerationId::MIN,
                    stored_size: 0,
                    segment_crc64: Some(0),
                    ec: ec_shape,
                },
                &mut dst,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::StalePayloadOperation {
                pg_id: 0,
                operation_epoch: ClusterEpoch::INITIAL,
                current_epoch,
            } if current_epoch == ClusterEpoch::new(2).unwrap()
        ));
        assert!(dst.is_empty());
    }

    #[test]
    fn stale_storage_cluster_handle_cannot_use_current_epoch_location() {
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
        let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
            Arc::clone(&map),
            ClusterEpoch::new(2).unwrap(),
        )
        .unwrap();
        let data_pg_id = DataPgId::new(PgId::new(0));
        let location = current_cluster
            .place_payload_shards(data_pg_id, ec_shape, b"current-epoch-location")
            .unwrap()[0];
        let key = ShardKey::new(&[47; 16], 1, location.shard_index().get());

        let err = stale_cluster
            .place_payload_shards(data_pg_id, ec_shape, b"stale-placement")
            .unwrap_err();

        assert!(matches!(
            err,
            ClusterBuildError::StalePayloadPlacement {
                pg_id: 0,
                operation_epoch,
                current_epoch,
            } if operation_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ));
        let err = stale_cluster
            .write_payload_shard(location, &key, b"must not write")
            .unwrap_err();
        assert!(matches!(
            err,
            ShardIoError::StaleOperationEpoch {
                node_id,
                pg_id: 0,
                operation_epoch,
                current_epoch,
            } if node_id == location.node_id().as_u32()
                && operation_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ));
        let node = map.node(location.node_id()).unwrap();
        assert!(matches!(
            node.storage_node().read_shard_file(data_pg_id.get(), &key),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn stale_storage_cluster_handle_rejects_bucket_metadata_before_mutation() {
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
        let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
            Arc::clone(&map),
            ClusterEpoch::new(2).unwrap(),
        )
        .unwrap();
        let bucket = crate::BucketName::try_from("stale-bucket".to_string()).unwrap();
        let owner_canonical_id = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let create = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner_canonical_id,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
        };

        let err = stale_cluster
            .create_bucket_with_config_and_load_info(&create)
            .unwrap_err();

        assert!(matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::StaleMetadataPrimaryBridge {
                metadata_node_id: 0,
                operation_epoch,
                current_epoch,
            }) if operation_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ));
        let err = current_cluster.head_bucket_info(&bucket).unwrap_err();
        assert!(matches!(
            err,
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::BucketNotFound { .. })
        ));
    }

    #[test]
    fn stale_storage_cluster_handle_rejects_object_metadata_before_mutation() {
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
        let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
            Arc::clone(&map),
            ClusterEpoch::new(2).unwrap(),
        )
        .unwrap();
        let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
        let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
        let reservation_id = crate::SessionId::try_from("02".repeat(16)).unwrap();

        let err = stale_cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap_err();

        assert!(matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataPrimaryBridge {
                metadata_node_id: 0,
                operation_epoch,
                current_epoch,
            }) if operation_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ));
        let err = current_cluster
            .test_object_generation_reservation_for(&bucket, &key, &reservation_id)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::ObjectPgActionError::Metadata(
                crate::MetadataError::ObjectGenerationReservationNotFound { .. }
            )
        ));
    }

    #[test]
    fn stale_storage_cluster_handle_rejects_multipart_metadata_before_lookup() {
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
        let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
            Arc::clone(&map),
            ClusterEpoch::new(2).unwrap(),
        )
        .unwrap();
        let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
        let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
        let upload_id = crate::UploadId::try_from(".".repeat(128)).unwrap();

        let err = stale_cluster
            .load_multipart_upload(&bucket, &key, &upload_id)
            .unwrap_err();

        assert!(matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::StaleMetadataPrimaryBridge {
                metadata_node_id: 0,
                operation_epoch,
                current_epoch,
            }) if operation_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ));
        let err = current_cluster
            .load_multipart_upload(&bucket, &key, &upload_id)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::NoSuchUpload { .. })
        ));
    }

    #[test]
    fn object_payload_lease_token_releases_after_cluster_epoch_transition() {
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
        let mut map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
        let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
        let generation_id = crate::GenerationId::MIN;

        let lease = current_cluster
            .acquire_object_payload_lease(&bucket, &key, generation_id)
            .unwrap();
        assert_eq!(
            current_cluster.object_payload_lease_count(&bucket, &key, generation_id),
            1
        );
        drop(current_cluster);

        Arc::get_mut(&mut map).unwrap().epoch = ClusterEpoch::new(2).unwrap();
        let current_epoch_cluster =
            crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
            Arc::clone(&map),
            ClusterEpoch::INITIAL,
        )
        .unwrap();

        let err = match stale_cluster.acquire_object_payload_lease(&bucket, &key, generation_id) {
            Ok(_) => panic!("stale cluster handle acquired a payload lease"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            StoreError::StaleMetadataPrimaryBridge {
                metadata_node_id: 0,
                operation_epoch,
                current_epoch,
            } if operation_epoch == ClusterEpoch::INITIAL
                && current_epoch == ClusterEpoch::new(2).unwrap()
        ));
        stale_cluster.enqueue_object_payload_reclaim(&bucket, &key, generation_id);
        assert!(current_epoch_cluster.try_take_reclaim_work().is_none());

        let released = lease.release();
        assert_eq!(released.remaining(), 0);
        assert_eq!(
            current_epoch_cluster.object_payload_lease_count(&bucket, &key, generation_id),
            0
        );

        released.enqueue_object_payload_reclaim();
        assert!(matches!(
            current_epoch_cluster.try_take_reclaim_work(),
            Some(crate::ReclaimWorkItem::ObjectPayload((
                queued_bucket,
                queued_key,
                queued_generation_id
            ))) if queued_bucket == bucket
                && queued_key == key
                && queued_generation_id == generation_id
        ));
    }

    #[test]
    fn placed_payload_shard_io_rejects_key_location_shard_index_mismatch() {
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
        let map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let data_pg_id = DataPgId::new(crate::PgId::new(1));
        let locations = cluster
            .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
            .unwrap();
        let shard_0_location = locations[0];
        let shard_3_location = locations[3];
        let shard_0_key = ShardKey::new(&[31; 16], 42, shard_0_location.shard_index().get());

        let err = cluster
            .write_payload_shard(shard_3_location, &shard_0_key, b"wrong shard")
            .unwrap_err();

        assert!(matches!(
            err,
            ShardIoError::ShardIndexMismatch {
                location_shard_index: 3,
                key_shard_index: 0,
                ..
            }
        ));
        assert!(matches!(
            map.node(shard_3_location.node_id())
                .unwrap()
                .storage_node()
                .read_shard_file(data_pg_id.get(), &shard_0_key),
            Err(StoreError::NotFound)
        ));
        assert!(matches!(
            map.node(shard_0_location.node_id())
                .unwrap()
                .storage_node()
                .read_shard_file(data_pg_id.get(), &shard_0_key),
            Err(StoreError::NotFound)
        ));

        let ack = cluster
            .write_payload_shard(shard_0_location, &shard_0_key, b"right shard")
            .unwrap();
        assert!(matches!(
            cluster.read_payload_shard(shard_3_location, &shard_0_key, ack),
            Err(ShardIoError::ShardIndexMismatch {
                location_shard_index: 3,
                key_shard_index: 0,
                ..
            })
        ));
        assert!(matches!(
            cluster.delete_payload_shard(shard_3_location, &shard_0_key),
            Err(ShardIoError::ShardIndexMismatch {
                location_shard_index: 3,
                key_shard_index: 0,
                ..
            })
        ));
        assert_eq!(
            cluster
                .read_payload_shard(shard_0_location, &shard_0_key, ack)
                .unwrap(),
            b"right shard"
        );
    }

    #[test]
    fn payload_shard_io_rejects_stale_location_epoch() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let map = LocalClusterMap::open(
            tmp.path(),
            &node_ids,
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap();
        let location = ShardLocation::new(
            ClusterEpoch::new(2).unwrap(),
            DataPgId::new(crate::PgId::new(0)),
            ShardIndex::new(0),
            NodeId::new(0),
        );
        let key = ShardKey::new(&[23; 16], 1, 0);

        let err = map
            .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"stale")
            .unwrap_err();

        assert!(matches!(
            err,
            ShardIoError::StaleLocation {
                location_epoch,
                current_epoch,
                ..
            } if location_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ));
    }

    #[test]
    fn payload_shard_io_rejects_stale_operation_epoch_before_touching_node_store() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let map = LocalClusterMap::open(
            tmp.path(),
            &node_ids,
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap();
        let location = ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(0)),
            ShardIndex::new(0),
            NodeId::new(0),
        );
        let key = ShardKey::new(&[41; 16], 1, 0);

        let err = map
            .write_payload_shard(ClusterEpoch::new(2).unwrap(), location, &key, b"stale op")
            .unwrap_err();

        assert!(matches!(
            err,
            ShardIoError::StaleOperationEpoch {
                node_id: 0,
                pg_id: 0,
                operation_epoch,
                current_epoch,
            } if operation_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ));
        let node = map.node(NodeId::new(0)).unwrap();
        assert!(matches!(
            node.storage_node().read_shard_file(0, &key),
            Err(StoreError::NotFound)
        ));
    }

    #[test]
    fn payload_shard_io_rejects_node_outside_acting_set() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let map = LocalClusterMap::open(
            tmp.path(),
            &node_ids,
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap();
        let location = ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(crate::PgId::new(0)),
            ShardIndex::new(0),
            NodeId::new(99),
        );
        let key = ShardKey::new(&[29; 16], 1, 0);

        let err = map
            .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"unknown")
            .unwrap_err();

        assert!(matches!(
            err,
            ShardIoError::NodeNotInActingSet {
                node_id: 99,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
            }
        ));
    }

    #[test]
    fn placed_segment_recovery_propagates_non_active_pg_route() {
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
        let mut map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let segment = write_committed_direct_segment(&cluster, b"phase-five-route-read");
        let data_pg_id = DataPgId::new(PgId::new(segment.written.data_pg_id));
        drop(cluster);

        Arc::get_mut(&mut map)
            .unwrap()
            .pg_routes
            .get_mut(&data_pg_id.pg_id())
            .unwrap()
            .state = PgState::Peering;
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let shard_size = segment
            .payload
            .len()
            .div_ceil(usize::from(segment.written.ec.k));
        let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
        let mut present_count = 0;

        let err = cluster
            .try_load_placed_segment_shard(
                segment.written.data_pg_id,
                &segment.segment_okh,
                segment.generation_id,
                &segment.locations,
                0,
                shard_size,
                &mut all_shards,
                &mut present_count,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::ShardPgNotActive {
                node_id,
                pg_id,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: PgState::Peering,
            } if node_id == segment.locations[0].node_id().as_u32()
                && pg_id == data_pg_id.get()
        ));
        assert_eq!(present_count, 0);
        assert!(all_shards.iter().all(Option::is_none));
    }

    #[test]
    fn placed_payload_delete_propagates_missing_pg_route() {
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
        let cluster =
            crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();

        let err = cluster
            .delete_payload_shard_set(99, ec_shape, &[17; 16], crate::GenerationId::MIN)
            .unwrap_err();

        assert!(matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::ClusterPgNotFound {
                pg_id: 99,
                cluster_epoch: ClusterEpoch::INITIAL,
            })
        ));
    }

    #[test]
    fn placed_segment_recovery_propagates_missing_shard_pg_route() {
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
        let cluster =
            crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
        let segment = write_committed_direct_segment(&cluster, b"phase-five-missing-pg-route");
        let shard_size = segment
            .payload
            .len()
            .div_ceil(usize::from(segment.written.ec.k));
        let mut locations = segment.locations.clone();
        locations[0] = ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(99)),
            ShardIndex::new(0),
            locations[0].node_id(),
        );
        let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
        let mut present_count = 0;

        let err = cluster
            .try_load_placed_segment_shard(
                segment.written.data_pg_id,
                &segment.segment_okh,
                segment.generation_id,
                &locations,
                0,
                shard_size,
                &mut all_shards,
                &mut present_count,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::ShardPgNotFound {
                node_id,
                pg_id: 99,
                cluster_epoch: ClusterEpoch::INITIAL,
            } if node_id == locations[0].node_id().as_u32()
        ));
        assert_eq!(present_count, 0);
        assert!(all_shards.iter().all(Option::is_none));
    }

    #[test]
    fn placed_segment_recovery_propagates_node_not_in_acting_set() {
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
        let cluster =
            crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
        let segment = write_committed_direct_segment(&cluster, b"phase-five-acting-set-route");
        let shard_size = segment
            .payload
            .len()
            .div_ceil(usize::from(segment.written.ec.k));
        let mut locations = segment.locations.clone();
        locations[0] = ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(segment.written.data_pg_id)),
            ShardIndex::new(0),
            NodeId::new(99),
        );
        let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
        let mut present_count = 0;

        let err = cluster
            .try_load_placed_segment_shard(
                segment.written.data_pg_id,
                &segment.segment_okh,
                segment.generation_id,
                &locations,
                0,
                shard_size,
                &mut all_shards,
                &mut present_count,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::NodeNotInActingSet {
                node_id: 99,
                pg_id,
                cluster_epoch: ClusterEpoch::INITIAL,
            } if pg_id == segment.written.data_pg_id
        ));
        assert_eq!(present_count, 0);
        assert!(all_shards.iter().all(Option::is_none));
    }

    #[test]
    fn placed_segment_recovery_propagates_stale_shard_location() {
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
        let cluster =
            crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
        let segment = write_committed_direct_segment(&cluster, b"phase-five-stale-location");
        let shard_size = segment
            .payload
            .len()
            .div_ceil(usize::from(segment.written.ec.k));
        let mut locations = segment.locations.clone();
        locations[0] = ShardLocation::new(
            ClusterEpoch::new(2).unwrap(),
            DataPgId::new(PgId::new(segment.written.data_pg_id)),
            ShardIndex::new(0),
            locations[0].node_id(),
        );
        let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
        let mut present_count = 0;

        let err = cluster
            .try_load_placed_segment_shard(
                segment.written.data_pg_id,
                &segment.segment_okh,
                segment.generation_id,
                &locations,
                0,
                shard_size,
                &mut all_shards,
                &mut present_count,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::StaleShardLocation {
                node_id,
                pg_id,
                location_epoch,
                current_epoch: ClusterEpoch::INITIAL,
            } if node_id == locations[0].node_id().as_u32()
                && pg_id == segment.written.data_pg_id
                && location_epoch == ClusterEpoch::new(2).unwrap()
        ));
        assert_eq!(present_count, 0);
        assert!(all_shards.iter().all(Option::is_none));
    }

    #[test]
    fn placed_segment_recovery_propagates_shard_index_mismatch() {
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
        let cluster =
            crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
        let segment = write_committed_direct_segment(&cluster, b"phase-five-shard-index-route");
        let shard_size = segment
            .payload
            .len()
            .div_ceil(usize::from(segment.written.ec.k));
        let mut locations = segment.locations.clone();
        locations[0] = ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(segment.written.data_pg_id)),
            ShardIndex::new(1),
            locations[0].node_id(),
        );
        let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
        let mut present_count = 0;

        let err = cluster
            .try_load_placed_segment_shard(
                segment.written.data_pg_id,
                &segment.segment_okh,
                segment.generation_id,
                &locations,
                0,
                shard_size,
                &mut all_shards,
                &mut present_count,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::ShardIndexMismatch {
                node_id,
                pg_id,
                cluster_epoch: ClusterEpoch::INITIAL,
                location_shard_index: 1,
                key_shard_index: 0,
            } if node_id == locations[0].node_id().as_u32()
                && pg_id == segment.written.data_pg_id
        ));
        assert_eq!(present_count, 0);
        assert!(all_shards.iter().all(Option::is_none));
    }

    #[test]
    fn placed_segment_recovery_wraps_node_store_error_with_shard_route() {
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
        let mut map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let segment = write_committed_direct_segment(&cluster, b"phase-five-shard-store-route");
        drop(cluster);

        Arc::get_mut(&mut map).unwrap().pg_routes.insert(
            PgId::new(99),
            LocalPgRoute::active(
                ClusterEpoch::INITIAL,
                PgId::new(99),
                NodeId::new(0),
                Arc::<[NodeId]>::from(node_ids.to_vec()),
            ),
        );
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let shard_size = segment
            .payload
            .len()
            .div_ceil(usize::from(segment.written.ec.k));
        let mut locations = segment.locations.clone();
        locations[0] = ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(99)),
            ShardIndex::new(0),
            locations[0].node_id(),
        );
        let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
        let mut present_count = 0;

        let err = cluster
            .try_load_placed_segment_shard(
                segment.written.data_pg_id,
                &segment.segment_okh,
                segment.generation_id,
                &locations,
                0,
                shard_size,
                &mut all_shards,
                &mut present_count,
            )
            .unwrap_err();

        match err {
            StoreError::ShardStore {
                node_id,
                pg_id: 99,
                cluster_epoch: ClusterEpoch::INITIAL,
                source,
            } => {
                assert_eq!(node_id, locations[0].node_id().as_u32());
                assert!(matches!(*source, StoreError::PgNotFound { pg_id: 99 }));
            }
            other => panic!("expected shard store error with route context, got {other:?}"),
        }
        assert_eq!(present_count, 0);
        assert!(all_shards.iter().all(Option::is_none));
    }

    #[test]
    fn placed_segment_recovery_treats_length_corrupt_shard_as_recoverable() {
        for extra_length in [false, true] {
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
                &[0],
                SharedStorageNode::DEFAULT_EC_SHAPE,
            )
            .unwrap();
            let segment =
                write_committed_direct_segment(&cluster, b"phase-five-length-corrupt-payload");
            let shard_size = segment
                .payload
                .len()
                .div_ceil(usize::from(segment.written.ec.k));
            let corrupt_len = if extra_length { shard_size + 1 } else { 1 };
            let shard_path = cluster
                .test_payload_shard_file_path(
                    segment.written.data_pg_id,
                    segment.written.ec,
                    &segment.segment_okh,
                    segment.generation_id,
                    0,
                )
                .unwrap();
            std::fs::write(&shard_path, vec![0xAB; corrupt_len]).unwrap();

            let mut recovered = Vec::new();
            cluster
                .read_segment_payload_stored_bytes_into(
                    crate::SegmentStoredBytesRequest {
                        data_pg_id: segment.written.data_pg_id,
                        segment_okh: segment.segment_okh,
                        segment_vid: segment.generation_id,
                        stored_size: segment.payload.len(),
                        segment_crc64: Some(checksum::crc64::checksum(&segment.payload)),
                        ec: segment.written.ec,
                    },
                    &mut recovered,
                )
                .unwrap();

            assert_eq!(
                recovered, segment.payload,
                "failed to recover when corrupt shard extra_length={extra_length}"
            );
        }
    }

    #[test]
    fn payload_shard_io_rejects_unknown_pg_before_touching_node_store() {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let map = LocalClusterMap::open(
            tmp.path(),
            &node_ids,
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap();
        let location = ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(99)),
            ShardIndex::new(0),
            NodeId::new(0),
        );
        let key = ShardKey::new(&[37; 16], 1, 0);

        let err = map
            .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"unknown pg")
            .unwrap_err();

        assert!(matches!(
            err,
            ShardIoError::PgNotFound {
                node_id: 0,
                pg_id: 99,
                cluster_epoch: ClusterEpoch::INITIAL,
            }
        ));
        assert!(!tmp.path().join("node-0000").join("pg-0099").exists());
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
    fn rejects_empty_pg_set_before_preparing_dirs() {
        let tmp = test_util::tempdir();
        let node_dir = tmp.path().join("node-0000");
        let err = LocalClusterMap::open_with_configs(
            NodeId::new(0),
            [LocalNodeStoreConfig::new(NodeId::new(0), &node_dir)],
            &[],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap_err();

        assert!(matches!(err, ClusterBuildError::EmptyPgSet));
        assert!(
            !node_dir.exists(),
            "PG validation must run before preparing local node directories"
        );
    }

    #[test]
    fn rejects_duplicate_pg_ids_before_preparing_dirs() {
        let tmp = test_util::tempdir();
        let node_dir = tmp.path().join("node-0000");
        let err = LocalClusterMap::open_with_configs(
            NodeId::new(0),
            [LocalNodeStoreConfig::new(NodeId::new(0), &node_dir)],
            &[0, 1, 1],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap_err();

        assert!(matches!(err, ClusterBuildError::DuplicatePgId { pg_id: 1 }));
        assert!(
            !node_dir.exists(),
            "PG validation must run before preparing local node directories"
        );
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
