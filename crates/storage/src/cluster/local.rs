use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use placement::{NodeId, PlacementConstraint, PlacementError, TopologyKey};

use super::ShardLocation;
use crate::error::{ClusterBuildError, ShardIoError, StoreError};
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandLogIndex};
use crate::{
    BucketName, ClusterEpoch, DataPgId, EcShape, GenerationId, ObjectKey, PgId, PgState,
    ReclaimWorkItem, ShardIndex, ShardKey, SharedStorageNode, WriteAck,
};

const PAYLOAD_SHARD_PLACEMENT_KEY_DOMAIN: &[u8] = b"argmin/payload-shard-placement/v1";
const LOCAL_RECLAIM_WORKER_WAIT_POLL_MILLIS: u64 = 100;

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
pub(crate) struct LocalClusterRuntimeState {
    object_payload_leases: Mutex<HashMap<LocalReclaimRoot, usize>>,
    reclaim_queue: (Mutex<LocalReclaimQueueState>, Condvar),
    metadata_command_indexes: Mutex<HashMap<PgId, u64>>,
    pending_metadata_commands: Mutex<HashMap<(PgId, BucketName), MetadataCommandEnvelope>>,
}

type LocalReclaimRoot = (BucketName, ObjectKey, GenerationId);

#[derive(Debug)]
struct LocalReclaimQueueState {
    object_queue: VecDeque<LocalReclaimRoot>,
    queued_objects: HashSet<LocalReclaimRoot>,
    bucket_delete_queue: VecDeque<BucketName>,
    queued_bucket_deletes: HashSet<BucketName>,
}

impl LocalClusterRuntimeState {
    fn new() -> Self {
        Self {
            object_payload_leases: Mutex::new(HashMap::new()),
            reclaim_queue: (
                Mutex::new(LocalReclaimQueueState {
                    object_queue: VecDeque::new(),
                    queued_objects: HashSet::new(),
                    bucket_delete_queue: VecDeque::new(),
                    queued_bucket_deletes: HashSet::new(),
                }),
                Condvar::new(),
            ),
            metadata_command_indexes: Mutex::new(HashMap::new()),
            pending_metadata_commands: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn next_metadata_command_log_index(&self, pg_id: PgId) -> MetadataCommandLogIndex {
        let mut indexes = self
            .metadata_command_indexes
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let index = indexes.entry(pg_id).or_insert(0);
        *index = index
            .checked_add(1)
            .expect("metadata command log index overflow");
        MetadataCommandLogIndex::new(*index).expect("metadata command log index starts at one")
    }

    pub(crate) fn pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) -> Option<MetadataCommandEnvelope> {
        self.pending_metadata_commands
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(pg_id, bucket.clone()))
            .cloned()
    }

    pub(crate) fn set_pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: MetadataCommandEnvelope,
    ) {
        let previous = self
            .pending_metadata_commands
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((pg_id, bucket.clone()), command);
        debug_assert!(
            previous.is_none(),
            "bucket metadata command stream already has a pending command"
        );
    }

    pub(crate) fn remove_pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) {
        self.pending_metadata_commands
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(pg_id, bucket.clone()));
    }

    pub(crate) fn clear_pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
    ) {
        self.pending_metadata_commands
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(pending_pg_id, pending_bucket), _| {
                *pending_pg_id != pg_id || pending_bucket != bucket
            });
    }

    pub(crate) fn acquire_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        let mut leases = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *leases
            .entry((bucket.clone(), key.clone(), generation_id))
            .or_insert(0) += 1;
    }

    pub(crate) fn release_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        let mut leases = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let root = (bucket.clone(), key.clone(), generation_id);
        let entry = leases
            .get_mut(&root)
            .expect("object payload lease release without acquire");
        *entry -= 1;
        let remaining = *entry;
        if remaining == 0 {
            leases.remove(&root);
        }
        remaining
    }

    pub(crate) fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        let leases = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        leases
            .get(&(bucket.clone(), key.clone(), generation_id))
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        let leases = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        leases
            .iter()
            .filter(|((lease_bucket, _, _), _)| lease_bucket == bucket)
            .map(|(_, count)| *count)
            .sum()
    }

    pub(crate) fn enqueue_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        let root = (bucket.clone(), key.clone(), generation_id);
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        if state.queued_objects.insert(root.clone()) {
            state.object_queue.push_back(root);
            cv.notify_one();
        }
    }

    pub(crate) fn enqueue_bucket_delete_finalize(&self, bucket: &BucketName) {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        let bucket = bucket.clone();
        if state.queued_bucket_deletes.insert(bucket.clone()) {
            state.bucket_delete_queue.push_back(bucket);
            cv.notify_one();
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn try_take_reclaim_work(&self) -> Option<ReclaimWorkItem> {
        let mut state = self
            .reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(root) = state.object_queue.pop_front() {
            state.queued_objects.remove(&root);
            return Some(ReclaimWorkItem::ObjectPayload(root));
        }
        let bucket = state.bucket_delete_queue.pop_front()?;
        state.queued_bucket_deletes.remove(&bucket);
        Some(ReclaimWorkItem::BucketDelete(bucket))
    }

    pub(crate) fn wait_for_reclaim_work(&self, stop: &AtomicBool) -> Option<ReclaimWorkItem> {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        while state.object_queue.is_empty()
            && state.bucket_delete_queue.is_empty()
            && !stop.load(Ordering::SeqCst)
        {
            let (next_state, _) = cv
                .wait_timeout(
                    state,
                    std::time::Duration::from_millis(LOCAL_RECLAIM_WORKER_WAIT_POLL_MILLIS),
                )
                .unwrap_or_else(|e| e.into_inner());
            state = next_state;
        }
        if stop.load(Ordering::SeqCst) {
            return None;
        }
        if let Some(root) = state.object_queue.pop_front() {
            state.queued_objects.remove(&root);
            return Some(ReclaimWorkItem::ObjectPayload(root));
        }
        let bucket = state.bucket_delete_queue.pop_front()?;
        state.queued_bucket_deletes.remove(&bucket);
        Some(ReclaimWorkItem::BucketDelete(bucket))
    }

    pub(crate) fn wake_reclaim_workers(&self) {
        self.reclaim_queue.1.notify_all();
    }
}

#[derive(Debug)]
pub struct LocalClusterMap {
    epoch: ClusterEpoch,
    metadata_primary_node_id: NodeId,
    nodes: BTreeMap<NodeId, LocalNodeStore>,
    pg_routes: BTreeMap<PgId, LocalPgRoute>,
    placement_map: placement::ClusterMap,
    runtime_state: Arc<LocalClusterRuntimeState>,
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
            runtime_state: Arc::new(LocalClusterRuntimeState::new()),
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

    pub(crate) fn runtime_state(&self) -> Arc<LocalClusterRuntimeState> {
        Arc::clone(&self.runtime_state)
    }

    pub(crate) fn metadata_pg_primary_node(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<&LocalNodeStore, StoreError> {
        if operation_epoch != self.epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch,
                current_epoch: self.epoch,
            });
        }

        let route = self
            .pg_routes
            .get(&pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
            })?;
        if route.cluster_epoch() != self.epoch {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.epoch,
            });
        }
        if !route.is_active() {
            return Err(StoreError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
                state: route.state(),
            });
        }
        let node_id = route.primary_node_id();
        if !route.contains_node(node_id) {
            return Err(StoreError::NodeNotInActingSet {
                node_id: node_id.as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
            });
        }

        self.nodes.get(&node_id).ok_or(StoreError::NodeNotFound {
            node_id: node_id.as_u32(),
            pg_id: pg_id.get(),
            cluster_epoch: self.epoch,
        })
    }

    pub(crate) fn metadata_pg_acting_nodes(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<Vec<&LocalNodeStore>, StoreError> {
        if operation_epoch != self.epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch,
                current_epoch: self.epoch,
            });
        }

        let route = self
            .pg_routes
            .get(&pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
            })?;
        if route.cluster_epoch() != self.epoch {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.epoch,
            });
        }
        if !route.is_active() {
            return Err(StoreError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
                state: route.state(),
            });
        }
        if !route.contains_node(route.primary_node_id()) {
            return Err(StoreError::NodeNotInActingSet {
                node_id: route.primary_node_id().as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
            });
        }

        route
            .acting_set()
            .iter()
            .map(|node_id| {
                self.nodes.get(node_id).ok_or(StoreError::NodeNotFound {
                    node_id: node_id.as_u32(),
                    pg_id: pg_id.get(),
                    cluster_epoch: self.epoch,
                })
            })
            .collect()
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
    use crate::metadata_command::{
        BucketPropertyMutation, BucketSubresourceMutation, MetadataCommandPayload,
    };
    use proptest::prelude::*;
    use proptest::test_runner::{TestCaseError, TestCaseResult};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};

    static METADATA_COMMAND_APPLY_HOOK_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

    fn lock_metadata_command_apply_hook_test() -> std::sync::MutexGuard<'static, ()> {
        METADATA_COMMAND_APPLY_HOOK_TEST_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

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
        write_committed_direct_segment_for(cluster, &bucket, &key, payload)
    }

    fn write_committed_direct_segment_for(
        cluster: &crate::StorageCluster,
        bucket: &crate::BucketName,
        key: &crate::ObjectKey,
        payload: &[u8],
    ) -> CommittedDirectSegment {
        write_committed_direct_segment_for_with_okh(cluster, bucket, key, [41; 16], payload)
    }

    fn write_committed_direct_segment_for_with_okh(
        cluster: &crate::StorageCluster,
        bucket: &crate::BucketName,
        key: &crate::ObjectKey,
        segment_okh: [u8; 16],
        payload: &[u8],
    ) -> CommittedDirectSegment {
        let reservation_id = crate::SessionId::try_from("01".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(bucket, key, &reservation_id)
            .unwrap();
        let written = cluster
            .write_direct_put_segment_payload_shards(
                bucket,
                key,
                generation_id,
                0,
                &segment_okh,
                payload,
            )
            .unwrap();
        let commit_req = crate::CommitDirectPutObjectReq {
            bucket: bucket.clone(),
            key: key.clone(),
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

    fn bucket_key_with_distinct_object_and_data_pg(
        topology: &crate::PgTopology,
    ) -> (crate::BucketName, crate::ObjectKey, u32, u32) {
        let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
        for index in 0..1000 {
            let key = crate::ObjectKey::try_from(format!("key-{index}")).unwrap();
            let object_pg = topology.object_pg_for(&bucket, &key);
            let data_pg = topology
                .object_generation_segment_data_pg(&bucket, &key, crate::GenerationId::MIN, 0)
                .get();
            if object_pg != data_pg {
                return (bucket, key, object_pg, data_pg);
            }
        }
        panic!("test topology did not produce distinct object/data PGs");
    }

    fn key_for_object_pg(
        topology: &crate::PgTopology,
        bucket: &crate::BucketName,
        target_pg_id: u32,
        prefix: &str,
    ) -> crate::ObjectKey {
        for index in 0..10_000 {
            let key = crate::ObjectKey::try_from(format!("{prefix}{index}")).unwrap();
            if topology.object_pg_for(bucket, &key) == target_pg_id {
                return key;
            }
        }
        panic!("test topology did not produce a key for PG {target_pg_id}");
    }

    fn bucket_for_pg(
        topology: &crate::PgTopology,
        target_pg_id: u32,
        prefix: &str,
    ) -> crate::BucketName {
        for index in 0..10_000 {
            let bucket = crate::BucketName::try_from(format!("{prefix}{index}")).unwrap();
            if topology.bucket_pg_for(&bucket) == target_pg_id {
                return bucket;
            }
        }
        panic!("test topology did not produce a bucket for PG {target_pg_id}");
    }

    fn create_test_bucket(cluster: &crate::StorageCluster, bucket: &crate::BucketName) {
        let owner = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        cluster
            .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: crate::BucketVersioningState::Disabled,
                object_lock: crate::BucketObjectLockConfig::default(),
            })
            .unwrap();
    }

    fn seed_bucket_record(
        map: &LocalClusterMap,
        node_id: NodeId,
        pg_id: u32,
        bucket: &crate::BucketName,
        owner: &crate::CanonicalUserId,
    ) {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id).unwrap();
        crate::PgMetadataStore::create_bucket(
            &*pg,
            bucket,
            "owner",
            owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
    }

    fn upload_id_from_label(label: &str) -> crate::UploadId {
        let mut upload_id = String::from(label);
        upload_id.extend(std::iter::repeat_n(
            '.',
            crate::UPLOAD_ID_LEN - upload_id.len(),
        ));
        crate::UploadId::try_from(upload_id).unwrap()
    }

    fn seed_multipart_upload_record(
        map: &LocalClusterMap,
        node_id: NodeId,
        pg_id: u32,
        bucket: &crate::BucketName,
        key: &crate::ObjectKey,
        upload_id: &crate::UploadId,
        state: crate::UploadState,
    ) {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id).unwrap();
        crate::PgMetadataStore::create_multipart_upload(
            &*pg,
            &crate::CreateMultipartUploadReq {
                upload_id: upload_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                initiator: None,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                object_lock: crate::ObjectLockState::default(),
                checksum: None,
                encryption: crate::ObjectEncryption::None,
            },
        )
        .unwrap();
        if state != crate::UploadState::InProgress {
            crate::PgMetadataStore::set_upload_state(&*pg, upload_id, state).unwrap();
        }
    }

    fn seed_completed_multipart_upload_record(
        map: &LocalClusterMap,
        node_id: NodeId,
        pg_id: u32,
        bucket: &crate::BucketName,
        key: &crate::ObjectKey,
        upload_id: &crate::UploadId,
        completion_order: u64,
    ) {
        seed_multipart_upload_record(
            map,
            node_id,
            pg_id,
            bucket,
            key,
            upload_id,
            crate::UploadState::InProgress,
        );
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id).unwrap();
        let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, upload_id).unwrap();
        let version_id = crate::VersionId::Null;
        let part = crate::ObjectPartRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            part_number: 1,
            size: 1,
            etag: vec![completion_order as u8; 8],
            etag_kind: crate::EtagKind::Crc64,
            part_okh: [completion_order as u8; 16],
            part_vid: upload.object_generation_id,
            ec_k: 2,
            ec_m: 1,
            data_pg_id: pg_id,
            checksum: None,
        };
        crate::PgMetadataStore::complete_multipart_commit(
            &*pg,
            upload_id,
            completion_order,
            &crate::CommitMultipartReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                generation_id: upload.object_generation_id,
                size: part.size,
                etag_crc64: [completion_order as u8; 8],
                ec: EcShape { k: 2, m: 1 },
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            },
            &[part],
        )
        .unwrap();
    }

    fn set_route_primary(map: &mut LocalClusterMap, pg_id: u32, primary_node_id: NodeId) {
        let route = map.pg_routes.get_mut(&PgId::new(pg_id)).unwrap();
        route.acting_set = Arc::from([NodeId::new(0), NodeId::new(1), NodeId::new(2)]);
        route.primary_node_id = primary_node_id;
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
        MetadataOperationStale(u8),
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
                3 => any::<u8>().prop_map(LocalClusterTraceOp::MetadataOperationStale),
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

    fn assert_stale_metadata_operation_error(
        err: crate::ObjectPgActionError,
        current_epoch: ClusterEpoch,
    ) -> TestCaseResult {
        let expected = matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id: 0,
                operation_epoch,
                current_epoch: err_current_epoch,
            }) if operation_epoch == stale_epoch_for(current_epoch)
                && err_current_epoch == current_epoch
        );
        prop_assert!(expected, "unexpected metadata operation error: {err:?}");
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
                LocalClusterTraceOp::MetadataOperationStale(seed) => {
                    let cluster = stale_cluster(&map, current_epoch);
                    let bucket = trace_bucket(*seed);
                    let key = trace_key(*seed);
                    let reservation_id = trace_session(*seed);
                    let err = cluster
                        .reserve_put_object_generation(&bucket, &key, &reservation_id)
                        .unwrap_err();
                    assert_stale_metadata_operation_error(err, current_epoch)?;
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
                    if pg_state != PgState::Active {
                        let err = match cluster.acquire_object_payload_lease(
                            &bucket,
                            &key,
                            generation_id,
                        ) {
                            Ok(_) => {
                                return Err(TestCaseError::fail(
                                    "inactive PG acquired a payload lease",
                                ));
                            }
                            Err(err) => err,
                        };
                        let expected = matches!(
                            err,
                            StoreError::PgNotActive {
                                pg_id: 0,
                                cluster_epoch,
                                state,
                            } if cluster_epoch == current_epoch && state == pg_state
                        );
                        prop_assert!(expected, "unexpected inactive lease acquire error: {err:?}");
                        continue;
                    }
                    let lease = cluster
                        .acquire_object_payload_lease(&bucket, &key, generation_id)
                        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
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
    fn metadata_pg_primary_node_routes_by_pg_primary_and_fails_closed() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();

        {
            let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
            route.primary_node_id = NodeId::new(2);
        }
        assert_eq!(
            map.metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
                .unwrap()
                .node_id(),
            NodeId::new(2)
        );

        {
            let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
            route.cluster_epoch = ClusterEpoch::new(2).unwrap();
        }
        let err = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::StaleMetadataRoute {
                pg_id: 1,
                route_epoch,
                current_epoch,
            } if route_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ));
        {
            let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
            route.cluster_epoch = ClusterEpoch::INITIAL;
        }

        let err = map
            .metadata_pg_primary_node(ClusterEpoch::new(2).unwrap(), PgId::new(1))
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::StaleMetadataOperation {
                pg_id: 1,
                operation_epoch,
                current_epoch,
            } if operation_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ));

        {
            let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
            route.state = PgState::Peering;
        }
        let err = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::PgNotActive {
                pg_id: 1,
                cluster_epoch,
                state: PgState::Peering,
            } if cluster_epoch == ClusterEpoch::INITIAL
        ));

        {
            let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
            route.state = PgState::Active;
            route.acting_set = Arc::from([NodeId::new(0), NodeId::new(1)]);
            route.primary_node_id = NodeId::new(2);
        }
        let err = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::NodeNotInActingSet {
                node_id: 2,
                pg_id: 1,
                cluster_epoch,
            } if cluster_epoch == ClusterEpoch::INITIAL
        ));

        {
            let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
            route.acting_set = Arc::from([NodeId::new(99)]);
            route.primary_node_id = NodeId::new(99);
        }
        let err = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::NodeNotFound {
                node_id: 99,
                pg_id: 1,
                cluster_epoch,
            } if cluster_epoch == ClusterEpoch::INITIAL
        ));

        let err = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(9))
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::ClusterPgNotFound {
                pg_id: 9,
                cluster_epoch,
            } if cluster_epoch == ClusterEpoch::INITIAL
        ));
    }

    #[test]
    fn direct_put_registers_payload_acks_on_routed_data_pg_primary() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));
        set_route_primary(&mut map, data_pg, NodeId::new(2));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let reservation_id = crate::SessionId::try_from("02".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        assert_eq!(generation_id, crate::GenerationId::MIN);

        let payload = b"direct put payload with routed data acks";
        let segment_okh = [62; 16];
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
        assert_eq!(written.data_pg_id, data_pg);

        let commit_req = crate::CommitDirectPutObjectReq {
            bucket: bucket.clone(),
            key: key.clone(),
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

        let first_shard_key = &written.written_shards[0].key;
        assert!(map
            .nodes
            .get(&NodeId::new(2))
            .unwrap()
            .storage_node()
            .test_shard_exists(data_pg, first_shard_key)
            .unwrap());
        assert!(!map
            .nodes
            .get(&NodeId::new(1))
            .unwrap()
            .storage_node()
            .test_shard_exists(data_pg, first_shard_key)
            .unwrap());

        let mut readback = Vec::new();
        cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: data_pg,
                    segment_okh,
                    segment_vid: generation_id,
                    stored_size: payload.len(),
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    ec: written.ec,
                },
                &mut readback,
            )
            .unwrap();
        assert_eq!(readback, payload);
    }

    #[test]
    fn stream_append_registers_payload_acks_on_routed_data_pg_primary() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));
        set_route_primary(&mut map, data_pg, NodeId::new(2));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let session_id = crate::SessionId::try_from("03".repeat(16)).unwrap();
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();

        let payload = b"stream append payload with routed data acks";
        let segment_okh = crate::stream_segment_key_hash(&session_id, 0);
        let (_, segment_record) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: payload.len() as u64,
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    segment_okh,
                },
            )
            .unwrap();
        assert_eq!(segment_record.data_pg_id, data_pg);

        let written = cluster
            .write_stream_segment_payload_shards(&segment_record, payload)
            .unwrap();
        let shard_batch: Vec<(&ShardKey, crate::WriteAck)> = written
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                0,
                &segment_record,
                &shard_batch,
            )
            .unwrap();

        let first_shard_key = &written[0].key;
        assert!(map
            .nodes
            .get(&NodeId::new(2))
            .unwrap()
            .storage_node()
            .test_shard_exists(data_pg, first_shard_key)
            .unwrap());
        assert!(!map
            .nodes
            .get(&NodeId::new(1))
            .unwrap()
            .storage_node()
            .test_shard_exists(data_pg, first_shard_key)
            .unwrap());

        let mut readback = Vec::new();
        cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: data_pg,
                    segment_okh: segment_record.segment_okh,
                    segment_vid: segment_record.segment_vid,
                    stored_size: payload.len(),
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    ec: EcShape {
                        k: segment_record.ec_k,
                        m: segment_record.ec_m,
                    },
                },
                &mut readback,
            )
            .unwrap();
        assert_eq!(readback, payload);
    }

    #[test]
    fn lease_release_requeues_routed_reclaim_after_worker_defers_for_active_lease() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));
        set_route_primary(&mut map, data_pg, NodeId::new(2));

        let cluster = crate::StorageCluster::from_local_map(Arc::new(map)).unwrap();
        let committed =
            write_committed_direct_segment_for(&cluster, &bucket, &key, b"leased payload");
        assert_eq!(committed.written.data_pg_id, data_pg);

        let lease = cluster
            .acquire_object_payload_lease(&bucket, &key, committed.generation_id)
            .unwrap();
        let delete_outcome = cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
        assert!(matches!(
            delete_outcome.deleted,
            crate::DeletedCurrentObject::Live {
                generation_id,
                ..
            } if generation_id == committed.generation_id
        ));
        assert!(
            cluster
                .payload_reclaim_exists(&bucket, &key, committed.generation_id)
                .unwrap(),
            "delete should create reclaim metadata on the routed object PG primary"
        );

        cluster.enqueue_object_payload_reclaim(&bucket, &key, committed.generation_id);
        assert!(matches!(
            cluster.try_take_reclaim_work(),
            Some(crate::ReclaimWorkItem::ObjectPayload((
                queued_bucket,
                queued_key,
                queued_generation_id
            ))) if queued_bucket == bucket
                && queued_key == key
                && queued_generation_id == committed.generation_id
        ));
        assert!(
            !cluster
                .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
                .unwrap(),
            "active lease should make the worker defer reclaim"
        );
        assert!(cluster.try_take_reclaim_work().is_none());

        let released = lease.release();
        assert_eq!(released.remaining(), 0);
        assert!(
            released.payload_reclaim_exists().unwrap(),
            "released lease must find reclaim metadata through the routed object PG primary"
        );
        released.enqueue_object_payload_reclaim();
        assert!(matches!(
            cluster.try_take_reclaim_work(),
            Some(crate::ReclaimWorkItem::ObjectPayload((
                queued_bucket,
                queued_key,
                queued_generation_id
            ))) if queued_bucket == bucket
                && queued_key == key
                && queued_generation_id == committed.generation_id
        ));

        assert!(
            cluster
                .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
                .unwrap(),
            "lease release requeue should make the deferred reclaim retryable"
        );
        assert!(!cluster
            .payload_reclaim_exists(&bucket, &key, committed.generation_id)
            .unwrap());
        for shard_index in 0..committed.written.ec.k + committed.written.ec.m {
            assert!(
                !cluster
                    .test_payload_shard_file_exists(
                        committed.written.data_pg_id,
                        committed.written.ec,
                        &committed.segment_okh,
                        committed.generation_id,
                        shard_index,
                    )
                    .unwrap(),
                "retried reclaim should delete placed shard {shard_index}"
            );
        }
    }

    #[test]
    fn composite_object_listings_fan_out_to_routed_pg_primaries() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let key_a = key_for_object_pg(topology, &bucket, 1, "dir/a/file-");
        let key_b = key_for_object_pg(topology, &bucket, 2, "dir/b/file-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        set_route_primary(&mut map, 2, NodeId::new(2));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        write_committed_direct_segment_for_with_okh(
            &cluster,
            &bucket,
            &key_b,
            [52; 16],
            b"payload-b",
        );
        write_committed_direct_segment_for_with_okh(
            &cluster,
            &bucket,
            &key_a,
            [53; 16],
            b"payload-a",
        );

        let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
        assert!(bridge_node.test_get_object_meta(&bucket, &key_a).is_err());
        assert!(bridge_node.test_get_object_meta(&bucket, &key_b).is_err());

        let all_objects = cluster.list_all_objects_for_bucket(&bucket).unwrap();
        assert_eq!(
            all_objects
                .iter()
                .map(|object| object.key())
                .collect::<Vec<_>>(),
            vec![&key_a, &key_b]
        );

        let listed = cluster
            .list_objects_for_bucket(&bucket, None, None, None, 100, 100)
            .unwrap();
        assert_eq!(
            listed
                .objects
                .iter()
                .map(|object| object.key())
                .collect::<Vec<_>>(),
            vec![&key_a, &key_b]
        );

        let prefix = crate::ObjectKey::try_from("dir/".to_string()).unwrap();
        let delimited = cluster
            .list_objects_for_bucket(&bucket, Some(&prefix), Some("/"), None, 100, 100)
            .unwrap();
        assert!(delimited.objects.is_empty());
        assert_eq!(
            delimited
                .common_prefixes
                .iter()
                .map(crate::ObjectKey::as_str)
                .collect::<Vec<_>>(),
            vec!["dir/a/", "dir/b/"]
        );

        let all_versions = cluster
            .list_all_object_versions_for_bucket(&bucket)
            .unwrap();
        assert_eq!(
            all_versions
                .iter()
                .map(|object| object.key())
                .collect::<Vec<_>>(),
            vec![&key_a, &key_b]
        );

        let listed_versions = cluster
            .list_object_versions_for_bucket(&bucket, None, None, None, 100)
            .unwrap();
        assert_eq!(
            listed_versions
                .versions
                .iter()
                .map(|object| object.key())
                .collect::<Vec<_>>(),
            vec![&key_a, &key_b]
        );
    }

    #[test]
    fn composite_bucket_listing_fan_out_to_routed_pg_primaries() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let (bucket_a, bucket_b) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            (
                bucket_for_pg(topology, 1, "routed-bucket-a-"),
                bucket_for_pg(topology, 2, "routed-bucket-b-"),
            )
        };
        set_route_primary(&mut map, 1, NodeId::new(1));
        set_route_primary(&mut map, 2, NodeId::new(2));

        let owner = crate::CanonicalUserId::from_principal("owner");
        seed_bucket_record(&map, NodeId::new(1), 1, &bucket_a, &owner);
        seed_bucket_record(&map, NodeId::new(2), 2, &bucket_b, &owner);

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

        let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
        assert!(bridge_node.test_head_bucket_raw(&bucket_a).is_err());
        assert!(bridge_node.test_head_bucket_raw(&bucket_b).is_err());

        let buckets = cluster.list_buckets_for_owner(owner.as_str()).unwrap();
        assert_eq!(
            buckets
                .iter()
                .map(|bucket| &bucket.name)
                .collect::<Vec<_>>(),
            vec![&bucket_a, &bucket_b]
        );
    }

    #[test]
    fn create_bucket_command_applies_to_all_acting_pg_nodes() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "replicated-create-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let owner = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let created = cluster
            .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: true,
                public_write: false,
                versioning: crate::BucketVersioningState::Enabled,
                object_lock: crate::BucketObjectLockConfig::default(),
            })
            .unwrap();
        let created = match created {
            crate::BucketCreateAttemptOutcome::Created(info) => info,
            crate::BucketCreateAttemptOutcome::Exists(_) => {
                panic!("fresh bucket unexpectedly existed")
            }
        };

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.name, created.name);
            assert_eq!(info.owner_principal, created.owner_principal);
            assert_eq!(info.owner_canonical_id, created.owner_canonical_id);
            assert_eq!(info.created_at, created.created_at);
            assert_eq!(info.versioning, created.versioning);
            assert_eq!(info.acl_grants, created.acl_grants);
            assert_eq!(info.public_read, created.public_read);
            assert_eq!(info.public_write, created.public_write);
            assert_eq!(
                info.bucket_execution_generation,
                created.bucket_execution_generation
            );
        }

        let exists = cluster
            .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: true,
                public_write: false,
                versioning: crate::BucketVersioningState::Enabled,
                object_lock: crate::BucketObjectLockConfig::default(),
            })
            .unwrap();
        assert!(matches!(
            exists,
            crate::BucketCreateAttemptOutcome::Exists(info) if info.name == bucket
        ));
    }

    #[test]
    fn create_bucket_command_retry_reuses_pending_partial_replica_command() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "partial-create-retry-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let owner = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CreateBucket(create)
                        if create.name == hook_bucket
                            && node_id == NodeId::new(2)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected metadata command apply failure",
                            source: std::io::Error::other(
                                "injected metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));
        let create_config = || crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: true,
            public_write: false,
            versioning: crate::BucketVersioningState::Enabled,
            object_lock: crate::BucketObjectLockConfig::default(),
        };

        let err = cluster
            .create_bucket_with_config_and_load_info(&create_config())
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "injected metadata command apply failure",
                    ..
                })
            ),
            "expected injected replica failure, got {err:?}"
        );
        assert!(!fail_once.load(Ordering::SeqCst));

        let partial_info = {
            let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = applied_replica.get_pg(1).unwrap();
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
        };
        for node_id in [NodeId::new(1), NodeId::new(2)] {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            assert!(
                crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).is_err(),
                "node {node_id:?} should not have the partially applied bucket"
            );
        }

        let retried = cluster
            .create_bucket_with_config_and_load_info(&create_config())
            .unwrap();
        assert!(matches!(
            retried,
            crate::BucketCreateAttemptOutcome::Created(info)
                if info.name == bucket
                    && info.created_at == partial_info.created_at
                    && info.bucket_execution_generation
                        == partial_info.bucket_execution_generation
        ));

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.name, partial_info.name);
            assert_eq!(info.owner_principal, partial_info.owner_principal);
            assert_eq!(info.owner_canonical_id, partial_info.owner_canonical_id);
            assert_eq!(info.created_at, partial_info.created_at);
            assert_eq!(info.state, partial_info.state);
            assert_eq!(info.versioning, partial_info.versioning);
            assert_eq!(info.object_lock, partial_info.object_lock);
            assert_eq!(info.acl_grants, partial_info.acl_grants);
            assert_eq!(info.public_read, partial_info.public_read);
            assert_eq!(info.public_write, partial_info.public_write);
            assert_eq!(
                info.bucket_execution_generation,
                partial_info.bucket_execution_generation
            );
        }
    }

    #[test]
    fn put_bucket_versioning_command_applies_to_all_acting_pg_nodes() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "replicated-versioning-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let original = {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            primary.test_head_bucket_raw(&bucket).unwrap()
        };

        let updated = cluster
            .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
            .unwrap();
        assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
        assert!(updated.bucket_execution_generation > original.bucket_execution_generation);

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.versioning, updated.versioning);
            assert_eq!(
                info.bucket_execution_generation,
                updated.bucket_execution_generation
            );
        }
    }

    #[test]
    fn put_bucket_versioning_command_retry_reuses_pending_partial_replica_command() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "partial-versioning-retry-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::PutBucketVersioning(versioning)
                        if versioning.name == hook_bucket
                            && node_id == NodeId::new(2)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected metadata command apply failure",
                            source: std::io::Error::other(
                                "injected metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "injected metadata command apply failure",
                    ..
                })
            ),
            "expected injected replica failure, got {err:?}"
        );
        assert!(!fail_once.load(Ordering::SeqCst));

        let partial_info = {
            let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = applied_replica.get_pg(1).unwrap();
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
        };
        assert_eq!(
            partial_info.versioning,
            crate::BucketVersioningState::Enabled
        );
        for node_id in [NodeId::new(1), NodeId::new(2)] {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            assert_eq!(
                crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                    .unwrap()
                    .versioning,
                crate::BucketVersioningState::Disabled,
                "node {node_id:?} should not have the partially applied versioning update"
            );
        }

        let retried = cluster
            .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
            .unwrap();
        assert_eq!(retried.versioning, crate::BucketVersioningState::Enabled);
        assert_eq!(
            retried.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
            assert_eq!(
                info.bucket_execution_generation,
                partial_info.bucket_execution_generation
            );
        }
    }

    #[test]
    fn pending_bucket_metadata_command_blocks_later_acl_until_versioning_retry_converges() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "pending-stream-acl-block-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::PutBucketVersioning(versioning)
                        if versioning.name == hook_bucket
                            && node_id == NodeId::new(2)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected metadata command apply failure",
                            source: std::io::Error::other(
                                "injected metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "injected metadata command apply failure",
                    ..
                })
            ),
            "expected injected replica failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(!fail_once.load(Ordering::SeqCst));

        let pending = map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(1), &bucket)
            .expect("failed versioning command should remain pending");
        assert!(matches!(
            pending.payload(),
            MetadataCommandPayload::PutBucketVersioning(versioning)
                if versioning.name == bucket
                    && versioning.state == crate::BucketVersioningState::Enabled
        ));
        let partial_info = {
            let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = applied_replica.get_pg(1).unwrap();
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
        };
        assert_eq!(
            partial_info.versioning,
            crate::BucketVersioningState::Enabled
        );

        let acl_grants = crate::AclGrants::default();
        let acl_err = cluster
            .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
            .unwrap_err();
        assert!(
            matches!(
                acl_err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "unexpected pending put bucket versioning command for bucket acl",
                    ..
                })
            ),
            "expected pending versioning command to block ACL update, got {acl_err:?}"
        );
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert!(!info.public_read);
            assert!(!info.public_write);
        }

        let retried = cluster
            .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
            .unwrap();
        assert_eq!(retried.versioning, crate::BucketVersioningState::Enabled);
        assert_eq!(
            retried.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );

        let acl_updated = cluster
            .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
            .unwrap();
        assert_eq!(
            acl_updated.versioning,
            crate::BucketVersioningState::Enabled
        );
        assert!(acl_updated.public_read);
        assert!(!acl_updated.public_write);
        assert!(
            acl_updated.bucket_execution_generation > retried.bucket_execution_generation,
            "later ACL command must reserve a newer execution generation after retry convergence"
        );

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
            assert!(info.public_read);
            assert!(!info.public_write);
            assert_eq!(
                info.bucket_execution_generation,
                acl_updated.bucket_execution_generation
            );
        }
    }

    #[test]
    fn put_bucket_acl_command_applies_to_all_acting_pg_nodes() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "replicated-acl-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let original = {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            primary.test_head_bucket_raw(&bucket).unwrap()
        };
        let acl_grants = crate::AclGrants::default();

        let updated = cluster
            .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
            .unwrap();
        assert_eq!(updated.acl_grants, acl_grants);
        assert!(updated.public_read);
        assert!(!updated.public_write);
        assert!(updated.bucket_execution_generation > original.bucket_execution_generation);

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.acl_grants, updated.acl_grants);
            assert_eq!(info.public_read, updated.public_read);
            assert_eq!(info.public_write, updated.public_write);
            assert_eq!(
                info.bucket_execution_generation,
                updated.bucket_execution_generation
            );
        }
    }

    #[test]
    fn put_bucket_acl_command_retry_reuses_pending_partial_replica_command() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "partial-acl-retry-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let acl_grants = crate::AclGrants::default();
        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::PutBucketAcl(acl)
                        if acl.name == hook_bucket
                            && node_id == NodeId::new(2)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected metadata command apply failure",
                            source: std::io::Error::other(
                                "injected metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "injected metadata command apply failure",
                    ..
                })
            ),
            "expected injected replica failure, got {err:?}"
        );
        assert!(!fail_once.load(Ordering::SeqCst));

        let partial_info = {
            let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = applied_replica.get_pg(1).unwrap();
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
        };
        assert!(partial_info.public_read);
        assert!(!partial_info.public_write);
        for node_id in [NodeId::new(1), NodeId::new(2)] {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert!(!info.public_read);
            assert!(
                !info.public_write,
                "node {node_id:?} should not have the partially applied ACL update"
            );
        }

        let retried = cluster
            .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
            .unwrap();
        assert!(retried.public_read);
        assert!(!retried.public_write);
        assert_eq!(
            retried.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.acl_grants, acl_grants);
            assert!(info.public_read);
            assert!(!info.public_write);
            assert_eq!(
                info.bucket_execution_generation,
                partial_info.bucket_execution_generation
            );
        }
    }

    #[test]
    fn bucket_property_commands_apply_to_all_acting_pg_nodes() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "replicated-property-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let mut previous_generation = {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            primary.test_head_bucket_raw(&bucket).unwrap()
        }
        .bucket_execution_generation;
        let updated = cluster
            .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;

        let object_lock = crate::BucketObjectLockConfig {
            enabled: true,
            default_retention: Some(crate::ObjectLockDefaultRetention {
                mode: crate::ObjectLockMode::Governance,
                period: crate::RetentionPeriod::days(3).unwrap(),
            }),
        };
        let updated = cluster
            .put_bucket_object_lock_and_load_info(&bucket, object_lock)
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.object_lock, object_lock);
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let encryption = crate::BucketEncryptionConfig {
            default_encryption: Some(crate::ManagedEncryptionAlgorithm::Aes256),
            sse_c_blocked: false,
        };
        let updated = cluster
            .put_bucket_encryption_and_load_info(&bucket, encryption)
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.encryption, encryption.effective());
            assert_eq!(
                crate::PgMetadataStore::get_bucket_encryption(&*pg, &bucket).unwrap(),
                encryption
            );
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let public_access_block = crate::PublicAccessBlockConfig {
            block_public_acls: true,
            ignore_public_acls: false,
            block_public_policy: true,
            restrict_public_buckets: false,
        };
        let updated = cluster
            .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.public_access_block, Some(public_access_block));
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let updated = cluster
            .delete_bucket_public_access_block_and_load_info(&bucket)
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.public_access_block, None);
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let ownership_controls = crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::BucketOwnerPreferred,
        };
        let updated = cluster
            .put_bucket_ownership_controls_and_load_info(&bucket, ownership_controls)
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.ownership_controls, Some(ownership_controls));
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let updated = cluster
            .delete_bucket_ownership_controls_and_load_info(&bucket)
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.ownership_controls, None);
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let updated = cluster
            .put_bucket_abac_enabled_and_load_info(&bucket, true)
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert!(info.bucket_abac_enabled);
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }
    }

    #[test]
    fn bucket_property_command_retry_reuses_pending_partial_replica_command() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "partial-property-retry-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let public_access_block = crate::PublicAccessBlockConfig {
            block_public_acls: true,
            ignore_public_acls: true,
            block_public_policy: false,
            restrict_public_buckets: true,
        };
        let expected_mutation =
            BucketPropertyMutation::PublicAccessBlock(Some(public_access_block));
        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::PutBucketProperty(property)
                        if property.name == hook_bucket
                            && property.mutation == expected_mutation
                            && node_id == NodeId::new(2)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected metadata command apply failure",
                            source: std::io::Error::other(
                                "injected metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "injected metadata command apply failure",
                    ..
                })
            ),
            "expected injected replica failure, got {err:?}"
        );
        assert!(!fail_once.load(Ordering::SeqCst));

        let partial_info = {
            let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = applied_replica.get_pg(1).unwrap();
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
        };
        assert_eq!(partial_info.public_access_block, Some(public_access_block));
        for node_id in [NodeId::new(1), NodeId::new(2)] {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            assert_eq!(
                crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                    .unwrap()
                    .public_access_block,
                None,
                "node {node_id:?} should not have the partially applied property update"
            );
        }

        let retried = cluster
            .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
            .unwrap();
        assert_eq!(retried.public_access_block, Some(public_access_block));
        assert_eq!(
            retried.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.public_access_block, Some(public_access_block));
            assert_eq!(
                info.bucket_execution_generation,
                partial_info.bucket_execution_generation
            );
        }
    }

    #[test]
    fn invalid_bucket_property_command_does_not_poison_bucket_command_stream() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "invalid-property-no-poison-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let initial_generation = {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            primary.test_head_bucket_raw(&bucket).unwrap()
        }
        .bucket_execution_generation;

        let invalid_object_lock = crate::BucketObjectLockConfig {
            enabled: true,
            default_retention: None,
        };
        let err = cluster
            .put_bucket_object_lock_and_load_info(&bucket, invalid_object_lock)
            .unwrap_err();
        match err {
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
                context: "put bucket object lock",
                source: rusqlite::Error::SqliteFailure(_, Some(message)),
            }) if message == "bucket object lock requires enabled versioning" => {}
            other => panic!("expected object-lock storage validation error, got {other:?}"),
        }
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(1), &bucket)
                .is_none(),
            "deterministic validation failures must not leave pending commands"
        );
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.object_lock, crate::BucketObjectLockConfig::default());
            assert_eq!(info.bucket_execution_generation, initial_generation);
        }

        let public_access_block = crate::PublicAccessBlockConfig {
            block_public_acls: true,
            ignore_public_acls: false,
            block_public_policy: true,
            restrict_public_buckets: false,
        };
        let updated = cluster
            .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
            .unwrap();
        assert_eq!(updated.public_access_block, Some(public_access_block));
        assert!(updated.bucket_execution_generation > initial_generation);

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.public_access_block, Some(public_access_block));
            assert_eq!(
                info.bucket_execution_generation,
                updated.bucket_execution_generation
            );
        }
    }

    #[test]
    fn bucket_subresource_commands_apply_to_all_acting_pg_nodes() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "replicated-subresource-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let mut previous_generation = {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            primary.test_head_bucket_raw(&bucket).unwrap()
        }
        .bucket_execution_generation;

        let policy_body = r#"{"Statement":[]}"#;
        let updated = cluster
            .put_bucket_subresource_and_load_info(
                &bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Policy,
                    body: policy_body,
                    aux: crate::BucketSubresourceAux::policy(true),
                },
            )
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            let stored = crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Policy,
            )
            .unwrap()
            .unwrap();
            assert_eq!(stored.body, policy_body);
            assert_eq!(stored.generation, Some(1));
            assert_eq!(stored.aux, crate::BucketSubresourceAux::policy(true));
            assert!(info.bucket_policy_present);
            assert!(info.bucket_policy_public);
            assert_eq!(info.bucket_policy_generation, 1);
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let tags_body = "<Tagging><TagSet/></Tagging>";
        let updated = cluster
            .put_bucket_subresource_and_load_info(
                &bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Tagging,
                    body: tags_body,
                    aux: crate::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            let stored = crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Tagging,
            )
            .unwrap()
            .unwrap();
            assert_eq!(stored.body, tags_body);
            assert_eq!(stored.generation, Some(1));
            assert_eq!(stored.aux, crate::BucketSubresourceAux::None);
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let updated = cluster
            .delete_bucket_subresource_and_load_info(&bucket, crate::BucketSubresourceKind::Tagging)
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert!(crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Tagging,
            )
            .unwrap()
            .is_none());
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let lifecycle_body = "<LifecycleConfiguration/>";
        let updated = cluster
            .put_bucket_subresource_and_load_info(
                &bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Lifecycle,
                    body: lifecycle_body,
                    aux: crate::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            let stored = crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Lifecycle,
            )
            .unwrap()
            .unwrap();
            assert_eq!(stored.body, lifecycle_body);
            assert_eq!(stored.generation, Some(1));
            assert!(info.bucket_lifecycle_present);
            assert_eq!(info.bucket_lifecycle_generation, 1);
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let cors_body = "<CORSConfiguration/>";
        let updated = cluster
            .put_bucket_subresource_and_load_info(
                &bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Cors,
                    body: cors_body,
                    aux: crate::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            let stored = crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Cors,
            )
            .unwrap()
            .unwrap();
            assert_eq!(stored.body, cors_body);
            assert_eq!(stored.generation, Some(1));
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let updated = cluster
            .delete_bucket_subresource_and_load_info(&bucket, crate::BucketSubresourceKind::Cors)
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert!(crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Cors,
            )
            .unwrap()
            .is_none());
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let updated = cluster
            .delete_bucket_subresource_and_load_info(&bucket, crate::BucketSubresourceKind::Policy)
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert!(crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Policy,
            )
            .unwrap()
            .is_none());
            assert!(!info.bucket_policy_present);
            assert!(!info.bucket_policy_public);
            assert_eq!(info.bucket_policy_generation, 2);
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }

        let updated = cluster
            .delete_bucket_subresource_and_load_info(
                &bucket,
                crate::BucketSubresourceKind::Lifecycle,
            )
            .unwrap();
        assert!(updated.bucket_execution_generation > previous_generation);
        previous_generation = updated.bucket_execution_generation;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert!(crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Lifecycle,
            )
            .unwrap()
            .is_none());
            assert!(!info.bucket_lifecycle_present);
            assert_eq!(info.bucket_lifecycle_generation, 2);
            assert_eq!(info.bucket_execution_generation, previous_generation);
        }
    }

    #[test]
    fn bucket_subresource_command_retry_reuses_pending_partial_replica_command() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "partial-subresource-retry-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let policy_body = r#"{"Statement":[]}"#;
        let expected_mutation = BucketSubresourceMutation::Put {
            kind: crate::BucketSubresourceKind::Policy,
            body: policy_body.to_owned(),
            aux: crate::BucketSubresourceAux::policy(false),
        };
        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::PutBucketSubresource(subresource)
                        if subresource.name == hook_bucket
                            && subresource.mutation == expected_mutation
                            && node_id == NodeId::new(2)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected metadata command apply failure",
                            source: std::io::Error::other(
                                "injected metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .put_bucket_subresource_and_load_info(
                &bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Policy,
                    body: policy_body,
                    aux: crate::BucketSubresourceAux::policy(false),
                },
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "injected metadata command apply failure",
                    ..
                })
            ),
            "expected injected replica failure, got {err:?}"
        );
        assert!(!fail_once.load(Ordering::SeqCst));

        let partial_info = {
            let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = applied_replica.get_pg(1).unwrap();
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
        };
        assert!(partial_info.bucket_policy_present);
        assert!(!partial_info.bucket_policy_public);
        assert_eq!(partial_info.bucket_policy_generation, 1);
        for node_id in [NodeId::new(1), NodeId::new(2)] {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert!(
                !info.bucket_policy_present,
                "node {node_id:?} should not have the partially applied policy"
            );
        }

        let retried = cluster
            .put_bucket_subresource_and_load_info(
                &bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Policy,
                    body: policy_body,
                    aux: crate::BucketSubresourceAux::policy(false),
                },
            )
            .unwrap();
        assert!(retried.bucket_policy_present);
        assert!(!retried.bucket_policy_public);
        assert_eq!(
            retried.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            let stored = crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Policy,
            )
            .unwrap()
            .unwrap();
            assert_eq!(stored.body, policy_body);
            assert_eq!(stored.generation, Some(1));
            assert_eq!(stored.aux, crate::BucketSubresourceAux::policy(false));
            assert!(info.bucket_policy_present);
            assert!(!info.bucket_policy_public);
            assert_eq!(info.bucket_policy_generation, 1);
            assert_eq!(
                info.bucket_execution_generation,
                partial_info.bucket_execution_generation
            );
        }
    }

    #[test]
    fn invalid_bucket_subresource_command_does_not_poison_bucket_command_stream() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "invalid-subresource-no-poison-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let initial_generation = {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            primary.test_head_bucket_raw(&bucket).unwrap()
        }
        .bucket_execution_generation;

        let err = cluster
            .put_bucket_subresource_and_load_info(
                &bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Tagging,
                    body: "<Tagging/>",
                    aux: crate::BucketSubresourceAux::policy(true),
                },
            )
            .unwrap_err();
        match err {
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
                context: "put bucket subresource",
                source: rusqlite::Error::InvalidParameterName(message),
            }) if message.contains("Tagging does not support aux") => {}
            other => panic!("expected subresource storage validation error, got {other:?}"),
        }
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(1), &bucket)
                .is_none(),
            "deterministic validation failures must not leave pending commands"
        );
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.bucket_execution_generation, initial_generation);
            assert!(crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Tagging,
            )
            .unwrap()
            .is_none());
        }

        let tags_body = "<Tagging><TagSet/></Tagging>";
        let updated = cluster
            .put_bucket_subresource_and_load_info(
                &bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Tagging,
                    body: tags_body,
                    aux: crate::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        assert!(updated.bucket_execution_generation > initial_generation);

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            let stored = crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Tagging,
            )
            .unwrap()
            .unwrap();
            assert_eq!(stored.body, tags_body);
            assert_eq!(
                info.bucket_execution_generation,
                updated.bucket_execution_generation
            );
        }
    }

    #[test]
    fn finalized_bucket_delete_clears_pending_versioning_command_for_recreate() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "partial-versioning-delete-recreate-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::PutBucketVersioning(versioning)
                        if versioning.name == hook_bucket
                            && node_id == NodeId::new(2)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected metadata command apply failure",
                            source: std::io::Error::other(
                                "injected metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "injected metadata command apply failure",
                    ..
                })
            ),
            "expected injected replica failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(1), &bucket)
                .is_some(),
            "failed versioning command should remain pending before delete"
        );
        let old_partial_generation = {
            let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = applied_replica.get_pg(1).unwrap();
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .bucket_execution_generation
        };

        cluster.begin_bucket_delete(&bucket).unwrap();
        assert_eq!(
            cluster.try_finalize_bucket_delete(&bucket).unwrap(),
            crate::BucketDeleteFinalizeOutcome::Finalized
        );
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(1), &bucket)
                .is_none(),
            "finalized delete must clear stale pending commands for the old bucket incarnation"
        );

        let owner = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let recreated = cluster
            .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: crate::BucketVersioningState::Disabled,
                object_lock: crate::BucketObjectLockConfig::default(),
            })
            .unwrap();
        let recreated_generation = match recreated {
            crate::BucketCreateAttemptOutcome::Created(info) => info.bucket_execution_generation,
            other => panic!("expected recreated bucket, got {other:?}"),
        };
        assert!(recreated_generation > old_partial_generation);

        let updated = cluster
            .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
            .unwrap();
        assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
        assert!(updated.bucket_execution_generation > recreated_generation);

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
            assert_eq!(
                info.bucket_execution_generation,
                updated.bucket_execution_generation
            );
        }
    }

    #[test]
    fn finalized_bucket_delete_removes_replicated_create_rows() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "delete-recreate-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let created_generation = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .test_head_bucket_raw(&bucket)
            .unwrap()
            .bucket_execution_generation;
        cluster
            .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
            .unwrap();
        let pre_delete_generation = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .test_head_bucket_raw(&bucket)
            .unwrap()
            .bucket_execution_generation;
        assert!(pre_delete_generation > created_generation);

        cluster.begin_bucket_delete(&bucket).unwrap();
        assert_eq!(
            cluster.try_finalize_bucket_delete(&bucket).unwrap(),
            crate::BucketDeleteFinalizeOutcome::Finalized
        );

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            assert!(crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).is_err());
        }

        let owner = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let recreated = cluster
            .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: crate::BucketVersioningState::Disabled,
                object_lock: crate::BucketObjectLockConfig::default(),
            })
            .unwrap();
        assert!(matches!(
            recreated,
            crate::BucketCreateAttemptOutcome::Created(info)
                if info.name == bucket
                    && info.bucket_execution_generation > pre_delete_generation
        ));

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            assert_eq!(
                crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                    .unwrap()
                    .name,
                bucket
            );
        }
    }

    #[test]
    fn bucket_snapshot_pair_routes_to_bucket_pg_primaries() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let (source_bucket, destination_bucket) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            (
                bucket_for_pg(topology, 1, "snapshot-source-"),
                bucket_for_pg(topology, 2, "snapshot-destination-"),
            )
        };
        set_route_primary(&mut map, 1, NodeId::new(1));
        set_route_primary(&mut map, 2, NodeId::new(2));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &source_bucket);
        create_test_bucket(&cluster, &destination_bucket);
        cluster
            .put_bucket_subresource_and_load_info(
                &source_bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Tagging,
                    body: "<Tagging><TagSet><Tag><Key>src</Key><Value>1</Value></Tag></TagSet></Tagging>",
                    aux: crate::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        cluster
            .put_bucket_subresource_and_load_info(
                &destination_bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Cors,
                    body: "<CORSConfiguration/>",
                    aux: crate::BucketSubresourceAux::None,
                },
            )
            .unwrap();

        let pair = cluster
            .load_bucket_snapshot_pair(
                (
                    &source_bucket,
                    crate::BucketSnapshotRequest {
                        tags: crate::BucketSnapshotTagsRequest::Always,
                        ..Default::default()
                    },
                ),
                (
                    &destination_bucket,
                    crate::BucketSnapshotRequest {
                        cors: true,
                        ..Default::default()
                    },
                ),
            )
            .unwrap();
        assert_eq!(pair.source().bucket.name, source_bucket);
        assert_eq!(pair.destination().bucket.name, destination_bucket);
        assert_eq!(
            pair.source().tags,
            crate::LoadedBucketSubresource::Loaded(
                "<Tagging><TagSet><Tag><Key>src</Key><Value>1</Value></Tag></TagSet></Tagging>"
                    .to_string()
            )
        );
        assert_eq!(
            pair.destination().cors,
            crate::LoadedBucketSubresource::Loaded("<CORSConfiguration/>".to_string())
        );
    }

    #[test]
    fn composite_multipart_and_lifecycle_scans_fan_out_to_routed_pg_primaries() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let upload_bucket =
            crate::BucketName::try_from("multipart-fanout-bucket".to_string()).unwrap();
        let lifecycle_bucket = bucket_for_pg(topology, 1, "lifecycle-bucket-");
        let aborting_bucket = bucket_for_pg(topology, 2, "aborting-bucket-");
        let key_a = key_for_object_pg(topology, &upload_bucket, 1, "uploads/a-");
        let key_b = key_for_object_pg(topology, &upload_bucket, 2, "uploads/b-");
        let aborting_key = key_for_object_pg(topology, &aborting_bucket, 2, "abort-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        set_route_primary(&mut map, 2, NodeId::new(2));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &lifecycle_bucket);
        create_test_bucket(&cluster, &aborting_bucket);
        cluster
            .put_bucket_subresource_and_load_info(
                &lifecycle_bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration/>",
                    aux: crate::BucketSubresourceAux::None,
                },
            )
            .unwrap();

        let upload_a = upload_id_from_label("routedUploadA");
        let upload_b = upload_id_from_label("routedUploadB");
        let aborting_upload = upload_id_from_label("abortingUpload");
        seed_multipart_upload_record(
            &map,
            NodeId::new(1),
            1,
            &upload_bucket,
            &key_a,
            &upload_a,
            crate::UploadState::InProgress,
        );
        seed_multipart_upload_record(
            &map,
            NodeId::new(2),
            2,
            &upload_bucket,
            &key_b,
            &upload_b,
            crate::UploadState::InProgress,
        );
        seed_multipart_upload_record(
            &map,
            NodeId::new(2),
            2,
            &aborting_bucket,
            &aborting_key,
            &aborting_upload,
            crate::UploadState::Aborting,
        );

        let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
        assert!(bridge_node
            .test_list_multipart_uploads_for_bucket(&upload_bucket)
            .unwrap()
            .is_empty());
        let all_uploads = cluster
            .list_all_multipart_uploads_for_bucket(&upload_bucket)
            .unwrap();
        assert_eq!(
            all_uploads
                .iter()
                .map(|upload| (&upload.key, &upload.upload_id))
                .collect::<Vec<_>>(),
            vec![(&key_a, &upload_a), (&key_b, &upload_b)]
        );

        let mut listed_uploads = cluster
            .list_multipart_uploads_for_bucket(&upload_bucket, None, None, None, 100, 100)
            .unwrap()
            .uploads;
        listed_uploads.sort_by(|left, right| left.key.cmp(&right.key));
        assert_eq!(
            listed_uploads
                .iter()
                .map(|upload| (&upload.key, &upload.upload_id))
                .collect::<Vec<_>>(),
            vec![(&key_a, &upload_a), (&key_b, &upload_b)]
        );

        let sweep = cluster.list_lifecycle_sweep_buckets().unwrap();
        assert_eq!(
            sweep
                .lifecycle_buckets
                .iter()
                .map(|bucket| &bucket.name)
                .collect::<Vec<_>>(),
            vec![&lifecycle_bucket]
        );
        assert_eq!(sweep.aborting_buckets, vec![aborting_bucket]);
    }

    #[test]
    fn completed_multipart_prune_fans_out_to_routed_pg_primaries() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = crate::BucketName::try_from("completed-prune-bucket".to_string()).unwrap();
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let older_key = key_for_object_pg(topology, &bucket, 1, "older-");
        let newer_key = key_for_object_pg(topology, &bucket, 2, "newer-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        set_route_primary(&mut map, 2, NodeId::new(2));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let older_upload = upload_id_from_label("olderCompleted");
        let newer_upload = upload_id_from_label("newerCompleted");
        seed_completed_multipart_upload_record(
            &map,
            NodeId::new(1),
            1,
            &bucket,
            &older_key,
            &older_upload,
            1,
        );
        seed_completed_multipart_upload_record(
            &map,
            NodeId::new(2),
            2,
            &bucket,
            &newer_key,
            &newer_upload,
            2,
        );

        let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
        assert!(bridge_node
            .get_pg(1)
            .unwrap()
            .list_completed_multipart_uploads_for_bucket(bucket.as_str())
            .unwrap()
            .is_empty());
        assert!(bridge_node
            .get_pg(2)
            .unwrap()
            .list_completed_multipart_uploads_for_bucket(bucket.as_str())
            .unwrap()
            .is_empty());

        cluster
            .prune_completed_multipart_uploads_for_bucket_with_limit(&bucket, 1)
            .unwrap();

        let node_one_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        assert!(
            crate::PgMetadataStore::get_completed_multipart_upload(&*node_one_pg, &older_upload)
                .unwrap()
                .is_none(),
            "older routed completed-upload tombstone should be pruned"
        );
        let node_two_pg = map
            .node(NodeId::new(2))
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        assert!(
            crate::PgMetadataStore::get_completed_multipart_upload(&*node_two_pg, &newer_upload)
                .unwrap()
                .is_some(),
            "newer routed completed-upload tombstone should be retained"
        );
    }

    #[test]
    fn bucket_delete_and_finalize_fan_out_to_routed_pg_primaries() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let (bucket, live_key, tombstone_key) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "delete-bucket-");
            let live_key = key_for_object_pg(topology, &bucket, 2, "live-");
            let tombstone_key = key_for_object_pg(topology, &bucket, 2, "tombstone-");
            (bucket, live_key, tombstone_key)
        };
        set_route_primary(&mut map, 1, NodeId::new(1));
        set_route_primary(&mut map, 2, NodeId::new(2));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        write_committed_direct_segment_for_with_okh(
            &cluster, &bucket, &live_key, [61; 16], b"live",
        );

        let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
        assert!(bridge_node
            .test_get_object_meta(&bucket, &live_key)
            .is_err());

        let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
            ),
            "routed non-empty bucket should reject delete, got {err:?}"
        );

        let node_two = map.node(NodeId::new(2)).unwrap().storage_node();
        let node_two_pg = node_two.get_pg(2).unwrap();
        crate::PgMetadataStore::delete_object_meta(&*node_two_pg, &bucket, &live_key).unwrap();
        drop(node_two_pg);

        let completed_upload = upload_id_from_label("deleteCompleted");
        seed_completed_multipart_upload_record(
            &map,
            NodeId::new(2),
            2,
            &bucket,
            &tombstone_key,
            &completed_upload,
            1,
        );
        let node_two_pg = node_two.get_pg(2).unwrap();
        crate::PgMetadataStore::delete_object_meta(&*node_two_pg, &bucket, &tombstone_key).unwrap();
        assert!(crate::PgMetadataStore::get_completed_multipart_upload(
            &*node_two_pg,
            &completed_upload
        )
        .unwrap()
        .is_some());
        drop(node_two_pg);

        cluster.begin_bucket_delete(&bucket).unwrap();
        assert_eq!(
            cluster.try_finalize_bucket_delete(&bucket).unwrap(),
            crate::BucketDeleteFinalizeOutcome::Finalized
        );

        assert!(map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .test_head_bucket_raw(&bucket)
            .is_err());
        let node_two_pg = node_two.get_pg(2).unwrap();
        assert!(
            crate::PgMetadataStore::get_completed_multipart_upload(
                &*node_two_pg,
                &completed_upload
            )
            .unwrap()
            .is_none(),
            "finalization should prune routed completed-upload tombstones"
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
            crate::BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation {
                pg_id: 0,
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
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id: 0,
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
            crate::BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation {
                pg_id: 0,
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
            StoreError::StaleMetadataOperation {
                pg_id: 0,
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
            StoreError::PgNotActive {
                pg_id,
                cluster_epoch,
                state: PgState::Peering,
            } if pg_id == data_pg_id.get()
                && cluster_epoch == ClusterEpoch::INITIAL
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
