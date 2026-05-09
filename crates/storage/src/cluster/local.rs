use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use placement::{NodeId, PlacementConstraint, PlacementError, TopologyKey};

use super::ShardLocation;
use crate::error::{ClusterBuildError, ShardIoError, StoreError};
use crate::metadata_command::{
    MetadataCommandAcceptance, MetadataCommandEnvelope, MetadataCommandLogIndex,
    MetadataCommandReplicaState,
};
use crate::{
    BucketName, ClusterEpoch, DataPgId, EcShape, GenerationId, ObjectKey, PgId, PgState,
    ReclaimWorkItem, SessionId, ShardIndex, ShardKey, SharedStorageNode, WriteAck,
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
    object_payload_leases: Mutex<LocalObjectPayloadLeaseState>,
    reclaim_queue: (Mutex<LocalReclaimQueueState>, Condvar),
    metadata_command_indexes: Mutex<HashMap<PgId, u64>>,
    metadata_command_apply_lock: Mutex<()>,
    pending_metadata_commands: Mutex<HashMap<(PgId, BucketName), MetadataCommandEnvelope>>,
    stream_segment_vids: Mutex<HashMap<SessionId, u64>>,
}

type LocalReclaimRoot = (BucketName, ObjectKey, GenerationId);

#[derive(Debug, Default)]
struct LocalObjectPayloadLeaseState {
    leases: HashMap<LocalReclaimRoot, usize>,
    reclaim_fences: HashSet<LocalReclaimRoot>,
    active_reclaims: HashSet<LocalReclaimRoot>,
}

#[derive(Debug)]
struct LocalReclaimQueueState {
    object_queue: VecDeque<LocalReclaimRoot>,
    queued_objects: HashSet<LocalReclaimRoot>,
    bucket_delete_queue: VecDeque<BucketName>,
    queued_bucket_deletes: HashSet<BucketName>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingMetadataCommandConflict;

impl LocalClusterRuntimeState {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_metadata_command_indexes(HashMap::new())
    }

    fn with_metadata_command_indexes(metadata_command_indexes: HashMap<PgId, u64>) -> Self {
        Self {
            object_payload_leases: Mutex::new(LocalObjectPayloadLeaseState::default()),
            reclaim_queue: (
                Mutex::new(LocalReclaimQueueState {
                    object_queue: VecDeque::new(),
                    queued_objects: HashSet::new(),
                    bucket_delete_queue: VecDeque::new(),
                    queued_bucket_deletes: HashSet::new(),
                }),
                Condvar::new(),
            ),
            metadata_command_indexes: Mutex::new(metadata_command_indexes),
            metadata_command_apply_lock: Mutex::new(()),
            pending_metadata_commands: Mutex::new(HashMap::new()),
            stream_segment_vids: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn allocate_stream_segment_vid(&self, session_id: &SessionId) -> GenerationId {
        let mut stream_segment_vids = self
            .stream_segment_vids
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let next_vid = stream_segment_vids
            .entry(session_id.clone())
            .or_insert(GenerationId::MIN.get());
        let current_vid = *next_vid;
        *next_vid = next_vid
            .checked_add(1)
            .expect("stream segment payload generation id overflow");
        GenerationId::new(current_vid).expect("stream segment generation ids start at one")
    }

    pub(crate) fn clear_stream_segment_vid_allocator(&self, session_id: &SessionId) {
        self.stream_segment_vids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(session_id);
    }

    #[cfg(test)]
    pub(crate) fn test_stream_segment_vid_allocator_next(
        &self,
        session_id: &SessionId,
    ) -> Option<u64> {
        self.stream_segment_vids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(session_id)
            .copied()
    }

    pub(crate) fn lock_metadata_command_apply(&self) -> MutexGuard<'_, ()> {
        self.metadata_command_apply_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner())
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

    pub(crate) fn try_set_pending_metadata_command_for_bucket(
        &self,
        pg_id: PgId,
        bucket: &BucketName,
        command: MetadataCommandEnvelope,
    ) -> Result<(), PendingMetadataCommandConflict> {
        let mut pending_commands = self
            .pending_metadata_commands
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match pending_commands.entry((pg_id, bucket.clone())) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(command);
                Ok(())
            }
            std::collections::hash_map::Entry::Occupied(_) => Err(PendingMetadataCommandConflict),
        }
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
    ) -> bool {
        let root = (bucket.clone(), key.clone(), generation_id);
        let mut state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state.reclaim_fences.contains(&root) {
            return false;
        }
        *state.leases.entry(root).or_insert(0) += 1;
        true
    }

    pub(crate) fn release_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        let mut state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let root = (bucket.clone(), key.clone(), generation_id);
        let entry = state
            .leases
            .get_mut(&root)
            .expect("object payload lease release without acquire");
        *entry -= 1;
        let remaining = *entry;
        if remaining == 0 {
            state.leases.remove(&root);
        }
        remaining
    }

    pub(crate) fn try_begin_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        let root = (bucket.clone(), key.clone(), generation_id);
        let mut state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state.leases.get(&root).copied().unwrap_or(0) != 0
            || state.active_reclaims.contains(&root)
        {
            return false;
        }
        state.active_reclaims.insert(root.clone());
        state.reclaim_fences.insert(root);
        true
    }

    pub(crate) fn finish_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        keep_fence: bool,
    ) {
        let mut state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let root = (bucket.clone(), key.clone(), generation_id);
        state.active_reclaims.remove(&root);
        if !keep_fence {
            state.reclaim_fences.remove(&root);
        }
    }

    pub(crate) fn clear_object_payload_reclaim_fence(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        self.object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reclaim_fences
            .remove(&(bucket.clone(), key.clone(), generation_id));
    }

    pub(crate) fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        let state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state
            .leases
            .get(&(bucket.clone(), key.clone(), generation_id))
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        let state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state
            .leases
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
        validate_metadata_command_replay_state(&nodes, &pg_ids, ClusterEpoch::INITIAL)?;

        let metadata_primary = nodes
            .get(&metadata_primary_node_id)
            .expect("validated metadata primary should have been opened");
        let metadata_command_indexes =
            seed_metadata_command_indexes(&nodes, &pg_ids, ClusterEpoch::INITIAL)?;

        Ok(Self {
            epoch: ClusterEpoch::INITIAL,
            metadata_primary_node_id,
            pg_routes,
            placement_map,
            runtime_state: Arc::new(LocalClusterRuntimeState::with_metadata_command_indexes(
                metadata_command_indexes,
            )),
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

    pub(crate) fn validate_metadata_command_for_replica(
        &self,
        origin_node_id: NodeId,
        target_node_id: NodeId,
        target_pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let command_pg_id = command.id().pg_id();
        if command_pg_id != target_pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id: target_node_id.as_u32(),
                command_pg_id: command_pg_id.get(),
                target_pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
            });
        }

        let command_epoch = command.id().cluster_epoch();
        if command_epoch != self.epoch {
            return Err(StoreError::StaleMetadataCommand {
                node_id: target_node_id.as_u32(),
                pg_id: target_pg_id.get(),
                command_epoch,
                current_epoch: self.epoch,
            });
        }

        let route = self
            .pg_routes
            .get(&target_pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
            })?;
        if route.cluster_epoch() != self.epoch {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: target_pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.epoch,
            });
        }
        if !route.is_active() {
            return Err(StoreError::PgNotActive {
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
                state: route.state(),
            });
        }
        let primary_node_id = route.primary_node_id();
        if !route.contains_node(primary_node_id) {
            return Err(StoreError::NodeNotInActingSet {
                node_id: primary_node_id.as_u32(),
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
            });
        }
        if origin_node_id != primary_node_id {
            return Err(StoreError::MetadataCommandFromNonPrimary {
                node_id: target_node_id.as_u32(),
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
                origin_node_id: origin_node_id.as_u32(),
                primary_node_id: primary_node_id.as_u32(),
            });
        }
        if !route.contains_node(target_node_id) {
            return Err(StoreError::NodeNotInActingSet {
                node_id: target_node_id.as_u32(),
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
            });
        }
        let target_node = self
            .nodes
            .get(&target_node_id)
            .ok_or(StoreError::NodeNotFound {
                node_id: target_node_id.as_u32(),
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
            })?;

        let target_pg = target_node.storage_node().get_pg(target_pg_id.get())?;
        target_pg.metadata_command_acceptance(target_node_id.as_u32(), command)
    }

    pub(crate) fn validate_metadata_command_abandon_for_replica(
        &self,
        origin_node_id: NodeId,
        target_node_id: NodeId,
        target_pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        let command_pg_id = command.id().pg_id();
        if command_pg_id != target_pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id: target_node_id.as_u32(),
                command_pg_id: command_pg_id.get(),
                target_pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
            });
        }

        let command_epoch = command.id().cluster_epoch();
        if command_epoch != self.epoch {
            return Err(StoreError::StaleMetadataCommand {
                node_id: target_node_id.as_u32(),
                pg_id: target_pg_id.get(),
                command_epoch,
                current_epoch: self.epoch,
            });
        }

        let route = self
            .pg_routes
            .get(&target_pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
            })?;
        if route.cluster_epoch() != self.epoch {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: target_pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.epoch,
            });
        }
        if !route.is_active() {
            return Err(StoreError::PgNotActive {
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
                state: route.state(),
            });
        }
        let primary_node_id = route.primary_node_id();
        if !route.contains_node(primary_node_id) {
            return Err(StoreError::NodeNotInActingSet {
                node_id: primary_node_id.as_u32(),
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
            });
        }
        if origin_node_id != primary_node_id {
            return Err(StoreError::MetadataCommandFromNonPrimary {
                node_id: target_node_id.as_u32(),
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
                origin_node_id: origin_node_id.as_u32(),
                primary_node_id: primary_node_id.as_u32(),
            });
        }
        if !route.contains_node(target_node_id) {
            return Err(StoreError::NodeNotInActingSet {
                node_id: target_node_id.as_u32(),
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
            });
        }
        let target_node = self
            .nodes
            .get(&target_node_id)
            .ok_or(StoreError::NodeNotFound {
                node_id: target_node_id.as_u32(),
                pg_id: target_pg_id.get(),
                cluster_epoch: self.epoch,
            })?;
        let target_pg = target_node.storage_node().get_pg(target_pg_id.get())?;
        target_pg.metadata_command_abandon_acceptance(target_node_id.as_u32(), command)
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

fn seed_metadata_command_indexes(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_ids: &[PgId],
    cluster_epoch: ClusterEpoch,
) -> Result<HashMap<PgId, u64>, ClusterBuildError> {
    let mut indexes = HashMap::new();
    for &pg_id in pg_ids {
        let mut max_index = 0_u64;
        for node in nodes.values() {
            let pg = node.storage_node().get_pg(pg_id.get()).map_err(|source| {
                ClusterBuildError::OpenLocalNode {
                    node_id: node.node_id().as_u32(),
                    source,
                }
            })?;
            max_index = max_index.max(pg.max_metadata_command_log_index(cluster_epoch).map_err(
                |source| ClusterBuildError::OpenLocalNode {
                    node_id: node.node_id().as_u32(),
                    source,
                },
            )?);
        }
        if max_index != 0 {
            indexes.insert(pg_id, max_index);
        }
    }
    Ok(indexes)
}

fn validate_metadata_command_replay_state(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_ids: &[PgId],
    cluster_epoch: ClusterEpoch,
) -> Result<(), ClusterBuildError> {
    for &pg_id in pg_ids {
        let mut reference: Option<(NodeId, MetadataCommandReplicaState)> = None;
        for node in nodes.values() {
            let node_id = node.node_id();
            let pg = node.storage_node().get_pg(pg_id.get()).map_err(|source| {
                ClusterBuildError::OpenLocalNode {
                    node_id: node_id.as_u32(),
                    source,
                }
            })?;
            let state = pg
                .validate_metadata_command_replay_state(node_id.as_u32(), cluster_epoch)
                .map_err(|source| ClusterBuildError::OpenLocalNode {
                    node_id: node_id.as_u32(),
                    source,
                })?;
            if let Some((reference_node_id, reference_state)) = reference {
                if state != reference_state {
                    return Err(ClusterBuildError::OpenLocalNode {
                        node_id: node_id.as_u32(),
                        source: StoreError::MetadataCommandReplicaStateDiverged {
                            node_id: node_id.as_u32(),
                            reference_node_id: reference_node_id.as_u32(),
                            pg_id: pg_id.get(),
                            cluster_epoch: state.cluster_epoch,
                            reference_cluster_epoch: reference_state.cluster_epoch,
                            applied_log_index: state.applied_log_index,
                            reference_applied_log_index: reference_state.applied_log_index,
                            applied_log_hash: state.applied_log_hash,
                            reference_applied_log_hash: reference_state.applied_log_hash,
                            state_digest: state.state_digest,
                            reference_state_digest: reference_state.state_digest,
                        },
                    });
                }
            } else {
                reference = Some((node_id, state));
            }
        }
    }
    Ok(())
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
        metadata_command_log_hash, AdvanceCompletedMultipartUploadSequenceCommand,
        BucketPropertyMutation, BucketSubresourceMutation, CreateBucketCommand,
        MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex,
        MetadataCommandPayload, PutBucketAclCommand, PutObjectMetadataCommand,
    };
    use proptest::prelude::*;
    use proptest::test_runner::{TestCaseError, TestCaseResult};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, OnceLock};
    use std::time::Duration;

    static METADATA_COMMAND_APPLY_HOOK_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
    static PAYLOAD_CLEANUP_HOOK_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

    fn lock_metadata_command_apply_hook_test() -> std::sync::MutexGuard<'static, ()> {
        METADATA_COMMAND_APPLY_HOOK_TEST_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn lock_payload_cleanup_hook_test() -> std::sync::MutexGuard<'static, ()> {
        PAYLOAD_CLEANUP_HOOK_TEST_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    struct CommittedDirectSegment {
        version_id: crate::VersionId,
        generation_id: crate::GenerationId,
        segment_okh: [u8; 16],
        payload: Vec<u8>,
        written: crate::DirectPutWrittenSegment,
        locations: Vec<ShardLocation>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct ReplayStateSnapshot {
        state: crate::metadata_command::MetadataCommandReplicaState,
        max_log_index: u64,
    }

    fn collect_metadata_replay_snapshot(
        map: &LocalClusterMap,
        node_ids: &[NodeId],
        pg_ids: &[u32],
    ) -> std::collections::BTreeMap<(u32, u32), ReplayStateSnapshot> {
        let mut snapshot = std::collections::BTreeMap::new();
        for &node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            for &pg_id in pg_ids {
                let pg = node.get_pg(pg_id).unwrap();
                snapshot.insert(
                    (node_id.as_u32(), pg_id),
                    ReplayStateSnapshot {
                        state: pg.metadata_command_replica_state().unwrap(),
                        max_log_index: pg
                            .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                            .unwrap(),
                    },
                );
            }
        }
        snapshot
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
        write_committed_direct_segment_for_with_versioning(
            cluster,
            bucket,
            key,
            crate::BucketVersioningState::Disabled,
            [1; 16],
            segment_okh,
            payload,
        )
    }

    fn write_committed_direct_segment_for_with_versioning(
        cluster: &crate::StorageCluster,
        bucket: &crate::BucketName,
        key: &crate::ObjectKey,
        versioning: crate::BucketVersioningState,
        reservation_bytes: [u8; 16],
        segment_okh: [u8; 16],
        payload: &[u8],
    ) -> CommittedDirectSegment {
        let reservation_id = crate::SessionId::try_from(
            reservation_bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        )
        .unwrap();
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
            versioning,
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
        let outcome = cluster
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
            version_id: outcome.version_id,
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

    fn create_bucket_metadata_command(
        pg_id: PgId,
        log_index: u64,
        bucket: crate::BucketName,
    ) -> MetadataCommandEnvelope {
        let owner = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
        };
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                MetadataCommandLogIndex::new(log_index).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config(&config, 1_234, log_index).unwrap(),
            ),
        )
    }

    fn create_test_bucket_with_versioning(
        cluster: &crate::StorageCluster,
        bucket: &crate::BucketName,
        versioning: crate::BucketVersioningState,
    ) {
        create_test_bucket(cluster, bucket);
        if versioning != crate::BucketVersioningState::Disabled {
            cluster
                .put_bucket_versioning_and_load_info(bucket, versioning)
                .unwrap();
        }
    }

    fn put_test_lifecycle(cluster: &crate::StorageCluster, bucket: &crate::BucketName) {
        cluster
            .put_bucket_subresource_and_load_info(
                bucket,
                crate::PutBucketSubresource {
                    kind: crate::BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration/>",
                    aux: crate::BucketSubresourceAux::None,
                },
            )
            .unwrap();
    }

    fn direct_put_commit_req(
        bucket: &crate::BucketName,
        key: &crate::ObjectKey,
        reservation_id: crate::SessionId,
        generation_id: crate::GenerationId,
        payload: &[u8],
        segment_okh: [u8; 16],
        written: &crate::DirectPutWrittenSegment,
    ) -> crate::CommitDirectPutObjectReq {
        crate::CommitDirectPutObjectReq {
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
        }
    }

    fn assert_direct_put_metadata_on_acting_nodes(
        map: &LocalClusterMap,
        node_ids: &[NodeId],
        object_pg: u32,
        commit_req: &crate::CommitDirectPutObjectReq,
        outcome: &crate::FinalizeDirectPutObjectOutcome,
    ) {
        let expected_segment = crate::ObjectSegmentRecord {
            bucket: commit_req.bucket.clone(),
            key: commit_req.key.clone(),
            version_id: outcome.version_id,
            segment_index: commit_req.segment_index,
            size: commit_req.size,
            segment_crc64: commit_req.segment_crc64,
            segment_okh: commit_req.segment_okh,
            segment_vid: commit_req.segment_vid,
            data_pg_id: commit_req.data_pg_id,
            ec_k: commit_req.ec.k,
            ec_m: commit_req.ec.m,
        };

        for node_id in node_ids {
            let node = map.node(*node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let stored =
                crate::PgMetadataStore::get_object_meta(&*pg, &commit_req.bucket, &commit_req.key)
                    .unwrap();
            let live = stored.as_live().unwrap();
            assert_eq!(live.version_id, outcome.version_id);
            assert_eq!(live.generation_id, commit_req.generation_id);
            assert_eq!(live.size, commit_req.size);
            assert_eq!(
                live.etag,
                crate::ObjectEtag::single_part(commit_req.etag_crc64)
            );
            assert_eq!(live.last_modified, outcome.live_last_modified);
            assert_eq!(live.metadata_blob, Some(commit_req.metadata_blob.clone()));
            assert_eq!(
                live.system_metadata_blob,
                Some(commit_req.system_metadata_blob.clone())
            );
            assert_eq!(live.object_lock, commit_req.object_lock);
            assert_eq!(live.encryption, commit_req.encryption);
            assert_eq!(
                crate::PgMetadataStore::get_object_segments(
                    &*pg,
                    &commit_req.bucket,
                    &commit_req.key,
                    outcome.version_id,
                )
                .unwrap(),
                vec![expected_segment.clone()]
            );
            assert!(matches!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &commit_req.bucket,
                    &commit_req.key,
                    &commit_req.generation_reservation_id,
                ),
                Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
            ));
        }
    }

    fn assert_object_version_counter_on_acting_nodes(
        map: &LocalClusterMap,
        node_ids: &[NodeId],
        object_pg: u32,
        bucket: &crate::BucketName,
        key: &crate::ObjectKey,
        expected_next_version_id: u64,
    ) {
        for node_id in node_ids {
            let node = map.node(*node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let next_version_id: i64 = pg
                .connection()
                .query_row(
                    "SELECT COALESCE(MAX(next_version_id), 0) \
                     FROM object_version_counters WHERE bucket = ?1 AND key = ?2",
                    rusqlite::params![bucket.as_str(), key.as_str()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                next_version_id as u64, expected_next_version_id,
                "unexpected object version counter on node {node_id:?}"
            );
        }
    }

    fn assert_bucket_execution_counter_on_acting_nodes(
        map: &LocalClusterMap,
        node_ids: &[NodeId],
        bucket_pg: u32,
        expected_current_generation: u64,
    ) {
        for node_id in node_ids {
            let node = map.node(*node_id).unwrap().storage_node();
            let pg = node.get_pg(bucket_pg).unwrap();
            let next_generation: i64 = pg
                .connection()
                .query_row(
                    "SELECT next_bucket_execution_generation \
                     FROM pg_counters WHERE singleton = 0",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                next_generation as u64, expected_current_generation,
                "unexpected bucket execution counter on node {node_id:?}"
            );
        }
    }

    fn seed_streamed_multipart_completion(
        cluster: &crate::StorageCluster,
        bucket: &crate::BucketName,
        key: &crate::ObjectKey,
        upload_label: &str,
    ) -> (
        crate::CompleteMultipartCommitRequest,
        crate::MultipartPartSegmentRecord,
    ) {
        seed_streamed_multipart_completion_with_existing(cluster, bucket, key, upload_label, false)
    }

    fn seed_streamed_multipart_completion_with_existing(
        cluster: &crate::StorageCluster,
        bucket: &crate::BucketName,
        key: &crate::ObjectKey,
        upload_label: &str,
        expect_existing_object: bool,
    ) -> (
        crate::CompleteMultipartCommitRequest,
        crate::MultipartPartSegmentRecord,
    ) {
        let upload_id = upload_id_from_label(upload_label);
        cluster
            .create_multipart_upload(
                bucket,
                key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert_eq!(existing_object.is_some(), expect_existing_object);
                    Ok::<_, ()>((
                        (),
                        crate::CreateMultipartUploadReq {
                            upload_id: upload_id.clone(),
                            bucket: bucket.clone(),
                            key: key.clone(),
                            tags: None,
                            metadata_blob: crate::SerializedMetadataBlob::default(),
                            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
                            owner: crate::OwnerIdentity::from_principal("owner"),
                            acl_grants: crate::AclGrants::default(),
                            public_read: false,
                            object_lock: crate::ObjectLockState::default(),
                            checksum: None,
                            encryption: crate::ObjectEncryption::None,
                        },
                    ))
                },
            )
            .unwrap()
            .unwrap();
        let upload = cluster
            .load_in_progress_multipart_upload(bucket, key, &upload_id)
            .unwrap();
        let (_shard_keys, part, mut expected_segment) = upload_streamed_test_multipart_part(
            cluster,
            bucket,
            key,
            &upload_id,
            1,
            [0xCD; 16],
            b"streamed completion",
        );

        expected_segment.version_id = crate::VersionId::Null.to_u64();
        (
            crate::CompleteMultipartCommitRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id,
                versioning: crate::BucketVersioningState::Disabled,
                owner: upload.owner,
                acl_grants: upload.acl_grants,
                public_read: upload.public_read,
                generation_id: upload.object_generation_id,
                size: part.size,
                etag_crc64: [0x44; 8],
                tags: upload.tags,
                metadata_blob: Some(upload.metadata_blob),
                system_metadata_blob: Some(upload.system_metadata_blob),
                object_lock: upload.object_lock,
                encryption: upload.encryption,
                part_records: vec![part],
            },
            expected_segment,
        )
    }

    fn assert_streamed_multipart_completion_on_acting_nodes(
        map: &LocalClusterMap,
        node_ids: &[NodeId],
        object_pg: u32,
        req: &crate::CompleteMultipartCommitRequest,
        expected_segment: &crate::MultipartPartSegmentRecord,
        outcome: &crate::CompleteMultipartCommitOutcome,
    ) {
        assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
            map,
            node_ids,
            object_pg,
            req,
            expected_segment,
            outcome,
            1,
        );
    }

    fn assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        map: &LocalClusterMap,
        node_ids: &[NodeId],
        object_pg: u32,
        req: &crate::CompleteMultipartCommitRequest,
        expected_segment: &crate::MultipartPartSegmentRecord,
        outcome: &crate::CompleteMultipartCommitOutcome,
        expected_write_sequence: u64,
    ) {
        let mut expected_completion_order = None;
        for node_id in node_ids {
            let node = map.node(*node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &req.bucket, &req.key)
                .unwrap()
                .as_live()
                .unwrap()
                .clone();
            assert_eq!(stored.version_id, outcome.version_id);
            assert_eq!(stored.generation_id, req.generation_id);
            assert_eq!(stored.size, req.size);
            assert_eq!(stored.last_modified, outcome.live_last_modified);
            assert_eq!(stored.layout.parts_count(), Some(1));
            assert_eq!(
                pg.object_write_sequence(req.bucket.as_str(), req.key.as_str(), outcome.version_id)
                    .unwrap(),
                Some(expected_write_sequence)
            );

            let parts = crate::PgMetadataStore::get_object_parts(
                &*pg,
                &req.bucket,
                &req.key,
                outcome.version_id,
            )
            .unwrap();
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].part_okh, [0u8; 16]);
            assert_eq!(parts[0].part_vid, req.part_records[0].part_vid);

            let segments = crate::PgMetadataStore::get_multipart_part_segments(
                &*pg,
                &req.bucket,
                &req.key,
                outcome.version_id,
                1,
            )
            .unwrap();
            assert_eq!(segments, vec![expected_segment.clone()]);
            assert!(matches!(
                crate::PgMetadataStore::get_multipart_upload(&*pg, &req.upload_id),
                Err(crate::MetadataError::NoSuchUpload { .. })
            ));
            let completed_uploads = pg
                .list_completed_multipart_uploads_for_bucket(req.bucket.as_str())
                .unwrap();
            assert_eq!(completed_uploads.len(), 1);
            assert_eq!(completed_uploads[0].0, req.upload_id);
            if let Some(expected) = expected_completion_order {
                assert_eq!(completed_uploads[0].1, expected);
            } else {
                expected_completion_order = Some(completed_uploads[0].1);
            }
            let bucket_pg = node
                .get_pg(node.pg_topology().bucket_pg_for(&req.bucket))
                .unwrap();
            assert_eq!(
                bucket_pg
                    .completed_multipart_upload_sequence_for_bucket(&req.bucket)
                    .unwrap(),
                completed_uploads[0].1
            );
        }
    }

    fn completed_multipart_order_on_node(
        map: &LocalClusterMap,
        node_id: NodeId,
        object_pg: u32,
        bucket: &crate::BucketName,
        upload_id: &crate::UploadId,
    ) -> u64 {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        pg.list_completed_multipart_uploads_for_bucket(bucket.as_str())
            .unwrap()
            .into_iter()
            .find_map(|(stored_upload_id, completion_order)| {
                (stored_upload_id == *upload_id).then_some(completion_order)
            })
            .unwrap_or_else(|| panic!("completed upload {upload_id:?} not found on PG {object_pg}"))
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
            pg.refresh_metadata_command_state_digest().unwrap();
        }
    }

    fn upload_streamed_test_multipart_part(
        cluster: &crate::StorageCluster,
        bucket: &crate::BucketName,
        key: &crate::ObjectKey,
        upload_id: &crate::UploadId,
        part_number: u32,
        segment_okh: [u8; 16],
        payload: &[u8],
    ) -> (
        Vec<ShardKey>,
        crate::MultipartPartRecord,
        crate::MultipartPartSegmentRecord,
    ) {
        let session_seed = segment_okh[0];
        let session_id =
            crate::SessionId::try_from(format!("{session_seed:02x}").repeat(16)).unwrap();
        let upload = cluster
            .load_in_progress_multipart_upload(bucket, key, upload_id)
            .unwrap();
        cluster
            .create_upload_part_stream_session(
                &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
                part_number,
                &session_id,
            )
            .unwrap();

        let (_target, segment) = cluster
            .prepare_stream_segment_append(
                bucket,
                key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: payload.len() as u64,
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    segment_okh,
                },
            )
            .unwrap();
        let written_shards = cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        cluster
            .commit_stream_segment_append(
                bucket,
                key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();

        let part = cluster
            .finalize_upload_part_stream(
                bucket,
                key,
                upload_id,
                &session_id,
                part_number,
                |snapshot| {
                    let generation = snapshot
                        .existing_part_generation
                        .map_or(0, |generation| generation + 1);
                    let part = crate::MultipartPartRecord {
                        upload_id: upload_id.clone(),
                        part_number,
                        generation,
                        size: payload.len() as u64,
                        etag: vec![session_seed; 8],
                        etag_kind: crate::EtagKind::Crc64,
                        part_okh: [0u8; 16],
                        part_vid: crate::GenerationId::new(u64::from(generation) + 1).unwrap(),
                        ec_k: segment.ec_k,
                        ec_m: segment.ec_m,
                        last_modified: 123,
                        checksum: None,
                    };
                    let segments = snapshot
                        .staging_segments
                        .iter()
                        .map(|staged| crate::MultipartPartSegmentRecord {
                            bucket: bucket.clone(),
                            key: key.clone(),
                            upload_id: upload_id.clone(),
                            version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
                            part_number,
                            segment_index: staged.segment_index,
                            size: staged.size,
                            segment_crc64: staged.segment_crc64,
                            segment_okh: staged.segment_okh,
                            segment_vid: staged.segment_vid,
                            data_pg_id: staged.data_pg_id,
                            ec_k: staged.ec_k,
                            ec_m: staged.ec_m,
                        })
                        .collect::<Vec<_>>();
                    Ok::<_, ()>(crate::PreparedStreamPartCommit {
                        value: part.clone(),
                        part,
                        segments,
                    })
                },
            )
            .unwrap()
            .unwrap()
            .value;

        let uploaded_segment = crate::MultipartPartSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
            part_number,
            segment_index: segment.segment_index,
            size: segment.size,
            segment_crc64: segment.segment_crc64,
            segment_okh: segment.segment_okh,
            segment_vid: segment.segment_vid,
            data_pg_id: segment.data_pg_id,
            ec_k: segment.ec_k,
            ec_m: segment.ec_m,
        };
        (
            written_shards
                .into_iter()
                .map(|written| written.key)
                .collect(),
            part,
            uploaded_segment,
        )
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
        pg.connection()
            .execute(
                "UPDATE completed_multipart_uploads SET completed_at = ?1 WHERE upload_id = ?2",
                rusqlite::params![completion_order as i64, upload_id.as_str()],
            )
            .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
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
            Just(PgState::Inconsistent),
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
    fn metadata_routes_reject_all_non_active_pg_states() {
        let non_active_states = [
            PgState::Peering,
            PgState::Degraded,
            PgState::Backfilling,
            PgState::Inconsistent,
        ];

        for state in non_active_states {
            let tmp = test_util::tempdir();
            let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
            let ec_shape = EcShape { k: 2, m: 1 };
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "non-active-route-");
            let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
            map.pg_routes.get_mut(&PgId::new(1)).unwrap().state = state;

            let err = map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
                .unwrap_err();
            assert!(matches!(
                err,
                StoreError::PgNotActive {
                    pg_id: 1,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    state: err_state,
                } if err_state == state
            ));

            let err = map
                .metadata_pg_acting_nodes(ClusterEpoch::INITIAL, PgId::new(1))
                .unwrap_err();
            assert!(matches!(
                err,
                StoreError::PgNotActive {
                    pg_id: 1,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    state: err_state,
                } if err_state == state
            ));

            let err = map
                .validate_metadata_command_for_replica(
                    NodeId::new(0),
                    NodeId::new(1),
                    PgId::new(1),
                    &command,
                )
                .unwrap_err();
            assert!(matches!(
                err,
                StoreError::PgNotActive {
                    pg_id: 1,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    state: err_state,
                } if err_state == state
            ));

            let err = map
                .validate_metadata_command_abandon_for_replica(
                    NodeId::new(0),
                    NodeId::new(1),
                    PgId::new(1),
                    &command,
                )
                .unwrap_err();
            assert!(matches!(
                err,
                StoreError::PgNotActive {
                    pg_id: 1,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    state: err_state,
                } if err_state == state
            ));
        }
    }

    #[test]
    fn metadata_write_fails_closed_when_required_replica_is_missing() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "strict-metadata-replica-");
        {
            let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
            route.primary_node_id = NodeId::new(1);
            route.acting_set = Arc::from([NodeId::new(0), NodeId::new(99), NodeId::new(1)]);
        }

        let mut map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let owner = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let config = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
        };

        let err = cluster
            .create_bucket_with_config_and_load_info(&config)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::NodeNotFound {
                node_id: 99,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
            })
        ));
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(1), &bucket)
                .is_some(),
            "failed strict write should keep the command pending for replica recovery"
        );
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::head_bucket(&*pg, &bucket),
                Err(crate::MetadataError::BucketNotFound { .. })
            ));
        }

        drop(cluster);
        {
            let route = Arc::get_mut(&mut map)
                .unwrap()
                .pg_routes
                .get_mut(&PgId::new(1))
                .unwrap();
            route.acting_set = Arc::from(node_ids);
        }
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        cluster
            .create_bucket_with_config_and_load_info(&config)
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(1), &bucket)
            .is_none());
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
            assert_eq!(info.name, bucket);
        }
    }

    #[test]
    fn metadata_command_replica_acceptance_rejects_invalid_route_context() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "replica-acceptance-");
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
        {
            let route = map.pg_routes.get_mut(&PgId::new(1)).unwrap();
            route.primary_node_id = NodeId::new(1);
        }

        let err = map
            .validate_metadata_command_for_replica(
                NodeId::new(0),
                NodeId::new(2),
                PgId::new(1),
                &command,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::MetadataCommandFromNonPrimary {
                node_id: 2,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                origin_node_id: 0,
                primary_node_id: 1,
            }
        ));

        let stale_command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(2).unwrap(),
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            command.payload().clone(),
        );
        let err = map
            .validate_metadata_command_for_replica(
                NodeId::new(1),
                NodeId::new(2),
                PgId::new(1),
                &stale_command,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::StaleMetadataCommand {
                node_id: 2,
                pg_id: 1,
                command_epoch,
                current_epoch: ClusterEpoch::INITIAL,
            } if command_epoch == ClusterEpoch::new(2).unwrap()
        ));

        let err = map
            .validate_metadata_command_for_replica(
                NodeId::new(1),
                NodeId::new(2),
                PgId::new(0),
                &command,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::MetadataCommandWrongPg {
                node_id: 2,
                command_pg_id: 1,
                target_pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
            }
        ));

        let err = map
            .validate_metadata_command_for_replica(
                NodeId::new(1),
                NodeId::new(99),
                PgId::new(1),
                &command,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::NodeNotInActingSet {
                node_id: 99,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
            }
        ));
    }

    #[test]
    fn metadata_command_apply_validates_origin_and_duplicate_conflict_without_mutation() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let first_bucket = bucket_for_pg(topology, 1, "accepted-command-");
        let conflict_bucket = bucket_for_pg(topology, 1, "conflicting-command-");
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let first_command = create_bucket_metadata_command(PgId::new(1), 1, first_bucket.clone());

        let err = cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &first_command)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandFromNonPrimary {
                node_id: 0,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                origin_node_id: 0,
                primary_node_id: 1,
            })
        ));
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::head_bucket(&*pg, &first_bucket),
                Err(crate::MetadataError::BucketNotFound { .. })
            ));
        }

        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first_command)
            .unwrap();
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first_command)
            .unwrap();
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
            assert_eq!(info.name, first_bucket);
        }

        let conflicting_command =
            create_bucket_metadata_command(PgId::new(1), 1, conflict_bucket.clone());
        let err = cluster
            .test_apply_metadata_command_to_acting_set_from_origin(
                NodeId::new(1),
                &conflicting_command,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
                node_id: 0,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 1,
            })
        ));
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::head_bucket(&*pg, &conflict_bucket),
                Err(crate::MetadataError::BucketNotFound { .. })
            ));
        }
    }

    #[test]
    fn metadata_command_apply_accepts_unseen_lower_index_after_higher_index() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let earlier_bucket = bucket_for_pg(topology, 1, "earlier-command-");
        let later_bucket = bucket_for_pg(topology, 1, "later-command-");
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let earlier_command =
            create_bucket_metadata_command(PgId::new(1), 1, earlier_bucket.clone());
        let later_command = create_bucket_metadata_command(PgId::new(1), 2, later_bucket.clone());

        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &later_command)
            .unwrap();
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, 0);
            assert_eq!(state.applied_log_hash, 0);
            let later = crate::PgMetadataStore::head_bucket(&*pg, &later_bucket).unwrap();
            assert_eq!(later.name, later_bucket);
        }

        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &earlier_command)
            .unwrap();

        let first_hash = metadata_command_log_hash(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
            0,
            earlier_command.checksum_crc64(),
        );
        let second_hash = metadata_command_log_hash(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(2).unwrap(),
            first_hash,
            later_command.checksum_crc64(),
        );
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, 2);
            assert_eq!(state.applied_log_hash, second_hash);
            let earlier = crate::PgMetadataStore::head_bucket(&*pg, &earlier_bucket).unwrap();
            assert_eq!(earlier.name, earlier_bucket);
            let later = crate::PgMetadataStore::head_bucket(&*pg, &later_bucket).unwrap();
            assert_eq!(later.name, later_bucket);
        }
    }

    #[test]
    fn metadata_command_log_state_survives_local_cluster_reopen() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let bucket = {
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "durable-command-");
            set_route_primary(&mut map, 1, NodeId::new(1));
            let map = Arc::new(map);
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
                .unwrap();
            bucket
        };

        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap();

        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, 1);
            let info = crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
            assert_eq!(info.name, bucket);
        }
    }

    #[test]
    fn local_cluster_reopen_preserves_mixed_storage_cluster_history() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let pg_ids = [0, 1, 2, 3];
        let ec_shape = EcShape { k: 2, m: 1 };
        let (bucket, key, object_pg, committed, before_reopen) = {
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
            let (bucket, key, object_pg, data_pg) = {
                let topology = map
                    .node(NodeId::new(0))
                    .unwrap()
                    .storage_node()
                    .pg_topology();
                let bucket = bucket_for_pg(topology, 1, "replay-harness-");
                let key = key_for_object_pg(topology, &bucket, 2, "replay-object-");
                let data_pg = topology
                    .object_generation_segment_data_pg(&bucket, &key, crate::GenerationId::MIN, 0)
                    .get();
                let object_pg = topology.object_pg_for(&bucket, &key);
                (bucket, key, object_pg, data_pg)
            };
            assert_eq!(object_pg, 2);
            set_route_primary(&mut map, 1, NodeId::new(1));
            set_route_primary(&mut map, object_pg, NodeId::new(2));
            set_route_primary(&mut map, data_pg, NodeId::new(0));

            let map = Arc::new(map);
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            create_test_bucket_with_versioning(
                &cluster,
                &bucket,
                crate::BucketVersioningState::Enabled,
            );
            put_test_lifecycle(&cluster, &bucket);
            let committed = write_committed_direct_segment_for_with_versioning(
                &cluster,
                &bucket,
                &key,
                crate::BucketVersioningState::Enabled,
                [91; 16],
                [92; 16],
                b"phase 7.4 replay harness object",
            );
            let tags =
                "<Tagging><TagSet><Tag><Key>phase</Key><Value>7.4</Value></Tag></TagSet></Tagging>";
            let tagged_version = cluster
                .put_object_tags_if(&bucket, &key, None, tags, |stored| {
                    Ok::<_, ()>(stored.version_id())
                })
                .unwrap()
                .unwrap();
            assert_eq!(tagged_version, committed.version_id);
            let before_reopen = collect_metadata_replay_snapshot(&map, &node_ids, &pg_ids);
            drop(cluster);
            drop(map);
            (bucket, key, object_pg, committed, before_reopen)
        };

        let mut reopened = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
        set_route_primary(&mut reopened, 1, NodeId::new(1));
        set_route_primary(&mut reopened, object_pg, NodeId::new(2));
        let after_reopen = collect_metadata_replay_snapshot(&reopened, &node_ids, &pg_ids);
        assert_eq!(after_reopen, before_reopen);

        for node_id in node_ids {
            let pg = reopened
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            let stored = crate::PgMetadataStore::get_object_version(
                &*pg,
                &bucket,
                &key,
                committed.version_id,
            )
            .unwrap();
            let live = stored.as_live().unwrap();
            assert_eq!(
                live.tags.as_ref().map(|tags| tags.as_str()),
                Some(
                    "<Tagging><TagSet><Tag><Key>phase</Key><Value>7.4</Value></Tag></TagSet></Tagging>"
                )
            );
            assert_eq!(live.size, committed.payload.len() as u64);
        }
    }

    #[test]
    fn local_cluster_reopen_rejects_missing_applied_command_log_entry() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        {
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "missing-log-");
            set_route_primary(&mut map, 1, NodeId::new(1));
            let map = Arc::new(map);
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
                .unwrap();
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            node_zero_pg
                .connection()
                .execute("DELETE FROM metadata_command_log WHERE log_index = 1", [])
                .unwrap();
        }

        let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
        assert!(matches!(
            err,
            ClusterBuildError::OpenLocalNode {
                node_id: 0,
                source: StoreError::MetadataCommandLogConflict {
                    pg_id: 1,
                    log_index: 1,
                    ..
                }
            }
        ));
    }

    #[test]
    fn local_cluster_reopen_rejects_reordered_applied_command_log_entry() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        {
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let first_bucket = bucket_for_pg(topology, 1, "reordered-log-first-");
            let second_bucket = bucket_for_pg(topology, 1, "reordered-log-second-");
            set_route_primary(&mut map, 1, NodeId::new(1));
            let map = Arc::new(map);
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            let first_command = create_bucket_metadata_command(PgId::new(1), 1, first_bucket);
            let second_command = create_bucket_metadata_command(PgId::new(1), 2, second_bucket);
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(
                    NodeId::new(1),
                    &first_command,
                )
                .unwrap();
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(
                    NodeId::new(1),
                    &second_command,
                )
                .unwrap();
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            node_zero_pg
                .connection()
                .execute(
                    "UPDATE metadata_command_log \
                     SET command_checksum = ?1, command_bytes = ?2 \
                     WHERE log_index = 1",
                    rusqlite::params![
                        second_command.checksum_crc64() as i64,
                        second_command.command_bytes(),
                    ],
                )
                .unwrap();
        }

        let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
        assert!(matches!(
            err,
            ClusterBuildError::OpenLocalNode {
                node_id: 0,
                source: StoreError::MetadataCommandLogConflict {
                    pg_id: 1,
                    log_index: 1,
                    ..
                }
            }
        ));
    }

    #[test]
    fn local_cluster_reopen_rejects_corrupt_applied_command_log_hash() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        {
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "corrupt-log-hash-");
            set_route_primary(&mut map, 1, NodeId::new(1));
            let map = Arc::new(map);
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
                .unwrap();
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            node_zero_pg
                .connection()
                .execute(
                    "UPDATE metadata_command_log SET previous_log_hash = ?1 WHERE log_index = 1",
                    rusqlite::params![123_i64],
                )
                .unwrap();
        }

        let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
        assert!(matches!(
            err,
            ClusterBuildError::OpenLocalNode {
                node_id: 0,
                source: StoreError::MetadataCommandLogHashMismatch {
                    pg_id: 1,
                    log_index: 1,
                    ..
                }
            }
        ));
    }

    #[test]
    fn local_cluster_reopen_rejects_materialized_state_digest_mismatch() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        {
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "corrupt-state-");
            set_route_primary(&mut map, 1, NodeId::new(1));
            let map = Arc::new(map);
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
                .unwrap();
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            node_zero_pg
                .connection()
                .execute(
                    "UPDATE buckets SET public_read = 1 WHERE name = ?1",
                    rusqlite::params![bucket.as_str()],
                )
                .unwrap();
        }

        let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
        assert!(matches!(
            err,
            ClusterBuildError::OpenLocalNode {
                node_id: 0,
                source: StoreError::MetadataStateDigestMismatch { pg_id: 1, .. }
            }
        ));
    }

    #[test]
    fn local_cluster_reopen_rejects_replica_materialized_state_digest_mismatch() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        {
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "corrupt-replica-state-");
            set_route_primary(&mut map, 1, NodeId::new(1));
            let map = Arc::new(map);
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
                .unwrap();
            let replica_pg = map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            replica_pg
                .connection()
                .execute(
                    "UPDATE buckets SET public_read = 1 WHERE name = ?1",
                    rusqlite::params![bucket.as_str()],
                )
                .unwrap();
        }

        let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
        assert!(
            matches!(
                err,
                ClusterBuildError::OpenLocalNode {
                    node_id: 1,
                    source: StoreError::MetadataStateDigestMismatch { pg_id: 1, .. }
                }
            ),
            "unexpected reopen error: {err:?}"
        );
    }

    #[test]
    fn local_cluster_reopen_rejects_missing_replica_state_for_nonempty_pg() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        {
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "missing-replica-state-");
            set_route_primary(&mut map, 1, NodeId::new(1));
            let map = Arc::new(map);
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
                .unwrap();
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            node_zero_pg
                .connection()
                .execute(
                    "DELETE FROM metadata_command_replica_state WHERE singleton = 0",
                    [],
                )
                .unwrap();
        }

        let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
        assert!(matches!(
            err,
            ClusterBuildError::OpenLocalNode {
                node_id: 0,
                source: StoreError::MetadataCommandReplicaStateMissing { pg_id: 1 }
            }
        ));
    }

    #[test]
    fn local_cluster_reopen_rejects_replica_state_disagreement() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        {
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let first_bucket = bucket_for_pg(topology, 1, "replica-agree-first-");
            let second_bucket = bucket_for_pg(topology, 1, "replica-agree-second-");
            set_route_primary(&mut map, 1, NodeId::new(1));
            let map = Arc::new(map);
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            let first_command = create_bucket_metadata_command(PgId::new(1), 1, first_bucket);
            let second_command =
                create_bucket_metadata_command(PgId::new(1), 2, second_bucket.clone());
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(
                    NodeId::new(1),
                    &first_command,
                )
                .unwrap();
            let stale_state = {
                let node_zero_pg = map
                    .node(NodeId::new(0))
                    .unwrap()
                    .storage_node()
                    .get_pg(1)
                    .unwrap();
                (
                    node_zero_pg.metadata_command_replica_state().unwrap(),
                    node_zero_pg
                        .connection()
                        .query_row(
                            "SELECT next_bucket_execution_generation \
                             FROM pg_counters WHERE singleton = 0",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                )
            };
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(
                    NodeId::new(1),
                    &second_command,
                )
                .unwrap();
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            node_zero_pg
                .connection()
                .execute("DELETE FROM metadata_command_log WHERE log_index = 2", [])
                .unwrap();
            node_zero_pg
                .connection()
                .execute(
                    "DELETE FROM buckets WHERE name = ?1",
                    rusqlite::params![second_bucket.as_str()],
                )
                .unwrap();
            node_zero_pg
                .connection()
                .execute(
                    "UPDATE pg_counters \
                     SET next_bucket_execution_generation = ?1 \
                     WHERE singleton = 0",
                    rusqlite::params![stale_state.1],
                )
                .unwrap();
            node_zero_pg
                .connection()
                .execute(
                    "UPDATE metadata_command_replica_state \
                     SET cluster_epoch = ?1, applied_log_index = ?2, \
                         applied_log_hash = ?3, state_digest = ?4 \
                     WHERE singleton = 0",
                    rusqlite::params![
                        stale_state.0.cluster_epoch.get() as i64,
                        stale_state.0.applied_log_index as i64,
                        stale_state.0.applied_log_hash as i64,
                        stale_state.0.state_digest as i64,
                    ],
                )
                .unwrap();
        }

        let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
        assert!(
            matches!(
                err,
                ClusterBuildError::OpenLocalNode {
                    node_id: 1,
                    source: StoreError::MetadataCommandReplicaStateDiverged {
                        pg_id: 1,
                        reference_node_id: 0,
                        applied_log_index: 2,
                        reference_applied_log_index: 1,
                        ..
                    }
                }
            ),
            "unexpected reopen error: {err:?}"
        );
    }

    #[test]
    fn local_cluster_reopen_rejects_same_state_with_different_history() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        {
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "same-state-history-");
            let alternate_bucket = bucket_for_pg(topology, 1, "alternate-history-");
            set_route_primary(&mut map, 1, NodeId::new(1));
            let map = Arc::new(map);
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
                .unwrap();

            let alternate_command =
                create_bucket_metadata_command(PgId::new(1), 1, alternate_bucket);
            let alternate_hash = metadata_command_log_hash(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
                0,
                alternate_command.checksum_crc64(),
            );
            let replica_pg = map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            replica_pg
                .connection()
                .execute(
                    "UPDATE metadata_command_log \
                     SET command_checksum = ?1, command_bytes = ?2, \
                         previous_log_hash = ?3, log_hash = ?4 \
                     WHERE log_index = 1",
                    rusqlite::params![
                        alternate_command.checksum_crc64() as i64,
                        alternate_command.command_bytes(),
                        0_i64,
                        alternate_hash as i64,
                    ],
                )
                .unwrap();
            replica_pg
                .connection()
                .execute(
                    "UPDATE metadata_command_replica_state \
                     SET applied_log_hash = ?1 \
                     WHERE singleton = 0",
                    rusqlite::params![alternate_hash as i64],
                )
                .unwrap();
        }

        let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
        assert!(
            matches!(
                err,
                ClusterBuildError::OpenLocalNode {
                    node_id: 1,
                    source: StoreError::MetadataCommandReplicaStateDiverged {
                        pg_id: 1,
                        reference_node_id: 0,
                        applied_log_index: 1,
                        reference_applied_log_index: 1,
                        ..
                    }
                }
            ),
            "unexpected reopen error: {err:?}"
        );
    }

    #[test]
    fn metadata_command_log_index_allocator_seeds_from_reopened_log() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let (first_bucket, second_bucket) = {
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
            let topology = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let first_bucket = bucket_for_pg(topology, 1, "reopen-index-first-");
            let second_bucket = bucket_for_pg(topology, 1, "reopen-index-second-");
            set_route_primary(&mut map, 1, NodeId::new(1));
            let map = Arc::new(map);
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            create_test_bucket(&cluster, &first_bucket);
            (first_bucket, second_bucket)
        };

        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &second_bucket);

        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, 2);
            let first = crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
            assert_eq!(first.name, first_bucket);
            let second = crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
            assert_eq!(second.name, second_bucket);
        }
    }

    #[test]
    fn zero_apply_command_failure_records_tombstone_for_later_hash_chain_convergence() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let first_bucket = bucket_for_pg(topology, 1, "zero-apply-first-");
        let second_bucket = bucket_for_pg(topology, 1, "zero-apply-second-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let fail_once_hook = Arc::clone(&fail_once);
        let first_bucket_for_hook = first_bucket.clone();
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CreateBucket(create)
                        if create.bucket.name == first_bucket_for_hook
                            && node_id == NodeId::new(0)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected zero-apply create bucket failure",
                            source: std::io::Error::other(
                                "injected zero-apply create bucket failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let owner = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let err = cluster
            .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
                name: first_bucket.as_str(),
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: crate::BucketVersioningState::Disabled,
                object_lock: crate::BucketObjectLockConfig::default(),
            })
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "injected zero-apply create bucket failure",
                    ..
                })
            ),
            "expected injected zero-apply failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(1), &first_bucket)
            .is_none());
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, 1);
            assert!(matches!(
                crate::PgMetadataStore::head_bucket(&*pg, &first_bucket),
                Err(crate::MetadataError::BucketNotFound { .. })
            ));
        }

        create_test_bucket(&cluster, &second_bucket);
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, 2);
            let second = crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
            assert_eq!(second.name, second_bucket);
        }

        create_test_bucket(&cluster, &first_bucket);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(1), &first_bucket)
            .is_none());
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, 3);
            let first = crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
            assert_eq!(first.name, first_bucket);
            let second = crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
            assert_eq!(second.name, second_bucket);
        }
    }

    #[test]
    fn partial_tombstone_recording_retries_as_idempotent_abandon() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "partial-tombstone-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());

        {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            node_zero_pg
                .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
                .unwrap();
        }

        cluster
            .record_abandoned_metadata_command_to_acting_set(&command)
            .unwrap();
        cluster
            .record_abandoned_metadata_command_to_acting_set(&command)
            .unwrap();

        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, 1);
            assert!(matches!(
                crate::PgMetadataStore::head_bucket(&*pg, &bucket),
                Err(crate::MetadataError::BucketNotFound { .. })
            ));
        }
    }

    #[test]
    fn partial_abandoned_create_bucket_retry_rebuilds_command_before_reporting_created() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "partial-create-tombstone-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let pg_id = PgId::new(1);
        let abandoned_index = map
            .runtime_state()
            .next_metadata_command_log_index(pg_id)
            .get();
        let command = create_bucket_metadata_command(pg_id, abandoned_index, bucket.clone());
        map.runtime_state()
            .try_set_pending_metadata_command_for_bucket(pg_id, &bucket, command.clone())
            .unwrap();
        {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            node_zero_pg
                .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
                .unwrap();
        }

        let owner = crate::CanonicalUserId::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let outcome = cluster
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
        let crate::BucketCreateAttemptOutcome::Created(info) = outcome else {
            panic!("abandoned create retry must create a fresh bucket, got {outcome:?}");
        };
        assert_eq!(info.name, bucket);
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, abandoned_index + 1);
            let stored = crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
            assert_eq!(stored.name, bucket);
        }
    }

    #[test]
    fn partial_abandoned_reservation_retry_does_not_report_skipped_command_success() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let pg_id = PgId::new(object_pg);
        let reservation_id = crate::SessionId::try_from("58".repeat(16)).unwrap();
        let skipped_generation_id = crate::GenerationId::MIN;
        let command = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                cluster.operation_epoch(),
                pg_id,
                map.runtime_state().next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::ReserveObjectGeneration(
                crate::metadata_command::ReserveObjectGenerationCommand::new(
                    bucket.clone(),
                    key.clone(),
                    reservation_id.clone(),
                    skipped_generation_id,
                    123,
                ),
            ),
        );
        map.runtime_state()
            .try_set_pending_metadata_command_for_bucket(pg_id, &bucket, command.clone())
            .unwrap();
        {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            node_zero_pg
                .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
                .unwrap();
        }

        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        assert_eq!(generation_id, skipped_generation_id);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, &bucket)
            .is_none());
        for node_id in node_ids {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            assert_eq!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &reservation_id
                )
                .unwrap(),
                generation_id
            );
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, 2);
        }
    }

    #[test]
    fn abandoned_put_object_stream_create_releases_reserved_generation_on_drain() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let pg_id = PgId::new(object_pg);
        let session_id = crate::SessionId::try_from("59".repeat(16)).unwrap();
        cluster
            .reserve_put_object_generation(&bucket, &key, &session_id)
            .unwrap();
        let command = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                cluster.operation_epoch(),
                pg_id,
                map.runtime_state().next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request(
                    crate::CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                    123,
                ),
            )),
        );
        map.runtime_state()
            .try_set_pending_metadata_command_for_bucket(pg_id, &bucket, command.clone())
            .unwrap();
        {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            node_zero_pg
                .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
                .unwrap();
        }

        cluster
            .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, &bucket)
            .is_none());
        for node_id in node_ids {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &session_id
                ),
                Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
            ));
        }
    }

    #[test]
    fn abandoned_put_object_stream_create_release_failure_leaves_pending_retry() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let pg_id = PgId::new(object_pg);
        let session_id = crate::SessionId::try_from("5a".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &session_id)
            .unwrap();
        let command = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                cluster.operation_epoch(),
                pg_id,
                map.runtime_state().next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request(
                    crate::CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                    123,
                ),
            )),
        );
        map.runtime_state()
            .try_set_pending_metadata_command_for_bucket(pg_id, &bucket, command.clone())
            .unwrap();
        {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            node_zero_pg
                .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
                .unwrap();
        }

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let hook_session = session_id.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::ReleaseObjectGeneration(release)
                        if release.matches_request(&hook_bucket, &hook_key, &hook_session)
                            && node_id == NodeId::new(0)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected abandoned stream create release failure",
                            source: std::io::Error::other(
                                "injected abandoned stream create release failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected abandoned stream create release failure",
                    ..
                })
            ),
            "expected required release failure, got {err:?}"
        );
        let pending = map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, &bucket)
            .expect("release cleanup must remain pending after failure");
        assert!(matches!(
            pending.payload(),
            MetadataCommandPayload::ReleaseObjectGeneration(release)
                if release.matches_request(&bucket, &key, &session_id)
        ));
        for node_id in node_ids {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            assert_eq!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &session_id
                )
                .unwrap(),
                generation_id
            );
        }

        drop(hook_guard);
        cluster
            .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, &bucket)
            .is_none());
        for node_id in node_ids {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &session_id
                ),
                Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
            ));
        }
    }

    #[test]
    fn zero_apply_generation_reservation_records_tombstone_and_later_reserves() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let before_index = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap()
            .applied_log_index;

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::ReserveObjectGeneration(reservation)
                        if reservation.bucket == hook_bucket
                            && reservation.key == hook_key
                            && node_id == NodeId::new(0)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected zero-apply reservation failure",
                            source: std::io::Error::other(
                                "injected zero-apply reservation failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let reservation_id = crate::SessionId::try_from("52".repeat(16)).unwrap();
        let err = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected zero-apply reservation failure",
                    ..
                })
            ),
            "expected injected zero-apply reservation failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, before_index + 1);
            assert!(matches!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &reservation_id
                ),
                Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
            ));
        }

        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        assert!(generation_id.get() >= crate::GenerationId::MIN.get());
        for node_id in node_ids {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, before_index + 2);
            assert_eq!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &reservation_id
                )
                .unwrap(),
                generation_id
            );
        }
    }

    #[test]
    fn zero_apply_direct_put_commit_records_tombstone_and_cleans_new_payload() {
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
        create_test_bucket(&cluster, &bucket);
        let reservation_id = crate::SessionId::try_from("53".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let before_index = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap()
            .applied_log_index;
        let payload = b"direct put zero apply tombstone";
        let segment_okh = [93; 16];
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
        let commit_req = direct_put_commit_req(
            &bucket,
            &key,
            reservation_id.clone(),
            generation_id,
            payload,
            segment_okh,
            &written,
        );

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.object.bucket == hook_bucket
                            && commit.object.key == hook_key
                            && node_id == NodeId::new(0)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected zero-apply direct put commit failure",
                            source: std::io::Error::other(
                                "injected zero-apply direct put commit failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .commit_direct_put_object_from_payload_shards(
                &commit_req,
                &written.written_shards,
                |_| Ok::<(), ()>(()),
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected zero-apply direct put commit failure",
                    ..
                })
            ),
            "expected injected zero-apply direct PUT commit failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        for node_id in node_ids {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            let state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(state.applied_log_index, before_index + 2);
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
            assert!(matches!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &reservation_id
                ),
                Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
            ));
        }
    }

    #[test]
    fn abandoned_matching_direct_put_commit_cleans_pending_and_current_payload() {
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
        create_test_bucket(&cluster, &bucket);

        let reservation_id = crate::SessionId::try_from("54".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();

        let abandoned_payload = b"abandoned direct put payload";
        let abandoned_okh = [94; 16];
        let abandoned_written = cluster
            .write_direct_put_segment_payload_shards(
                &bucket,
                &key,
                generation_id,
                0,
                &abandoned_okh,
                abandoned_payload,
            )
            .unwrap();
        let abandoned_req = direct_put_commit_req(
            &bucket,
            &key,
            reservation_id.clone(),
            generation_id,
            abandoned_payload,
            abandoned_okh,
            &abandoned_written,
        );
        let object_node = cluster.object_metadata_primary_node(&bucket, &key).unwrap();
        let object_pg_store = object_node.get_pg(object_pg).unwrap();
        let command = cluster
            .prepare_commit_direct_put_object_command(
                PgId::new(object_pg),
                &object_pg_store,
                &abandoned_req,
                crate::VersionId::Null,
            )
            .unwrap();
        drop(object_pg_store);
        let abandoned_shard_batch: Vec<(&ShardKey, crate::WriteAck)> = abandoned_written
            .written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        cluster
            .register_payload_shard_acks(data_pg, &abandoned_shard_batch)
            .unwrap();

        map.runtime_state()
            .try_set_pending_metadata_command_for_bucket(
                PgId::new(object_pg),
                &bucket,
                command.clone(),
            )
            .unwrap();
        {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            node_zero_pg
                .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
                .unwrap();
        }

        let current_payload = b"current direct put retry payload";
        let current_okh = [95; 16];
        let current_written = cluster
            .write_direct_put_segment_payload_shards(
                &bucket,
                &key,
                generation_id,
                0,
                &current_okh,
                current_payload,
            )
            .unwrap();
        let current_req = direct_put_commit_req(
            &bucket,
            &key,
            reservation_id.clone(),
            generation_id,
            current_payload,
            current_okh,
            &current_written,
        );
        let current_shard_batch: Vec<(&ShardKey, crate::WriteAck)> = current_written
            .written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        cluster
            .register_payload_shard_acks(data_pg, &current_shard_batch)
            .unwrap();

        let data_primary = map.node(NodeId::new(2)).unwrap().storage_node();
        for written in abandoned_written
            .written_shards
            .iter()
            .chain(current_written.written_shards.iter())
        {
            assert!(data_primary
                .test_shard_exists(data_pg, &written.key)
                .unwrap());
        }

        let err = cluster
            .commit_direct_put_object_from_payload_shards(
                &current_req,
                &current_written.written_shards,
                |_| -> Result<(), ()> {
                    panic!("matching abandoned pending direct PUT must not build a new command")
                },
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "abandoned pending command for direct put commit",
                    ..
                })
            ),
            "expected abandoned direct PUT conflict, got {err:?}"
        );

        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &reservation_id
                ),
                Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
            ));
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
        }

        for shard_index in 0..abandoned_written.ec.k + abandoned_written.ec.m {
            assert!(!cluster
                .test_payload_shard_file_exists(
                    abandoned_written.data_pg_id,
                    abandoned_written.ec,
                    &abandoned_okh,
                    generation_id,
                    shard_index,
                )
                .unwrap());
            assert!(!cluster
                .test_payload_shard_file_exists(
                    current_written.data_pg_id,
                    current_written.ec,
                    &current_okh,
                    generation_id,
                    shard_index,
                )
                .unwrap());
        }
        for written in abandoned_written
            .written_shards
            .iter()
            .chain(current_written.written_shards.iter())
        {
            assert!(!data_primary
                .test_shard_exists(data_pg, &written.key)
                .unwrap());
        }
    }

    #[test]
    fn direct_put_commit_drains_unrelated_pending_command_before_publish() {
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
        create_test_bucket(&cluster, &bucket);
        let pending_key = key_for_object_pg(
            map.node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology(),
            &bucket,
            object_pg,
            "pending-tags-",
        );
        write_committed_direct_segment_for(
            &cluster,
            &bucket,
            &pending_key,
            b"unrelated pending object",
        );

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = pending_key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::PutObjectMetadata(update)
                        if update.object.bucket == hook_bucket
                            && update.object.key == hook_key
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected unrelated metadata command apply failure",
                            source: std::io::Error::other(
                                "injected unrelated metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let tags =
            "<Tagging><TagSet><Tag><Key>phase</Key><Value>pending</Value></Tag></TagSet></Tagging>";
        let err = cluster
            .put_object_tags_if(&bucket, &pending_key, None, tags, |stored| {
                Ok::<_, ()>(stored.version_id())
            })
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected unrelated metadata command apply failure",
                    ..
                })
            ),
            "expected injected unrelated pending command failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "unrelated object metadata command must remain pending"
        );

        let reservation_id = crate::SessionId::try_from("54".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let payload = b"direct put drains unrelated pending command";
        let segment_okh = [54; 16];
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
        let mut commit_req = direct_put_commit_req(
            &bucket,
            &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            &written,
        );
        commit_req.versioning = crate::BucketVersioningState::Enabled;

        let outcome = cluster
            .commit_direct_put_object_from_payload_shards(
                &commit_req,
                &written.written_shards,
                |_| Ok::<_, ()>(()),
            )
            .unwrap()
            .unwrap();
        assert_eq!(outcome.live_size, payload.len() as u64);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        assert_direct_put_metadata_on_acting_nodes(
            &map,
            &node_ids,
            object_pg,
            &commit_req,
            &outcome,
        );
        assert_object_version_counter_on_acting_nodes(
            &map,
            &node_ids,
            object_pg,
            &bucket,
            &key,
            outcome.version_id.to_u64() + 1,
        );

        for node_id in node_ids {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            let stored =
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &pending_key).unwrap();
            assert_eq!(stored.as_live().unwrap().tags.as_deref(), Some(tags));
        }
    }

    #[test]
    fn reserve_object_version_retry_reuses_pending_partial_replica_command() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let primary_node = cluster.object_metadata_primary_node(&bucket, &key).unwrap();
        let pg_id = PgId::new(object_pg);

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::ReserveObjectVersion(reservation)
                        if reservation.bucket == hook_bucket
                            && reservation.key == hook_key
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected reserve object version apply failure",
                            source: std::io::Error::other(
                                "injected reserve object version apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .reserve_next_object_version(pg_id, &bucket, &key, primary_node)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected reserve object version apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);

        let pending = map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, &bucket)
            .expect("partial version reservation must remain pending");
        let MetadataCommandPayload::ReserveObjectVersion(reservation) = pending.payload() else {
            panic!("expected pending ReserveObjectVersion, got {pending:?}");
        };
        assert_eq!(reservation.version_id, crate::VersionId::from_u64(1));
        for node_id in [NodeId::new(0), NodeId::new(2)] {
            assert_object_version_counter_on_acting_nodes(
                &map,
                &[node_id],
                object_pg,
                &bucket,
                &key,
                2,
            );
        }
        assert_object_version_counter_on_acting_nodes(
            &map,
            &[NodeId::new(1)],
            object_pg,
            &bucket,
            &key,
            0,
        );

        let reserved = cluster
            .reserve_next_object_version(pg_id, &bucket, &key, primary_node)
            .unwrap();
        assert_eq!(reserved, crate::VersionId::from_u64(1));
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, &bucket)
            .is_none());
        assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 2);
    }

    #[test]
    fn reserve_object_version_partial_apply_after_reopen_fails_closed() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let primary_node = cluster.object_metadata_primary_node(&bucket, &key).unwrap();
        let pg_id = PgId::new(object_pg);

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::ReserveObjectVersion(reservation)
                        if reservation.bucket == hook_bucket
                            && reservation.key == hook_key
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected lost reserve object version apply failure",
                            source: std::io::Error::other(
                                "injected lost reserve object version apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .reserve_next_object_version(pg_id, &bucket, &key, primary_node)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected lost reserve object version apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);

        assert_object_version_counter_on_acting_nodes(
            &map,
            &[NodeId::new(0), NodeId::new(2)],
            object_pg,
            &bucket,
            &key,
            2,
        );
        assert_object_version_counter_on_acting_nodes(
            &map,
            &[NodeId::new(1)],
            object_pg,
            &bucket,
            &key,
            0,
        );

        drop(cluster);
        drop(map);

        let err =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap_err();
        match err {
            ClusterBuildError::OpenLocalNode {
                source:
                    StoreError::MetadataCommandReplicaStateDiverged {
                        pg_id,
                        applied_log_index,
                        reference_applied_log_index,
                        ..
                    },
                ..
            } => {
                assert_eq!(pg_id, object_pg);
                assert_ne!(applied_log_index, reference_applied_log_index);
            }
            other => panic!("expected divergent replica state on reopen, got {other:?}"),
        }
    }

    #[test]
    fn direct_put_action_failure_does_not_reserve_object_version() {
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
        create_test_bucket_with_versioning(
            &cluster,
            &bucket,
            crate::BucketVersioningState::Enabled,
        );
        let reservation_id = crate::SessionId::try_from("75".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let payload = b"conditional direct put should not reserve a version";
        let segment_okh = [75; 16];
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
        let mut commit_req = direct_put_commit_req(
            &bucket,
            &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            &written,
        );
        commit_req.versioning = crate::BucketVersioningState::Enabled;

        let err = cluster
            .commit_direct_put_object_from_payload_shards(
                &commit_req,
                &written.written_shards,
                |_| Err::<(), _>("conditional write rejected"),
            )
            .unwrap()
            .unwrap_err();
        assert_eq!(err, "conditional write rejected");
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 0);
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
        }
    }

    #[test]
    fn metadata_command_log_checksum_mismatch_prevents_ack() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "checksum-mismatch-");
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap();

        {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            node_zero_pg
                .connection()
                .execute(
                    "UPDATE metadata_command_log SET command_checksum = ?1 WHERE log_index = 1",
                    rusqlite::params![command.checksum_crc64().wrapping_add(1) as i64],
                )
                .unwrap();
        }

        let err = cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogChecksumMismatch {
                node_id: 0,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 1,
                ..
            })
        ));
    }

    #[test]
    fn metadata_command_log_bytes_mismatch_prevents_ack() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "bytes-mismatch-");
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap();

        {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            node_zero_pg
                .connection()
                .execute(
                    "UPDATE metadata_command_log SET command_bytes = ?1 WHERE log_index = 1",
                    rusqlite::params![b"corrupt-command-bytes".as_slice()],
                )
                .unwrap();
        }

        let err = cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogChecksumMismatch {
                node_id: 0,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 1,
                ..
            })
        ));
    }

    #[test]
    fn metadata_state_digest_mismatch_prevents_ack() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let first_bucket = bucket_for_pg(topology, 1, "digest-source-");
        let second_bucket = bucket_for_pg(topology, 1, "digest-next-");
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let first_command = create_bucket_metadata_command(PgId::new(1), 1, first_bucket.clone());
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first_command)
            .unwrap();

        let expected_digest = {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            let expected_digest = node_zero_pg
                .metadata_command_replica_state()
                .unwrap()
                .state_digest;
            node_zero_pg
                .connection()
                .execute(
                    "UPDATE buckets SET public_read = 1 WHERE name = ?1",
                    rusqlite::params![&first_bucket],
                )
                .unwrap();
            expected_digest
        };

        let second_command = create_bucket_metadata_command(PgId::new(1), 2, second_bucket.clone());
        let err = cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &second_command)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataStateDigestMismatch {
                node_id: 0,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                expected_digest: digest,
                actual_digest: _,
            }) if digest == expected_digest
        ));
        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::head_bucket(&*pg, &second_bucket),
                Err(crate::MetadataError::BucketNotFound { .. })
            ));
        }
    }

    #[test]
    fn metadata_state_digest_ignores_bucket_write_reservation_counters() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let reserved_bucket = bucket_for_pg(topology, 1, "digest-reserved-");
        let next_bucket = bucket_for_pg(topology, 1, "digest-after-reservation-");
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let first_command =
            create_bucket_metadata_command(PgId::new(1), 1, reserved_bucket.clone());
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first_command)
            .unwrap();

        {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            let reserved = crate::PgMetadataStore::acquire_bucket_write_reservation(
                &*node_zero_pg,
                &reserved_bucket,
            )
            .unwrap();
            assert_eq!(reserved.active_write_reservations, 1);
            crate::PgMetadataStore::begin_bucket_write_drain(&*node_zero_pg, &reserved_bucket)
                .unwrap();
            let draining =
                crate::PgMetadataStore::head_bucket_raw(&*node_zero_pg, &reserved_bucket).unwrap();
            assert!(draining.write_reservations_blocked);
        }

        let second_command = create_bucket_metadata_command(PgId::new(1), 2, next_bucket.clone());
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &second_command)
            .unwrap();

        {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            crate::PgMetadataStore::release_bucket_write_reservation(
                &*node_zero_pg,
                &reserved_bucket,
            )
            .unwrap();
            crate::PgMetadataStore::end_bucket_write_drain(&*node_zero_pg, &reserved_bucket)
                .unwrap();
            let info = crate::PgMetadataStore::head_bucket(&*node_zero_pg, &next_bucket).unwrap();
            assert_eq!(info.name, next_bucket);
        }
    }

    #[test]
    fn bucket_metadata_commands_ignore_primary_local_write_reservation_state() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "bucket-command-reserved-");
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);

        {
            let primary_pg = map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            let reserved =
                crate::PgMetadataStore::acquire_bucket_write_reservation(&*primary_pg, &bucket)
                    .unwrap();
            assert_eq!(reserved.active_write_reservations, 1);
        }

        let acl_grants = crate::AclGrants::default();
        cluster
            .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
            .unwrap();
        cluster
            .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
            .unwrap();
        let public_access_block = crate::PublicAccessBlockConfig {
            block_public_acls: true,
            ignore_public_acls: true,
            block_public_policy: true,
            restrict_public_buckets: true,
        };
        cluster
            .put_bucket_public_access_block_and_load_info(&bucket, public_access_block)
            .unwrap();

        for node_id in node_ids {
            let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.acl_grants, acl_grants);
            assert!(info.public_read);
            assert!(!info.public_write);
            assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
            assert_eq!(info.public_access_block, Some(public_access_block));
            if node_id == NodeId::new(1) {
                assert_eq!(info.active_write_reservations, 1);
            } else {
                assert_eq!(info.active_write_reservations, 0);
            }
        }

        let primary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        crate::PgMetadataStore::release_bucket_write_reservation(&*primary_pg, &bucket).unwrap();
    }

    #[test]
    fn metadata_state_digest_covers_completed_multipart_order_sequence() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let allocated_bucket = bucket_for_pg(topology, 1, "digest-mpu-order-");
        let next_bucket = bucket_for_pg(topology, 1, "digest-after-mpu-order-");
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let first_command =
            create_bucket_metadata_command(PgId::new(1), 1, allocated_bucket.clone());
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first_command)
            .unwrap();

        {
            let node_zero_pg = map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            node_zero_pg
                .advance_completed_multipart_upload_sequence_for_bucket(&allocated_bucket, 7)
                .unwrap();
            assert_eq!(
                node_zero_pg
                    .completed_multipart_upload_sequence_for_bucket(&allocated_bucket)
                    .unwrap(),
                7
            );
        }

        let second_command = create_bucket_metadata_command(PgId::new(1), 2, next_bucket.clone());
        let err = cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &second_command)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(
                    StoreError::MetadataStateDigestMismatch { .. }
                )
            ),
            "expected direct completed-MPU sequence mutation to trip digest mismatch, got {err:?}"
        );
    }

    #[test]
    fn local_cluster_reopen_rejects_large_command_stream_materialized_tamper() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        set_route_primary(&mut map, 1, NodeId::new(1));
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology()
            .clone();
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let mut first_bucket = None;

        for log_index in 1..=129 {
            let bucket = bucket_for_pg(&topology, 1, &format!("digest-verified-{log_index}-"));
            if first_bucket.is_none() {
                first_bucket = Some(bucket.clone());
            }
            let command = create_bucket_metadata_command(PgId::new(1), log_index, bucket.clone());
            cluster
                .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
                .unwrap();
        }

        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let state = node_zero_pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 129);
        assert_ne!(state.state_digest, 0);
        node_zero_pg
            .connection()
            .execute(
                "UPDATE buckets SET public_read = 1 WHERE name = ?1",
                rusqlite::params![first_bucket.unwrap().as_str()],
            )
            .unwrap();

        drop(node_zero_pg);
        drop(cluster);
        drop(map);

        let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
        assert!(
            matches!(
                err,
                ClusterBuildError::OpenLocalNode {
                    node_id: 0,
                    source: StoreError::MetadataStateDigestMismatch { pg_id: 1, .. }
                }
            ),
            "unexpected reopen error: {err:?}"
        );
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
    fn object_generation_reservation_commands_apply_to_all_acting_object_pg_nodes() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "object-reservation-");
            let object_pg = 2;
            let key = key_for_object_pg(topology, &bucket, object_pg, "key-");
            (bucket, key, object_pg)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let reservation_id = crate::SessionId::try_from("12".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        assert_eq!(generation_id, crate::GenerationId::MIN);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert_eq!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &reservation_id,
                )
                .unwrap(),
                generation_id
            );
        }

        cluster
            .release_object_generation_reservation(&bucket, &key, &reservation_id)
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &reservation_id,
                ),
                Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
            ));
        }
    }

    #[test]
    fn direct_put_metadata_command_applies_to_all_acting_object_pg_nodes() {
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
        let reservation_id = crate::SessionId::try_from("13".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let payload = b"direct put metadata command replication";
        let segment_okh = [63; 16];
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
        let mut commit_req = direct_put_commit_req(
            &bucket,
            &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            &written,
        );
        commit_req.versioning = crate::BucketVersioningState::Enabled;

        let outcome = cluster
            .commit_direct_put_object_from_payload_shards(
                &commit_req,
                &written.written_shards,
                |_| Ok::<(), ()>(()),
            )
            .unwrap()
            .unwrap();

        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        assert_direct_put_metadata_on_acting_nodes(
            &map,
            &node_ids,
            object_pg,
            &commit_req,
            &outcome,
        );
        assert_object_version_counter_on_acting_nodes(
            &map,
            &node_ids,
            object_pg,
            &bucket,
            &key,
            outcome.version_id.to_u64() + 1,
        );
    }

    #[test]
    fn stream_put_staging_commands_apply_to_all_acting_object_pg_nodes() {
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
        create_test_bucket(&cluster, &bucket);
        let session_id = crate::SessionId::try_from("32".repeat(16)).unwrap();
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
            assert_eq!(session.bucket, bucket);
            assert_eq!(session.key, key);
            assert!(matches!(
                session.target,
                crate::StreamUploadTarget::PutObject
            ));
        }

        let payload = b"stream staging command replication";
        let (_target, segment) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: payload.len() as u64,
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    segment_okh: [88; 16],
                },
            )
            .unwrap();
        assert_eq!(segment.data_pg_id, data_pg);
        let written_shards = cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch: Vec<(&crate::ShardKey, crate::WriteAck)> = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert_eq!(
                crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
                vec![segment.clone()]
            );
        }

        cluster
            .abort_stream_upload_session(&bucket, &key, &session_id)
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ));
            assert!(
                crate::PgMetadataStore::list_stream_segments(&*pg, &session_id)
                    .unwrap()
                    .is_empty()
            );
        }

        let mut readback = Vec::new();
        assert!(cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: segment.data_pg_id,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    stored_size: payload.len(),
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                },
                &mut readback,
            )
            .is_err());
    }

    #[test]
    fn multipart_create_command_applies_to_all_acting_object_pg_nodes() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let upload_id = upload_id_from_label("mpucreatecommand");
        let create = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: Some(crate::SerializedTagSet::new("<Tagging/>".to_string())),
            metadata_blob: crate::SerializedMetadataBlob::new(vec![1, 2, 3]),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::new(vec![4, 5, 6]),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: true,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };

        let outcome = cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>((11_u8, create.clone()))
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(outcome.value, 11);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        let mut generation_id = None;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap();
            assert_eq!(upload.bucket, bucket);
            assert_eq!(upload.key, key);
            assert_eq!(upload.initiated_at, outcome.initiated_at);
            assert_eq!(upload.tags, create.tags);
            assert_eq!(upload.metadata_blob, create.metadata_blob);
            assert_eq!(upload.system_metadata_blob, create.system_metadata_blob);
            assert_eq!(upload.initiator, create.initiator);
            assert_eq!(upload.owner, create.owner);
            assert_eq!(upload.acl_grants, create.acl_grants);
            assert_eq!(upload.public_read, create.public_read);
            assert_eq!(upload.object_lock, create.object_lock);
            assert_eq!(upload.checksum, create.checksum);
            assert_eq!(upload.encryption, create.encryption);
            if let Some(generation_id) = generation_id {
                assert_eq!(upload.object_generation_id, generation_id);
            } else {
                generation_id = Some(upload.object_generation_id);
            }
        }
    }

    #[test]
    fn multipart_create_partial_apply_retry_reuses_pending_command() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let upload_id = upload_id_from_label("mpucreateretry");
        let create = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_upload_id = upload_id.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CreateMultipartUpload(create)
                        if create.upload.upload_id == hook_upload_id
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected multipart create metadata command apply failure",
                            source: std::io::Error::other(
                                "injected multipart create metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "injected multipart create metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);

        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial multipart create command must remain pending"
        );
        let replica_upload = {
            let replica = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = replica.get_pg(object_pg).unwrap();
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap()
        };
        {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            let pg = primary.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
                Err(crate::MetadataError::NoSuchUpload { .. })
            ));
        }

        let retry = cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>((7_u8, create.clone()))
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(retry.value, 7);
        assert_eq!(retry.initiated_at, replica_upload.initiated_at);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap();
            assert_eq!(upload.bucket, bucket);
            assert_eq!(upload.key, key);
            assert_eq!(upload.initiated_at, replica_upload.initiated_at);
            assert_eq!(
                upload.object_generation_id,
                replica_upload.object_generation_id
            );
        }
    }

    #[test]
    fn multipart_create_retry_rejects_same_request_with_mismatched_generation() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let upload_id = upload_id_from_label("mpurowmismatch");
        let create = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };

        cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap()
            .unwrap();
        {
            let primary = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = primary.get_pg(object_pg).unwrap();
            let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap();
            let mismatched_generation =
                crate::GenerationId::new(upload.object_generation_id.get() + 1).unwrap();
            pg.connection()
                .execute(
                    "UPDATE multipart_uploads SET object_generation_id = ?1 WHERE upload_id = ?2",
                    rusqlite::params![mismatched_generation.get() as i64, upload_id.as_str()],
                )
                .unwrap();
        }

        let err = cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
                    context: "create multipart upload existing upload mismatch",
                    ..
                })
            ),
            "expected exact multipart upload row mismatch, got {err:?}"
        );
    }

    #[test]
    fn multipart_abort_command_removes_upload_from_all_acting_object_pg_nodes() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);

        let first_upload_id = upload_id_from_label("mpuabortonallnodes");
        let first_create = crate::CreateMultipartUploadReq {
            upload_id: first_upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), first_create.clone()))
                },
            )
            .unwrap()
            .unwrap();
        assert!(cluster
            .abort_multipart_upload(&bucket, &key, &first_upload_id)
            .unwrap());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_multipart_upload(&*pg, &first_upload_id),
                Err(crate::MetadataError::NoSuchUpload { .. })
            ));
        }

        let second_upload_id = upload_id_from_label("mpuabortedfresh");
        let second_create = crate::CreateMultipartUploadReq {
            upload_id: second_upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), second_create.clone()))
                },
            )
            .unwrap()
            .unwrap();
        let mut second_generation = None;
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let upload =
                crate::PgMetadataStore::get_multipart_upload(&*pg, &second_upload_id).unwrap();
            assert_eq!(upload.bucket, bucket);
            assert_eq!(upload.key, key);
            if let Some(second_generation) = second_generation {
                assert_eq!(upload.object_generation_id, second_generation);
            } else {
                second_generation = Some(upload.object_generation_id);
            }
        }
    }

    #[test]
    fn multipart_abort_partial_apply_retry_cleans_uploaded_part_payload() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let upload_id = upload_id_from_label("mpuabandretryclean");
        let create = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap()
            .unwrap();

        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let part_number = 1;
        let (shard_keys, _uploaded_part, uploaded_segment) = upload_streamed_test_multipart_part(
            &cluster,
            &bucket,
            &key,
            &upload_id,
            part_number,
            [0xAB; 16],
            b"uploaded part payload",
        );
        let data_pg_id = uploaded_segment.data_pg_id;
        let part_okh = uploaded_segment.segment_okh;
        let part_vid = uploaded_segment.segment_vid;

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_upload_id = upload_id.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::AbortMultipartUpload(abort)
                        if abort.upload_id == hook_upload_id
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected multipart abort metadata command apply failure",
                            source: std::io::Error::other(
                                "injected multipart abort metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .abort_multipart_upload(&bucket, &key, &upload_id)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected multipart abort metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);

        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial multipart abort command must remain pending with cleanup refs"
        );
        {
            let primary_pg = primary.get_pg(object_pg).unwrap();
            let upload =
                crate::PgMetadataStore::get_multipart_upload(&*primary_pg, &upload_id).unwrap();
            assert_eq!(upload.state, crate::UploadState::InProgress);
            assert!(crate::PgMetadataStore::get_multipart_part(
                &*primary_pg,
                &upload_id,
                part_number
            )
            .is_ok());
        }
        for shard_index in 0..ec_shape.k + ec_shape.m {
            assert!(cluster
                .test_payload_shard_file_exists(
                    data_pg_id,
                    ec_shape,
                    &part_okh,
                    part_vid,
                    shard_index
                )
                .unwrap());
        }

        assert!(cluster
            .abort_multipart_upload(&bucket, &key, &upload_id)
            .unwrap());
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
                Err(crate::MetadataError::NoSuchUpload { .. })
            ));
        }
        for (shard_index, key) in shard_keys.iter().enumerate() {
            assert!(
                !cluster
                    .test_payload_shard_file_exists(
                        data_pg_id,
                        ec_shape,
                        &part_okh,
                        part_vid,
                        shard_index as u8
                    )
                    .unwrap(),
                "retrying pending abort should delete placed shard {key:?}"
            );
        }
    }

    #[test]
    fn multipart_abort_retries_after_pending_install_conflict() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let upload_id = upload_id_from_label("mpuabortinstall");
        let create = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap()
            .unwrap();

        let pg_id = PgId::new(object_pg);
        let unrelated_session_id = crate::SessionId::try_from("37".repeat(16)).unwrap();
        let injected = Arc::new(AtomicBool::new(false));
        let injected_for_hook = Arc::clone(&injected);
        let map_for_hook = Arc::clone(&map);
        let bucket_for_hook = bucket.clone();
        let key_for_hook = key.clone();
        let session_for_hook = unrelated_session_id.clone();
        let command_epoch = cluster.operation_epoch();
        let _hook_guard =
            cluster.test_install_before_abort_multipart_pending_install_hook(Arc::new(move || {
                if injected_for_hook.swap(true, Ordering::SeqCst) {
                    return;
                }
                let command = MetadataCommandEnvelope::new(
                    crate::metadata_command::MetadataCommandId::new(
                        command_epoch,
                        pg_id,
                        map_for_hook
                            .runtime_state()
                            .next_metadata_command_log_index(pg_id),
                    ),
                    MetadataCommandPayload::CreateStreamUpload(Box::new(
                        crate::metadata_command::CreateStreamUploadCommand::from_request(
                            crate::CreateStreamUploadReq {
                                session_id: session_for_hook.clone(),
                                bucket: bucket_for_hook.clone(),
                                key: key_for_hook.clone(),
                                target: crate::StreamUploadTarget::PutObject,
                                encryption: crate::ObjectEncryption::None,
                            },
                            123,
                        ),
                    )),
                );
                map_for_hook
                    .runtime_state()
                    .try_set_pending_metadata_command_for_bucket(pg_id, &bucket_for_hook, command)
                    .expect("abort pending-install hook should win the empty pending slot");
            }));

        assert!(cluster
            .abort_multipart_upload(&bucket, &key, &upload_id)
            .unwrap());
        assert!(
            injected.load(Ordering::SeqCst),
            "test hook must exercise the abort pending-install conflict window"
        );
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
                Err(crate::MetadataError::NoSuchUpload { .. })
            ));
            let session =
                crate::PgMetadataStore::get_stream_upload(&*pg, &unrelated_session_id).unwrap();
            assert_eq!(session.bucket, bucket);
            assert_eq!(session.key, key);
            assert_eq!(session.target, crate::StreamUploadTarget::PutObject);
        }
    }

    #[test]
    fn multipart_abort_pending_install_conflict_cleans_upload_part_stream_session() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let upload_id = upload_id_from_label("mpuabortstream");
        let create = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap()
            .unwrap();

        let pg_id = PgId::new(object_pg);
        let session_id = crate::SessionId::try_from("38".repeat(16)).unwrap();
        let injected = Arc::new(AtomicBool::new(false));
        let injected_for_hook = Arc::clone(&injected);
        let map_for_hook = Arc::clone(&map);
        let bucket_for_hook = bucket.clone();
        let key_for_hook = key.clone();
        let upload_for_hook = upload_id.clone();
        let session_for_hook = session_id.clone();
        let command_epoch = cluster.operation_epoch();
        let _hook_guard =
            cluster.test_install_before_abort_multipart_pending_install_hook(Arc::new(move || {
                if injected_for_hook.swap(true, Ordering::SeqCst) {
                    return;
                }
                let command = MetadataCommandEnvelope::new(
                    crate::metadata_command::MetadataCommandId::new(
                        command_epoch,
                        pg_id,
                        map_for_hook
                            .runtime_state()
                            .next_metadata_command_log_index(pg_id),
                    ),
                    MetadataCommandPayload::CreateStreamUpload(Box::new(
                        crate::metadata_command::CreateStreamUploadCommand::from_request(
                            crate::CreateStreamUploadReq {
                                session_id: session_for_hook.clone(),
                                bucket: bucket_for_hook.clone(),
                                key: key_for_hook.clone(),
                                target: crate::StreamUploadTarget::UploadPart {
                                    upload_id: upload_for_hook.clone(),
                                    part_number: 1,
                                },
                                encryption: crate::ObjectEncryption::None,
                            },
                            123,
                        ),
                    )),
                );
                map_for_hook
                    .runtime_state()
                    .try_set_pending_metadata_command_for_bucket(pg_id, &bucket_for_hook, command)
                    .expect("abort pending-install hook should win the empty pending slot");
            }));

        assert!(cluster
            .abort_multipart_upload(&bucket, &key, &upload_id)
            .unwrap());
        assert!(
            injected.load(Ordering::SeqCst),
            "test hook must exercise the UploadPart stream creation conflict window"
        );
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
                Err(crate::MetadataError::NoSuchUpload { .. })
            ));
            assert!(matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ));
        }
    }

    #[test]
    fn multipart_abort_zero_apply_leaves_upload_in_progress_before_retry() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let upload_id = upload_id_from_label("mpuabortingblocks");
        let create = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap()
            .unwrap();

        let session_id = crate::SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap();
        let upload = cluster
            .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
            .unwrap();
        cluster
            .create_upload_part_stream_session(
                &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
                1,
                &session_id,
            )
            .unwrap();
        let staged_payload = b"staged upload part segment";
        let staged_okh = [0xCD; 16];
        let (_target, staged_segment) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: staged_payload.len() as u64,
                    segment_crc64: Some(checksum::crc64::checksum(staged_payload)),
                    segment_okh: staged_okh,
                },
            )
            .unwrap();
        let staged_shards = cluster
            .write_stream_segment_payload_shards(&staged_segment, staged_payload)
            .unwrap();
        let staged_shard_batch = staged_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                staged_segment.segment_index,
                &staged_segment,
                &staged_shard_batch,
            )
            .unwrap();

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_upload_id = upload_id.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::AbortMultipartUpload(abort)
                        if abort.upload_id == hook_upload_id
                            && node_id == NodeId::new(0)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected zero-apply multipart abort failure",
                            source: std::io::Error::other(
                                "injected zero-apply multipart abort failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .abort_multipart_upload(&bucket, &key, &upload_id)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected zero-apply multipart abort failure",
                    ..
                })
            ),
            "expected injected zero-apply failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            let pg = primary.get_pg(object_pg).unwrap();
            let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap();
            assert_eq!(upload.state, crate::UploadState::InProgress);
            assert!(crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).is_ok());
            assert_eq!(
                crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
                vec![staged_segment.clone()]
            );
        }
        for shard_index in 0..ec_shape.k + ec_shape.m {
            assert!(cluster
                .test_payload_shard_file_exists(
                    staged_segment.data_pg_id,
                    ec_shape,
                    &staged_segment.segment_okh,
                    staged_segment.segment_vid,
                    shard_index
                )
                .unwrap());
        }

        assert!(cluster
            .abort_multipart_upload(&bucket, &key, &upload_id)
            .unwrap());
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
                Err(crate::MetadataError::NoSuchUpload { .. })
            ));
            assert!(matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ));
        }
        for shard_index in 0..ec_shape.k + ec_shape.m {
            assert!(
                !cluster
                    .test_payload_shard_file_exists(
                        staged_segment.data_pg_id,
                        ec_shape,
                        &staged_segment.segment_okh,
                        staged_segment.segment_vid,
                        shard_index
                    )
                    .unwrap(),
                "retrying abort should delete staged UploadPart stream shard {shard_index}"
            );
        }
    }

    #[test]
    fn lifecycle_multipart_abort_uses_command_and_cleans_uploaded_part_payload() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        put_test_lifecycle(&cluster, &bucket);

        let upload_id = upload_id_from_label("mpulifecycleabort");
        let create = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap()
            .unwrap();

        let part_number = 1;
        let (shard_keys, _uploaded_part, uploaded_segment) = upload_streamed_test_multipart_part(
            &cluster,
            &bucket,
            &key,
            &upload_id,
            part_number,
            [0xBC; 16],
            b"lifecycle uploaded part payload",
        );
        let data_pg_id = uploaded_segment.data_pg_id;
        let part_okh = uploaded_segment.segment_okh;
        let part_vid = uploaded_segment.segment_vid;

        let aborted = cluster
            .abort_multipart_upload_if_due(&bucket, &key, &upload_id, |raw_lifecycle, upload| {
                assert_eq!(raw_lifecycle, Some("<LifecycleConfiguration/>"));
                assert_eq!(upload.state, crate::UploadState::InProgress);
                Ok::<bool, ()>(true)
            })
            .unwrap()
            .unwrap();
        assert!(aborted);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
                Err(crate::MetadataError::NoSuchUpload { .. })
            ));
        }
        for (shard_index, key) in shard_keys.iter().enumerate() {
            assert!(
                !cluster
                    .test_payload_shard_file_exists(
                        data_pg_id,
                        ec_shape,
                        &part_okh,
                        part_vid,
                        shard_index as u8
                    )
                    .unwrap(),
                "lifecycle abort should delete placed shard {key:?}"
            );
        }
    }

    #[test]
    fn stream_put_create_partial_apply_retry_reuses_existing_session() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let session_id = crate::SessionId::try_from("34".repeat(16)).unwrap();
        let create = crate::CreateStreamUploadReq {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: crate::StreamUploadTarget::PutObject,
            encryption: crate::ObjectEncryption::None,
        };

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_session_id = session_id.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CreateStreamUpload(create)
                        if create.session.session_id == hook_session_id
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected stream create metadata command apply failure",
                            source: std::io::Error::other(
                                "injected stream create metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .create_put_object_stream_session(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "injected stream create metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);

        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial stream create command must remain pending"
        );
        let replica_created_at = {
            let replica = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = replica.get_pg(object_pg).unwrap();
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id)
                .unwrap()
                .created_at
        };
        {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            let pg = primary.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ));
        }

        let retry_value = cluster
            .create_put_object_stream_session(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>((7_u8, create.clone()))
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(retry_value, 7);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
            assert_eq!(session.bucket, bucket);
            assert_eq!(session.key, key);
            assert_eq!(session.created_at, replica_created_at);
            assert!(matches!(
                session.target,
                crate::StreamUploadTarget::PutObject
            ));
            assert_eq!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &session_id,
                )
                .unwrap(),
                crate::GenerationId::MIN
            );
        }
    }

    #[test]
    fn stream_put_create_retry_rejects_same_request_with_mismatched_created_at() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let session_id = crate::SessionId::try_from("7a".repeat(16)).unwrap();
        let create = crate::CreateStreamUploadReq {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: crate::StreamUploadTarget::PutObject,
            encryption: crate::ObjectEncryption::None,
        };

        cluster
            .create_put_object_stream_session(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap()
            .unwrap();
        {
            let primary = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = primary.get_pg(object_pg).unwrap();
            let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
            pg.connection()
                .execute(
                    "UPDATE stream_uploads SET created_at = ?1 WHERE session_id = ?2",
                    rusqlite::params![
                        session.created_at.saturating_add(1) as i64,
                        session_id.as_str()
                    ],
                )
                .unwrap();
        }

        let err = cluster
            .create_put_object_stream_session(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
                    context: "create stream upload existing session mismatch",
                    ..
                })
            ),
            "expected exact stream session row mismatch, got {err:?}"
        );
    }

    #[test]
    fn stream_put_create_drains_unrelated_pending_create_before_new_session() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let first_session_id = crate::SessionId::try_from("37".repeat(16)).unwrap();
        let second_session_id = crate::SessionId::try_from("38".repeat(16)).unwrap();
        let first_create = crate::CreateStreamUploadReq {
            session_id: first_session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: crate::StreamUploadTarget::PutObject,
            encryption: crate::ObjectEncryption::None,
        };
        let second_create = crate::CreateStreamUploadReq {
            session_id: second_session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: crate::StreamUploadTarget::PutObject,
            encryption: crate::ObjectEncryption::None,
        };

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_session_id = first_session_id.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CreateStreamUpload(create)
                        if create.session.session_id == hook_session_id
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected stream create metadata command apply failure",
                            source: std::io::Error::other(
                                "injected stream create metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        cluster
            .create_put_object_stream_session(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), first_create.clone()))
                },
            )
            .unwrap_err();
        drop(hook_guard);

        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial first stream create command must remain pending"
        );

        let value = cluster
            .create_put_object_stream_session(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>((9_u8, second_create.clone()))
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(value, 9);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            crate::PgMetadataStore::get_stream_upload(&*pg, &first_session_id).unwrap();
            crate::PgMetadataStore::get_stream_upload(&*pg, &second_session_id).unwrap();
        }
    }

    #[test]
    fn stream_put_create_retries_after_pending_install_conflict() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let session_id = crate::SessionId::try_from("39".repeat(16)).unwrap();
        let unrelated_session_id = crate::SessionId::try_from("3a".repeat(16)).unwrap();
        let create = crate::CreateStreamUploadReq {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: crate::StreamUploadTarget::PutObject,
            encryption: crate::ObjectEncryption::None,
        };

        let pg_id = PgId::new(object_pg);
        let _serial = lock_metadata_command_apply_hook_test();
        let injected = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let injected_for_hook = Arc::clone(&injected);
        let map_for_hook = Arc::clone(&map);
        let bucket_for_hook = bucket.clone();
        let key_for_hook = key.clone();
        let session_for_hook = unrelated_session_id.clone();
        let command_epoch = cluster.operation_epoch();
        let _hook_guard = cluster.test_install_before_stream_put_create_pending_install_hook(
            Arc::new(move || {
                if injected_for_hook.swap(true, Ordering::SeqCst) {
                    return;
                }
                let command = MetadataCommandEnvelope::new(
                    crate::metadata_command::MetadataCommandId::new(
                        command_epoch,
                        pg_id,
                        map_for_hook
                            .runtime_state()
                            .next_metadata_command_log_index(pg_id),
                    ),
                    MetadataCommandPayload::CreateStreamUpload(Box::new(
                        crate::metadata_command::CreateStreamUploadCommand::from_request(
                            crate::CreateStreamUploadReq {
                                session_id: session_for_hook.clone(),
                                bucket: bucket_for_hook.clone(),
                                key: key_for_hook.clone(),
                                target: crate::StreamUploadTarget::PutObject,
                                encryption: crate::ObjectEncryption::None,
                            },
                            123,
                        ),
                    )),
                );
                map_for_hook
                    .runtime_state()
                    .try_set_pending_metadata_command_for_bucket(pg_id, &bucket_for_hook, command)
                    .expect("stream create pending-install hook should win the empty pending slot");
            }),
        );

        let attempts_for_action = Arc::clone(&attempts);
        let value = cluster
            .create_put_object_stream_session(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    attempts_for_action.fetch_add(1, Ordering::SeqCst);
                    assert!(existing_object.is_none());
                    Ok::<_, ()>((13_u8, create.clone()))
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(value, 13);
        assert!(injected.load(Ordering::SeqCst));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
            assert_eq!(session.bucket, bucket);
            assert_eq!(session.key, key);
            let unrelated =
                crate::PgMetadataStore::get_stream_upload(&*pg, &unrelated_session_id).unwrap();
            assert_eq!(unrelated.bucket, bucket);
            assert_eq!(unrelated.key, key);
        }
    }

    #[test]
    fn stream_abort_missing_session_does_not_succeed_after_unrelated_pending_command() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);

        let missing_session_id = crate::SessionId::try_from("35".repeat(16)).unwrap();
        let unrelated_session_id = crate::SessionId::try_from("36".repeat(16)).unwrap();
        let pg_id = PgId::new(object_pg);
        let command = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                cluster.operation_epoch(),
                pg_id,
                map.runtime_state().next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request(
                    crate::CreateStreamUploadReq {
                        session_id: unrelated_session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                    123,
                ),
            )),
        );
        map.runtime_state()
            .try_set_pending_metadata_command_for_bucket(pg_id, &bucket, command)
            .unwrap();

        let error = cluster
            .abort_stream_upload_session(&bucket, &key, &missing_session_id)
            .unwrap_err();
        assert!(
            matches!(
                error,
                crate::ObjectPgActionError::Metadata(
                    crate::MetadataError::StreamSessionNotFound { .. }
                )
            ),
            "unrelated pending command must not make missing stream abort idempotent: {error:?}"
        );
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let session =
                crate::PgMetadataStore::get_stream_upload(&*pg, &unrelated_session_id).unwrap();
            assert_eq!(session.bucket, bucket);
            assert_eq!(session.key, key);
        }
    }

    #[test]
    fn pending_metadata_command_insert_rejects_existing_without_overwrite() {
        let runtime_state = LocalClusterRuntimeState::new();
        let pg_id = PgId::new(3);
        let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
        let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
        let session_one = crate::SessionId::try_from("41".repeat(16)).unwrap();
        let session_two = crate::SessionId::try_from("42".repeat(16)).unwrap();

        let make_release_command = |session_id: crate::SessionId| {
            let command_id = crate::metadata_command::MetadataCommandId::new(
                crate::ClusterEpoch::INITIAL,
                pg_id,
                runtime_state.next_metadata_command_log_index(pg_id),
            );
            MetadataCommandEnvelope::new(
                command_id,
                MetadataCommandPayload::ReleaseObjectGeneration(
                    crate::metadata_command::ReleaseObjectGenerationCommand::new(
                        bucket.clone(),
                        key.clone(),
                        session_id,
                    ),
                ),
            )
        };
        let command_one = make_release_command(session_one);
        let command_two = make_release_command(session_two);

        runtime_state
            .try_set_pending_metadata_command_for_bucket(pg_id, &bucket, command_one.clone())
            .unwrap();
        let result =
            runtime_state.try_set_pending_metadata_command_for_bucket(pg_id, &bucket, command_two);

        assert_eq!(result, Err(PendingMetadataCommandConflict));
        assert_eq!(
            runtime_state
                .pending_metadata_command_for_bucket(pg_id, &bucket)
                .unwrap(),
            command_one
        );
    }

    #[test]
    fn stream_put_append_partial_apply_keeps_payload_for_pending_retry() {
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
        create_test_bucket(&cluster, &bucket);
        let session_id = crate::SessionId::try_from("33".repeat(16)).unwrap();
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
        let payload = b"stream append partial apply";
        let (_target, segment) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: payload.len() as u64,
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    segment_okh: [89; 16],
                },
            )
            .unwrap();
        let written_shards = cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch: Vec<(&crate::ShardKey, crate::WriteAck)> = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_session_id = session_id.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::AppendStreamSegment(append)
                        if append.segment.session_id == hook_session_id
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected stream append metadata command apply failure",
                            source: std::io::Error::other(
                                "injected stream append metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected stream append metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial stream append command must remain pending"
        );
        {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            let pg = primary.get_pg(object_pg).unwrap();
            assert!(
                crate::PgMetadataStore::list_stream_segments(&*pg, &session_id)
                    .unwrap()
                    .is_empty()
            );
        }
        for node_id in [NodeId::new(0), NodeId::new(2)] {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert_eq!(
                crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
                vec![segment.clone()]
            );
        }

        let mut readback = Vec::new();
        cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: segment.data_pg_id,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    stored_size: payload.len(),
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                },
                &mut readback,
            )
            .unwrap();
        assert_eq!(readback, payload);

        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert_eq!(
                crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
                vec![segment.clone()]
            );
        }
    }

    #[test]
    fn stream_abort_pending_drain_clears_runtime_vid_allocator() {
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
        create_test_bucket(&cluster, &bucket);
        let session_id = crate::SessionId::try_from("45".repeat(16)).unwrap();
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
        let payload = b"stream abort pending drain allocator cleanup";
        let (_target, segment) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: payload.len() as u64,
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    segment_okh: [0x45; 16],
                },
            )
            .unwrap();
        assert_eq!(
            map.runtime_state()
                .test_stream_segment_vid_allocator_next(&session_id),
            Some(2)
        );
        let written_shards = cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_session_id = session_id.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::AbortStreamUpload(abort)
                        if abort.session_id == hook_session_id
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected stream abort metadata command apply failure",
                            source: std::io::Error::other(
                                "injected stream abort metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .abort_stream_upload_session(&bucket, &key, &session_id)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected stream abort metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial abort command must remain pending"
        );
        assert_eq!(
            map.runtime_state()
                .test_stream_segment_vid_allocator_next(&session_id),
            Some(2)
        );

        let next_reservation_id = crate::SessionId::try_from("46".repeat(16)).unwrap();
        cluster
            .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        assert_eq!(
            map.runtime_state()
                .test_stream_segment_vid_allocator_next(&session_id),
            None
        );
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ));
        }
    }

    #[test]
    fn stream_put_finalize_pending_drain_clears_runtime_vid_allocator() {
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
        create_test_bucket(&cluster, &bucket);
        let session_id = crate::SessionId::try_from("49".repeat(16)).unwrap();
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
        let payload = b"stream put finalize pending drain allocator cleanup";
        let payload_crc64 = checksum::crc64::checksum(payload);
        let (_target, segment) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: payload.len() as u64,
                    segment_crc64: Some(payload_crc64),
                    segment_okh: [0x49; 16],
                },
            )
            .unwrap();
        assert_eq!(
            map.runtime_state()
                .test_stream_segment_vid_allocator_next(&session_id),
            Some(2)
        );
        let written_shards = cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_session_id = session_id.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.generation_reservation_id == hook_session_id
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected stream put finalize metadata command apply failure",
                            source: std::io::Error::other(
                                "injected stream put finalize metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .finalize_put_object_stream(&bucket, &key, &session_id, payload.len() as u64, |_| {
                Ok::<_, ()>(crate::PreparedStreamPutCommit {
                    value: (),
                    versioning: crate::BucketVersioningState::Disabled,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    size: payload.len() as u64,
                    etag_crc64: payload_crc64,
                    tags: None,
                    metadata_blob: crate::SerializedMetadataBlob::default(),
                    system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                })
            })
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected stream put finalize metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial stream PUT finalize command must remain pending"
        );
        assert_eq!(
            map.runtime_state()
                .test_stream_segment_vid_allocator_next(&session_id),
            Some(2)
        );

        let next_reservation_id = crate::SessionId::try_from("4a".repeat(16)).unwrap();
        cluster
            .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        assert_eq!(
            map.runtime_state()
                .test_stream_segment_vid_allocator_next(&session_id),
            None
        );
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ));
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
            let live = stored.as_live().unwrap();
            assert_eq!(live.size, payload.len() as u64);
            assert_eq!(live.generation_id, crate::GenerationId::MIN);
        }
    }

    #[test]
    fn versioned_stream_put_finalize_reserves_object_version_through_command_stream() {
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
        create_test_bucket_with_versioning(
            &cluster,
            &bucket,
            crate::BucketVersioningState::Enabled,
        );
        let session_id = crate::SessionId::try_from("76".repeat(16)).unwrap();
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
        let payload = b"versioned stream put finalization";
        let payload_crc64 = checksum::crc64::checksum(payload);
        let (_target, segment) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: payload.len() as u64,
                    segment_crc64: Some(payload_crc64),
                    segment_okh: [0x76; 16],
                },
            )
            .unwrap();
        let written_shards = cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();

        let outcome = cluster
            .finalize_put_object_stream(&bucket, &key, &session_id, payload.len() as u64, |_| {
                Ok::<_, ()>(crate::PreparedStreamPutCommit {
                    value: (),
                    versioning: crate::BucketVersioningState::Enabled,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    size: payload.len() as u64,
                    etag_crc64: payload_crc64,
                    tags: None,
                    metadata_blob: crate::SerializedMetadataBlob::default(),
                    system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                })
            })
            .unwrap()
            .unwrap();
        assert_eq!(outcome.version_id, crate::VersionId::from_u64(1));

        assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 2);
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
            let live = stored.as_live().unwrap();
            assert_eq!(live.version_id, outcome.version_id);
            assert_eq!(live.size, payload.len() as u64);
            assert_eq!(
                pg.object_write_sequence(bucket.as_str(), key.as_str(), outcome.version_id,)
                    .unwrap(),
                Some(1),
            );
            assert!(matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ));
        }
    }

    #[test]
    fn stream_part_finalize_pending_drain_clears_runtime_vid_allocator() {
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
        create_test_bucket(&cluster, &bucket);
        let upload_id = upload_id_from_label("partfinalizedrain");
        let create = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap()
            .unwrap();
        let session_id = crate::SessionId::try_from("47".repeat(16)).unwrap();
        let upload = cluster
            .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
            .unwrap();
        cluster
            .create_upload_part_stream_session(
                &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
                1,
                &session_id,
            )
            .unwrap();
        let payload = b"stream part finalize pending drain allocator cleanup";
        let (_target, segment) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: payload.len() as u64,
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    segment_okh: [0x47; 16],
                },
            )
            .unwrap();
        assert_eq!(
            map.runtime_state()
                .test_stream_segment_vid_allocator_next(&session_id),
            Some(2)
        );
        let written_shards = cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_session_id = session_id.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CommitStreamPart(commit)
                        if commit.session_id == hook_session_id
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected stream part metadata command apply failure",
                            source: std::io::Error::other(
                                "injected stream part metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let expected_part = crate::MultipartPartRecord {
            upload_id: upload_id.clone(),
            part_number: 1,
            generation: 0,
            size: payload.len() as u64,
            etag: vec![0x47; 8],
            etag_kind: crate::EtagKind::Crc64,
            part_okh: [0u8; 16],
            part_vid: crate::GenerationId::MIN,
            ec_k: segment.ec_k,
            ec_m: segment.ec_m,
            last_modified: 123_456,
            checksum: None,
        };
        let expected_segments = vec![crate::MultipartPartSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
            part_number: 1,
            segment_index: segment.segment_index,
            size: segment.size,
            segment_crc64: segment.segment_crc64,
            segment_okh: segment.segment_okh,
            segment_vid: segment.segment_vid,
            data_pg_id: segment.data_pg_id,
            ec_k: segment.ec_k,
            ec_m: segment.ec_m,
        }];
        let err = cluster
            .finalize_upload_part_stream(&bucket, &key, &upload_id, &session_id, 1, |_| {
                Ok::<_, ()>(crate::PreparedStreamPartCommit {
                    value: (),
                    part: expected_part.clone(),
                    segments: expected_segments.clone(),
                })
            })
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected stream part metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial stream part command must remain pending"
        );
        assert_eq!(
            map.runtime_state()
                .test_stream_segment_vid_allocator_next(&session_id),
            Some(2)
        );

        let next_reservation_id = crate::SessionId::try_from("48".repeat(16)).unwrap();
        cluster
            .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        assert_eq!(
            map.runtime_state()
                .test_stream_segment_vid_allocator_next(&session_id),
            None
        );
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ));
            assert_eq!(
                crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
                expected_part
            );
            assert_eq!(
                crate::PgMetadataStore::get_multipart_part_segments_for_upload_part(
                    &*pg, &bucket, &key, &upload_id, 1
                )
                .unwrap(),
                expected_segments
            );
        }
    }

    #[test]
    fn stream_segment_prepare_uses_local_runtime_vid_allocator() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, _object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let session_id = crate::SessionId::try_from("7b".repeat(16)).unwrap();
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
        let req = crate::PrepareStreamUploadSegmentAppendReq {
            session_id: session_id.clone(),
            segment_index: 0,
            size: 16,
            segment_crc64: Some(1),
            segment_okh: [42; 16],
        };

        let (_target, first) = cluster
            .prepare_stream_segment_append(&bucket, &key, &req)
            .unwrap();
        let (_target, second) = cluster
            .prepare_stream_segment_append(&bucket, &key, &req)
            .unwrap();

        assert_eq!(first.segment_vid, crate::GenerationId::MIN);
        assert_eq!(second.segment_vid, crate::GenerationId::new(2).unwrap());
        assert_eq!(first.segment_okh, second.segment_okh);
        assert_eq!(first.segment_index, second.segment_index);
    }

    #[test]
    fn stream_segment_prepare_allocates_vid_after_validation() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, _object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let session_id = crate::SessionId::try_from("7c".repeat(16)).unwrap();
        let req = crate::PrepareStreamUploadSegmentAppendReq {
            session_id: session_id.clone(),
            segment_index: 0,
            size: 16,
            segment_crc64: Some(1),
            segment_okh: [42; 16],
        };

        let err = cluster
            .prepare_stream_segment_append(&bucket, &key, &req)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::ObjectPgActionError::Metadata(
                crate::MetadataError::StreamSessionNotFound { .. }
            )
        ));

        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
        let (_target, segment) = cluster
            .prepare_stream_segment_append(&bucket, &key, &req)
            .unwrap();

        assert_eq!(segment.segment_vid, crate::GenerationId::MIN);
    }

    #[test]
    fn direct_put_metadata_command_retry_reuses_pending_partial_replica_command() {
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
        let reservation_id = crate::SessionId::try_from("14".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let payload = b"direct put metadata command partial apply retry";
        let segment_okh = [64; 16];
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
        let mut commit_req = direct_put_commit_req(
            &bucket,
            &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            &written,
        );
        commit_req.versioning = crate::BucketVersioningState::Enabled;

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.object.bucket == hook_bucket
                            && commit.object.key == hook_key
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected direct put metadata command apply failure",
                            source: std::io::Error::other(
                                "injected direct put metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .commit_direct_put_object_from_payload_shards(
                &commit_req,
                &written.written_shards,
                |_| Ok::<(), ()>(()),
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected direct put metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(!fail_once.load(Ordering::SeqCst));
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial direct PUT metadata command must remain pending"
        );
        for node_id in [NodeId::new(0), NodeId::new(2)] {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
            assert_eq!(stored.as_live().unwrap().generation_id, generation_id);
        }
        {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            let pg = primary.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
        }

        let outcome = cluster
            .commit_direct_put_object_from_payload_shards(
                &commit_req,
                &written.written_shards,
                |_| -> Result<(), ()> { panic!("retry must reuse the pending direct PUT command") },
            )
            .unwrap()
            .unwrap();

        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        assert_direct_put_metadata_on_acting_nodes(
            &map,
            &node_ids,
            object_pg,
            &commit_req,
            &outcome,
        );
        assert_object_version_counter_on_acting_nodes(
            &map,
            &node_ids,
            object_pg,
            &bucket,
            &key,
            outcome.version_id.to_u64() + 1,
        );
    }

    #[test]
    fn object_generation_reservation_entry_drains_pending_direct_put_commit() {
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
        let reservation_id = crate::SessionId::try_from("22".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let payload = b"direct put metadata command production retry";
        let segment_okh = [72; 16];
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
        let commit_req = direct_put_commit_req(
            &bucket,
            &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            &written,
        );

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.object.bucket == hook_bucket
                            && commit.object.key == hook_key
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected direct put metadata command apply failure",
                            source: std::io::Error::other(
                                "injected direct put metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .commit_direct_put_object_from_payload_shards(
                &commit_req,
                &written.written_shards,
                |_| Ok::<(), ()>(()),
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected direct put metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial direct PUT metadata command must remain pending"
        );

        let next_reservation_id = crate::SessionId::try_from("23".repeat(16)).unwrap();
        let next_generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
            .unwrap();
        assert!(next_generation_id.get() > generation_id.get());
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
            assert_eq!(stored.as_live().unwrap().generation_id, generation_id);
            assert_eq!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &next_reservation_id,
                )
                .unwrap(),
                next_generation_id
            );
        }
    }

    #[test]
    fn object_delete_drains_pending_direct_put_commit_before_delete() {
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
        let old_committed =
            write_committed_direct_segment_for(&cluster, &bucket, &key, b"old direct object");

        let reservation_id = crate::SessionId::try_from("22".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        assert!(generation_id.get() > old_committed.generation_id.get());
        let payload = b"new direct object with pending command";
        let segment_okh = [73; 16];
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
        let commit_req = direct_put_commit_req(
            &bucket,
            &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            &written,
        );

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.object.bucket == hook_bucket
                            && commit.object.key == hook_key
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected direct put metadata command apply failure",
                            source: std::io::Error::other(
                                "injected direct put metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .commit_direct_put_object_from_payload_shards(
                &commit_req,
                &written.written_shards,
                |_| Ok::<(), ()>(()),
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected direct put metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial direct PUT metadata command must remain pending"
        );
        {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            let pg = primary.get_pg(object_pg).unwrap();
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
            assert_eq!(
                stored.as_live().unwrap().generation_id,
                old_committed.generation_id
            );
        }

        let outcome = cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                let stored = stored.expect("pending direct PUT should be applied before delete");
                let live = stored
                    .as_live()
                    .expect("pending direct PUT should publish a live object");
                assert_eq!(live.generation_id, generation_id);
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
        assert!(matches!(
            outcome.deleted,
            crate::DeletedCurrentObject::Live {
                generation_id: deleted_generation_id,
                ..
            } if deleted_generation_id == generation_id
        ));
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
        }

        let next_reservation_id = crate::SessionId::try_from("23".repeat(16)).unwrap();
        let next_generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
            .unwrap();
        assert!(next_generation_id.get() > generation_id.get());
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(
                matches!(
                    crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                    Err(crate::MetadataError::ObjectNotFound)
                ),
                "later reservation must not resurrect the deleted pending direct PUT on node {node_id:?}"
            );
            assert_eq!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*pg,
                    &bucket,
                    &key,
                    &next_reservation_id,
                )
                .unwrap(),
                next_generation_id
            );
        }
    }

    #[test]
    fn multipart_completion_command_publishes_streamed_part_segments_to_all_acting_nodes() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let (mut req, mut expected_segment) =
            seed_streamed_multipart_completion(&cluster, &bucket, &key, "streamedcomplete");
        req.versioning = crate::BucketVersioningState::Enabled;

        let replacement_session_id = crate::SessionId::try_from("52".repeat(16)).unwrap();
        let upload = cluster
            .load_in_progress_multipart_upload(&bucket, &key, &req.upload_id)
            .unwrap();
        cluster
            .create_upload_part_stream_session(
                &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
                2,
                &replacement_session_id,
            )
            .unwrap();
        let replacement_payload = b"replacement stream session";
        let replacement_okh = [0x52; 16];
        let (_target, replacement_segment) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: replacement_session_id.clone(),
                    segment_index: 0,
                    size: replacement_payload.len() as u64,
                    segment_crc64: Some(checksum::crc64::checksum(replacement_payload)),
                    segment_okh: replacement_okh,
                },
            )
            .unwrap();
        let replacement_shards = cluster
            .write_stream_segment_payload_shards(&replacement_segment, replacement_payload)
            .unwrap();
        let replacement_shard_batch = replacement_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &replacement_session_id,
                replacement_segment.segment_index,
                &replacement_segment,
                &replacement_shard_batch,
            )
            .unwrap();
        for shard_index in 0..ec_shape.k + ec_shape.m {
            assert!(cluster
                .test_payload_shard_file_exists(
                    replacement_segment.data_pg_id,
                    ec_shape,
                    &replacement_segment.segment_okh,
                    replacement_segment.segment_vid,
                    shard_index
                )
                .unwrap());
        }

        let outcome = cluster
            .complete_multipart_upload_commit_serialized(req.clone(), 16)
            .unwrap();
        expected_segment.version_id = outcome.version_id.to_u64();

        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        assert_streamed_multipart_completion_on_acting_nodes(
            &map,
            &node_ids,
            object_pg,
            &req,
            &expected_segment,
            &outcome,
        );
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &replacement_session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ));
            assert!(
                crate::PgMetadataStore::list_stream_segments(&*pg, &replacement_session_id)
                    .unwrap()
                    .is_empty(),
                "completion must delete staged replacement stream segments on node {node_id:?}"
            );
        }
        for shard_index in 0..ec_shape.k + ec_shape.m {
            assert!(
                !cluster
                    .test_payload_shard_file_exists(
                        replacement_segment.data_pg_id,
                        ec_shape,
                        &replacement_segment.segment_okh,
                        replacement_segment.segment_vid,
                        shard_index
                    )
                    .unwrap(),
                "completion must delete staged replacement stream shard {shard_index}"
            );
        }
        assert_object_version_counter_on_acting_nodes(
            &map,
            &node_ids,
            object_pg,
            &bucket,
            &key,
            outcome.version_id.to_u64() + 1,
        );
    }

    #[test]
    fn multipart_completion_over_standard_object_reopens_with_valid_digest() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let pg_ids = [0, 1, 2, 3];
        let ec_shape = EcShape { k: 2, m: 1 };
        let map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap());
        let (bucket, key, object_pg, _) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };

        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let old = write_committed_direct_segment_for(
            &cluster,
            &bucket,
            &key,
            b"old standard object payload",
        );
        let (req, expected_segment) = seed_streamed_multipart_completion_with_existing(
            &cluster,
            &bucket,
            &key,
            "stdoverwrite",
            true,
        );

        let outcome = cluster
            .complete_multipart_upload_commit_serialized(req.clone(), 16)
            .unwrap();
        assert!(
            matches!(
                outcome.stale_payload,
                Some(crate::CompletedMultipartStalePayload::Segments { generation_id, .. })
                    if generation_id == old.generation_id
            ),
            "multipart overwrite should record stale standard payload"
        );
        assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
            &map,
            &node_ids,
            object_pg,
            &req,
            &expected_segment,
            &outcome,
            2,
        );

        drop(cluster);
        drop(map);
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
            .expect("multipart overwrite should leave restart digest valid");
    }

    #[test]
    fn versioned_direct_put_and_multipart_completion_allocate_versions_via_command_stream() {
        #[derive(Default)]
        struct VersionRaceState {
            direct_at_apply: bool,
            multipart_at_apply: bool,
            release_direct: bool,
        }

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
        create_test_bucket_with_versioning(
            &cluster,
            &bucket,
            crate::BucketVersioningState::Enabled,
        );
        let (mut multipart_req, mut expected_multipart_segment) =
            seed_streamed_multipart_completion(&cluster, &bucket, &key, "versionraceupload");
        multipart_req.versioning = crate::BucketVersioningState::Enabled;

        let reservation_id = crate::SessionId::try_from("73".repeat(16)).unwrap();
        let direct_generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let direct_payload = b"versioned direct put races multipart completion";
        let direct_okh = [73; 16];
        let direct_written = cluster
            .write_direct_put_segment_payload_shards(
                &bucket,
                &key,
                direct_generation_id,
                0,
                &direct_okh,
                direct_payload,
            )
            .unwrap();
        let mut direct_req = direct_put_commit_req(
            &bucket,
            &key,
            reservation_id.clone(),
            direct_generation_id,
            direct_payload,
            direct_okh,
            &direct_written,
        );
        direct_req.versioning = crate::BucketVersioningState::Enabled;

        let _serial = lock_metadata_command_apply_hook_test();
        let race_state = Arc::new((Mutex::new(VersionRaceState::default()), Condvar::new()));
        let direct_seen = Arc::new(AtomicBool::new(false));
        let multipart_seen = Arc::new(AtomicBool::new(false));
        let hook_key = key.clone();
        let hook_reservation_id = reservation_id.clone();
        let hook_upload_id = multipart_req.upload_id.clone();
        let expected_multipart_req = multipart_req.clone();
        let hook_race_state = Arc::clone(&race_state);
        let hook_direct_seen = Arc::clone(&direct_seen);
        let hook_multipart_seen = Arc::clone(&multipart_seen);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |_node_id, command| {
                let (lock, cvar) = &*hook_race_state;
                match command.payload() {
                    MetadataCommandPayload::CommitDirectPutObject(commit)
                        if commit.object.key == hook_key
                            && commit.generation_reservation_id == hook_reservation_id
                            && !hook_direct_seen.swap(true, Ordering::SeqCst) =>
                    {
                        let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                        state.direct_at_apply = true;
                        cvar.notify_all();
                        while !state.release_direct {
                            state = cvar.wait(state).unwrap_or_else(|e| e.into_inner());
                        }
                    }
                    MetadataCommandPayload::CommitMultipartObject(commit)
                        if commit.upload_id == hook_upload_id
                            && !hook_multipart_seen.swap(true, Ordering::SeqCst) =>
                    {
                        let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                        state.multipart_at_apply = true;
                        cvar.notify_all();
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let direct_cluster = Arc::clone(&cluster);
        let direct_written_shards = direct_written.written_shards.clone();
        let direct_thread = std::thread::spawn(move || {
            direct_cluster.commit_direct_put_object_from_payload_shards(
                &direct_req,
                &direct_written_shards,
                |_| Ok::<_, ()>(()),
            )
        });

        {
            let (lock, cvar) = &*race_state;
            let state = lock.lock().unwrap_or_else(|e| e.into_inner());
            let (state, _) = cvar
                .wait_timeout_while(state, Duration::from_secs(5), |state| {
                    !state.direct_at_apply
                })
                .unwrap();
            assert!(
                state.direct_at_apply,
                "direct PUT did not reach command apply"
            );
        }

        let multipart_cluster = Arc::clone(&cluster);
        let multipart_thread = std::thread::spawn(move || {
            multipart_cluster.complete_multipart_upload_commit_serialized(multipart_req, 16)
        });

        {
            let (lock, cvar) = &*race_state;
            let state = lock.lock().unwrap_or_else(|e| e.into_inner());
            let (mut state, _) = cvar
                .wait_timeout_while(state, Duration::from_millis(100), |state| {
                    !state.multipart_at_apply
                })
                .unwrap();
            assert!(
                !state.multipart_at_apply,
                "multipart completion applied while direct PUT held the bucket command stream"
            );
            state.release_direct = true;
            cvar.notify_all();
        }

        let direct_outcome = direct_thread.join().unwrap().unwrap().unwrap();
        let multipart_outcome = multipart_thread.join().unwrap().unwrap();
        drop(hook_guard);

        assert_eq!(direct_outcome.version_id, crate::VersionId::from_u64(1));
        assert_eq!(multipart_outcome.version_id, crate::VersionId::from_u64(2));
        expected_multipart_segment.version_id = multipart_outcome.version_id.to_u64();

        for node_id in node_ids {
            let pg = map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(object_pg)
                .unwrap();
            let direct_version = crate::PgMetadataStore::get_object_version(
                &*pg,
                &bucket,
                &key,
                direct_outcome.version_id,
            )
            .unwrap();
            let direct_live = direct_version.as_live().unwrap();
            assert_eq!(direct_live.generation_id, direct_generation_id);
            assert!(direct_live.became_noncurrent_at.is_some());
        }
        assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
            &map,
            &node_ids,
            object_pg,
            &expected_multipart_req,
            &expected_multipart_segment,
            &multipart_outcome,
            2,
        );
        assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
    }

    #[test]
    fn stream_upload_part_staging_and_finalize_use_object_metadata_commands() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let upload_id = upload_id_from_label("streampartcmd");
        let create = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap()
            .unwrap();

        let session_id = crate::SessionId::try_from("43".repeat(16)).unwrap();
        let upload = cluster
            .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
            .unwrap();
        cluster
            .create_upload_part_stream_session(
                &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
                1,
                &session_id,
            )
            .unwrap();

        let payload = b"streamed multipart command part";
        let segment_okh = [0x43; 16];
        let (_target, segment) = cluster
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
        let written_shards = cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();

        let expected_part = cluster
            .finalize_upload_part_stream(&bucket, &key, &upload_id, &session_id, 1, |snapshot| {
                let generation = snapshot
                    .existing_part_generation
                    .map_or(0, |generation| generation + 1);
                let part = crate::MultipartPartRecord {
                    upload_id: upload_id.clone(),
                    part_number: 1,
                    generation,
                    size: payload.len() as u64,
                    etag: vec![0x55; 8],
                    etag_kind: crate::EtagKind::Crc64,
                    part_okh: [0u8; 16],
                    part_vid: crate::GenerationId::new(u64::from(generation) + 1).unwrap(),
                    ec_k: segment.ec_k,
                    ec_m: segment.ec_m,
                    last_modified: 123_456,
                    checksum: None,
                };
                let segments = snapshot
                    .staging_segments
                    .iter()
                    .map(|staged| crate::MultipartPartSegmentRecord {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        upload_id: upload_id.clone(),
                        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
                        part_number: 1,
                        segment_index: staged.segment_index,
                        size: staged.size,
                        segment_crc64: staged.segment_crc64,
                        segment_okh: staged.segment_okh,
                        segment_vid: staged.segment_vid,
                        data_pg_id: staged.data_pg_id,
                        ec_k: staged.ec_k,
                        ec_m: staged.ec_m,
                    })
                    .collect::<Vec<_>>();
                Ok::<_, ()>(crate::PreparedStreamPartCommit {
                    value: part.clone(),
                    part,
                    segments,
                })
            })
            .unwrap()
            .unwrap()
            .value;

        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        let expected_segments = vec![crate::MultipartPartSegmentRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
            part_number: 1,
            segment_index: segment.segment_index,
            size: segment.size,
            segment_crc64: segment.segment_crc64,
            segment_okh: segment.segment_okh,
            segment_vid: segment.segment_vid,
            data_pg_id: segment.data_pg_id,
            ec_k: segment.ec_k,
            ec_m: segment.ec_m,
        }];
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ));
            assert_eq!(
                crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
                expected_part
            );
            assert_eq!(
                crate::PgMetadataStore::get_multipart_part_segments_for_upload_part(
                    &*pg, &bucket, &key, &upload_id, 1
                )
                .unwrap(),
                expected_segments
            );
        }
    }

    fn stream_upload_part_create_rejects_raced_upload_state(target_state: crate::UploadState) {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let upload_id = upload_id_from_label("streamraced");
        let create = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: crate::ObjectEncryption::None,
        };
        cluster
            .create_multipart_upload(
                &bucket,
                &key,
                crate::BucketSnapshotRequest::default(),
                |_snapshot, existing_object| {
                    assert!(existing_object.is_none());
                    Ok::<_, ()>(((), create.clone()))
                },
            )
            .unwrap()
            .unwrap();

        let _serial = lock_metadata_command_apply_hook_test();
        let did_flip = Arc::new(AtomicBool::new(false));
        let hook_map = Arc::clone(&map);
        let hook_upload_id = upload_id.clone();
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let hook_node_ids = node_ids;
        let hook_did_flip = Arc::clone(&did_flip);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |_node_id, command| {
                if let MetadataCommandPayload::CreateStreamUpload(create) = command.payload() {
                    let is_target_upload = matches!(
                        &create.session.target,
                        crate::StreamUploadTarget::UploadPart { upload_id, .. }
                            if upload_id == &hook_upload_id
                    );
                    if create.session.bucket == hook_bucket
                        && create.session.key == hook_key
                        && is_target_upload
                        && !hook_did_flip.swap(true, Ordering::SeqCst)
                    {
                        for node_id in hook_node_ids {
                            let node = hook_map.node(node_id).unwrap().storage_node();
                            let pg = node.get_pg(object_pg).unwrap();
                            crate::PgMetadataStore::set_upload_state(
                                &*pg,
                                &hook_upload_id,
                                target_state,
                            )
                            .unwrap();
                            pg.refresh_metadata_command_state_digest().unwrap();
                        }
                    }
                }
                Ok(())
            },
        ));

        let session_id = crate::SessionId::try_from("44".repeat(16)).unwrap();
        let upload = cluster
            .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
            .unwrap();
        let err = cluster
            .create_upload_part_stream_session(
                &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
                1,
                &session_id,
            )
            .unwrap_err();
        drop(hook_guard);

        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Metadata(crate::MetadataError::NoSuchUpload { .. })
            ),
            "expected raced upload state to reject stream session creation, got {err:?}"
        );
        assert!(did_flip.load(Ordering::SeqCst));
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ));
        }
    }

    #[test]
    fn stream_upload_part_create_rejects_raced_multipart_abort() {
        stream_upload_part_create_rejects_raced_upload_state(crate::UploadState::Aborting);
    }

    #[test]
    fn stream_upload_part_create_rejects_raced_multipart_completion() {
        stream_upload_part_create_rejects_raced_upload_state(crate::UploadState::Completing);
    }

    #[test]
    fn multipart_completion_order_is_bucket_primary_serialized_across_object_pgs() {
        #[derive(Default)]
        struct CompletionRaceState {
            first_at_apply: bool,
            second_at_apply: bool,
            release_first: bool,
        }

        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key_a, key_b, object_pg_a, object_pg_b) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 0, "mpu-order-bucket-");
            let key_a = key_for_object_pg(topology, &bucket, 1, "mpu-order-a-");
            let key_b = key_for_object_pg(topology, &bucket, 2, "mpu-order-b-");
            (bucket, key_a, key_b, 1, 2)
        };
        set_route_primary(&mut map, object_pg_a, NodeId::new(1));
        set_route_primary(&mut map, object_pg_b, NodeId::new(2));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let (req_a, _) =
            seed_streamed_multipart_completion(&cluster, &bucket, &key_a, "bucketordera");
        let (req_b, _) =
            seed_streamed_multipart_completion(&cluster, &bucket, &key_b, "bucketorderb");

        let _serial = lock_metadata_command_apply_hook_test();
        let race_state = Arc::new((Mutex::new(CompletionRaceState::default()), Condvar::new()));
        let first_seen = Arc::new(AtomicBool::new(false));
        let second_seen = Arc::new(AtomicBool::new(false));
        let hook_key_a = key_a.clone();
        let hook_key_b = key_b.clone();
        let hook_race_state = Arc::clone(&race_state);
        let hook_first_seen = Arc::clone(&first_seen);
        let hook_second_seen = Arc::clone(&second_seen);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |_node_id, command| {
                let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload()
                else {
                    return Ok(());
                };
                let (lock, cvar) = &*hook_race_state;
                if commit.object.key == hook_key_a && !hook_first_seen.swap(true, Ordering::SeqCst)
                {
                    let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                    state.first_at_apply = true;
                    cvar.notify_all();
                    while !state.release_first {
                        state = cvar.wait(state).unwrap_or_else(|e| e.into_inner());
                    }
                } else if commit.object.key == hook_key_b
                    && !hook_second_seen.swap(true, Ordering::SeqCst)
                {
                    let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                    state.second_at_apply = true;
                    cvar.notify_all();
                }
                Ok(())
            },
        ));

        let cluster_a = Arc::clone(&cluster);
        let first = std::thread::spawn(move || {
            cluster_a.complete_multipart_upload_commit_serialized(req_a, 16)
        });

        {
            let (lock, cvar) = &*race_state;
            let state = lock.lock().unwrap_or_else(|e| e.into_inner());
            let (state, _) = cvar
                .wait_timeout_while(state, Duration::from_secs(5), |state| !state.first_at_apply)
                .unwrap();
            assert!(
                state.first_at_apply,
                "first completion did not reach command apply"
            );
        }

        let cluster_b = Arc::clone(&cluster);
        let second_upload_id = req_b.upload_id.clone();
        let second = std::thread::spawn(move || {
            cluster_b.complete_multipart_upload_commit_serialized(req_b, 16)
        });

        {
            let (lock, cvar) = &*race_state;
            let state = lock.lock().unwrap_or_else(|e| e.into_inner());
            let (mut state, _) = cvar
                .wait_timeout_while(state, Duration::from_millis(100), |state| {
                    !state.second_at_apply
                })
                .unwrap();
            state.release_first = true;
            cvar.notify_all();
        }

        let first_outcome = first.join().unwrap().unwrap();
        let second_outcome = second.join().unwrap().unwrap();
        drop(hook_guard);

        assert_eq!(first_outcome.version_id, crate::VersionId::Null);
        assert_eq!(second_outcome.version_id, crate::VersionId::Null);
        let mut orders = vec![
            completed_multipart_order_on_node(
                &map,
                NodeId::new(0),
                object_pg_a,
                &bucket,
                &upload_id_from_label("bucketordera"),
            ),
            completed_multipart_order_on_node(
                &map,
                NodeId::new(0),
                object_pg_b,
                &bucket,
                &second_upload_id,
            ),
        ];
        orders.sort_unstable();
        assert_eq!(orders, vec![1, 2]);

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let bucket_pg = node
                .get_pg(node.pg_topology().bucket_pg_for(&bucket))
                .unwrap();
            assert_eq!(
                bucket_pg
                    .completed_multipart_upload_sequence_for_bucket(&bucket)
                    .unwrap(),
                2,
                "node {node_id:?} did not catch up bucket completed MPU order"
            );
        }
    }

    #[test]
    fn multipart_completion_zero_apply_failure_retains_pending_command_for_retry() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let (req, expected_segment) =
            seed_streamed_multipart_completion(&cluster, &bucket, &key, "zerofailcomplete");

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::CommitMultipartObject(commit)
                        if commit.object.bucket == hook_bucket
                            && commit.object.key == hook_key
                            && node_id == NodeId::new(0)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected multipart completion command apply failure",
                            source: std::io::Error::other(
                                "injected multipart completion command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .complete_multipart_upload_commit_serialized(req.clone(), 16)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected multipart completion command apply failure",
                    ..
                })
            ),
            "expected injected zero-apply failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(!fail_once.load(Ordering::SeqCst));
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "zero-apply multipart completion failure must keep its pending command"
        );
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
        }

        let outcome = cluster
            .complete_multipart_upload_commit_serialized(req.clone(), 16)
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        assert_streamed_multipart_completion_on_acting_nodes(
            &map,
            &node_ids,
            object_pg,
            &req,
            &expected_segment,
            &outcome,
        );
    }

    #[test]
    fn object_delete_metadata_command_applies_to_all_acting_object_pg_nodes() {
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
        let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"delete me");

        let outcome = cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
        assert!(matches!(
            outcome.deleted,
            crate::DeletedCurrentObject::Live {
                generation_id,
                ..
            } if generation_id == committed.generation_id
        ));

        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
            assert!(
                crate::PgMetadataStore::payload_reclaim_exists(
                    &*pg,
                    &bucket,
                    &key,
                    committed.generation_id
                )
                .unwrap(),
                "delete command should publish reclaim metadata on node {node_id:?}"
            );
        }
    }

    #[test]
    fn object_delete_metadata_command_retry_reuses_pending_partial_replica_command() {
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
        let committed =
            write_committed_direct_segment_for(&cluster, &bucket, &key, b"partial delete");

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::DeleteObjectVersion(delete)
                        if delete.bucket == hook_bucket
                            && delete.key == hook_key
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected object delete metadata command apply failure",
                            source: std::io::Error::other(
                                "injected object delete metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected object delete metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(!fail_once.load(Ordering::SeqCst));
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial object delete metadata command must remain pending"
        );
        for node_id in [NodeId::new(0), NodeId::new(2)] {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
        }
        {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            let pg = primary.get_pg(object_pg).unwrap();
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
            assert_eq!(
                stored.as_live().unwrap().generation_id,
                committed.generation_id
            );
        }

        let outcome = cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
        assert!(matches!(
            outcome.deleted,
            crate::DeletedCurrentObject::Live {
                generation_id,
                ..
            } if generation_id == committed.generation_id
        ));
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
            assert!(crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap());
        }
    }

    #[test]
    fn completed_multipart_order_drains_same_pg_object_command_with_cleanup_hooks() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, pg_id) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            let bucket = bucket_for_pg(topology, 1, "mpu-order-drains-object-");
            let key = key_for_object_pg(topology, &bucket, 1, "same-pg-key-");
            (bucket, key, 1)
        };
        set_route_primary(&mut map, pg_id, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let committed = write_committed_direct_segment_for(
            &cluster,
            &bucket,
            &key,
            b"same pg object command cleanup",
        );
        assert!(cluster.try_take_reclaim_work().is_none());

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::DeleteObjectVersion(delete)
                        if delete.bucket == hook_bucket
                            && delete.key == hook_key
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected same-pg object delete apply failure",
                            source: std::io::Error::other(
                                "injected same-pg object delete apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected same-pg object delete apply failure",
                    ..
                })
            ),
            "expected injected delete failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(!fail_once.load(Ordering::SeqCst));
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(pg_id), &bucket)
            .is_some());
        assert!(cluster.try_take_reclaim_work().is_none());

        let completion_order = cluster
            .test_reserve_completed_multipart_upload_order(&bucket)
            .unwrap();
        assert_eq!(completion_order, 1);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(pg_id), &bucket)
            .is_none());
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
        assert!(cluster.try_take_reclaim_work().is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(pg_id).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
            assert!(crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap());
            let info =
                crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.completed_multipart_upload_sequence, completion_order);
        }
    }

    #[test]
    fn object_metadata_update_commands_apply_to_all_acting_object_pg_nodes() {
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
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"object metadata");
        let tags =
            "<Tagging><TagSet><Tag><Key>tier</Key><Value>hot</Value></Tag></TagSet></Tagging>";
        let retention = crate::ObjectRetention {
            mode: crate::ObjectLockMode::Governance,
            retain_until_unix_seconds: 123_456,
        };
        let acl_grants = crate::AclGrants::default();

        let tagged_version = cluster
            .put_object_tags_if(&bucket, &key, None, tags, |stored| {
                Ok::<_, ()>(stored.version_id())
            })
            .unwrap()
            .unwrap();
        assert_eq!(tagged_version, crate::VersionId::Null);
        cluster
            .put_object_retention_if(&bucket, &key, None, retention, |stored| {
                Ok::<_, ()>(stored.version_id())
            })
            .unwrap()
            .unwrap();
        cluster
            .put_object_legal_hold_if(
                &bucket,
                &key,
                None,
                crate::StoredLegalHoldStatus::On,
                |stored| Ok::<_, ()>(stored.version_id()),
            )
            .unwrap()
            .unwrap();
        let acl_version = cluster
            .put_object_acl_if(&bucket, &key, None, |stored| {
                Ok::<_, ()>((stored.version_id(), acl_grants.clone(), true))
            })
            .unwrap()
            .unwrap();
        assert_eq!(acl_version, crate::VersionId::Null);

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let stored = crate::PgMetadataStore::get_object_version(
                &*pg,
                &bucket,
                &key,
                crate::VersionId::Null,
            )
            .unwrap();
            let live = stored.as_live().unwrap();
            assert_eq!(live.tags.as_ref().map(|tags| tags.as_str()), Some(tags));
            assert_eq!(live.object_lock.retention, Some(retention));
            assert_eq!(
                live.object_lock.legal_hold,
                crate::StoredLegalHoldStatus::On
            );
            assert_eq!(live.acl_grants, acl_grants);
            assert!(live.public_read);
        }

        cluster
            .delete_object_tags_if(&bucket, &key, None, |stored| {
                Ok::<_, ()>(stored.version_id())
            })
            .unwrap()
            .unwrap();
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert_eq!(
                crate::PgMetadataStore::get_object_tags(
                    &*pg,
                    &bucket,
                    &key,
                    crate::VersionId::Null,
                )
                .unwrap(),
                None
            );
        }
    }

    #[test]
    fn object_metadata_update_retry_converges_pending_partial_replica_command() {
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
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"partial object metadata");
        let tags =
            "<Tagging><TagSet><Tag><Key>retry</Key><Value>yes</Value></Tag></TagSet></Tagging>";
        fn require_tags_absent(
            stored: &crate::StoredObject,
        ) -> Result<crate::VersionId, &'static str> {
            if stored.as_live().unwrap().tags.is_some() {
                Err("tags already visible before pending command convergence")
            } else {
                Ok(stored.version_id())
            }
        }

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let hook_bucket = bucket.clone();
        let hook_key = key.clone();
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::PutObjectMetadata(update)
                        if update.object.bucket == hook_bucket
                            && update.object.key == hook_key
                            && node_id == NodeId::new(1)
                            && fail_once_hook.swap(false, Ordering::SeqCst) =>
                    {
                        return Err(StoreError::Io {
                            context: "injected object metadata command apply failure",
                            source: std::io::Error::other(
                                "injected object metadata command apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .put_object_tags_if(&bucket, &key, None, tags, require_tags_absent)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "injected object metadata command apply failure",
                    ..
                })
            ),
            "expected injected primary failure, got {err:?}"
        );
        drop(hook_guard);
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_some(),
            "partial object metadata command must remain pending"
        );
        for node_id in [NodeId::new(0), NodeId::new(2)] {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert_eq!(
                crate::PgMetadataStore::get_object_tags(
                    &*pg,
                    &bucket,
                    &key,
                    crate::VersionId::Null,
                )
                .unwrap()
                .as_deref(),
                Some(tags)
            );
        }
        {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            let pg = primary.get_pg(object_pg).unwrap();
            assert_eq!(
                crate::PgMetadataStore::get_object_tags(
                    &*pg,
                    &bucket,
                    &key,
                    crate::VersionId::Null,
                )
                .unwrap(),
                None
            );
        }

        cluster
            .put_object_tags_if(&bucket, &key, None, tags, require_tags_absent)
            .unwrap()
            .unwrap();
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert_eq!(
                crate::PgMetadataStore::get_object_tags(
                    &*pg,
                    &bucket,
                    &key,
                    crate::VersionId::Null,
                )
                .unwrap()
                .as_deref(),
                Some(tags)
            );
        }
    }

    #[test]
    fn object_metadata_retry_rejects_same_mutation_with_mismatched_post_image() {
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
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"metadata mismatch");
        let tags =
            "<Tagging><TagSet><Tag><Key>retry</Key><Value>no</Value></Tag></TagSet></Tagging>";

        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
            .unwrap()
            .into_live()
            .unwrap();
        drop(pg);
        let mut mismatched_post_image = stored.clone();
        mismatched_post_image.tags = Some(crate::SerializedTagSet::new(tags.to_string()));
        mismatched_post_image.public_read = !stored.public_read;
        let pg_id = PgId::new(object_pg);
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                map.runtime_state().next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                object: mismatched_post_image,
            })),
        );
        map.runtime_state()
            .try_set_pending_metadata_command_for_bucket(pg_id, &bucket, command)
            .unwrap();

        let err = cluster
            .put_object_tags_if(&bucket, &key, None, tags, |stored| {
                Ok::<_, ()>(stored.version_id())
            })
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "conflicting pending command for object metadata update",
                    ..
                })
            ),
            "expected conflicting post-image error, got {err:?}"
        );
        let pg = primary.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().tags, None);
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(pg_id, &bucket)
            .is_some());
    }

    #[test]
    fn object_metadata_command_rejects_non_metadata_post_image_mismatch() {
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
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"metadata apply mismatch");
        let tags =
            "<Tagging><TagSet><Tag><Key>apply</Key><Value>no</Value></Tag></TagSet></Tagging>";
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let mut post_image = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
            .unwrap()
            .into_live()
            .unwrap();
        drop(pg);
        post_image.tags = Some(crate::SerializedTagSet::new(tags.to_string()));
        post_image.size += 1;

        let pg_id = PgId::new(object_pg);
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                map.runtime_state().next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::PutObjectMetadata(Box::new(PutObjectMetadataCommand {
                object: post_image,
            })),
        );
        let err = cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
                    context: "put object metadata command preimage mismatch",
                    ..
                })
            ),
            "expected preimage mismatch, got {err:?}"
        );
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
            assert_eq!(stored.as_live().unwrap().tags, None);
        }
    }

    #[test]
    fn lifecycle_current_expiration_delete_command_applies_to_all_acting_object_pg_nodes() {
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
        create_test_bucket(&cluster, &bucket);
        put_test_lifecycle(&cluster, &bucket);
        let committed =
            write_committed_direct_segment_for(&cluster, &bucket, &key, b"expired current");

        let outcome = cluster
            .expire_current_object_if_due(&bucket, &key, committed.version_id, |raw, record| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert_eq!(record.generation_id, committed.generation_id);
                Ok::<_, ()>(true)
            })
            .unwrap()
            .unwrap()
            .expect("current object should expire");
        assert_eq!(outcome.reclaim_generation_id, Some(committed.generation_id));
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
        assert!(cluster.try_take_reclaim_work().is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ));
            assert!(crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap());
        }
    }

    #[test]
    fn lifecycle_suspended_current_expiration_replaces_null_live_on_all_acting_nodes() {
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
        create_test_bucket_with_versioning(
            &cluster,
            &bucket,
            crate::BucketVersioningState::Suspended,
        );
        put_test_lifecycle(&cluster, &bucket);
        let committed =
            write_committed_direct_segment_for(&cluster, &bucket, &key, b"suspended current");
        assert_eq!(committed.version_id, crate::VersionId::Null);

        let outcome = cluster
            .expire_current_object_if_due(&bucket, &key, committed.version_id, |raw, record| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert_eq!(record.generation_id, committed.generation_id);
                Ok::<_, ()>(true)
            })
            .unwrap()
            .unwrap()
            .expect("suspended null live object should expire");
        assert_eq!(outcome.reclaim_generation_id, Some(committed.generation_id));
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
        assert!(cluster.try_take_reclaim_work().is_none());

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let stored = crate::PgMetadataStore::get_object_version(
                &*pg,
                &bucket,
                &key,
                crate::VersionId::Null,
            )
            .unwrap();
            assert!(matches!(stored, crate::StoredObject::DeleteMarker(_)));
            assert!(crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap());
            assert!(
                crate::PgMetadataStore::get_object_segments(
                    &*pg,
                    &bucket,
                    &key,
                    crate::VersionId::Null,
                )
                .unwrap()
                .is_empty(),
                "null live segment rows should be removed on node {node_id:?}"
            );
        }
    }

    #[test]
    fn lifecycle_enabled_current_expiration_reserves_delete_marker_version() {
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
        create_test_bucket_with_versioning(
            &cluster,
            &bucket,
            crate::BucketVersioningState::Enabled,
        );
        put_test_lifecycle(&cluster, &bucket);
        let committed = write_committed_direct_segment_for_with_versioning(
            &cluster,
            &bucket,
            &key,
            crate::BucketVersioningState::Enabled,
            [0x77; 16],
            [0x78; 16],
            b"enabled lifecycle current",
        );
        assert_eq!(committed.version_id, crate::VersionId::from_u64(1));

        let outcome = cluster
            .expire_current_object_if_due(&bucket, &key, committed.version_id, |raw, record| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert_eq!(record.generation_id, committed.generation_id);
                Ok::<_, ()>(true)
            })
            .unwrap()
            .unwrap()
            .expect("enabled current live object should expire");
        assert_eq!(outcome.reclaim_generation_id, None);
        assert!(cluster.try_take_reclaim_work().is_none());

        assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let current = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
            let crate::StoredObject::DeleteMarker(marker) = current else {
                panic!("expected current delete marker on node {node_id:?}, got {current:?}");
            };
            assert_eq!(marker.version_id, crate::VersionId::from_u64(2));

            let stored_live = crate::PgMetadataStore::get_object_version(
                &*pg,
                &bucket,
                &key,
                committed.version_id,
            )
            .unwrap();
            let live = stored_live.as_live().unwrap();
            assert_eq!(live.generation_id, committed.generation_id);
            assert!(live.became_noncurrent_at.is_some());
            assert!(
                !crate::PgMetadataStore::payload_reclaim_exists(
                    &*pg,
                    &bucket,
                    &key,
                    committed.generation_id,
                )
                .unwrap(),
                "enabled current expiration should not reclaim the preserved live version"
            );
        }
    }

    #[test]
    fn lifecycle_noncurrent_and_delete_marker_expiration_use_object_commands() {
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
        create_test_bucket_with_versioning(
            &cluster,
            &bucket,
            crate::BucketVersioningState::Enabled,
        );
        put_test_lifecycle(&cluster, &bucket);
        let older = write_committed_direct_segment_for_with_versioning(
            &cluster,
            &bucket,
            &key,
            crate::BucketVersioningState::Enabled,
            [11; 16],
            [51; 16],
            b"older version",
        );
        let current = write_committed_direct_segment_for_with_versioning(
            &cluster,
            &bucket,
            &key,
            crate::BucketVersioningState::Enabled,
            [12; 16],
            [52; 16],
            b"current version",
        );

        let reclaimed = cluster
            .delete_noncurrent_live_versions_if_due(&bucket, &key, |raw, versions| {
                assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                assert!(versions
                    .iter()
                    .any(|stored| stored.version_id() == older.version_id));
                Ok::<_, ()>(HashSet::from([older.version_id]))
            })
            .unwrap()
            .unwrap();
        assert_eq!(reclaimed, vec![older.generation_id]);

        let marker = cluster
            .insert_current_delete_marker_if(
                &bucket,
                &key,
                crate::OwnerIdentity::from_principal("owner"),
                |_| Ok::<_, ()>(()),
            )
            .unwrap()
            .unwrap();

        let deleted_marker = cluster
            .delete_expired_delete_marker_if_due(
                &bucket,
                &key,
                marker.version_id,
                |raw, versions| {
                    assert_eq!(raw, Some("<LifecycleConfiguration/>"));
                    assert!(versions.iter().any(|stored| {
                        stored.version_id() == marker.version_id
                            && matches!(stored, crate::StoredObject::DeleteMarker(_))
                    }));
                    Ok::<_, ()>(true)
                },
            )
            .unwrap()
            .unwrap();
        assert!(deleted_marker);

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(matches!(
                crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, older.version_id,),
                Err(crate::MetadataError::ObjectNotFound)
            ));
            assert!(crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                older.generation_id
            )
            .unwrap());
            assert!(matches!(
                crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, marker.version_id,),
                Err(crate::MetadataError::ObjectNotFound)
            ));
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
            assert_eq!(stored.version_id(), current.version_id);
        }
    }

    #[test]
    fn insert_delete_marker_metadata_command_applies_to_all_acting_object_pg_nodes() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, object_pg, _) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        set_route_primary(&mut map, object_pg, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let owner = crate::OwnerIdentity::from_principal("owner");

        let marker = cluster
            .insert_current_delete_marker_if(&bucket, &key, owner.clone(), |stored| {
                assert!(stored.is_none());
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
        assert_eq!(marker.version_id, crate::VersionId::from_u64(1));

        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
            match stored {
                crate::StoredObject::DeleteMarker(record) => {
                    assert_eq!(record.version_id, marker.version_id);
                    assert_eq!(record.owner, owner);
                }
                other => panic!("expected delete marker on node {node_id:?}, got {other:?}"),
            }
        }
        assert_object_version_counter_on_acting_nodes(
            &map,
            &node_ids,
            object_pg,
            &bucket,
            &key,
            marker.version_id.to_u64() + 1,
        );
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
    fn payload_reclaim_in_progress_blocks_new_payload_leases() {
        let _serial = lock_payload_cleanup_hook_test();
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
            write_committed_direct_segment_for(&cluster, &bucket, &key, b"reclaim race payload");
        cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();

        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let hook_gate = Arc::clone(&gate);
        let _hook_guard =
            cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |_| {
                let (lock, cv) = &*hook_gate;
                let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                if !state.0 {
                    state.0 = true;
                    cv.notify_all();
                    while !state.1 {
                        state = cv.wait(state).unwrap_or_else(|e| e.into_inner());
                    }
                }
                Ok(())
            }));

        let reclaim_cluster = Arc::clone(&cluster);
        let reclaim_bucket = bucket.clone();
        let reclaim_key = key.clone();
        let reclaim_generation_id = committed.generation_id;
        let reclaim_thread = std::thread::spawn(move || {
            reclaim_cluster.reclaim_object_payload_if_unleased(
                &reclaim_bucket,
                &reclaim_key,
                reclaim_generation_id,
            )
        });

        let (lock, cv) = &*gate;
        let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
        while !state.0 {
            state = cv.wait(state).unwrap_or_else(|e| e.into_inner());
        }
        let err = match cluster.acquire_object_payload_lease(&bucket, &key, committed.generation_id)
        {
            Ok(_) => panic!("new lease acquired after payload reclaim started"),
            Err(error) => error,
        };
        assert!(
            matches!(err, crate::StoreError::NotFound),
            "new leases must be rejected once reclaim starts deleting payload, got {err:?}"
        );
        state.1 = true;
        cv.notify_all();
        drop(state);

        assert!(reclaim_thread.join().unwrap().unwrap());
        assert!(!cluster
            .payload_reclaim_exists(&bucket, &key, committed.generation_id)
            .unwrap());
    }

    #[test]
    fn reclaim_payload_cleanup_failure_keeps_payload_lease_fence_until_retry() {
        let _serial = lock_payload_cleanup_hook_test();
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

        let committed =
            write_committed_direct_segment_for(&cluster, &bucket, &key, b"cleanup fence payload");
        cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();

        let failed_ack_delete = Arc::new(AtomicBool::new(false));
        let failed_ack_delete_hook = Arc::clone(&failed_ack_delete);
        let hook_guard = cluster.test_install_before_metadata_primary_payload_ack_delete_hook(
            Arc::new(move |_shard_key| {
                if !failed_ack_delete_hook.swap(true, Ordering::SeqCst) {
                    return Err(crate::StoreError::Io {
                        context: "injected reclaim ack delete failure",
                        source: std::io::Error::other("injected reclaim ack delete failure"),
                    });
                }
                Ok(())
            }),
        );
        let err = cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::ObjectPgActionError::Store(crate::StoreError::Io {
                context: "injected reclaim ack delete failure",
                ..
            })
        ));
        assert!(failed_ack_delete.load(Ordering::SeqCst));
        assert!(
            map.runtime_state()
                .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
                .is_none(),
            "ack cleanup failure happens before the reclaim metadata delete command is installed"
        );
        assert!(cluster
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
                "placed shard {shard_index} should already be deleted before ack cleanup fails"
            );
        }

        let err = match cluster.acquire_object_payload_lease(&bucket, &key, committed.generation_id)
        {
            Ok(_) => panic!("new lease acquired after payload cleanup failed mid-reclaim"),
            Err(error) => error,
        };
        assert!(
            matches!(err, crate::StoreError::NotFound),
            "failed mid-reclaim cleanup must keep leases fenced until retry converges, got {err:?}"
        );
        drop(hook_guard);

        assert!(cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap());
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        assert!(!cluster
            .payload_reclaim_exists(&bucket, &key, committed.generation_id)
            .unwrap());
    }

    #[test]
    fn reclaim_payload_metadata_delete_applies_to_object_pg_acting_set() {
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

        let committed =
            write_committed_direct_segment_for(&cluster, &bucket, &key, b"reclaim payload");
        cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(
                crate::PgMetadataStore::payload_reclaim_exists(
                    &*pg,
                    &bucket,
                    &key,
                    committed.generation_id,
                )
                .unwrap(),
                "delete should publish reclaim metadata on node {node_id:?}"
            );
        }

        assert!(cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap());
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(
                !crate::PgMetadataStore::payload_reclaim_exists(
                    &*pg,
                    &bucket,
                    &key,
                    committed.generation_id,
                )
                .unwrap(),
                "reclaim command should delete metadata on node {node_id:?}"
            );
        }
        for shard_index in 0..committed.written.ec.k + committed.written.ec.m {
            assert!(!cluster
                .test_payload_shard_file_exists(
                    committed.written.data_pg_id,
                    committed.written.ec,
                    &committed.segment_okh,
                    committed.generation_id,
                    shard_index,
                )
                .unwrap());
        }
    }

    #[test]
    fn reclaim_payload_metadata_delete_retry_reuses_pending_partial_command() {
        let _serial = lock_metadata_command_apply_hook_test();
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

        let committed =
            write_committed_direct_segment_for(&cluster, &bucket, &key, b"retry payload");
        cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();

        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            |node_id, command| {
                if matches!(
                    command.payload(),
                    MetadataCommandPayload::DeleteObjectPayloadReclaim(_)
                ) && node_id == NodeId::new(2)
                {
                    return Err(crate::StoreError::Io {
                        context: "injected reclaim metadata command apply failure",
                        source: std::io::Error::other(
                            "injected reclaim metadata command apply failure",
                        ),
                    });
                }
                Ok(())
            },
        ));
        let err = cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::ObjectPgActionError::Store(crate::StoreError::Io {
                context: "injected reclaim metadata command apply failure",
                ..
            })
        ));
        let pending = map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .expect("partial reclaim metadata delete must keep pending command");
        assert!(matches!(
            pending.payload(),
            MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
                if delete.matches_request(&bucket, &key, committed.generation_id)
        ));
        let err = match cluster.acquire_object_payload_lease(&bucket, &key, committed.generation_id)
        {
            Ok(_) => panic!("new lease acquired while reclaim metadata delete was pending"),
            Err(error) => error,
        };
        assert!(
            matches!(err, crate::StoreError::NotFound),
            "pending reclaim metadata delete must fence new leases after payload deletion starts, got {err:?}"
        );
        for (node_id, expected_exists) in [
            (NodeId::new(0), false),
            (NodeId::new(1), true),
            (NodeId::new(2), true),
        ] {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert_eq!(
                crate::PgMetadataStore::payload_reclaim_exists(
                    &*pg,
                    &bucket,
                    &key,
                    committed.generation_id,
                )
                .unwrap(),
                expected_exists,
                "partial apply state mismatch on node {node_id:?}"
            );
        }
        drop(hook_guard);

        assert!(cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap());
        assert!(map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(object_pg), &bucket)
            .is_none());
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(object_pg).unwrap();
            assert!(!crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id,
            )
            .unwrap());
        }
        let lease = cluster
            .acquire_object_payload_lease(&bucket, &key, committed.generation_id)
            .expect("converged reclaim metadata delete must clear the in-memory lease fence");
        drop(lease);
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
        let bridge_pg_a = bridge_node.get_pg(1).unwrap();
        crate::PgMetadataStore::delete_object_meta(&*bridge_pg_a, &bucket, &key_a).unwrap();
        drop(bridge_pg_a);
        let bridge_pg_b = bridge_node.get_pg(2).unwrap();
        crate::PgMetadataStore::delete_object_meta(&*bridge_pg_b, &bucket, &key_b).unwrap();
        drop(bridge_pg_b);
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
            .list_object_versions_for_bucket(&bucket, None, None, None, None, 100)
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
        assert_bucket_execution_counter_on_acting_nodes(
            &map,
            &node_ids,
            1,
            created.bucket_execution_generation,
        );

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
                        if create.bucket.name == hook_bucket
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
                        if versioning.bucket.name == hook_bucket
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
                        if versioning.bucket.name == hook_bucket
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
                if versioning.bucket.name == bucket
                    && versioning.bucket.versioning == crate::BucketVersioningState::Enabled
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
                        if acl.bucket.name == hook_bucket
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
    fn bucket_acl_drains_pending_completed_multipart_sequence_command() {
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
            bucket_for_pg(topology, 1, "acl-drains-mpu-sequence-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);

        let pg_id = PgId::new(1);
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                crate::ClusterEpoch::INITIAL,
                pg_id,
                map.runtime_state().next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
                AdvanceCompletedMultipartUploadSequenceCommand {
                    bucket: bucket.clone(),
                    completion_order: 7,
                },
            ),
        );
        map.runtime_state()
            .try_set_pending_metadata_command_for_bucket(pg_id, &bucket, command.clone())
            .unwrap();

        let _serial = lock_metadata_command_apply_hook_test();
        let apply_count = Arc::new(AtomicUsize::new(0));
        let hook_bucket = bucket.clone();
        let apply_count_hook = Arc::clone(&apply_count);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |_node_id, command| {
                match command.payload() {
                    MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(advance)
                        if advance.bucket == hook_bucket
                            && apply_count_hook.fetch_add(1, Ordering::SeqCst) == 1 =>
                    {
                        return Err(StoreError::Io {
                            context: "injected completed multipart sequence apply failure",
                            source: std::io::Error::other(
                                "injected completed multipart sequence apply failure",
                            ),
                        });
                    }
                    _ => {}
                }
                Ok(())
            },
        ));

        let err = cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "injected completed multipart sequence apply failure",
                    ..
                })
            ),
            "expected injected sequence apply failure, got {err:?}"
        );
        drop(hook_guard);

        let acl_grants = crate::AclGrants::default();
        let updated = cluster
            .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
            .unwrap();
        assert!(updated.public_read);
        assert!(!updated.public_write);

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info =
                crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.acl_grants, updated.acl_grants);
            assert!(info.public_read);
            assert!(!info.public_write);
            assert_eq!(info.completed_multipart_upload_sequence, 7);
        }
    }

    #[test]
    fn existing_create_bucket_preserves_pending_acl_command_for_retry() {
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
            bucket_for_pg(topology, 1, "partial-acl-create-exists-")
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
                        if acl.bucket.name == hook_bucket
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

        let pending_before = map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(1), &bucket)
            .expect("failed ACL command should remain pending");
        assert!(matches!(
            pending_before.payload(),
            MetadataCommandPayload::PutBucketAcl(acl)
                if acl.bucket.name == bucket && acl.bucket.public_read && !acl.bucket.public_write
        ));
        let partial_info = {
            let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = applied_replica.get_pg(1).unwrap();
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
        };
        assert!(partial_info.public_read);
        assert!(!partial_info.public_write);
        let primary_info = {
            let primary = map.node(NodeId::new(1)).unwrap().storage_node();
            let pg = primary.get_pg(1).unwrap();
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
        };
        assert!(!primary_info.public_read);
        assert!(!primary_info.public_write);

        let attacker_owner = crate::CanonicalUserId::from_principal("attacker");
        let exists = cluster
            .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: "attacker",
                owner_canonical_id: &attacker_owner,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: crate::BucketVersioningState::Disabled,
                object_lock: crate::BucketObjectLockConfig::default(),
            })
            .unwrap();
        assert!(matches!(
            exists,
            crate::BucketCreateAttemptOutcome::Exists(info)
                if info.owner_principal == "owner"
                    && info.owner_canonical_id
                        == crate::CanonicalUserId::from_principal("owner")
        ));

        let pending_after = map
            .runtime_state()
            .pending_metadata_command_for_bucket(PgId::new(1), &bucket)
            .expect("existing CreateBucket must not drop the pending ACL command");
        assert_eq!(pending_after.id(), pending_before.id());
        assert_eq!(pending_after.payload(), pending_before.payload());

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
            assert!(info.public_read);
            assert!(!info.public_write);
            assert_eq!(
                info.bucket_execution_generation,
                partial_info.bucket_execution_generation
            );
        }
    }

    #[test]
    fn bucket_acl_retry_rejects_same_acl_with_mismatched_post_image() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "acl-post-image-conflict-")
        };

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let acl_grants = crate::AclGrants::default();
        let current = {
            let node = map.node(NodeId::new(0)).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            crate::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap()
        };
        let mut update = PutBucketAclCommand::from_bucket(
            current.with_execution_generation(77),
            acl_grants.clone(),
            true,
            false,
        );
        update.bucket.bucket_policy_public = true;
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                cluster.operation_epoch(),
                PgId::new(1),
                MetadataCommandLogIndex::new(77).unwrap(),
            ),
            MetadataCommandPayload::PutBucketAcl(update),
        );
        map.runtime_state()
            .try_set_pending_metadata_command_for_bucket(PgId::new(1), &bucket, command)
            .unwrap();

        let err = cluster
            .put_bucket_acl_and_load_info(&bucket, &acl_grants, true, false)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::Io {
                    context: "conflicting pending put bucket acl command",
                    ..
                })
            ),
            "expected conflicting pending ACL command, got {err:?}"
        );
        let info = cluster.head_bucket_info(&bucket).unwrap();
        assert!(!info.public_read);
        assert!(!info.bucket_policy_public);
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
                        if property.bucket.name == hook_bucket
                            && property.effect == expected_mutation.effect()
                            && property.bucket.public_access_block == Some(public_access_block)
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
                        if versioning.bucket.name == hook_bucket
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
        let deleting_generation = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .test_head_bucket_raw(&bucket)
            .unwrap()
            .bucket_execution_generation;
        assert!(deleting_generation > pre_delete_generation);
        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(1).unwrap();
            let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
            assert_eq!(info.state, crate::BucketState::Deleting);
            assert_eq!(info.bucket_execution_generation, deleting_generation);
        }
        assert_bucket_execution_counter_on_acting_nodes(&map, &node_ids, 1, deleting_generation);

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
    fn finalized_bucket_delete_fails_closed_on_active_replica() {
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
            bucket_for_pg(topology, 1, "delete-diverged-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));

        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        cluster.begin_bucket_delete(&bucket).unwrap();

        let divergent_node = map.node(NodeId::new(0)).unwrap().storage_node();
        let divergent_pg = divergent_node.get_pg(1).unwrap();
        crate::PgMetadataStore::delete_finalized_bucket(&*divergent_pg, &bucket).unwrap();
        divergent_pg
            .refresh_metadata_command_state_digest()
            .unwrap();
        crate::PgMetadataStore::create_bucket(
            &*divergent_pg,
            &bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        divergent_pg
            .refresh_metadata_command_state_digest()
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*divergent_pg, &bucket)
                .unwrap()
                .state,
            crate::BucketState::Active
        );
        drop(divergent_pg);

        let err = cluster.try_finalize_bucket_delete(&bucket).unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Metadata(
                    crate::MetadataError::BucketNotFinalizedForDelete {
                        state: crate::BucketState::Active
                    }
                )
            ),
            "expected active replica to fail finalized delete, got {err:?}"
        );

        let divergent_pg = divergent_node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*divergent_pg, &bucket)
                .unwrap()
                .state,
            crate::BucketState::Active
        );
        let primary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket)
                .unwrap()
                .state,
            crate::BucketState::Deleting
        );
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
        let post_prune_bucket = bucket_for_pg(topology, 1, "post-prune-");
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
        drop(node_one_pg);
        drop(node_two_pg);

        create_test_bucket(&cluster, &post_prune_bucket);
    }

    #[test]
    fn completed_multipart_prune_partial_command_retries_and_preserves_digest() {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
        let bucket =
            crate::BucketName::try_from("completed-prune-retry-bucket".to_string()).unwrap();
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let key = key_for_object_pg(topology, &bucket, 1, "retry-");
        let post_prune_bucket = bucket_for_pg(topology, 1, "post-prune-retry-");
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

        let upload_id = upload_id_from_label("retryCompleted");
        for node_id in node_ids {
            seed_completed_multipart_upload_record(&map, node_id, 1, &bucket, &key, &upload_id, 1);
        }

        let _serial = lock_metadata_command_apply_hook_test();
        let fail_once = Arc::new(AtomicBool::new(true));
        let fail_once_hook = Arc::clone(&fail_once);
        let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
            move |node_id, command| {
                if matches!(
                    command.payload(),
                    MetadataCommandPayload::DeleteCompletedMultipartUpload(_)
                ) && node_id == NodeId::new(2)
                    && fail_once_hook.swap(false, Ordering::SeqCst)
                {
                    return Err(StoreError::Io {
                        context: "injected completed multipart prune failure",
                        source: std::io::Error::other("injected completed multipart prune failure"),
                    });
                }
                Ok(())
            },
        ));

        let err = cluster
            .prune_completed_multipart_uploads_for_bucket_with_limit(&bucket, 0)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::Io { .. })
            ),
            "expected injected partial prune failure, got {err:?}"
        );
        assert!(
            crate::PgMetadataStore::get_completed_multipart_upload(
                &*map
                    .node(NodeId::new(0))
                    .unwrap()
                    .storage_node()
                    .get_pg(1)
                    .unwrap(),
                &upload_id,
            )
            .unwrap()
            .is_none(),
            "first applied replica should have deleted the tombstone"
        );
        assert!(
            crate::PgMetadataStore::get_completed_multipart_upload(
                &*map
                    .node(NodeId::new(2))
                    .unwrap()
                    .storage_node()
                    .get_pg(1)
                    .unwrap(),
                &upload_id,
            )
            .unwrap()
            .is_some(),
            "failed replica should still have the tombstone before retry"
        );
        drop(hook_guard);

        cluster
            .prune_completed_multipart_uploads_for_bucket_with_limit(&bucket, 0)
            .unwrap();
        for node_id in node_ids {
            assert!(
                crate::PgMetadataStore::get_completed_multipart_upload(
                    &*map.node(node_id).unwrap().storage_node().get_pg(1).unwrap(),
                    &upload_id,
                )
                .unwrap()
                .is_none(),
                "retry should delete tombstone on node {node_id:?}"
            );
        }
        create_test_bucket(&cluster, &post_prune_bucket);
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
        assert!(bridge_node.test_get_object_meta(&bucket, &live_key).is_ok());

        let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
            ),
            "routed non-empty bucket should reject delete, got {err:?}"
        );

        for node_id in node_ids {
            let node = map.node(node_id).unwrap().storage_node();
            let pg = node.get_pg(2).unwrap();
            crate::PgMetadataStore::delete_object_meta(&*pg, &bucket, &live_key).unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
        }

        let node_two = map.node(NodeId::new(2)).unwrap().storage_node();
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
        node_two_pg.refresh_metadata_command_state_digest().unwrap();
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
    fn payload_placement_and_shard_io_reject_all_non_active_pg_states() {
        let non_active_states = [
            PgState::Peering,
            PgState::Degraded,
            PgState::Backfilling,
            PgState::Inconsistent,
        ];

        for state in non_active_states {
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
            let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
            map.pg_routes.get_mut(&PgId::new(0)).unwrap().state = state;
            let location = ShardLocation::new(
                ClusterEpoch::INITIAL,
                DataPgId::new(PgId::new(0)),
                ShardIndex::new(0),
                NodeId::new(0),
            );
            let key = ShardKey::new(&[47; 16], 1, 0);

            let err = map
                .place_payload_shards(
                    ClusterEpoch::INITIAL,
                    DataPgId::new(PgId::new(0)),
                    ec_shape,
                    b"non-active-placement",
                )
                .unwrap_err();
            assert!(matches!(
                err,
                ClusterBuildError::PgNotActive {
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    state: err_state,
                } if err_state == state
            ));

            let err = map
                .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"non-active")
                .unwrap_err();
            assert!(matches!(
                err,
                ShardIoError::PgNotActive {
                    node_id: 0,
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    state: err_state,
                } if err_state == state
            ));

            let err = map
                .read_payload_shard(
                    ClusterEpoch::INITIAL,
                    location,
                    &key,
                    WriteAck {
                        crc64: 0,
                        stored_size: 1,
                    },
                )
                .unwrap_err();
            assert!(matches!(
                err,
                ShardIoError::PgNotActive {
                    node_id: 0,
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    state: err_state,
                } if err_state == state
            ));

            let err = map
                .delete_payload_shard(ClusterEpoch::INITIAL, location, &key)
                .unwrap_err();
            assert!(matches!(
                err,
                ShardIoError::PgNotActive {
                    node_id: 0,
                    pg_id: 0,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    state: err_state,
                } if err_state == state
            ));

            assert!(matches!(
                map.node(NodeId::new(0))
                    .unwrap()
                    .storage_node()
                    .read_shard_file(0, &key),
                Err(StoreError::NotFound)
            ));
        }
    }

    #[test]
    fn direct_put_payload_write_fails_closed_when_required_shard_node_leaves_acting_set() {
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
        let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
        let (bucket, key, _object_pg, data_pg) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };

        let mut map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        let reservation_id = crate::SessionId::try_from("56".repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let segment_okh = [96; 16];
        let placement_key =
            super::super::segment_payload_placement_key(&segment_okh, generation_id);
        let locations = cluster
            .place_payload_shards(DataPgId::new(PgId::new(data_pg)), ec_shape, &placement_key)
            .unwrap();
        let (removed_shard_index, removed_node) = locations
            .iter()
            .enumerate()
            .rev()
            .map(|(index, location)| (index, location.node_id()))
            .find(|(_, node_id)| *node_id != NodeId::new(0))
            .expect("test placement should use a non-primary shard node");
        assert!(
            removed_shard_index > 0,
            "test must fail after at least one earlier shard write"
        );
        drop(cluster);

        {
            let route = Arc::get_mut(&mut map)
                .unwrap()
                .pg_routes
                .get_mut(&PgId::new(data_pg))
                .unwrap();
            let acting_set: Vec<NodeId> = node_ids
                .into_iter()
                .filter(|node_id| *node_id != removed_node)
                .collect();
            if route.primary_node_id == removed_node {
                route.primary_node_id = acting_set[0];
            }
            route.acting_set = Arc::from(acting_set);
        }
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

        let err = cluster
            .write_direct_put_segment_payload_shards(
                &bucket,
                &key,
                generation_id,
                0,
                &segment_okh,
                b"strict payload write requires every placed shard",
            )
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::NodeNotInActingSet {
                node_id,
                pg_id,
                cluster_epoch: ClusterEpoch::INITIAL,
            } if node_id == removed_node.as_u32() && pg_id == data_pg
        ));
        for shard_index in 0..ec_shape.k + ec_shape.m {
            assert!(
                !cluster
                    .test_payload_shard_file_exists(
                        data_pg,
                        ec_shape,
                        &segment_okh,
                        generation_id,
                        shard_index,
                    )
                    .unwrap(),
                "failed strict payload write must not leave shard {shard_index}"
            );
        }
    }

    #[test]
    fn placed_segment_recovery_propagates_non_active_pg_route() {
        let non_active_states = [
            PgState::Peering,
            PgState::Degraded,
            PgState::Backfilling,
            PgState::Inconsistent,
        ];

        for state in non_active_states {
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
            let segment = write_committed_direct_segment(&cluster, b"phase-six-seven-route-read");
            let data_pg_id = DataPgId::new(PgId::new(segment.written.data_pg_id));
            drop(cluster);

            Arc::get_mut(&mut map)
                .unwrap()
                .pg_routes
                .get_mut(&data_pg_id.pg_id())
                .unwrap()
                .state = state;
            let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
            let shard_size = segment
                .payload
                .len()
                .div_ceil(usize::from(segment.written.ec.k));
            let mut all_shards =
                vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
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
                    state: err_state,
                } if pg_id == data_pg_id.get()
                    && cluster_epoch == ClusterEpoch::INITIAL
                    && err_state == state
            ));
            assert_eq!(present_count, 0);
            assert!(all_shards.iter().all(Option::is_none));
        }
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
