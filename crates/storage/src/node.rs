/// LocalStorageNode and SharedStorageNode — manage multiple PgStores on a single node.
use ec::{EcConfig, ErasureCodec};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::Read;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::OnceLock;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use placement::NodeId;
#[cfg(any(test, feature = "test-hooks"))]
use s3_types::VersionId;
#[cfg(test)]
use s3_types::{AclGrants, BucketObjectLockConfig, BucketVersioningState, CanonicalUserId};

use super::clients::{
    BucketMetadataNodeClient, BucketWriteReservationNodeClient, DirectPutMetadataNodeClient,
    LocalStorageNodeClient, MetadataCommandNodeClient, ObjectGenerationMetadataNodeClient,
    ObjectListingMetadataNodeClient, ObjectMutationMetadataNodeClient,
    ObjectPayloadLeaseNodeClient, ObjectReadMetadataNodeClient, ObjectVersionMetadataNodeClient,
    PlacedShardNodeClient, RetainedBucketWriteReservationNodeClient,
    RetainedObjectMutationMetadataNodeClient, RetainedObjectPayloadReclaimNodeClient,
    RetainedPlacedShardNodeClient, RetainedShardAckNodeClient, ShardAckNodeClient,
    ShardReadHandleNodeClient, ShardScavengerNodeClient,
};
use super::{BucketPgId, DataPgId, ObjectMetadataPgId, ObjectMetadataScanPgId};
use crate::cluster::ProcessLocalRegistryKey;
use crate::control_plane::{
    NodeHeartbeat, NodePgHeartbeatObservation, PendingMetadataCommandObservation, PgMetadataProof,
};
use crate::data_dir::prepare_private_data_dir;
#[cfg(test)]
use crate::error::BucketWriteDrainError;
use crate::error::{BucketSnapshotLoadError, ObjectPgActionError, StoreError};
use crate::metadata_command::ObjectPayloadReclaimClaimProof;
#[cfg(test)]
use crate::metadata_command::{
    CreateBucketCommand, MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogIndex,
    MetadataCommandPayload,
};
use crate::node_runtime::pg_store::{
    PgClusterMapHistoryReferenceSummary, PgClusterMapHistoryRouteReferences, PgStore,
    PgStoreRecoveryContext, ScavengerShardFileScan,
};
use crate::node_runtime::traits::{PgMetadataStore, ShardStore, StorageNode};
use crate::pg_topology::PgTopology;
#[cfg(test)]
use crate::types::CreateBucketConfig;
#[cfg(any(test, feature = "test-hooks"))]
use crate::types::ListMultipartUploadsReq;
#[cfg(test)]
use crate::types::{
    AuthorizedMultipartUploadRecord, BucketState, ListObjectVersionsReq, ListedMultipartParts,
    MultipartCompletionPreflight, MultipartCompletionSnapshot, MultipartUploadManagementLookup,
    ObjectReadSnapshotOutcome,
};
use crate::types::{
    BucketDeleteFinalizeRoot, BucketInfo, BucketName, BucketSnapshot, BucketSnapshotRequest,
    BucketSubresourceKind, EcShape, GenerationId, LoadedBucketSubresource, ObjectKey,
    ObjectReadAuthSubject, ObjectReadAuthSubjectIdentity, ObjectReadSnapshot, ShardKey,
    StoredObject, WriteAck,
};
#[cfg(any(test, feature = "test-hooks"))]
use crate::types::{
    CreateStreamUploadReq, ListPartsReq, ListPartsResp, MultipartPartRecord,
    MultipartPartSegmentRecord, MultipartReclaimRecord, MultipartUploadRecord, ObjectPartRecord,
    ObjectSegmentRecord, ObjectSegmentsReclaimRecord, PayloadReclaimRoot, PutLiveObjectReq,
    SessionId, StreamUploadRecord, StreamUploadSegmentRecord, UploadId, UploadState,
};
#[cfg(test)]
use crate::types::{StreamUploadState, StreamUploadTarget};
use crate::{ClusterEpoch, PgId, PgState};

const TRACE_TARGET: &str = "storage";
const RECLAIM_WORKER_WAIT_POLL_MILLIS: u64 = 100;
pub(crate) const OBJECT_PAYLOAD_RECLAIM_MAX_OUTSTANDING_PER_PG: usize = 2;
#[path = "node/bucket_ops.rs"]
mod bucket_ops;
#[path = "node/multipart_ops.rs"]
mod multipart_ops;
#[path = "node/object_metadata_ops.rs"]
mod object_metadata_ops;
#[path = "node/object_read_ops.rs"]
mod object_read_ops;
#[path = "node/stream_ops.rs"]
mod stream_ops;

struct PgDataPaths {
    shards_dir: PathBuf,
    tmp_dir: PathBuf,
}

struct EncodeScratchPool {
    max_cached: usize,
    cached: Mutex<Vec<Vec<u8>>>,
    #[cfg(any(test, feature = "test-hooks"))]
    allocations: std::sync::atomic::AtomicUsize,
}

struct EncodeScratch {
    pool: Arc<EncodeScratchPool>,
    buf: Option<Vec<u8>>,
}

struct StorageEcWriteState {
    codec: ErasureCodec,
    scratch: Arc<EncodeScratchPool>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct BucketPgTestGuard<'a> {
    guard: MutexGuard<'a, PgStore>,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct DirectPutMetadataPublishTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct ObjectMetadataCommandPublishTestHookGuard {
    scope_id: usize,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for BucketPgTestGuard<'_> {
    fn drop(&mut self) {
        let _ = &self.guard;
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for DirectPutMetadataPublishTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            AFTER_DIRECT_PUT_METADATA_PUBLISH_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks.lock().unwrap().remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for ObjectMetadataCommandPublishTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            AFTER_OBJECT_METADATA_COMMAND_PUBLISH_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks.lock().unwrap().remove(&self.scope_id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default, Clone)]
pub struct BucketScopedTestHooks {
    pub target: Option<BucketName>,
    pub before_bucket_write_drain_wait: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_lifecycle_context_load: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_lifecycle_bucket_write_proof_acquire: Option<Arc<dyn Fn() + Send + Sync>>,
    pub before_begin_bucket_delete_drain: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_begin_bucket_delete_drain: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_bucket_delete_finalize_claim: Option<Arc<dyn Fn() + Send + Sync>>,
    pub after_bucket_delete_finalize: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[cfg(any(test, feature = "test-hooks"))]
static BUCKET_SCOPED_TEST_HOOKS: OnceLock<Mutex<BucketScopedTestHooks>> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
type DirectPutMetadataPublishHook = Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
static AFTER_DIRECT_PUT_METADATA_PUBLISH_HOOKS: OnceLock<
    Mutex<HashMap<usize, DirectPutMetadataPublishHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
type ObjectMetadataCommandPublishHook =
    Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
static AFTER_OBJECT_METADATA_COMMAND_PUBLISH_HOOKS: OnceLock<
    Mutex<HashMap<usize, ObjectMetadataCommandPublishHook>>,
> = OnceLock::new();

#[cfg(any(test, feature = "test-hooks"))]
pub struct BucketScopedTestHookGuard;

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for BucketScopedTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            BUCKET_SCOPED_TEST_HOOKS.get_or_init(|| Mutex::new(BucketScopedTestHooks::default()));
        *hooks.lock().unwrap() = BucketScopedTestHooks::default();
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub fn install_bucket_scoped_test_hooks(hooks: BucketScopedTestHooks) -> BucketScopedTestHookGuard {
    let slot =
        BUCKET_SCOPED_TEST_HOOKS.get_or_init(|| Mutex::new(BucketScopedTestHooks::default()));
    *slot.lock().unwrap() = hooks;
    BucketScopedTestHookGuard
}

#[cfg(any(test, feature = "test-hooks"))]
fn maybe_run_bucket_scoped_test_hook(
    bucket: &BucketName,
    project: impl FnOnce(BucketScopedTestHooks) -> Option<Arc<dyn Fn() + Send + Sync>>,
) {
    let hooks = BUCKET_SCOPED_TEST_HOOKS
        .get_or_init(|| Mutex::new(BucketScopedTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks.target.as_ref().is_some_and(|target| target == bucket) {
        if let Some(hook) = project(hooks) {
            hook();
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_bucket_write_drain_wait_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.before_bucket_write_drain_wait)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) fn maybe_run_bucket_write_drain_wait_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_before_lifecycle_context_load_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.before_lifecycle_context_load)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) fn maybe_run_before_lifecycle_context_load_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_before_lifecycle_bucket_write_proof_acquire_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| {
        hooks.before_lifecycle_bucket_write_proof_acquire
    })
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) fn maybe_run_before_lifecycle_bucket_write_proof_acquire_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_after_begin_bucket_delete_drain_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.after_begin_bucket_delete_drain)
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_before_begin_bucket_delete_drain_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.before_begin_bucket_delete_drain)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) fn maybe_run_before_begin_bucket_delete_drain_hook(_: &BucketName) {}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) fn maybe_run_after_begin_bucket_delete_drain_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_after_bucket_delete_finalize_claim_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.after_bucket_delete_finalize_claim)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) fn maybe_run_after_bucket_delete_finalize_claim_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_after_bucket_delete_finalize_hook(bucket: &BucketName) {
    maybe_run_bucket_scoped_test_hook(bucket, |hooks| hooks.after_bucket_delete_finalize)
}

#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) fn maybe_run_after_bucket_delete_finalize_hook(_: &BucketName) {}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_after_direct_put_metadata_publish_hook(
    scope_id: usize,
) -> Result<(), ObjectPgActionError> {
    let hook = AFTER_DIRECT_PUT_METADATA_PUBLISH_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .get(&scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook()?;
    }
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn maybe_run_after_object_metadata_command_publish_hook(
    scope_id: usize,
) -> Result<(), ObjectPgActionError> {
    let hook = AFTER_OBJECT_METADATA_COMMAND_PUBLISH_HOOKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .get(&scope_id)
        .cloned();
    if let Some(hook) = hook {
        hook()?;
    }
    Ok(())
}

/// A local storage node managing multiple PG stores.
///
/// On construction, takes a data directory and a list of PG IDs.
/// Creates PG directories if they don't exist. Routes operations
/// to the appropriate PgStore.
pub struct LocalStorageNode {
    stores: HashMap<u32, PgStore>,
    pg_id_list: Vec<u32>,
    data_dir: PathBuf,
}

impl LocalStorageNode {
    /// Open a storage node, creating PG directories as needed.
    pub fn open(data_dir: &Path, pg_ids: &[u32]) -> Result<Self, StoreError> {
        prepare_private_data_dir(data_dir).map_err(|e| StoreError::Io {
            context: "prepare private data dir",
            source: e,
        })?;

        let mut stores = HashMap::with_capacity(pg_ids.len());
        let mut pg_id_list = Vec::with_capacity(pg_ids.len());

        for &pg_id in pg_ids {
            let pg_dir = data_dir.join(format!("pg-{pg_id:04}"));
            let store = PgStore::open(&pg_dir, pg_id)?;
            stores.insert(pg_id, store);
            pg_id_list.push(pg_id);
        }

        pg_id_list.sort_unstable();

        Ok(Self {
            stores,
            pg_id_list,
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Return the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Get a reference to the PgStore for the given PG ID.
    ///
    /// This returns the full PgStore which implements both ShardStore
    /// and PgMetadataStore.
    #[cfg(test)]
    pub(crate) fn get_pg(&self, pg_id: u32) -> Result<&PgStore, StoreError> {
        observability::trace_scope!(TRACE_TARGET, "LocalStorageNode::get_pg", "pg_id={}", pg_id);
        self.stores
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })
    }
}

impl StorageNode for LocalStorageNode {
    fn get_pg_store(&self, pg_id: u32) -> Result<&dyn ShardStore, StoreError> {
        self.stores
            .get(&pg_id)
            .map(|s| s as &dyn ShardStore)
            .ok_or(StoreError::PgNotFound { pg_id })
    }

    fn pg_ids(&self) -> &[u32] {
        &self.pg_id_list
    }
}

/// A shared storage node managing multiple PG stores behind mutexes.
///
/// All frontends share one `Arc<SharedStorageNode>`. Each PG's `PgStore`
/// is wrapped in a `std::sync::Mutex`, serializing all operations within
/// a PG while allowing parallelism across PGs.
pub struct SharedStorageNode {
    process_local_registry_key: ProcessLocalRegistryKey,
    stores: HashMap<u32, Mutex<PgStore>>,
    pg_paths: HashMap<u32, PgDataPaths>,
    pg_id_list: Vec<u32>,
    pg_topology: PgTopology,
    default_ec_shape: EcShape,
    data_dir: PathBuf,
    object_payload_leases: Mutex<ObjectPayloadLeaseState>,
    reclaim_queue: (Mutex<ReclaimQueueState>, Condvar),
    ec_write_states: Mutex<HashMap<EcShape, Arc<StorageEcWriteState>>>,
}

/// Opaque process-local node runtime used by the cluster facade.
///
/// Cluster routing receives only the node-client adapter plus the small set of
/// startup and encoding operations that cannot yet flow through a node-client
/// trait. The raw [`SharedStorageNode`] never reaches cluster routing in
/// production builds.
#[derive(Clone)]
pub(crate) struct LocalNodeRuntime {
    node: Arc<SharedStorageNode>,
    client: Arc<LocalStorageNodeClient>,
}

pub(crate) struct LocalNodeClients {
    pub(crate) object_payload_lease: Arc<dyn ObjectPayloadLeaseNodeClient>,
    pub(crate) retained_object_payload_reclaim: Arc<dyn RetainedObjectPayloadReclaimNodeClient>,
    pub(crate) bucket_metadata: Arc<dyn BucketMetadataNodeClient>,
    pub(crate) bucket_write_reservation: Arc<dyn BucketWriteReservationNodeClient>,
    pub(crate) retained_bucket_write_reservation: Arc<dyn RetainedBucketWriteReservationNodeClient>,
    pub(crate) object_generation_metadata: Arc<dyn ObjectGenerationMetadataNodeClient>,
    pub(crate) object_version_metadata: Arc<dyn ObjectVersionMetadataNodeClient>,
    pub(crate) direct_put_metadata: Arc<dyn DirectPutMetadataNodeClient>,
    pub(crate) object_listing_metadata: Arc<dyn ObjectListingMetadataNodeClient>,
    pub(crate) object_mutation_metadata: Arc<dyn ObjectMutationMetadataNodeClient>,
    pub(crate) retained_object_mutation_metadata: Arc<dyn RetainedObjectMutationMetadataNodeClient>,
    pub(crate) object_read_metadata: Arc<dyn ObjectReadMetadataNodeClient>,
    pub(crate) metadata_command: Arc<dyn MetadataCommandNodeClient>,
    pub(crate) shard: Arc<dyn PlacedShardNodeClient>,
    pub(crate) retained_shard: Arc<dyn RetainedPlacedShardNodeClient>,
    pub(crate) shard_ack: Arc<dyn ShardAckNodeClient>,
    pub(crate) retained_shard_ack: Arc<dyn RetainedShardAckNodeClient>,
    pub(crate) shard_read_handle: Arc<dyn ShardReadHandleNodeClient>,
    pub(crate) shard_scavenger: Arc<dyn ShardScavengerNodeClient>,
}

fn allocate_process_local_registry_key() -> Result<ProcessLocalRegistryKey, StoreError> {
    ProcessLocalRegistryKey::allocate().ok_or_else(|| StoreError::Io {
        context: "allocate process-local storage registry key",
        source: std::io::Error::other("process-local storage registry key space exhausted"),
    })
}

impl LocalNodeRuntime {
    pub(crate) fn open(
        node_id: NodeId,
        data_dir: &Path,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        initial_cluster_epoch: ClusterEpoch,
    ) -> Result<Self, StoreError> {
        let node = Arc::new(SharedStorageNode::open_with_default_ec_shape_and_epoch(
            data_dir,
            pg_ids,
            default_ec_shape,
            initial_cluster_epoch,
        )?);
        Ok(Self::from_node(node_id, node))
    }

    pub(crate) fn topology_only(
        node_id: NodeId,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
    ) -> Result<Self, StoreError> {
        let node = Arc::new(SharedStorageNode::topology_only(pg_ids, default_ec_shape)?);
        Ok(Self::from_node(node_id, node))
    }

    fn from_node(node_id: NodeId, node: Arc<SharedStorageNode>) -> Self {
        let client = Arc::new(LocalStorageNodeClient::new(node_id, Arc::clone(&node)));
        Self { node, client }
    }

    pub(crate) fn clients(&self) -> LocalNodeClients {
        LocalNodeClients {
            object_payload_lease: self.client.clone(),
            retained_object_payload_reclaim: self.client.clone(),
            bucket_metadata: self.client.clone(),
            bucket_write_reservation: self.client.clone(),
            retained_bucket_write_reservation: self.client.clone(),
            object_generation_metadata: self.client.clone(),
            object_version_metadata: self.client.clone(),
            direct_put_metadata: self.client.clone(),
            object_listing_metadata: self.client.clone(),
            object_mutation_metadata: self.client.clone(),
            retained_object_mutation_metadata: self.client.clone(),
            object_read_metadata: self.client.clone(),
            metadata_command: self.client.clone(),
            shard: self.client.clone(),
            retained_shard: self.client.clone(),
            shard_ack: self.client.clone(),
            retained_shard_ack: self.client.clone(),
            shard_read_handle: self.client.clone(),
            shard_scavenger: self.client.clone(),
        }
    }

    pub(crate) fn process_local_registry_key(&self) -> ProcessLocalRegistryKey {
        self.node.process_local_registry_key
    }

    pub(crate) fn prepare_metadata_command_recovery(
        &self,
        node_id: NodeId,
    ) -> Result<(), StoreError> {
        self.node.prepare_pg_metadata_command_recovery(node_id)
    }

    pub(crate) fn recover_metadata_command_state(&self, node_id: NodeId) -> Result<(), StoreError> {
        self.node.recover_pg_metadata_command_state(node_id)
    }

    pub(crate) fn pg_topology(&self) -> &PgTopology {
        self.node.pg_topology()
    }

    pub(crate) fn bucket_metadata_pg_for(&self, bucket: &BucketName) -> BucketPgId {
        self.node.bucket_metadata_pg_for(bucket)
    }

    pub(crate) fn bucket_metadata_pg(&self, pg_id: PgId) -> Option<BucketPgId> {
        self.node.bucket_metadata_pg(pg_id)
    }

    pub(crate) fn object_metadata_pg_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> ObjectMetadataPgId {
        self.node.object_metadata_pg_for(bucket, key)
    }

    pub(crate) fn object_metadata_scan_pg(&self, pg_id: PgId) -> Option<ObjectMetadataScanPgId> {
        self.node.object_metadata_scan_pg(pg_id)
    }

    pub(crate) fn data_pg(&self, pg_id: PgId) -> Option<DataPgId> {
        self.node.data_pg(pg_id)
    }

    pub(crate) fn write_erasure_coded_segment_shards_with<F>(
        &self,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        data: &[u8],
        ec: EcShape,
        write_shards: F,
    ) -> Result<Vec<crate::WrittenShardAck>, StoreError>
    where
        F: FnOnce(&[(ShardKey, &[u8])]) -> Result<Vec<(ShardKey, WriteAck)>, StoreError>,
    {
        self.node.write_erasure_coded_segment_shards_with(
            segment_okh,
            segment_vid,
            data,
            ec,
            write_shards,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_node(&self) -> &Arc<SharedStorageNode> {
        &self.node
    }
}

type ReclaimRoot = (BucketName, ObjectKey, GenerationId);

/// Opaque authority to continue a DeleteBucket begin attempt that storage has
/// already authorized or recovered from durable attempt state.
///
/// The subject fields are intentionally private. Ordinary callers may inspect
/// them for scheduling and stale-work detection, but only storage-owned
/// recovery and enqueue paths can mint this authority.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BucketDeleteBeginRoot {
    pub(crate) bucket: BucketName,
    pub(crate) bucket_execution_generation: u64,
    pub(crate) bucket_incarnation_generation: u64,
}

impl BucketDeleteBeginRoot {
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    pub fn bucket_execution_generation(&self) -> u64 {
        self.bucket_execution_generation
    }

    pub fn bucket_incarnation_generation(&self) -> u64 {
        self.bucket_incarnation_generation
    }
}

#[derive(Debug, Default)]
struct ObjectPayloadLeaseState {
    leases: HashMap<ReclaimRoot, usize>,
    reclaim_fences: HashMap<ReclaimRoot, ObjectPayloadReclaimClaimProof>,
    active_reclaims: HashMap<ReclaimRoot, ObjectPayloadReclaimClaimProof>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReclaimWorkItem {
    ObjectPayload(ReclaimRoot),
    BucketDeleteBegin(BucketDeleteBeginRoot),
    BucketDelete(BucketDeleteFinalizeRoot),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReclaimQueueInsert {
    Queued,
    Deduplicated,
    PgCapacityDeferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketDeleteFinalizeOutcome {
    NotFound,
    NotDeleting,
    StaleIncarnation,
    Pending,
    Finalized,
}

impl BucketDeleteFinalizeOutcome {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::NotFound | Self::NotDeleting | Self::StaleIncarnation | Self::Finalized
        )
    }
}

#[derive(Debug, Clone)]
pub enum BucketCreateAttemptOutcome {
    Created(BucketInfo),
    Exists(BucketInfo),
}

struct ReclaimQueueState {
    work_queue: VecDeque<ReclaimWorkItem>,
    queued_objects: HashSet<ReclaimRoot>,
    queued_bucket_delete_begins: HashSet<BucketDeleteBeginRoot>,
    queued_bucket_deletes: HashSet<BucketDeleteFinalizeRoot>,
    outstanding_bucket_deletes: HashSet<BucketDeleteFinalizeRoot>,
}

impl SharedStorageNode {
    pub const DEFAULT_EC_SHAPE: EcShape = EcShape { k: 4, m: 2 };

    pub(crate) fn topology_only(
        pg_ids: &[u32],
        default_ec_shape: EcShape,
    ) -> Result<Self, StoreError> {
        EcConfig::new(default_ec_shape.k, default_ec_shape.m).map_err(|error| {
            StoreError::ErasureCoding {
                context: "validate storage default ec shape",
                reason: error.to_string(),
            }
        })?;
        let mut pg_id_list = pg_ids.to_vec();
        pg_id_list.sort_unstable();

        let pg_topology =
            PgTopology::new(pg_ids).map_err(|source| StoreError::InvalidPgTopology { source })?;

        Ok(Self {
            process_local_registry_key: allocate_process_local_registry_key()?,
            stores: HashMap::new(),
            pg_paths: HashMap::new(),
            pg_id_list,
            pg_topology,
            default_ec_shape,
            data_dir: PathBuf::new(),
            object_payload_leases: Mutex::new(ObjectPayloadLeaseState::default()),
            reclaim_queue: (
                Mutex::new(ReclaimQueueState {
                    work_queue: VecDeque::new(),
                    queued_objects: HashSet::new(),
                    queued_bucket_delete_begins: HashSet::new(),
                    queued_bucket_deletes: HashSet::new(),
                    outstanding_bucket_deletes: HashSet::new(),
                }),
                Condvar::new(),
            ),
            ec_write_states: Mutex::new(HashMap::new()),
        })
    }

    /// Open a shared storage node, creating PG directories as needed.
    pub fn open(data_dir: &Path, pg_ids: &[u32]) -> Result<Self, StoreError> {
        Self::open_with_default_ec_shape(data_dir, pg_ids, Self::DEFAULT_EC_SHAPE)
    }

    pub fn open_with_default_ec_shape(
        data_dir: &Path,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
    ) -> Result<Self, StoreError> {
        Self::open_with_default_ec_shape_and_epoch(
            data_dir,
            pg_ids,
            default_ec_shape,
            ClusterEpoch::INITIAL,
        )
    }

    pub(crate) fn open_with_default_ec_shape_and_epoch(
        data_dir: &Path,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        initial_cluster_epoch: ClusterEpoch,
    ) -> Result<Self, StoreError> {
        EcConfig::new(default_ec_shape.k, default_ec_shape.m).map_err(|error| {
            StoreError::ErasureCoding {
                context: "validate storage default ec shape",
                reason: error.to_string(),
            }
        })?;
        prepare_private_data_dir(data_dir).map_err(|e| StoreError::Io {
            context: "prepare private data dir",
            source: e,
        })?;

        let mut stores = HashMap::with_capacity(pg_ids.len());
        let mut pg_paths = HashMap::with_capacity(pg_ids.len());
        let mut pg_id_list = Vec::with_capacity(pg_ids.len());

        for &pg_id in pg_ids {
            let pg_dir = data_dir.join(format!("pg-{pg_id:04}"));
            let store =
                PgStore::open_with_initial_cluster_epoch(&pg_dir, pg_id, initial_cluster_epoch)?;
            let shards_dir = pg_dir.join("shards");
            let tmp_dir = pg_dir.join("tmp");
            stores.insert(pg_id, Mutex::new(store));
            pg_paths.insert(
                pg_id,
                PgDataPaths {
                    shards_dir,
                    tmp_dir,
                },
            );
            pg_id_list.push(pg_id);
        }

        pg_id_list.sort_unstable();

        let pg_topology =
            PgTopology::new(pg_ids).map_err(|source| StoreError::InvalidPgTopology { source })?;

        Ok(Self {
            process_local_registry_key: allocate_process_local_registry_key()?,
            stores,
            pg_paths,
            pg_id_list,
            pg_topology,
            default_ec_shape,
            data_dir: data_dir.to_path_buf(),
            object_payload_leases: Mutex::new(ObjectPayloadLeaseState::default()),
            reclaim_queue: (
                Mutex::new(ReclaimQueueState {
                    work_queue: VecDeque::new(),
                    queued_objects: HashSet::new(),
                    queued_bucket_delete_begins: HashSet::new(),
                    queued_bucket_deletes: HashSet::new(),
                    outstanding_bucket_deletes: HashSet::new(),
                }),
                Condvar::new(),
            ),
            ec_write_states: Mutex::new(HashMap::new()),
        })
    }

    pub fn default_ec_shape(&self) -> EcShape {
        self.default_ec_shape
    }

    /// Return the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Return the sorted list of PG IDs.
    pub fn pg_ids(&self) -> &[u32] {
        &self.pg_id_list
    }

    pub fn pg_topology(&self) -> &PgTopology {
        &self.pg_topology
    }

    pub(in crate::node_runtime) fn bucket_metadata_pg_for(
        &self,
        bucket: &BucketName,
    ) -> BucketPgId {
        BucketPgId(PgId::new(self.pg_topology.bucket_pg_for(bucket)))
    }

    pub(in crate::node_runtime) fn bucket_metadata_pg(&self, pg_id: PgId) -> Option<BucketPgId> {
        self.pg_id_list
            .binary_search(&pg_id.get())
            .ok()
            .map(|_| BucketPgId(pg_id))
    }

    pub(in crate::node_runtime) fn object_metadata_pg_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> ObjectMetadataPgId {
        ObjectMetadataPgId(PgId::new(self.pg_topology.object_pg_for(bucket, key)))
    }

    pub(in crate::node_runtime) fn object_metadata_scan_pg(
        &self,
        pg_id: PgId,
    ) -> Option<ObjectMetadataScanPgId> {
        self.pg_id_list
            .binary_search(&pg_id.get())
            .ok()
            .map(|_| ObjectMetadataScanPgId(pg_id))
    }

    pub(in crate::node_runtime) fn data_pg(&self, pg_id: PgId) -> Option<DataPgId> {
        self.pg_id_list
            .binary_search(&pg_id.get())
            .ok()
            .map(|_| DataPgId::from_validated_placement(pg_id))
    }

    pub fn cluster_map_history_reference_summary(
        &self,
    ) -> Result<PgClusterMapHistoryReferenceSummary, StoreError> {
        Ok(self.cluster_map_history_route_references()?.summary())
    }

    pub fn cluster_map_history_route_references(
        &self,
    ) -> Result<PgClusterMapHistoryRouteReferences, StoreError> {
        let mut references = PgClusterMapHistoryRouteReferences::default();
        for pg in self.stores.values() {
            let pg = pg.lock().unwrap_or_else(|e| e.into_inner());
            references.merge(pg.cluster_map_history_route_references(&self.pg_topology)?)?;
        }
        Ok(references)
    }

    /// Recover every opened PG store on this node after open, before the node
    /// serves any request. See `PgStore::recover` and the "PG Store Recovery
    /// Boundary" guide section. The caller supplies the owning node id; the
    /// recovery epoch is read from each store's own replica state.
    pub fn recover_pg_metadata_command_state(&self, node_id: NodeId) -> Result<(), StoreError> {
        let ctx = PgStoreRecoveryContext::for_node(node_id);
        for &pg_id in self.pg_id_list.iter() {
            self.get_pg(pg_id)?.recover(ctx)?;
        }
        Ok(())
    }

    /// Recovery phase A for clustered open paths: clean epoch-mismatched orphan
    /// pending command slots on every opened PG. This must run before a
    /// cluster-wide convergence pass, which would otherwise reject an orphan
    /// through the epoch-checked pending-slot read before full recovery could
    /// clean it. Same-epoch primary pending slots are preserved for convergence.
    pub(crate) fn prepare_pg_metadata_command_recovery(
        &self,
        node_id: NodeId,
    ) -> Result<(), StoreError> {
        let ctx = PgStoreRecoveryContext::for_node(node_id);
        for &pg_id in self.pg_id_list.iter() {
            self.get_pg(pg_id)?
                .recover_clean_orphan_pending_command_slots(ctx)?;
        }
        Ok(())
    }

    pub fn bucket_pg_id_for(&self, bucket: &BucketName) -> u32 {
        self.pg_topology.bucket_pg_for(bucket)
    }

    fn ec_write_state(&self, shape: EcShape) -> Result<Arc<StorageEcWriteState>, StoreError> {
        if let Some(existing) = self.ec_write_states.lock().unwrap().get(&shape).cloned() {
            return Ok(existing);
        }

        let config =
            EcConfig::new(shape.k, shape.m).map_err(|error| StoreError::ErasureCoding {
                context: "build erasure coding config",
                reason: error.to_string(),
            })?;
        let state = Arc::new(StorageEcWriteState {
            codec: ErasureCodec::new(config).map_err(|error| StoreError::ErasureCoding {
                context: "build erasure coding codec",
                reason: error.to_string(),
            })?,
            scratch: Arc::new(EncodeScratchPool::new(config)),
        });

        let mut guard = self.ec_write_states.lock().unwrap();
        Ok(guard
            .entry(shape)
            .or_insert_with(|| Arc::clone(&state))
            .clone())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_ec_scratch_allocation_count(&self, shape: EcShape) -> usize {
        self.ec_write_states
            .lock()
            .unwrap()
            .get(&shape)
            .map_or(0, |state| state.scratch.allocation_count())
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_bucket_pg_available(
        &self,
        bucket: &BucketName,
    ) -> Result<bool, BucketSnapshotLoadError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let Some(pg) = self.stores.get(&pg_id) else {
            return Err(StoreError::PgNotFound { pg_id }.into());
        };
        match pg.try_lock() {
            Ok(_guard) => Ok(true),
            Err(std::sync::TryLockError::WouldBlock) => Ok(false),
            Err(std::sync::TryLockError::Poisoned(error)) => {
                drop(error.into_inner());
                Ok(true)
            }
        }
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_object_pg_available(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = self.pg_topology.object_pg_for(bucket, key);
        let Some(pg) = self.stores.get(&pg_id) else {
            return Err(StoreError::PgNotFound { pg_id }.into());
        };
        match pg.try_lock() {
            Ok(_guard) => Ok(true),
            Err(std::sync::TryLockError::WouldBlock) => Ok(false),
            Err(std::sync::TryLockError::Poisoned(error)) => {
                drop(error.into_inner());
                Ok(true)
            }
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_bucket_pg_id_for(&self, bucket: &BucketName) -> u32 {
        self.bucket_pg_id_for(bucket)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_head_bucket_raw(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg_id = self.test_bucket_pg_id_for(bucket);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.head_bucket_raw(bucket)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_object_pg_id_for(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.pg_topology.object_pg_for(bucket, key)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_data_pg_id_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> u32 {
        self.pg_topology
            .object_generation_segment_data_pg(bucket, key, generation_id, 0)
            .get()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_object_generation_reservation_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        Ok(pg.get_object_generation_reservation(bucket, key, reservation_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_multipart_part_data_pg_id_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        object_generation_id: GenerationId,
        part_number: u32,
    ) -> u32 {
        self.pg_topology
            .object_generation_multipart_part_data_pg(
                bucket,
                key,
                object_generation_id,
                part_number,
            )
            .get()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_meta(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<StoredObject, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_object_meta(bucket, key)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_multipart_upload(upload_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_multipart_part(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u16,
    ) -> Result<MultipartPartRecord, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_multipart_part(upload_id, u32::from(part_number))?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_multipart_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        req: &ListPartsReq,
    ) -> Result<ListPartsResp, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.list_multipart_parts(req)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<MultipartUploadRecord>, ObjectPgActionError> {
        let mut uploads = Vec::new();
        for &pg_id in &self.pg_id_list {
            let pg = self.get_pg(pg_id)?;
            let listed = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: bucket.clone(),
                prefix: None,
                page_start: None,
                max_uploads: u32::MAX,
            })?;
            uploads.extend(listed.uploads);
        }
        Ok(uploads)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectSegmentRecord>, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_object_segments(bucket, key, version_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_replace_live_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        let live = match pg.get_object_meta(bucket, key)? {
            StoredObject::Live(record) => record,
            StoredObject::DeleteMarker(_) => return Err(StoreError::NotFound.into()),
        };
        assert_eq!(
            live.version_id, version_id,
            "test_replace_live_object_segments called for non-current live version"
        );
        pg.put_object_with_segments(
            &PutLiveObjectReq {
                bucket: live.bucket,
                key: live.key,
                version_id: live.version_id,
                owner: live.owner,
                acl_grants: live.acl_grants,
                public_read: live.public_read,
                generation_id: live.generation_id,
                size: live.size,
                etag: live.etag,
                ec: live.ec,
                layout: live.layout,
                tags: live.tags,
                metadata_blob: live.metadata_blob,
                system_metadata_blob: live.system_metadata_blob,
                object_lock: live.object_lock,
                encryption: live.encryption,
            },
            segments,
        )?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectPartRecord>, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_object_parts(bucket, key, version_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_replace_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        parts: &[ObjectPartRecord],
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        pg.delete_object_parts(bucket, key, version_id)?;
        Ok(pg.commit_object_parts(parts)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<StoredObject, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_object_version(bucket, key, version_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectSegmentsReclaimRecord>, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_object_segments_reclaim(bucket, key, generation_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_put_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reclaim: &ObjectSegmentsReclaimRecord,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.put_object_segments_reclaim(reclaim)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_put_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reclaim: &MultipartReclaimRecord,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.put_multipart_reclaim(reclaim)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.payload_reclaim_exists(bucket, key, generation_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_bucket_payload_reclaim_roots(
        &self,
        bucket: &BucketName,
    ) -> Result<Vec<PayloadReclaimRoot>, ObjectPgActionError> {
        let mut roots = Vec::new();
        for &pg_id in &self.pg_id_list {
            let pg = self.get_pg(pg_id)?;
            if let Some(root) = PgMetadataStore::get_bucket_payload_reclaim_root(&*pg, bucket)? {
                roots.push(root);
            }
        }
        Ok(roots)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_force_became_noncurrent_at(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        became_noncurrent_at: u64,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        pg.test_force_object_became_noncurrent_at(bucket, key, version_id, became_noncurrent_at)?;
        pg.refresh_metadata_command_state_digest()?;
        Ok(())
    }

    #[cfg(test)]
    pub fn test_create_deleting_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let owner_canonical_id = CanonicalUserId::from_principal("default-owner");
        let acl_grants = AclGrants::default();
        let create = CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "default-owner",
            owner_canonical_id: &owner_canonical_id,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let _ = self
            .create_bucket_with_config_and_load_info(&create)
            .map_err(|err| match err {
                BucketSnapshotLoadError::Store(err) => BucketWriteDrainError::Store(err),
                BucketSnapshotLoadError::Metadata(err) => BucketWriteDrainError::Metadata(err),
            })?;
        self.mark_bucket_deleting(bucket)?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_get_all_multipart_part_segments_for_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Vec<MultipartPartSegmentRecord>, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.get_all_multipart_part_segments_for_upload(upload_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_set_upload_state(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        state: UploadState,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        pg.set_upload_state(upload_id, state)?;
        pg.refresh_metadata_command_state_digest()?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_stream_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.list_stream_segments(session_id)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_force_stream_upload_created_at(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        created_at: u64,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(bucket, key);
        let pg = self.get_pg(pg_id)?;
        pg.test_force_stream_upload_created_at(session_id, created_at)?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_list_all_stream_uploads(
        &self,
    ) -> Result<Vec<StreamUploadRecord>, ObjectPgActionError> {
        let mut sessions = Vec::new();
        for &pg_id in &self.pg_id_list {
            let pg = self.get_pg(pg_id)?;
            sessions.extend(pg.list_all_stream_uploads()?);
        }
        Ok(sessions)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_create_stream_upload(
        &self,
        req: &CreateStreamUploadReq,
    ) -> Result<(), ObjectPgActionError> {
        let pg_id = self.test_object_pg_id_for(&req.bucket, &req.key);
        let pg = self.get_pg(pg_id)?;
        Ok(pg.create_stream_upload(req)?)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_hook_scope_id(&self) -> usize {
        std::ptr::from_ref(self).addr()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_after_direct_put_metadata_publish_hook(
        &self,
        hook: Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>,
    ) -> DirectPutMetadataPublishTestHookGuard {
        let scope_id = self.test_hook_scope_id();
        let hooks =
            AFTER_DIRECT_PUT_METADATA_PUBLISH_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks.lock().unwrap().insert(scope_id, hook);
        DirectPutMetadataPublishTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_install_after_object_metadata_command_publish_hook(
        &self,
        hook: Arc<dyn Fn() -> Result<(), ObjectPgActionError> + Send + Sync>,
    ) -> ObjectMetadataCommandPublishTestHookGuard {
        let scope_id = self.test_hook_scope_id();
        let hooks =
            AFTER_OBJECT_METADATA_COMMAND_PUBLISH_HOOKS.get_or_init(|| Mutex::new(HashMap::new()));
        hooks.lock().unwrap().insert(scope_id, hook);
        ObjectMetadataCommandPublishTestHookGuard { scope_id }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_shard_exists(&self, pg_id: u32, key: &ShardKey) -> Result<bool, StoreError> {
        let pg = self.get_pg(pg_id)?;
        match pg.stat_shard(key) {
            Ok(_) => Ok(true),
            Err(StoreError::NotFound) => Ok(false),
            Err(other) => Err(other),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_lock_bucket_pg(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketPgTestGuard<'_>, StoreError> {
        let pg_id = self.test_bucket_pg_id_for(bucket);
        let guard = self.get_pg(pg_id)?;
        Ok(BucketPgTestGuard { guard })
    }

    /// Lock and return a guard for the given PG.
    pub(crate) fn get_pg(&self, pg_id: u32) -> Result<MutexGuard<'_, PgStore>, StoreError> {
        observability::trace_scope!(TRACE_TARGET, "SharedStorageNode::get_pg", "pg_id={}", pg_id);
        let mutex = self
            .stores
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        Ok(mutex.lock().unwrap_or_else(|e| e.into_inner()))
    }

    pub fn pg_heartbeat_observation(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        state: PgState,
    ) -> Result<NodePgHeartbeatObservation, StoreError> {
        let pg = self.get_pg(pg_id.get())?;
        let metadata_epoch = pg.metadata_command_replica_state()?.cluster_epoch;
        let metadata_state =
            pg.metadata_command_replica_state_for_heartbeat(node_id.as_u32(), metadata_epoch)?;
        let pending_slot = pg.pending_metadata_command_slot_any_epoch(node_id.as_u32())?;

        // Heartbeat is a serving-time observation path. It reports proof and
        // pending-slot presence, but it must not reconcile or delete pending
        // slots. A legitimate command can install a future-epoch pending slot
        // before apply/record advances durable replica state, and terminal
        // same-epoch slots are also command/recovery cleanup work, not
        // heartbeat work.
        let pending_metadata_command = pending_slot.map(|slot| {
            PendingMetadataCommandObservation::new(
                slot.id.cluster_epoch(),
                NonZeroU64::new(slot.id.log_index().get())
                    .expect("pending metadata command log index is nonzero"),
                slot.command_checksum,
            )
        });
        Ok(NodePgHeartbeatObservation {
            pg_id,
            state,
            metadata_proof: PgMetadataProof {
                applied_log_index: metadata_state.applied_log_index,
                applied_log_hash: metadata_state.applied_log_hash,
                state_digest: metadata_state.state_digest,
            },
            pending_metadata_command,
        })
    }

    pub fn control_plane_heartbeat(
        &self,
        node_id: NodeId,
        node_incarnation: u64,
        endpoint: impl Into<String>,
        observed_epoch: ClusterEpoch,
        requested_lease_duration_ms: u64,
        pg_states: impl IntoIterator<Item = (PgId, PgState)>,
    ) -> Result<NodeHeartbeat, StoreError> {
        let pg_observations = pg_states
            .into_iter()
            .map(|(pg_id, state)| self.pg_heartbeat_observation(node_id, pg_id, state))
            .collect::<Result<Vec<_>, _>>()?;
        let cluster_map_history_route_references = self.cluster_map_history_route_references()?;
        Ok(NodeHeartbeat {
            node_id,
            node_incarnation,
            endpoint: endpoint.into(),
            observed_epoch,
            requested_lease_duration_ms,
            cluster_map_history_route_references,
            pg_observations,
        })
    }

    /// Write a shard file durably without taking the per-PG mutex.
    ///
    /// This is used on the write hot path so file IO and fsync do not hold the
    /// PG metadata lock. Callers must publish the corresponding shard row under
    /// the PG mutex afterward before the shard becomes visible to reads.
    pub(crate) fn write_shard_file(
        &self,
        pg_id: u32,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::write_shard_file",
            "pg_id={} shard={} bytes={}",
            pg_id,
            key,
            data.len()
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        PgStore::write_shard_file_durable(&paths.tmp_dir, &paths.shards_dir, key, data)
    }

    pub(crate) fn write_shard_file_if_absent(
        &self,
        pg_id: u32,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::write_shard_file_if_absent",
            "pg_id={} shard={} bytes={}",
            pg_id,
            key,
            data.len()
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        PgStore::write_shard_file_durable_if_absent(&paths.tmp_dir, &paths.shards_dir, key, data)
    }

    /// Read a shard file directly without taking the per-PG mutex.
    pub(crate) fn read_shard_file(
        &self,
        pg_id: u32,
        key: &ShardKey,
    ) -> Result<Vec<u8>, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::read_shard_file",
            "pg_id={} shard={}",
            pg_id,
            key
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        let shard_path = PgStore::shard_path_for_shards_dir(&paths.shards_dir, key);
        fs::read(&shard_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                return StoreError::NotFound;
            }
            StoreError::Io {
                context: "read shard file",
                source: e,
            }
        })
    }

    /// Read a shard file directly into a caller-provided buffer without taking
    /// the per-PG mutex.
    pub(crate) fn read_shard_file_into(
        &self,
        pg_id: u32,
        key: &ShardKey,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::read_shard_file_into",
            "pg_id={} shard={} bytes={}",
            pg_id,
            key,
            dst.len()
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        let shard_path = PgStore::shard_path_for_shards_dir(&paths.shards_dir, key);
        let mut file = fs::File::open(&shard_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                return StoreError::NotFound;
            }
            StoreError::Io {
                context: "open shard file",
                source: e,
            }
        })?;

        file.read_exact(dst).map_err(|e| StoreError::Io {
            context: "read shard file",
            source: e,
        })?;

        let mut extra = [0u8; 1];
        match file.read(&mut extra) {
            Ok(0) => Ok(()),
            Ok(_) => Err(StoreError::Io {
                context: "read shard file length mismatch",
                source: std::io::Error::from(std::io::ErrorKind::InvalidData),
            }),
            Err(e) => Err(StoreError::Io {
                context: "read shard file",
                source: e,
            }),
        }
    }

    /// Delete a shard file directly without mutating the PG shard metadata row.
    pub(crate) fn delete_shard_file(&self, pg_id: u32, key: &ShardKey) -> Result<(), StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::delete_shard_file",
            "pg_id={} shard={}",
            pg_id,
            key
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        let shard_path = PgStore::shard_path_for_shards_dir(&paths.shards_dir, key);
        match fs::remove_file(&shard_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StoreError::Io {
                context: "unlink shard file",
                source: e,
            }),
        }
    }

    /// List local shard files for scavenger audit without taking the per-PG
    /// metadata mutex.
    pub(crate) fn list_scavenger_shard_files(
        &self,
        pg_id: u32,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "SharedStorageNode::list_scavenger_shard_files",
            "pg_id={}",
            pg_id
        );
        let paths = self
            .pg_paths
            .get(&pg_id)
            .ok_or(StoreError::PgNotFound { pg_id })?;
        PgStore::list_scavenger_shard_files_in_dir(&paths.shards_dir)
    }

    /// Acquire an in-memory lease on an object payload generation.
    ///
    /// Returns false if this storage node has fenced the generation for
    /// physical reclaim.
    pub(crate) fn try_acquire_object_payload_lease(
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
        if state.reclaim_fences.contains_key(&root) {
            return false;
        }
        *state.leases.entry(root).or_insert(0) += 1;
        true
    }

    /// Release an in-memory lease on an object payload generation.
    ///
    /// Returns the remaining active lease count after release.
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
            .get_mut(&(bucket.clone(), key.clone(), generation_id))
            .expect("object payload lease release without acquire");
        *entry -= 1;
        let remaining = *entry;
        if remaining == 0 {
            state.leases.remove(&root);
        }
        remaining
    }

    /// Fence an object payload generation for physical reclaim.
    ///
    /// Returns false if any read lease or another reclaim is active.
    pub(crate) fn try_begin_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> bool {
        let root = (bucket.clone(), key.clone(), generation_id);
        let mut state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state.leases.get(&root).copied().unwrap_or(0) != 0 {
            return false;
        }
        if let Some(active) = state.active_reclaims.get(&root) {
            return active == authority;
        }
        state
            .active_reclaims
            .insert(root.clone(), authority.clone());
        state.reclaim_fences.insert(root, authority.clone());
        true
    }

    /// Finish physical reclaim fencing for a generation.
    pub(crate) fn finish_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        authority: &ObjectPayloadReclaimClaimProof,
        keep_fence: bool,
    ) -> bool {
        let root = (bucket.clone(), key.clone(), generation_id);
        let mut state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state
            .active_reclaims
            .get(&root)
            .is_some_and(|active| active != authority)
            || state
                .reclaim_fences
                .get(&root)
                .is_some_and(|fence| fence != authority)
        {
            return false;
        }
        state.active_reclaims.remove(&root);
        if !keep_fence {
            state.reclaim_fences.remove(&root);
        }
        true
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_reclaim_is_active(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .active_reclaims
            .contains_key(&(bucket.clone(), key.clone(), generation_id))
    }

    /// Clear a reclaim fence after a matching terminal reclaim command has converged.
    pub(crate) fn clear_object_payload_reclaim_fence(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> bool {
        let mut state = self
            .object_payload_leases
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let root = (bucket.clone(), key.clone(), generation_id);
        if state
            .reclaim_fences
            .get(&root)
            .is_some_and(|fence| fence != authority)
        {
            return false;
        }
        state.reclaim_fences.remove(&root);
        true
    }

    /// Return the number of active object-payload leases for a generation.
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

    /// Return the number of active object-payload leases for a bucket.
    #[cfg(any(test, feature = "test-hooks"))]
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

    /// Queue a payload generation for background reclaim.
    pub fn enqueue_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        matches!(
            self.enqueue_object_payload_reclaim_for_pg(
                bucket,
                key,
                generation_id,
                self.pg_topology.object_pg_for(bucket, key)
            ),
            ReclaimQueueInsert::Queued
        )
    }

    pub(crate) fn enqueue_object_payload_reclaim_for_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        _pg_id: u32,
    ) -> ReclaimQueueInsert {
        let root = (bucket.clone(), key.clone(), generation_id);
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        if state.queued_objects.insert(root.clone()) {
            state
                .work_queue
                .push_back(ReclaimWorkItem::ObjectPayload(root));
            Self::emit_reclaim_queue_action(&state, "object_payload", "enqueue");
            cv.notify_one();
            ReclaimQueueInsert::Queued
        } else {
            Self::emit_reclaim_queue_action(&state, "object_payload", "deduplicate");
            ReclaimQueueInsert::Deduplicated
        }
    }

    /// Queue a bucket for deferred final deletion once reclaim is drained.
    pub fn enqueue_bucket_delete_finalize(&self, root: BucketDeleteFinalizeRoot) -> bool {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        state.outstanding_bucket_deletes.insert(root.clone());
        if state.queued_bucket_deletes.insert(root.clone()) {
            state
                .work_queue
                .push_back(ReclaimWorkItem::BucketDelete(root));
            Self::emit_reclaim_queue_action(&state, "bucket_delete", "enqueue");
            cv.notify_one();
            true
        } else {
            Self::emit_reclaim_queue_action(&state, "bucket_delete", "deduplicate");
            false
        }
    }

    /// Queue an active-bucket DeleteBucket begin attempt for background retry.
    pub(crate) fn enqueue_bucket_delete_begin(&self, root: BucketDeleteBeginRoot) -> bool {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        if state.queued_bucket_delete_begins.insert(root.clone()) {
            state
                .work_queue
                .push_back(ReclaimWorkItem::BucketDeleteBegin(root));
            Self::emit_reclaim_queue_action(&state, "bucket_delete_begin", "enqueue");
            cv.notify_one();
            true
        } else {
            Self::emit_reclaim_queue_action(&state, "bucket_delete_begin", "deduplicate");
            false
        }
    }

    pub fn finish_bucket_delete_finalize_work(&self, root: &BucketDeleteFinalizeRoot) {
        let mut state = self
            .reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state.queued_bucket_deletes.remove(root);
        state.queued_bucket_delete_begins.retain(|begin| {
            begin.bucket != root.bucket
                || begin.bucket_incarnation_generation != root.bucket_incarnation_generation
        });
        state.work_queue.retain(|work| match work {
            ReclaimWorkItem::BucketDelete(queued_root) => queued_root != root,
            ReclaimWorkItem::BucketDeleteBegin(begin) => {
                begin.bucket != root.bucket
                    || begin.bucket_incarnation_generation != root.bucket_incarnation_generation
            }
            ReclaimWorkItem::ObjectPayload(_) => true,
        });
        if state.outstanding_bucket_deletes.remove(root) {
            Self::emit_reclaim_queue_action(&state, "bucket_delete", "finish");
        }
    }

    /// Take one queued reclaim work item if immediately available.
    pub fn try_take_reclaim_work(&self) -> Option<ReclaimWorkItem> {
        let mut state = self
            .reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        Self::pop_reclaim_work(&mut state)
    }

    /// Block until reclaim work is available, or stop has been requested.
    pub fn wait_for_reclaim_work(&self, stop: &AtomicBool) -> Option<ReclaimWorkItem> {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        while state.work_queue.is_empty() && !stop.load(Ordering::SeqCst) {
            let (next_state, _) = cv
                .wait_timeout(
                    state,
                    std::time::Duration::from_millis(RECLAIM_WORKER_WAIT_POLL_MILLIS),
                )
                .unwrap_or_else(|e| e.into_inner());
            state = next_state;
        }
        if stop.load(Ordering::SeqCst) {
            return None;
        }
        Self::pop_reclaim_work(&mut state)
    }

    /// Wake reclaim workers so they can observe shutdown or new work.
    pub fn wake_reclaim_workers(&self) {
        self.reclaim_queue.1.notify_all();
    }

    fn pop_reclaim_work(state: &mut ReclaimQueueState) -> Option<ReclaimWorkItem> {
        let work = state.work_queue.pop_front()?;
        match &work {
            ReclaimWorkItem::ObjectPayload(root) => {
                state.queued_objects.remove(root);
                Self::emit_reclaim_queue_action(state, "object_payload", "dequeue");
            }
            ReclaimWorkItem::BucketDeleteBegin(root) => {
                state.queued_bucket_delete_begins.remove(root);
                Self::emit_reclaim_queue_action(state, "bucket_delete_begin", "dequeue");
            }
            ReclaimWorkItem::BucketDelete(root) => {
                state.queued_bucket_deletes.remove(root);
                Self::emit_reclaim_queue_action(state, "bucket_delete", "dequeue");
            }
        }
        Some(work)
    }

    fn emit_reclaim_queue_action(
        state: &ReclaimQueueState,
        work_kind: &'static str,
        action: &'static str,
    ) {
        let _ = observability::emit_reclaim_queue_action(
            TRACE_TARGET,
            observability::ReclaimQueueSummary {
                work_kind,
                action,
                queue_depth: state.work_queue.len(),
                object_payload_depth: state.queued_objects.len(),
                object_payload_outstanding_depth: 0,
                bucket_delete_begin_depth: state.queued_bucket_delete_begins.len(),
                bucket_delete_finalize_depth: state.queued_bucket_deletes.len(),
                bucket_delete_finalize_outstanding_depth: state.outstanding_bucket_deletes.len(),
            },
        );
    }
}

impl EncodeScratchPool {
    fn new(_: EcConfig) -> Self {
        let max_cached = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);
        Self {
            max_cached,
            cached: Mutex::new(Vec::new()),
            #[cfg(any(test, feature = "test-hooks"))]
            allocations: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn checkout(self: &Arc<Self>, required_len: usize) -> EncodeScratch {
        let mut cached = self.cached.lock().unwrap();
        let maybe_idx = cached.iter().rposition(|buf| buf.len() >= required_len);
        let mut buf = maybe_idx.map_or_else(
            || {
                #[cfg(any(test, feature = "test-hooks"))]
                self.allocations.fetch_add(1, Ordering::Relaxed);
                vec![0u8; required_len]
            },
            |idx| cached.swap_remove(idx),
        );
        drop(cached);
        if buf.len() < required_len {
            buf.resize(required_len, 0);
        }
        EncodeScratch {
            pool: Arc::clone(self),
            buf: Some(buf),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn allocation_count(&self) -> usize {
        self.allocations.load(Ordering::Relaxed)
    }
}

impl EncodeScratch {
    fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        debug_assert!(len <= self.buf.as_ref().unwrap().len());
        &mut self.buf.as_mut().unwrap()[..len]
    }

    fn as_slice(&self, len: usize) -> &[u8] {
        debug_assert!(len <= self.buf.as_ref().unwrap().len());
        &self.buf.as_ref().unwrap()[..len]
    }
}

impl Drop for EncodeScratch {
    fn drop(&mut self) {
        let Some(buf) = self.buf.take() else {
            return;
        };
        let mut cached = self.pool.cached.lock().unwrap();
        if cached.len() < self.pool.max_cached {
            cached.push(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket_name(name: &str) -> BucketName {
        BucketName::try_from(name).unwrap()
    }

    fn object_key(key: &str) -> ObjectKey {
        ObjectKey::try_from(key.to_string()).unwrap()
    }

    fn put_standard_object_for_snapshot_test(
        node: &SharedStorageNode,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        size: u64,
    ) -> StoredObject {
        let object_pg = node
            .get_pg(node.pg_topology().object_pg_for(bucket, key))
            .unwrap();
        object_pg
            .put_object_with_segments(
                &PutLiveObjectReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: VersionId::Null,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id,
                    size,
                    etag: crate::ObjectEtag::single_part(size),
                    ec: SharedStorageNode::DEFAULT_EC_SHAPE,
                    layout: crate::ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                },
                &[],
            )
            .unwrap();
        object_pg.get_object_meta(bucket, key).unwrap()
    }

    #[test]
    fn data_dir_accessor() {
        let tmp = test_util::tempdir();
        let node = LocalStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        assert_eq!(node.data_dir(), tmp.path());
    }

    #[test]
    fn get_pg_store_not_found() {
        let tmp = test_util::tempdir();
        let node = LocalStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let result = node.get_pg_store(999);
        assert!(result.is_err());
    }

    #[test]
    fn get_pg_valid() {
        let tmp = test_util::tempdir();
        let node = LocalStorageNode::open(tmp.path(), &[0, 1, 2]).unwrap();
        assert!(node.get_pg(0).is_ok());
        assert!(node.get_pg(1).is_ok());
        assert!(node.get_pg(2).is_ok());
        assert!(node.get_pg(3).is_err());
    }

    #[test]
    fn pg_ids_sorted() {
        let tmp = test_util::tempdir();
        let node = LocalStorageNode::open(tmp.path(), &[5, 2, 8, 1]).unwrap();
        assert_eq!(node.pg_ids(), &[1, 2, 5, 8]);
    }

    #[test]
    fn shared_storage_node_rejects_invalid_default_ec_shape() {
        let tmp = test_util::tempdir();
        let err = match SharedStorageNode::open_with_default_ec_shape(
            tmp.path(),
            &[0, 1],
            EcShape { k: 0, m: 2 },
        ) {
            Ok(_) => panic!("expected invalid default EC shape to be rejected"),
            Err(err) => err,
        };

        assert!(matches!(err, StoreError::ErasureCoding { .. }));
    }

    #[test]
    fn shared_storage_node_rejects_empty_pg_topology_without_panic() {
        let tmp = test_util::tempdir();
        let err = match SharedStorageNode::open(tmp.path(), &[]) {
            Ok(_) => panic!("expected empty PG topology to be rejected"),
            Err(err) => err,
        };

        assert!(matches!(
            err,
            StoreError::InvalidPgTopology {
                source: crate::pg_topology::PgTopologyError::Empty
            }
        ));
    }

    #[test]
    fn topology_only_storage_node_rejects_empty_pg_topology_without_panic() {
        let err = match SharedStorageNode::topology_only(&[], SharedStorageNode::DEFAULT_EC_SHAPE) {
            Ok(_) => panic!("expected empty PG topology to be rejected"),
            Err(err) => err,
        };

        assert!(matches!(
            err,
            StoreError::InvalidPgTopology {
                source: crate::pg_topology::PgTopologyError::Empty
            }
        ));
    }

    // ── SharedStorageNode tests ──────────────────────────────────────

    #[test]
    fn shared_node_open_and_get_pg() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1, 2]).unwrap();
        assert!(node.get_pg(0).is_ok());
        assert!(node.get_pg(1).is_ok());
        assert!(node.get_pg(2).is_ok());
        assert!(node.get_pg(3).is_err());
    }

    #[test]
    fn shared_node_pg_ids_sorted() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[5, 2, 8, 1]).unwrap();
        assert_eq!(node.pg_ids(), &[1, 2, 5, 8]);
    }

    #[test]
    fn shared_node_data_dir() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        assert_eq!(node.data_dir(), tmp.path());
    }

    #[test]
    fn shared_node_pg_heartbeat_observation_uses_metadata_replica_state() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let metadata_state = {
            let pg = node.get_pg(0).unwrap();
            pg.metadata_command_replica_state().unwrap()
        };

        let observation = node
            .pg_heartbeat_observation(NodeId::new(7), PgId::new(0), PgState::Peering)
            .unwrap();

        assert_eq!(observation.pg_id, PgId::new(0));
        assert_eq!(observation.state, PgState::Peering);
        assert_eq!(
            observation.metadata_proof.applied_log_index,
            metadata_state.applied_log_index
        );
        assert_eq!(
            observation.metadata_proof.applied_log_hash,
            metadata_state.applied_log_hash
        );
        assert_eq!(
            observation.metadata_proof.state_digest,
            metadata_state.state_digest
        );
        assert!(!observation.has_pending_metadata_command());
    }

    #[test]
    fn shared_node_pg_heartbeat_observation_reports_pending_metadata_command() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let bucket = bucket_name("pending-heartbeat");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let config = CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config(&config, 123, 1).unwrap(),
            ),
        );
        {
            let pg = node.get_pg(0).unwrap();
            pg.try_insert_pending_metadata_command_slot(7, &command, Some(&bucket))
                .unwrap();
        }

        let observation = node
            .pg_heartbeat_observation(NodeId::new(7), PgId::new(0), PgState::Peering)
            .unwrap();

        assert!(observation.has_pending_metadata_command());
    }

    #[test]
    fn shared_node_pg_heartbeat_observation_preserves_epoch_mismatched_pending_command() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let bucket = bucket_name("future-pending-heartbeat");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let config = CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let future_epoch = ClusterEpoch::new(2).unwrap();
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                future_epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config(&config, 123, 1).unwrap(),
            ),
        );
        let metadata_state = {
            let pg = node.get_pg(0).unwrap();
            pg.try_insert_pending_metadata_command_slot(7, &command, Some(&bucket))
                .unwrap();
            assert!(pg
                .pending_metadata_command_slot_any_epoch(7)
                .unwrap()
                .is_some());
            pg.metadata_command_replica_state().unwrap()
        };

        let observation = node
            .pg_heartbeat_observation(NodeId::new(7), PgId::new(0), PgState::Peering)
            .unwrap();

        assert_eq!(
            observation.metadata_proof.applied_log_index,
            metadata_state.applied_log_index
        );
        assert!(observation.has_pending_metadata_command());
        let pg = node.get_pg(0).unwrap();
        let slot = pg
            .pending_metadata_command_slot_any_epoch(7)
            .unwrap()
            .expect("heartbeat must preserve epoch-mismatched pending slot");
        assert_eq!(slot.id, command.id());
        assert_eq!(slot.command_checksum, command.checksum_crc64());
    }

    #[test]
    fn shared_node_pg_heartbeat_observation_preserves_terminal_pending_command() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let bucket = bucket_name("terminal-pending-heartbeat");
        let owner = crate::OwnerIdentity::from_principal("owner");
        let config = CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config(&config, 123, 1).unwrap(),
            ),
        );
        {
            let pg = node.get_pg(0).unwrap();
            pg.try_insert_pending_metadata_command_slot(7, &command, Some(&bucket))
                .unwrap();
            pg.record_metadata_command_applied(7, &command).unwrap();
            assert!(pg
                .pending_metadata_command_slot(7, ClusterEpoch::INITIAL)
                .unwrap()
                .is_some());
        }

        let observation = node
            .pg_heartbeat_observation(NodeId::new(7), PgId::new(0), PgState::Active)
            .unwrap();

        assert_eq!(observation.metadata_proof.applied_log_index, 1);
        assert!(observation.has_pending_metadata_command());
        let pg = node.get_pg(0).unwrap();
        let slot = pg
            .pending_metadata_command_slot(7, ClusterEpoch::INITIAL)
            .unwrap()
            .expect("heartbeat must preserve terminal pending slot");
        assert_eq!(slot.id, command.id());
        assert_eq!(slot.command_checksum, command.checksum_crc64());
    }

    #[test]
    fn shared_node_pg_heartbeat_observation_validates_cached_metadata_state() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        {
            let pg = node.get_pg(0).unwrap();
            pg.test_increment_metadata_command_replica_state_digest()
                .unwrap();
        }

        let err = node
            .pg_heartbeat_observation(NodeId::new(7), PgId::new(0), PgState::Peering)
            .unwrap_err();

        assert!(
            matches!(
                err,
                StoreError::MetadataStateDigestMismatch {
                    node_id: 7,
                    pg_id: 0,
                    ..
                }
            ),
            "heartbeat proof must not advertise a corrupted replay state: {err:?}"
        );
    }

    fn create_bucket_command_for(bucket: &str, epoch: ClusterEpoch) -> MetadataCommandEnvelope {
        let owner = crate::OwnerIdentity::from_principal("owner");
        let config = CreateBucketConfig {
            name: bucket,
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                epoch,
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config(&config, 123, 1).unwrap(),
            ),
        )
    }

    #[test]
    fn shared_node_recover_succeeds_on_clean_store() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        node.recover_pg_metadata_command_state(NodeId::new(7))
            .expect("recovering a freshly opened clean store must succeed");
    }

    #[test]
    fn shared_node_recover_cleans_older_epoch_orphan_pending_command() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let newer_epoch = ClusterEpoch::new(2).unwrap();
        let newer_bucket = bucket_name("newer-state-recover");
        let newer_command = create_bucket_command_for(newer_bucket.as_str(), newer_epoch);
        let old_bucket = bucket_name("old-pending-recover");
        let old_command = create_bucket_command_for(old_bucket.as_str(), ClusterEpoch::INITIAL);
        let metadata_state = {
            let pg = node.get_pg(0).unwrap();
            pg.apply_metadata_command_and_record(7, &newer_command)
                .unwrap();
            let metadata_state = pg.metadata_command_replica_state().unwrap();
            assert_eq!(metadata_state.cluster_epoch, newer_epoch);
            pg.try_insert_pending_metadata_command_slot(7, &old_command, Some(&old_bucket))
                .unwrap();
            assert!(pg
                .pending_metadata_command_slot_any_epoch(7)
                .unwrap()
                .is_some());
            metadata_state
        };

        // Recovery may clean epoch-mismatched orphans only when the slot is
        // older than the store's replica-state epoch. A future-epoch slot can
        // still be an in-flight first command for that epoch and must fail
        // closed without acting-set evidence.
        node.recover_pg_metadata_command_state(NodeId::new(7))
            .expect("recovery must reconcile an epoch-mismatched orphan slot");

        let pg = node.get_pg(0).unwrap();
        assert!(
            pg.pending_metadata_command_slot_any_epoch(7)
                .unwrap()
                .is_none(),
            "orphan pending slot must be removed by recovery"
        );
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, metadata_state.applied_log_index);
        assert_eq!(state.cluster_epoch, metadata_state.cluster_epoch);
    }

    #[test]
    fn shared_node_recover_preserves_terminal_pending_command() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let bucket = bucket_name("terminal-pending-recover");
        let command = create_bucket_command_for(bucket.as_str(), ClusterEpoch::INITIAL);
        {
            let pg = node.get_pg(0).unwrap();
            pg.try_insert_pending_metadata_command_slot(7, &command, Some(&bucket))
                .unwrap();
            pg.record_metadata_command_applied(7, &command).unwrap();
            assert!(pg
                .pending_metadata_command_slot(7, ClusterEpoch::INITIAL)
                .unwrap()
                .is_some());
        }

        node.recover_pg_metadata_command_state(NodeId::new(7))
            .expect("recovery must validate a terminal pending slot");

        let pg = node.get_pg(0).unwrap();
        let slot = pg
            .pending_metadata_command_slot(7, ClusterEpoch::INITIAL)
            .unwrap()
            .expect("local recovery must preserve terminal pending slot until acting-set evidence");
        assert_eq!(slot.id, command.id());
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 1);
    }

    #[test]
    fn shared_node_recover_fails_closed_on_corrupted_state_digest() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        {
            let pg = node.get_pg(0).unwrap();
            pg.test_increment_metadata_command_replica_state_digest()
                .unwrap();
        }

        let err = node
            .recover_pg_metadata_command_state(NodeId::new(7))
            .unwrap_err();

        assert!(
            matches!(
                err,
                StoreError::MetadataStateDigestMismatch {
                    node_id: 7,
                    pg_id: 0,
                    ..
                }
            ),
            "recovery must fail closed on a corrupted state digest: {err:?}"
        );
    }

    #[test]
    fn shared_node_pg_heartbeat_observation_does_not_replay_applied_log_prefix() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        {
            let pg = node.get_pg(0).unwrap();
            let owner = crate::OwnerIdentity::from_principal("owner");
            for index in 1..=4 {
                let bucket = bucket_name(&format!("heartbeat-replay-{index}"));
                let config = CreateBucketConfig {
                    name: bucket.as_str(),
                    owner_principal: &owner.principal,
                    owner_canonical_id: &owner.canonical_id,
                    acl_grants: &AclGrants::default(),
                    public_read: false,
                    public_write: false,
                    versioning: BucketVersioningState::Disabled,
                    object_lock: BucketObjectLockConfig::default(),
                    ownership_controls: crate::BucketOwnershipControls {
                        object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                    },
                };
                let command = MetadataCommandEnvelope::new(
                    MetadataCommandId::new(
                        ClusterEpoch::INITIAL,
                        PgId::new(0),
                        MetadataCommandLogIndex::new(index).unwrap(),
                    ),
                    MetadataCommandPayload::CreateBucket(
                        CreateBucketCommand::from_config(&config, 123, index).unwrap(),
                    ),
                );
                pg.record_metadata_command_applied(7, &command).unwrap();
            }
            assert_eq!(
                pg.metadata_command_replica_state()
                    .unwrap()
                    .applied_log_index,
                4
            );
        }

        let before = {
            let pg = node.get_pg(0).unwrap();
            pg.test_metadata_command_log_replay_validation_entries()
        };
        let observation = node
            .pg_heartbeat_observation(NodeId::new(7), PgId::new(0), PgState::Peering)
            .unwrap();
        let after = {
            let pg = node.get_pg(0).unwrap();
            pg.test_metadata_command_log_replay_validation_entries()
        };

        assert_eq!(
            observation.metadata_proof.applied_log_index, 4,
            "heartbeat should report the maintained metadata proof"
        );
        assert_eq!(
            after, before,
            "heartbeat must not run full command-log replay validation"
        );
    }

    #[test]
    fn shared_node_control_plane_heartbeat_includes_pg_proofs() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();

        let heartbeat = node
            .control_plane_heartbeat(
                NodeId::new(7),
                42,
                "node-7.sock",
                ClusterEpoch::new(3).unwrap(),
                1_000,
                [
                    (PgId::new(0), PgState::Peering),
                    (PgId::new(1), PgState::Active),
                ],
            )
            .unwrap();

        assert_eq!(heartbeat.node_id, NodeId::new(7));
        assert_eq!(heartbeat.node_incarnation, 42);
        assert_eq!(heartbeat.endpoint, "node-7.sock");
        assert_eq!(heartbeat.observed_epoch, ClusterEpoch::new(3).unwrap());
        assert_eq!(heartbeat.requested_lease_duration_ms, 1_000);
        assert_eq!(
            heartbeat.cluster_map_history_route_references,
            node.cluster_map_history_route_references().unwrap()
        );
        assert_eq!(heartbeat.pg_observations.len(), 2);
        assert_eq!(heartbeat.pg_observations[0].pg_id, PgId::new(0));
        assert_eq!(heartbeat.pg_observations[0].state, PgState::Peering);
        assert_eq!(heartbeat.pg_observations[1].pg_id, PgId::new(1));
        assert_eq!(heartbeat.pg_observations[1].state, PgState::Active);

        for observation in &heartbeat.pg_observations {
            let metadata_state = {
                let pg = node.get_pg(observation.pg_id.get()).unwrap();
                pg.metadata_command_replica_state().unwrap()
            };
            assert_eq!(
                observation.metadata_proof.applied_log_index,
                metadata_state.applied_log_index
            );
            assert_eq!(
                observation.metadata_proof.applied_log_hash,
                metadata_state.applied_log_hash
            );
            assert_eq!(
                observation.metadata_proof.state_digest,
                metadata_state.state_digest
            );
        }
    }

    fn create_bucket_for_snapshot_test(node: &SharedStorageNode, name: &str) -> BucketName {
        let bucket = bucket_name(name);
        let bucket_pg = node
            .get_pg(node.pg_topology().bucket_pg_for(&bucket))
            .unwrap();
        bucket_pg
            .create_bucket(
                &bucket,
                "owner",
                &s3_types::CanonicalUserId::from_principal("owner"),
                &s3_types::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
        bucket
    }

    #[test]
    fn load_bucket_snapshot_loads_tags_when_abac_enabled() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let bucket_pg = node
            .get_pg(node.pg_topology().bucket_pg_for(&bucket))
            .unwrap();
        bucket_pg
            .put_bucket_subresource(
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: crate::types::BucketSubresourceKind::Tagging,
                    body: "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        bucket_pg.put_bucket_abac_enabled(&bucket, true).unwrap();
        drop(bucket_pg);

        let snapshot = node
            .load_bucket_snapshot(
                &bucket,
                crate::types::BucketSnapshotRequest {
                    tags: crate::types::BucketSnapshotTagsRequest::IfBucketAbacEnabled,
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(matches!(
            snapshot.tags,
            crate::types::LoadedBucketSubresource::Loaded(_)
        ));
    }

    #[test]
    fn load_bucket_snapshot_skips_tags_when_abac_disabled() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let bucket_pg = node
            .get_pg(node.pg_topology().bucket_pg_for(&bucket))
            .unwrap();
        bucket_pg
            .put_bucket_subresource(
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: crate::types::BucketSubresourceKind::Tagging,
                    body: "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        drop(bucket_pg);

        let snapshot = node
            .load_bucket_snapshot(
                &bucket,
                crate::types::BucketSnapshotRequest {
                    tags: crate::types::BucketSnapshotTagsRequest::IfBucketAbacEnabled,
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(matches!(
            snapshot.tags,
            crate::types::LoadedBucketSubresource::NotRequested
        ));
    }

    #[test]
    fn object_read_snapshot_subject_rejects_changed_object_row() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let key = object_key("key");
        let original =
            put_standard_object_for_snapshot_test(&node, &bucket, &key, GenerationId::MIN, 1);
        let subject = node
            .load_object_read_auth_subject(&bucket, &key, None)
            .unwrap();
        assert_eq!(subject.stored, original);

        put_standard_object_for_snapshot_test(
            &node,
            &bucket,
            &key,
            GenerationId::new(2).unwrap(),
            2,
        );

        let err = node
            .load_object_read_snapshot_for_subject(
                &bucket,
                &key,
                None,
                &subject.identity,
                crate::ObjectReadSnapshotMode::MetadataOnly,
            )
            .unwrap_err();
        assert!(matches!(err, ObjectPgActionError::StaleObjectReadSubject));

        let fresh_subject = node
            .load_object_read_auth_subject(&bucket, &key, None)
            .unwrap();
        let snapshot = node
            .load_object_read_snapshot_for_subject(
                &bucket,
                &key,
                None,
                &fresh_subject.identity,
                crate::ObjectReadSnapshotMode::MetadataOnly,
            )
            .unwrap();
        assert_eq!(snapshot.stored, fresh_subject.stored);
    }

    #[test]
    fn object_read_snapshot_subject_treats_missing_object_as_stale() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let key = object_key("key");
        put_standard_object_for_snapshot_test(&node, &bucket, &key, GenerationId::MIN, 1);
        let subject = node
            .load_object_read_auth_subject(&bucket, &key, None)
            .unwrap();

        let object_pg = node
            .get_pg(node.pg_topology().object_pg_for(&bucket, &key))
            .unwrap();
        object_pg.delete_object_meta(&bucket, &key).unwrap();
        drop(object_pg);

        let err = node
            .load_object_read_snapshot_for_subject(
                &bucket,
                &key,
                None,
                &subject.identity,
                crate::ObjectReadSnapshotMode::MetadataOnly,
            )
            .unwrap_err();
        assert!(matches!(err, ObjectPgActionError::StaleObjectReadSubject));
    }

    #[test]
    fn load_bucket_snapshot_keeps_loaded_tag_view_after_bucket_mutation() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        let bucket_pg_id = node.pg_topology().bucket_pg_for(&bucket);
        let bucket_pg = node.get_pg(bucket_pg_id).unwrap();
        bucket_pg
            .put_bucket_subresource(
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: crate::types::BucketSubresourceKind::Tagging,
                    body: "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        bucket_pg.put_bucket_abac_enabled(&bucket, true).unwrap();
        drop(bucket_pg);

        let snapshot = node
            .load_bucket_snapshot(
                &bucket,
                crate::types::BucketSnapshotRequest {
                    tags: crate::types::BucketSnapshotTagsRequest::IfBucketAbacEnabled,
                    ..Default::default()
                },
            )
            .unwrap();

        let bucket_pg = node.get_pg(bucket_pg_id).unwrap();
        bucket_pg
            .put_bucket_subresource(
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: crate::types::BucketSubresourceKind::Tagging,
                    body: "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();

        assert_eq!(
            snapshot.tags,
            crate::types::LoadedBucketSubresource::Loaded(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>".to_owned()
            )
        );
    }

    #[test]
    fn finish_bucket_write_snapshot_operation_preserves_action_error_over_release_error() {
        let result = SharedStorageNode::finish_bucket_write_snapshot_operation::<(), &'static str>(
            Ok(Err("action failed")),
            Err(crate::error::MetadataError::BucketWriteDraining.into()),
        )
        .unwrap();
        assert_eq!(result, Err("action failed"));
    }

    #[test]
    fn try_finalize_bucket_delete_finalizes_empty_deleting_bucket() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = create_bucket_for_snapshot_test(&node, "bucket");
        node.mark_bucket_deleting(&bucket).unwrap();

        assert_eq!(
            node.try_finalize_bucket_delete(&bucket).unwrap(),
            BucketDeleteFinalizeOutcome::Finalized
        );
        assert_eq!(
            node.try_finalize_bucket_delete(&bucket).unwrap(),
            BucketDeleteFinalizeOutcome::NotFound
        );
    }

    #[test]
    fn bucket_delete_finalize_outstanding_survives_dequeue_until_finish() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = bucket_name("bucket");
        let root = BucketDeleteFinalizeRoot {
            bucket: bucket.clone(),
            bucket_incarnation_generation: 1,
        };

        assert!(node.enqueue_bucket_delete_finalize(root.clone()));
        {
            let state = node.reclaim_queue.0.lock().unwrap();
            assert_eq!(state.queued_bucket_deletes.len(), 1);
            assert_eq!(state.outstanding_bucket_deletes.len(), 1);
        }

        assert_eq!(
            node.try_take_reclaim_work(),
            Some(ReclaimWorkItem::BucketDelete(root.clone()))
        );
        {
            let state = node.reclaim_queue.0.lock().unwrap();
            assert!(state.queued_bucket_deletes.is_empty());
            assert_eq!(state.outstanding_bucket_deletes.len(), 1);
        }

        node.finish_bucket_delete_finalize_work(&root);
        let state = node.reclaim_queue.0.lock().unwrap();
        assert!(state.queued_bucket_deletes.is_empty());
        assert!(state.outstanding_bucket_deletes.is_empty());
    }

    #[test]
    fn finish_bucket_delete_finalize_work_purges_stale_queued_items() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = bucket_name("bucket");
        let other_bucket = bucket_name("other");
        let stale_begin = BucketDeleteBeginRoot {
            bucket: bucket.clone(),
            bucket_execution_generation: 10,
            bucket_incarnation_generation: 20,
        };
        let other_begin = BucketDeleteBeginRoot {
            bucket: other_bucket.clone(),
            bucket_execution_generation: 11,
            bucket_incarnation_generation: 21,
        };
        let root = BucketDeleteFinalizeRoot {
            bucket: bucket.clone(),
            bucket_incarnation_generation: 20,
        };
        let other_root = BucketDeleteFinalizeRoot {
            bucket: other_bucket.clone(),
            bucket_incarnation_generation: 21,
        };

        assert!(node.enqueue_bucket_delete_finalize(root.clone()));
        assert_eq!(
            node.try_take_reclaim_work(),
            Some(ReclaimWorkItem::BucketDelete(root.clone()))
        );
        assert!(node.enqueue_bucket_delete_finalize(root.clone()));
        assert!(node.enqueue_bucket_delete_begin(stale_begin));
        assert!(node.enqueue_bucket_delete_begin(other_begin.clone()));
        assert!(node.enqueue_bucket_delete_finalize(other_root.clone()));

        node.finish_bucket_delete_finalize_work(&root);

        assert_eq!(
            node.try_take_reclaim_work(),
            Some(ReclaimWorkItem::BucketDeleteBegin(other_begin))
        );
        assert_eq!(
            node.try_take_reclaim_work(),
            Some(ReclaimWorkItem::BucketDelete(other_root))
        );
        assert_eq!(node.try_take_reclaim_work(), None);
    }

    #[test]
    fn old_bucket_delete_completion_does_not_clear_recreated_incarnation() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0, 1]).unwrap();
        let bucket = bucket_name("bucket");
        let old_root = BucketDeleteFinalizeRoot {
            bucket: bucket.clone(),
            bucket_incarnation_generation: 20,
        };
        let recreated_root = BucketDeleteFinalizeRoot {
            bucket,
            bucket_incarnation_generation: 21,
        };

        assert!(node.enqueue_bucket_delete_finalize(old_root.clone()));
        assert_eq!(
            node.try_take_reclaim_work(),
            Some(ReclaimWorkItem::BucketDelete(old_root.clone()))
        );
        assert!(node.enqueue_bucket_delete_finalize(recreated_root.clone()));

        node.finish_bucket_delete_finalize_work(&old_root);

        assert_eq!(
            node.try_take_reclaim_work(),
            Some(ReclaimWorkItem::BucketDelete(recreated_root))
        );
        assert_eq!(
            node.reclaim_queue
                .0
                .lock()
                .unwrap()
                .outstanding_bucket_deletes
                .len(),
            1
        );
    }

    #[test]
    fn shared_node_read_shard_file_roundtrip() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let key = crate::types::ShardKey::new(&[0xAB; 16], 7, 0);
        {
            let pg = node.get_pg(0).unwrap();
            pg.write_shard(&key, b"hello").unwrap();
        }

        let data = node.read_shard_file(0, &key).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn shared_node_read_shard_file_into_roundtrip() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let key = crate::types::ShardKey::new(&[0xBC; 16], 7, 0);
        {
            let pg = node.get_pg(0).unwrap();
            pg.write_shard(&key, b"hello").unwrap();
        }

        let mut buf = [0u8; 5];
        node.read_shard_file_into(0, &key, &mut buf).unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn shared_node_read_shard_file_into_length_mismatch() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let key = crate::types::ShardKey::new(&[0xCE; 16], 7, 0);
        {
            let pg = node.get_pg(0).unwrap();
            pg.write_shard(&key, b"hello").unwrap();
        }

        let mut buf = [0u8; 4];
        let err = node.read_shard_file_into(0, &key, &mut buf).unwrap_err();
        match err {
            StoreError::Io { context, source } => {
                assert_eq!(context, "read shard file length mismatch");
                assert_eq!(source.kind(), std::io::ErrorKind::InvalidData);
            }
            other => panic!("expected length mismatch io error, got {other:?}"),
        }
    }

    #[test]
    fn shared_node_read_shard_file_missing() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let key = crate::types::ShardKey::new(&[0xCD; 16], 7, 0);
        let err = node.read_shard_file(0, &key).unwrap_err();
        assert!(matches!(err, StoreError::NotFound));
    }

    #[test]
    fn shared_node_write_shard_file_requires_metadata_registration() {
        let tmp = test_util::tempdir();
        let node = SharedStorageNode::open(tmp.path(), &[0]).unwrap();
        let key = crate::types::ShardKey::new(&[0xDD; 16], 7, 0);

        let ack = node.write_shard_file(0, &key, b"hello").unwrap();
        assert_eq!(node.read_shard_file(0, &key).unwrap(), b"hello");

        let pg = node.get_pg(0).unwrap();
        let err = pg.read_shard(&key).unwrap_err();
        assert!(matches!(err, StoreError::NotFound));
        pg.register_written_shard(&key, ack).unwrap();
        assert_eq!(pg.read_shard(&key).unwrap().data, b"hello");
    }

    #[test]
    fn shared_node_scavenger_file_scan_does_not_wait_for_pg_mutex() {
        let tmp = test_util::tempdir();
        let node = Arc::new(SharedStorageNode::open(tmp.path(), &[0]).unwrap());
        let key = crate::types::ShardKey::new(&[0xDE; 16], 7, 0);
        node.write_shard_file(0, &key, b"hello").unwrap();

        let pg_guard = node.get_pg(0).unwrap();
        let scan_node = Arc::clone(&node);
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let result = scan_node
                .list_scavenger_shard_files(0)
                .map(|scan| scan.files.len());
            tx.send(result).unwrap();
        });

        let scanned = rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("scavenger file scan should not wait for PgStore mutex")
            .unwrap();
        assert_eq!(scanned, 1);
        drop(pg_guard);
        handle.join().unwrap();
    }
}
