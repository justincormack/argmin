// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::Barrier;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use checksum::{ChecksumAlgorithm, ChecksumHasher};
use placement::{NodeId, PlacementConstraint, PlacementError, TopologyKey};

use super::{AcquiredObjectPayloadNodeLeases, ProcessLocalRegistryKey, ShardLocation};
use crate::control_plane::{
    digest_pg_routes, reconstruct_sparse_pg_route_at_epoch, ClusterRuntimeMapSnapshot,
    NodeRouteSnapshot, PgRouteSnapshot,
};
use crate::control_plane_lease::{
    validate_process_lease_clock, BoundRouteMapLease, CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
};
use crate::data_dir::prepare_private_data_dir;
use crate::error::{ClusterBuildError, ShardIoError, StoreError};
#[cfg(test)]
use crate::metadata_command::MetadataCommandLogIndex;
use crate::metadata_command::{
    MetadataCommandAcceptance, MetadataCommandEnvelope, MetadataCommandReplicaState,
    ObjectPayloadReclaimClaimProof,
};
#[cfg(any(test, feature = "test-hooks"))]
use crate::node::SharedStorageNode;
use crate::node::{
    LocalNodeRuntime, ReclaimQueueInsert, OBJECT_PAYLOAD_RECLAIM_MAX_OUTSTANDING_PER_PG,
};
use crate::node_client::{
    BucketMetadataNodeClient, BucketWriteReservationNodeClient, DirectPutMetadataNodeClient,
    LocalUnixStorageNodeClientAdmissionSettings, MetadataCommandInspectionNodeClient,
    MetadataCommandNodeClient, MetadataCommandPeeringNodeClient, MetadataCommandRecoveryNodeClient,
    MetadataReadAuthorization, ObjectGenerationMetadataNodeClient, ObjectListingMetadataNodeClient,
    ObjectMutationMetadataNodeClient, ObjectPayloadLeaseNodeClient, ObjectPayloadLeaseNodeLease,
    ObjectReadMetadataNodeClient, ObjectVersionMetadataNodeClient, PlacedShardNodeClient,
    PlacedShardRoute, RetainedBucketWriteReservationNodeClient, RetainedMetadataCommandNodeClient,
    RetainedObjectMutationMetadataNodeClient, RetainedObjectPayloadReclaimNodeClient,
    RetainedObjectPayloadReclaimRoute, RetainedPlacedShardNodeClient, RetainedPlacedShardRoute,
    RetainedShardAckNodeClient, ShardAckNodeClient, ShardReadHandleNodeClient,
    ShardScavengerNodeClient, ShardScavengerObservationNodeClient, UnixStorageNodeClient,
    UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_LIMIT,
    UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
    UNIX_STORAGE_NODE_MIN_RPC_ADMISSION_LIMIT,
};
use crate::pg_store::MetadataCommandStartupDisposition;
#[cfg(test)]
use crate::pg_store::PgClusterMapHistoryReferenceSummary;
use crate::pg_topology::PgTopology;
use crate::storage_rpc_transport::{StorageRpcClientEndpoint, StorageRpcEndpointAuthorityIdentity};
use crate::types::{AdmittedRouteEffectFence, PlacedSegmentShardRepairWorkItem};
use crate::{
    BucketDeleteFinalizeRoot, BucketName, BucketPgId, ClusterEpoch, DataPgId, EcShape,
    GenerationId, MetadataError, ObjectKey, ObjectMetadataPgId, ObjectMetadataScanPgId, PgId,
    PgState, ReclaimWorkItem, RouteMapValidity, ShardIndex, ShardKey, WriteAck, WrittenShardAck,
};

const PAYLOAD_SHARD_PLACEMENT_KEY_DOMAIN: &[u8] = b"argmin/payload-shard-placement/v1";
const STATIC_ROUTE_MAP_CONTENT_DIGEST_DOMAIN: &[u8] = b"argmin/static-route-map-content/v3";
const LOCAL_RECLAIM_WORKER_WAIT_POLL_MILLIS: u64 = 100;
const LOCAL_PLACED_SEGMENT_SHARD_REPAIR_WORKER_WAIT_POLL_MILLIS: u64 = 100;
const METADATA_COMMAND_RECOVERY_WAIT_TIMEOUT: Duration = Duration::from_secs(1);
const LOCAL_PLACED_SEGMENT_SHARD_REPAIR_HINT_QUEUE_LIMIT: usize = 4096;

#[cfg(test)]
type OpenMetadataCommandAfterApplyTestHook = Arc<
    dyn Fn(
            &BTreeMap<NodeId, LocalNodeStore>,
            &BTreeMap<PgId, LocalPgRoute>,
            NodeId,
            &MetadataCommandEnvelope,
        ) + Send
        + Sync,
>;

#[cfg(test)]
static OPEN_METADATA_COMMAND_AFTER_APPLY_TEST_HOOK: std::sync::OnceLock<
    Mutex<Option<OpenMetadataCommandAfterApplyTestHook>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
pub(super) struct OpenMetadataCommandAfterApplyTestHookGuard;

#[cfg(test)]
impl Drop for OpenMetadataCommandAfterApplyTestHookGuard {
    fn drop(&mut self) {
        *OPEN_METADATA_COMMAND_AFTER_APPLY_TEST_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
    }
}

#[cfg(test)]
pub(super) fn test_install_open_metadata_command_after_apply_hook(
    hook: OpenMetadataCommandAfterApplyTestHook,
) -> OpenMetadataCommandAfterApplyTestHookGuard {
    let mut slot = OPEN_METADATA_COMMAND_AFTER_APPLY_TEST_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    assert!(
        slot.is_none(),
        "open metadata-command apply hook already installed"
    );
    *slot = Some(hook);
    OpenMetadataCommandAfterApplyTestHookGuard
}

#[cfg(test)]
fn maybe_run_open_metadata_command_after_apply_hook(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_routes: &BTreeMap<PgId, LocalPgRoute>,
    node_id: NodeId,
    command: &MetadataCommandEnvelope,
) {
    let hook = OPEN_METADATA_COMMAND_AFTER_APPLY_TEST_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    if let Some(hook) = hook {
        hook(nodes, pg_routes, node_id, command);
    }
}

fn object_payload_lease_node_is_unavailable(error: &StoreError) -> bool {
    if matches!(error, StoreError::StorageRpcResourceExhausted { .. }) {
        return true;
    }
    if error.storage_node_failure_class()
        == Some(crate::error::StorageNodeFailureClass::TransportInterrupted)
    {
        return true;
    }
    matches!(
        error,
        StoreError::Io {
            context: "connect storage-node object-payload lease RPC endpoint",
            source,
        } if matches!(
            source.kind(),
            std::io::ErrorKind::NotFound
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::UnexpectedEof
        )
    )
}

fn static_route_digest_len(hasher: &mut ChecksumHasher, len: usize) {
    static_route_digest_u64(
        hasher,
        u64::try_from(len).expect("static route-map collection length must fit u64"),
    );
}

fn static_route_digest_bytes(hasher: &mut ChecksumHasher, bytes: &[u8]) {
    static_route_digest_len(hasher, bytes.len());
    hasher.update(bytes);
}

fn static_route_digest_u64(hasher: &mut ChecksumHasher, value: u64) {
    hasher.update(&value.to_be_bytes());
}

fn static_route_digest_u32(hasher: &mut ChecksumHasher, value: u32) {
    hasher.update(&value.to_be_bytes());
}

fn static_route_digest_u8(hasher: &mut ChecksumHasher, value: u8) {
    hasher.update(&[value]);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LocalRouteMapLeaseSnapshot {
    pub(crate) validity: RouteMapValidity,
    pub(crate) local_valid_until_monotonic_ms: Option<u64>,
}

impl LocalRouteMapLeaseSnapshot {
    fn unbound(validity: RouteMapValidity) -> Self {
        Self {
            validity,
            local_valid_until_monotonic_ms: if validity == RouteMapValidity::Forever {
                None
            } else {
                Some(0)
            },
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn test_process_local_route_map_deadline(validity: RouteMapValidity) -> Option<u64> {
    let valid_until_ms = validity.valid_until_ms()?;
    let remaining_ms = valid_until_ms.saturating_sub(crate::clock::current_time_millis());
    Some(crate::clock::monotonic_time_millis().saturating_add(remaining_ms))
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalUnixShardNodeClientConfig {
    node_id: NodeId,
    socket_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LocalUnixStorageNodeClientConfig {
    node_id: NodeId,
    endpoint: StorageRpcClientEndpoint,
    rpc_admission_limit: usize,
    rpc_admission_wait_timeout: Duration,
    rpc_control_admission_wait_timeout: Duration,
    rpc_auth: Option<Arc<crate::StorageRpcClientAuthConfig>>,
}

impl LocalUnixShardNodeClientConfig {
    pub fn new(node_id: NodeId, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl LocalUnixStorageNodeClientConfig {
    pub const DEFAULT_RPC_ADMISSION_LIMIT: usize = UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_LIMIT;
    pub const MIN_RPC_ADMISSION_LIMIT: usize = UNIX_STORAGE_NODE_MIN_RPC_ADMISSION_LIMIT;
    pub const DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT: Duration =
        UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT;
    pub const DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT: Duration =
        crate::node_client::UNIX_STORAGE_NODE_DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT;

    pub fn new(node_id: NodeId, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            endpoint: StorageRpcClientEndpoint::unix(socket_path),
            rpc_admission_limit: Self::DEFAULT_RPC_ADMISSION_LIMIT,
            rpc_admission_wait_timeout: Self::DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
            rpc_control_admission_wait_timeout: Self::DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
            rpc_auth: None,
        }
    }

    pub fn from_runtime_node_route(node: &NodeRouteSnapshot) -> Self {
        Self::new(node.node_id(), node.endpoint())
    }

    pub fn with_rpc_admission_limit(
        node_id: NodeId,
        socket_path: impl Into<PathBuf>,
        rpc_admission_limit: usize,
    ) -> Self {
        Self {
            node_id,
            endpoint: StorageRpcClientEndpoint::unix(socket_path),
            rpc_admission_limit,
            rpc_admission_wait_timeout: Self::DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
            rpc_control_admission_wait_timeout: Self::DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
            rpc_auth: None,
        }
    }

    pub fn with_rpc_endpoint_and_admission_settings(
        node_id: NodeId,
        endpoint: StorageRpcClientEndpoint,
        settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Self {
        Self {
            node_id,
            endpoint,
            rpc_admission_limit: settings.rpc_admission_limit(),
            rpc_admission_wait_timeout: settings.rpc_admission_wait_timeout(),
            rpc_control_admission_wait_timeout: settings.rpc_control_admission_wait_timeout(),
            rpc_auth: None,
        }
    }

    pub fn with_rpc_admission_settings(
        node_id: NodeId,
        socket_path: impl Into<PathBuf>,
        settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Self {
        Self {
            node_id,
            endpoint: StorageRpcClientEndpoint::unix(socket_path),
            rpc_admission_limit: settings.rpc_admission_limit(),
            rpc_admission_wait_timeout: settings.rpc_admission_wait_timeout(),
            rpc_control_admission_wait_timeout: settings.rpc_control_admission_wait_timeout(),
            rpc_auth: None,
        }
    }

    #[must_use]
    pub(crate) fn with_rpc_auth(mut self, rpc_auth: crate::StorageRpcClientAuthConfig) -> Self {
        self.rpc_auth = Some(Arc::new(rpc_auth));
        self
    }

    pub(crate) fn with_optional_rpc_auth(
        mut self,
        rpc_auth: Option<crate::StorageRpcClientAuthConfig>,
    ) -> Self {
        self.rpc_auth = rpc_auth.map(Arc::new);
        self
    }

    #[must_use]
    pub fn with_frontend_rpc_auth(
        self,
        capability: crate::FrontendStorageRpcClientCapability,
    ) -> Self {
        self.with_rpc_auth(capability.into())
    }

    #[must_use]
    pub fn with_optional_frontend_rpc_auth(
        self,
        capability: Option<crate::FrontendStorageRpcClientCapability>,
    ) -> Self {
        self.with_optional_rpc_auth(capability.map(Into::into))
    }

    #[must_use]
    pub fn with_maintenance_rpc_auth(
        self,
        capability: crate::MaintenanceStorageRpcClientCapability,
    ) -> Self {
        self.with_rpc_auth(capability.into())
    }

    #[must_use]
    pub fn with_storage_node_rpc_auth(
        self,
        capability: crate::StorageNodeStorageRpcClientCapability,
    ) -> Self {
        self.with_rpc_auth(capability.into())
    }

    pub fn with_rpc_admission_settings_from_runtime_node_route(
        node: &NodeRouteSnapshot,
        settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Self {
        Self::with_rpc_admission_settings(node.node_id(), node.endpoint(), settings)
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn socket_path(&self) -> Option<&Path> {
        self.endpoint.unix_socket_path()
    }

    pub fn rpc_admission_limit(&self) -> usize {
        self.rpc_admission_limit
    }

    pub fn rpc_admission_wait_timeout(&self) -> Duration {
        self.rpc_admission_wait_timeout
    }

    pub fn rpc_control_admission_wait_timeout(&self) -> Duration {
        self.rpc_control_admission_wait_timeout
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalUnixMetadataCommandNodeClientConfig {
    node_id: NodeId,
    socket_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LocalUnixBucketMetadataNodeClientConfig {
    node_id: NodeId,
    socket_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LocalUnixBucketWriteReservationNodeClientConfig {
    node_id: NodeId,
    socket_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LocalUnixObjectGenerationMetadataNodeClientConfig {
    node_id: NodeId,
    socket_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LocalUnixObjectVersionMetadataNodeClientConfig {
    node_id: NodeId,
    socket_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LocalUnixDirectPutMetadataNodeClientConfig {
    node_id: NodeId,
    socket_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LocalUnixObjectMutationMetadataNodeClientConfig {
    node_id: NodeId,
    socket_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LocalUnixObjectReadMetadataNodeClientConfig {
    node_id: NodeId,
    socket_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct LocalUnixObjectListingMetadataNodeClientConfig {
    node_id: NodeId,
    socket_path: PathBuf,
}

impl LocalUnixMetadataCommandNodeClientConfig {
    pub fn new(node_id: NodeId, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl LocalUnixBucketMetadataNodeClientConfig {
    pub fn new(node_id: NodeId, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl LocalUnixBucketWriteReservationNodeClientConfig {
    pub fn new(node_id: NodeId, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl LocalUnixObjectGenerationMetadataNodeClientConfig {
    pub fn new(node_id: NodeId, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl LocalUnixObjectVersionMetadataNodeClientConfig {
    pub fn new(node_id: NodeId, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl LocalUnixDirectPutMetadataNodeClientConfig {
    pub fn new(node_id: NodeId, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl LocalUnixObjectMutationMetadataNodeClientConfig {
    pub fn new(node_id: NodeId, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl LocalUnixObjectReadMetadataNodeClientConfig {
    pub fn new(node_id: NodeId, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl LocalUnixObjectListingMetadataNodeClientConfig {
    pub fn new(node_id: NodeId, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LocalRouteExecutionEndpoint {
    Embedded(PathBuf),
    TopologyOnly,
    RpcUnix(PathBuf),
    RpcTcp(String),
}

impl LocalRouteExecutionEndpoint {
    fn for_local_store(data_dir: &Path) -> Self {
        if data_dir.as_os_str().is_empty() {
            Self::TopologyOnly
        } else {
            Self::Embedded(data_dir.to_path_buf())
        }
    }

    fn for_rpc(endpoint: &StorageRpcClientEndpoint) -> Self {
        match endpoint.authority_identity() {
            StorageRpcEndpointAuthorityIdentity::Unix(path) => Self::RpcUnix(path.to_path_buf()),
            StorageRpcEndpointAuthorityIdentity::Tcp(endpoint) => Self::RpcTcp(endpoint.to_owned()),
        }
    }

    fn matches_advertised_endpoint(&self, advertised_endpoint: &str) -> bool {
        match self {
            Self::RpcUnix(path) => path.as_os_str().as_bytes() == advertised_endpoint.as_bytes(),
            Self::RpcTcp(endpoint) => endpoint == advertised_endpoint,
            Self::Embedded(_) | Self::TopologyOnly => false,
        }
    }
}

#[derive(Clone)]
pub struct LocalNodeStore {
    node_id: NodeId,
    data_dir: PathBuf,
    route_execution_endpoint: LocalRouteExecutionEndpoint,
    route_authority_advertised_endpoint: Option<String>,
    runtime: LocalNodeRuntime,
    object_payload_lease_client: Arc<dyn ObjectPayloadLeaseNodeClient>,
    retained_object_payload_reclaim_client: Arc<dyn RetainedObjectPayloadReclaimNodeClient>,
    bucket_metadata_client: Arc<dyn BucketMetadataNodeClient>,
    bucket_metadata_unix_socket_path: Option<PathBuf>,
    bucket_write_reservation_client: Arc<dyn BucketWriteReservationNodeClient>,
    retained_bucket_write_reservation_client: Arc<dyn RetainedBucketWriteReservationNodeClient>,
    bucket_write_reservation_unix_socket_path: Option<PathBuf>,
    object_generation_metadata_client: Arc<dyn ObjectGenerationMetadataNodeClient>,
    object_version_metadata_client: Arc<dyn ObjectVersionMetadataNodeClient>,
    direct_put_metadata_client: Arc<dyn DirectPutMetadataNodeClient>,
    object_listing_metadata_client: Arc<dyn ObjectListingMetadataNodeClient>,
    object_mutation_metadata_client: Arc<dyn ObjectMutationMetadataNodeClient>,
    retained_object_mutation_metadata_client: Arc<dyn RetainedObjectMutationMetadataNodeClient>,
    object_read_metadata_client: Arc<dyn ObjectReadMetadataNodeClient>,
    metadata_command_client: Arc<dyn MetadataCommandNodeClient>,
    metadata_command_inspection_client: Arc<dyn MetadataCommandInspectionNodeClient>,
    metadata_command_peering_client: Arc<dyn MetadataCommandPeeringNodeClient>,
    metadata_command_recovery_client: Arc<dyn MetadataCommandRecoveryNodeClient>,
    retained_metadata_command_client: Arc<dyn RetainedMetadataCommandNodeClient>,
    shard_client: Arc<dyn PlacedShardNodeClient>,
    retained_shard_client: Arc<dyn RetainedPlacedShardNodeClient>,
    shard_ack_client: Arc<dyn ShardAckNodeClient>,
    retained_shard_ack_client: Arc<dyn RetainedShardAckNodeClient>,
    shard_read_handle_client: Arc<dyn ShardReadHandleNodeClient>,
    shard_scavenger_client: Arc<dyn ShardScavengerNodeClient>,
    shard_scavenger_observation_client: Arc<dyn ShardScavengerObservationNodeClient>,
}

pub(crate) struct MetadataPgReadNode<'a> {
    node: &'a LocalNodeStore,
    authorization: MetadataReadAuthorization,
}

impl<'a> MetadataPgReadNode<'a> {
    pub(crate) fn node_id(&self) -> NodeId {
        self.node.node_id()
    }

    pub(crate) fn authorization(&self) -> MetadataReadAuthorization {
        self.authorization
    }

    pub(crate) fn bucket_metadata_client(&self) -> &'a Arc<dyn BucketMetadataNodeClient> {
        self.node.bucket_metadata_client()
    }

    pub(crate) fn object_read_metadata_client(&self) -> &'a Arc<dyn ObjectReadMetadataNodeClient> {
        self.node.object_read_metadata_client()
    }

    pub(crate) fn object_listing_metadata_client(
        &self,
    ) -> &'a Arc<dyn ObjectListingMetadataNodeClient> {
        self.node.object_listing_metadata_client()
    }
}

impl LocalNodeStore {
    fn new(node_id: NodeId, data_dir: PathBuf, runtime: LocalNodeRuntime) -> Self {
        let clients = runtime.clients();
        let route_execution_endpoint = LocalRouteExecutionEndpoint::for_local_store(&data_dir);
        Self {
            node_id,
            data_dir,
            route_execution_endpoint,
            route_authority_advertised_endpoint: None,
            runtime,
            object_payload_lease_client: clients.object_payload_lease,
            retained_object_payload_reclaim_client: clients.retained_object_payload_reclaim,
            bucket_metadata_client: clients.bucket_metadata,
            bucket_metadata_unix_socket_path: None,
            bucket_write_reservation_client: clients.bucket_write_reservation,
            retained_bucket_write_reservation_client: clients.retained_bucket_write_reservation,
            bucket_write_reservation_unix_socket_path: None,
            object_generation_metadata_client: clients.object_generation_metadata,
            object_version_metadata_client: clients.object_version_metadata,
            direct_put_metadata_client: clients.direct_put_metadata,
            object_listing_metadata_client: clients.object_listing_metadata,
            object_mutation_metadata_client: clients.object_mutation_metadata,
            retained_object_mutation_metadata_client: clients.retained_object_mutation_metadata,
            object_read_metadata_client: clients.object_read_metadata,
            metadata_command_client: clients.metadata_command,
            metadata_command_inspection_client: clients.metadata_command_inspection,
            metadata_command_peering_client: clients.metadata_command_peering,
            metadata_command_recovery_client: clients.metadata_command_recovery,
            retained_metadata_command_client: clients.retained_metadata_command,
            shard_client: clients.shard,
            retained_shard_client: clients.retained_shard,
            shard_ack_client: clients.shard_ack,
            retained_shard_ack_client: clients.retained_shard_ack,
            shard_read_handle_client: clients.shard_read_handle,
            shard_scavenger_client: clients.shard_scavenger,
            shard_scavenger_observation_client: clients.shard_scavenger_observation,
        }
    }

    fn topology_only(
        node_id: NodeId,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
    ) -> Result<Self, StoreError> {
        let runtime = LocalNodeRuntime::topology_only(node_id, pg_ids, default_ec_shape)?;
        Ok(Self::new(node_id, PathBuf::new(), runtime))
    }

    pub(crate) fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    fn runtime(&self) -> &LocalNodeRuntime {
        &self.runtime
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_node(&self) -> &Arc<SharedStorageNode> {
        self.runtime.test_node()
    }

    #[cfg(test)]
    fn storage_node(&self) -> &Arc<SharedStorageNode> {
        self.test_node()
    }

    pub(crate) fn object_payload_lease_client(&self) -> &Arc<dyn ObjectPayloadLeaseNodeClient> {
        &self.object_payload_lease_client
    }

    pub(crate) fn retained_object_payload_reclaim_client(
        &self,
    ) -> &Arc<dyn RetainedObjectPayloadReclaimNodeClient> {
        &self.retained_object_payload_reclaim_client
    }

    pub(crate) fn bucket_metadata_client(&self) -> &Arc<dyn BucketMetadataNodeClient> {
        &self.bucket_metadata_client
    }

    pub(crate) fn bucket_write_reservation_client(
        &self,
    ) -> &Arc<dyn BucketWriteReservationNodeClient> {
        &self.bucket_write_reservation_client
    }

    pub(crate) fn retained_bucket_write_reservation_client(
        &self,
    ) -> &Arc<dyn RetainedBucketWriteReservationNodeClient> {
        &self.retained_bucket_write_reservation_client
    }

    pub(crate) fn object_generation_metadata_client(
        &self,
    ) -> &Arc<dyn ObjectGenerationMetadataNodeClient> {
        &self.object_generation_metadata_client
    }

    pub(crate) fn object_version_metadata_client(
        &self,
    ) -> &Arc<dyn ObjectVersionMetadataNodeClient> {
        &self.object_version_metadata_client
    }

    pub(crate) fn direct_put_metadata_client(&self) -> &Arc<dyn DirectPutMetadataNodeClient> {
        &self.direct_put_metadata_client
    }

    pub(crate) fn object_listing_metadata_client(
        &self,
    ) -> &Arc<dyn ObjectListingMetadataNodeClient> {
        &self.object_listing_metadata_client
    }

    pub(crate) fn object_mutation_metadata_client(
        &self,
    ) -> &Arc<dyn ObjectMutationMetadataNodeClient> {
        &self.object_mutation_metadata_client
    }

    pub(crate) fn retained_object_mutation_metadata_client(
        &self,
    ) -> &Arc<dyn RetainedObjectMutationMetadataNodeClient> {
        &self.retained_object_mutation_metadata_client
    }

    pub(crate) fn object_read_metadata_client(&self) -> &Arc<dyn ObjectReadMetadataNodeClient> {
        &self.object_read_metadata_client
    }

    pub(crate) fn metadata_command_client(&self) -> &Arc<dyn MetadataCommandNodeClient> {
        &self.metadata_command_client
    }

    pub(crate) fn metadata_command_inspection_client(
        &self,
    ) -> &Arc<dyn MetadataCommandInspectionNodeClient> {
        &self.metadata_command_inspection_client
    }

    pub(crate) fn metadata_command_peering_client(
        &self,
    ) -> &Arc<dyn MetadataCommandPeeringNodeClient> {
        &self.metadata_command_peering_client
    }

    pub(crate) fn metadata_command_recovery_client(
        &self,
    ) -> &Arc<dyn MetadataCommandRecoveryNodeClient> {
        &self.metadata_command_recovery_client
    }

    pub(crate) fn retained_metadata_command_client(
        &self,
    ) -> &Arc<dyn RetainedMetadataCommandNodeClient> {
        &self.retained_metadata_command_client
    }

    pub(crate) fn shard_client(&self) -> &Arc<dyn PlacedShardNodeClient> {
        &self.shard_client
    }

    pub(crate) fn retained_shard_client(&self) -> &Arc<dyn RetainedPlacedShardNodeClient> {
        &self.retained_shard_client
    }

    pub(crate) fn shard_ack_client(&self) -> &Arc<dyn ShardAckNodeClient> {
        &self.shard_ack_client
    }

    pub(crate) fn retained_shard_ack_client(&self) -> &Arc<dyn RetainedShardAckNodeClient> {
        &self.retained_shard_ack_client
    }

    pub(crate) fn shard_read_handle_client(&self) -> &Arc<dyn ShardReadHandleNodeClient> {
        &self.shard_read_handle_client
    }

    pub(crate) fn shard_scavenger_client(&self) -> &Arc<dyn ShardScavengerNodeClient> {
        &self.shard_scavenger_client
    }

    pub(crate) fn shard_scavenger_observation_client(
        &self,
    ) -> &Arc<dyn ShardScavengerObservationNodeClient> {
        &self.shard_scavenger_observation_client
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
    node_id: NodeId,
    route: Box<dyn PlacedShardRoute + 'a>,
    read_handle_client: &'a dyn ShardReadHandleNodeClient,
    location: ShardLocation,
    key: ShardKey,
}

impl LocalShardNodeClient<'_> {
    fn write_shard(&self, data: &[u8]) -> Result<WriteAck, ShardIoError> {
        self.route
            .write_placed_shard(data)
            .map_err(|source| self.store_error(source))
    }

    fn write_shard_with_effect_fence(
        &self,
        data: &[u8],
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<WriteAck, ShardIoError> {
        self.route
            .write_placed_shard_with_effect_fence(data, effect_fence)
            .map_err(|source| self.store_error(source))
    }

    fn repair_shard(&self, data: &[u8]) -> Result<WriteAck, ShardIoError> {
        self.route
            .repair_placed_shard(data)
            .map_err(|source| self.store_error(source))
    }

    fn read_shard(&self, expected: WriteAck) -> Result<Vec<u8>, ShardIoError> {
        let mut read_handle = self.acquire_read_handle()?;
        let mut data = vec![
            0;
            usize::try_from(expected.stored_size).map_err(|_| {
                self.store_error(StoreError::PayloadShardSetMismatch {
                    reason: "payload shard size exceeds addressable memory".to_string(),
                })
            })?
        ];
        let data_result = read_handle
            .read_placed_shard_into(self.location, &self.key, expected, &mut data)
            .map_err(|source| self.store_error(source));
        if let Err(error) = read_handle.release() {
            return Err(self.store_error(error));
        }
        data_result?;
        Ok(data)
    }

    #[cfg(test)]
    fn read_shard_into(&self, expected: WriteAck, dst: &mut [u8]) -> Result<(), ShardIoError> {
        if dst.len() as u64 != expected.stored_size {
            return Err(self.store_error(StoreError::Io {
                context: "read payload shard buffer size mismatch",
                source: std::io::Error::from(std::io::ErrorKind::InvalidData),
            }));
        }
        let mut read_handle = self.acquire_read_handle()?;
        let read_result = read_handle
            .read_placed_shard_into(self.location, &self.key, expected, dst)
            .map_err(|source| self.store_error(source));
        if let Err(error) = read_handle.release() {
            return Err(self.store_error(error));
        }
        read_result
    }

    fn delete_shard(&self) -> Result<(), ShardIoError> {
        self.route
            .delete_placed_shard()
            .map_err(|source| self.store_error(source))
    }

    fn acquire_read_handle(
        &self,
    ) -> Result<Box<dyn crate::node_client::ShardReadHandleLease>, ShardIoError> {
        self.read_handle_client
            .open_shard_read_handle_route(
                self.location.cluster_epoch(),
                &self.read_operation_id(&self.key),
                vec![(self.location, self.key.clone())],
            )
            .and_then(|route| route.acquire())
            .map_err(|source| self.store_error(source))
    }

    fn read_operation_id(&self, key: &ShardKey) -> String {
        let hex_key = key.hex_bytes();
        let hex_key = std::str::from_utf8(&hex_key).expect("shard key hex is valid ASCII");
        format!(
            "read:{}:{}:{}:{}",
            self.location.cluster_epoch().get(),
            self.location.data_pg_id().get(),
            self.node_id.as_u32(),
            hex_key
        )
    }

    fn read_operation_id_for_keys(&self, keys: &[ShardKey]) -> String {
        let mut id = format!(
            "read-batch:{}:{}:{}",
            self.location.cluster_epoch().get(),
            self.location.data_pg_id().get(),
            self.node_id.as_u32()
        );
        for key in keys {
            let hex_key = key.hex_bytes();
            let hex_key = std::str::from_utf8(&hex_key).expect("shard key hex is valid ASCII");
            id.push(':');
            id.push_str(hex_key);
        }
        id
    }

    fn store_error(&self, source: StoreError) -> ShardIoError {
        ShardIoError::Store {
            node_id: self.node_id.as_u32(),
            pg_id: self.location.data_pg_id().get(),
            cluster_epoch: self.location.cluster_epoch(),
            source,
        }
    }
}

pub(crate) struct LocalShardReadHandleSet {
    leases: Vec<(
        ShardLocation,
        Box<dyn crate::node_client::ShardReadHandleLease>,
    )>,
    released: bool,
}

impl LocalShardReadHandleSet {
    fn new() -> Self {
        Self {
            leases: Vec::new(),
            released: false,
        }
    }

    fn push(
        &mut self,
        location: ShardLocation,
        lease: Box<dyn crate::node_client::ShardReadHandleLease>,
    ) {
        self.leases.push((location, lease));
    }

    pub(crate) fn release(&mut self) -> Result<(), ShardIoError> {
        if self.released {
            return Ok(());
        }
        let mut first_error = None;
        for (location, lease) in &mut self.leases {
            if let Err(source) = lease.release() {
                first_error.get_or_insert_with(|| ShardIoError::Store {
                    node_id: location.node_id().as_u32(),
                    pg_id: location.data_pg_id().get(),
                    cluster_epoch: location.cluster_epoch(),
                    source,
                });
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        self.released = true;
        Ok(())
    }

    fn read_placed_shard_into(
        &mut self,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), ShardIoError> {
        let lease = self
            .leases
            .iter_mut()
            .find(|(leased_location, _)| leased_location.node_id() == location.node_id())
            .map(|(_, lease)| lease)
            .ok_or_else(|| ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source: StoreError::RouteCapabilitySubjectMismatch {
                    operation: "read shard without a retained node read-handle session",
                },
            })?;
        lease
            .read_placed_shard_into(location, key, expected, dst)
            .map_err(|source| ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source,
            })
    }
}

impl Drop for LocalShardReadHandleSet {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

struct LocalPlacedSegmentShardSubject {
    operation_epoch: ClusterEpoch,
    data_pg_id: DataPgId,
    segment_okh: [u8; 16],
    segment_vid: GenerationId,
    locations: Vec<ShardLocation>,
}

impl LocalPlacedSegmentShardSubject {
    fn locations(&self) -> &[ShardLocation] {
        &self.locations
    }

    fn location(&self, shard_index: usize) -> Option<ShardLocation> {
        self.locations.get(shard_index).copied()
    }

    fn shard_key(&self, shard_index: usize) -> Option<ShardKey> {
        let location = self.location(shard_index)?;
        Some(ShardKey::new(
            &self.segment_okh,
            self.segment_vid.get(),
            location.shard_index().get(),
        ))
    }

    fn location_and_key(
        &self,
        shard_index: usize,
    ) -> Result<(ShardLocation, ShardKey), ShardIoError> {
        let Some(location) = self.location(shard_index) else {
            return Err(self.shard_subject_mismatch(format!(
                "shard index {shard_index} is outside the placed segment's {} locations",
                self.locations.len()
            )));
        };
        let key = ShardKey::new(
            &self.segment_okh,
            self.segment_vid.get(),
            location.shard_index().get(),
        );
        Ok((location, key))
    }

    fn shard_subject_mismatch(&self, reason: String) -> ShardIoError {
        ShardIoError::Store {
            node_id: 0,
            pg_id: self.data_pg_id.get(),
            cluster_epoch: self.operation_epoch,
            source: StoreError::PayloadShardSetMismatch { reason },
        }
    }
}

/// Storage-owned current-route reader for one exact placed payload segment.
///
/// Construction derives every location and shard key from the segment
/// request. Parent cluster code can inspect or read only by shard index and
/// cannot pair an arbitrary location with an unrelated key.
pub(super) struct LocalPlacedSegmentShardReader<'a> {
    cluster_map: &'a LocalClusterMap,
    subject: LocalPlacedSegmentShardSubject,
}

impl<'a> LocalPlacedSegmentShardReader<'a> {
    pub(super) fn locations(&self) -> &[ShardLocation] {
        self.subject.locations()
    }

    pub(super) fn location(&self, shard_index: usize) -> Option<ShardLocation> {
        self.subject.location(shard_index)
    }

    pub(super) fn shard_key(&self, shard_index: usize) -> Option<ShardKey> {
        self.subject.shard_key(shard_index)
    }

    pub(super) fn read(
        &self,
        shard_index: usize,
        expected: WriteAck,
    ) -> Result<Vec<u8>, ShardIoError> {
        let (location, key) = self.subject.location_and_key(shard_index)?;
        self.cluster_map
            .read_payload_shard(self.subject.operation_epoch, location, &key, expected)
    }

    pub(super) fn acquire_read_handles(
        self,
        shard_indices: impl IntoIterator<Item = usize>,
    ) -> Result<LocalPlacedSegmentShardReadHandles<'a>, ShardIoError> {
        let mut leased_shard_indices = BTreeSet::new();
        let mut entries = Vec::new();
        for shard_index in shard_indices {
            if !leased_shard_indices.insert(shard_index) {
                return Err(self.subject.shard_subject_mismatch(format!(
                    "shard index {shard_index} was requested more than once"
                )));
            }
            entries.push(self.subject.location_and_key(shard_index)?);
        }
        let read_handles = self
            .cluster_map
            .acquire_payload_shard_read_handles(self.subject.operation_epoch, &entries)?;
        Ok(LocalPlacedSegmentShardReadHandles {
            reader: self,
            read_handles,
            leased_shard_indices,
        })
    }
}

/// Storage-owned deleter for one exact placed payload segment.
///
/// The caller may present a subset of shard keys from staged-write ownership
/// or a durable reclaim record, but cannot pair those keys with arbitrary
/// locations. Current-route construction derives placement from the installed
/// route; retained-route construction derives it from the exact reconstructed
/// placement snapshot. Both require every key to match the segment identity
/// before reaching the corresponding storage-node delete fence.
pub(super) struct LocalPlacedSegmentShardDeleter<'a> {
    cluster_map: &'a LocalClusterMap,
    subject: LocalPlacedSegmentShardSubject,
    route_kind: LocalPlacedSegmentShardDeleteRouteKind,
}

impl LocalPlacedSegmentShardDeleter<'_> {
    pub(super) fn delete_matching_key(&self, key: &ShardKey) -> Result<(), ShardIoError> {
        let shard_index = usize::from(key.shard_index().get());
        let (location, expected_key) = self.subject.location_and_key(shard_index)?;
        if *key != expected_key {
            return Err(self.subject.shard_subject_mismatch(format!(
                "shard key does not match placed segment identity at shard index {shard_index}"
            )));
        }
        match self.route_kind {
            LocalPlacedSegmentShardDeleteRouteKind::Current => {
                self.cluster_map.delete_payload_shard_for_current_route(
                    self.subject.operation_epoch,
                    location,
                    &expected_key,
                )
            }
            LocalPlacedSegmentShardDeleteRouteKind::Retained => self
                .cluster_map
                .delete_payload_shard_for_historical_cleanup(location, &expected_key),
        }
    }
}

enum LocalPlacedSegmentShardDeleteRouteKind {
    Current,
    Retained,
}

pub(super) struct LocalPlacedSegmentShardReadHandles<'a> {
    reader: LocalPlacedSegmentShardReader<'a>,
    read_handles: LocalShardReadHandleSet,
    leased_shard_indices: BTreeSet<usize>,
}

impl LocalPlacedSegmentShardReadHandles<'_> {
    pub(super) fn location(&self, shard_index: usize) -> Option<ShardLocation> {
        if !self.leased_shard_indices.contains(&shard_index) {
            return None;
        }
        self.reader.location(shard_index)
    }

    pub(super) fn shard_key(&self, shard_index: usize) -> Option<ShardKey> {
        if !self.leased_shard_indices.contains(&shard_index) {
            return None;
        }
        self.reader.shard_key(shard_index)
    }

    pub(super) fn read_into(
        &mut self,
        shard_index: usize,
        expected: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), ShardIoError> {
        if !self.leased_shard_indices.contains(&shard_index) {
            return Err(self.reader.subject.shard_subject_mismatch(format!(
                "shard index {shard_index} is not protected by this read-handle set"
            )));
        }
        let (location, key) = self.reader.subject.location_and_key(shard_index)?;
        self.read_handles
            .read_placed_shard_into(location, &key, expected, dst)
    }

    pub(super) fn release(&mut self) -> Result<(), ShardIoError> {
        self.read_handles.release()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPgRoute {
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    primary_node_id: NodeId,
    acting_set: Arc<[NodeId]>,
    state: PgState,
    metadata_read_route: Option<crate::control_plane::PgMetadataReadRoute>,
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
            metadata_read_route: None,
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

    pub(crate) fn metadata_read_route(&self) -> Option<crate::control_plane::PgMetadataReadRoute> {
        self.metadata_read_route
    }
}

impl From<&PgRouteSnapshot> for LocalPgRoute {
    fn from(route: &PgRouteSnapshot) -> Self {
        Self {
            cluster_epoch: route.cluster_epoch(),
            pg_id: route.pg_id(),
            primary_node_id: route.primary_node_id(),
            acting_set: Arc::from(route.acting_set()),
            state: route.state(),
            metadata_read_route: route.metadata_read_route(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct LocalClusterRuntimeState {
    reclaim_queue: (Mutex<LocalReclaimQueueState>, Condvar),
    active_object_payload_reclaims: Mutex<HashSet<LocalReclaimRoot>>,
    placed_segment_shard_repair_queue: (Mutex<LocalPlacedSegmentShardRepairQueueState>, Condvar),
    metadata_command_pg_locks: Mutex<HashMap<PgId, Arc<MetadataCommandPgLock>>>,
    metadata_command_recovery_flights:
        Arc<Mutex<HashMap<MetadataCommandRecoveryKey, Arc<MetadataCommandRecoveryFlight>>>>,
    #[cfg(test)]
    metadata_command_recovery_wait_hook: Mutex<Option<MetadataCommandRecoveryWaitTestHook>>,
}

/// FIFO serialization for metadata-command mutation and inspection on one PG.
///
/// Publication owners must not be starved by a stream of fresh pending-slot
/// installers; otherwise a durable command can hold the slot until every
/// waiting S3 request exhausts its work budget.
#[derive(Debug, Default)]
pub(crate) struct MetadataCommandPgLock {
    state: Mutex<MetadataCommandPgLockState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct MetadataCommandPgLockState {
    held: bool,
    next_ticket: u64,
    waiters: VecDeque<u64>,
}

pub(crate) struct MetadataCommandPgGuard<'a> {
    lock: &'a MetadataCommandPgLock,
}

impl MetadataCommandPgLock {
    fn enqueue(&self) -> (std::sync::MutexGuard<'_, MetadataCommandPgLockState>, u64) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let ticket = state.next_ticket;
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .expect("metadata command PG lock ticket space exhausted");
        state.waiters.push_back(ticket);
        (state, ticket)
    }

    pub(crate) fn lock(&self) -> MetadataCommandPgGuard<'_> {
        let (mut state, ticket) = self.enqueue();
        loop {
            if !state.held && state.waiters.front() == Some(&ticket) {
                state.waiters.pop_front();
                state.held = true;
                return MetadataCommandPgGuard { lock: self };
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    pub(crate) fn lock_until(&self, deadline: Instant) -> Option<MetadataCommandPgGuard<'_>> {
        if Instant::now() >= deadline {
            return None;
        }
        let (mut state, ticket) = self.enqueue();
        loop {
            if Instant::now() >= deadline {
                state.waiters.retain(|waiting| *waiting != ticket);
                self.changed.notify_all();
                return None;
            }
            if !state.held && state.waiters.front() == Some(&ticket) {
                state.waiters.pop_front();
                state.held = true;
                return Some(MetadataCommandPgGuard { lock: self });
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                continue;
            }
            let (next_state, _) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|error| error.into_inner());
            state = next_state;
        }
    }

    pub(crate) fn try_lock(&self) -> Option<MetadataCommandPgGuard<'_>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.held || !state.waiters.is_empty() {
            return None;
        }
        state.held = true;
        Some(MetadataCommandPgGuard { lock: self })
    }

    #[cfg(test)]
    fn waiting_for_test(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .waiters
            .len()
    }
}

impl Drop for MetadataCommandPgGuard<'_> {
    fn drop(&mut self) {
        let mut state = self
            .lock
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        debug_assert!(state.held);
        state.held = false;
        self.lock.changed.notify_all();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct MetadataCommandRecoveryKey {
    pg_id: PgId,
    log_index: u64,
    checksum_crc64: u64,
}

impl MetadataCommandRecoveryKey {
    fn new(pg_id: PgId, command: &MetadataCommandEnvelope) -> Self {
        Self {
            pg_id,
            log_index: command.id().log_index().get(),
            checksum_crc64: command.checksum_crc64(),
        }
    }
}

#[derive(Debug)]
struct MetadataCommandRecoveryFlight {
    state: Mutex<MetadataCommandRecoveryFlightState>,
    done: Condvar,
}

#[derive(Debug)]
struct MetadataCommandRecoveryFlightState {
    in_progress: bool,
    awaiting_authorized_recovery: bool,
    authorized_recovery_handoff_requested: bool,
    keys: HashSet<MetadataCommandRecoveryKey>,
    lineage_root: MetadataCommandEnvelope,
    lineage_tip: MetadataCommandEnvelope,
    root_disposition: MetadataCommandRecoveryRootDisposition,
    resolution: Option<MetadataCommandRecoveryResolution>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MetadataCommandRecoveryRootDisposition {
    TipOutcome,
    Abandoned {
        predecessor: Box<MetadataCommandEnvelope>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetadataCommandRecoveryResolution {
    Outcome(super::PendingMetadataCommandOutcome),
    OutcomeUnconfirmed,
    IrrevocableConvergencePending,
}

#[cfg(test)]
#[derive(Debug)]
struct MetadataCommandRecoveryWaitTestHook {
    key: MetadataCommandRecoveryKey,
    existing_flight_selections: usize,
    forced_timeouts_remaining: usize,
    wait_for_owner_selection: usize,
    timeout_selected: Arc<Barrier>,
    retry_selected: Arc<Barrier>,
}

#[cfg(test)]
enum MetadataCommandRecoveryWaitTestAction {
    ForceTimeout(Arc<Barrier>),
    WaitForOwner(Arc<Barrier>),
}

#[derive(Debug)]
pub(crate) enum MetadataCommandRecoveryAdmission {
    Leader(MetadataCommandRecoveryGuard),
    AwaitingAuthorizedRecovery {
        wait_us: u128,
        lineage_tip: MetadataCommandEnvelope,
        resolution: Option<MetadataCommandRecoveryResolution>,
    },
    Waited {
        wait_us: u128,
        lineage_tip: MetadataCommandEnvelope,
        resolution: Option<MetadataCommandRecoveryResolution>,
    },
    TimedOut {
        wait_us: u128,
        lineage_tip: MetadataCommandEnvelope,
        resolution: Option<MetadataCommandRecoveryResolution>,
    },
}

#[derive(Debug)]
pub(crate) struct MetadataCommandRecoveryGuard {
    root_key: MetadataCommandRecoveryKey,
    flight: Arc<MetadataCommandRecoveryFlight>,
    flights: Arc<Mutex<HashMap<MetadataCommandRecoveryKey, Arc<MetadataCommandRecoveryFlight>>>>,
    remove_flight_on_drop: bool,
}

impl MetadataCommandRecoveryGuard {
    pub(crate) fn matches_command(&self, pg_id: PgId, command: &MetadataCommandEnvelope) -> bool {
        self.root_key == MetadataCommandRecoveryKey::new(pg_id, command)
    }

    pub(crate) fn owns_lineage_command(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> bool {
        let key = MetadataCommandRecoveryKey::new(pg_id, command);
        let state = self
            .flight
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.in_progress && state.keys.contains(&key)
    }

    pub(crate) fn lineage_tip(&self) -> MetadataCommandEnvelope {
        self.flight
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .lineage_tip
            .clone()
    }

    pub(crate) fn lineage_root(&self) -> MetadataCommandEnvelope {
        self.flight
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .lineage_root
            .clone()
    }

    pub(crate) fn lineage_advanced_from(&self, command: &MetadataCommandEnvelope) -> bool {
        self.flight
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .lineage_tip
            != *command
    }

    pub(crate) fn root_disposition(&self) -> MetadataCommandRecoveryRootDisposition {
        self.flight
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .root_disposition
            .clone()
    }

    pub(crate) fn mark_irreversible_handoff(&self, resolution: MetadataCommandRecoveryResolution) {
        debug_assert!(matches!(
            resolution,
            MetadataCommandRecoveryResolution::OutcomeUnconfirmed
                | MetadataCommandRecoveryResolution::IrrevocableConvergencePending
        ));
        self.flight
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .resolution = Some(resolution);
    }

    pub(crate) fn record_outcome(&self, outcome: super::PendingMetadataCommandOutcome) {
        self.flight
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .resolution = Some(MetadataCommandRecoveryResolution::Outcome(outcome));
        self.flight.done.notify_all();
    }

    pub(crate) fn complete_with_outcome_and_relinquish_if_requested(
        mut self,
        outcome: super::PendingMetadataCommandOutcome,
    ) -> bool {
        let relinquished = {
            let mut flights = self
                .flights
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let mut state = self
                .flight
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.resolution = Some(MetadataCommandRecoveryResolution::Outcome(outcome));
            if outcome.retains_pending_slot() && state.authorized_recovery_handoff_requested {
                debug_assert!(state.in_progress);
                state.in_progress = false;
                state.awaiting_authorized_recovery = true;
                true
            } else {
                state.in_progress = false;
                state.awaiting_authorized_recovery = false;
                for key in &state.keys {
                    if flights
                        .get(key)
                        .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
                    {
                        flights.remove(key);
                    }
                }
                false
            }
        };
        self.flight.done.notify_all();
        self.remove_flight_on_drop = false;
        relinquished
    }

    pub(crate) fn relinquish_for_authorized_recovery(mut self) {
        {
            let mut state = self
                .flight
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            debug_assert!(state.in_progress);
            state.in_progress = false;
            state.awaiting_authorized_recovery = true;
        }
        self.flight.done.notify_all();
        self.remove_flight_on_drop = false;
    }

    pub(crate) fn bind_reissued_command(
        &self,
        pg_id: PgId,
        source: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
    ) -> Result<(), StoreError> {
        let source_key = MetadataCommandRecoveryKey::new(pg_id, source);
        let replacement_key = MetadataCommandRecoveryKey::new(pg_id, replacement);
        let source_id = source.id();
        let replacement_id = replacement.id();
        let valid_chain = source_id.pg_id() == pg_id
            && replacement_id.pg_id() == pg_id
            && replacement_id.cluster_epoch() == source_id.cluster_epoch()
            && replacement_id.log_index().get() > source_id.log_index().get()
            && replacement
                .payload()
                .is_authorized_recovery_derivative_of(source.payload());
        if !valid_chain {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "metadata-command-recovery-flight-reissue",
            });
        }

        let mut flights = self.flights.lock().unwrap_or_else(|e| e.into_inner());
        let mut state = self.flight.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.in_progress || !state.keys.contains(&source_key) {
            return Err(StoreError::MetadataCommandContention {
                context: "metadata command recovery flight source is no longer owned",
            });
        }
        let exact_derivative_rebind = matches!(
            &state.root_disposition,
            MetadataCommandRecoveryRootDisposition::Abandoned { predecessor }
                if **predecessor == *source && state.lineage_tip == *replacement
        );
        if replacement.payload() != source.payload()
            && state.root_disposition != MetadataCommandRecoveryRootDisposition::TipOutcome
            && !exact_derivative_rebind
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "metadata-command-recovery-flight-derivative-lineage",
            });
        }
        if let Some(existing) = flights.get(&replacement_key) {
            if !Arc::ptr_eq(existing, &self.flight) {
                return Err(StoreError::MetadataCommandContention {
                    context: "metadata command replacement already has a recovery owner",
                });
            }
        } else {
            flights.insert(replacement_key, Arc::clone(&self.flight));
        }
        state.keys.insert(replacement_key);
        if replacement.payload() != source.payload() && !exact_derivative_rebind {
            state.root_disposition = MetadataCommandRecoveryRootDisposition::Abandoned {
                predecessor: Box::new(source.clone()),
            };
        }
        state.lineage_tip = replacement.clone();
        Ok(())
    }

    pub(crate) fn rollback_reissued_command(
        &self,
        pg_id: PgId,
        source: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
    ) -> Result<(), StoreError> {
        let source_key = MetadataCommandRecoveryKey::new(pg_id, source);
        let replacement_key = MetadataCommandRecoveryKey::new(pg_id, replacement);
        let mut flights = self.flights.lock().unwrap_or_else(|e| e.into_inner());
        let mut state = self.flight.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.in_progress
            || !state.keys.contains(&source_key)
            || !state.keys.contains(&replacement_key)
            || state.lineage_tip != *replacement
        {
            return Err(StoreError::MetadataCommandContention {
                context: "metadata command recovery flight replacement is no longer rollbackable",
            });
        }
        if flights
            .get(&replacement_key)
            .is_none_or(|flight| !Arc::ptr_eq(flight, &self.flight))
        {
            return Err(StoreError::MetadataCommandContention {
                context: "metadata command replacement recovery ownership changed",
            });
        }
        flights.remove(&replacement_key);
        state.keys.remove(&replacement_key);
        if replacement.payload() != source.payload() {
            state.root_disposition = MetadataCommandRecoveryRootDisposition::TipOutcome;
        }
        state.lineage_tip = source.clone();
        Ok(())
    }
}

impl Drop for MetadataCommandRecoveryGuard {
    fn drop(&mut self) {
        if !self.remove_flight_on_drop {
            return;
        }
        let mut flights = self.flights.lock().unwrap_or_else(|e| e.into_inner());
        let mut state = self.flight.state.lock().unwrap_or_else(|e| e.into_inner());
        state.in_progress = false;
        state.awaiting_authorized_recovery = false;
        for key in &state.keys {
            if flights
                .get(key)
                .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
            {
                flights.remove(key);
            }
        }
        self.flight.done.notify_all();
    }
}

type LocalReclaimRoot = (BucketName, ObjectKey, GenerationId);
type LocalBucketDeleteBeginRoot = crate::BucketDeleteBeginRoot;

pub(crate) struct LocalObjectPayloadReclaimExecution<'a> {
    runtime: &'a LocalClusterRuntimeState,
    root: LocalReclaimRoot,
}

impl Drop for LocalObjectPayloadReclaimExecution<'_> {
    fn drop(&mut self) {
        let removed = self
            .runtime
            .active_object_payload_reclaims
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.root);
        debug_assert!(
            removed,
            "active object payload reclaim must remain registered"
        );
    }
}

#[derive(Debug)]
struct LocalReclaimQueueState {
    work_queue: VecDeque<ReclaimWorkItem>,
    queued_objects: HashSet<LocalReclaimRoot>,
    outstanding_objects: HashMap<LocalReclaimRoot, u32>,
    object_payload_outstanding_by_pg: HashMap<u32, usize>,
    queued_bucket_delete_begins: HashSet<LocalBucketDeleteBeginRoot>,
    queued_bucket_deletes: HashSet<BucketDeleteFinalizeRoot>,
    bucket_delete_admissions: HashMap<BucketDeleteFinalizeRoot, LocalBucketDeleteAdmissionState>,
}

#[derive(Debug, Default)]
struct LocalBucketDeleteAdmissionState {
    in_flight: usize,
    begin_roots: HashSet<LocalBucketDeleteBeginRoot>,
    finalizer_outstanding: bool,
}

impl LocalBucketDeleteAdmissionState {
    fn has_outstanding_work(&self) -> bool {
        self.finalizer_outstanding || !self.begin_roots.is_empty()
    }
}

pub(crate) struct LocalBucketDeleteFinalizeAdmission<'a> {
    runtime: &'a LocalClusterRuntimeState,
    root: BucketDeleteFinalizeRoot,
    resolved: bool,
}

impl LocalBucketDeleteFinalizeAdmission<'_> {
    pub(crate) fn commit(mut self) {
        let (state_lock, cv) = &self.runtime.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|error| error.into_inner());
        let admission = state
            .bucket_delete_admissions
            .get_mut(&self.root)
            .expect("bucket delete admission must remain registered until resolved");
        debug_assert!(admission.in_flight > 0);
        admission.in_flight -= 1;
        admission.finalizer_outstanding = true;
        if state.queued_bucket_deletes.insert(self.root.clone()) {
            state
                .work_queue
                .push_back(ReclaimWorkItem::BucketDelete(self.root.clone()));
            LocalClusterRuntimeState::emit_reclaim_queue_action(&state, "bucket_delete", "admit");
            cv.notify_one();
        }
        self.resolved = true;
    }

    pub(crate) fn retain(mut self, begin_root: LocalBucketDeleteBeginRoot) {
        assert_eq!(
            begin_root.finalize_root(),
            self.root,
            "retained bucket delete begin must match its admission root"
        );
        let (state_lock, _) = &self.runtime.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|error| error.into_inner());
        let admission = state
            .bucket_delete_admissions
            .get_mut(&self.root)
            .expect("bucket delete admission must remain registered until resolved");
        debug_assert!(admission.in_flight > 0);
        admission.in_flight -= 1;
        admission.begin_roots.insert(begin_root);
        self.resolved = true;
    }
}

impl Drop for LocalBucketDeleteFinalizeAdmission<'_> {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let mut state = self
            .runtime
            .reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(admission) = state.bucket_delete_admissions.get_mut(&self.root) {
            debug_assert!(admission.in_flight > 0);
            admission.in_flight -= 1;
            if admission.in_flight == 0 && !admission.has_outstanding_work() {
                state.bucket_delete_admissions.remove(&self.root);
            }
        }
        LocalClusterRuntimeState::emit_reclaim_queue_action(
            &state,
            "bucket_delete",
            "admission_cancel",
        );
    }
}

#[derive(Debug)]
struct LocalPlacedSegmentShardRepairQueueState {
    work_queue: VecDeque<PlacedSegmentShardRepairWorkItem>,
    queued: HashSet<PlacedSegmentShardRepairWorkItem>,
}

impl LocalClusterRuntimeState {
    fn new() -> Self {
        Self {
            reclaim_queue: (
                Mutex::new(LocalReclaimQueueState {
                    work_queue: VecDeque::new(),
                    queued_objects: HashSet::new(),
                    outstanding_objects: HashMap::new(),
                    object_payload_outstanding_by_pg: HashMap::new(),
                    queued_bucket_delete_begins: HashSet::new(),
                    queued_bucket_deletes: HashSet::new(),
                    bucket_delete_admissions: HashMap::new(),
                }),
                Condvar::new(),
            ),
            active_object_payload_reclaims: Mutex::new(HashSet::new()),
            placed_segment_shard_repair_queue: (
                Mutex::new(LocalPlacedSegmentShardRepairQueueState {
                    work_queue: VecDeque::new(),
                    queued: HashSet::new(),
                }),
                Condvar::new(),
            ),
            metadata_command_pg_locks: Mutex::new(HashMap::new()),
            metadata_command_recovery_flights: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            metadata_command_recovery_wait_hook: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_install_metadata_command_recovery_wait_hook(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        timeout_selected: Arc<Barrier>,
        retry_selected: Arc<Barrier>,
    ) {
        let mut hook = self
            .metadata_command_recovery_wait_hook
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert!(
            hook.is_none(),
            "metadata command recovery wait hook already installed"
        );
        *hook = Some(MetadataCommandRecoveryWaitTestHook {
            key: MetadataCommandRecoveryKey::new(pg_id, command),
            existing_flight_selections: 0,
            forced_timeouts_remaining: 1,
            wait_for_owner_selection: 2,
            timeout_selected,
            retry_selected,
        });
    }

    #[cfg(test)]
    pub(crate) fn test_install_metadata_command_recovery_owner_completion_hook(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        owner_release_selected: Arc<Barrier>,
    ) {
        let mut hook = self
            .metadata_command_recovery_wait_hook
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert!(
            hook.is_none(),
            "metadata command recovery wait hook already installed"
        );
        *hook = Some(MetadataCommandRecoveryWaitTestHook {
            key: MetadataCommandRecoveryKey::new(pg_id, command),
            existing_flight_selections: 0,
            forced_timeouts_remaining: 0,
            wait_for_owner_selection: 1,
            timeout_selected: Arc::new(Barrier::new(1)),
            retry_selected: owner_release_selected,
        });
    }

    #[cfg(test)]
    pub(crate) fn test_take_metadata_command_recovery_wait_hook_observation(
        &self,
    ) -> (usize, usize) {
        let hook = self
            .metadata_command_recovery_wait_hook
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .expect("metadata command recovery wait hook must be installed");
        (
            hook.existing_flight_selections,
            hook.forced_timeouts_remaining,
        )
    }

    pub(crate) fn metadata_command_pg_lock(&self, pg_id: PgId) -> Arc<MetadataCommandPgLock> {
        let mut locks = self
            .metadata_command_pg_locks
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        Arc::clone(
            locks
                .entry(pg_id)
                .or_insert_with(|| Arc::new(MetadataCommandPgLock::default())),
        )
    }

    pub(crate) fn metadata_command_recovery_handoff_source(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Option<MetadataCommandEnvelope> {
        let flights = self
            .metadata_command_recovery_flights
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let flight = flights.get(&MetadataCommandRecoveryKey::new(pg_id, command))?;
        let state = flight
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        (state.awaiting_authorized_recovery && state.lineage_tip == *command)
            .then(|| state.lineage_root.clone())
    }

    #[cfg(test)]
    pub(crate) fn test_metadata_command_pg_lock_ptr(&self, pg_id: PgId) -> usize {
        Arc::as_ptr(&self.metadata_command_pg_lock(pg_id)) as usize
    }

    #[cfg(test)]
    pub(crate) fn test_metadata_command_recovery_flight_count(&self) -> usize {
        self.metadata_command_recovery_flights
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    #[cfg(test)]
    pub(crate) fn test_metadata_command_recovery_awaiting_authorized(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> bool {
        let flights = self
            .metadata_command_recovery_flights
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        flights
            .get(&MetadataCommandRecoveryKey::new(pg_id, command))
            .is_some_and(|flight| {
                flight
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .awaiting_authorized_recovery
            })
    }

    #[cfg(test)]
    pub(crate) fn test_metadata_command_recovery_handoff_requested(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> bool {
        let flights = self
            .metadata_command_recovery_flights
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        flights
            .get(&MetadataCommandRecoveryKey::new(pg_id, command))
            .is_some_and(|flight| {
                flight
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .authorized_recovery_handoff_requested
            })
    }

    #[cfg(test)]
    pub(crate) fn test_metadata_command_recovery_commands_share_flight(
        &self,
        pg_id: PgId,
        first: &MetadataCommandEnvelope,
        second: &MetadataCommandEnvelope,
    ) -> bool {
        let flights = self
            .metadata_command_recovery_flights
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(first) = flights.get(&MetadataCommandRecoveryKey::new(pg_id, first)) else {
            return false;
        };
        let Some(second) = flights.get(&MetadataCommandRecoveryKey::new(pg_id, second)) else {
            return false;
        };
        Arc::ptr_eq(first, second)
    }

    #[cfg(test)]
    pub(crate) fn join_metadata_command_recovery(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> MetadataCommandRecoveryAdmission {
        let deadline = Instant::now()
            .checked_add(METADATA_COMMAND_RECOVERY_WAIT_TIMEOUT)
            .unwrap_or_else(Instant::now);
        self.join_metadata_command_recovery_until(pg_id, command, deadline)
    }

    pub(crate) fn join_metadata_command_recovery_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> MetadataCommandRecoveryAdmission {
        self.join_metadata_command_recovery_until_inner(
            pg_id, command, None, deadline, false, false,
        )
    }

    pub(crate) fn join_metadata_command_recovery_as_unrelated_drainer_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> MetadataCommandRecoveryAdmission {
        self.join_metadata_command_recovery_until_inner(pg_id, command, None, deadline, false, true)
    }

    pub(crate) fn join_metadata_command_recovery_and_wait_for_authorized_handoff_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> MetadataCommandRecoveryAdmission {
        self.join_metadata_command_recovery_until_inner(pg_id, command, None, deadline, true, false)
    }

    pub(crate) fn join_authorized_metadata_command_recovery_until(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        authorized_source: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> MetadataCommandRecoveryAdmission {
        self.join_metadata_command_recovery_until_inner(
            pg_id,
            command,
            Some(authorized_source),
            deadline,
            false,
            false,
        )
    }

    fn join_metadata_command_recovery_until_inner(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
        authorized_source: Option<&MetadataCommandEnvelope>,
        deadline: Instant,
        wait_for_authorized_handoff: bool,
        request_authorized_handoff_on_published: bool,
    ) -> MetadataCommandRecoveryAdmission {
        let key = MetadataCommandRecoveryKey::new(pg_id, command);
        let flights = Arc::clone(&self.metadata_command_recovery_flights);
        let (flight, is_leader) = {
            let mut flights_guard = flights.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(flight) = flights_guard.get(&key) {
                let flight = Arc::clone(flight);
                let mut state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
                let authorized_takeover = authorized_source.is_some_and(|source| {
                    if state.awaiting_authorized_recovery
                        && !state.in_progress
                        && state.lineage_root == *source
                    {
                        state.awaiting_authorized_recovery = false;
                        state.in_progress = true;
                        true
                    } else {
                        false
                    }
                });
                if request_authorized_handoff_on_published && !authorized_takeover {
                    state.authorized_recovery_handoff_requested = true;
                }
                drop(state);
                (flight, authorized_takeover)
            } else if Instant::now() >= deadline {
                if request_authorized_handoff_on_published {
                    let flight = Arc::new(MetadataCommandRecoveryFlight {
                        state: Mutex::new(MetadataCommandRecoveryFlightState {
                            in_progress: false,
                            awaiting_authorized_recovery: true,
                            authorized_recovery_handoff_requested: true,
                            keys: HashSet::from([key]),
                            lineage_root: command.clone(),
                            lineage_tip: command.clone(),
                            root_disposition: MetadataCommandRecoveryRootDisposition::TipOutcome,
                            resolution: None,
                        }),
                        done: Condvar::new(),
                    });
                    flights_guard.insert(key, flight);
                    return MetadataCommandRecoveryAdmission::AwaitingAuthorizedRecovery {
                        wait_us: 0,
                        lineage_tip: command.clone(),
                        resolution: None,
                    };
                }
                return MetadataCommandRecoveryAdmission::TimedOut {
                    wait_us: 0,
                    lineage_tip: command.clone(),
                    resolution: None,
                };
            } else {
                let flight = Arc::new(MetadataCommandRecoveryFlight {
                    state: Mutex::new(MetadataCommandRecoveryFlightState {
                        in_progress: true,
                        awaiting_authorized_recovery: false,
                        authorized_recovery_handoff_requested:
                            request_authorized_handoff_on_published,
                        keys: HashSet::from([key]),
                        lineage_root: command.clone(),
                        lineage_tip: command.clone(),
                        root_disposition: MetadataCommandRecoveryRootDisposition::TipOutcome,
                        resolution: None,
                    }),
                    done: Condvar::new(),
                });
                flights_guard.insert(key, Arc::clone(&flight));
                (flight, true)
            }
        };
        if is_leader {
            return MetadataCommandRecoveryAdmission::Leader(MetadataCommandRecoveryGuard {
                root_key: key,
                flight,
                flights,
                remove_flight_on_drop: true,
            });
        }
        {
            let state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.in_progress
                && matches!(
                    state.resolution,
                    Some(MetadataCommandRecoveryResolution::Outcome(outcome))
                        if outcome.retains_pending_slot()
                )
            {
                return MetadataCommandRecoveryAdmission::Waited {
                    wait_us: 0,
                    lineage_tip: state.lineage_tip.clone(),
                    resolution: state.resolution,
                };
            }
            if state.awaiting_authorized_recovery
                && !state.in_progress
                && (!wait_for_authorized_handoff
                    || matches!(
                        state.resolution,
                        Some(MetadataCommandRecoveryResolution::Outcome(_))
                    ))
            {
                return MetadataCommandRecoveryAdmission::AwaitingAuthorizedRecovery {
                    wait_us: 0,
                    lineage_tip: state.lineage_tip.clone(),
                    resolution: state.resolution,
                };
            }
        }
        if Instant::now() >= deadline {
            let state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
            return MetadataCommandRecoveryAdmission::TimedOut {
                wait_us: 0,
                lineage_tip: state.lineage_tip.clone(),
                resolution: state.resolution,
            };
        }

        #[cfg(test)]
        {
            let action = {
                let mut hook = self
                    .metadata_command_recovery_wait_hook
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                hook.as_mut().and_then(|hook| {
                    if hook.key != key {
                        return None;
                    }
                    hook.existing_flight_selections += 1;
                    if hook.forced_timeouts_remaining > 0 {
                        hook.forced_timeouts_remaining -= 1;
                        Some(MetadataCommandRecoveryWaitTestAction::ForceTimeout(
                            Arc::clone(&hook.timeout_selected),
                        ))
                    } else if hook.existing_flight_selections == hook.wait_for_owner_selection {
                        Some(MetadataCommandRecoveryWaitTestAction::WaitForOwner(
                            Arc::clone(&hook.retry_selected),
                        ))
                    } else {
                        None
                    }
                })
            };
            match action {
                Some(MetadataCommandRecoveryWaitTestAction::ForceTimeout(barrier)) => {
                    barrier.wait();
                    let state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
                    return MetadataCommandRecoveryAdmission::TimedOut {
                        wait_us: 0,
                        lineage_tip: state.lineage_tip.clone(),
                        resolution: state.resolution,
                    };
                }
                Some(MetadataCommandRecoveryWaitTestAction::WaitForOwner(barrier)) => {
                    barrier.wait();
                }
                None => {}
            }
        }

        let wait_started = Instant::now();
        let wait_timeout = deadline
            .saturating_duration_since(wait_started)
            .min(METADATA_COMMAND_RECOVERY_WAIT_TIMEOUT);
        let (guard, wait_result) = flight
            .done
            .wait_timeout_while(
                flight.state.lock().unwrap_or_else(|e| e.into_inner()),
                wait_timeout,
                |state| {
                    state.in_progress
                        || (wait_for_authorized_handoff && state.awaiting_authorized_recovery)
                },
            )
            .unwrap_or_else(|e| e.into_inner());
        let wait_us = wait_started.elapsed().as_micros();
        let lineage_tip = guard.lineage_tip.clone();
        let resolution = guard.resolution;
        if guard.awaiting_authorized_recovery
            && !guard.in_progress
            && (!wait_for_authorized_handoff
                || matches!(
                    guard.resolution,
                    Some(MetadataCommandRecoveryResolution::Outcome(_))
                ))
        {
            MetadataCommandRecoveryAdmission::AwaitingAuthorizedRecovery {
                wait_us,
                lineage_tip,
                resolution,
            }
        } else if guard.in_progress || guard.awaiting_authorized_recovery {
            debug_assert!(wait_result.timed_out());
            MetadataCommandRecoveryAdmission::TimedOut {
                wait_us,
                lineage_tip,
                resolution,
            }
        } else {
            MetadataCommandRecoveryAdmission::Waited {
                wait_us,
                lineage_tip,
                resolution,
            }
        }
    }

    pub(crate) fn enqueue_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        pg_id: u32,
    ) -> ReclaimQueueInsert {
        self.enqueue_object_payload_reclaim_for_pg(bucket, key, generation_id, pg_id)
    }

    pub(crate) fn try_acquire_object_payload_reclaim_execution(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Option<LocalObjectPayloadReclaimExecution<'_>> {
        let root = (bucket.clone(), key.clone(), generation_id);
        let mut active = self
            .active_object_payload_reclaims
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !active.insert(root.clone()) {
            return None;
        }
        Some(LocalObjectPayloadReclaimExecution {
            runtime: self,
            root,
        })
    }

    pub(crate) fn enqueue_object_payload_reclaim_for_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        pg_id: u32,
    ) -> ReclaimQueueInsert {
        let root = (bucket.clone(), key.clone(), generation_id);
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        if state.outstanding_objects.contains_key(&root) {
            Self::emit_reclaim_queue_action(&state, "object_payload", "deduplicate");
            return ReclaimQueueInsert::Deduplicated;
        }
        let outstanding = state
            .object_payload_outstanding_by_pg
            .get(&pg_id)
            .copied()
            .unwrap_or(0);
        if outstanding >= OBJECT_PAYLOAD_RECLAIM_MAX_OUTSTANDING_PER_PG {
            Self::emit_reclaim_queue_action(&state, "object_payload", "pg_capacity_deferred");
            return ReclaimQueueInsert::PgCapacityDeferred;
        }
        state.queued_objects.insert(root.clone());
        state.outstanding_objects.insert(root.clone(), pg_id);
        state
            .object_payload_outstanding_by_pg
            .insert(pg_id, outstanding + 1);
        state
            .work_queue
            .push_back(ReclaimWorkItem::ObjectPayload(root));
        Self::emit_reclaim_queue_action(&state, "object_payload", "enqueue");
        cv.notify_one();
        ReclaimQueueInsert::Queued
    }

    pub(crate) fn finish_object_payload_reclaim_work(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        let root = (bucket.clone(), key.clone(), generation_id);
        let mut state = self
            .reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(pg_id) = state.outstanding_objects.remove(&root) else {
            return;
        };
        state.queued_objects.remove(&root);
        if let Some(outstanding) = state.object_payload_outstanding_by_pg.get_mut(&pg_id) {
            *outstanding = outstanding.saturating_sub(1);
            if *outstanding == 0 {
                state.object_payload_outstanding_by_pg.remove(&pg_id);
            }
        }
        Self::emit_reclaim_queue_action(&state, "object_payload", "finish");
    }

    pub(crate) fn enqueue_bucket_delete_finalize(&self, root: BucketDeleteFinalizeRoot) -> bool {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        state
            .bucket_delete_admissions
            .entry(root.clone())
            .or_default()
            .finalizer_outstanding = true;
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

    pub(crate) fn try_admit_bucket_delete_finalize(
        &self,
        root: BucketDeleteFinalizeRoot,
        capacity: usize,
    ) -> Option<LocalBucketDeleteFinalizeAdmission<'_>> {
        let mut state = self
            .reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let already_tracked = state.bucket_delete_admissions.contains_key(&root);
        if !already_tracked && state.bucket_delete_admissions.len() >= capacity {
            Self::emit_reclaim_queue_action(&state, "bucket_delete", "admission_reject");
            return None;
        }
        state
            .bucket_delete_admissions
            .entry(root.clone())
            .or_default()
            .in_flight += 1;
        if !already_tracked {
            Self::emit_reclaim_queue_action(&state, "bucket_delete", "admission_reserve");
        }
        Some(LocalBucketDeleteFinalizeAdmission {
            runtime: self,
            root,
            resolved: false,
        })
    }

    pub(crate) fn enqueue_bucket_delete_begin(&self, root: LocalBucketDeleteBeginRoot) -> bool {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        let finalize_root = root.finalize_root();
        state
            .bucket_delete_admissions
            .entry(finalize_root)
            .or_default()
            .begin_roots
            .insert(root.clone());
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

    pub(crate) fn finish_bucket_delete_begin_work(&self, root: &LocalBucketDeleteBeginRoot) {
        let mut state = self
            .reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.queued_bucket_delete_begins.remove(root);
        state.work_queue.retain(
            |work| !matches!(work, ReclaimWorkItem::BucketDeleteBegin(queued) if queued == root),
        );
        let finalize_root = root.finalize_root();
        let mut remove_admission = false;
        let finished =
            if let Some(admission) = state.bucket_delete_admissions.get_mut(&finalize_root) {
                let removed = admission.begin_roots.remove(root);
                remove_admission = admission.in_flight == 0 && !admission.has_outstanding_work();
                removed
            } else {
                false
            };
        if remove_admission {
            state.bucket_delete_admissions.remove(&finalize_root);
        }
        if finished {
            Self::emit_reclaim_queue_action(&state, "bucket_delete_begin", "finish");
        }
    }

    pub(crate) fn promote_bucket_delete_begin_to_finalize(
        &self,
        root: &LocalBucketDeleteBeginRoot,
    ) {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|error| error.into_inner());
        state.queued_bucket_delete_begins.remove(root);
        state.work_queue.retain(
            |work| !matches!(work, ReclaimWorkItem::BucketDeleteBegin(queued) if queued == root),
        );
        let finalize_root = root.finalize_root();
        let admission = state
            .bucket_delete_admissions
            .entry(finalize_root.clone())
            .or_default();
        admission.begin_roots.remove(root);
        admission.finalizer_outstanding = true;
        if state.queued_bucket_deletes.insert(finalize_root.clone()) {
            state
                .work_queue
                .push_back(ReclaimWorkItem::BucketDelete(finalize_root));
            Self::emit_reclaim_queue_action(&state, "bucket_delete_begin", "promote");
            cv.notify_one();
        }
    }

    pub(crate) fn finish_bucket_delete_finalize_work(&self, root: &BucketDeleteFinalizeRoot) {
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
        let mut remove_admission = false;
        let finished = if let Some(admission) = state.bucket_delete_admissions.get_mut(root) {
            let was_outstanding = admission.has_outstanding_work();
            admission.begin_roots.clear();
            admission.finalizer_outstanding = false;
            remove_admission = admission.in_flight == 0 && !admission.has_outstanding_work();
            was_outstanding
        } else {
            false
        };
        if remove_admission {
            state.bucket_delete_admissions.remove(root);
        }
        if finished {
            Self::emit_reclaim_queue_action(&state, "bucket_delete", "finish");
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_bucket_delete_finalize_outstanding_depth(&self) -> usize {
        self.reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .bucket_delete_admissions
            .values()
            .filter(|admission| admission.has_outstanding_work())
            .count()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_reclaim_outstanding_depth(&self) -> usize {
        self.reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .outstanding_objects
            .len()
    }

    pub(crate) fn enqueue_placed_segment_shard_repair(
        &self,
        work_item: PlacedSegmentShardRepairWorkItem,
    ) -> bool {
        let (state_lock, cv) = &self.placed_segment_shard_repair_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        if state.queued.contains(&work_item) {
            Self::emit_shard_repair_queue_event(
                &state,
                Some(work_item.request.data_pg_id),
                "deduped",
            );
            return false;
        }
        if state.work_queue.len() >= LOCAL_PLACED_SEGMENT_SHARD_REPAIR_HINT_QUEUE_LIMIT {
            Self::emit_shard_repair_queue_event(
                &state,
                Some(work_item.request.data_pg_id),
                "queue_full",
            );
            return false;
        }
        state.queued.insert(work_item);
        state.work_queue.push_back(work_item);
        Self::emit_shard_repair_queue_event(&state, Some(work_item.request.data_pg_id), "queued");
        cv.notify_one();
        true
    }

    pub(crate) fn try_take_placed_segment_shard_repair_work(
        &self,
    ) -> Option<PlacedSegmentShardRepairWorkItem> {
        let mut state = self
            .placed_segment_shard_repair_queue
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let work = state.work_queue.pop_front()?;
        state.queued.remove(&work);
        Self::emit_shard_repair_queue_event(&state, Some(work.request.data_pg_id), "dequeued");
        Some(work)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn try_take_matching_placed_segment_shard_repair_work(
        &self,
        mut matches: impl FnMut(&PlacedSegmentShardRepairWorkItem) -> bool,
    ) -> Option<PlacedSegmentShardRepairWorkItem> {
        let mut state = self
            .placed_segment_shard_repair_queue
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let position = state.work_queue.iter().position(&mut matches)?;
        let work = state
            .work_queue
            .remove(position)
            .expect("matched repair queue position exists");
        state.queued.remove(&work);
        Self::emit_shard_repair_queue_event(&state, Some(work.request.data_pg_id), "dequeued");
        Some(work)
    }

    pub(crate) fn wait_for_placed_segment_shard_repair_work_poll(
        &self,
        stop: &AtomicBool,
    ) -> Option<PlacedSegmentShardRepairWorkItem> {
        let (state_lock, cv) = &self.placed_segment_shard_repair_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        if state.work_queue.is_empty() && !stop.load(Ordering::SeqCst) {
            let (next_state, _) = cv
                .wait_timeout(
                    state,
                    Duration::from_millis(
                        LOCAL_PLACED_SEGMENT_SHARD_REPAIR_WORKER_WAIT_POLL_MILLIS,
                    ),
                )
                .unwrap_or_else(|e| e.into_inner());
            state = next_state;
        }
        if stop.load(Ordering::SeqCst) {
            return None;
        }
        let work = state.work_queue.pop_front()?;
        state.queued.remove(&work);
        Self::emit_shard_repair_queue_event(&state, Some(work.request.data_pg_id), "dequeued");
        Some(work)
    }

    pub(crate) fn wake_placed_segment_shard_repair_workers(&self) {
        self.placed_segment_shard_repair_queue.1.notify_all();
    }

    pub(crate) fn try_take_reclaim_work(&self) -> Option<ReclaimWorkItem> {
        let mut state = self
            .reclaim_queue
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        Self::pop_reclaim_work(&mut state)
    }

    pub(crate) fn wait_for_reclaim_work_poll(&self, stop: &AtomicBool) -> Option<ReclaimWorkItem> {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        if state.work_queue.is_empty() && !stop.load(Ordering::SeqCst) {
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
        Self::pop_reclaim_work(&mut state)
    }

    pub(crate) fn wake_reclaim_workers(&self) {
        self.reclaim_queue.1.notify_all();
    }

    fn pop_reclaim_work(state: &mut LocalReclaimQueueState) -> Option<ReclaimWorkItem> {
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
        state: &LocalReclaimQueueState,
        work_kind: &'static str,
        action: &'static str,
    ) {
        let _ = observability::emit_reclaim_queue_action(
            super::TRACE_TARGET,
            observability::ReclaimQueueSummary {
                work_kind,
                action,
                queue_depth: state.work_queue.len(),
                object_payload_depth: state.queued_objects.len(),
                object_payload_outstanding_depth: state.outstanding_objects.len(),
                bucket_delete_begin_depth: state.queued_bucket_delete_begins.len(),
                bucket_delete_finalize_depth: state.queued_bucket_deletes.len(),
                bucket_delete_finalize_outstanding_depth: state
                    .bucket_delete_admissions
                    .values()
                    .filter(|admission| admission.has_outstanding_work())
                    .count(),
            },
        );
    }

    fn emit_shard_repair_queue_event(
        state: &LocalPlacedSegmentShardRepairQueueState,
        pg_id: Option<u32>,
        event: &'static str,
    ) {
        let _ = observability::emit_shard_repair_event(
            "storage",
            observability::ShardRepairEventSummary {
                pg_id,
                event,
                queue_depth: Some(state.work_queue.len()),
                shards_rewritten: None,
            },
        );
    }
}

#[derive(Debug)]
pub struct LocalClusterMap {
    epoch: ClusterEpoch,
    route_map_lease: RwLock<LocalRouteMapLeaseSnapshot>,
    metadata_primary_node_id: NodeId,
    nodes: BTreeMap<NodeId, LocalNodeStore>,
    pg_ids: Box<[u32]>,
    pg_topology: PgTopology,
    default_ec_shape: EcShape,
    pg_routes: BTreeMap<PgId, LocalPgRoute>,
    historical_pg_routes: BTreeMap<(PgId, ClusterEpoch), PgRouteSnapshot>,
    historical_cluster_epochs: BTreeSet<ClusterEpoch>,
    runtime_state: Arc<LocalClusterRuntimeState>,
    process_local_registry_key: ProcessLocalRegistryKey,
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
        Self::open_with_configs_and_epoch(
            metadata_primary_node_id,
            configs,
            pg_ids,
            default_ec_shape,
            ClusterEpoch::INITIAL,
        )
    }

    pub fn open_with_configs_and_epoch(
        metadata_primary_node_id: NodeId,
        configs: impl IntoIterator<Item = LocalNodeStoreConfig>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Self, ClusterBuildError> {
        Self::open_with_configs_inner(
            metadata_primary_node_id,
            configs,
            pg_ids,
            default_ec_shape,
            cluster_epoch,
            None,
            true,
        )
    }

    pub fn open_frontend_placeholder_with_configs_and_epoch(
        metadata_primary_node_id: NodeId,
        configs: impl IntoIterator<Item = LocalNodeStoreConfig>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Self, ClusterBuildError> {
        Self::open_with_configs_inner(
            metadata_primary_node_id,
            configs,
            pg_ids,
            default_ec_shape,
            cluster_epoch,
            None,
            false,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn open_frontend_with_configs_and_pg_routes(
        metadata_primary_node_id: NodeId,
        configs: impl IntoIterator<Item = LocalNodeStoreConfig>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
        pg_routes: impl IntoIterator<Item = LocalPgRoute>,
    ) -> Result<Self, ClusterBuildError> {
        Self::open_with_configs_inner(
            metadata_primary_node_id,
            configs,
            pg_ids,
            default_ec_shape,
            cluster_epoch,
            Some(pg_routes.into_iter().collect()),
            false,
        )
    }

    #[cfg(test)]
    pub(crate) fn open_frontend_with_configs_and_runtime_map(
        metadata_primary_node_id: NodeId,
        configs: impl IntoIterator<Item = LocalNodeStoreConfig>,
        default_ec_shape: EcShape,
        runtime_map: &ClusterRuntimeMapSnapshot,
    ) -> Result<Self, ClusterBuildError> {
        let pg_ids = runtime_map
            .pg_routes()
            .iter()
            .map(|route| route.pg_id().get())
            .collect::<Vec<_>>();
        let mut local_map = Self::open_with_configs_inner(
            metadata_primary_node_id,
            configs,
            &pg_ids,
            default_ec_shape,
            runtime_map.cluster_epoch(),
            Some(
                runtime_map
                    .pg_routes()
                    .iter()
                    .map(LocalPgRoute::from)
                    .collect(),
            ),
            false,
        )?;
        local_map.bind_runtime_map_advertised_endpoints(runtime_map);
        local_map.historical_pg_routes = runtime_map
            .historical_pg_routes()
            .iter()
            .map(|route| ((route.pg_id(), route.cluster_epoch()), route.clone()))
            .collect();
        local_map.historical_cluster_epochs = runtime_map
            .historical_cluster_epochs()
            .iter()
            .copied()
            .collect();
        let bound_lease = runtime_map
            .bind_process_local_lease_at(
                crate::clock::current_time_millis(),
                crate::clock::monotonic_time_millis(),
            )
            .map_err(|error| ClusterBuildError::RouteMapLeaseBinding {
                message: error.to_string(),
            })?;
        local_map.route_map_lease = RwLock::new(LocalRouteMapLeaseSnapshot {
            validity: runtime_map.validity(),
            local_valid_until_monotonic_ms: bound_lease
                .map(BoundRouteMapLease::local_valid_until_monotonic_ms),
        });
        Ok(local_map)
    }

    pub fn open_frontend_topology_only_with_epoch(
        metadata_primary_node_id: NodeId,
        node_ids: impl IntoIterator<Item = NodeId>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Self, ClusterBuildError> {
        let mut ordered_node_ids = Vec::new();
        let mut node_id_set = BTreeSet::<NodeId>::new();
        for node_id in node_ids {
            if !node_id_set.insert(node_id) {
                return Err(ClusterBuildError::DuplicateNodeId {
                    id: node_id.as_u32(),
                });
            }
            ordered_node_ids.push(node_id);
        }
        if ordered_node_ids.is_empty() {
            return Err(ClusterBuildError::EmptyCluster);
        }
        if !node_id_set.contains(&metadata_primary_node_id) {
            return Err(ClusterBuildError::MetadataPrimaryNotFound {
                id: metadata_primary_node_id.as_u32(),
            });
        }
        let pg_ids = validate_local_pg_ids(pg_ids)?;
        let placement_map = build_local_placement_map(node_id_set.iter().copied())?;
        validate_local_payload_placement(&placement_map, default_ec_shape)?;

        let acting_set = Arc::<[NodeId]>::from(node_id_set.iter().copied().collect::<Vec<_>>());
        let storage_pg_ids: Vec<u32> = pg_ids.iter().map(|pg_id| pg_id.get()).collect();
        let pg_topology = PgTopology::new(&storage_pg_ids).map_err(|reason| {
            ClusterBuildError::InvalidLocalPlacement {
                reason: reason.to_string(),
            }
        })?;
        let pg_routes = build_static_pg_routes(
            cluster_epoch,
            metadata_primary_node_id,
            Arc::clone(&acting_set),
            &pg_ids,
        );

        let mut nodes = BTreeMap::new();
        for node_id in ordered_node_ids {
            let node_store =
                LocalNodeStore::topology_only(node_id, &storage_pg_ids, default_ec_shape).map_err(
                    |source| ClusterBuildError::open_local_node(node_id.as_u32(), source),
                )?;
            nodes.insert(node_id, node_store);
        }
        let metadata_primary = nodes
            .get(&metadata_primary_node_id)
            .expect("validated metadata primary should have been opened");

        Ok(Self {
            epoch: cluster_epoch,
            metadata_primary_node_id,
            pg_ids: storage_pg_ids.into_boxed_slice(),
            pg_topology,
            default_ec_shape,
            pg_routes,
            historical_pg_routes: BTreeMap::new(),
            historical_cluster_epochs: BTreeSet::new(),
            route_map_lease: RwLock::new(LocalRouteMapLeaseSnapshot::unbound(
                RouteMapValidity::Forever,
            )),
            runtime_state: Arc::new(LocalClusterRuntimeState::new()),
            process_local_registry_key: metadata_primary.runtime().process_local_registry_key(),
            nodes,
        })
    }

    pub fn open_frontend_topology_only_with_pg_routes(
        metadata_primary_node_id: NodeId,
        node_ids: impl IntoIterator<Item = NodeId>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
        pg_routes: impl IntoIterator<Item = LocalPgRoute>,
    ) -> Result<Self, ClusterBuildError> {
        Self::open_frontend_topology_only_with_pg_routes_and_validity(
            metadata_primary_node_id,
            node_ids,
            pg_ids,
            default_ec_shape,
            cluster_epoch,
            pg_routes,
            RouteMapValidity::Forever,
        )
    }

    pub fn open_frontend_topology_only_with_pg_routes_and_validity(
        metadata_primary_node_id: NodeId,
        node_ids: impl IntoIterator<Item = NodeId>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
        pg_routes: impl IntoIterator<Item = LocalPgRoute>,
        route_map_validity: RouteMapValidity,
    ) -> Result<Self, ClusterBuildError> {
        let mut ordered_node_ids = Vec::new();
        let mut node_id_set = BTreeSet::<NodeId>::new();
        for node_id in node_ids {
            if !node_id_set.insert(node_id) {
                return Err(ClusterBuildError::DuplicateNodeId {
                    id: node_id.as_u32(),
                });
            }
            ordered_node_ids.push(node_id);
        }
        if ordered_node_ids.is_empty() {
            return Err(ClusterBuildError::EmptyCluster);
        }
        if !node_id_set.contains(&metadata_primary_node_id) {
            return Err(ClusterBuildError::MetadataPrimaryNotFound {
                id: metadata_primary_node_id.as_u32(),
            });
        }
        let pg_ids = validate_local_pg_ids(pg_ids)?;
        let placement_map = build_local_placement_map(node_id_set.iter().copied())?;
        validate_local_payload_placement(&placement_map, default_ec_shape)?;

        let storage_pg_ids: Vec<u32> = pg_ids.iter().map(|pg_id| pg_id.get()).collect();
        let pg_topology = PgTopology::new(&storage_pg_ids).map_err(|reason| {
            ClusterBuildError::InvalidLocalPlacement {
                reason: reason.to_string(),
            }
        })?;
        let pg_routes = build_validated_pg_routes(cluster_epoch, &node_id_set, &pg_ids, pg_routes)?;

        let mut nodes = BTreeMap::new();
        for node_id in ordered_node_ids {
            let node_store =
                LocalNodeStore::topology_only(node_id, &storage_pg_ids, default_ec_shape).map_err(
                    |source| ClusterBuildError::open_local_node(node_id.as_u32(), source),
                )?;
            nodes.insert(node_id, node_store);
        }
        let metadata_primary = nodes
            .get(&metadata_primary_node_id)
            .expect("validated metadata primary should have been opened");

        Ok(Self {
            epoch: cluster_epoch,
            metadata_primary_node_id,
            pg_ids: storage_pg_ids.into_boxed_slice(),
            pg_topology,
            default_ec_shape,
            pg_routes,
            historical_pg_routes: BTreeMap::new(),
            historical_cluster_epochs: BTreeSet::new(),
            route_map_lease: RwLock::new(LocalRouteMapLeaseSnapshot::unbound(route_map_validity)),
            runtime_state: Arc::new(LocalClusterRuntimeState::new()),
            process_local_registry_key: metadata_primary.runtime().process_local_registry_key(),
            nodes,
        })
    }

    pub fn open_frontend_topology_only_with_runtime_map(
        metadata_primary_node_id: NodeId,
        runtime_map: &ClusterRuntimeMapSnapshot,
        default_ec_shape: EcShape,
    ) -> Result<Self, ClusterBuildError> {
        let node_ids: Vec<NodeId> = runtime_map
            .nodes()
            .iter()
            .map(NodeRouteSnapshot::node_id)
            .collect();
        let pg_ids: Vec<u32> = runtime_map
            .pg_routes()
            .iter()
            .map(|route| route.pg_id().get())
            .collect();
        let pg_routes: Vec<LocalPgRoute> = runtime_map
            .pg_routes()
            .iter()
            .map(LocalPgRoute::from)
            .collect();

        let mut local_map = Self::open_frontend_topology_only_with_pg_routes_and_validity(
            metadata_primary_node_id,
            node_ids,
            &pg_ids,
            default_ec_shape,
            runtime_map.cluster_epoch(),
            pg_routes,
            runtime_map.validity(),
        )?;
        local_map.bind_runtime_map_advertised_endpoints(runtime_map);
        local_map.historical_pg_routes = runtime_map
            .historical_pg_routes()
            .iter()
            .map(|route| ((route.pg_id(), route.cluster_epoch()), route.clone()))
            .collect();
        local_map.historical_cluster_epochs = runtime_map
            .historical_cluster_epochs()
            .iter()
            .copied()
            .collect();
        let local_monotonic_ms = crate::clock::monotonic_time_millis();
        let bound_lease = runtime_map
            .bind_process_local_lease_at(crate::clock::current_time_millis(), local_monotonic_ms)
            .map_err(|error| ClusterBuildError::RouteMapLeaseBinding {
                message: error.to_string(),
            })?;
        local_map.route_map_lease = RwLock::new(LocalRouteMapLeaseSnapshot {
            validity: runtime_map.validity(),
            local_valid_until_monotonic_ms: bound_lease
                .map(BoundRouteMapLease::local_valid_until_monotonic_ms),
        });
        Ok(local_map)
    }

    pub(crate) fn open_runtime_map_with_existing_local_nodes(
        current: &Self,
        runtime_map: &ClusterRuntimeMapSnapshot,
    ) -> Result<Self, ClusterBuildError> {
        let mut local_map = Self::open_frontend_topology_only_with_runtime_map(
            current.metadata_primary_node_id,
            runtime_map,
            current.default_ec_shape,
        )?;
        let current_node_ids = current.nodes.keys().copied().collect::<Vec<_>>();
        let candidate_node_ids = local_map.nodes.keys().copied().collect::<Vec<_>>();
        if current_node_ids != candidate_node_ids {
            return Err(ClusterBuildError::HistoricalRecoveryNodeSetMismatch {
                current: current_node_ids.into_iter().map(NodeId::as_u32).collect(),
                candidate: candidate_node_ids.into_iter().map(NodeId::as_u32).collect(),
            });
        }
        local_map.nodes.clone_from(&current.nodes);
        local_map.bind_runtime_map_advertised_endpoints(runtime_map);
        local_map.inherit_process_local_state_from(current);
        Ok(local_map)
    }

    fn bind_runtime_map_advertised_endpoints(&mut self, runtime_map: &ClusterRuntimeMapSnapshot) {
        for route in runtime_map.nodes() {
            self.nodes
                .get_mut(&route.node_id())
                .expect("runtime-map node set was used to construct the local route map")
                .route_authority_advertised_endpoint = Some(route.endpoint().to_owned());
        }
    }

    pub(crate) fn inherit_process_local_state_from(&mut self, previous: &Self) {
        self.runtime_state = Arc::clone(&previous.runtime_state);
        self.process_local_registry_key = previous.process_local_registry_key;
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_clone_with_pg_routes(
        &self,
        cluster_epoch: ClusterEpoch,
        pg_routes: impl IntoIterator<Item = PgRouteSnapshot>,
        historical_pg_routes: impl IntoIterator<Item = PgRouteSnapshot>,
    ) -> Result<Self, ClusterBuildError> {
        let node_ids = self.nodes.keys().copied().collect::<BTreeSet<_>>();
        let pg_ids = self
            .pg_ids
            .iter()
            .map(|pg_id| PgId::new(*pg_id))
            .collect::<Vec<_>>();
        let pg_routes = build_validated_pg_routes(
            cluster_epoch,
            &node_ids,
            &pg_ids,
            pg_routes
                .into_iter()
                .map(|route| LocalPgRoute::from(&route)),
        )?;
        let historical_pg_routes: BTreeMap<_, _> = historical_pg_routes
            .into_iter()
            .map(|route| ((route.pg_id(), route.cluster_epoch()), route))
            .collect();
        let historical_cluster_epochs = historical_pg_routes
            .values()
            .map(PgRouteSnapshot::cluster_epoch)
            .collect();
        Ok(Self {
            epoch: cluster_epoch,
            route_map_lease: RwLock::new(LocalRouteMapLeaseSnapshot::unbound(
                RouteMapValidity::Forever,
            )),
            metadata_primary_node_id: self.metadata_primary_node_id,
            nodes: self.nodes.clone(),
            pg_ids: self.pg_ids.clone(),
            pg_topology: self.pg_topology.clone(),
            default_ec_shape: self.default_ec_shape,
            pg_routes,
            historical_pg_routes,
            historical_cluster_epochs,
            runtime_state: Arc::clone(&self.runtime_state),
            process_local_registry_key: self.process_local_registry_key,
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_clone_with_route_map_validity(&self, validity: RouteMapValidity) -> Self {
        Self {
            epoch: self.epoch,
            route_map_lease: RwLock::new(LocalRouteMapLeaseSnapshot {
                validity,
                local_valid_until_monotonic_ms: test_process_local_route_map_deadline(validity),
            }),
            metadata_primary_node_id: self.metadata_primary_node_id,
            nodes: self.nodes.clone(),
            pg_ids: self.pg_ids.clone(),
            pg_topology: self.pg_topology.clone(),
            default_ec_shape: self.default_ec_shape,
            pg_routes: self.pg_routes.clone(),
            historical_pg_routes: self.historical_pg_routes.clone(),
            historical_cluster_epochs: self.historical_cluster_epochs.clone(),
            runtime_state: Arc::clone(&self.runtime_state),
            process_local_registry_key: self.process_local_registry_key,
        }
    }

    fn open_with_configs_inner(
        metadata_primary_node_id: NodeId,
        configs: impl IntoIterator<Item = LocalNodeStoreConfig>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
        pg_routes: Option<Vec<LocalPgRoute>>,
        validate_local_metadata_command_replay: bool,
    ) -> Result<Self, ClusterBuildError> {
        let ValidatedEmbeddedLocalTopology {
            node_ids,
            pg_ids,
            configs: validated_configs,
        } = validate_embedded_local_topology(
            metadata_primary_node_id,
            configs,
            pg_ids,
            default_ec_shape,
        )?;

        let acting_set = Arc::<[NodeId]>::from(node_ids.iter().copied().collect::<Vec<_>>());
        let storage_pg_ids: Vec<u32> = pg_ids.iter().map(|pg_id| pg_id.get()).collect();
        let pg_topology = PgTopology::new(&storage_pg_ids).map_err(|reason| {
            ClusterBuildError::InvalidLocalPlacement {
                reason: reason.to_string(),
            }
        })?;
        let pg_routes = if let Some(pg_routes) = pg_routes {
            build_validated_pg_routes(cluster_epoch, &node_ids, &pg_ids, pg_routes)?
        } else {
            build_static_pg_routes(
                cluster_epoch,
                metadata_primary_node_id,
                Arc::clone(&acting_set),
                &pg_ids,
            )
        };

        let mut nodes = BTreeMap::new();
        for (node_id, canonical_data_dir) in validated_configs {
            let runtime = LocalNodeRuntime::open(
                node_id,
                &canonical_data_dir,
                &storage_pg_ids,
                default_ec_shape,
                cluster_epoch,
            )
            .map_err(|source| ClusterBuildError::open_local_node(node_id.as_u32(), source))?;
            nodes.insert(
                node_id,
                LocalNodeStore::new(node_id, canonical_data_dir, runtime),
            );
        }
        // Classify older pending commands with exact acting-set evidence before
        // touching either their resources or slots. A local replica cannot tell
        // whether an old command was unpublished or converged elsewhere.
        if validate_local_metadata_command_replay {
            let mut orphan_slots = Vec::new();
            let mut orphan_commands =
                BTreeMap::<(u64, u32, u64), (MetadataCommandEnvelope, bool)>::new();
            for (node_id, store) in &nodes {
                let orphans = store
                    .runtime()
                    .epoch_mismatched_pending_metadata_commands(*node_id)
                    .map_err(|source| {
                        ClusterBuildError::open_local_node(node_id.as_u32(), source)
                    })?;
                for (pg_id, pending) in orphans {
                    let command = pending.command;
                    let id = command.id();
                    let key = (
                        id.cluster_epoch().get(),
                        id.pg_id().get(),
                        id.log_index().get(),
                    );
                    if let Some((existing, publication_started)) = orphan_commands.get_mut(&key) {
                        if existing.checksum_crc64() != command.checksum_crc64()
                            || existing.command_bytes() != command.command_bytes()
                        {
                            return Err(ClusterBuildError::open_local_node(
                                node_id.as_u32(),
                                StoreError::MetadataCommandLogConflict {
                                    node_id: node_id.as_u32(),
                                    pg_id: pg_id.get(),
                                    cluster_epoch: id.cluster_epoch(),
                                    log_index: id.log_index().get(),
                                },
                            ));
                        }
                        *publication_started |= pending.publication_started;
                    } else {
                        orphan_commands.insert(key, (command.clone(), pending.publication_started));
                    }
                    orphan_slots.push((*node_id, pg_id, command));
                }
            }
            for (command, publication_started) in orphan_commands.values() {
                let route = pg_routes
                    .get(&command.id().pg_id())
                    .expect("validated command PG id should have a route");
                let mut terminal_evidence = None;
                let mut applied = 0usize;
                for actor in route.acting_set() {
                    let node = nodes
                        .get(actor)
                        .expect("validated acting-set node should be opened");
                    let disposition = node
                        .runtime()
                        .metadata_command_startup_disposition(*actor, command)
                        .map_err(|source| {
                            ClusterBuildError::open_local_node(actor.as_u32(), source)
                        })?;
                    let evidence = match disposition {
                        MetadataCommandStartupDisposition::Absent => None,
                        MetadataCommandStartupDisposition::Abandoned {
                            previous_log_hash,
                            log_hash,
                        } => Some((true, previous_log_hash, log_hash)),
                        MetadataCommandStartupDisposition::Applied {
                            previous_log_hash,
                            log_hash,
                        } => {
                            applied += 1;
                            Some((false, previous_log_hash, log_hash))
                        }
                    };
                    if let Some(evidence) = evidence {
                        if terminal_evidence.is_some_and(|expected| expected != evidence) {
                            return Err(ClusterBuildError::open_local_node(
                                actor.as_u32(),
                                StoreError::MetadataCommandLogConflict {
                                    node_id: actor.as_u32(),
                                    pg_id: command.id().pg_id().get(),
                                    cluster_epoch: command.id().cluster_epoch(),
                                    log_index: command.id().log_index().get(),
                                },
                            ));
                        }
                        terminal_evidence = Some(evidence);
                    }
                }
                if applied == route.acting_set().len() {
                    release_open_applied_metadata_command_bucket_write_reservations(
                        &nodes, &pg_routes, command,
                    )?;
                } else if applied == 0 && !publication_started {
                    release_open_unpublished_metadata_command_bucket_write_reservation(
                        &nodes, &pg_routes, command,
                    )?;
                } else {
                    let primary = route.primary_node_id();
                    return Err(ClusterBuildError::open_local_node(
                        primary.as_u32(),
                        StoreError::MetadataCommandLogConflict {
                            node_id: primary.as_u32(),
                            pg_id: command.id().pg_id().get(),
                            cluster_epoch: command.id().cluster_epoch(),
                            log_index: command.id().log_index().get(),
                        },
                    ));
                }
            }
            for (node_id, pg_id, command) in orphan_slots {
                let store = nodes
                    .get(&node_id)
                    .expect("orphan slot owner came from the opened local node map");
                let removed = store
                    .runtime()
                    .remove_epoch_mismatched_orphan_pending_metadata_command(
                        node_id, pg_id, &command,
                    )
                    .map_err(|source| {
                        ClusterBuildError::open_local_node(node_id.as_u32(), source)
                    })?;
                if !removed {
                    return Err(ClusterBuildError::open_local_node(
                        node_id.as_u32(),
                        StoreError::MetadataCommandLogConflict {
                            node_id: node_id.as_u32(),
                            pg_id: pg_id.get(),
                            cluster_epoch: command.id().cluster_epoch(),
                            log_index: command.id().log_index().get(),
                        },
                    ));
                }
            }
        }
        if validate_local_metadata_command_replay {
            validate_metadata_command_replay_state(&nodes, &pg_routes, &pg_ids, cluster_epoch)?;
        }
        // Recovery full pass: now that convergence has completed and same-epoch
        // pending slots have been used, reconcile any remaining terminal and
        // orphan slots on every node (including replicas the cluster-wide pass
        // does not clean) and fail closed on command-log or digest corruption
        // before the cluster is returned.
        if validate_local_metadata_command_replay {
            for (node_id, store) in &nodes {
                store
                    .runtime()
                    .recover_metadata_command_state(*node_id)
                    .map_err(|source| {
                        ClusterBuildError::open_local_node(node_id.as_u32(), source)
                    })?;
            }
        }

        let metadata_primary = nodes
            .get(&metadata_primary_node_id)
            .expect("validated metadata primary should have been opened");

        Ok(Self {
            epoch: cluster_epoch,
            metadata_primary_node_id,
            pg_ids: storage_pg_ids.into_boxed_slice(),
            pg_topology,
            default_ec_shape,
            pg_routes,
            historical_pg_routes: BTreeMap::new(),
            historical_cluster_epochs: BTreeSet::new(),
            route_map_lease: RwLock::new(LocalRouteMapLeaseSnapshot::unbound(
                RouteMapValidity::Forever,
            )),
            runtime_state: Arc::new(LocalClusterRuntimeState::new()),
            process_local_registry_key: metadata_primary.runtime().process_local_registry_key(),
            nodes,
        })
    }

    pub fn epoch(&self) -> ClusterEpoch {
        self.epoch
    }

    pub(super) fn static_route_map_content_digest(&self) -> [u8; 32] {
        let endpoints = self
            .nodes
            .iter()
            .map(|(node_id, node)| (*node_id, node.route_execution_endpoint.clone()))
            .collect::<BTreeMap<_, _>>();
        let current_routes = self
            .pg_routes
            .values()
            .map(|route| {
                PgRouteSnapshot::reconstructed(
                    route.cluster_epoch(),
                    route.pg_id(),
                    route.primary_node_id(),
                    route.acting_set().to_vec(),
                    route.state(),
                )
            })
            .collect::<Vec<_>>();
        let historical_routes = self
            .historical_pg_routes
            .values()
            .cloned()
            .collect::<Vec<_>>();
        static_route_map_content_digest(StaticRouteMapDigestInput {
            cluster_epoch: self.epoch,
            metadata_primary_node_id: self.metadata_primary_node_id,
            default_ec_shape: self.default_ec_shape,
            endpoints: &endpoints,
            pg_ids: &self.pg_ids,
            current_routes: &current_routes,
            historical_routes: &historical_routes,
            historical_cluster_epochs: &self.historical_cluster_epochs,
        })
    }

    pub(super) fn preflight_static_embedded_route_digest(
        metadata_primary_node_id: NodeId,
        configs: impl IntoIterator<Item = LocalNodeStoreConfig>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
    ) -> Result<[u8; 32], ClusterBuildError> {
        let ValidatedEmbeddedLocalTopology {
            node_ids,
            pg_ids,
            configs,
        } = validate_embedded_local_topology(
            metadata_primary_node_id,
            configs,
            pg_ids,
            default_ec_shape,
        )?;
        let acting_set = Arc::<[NodeId]>::from(node_ids.iter().copied().collect::<Vec<_>>());
        let current_routes =
            build_static_pg_routes(cluster_epoch, metadata_primary_node_id, acting_set, &pg_ids)
                .into_values()
                .map(|route| {
                    PgRouteSnapshot::reconstructed(
                        route.cluster_epoch(),
                        route.pg_id(),
                        route.primary_node_id(),
                        route.acting_set().to_vec(),
                        route.state(),
                    )
                })
                .collect::<Vec<_>>();
        let endpoints = configs
            .into_iter()
            .map(|(node_id, data_dir)| (node_id, LocalRouteExecutionEndpoint::Embedded(data_dir)))
            .collect::<BTreeMap<_, _>>();
        let raw_pg_ids = pg_ids.iter().map(|pg_id| pg_id.get()).collect::<Vec<_>>();
        Ok(static_route_map_content_digest(StaticRouteMapDigestInput {
            cluster_epoch,
            metadata_primary_node_id,
            default_ec_shape,
            endpoints: &endpoints,
            pg_ids: &raw_pg_ids,
            current_routes: &current_routes,
            historical_routes: &[],
            historical_cluster_epochs: &BTreeSet::new(),
        }))
    }

    pub(super) fn validate_dynamic_route_map_content(
        &self,
        runtime_map: &ClusterRuntimeMapSnapshot,
    ) -> Result<(), ClusterBuildError> {
        let local_node_ids = self.nodes.keys().copied().collect::<Vec<_>>();
        let authority_node_ids = runtime_map
            .nodes()
            .iter()
            .map(NodeRouteSnapshot::node_id)
            .collect::<Vec<_>>();
        if local_node_ids != authority_node_ids {
            return Err(ClusterBuildError::DynamicRouteAuthorityNodeSetMismatch);
        }
        for authority_node in runtime_map.nodes() {
            let local_node = self
                .nodes
                .get(&authority_node.node_id())
                .expect("equal node sets must contain the authority node");
            if local_node.route_authority_advertised_endpoint.as_deref()
                != Some(authority_node.endpoint())
            {
                return Err(
                    ClusterBuildError::DynamicRouteAuthorityNodeEndpointMismatch {
                        id: authority_node.node_id().as_u32(),
                    },
                );
            }
        }

        if self.pg_routes.len() != runtime_map.pg_routes().len()
            || runtime_map.pg_routes().iter().any(|authority_route| {
                self.pg_routes
                    .get(&authority_route.pg_id())
                    .is_none_or(|local_route| {
                        local_route.cluster_epoch() != authority_route.cluster_epoch()
                            || local_route.primary_node_id() != authority_route.primary_node_id()
                            || local_route.acting_set() != authority_route.acting_set()
                            || local_route.state() != authority_route.state()
                    })
            })
        {
            return Err(ClusterBuildError::DynamicRouteAuthorityPgRoutesMismatch);
        }

        if self.historical_pg_routes.len() != runtime_map.historical_pg_routes().len()
            || runtime_map
                .historical_pg_routes()
                .iter()
                .any(|authority_route| {
                    self.historical_pg_routes
                        .get(&(authority_route.pg_id(), authority_route.cluster_epoch()))
                        != Some(authority_route)
                })
        {
            return Err(ClusterBuildError::DynamicRouteAuthorityHistoricalPgRoutesMismatch);
        }
        if !self
            .historical_cluster_epochs
            .iter()
            .copied()
            .eq(runtime_map.historical_cluster_epochs().iter().copied())
        {
            return Err(ClusterBuildError::DynamicRouteAuthorityHistoricalEpochsMismatch);
        }
        Ok(())
    }

    pub(super) fn validate_installed_storage_rpc_clients(
        &self,
        runtime_map: &ClusterRuntimeMapSnapshot,
    ) -> Result<(), ClusterBuildError> {
        for authority_node in runtime_map.nodes() {
            let local_node = self
                .nodes
                .get(&authority_node.node_id())
                .ok_or(ClusterBuildError::DynamicRouteAuthorityNodeSetMismatch)?;
            if matches!(
                local_node.route_execution_endpoint,
                LocalRouteExecutionEndpoint::Embedded(_)
                    | LocalRouteExecutionEndpoint::TopologyOnly
            ) {
                return Err(ClusterBuildError::RuntimeMapStorageNodeClientMissing {
                    id: authority_node.node_id().as_u32(),
                });
            }
            if !local_node
                .route_execution_endpoint
                .matches_advertised_endpoint(authority_node.endpoint())
            {
                return Err(
                    ClusterBuildError::RemoteStorageNodeClientEndpointAuthorityMismatch {
                        id: authority_node.node_id().as_u32(),
                    },
                );
            }
        }
        Ok(())
    }

    pub fn route_map_valid_until_ms(&self) -> Option<u64> {
        self.route_map_lease_snapshot().validity.valid_until_ms()
    }

    pub fn route_map_validity(&self) -> RouteMapValidity {
        self.route_map_lease_snapshot().validity
    }

    pub(crate) fn route_map_lease_snapshot(&self) -> LocalRouteMapLeaseSnapshot {
        *self
            .route_map_lease
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(test)]
    pub(crate) fn route_map_lease_snapshot_with_hook<F>(
        &self,
        after_lock: F,
    ) -> LocalRouteMapLeaseSnapshot
    where
        F: FnOnce(),
    {
        let lease = self
            .route_map_lease
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        after_lock();
        *lease
    }

    #[cfg(test)]
    pub(crate) fn test_try_replace_route_map_lease_from(&self, candidate: &Self) -> bool {
        let candidate = candidate.route_map_lease_snapshot();
        match self.route_map_lease.try_write() {
            Ok(mut lease) => {
                *lease = candidate;
                true
            }
            Err(std::sync::TryLockError::WouldBlock) => false,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                *poisoned.into_inner() = candidate;
                true
            }
        }
    }

    pub(crate) fn replace_route_map_lease_from(&self, candidate: &Self) {
        self.replace_route_map_lease_snapshot(candidate.route_map_lease_snapshot());
    }

    pub(crate) fn replace_route_map_lease(
        &self,
        validity: RouteMapValidity,
        bound_lease: Option<BoundRouteMapLease>,
    ) {
        self.replace_route_map_lease_snapshot(LocalRouteMapLeaseSnapshot {
            validity,
            local_valid_until_monotonic_ms: bound_lease
                .map(BoundRouteMapLease::local_valid_until_monotonic_ms),
        });
    }

    fn replace_route_map_lease_snapshot(&self, candidate: LocalRouteMapLeaseSnapshot) {
        *self
            .route_map_lease
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = candidate;
    }

    pub(crate) fn expire_route_map_lease_at(
        &self,
        validity: RouteMapValidity,
        local_monotonic_ms: u64,
    ) {
        let mut lease = self
            .route_map_lease
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if lease.validity.valid_until_ms().unwrap_or(u64::MAX)
            > validity.valid_until_ms().unwrap_or(u64::MAX)
        {
            lease.validity = validity;
        }
        lease.local_valid_until_monotonic_ms = Some(
            lease
                .local_valid_until_monotonic_ms
                .map_or(local_monotonic_ms, |deadline| {
                    deadline.min(local_monotonic_ms)
                }),
        );
    }

    pub fn is_route_map_valid_at(&self, now_ms: u64) -> bool {
        self.route_map_validity().is_valid_at(now_ms)
    }

    #[cfg(test)]
    pub(crate) fn require_route_map_valid_at(&self, now_ms: u64) -> Result<(), StoreError> {
        match self.route_map_valid_until_ms() {
            Some(valid_until_ms) if valid_until_ms <= now_ms => Err(StoreError::RouteMapExpired {
                cluster_epoch: self.epoch,
                valid_until_ms,
                now_ms,
            }),
            _ => Ok(()),
        }
    }

    pub(crate) fn require_route_map_valid_now(&self) -> Result<(), StoreError> {
        let local_monotonic_ms = crate::clock::monotonic_time_millis();
        let lease = self.route_map_lease_snapshot();
        if !self.route_map_lease_snapshot_is_valid_at(lease, local_monotonic_ms) {
            return Err(StoreError::RouteMapExpired {
                cluster_epoch: self.epoch,
                valid_until_ms: lease.validity.valid_until_ms().unwrap_or(0),
                now_ms: crate::clock::current_time_millis(),
            });
        }
        Ok(())
    }

    fn require_route_map_valid_now_for_placement(
        &self,
        pg_id: PgId,
    ) -> Result<(), ClusterBuildError> {
        let local_monotonic_ms = crate::clock::monotonic_time_millis();
        let lease = self.route_map_lease_snapshot();
        match self.route_map_lease_snapshot_is_valid_at(lease, local_monotonic_ms) {
            false => Err(ClusterBuildError::RouteMapExpired {
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
                valid_until_ms: lease.validity.valid_until_ms().unwrap_or(0),
                now_ms: crate::clock::current_time_millis(),
            }),
            true => Ok(()),
        }
    }

    fn require_route_map_valid_now_for_shard_io(
        &self,
        pg_id: PgId,
        node_id: NodeId,
    ) -> Result<(), ShardIoError> {
        let local_monotonic_ms = crate::clock::monotonic_time_millis();
        let lease = self.route_map_lease_snapshot();
        match self.route_map_lease_snapshot_is_valid_at(lease, local_monotonic_ms) {
            false => Err(ShardIoError::RouteMapExpired {
                node_id: node_id.as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
                valid_until_ms: lease.validity.valid_until_ms().unwrap_or(0),
                now_ms: crate::clock::current_time_millis(),
            }),
            true => Ok(()),
        }
    }

    pub(crate) fn route_map_lease_snapshot_is_valid_at(
        &self,
        lease: LocalRouteMapLeaseSnapshot,
        local_monotonic_ms: u64,
    ) -> bool {
        let Some(deadline_ms) = lease.local_valid_until_monotonic_ms else {
            return true;
        };
        if validate_process_lease_clock(
            crate::clock::current_time_millis(),
            crate::clock::clock_health_time_millis(),
            CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        )
        .is_err()
        {
            self.expire_route_map_lease_at(
                RouteMapValidity::until_ms_saturating(crate::clock::current_time_millis()),
                local_monotonic_ms,
            );
            return false;
        }
        deadline_ms > local_monotonic_ms
    }

    pub fn metadata_primary_node_id(&self) -> NodeId {
        self.metadata_primary_node_id
    }

    pub(crate) fn metadata_primary(&self) -> &LocalNodeStore {
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

    pub fn pg_ids(&self) -> &[u32] {
        &self.pg_ids
    }

    pub(crate) fn pg_topology(&self) -> &PgTopology {
        &self.pg_topology
    }

    pub(crate) fn bucket_metadata_pg_for(&self, bucket: &BucketName) -> BucketPgId {
        self.metadata_primary()
            .runtime()
            .bucket_metadata_pg_for(bucket)
    }

    pub(crate) fn bucket_metadata_pg(&self, pg_id: PgId) -> Option<BucketPgId> {
        self.metadata_primary().runtime().bucket_metadata_pg(pg_id)
    }

    pub(crate) fn object_metadata_pg_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> ObjectMetadataPgId {
        self.metadata_primary()
            .runtime()
            .object_metadata_pg_for(bucket, key)
    }

    pub(crate) fn object_metadata_scan_pg(&self, pg_id: PgId) -> Option<ObjectMetadataScanPgId> {
        self.metadata_primary()
            .runtime()
            .object_metadata_scan_pg(pg_id)
    }

    pub(crate) fn data_pg(&self, pg_id: PgId) -> Option<DataPgId> {
        self.metadata_primary().runtime().data_pg(pg_id)
    }

    pub(crate) fn node(&self, node_id: NodeId) -> Option<&LocalNodeStore> {
        self.nodes.get(&node_id)
    }

    #[cfg(test)]
    pub(crate) fn cluster_map_history_reference_summary(
        &self,
    ) -> Result<PgClusterMapHistoryReferenceSummary, StoreError> {
        Ok(self.cluster_map_history_route_references()?.summary())
    }

    #[cfg(test)]
    pub(crate) fn cluster_map_history_route_references(
        &self,
    ) -> Result<crate::PgClusterMapHistoryRouteReferences, StoreError> {
        let mut references = crate::PgClusterMapHistoryRouteReferences::default();
        for node in self.nodes.values() {
            references.merge(
                node.shard_scavenger_client()
                    .cluster_map_history_route_references()?,
            )?;
        }
        Ok(references)
    }

    pub fn install_unix_shard_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixShardNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixShardNodeClientConfig> = configs.into_iter().collect();
        let mut seen = BTreeSet::<NodeId>::new();
        for config in &configs {
            if !seen.insert(config.node_id) {
                return Err(ClusterBuildError::DuplicateRemoteShardClientNodeId {
                    id: config.node_id.as_u32(),
                });
            }
            if !config.socket_path.is_absolute() {
                return Err(ClusterBuildError::RemoteShardClientSocketPathNotAbsolute {
                    path: config.socket_path.clone(),
                });
            }
            if !self.nodes.contains_key(&config.node_id) {
                return Err(ClusterBuildError::RemoteShardClientNodeNotFound {
                    id: config.node_id.as_u32(),
                });
            }
        }
        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote shard client node must exist");
            let client = Arc::new(UnixStorageNodeClient::new(
                config.node_id,
                self.epoch,
                config.socket_path,
            ));
            let shard_client: Arc<dyn PlacedShardNodeClient> = client.clone();
            let retained_shard_client: Arc<dyn RetainedPlacedShardNodeClient> = client.clone();
            let shard_ack_client: Arc<dyn ShardAckNodeClient> = client.clone();
            let retained_shard_ack_client: Arc<dyn RetainedShardAckNodeClient> = client.clone();
            let shard_read_handle_client: Arc<dyn ShardReadHandleNodeClient> = client.clone();
            let shard_scavenger_client: Arc<dyn ShardScavengerNodeClient> = client.clone();
            let shard_scavenger_observation_client: Arc<dyn ShardScavengerObservationNodeClient> =
                client;
            node.shard_client = shard_client;
            node.retained_shard_client = retained_shard_client;
            node.shard_ack_client = shard_ack_client;
            node.retained_shard_ack_client = retained_shard_ack_client;
            node.shard_read_handle_client = shard_read_handle_client;
            node.shard_scavenger_client = shard_scavenger_client;
            node.shard_scavenger_observation_client = shard_scavenger_observation_client;
        }
        Ok(())
    }

    pub fn install_unix_storage_node_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixStorageNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixStorageNodeClientConfig> = configs.into_iter().collect();
        self.validate_unix_storage_node_client_configs(&configs)?;
        let pg_topology = Arc::new(self.pg_topology.clone());

        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote storage-node client node must exist");
            let route_execution_endpoint = LocalRouteExecutionEndpoint::for_rpc(&config.endpoint);
            let unix_socket_path = config.endpoint.unix_socket_path().map(Path::to_path_buf);
            let client = Arc::new(
                UnixStorageNodeClient::with_endpoint_rpc_admission_settings_and_auth(
                    config.node_id,
                    self.epoch,
                    config.endpoint,
                    LocalUnixStorageNodeClientAdmissionSettings {
                        rpc_admission_limit: config.rpc_admission_limit,
                        rpc_admission_wait_timeout: config.rpc_admission_wait_timeout,
                        rpc_control_admission_wait_timeout: config
                            .rpc_control_admission_wait_timeout,
                    },
                    config.rpc_auth,
                )
                .with_pg_topology(Arc::clone(&pg_topology)),
            );
            let bucket_metadata_client: Arc<dyn BucketMetadataNodeClient> = client.clone();
            let bucket_write_reservation_client: Arc<dyn BucketWriteReservationNodeClient> =
                client.clone();
            let retained_bucket_write_reservation_client: Arc<
                dyn RetainedBucketWriteReservationNodeClient,
            > = client.clone();
            let metadata_command_client: Arc<dyn MetadataCommandNodeClient> = client.clone();
            let metadata_command_inspection_client: Arc<dyn MetadataCommandInspectionNodeClient> =
                client.clone();
            let metadata_command_peering_client: Arc<dyn MetadataCommandPeeringNodeClient> =
                client.clone();
            let metadata_command_recovery_client: Arc<dyn MetadataCommandRecoveryNodeClient> =
                client.clone();
            let retained_metadata_command_client: Arc<dyn RetainedMetadataCommandNodeClient> =
                client.clone();
            let object_generation_metadata_client: Arc<dyn ObjectGenerationMetadataNodeClient> =
                client.clone();
            let object_version_metadata_client: Arc<dyn ObjectVersionMetadataNodeClient> =
                client.clone();
            let direct_put_metadata_client: Arc<dyn DirectPutMetadataNodeClient> = client.clone();
            let object_listing_metadata_client: Arc<dyn ObjectListingMetadataNodeClient> =
                client.clone();
            let object_mutation_metadata_client: Arc<dyn ObjectMutationMetadataNodeClient> =
                client.clone();
            let retained_object_mutation_metadata_client: Arc<
                dyn RetainedObjectMutationMetadataNodeClient,
            > = client.clone();
            let object_read_metadata_client: Arc<dyn ObjectReadMetadataNodeClient> = client.clone();
            let object_payload_lease_client: Arc<dyn ObjectPayloadLeaseNodeClient> = client.clone();
            let retained_object_payload_reclaim_client: Arc<
                dyn RetainedObjectPayloadReclaimNodeClient,
            > = client.clone();
            let shard_client: Arc<dyn PlacedShardNodeClient> = client.clone();
            let retained_shard_client: Arc<dyn RetainedPlacedShardNodeClient> = client.clone();
            let shard_ack_client: Arc<dyn ShardAckNodeClient> = client.clone();
            let retained_shard_ack_client: Arc<dyn RetainedShardAckNodeClient> = client.clone();
            let shard_read_handle_client: Arc<dyn ShardReadHandleNodeClient> = client.clone();
            let shard_scavenger_client: Arc<dyn ShardScavengerNodeClient> = client.clone();
            let shard_scavenger_observation_client: Arc<dyn ShardScavengerObservationNodeClient> =
                client;

            node.bucket_metadata_client = bucket_metadata_client;
            node.route_execution_endpoint = route_execution_endpoint;
            node.bucket_metadata_unix_socket_path = unix_socket_path.clone();
            node.bucket_write_reservation_client = bucket_write_reservation_client;
            node.retained_bucket_write_reservation_client =
                retained_bucket_write_reservation_client;
            node.bucket_write_reservation_unix_socket_path = unix_socket_path;
            node.metadata_command_client = metadata_command_client;
            node.metadata_command_inspection_client = metadata_command_inspection_client;
            node.metadata_command_peering_client = metadata_command_peering_client;
            node.metadata_command_recovery_client = metadata_command_recovery_client;
            node.retained_metadata_command_client = retained_metadata_command_client;
            node.object_generation_metadata_client = object_generation_metadata_client;
            node.object_version_metadata_client = object_version_metadata_client;
            node.direct_put_metadata_client = direct_put_metadata_client;
            node.object_listing_metadata_client = object_listing_metadata_client;
            node.object_mutation_metadata_client = object_mutation_metadata_client;
            node.retained_object_mutation_metadata_client =
                retained_object_mutation_metadata_client;
            node.object_read_metadata_client = object_read_metadata_client;
            node.object_payload_lease_client = object_payload_lease_client;
            node.retained_object_payload_reclaim_client = retained_object_payload_reclaim_client;
            node.shard_client = shard_client;
            node.retained_shard_client = retained_shard_client;
            node.shard_ack_client = shard_ack_client;
            node.retained_shard_ack_client = retained_shard_ack_client;
            node.shard_read_handle_client = shard_read_handle_client;
            node.shard_scavenger_client = shard_scavenger_client;
            node.shard_scavenger_observation_client = shard_scavenger_observation_client;
        }
        Ok(())
    }

    fn validate_unix_storage_node_client_configs(
        &self,
        configs: &[LocalUnixStorageNodeClientConfig],
    ) -> Result<(), ClusterBuildError> {
        let mut seen = BTreeSet::<NodeId>::new();
        for config in configs {
            if !seen.insert(config.node_id) {
                return Err(ClusterBuildError::DuplicateRemoteStorageNodeClientNodeId {
                    id: config.node_id.as_u32(),
                });
            }
            if let Some(socket_path) = config.endpoint.unix_socket_path() {
                if !socket_path.is_absolute() {
                    return Err(
                        ClusterBuildError::RemoteStorageNodeClientSocketPathNotAbsolute {
                            path: socket_path.to_path_buf(),
                        },
                    );
                }
            }
            let node = self.nodes.get(&config.node_id).ok_or(
                ClusterBuildError::RemoteStorageNodeClientNodeNotFound {
                    id: config.node_id.as_u32(),
                },
            )?;
            if node
                .route_authority_advertised_endpoint
                .as_deref()
                .is_some_and(|advertised_endpoint| {
                    !LocalRouteExecutionEndpoint::for_rpc(&config.endpoint)
                        .matches_advertised_endpoint(advertised_endpoint)
                })
            {
                return Err(
                    ClusterBuildError::RemoteStorageNodeClientEndpointAuthorityMismatch {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if config.rpc_admission_limit == 0 {
                return Err(
                    ClusterBuildError::RemoteStorageNodeClientRpcAdmissionLimitZero {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if config.rpc_admission_wait_timeout.is_zero() {
                return Err(
                    ClusterBuildError::RemoteStorageNodeClientRpcAdmissionWaitTimeoutZero {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if config.rpc_control_admission_wait_timeout.is_zero() {
                return Err(
                    ClusterBuildError::RemoteStorageNodeClientRpcControlAdmissionWaitTimeoutZero {
                        id: config.node_id.as_u32(),
                    },
                );
            }
        }
        Ok(())
    }

    pub fn install_unix_metadata_command_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixMetadataCommandNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixMetadataCommandNodeClientConfig> = configs.into_iter().collect();
        let mut seen = BTreeSet::<NodeId>::new();
        for config in &configs {
            if !seen.insert(config.node_id) {
                return Err(
                    ClusterBuildError::DuplicateRemoteMetadataCommandClientNodeId {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if !config.socket_path.is_absolute() {
                return Err(
                    ClusterBuildError::RemoteMetadataCommandClientSocketPathNotAbsolute {
                        path: config.socket_path.clone(),
                    },
                );
            }
            if !self.nodes.contains_key(&config.node_id) {
                return Err(ClusterBuildError::RemoteMetadataCommandClientNodeNotFound {
                    id: config.node_id.as_u32(),
                });
            }
        }
        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote metadata-command client node must exist");
            let client = Arc::new(UnixStorageNodeClient::new(
                config.node_id,
                self.epoch,
                config.socket_path,
            ));
            let metadata_command_client: Arc<dyn MetadataCommandNodeClient> = client.clone();
            let metadata_command_inspection_client: Arc<dyn MetadataCommandInspectionNodeClient> =
                client.clone();
            let metadata_command_peering_client: Arc<dyn MetadataCommandPeeringNodeClient> =
                client.clone();
            let metadata_command_recovery_client: Arc<dyn MetadataCommandRecoveryNodeClient> =
                client.clone();
            let retained_metadata_command_client: Arc<dyn RetainedMetadataCommandNodeClient> =
                client;
            node.metadata_command_client = metadata_command_client;
            node.metadata_command_inspection_client = metadata_command_inspection_client;
            node.metadata_command_peering_client = metadata_command_peering_client;
            node.metadata_command_recovery_client = metadata_command_recovery_client;
            node.retained_metadata_command_client = retained_metadata_command_client;
        }
        Ok(())
    }

    pub fn install_unix_bucket_metadata_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixBucketMetadataNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixBucketMetadataNodeClientConfig> = configs.into_iter().collect();
        let pg_topology = Arc::new(self.pg_topology.clone());
        let mut seen = BTreeSet::<NodeId>::new();
        for config in &configs {
            if !seen.insert(config.node_id) {
                return Err(
                    ClusterBuildError::DuplicateRemoteBucketMetadataClientNodeId {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if !config.socket_path.is_absolute() {
                return Err(
                    ClusterBuildError::RemoteBucketMetadataClientSocketPathNotAbsolute {
                        path: config.socket_path.clone(),
                    },
                );
            }
            if !self.nodes.contains_key(&config.node_id) {
                return Err(ClusterBuildError::RemoteBucketMetadataClientNodeNotFound {
                    id: config.node_id.as_u32(),
                });
            }
            let node = self
                .nodes
                .get(&config.node_id)
                .expect("validated remote bucket metadata client node must exist");
            if let Some(bucket_write_reservation_socket_path) =
                node.bucket_write_reservation_unix_socket_path.as_deref()
            {
                if bucket_write_reservation_socket_path != config.socket_path.as_path() {
                    return Err(
                        ClusterBuildError::RemoteBucketMetadataClientMismatchedBucketWriteReservationClient {
                            id: config.node_id.as_u32(),
                            bucket_metadata_socket_path: config.socket_path.clone(),
                            bucket_write_reservation_socket_path:
                                bucket_write_reservation_socket_path.to_path_buf(),
                        },
                    );
                }
            }
        }
        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote bucket metadata client node must exist");
            let client = Arc::new(
                UnixStorageNodeClient::new(config.node_id, self.epoch, config.socket_path.clone())
                    .with_pg_topology(Arc::clone(&pg_topology)),
            );
            let bucket_metadata_client: Arc<dyn BucketMetadataNodeClient> = client;
            node.bucket_metadata_client = bucket_metadata_client;
            node.bucket_metadata_unix_socket_path = Some(config.socket_path);
        }
        Ok(())
    }

    pub fn install_unix_bucket_write_reservation_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixBucketWriteReservationNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixBucketWriteReservationNodeClientConfig> =
            configs.into_iter().collect();
        let pg_topology = Arc::new(self.pg_topology.clone());
        let mut seen = BTreeSet::<NodeId>::new();
        for config in &configs {
            if !seen.insert(config.node_id) {
                return Err(
                    ClusterBuildError::DuplicateRemoteBucketWriteReservationClientNodeId {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if !config.socket_path.is_absolute() {
                return Err(
                    ClusterBuildError::RemoteBucketWriteReservationClientSocketPathNotAbsolute {
                        path: config.socket_path.clone(),
                    },
                );
            }
            if !self.nodes.contains_key(&config.node_id) {
                return Err(
                    ClusterBuildError::RemoteBucketWriteReservationClientNodeNotFound {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            let node = self
                .nodes
                .get(&config.node_id)
                .expect("validated remote bucket write reservation client node must exist");
            match node.bucket_metadata_unix_socket_path.as_deref() {
                Some(bucket_metadata_socket_path)
                    if bucket_metadata_socket_path == config.socket_path.as_path() => {}
                Some(bucket_metadata_socket_path) => {
                    return Err(
                        ClusterBuildError::RemoteBucketWriteReservationClientMismatchedBucketMetadataClient {
                            id: config.node_id.as_u32(),
                            bucket_metadata_socket_path: bucket_metadata_socket_path.to_path_buf(),
                            bucket_write_reservation_socket_path: config.socket_path.clone(),
                        },
                    );
                }
                None => {
                    return Err(
                        ClusterBuildError::RemoteBucketWriteReservationClientMissingBucketMetadataClient {
                            id: config.node_id.as_u32(),
                        },
                    );
                }
            }
        }
        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote bucket write reservation client node must exist");
            let client = Arc::new(
                UnixStorageNodeClient::new(config.node_id, self.epoch, config.socket_path.clone())
                    .with_pg_topology(Arc::clone(&pg_topology)),
            );
            let bucket_write_reservation_client: Arc<dyn BucketWriteReservationNodeClient> =
                client.clone();
            let retained_bucket_write_reservation_client: Arc<
                dyn RetainedBucketWriteReservationNodeClient,
            > = client;
            node.bucket_write_reservation_client = bucket_write_reservation_client;
            node.retained_bucket_write_reservation_client =
                retained_bucket_write_reservation_client;
            node.bucket_write_reservation_unix_socket_path = Some(config.socket_path);
        }
        Ok(())
    }

    pub fn install_unix_object_generation_metadata_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixObjectGenerationMetadataNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixObjectGenerationMetadataNodeClientConfig> =
            configs.into_iter().collect();
        let mut seen = BTreeSet::<NodeId>::new();
        for config in &configs {
            if !seen.insert(config.node_id) {
                return Err(
                    ClusterBuildError::DuplicateRemoteObjectGenerationMetadataClientNodeId {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if !config.socket_path.is_absolute() {
                return Err(
                    ClusterBuildError::RemoteObjectGenerationMetadataClientSocketPathNotAbsolute {
                        path: config.socket_path.clone(),
                    },
                );
            }
            if !self.nodes.contains_key(&config.node_id) {
                return Err(
                    ClusterBuildError::RemoteObjectGenerationMetadataClientNodeNotFound {
                        id: config.node_id.as_u32(),
                    },
                );
            }
        }
        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote object-generation metadata client node must exist");
            let client = Arc::new(UnixStorageNodeClient::new(
                config.node_id,
                self.epoch,
                config.socket_path,
            ));
            let object_generation_metadata_client: Arc<dyn ObjectGenerationMetadataNodeClient> =
                client;
            node.object_generation_metadata_client = object_generation_metadata_client;
        }
        Ok(())
    }

    pub fn install_unix_object_version_metadata_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixObjectVersionMetadataNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixObjectVersionMetadataNodeClientConfig> =
            configs.into_iter().collect();
        let mut seen = BTreeSet::<NodeId>::new();
        for config in &configs {
            if !seen.insert(config.node_id) {
                return Err(
                    ClusterBuildError::DuplicateRemoteObjectVersionMetadataClientNodeId {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if !config.socket_path.is_absolute() {
                return Err(
                    ClusterBuildError::RemoteObjectVersionMetadataClientSocketPathNotAbsolute {
                        path: config.socket_path.clone(),
                    },
                );
            }
            if !self.nodes.contains_key(&config.node_id) {
                return Err(
                    ClusterBuildError::RemoteObjectVersionMetadataClientNodeNotFound {
                        id: config.node_id.as_u32(),
                    },
                );
            }
        }
        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote object-version metadata client node must exist");
            let client = Arc::new(UnixStorageNodeClient::new(
                config.node_id,
                self.epoch,
                config.socket_path,
            ));
            let object_version_metadata_client: Arc<dyn ObjectVersionMetadataNodeClient> = client;
            node.object_version_metadata_client = object_version_metadata_client;
        }
        Ok(())
    }

    pub fn install_unix_direct_put_metadata_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixDirectPutMetadataNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixDirectPutMetadataNodeClientConfig> =
            configs.into_iter().collect();
        let mut seen = BTreeSet::<NodeId>::new();
        for config in &configs {
            if !seen.insert(config.node_id) {
                return Err(
                    ClusterBuildError::DuplicateRemoteDirectPutMetadataClientNodeId {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if !config.socket_path.is_absolute() {
                return Err(
                    ClusterBuildError::RemoteDirectPutMetadataClientSocketPathNotAbsolute {
                        path: config.socket_path.clone(),
                    },
                );
            }
            if !self.nodes.contains_key(&config.node_id) {
                return Err(
                    ClusterBuildError::RemoteDirectPutMetadataClientNodeNotFound {
                        id: config.node_id.as_u32(),
                    },
                );
            }
        }
        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote direct PUT metadata client node must exist");
            let client = Arc::new(UnixStorageNodeClient::new(
                config.node_id,
                self.epoch,
                config.socket_path,
            ));
            let direct_put_metadata_client: Arc<dyn DirectPutMetadataNodeClient> = client;
            node.direct_put_metadata_client = direct_put_metadata_client;
        }
        Ok(())
    }

    pub fn install_unix_object_listing_metadata_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixObjectListingMetadataNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixObjectListingMetadataNodeClientConfig> =
            configs.into_iter().collect();
        let mut seen = BTreeSet::<NodeId>::new();
        for config in &configs {
            if !seen.insert(config.node_id) {
                return Err(
                    ClusterBuildError::DuplicateRemoteObjectListingMetadataClientNodeId {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if !config.socket_path.is_absolute() {
                return Err(
                    ClusterBuildError::RemoteObjectListingMetadataClientSocketPathNotAbsolute {
                        path: config.socket_path.clone(),
                    },
                );
            }
            if !self.nodes.contains_key(&config.node_id) {
                return Err(
                    ClusterBuildError::RemoteObjectListingMetadataClientNodeNotFound {
                        id: config.node_id.as_u32(),
                    },
                );
            }
        }
        let pg_topology = Arc::new(self.pg_topology.clone());
        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote object-listing metadata client node must exist");
            let client = Arc::new(
                UnixStorageNodeClient::new(config.node_id, self.epoch, config.socket_path)
                    .with_pg_topology(Arc::clone(&pg_topology)),
            );
            let object_listing_metadata_client: Arc<dyn ObjectListingMetadataNodeClient> = client;
            node.object_listing_metadata_client = object_listing_metadata_client;
        }
        Ok(())
    }

    pub fn install_unix_object_mutation_metadata_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixObjectMutationMetadataNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixObjectMutationMetadataNodeClientConfig> =
            configs.into_iter().collect();
        let mut seen = BTreeSet::<NodeId>::new();
        for config in &configs {
            if !seen.insert(config.node_id) {
                return Err(
                    ClusterBuildError::DuplicateRemoteObjectMutationMetadataClientNodeId {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if !config.socket_path.is_absolute() {
                return Err(
                    ClusterBuildError::RemoteObjectMutationMetadataClientSocketPathNotAbsolute {
                        path: config.socket_path.clone(),
                    },
                );
            }
            if !self.nodes.contains_key(&config.node_id) {
                return Err(
                    ClusterBuildError::RemoteObjectMutationMetadataClientNodeNotFound {
                        id: config.node_id.as_u32(),
                    },
                );
            }
        }
        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote object-mutation metadata client node must exist");
            let client = Arc::new(UnixStorageNodeClient::new(
                config.node_id,
                self.epoch,
                config.socket_path,
            ));
            let object_mutation_metadata_client: Arc<dyn ObjectMutationMetadataNodeClient> =
                client.clone();
            let retained_object_mutation_metadata_client: Arc<
                dyn RetainedObjectMutationMetadataNodeClient,
            > = client;
            node.object_mutation_metadata_client = object_mutation_metadata_client;
            node.retained_object_mutation_metadata_client =
                retained_object_mutation_metadata_client;
        }
        Ok(())
    }

    pub fn install_unix_object_read_metadata_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixObjectReadMetadataNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixObjectReadMetadataNodeClientConfig> =
            configs.into_iter().collect();
        let mut seen = BTreeSet::<NodeId>::new();
        for config in &configs {
            if !seen.insert(config.node_id) {
                return Err(
                    ClusterBuildError::DuplicateRemoteObjectReadMetadataClientNodeId {
                        id: config.node_id.as_u32(),
                    },
                );
            }
            if !config.socket_path.is_absolute() {
                return Err(
                    ClusterBuildError::RemoteObjectReadMetadataClientSocketPathNotAbsolute {
                        path: config.socket_path.clone(),
                    },
                );
            }
            if !self.nodes.contains_key(&config.node_id) {
                return Err(
                    ClusterBuildError::RemoteObjectReadMetadataClientNodeNotFound {
                        id: config.node_id.as_u32(),
                    },
                );
            }
        }
        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote object-read metadata client node must exist");
            let client = Arc::new(UnixStorageNodeClient::new(
                config.node_id,
                self.epoch,
                config.socket_path,
            ));
            let object_read_metadata_client: Arc<dyn ObjectReadMetadataNodeClient> = client;
            node.object_read_metadata_client = object_read_metadata_client;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn replace_object_payload_lease_client_for_tests(
        &mut self,
        node_id: NodeId,
        object_payload_lease_client: Arc<dyn ObjectPayloadLeaseNodeClient>,
    ) {
        let node = self
            .nodes
            .get_mut(&node_id)
            .expect("test object-payload lease client node must exist");
        node.object_payload_lease_client = object_payload_lease_client;
    }

    #[cfg(test)]
    pub(crate) fn replace_shard_client_for_tests(
        &mut self,
        node_id: NodeId,
        shard_client: Arc<dyn PlacedShardNodeClient>,
    ) {
        let node = self
            .nodes
            .get_mut(&node_id)
            .expect("test shard client node must exist");
        node.shard_client = shard_client;
    }

    #[cfg(test)]
    pub(crate) fn replace_shard_ack_client_for_tests(
        &mut self,
        node_id: NodeId,
        shard_ack_client: Arc<dyn ShardAckNodeClient>,
    ) {
        let node = self
            .nodes
            .get_mut(&node_id)
            .expect("test shard ack client node must exist");
        node.shard_ack_client = shard_ack_client;
    }

    #[cfg(test)]
    pub(crate) fn replace_shard_read_handle_client_for_tests(
        &mut self,
        node_id: NodeId,
        shard_read_handle_client: Arc<dyn ShardReadHandleNodeClient>,
    ) {
        let node = self
            .nodes
            .get_mut(&node_id)
            .expect("test shard read handle client node must exist");
        node.shard_read_handle_client = shard_read_handle_client;
    }

    pub fn pg_route(&self, pg_id: PgId) -> Option<&LocalPgRoute> {
        self.pg_routes.get(&pg_id)
    }

    pub fn pg_routes(&self) -> impl Iterator<Item = &LocalPgRoute> + '_ {
        self.pg_routes.values()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_historical_pg_routes(&self) -> impl Iterator<Item = &PgRouteSnapshot> + '_ {
        self.historical_pg_routes.values()
    }

    pub(crate) fn reconstructed_pg_route_at_epoch(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Option<PgRouteSnapshot> {
        let current_route = self.pg_route(pg_id).map(|route| {
            PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            )
        });
        reconstruct_sparse_pg_route_at_epoch(
            self.epoch,
            current_route,
            self.historical_pg_routes
                .range((pg_id, ClusterEpoch::INITIAL)..=(pg_id, self.epoch))
                .map(|(_, route)| route),
            self.historical_cluster_epochs.contains(&cluster_epoch),
            cluster_epoch,
        )
        .ok()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_historical_pg_routes(
        &mut self,
        routes: impl IntoIterator<Item = PgRouteSnapshot>,
    ) {
        self.historical_pg_routes = routes
            .into_iter()
            .map(|route| ((route.pg_id(), route.cluster_epoch()), route))
            .collect();
        self.historical_cluster_epochs = self
            .historical_pg_routes
            .values()
            .map(PgRouteSnapshot::cluster_epoch)
            .collect();
    }

    #[cfg(test)]
    pub(super) fn test_remove_historical_cluster_epoch(&mut self, epoch: ClusterEpoch) {
        assert!(
            self.historical_cluster_epochs.remove(&epoch),
            "test historical epoch must exist before it is removed"
        );
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_pg_routes(
        &mut self,
        routes: impl IntoIterator<Item = PgRouteSnapshot>,
    ) {
        self.pg_routes = routes
            .into_iter()
            .map(|route| (route.pg_id(), LocalPgRoute::from(&route)))
            .collect();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_set_route_map_validity(&mut self, validity: RouteMapValidity) {
        self.route_map_lease = RwLock::new(LocalRouteMapLeaseSnapshot {
            validity,
            local_valid_until_monotonic_ms: test_process_local_route_map_deadline(validity),
        });
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_store_route_map_validity(&self, validity: RouteMapValidity) {
        self.replace_route_map_lease_snapshot(LocalRouteMapLeaseSnapshot {
            validity,
            local_valid_until_monotonic_ms: test_process_local_route_map_deadline(validity),
        });
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_store_route_map_lease(
        &self,
        validity: RouteMapValidity,
        local_valid_until_monotonic_ms: Option<u64>,
    ) {
        self.replace_route_map_lease_snapshot(LocalRouteMapLeaseSnapshot {
            validity,
            local_valid_until_monotonic_ms,
        });
    }

    pub fn process_local_registry_key(&self) -> ProcessLocalRegistryKey {
        self.process_local_registry_key
    }

    pub fn default_ec_shape(&self) -> EcShape {
        self.default_ec_shape
    }

    pub fn bucket_pg_for(&self, bucket: &BucketName) -> u32 {
        self.pg_topology.bucket_pg_for(bucket)
    }

    pub fn object_pg_for(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.pg_topology.object_pg_for(bucket, key)
    }

    pub(crate) fn object_generation_segment_data_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segment_index: u32,
    ) -> DataPgId {
        self.data_pg(self.pg_topology.object_generation_segment_data_pg(
            bucket,
            key,
            generation_id,
            segment_index,
        ))
        .expect("installed payload placement must select a configured data PG")
    }

    #[cfg(test)]
    pub(crate) fn object_generation_multipart_part_data_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        part_number: u32,
    ) -> DataPgId {
        self.data_pg(self.pg_topology.object_generation_multipart_part_data_pg(
            bucket,
            key,
            generation_id,
            part_number,
        ))
        .expect("installed multipart placement must select a configured data PG")
    }

    pub(crate) fn write_erasure_coded_segment_shards_with<F, E>(
        &self,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        data: &[u8],
        ec: EcShape,
        write_shards: F,
    ) -> Result<Vec<WrittenShardAck>, E>
    where
        F: FnOnce(&[(ShardKey, &[u8])]) -> Result<Vec<(ShardKey, WriteAck)>, E>,
        E: From<StoreError>,
    {
        self.metadata_primary()
            .runtime()
            .write_erasure_coded_segment_shards_with(
                segment_okh,
                segment_vid,
                data,
                ec,
                write_shards,
            )
    }

    pub(crate) fn runtime_state(&self) -> Arc<LocalClusterRuntimeState> {
        Arc::clone(&self.runtime_state)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn try_acquire_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<AcquiredObjectPayloadNodeLeases, StoreError> {
        self.try_acquire_object_payload_lease_inner(bucket, key, generation_id, false)
    }

    pub(in crate::cluster) fn try_acquire_available_object_payload_lease(
        &self,
        authority: &super::request_ops::leased_object_snapshot::BroadObjectPayloadLeaseAuthority<
            '_,
        >,
    ) -> Result<AcquiredObjectPayloadNodeLeases, StoreError> {
        self.try_acquire_object_payload_lease_inner(
            authority.bucket(),
            authority.key(),
            authority.generation_id(),
            true,
        )
    }

    fn try_acquire_object_payload_lease_inner(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        retain_available: bool,
    ) -> Result<AcquiredObjectPayloadNodeLeases, StoreError> {
        let mut acquired: Vec<Box<dyn ObjectPayloadLeaseNodeLease>> =
            Vec::with_capacity(self.nodes.len());
        let mut acquired_node_ids = BTreeSet::new();
        let mut unavailable_error = None;
        for (node_id, node) in &self.nodes {
            let result = node
                .object_payload_lease_client()
                .open_object_payload_lease_route(self.epoch, bucket, key, generation_id)
                .and_then(|route| {
                    route.acquire_object_payload_lease(
                        crate::node_client::ObjectPayloadLeaseKind::BroadSnapshot,
                    )
                });
            match result {
                Ok(Some(lease)) => {
                    acquired.push(lease);
                    acquired_node_ids.insert(*node_id);
                }
                Ok(None) => {
                    return Ok(AcquiredObjectPayloadNodeLeases {
                        node_leases: Vec::new(),
                        leased_node_ids: BTreeSet::new(),
                        unavailable_error: None,
                        reclaim_fenced: true,
                    });
                }
                Err(error)
                    if retain_available && object_payload_lease_node_is_unavailable(&error) =>
                {
                    unavailable_error.get_or_insert(error);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(AcquiredObjectPayloadNodeLeases {
            node_leases: acquired,
            leased_node_ids: acquired_node_ids,
            unavailable_error,
            reclaim_fenced: false,
        })
    }

    pub(crate) fn try_acquire_object_payload_lease_on_locations(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        locations: &[ShardLocation],
    ) -> Result<Vec<Box<dyn ObjectPayloadLeaseNodeLease>>, StoreError> {
        let mut node_ids = BTreeMap::new();
        for location in locations {
            let route = self
                .reconstructed_pg_route_at_epoch(
                    location.data_pg_id().pg_id(),
                    location.cluster_epoch(),
                )
                .ok_or(StoreError::ClusterPgNotFound {
                    pg_id: location.data_pg_id().get(),
                    cluster_epoch: location.cluster_epoch(),
                })?;
            if route.state() != PgState::Active {
                return Err(StoreError::PgNotActive {
                    pg_id: location.data_pg_id().get(),
                    cluster_epoch: location.cluster_epoch(),
                    state: route.state(),
                });
            }
            if !route.acting_set().contains(&location.node_id()) {
                return Err(StoreError::NodeNotInActingSet {
                    node_id: location.node_id().as_u32(),
                    pg_id: location.data_pg_id().get(),
                    cluster_epoch: location.cluster_epoch(),
                });
            }
            node_ids
                .entry(location.node_id())
                .or_insert_with(|| location.data_pg_id().get());
        }

        let mut lease_clients = Vec::with_capacity(node_ids.len());
        for (node_id, pg_id) in node_ids {
            let node = self.nodes.get(&node_id).ok_or(StoreError::NodeNotFound {
                node_id: node_id.as_u32(),
                pg_id,
                cluster_epoch: self.epoch,
            })?;
            lease_clients.push((node_id, Arc::clone(node.object_payload_lease_client())));
        }

        let mut acquired = Vec::with_capacity(lease_clients.len());
        for (_, lease_client) in lease_clients {
            let result = lease_client
                .open_object_payload_lease_route(self.epoch, bucket, key, generation_id)
                .and_then(|route| {
                    route.acquire_object_payload_lease(
                        crate::node_client::ObjectPayloadLeaseKind::ShardLocations,
                    )
                });
            match result {
                Ok(Some(lease)) => {
                    acquired.push(lease);
                }
                Ok(None) => {
                    return Ok(Vec::new());
                }
                Err(error) => return Err(error),
            }
        }
        Ok(acquired)
    }

    pub(crate) fn try_begin_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> Result<bool, StoreError> {
        let mut acquired: Vec<Box<dyn RetainedObjectPayloadReclaimRoute + '_>> =
            Vec::with_capacity(self.nodes.len());
        for node in self.nodes.values() {
            let active_route = match node
                .object_payload_lease_client()
                .open_object_payload_lease_route(self.epoch, bucket, key, generation_id)
            {
                Ok(route) => route,
                Err(error) => {
                    for route in acquired {
                        let _ = route.finish_object_payload_reclaim(false);
                    }
                    return Err(error);
                }
            };
            let retained_route = match node
                .retained_object_payload_reclaim_client()
                .open_retained_object_payload_reclaim_route(
                    self.epoch,
                    bucket,
                    key,
                    generation_id,
                    authority,
                ) {
                Ok(route) => route,
                Err(error) => {
                    for route in acquired {
                        let _ = route.finish_object_payload_reclaim(false);
                    }
                    return Err(error);
                }
            };
            match active_route.try_begin_object_payload_reclaim(authority) {
                Ok(true) => {
                    acquired.push(retained_route);
                    continue;
                }
                Ok(false) => {}
                Err(error) => {
                    let _ = retained_route.finish_object_payload_reclaim(false);
                    for route in acquired {
                        let _ = route.finish_object_payload_reclaim(false);
                    }
                    return Err(error);
                }
            }
            for route in acquired {
                let _ = route.finish_object_payload_reclaim(false);
            }
            return Ok(false);
        }
        Ok(true)
    }

    pub(crate) fn finish_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        authority: &ObjectPayloadReclaimClaimProof,
        keep_fence: bool,
    ) -> Result<(), StoreError> {
        let mut first_error = None;
        for node in self.nodes.values() {
            let result = node
                .retained_object_payload_reclaim_client()
                .open_retained_object_payload_reclaim_route(
                    self.epoch,
                    bucket,
                    key,
                    generation_id,
                    authority,
                )
                .and_then(|route| route.finish_object_payload_reclaim(keep_fence));
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_object_payload_reclaim_is_active(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        self.nodes.values().any(|node| {
            node.test_node()
                .test_object_payload_reclaim_is_active(bucket, key, generation_id)
        })
    }

    pub(crate) fn clear_object_payload_reclaim_fence(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        authority: &ObjectPayloadReclaimClaimProof,
    ) -> Result<(), StoreError> {
        let mut first_error = None;
        for node in self.nodes.values() {
            let result = node
                .retained_object_payload_reclaim_client()
                .open_retained_object_payload_reclaim_route(
                    self.epoch,
                    bucket,
                    key,
                    generation_id,
                    authority,
                )
                .and_then(|route| route.clear_object_payload_reclaim_fence());
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<usize, StoreError> {
        let mut max_count = 0;
        for node in self.nodes.values() {
            max_count = max_count.max(
                node.object_payload_lease_client()
                    .open_object_payload_lease_route(self.epoch, bucket, key, generation_id)?
                    .object_payload_lease_count()?,
            );
        }
        Ok(max_count)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn object_payload_lease_holder_node_ids(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> HashSet<NodeId> {
        self.nodes
            .iter()
            .filter_map(|(node_id, node)| {
                let lease_count = node
                    .object_payload_lease_client()
                    .open_object_payload_lease_route(self.epoch, bucket, key, generation_id)
                    .and_then(|route| route.object_payload_lease_count())
                    .unwrap_or(0);
                (lease_count != 0).then_some(*node_id)
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn object_payload_lease_holder_node_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        self.object_payload_lease_holder_node_ids(bucket, key, generation_id)
            .len()
    }

    #[cfg(test)]
    pub(crate) fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        self.nodes
            .values()
            .map(|node| node.test_node().bucket_object_payload_lease_count(bucket))
            .max()
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn test_next_metadata_command_log_index(
        &self,
        pg_id: PgId,
    ) -> MetadataCommandLogIndex {
        let primary = self
            .metadata_pg_primary_node(self.epoch, pg_id)
            .expect("test PG primary should be routable");
        let pg = primary
            .test_node()
            .get_pg(pg_id.get())
            .expect("test PG primary should have the PG");
        let max_log_index = pg
            .max_metadata_command_log_index(self.epoch)
            .expect("test PG should load max metadata command log index");
        let max_pending_index = pg
            .pending_metadata_command_slot(primary.node_id().as_u32(), self.epoch)
            .expect("test PG should load pending metadata command slot")
            .map(|slot| slot.id.log_index().get())
            .unwrap_or_default();
        MetadataCommandLogIndex::new(max_log_index.max(max_pending_index) + 1)
            .expect("test metadata command log index should not overflow")
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
        self.require_route_map_valid_now()?;

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

    pub(crate) fn metadata_pg_read_node(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<MetadataPgReadNode<'_>, StoreError> {
        if operation_epoch != self.epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch,
                current_epoch: self.epoch,
            });
        }
        self.require_route_map_valid_now()?;

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
        let (node_id, authorization) = match route.state() {
            PgState::Active => (
                route.primary_node_id(),
                MetadataReadAuthorization::active(pg_id),
            ),
            PgState::Peering => {
                let read_route = route.metadata_read_route().ok_or(StoreError::PgNotActive {
                    pg_id: pg_id.get(),
                    cluster_epoch: self.epoch,
                    state: route.state(),
                })?;
                (
                    read_route.node_id(),
                    MetadataReadAuthorization::peering(pg_id, read_route),
                )
            }
            state => {
                return Err(StoreError::PgNotActive {
                    pg_id: pg_id.get(),
                    cluster_epoch: self.epoch,
                    state,
                });
            }
        };
        if !route.contains_node(node_id) {
            return Err(StoreError::NodeNotInActingSet {
                node_id: node_id.as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: self.epoch,
            });
        }
        let node = self.nodes.get(&node_id).ok_or(StoreError::NodeNotFound {
            node_id: node_id.as_u32(),
            pg_id: pg_id.get(),
            cluster_epoch: self.epoch,
        })?;
        Ok(MetadataPgReadNode {
            node,
            authorization,
        })
    }

    pub(crate) fn metadata_pg_primary_node_for_metadata_command_recovery(
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

    pub(crate) fn metadata_pg_primary_node_for_retained_cleanup(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<&LocalNodeStore, StoreError> {
        let route = self
            .reconstructed_pg_route_at_epoch(pg_id, operation_epoch)
            .ok_or(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch,
                current_epoch: self.epoch,
            })?;
        if route.cluster_epoch() != operation_epoch {
            return Err(StoreError::StaleMetadataRoute {
                pg_id: pg_id.get(),
                route_epoch: route.cluster_epoch(),
                current_epoch: self.epoch,
            });
        }
        if route.state() != PgState::Active {
            return Err(StoreError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: route.cluster_epoch(),
                state: route.state(),
            });
        }
        let node_id = route.primary_node_id();
        if !route.acting_set().contains(&node_id) {
            return Err(StoreError::NodeNotInActingSet {
                node_id: node_id.as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch: route.cluster_epoch(),
            });
        }

        self.nodes.get(&node_id).ok_or(StoreError::NodeNotFound {
            node_id: node_id.as_u32(),
            pg_id: pg_id.get(),
            cluster_epoch: route.cluster_epoch(),
        })
    }

    pub(crate) fn metadata_pg_acting_nodes(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<Vec<&LocalNodeStore>, StoreError> {
        self.metadata_pg_acting_nodes_with_allowed_states(
            operation_epoch,
            pg_id,
            &[PgState::Active],
        )
    }

    pub(crate) fn metadata_pg_acting_nodes_for_metadata_command_recovery(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<Vec<&LocalNodeStore>, StoreError> {
        self.metadata_pg_acting_nodes_with_allowed_states_and_validity(
            operation_epoch,
            pg_id,
            &[PgState::Active],
            false,
        )
    }

    pub(crate) fn metadata_pg_acting_nodes_for_peering_inspection(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<Vec<&LocalNodeStore>, StoreError> {
        self.metadata_pg_acting_nodes_with_allowed_states(
            operation_epoch,
            pg_id,
            &[PgState::Active, PgState::Peering],
        )
    }

    pub(crate) fn metadata_pg_acting_nodes_for_peering_replay(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<Vec<&LocalNodeStore>, StoreError> {
        self.metadata_pg_acting_nodes_with_allowed_states(
            operation_epoch,
            pg_id,
            &[PgState::Peering],
        )
    }

    fn metadata_pg_acting_nodes_with_allowed_states(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
        allowed_states: &[PgState],
    ) -> Result<Vec<&LocalNodeStore>, StoreError> {
        self.metadata_pg_acting_nodes_with_allowed_states_and_validity(
            operation_epoch,
            pg_id,
            allowed_states,
            true,
        )
    }

    fn metadata_pg_acting_nodes_with_allowed_states_and_validity(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
        allowed_states: &[PgState],
        require_validity: bool,
    ) -> Result<Vec<&LocalNodeStore>, StoreError> {
        if operation_epoch != self.epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: pg_id.get(),
                operation_epoch,
                current_epoch: self.epoch,
            });
        }
        if require_validity {
            self.require_route_map_valid_now()?;
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
        if !allowed_states.contains(&route.state()) {
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

    #[cfg(test)]
    pub(crate) fn validate_metadata_command_for_replica(
        &self,
        origin_node_id: NodeId,
        target_node_id: NodeId,
        target_pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.validate_metadata_command_for_replica_with_route_validity(
            origin_node_id,
            target_node_id,
            target_pg_id,
            command,
            true,
            None,
        )
    }

    pub(crate) fn validate_metadata_command_for_replica_until(
        &self,
        origin_node_id: NodeId,
        target_node_id: NodeId,
        target_pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.validate_metadata_command_for_replica_with_route_validity(
            origin_node_id,
            target_node_id,
            target_pg_id,
            command,
            true,
            Some(deadline),
        )
    }

    pub(crate) fn validate_metadata_command_for_replica_for_metadata_command_recovery_until(
        &self,
        origin_node_id: NodeId,
        target_node_id: NodeId,
        target_pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.validate_metadata_command_for_replica_with_route_validity(
            origin_node_id,
            target_node_id,
            target_pg_id,
            command,
            false,
            Some(deadline),
        )
    }

    fn validate_metadata_command_for_replica_with_route_validity(
        &self,
        origin_node_id: NodeId,
        target_node_id: NodeId,
        target_pg_id: PgId,
        command: &MetadataCommandEnvelope,
        require_validity: bool,
        deadline: Option<Instant>,
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
        if require_validity {
            self.require_route_map_valid_now()?;
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

        match deadline {
            Some(deadline) => target_node
                .metadata_command_inspection_client()
                .metadata_command_acceptance_until(target_pg_id, command, deadline),
            None => target_node
                .metadata_command_inspection_client()
                .metadata_command_acceptance(target_pg_id, command),
        }
    }

    #[cfg(test)]
    pub(crate) fn validate_metadata_command_abandon_for_replica(
        &self,
        origin_node_id: NodeId,
        target_node_id: NodeId,
        target_pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.validate_metadata_command_abandon_for_replica_inner(
            origin_node_id,
            target_node_id,
            target_pg_id,
            command,
            None,
        )
    }

    pub(crate) fn validate_metadata_command_abandon_for_replica_until(
        &self,
        origin_node_id: NodeId,
        target_node_id: NodeId,
        target_pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        self.validate_metadata_command_abandon_for_replica_inner(
            origin_node_id,
            target_node_id,
            target_pg_id,
            command,
            Some(deadline),
        )
    }

    fn validate_metadata_command_abandon_for_replica_inner(
        &self,
        origin_node_id: NodeId,
        target_node_id: NodeId,
        target_pg_id: PgId,
        command: &MetadataCommandEnvelope,
        deadline: Option<Instant>,
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
        match deadline {
            Some(deadline) => target_node
                .metadata_command_inspection_client()
                .metadata_command_abandon_acceptance_until(target_pg_id, command, deadline),
            None => target_node
                .metadata_command_inspection_client()
                .metadata_command_abandon_acceptance(target_pg_id, command),
        }
    }

    pub(crate) fn place_payload_shards(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
    ) -> Result<Vec<ShardLocation>, ClusterBuildError> {
        self.require_current_epoch_for_placement(operation_epoch, data_pg_id.pg_id())?;
        let route = self.require_active_pg_for_placement(data_pg_id.pg_id())?;
        Self::place_payload_shards_for_pg_route(
            operation_epoch,
            data_pg_id,
            ec_shape,
            stable_placement_key,
            route.acting_set(),
        )
    }

    pub(crate) fn place_payload_shards_for_pg_route(
        cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        ec_shape: EcShape,
        stable_placement_key: &[u8],
        acting_set: &[NodeId],
    ) -> Result<Vec<ShardLocation>, ClusterBuildError> {
        let ec_config = ec_config_for_shape(ec_shape)?;
        let total_shards = ec_config.total_shards();
        let placement_map = build_local_placement_map(acting_set.iter().copied())?;
        let placer = local_payload_placer(&placement_map, ec_shape)?;
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
                    cluster_epoch,
                    data_pg_id,
                    ShardIndex::new(shard_index as u8),
                    node_id,
                )
            })
            .collect())
    }

    #[cfg(test)]
    pub(crate) fn payload_shard_node(
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

    pub(crate) fn write_payload_shard(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .write_shard(data)
    }

    pub(crate) fn write_payload_shard_with_effect_fence(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
        data: &[u8],
        effect_fence: AdmittedRouteEffectFence,
    ) -> Result<WriteAck, ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .write_shard_with_effect_fence(data, effect_fence)
    }

    pub(crate) fn repair_payload_shard(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .repair_shard(data)
    }

    pub(super) fn open_placed_segment_shard_reader(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
    ) -> Result<LocalPlacedSegmentShardReader<'_>, ClusterBuildError> {
        Ok(LocalPlacedSegmentShardReader {
            cluster_map: self,
            subject: self.placed_segment_shard_subject(
                operation_epoch,
                data_pg_id,
                ec,
                segment_okh,
                segment_vid,
            )?,
        })
    }

    pub(super) fn open_placed_segment_shard_deleter(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
    ) -> Result<LocalPlacedSegmentShardDeleter<'_>, ClusterBuildError> {
        Ok(LocalPlacedSegmentShardDeleter {
            cluster_map: self,
            subject: self.placed_segment_shard_subject(
                operation_epoch,
                data_pg_id,
                ec,
                segment_okh,
                segment_vid,
            )?,
            route_kind: LocalPlacedSegmentShardDeleteRouteKind::Current,
        })
    }

    pub(super) fn open_retained_placed_segment_shard_deleter(
        &self,
        placement_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
    ) -> Result<LocalPlacedSegmentShardDeleter<'_>, StoreError> {
        let route = self
            .reconstructed_pg_route_at_epoch(data_pg_id.pg_id(), placement_epoch)
            .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "PG {} route for cluster epoch {} is not retained",
                    data_pg_id.get(),
                    placement_epoch.get()
                ),
            })?;
        if route.cluster_epoch() != placement_epoch {
            return Err(StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "retained PG {} route epoch {} does not match payload placement epoch {}",
                    data_pg_id.get(),
                    route.cluster_epoch().get(),
                    placement_epoch.get()
                ),
            });
        }
        if route.state() != PgState::Active {
            return Err(StoreError::PgNotActive {
                pg_id: data_pg_id.get(),
                cluster_epoch: route.cluster_epoch(),
                state: route.state(),
            });
        }
        let placement_key = super::segment_payload_placement_key(segment_okh, segment_vid);
        let locations = Self::place_payload_shards_for_pg_route(
            placement_epoch,
            data_pg_id,
            ec,
            &placement_key,
            route.acting_set(),
        )
        .map_err(super::cluster_build_error_to_store)?;
        Ok(LocalPlacedSegmentShardDeleter {
            cluster_map: self,
            subject: LocalPlacedSegmentShardSubject {
                operation_epoch: placement_epoch,
                data_pg_id,
                segment_okh: *segment_okh,
                segment_vid,
                locations,
            },
            route_kind: LocalPlacedSegmentShardDeleteRouteKind::Retained,
        })
    }

    fn placed_segment_shard_subject(
        &self,
        operation_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        ec: EcShape,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
    ) -> Result<LocalPlacedSegmentShardSubject, ClusterBuildError> {
        let placement_key = super::segment_payload_placement_key(segment_okh, segment_vid);
        let locations =
            self.place_payload_shards(operation_epoch, data_pg_id, ec, &placement_key)?;
        Ok(LocalPlacedSegmentShardSubject {
            operation_epoch,
            data_pg_id,
            segment_okh: *segment_okh,
            segment_vid,
            locations,
        })
    }

    fn read_payload_shard(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<Vec<u8>, ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .read_shard(expected)
    }

    #[cfg(test)]
    pub(super) fn test_read_payload_shard(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<Vec<u8>, ShardIoError> {
        self.read_payload_shard(operation_epoch, location, key, expected)
    }

    fn retained_payload_shard_route(
        &self,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<Box<dyn RetainedPlacedShardRoute + '_>, ShardIoError> {
        if location.shard_index() != key.shard_index() {
            return Err(ShardIoError::ShardIndexMismatch {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                location_shard_index: location.shard_index().get(),
                key_shard_index: key.shard_index().get(),
            });
        }
        let route = self
            .reconstructed_pg_route_at_epoch(
                location.data_pg_id().pg_id(),
                location.cluster_epoch(),
            )
            .ok_or_else(|| ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source: StoreError::PayloadShardSetMismatch {
                    reason: format!(
                        "PG {} route for cluster epoch {} is not retained",
                        location.data_pg_id().get(),
                        location.cluster_epoch().get()
                    ),
                },
            })?;
        if route.state() != PgState::Active {
            return Err(ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source: StoreError::PgNotActive {
                    pg_id: location.data_pg_id().get(),
                    cluster_epoch: route.cluster_epoch(),
                    state: route.state(),
                },
            });
        }
        if !route.acting_set().contains(&location.node_id()) {
            return Err(ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source: StoreError::NodeNotInActingSet {
                    node_id: location.node_id().as_u32(),
                    pg_id: location.data_pg_id().get(),
                    cluster_epoch: route.cluster_epoch(),
                },
            });
        }
        let node = self
            .nodes
            .get(&location.node_id())
            .ok_or(ShardIoError::NodeNotFound {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
            })?;
        node.retained_shard_client()
            .open_retained_placed_shard_route(location, key)
            .map_err(|source| ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source,
            })
    }

    pub(crate) fn read_payload_shard_for_historical_inspection(
        &self,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<(Vec<u8>, WriteAck), ShardIoError> {
        let (data, ack) = self
            .retained_payload_shard_route(location, key)?
            .read_placed_shard_for_historical_inspection()
            .map_err(|source| ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source,
            })?;
        if data.len() as u64 != ack.stored_size {
            return Err(ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source: StoreError::Io {
                    context: "read payload shard size mismatch",
                    source: std::io::Error::from(std::io::ErrorKind::InvalidData),
                },
            });
        }
        Ok((data, ack))
    }

    pub(crate) fn read_payload_shard_for_historical_inspection_into(
        &self,
        location: ShardLocation,
        key: &ShardKey,
        dst: &mut [u8],
    ) -> Result<WriteAck, ShardIoError> {
        let ack = self
            .retained_payload_shard_route(location, key)?
            .read_placed_shard_for_historical_inspection_into(dst)
            .map_err(|source| ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source,
            })?;
        if dst.len() as u64 != ack.stored_size {
            return Err(ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source: StoreError::Io {
                    context: "read payload shard size mismatch",
                    source: std::io::Error::from(std::io::ErrorKind::InvalidData),
                },
            });
        }
        Ok(ack)
    }

    #[cfg(test)]
    pub(crate) fn read_payload_shard_into(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .read_shard_into(expected, dst)
    }

    fn acquire_payload_shard_read_handles(
        &self,
        operation_epoch: ClusterEpoch,
        entries: &[(ShardLocation, ShardKey)],
    ) -> Result<LocalShardReadHandleSet, ShardIoError> {
        let mut groups: BTreeMap<NodeId, Vec<(ShardLocation, ShardKey)>> = BTreeMap::new();
        for (location, key) in entries {
            self.shard_node_client(operation_epoch, *location, key)?;
            groups
                .entry(location.node_id())
                .or_default()
                .push((*location, key.clone()));
        }

        let mut handle_set = LocalShardReadHandleSet::new();
        for (node_id, entries) in groups {
            let node = self
                .node(node_id)
                .expect("read handle group node was validated before grouping");
            let keys: Vec<ShardKey> = entries.iter().map(|(_, key)| key.clone()).collect();
            let first = entries
                .first()
                .expect("read handle group must contain at least one location");
            let client = self.shard_node_client(operation_epoch, first.0, &first.1)?;
            let read_operation_id = client.read_operation_id_for_keys(&keys);
            match node
                .shard_read_handle_client()
                .open_shard_read_handle_route(operation_epoch, &read_operation_id, entries.clone())
                .and_then(|route| route.acquire())
            {
                Ok(lease) => handle_set.push(first.0, lease),
                Err(source) => {
                    handle_set.release()?;
                    return Err(ShardIoError::Store {
                        node_id: node_id.as_u32(),
                        pg_id: first.0.data_pg_id().get(),
                        cluster_epoch: first.0.cluster_epoch(),
                        source,
                    });
                }
            }
        }
        Ok(handle_set)
    }

    fn delete_payload_shard_for_current_route(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .delete_shard()
    }

    #[cfg(test)]
    pub(super) fn delete_payload_shard(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        self.delete_payload_shard_for_current_route(operation_epoch, location, key)
    }

    fn delete_payload_shard_for_historical_cleanup(
        &self,
        location: ShardLocation,
        key: &ShardKey,
    ) -> Result<(), ShardIoError> {
        if location.shard_index() != key.shard_index() {
            return Err(ShardIoError::ShardIndexMismatch {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                location_shard_index: location.shard_index().get(),
                key_shard_index: key.shard_index().get(),
            });
        }
        let node = self
            .nodes
            .get(&location.node_id())
            .ok_or(ShardIoError::NodeNotFound {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
            })?;
        node.retained_shard_client()
            .open_retained_placed_shard_route(location, key)
            .and_then(|route| route.delete_placed_shard_for_historical_cleanup())
            .map_err(|source| ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source,
            })
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
        let shard_route = node
            .shard_client()
            .open_placed_shard_route(location, key)
            .map_err(|source| ShardIoError::Store {
                node_id: location.node_id().as_u32(),
                pg_id: location.data_pg_id().get(),
                cluster_epoch: location.cluster_epoch(),
                source,
            })?;
        Ok(LocalShardNodeClient {
            node_id: node.shard_client().node_id(),
            route: shard_route,
            read_handle_client: node.shard_read_handle_client().as_ref(),
            location,
            key: key.clone(),
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
        self.require_route_map_valid_now_for_placement(pg_id)?;
        Ok(())
    }

    fn require_active_pg_for_placement(
        &self,
        pg_id: PgId,
    ) -> Result<&LocalPgRoute, ClusterBuildError> {
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
        Ok(route)
    }

    fn require_active_pg_for_shard_io(
        &self,
        pg_id: PgId,
        node_id: NodeId,
    ) -> Result<&LocalPgRoute, ShardIoError> {
        self.require_route_map_valid_now_for_shard_io(pg_id, node_id)?;
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

struct StaticRouteMapDigestInput<'a> {
    cluster_epoch: ClusterEpoch,
    metadata_primary_node_id: NodeId,
    default_ec_shape: EcShape,
    endpoints: &'a BTreeMap<NodeId, LocalRouteExecutionEndpoint>,
    pg_ids: &'a [u32],
    current_routes: &'a [PgRouteSnapshot],
    historical_routes: &'a [PgRouteSnapshot],
    historical_cluster_epochs: &'a BTreeSet<ClusterEpoch>,
}

fn static_route_map_content_digest(input: StaticRouteMapDigestInput<'_>) -> [u8; 32] {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    static_route_digest_bytes(&mut hasher, STATIC_ROUTE_MAP_CONTENT_DIGEST_DOMAIN);
    static_route_digest_u64(&mut hasher, input.cluster_epoch.get());
    static_route_digest_u32(&mut hasher, input.metadata_primary_node_id.as_u32());
    static_route_digest_u8(&mut hasher, input.default_ec_shape.k);
    static_route_digest_u8(&mut hasher, input.default_ec_shape.m);

    static_route_digest_len(&mut hasher, input.endpoints.len());
    for (node_id, endpoint) in input.endpoints {
        static_route_digest_u32(&mut hasher, node_id.as_u32());
        match endpoint {
            LocalRouteExecutionEndpoint::Embedded(data_dir) => {
                static_route_digest_u8(&mut hasher, 1);
                static_route_digest_bytes(&mut hasher, data_dir.as_os_str().as_bytes());
            }
            LocalRouteExecutionEndpoint::TopologyOnly => {
                static_route_digest_u8(&mut hasher, 2);
            }
            LocalRouteExecutionEndpoint::RpcUnix(socket_path) => {
                static_route_digest_u8(&mut hasher, 3);
                static_route_digest_bytes(&mut hasher, socket_path.as_os_str().as_bytes());
            }
            LocalRouteExecutionEndpoint::RpcTcp(endpoint) => {
                static_route_digest_u8(&mut hasher, 4);
                static_route_digest_bytes(&mut hasher, endpoint.as_bytes());
            }
        }
    }

    static_route_digest_len(&mut hasher, input.pg_ids.len());
    for pg_id in input.pg_ids {
        static_route_digest_u32(&mut hasher, *pg_id);
    }
    digest_pg_routes(&mut hasher, input.current_routes);
    digest_pg_routes(&mut hasher, input.historical_routes);
    static_route_digest_len(&mut hasher, input.historical_cluster_epochs.len());
    for epoch in input.historical_cluster_epochs {
        static_route_digest_u64(&mut hasher, epoch.get());
    }

    hasher
        .finalize()
        .bytes()
        .try_into()
        .expect("SHA-256 static route-map digest must contain 32 bytes")
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

fn build_validated_pg_routes(
    cluster_epoch: ClusterEpoch,
    node_ids: &BTreeSet<NodeId>,
    pg_ids: &[PgId],
    routes: impl IntoIterator<Item = LocalPgRoute>,
) -> Result<BTreeMap<PgId, LocalPgRoute>, ClusterBuildError> {
    let configured: BTreeSet<PgId> = pg_ids.iter().copied().collect();
    let mut pg_routes = BTreeMap::new();
    for route in routes {
        let route_pg_id = route.pg_id();
        if route.cluster_epoch() != cluster_epoch {
            return Err(ClusterBuildError::RouteClusterEpochMismatch {
                pg_id: route_pg_id.get(),
                route_epoch: route.cluster_epoch(),
                cluster_epoch,
            });
        }
        if !configured.contains(&route_pg_id) {
            return Err(ClusterBuildError::RoutePgNotConfigured {
                pg_id: route_pg_id.get(),
            });
        }
        if !route.acting_set().contains(&route.primary_node_id()) {
            return Err(ClusterBuildError::RoutePrimaryNotInActingSet {
                pg_id: route_pg_id.get(),
                primary_node_id: route.primary_node_id().as_u32(),
            });
        }
        for &node_id in route.acting_set() {
            if !node_ids.contains(&node_id) {
                return Err(ClusterBuildError::RouteActingSetNodeNotFound {
                    pg_id: route_pg_id.get(),
                    node_id: node_id.as_u32(),
                });
            }
        }
        if pg_routes.insert(route_pg_id, route).is_some() {
            return Err(ClusterBuildError::DuplicatePgRoute {
                pg_id: route_pg_id.get(),
            });
        }
    }
    for &pg_id in pg_ids {
        if !pg_routes.contains_key(&pg_id) {
            return Err(ClusterBuildError::MissingPgRoute { pg_id: pg_id.get() });
        }
    }
    Ok(pg_routes)
}

fn validate_metadata_command_replay_state(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_routes: &BTreeMap<PgId, LocalPgRoute>,
    pg_ids: &[PgId],
    cluster_epoch: ClusterEpoch,
) -> Result<(), ClusterBuildError> {
    for &pg_id in pg_ids {
        let primary_node_id = pg_routes
            .get(&pg_id)
            .expect("validated PG id should have a route")
            .primary_node_id();
        let mut replica_states = Vec::new();
        let mut primary_pending_command = None;
        let mut primary_pending_publication_started = false;
        for node in nodes.values() {
            let node_id = node.node_id();
            let peering_route = node
                .metadata_command_peering_client()
                .open_metadata_command_peering_route(pg_id, cluster_epoch)
                .map_err(|source| ClusterBuildError::open_local_node(node_id.as_u32(), source))?;
            let state = if node_id == primary_node_id {
                peering_route.validate_metadata_command_replay_state_preserving_pending_slot()
            } else {
                peering_route.validate_metadata_command_replay_state()
            }
            .map_err(|source| ClusterBuildError::open_local_node(node_id.as_u32(), source))?;
            let pending_command = node
                .metadata_command_inspection_client()
                .pending_metadata_command_envelope(pg_id, cluster_epoch)
                .map_err(|source| ClusterBuildError::open_local_node(node_id.as_u32(), source))?;
            if let Some(pending) = pending_command.as_ref() {
                if node_id != primary_node_id {
                    return Err(ClusterBuildError::open_local_node(
                        node_id.as_u32(),
                        StoreError::MetadataCommandPendingOnNonPrimary {
                            node_id: node_id.as_u32(),
                            primary_node_id: primary_node_id.as_u32(),
                            pg_id: pg_id.get(),
                            cluster_epoch,
                        },
                    ));
                }
                let marker_deadline = Instant::now()
                    .checked_add(super::request_ops::METADATA_COMMAND_PUBLICATION_CONFIRM_BUDGET)
                    .expect("metadata command marker deadline must fit in Instant");
                primary_pending_publication_started = node
                    .metadata_command_client()
                    .open_metadata_command_critical_section_until(
                        pg_id,
                        cluster_epoch,
                        marker_deadline,
                    )
                    .and_then(|section| {
                        section.pending_metadata_command_publication_started_until(
                            pending,
                            marker_deadline,
                        )
                    })
                    .map_err(|source| {
                        ClusterBuildError::open_local_node(node_id.as_u32(), source)
                    })?;
                primary_pending_command = pending_command;
            }
            replica_states.push((node_id, state));
        }
        let should_converge_primary_pending =
            validate_metadata_command_replica_agreement_or_in_flight_recovery(
                nodes,
                pg_id,
                primary_node_id,
                cluster_epoch,
                &replica_states,
                primary_pending_command.as_ref(),
                primary_pending_publication_started,
            )?;
        if should_converge_primary_pending {
            let command = primary_pending_command
                .as_ref()
                .expect("in-flight recovery convergence requires a primary pending command");
            converge_in_flight_metadata_command_on_open(
                nodes,
                pg_routes,
                pg_id,
                primary_node_id,
                cluster_epoch,
                command,
                primary_pending_publication_started,
            )?;
        } else if let Some(command) = primary_pending_command.as_ref() {
            let primary_state = replica_states
                .iter()
                .find_map(|(node_id, state)| (*node_id == primary_node_id).then_some(state))
                .expect("validated route primary must be in local node set");
            if command.id().log_index().get() <= primary_state.applied_log_index {
                release_open_applied_metadata_command_bucket_write_reservations(
                    nodes, pg_routes, command,
                )?;
                clean_converged_primary_terminal_pending_slot(
                    nodes,
                    pg_id,
                    primary_node_id,
                    command,
                )?;
            }
        }
    }
    Ok(())
}

fn clean_converged_primary_terminal_pending_slot(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_id: PgId,
    primary_node_id: NodeId,
    command: &MetadataCommandEnvelope,
) -> Result<(), ClusterBuildError> {
    let primary = nodes
        .get(&primary_node_id)
        .expect("validated route primary must be in local node set");
    primary
        .metadata_command_client()
        .remove_pending_metadata_command_slot(pg_id, command)
        .map_err(|source| ClusterBuildError::open_local_node(primary_node_id.as_u32(), source))?;
    Ok(())
}

fn release_open_applied_metadata_command_bucket_write_reservations(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_routes: &BTreeMap<PgId, LocalPgRoute>,
    command: &MetadataCommandEnvelope,
) -> Result<(), ClusterBuildError> {
    if matches!(
        command.payload(),
        crate::metadata_command::MetadataCommandPayload::CreateStreamUpload(create)
            if create.session.target == crate::StreamUploadTarget::PutObject
    ) {
        return Ok(());
    }
    let topology = nodes
        .values()
        .next()
        .expect("local cluster must contain at least one node")
        .runtime()
        .pg_topology();
    for proof in command
        .payload()
        .bucket_write_reservation_route_dependencies()
    {
        let bucket_pg_id = PgId::new(topology.bucket_pg_for(&proof.bucket));
        let primary_node_id = pg_routes
            .get(&bucket_pg_id)
            .expect("validated bucket PG id should have a route")
            .primary_node_id();
        let node = nodes
            .get(&primary_node_id)
            .expect("validated route primary must be in local node set");
        let bucket_pg = node.runtime().bucket_metadata_pg_for(&proof.bucket);
        node.retained_bucket_write_reservation_client()
            .open_retained_bucket_write_reservation_route(bucket_pg, &proof.bucket)
            .and_then(|route| route.release_metadata_command_bucket_write_reservation(proof))
            .map_err(|source| {
                ClusterBuildError::open_local_node(
                    primary_node_id.as_u32(),
                    StoreError::Io {
                        context:
                            "release metadata command bucket write reservations on local cluster open",
                        source: std::io::Error::other(source),
                    },
                )
            })?;
    }
    Ok(())
}

fn release_open_unpublished_metadata_command_bucket_write_reservation(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_routes: &BTreeMap<PgId, LocalPgRoute>,
    command: &MetadataCommandEnvelope,
) -> Result<(), ClusterBuildError> {
    let Some(proof) = command.payload().primary_bucket_write_reservation_proof() else {
        return Ok(());
    };
    release_open_bucket_write_reservation(nodes, pg_routes, proof)
}

fn release_open_bucket_write_reservation(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_routes: &BTreeMap<PgId, LocalPgRoute>,
    proof: &crate::metadata_command::BucketWriteReservationProof,
) -> Result<(), ClusterBuildError> {
    let topology = nodes
        .values()
        .next()
        .expect("local cluster must contain at least one node")
        .runtime()
        .pg_topology();
    let bucket_pg_id = PgId::new(topology.bucket_pg_for(&proof.bucket));
    let primary_node_id = pg_routes
        .get(&bucket_pg_id)
        .expect("validated bucket PG id should have a route")
        .primary_node_id();
    let node = nodes
        .get(&primary_node_id)
        .expect("validated route primary must be in local node set");
    let bucket_pg = node.runtime().bucket_metadata_pg_for(&proof.bucket);
    node.retained_bucket_write_reservation_client()
        .open_retained_bucket_write_reservation_route(bucket_pg, &proof.bucket)
        .and_then(|route| route.release_metadata_command_bucket_write_reservation(proof))
        .map_err(|source| {
            ClusterBuildError::open_local_node(
                primary_node_id.as_u32(),
                StoreError::Io {
                    context:
                        "release metadata command bucket write reservation on local cluster open",
                    source: std::io::Error::other(source),
                },
            )
        })
}

fn converge_in_flight_metadata_command_on_open(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_routes: &BTreeMap<PgId, LocalPgRoute>,
    pg_id: PgId,
    primary_node_id: NodeId,
    cluster_epoch: ClusterEpoch,
    command: &MetadataCommandEnvelope,
    publication_started: bool,
) -> Result<(), ClusterBuildError> {
    let mut nodes_primary_last = nodes.iter().collect::<Vec<_>>();
    nodes_primary_last.sort_by_key(|(node_id, _node)| **node_id == primary_node_id);
    let command_already_irrevocable = publication_started
        || nodes_primary_last
            .iter()
            .try_fold(false, |witnessed, (node_id, node)| {
                node.metadata_command_inspection_client()
                    .applied_metadata_command_log_entry_hashes(pg_id, command)
                    .map(|hashes| witnessed || hashes.is_some())
                    .map_err(|source| ClusterBuildError::open_local_node(node_id.as_u32(), source))
            })?;
    if !command_already_irrevocable {
        validate_open_metadata_command_bucket_write_reservation(nodes, pg_routes, command)?;
    }
    for (node_id, node) in nodes_primary_last {
        node.metadata_command_client()
            .apply_metadata_command_and_record(pg_id, command)
            .map_err(|source| {
                ClusterBuildError::open_local_node(
                    node_id.as_u32(),
                    bucket_snapshot_error_to_store_error(source),
                )
            })?;
        #[cfg(test)]
        maybe_run_open_metadata_command_after_apply_hook(nodes, pg_routes, *node_id, command);
    }

    release_open_applied_metadata_command_bucket_write_reservations(nodes, pg_routes, command)?;

    let mut converged_states = Vec::new();
    for node in nodes.values() {
        let node_id = node.node_id();
        let peering_route = node
            .metadata_command_peering_client()
            .open_metadata_command_peering_route(pg_id, cluster_epoch)
            .map_err(|source| ClusterBuildError::open_local_node(node_id.as_u32(), source))?;
        let state = peering_route
            .validate_metadata_command_replay_state()
            .map_err(|source| ClusterBuildError::open_local_node(node_id.as_u32(), source))?;
        converged_states.push((node_id, state));
    }
    validate_metadata_command_replica_agreement_or_in_flight_recovery(
        nodes,
        pg_id,
        primary_node_id,
        cluster_epoch,
        &converged_states,
        None,
        false,
    )
    .map(|_| ())
}

fn validate_open_metadata_command_bucket_write_reservation(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_routes: &BTreeMap<PgId, LocalPgRoute>,
    command: &MetadataCommandEnvelope,
) -> Result<(), ClusterBuildError> {
    let Some(proof) = command_bucket_write_reservation_proof(command) else {
        return Ok(());
    };
    if proof.cluster_epoch != command.id().cluster_epoch() {
        return Err(ClusterBuildError::open_local_node(
            0,
            StoreError::Io {
                context: "validate metadata command bucket write reservation on local cluster open",
                source: std::io::Error::other(MetadataError::BucketWriteReservationConflict {
                    reservation_id: proof.reservation_id.clone(),
                }),
            },
        ));
    }
    let topology = nodes
        .values()
        .next()
        .expect("local cluster must contain at least one node")
        .runtime()
        .pg_topology();
    let bucket_pg_id = PgId::new(topology.bucket_pg_for(&proof.bucket));
    let primary_node_id = pg_routes
        .get(&bucket_pg_id)
        .expect("validated bucket PG id should have a route")
        .primary_node_id();
    let node = nodes
        .get(&primary_node_id)
        .expect("validated route primary must be in local node set");
    let bucket_pg = node.runtime().bucket_metadata_pg_for(&proof.bucket);
    node.bucket_write_reservation_client()
        .open_bucket_write_reservation_route(proof.cluster_epoch, bucket_pg, &proof.bucket)
        .and_then(|route| route.validate_bucket_write_reservation_proof(proof))
        .map_err(|source| {
            ClusterBuildError::open_local_node(
                primary_node_id.as_u32(),
                StoreError::Io {
                    context:
                        "validate metadata command bucket write reservation on local cluster open",
                    source: std::io::Error::other(source),
                },
            )
        })
}

fn command_bucket_write_reservation_proof(
    command: &MetadataCommandEnvelope,
) -> Option<&crate::metadata_command::BucketWriteReservationProof> {
    command.payload().primary_bucket_write_reservation_proof()
}

fn bucket_snapshot_error_to_store_error(error: crate::BucketSnapshotLoadError) -> StoreError {
    match error {
        crate::BucketSnapshotLoadError::Store(error) => error,
        crate::BucketSnapshotLoadError::Metadata(source) => StoreError::Io {
            context: "recover in-flight metadata command on local cluster open",
            source: std::io::Error::other(source),
        },
    }
}

fn validate_metadata_command_replica_agreement_or_in_flight_recovery(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_id: PgId,
    primary_node_id: NodeId,
    cluster_epoch: ClusterEpoch,
    replica_states: &[(NodeId, MetadataCommandReplicaState)],
    primary_pending_command: Option<&MetadataCommandEnvelope>,
    primary_pending_publication_started: bool,
) -> Result<bool, ClusterBuildError> {
    let Some((reference_node_id, reference_state)) = replica_states.first() else {
        return Ok(false);
    };
    let replicas_equal = replica_states
        .iter()
        .all(|(_node_id, state)| state == reference_state);
    if !primary_pending_publication_started && replicas_equal {
        return Ok(false);
    }

    let primary_state = replica_states
        .iter()
        .find_map(|(node_id, state)| (*node_id == primary_node_id).then_some(state))
        .expect("validated route primary must be in local node set");
    let Some(command) = primary_pending_command else {
        let (node_id, state) = first_divergent_replica(replica_states, *reference_node_id);
        return Err(metadata_command_replica_state_diverged_error(
            pg_id,
            node_id,
            *reference_node_id,
            state,
            reference_state,
        ));
    };
    if command.id().cluster_epoch() != cluster_epoch || command.id().pg_id() != pg_id {
        return Err(metadata_command_replica_state_diverged_error(
            pg_id,
            primary_node_id,
            *reference_node_id,
            primary_state,
            reference_state,
        ));
    }
    let command_index = command.id().log_index().get();
    if replicas_equal && primary_state.applied_log_index == command_index {
        let mut expected_hashes = None;
        for (node_id, state) in replica_states {
            let node = nodes
                .get(node_id)
                .expect("replica state node must exist in local node set");
            let Some(hashes) = node
                .metadata_command_inspection_client()
                .applied_metadata_command_log_entry_hashes(pg_id, command)
                .map_err(|source| ClusterBuildError::open_local_node(node_id.as_u32(), source))?
            else {
                return Err(metadata_command_replica_state_diverged_error(
                    pg_id,
                    *node_id,
                    primary_node_id,
                    state,
                    primary_state,
                ));
            };
            if expected_hashes.is_some_and(|expected| expected != hashes) {
                return Err(metadata_command_replica_state_diverged_error(
                    pg_id,
                    *node_id,
                    primary_node_id,
                    state,
                    primary_state,
                ));
            }
            expected_hashes = Some(hashes);
        }
        return Ok(false);
    }
    if primary_state.applied_log_index == command_index {
        let previous_index = command_index.checked_sub(1).ok_or_else(|| {
            metadata_command_replica_state_diverged_error(
                pg_id,
                primary_node_id,
                *reference_node_id,
                primary_state,
                reference_state,
            )
        })?;
        let mut unadvanced_reference: Option<(NodeId, &MetadataCommandReplicaState)> = None;
        let mut advanced_reference: Option<(NodeId, &MetadataCommandReplicaState)> = None;
        for (node_id, state) in replica_states {
            if state.cluster_epoch != primary_state.cluster_epoch {
                return Err(metadata_command_replica_state_diverged_error(
                    pg_id,
                    *node_id,
                    primary_node_id,
                    state,
                    primary_state,
                ));
            }
            if state.applied_log_index == previous_index {
                if let Some((reference_node_id, reference_state)) = unadvanced_reference {
                    if state != reference_state {
                        return Err(metadata_command_replica_state_diverged_error(
                            pg_id,
                            *node_id,
                            reference_node_id,
                            state,
                            reference_state,
                        ));
                    }
                } else {
                    unadvanced_reference = Some((*node_id, state));
                }
                continue;
            }
            if state.applied_log_index != command_index {
                return Err(metadata_command_replica_state_diverged_error(
                    pg_id,
                    *node_id,
                    primary_node_id,
                    state,
                    primary_state,
                ));
            }
            if let Some((reference_node_id, reference_state)) = advanced_reference {
                if state != reference_state {
                    return Err(metadata_command_replica_state_diverged_error(
                        pg_id,
                        *node_id,
                        reference_node_id,
                        state,
                        reference_state,
                    ));
                }
            } else {
                advanced_reference = Some((*node_id, state));
            }
        }
        let Some((_unadvanced_node_id, unadvanced_state)) = unadvanced_reference else {
            let (node_id, state) = first_divergent_replica(replica_states, primary_node_id);
            return Err(metadata_command_replica_state_diverged_error(
                pg_id,
                node_id,
                primary_node_id,
                state,
                primary_state,
            ));
        };
        for (node_id, state) in replica_states
            .iter()
            .filter(|(_node_id, state)| state.applied_log_index == command_index)
        {
            let node = nodes
                .get(node_id)
                .expect("replica state node must exist in local node set");
            let matches_pending = node
                .metadata_command_inspection_client()
                .has_matching_applied_metadata_command_log_entry(
                    pg_id,
                    command,
                    unadvanced_state.applied_log_hash.value(),
                )
                .map_err(|source| ClusterBuildError::open_local_node(node_id.as_u32(), source))?;
            if !matches_pending {
                return Err(metadata_command_replica_state_diverged_error(
                    pg_id,
                    *node_id,
                    primary_node_id,
                    state,
                    primary_state,
                ));
            }
        }
        return Ok(true);
    }
    let next_index = primary_state
        .applied_log_index
        .checked_add(1)
        .ok_or_else(|| {
            metadata_command_replica_state_diverged_error(
                pg_id,
                primary_node_id,
                *reference_node_id,
                primary_state,
                reference_state,
            )
        })?;
    if command_index != next_index {
        return Err(metadata_command_replica_state_diverged_error(
            pg_id,
            primary_node_id,
            *reference_node_id,
            primary_state,
            reference_state,
        ));
    }

    let mut advanced_reference: Option<(NodeId, &MetadataCommandReplicaState)> = None;
    for (node_id, state) in replica_states {
        if state == primary_state {
            continue;
        }
        if state.cluster_epoch != primary_state.cluster_epoch
            || state.applied_log_index != next_index
        {
            return Err(metadata_command_replica_state_diverged_error(
                pg_id,
                *node_id,
                primary_node_id,
                state,
                primary_state,
            ));
        }
        let node = nodes
            .get(node_id)
            .expect("replica state node must exist in local node set");
        let matches_pending = node
            .metadata_command_inspection_client()
            .has_matching_applied_metadata_command_log_entry(
                pg_id,
                command,
                primary_state.applied_log_hash.value(),
            )
            .map_err(|source| ClusterBuildError::open_local_node(node_id.as_u32(), source))?;
        if !matches_pending {
            return Err(metadata_command_replica_state_diverged_error(
                pg_id,
                *node_id,
                primary_node_id,
                state,
                primary_state,
            ));
        }
        if let Some((advanced_reference_node_id, advanced_reference_state)) = advanced_reference {
            if state != advanced_reference_state {
                return Err(metadata_command_replica_state_diverged_error(
                    pg_id,
                    *node_id,
                    advanced_reference_node_id,
                    state,
                    advanced_reference_state,
                ));
            }
        } else {
            advanced_reference = Some((*node_id, state));
        }
    }
    Ok(true)
}

fn first_divergent_replica(
    replica_states: &[(NodeId, MetadataCommandReplicaState)],
    reference_node_id: NodeId,
) -> (NodeId, &MetadataCommandReplicaState) {
    let reference_state = replica_states
        .iter()
        .find_map(|(node_id, state)| (*node_id == reference_node_id).then_some(state))
        .expect("reference node must be present");
    replica_states
        .iter()
        .find(|(_node_id, state)| *state != *reference_state)
        .map(|(node_id, state)| (*node_id, state))
        .expect("caller must provide divergent replica states")
}

fn metadata_command_replica_state_diverged_error(
    pg_id: PgId,
    node_id: NodeId,
    reference_node_id: NodeId,
    state: &MetadataCommandReplicaState,
    reference_state: &MetadataCommandReplicaState,
) -> ClusterBuildError {
    ClusterBuildError::open_local_node(
        node_id.as_u32(),
        StoreError::MetadataCommandReplicaStateDiverged {
            node_id: node_id.as_u32(),
            reference_node_id: reference_node_id.as_u32(),
            pg_id: pg_id.get(),
            cluster_epoch: state.cluster_epoch,
            reference_cluster_epoch: reference_state.cluster_epoch,
            applied_log_index: state.applied_log_index,
            reference_applied_log_index: reference_state.applied_log_index,
            applied_log_hash: state.applied_log_hash.value(),
            reference_applied_log_hash: reference_state.applied_log_hash.value(),
            state_digest: state.state_digest.value(),
            reference_state_digest: reference_state.state_digest.value(),
        },
    )
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

struct ValidatedEmbeddedLocalTopology {
    node_ids: BTreeSet<NodeId>,
    pg_ids: Vec<PgId>,
    configs: Vec<(NodeId, PathBuf)>,
}

fn validate_embedded_local_topology(
    metadata_primary_node_id: NodeId,
    configs: impl IntoIterator<Item = LocalNodeStoreConfig>,
    pg_ids: &[u32],
    default_ec_shape: EcShape,
) -> Result<ValidatedEmbeddedLocalTopology, ClusterBuildError> {
    let configs = configs.into_iter().collect::<Vec<_>>();
    if configs.is_empty() {
        return Err(ClusterBuildError::EmptyCluster);
    }

    let mut node_ids = BTreeSet::new();
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
        if let Some(first_node_id) = data_dirs.insert(canonical_data_dir.clone(), config.node_id) {
            return Err(ClusterBuildError::DuplicateDataDir {
                first_node_id: first_node_id.as_u32(),
                duplicate_node_id: config.node_id.as_u32(),
                data_dir: canonical_data_dir,
            });
        }
        validated_configs.push((config.node_id, canonical_data_dir));
    }

    Ok(ValidatedEmbeddedLocalTopology {
        node_ids,
        pg_ids,
        configs: validated_configs,
    })
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
    let placement_config = placement::PlacementConfig::new(ec_config.total_shards())
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
    prepare_private_data_dir(data_dir).map_err(|source| {
        ClusterBuildError::open_local_node(
            node_id.as_u32(),
            StoreError::Io {
                context: "prepare private local node data dir",
                source,
            },
        )
    })?;
    data_dir.canonicalize().map_err(|source| {
        ClusterBuildError::open_local_node(
            node_id.as_u32(),
            StoreError::Io {
                context: "canonicalize local node data dir",
                source,
            },
        )
    })
}

#[cfg(test)]
mod tests;
