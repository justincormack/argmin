use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use placement::{NodeId, PlacementConstraint, PlacementError, TopologyKey};

use super::ShardLocation;
use crate::control_plane::{ClusterRuntimeMapSnapshot, NodeRouteSnapshot, PgRouteSnapshot};
use crate::error::{ClusterBuildError, ShardIoError, StoreError};
#[cfg(test)]
use crate::metadata_command::MetadataCommandLogIndex;
use crate::metadata_command::{
    MetadataCommandAcceptance, MetadataCommandEnvelope, MetadataCommandReplicaState,
};
use crate::node::{ReclaimQueueInsert, OBJECT_PAYLOAD_RECLAIM_MAX_OUTSTANDING_PER_PG};
use crate::node_client::{
    BucketMetadataNodeClient, BucketWriteReservationNodeClient, DirectPutMetadataNodeClient,
    LocalStorageNodeClient, MetadataCommandNodeClient, ObjectGenerationMetadataNodeClient,
    ObjectListingMetadataNodeClient, ObjectMutationMetadataNodeClient,
    ObjectReadMetadataNodeClient, ObjectVersionMetadataNodeClient, PlacedShardNodeClient,
    ShardAckNodeClient, ShardReadHandleNodeClient, ShardScavengerNodeClient, StorageNodeClient,
    UnixStorageNodeClient, UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_LIMIT,
    UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
    UNIX_STORAGE_NODE_MIN_RPC_ADMISSION_LIMIT,
};
use crate::pg_topology::PgTopology;
use crate::{
    BucketName, ClusterEpoch, DataPgId, EcShape, GenerationId, MetadataError, ObjectKey, PgId,
    PgState, ReclaimWorkItem, ShardIndex, ShardKey, SharedStorageNode, WriteAck, WrittenShardAck,
};

const PAYLOAD_SHARD_PLACEMENT_KEY_DOMAIN: &[u8] = b"argmin/payload-shard-placement/v1";
const LOCAL_RECLAIM_WORKER_WAIT_POLL_MILLIS: u64 = 100;
const METADATA_COMMAND_RECOVERY_WAIT_TIMEOUT: Duration = Duration::from_secs(1);

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalUnixStorageNodeClientConfig {
    node_id: NodeId,
    socket_path: PathBuf,
    rpc_admission_limit: usize,
    rpc_admission_wait_timeout: Duration,
    rpc_control_admission_wait_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalUnixStorageNodeClientAdmissionSettings {
    rpc_admission_limit: usize,
    rpc_admission_wait_timeout: Duration,
    rpc_control_admission_wait_timeout: Duration,
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
            socket_path: socket_path.into(),
            rpc_admission_limit: Self::DEFAULT_RPC_ADMISSION_LIMIT,
            rpc_admission_wait_timeout: Self::DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
            rpc_control_admission_wait_timeout: Self::DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
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
            socket_path: socket_path.into(),
            rpc_admission_limit,
            rpc_admission_wait_timeout: Self::DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
            rpc_control_admission_wait_timeout: Self::DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
        }
    }

    pub fn with_rpc_admission(
        node_id: NodeId,
        socket_path: impl Into<PathBuf>,
        rpc_admission_limit: usize,
        rpc_admission_wait_timeout: Duration,
        rpc_control_admission_wait_timeout: Duration,
    ) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
            rpc_admission_limit,
            rpc_admission_wait_timeout,
            rpc_control_admission_wait_timeout,
        }
    }

    pub fn with_rpc_admission_settings(
        node_id: NodeId,
        socket_path: impl Into<PathBuf>,
        settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Self {
        Self {
            node_id,
            socket_path: socket_path.into(),
            rpc_admission_limit: settings.rpc_admission_limit,
            rpc_admission_wait_timeout: settings.rpc_admission_wait_timeout,
            rpc_control_admission_wait_timeout: settings.rpc_control_admission_wait_timeout,
        }
    }

    pub fn with_rpc_admission_from_runtime_node_route(
        node: &NodeRouteSnapshot,
        rpc_admission_limit: usize,
        rpc_admission_wait_timeout: Duration,
        rpc_control_admission_wait_timeout: Duration,
    ) -> Self {
        Self::with_rpc_admission(
            node.node_id(),
            node.endpoint(),
            rpc_admission_limit,
            rpc_admission_wait_timeout,
            rpc_control_admission_wait_timeout,
        )
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

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
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

impl LocalUnixStorageNodeClientAdmissionSettings {
    pub const DEFAULT: Self = Self {
        rpc_admission_limit: LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_LIMIT,
        rpc_admission_wait_timeout:
            LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
        rpc_control_admission_wait_timeout:
            LocalUnixStorageNodeClientConfig::DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
    };

    pub fn new(
        rpc_admission_limit: usize,
        rpc_admission_wait_timeout: Duration,
        rpc_control_admission_wait_timeout: Duration,
    ) -> Self {
        Self {
            rpc_admission_limit,
            rpc_admission_wait_timeout,
            rpc_control_admission_wait_timeout,
        }
    }

    pub fn rpc_admission_limit(self) -> usize {
        self.rpc_admission_limit
    }

    pub fn rpc_admission_wait_timeout(self) -> Duration {
        self.rpc_admission_wait_timeout
    }

    pub fn rpc_control_admission_wait_timeout(self) -> Duration {
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

pub struct LocalNodeStore {
    node_id: NodeId,
    data_dir: PathBuf,
    storage_node: Arc<SharedStorageNode>,
    storage_client: Arc<dyn StorageNodeClient>,
    bucket_metadata_client: Arc<dyn BucketMetadataNodeClient>,
    bucket_metadata_unix_socket_path: Option<PathBuf>,
    bucket_write_reservation_client: Arc<dyn BucketWriteReservationNodeClient>,
    bucket_write_reservation_unix_socket_path: Option<PathBuf>,
    object_generation_metadata_client: Arc<dyn ObjectGenerationMetadataNodeClient>,
    object_version_metadata_client: Arc<dyn ObjectVersionMetadataNodeClient>,
    direct_put_metadata_client: Arc<dyn DirectPutMetadataNodeClient>,
    object_listing_metadata_client: Arc<dyn ObjectListingMetadataNodeClient>,
    object_mutation_metadata_client: Arc<dyn ObjectMutationMetadataNodeClient>,
    object_read_metadata_client: Arc<dyn ObjectReadMetadataNodeClient>,
    metadata_command_client: Arc<dyn MetadataCommandNodeClient>,
    shard_client: Arc<dyn PlacedShardNodeClient>,
    shard_ack_client: Arc<dyn ShardAckNodeClient>,
    shard_read_handle_client: Arc<dyn ShardReadHandleNodeClient>,
    shard_scavenger_client: Arc<dyn ShardScavengerNodeClient>,
}

impl LocalNodeStore {
    fn new(node_id: NodeId, data_dir: PathBuf, storage_node: Arc<SharedStorageNode>) -> Self {
        let local_client = Arc::new(LocalStorageNodeClient::new(
            node_id,
            Arc::clone(&storage_node),
        ));
        let storage_client: Arc<dyn StorageNodeClient> = local_client.clone();
        let bucket_metadata_client: Arc<dyn BucketMetadataNodeClient> = local_client.clone();
        let bucket_write_reservation_client: Arc<dyn BucketWriteReservationNodeClient> =
            local_client.clone();
        let object_generation_metadata_client: Arc<dyn ObjectGenerationMetadataNodeClient> =
            local_client.clone();
        let object_version_metadata_client: Arc<dyn ObjectVersionMetadataNodeClient> =
            local_client.clone();
        let direct_put_metadata_client: Arc<dyn DirectPutMetadataNodeClient> = local_client.clone();
        let object_listing_metadata_client: Arc<dyn ObjectListingMetadataNodeClient> =
            local_client.clone();
        let object_mutation_metadata_client: Arc<dyn ObjectMutationMetadataNodeClient> =
            local_client.clone();
        let object_read_metadata_client: Arc<dyn ObjectReadMetadataNodeClient> =
            local_client.clone();
        let metadata_command_client: Arc<dyn MetadataCommandNodeClient> = local_client.clone();
        let shard_client: Arc<dyn PlacedShardNodeClient> = local_client.clone();
        let shard_ack_client: Arc<dyn ShardAckNodeClient> = local_client.clone();
        let shard_read_handle_client: Arc<dyn ShardReadHandleNodeClient> = local_client.clone();
        let shard_scavenger_client: Arc<dyn ShardScavengerNodeClient> = local_client;
        Self {
            node_id,
            data_dir,
            storage_node,
            storage_client,
            bucket_metadata_client,
            bucket_metadata_unix_socket_path: None,
            bucket_write_reservation_client,
            bucket_write_reservation_unix_socket_path: None,
            object_generation_metadata_client,
            object_version_metadata_client,
            direct_put_metadata_client,
            object_listing_metadata_client,
            object_mutation_metadata_client,
            object_read_metadata_client,
            metadata_command_client,
            shard_client,
            shard_ack_client,
            shard_read_handle_client,
            shard_scavenger_client,
        }
    }

    fn topology_only(
        node_id: NodeId,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
    ) -> Result<Self, StoreError> {
        let storage_node = Arc::new(SharedStorageNode::topology_only(pg_ids, default_ec_shape)?);
        Ok(Self::new(node_id, PathBuf::new(), storage_node))
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

    pub(crate) fn storage_client(&self) -> &Arc<dyn StorageNodeClient> {
        &self.storage_client
    }

    pub(crate) fn bucket_metadata_client(&self) -> &Arc<dyn BucketMetadataNodeClient> {
        &self.bucket_metadata_client
    }

    pub(crate) fn bucket_write_reservation_client(
        &self,
    ) -> &Arc<dyn BucketWriteReservationNodeClient> {
        &self.bucket_write_reservation_client
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

    pub(crate) fn object_read_metadata_client(&self) -> &Arc<dyn ObjectReadMetadataNodeClient> {
        &self.object_read_metadata_client
    }

    pub(crate) fn metadata_command_client(&self) -> &Arc<dyn MetadataCommandNodeClient> {
        &self.metadata_command_client
    }

    pub(crate) fn shard_client(&self) -> &Arc<dyn PlacedShardNodeClient> {
        &self.shard_client
    }

    pub(crate) fn shard_ack_client(&self) -> &Arc<dyn ShardAckNodeClient> {
        &self.shard_ack_client
    }

    pub(crate) fn shard_read_handle_client(&self) -> &Arc<dyn ShardReadHandleNodeClient> {
        &self.shard_read_handle_client
    }

    pub(crate) fn shard_scavenger_client(&self) -> &Arc<dyn ShardScavengerNodeClient> {
        &self.shard_scavenger_client
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
    client: &'a dyn PlacedShardNodeClient,
    read_handle_client: &'a dyn ShardReadHandleNodeClient,
    cluster_epoch: ClusterEpoch,
    data_pg_id: DataPgId,
    location: ShardLocation,
}

impl LocalShardNodeClient<'_> {
    fn write_shard(&self, key: &ShardKey, data: &[u8]) -> Result<WriteAck, ShardIoError> {
        self.client
            .write_placed_shard(self.data_pg_id, key, data)
            .map_err(|source| self.store_error(source))
    }

    fn read_shard(&self, key: &ShardKey, expected: WriteAck) -> Result<Vec<u8>, ShardIoError> {
        let mut read_handle = self.acquire_read_handle(key)?;
        let data_result = self
            .client
            .read_placed_shard(self.data_pg_id, key, expected)
            .map_err(|source| self.store_error(source));
        if let Err(error) = read_handle.release() {
            return Err(self.store_error(error));
        }
        let data = data_result?;
        self.verify_read_ack(expected, &data)?;
        Ok(data)
    }

    #[cfg(test)]
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
        let mut read_handle = self.acquire_read_handle(key)?;
        let read_result = self
            .client
            .read_placed_shard_into(self.data_pg_id, key, expected, dst)
            .map_err(|source| self.store_error(source));
        if let Err(error) = read_handle.release() {
            return Err(self.store_error(error));
        }
        read_result?;
        self.verify_read_ack(expected, dst)
    }

    fn read_shard_into_without_handle(
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
        self.client
            .read_placed_shard_into(self.data_pg_id, key, expected, dst)
            .map_err(|source| self.store_error(source))?;
        self.verify_read_ack(expected, dst)
    }

    fn delete_shard(&self, key: &ShardKey) -> Result<(), ShardIoError> {
        self.client
            .delete_placed_shard(self.data_pg_id, key)
            .map_err(|source| self.store_error(source))
    }

    fn acquire_read_handle(
        &self,
        key: &ShardKey,
    ) -> Result<Box<dyn crate::node_client::ShardReadHandleLease>, ShardIoError> {
        self.read_handle_client
            .acquire_read_handles(
                &self.read_operation_id(key),
                vec![(self.location, key.clone())],
            )
            .map_err(|source| self.store_error(source))
    }

    fn read_operation_id(&self, key: &ShardKey) -> String {
        let hex_key = key.hex_bytes();
        let hex_key = std::str::from_utf8(&hex_key).expect("shard key hex is valid ASCII");
        format!(
            "read:{}:{}:{}:{}",
            self.cluster_epoch.get(),
            self.data_pg_id.get(),
            self.node_id.as_u32(),
            hex_key
        )
    }

    fn read_operation_id_for_keys(&self, keys: &[ShardKey]) -> String {
        let mut id = format!(
            "read-batch:{}:{}:{}",
            self.cluster_epoch.get(),
            self.data_pg_id.get(),
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
}

impl Drop for LocalShardReadHandleSet {
    fn drop(&mut self) {
        let _ = self.release();
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

impl From<&PgRouteSnapshot> for LocalPgRoute {
    fn from(route: &PgRouteSnapshot) -> Self {
        Self {
            cluster_epoch: route.cluster_epoch(),
            pg_id: route.pg_id(),
            primary_node_id: route.primary_node_id(),
            acting_set: Arc::from(route.acting_set()),
            state: route.state(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct LocalClusterRuntimeState {
    reclaim_queue: (Mutex<LocalReclaimQueueState>, Condvar),
    metadata_command_pg_locks: Mutex<HashMap<PgId, Arc<Mutex<()>>>>,
    metadata_command_recovery_flights:
        Arc<Mutex<HashMap<MetadataCommandRecoveryKey, Arc<MetadataCommandRecoveryFlight>>>>,
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
    in_progress: Mutex<bool>,
    done: Condvar,
}

#[derive(Debug)]
pub(crate) enum MetadataCommandRecoveryAdmission {
    Leader(MetadataCommandRecoveryGuard),
    Waited { wait_us: u128 },
    TimedOut { wait_us: u128 },
}

#[derive(Debug)]
pub(crate) struct MetadataCommandRecoveryGuard {
    key: MetadataCommandRecoveryKey,
    flight: Arc<MetadataCommandRecoveryFlight>,
    flights: Arc<Mutex<HashMap<MetadataCommandRecoveryKey, Arc<MetadataCommandRecoveryFlight>>>>,
}

impl Drop for MetadataCommandRecoveryGuard {
    fn drop(&mut self) {
        {
            let mut in_progress = self
                .flight
                .in_progress
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *in_progress = false;
            self.flight.done.notify_all();
        }
        let mut flights = self.flights.lock().unwrap_or_else(|e| e.into_inner());
        if flights
            .get(&self.key)
            .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
        {
            flights.remove(&self.key);
        }
    }
}

type LocalReclaimRoot = (BucketName, ObjectKey, GenerationId);

#[derive(Debug)]
struct LocalReclaimQueueState {
    work_queue: VecDeque<ReclaimWorkItem>,
    queued_objects: HashSet<LocalReclaimRoot>,
    outstanding_objects: HashMap<LocalReclaimRoot, u32>,
    object_payload_outstanding_by_pg: HashMap<u32, usize>,
    queued_bucket_deletes: HashSet<BucketName>,
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
                    queued_bucket_deletes: HashSet::new(),
                }),
                Condvar::new(),
            ),
            metadata_command_pg_locks: Mutex::new(HashMap::new()),
            metadata_command_recovery_flights: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn metadata_command_pg_lock(&self, pg_id: PgId) -> Arc<Mutex<()>> {
        let mut locks = self
            .metadata_command_pg_locks
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        Arc::clone(
            locks
                .entry(pg_id)
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    pub(crate) fn join_metadata_command_recovery(
        &self,
        pg_id: PgId,
        command: &MetadataCommandEnvelope,
    ) -> MetadataCommandRecoveryAdmission {
        let key = MetadataCommandRecoveryKey::new(pg_id, command);
        let flights = Arc::clone(&self.metadata_command_recovery_flights);
        let (flight, is_leader) = {
            let mut flights_guard = flights.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(flight) = flights_guard.get(&key) {
                (Arc::clone(flight), false)
            } else {
                let flight = Arc::new(MetadataCommandRecoveryFlight {
                    in_progress: Mutex::new(true),
                    done: Condvar::new(),
                });
                flights_guard.insert(key, Arc::clone(&flight));
                (flight, true)
            }
        };
        if is_leader {
            return MetadataCommandRecoveryAdmission::Leader(MetadataCommandRecoveryGuard {
                key,
                flight,
                flights,
            });
        }

        let wait_started = Instant::now();
        let (guard, wait_result) = flight
            .done
            .wait_timeout_while(
                flight.in_progress.lock().unwrap_or_else(|e| e.into_inner()),
                METADATA_COMMAND_RECOVERY_WAIT_TIMEOUT,
                |in_progress| *in_progress,
            )
            .unwrap_or_else(|e| e.into_inner());
        let wait_us = wait_started.elapsed().as_micros();
        if *guard {
            debug_assert!(wait_result.timed_out());
            MetadataCommandRecoveryAdmission::TimedOut { wait_us }
        } else {
            MetadataCommandRecoveryAdmission::Waited { wait_us }
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

    pub(crate) fn enqueue_bucket_delete_finalize(&self, bucket: &BucketName) -> bool {
        let (state_lock, cv) = &self.reclaim_queue;
        let mut state = state_lock.lock().unwrap_or_else(|e| e.into_inner());
        let bucket = bucket.clone();
        if state.queued_bucket_deletes.insert(bucket.clone()) {
            state
                .work_queue
                .push_back(ReclaimWorkItem::BucketDelete(bucket));
            Self::emit_reclaim_queue_action(&state, "bucket_delete", "enqueue");
            cv.notify_one();
            true
        } else {
            Self::emit_reclaim_queue_action(&state, "bucket_delete", "deduplicate");
            false
        }
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
            ReclaimWorkItem::BucketDelete(bucket) => {
                state.queued_bucket_deletes.remove(bucket);
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
                bucket_delete_depth: state.queued_bucket_deletes.len(),
            },
        );
    }
}

#[derive(Debug)]
pub struct LocalClusterMap {
    epoch: ClusterEpoch,
    route_map_valid_until_ms: Option<u64>,
    metadata_primary_node_id: NodeId,
    nodes: BTreeMap<NodeId, LocalNodeStore>,
    pg_ids: Box<[u32]>,
    pg_topology: PgTopology,
    default_ec_shape: EcShape,
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
            false,
        )
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
                    |source| ClusterBuildError::OpenLocalNode {
                        node_id: node_id.as_u32(),
                        source,
                    },
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
            route_map_valid_until_ms: None,
            placement_map,
            runtime_state: Arc::new(LocalClusterRuntimeState::new()),
            process_local_registry_key: Arc::as_ptr(metadata_primary.storage_node()) as usize,
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
            None,
        )
    }

    pub fn open_frontend_topology_only_with_pg_routes_and_validity(
        metadata_primary_node_id: NodeId,
        node_ids: impl IntoIterator<Item = NodeId>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
        pg_routes: impl IntoIterator<Item = LocalPgRoute>,
        route_map_valid_until_ms: Option<u64>,
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
                    |source| ClusterBuildError::OpenLocalNode {
                        node_id: node_id.as_u32(),
                        source,
                    },
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
            route_map_valid_until_ms,
            placement_map,
            runtime_state: Arc::new(LocalClusterRuntimeState::new()),
            process_local_registry_key: Arc::as_ptr(metadata_primary.storage_node()) as usize,
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

        Self::open_frontend_topology_only_with_pg_routes_and_validity(
            metadata_primary_node_id,
            node_ids,
            &pg_ids,
            default_ec_shape,
            runtime_map.cluster_epoch(),
            pg_routes,
            runtime_map.valid_until_ms(),
        )
    }

    fn open_with_configs_inner(
        metadata_primary_node_id: NodeId,
        configs: impl IntoIterator<Item = LocalNodeStoreConfig>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
        validate_local_metadata_command_replay: bool,
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
        if validate_local_metadata_command_replay {
            validate_metadata_command_replay_state(&nodes, &pg_routes, &pg_ids, cluster_epoch)?;
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
            route_map_valid_until_ms: None,
            placement_map,
            runtime_state: Arc::new(LocalClusterRuntimeState::new()),
            process_local_registry_key: Arc::as_ptr(metadata_primary.storage_node()) as usize,
            nodes,
        })
    }

    pub fn epoch(&self) -> ClusterEpoch {
        self.epoch
    }

    pub fn route_map_valid_until_ms(&self) -> Option<u64> {
        self.route_map_valid_until_ms
    }

    pub fn is_route_map_valid_at(&self, now_ms: u64) -> bool {
        self.route_map_valid_until_ms
            .is_none_or(|valid_until_ms| valid_until_ms > now_ms)
    }

    pub fn require_route_map_valid_at(&self, now_ms: u64) -> Result<(), StoreError> {
        match self.route_map_valid_until_ms {
            Some(valid_until_ms) if valid_until_ms <= now_ms => Err(StoreError::RouteMapExpired {
                cluster_epoch: self.epoch,
                valid_until_ms,
                now_ms,
            }),
            _ => Ok(()),
        }
    }

    fn require_route_map_valid_now(&self) -> Result<(), StoreError> {
        self.require_route_map_valid_at(crate::clock::current_time_millis())
    }

    fn require_route_map_valid_now_for_placement(
        &self,
        pg_id: PgId,
    ) -> Result<(), ClusterBuildError> {
        let now_ms = crate::clock::current_time_millis();
        match self.route_map_valid_until_ms {
            Some(valid_until_ms) if valid_until_ms <= now_ms => {
                Err(ClusterBuildError::RouteMapExpired {
                    pg_id: pg_id.get(),
                    cluster_epoch: self.epoch,
                    valid_until_ms,
                    now_ms,
                })
            }
            _ => Ok(()),
        }
    }

    fn require_route_map_valid_now_for_shard_io(
        &self,
        pg_id: PgId,
        node_id: NodeId,
    ) -> Result<(), ShardIoError> {
        let now_ms = crate::clock::current_time_millis();
        match self.route_map_valid_until_ms {
            Some(valid_until_ms) if valid_until_ms <= now_ms => {
                Err(ShardIoError::RouteMapExpired {
                    node_id: node_id.as_u32(),
                    pg_id: pg_id.get(),
                    cluster_epoch: self.epoch,
                    valid_until_ms,
                    now_ms,
                })
            }
            _ => Ok(()),
        }
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

    pub fn pg_ids(&self) -> &[u32] {
        &self.pg_ids
    }

    pub fn node(&self, node_id: NodeId) -> Option<&LocalNodeStore> {
        self.nodes.get(&node_id)
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
            let shard_ack_client: Arc<dyn ShardAckNodeClient> = client.clone();
            let shard_read_handle_client: Arc<dyn ShardReadHandleNodeClient> = client.clone();
            let shard_scavenger_client: Arc<dyn ShardScavengerNodeClient> = client;
            node.shard_client = shard_client;
            node.shard_ack_client = shard_ack_client;
            node.shard_read_handle_client = shard_read_handle_client;
            node.shard_scavenger_client = shard_scavenger_client;
        }
        Ok(())
    }

    pub fn install_unix_storage_node_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixStorageNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixStorageNodeClientConfig> = configs.into_iter().collect();
        self.validate_unix_storage_node_client_configs(&configs)?;

        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote storage-node client node must exist");
            let client = Arc::new(UnixStorageNodeClient::with_rpc_admission_settings(
                config.node_id,
                self.epoch,
                config.socket_path.clone(),
                config.rpc_admission_limit,
                config.rpc_admission_wait_timeout,
                config.rpc_control_admission_wait_timeout,
            ));
            let bucket_metadata_client: Arc<dyn BucketMetadataNodeClient> = client.clone();
            let bucket_write_reservation_client: Arc<dyn BucketWriteReservationNodeClient> =
                client.clone();
            let metadata_command_client: Arc<dyn MetadataCommandNodeClient> = client.clone();
            let object_generation_metadata_client: Arc<dyn ObjectGenerationMetadataNodeClient> =
                client.clone();
            let object_version_metadata_client: Arc<dyn ObjectVersionMetadataNodeClient> =
                client.clone();
            let direct_put_metadata_client: Arc<dyn DirectPutMetadataNodeClient> = client.clone();
            let object_listing_metadata_client: Arc<dyn ObjectListingMetadataNodeClient> =
                client.clone();
            let object_mutation_metadata_client: Arc<dyn ObjectMutationMetadataNodeClient> =
                client.clone();
            let object_read_metadata_client: Arc<dyn ObjectReadMetadataNodeClient> = client.clone();
            let shard_client: Arc<dyn PlacedShardNodeClient> = client.clone();
            let shard_ack_client: Arc<dyn ShardAckNodeClient> = client.clone();
            let shard_read_handle_client: Arc<dyn ShardReadHandleNodeClient> = client.clone();
            let shard_scavenger_client: Arc<dyn ShardScavengerNodeClient> = client;

            node.bucket_metadata_client = bucket_metadata_client;
            node.bucket_metadata_unix_socket_path = Some(config.socket_path.clone());
            node.bucket_write_reservation_client = bucket_write_reservation_client;
            node.bucket_write_reservation_unix_socket_path = Some(config.socket_path);
            node.metadata_command_client = metadata_command_client;
            node.object_generation_metadata_client = object_generation_metadata_client;
            node.object_version_metadata_client = object_version_metadata_client;
            node.direct_put_metadata_client = direct_put_metadata_client;
            node.object_listing_metadata_client = object_listing_metadata_client;
            node.object_mutation_metadata_client = object_mutation_metadata_client;
            node.object_read_metadata_client = object_read_metadata_client;
            node.shard_client = shard_client;
            node.shard_ack_client = shard_ack_client;
            node.shard_read_handle_client = shard_read_handle_client;
            node.shard_scavenger_client = shard_scavenger_client;
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
            if !config.socket_path.is_absolute() {
                return Err(
                    ClusterBuildError::RemoteStorageNodeClientSocketPathNotAbsolute {
                        path: config.socket_path.clone(),
                    },
                );
            }
            if !self.nodes.contains_key(&config.node_id) {
                return Err(ClusterBuildError::RemoteStorageNodeClientNodeNotFound {
                    id: config.node_id.as_u32(),
                });
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
            let metadata_command_client: Arc<dyn MetadataCommandNodeClient> = client;
            node.metadata_command_client = metadata_command_client;
        }
        Ok(())
    }

    pub fn install_unix_bucket_metadata_clients(
        &mut self,
        configs: impl IntoIterator<Item = LocalUnixBucketMetadataNodeClientConfig>,
    ) -> Result<(), ClusterBuildError> {
        let configs: Vec<LocalUnixBucketMetadataNodeClientConfig> = configs.into_iter().collect();
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
            let client = Arc::new(UnixStorageNodeClient::new(
                config.node_id,
                self.epoch,
                config.socket_path.clone(),
            ));
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
            let client = Arc::new(UnixStorageNodeClient::new(
                config.node_id,
                self.epoch,
                config.socket_path.clone(),
            ));
            let bucket_write_reservation_client: Arc<dyn BucketWriteReservationNodeClient> = client;
            node.bucket_write_reservation_client = bucket_write_reservation_client;
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
        for config in configs {
            let node = self
                .nodes
                .get_mut(&config.node_id)
                .expect("validated remote object-listing metadata client node must exist");
            let client = Arc::new(UnixStorageNodeClient::new(
                config.node_id,
                self.epoch,
                config.socket_path,
            ));
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
            let object_mutation_metadata_client: Arc<dyn ObjectMutationMetadataNodeClient> = client;
            node.object_mutation_metadata_client = object_mutation_metadata_client;
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

    pub fn process_local_registry_key(&self) -> usize {
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

    pub fn object_generation_segment_data_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segment_index: u32,
    ) -> DataPgId {
        self.pg_topology.object_generation_segment_data_pg(
            bucket,
            key,
            generation_id,
            segment_index,
        )
    }

    pub fn object_generation_multipart_part_data_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        part_number: u32,
    ) -> DataPgId {
        self.pg_topology.object_generation_multipart_part_data_pg(
            bucket,
            key,
            generation_id,
            part_number,
        )
    }

    pub(crate) fn write_erasure_coded_segment_shards_with<F>(
        &self,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        data: &[u8],
        ec: EcShape,
        write_shards: F,
    ) -> Result<Vec<WrittenShardAck>, StoreError>
    where
        F: FnOnce(&[(ShardKey, &[u8])]) -> Result<Vec<(ShardKey, WriteAck)>, StoreError>,
    {
        self.metadata_primary()
            .storage_node()
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
    pub(crate) fn object_payload_lease_storage_clients(&self) -> Vec<Arc<dyn StorageNodeClient>> {
        self.nodes
            .values()
            .map(|node| Arc::clone(node.storage_client()))
            .collect()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn try_acquire_object_payload_lease(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        let mut acquired = Vec::with_capacity(self.nodes.len());
        for node in self.nodes.values() {
            if node
                .storage_client()
                .try_acquire_object_payload_lease(bucket, key, generation_id)
            {
                acquired.push(Arc::clone(node.storage_client()));
                continue;
            }
            for storage_client in acquired {
                storage_client.release_object_payload_lease(bucket, key, generation_id);
            }
            return false;
        }
        true
    }

    pub(crate) fn try_acquire_object_payload_lease_on_locations(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        locations: &[ShardLocation],
    ) -> Result<Vec<Arc<dyn StorageNodeClient>>, StoreError> {
        let mut node_ids = BTreeMap::new();
        for location in locations {
            if location.cluster_epoch() != self.epoch {
                return Err(StoreError::StaleMetadataOperation {
                    pg_id: location.data_pg_id().get(),
                    operation_epoch: location.cluster_epoch(),
                    current_epoch: self.epoch,
                });
            }
            let route = self.pg_routes.get(&location.data_pg_id().pg_id()).ok_or(
                StoreError::ClusterPgNotFound {
                    pg_id: location.data_pg_id().get(),
                    cluster_epoch: self.epoch,
                },
            )?;
            if route.cluster_epoch() != self.epoch {
                return Err(StoreError::StaleMetadataRoute {
                    pg_id: location.data_pg_id().get(),
                    route_epoch: route.cluster_epoch(),
                    current_epoch: self.epoch,
                });
            }
            if !route.is_active() {
                return Err(StoreError::PgNotActive {
                    pg_id: location.data_pg_id().get(),
                    cluster_epoch: self.epoch,
                    state: route.state(),
                });
            }
            if !route.contains_node(location.node_id()) {
                return Err(StoreError::NodeNotInActingSet {
                    node_id: location.node_id().as_u32(),
                    pg_id: location.data_pg_id().get(),
                    cluster_epoch: self.epoch,
                });
            }
            node_ids
                .entry(location.node_id())
                .or_insert_with(|| location.data_pg_id().get());
        }

        let mut storage_clients = Vec::with_capacity(node_ids.len());
        for (node_id, pg_id) in node_ids {
            let node = self.nodes.get(&node_id).ok_or(StoreError::NodeNotFound {
                node_id: node_id.as_u32(),
                pg_id,
                cluster_epoch: self.epoch,
            })?;
            storage_clients.push(Arc::clone(node.storage_client()));
        }

        let mut acquired = Vec::with_capacity(storage_clients.len());
        for storage_client in storage_clients {
            if storage_client.try_acquire_object_payload_lease(bucket, key, generation_id) {
                acquired.push(storage_client);
                continue;
            }
            for storage_client in acquired {
                storage_client.release_object_payload_lease(bucket, key, generation_id);
            }
            return Ok(Vec::new());
        }
        Ok(acquired)
    }

    pub(crate) fn try_begin_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> bool {
        let mut acquired = Vec::with_capacity(self.nodes.len());
        for node in self.nodes.values() {
            if node
                .storage_client()
                .try_begin_object_payload_reclaim(bucket, key, generation_id)
            {
                acquired.push(Arc::clone(node.storage_client()));
                continue;
            }
            for storage_client in acquired {
                storage_client.finish_object_payload_reclaim(bucket, key, generation_id, false);
            }
            return false;
        }
        true
    }

    pub(crate) fn finish_object_payload_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        keep_fence: bool,
    ) {
        for node in self.nodes.values() {
            node.storage_client().finish_object_payload_reclaim(
                bucket,
                key,
                generation_id,
                keep_fence,
            );
        }
    }

    pub(crate) fn clear_object_payload_reclaim_fence(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        for node in self.nodes.values() {
            node.storage_client()
                .clear_object_payload_reclaim_fence(bucket, key, generation_id);
        }
    }

    pub(crate) fn object_payload_lease_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        self.nodes
            .values()
            .map(|node| {
                node.storage_client()
                    .object_payload_lease_count(bucket, key, generation_id)
            })
            .max()
            .unwrap_or(0)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn object_payload_lease_holder_node_count(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> usize {
        self.nodes
            .values()
            .filter(|node| {
                node.storage_client()
                    .object_payload_lease_count(bucket, key, generation_id)
                    != 0
            })
            .count()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn bucket_object_payload_lease_count(&self, bucket: &BucketName) -> usize {
        self.nodes
            .values()
            .map(|node| {
                node.storage_client()
                    .bucket_object_payload_lease_count(bucket)
            })
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
            .storage_node()
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

    fn metadata_pg_acting_nodes_with_allowed_states(
        &self,
        operation_epoch: ClusterEpoch,
        pg_id: PgId,
        allowed_states: &[PgState],
    ) -> Result<Vec<&LocalNodeStore>, StoreError> {
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
        self.require_route_map_valid_now()?;

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

        target_node
            .metadata_command_client()
            .metadata_command_acceptance(target_pg_id, command)
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
        target_node
            .metadata_command_client()
            .metadata_command_abandon_acceptance(target_pg_id, command)
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

    pub(crate) fn read_payload_shard(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<Vec<u8>, ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .read_shard(key, expected)
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
            .read_shard_into(key, expected, dst)
    }

    pub(crate) fn acquire_payload_shard_read_handles(
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
                .acquire_read_handles(&read_operation_id, entries.clone())
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

    pub(crate) fn read_payload_shard_into_without_handle(
        &self,
        operation_epoch: ClusterEpoch,
        location: ShardLocation,
        key: &ShardKey,
        expected: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), ShardIoError> {
        self.shard_node_client(operation_epoch, location, key)?
            .read_shard_into_without_handle(key, expected, dst)
    }

    pub(crate) fn delete_payload_shard(
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
            node_id: node.shard_client().node_id(),
            client: node.shard_client().as_ref(),
            read_handle_client: node.shard_read_handle_client().as_ref(),
            cluster_epoch: self.epoch,
            data_pg_id: location.data_pg_id(),
            location,
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
        for node in nodes.values() {
            let node_id = node.node_id();
            let state = if node_id == primary_node_id {
                node.metadata_command_client()
                    .validate_metadata_command_replay_state_preserving_pending_slot(
                        pg_id,
                        cluster_epoch,
                    )
            } else {
                node.metadata_command_client()
                    .validate_metadata_command_replay_state(pg_id, cluster_epoch)
            }
            .map_err(|source| ClusterBuildError::OpenLocalNode {
                node_id: node_id.as_u32(),
                source,
            })?;
            let pending_command = node
                .metadata_command_client()
                .pending_metadata_command_envelope(pg_id, cluster_epoch)
                .map_err(|source| ClusterBuildError::OpenLocalNode {
                    node_id: node_id.as_u32(),
                    source,
                })?;
            if pending_command.is_some() && node_id != primary_node_id {
                return Err(ClusterBuildError::OpenLocalNode {
                    node_id: node_id.as_u32(),
                    source: StoreError::MetadataCommandPendingOnNonPrimary {
                        node_id: node_id.as_u32(),
                        primary_node_id: primary_node_id.as_u32(),
                        pg_id: pg_id.get(),
                        cluster_epoch,
                    },
                });
            }
            if pending_command.is_some() && node_id == primary_node_id {
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
            )?;
        } else if let Some(command) = primary_pending_command.as_ref() {
            let primary_state = replica_states
                .iter()
                .find_map(|(node_id, state)| (*node_id == primary_node_id).then_some(state))
                .expect("validated route primary must be in local node set");
            if command.id().log_index().get() <= primary_state.applied_log_index {
                release_open_metadata_command_bucket_write_reservation(nodes, pg_routes, command)?;
                clean_terminal_primary_pending_slot_on_open(
                    nodes,
                    pg_id,
                    primary_node_id,
                    cluster_epoch,
                    command,
                )?;
            }
        }
    }
    Ok(())
}

fn clean_terminal_primary_pending_slot_on_open(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_id: PgId,
    primary_node_id: NodeId,
    cluster_epoch: ClusterEpoch,
    command: &MetadataCommandEnvelope,
) -> Result<(), ClusterBuildError> {
    let primary = nodes
        .get(&primary_node_id)
        .expect("validated route primary must be in local node set");
    primary
        .metadata_command_client()
        .remove_pending_metadata_command_slot(pg_id, command)
        .map_err(|source| ClusterBuildError::OpenLocalNode {
            node_id: primary_node_id.as_u32(),
            source,
        })?;
    if primary
        .metadata_command_client()
        .pending_metadata_command_envelope(pg_id, cluster_epoch)
        .map_err(|source| ClusterBuildError::OpenLocalNode {
            node_id: primary_node_id.as_u32(),
            source,
        })?
        .is_some()
    {
        return Err(ClusterBuildError::OpenLocalNode {
            node_id: primary_node_id.as_u32(),
            source: StoreError::MetadataCommandLogConflict {
                node_id: primary_node_id.as_u32(),
                pg_id: pg_id.get(),
                cluster_epoch,
                log_index: command.id().log_index().get(),
            },
        });
    }
    Ok(())
}

fn release_open_metadata_command_bucket_write_reservation(
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
    let Some(proof) = command_bucket_write_reservation_proof(command) else {
        return Ok(());
    };
    let topology = nodes
        .values()
        .next()
        .expect("local cluster must contain at least one node")
        .storage_node()
        .pg_topology();
    let bucket_pg_id = PgId::new(topology.bucket_pg_for(&proof.bucket));
    let primary_node_id = pg_routes
        .get(&bucket_pg_id)
        .expect("validated bucket PG id should have a route")
        .primary_node_id();
    let node = nodes
        .get(&primary_node_id)
        .expect("validated route primary must be in local node set");
    node.bucket_write_reservation_client()
        .release_metadata_command_bucket_write_reservation(bucket_pg_id, proof)
        .map_err(|source| ClusterBuildError::OpenLocalNode {
            node_id: primary_node_id.as_u32(),
            source: StoreError::Io {
                context: "release metadata command bucket write reservation on local cluster open",
                source: std::io::Error::other(source),
            },
        })?;
    Ok(())
}

fn converge_in_flight_metadata_command_on_open(
    nodes: &BTreeMap<NodeId, LocalNodeStore>,
    pg_routes: &BTreeMap<PgId, LocalPgRoute>,
    pg_id: PgId,
    primary_node_id: NodeId,
    cluster_epoch: ClusterEpoch,
    command: &MetadataCommandEnvelope,
) -> Result<(), ClusterBuildError> {
    let mut nodes_primary_last = nodes.iter().collect::<Vec<_>>();
    nodes_primary_last.sort_by_key(|(node_id, _node)| **node_id == primary_node_id);
    for (node_id, node) in nodes_primary_last {
        validate_open_metadata_command_bucket_write_reservation(nodes, pg_routes, command)?;
        node.metadata_command_client()
            .apply_metadata_command_and_record(pg_id, command)
            .map_err(|source| ClusterBuildError::OpenLocalNode {
                node_id: node_id.as_u32(),
                source: bucket_snapshot_error_to_store_error(source),
            })?;
    }

    release_open_metadata_command_bucket_write_reservation(nodes, pg_routes, command)?;
    clean_terminal_primary_pending_slot_on_open(
        nodes,
        pg_id,
        primary_node_id,
        cluster_epoch,
        command,
    )?;

    let mut converged_states = Vec::new();
    for node in nodes.values() {
        let node_id = node.node_id();
        let state = node
            .metadata_command_client()
            .validate_metadata_command_replay_state(pg_id, cluster_epoch)
            .map_err(|source| ClusterBuildError::OpenLocalNode {
                node_id: node_id.as_u32(),
                source,
            })?;
        converged_states.push((node_id, state));
    }
    validate_metadata_command_replica_agreement_or_in_flight_recovery(
        nodes,
        pg_id,
        primary_node_id,
        cluster_epoch,
        &converged_states,
        None,
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
        return Err(ClusterBuildError::OpenLocalNode {
            node_id: 0,
            source: StoreError::Io {
                context: "validate metadata command bucket write reservation on local cluster open",
                source: std::io::Error::other(MetadataError::BucketWriteReservationConflict {
                    reservation_id: proof.reservation_id.clone(),
                }),
            },
        });
    }
    let topology = nodes
        .values()
        .next()
        .expect("local cluster must contain at least one node")
        .storage_node()
        .pg_topology();
    let bucket_pg_id = PgId::new(topology.bucket_pg_for(&proof.bucket));
    let primary_node_id = pg_routes
        .get(&bucket_pg_id)
        .expect("validated bucket PG id should have a route")
        .primary_node_id();
    let node = nodes
        .get(&primary_node_id)
        .expect("validated route primary must be in local node set");
    node.bucket_write_reservation_client()
        .validate_bucket_write_reservation_proof(bucket_pg_id, proof)
        .map_err(|source| ClusterBuildError::OpenLocalNode {
            node_id: primary_node_id.as_u32(),
            source: StoreError::Io {
                context: "validate metadata command bucket write reservation on local cluster open",
                source: std::io::Error::other(source),
            },
        })
}

fn command_bucket_write_reservation_proof(
    command: &MetadataCommandEnvelope,
) -> Option<&crate::metadata_command::BucketWriteReservationProof> {
    match command.payload() {
        crate::metadata_command::MetadataCommandPayload::CommitDirectPutObject(commit) => {
            Some(&commit.bucket_write_reservation)
        }
        crate::metadata_command::MetadataCommandPayload::CommitMultipartObject(commit) => {
            Some(&commit.bucket_write_reservation)
        }
        crate::metadata_command::MetadataCommandPayload::CreateStreamUpload(create) => {
            Some(&create.bucket_write_reservation)
        }
        crate::metadata_command::MetadataCommandPayload::CommitStreamPart(commit) => {
            Some(&commit.bucket_write_reservation)
        }
        crate::metadata_command::MetadataCommandPayload::PutObjectMetadata(update) => {
            Some(&update.bucket_write_reservation)
        }
        crate::metadata_command::MetadataCommandPayload::DeleteObjectVersion(delete) => {
            Some(&delete.bucket_write_reservation)
        }
        crate::metadata_command::MetadataCommandPayload::InsertDeleteMarker(marker) => {
            Some(&marker.bucket_write_reservation)
        }
        crate::metadata_command::MetadataCommandPayload::CreateMultipartUpload(create) => {
            Some(&create.bucket_write_reservation)
        }
        crate::metadata_command::MetadataCommandPayload::AbortMultipartUpload(abort) => {
            Some(&abort.bucket_write_reservation)
        }
        _ => None,
    }
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
) -> Result<bool, ClusterBuildError> {
    let Some((reference_node_id, reference_state)) = replica_states.first() else {
        return Ok(false);
    };
    if replica_states
        .iter()
        .all(|(_node_id, state)| state == reference_state)
    {
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
                .metadata_command_client()
                .has_matching_applied_metadata_command_log_entry(
                    pg_id,
                    command,
                    unadvanced_state.applied_log_hash,
                )
                .map_err(|source| ClusterBuildError::OpenLocalNode {
                    node_id: node_id.as_u32(),
                    source,
                })?;
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
            .metadata_command_client()
            .has_matching_applied_metadata_command_log_entry(
                pg_id,
                command,
                primary_state.applied_log_hash,
            )
            .map_err(|source| ClusterBuildError::OpenLocalNode {
                node_id: node_id.as_u32(),
                source,
            })?;
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
    ClusterBuildError::OpenLocalNode {
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
    }
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
mod tests;
