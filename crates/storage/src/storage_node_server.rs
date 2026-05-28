use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::StoreError;
use crate::node::SharedStorageNode;
use crate::storage_rpc::{
    decode_read_handle_acquire_request, decode_read_handle_release_request, encode_health_response,
    encode_read_handle_acquire_response, encode_read_handle_release_response,
    encode_storage_rpc_error_response, encode_storage_rpc_success_response,
    read_storage_rpc_frame_from, write_storage_rpc_frame_to, StorageRpcErrorCode,
    StorageRpcErrorResponse, StorageRpcFrame, StorageRpcHealthResponse, StorageRpcMessageKind,
    StorageRpcReadHandleAcquireRequest, StorageRpcReadHandleAcquireResponse,
    StorageRpcReadHandleReleaseRequest, StorageRpcReadHandleReleaseResponse, StorageRpcStreamError,
    STORAGE_RPC_FRAME_ENCODING_VERSION,
};
use crate::types::{ClusterEpoch, PgState};
use crate::{NodeId, ShardLocation};

const DATA_DIR_LOCK_FILE: &str = ".argmin-storage-node.lock";
const LOCK_EX: i32 = 2;
const LOCK_NB: i32 = 4;

extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}

#[derive(Debug, Clone)]
pub struct StorageNodeProcessConfig {
    pub node_id: NodeId,
    pub cluster_epoch: ClusterEpoch,
    pub data_dir: PathBuf,
    pub pg_ids: Vec<u32>,
    pub socket_path: PathBuf,
    pub pg_routes: Vec<StorageNodePgRoute>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageNodePgRoute {
    pub pg_id: u32,
    pub cluster_epoch: ClusterEpoch,
    pub state: PgState,
    pub acting_set: Vec<NodeId>,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageNodeServerError {
    #[error("storage-node PG set must not be empty")]
    EmptyPgSet,
    #[error("duplicate storage-node PG id {pg_id}")]
    DuplicatePgId { pg_id: u32 },
    #[error("duplicate storage-node PG route {pg_id}")]
    DuplicatePgRoute { pg_id: u32 },
    #[error("storage-node PG {pg_id} is missing a route")]
    MissingPgRoute { pg_id: u32 },
    #[error("storage-node PG route {pg_id} is not configured for this node")]
    RoutePgNotConfigured { pg_id: u32 },
    #[error("storage-node PG route {pg_id} is inconsistent across static config")]
    InconsistentPgRoute { pg_id: u32 },
    #[error("duplicate storage-node id {id}")]
    DuplicateNodeId { id: u32 },
    #[error(
        "storage node {duplicate_node_id} shares data directory {data_dir:?} with storage node {first_node_id}"
    )]
    DuplicateDataDir {
        first_node_id: u32,
        duplicate_node_id: u32,
        data_dir: PathBuf,
    },
    #[error(
        "storage node {duplicate_node_id} shares Unix socket path {socket_path:?} with storage node {first_node_id}"
    )]
    DuplicateSocketPath {
        first_node_id: u32,
        duplicate_node_id: u32,
        socket_path: PathBuf,
    },
    #[error("storage-node socket path {path:?} has no parent directory")]
    SocketPathMissingParent { path: PathBuf },
    #[error("storage-node socket path {path:?} must be absolute")]
    SocketPathNotAbsolute { path: PathBuf },
    #[error("storage-node socket path {path:?} has no file name")]
    SocketPathMissingFileName { path: PathBuf },
    #[error("storage-node socket directory {path:?} must be private; mode is {mode:#o}")]
    SocketDirectoryNotPrivate { path: PathBuf, mode: u32 },
    #[error("storage-node socket path {path:?} already exists")]
    SocketPathExists { path: PathBuf },
    #[error("storage-node data directory {path:?} is already locked")]
    DataDirAlreadyLocked { path: PathBuf },
    #[error("storage-node I/O error during {context} for {path:?}: {source}")]
    Io {
        context: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to open storage node: {0}")]
    Store(#[from] StoreError),
    #[error("storage RPC stream error: {message}")]
    RpcStream { message: String },
    #[error("storage RPC response payload error: {message}")]
    ResponsePayload { message: String },
}

pub fn validate_storage_node_process_configs(
    configs: &[StorageNodeProcessConfig],
) -> Result<(), StorageNodeServerError> {
    let mut node_ids = BTreeMap::<u32, ()>::new();
    let mut data_dirs = BTreeMap::<PathBuf, NodeId>::new();
    let mut socket_paths = BTreeMap::<PathBuf, NodeId>::new();
    let mut pg_routes = BTreeMap::<u32, StorageNodePgRoute>::new();
    for config in configs {
        if node_ids.insert(config.node_id.as_u32(), ()).is_some() {
            return Err(StorageNodeServerError::DuplicateNodeId {
                id: config.node_id.as_u32(),
            });
        }
        validate_pg_ids(&config.pg_ids)?;
        validate_pg_routes(&config.pg_ids, &config.pg_routes)?;
        let data_dir = canonicalize_existing_or_parent(&config.data_dir, "data directory")?;
        if let Some(first_node_id) = data_dirs.insert(data_dir.clone(), config.node_id) {
            return Err(StorageNodeServerError::DuplicateDataDir {
                first_node_id: first_node_id.as_u32(),
                duplicate_node_id: config.node_id.as_u32(),
                data_dir,
            });
        }
        let socket_path = canonical_socket_path(&config.socket_path)?;
        if let Some(first_node_id) = socket_paths.insert(socket_path.clone(), config.node_id) {
            return Err(StorageNodeServerError::DuplicateSocketPath {
                first_node_id: first_node_id.as_u32(),
                duplicate_node_id: config.node_id.as_u32(),
                socket_path,
            });
        }
        for route in &config.pg_routes {
            match pg_routes.get(&route.pg_id) {
                Some(existing) if existing != route => {
                    return Err(StorageNodeServerError::InconsistentPgRoute { pg_id: route.pg_id })
                }
                Some(_) => {}
                None => {
                    pg_routes.insert(route.pg_id, route.clone());
                }
            }
        }
    }
    Ok(())
}

pub struct StorageNodeServer {
    config: StorageNodeProcessConfig,
    _data_dir_lock: StorageNodeDataDirLock,
    _node: SharedStorageNode,
    listener: UnixListener,
    read_handles: Mutex<StorageNodeReadHandleState>,
}

impl StorageNodeServer {
    pub fn bind(config: StorageNodeProcessConfig) -> Result<Self, StorageNodeServerError> {
        validate_pg_ids(&config.pg_ids)?;
        validate_pg_routes(&config.pg_ids, &config.pg_routes)?;
        validate_socket_directory(&config.socket_path)?;
        let data_dir_lock = StorageNodeDataDirLock::acquire(&config.data_dir)?;
        cleanup_stale_socket_path(&config.socket_path)?;
        let node = SharedStorageNode::open(&config.data_dir, &config.pg_ids)?;
        let listener = UnixListener::bind(&config.socket_path).map_err(|source| {
            StorageNodeServerError::Io {
                context: "bind storage-node socket",
                path: config.socket_path.clone(),
                source,
            }
        })?;
        Ok(Self {
            config,
            _data_dir_lock: data_dir_lock,
            _node: node,
            listener,
            read_handles: Mutex::new(StorageNodeReadHandleState::default()),
        })
    }

    pub fn accept_one(&self) -> Result<(), StorageNodeServerError> {
        let (mut stream, _) =
            self.listener
                .accept()
                .map_err(|source| StorageNodeServerError::Io {
                    context: "accept storage-node connection",
                    path: self.config.socket_path.clone(),
                    source,
                })?;
        self.handle_session(&mut stream)
    }

    fn handle_session(&self, stream: &mut UnixStream) -> Result<(), StorageNodeServerError> {
        let mut session = StorageNodeSession::new(&self.read_handles);
        loop {
            let frame = match read_storage_rpc_frame_from(stream) {
                Ok(frame) => frame,
                Err(StorageRpcStreamError::Io(error))
                    if matches!(
                        error.kind(),
                        io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::BrokenPipe
                    ) =>
                {
                    return Ok(());
                }
                Err(error) => return Err(rpc_stream_error(error)),
            };
            let response = self.dispatch_frame(&mut session, &frame)?;
            write_storage_rpc_frame_to(stream, &response).map_err(rpc_stream_error)?;
        }
    }

    fn dispatch_frame(
        &self,
        session: &mut StorageNodeSession<'_>,
        frame: &StorageRpcFrame,
    ) -> Result<StorageRpcFrame, StorageNodeServerError> {
        let payload = match frame.kind {
            StorageRpcMessageKind::Health => {
                if frame.payload.is_empty() {
                    let health = StorageRpcHealthResponse {
                        protocol_version: STORAGE_RPC_FRAME_ENCODING_VERSION,
                        node_id: self.config.node_id,
                        cluster_epoch: self.config.cluster_epoch,
                    };
                    let health_payload = encode_health_response(&health);
                    Ok(encode_storage_rpc_success_response(&health_payload))
                } else {
                    encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: "health request payload must be empty".to_string(),
                    })
                }
            }
            StorageRpcMessageKind::ReadHandlesAcquire => {
                match decode_read_handle_acquire_request(&frame.payload) {
                    Ok(request) => self.read_handles_acquire_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            StorageRpcMessageKind::ReadHandlesRelease => {
                match decode_read_handle_release_request(&frame.payload) {
                    Ok(request) => self.read_handles_release_response(session, request),
                    Err(error) => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                        code: StorageRpcErrorCode::PayloadDecode,
                        message: error.to_string(),
                    }),
                }
            }
            kind => self.unsupported_operation_response(kind),
        }
        .map_err(|error| StorageNodeServerError::ResponsePayload {
            message: error.to_string(),
        })?;
        Ok(StorageRpcFrame {
            request_id: frame.request_id,
            kind: frame.kind,
            payload,
        })
    }

    fn read_handles_acquire_response(
        &self,
        session: &mut StorageNodeSession<'_>,
        request: StorageRpcReadHandleAcquireRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        if let Err(error) = self.validate_shard_locations(&request.locations) {
            return encode_storage_rpc_error_response(&error);
        }
        let response = match session.acquire_read_handles(request) {
            Ok(locations) => {
                let payload =
                    encode_read_handle_acquire_response(&StorageRpcReadHandleAcquireResponse {
                        locations,
                    })?;
                encode_storage_rpc_success_response(&payload)
            }
            Err(error) => encode_storage_rpc_error_response(&error)?,
        };
        Ok(response)
    }

    fn read_handles_release_response(
        &self,
        session: &mut StorageNodeSession<'_>,
        request: StorageRpcReadHandleReleaseRequest,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        session.release_read_handles(&request.read_operation_id);
        let payload = encode_read_handle_release_response(&StorageRpcReadHandleReleaseResponse);
        Ok(encode_storage_rpc_success_response(&payload))
    }

    fn validate_shard_locations(
        &self,
        locations: &[ShardLocation],
    ) -> Result<(), StorageRpcErrorResponse> {
        for &location in locations {
            self.validate_shard_location(location)?;
        }
        Ok(())
    }

    fn validate_shard_location(
        &self,
        location: ShardLocation,
    ) -> Result<(), StorageRpcErrorResponse> {
        if location.node_id() != self.config.node_id {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownNode,
                message: format!(
                    "request targets node {}, but this storage node is {}",
                    location.node_id().as_u32(),
                    self.config.node_id.as_u32()
                ),
            });
        }
        if location.cluster_epoch() != self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::StaleShardLocation,
                message: format!(
                    "request shard location epoch {} does not match storage-node epoch {}",
                    location.cluster_epoch().get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        let pg_id = location.data_pg_id().get();
        let Some(route) = self
            .config
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id)
        else {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnknownPg,
                message: format!("PG {pg_id} is not configured on this storage node"),
            });
        };
        if route.cluster_epoch != self.config.cluster_epoch {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::WrongClusterEpoch,
                message: format!(
                    "PG {pg_id} route epoch {} does not match storage-node epoch {}",
                    route.cluster_epoch.get(),
                    self.config.cluster_epoch.get()
                ),
            });
        }
        if route.state != PgState::Active {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::InactivePgRoute,
                message: format!("PG {pg_id} route is {}", route.state),
            });
        }
        if !route.acting_set.contains(&self.config.node_id) {
            return Err(StorageRpcErrorResponse {
                code: StorageRpcErrorCode::NonActingSetAccess,
                message: format!(
                    "storage node {} is not in acting set for PG {pg_id}",
                    self.config.node_id.as_u32()
                ),
            });
        }
        Ok(())
    }

    fn unsupported_operation_response(
        &self,
        kind: StorageRpcMessageKind,
    ) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
        encode_storage_rpc_error_response(&StorageRpcErrorResponse {
            code: StorageRpcErrorCode::UnsupportedOperation,
            message: format!("{kind:?} is not implemented by this storage-node server slice"),
        })
    }

    #[cfg(test)]
    fn read_handle_count(&self, location: ShardLocation) -> usize {
        self.read_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .count(location)
    }
}

#[derive(Debug, Default)]
struct StorageNodeReadHandleState {
    location_counts: BTreeMap<ShardLocationKey, usize>,
}

impl StorageNodeReadHandleState {
    fn acquire(&mut self, locations: &[ShardLocation]) {
        for location in locations {
            *self
                .location_counts
                .entry(ShardLocationKey::from(*location))
                .or_insert(0) += 1;
        }
    }

    fn release(&mut self, locations: &[ShardLocation]) {
        for location in locations {
            let key = ShardLocationKey::from(*location);
            let entry = self
                .location_counts
                .get_mut(&key)
                .expect("read handle release without acquire");
            *entry -= 1;
            if *entry == 0 {
                self.location_counts.remove(&key);
            }
        }
    }

    #[cfg(test)]
    fn count(&self, location: ShardLocation) -> usize {
        self.location_counts
            .get(&ShardLocationKey::from(location))
            .copied()
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ShardLocationKey {
    cluster_epoch: u64,
    data_pg_id: u32,
    shard_index: u8,
    node_id: u32,
}

impl From<ShardLocation> for ShardLocationKey {
    fn from(location: ShardLocation) -> Self {
        Self {
            cluster_epoch: location.cluster_epoch().get(),
            data_pg_id: location.data_pg_id().get(),
            shard_index: location.shard_index().get(),
            node_id: location.node_id().as_u32(),
        }
    }
}

#[derive(Debug)]
struct StorageNodeSession<'a> {
    shared_handles: &'a Mutex<StorageNodeReadHandleState>,
    read_operations: BTreeMap<String, SessionReadHandle>,
}

impl<'a> StorageNodeSession<'a> {
    fn new(shared_handles: &'a Mutex<StorageNodeReadHandleState>) -> Self {
        Self {
            shared_handles,
            read_operations: BTreeMap::new(),
        }
    }

    fn acquire_read_handles(
        &mut self,
        request: StorageRpcReadHandleAcquireRequest,
    ) -> Result<Vec<ShardLocation>, StorageRpcErrorResponse> {
        match self.read_operations.get(&request.read_operation_id) {
            Some(existing) if existing.locations == request.locations && existing.is_acquired => {
                return Ok(existing.locations.clone());
            }
            Some(_) => {
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::Internal,
                    message: format!(
                        "read operation {} was already acquired with different shard locations",
                        request.read_operation_id
                    ),
                });
            }
            None => {}
        }

        self.shared_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .acquire(&request.locations);
        self.read_operations.insert(
            request.read_operation_id,
            SessionReadHandle {
                locations: request.locations.clone(),
                is_acquired: true,
            },
        );
        Ok(request.locations)
    }

    fn release_read_handles(&mut self, read_operation_id: &str) {
        let Some(existing) = self.read_operations.remove(read_operation_id) else {
            return;
        };
        if !existing.is_acquired {
            return;
        }
        self.shared_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .release(&existing.locations);
    }
}

impl Drop for StorageNodeSession<'_> {
    fn drop(&mut self) {
        let mut shared_handles = self
            .shared_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for existing in self.read_operations.values_mut() {
            if existing.is_acquired {
                shared_handles.release(&existing.locations);
                existing.is_acquired = false;
            }
        }
    }
}

#[derive(Debug)]
struct SessionReadHandle {
    locations: Vec<ShardLocation>,
    is_acquired: bool,
}

fn rpc_stream_error(error: StorageRpcStreamError) -> StorageNodeServerError {
    StorageNodeServerError::RpcStream {
        message: error.to_string(),
    }
}

impl Drop for StorageNodeServer {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.config.socket_path);
    }
}

struct StorageNodeDataDirLock {
    _file: File,
}

impl StorageNodeDataDirLock {
    fn acquire(data_dir: &Path) -> Result<Self, StorageNodeServerError> {
        fs::create_dir_all(data_dir).map_err(|source| StorageNodeServerError::Io {
            context: "create storage-node data directory",
            path: data_dir.to_path_buf(),
            source,
        })?;
        let path = data_dir.join(DATA_DIR_LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| StorageNodeServerError::Io {
                context: "open storage-node data-dir lock",
                path: path.clone(),
                source,
            })?;
        // SAFETY: flock operates on a valid file descriptor owned by `file`.
        // The descriptor remains open for the lifetime of StorageNodeDataDirLock.
        let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
        if rc != 0 {
            let source = io::Error::last_os_error();
            return if source.kind() == io::ErrorKind::WouldBlock {
                Err(StorageNodeServerError::DataDirAlreadyLocked {
                    path: data_dir.to_path_buf(),
                })
            } else {
                Err(StorageNodeServerError::Io {
                    context: "lock storage-node data directory",
                    path,
                    source,
                })
            };
        }
        Ok(Self { _file: file })
    }
}

impl std::fmt::Debug for StorageNodeDataDirLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageNodeDataDirLock")
            .finish_non_exhaustive()
    }
}

impl Drop for StorageNodeDataDirLock {
    fn drop(&mut self) {}
}

fn validate_pg_ids(pg_ids: &[u32]) -> Result<(), StorageNodeServerError> {
    if pg_ids.is_empty() {
        return Err(StorageNodeServerError::EmptyPgSet);
    }
    let mut seen = BTreeMap::<u32, ()>::new();
    for &pg_id in pg_ids {
        if seen.insert(pg_id, ()).is_some() {
            return Err(StorageNodeServerError::DuplicatePgId { pg_id });
        }
    }
    Ok(())
}

fn validate_pg_routes(
    pg_ids: &[u32],
    routes: &[StorageNodePgRoute],
) -> Result<(), StorageNodeServerError> {
    let configured: BTreeMap<u32, ()> = pg_ids.iter().map(|&pg_id| (pg_id, ())).collect();
    let mut seen = BTreeMap::<u32, ()>::new();
    for route in routes {
        if seen.insert(route.pg_id, ()).is_some() {
            return Err(StorageNodeServerError::DuplicatePgRoute { pg_id: route.pg_id });
        }
        if !configured.contains_key(&route.pg_id) {
            return Err(StorageNodeServerError::RoutePgNotConfigured { pg_id: route.pg_id });
        }
    }
    for &pg_id in pg_ids {
        if !seen.contains_key(&pg_id) {
            return Err(StorageNodeServerError::MissingPgRoute { pg_id });
        }
    }
    Ok(())
}

fn validate_socket_directory(socket_path: &Path) -> Result<(), StorageNodeServerError> {
    validate_absolute_socket_path(socket_path)?;
    let parent =
        socket_path
            .parent()
            .ok_or_else(|| StorageNodeServerError::SocketPathMissingParent {
                path: socket_path.to_path_buf(),
            })?;
    socket_path
        .file_name()
        .ok_or_else(|| StorageNodeServerError::SocketPathMissingFileName {
            path: socket_path.to_path_buf(),
        })?;
    let metadata = fs::metadata(parent).map_err(|source| StorageNodeServerError::Io {
        context: "stat storage-node socket directory",
        path: parent.to_path_buf(),
        source,
    })?;
    let mode = metadata.permissions().mode() & 0o777;
    if !metadata.is_dir() || mode & 0o077 != 0 {
        return Err(StorageNodeServerError::SocketDirectoryNotPrivate {
            path: parent.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

fn canonical_socket_path(path: &Path) -> Result<PathBuf, StorageNodeServerError> {
    validate_absolute_socket_path(path)?;
    let parent = path
        .parent()
        .ok_or_else(|| StorageNodeServerError::SocketPathMissingParent {
            path: path.to_path_buf(),
        })?;
    let file_name =
        path.file_name()
            .ok_or_else(|| StorageNodeServerError::SocketPathMissingFileName {
                path: path.to_path_buf(),
            })?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|source| StorageNodeServerError::Io {
            context: "canonicalize storage-node socket directory",
            path: parent.to_path_buf(),
            source,
        })?;
    Ok(canonical_parent.join(file_name))
}

fn cleanup_stale_socket_path(socket_path: &Path) -> Result<(), StorageNodeServerError> {
    let metadata =
        match fs::symlink_metadata(socket_path).map_err(|source| StorageNodeServerError::Io {
            context: "stat storage-node socket path",
            path: socket_path.to_path_buf(),
            source,
        }) {
            Ok(metadata) => metadata,
            Err(StorageNodeServerError::Io { source, .. })
                if source.kind() == io::ErrorKind::NotFound =>
            {
                return Ok(())
            }
            Err(error) => return Err(error),
        };
    if !metadata.file_type().is_socket() {
        return Err(StorageNodeServerError::SocketPathExists {
            path: socket_path.to_path_buf(),
        });
    }
    match UnixStream::connect(socket_path) {
        Ok(_) => Err(StorageNodeServerError::SocketPathExists {
            path: socket_path.to_path_buf(),
        }),
        Err(source)
            if matches!(
                source.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) =>
        {
            fs::remove_file(socket_path).map_err(|source| StorageNodeServerError::Io {
                context: "remove stale storage-node socket",
                path: socket_path.to_path_buf(),
                source,
            })
        }
        Err(source) => Err(StorageNodeServerError::Io {
            context: "connect existing storage-node socket",
            path: socket_path.to_path_buf(),
            source,
        }),
    }
}

fn validate_absolute_socket_path(path: &Path) -> Result<(), StorageNodeServerError> {
    if path.is_absolute() {
        Ok(())
    } else {
        Err(StorageNodeServerError::SocketPathNotAbsolute {
            path: path.to_path_buf(),
        })
    }
}

fn canonicalize_existing_or_parent(
    path: &Path,
    context: &'static str,
) -> Result<PathBuf, StorageNodeServerError> {
    if path.exists() {
        return path
            .canonicalize()
            .map_err(|source| StorageNodeServerError::Io {
                context,
                path: path.to_path_buf(),
                source,
            });
    }
    let parent = path
        .parent()
        .ok_or_else(|| StorageNodeServerError::SocketPathMissingParent {
            path: path.to_path_buf(),
        })?;
    let file_name =
        path.file_name()
            .ok_or_else(|| StorageNodeServerError::SocketPathMissingFileName {
                path: path.to_path_buf(),
            })?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|source| StorageNodeServerError::Io {
            context,
            path: parent.to_path_buf(),
            source,
        })?;
    Ok(canonical_parent.join(file_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    use std::sync::Arc;
    use std::thread;

    use crate::storage_rpc::{
        decode_health_response, decode_read_handle_acquire_response,
        decode_read_handle_release_response, decode_storage_rpc_response_payload,
        encode_read_handle_acquire_request, encode_read_handle_release_request,
        encode_storage_rpc_frame, read_storage_rpc_frame_from, write_storage_rpc_frame_to,
        StorageRpcReadHandleAcquireRequest, StorageRpcReadHandleReleaseRequest,
    };
    use crate::types::{DataPgId, PgId, ShardIndex};

    fn test_config(tmp: &test_util::TempDir) -> StorageNodeProcessConfig {
        StorageNodeProcessConfig {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            data_dir: tmp.path().join("node"),
            pg_ids: vec![0],
            socket_path: tmp.path().join("sock").join("storage.sock"),
            pg_routes: vec![StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                state: PgState::Active,
                acting_set: vec![NodeId::new(7)],
            }],
        }
    }

    fn private_socket_dir(path: &Path) {
        fs::create_dir_all(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn bind_error(config: StorageNodeProcessConfig) -> StorageNodeServerError {
        match StorageNodeServer::bind(config) {
            Ok(_) => panic!("expected storage-node bind to fail"),
            Err(error) => error,
        }
    }

    fn read_handle_acquire_payload(read_operation_id: &str, location: ShardLocation) -> Vec<u8> {
        encode_read_handle_acquire_request(&StorageRpcReadHandleAcquireRequest {
            read_operation_id: read_operation_id.to_string(),
            locations: vec![location],
        })
        .unwrap()
    }

    fn test_location(epoch: u64, pg_id: u32, node_id: u32) -> ShardLocation {
        test_location_with_shard(epoch, pg_id, node_id, 0)
    }

    fn test_location_with_shard(
        epoch: u64,
        pg_id: u32,
        node_id: u32,
        shard_index: u8,
    ) -> ShardLocation {
        ShardLocation::new(
            ClusterEpoch::new(epoch).unwrap(),
            DataPgId::new(PgId::new(pg_id)),
            ShardIndex::new(shard_index),
            NodeId::new(node_id),
        )
    }

    fn read_handle_release_payload(read_operation_id: &str) -> Vec<u8> {
        encode_read_handle_release_request(&StorageRpcReadHandleReleaseRequest {
            read_operation_id: read_operation_id.to_string(),
        })
        .unwrap()
    }

    fn send_frame(
        client: &mut UnixStream,
        request_id: u64,
        kind: StorageRpcMessageKind,
        payload: Vec<u8>,
    ) -> StorageRpcFrame {
        let request = StorageRpcFrame {
            request_id,
            kind,
            payload,
        };
        write_storage_rpc_frame_to(client, &request).unwrap();
        read_storage_rpc_frame_from(client).unwrap()
    }

    fn send_read_handle_acquire(
        config: StorageNodeProcessConfig,
        location: ShardLocation,
    ) -> StorageRpcErrorResponse {
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let request = StorageRpcFrame {
            request_id: 7,
            kind: StorageRpcMessageKind::ReadHandlesAcquire,
            payload: read_handle_acquire_payload("read-op", location),
        };
        write_storage_rpc_frame_to(&mut client, &request).unwrap();
        let response = read_storage_rpc_frame_from(&mut client).unwrap();
        drop(client);
        join.join().unwrap();

        decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err()
    }

    #[test]
    fn storage_node_server_answers_health_request() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let request = StorageRpcFrame {
            request_id: 42,
            kind: StorageRpcMessageKind::Health,
            payload: Vec::new(),
        };
        write_storage_rpc_frame_to(&mut client, &request).unwrap();
        let response = read_storage_rpc_frame_from(&mut client).unwrap();
        drop(client);
        join.join().unwrap();

        assert_eq!(response.request_id, 42);
        assert_eq!(response.kind, StorageRpcMessageKind::Health);
        let health_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let health = decode_health_response(&health_payload).unwrap();
        assert_eq!(health.node_id, NodeId::new(7));
        assert_eq!(health.cluster_epoch, ClusterEpoch::new(1).unwrap());
    }

    #[test]
    fn storage_node_server_rejects_non_private_socket_directory() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        fs::create_dir_all(config.socket_path.parent().unwrap()).unwrap();
        fs::set_permissions(
            config.socket_path.parent().unwrap(),
            fs::Permissions::from_mode(0o777),
        )
        .unwrap();

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketDirectoryNotPrivate { .. }
        ));
    }

    #[test]
    fn storage_node_server_rejects_second_owner_for_same_data_dir() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let _first = StorageNodeServer::bind(config.clone()).unwrap();
        let mut second = config;
        second.socket_path = tmp.path().join("sock").join("other.sock");

        let err = bind_error(second);

        assert!(matches!(
            err,
            StorageNodeServerError::DataDirAlreadyLocked { .. }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_duplicate_socket_paths() {
        let tmp = test_util::tempdir();
        private_socket_dir(&tmp.path().join("sock"));
        let first = test_config(&tmp);
        let mut second = first.clone();
        second.node_id = NodeId::new(8);
        second.data_dir = tmp.path().join("node-2");

        let err = validate_storage_node_process_configs(&[first, second]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::DuplicateSocketPath { .. }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_duplicate_pg_routes() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes.push(config.pg_routes[0].clone());

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::DuplicatePgRoute { pg_id: 0 }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_unconfigured_pg_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].pg_id = 9;

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::RoutePgNotConfigured { pg_id: 9 }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_missing_pg_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids.push(1);

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::MissingPgRoute { pg_id: 1 }
        ));
    }

    #[test]
    fn storage_node_bind_rejects_missing_pg_route() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_ids.push(1);
        private_socket_dir(config.socket_path.parent().unwrap());

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::MissingPgRoute { pg_id: 1 }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_inconsistent_pg_routes() {
        let tmp = test_util::tempdir();
        private_socket_dir(&tmp.path().join("sock"));
        let first = test_config(&tmp);
        let mut second = first.clone();
        second.node_id = NodeId::new(8);
        second.data_dir = tmp.path().join("node-2");
        second.socket_path = tmp.path().join("sock").join("storage-2.sock");
        second.pg_routes[0].acting_set = vec![NodeId::new(8)];

        let err = validate_storage_node_process_configs(&[first, second]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::InconsistentPgRoute { pg_id: 0 }
        ));
    }

    #[test]
    fn storage_node_static_config_rejects_relative_socket_path() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.socket_path = PathBuf::from("relative.sock");

        let err = validate_storage_node_process_configs(&[config]).unwrap_err();

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathNotAbsolute { .. }
        ));
    }

    #[test]
    fn storage_node_server_rejects_relative_socket_path() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.socket_path = PathBuf::from("relative.sock");

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathNotAbsolute { .. }
        ));
    }

    #[test]
    fn storage_node_server_removes_stale_socket_path_on_restart() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let stale = UnixListener::bind(&config.socket_path).unwrap();
        drop(stale);
        assert!(config.socket_path.exists());

        let _server = StorageNodeServer::bind(config).unwrap();
    }

    #[test]
    fn storage_node_server_rejects_active_socket_owner() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let _active = UnixListener::bind(&config.socket_path).unwrap();

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathExists { .. }
        ));
    }

    #[test]
    fn storage_node_server_rejects_existing_non_socket_path() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        File::create(&config.socket_path).unwrap();

        let err = bind_error(config);

        assert!(matches!(
            err,
            StorageNodeServerError::SocketPathExists { .. }
        ));
    }

    #[test]
    fn storage_node_server_returns_unsupported_operation_error() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server.accept_one().unwrap());

        let mut client = UnixStream::connect(socket_path).unwrap();
        let frame_bytes =
            encode_storage_rpc_frame(9, StorageRpcMessageKind::ShardRead, b"").unwrap();
        client.write_all(&frame_bytes).unwrap();
        let response = read_storage_rpc_frame_from(&mut client).unwrap();
        drop(client);
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::UnsupportedOperation);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_wrong_node() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);

        let error = send_read_handle_acquire(config, test_location(1, 0, 8));

        assert_eq!(error.code, StorageRpcErrorCode::UnknownNode);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_stale_location_epoch() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);

        let error = send_read_handle_acquire(config, test_location(2, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::StaleShardLocation);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_unknown_pg() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);

        let error = send_read_handle_acquire(config, test_location(1, 9, 7));

        assert_eq!(error.code, StorageRpcErrorCode::UnknownPg);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_wrong_route_epoch() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].cluster_epoch = ClusterEpoch::new(2).unwrap();

        let error = send_read_handle_acquire(config, test_location(1, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::WrongClusterEpoch);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_inactive_pg() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].state = PgState::Peering;

        let error = send_read_handle_acquire(config, test_location(1, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::InactivePgRoute);
    }

    #[test]
    fn storage_node_server_rejects_read_handle_acquire_for_non_acting_set() {
        let tmp = test_util::tempdir();
        let mut config = test_config(&tmp);
        config.pg_routes[0].acting_set = vec![NodeId::new(8)];

        let error = send_read_handle_acquire(config, test_location(1, 0, 7));

        assert_eq!(error.code, StorageRpcErrorCode::NonActingSetAccess);
    }

    #[test]
    fn storage_node_server_validates_route_before_acquiring_read_handle() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let location = test_location(1, 0, 7);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let response = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );

        let success_payload = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap();
        let acquired = decode_read_handle_acquire_response(&success_payload).unwrap();
        assert_eq!(acquired.locations, vec![location]);
        assert_eq!(server.read_handle_count(location), 1);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(location), 0);
    }

    #[test]
    fn storage_node_server_retries_lost_read_handle_acquire_without_extra_count() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let location = test_location(1, 0, 7);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );
        let second = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );

        for response in [first, second] {
            let success_payload = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
            let acquired = decode_read_handle_acquire_response(&success_payload).unwrap();
            assert_eq!(acquired.locations, vec![location]);
        }
        assert_eq!(server.read_handle_count(location), 1);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(location), 0);
    }

    #[test]
    fn storage_node_server_retries_lost_read_handle_release_without_error_or_leak() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let location = test_location(1, 0, 7);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let acquire = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", location),
        );
        decode_storage_rpc_response_payload(&acquire.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(location), 1);

        let first_release = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-op"),
        );
        let second_release = send_frame(
            &mut client,
            9,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-op"),
        );

        for response in [first_release, second_release] {
            let success_payload = decode_storage_rpc_response_payload(&response.payload)
                .unwrap()
                .unwrap();
            decode_read_handle_release_response(&success_payload).unwrap();
            assert_eq!(server.read_handle_count(location), 0);
        }
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(location), 0);
    }

    #[test]
    fn storage_node_server_release_removes_completed_read_operation() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let first_location = test_location(1, 0, 7);
        let second_location = test_location_with_shard(1, 0, 7, 1);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first_acquire = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", first_location),
        );
        decode_storage_rpc_response_payload(&first_acquire.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(first_location), 1);

        let release = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesRelease,
            read_handle_release_payload("read-op"),
        );
        decode_storage_rpc_response_payload(&release.payload)
            .unwrap()
            .unwrap();
        assert_eq!(server.read_handle_count(first_location), 0);

        let second_acquire = send_frame(
            &mut client,
            9,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", second_location),
        );
        let success_payload = decode_storage_rpc_response_payload(&second_acquire.payload)
            .unwrap()
            .unwrap();
        let acquired = decode_read_handle_acquire_response(&success_payload).unwrap();
        assert_eq!(acquired.locations, vec![second_location]);
        assert_eq!(server.read_handle_count(first_location), 0);
        assert_eq!(server.read_handle_count(second_location), 1);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(second_location), 0);
    }

    #[test]
    fn storage_node_server_rejects_read_operation_id_reuse_for_different_locations() {
        let tmp = test_util::tempdir();
        let config = test_config(&tmp);
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
        let server_for_thread = Arc::clone(&server);
        let socket_path = config.socket_path.clone();
        let join = thread::spawn(move || server_for_thread.accept_one().unwrap());
        let first_location = test_location(1, 0, 7);
        let second_location = test_location_with_shard(1, 0, 7, 1);

        let mut client = UnixStream::connect(socket_path).unwrap();
        let first = send_frame(
            &mut client,
            7,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", first_location),
        );
        decode_storage_rpc_response_payload(&first.payload)
            .unwrap()
            .unwrap();
        let second = send_frame(
            &mut client,
            8,
            StorageRpcMessageKind::ReadHandlesAcquire,
            read_handle_acquire_payload("read-op", second_location),
        );

        let error = decode_storage_rpc_response_payload(&second.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::Internal);
        assert_eq!(server.read_handle_count(first_location), 1);
        assert_eq!(server.read_handle_count(second_location), 0);
        drop(client);
        join.join().unwrap();
        assert_eq!(server.read_handle_count(first_location), 0);
    }
}
