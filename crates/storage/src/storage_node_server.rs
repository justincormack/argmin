use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};

use crate::error::StoreError;
use crate::node::SharedStorageNode;
use crate::storage_rpc::{
    encode_health_response, encode_storage_rpc_error_response, encode_storage_rpc_success_response,
    read_storage_rpc_frame_from, write_storage_rpc_frame_to, StorageRpcErrorCode,
    StorageRpcErrorResponse, StorageRpcFrame, StorageRpcHealthResponse, StorageRpcMessageKind,
    StorageRpcStreamError, STORAGE_RPC_FRAME_ENCODING_VERSION,
};
use crate::types::ClusterEpoch;
use crate::NodeId;

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
}

#[derive(Debug, thiserror::Error)]
pub enum StorageNodeServerError {
    #[error("storage-node PG set must not be empty")]
    EmptyPgSet,
    #[error("duplicate storage-node PG id {pg_id}")]
    DuplicatePgId { pg_id: u32 },
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
    for config in configs {
        if node_ids.insert(config.node_id.as_u32(), ()).is_some() {
            return Err(StorageNodeServerError::DuplicateNodeId {
                id: config.node_id.as_u32(),
            });
        }
        validate_pg_ids(&config.pg_ids)?;
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
    }
    Ok(())
}

pub struct StorageNodeServer {
    config: StorageNodeProcessConfig,
    _data_dir_lock: StorageNodeDataDirLock,
    _node: SharedStorageNode,
    listener: UnixListener,
}

impl StorageNodeServer {
    pub fn bind(config: StorageNodeProcessConfig) -> Result<Self, StorageNodeServerError> {
        validate_pg_ids(&config.pg_ids)?;
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
        self.handle_one_frame(&mut stream)
    }

    fn handle_one_frame(&self, stream: &mut UnixStream) -> Result<(), StorageNodeServerError> {
        let frame = read_storage_rpc_frame_from(stream).map_err(rpc_stream_error)?;
        let response = self.dispatch_frame(&frame)?;
        write_storage_rpc_frame_to(stream, &response).map_err(rpc_stream_error)?;
        Ok(())
    }

    fn dispatch_frame(
        &self,
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
            kind => encode_storage_rpc_error_response(&StorageRpcErrorResponse {
                code: StorageRpcErrorCode::UnsupportedOperation,
                message: format!("{kind:?} is not implemented by this storage-node server slice"),
            }),
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
    use std::thread;

    use crate::storage_rpc::{
        decode_health_response, decode_storage_rpc_response_payload, encode_storage_rpc_frame,
        read_storage_rpc_frame_from, write_storage_rpc_frame_to,
    };

    fn test_config(tmp: &test_util::TempDir) -> StorageNodeProcessConfig {
        StorageNodeProcessConfig {
            node_id: NodeId::new(7),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            data_dir: tmp.path().join("node"),
            pg_ids: vec![0],
            socket_path: tmp.path().join("sock").join("storage.sock"),
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
        join.join().unwrap();

        let error = decode_storage_rpc_response_payload(&response.payload)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, StorageRpcErrorCode::UnsupportedOperation);
    }
}
