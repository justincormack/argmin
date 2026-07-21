use crate::config::ConfiguredStaticClusterIdentity;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use storage::control_plane::ensure_control_plane_state_parent_directory;
use storage::{ClusterEpoch, EcShape, LocalClusterMap, LocalNodeStoreConfig, NodeId};

const STORAGE_IDENTITY_FILE_NAME: &str = ".argmin-static-storage.identity";
const STORAGE_INITIALIZING_FILE_NAME: &str = ".argmin-static-storage.initializing";
const STORAGE_INITIALIZING_NEXT_FILE_NAME: &str = ".argmin-static-storage.initializing.next";
const STORAGE_IDENTITY_NEXT_FILE_NAME: &str = ".argmin-static-storage.identity.next";
const STORAGE_INITIALIZATION_LOCK_FILE_NAME: &str = ".argmin-static-storage.lock";
const STORAGE_IDENTITY_MAGIC: &[u8; 8] = b"ARGSSID\0";
const SQLITE_FILE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
const STORAGE_IDENTITY_VERSION: u16 = 1;
const STORAGE_IDENTITY_MAX_BYTES: u64 = 1_024;
const CONTROL_PLANE_IDENTITY_MAGIC: &[u8; 8] = b"ARGSCPID";
const CONTROL_PLANE_IDENTITY_VERSION: u16 = 2;
const CONTROL_PLANE_IDENTITY_DIGEST_BYTES: usize = 64;
const CONTROL_PLANE_IDENTITY_MAX_BYTES: u64 = 1_024;
const PRIVATE_FILE_MODE: u32 = 0o600;
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaticStorageIdentity {
    cluster_id: String,
    topology_generation: u64,
    topology_digest: String,
    process_id: String,
    process_identity_digest: String,
    storage_node_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaticControlPlaneIdentity {
    cluster_id: String,
    topology_generation: u64,
    topology_digest: String,
    process_id: String,
    process_identity_digest: String,
    raft_node_id: u64,
    established: bool,
}

impl StaticControlPlaneIdentity {
    fn new(config: &ConfiguredStaticClusterIdentity, raft_node_id: u64) -> Self {
        Self {
            cluster_id: config.cluster_id.clone(),
            topology_generation: config.topology_generation,
            topology_digest: config.topology_digest.clone(),
            process_id: config.process_id.clone(),
            process_identity_digest: config.process_identity_digest.clone(),
            raft_node_id,
            established: false,
        }
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::with_capacity(256);
        bytes.extend_from_slice(CONTROL_PLANE_IDENTITY_MAGIC);
        bytes.extend_from_slice(&CONTROL_PLANE_IDENTITY_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.topology_generation.to_be_bytes());
        bytes.extend_from_slice(&self.raft_node_id.to_be_bytes());
        bytes.push(u8::from(self.established));
        encode_string(&mut bytes, &self.cluster_id, "cluster id")?;
        encode_string(&mut bytes, &self.topology_digest, "topology digest")?;
        encode_string(&mut bytes, &self.process_id, "process id")?;
        encode_string(
            &mut bytes,
            &self.process_identity_digest,
            "process identity digest",
        )?;
        let digest = auth::canonical::sha256_hex(&bytes);
        bytes.extend_from_slice(digest.as_bytes());
        if bytes.len() > CONTROL_PLANE_IDENTITY_MAX_BYTES as usize {
            return Err("static control-plane identity exceeds its size bound".to_string());
        }
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        let body_len = bytes
            .len()
            .checked_sub(CONTROL_PLANE_IDENTITY_DIGEST_BYTES)
            .ok_or_else(|| "static control-plane identity has a truncated digest".to_string())?;
        let (body, actual_digest) = bytes.split_at(body_len);
        let expected_digest = auth::canonical::sha256_hex(body);
        if actual_digest != expected_digest.as_bytes() {
            return Err("static control-plane identity has an invalid digest".to_string());
        }
        let mut decoder = IdentityDecoder::new(body);
        let magic = decoder.take(CONTROL_PLANE_IDENTITY_MAGIC.len(), "magic")?;
        if magic != CONTROL_PLANE_IDENTITY_MAGIC {
            return Err("static control-plane identity has invalid magic".to_string());
        }
        let version = decoder.read_u16("version")?;
        if version != CONTROL_PLANE_IDENTITY_VERSION {
            return Err("static control-plane identity has an unsupported version".to_string());
        }
        let topology_generation = decoder.read_u64("topology generation")?;
        let raft_node_id = decoder.read_u64("Raft node id")?;
        let established = match decoder.take(1, "established flag")?[0] {
            0 => false,
            1 => true,
            _ => {
                return Err(
                    "static control-plane identity has an invalid established flag".to_string(),
                );
            }
        };
        let cluster_id = decoder.read_string("cluster id")?;
        let topology_digest = decoder.read_string("topology digest")?;
        let process_id = decoder.read_string("process id")?;
        let process_identity_digest = decoder.read_string("process identity digest")?;
        if !decoder.is_empty() {
            return Err("static control-plane identity has trailing bytes".to_string());
        }
        Ok(Self {
            cluster_id,
            topology_generation,
            topology_digest,
            process_id,
            process_identity_digest,
            raft_node_id,
            established,
        })
    }

    fn verify(&self, expected: &Self) -> Result<(), String> {
        if self.cluster_id != expected.cluster_id {
            return Err("static control-plane identity belongs to a different cluster".to_string());
        }
        if self.topology_generation != expected.topology_generation {
            return Err(
                "static control-plane identity has a different topology generation".to_string(),
            );
        }
        if self.topology_digest != expected.topology_digest {
            return Err(
                "static control-plane identity has a different topology digest".to_string(),
            );
        }
        if self.process_id != expected.process_id {
            return Err("static control-plane identity belongs to a different process".to_string());
        }
        if self.process_identity_digest != expected.process_identity_digest {
            return Err(
                "static control-plane identity has a different process identity".to_string(),
            );
        }
        if self.raft_node_id != expected.raft_node_id {
            return Err(
                "static control-plane identity belongs to a different Raft node".to_string(),
            );
        }
        Ok(())
    }
}

pub(crate) fn initialize_static_control_plane_identity(
    identity: &ConfiguredStaticClusterIdentity,
    raft_node_id: u64,
    state_path: &Path,
) -> Result<(), String> {
    initialize_static_control_plane_identity_after_lock(identity, raft_node_id, state_path, || {})
}

fn initialize_static_control_plane_identity_after_lock<F>(
    identity: &ConfiguredStaticClusterIdentity,
    raft_node_id: u64,
    state_path: &Path,
    after_lock: F,
) -> Result<(), String>
where
    F: FnOnce(),
{
    let _state_lock = crate::acquire_control_plane_state_lock(state_path)?;
    after_lock();
    let expected = StaticControlPlaneIdentity::new(identity, raft_node_id);
    let identity_path = static_control_plane_identity_path(state_path);
    if identity_path.try_exists().map_err(|error| {
        format!(
            "inspect static control-plane identity {}: {error}",
            identity_path.display()
        )
    })? {
        let actual = read_static_control_plane_identity(&identity_path)?;
        actual.verify(&expected)?;
        if actual.established {
            return Err(
                "static control-plane identity is already established; use ordinary startup"
                    .to_string(),
            );
        }
        return Ok(());
    }
    if control_plane_state_evidence_exists(state_path)? {
        return Err(format!(
            "cannot initialize static control-plane identity beside existing durable state {}; use an explicit replacement ceremony",
            state_path.display()
        ));
    }
    ensure_private_control_plane_state_parent(state_path)?;
    publish_new_static_control_plane_identity(&identity_path, &expected)
}

pub(crate) fn bind_static_control_plane_identity(
    identity: &ConfiguredStaticClusterIdentity,
    raft_node_id: u64,
    state_path: &Path,
) -> Result<bool, String> {
    let expected = StaticControlPlaneIdentity::new(identity, raft_node_id);
    let identity_path = static_control_plane_identity_path(state_path);
    if identity_path.try_exists().map_err(|error| {
        format!(
            "inspect static control-plane identity {}: {error}",
            identity_path.display()
        )
    })? {
        let actual = read_static_control_plane_identity(&identity_path)?;
        actual.verify(&expected)?;
        if actual.established && !control_plane_established_restart_set_exists(state_path)? {
            return Err(format!(
                "established static control-plane identity exists without its durable Raft state {}; relocate the complete state set or use an explicit replacement ceremony",
                state_path.display()
            ));
        }
        return Ok(actual.established);
    }

    let existing_state = control_plane_state_evidence_exists(state_path)?;
    Err(if existing_state {
        format!(
            "static control-plane identity is missing beside existing durable state {}; relocate the complete state set or use an explicit replacement ceremony",
            state_path.display()
        )
    } else {
        format!(
            "static control-plane identity is not initialized beside {}; run initialize-cluster-state before startup",
            state_path.display()
        )
    })
}

pub(crate) fn mark_static_control_plane_identity_established(
    identity: &ConfiguredStaticClusterIdentity,
    raft_node_id: u64,
    state_path: &Path,
) -> Result<(), String> {
    if !control_plane_established_restart_set_exists(state_path)? {
        return Err(
            "cannot establish static control-plane identity before the durable Raft artifact and sentinel exist"
                .to_string(),
        );
    }
    let identity_path = static_control_plane_identity_path(state_path);
    let mut actual = read_static_control_plane_identity(&identity_path)?;
    actual.verify(&StaticControlPlaneIdentity::new(identity, raft_node_id))?;
    if actual.established {
        return Ok(());
    }
    actual.established = true;
    replace_static_control_plane_identity(&identity_path, &actual)
}

fn replace_static_control_plane_identity(
    identity_path: &Path,
    identity: &StaticControlPlaneIdentity,
) -> Result<(), String> {
    let parent = identity_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let next_path = identity_path.with_extension("identity.next");
    if next_path.try_exists().map_err(|error| {
        format!(
            "inspect prepared static control-plane identity {}: {error}",
            next_path.display()
        )
    })? {
        fs::remove_file(&next_path).map_err(|error| {
            format!(
                "remove stale prepared static control-plane identity {}: {error}",
                next_path.display()
            )
        })?;
        sync_directory(
            parent,
            "sync removal of stale static control-plane identity",
        )?;
    }
    create_control_plane_identity_file(&next_path, identity)?;
    sync_directory(parent, "sync prepared static control-plane identity")?;
    fs::rename(&next_path, identity_path).map_err(|error| {
        format!(
            "publish static control-plane identity {}: {error}",
            identity_path.display()
        )
    })?;
    sync_directory(parent, "sync published static control-plane identity")
}

fn static_control_plane_identity_path(state_path: &Path) -> std::path::PathBuf {
    let file_name = state_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("control-plane.state");
    state_path.with_file_name(format!("{file_name}.static-identity"))
}

fn publish_new_static_control_plane_identity(
    identity_path: &Path,
    identity: &StaticControlPlaneIdentity,
) -> Result<(), String> {
    let parent = identity_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let next_path = identity_path.with_extension("identity.next");
    if next_path.try_exists().map_err(|error| {
        format!(
            "inspect prepared static control-plane identity {}: {error}",
            next_path.display()
        )
    })? {
        fs::remove_file(&next_path).map_err(|error| {
            format!(
                "remove incomplete static control-plane identity {}: {error}",
                next_path.display()
            )
        })?;
        sync_directory(
            parent,
            "sync removal of incomplete static control-plane identity",
        )?;
    }
    create_control_plane_identity_file(&next_path, identity)?;
    sync_directory(parent, "sync prepared static control-plane identity")?;
    fs::rename(&next_path, identity_path).map_err(|error| {
        format!(
            "publish static control-plane identity {}: {error}",
            identity_path.display()
        )
    })?;
    sync_directory(parent, "sync published static control-plane identity")
}

fn ensure_private_control_plane_state_parent(state_path: &Path) -> Result<(), String> {
    ensure_control_plane_state_parent_directory(state_path)
        .map_err(|error| format!("durably create static control-plane state directory: {error}"))?;
    let parent = state_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            "static control-plane state path must have an explicit parent directory".to_string()
        })?;
    fs::set_permissions(parent, fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE)).map_err(
        |error| {
            format!(
                "set private permissions on static control-plane state directory {}: {error}",
                parent.display()
            )
        },
    )?;
    sync_directory(parent, "sync static control-plane state directory")
}

fn control_plane_state_evidence_exists(state_path: &Path) -> Result<bool, String> {
    let file_name = state_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("control-plane.state");
    for path in [
        state_path.to_path_buf(),
        state_path.with_file_name(format!("{file_name}.sentinel")),
        state_path.with_file_name(format!("{file_name}.wal")),
    ] {
        if path.try_exists().map_err(|error| {
            format!(
                "inspect existing static control-plane state {}: {error}",
                path.display()
            )
        })? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn control_plane_established_restart_set_exists(state_path: &Path) -> Result<bool, String> {
    let file_name = state_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("control-plane.state");
    let sentinel_path = state_path.with_file_name(format!("{file_name}.sentinel"));
    Ok(state_file_is_nonempty_regular(state_path)?
        && state_file_is_nonempty_regular(&sentinel_path)?)
}

fn state_file_is_nonempty_regular(path: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() {
                return Err(format!(
                    "static control-plane state evidence {} is not a regular file",
                    path.display()
                ));
            }
            Ok(metadata.len() != 0)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!(
            "inspect static control-plane state evidence {}: {error}",
            path.display()
        )),
    }
}

fn create_control_plane_identity_file(
    path: &Path,
    identity: &StaticControlPlaneIdentity,
) -> Result<(), String> {
    let bytes = identity.encode()?;
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .mode(PRIVATE_FILE_MODE);
    let mut file = options.open(path).map_err(|error| {
        format!(
            "create static control-plane identity {}: {error}",
            path.display()
        )
    })?;
    file.write_all(&bytes).map_err(|error| {
        format!(
            "write static control-plane identity {}: {error}",
            path.display()
        )
    })?;
    file.sync_all().map_err(|error| {
        format!(
            "sync static control-plane identity {}: {error}",
            path.display()
        )
    })
}

fn read_static_control_plane_identity(path: &Path) -> Result<StaticControlPlaneIdentity, String> {
    let bytes = read_bounded_identity_file(
        path,
        CONTROL_PLANE_IDENTITY_MAX_BYTES,
        "static control-plane identity",
    )?;
    StaticControlPlaneIdentity::decode(&bytes)
}

impl StaticStorageIdentity {
    fn new(config: &ConfiguredStaticClusterIdentity, storage_node_id: u32) -> Self {
        Self {
            cluster_id: config.cluster_id.clone(),
            topology_generation: config.topology_generation,
            topology_digest: config.topology_digest.clone(),
            process_id: config.process_id.clone(),
            process_identity_digest: config.process_identity_digest.clone(),
            storage_node_id,
        }
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::with_capacity(256);
        bytes.extend_from_slice(STORAGE_IDENTITY_MAGIC);
        bytes.extend_from_slice(&STORAGE_IDENTITY_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.topology_generation.to_be_bytes());
        bytes.extend_from_slice(&self.storage_node_id.to_be_bytes());
        encode_string(&mut bytes, &self.cluster_id, "cluster id")?;
        encode_string(&mut bytes, &self.topology_digest, "topology digest")?;
        encode_string(&mut bytes, &self.process_id, "process id")?;
        encode_string(
            &mut bytes,
            &self.process_identity_digest,
            "process identity digest",
        )?;
        if bytes.len() > STORAGE_IDENTITY_MAX_BYTES as usize {
            return Err("static storage identity exceeds its size bound".to_string());
        }
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut decoder = IdentityDecoder::new(bytes);
        decoder.expect_magic()?;
        let version = decoder.read_u16("version")?;
        if version != STORAGE_IDENTITY_VERSION {
            return Err("static storage identity has an unsupported version".to_string());
        }
        let topology_generation = decoder.read_u64("topology generation")?;
        let storage_node_id = decoder.read_u32("storage node id")?;
        let cluster_id = decoder.read_string("cluster id")?;
        let topology_digest = decoder.read_string("topology digest")?;
        let process_id = decoder.read_string("process id")?;
        let process_identity_digest = decoder.read_string("process identity digest")?;
        if !decoder.is_empty() {
            return Err("static storage identity has trailing bytes".to_string());
        }
        Ok(Self {
            cluster_id,
            topology_generation,
            topology_digest,
            process_id,
            process_identity_digest,
            storage_node_id,
        })
    }

    fn verify(&self, expected: &Self) -> Result<(), String> {
        if self.cluster_id != expected.cluster_id {
            return Err("static storage identity belongs to a different cluster".to_string());
        }
        if self.topology_generation != expected.topology_generation {
            return Err("static storage identity has a different topology generation".to_string());
        }
        if self.topology_digest != expected.topology_digest {
            return Err("static storage identity has a different topology digest".to_string());
        }
        if self.process_id != expected.process_id {
            return Err("static storage identity belongs to a different process".to_string());
        }
        if self.process_identity_digest != expected.process_identity_digest {
            return Err("static storage identity has a different process identity".to_string());
        }
        if self.storage_node_id != expected.storage_node_id {
            return Err("static storage identity belongs to a different storage node".to_string());
        }
        Ok(())
    }
}

pub(crate) fn initialize_standalone_storage(
    identity: &ConfiguredStaticClusterIdentity,
    storage_node_id: u32,
    data_dir: &Path,
    pg_ids: &[u32],
    ec_shape: EcShape,
    initial_cluster_epoch: ClusterEpoch,
) -> Result<(), String> {
    let expected = StaticStorageIdentity::new(identity, storage_node_id);
    let expected_bytes = expected.encode()?;
    ensure_private_data_directory_durable(data_dir)?;
    let _initialization_lock = acquire_storage_directory_lock(data_dir)?;

    let identity_path = data_dir.join(STORAGE_IDENTITY_FILE_NAME);
    if identity_path.try_exists().map_err(|error| {
        format!(
            "inspect static storage identity {}: {error}",
            identity_path.display()
        )
    })? {
        verify_static_storage_identity(&identity_path, &expected)?;
        verify_static_storage_pg_state(data_dir, pg_ids, &expected_bytes)?;
        remove_completed_initialization_marker(data_dir, &expected)?;
        return Ok(());
    }

    let marker_path = data_dir.join(STORAGE_INITIALIZING_FILE_NAME);
    if marker_path.try_exists().map_err(|error| {
        format!(
            "inspect static storage initialization marker {}: {error}",
            marker_path.display()
        )
    })? {
        verify_static_storage_identity(&marker_path, &expected)?;
        sync_identity_file(&marker_path, "sync static storage initialization marker")?;
        sync_directory(data_dir, "sync static storage initialization marker")?;
    } else {
        publish_initialization_marker(data_dir, &expected)?;
    }

    let node_id = NodeId::new(storage_node_id);
    let cluster = LocalClusterMap::open_with_configs_and_epoch(
        node_id,
        [LocalNodeStoreConfig::new(node_id, data_dir)],
        pg_ids,
        ec_shape,
        initial_cluster_epoch,
    )
    .map_err(|error| format!("initialize static standalone storage: {error}"))?;
    drop(cluster);
    for pg_id in pg_ids {
        storage::initialize_pg_durable_identity(
            &data_dir.join(format!("pg-{pg_id:04}")),
            *pg_id,
            &expected_bytes,
        )
        .map_err(|error| format!("bind static storage PG {pg_id} identity: {error}"))?;
    }
    sync_initialized_pg_state(data_dir, pg_ids, &expected_bytes)?;

    publish_static_storage_identity(data_dir, &expected)?;
    Ok(())
}

pub(crate) fn lock_and_verify_standalone_storage_startup(
    identity: &ConfiguredStaticClusterIdentity,
    storage_node_id: u32,
    data_dir: &Path,
    pg_ids: &[u32],
) -> Result<StaticStorageRuntimeLock, String> {
    if !data_dir.try_exists().map_err(|error| {
        format!(
            "inspect static storage directory {}: {error}",
            data_dir.display()
        )
    })? {
        return Err(format!(
            "static storage directory {} is missing; run initialize-cluster-state before startup",
            data_dir.display()
        ));
    }
    let directory_lock = acquire_storage_directory_lock(data_dir)?;
    let expected = StaticStorageIdentity::new(identity, storage_node_id);
    let identity_path = data_dir.join(STORAGE_IDENTITY_FILE_NAME);
    if !identity_path.try_exists().map_err(|error| {
        format!(
            "inspect static storage identity {}: {error}",
            identity_path.display()
        )
    })? {
        return Err(format!(
            "static storage identity is missing from {}; run initialize-cluster-state before startup",
            data_dir.display()
        ));
    }
    verify_static_storage_identity(&identity_path, &expected)?;
    verify_static_storage_pg_state(data_dir, pg_ids, &expected.encode()?)?;
    remove_completed_initialization_marker(data_dir, &expected)?;
    Ok(StaticStorageRuntimeLock {
        _directory_lock: directory_lock,
    })
}

fn publish_static_storage_identity(
    data_dir: &Path,
    expected: &StaticStorageIdentity,
) -> Result<(), String> {
    let identity_path = data_dir.join(STORAGE_IDENTITY_FILE_NAME);
    let next_path = data_dir.join(STORAGE_IDENTITY_NEXT_FILE_NAME);
    if next_path.try_exists().map_err(|error| {
        format!(
            "inspect static storage identity temporary file {}: {error}",
            next_path.display()
        )
    })? {
        fs::remove_file(&next_path).map_err(|error| {
            format!(
                "remove incomplete static storage identity {}: {error}",
                next_path.display()
            )
        })?;
        sync_directory(
            data_dir,
            "sync removal of incomplete static storage identity",
        )?;
    }
    create_identity_file(&next_path, expected)?;
    sync_directory(data_dir, "sync prepared static storage identity")?;
    fs::rename(&next_path, &identity_path).map_err(|error| {
        format!(
            "publish static storage identity {}: {error}",
            identity_path.display()
        )
    })?;
    sync_directory(data_dir, "sync published static storage identity")?;

    let marker_path = data_dir.join(STORAGE_INITIALIZING_FILE_NAME);
    fs::remove_file(&marker_path).map_err(|error| {
        format!(
            "remove static storage initialization marker {}: {error}",
            marker_path.display()
        )
    })?;
    sync_directory(data_dir, "sync static storage initialization completion")
}

fn publish_initialization_marker(
    data_dir: &Path,
    expected: &StaticStorageIdentity,
) -> Result<(), String> {
    let marker_path = data_dir.join(STORAGE_INITIALIZING_FILE_NAME);
    let next_path = data_dir.join(STORAGE_INITIALIZING_NEXT_FILE_NAME);
    if next_path.try_exists().map_err(|error| {
        format!(
            "inspect static storage initialization temporary file {}: {error}",
            next_path.display()
        )
    })? {
        require_only_directory_entry(data_dir, STORAGE_INITIALIZING_NEXT_FILE_NAME)?;
        fs::remove_file(&next_path).map_err(|error| {
            format!(
                "remove unpublished static storage initialization marker {}: {error}",
                next_path.display()
            )
        })?;
        sync_directory(
            data_dir,
            "sync removal of unpublished static storage initialization marker",
        )?;
    }
    require_empty_storage_directory(data_dir)?;
    create_identity_file(&next_path, expected)?;
    sync_directory(
        data_dir,
        "sync prepared static storage initialization marker",
    )?;
    fs::rename(&next_path, &marker_path).map_err(|error| {
        format!(
            "publish static storage initialization marker {}: {error}",
            marker_path.display()
        )
    })?;
    sync_directory(
        data_dir,
        "sync published static storage initialization marker",
    )
}

fn remove_completed_initialization_marker(
    data_dir: &Path,
    expected: &StaticStorageIdentity,
) -> Result<(), String> {
    let marker_path = data_dir.join(STORAGE_INITIALIZING_FILE_NAME);
    if !marker_path.try_exists().map_err(|error| {
        format!(
            "inspect static storage initialization marker {}: {error}",
            marker_path.display()
        )
    })? {
        return Ok(());
    }
    verify_static_storage_identity(&marker_path, expected)?;
    fs::remove_file(&marker_path).map_err(|error| {
        format!(
            "remove completed static storage initialization marker {}: {error}",
            marker_path.display()
        )
    })?;
    sync_directory(data_dir, "sync completed static storage initialization")
}

fn require_empty_storage_directory(data_dir: &Path) -> Result<(), String> {
    if non_lock_directory_entries(data_dir)?.next().is_some() {
        return Err(format!(
            "static storage directory {} is nonempty but has no durable identity",
            data_dir.display()
        ));
    }
    Ok(())
}

fn require_only_directory_entry(data_dir: &Path, expected_name: &str) -> Result<(), String> {
    let mut entries = non_lock_directory_entries(data_dir)?;
    let Some(entry) = entries.next() else {
        return Err("static storage initialization temporary file disappeared".to_string());
    };
    if entry.file_name() != expected_name || entries.next().is_some() {
        return Err(format!(
            "static storage directory {} contains state without a published initialization marker",
            data_dir.display()
        ));
    }
    Ok(())
}

fn non_lock_directory_entries(path: &Path) -> Result<impl Iterator<Item = fs::DirEntry>, String> {
    let entries = fs::read_dir(path)
        .map_err(|error| format!("read static storage directory {}: {error}", path.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            format!(
                "read static storage directory entry in {}: {error}",
                path.display()
            )
        })?;
    Ok(entries
        .into_iter()
        .filter(|entry| entry.file_name() != STORAGE_INITIALIZATION_LOCK_FILE_NAME))
}

fn verify_static_storage_pg_state(
    data_dir: &Path,
    pg_ids: &[u32],
    expected_identity_bytes: &[u8],
) -> Result<(), String> {
    for pg_id in pg_ids {
        let pg_dir = data_dir.join(format!("pg-{pg_id:04}"));
        require_real_directory(&pg_dir, "static storage PG directory")?;
        let metadata_path = pg_dir.join("metadata.db");
        require_sqlite_metadata_file(&metadata_path, *pg_id)?;
        storage::verify_pg_durable_identity(&pg_dir, *pg_id, expected_identity_bytes)
            .map_err(|error| format!("verify static storage PG {pg_id} identity: {error}"))?;
        let inventory = storage::inspect_pg_shard_inventory(&pg_dir, *pg_id)
            .map_err(|error| format!("inspect static storage PG {pg_id} inventory: {error}"))?;
        if !inventory.authoritative_inventory_is_complete() {
            return Err(format!(
                "static standalone storage PG {pg_id} has incomplete authoritative shard \
                 inventory: rows={} files={} missing={} size_mismatches={}",
                inventory.shard_row_count,
                inventory.shard_file_count,
                inventory.authoritative_missing_file_count,
                inventory.authoritative_size_mismatch_count,
            ));
        }
    }
    Ok(())
}

fn sync_initialized_pg_state(
    data_dir: &Path,
    pg_ids: &[u32],
    expected_identity_bytes: &[u8],
) -> Result<(), String> {
    verify_static_storage_pg_state(data_dir, pg_ids, expected_identity_bytes)?;
    for pg_id in pg_ids {
        let pg_dir = data_dir.join(format!("pg-{pg_id:04}"));
        File::open(pg_dir.join("metadata.db"))
            .and_then(|file| file.sync_all())
            .map_err(|error| format!("sync initialized PG {pg_id} metadata: {error}"))?;
        sync_directory(&pg_dir, "sync initialized static storage PG directory")?;
    }
    sync_directory(data_dir, "sync initialized static storage root")
}

fn require_real_directory(path: &Path, context: &str) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("{context} {} is unavailable: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "{context} {} is not a real directory",
            path.display()
        ));
    }
    Ok(())
}

fn require_sqlite_metadata_file(path: &Path, pg_id: u32) -> Result<(), String> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let mut file = options
        .open(path)
        .map_err(|error| format!("static storage PG {pg_id} metadata is unavailable: {error}"))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("inspect static storage PG {pg_id} metadata: {error}"))?;
    if !metadata.is_file() {
        return Err(format!(
            "static storage PG {pg_id} metadata is not a regular file"
        ));
    }
    let mut magic = [0_u8; SQLITE_FILE_MAGIC.len()];
    file.read_exact(&mut magic)
        .map_err(|error| format!("read static storage PG {pg_id} metadata header: {error}"))?;
    if magic != *SQLITE_FILE_MAGIC {
        return Err(format!(
            "static storage PG {pg_id} metadata has invalid SQLite identity"
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct StaticStorageDirectoryLock {
    _file: File,
}

#[derive(Debug)]
pub(crate) struct StaticStorageRuntimeLock {
    _directory_lock: StaticStorageDirectoryLock,
}

fn acquire_storage_directory_lock(data_dir: &Path) -> Result<StaticStorageDirectoryLock, String> {
    let path = data_dir.join(STORAGE_INITIALIZATION_LOCK_FILE_NAME);
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .mode(PRIVATE_FILE_MODE);
    let file = options.open(&path).map_err(|error| {
        format!(
            "open static storage initialization lock {}: {error}",
            path.display()
        )
    })?;
    // SAFETY: `file` owns a valid descriptor for the lifetime of the lock.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        return if error.kind() == std::io::ErrorKind::WouldBlock {
            Err(format!(
                "static storage initialization or runtime is already active for {}",
                data_dir.display()
            ))
        } else {
            Err(format!(
                "lock static storage initialization for {}: {error}",
                data_dir.display()
            ))
        };
    }
    Ok(StaticStorageDirectoryLock { _file: file })
}

fn ensure_private_data_directory_durable(data_dir: &Path) -> Result<(), String> {
    let sentinel_path = data_dir.join(STORAGE_IDENTITY_FILE_NAME);
    ensure_control_plane_state_parent_directory(&sentinel_path)
        .map_err(|error| format!("durably create static storage directory: {error}"))?;
    fs::set_permissions(data_dir, fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE)).map_err(
        |error| {
            format!(
                "set private permissions on static storage directory {}: {error}",
                data_dir.display()
            )
        },
    )?;
    File::open(data_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            format!(
                "sync private static storage directory {}: {error}",
                data_dir.display()
            )
        })
}

fn create_identity_file(path: &Path, identity: &StaticStorageIdentity) -> Result<(), String> {
    let bytes = identity.encode()?;
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .mode(PRIVATE_FILE_MODE);
    let mut file = options
        .open(path)
        .map_err(|error| format!("create static storage identity {}: {error}", path.display()))?;
    file.write_all(&bytes)
        .map_err(|error| format!("write static storage identity {}: {error}", path.display()))?;
    file.sync_all()
        .map_err(|error| format!("sync static storage identity {}: {error}", path.display()))
}

fn verify_static_storage_identity(
    path: &Path,
    expected: &StaticStorageIdentity,
) -> Result<(), String> {
    let actual = read_static_storage_identity(path)?;
    actual.verify(expected)
}

fn sync_identity_file(path: &Path, context: &'static str) -> Result<(), String> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    options
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| format!("{context} {}: {error}", path.display()))
}

fn read_static_storage_identity(path: &Path) -> Result<StaticStorageIdentity, String> {
    let bytes =
        read_bounded_identity_file(path, STORAGE_IDENTITY_MAX_BYTES, "static storage identity")?;
    StaticStorageIdentity::decode(&bytes)
}

fn read_bounded_identity_file(
    path: &Path,
    max_bytes: u64,
    label: &'static str,
) -> Result<Vec<u8>, String> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .map_err(|error| format!("open {label} {}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("inspect {label} {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{label} {} is not a regular file", path.display()));
    }
    if metadata.len() > max_bytes {
        return Err(format!("{label} exceeds its size bound"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read {label} {}: {error}", path.display()))?;
    if bytes.len() > max_bytes as usize {
        return Err(format!("{label} exceeds its size bound"));
    }
    Ok(bytes)
}

fn encode_string(bytes: &mut Vec<u8>, value: &str, field: &str) -> Result<(), String> {
    let len = u16::try_from(value.len())
        .map_err(|_| format!("static storage identity {field} is too long"))?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn sync_directory(path: &Path, context: &'static str) -> Result<(), String> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("{context} {}: {error}", path.display()))
}

struct IdentityDecoder<'a> {
    remaining: &'a [u8],
}

impl<'a> IdentityDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn expect_magic(&mut self) -> Result<(), String> {
        let magic = self.take(STORAGE_IDENTITY_MAGIC.len(), "magic")?;
        if magic != STORAGE_IDENTITY_MAGIC {
            return Err("static storage identity has invalid magic".to_string());
        }
        Ok(())
    }

    fn read_u16(&mut self, field: &str) -> Result<u16, String> {
        let bytes: [u8; 2] = self
            .take(2, field)?
            .try_into()
            .map_err(|_| format!("static storage identity has truncated {field}"))?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn read_u32(&mut self, field: &str) -> Result<u32, String> {
        let bytes: [u8; 4] = self
            .take(4, field)?
            .try_into()
            .map_err(|_| format!("static storage identity has truncated {field}"))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self, field: &str) -> Result<u64, String> {
        let bytes: [u8; 8] = self
            .take(8, field)?
            .try_into()
            .map_err(|_| format!("static storage identity has truncated {field}"))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_string(&mut self, field: &str) -> Result<String, String> {
        let len = usize::from(self.read_u16(field)?);
        let bytes = self.take(len, field)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| format!("static storage identity has invalid UTF-8 in {field}"))
    }

    fn take(&mut self, len: usize, field: &str) -> Result<&'a [u8], String> {
        if self.remaining.len() < len {
            return Err(format!("static storage identity has truncated {field}"));
        }
        let (value, remaining) = self.remaining.split_at(len);
        self.remaining = remaining;
        Ok(value)
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::ShardKey;

    fn identity(process_id: &str, process_digest: &str) -> ConfiguredStaticClusterIdentity {
        ConfiguredStaticClusterIdentity {
            cluster_id: "cluster-a".to_string(),
            topology_generation: 7,
            topology_digest: "a".repeat(64),
            process_id: process_id.to_string(),
            process_identity_digest: process_digest.repeat(64),
        }
    }

    fn initialize_storage(
        identity: &ConfiguredStaticClusterIdentity,
        storage_node_id: u32,
        data_dir: &Path,
        pg_ids: &[u32],
        ec_shape: EcShape,
    ) -> Result<(), String> {
        initialize_standalone_storage(
            identity,
            storage_node_id,
            data_dir,
            pg_ids,
            ec_shape,
            ClusterEpoch::INITIAL,
        )
    }

    fn write_control_plane_restart_set(state_path: &Path) {
        let file_name = state_path.file_name().unwrap().to_str().unwrap();
        fs::write(state_path, b"durable artifact").unwrap();
        fs::write(
            state_path.with_file_name(format!("{file_name}.sentinel")),
            b"durable sentinel",
        )
        .unwrap();
    }

    #[test]
    fn static_control_plane_identity_requires_explicit_initialization_and_reopens_exact_identity() {
        let temp = test_util::tempdir();
        let state_path = temp.path().join("control.state");
        let expected = identity("control-1", "b");

        let error = bind_static_control_plane_identity(&expected, 101, &state_path).unwrap_err();
        assert!(error.contains("run initialize-cluster-state"));
        initialize_static_control_plane_identity(&expected, 101, &state_path).unwrap();
        assert!(!bind_static_control_plane_identity(&expected, 101, &state_path).unwrap());

        let identity_path = static_control_plane_identity_path(&state_path);
        assert!(identity_path.is_file());
        assert_eq!(
            read_static_control_plane_identity(&identity_path).unwrap(),
            StaticControlPlaneIdentity::new(&expected, 101)
        );
    }

    #[test]
    fn static_control_plane_identity_initialization_rejects_concurrent_initializer() {
        let temp = test_util::tempdir();
        let state_path = temp.path().join("control.state");
        let expected = identity("control-1", "b");
        let first_path = state_path.clone();
        let first_expected = expected.clone();
        let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let first = std::thread::spawn(move || {
            initialize_static_control_plane_identity_after_lock(
                &first_expected,
                101,
                &first_path,
                || {
                    locked_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                },
            )
        });
        locked_rx.recv().unwrap();

        let error =
            initialize_static_control_plane_identity(&expected, 101, &state_path).unwrap_err();
        assert!(error.contains("already locked by another manager"));

        release_tx.send(()).unwrap();
        first.join().unwrap().unwrap();
        bind_static_control_plane_identity(&expected, 101, &state_path).unwrap();
    }

    #[test]
    fn static_control_plane_identity_rejects_changed_process_or_topology() {
        let temp = test_util::tempdir();
        let state_path = temp.path().join("control.state");
        initialize_static_control_plane_identity(&identity("control-1", "b"), 101, &state_path)
            .unwrap();

        let process_error =
            bind_static_control_plane_identity(&identity("control-2", "c"), 101, &state_path)
                .unwrap_err();
        assert!(process_error.contains("different process"));

        let mut changed_topology = identity("control-1", "b");
        changed_topology.topology_generation += 1;
        let topology_error =
            bind_static_control_plane_identity(&changed_topology, 101, &state_path).unwrap_err();
        assert!(topology_error.contains("topology generation"));
    }

    #[test]
    fn static_control_plane_identity_rejects_existing_state_without_sidecar() {
        let temp = test_util::tempdir();
        let state_path = temp.path().join("control.state");
        fs::write(&state_path, b"existing artifact").unwrap();

        let error =
            bind_static_control_plane_identity(&identity("control-1", "b"), 101, &state_path)
                .unwrap_err();

        assert!(error.contains("identity is missing beside existing durable state"));
    }

    #[test]
    fn static_control_plane_identity_rejects_existing_wal_without_sidecar() {
        let temp = test_util::tempdir();
        let state_path = temp.path().join("control.state");
        fs::write(temp.path().join("control.state.wal"), b"existing WAL").unwrap();

        let error =
            bind_static_control_plane_identity(&identity("control-1", "b"), 101, &state_path)
                .unwrap_err();

        assert!(error.contains("identity is missing beside existing durable state"));
    }

    #[test]
    fn static_control_plane_established_identity_rejects_empty_destination() {
        let temp = test_util::tempdir();
        let state_path = temp.path().join("control.state");
        let expected = identity("control-1", "b");
        initialize_static_control_plane_identity(&expected, 101, &state_path).unwrap();
        write_control_plane_restart_set(&state_path);
        mark_static_control_plane_identity_established(&expected, 101, &state_path).unwrap();
        fs::remove_file(&state_path).unwrap();

        let error = bind_static_control_plane_identity(&expected, 101, &state_path).unwrap_err();

        assert!(error.contains("established static control-plane identity exists without"));
    }

    #[test]
    fn static_control_plane_complete_relocation_preserves_identity_binding() {
        let temp = test_util::tempdir();
        let source_path = temp.path().join("source.state");
        let destination_path = temp.path().join("destination.state");
        let expected = identity("control-1", "b");
        initialize_static_control_plane_identity(&expected, 101, &source_path).unwrap();
        write_control_plane_restart_set(&source_path);
        mark_static_control_plane_identity_established(&expected, 101, &source_path).unwrap();

        fs::copy(&source_path, &destination_path).unwrap();
        fs::copy(
            source_path.with_file_name("source.state.sentinel"),
            destination_path.with_file_name("destination.state.sentinel"),
        )
        .unwrap();
        fs::copy(
            static_control_plane_identity_path(&source_path),
            static_control_plane_identity_path(&destination_path),
        )
        .unwrap();

        assert!(
            bind_static_control_plane_identity(&expected, 101, &destination_path).unwrap(),
            "relocated established state must retain its lifecycle state"
        );
    }

    #[test]
    fn static_control_plane_established_identity_rejects_empty_wal_as_restart_evidence() {
        let temp = test_util::tempdir();
        let state_path = temp.path().join("control.state");
        let expected = identity("control-1", "b");
        initialize_static_control_plane_identity(&expected, 101, &state_path).unwrap();
        write_control_plane_restart_set(&state_path);
        mark_static_control_plane_identity_established(&expected, 101, &state_path).unwrap();
        fs::remove_file(&state_path).unwrap();
        fs::remove_file(temp.path().join("control.state.sentinel")).unwrap();
        File::create(temp.path().join("control.state.wal")).unwrap();

        let error = bind_static_control_plane_identity(&expected, 101, &state_path).unwrap_err();

        assert!(error.contains("established static control-plane identity exists without"));
    }

    #[test]
    fn static_control_plane_identity_digest_covers_established_state() {
        let temp = test_util::tempdir();
        let state_path = temp.path().join("control.state");
        let expected = identity("control-1", "b");
        initialize_static_control_plane_identity(&expected, 101, &state_path).unwrap();
        write_control_plane_restart_set(&state_path);
        mark_static_control_plane_identity_established(&expected, 101, &state_path).unwrap();
        let identity_path = static_control_plane_identity_path(&state_path);
        let mut bytes = fs::read(&identity_path).unwrap();
        let established_offset = CONTROL_PLANE_IDENTITY_MAGIC.len() + 2 + 8 + 8;
        assert_eq!(bytes[established_offset], 1);
        bytes[established_offset] = 0;
        fs::write(&identity_path, bytes).unwrap();

        let error = bind_static_control_plane_identity(&expected, 101, &state_path).unwrap_err();

        assert!(error.contains("invalid digest"));
    }

    #[test]
    fn static_storage_requires_explicit_initialization() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("storage");

        let error =
            lock_and_verify_standalone_storage_startup(&identity("all-1", "b"), 1, &data_dir, &[0])
                .unwrap_err();

        assert!(error.contains("initialize-cluster-state"));
        assert!(!data_dir.exists());
    }

    #[test]
    fn static_storage_initialization_publishes_complete_identity_bound_state() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("storage");
        let expected = identity("all-1", "b");

        initialize_storage(&expected, 1, &data_dir, &[0, 1], EcShape { k: 1, m: 0 }).unwrap();

        lock_and_verify_standalone_storage_startup(&expected, 1, &data_dir, &[0, 1]).unwrap();
        assert!(data_dir.join(STORAGE_IDENTITY_FILE_NAME).is_file());
        assert!(!data_dir.join(STORAGE_INITIALIZING_FILE_NAME).exists());
        assert!(data_dir.join("pg-0000/metadata.db").is_file());
        assert!(data_dir.join("pg-0001/metadata.db").is_file());
    }

    #[test]
    fn static_storage_rejects_wrong_process_identity() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("storage");
        initialize_storage(
            &identity("all-1", "b"),
            1,
            &data_dir,
            &[0],
            EcShape { k: 1, m: 0 },
        )
        .unwrap();

        let error =
            lock_and_verify_standalone_storage_startup(&identity("all-2", "c"), 1, &data_dir, &[0])
                .unwrap_err();

        assert!(error.contains("different process"));
    }

    #[test]
    fn static_storage_rejects_wrong_cluster_and_topology_generation() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("storage");
        let expected = identity("all-1", "b");
        initialize_storage(&expected, 1, &data_dir, &[0], EcShape { k: 1, m: 0 }).unwrap();

        let mut wrong_cluster = expected.clone();
        wrong_cluster.cluster_id = "cluster-b".to_string();
        assert!(
            lock_and_verify_standalone_storage_startup(&wrong_cluster, 1, &data_dir, &[0])
                .unwrap_err()
                .contains("different cluster")
        );

        let mut wrong_generation = expected;
        wrong_generation.topology_generation += 1;
        assert!(
            lock_and_verify_standalone_storage_startup(&wrong_generation, 1, &data_dir, &[0])
                .unwrap_err()
                .contains("different topology generation")
        );
    }

    #[test]
    fn static_storage_accepts_complete_relocation() {
        let temp = test_util::tempdir();
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        let expected = identity("all-1", "b");
        initialize_storage(&expected, 1, &source, &[0], EcShape { k: 1, m: 0 }).unwrap();

        fs::rename(&source, &destination).unwrap();

        lock_and_verify_standalone_storage_startup(&expected, 1, &destination, &[0]).unwrap();
    }

    #[test]
    fn static_storage_restart_accepts_unindexed_crash_residue() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("storage");
        let expected = identity("all-1", "b");
        initialize_storage(&expected, 1, &data_dir, &[0], EcShape { k: 1, m: 0 }).unwrap();
        let key = ShardKey::new(&[7; 16], 11, 0);
        let prefix_dir = data_dir.join("pg-0000/shards").join(key.hex_prefix());
        fs::create_dir(&prefix_dir).unwrap();
        fs::write(
            prefix_dir.join(key.to_string()),
            b"unregistered crash residue",
        )
        .unwrap();

        lock_and_verify_standalone_storage_startup(&expected, 1, &data_dir, &[0]).unwrap();
    }

    #[test]
    fn static_storage_rejects_symlinked_shard_root() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("storage");
        let external_shards = temp.path().join("external-shards");
        let expected = identity("all-1", "b");
        initialize_storage(&expected, 1, &data_dir, &[0], EcShape { k: 1, m: 0 }).unwrap();
        fs::rename(data_dir.join("pg-0000/shards"), &external_shards).unwrap();
        std::os::unix::fs::symlink(&external_shards, data_dir.join("pg-0000/shards")).unwrap();

        let error =
            lock_and_verify_standalone_storage_startup(&expected, 1, &data_dir, &[0]).unwrap_err();

        assert!(error.contains("shard root is not a real directory"));
    }

    #[test]
    fn static_storage_rejects_identity_only_relocation() {
        let temp = test_util::tempdir();
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        let expected = identity("all-1", "b");
        initialize_storage(&expected, 1, &source, &[0], EcShape { k: 1, m: 0 }).unwrap();
        fs::create_dir(&destination).unwrap();
        fs::copy(
            source.join(STORAGE_IDENTITY_FILE_NAME),
            destination.join(STORAGE_IDENTITY_FILE_NAME),
        )
        .unwrap();

        let error = lock_and_verify_standalone_storage_startup(&expected, 1, &destination, &[0])
            .unwrap_err();

        assert!(error.contains("PG directory"));
    }

    #[test]
    fn static_storage_rejects_identity_with_placeholder_pg_database() {
        let temp = test_util::tempdir();
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        let expected = identity("all-1", "b");
        initialize_storage(&expected, 1, &source, &[0], EcShape { k: 1, m: 0 }).unwrap();
        fs::create_dir_all(destination.join("pg-0000")).unwrap();
        fs::copy(
            source.join(STORAGE_IDENTITY_FILE_NAME),
            destination.join(STORAGE_IDENTITY_FILE_NAME),
        )
        .unwrap();
        fs::write(destination.join("pg-0000/metadata.db"), b"").unwrap();

        let error = lock_and_verify_standalone_storage_startup(&expected, 1, &destination, &[0])
            .unwrap_err();

        assert!(error.contains("metadata header"));
    }

    #[test]
    fn static_storage_rejects_identity_with_unbound_valid_pg_database() {
        let temp = test_util::tempdir();
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        let expected = identity("all-1", "b");
        initialize_storage(&expected, 1, &source, &[0], EcShape { k: 1, m: 0 }).unwrap();
        fs::create_dir(&destination).unwrap();
        fs::copy(
            source.join(STORAGE_IDENTITY_FILE_NAME),
            destination.join(STORAGE_IDENTITY_FILE_NAME),
        )
        .unwrap();
        let node_id = NodeId::new(1);
        let placeholder = LocalClusterMap::open_with_configs(
            node_id,
            [LocalNodeStoreConfig::new(node_id, &destination)],
            &[0],
            EcShape { k: 1, m: 0 },
        )
        .unwrap();
        drop(placeholder);

        let error = lock_and_verify_standalone_storage_startup(&expected, 1, &destination, &[0])
            .unwrap_err();

        assert!(error.contains("durable identity"));
    }

    #[test]
    fn static_storage_initialization_rejects_nonempty_unbound_directory() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("storage");
        fs::create_dir(&data_dir).unwrap();
        fs::write(data_dir.join("metadata.db"), b"unbound").unwrap();

        let error = initialize_storage(
            &identity("all-1", "b"),
            1,
            &data_dir,
            &[0],
            EcShape { k: 1, m: 0 },
        )
        .unwrap_err();

        assert!(error.contains("nonempty but has no durable identity"));
    }

    #[test]
    fn static_storage_initialization_resumes_matching_incomplete_initialization() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("storage");
        let expected = identity("all-1", "b");
        ensure_private_data_directory_durable(&data_dir).unwrap();
        create_identity_file(
            &data_dir.join(STORAGE_INITIALIZING_FILE_NAME),
            &StaticStorageIdentity::new(&expected, 1),
        )
        .unwrap();
        fs::create_dir(data_dir.join("pg-0000")).unwrap();

        initialize_storage(&expected, 1, &data_dir, &[0], EcShape { k: 1, m: 0 }).unwrap();

        lock_and_verify_standalone_storage_startup(&expected, 1, &data_dir, &[0]).unwrap();
    }

    #[test]
    fn static_storage_normal_startup_rejects_incomplete_initialization() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("storage");
        let expected = identity("all-1", "b");
        ensure_private_data_directory_durable(&data_dir).unwrap();
        create_identity_file(
            &data_dir.join(STORAGE_INITIALIZING_FILE_NAME),
            &StaticStorageIdentity::new(&expected, 1),
        )
        .unwrap();

        let error =
            lock_and_verify_standalone_storage_startup(&expected, 1, &data_dir, &[0]).unwrap_err();

        assert!(error.contains("initialize-cluster-state"));
    }

    #[test]
    fn static_storage_initialization_discards_unpublished_torn_marker() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("storage");
        let expected = identity("all-1", "b");
        ensure_private_data_directory_durable(&data_dir).unwrap();
        fs::write(data_dir.join(STORAGE_INITIALIZING_NEXT_FILE_NAME), b"torn").unwrap();

        initialize_storage(&expected, 1, &data_dir, &[0], EcShape { k: 1, m: 0 }).unwrap();

        lock_and_verify_standalone_storage_startup(&expected, 1, &data_dir, &[0]).unwrap();
    }

    #[test]
    fn static_storage_initialization_rejects_concurrent_initializer() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("storage");
        let expected = identity("all-1", "b");
        ensure_private_data_directory_durable(&data_dir).unwrap();
        let _lock = acquire_storage_directory_lock(&data_dir).unwrap();

        let error =
            initialize_storage(&expected, 1, &data_dir, &[0], EcShape { k: 1, m: 0 }).unwrap_err();

        assert!(error.contains("already active"));
    }
}
