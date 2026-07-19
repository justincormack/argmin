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
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .map_err(|error| format!("open static storage identity {}: {error}", path.display()))?;
    let metadata = file.metadata().map_err(|error| {
        format!(
            "inspect static storage identity {}: {error}",
            path.display()
        )
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "static storage identity {} is not a regular file",
            path.display()
        ));
    }
    if metadata.len() > STORAGE_IDENTITY_MAX_BYTES {
        return Err("static storage identity exceeds its size bound".to_string());
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(STORAGE_IDENTITY_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read static storage identity {}: {error}", path.display()))?;
    if bytes.len() > STORAGE_IDENTITY_MAX_BYTES as usize {
        return Err("static storage identity exceeds its size bound".to_string());
    }
    StaticStorageIdentity::decode(&bytes)
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
