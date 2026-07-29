use checksum::{ChecksumAlgorithm, ChecksumHasher};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use thiserror::Error;

const IDENTITY_FILE_NAME: &str = ".argmin-standalone-route.identity";
const IDENTITY_NEXT_FILE_NAME: &str = ".argmin-standalone-route.identity.next";
const INITIALIZING_FILE_NAME: &str = ".argmin-standalone-route.initializing";
const LOCK_FILE_NAME: &str = ".argmin-standalone-route.lock";
const IDENTITY_MAGIC: &[u8; 8] = b"ARGSRTID";
const INITIALIZING_MAGIC: &[u8; 8] = b"ARGSRTIN";
const FORMAT_VERSION: u16 = 1;
const DIGEST_LEN: usize = 32;
const IDENTITY_LEN: usize = IDENTITY_MAGIC.len() + 2 + DIGEST_LEN + DIGEST_LEN;
const PRIVATE_FILE_MODE: u32 = 0o600;
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const MAX_INITIALIZING_SCAFFOLDING_ENTRIES: usize = 1_024;
const MAX_INITIALIZING_SCAFFOLDING_DEPTH: usize = 64;

#[derive(Debug, Error)]
pub enum StandaloneRouteIdentityError {
    #[error("standalone route identity requires static route authority")]
    DynamicAuthority,
    #[error("standalone route identity directory {path} contains state without a durable route identity")]
    StateWithoutIdentity { path: PathBuf },
    #[error("standalone route identity directory {path} is already in use")]
    LockContended { path: PathBuf },
    #[error("standalone route identity does not match the configured static topology")]
    IdentityMismatch,
    #[error("standalone route identity is invalid: {reason}")]
    InvalidIdentity { reason: &'static str },
    #[error("{operation} failed for standalone route identity {path}: {kind:?}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        kind: io::ErrorKind,
    },
}

impl StandaloneRouteIdentityError {
    fn io(operation: &'static str, path: &Path, error: io::Error) -> Self {
        Self::Io {
            operation,
            path: path.to_path_buf(),
            kind: error.kind(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StandaloneRouteIdentity(pub(crate) [u8; DIGEST_LEN]);

impl std::fmt::Debug for StandaloneRouteIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("StandaloneRouteIdentity")
            .field(&"<opaque>")
            .finish()
    }
}

impl StandaloneRouteIdentity {
    /// Compose two storage-owned route identities for a process that hosts
    /// both routing and storage-node responsibilities.
    #[must_use]
    pub fn combined_with(self, other: Self) -> Self {
        let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
        hasher.update(b"argmin/standalone-combined-route-identity/v1");
        hasher.update(&self.0);
        hasher.update(&other.0);
        Self(
            hasher
                .finalize()
                .bytes()
                .try_into()
                .expect("SHA-256 combined standalone route identity contains 32 bytes"),
        )
    }
}

#[derive(Debug)]
pub struct StandaloneRouteIdentityPreparation {
    data_dir: PathBuf,
    directory: File,
    directory_identity: FileIdentity,
    lock_file: File,
    lock_identity: FileIdentity,
    existing_identity: Option<StandaloneRouteIdentity>,
    existing_identity_file: Option<FileIdentity>,
    pending_identity: Option<StandaloneRouteIdentity>,
    pending_identity_file: Option<FileIdentity>,
    initializing_file: Option<FileIdentity>,
}

#[derive(Debug)]
pub struct StandaloneRouteIdentityLock {
    _data_dir: PathBuf,
    _directory: File,
    _lock_file: File,
    _directory_identity: FileIdentity,
    _lock_identity: FileIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

impl StandaloneRouteIdentityPreparation {
    pub fn acquire(data_dir: &Path) -> Result<Self, StandaloneRouteIdentityError> {
        match fs::symlink_metadata(data_dir) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(StandaloneRouteIdentityError::InvalidIdentity {
                    reason: "identity directory must not be a symbolic link",
                });
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(StandaloneRouteIdentityError::io(
                    "inspect directory",
                    data_dir,
                    error,
                ));
            }
        }
        crate::data_dir::prepare_private_data_dir(data_dir).map_err(|error| {
            StandaloneRouteIdentityError::io("prepare directory", data_dir, error)
        })?;
        if fs::symlink_metadata(data_dir)
            .map_err(|error| {
                StandaloneRouteIdentityError::io("inspect directory", data_dir, error)
            })?
            .file_type()
            .is_symlink()
        {
            return Err(StandaloneRouteIdentityError::InvalidIdentity {
                reason: "identity directory must not be a symbolic link",
            });
        }
        let directory = open_directory(data_dir)?;
        let directory_identity = require_directory_path_identity(data_dir, &directory)?;
        lock_exclusive_nonblocking(&directory, data_dir, "lock identity directory")?;
        let lock_file = open_private_lock_file_at(&directory, data_dir)?;
        let lock_identity =
            FileIdentity::from_metadata(&lock_file.metadata().map_err(|error| {
                StandaloneRouteIdentityError::io(
                    "inspect lock",
                    &data_dir.join(LOCK_FILE_NAME),
                    error,
                )
            })?);
        lock_exclusive_nonblocking(&lock_file, data_dir, "lock identity entry")?;

        let (existing_identity, existing_identity_file) =
            if path_exists_at(&directory, data_dir, IDENTITY_FILE_NAME, "inspect identity")? {
                let (identity, file_identity) =
                    read_identity_at(&directory, data_dir, IDENTITY_FILE_NAME)?;
                (Some(identity), Some(file_identity))
            } else {
                (None, None)
            };
        let (pending_identity, pending_identity_file) = if path_exists_at(
            &directory,
            data_dir,
            IDENTITY_NEXT_FILE_NAME,
            "inspect prepared identity",
        )? {
            let (identity, file_identity) =
                read_identity_at(&directory, data_dir, IDENTITY_NEXT_FILE_NAME)?;
            (Some(identity), Some(file_identity))
        } else {
            (None, None)
        };
        let initializing_exists = path_exists_at(
            &directory,
            data_dir,
            INITIALIZING_FILE_NAME,
            "inspect initialization marker",
        )?;
        let mut initializing_file = initializing_exists
            .then(|| verify_initializing_marker_at(&directory, data_dir))
            .transpose()?;

        match (existing_identity, pending_identity, initializing_exists) {
            (Some(_), Some(_), _) => {
                return Err(StandaloneRouteIdentityError::InvalidIdentity {
                    reason: "published and prepared identities coexist",
                });
            }
            (None, Some(_), false) => {
                return Err(StandaloneRouteIdentityError::StateWithoutIdentity {
                    path: data_dir.to_path_buf(),
                });
            }
            (None, _, true) => require_initializing_scaffolding(data_dir)?,
            (None, None, false) => {
                require_empty_except_lock(data_dir)?;
                write_new_file_at(
                    &directory,
                    data_dir,
                    INITIALIZING_FILE_NAME,
                    &[INITIALIZING_MAGIC.as_slice(), &FORMAT_VERSION.to_be_bytes()].concat(),
                    "create initialization marker",
                )?;
                sync_directory(&directory, data_dir, "sync initialization marker")?;
                initializing_file = Some(verify_initializing_marker_at(&directory, data_dir)?);
            }
            (Some(_), None, _) => {}
        }

        require_directory_path_identity_matches(data_dir, &directory, directory_identity)?;
        require_lock_entry_matches(data_dir, &directory, &lock_file, lock_identity)?;

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            directory,
            directory_identity,
            lock_file,
            lock_identity,
            existing_identity,
            existing_identity_file,
            pending_identity,
            pending_identity_file,
            initializing_file,
        })
    }

    pub fn bind(
        self,
        expected: StandaloneRouteIdentity,
    ) -> Result<StandaloneRouteIdentityLock, StandaloneRouteIdentityError> {
        self.require_capability_unchanged()?;
        self.require_state_unchanged()?;
        if let Some(actual) = self.existing_identity {
            if actual != expected {
                return Err(StandaloneRouteIdentityError::IdentityMismatch);
            }
        } else if let Some(pending) = self.pending_identity {
            if pending != expected {
                return Err(StandaloneRouteIdentityError::IdentityMismatch);
            }
            publish_prepared_identity_at(&self.directory, &self.data_dir)?;
        } else {
            publish_identity_at(&self.directory, &self.data_dir, expected)?;
        }

        if path_exists_at(
            &self.directory,
            &self.data_dir,
            INITIALIZING_FILE_NAME,
            "inspect initialization marker",
        )? {
            unlink_file_at(
                &self.directory,
                &self.data_dir,
                INITIALIZING_FILE_NAME,
                "remove initialization marker",
            )?;
            sync_directory(
                &self.directory,
                &self.data_dir,
                "sync completed identity binding",
            )?;
        }
        self.require_capability_unchanged()?;

        Ok(StandaloneRouteIdentityLock {
            _data_dir: self.data_dir,
            _directory: self.directory,
            _lock_file: self.lock_file,
            _directory_identity: self.directory_identity,
            _lock_identity: self.lock_identity,
        })
    }

    fn require_capability_unchanged(&self) -> Result<(), StandaloneRouteIdentityError> {
        require_directory_path_identity_matches(
            &self.data_dir,
            &self.directory,
            self.directory_identity,
        )?;
        require_lock_entry_matches(
            &self.data_dir,
            &self.directory,
            &self.lock_file,
            self.lock_identity,
        )
    }

    fn require_state_unchanged(&self) -> Result<(), StandaloneRouteIdentityError> {
        require_identity_artifact_matches(
            &self.directory,
            &self.data_dir,
            IDENTITY_FILE_NAME,
            self.existing_identity,
            self.existing_identity_file,
            "published identity changed after acquisition",
        )?;
        require_identity_artifact_matches(
            &self.directory,
            &self.data_dir,
            IDENTITY_NEXT_FILE_NAME,
            self.pending_identity,
            self.pending_identity_file,
            "prepared identity changed after acquisition",
        )?;
        require_marker_artifact_matches(&self.directory, &self.data_dir, self.initializing_file)?;
        if self.existing_identity.is_none() {
            require_initializing_scaffolding(&self.data_dir)?;
        }
        Ok(())
    }
}

fn open_directory(path: &Path) -> Result<File, StandaloneRouteIdentityError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|error| StandaloneRouteIdentityError::io("open directory", path, error))
}

fn lock_exclusive_nonblocking(
    file: &File,
    data_dir: &Path,
    operation: &'static str,
) -> Result<(), StandaloneRouteIdentityError> {
    // SAFETY: `flock` operates on the valid descriptor owned by `file`.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::WouldBlock {
        Err(StandaloneRouteIdentityError::LockContended {
            path: data_dir.to_path_buf(),
        })
    } else {
        Err(StandaloneRouteIdentityError::io(operation, data_dir, error))
    }
}

fn open_private_lock_file_at(
    directory: &File,
    data_dir: &Path,
) -> Result<File, StandaloneRouteIdentityError> {
    let path = data_dir.join(LOCK_FILE_NAME);
    let file = open_file_at(
        directory,
        LOCK_FILE_NAME,
        libc::O_RDWR | libc::O_CREAT | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        PRIVATE_FILE_MODE,
    )
    .map_err(|error| StandaloneRouteIdentityError::io("open lock", &path, error))?;
    require_private_regular_file(&path, &file)?;
    Ok(file)
}

fn open_file_at(
    directory: &File,
    name: &'static str,
    flags: libc::c_int,
    mode: u32,
) -> io::Result<File> {
    let name = std::ffi::CString::new(name).expect("static identity artifact name has no NUL");
    // SAFETY: `directory` and `name` remain valid for the call. A successful
    // call returns one owned descriptor, transferred immediately to `File`.
    let descriptor = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags, mode) };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `descriptor` is a new owned descriptor returned by `openat`.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn require_directory_path_identity(
    path: &Path,
    directory: &File,
) -> Result<FileIdentity, StandaloneRouteIdentityError> {
    let held_metadata = directory
        .metadata()
        .map_err(|error| StandaloneRouteIdentityError::io("inspect directory", path, error))?;
    if !held_metadata.is_dir() {
        return Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "identity directory descriptor is not a directory",
        });
    }
    let identity = FileIdentity::from_metadata(&held_metadata);
    require_directory_path_identity_matches(path, directory, identity)?;
    Ok(identity)
}

fn require_directory_path_identity_matches(
    path: &Path,
    directory: &File,
    expected: FileIdentity,
) -> Result<(), StandaloneRouteIdentityError> {
    let held_metadata = directory
        .metadata()
        .map_err(|error| StandaloneRouteIdentityError::io("inspect directory", path, error))?;
    let path_metadata = fs::symlink_metadata(path).map_err(|error| {
        StandaloneRouteIdentityError::io("inspect directory entry", path, error)
    })?;
    if !path_metadata.is_dir()
        || FileIdentity::from_metadata(&held_metadata) != expected
        || FileIdentity::from_metadata(&path_metadata) != expected
    {
        return Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "identity directory entry changed after acquisition",
        });
    }
    Ok(())
}

fn require_lock_entry_matches(
    data_dir: &Path,
    directory: &File,
    held_lock: &File,
    expected: FileIdentity,
) -> Result<(), StandaloneRouteIdentityError> {
    let path = data_dir.join(LOCK_FILE_NAME);
    let entry = open_file_at(
        directory,
        LOCK_FILE_NAME,
        libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        0,
    )
    .map_err(|error| StandaloneRouteIdentityError::io("reopen lock entry", &path, error))?;
    require_private_regular_file(&path, &entry)?;
    let held_identity =
        FileIdentity::from_metadata(&held_lock.metadata().map_err(|error| {
            StandaloneRouteIdentityError::io("inspect held lock", &path, error)
        })?);
    let entry_identity =
        FileIdentity::from_metadata(&entry.metadata().map_err(|error| {
            StandaloneRouteIdentityError::io("inspect lock entry", &path, error)
        })?);
    if held_identity != expected || entry_identity != expected {
        return Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "identity lock entry changed after acquisition",
        });
    }
    Ok(())
}

fn require_private_regular_file(
    path: &Path,
    file: &File,
) -> Result<(), StandaloneRouteIdentityError> {
    let metadata = file
        .metadata()
        .map_err(|error| StandaloneRouteIdentityError::io("inspect file", path, error))?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "identity artifact is not a uniquely linked regular file",
        });
    }
    if metadata.permissions().mode() & 0o777 != PRIVATE_FILE_MODE {
        return Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "identity artifact permissions are not private",
        });
    }
    Ok(())
}

fn path_exists_at(
    directory: &File,
    data_dir: &Path,
    name: &'static str,
    operation: &'static str,
) -> Result<bool, StandaloneRouteIdentityError> {
    match open_file_at(
        directory,
        name,
        libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        0,
    ) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(StandaloneRouteIdentityError::io(
            operation,
            &data_dir.join(name),
            error,
        )),
    }
}

fn require_identity_artifact_matches(
    directory: &File,
    data_dir: &Path,
    name: &'static str,
    expected_identity: Option<StandaloneRouteIdentity>,
    expected_file: Option<FileIdentity>,
    reason: &'static str,
) -> Result<(), StandaloneRouteIdentityError> {
    let exists = path_exists_at(directory, data_dir, name, "reinspect identity artifact")?;
    match (expected_identity, expected_file, exists) {
        (None, None, false) => Ok(()),
        (Some(expected_identity), Some(expected_file), true) => {
            let (actual_identity, actual_file) = read_identity_at(directory, data_dir, name)?;
            if actual_identity == expected_identity && actual_file == expected_file {
                Ok(())
            } else {
                Err(StandaloneRouteIdentityError::InvalidIdentity { reason })
            }
        }
        _ => Err(StandaloneRouteIdentityError::InvalidIdentity { reason }),
    }
}

fn require_marker_artifact_matches(
    directory: &File,
    data_dir: &Path,
    expected_file: Option<FileIdentity>,
) -> Result<(), StandaloneRouteIdentityError> {
    let exists = path_exists_at(
        directory,
        data_dir,
        INITIALIZING_FILE_NAME,
        "reinspect initialization marker",
    )?;
    match (expected_file, exists) {
        (None, false) => Ok(()),
        (Some(expected_file), true) => {
            let actual_file = verify_initializing_marker_at(directory, data_dir)?;
            if actual_file == expected_file {
                Ok(())
            } else {
                Err(StandaloneRouteIdentityError::InvalidIdentity {
                    reason: "initialization marker changed after acquisition",
                })
            }
        }
        _ => Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "initialization marker changed after acquisition",
        }),
    }
}

fn require_empty_except_lock(data_dir: &Path) -> Result<(), StandaloneRouteIdentityError> {
    let entries = fs::read_dir(data_dir)
        .map_err(|error| StandaloneRouteIdentityError::io("read directory", data_dir, error))?;
    for entry in entries {
        let entry = entry
            .map_err(|error| StandaloneRouteIdentityError::io("read directory", data_dir, error))?;
        if entry.file_name() != LOCK_FILE_NAME {
            return Err(StandaloneRouteIdentityError::StateWithoutIdentity {
                path: data_dir.to_path_buf(),
            });
        }
    }
    Ok(())
}

fn require_initializing_scaffolding(data_dir: &Path) -> Result<(), StandaloneRouteIdentityError> {
    let mut remaining = MAX_INITIALIZING_SCAFFOLDING_ENTRIES;
    require_initializing_scaffolding_inner(data_dir, data_dir, true, 0, &mut remaining)
}

fn require_initializing_scaffolding_inner(
    root: &Path,
    directory: &Path,
    is_root: bool,
    depth: usize,
    remaining: &mut usize,
) -> Result<(), StandaloneRouteIdentityError> {
    let entries = fs::read_dir(directory)
        .map_err(|error| StandaloneRouteIdentityError::io("read directory", directory, error))?;
    for entry in entries {
        if *remaining == 0 {
            return Err(StandaloneRouteIdentityError::StateWithoutIdentity {
                path: root.to_path_buf(),
            });
        }
        *remaining -= 1;
        let entry = entry.map_err(|error| {
            StandaloneRouteIdentityError::io("read directory", directory, error)
        })?;
        let name = entry.file_name();
        if is_root
            && (name == LOCK_FILE_NAME
                || name == INITIALIZING_FILE_NAME
                || name == IDENTITY_NEXT_FILE_NAME)
        {
            continue;
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            StandaloneRouteIdentityError::io("inspect initialization scaffolding", &path, error)
        })?;
        if !metadata.is_dir()
            || metadata.permissions().mode() & 0o777 != PRIVATE_DIRECTORY_MODE
            || depth == MAX_INITIALIZING_SCAFFOLDING_DEPTH
        {
            return Err(StandaloneRouteIdentityError::StateWithoutIdentity {
                path: root.to_path_buf(),
            });
        }
        require_initializing_scaffolding_inner(root, &path, false, depth + 1, remaining)?;
    }
    Ok(())
}

fn write_new_file_at(
    directory: &File,
    data_dir: &Path,
    name: &'static str,
    bytes: &[u8],
    operation: &'static str,
) -> Result<(), StandaloneRouteIdentityError> {
    let path = data_dir.join(name);
    let mut file = open_file_at(
        directory,
        name,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        PRIVATE_FILE_MODE,
    )
    .map_err(|error| StandaloneRouteIdentityError::io(operation, &path, error))?;
    require_private_regular_file(&path, &file)?;
    file.write_all(bytes)
        .map_err(|error| StandaloneRouteIdentityError::io("write file", &path, error))?;
    file.sync_all()
        .map_err(|error| StandaloneRouteIdentityError::io("sync file", &path, error))
}

fn read_bounded_file_at(
    directory: &File,
    data_dir: &Path,
    name: &'static str,
    expected_len: usize,
) -> Result<(Vec<u8>, FileIdentity), StandaloneRouteIdentityError> {
    let path = data_dir.join(name);
    let mut file = open_file_at(
        directory,
        name,
        libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        0,
    )
    .map_err(|error| StandaloneRouteIdentityError::io("open file", &path, error))?;
    require_private_regular_file(&path, &file)?;
    let metadata = file
        .metadata()
        .map_err(|error| StandaloneRouteIdentityError::io("inspect file", &path, error))?;
    let file_identity = FileIdentity::from_metadata(&metadata);
    let actual_len = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    if actual_len != expected_len {
        return Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "identity artifact has an invalid length",
        });
    }
    let mut bytes = Vec::with_capacity(expected_len);
    file.read_to_end(&mut bytes)
        .map_err(|error| StandaloneRouteIdentityError::io("read file", &path, error))?;
    Ok((bytes, file_identity))
}

fn verify_initializing_marker_at(
    directory: &File,
    data_dir: &Path,
) -> Result<FileIdentity, StandaloneRouteIdentityError> {
    let expected = [INITIALIZING_MAGIC.as_slice(), &FORMAT_VERSION.to_be_bytes()].concat();
    let (actual, file_identity) =
        read_bounded_file_at(directory, data_dir, INITIALIZING_FILE_NAME, expected.len())?;
    if actual != expected {
        return Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "initialization marker is invalid",
        });
    }
    Ok(file_identity)
}

fn encode_identity(identity: StandaloneRouteIdentity) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(IDENTITY_LEN);
    bytes.extend_from_slice(IDENTITY_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&identity.0);
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    hasher.update(&bytes);
    bytes.extend_from_slice(hasher.finalize().bytes());
    bytes
}

fn read_identity_at(
    directory: &File,
    data_dir: &Path,
    name: &'static str,
) -> Result<(StandaloneRouteIdentity, FileIdentity), StandaloneRouteIdentityError> {
    let (bytes, file_identity) = read_bounded_file_at(directory, data_dir, name, IDENTITY_LEN)?;
    if &bytes[..IDENTITY_MAGIC.len()] != IDENTITY_MAGIC {
        return Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "identity magic is invalid",
        });
    }
    let version_offset = IDENTITY_MAGIC.len();
    let version = u16::from_be_bytes([bytes[version_offset], bytes[version_offset + 1]]);
    if version != FORMAT_VERSION {
        return Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "identity version is unsupported",
        });
    }
    let digest_offset = version_offset + 2;
    let checksum_offset = digest_offset + DIGEST_LEN;
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    hasher.update(&bytes[..checksum_offset]);
    if hasher.finalize().bytes() != &bytes[checksum_offset..] {
        return Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "identity checksum is invalid",
        });
    }
    let digest = bytes[digest_offset..checksum_offset]
        .try_into()
        .expect("validated standalone route digest length");
    Ok((StandaloneRouteIdentity(digest), file_identity))
}

fn publish_identity_at(
    directory: &File,
    data_dir: &Path,
    identity: StandaloneRouteIdentity,
) -> Result<(), StandaloneRouteIdentityError> {
    if path_exists_at(
        directory,
        data_dir,
        IDENTITY_NEXT_FILE_NAME,
        "inspect prepared identity",
    )? {
        return Err(StandaloneRouteIdentityError::InvalidIdentity {
            reason: "unexpected prepared identity exists",
        });
    }
    write_new_file_at(
        directory,
        data_dir,
        IDENTITY_NEXT_FILE_NAME,
        &encode_identity(identity),
        "create prepared identity",
    )?;
    sync_directory(directory, data_dir, "sync prepared identity")?;
    publish_prepared_identity_at(directory, data_dir)
}

fn publish_prepared_identity_at(
    directory: &File,
    data_dir: &Path,
) -> Result<(), StandaloneRouteIdentityError> {
    rename_file_at(
        directory,
        data_dir,
        IDENTITY_NEXT_FILE_NAME,
        IDENTITY_FILE_NAME,
        "publish identity",
    )?;
    sync_directory(directory, data_dir, "sync published identity")
}

fn rename_file_at(
    directory: &File,
    data_dir: &Path,
    source: &'static str,
    destination: &'static str,
    operation: &'static str,
) -> Result<(), StandaloneRouteIdentityError> {
    let source_name =
        std::ffi::CString::new(source).expect("static identity artifact name has no NUL");
    let destination_name =
        std::ffi::CString::new(destination).expect("static identity artifact name has no NUL");
    // SAFETY: both names and the directory descriptor remain valid for the
    // call, and both operations are constrained to the held directory.
    let result = unsafe {
        libc::renameat(
            directory.as_raw_fd(),
            source_name.as_ptr(),
            directory.as_raw_fd(),
            destination_name.as_ptr(),
        )
    };
    if result != 0 {
        return Err(StandaloneRouteIdentityError::io(
            operation,
            &data_dir.join(destination),
            io::Error::last_os_error(),
        ));
    }
    Ok(())
}

fn unlink_file_at(
    directory: &File,
    data_dir: &Path,
    name: &'static str,
    operation: &'static str,
) -> Result<(), StandaloneRouteIdentityError> {
    let relative_name =
        std::ffi::CString::new(name).expect("static identity artifact name has no NUL");
    // SAFETY: the name and held directory descriptor remain valid for the
    // call. Flags zero removes a non-directory entry only.
    let result = unsafe { libc::unlinkat(directory.as_raw_fd(), relative_name.as_ptr(), 0) };
    if result != 0 {
        return Err(StandaloneRouteIdentityError::io(
            operation,
            &data_dir.join(name),
            io::Error::last_os_error(),
        ));
    }
    Ok(())
}

fn sync_directory(
    directory: &File,
    path: &Path,
    operation: &'static str,
) -> Result<(), StandaloneRouteIdentityError> {
    directory
        .sync_all()
        .map_err(|error| StandaloneRouteIdentityError::io(operation, path, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(byte: u8) -> StandaloneRouteIdentity {
        StandaloneRouteIdentity([byte; DIGEST_LEN])
    }

    #[test]
    fn fresh_binding_is_exact_and_reopens() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("standalone");
        let lock = StandaloneRouteIdentityPreparation::acquire(&data_dir)
            .unwrap()
            .bind(identity(7))
            .unwrap();
        drop(lock);

        StandaloneRouteIdentityPreparation::acquire(&data_dir)
            .unwrap()
            .bind(identity(7))
            .unwrap();
        let bytes = fs::read(data_dir.join(IDENTITY_FILE_NAME)).unwrap();
        assert_eq!(
            bytes,
            vec![
                65, 82, 71, 83, 82, 84, 73, 68, 0, 1, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 5, 181, 150, 88, 199, 142, 179,
                251, 164, 203, 6, 107, 15, 222, 83, 89, 129, 213, 92, 193, 227, 78, 15, 219, 234,
                128, 56, 14, 42, 67, 194, 57,
            ]
        );
    }

    #[test]
    fn topology_change_fails_closed() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("standalone");
        drop(
            StandaloneRouteIdentityPreparation::acquire(&data_dir)
                .unwrap()
                .bind(identity(7))
                .unwrap(),
        );

        assert!(matches!(
            StandaloneRouteIdentityPreparation::acquire(&data_dir)
                .unwrap()
                .bind(identity(8)),
            Err(StandaloneRouteIdentityError::IdentityMismatch)
        ));
    }

    #[test]
    fn missing_identity_beside_state_fails_closed() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("standalone");
        crate::data_dir::prepare_private_data_dir(&data_dir).unwrap();
        fs::write(data_dir.join("existing-state"), b"state").unwrap();

        assert!(matches!(
            StandaloneRouteIdentityPreparation::acquire(&data_dir),
            Err(StandaloneRouteIdentityError::StateWithoutIdentity { .. })
        ));
    }

    #[test]
    fn initialization_marker_rejects_arbitrary_state_without_identity() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("standalone");
        let preparation = StandaloneRouteIdentityPreparation::acquire(&data_dir).unwrap();
        fs::write(data_dir.join("new-storage-state"), b"state").unwrap();
        drop(preparation);

        assert!(matches!(
            StandaloneRouteIdentityPreparation::acquire(&data_dir),
            Err(StandaloneRouteIdentityError::StateWithoutIdentity { .. })
        ));
    }

    #[test]
    fn bind_revalidates_unpublished_marker_scaffolding_and_pending_identity() {
        let temp = test_util::tempdir();

        let state_dir = temp.path().join("state");
        let preparation = StandaloneRouteIdentityPreparation::acquire(&state_dir).unwrap();
        fs::write(state_dir.join("late-storage-state"), b"state").unwrap();
        assert!(matches!(
            preparation.bind(identity(4)),
            Err(StandaloneRouteIdentityError::StateWithoutIdentity { .. })
        ));

        let added_pending_dir = temp.path().join("added-pending");
        let preparation = StandaloneRouteIdentityPreparation::acquire(&added_pending_dir).unwrap();
        fs::write(
            added_pending_dir.join(IDENTITY_NEXT_FILE_NAME),
            encode_identity(identity(4)),
        )
        .unwrap();
        fs::set_permissions(
            added_pending_dir.join(IDENTITY_NEXT_FILE_NAME),
            fs::Permissions::from_mode(PRIVATE_FILE_MODE),
        )
        .unwrap();
        assert!(matches!(
            preparation.bind(identity(4)),
            Err(StandaloneRouteIdentityError::InvalidIdentity {
                reason: "prepared identity changed after acquisition"
            })
        ));

        let replaced_pending_dir = temp.path().join("replaced-pending");
        let preparation =
            StandaloneRouteIdentityPreparation::acquire(&replaced_pending_dir).unwrap();
        write_new_file_at(
            &preparation.directory,
            &replaced_pending_dir,
            IDENTITY_NEXT_FILE_NAME,
            &encode_identity(identity(5)),
            "test prepared identity",
        )
        .unwrap();
        drop(preparation);
        let preparation =
            StandaloneRouteIdentityPreparation::acquire(&replaced_pending_dir).unwrap();
        fs::write(
            replaced_pending_dir.join(IDENTITY_NEXT_FILE_NAME),
            encode_identity(identity(6)),
        )
        .unwrap();
        assert!(matches!(
            preparation.bind(identity(5)),
            Err(StandaloneRouteIdentityError::InvalidIdentity {
                reason: "prepared identity changed after acquisition"
            })
        ));

        let marker_dir = temp.path().join("marker");
        let preparation = StandaloneRouteIdentityPreparation::acquire(&marker_dir).unwrap();
        fs::remove_file(marker_dir.join(INITIALIZING_FILE_NAME)).unwrap();
        fs::write(
            marker_dir.join(INITIALIZING_FILE_NAME),
            [INITIALIZING_MAGIC.as_slice(), &FORMAT_VERSION.to_be_bytes()].concat(),
        )
        .unwrap();
        fs::set_permissions(
            marker_dir.join(INITIALIZING_FILE_NAME),
            fs::Permissions::from_mode(PRIVATE_FILE_MODE),
        )
        .unwrap();
        assert!(matches!(
            preparation.bind(identity(4)),
            Err(StandaloneRouteIdentityError::InvalidIdentity {
                reason: "initialization marker changed after acquisition"
            })
        ));
    }

    #[test]
    fn fifo_identity_artifacts_fail_closed_without_blocking() {
        use std::os::unix::ffi::OsStrExt;

        for name in [
            IDENTITY_FILE_NAME,
            IDENTITY_NEXT_FILE_NAME,
            INITIALIZING_FILE_NAME,
        ] {
            let temp = test_util::tempdir();
            let data_dir = temp.path().join("standalone");
            drop(StandaloneRouteIdentityPreparation::acquire(&data_dir).unwrap());
            let path = data_dir.join(name);
            if name == INITIALIZING_FILE_NAME {
                fs::remove_file(&path).unwrap();
            }
            let path_bytes = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
            // SAFETY: `path_bytes` is a valid NUL-terminated path for the call.
            assert_eq!(
                unsafe { libc::mkfifo(path_bytes.as_ptr(), PRIVATE_FILE_MODE) },
                0
            );

            assert!(matches!(
                StandaloneRouteIdentityPreparation::acquire(&data_dir),
                Err(StandaloneRouteIdentityError::InvalidIdentity { .. })
            ));
        }
    }

    #[test]
    fn interrupted_initialization_accepts_only_empty_private_scaffolding() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("standalone");
        let preparation = StandaloneRouteIdentityPreparation::acquire(&data_dir).unwrap();
        crate::data_dir::prepare_private_data_dir(&data_dir.join("nodes/node-0001")).unwrap();
        drop(preparation);

        StandaloneRouteIdentityPreparation::acquire(&data_dir)
            .unwrap()
            .bind(identity(4))
            .unwrap();
        assert!(matches!(
            StandaloneRouteIdentityPreparation::acquire(&data_dir)
                .unwrap()
                .bind(identity(5)),
            Err(StandaloneRouteIdentityError::IdentityMismatch)
        ));
    }

    #[test]
    fn interrupted_prepared_identity_publication_is_validated_and_completed() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("standalone");
        let preparation = StandaloneRouteIdentityPreparation::acquire(&data_dir).unwrap();
        write_new_file_at(
            &preparation.directory,
            &data_dir,
            IDENTITY_NEXT_FILE_NAME,
            &encode_identity(identity(6)),
            "test prepared identity",
        )
        .unwrap();
        sync_directory(
            &preparation.directory,
            &data_dir,
            "test sync prepared identity",
        )
        .unwrap();
        drop(preparation);

        StandaloneRouteIdentityPreparation::acquire(&data_dir)
            .unwrap()
            .bind(identity(6))
            .unwrap();
        assert!(data_dir.join(IDENTITY_FILE_NAME).exists());
        assert!(!data_dir.join(IDENTITY_NEXT_FILE_NAME).exists());
        assert!(!data_dir.join(INITIALIZING_FILE_NAME).exists());
    }

    #[test]
    fn replaced_lock_entry_prevents_binding() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("standalone");
        let first = StandaloneRouteIdentityPreparation::acquire(&data_dir).unwrap();
        fs::remove_file(data_dir.join(LOCK_FILE_NAME)).unwrap();
        let replacement_entry = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(PRIVATE_FILE_MODE)
            .open(data_dir.join(LOCK_FILE_NAME))
            .unwrap();

        assert!(matches!(
            StandaloneRouteIdentityPreparation::acquire(&data_dir),
            Err(StandaloneRouteIdentityError::LockContended { .. })
        ));

        assert!(matches!(
            first.bind(identity(7)),
            Err(StandaloneRouteIdentityError::InvalidIdentity {
                reason: "identity lock entry changed after acquisition"
            })
        ));
        assert!(!data_dir.join(IDENTITY_FILE_NAME).exists());
        drop(replacement_entry);
        StandaloneRouteIdentityPreparation::acquire(&data_dir)
            .unwrap()
            .bind(identity(7))
            .unwrap();
    }

    #[test]
    fn replaced_directory_entry_prevents_binding_into_replacement() {
        let temp = test_util::tempdir();
        let data_dir = temp.path().join("standalone");
        let displaced_dir = temp.path().join("displaced");
        let first = StandaloneRouteIdentityPreparation::acquire(&data_dir).unwrap();
        fs::rename(&data_dir, &displaced_dir).unwrap();
        let replacement = StandaloneRouteIdentityPreparation::acquire(&data_dir).unwrap();

        assert!(matches!(
            first.bind(identity(8)),
            Err(StandaloneRouteIdentityError::InvalidIdentity {
                reason: "identity directory entry changed after acquisition"
            })
        ));
        assert!(!data_dir.join(IDENTITY_FILE_NAME).exists());
        assert!(!displaced_dir.join(IDENTITY_FILE_NAME).exists());
        replacement.bind(identity(8)).unwrap();
    }

    #[test]
    fn malformed_identity_and_marker_fail_closed() {
        let temp = test_util::tempdir();
        let identity_dir = temp.path().join("identity");
        drop(
            StandaloneRouteIdentityPreparation::acquire(&identity_dir)
                .unwrap()
                .bind(identity(3))
                .unwrap(),
        );
        let identity_path = identity_dir.join(IDENTITY_FILE_NAME);
        let mut bytes = fs::read(&identity_path).unwrap();
        bytes[10] ^= 0x80;
        fs::write(&identity_path, bytes).unwrap();
        assert!(matches!(
            StandaloneRouteIdentityPreparation::acquire(&identity_dir),
            Err(StandaloneRouteIdentityError::InvalidIdentity { .. })
        ));

        let marker_dir = temp.path().join("marker");
        let preparation = StandaloneRouteIdentityPreparation::acquire(&marker_dir).unwrap();
        drop(preparation);
        fs::write(marker_dir.join(INITIALIZING_FILE_NAME), b"bad").unwrap();
        assert!(matches!(
            StandaloneRouteIdentityPreparation::acquire(&marker_dir),
            Err(StandaloneRouteIdentityError::InvalidIdentity { .. })
        ));
    }

    #[test]
    fn unsupported_identity_versions_fail_closed_after_valid_checksum() {
        for version in [0_u16, FORMAT_VERSION + 1] {
            let temp = test_util::tempdir();
            let data_dir = temp.path().join(format!("version-{version}"));
            drop(
                StandaloneRouteIdentityPreparation::acquire(&data_dir)
                    .unwrap()
                    .bind(identity(2))
                    .unwrap(),
            );
            let identity_path = data_dir.join(IDENTITY_FILE_NAME);
            let mut bytes = fs::read(&identity_path).unwrap();
            bytes[IDENTITY_MAGIC.len()..IDENTITY_MAGIC.len() + 2]
                .copy_from_slice(&version.to_be_bytes());
            let checksum_offset = IDENTITY_LEN - DIGEST_LEN;
            let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
            hasher.update(&bytes[..checksum_offset]);
            bytes[checksum_offset..].copy_from_slice(hasher.finalize().bytes());
            fs::write(&identity_path, bytes).unwrap();

            assert!(matches!(
                StandaloneRouteIdentityPreparation::acquire(&data_dir),
                Err(StandaloneRouteIdentityError::InvalidIdentity {
                    reason: "identity version is unsupported"
                })
            ));
        }
    }

    #[test]
    fn runtime_lock_and_symlink_directory_fail_closed() {
        use std::os::unix::fs::symlink;

        let temp = test_util::tempdir();
        let data_dir = temp.path().join("locked");
        let preparation = StandaloneRouteIdentityPreparation::acquire(&data_dir).unwrap();
        assert!(matches!(
            StandaloneRouteIdentityPreparation::acquire(&data_dir),
            Err(StandaloneRouteIdentityError::LockContended { .. })
        ));
        drop(preparation);

        let target = temp.path().join("target");
        crate::data_dir::prepare_private_data_dir(&target).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).unwrap();
        let linked = temp.path().join("linked");
        symlink(&target, &linked).unwrap();
        assert!(matches!(
            StandaloneRouteIdentityPreparation::acquire(&linked),
            Err(StandaloneRouteIdentityError::InvalidIdentity {
                reason: "identity directory must not be a symbolic link"
            })
        ));
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o755,
            "rejecting a symlink must not modify its target"
        );
    }

    #[test]
    fn combined_identity_binds_both_ordered_components_and_is_opaque() {
        let combined = identity(1).combined_with(identity(2));
        assert_ne!(combined, identity(1).combined_with(identity(3)));
        assert_ne!(combined, identity(2).combined_with(identity(1)));
        assert_eq!(
            format!("{combined:?}"),
            "StandaloneRouteIdentity(\"<opaque>\")"
        );
    }
}
