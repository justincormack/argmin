//! Shared test utilities for argmin.
//!
//! Provides a temp directory helper that prevents `/tmp` from filling up
//! with leftover test data when test processes are interrupted.
//!
//! # Usage
//!
//! Replace `tempfile::tempdir()` with `test_util::tempdir()`:
//!
//! ```no_run
//! let tmp = test_util::tempdir();
//! // tmp.path() works like tempfile::TempDir
//! // cleaned up on drop, and stale dirs from dead processes
//! // are cleaned up on next test run
//! ```
//!
//! Set `ARGMIN_KEEP_TEST_DIRS=1` to preserve directories for debugging.

use std::fs::{File, OpenOptions};
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::{
    DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _,
};
use std::path::{Path, PathBuf};
use std::sync::{Once, OnceLock};

/// Base directory under the system temp dir for all test data.
const BASE_DIR_NAME: &str = "argmin-tests";
const INIT_LOCK_FILE_NAME: &str = ".init.lock";
const SESSION_LOCK_FILE_NAME: &str = ".session.lock";

static INIT: Once = Once::new();
static SESSION_LOCK: OnceLock<File> = OnceLock::new();

#[cfg(unix)]
fn effective_uid() -> libc::uid_t {
    // SAFETY: geteuid has no preconditions and does not mutate state.
    unsafe { libc::geteuid() }
}

/// Return the base directory for all test sessions.
fn base_dir() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        // macOS normally gives each process a long /var/folders/... TMPDIR,
        // while sockaddr_un::sun_path holds only 104 bytes. Keep test paths
        // short enough for the Unix transports and retain per-user isolation.
        PathBuf::from("/tmp").join(format!("{BASE_DIR_NAME}-{}", effective_uid()))
    }

    #[cfg(not(target_os = "macos"))]
    std::env::temp_dir().join(BASE_DIR_NAME)
}

/// Return the session directory for the current process.
fn session_dir() -> PathBuf {
    base_dir().join(format!("{}", std::process::id()))
}

/// Clean up session directories belonging to dead processes.
///
/// Scans the platform test-temp root for subdirectories named after PIDs.
/// A directory is removed only when its lock can be inspected and acquired, or
/// when it has no lock and its PID is no longer running. Inspection failures
/// leave the directory untouched.
/// Skips cleanup entirely if `ARGMIN_KEEP_TEST_DIRS=1`.
fn cleanup_stale_sessions() {
    if std::env::var("ARGMIN_KEEP_TEST_DIRS").as_deref() == Ok("1") {
        return;
    }

    let base = base_dir();
    cleanup_stale_sessions_in(&base, std::process::id());
}

fn cleanup_stale_sessions_in(base: &Path, current_pid: u32) {
    let entries = match std::fs::read_dir(base) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        // The root is private, so newly-created entries can only belong to us.
        // Reject entries that do not satisfy the current directory contract
        // before constructing paths beneath them or recursively removing them.
        #[cfg(unix)]
        if ensure_private_directory(&entry.path()).is_err() {
            continue;
        }

        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Only look at directories named as PIDs.
        let pid: u32 = match name_str.parse() {
            Ok(p) => p,
            Err(_) => continue,
        };

        // Skip our own PID.
        if pid == current_pid {
            continue;
        }

        let lock_path = entry.path().join(SESSION_LOCK_FILE_NAME);
        if lock_path.exists() {
            if let Ok(false) = session_lock_is_held(&lock_path) {
                let _ = std::fs::remove_dir_all(entry.path());
            }
            continue;
        }

        // Check if the process is still alive.
        if process_alive(pid) {
            continue;
        }

        // Dead process — clean up its session directory.
        let _ = std::fs::remove_dir_all(entry.path());
    }
}

#[cfg(unix)]
fn validate_private_directory(path: &Path) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("test directory is not a real directory: {}", path.display()),
        ));
    }
    if metadata.uid() != effective_uid() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "test directory is not owned by the effective UID: {}",
                path.display()
            ),
        ));
    }
    if metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("test directory is not mode 0700: {}", path.display()),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_private_directory(path: &Path) -> std::io::Result<()> {
    match std::fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(err) => return Err(err),
    }

    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("test directory is not a real directory: {}", path.display()),
        ));
    }
    if metadata.uid() != effective_uid() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "test directory is not owned by the effective UID: {}",
                path.display()
            ),
        ));
    }

    validate_private_directory(path)
}

#[cfg(not(unix))]
fn ensure_private_directory(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)
}

fn open_lock_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);

    let file = options.open(path)?;
    #[cfg(unix)]
    {
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("test lock is not a regular file: {}", path.display()),
            ));
        }
        if metadata.uid() != effective_uid() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "test lock is not owned by the effective UID: {}",
                    path.display()
                ),
            ));
        }
        if metadata.permissions().mode() & 0o777 != 0o600 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("test lock is not mode 0600: {}", path.display()),
            ));
        }
    }
    Ok(file)
}

#[cfg(unix)]
fn flock_exclusive(file: &File, nonblocking: bool) -> std::io::Result<()> {
    let mut operation = libc::LOCK_EX;
    if nonblocking {
        operation |= libc::LOCK_NB;
    }
    // SAFETY: `flock` operates on a valid open file descriptor owned by `file`.
    let ret = unsafe { libc::flock(file.as_raw_fd(), operation) };
    if ret == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn flock_exclusive(_file: &File, _nonblocking: bool) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn session_lock_is_held(path: &Path) -> std::io::Result<bool> {
    let file = open_lock_file(path)?;
    match flock_exclusive(&file, true) {
        Ok(()) => Ok(false),
        Err(err) if err.raw_os_error() == Some(libc::EWOULDBLOCK) => Ok(true),
        Err(err) => Err(err),
    }
}

#[cfg(not(unix))]
fn session_lock_is_held(_path: &Path) -> std::io::Result<bool> {
    Ok(true)
}

/// Check if a process with the given PID is still running.
#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    // kill(pid, 0) returns 0 if the process exists and we have
    // permission to signal it, or EPERM if it exists but we can't
    // signal it. Returns ESRCH if it doesn't exist.
    // SAFETY: signal 0 never actually sends a signal.
    let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if ret == 0 {
        return true;
    }
    // EPERM means the process exists but we can't signal it.
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Non-Unix fallback: assume the process is alive (skip cleanup).
#[cfg(not(unix))]
fn process_alive(_pid: u32) -> bool {
    true
}

/// Ensure the session directory exists and stale sessions are cleaned up.
///
/// Called once per process via `Once`.
fn init() {
    let base = base_dir();
    ensure_private_directory(&base).expect("failed to secure test temp root");
    let init_lock = open_lock_file(&base.join(INIT_LOCK_FILE_NAME))
        .expect("failed to open test init lock file");
    flock_exclusive(&init_lock, false).expect("failed to lock test init lock file");

    let session = session_dir();
    ensure_private_directory(&session).expect("failed to secure test session directory");
    let session_lock = open_lock_file(&session.join(SESSION_LOCK_FILE_NAME))
        .expect("failed to open test session lock file");
    flock_exclusive(&session_lock, false).expect("failed to lock test session lock file");
    SESSION_LOCK
        .set(session_lock)
        .expect("test session lock already initialized");
    cleanup_stale_sessions();
}

/// A temporary directory that is cleaned up on drop.
///
/// Wraps `tempfile::TempDir` but places directories under a per-process
/// session directory so stale dirs from dead processes can be identified
/// and cleaned up.
pub struct TempDir {
    inner: Option<tempfile::TempDir>,
}

impl TempDir {
    /// Return the path to the temporary directory.
    pub fn path(&self) -> &Path {
        self.inner.as_ref().unwrap().path()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if std::env::var("ARGMIN_KEEP_TEST_DIRS").as_deref() == Ok("1") {
            // Leak the inner TempDir so it's not cleaned up.
            if let Some(inner) = self.inner.take() {
                let path = inner.keep();
                eprintln!("ARGMIN_KEEP_TEST_DIRS: keeping {}", path.display());
            }
        }
        // Otherwise inner drops normally, removing the directory.
    }
}

/// Create a temporary directory for test use.
///
/// The directory is placed under the platform test-temp root and a PID so that:
/// - Concurrent test runs don't interfere (each has its own PID subdir)
/// - Stale directories from dead processes are cleaned up on next run
/// - `ARGMIN_KEEP_TEST_DIRS=1` preserves directories for debugging
pub fn tempdir() -> TempDir {
    INIT.call_once(init);

    let inner = tempfile::Builder::new()
        .prefix("t-")
        .tempdir_in(session_dir())
        .expect("failed to create test temp directory");

    TempDir { inner: Some(inner) }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    #[test]
    fn private_directory_creation_rejects_non_private_permissions() {
        use std::os::unix::fs::PermissionsExt as _;

        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("non-private-root");
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let error = super::ensure_private_directory(&path).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            std::fs::symlink_metadata(path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755,
        );
    }

    #[cfg(unix)]
    #[test]
    fn private_directory_creation_rejects_symlink() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("target");
        let path = parent.path().join("root-link");
        std::fs::create_dir(&target).unwrap();
        symlink(target, &path).unwrap();

        assert!(super::ensure_private_directory(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn lock_file_open_rejects_non_private_permissions_and_symlink() {
        use std::os::unix::fs::{symlink, PermissionsExt as _};

        let parent = tempfile::tempdir().unwrap();
        let lock_path = parent.path().join("lock");
        std::fs::write(&lock_path, []).unwrap();
        std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let error = super::open_lock_file(&lock_path).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            std::fs::symlink_metadata(&lock_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o644,
        );

        let link_path = parent.path().join("lock-link");
        symlink(&lock_path, &link_path).unwrap();
        assert!(super::open_lock_file(&link_path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn stale_session_cleanup_preserves_live_session_with_wrong_mode_lock() {
        use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};

        let parent = tempfile::tempdir().unwrap();
        let base = parent.path().join("argmin-tests");
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&base)
            .unwrap();

        let live_pid = std::process::id();
        let session = base.join(live_pid.to_string());
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&session)
            .unwrap();
        let marker = session.join("must-survive");
        std::fs::write(&marker, []).unwrap();

        let lock_path = session.join(super::SESSION_LOCK_FILE_NAME);
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&lock_path)
            .unwrap();
        std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        super::flock_exclusive(&lock, false).unwrap();

        let different_current_pid = live_pid.checked_add(1).unwrap_or(live_pid - 1);
        super::cleanup_stale_sessions_in(&base, different_current_pid);

        assert!(session.is_dir());
        assert!(marker.is_file());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn tempdir_leaves_room_for_descriptive_unix_socket_paths() {
        use std::os::unix::ffi::OsStrExt as _;

        let tmp = super::tempdir();
        let socket_path = tmp
            .path()
            .join("sockets")
            .join("historical-storage-node-12345.sock");

        assert!(
            socket_path.as_os_str().as_bytes().len() < 104,
            "representative Unix socket path exceeds Darwin sun_path: {}",
            socket_path.display()
        );
    }
}
