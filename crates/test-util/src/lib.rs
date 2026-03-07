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

use std::path::{Path, PathBuf};
use std::sync::Once;

/// Base directory under the system temp dir for all test data.
const BASE_DIR_NAME: &str = "argmin-tests";

static INIT: Once = Once::new();

/// Return the base directory (`$TMPDIR/argmin-tests/`).
fn base_dir() -> PathBuf {
    std::env::temp_dir().join(BASE_DIR_NAME)
}

/// Return the session directory for the current process.
fn session_dir() -> PathBuf {
    base_dir().join(format!("{}", std::process::id()))
}

/// Clean up session directories belonging to dead processes.
///
/// Scans `$TMPDIR/argmin-tests/` for subdirectories named after PIDs.
/// If the PID is no longer running, removes that directory tree.
/// Skips cleanup entirely if `ARGMIN_KEEP_TEST_DIRS=1`.
fn cleanup_stale_sessions() {
    if std::env::var("ARGMIN_KEEP_TEST_DIRS").as_deref() == Ok("1") {
        return;
    }

    let base = base_dir();
    let entries = match std::fs::read_dir(&base) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Only look at directories named as PIDs.
        let pid: u32 = match name_str.parse() {
            Ok(p) => p,
            Err(_) => continue,
        };

        // Skip our own PID.
        if pid == std::process::id() {
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
    let session = session_dir();
    let _ = std::fs::create_dir_all(&session);
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
/// The directory is placed under `$TMPDIR/argmin-tests/<pid>/` so that:
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
