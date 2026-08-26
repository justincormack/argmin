// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::ffi::OsString;
use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use storage::test_support::{
    observe_single_authority_durable_state, prepare_unsupported_authority_clock_checkpoint_restart,
    prepare_unsupported_single_authority_identity_restart,
    prepare_unsupported_single_authority_initialization_restart,
    prepare_unsupported_single_authority_journal_file_restart,
    prepare_unsupported_single_authority_journal_record_restart,
    TestUnsupportedAuthorityClockCheckpointVersion, TestUnsupportedSingleAuthorityIdentityVersion,
    TestUnsupportedSingleAuthorityInitializationVersion,
    TestUnsupportedSingleAuthorityJournalFileVersion,
    TestUnsupportedSingleAuthorityJournalRecordVersion,
};

struct TestDir {
    path: PathBuf,
    _temp: test_util::TempDir,
}

struct ChildGuard {
    child: Option<Child>,
}

impl TestDir {
    fn new() -> Self {
        let temp = test_util::tempdir();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700))
            .expect("test directory should be private");
        Self {
            path: temp.path().to_path_buf(),
            _temp: temp,
        }
    }
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child
            .as_mut()
            .expect("standalone process should not be inspected after stop")
            .try_wait()
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

fn argmin_s3_bin() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_argmin-s3")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let current = std::env::current_exe().expect("current test executable path");
            current
                .parent()
                .and_then(Path::parent)
                .expect("integration test should run from target/*/deps")
                .join("argmin-s3")
        })
}

fn spawn_standalone_control_plane(
    bin: &Path,
    test_dir: &Path,
    state_path: &Path,
    control_socket: &Path,
    run: &str,
) -> ChildGuard {
    let stdout = File::create(test_dir.join(format!("{run}.stdout.log")))
        .expect("stdout log should be created");
    let stderr = File::create(test_dir.join(format!("{run}.stderr.log")))
        .expect("stderr log should be created");
    let storage_node_sockets = format!(
        "0={},1={}",
        test_dir.join("storage-node-0.sock").display(),
        test_dir.join("storage-node-1.sock").display()
    );
    let child = Command::new(bin)
        .env_clear()
        .env("ARGMIN_ACCOUNT_ID", "123456789012")
        .env("ARGMIN_ACCESS_KEY_ID", "standalone-process-test-access")
        .env("ARGMIN_SECRET_ACCESS_KEY", "standalone-process-test-secret")
        .env("ARGMIN_TEST_ENV_SHAPED_CONFIG", "1")
        .env("ARGMIN_PROCESS_ROLE", "control-plane")
        .env("ARGMIN_DATA_DIR", test_dir.join("data"))
        .env("ARGMIN_PG_COUNT", "1")
        .env("ARGMIN_STORAGE_PG_IDS", "0")
        .env("ARGMIN_EC_K", "1")
        .env("ARGMIN_EC_M", "1")
        .env("ARGMIN_LOCAL_NODE_COUNT", "2")
        .env("ARGMIN_STORAGE_NODE_SOCKETS", storage_node_sockets)
        .env("ARGMIN_CONTROL_PLANE_STATE_PATH", state_path)
        .env("ARGMIN_CONTROL_PLANE_SOCKET_PATH", control_socket)
        .env(
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            "standalone-process-test-cluster",
        )
        .env(
            "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS",
            "0=storage-node-0:1:storage-secret-0,1=storage-node-1:1:storage-secret-1",
        )
        .env(
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
            "runtime-map-ready=frontend-runtime-map-ready:1:frontend-secret",
        )
        .env(
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
            "server-admin=admin-server-admin:1:admin-secret",
        )
        .env(
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID",
            "server-admin",
        )
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("standalone control-plane process should start");
    ChildGuard::new(child)
}

fn process_logs(test_dir: &Path, run: &str) -> String {
    let stdout = fs::read_to_string(test_dir.join(format!("{run}.stdout.log")))
        .unwrap_or_else(|error| format!("<failed to read stdout: {error}>"));
    let stderr = fs::read_to_string(test_dir.join(format!("{run}.stderr.log")))
        .unwrap_or_else(|error| format!("<failed to read stderr: {error}>"));
    format!("stdout:\n{stdout}\nstderr:\n{stderr}")
}

fn wait_for_listener(child: &mut ChildGuard, socket: &Path, test_dir: &Path, run: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match child.try_wait() {
            Ok(None) => {}
            Ok(Some(status)) => panic!(
                "standalone control-plane exited before binding with {status}\n{}",
                process_logs(test_dir, run)
            ),
            Err(error) => panic!("standalone control-plane status should be readable: {error}"),
        }
        if UnixStream::connect(socket).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "standalone control-plane did not bind {}\n{}",
            socket.display(),
            process_logs(test_dir, run)
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_failure(child: &mut ChildGuard, test_dir: &Path, run: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                assert!(
                    !status.success(),
                    "invalid standalone restart unexpectedly succeeded\n{}",
                    process_logs(test_dir, run)
                );
                return;
            }
            Ok(None) => {}
            Err(error) => panic!("standalone control-plane status should be readable: {error}"),
        }
        if Instant::now() >= deadline {
            panic!(
                "invalid standalone restart did not fail before the deadline\n{}",
                process_logs(test_dir, run)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn remove_socket_if_present(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("failed to remove stale socket {}: {error}", path.display()),
    }
}

fn create_control_plane_state_lock_file(state_path: &Path) {
    let file_name = state_path
        .file_name()
        .expect("test state path should have a file name");
    let mut lock_name = OsString::from(file_name);
    lock_name.push(".lock");
    let mut lock_path = state_path.to_path_buf();
    lock_path.set_file_name(lock_name);
    File::create(lock_path).expect("empty process lock file should be created");
}

#[test]
fn unsupported_authority_clock_checkpoint_versions_fail_before_standalone_open_or_binding() {
    let bin = argmin_s3_bin();
    for version in [
        TestUnsupportedAuthorityClockCheckpointVersion::One,
        TestUnsupportedAuthorityClockCheckpointVersion::Three,
    ] {
        let test_dir = TestDir::new();
        let state_dir = test_dir.path.join("state");
        fs::create_dir(&state_dir).expect("state directory should be created");
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
            .expect("state directory should be private");
        let state_path = state_dir.join("control.state");
        let control_socket = test_dir.path.join("control.sock");
        let recovery_socket = storage::control_plane_clock_recovery_socket_path(&control_socket);

        let mut initial = spawn_standalone_control_plane(
            &bin,
            &test_dir.path,
            &state_path,
            &control_socket,
            "initial",
        );
        wait_for_listener(&mut initial, &control_socket, &test_dir.path, "initial");
        initial.stop();
        remove_socket_if_present(&control_socket);
        remove_socket_if_present(&recovery_socket);

        let expected = prepare_unsupported_authority_clock_checkpoint_restart(&state_path, version)
            .expect("unsupported checkpoint fixture should be installed");
        let run = format!("unsupported-v{}", version.encoded_version());
        let mut restarted = spawn_standalone_control_plane(
            &bin,
            &test_dir.path,
            &state_path,
            &control_socket,
            &run,
        );
        wait_for_failure(&mut restarted, &test_dir.path, &run);

        let logs = process_logs(&test_dir.path, &run);
        assert!(
            logs.contains(&format!(
                "unsupported checkpoint version {}",
                version.encoded_version()
            )),
            "standalone restart did not report the retained version rejection\n{logs}"
        );
        assert!(
            !control_socket.exists(),
            "ordinary listener was bound before checkpoint rejection"
        );
        assert!(
            !recovery_socket.exists(),
            "clock-recovery listener was bound before checkpoint rejection"
        );
        assert_eq!(
            observe_single_authority_durable_state(&state_path)
                .expect("post-failure durable state should be observable"),
            expected,
            "checkpoint rejection must preserve the checkpoint, recoverable journal tail, and every other durable artifact"
        );
    }
}

#[test]
fn unsupported_single_authority_identity_versions_fail_before_state_creation_or_binding() {
    let bin = argmin_s3_bin();
    for version in [
        TestUnsupportedSingleAuthorityIdentityVersion::Zero,
        TestUnsupportedSingleAuthorityIdentityVersion::Two,
    ] {
        let test_dir = TestDir::new();
        let state_dir = test_dir.path.join("state");
        fs::create_dir(&state_dir).expect("state directory should be created");
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
            .expect("state directory should be private");
        let state_path = state_dir.join("control.state");
        create_control_plane_state_lock_file(&state_path);
        let control_socket = test_dir.path.join("control.sock");
        let recovery_socket = storage::control_plane_clock_recovery_socket_path(&control_socket);
        let expected = prepare_unsupported_single_authority_identity_restart(&state_path, version)
            .expect("unsupported durable-identity fixture should be installed");

        let run = format!("unsupported-identity-v{}", version.encoded_version());
        let mut process = spawn_standalone_control_plane(
            &bin,
            &test_dir.path,
            &state_path,
            &control_socket,
            &run,
        );
        wait_for_failure(&mut process, &test_dir.path, &run);

        let logs = process_logs(&test_dir.path, &run);
        assert!(
            logs.contains(&format!(
                "unsupported single-authority durable identity version {}",
                version.encoded_version()
            )),
            "standalone restart did not report the retained identity-version rejection\n{logs}"
        );
        assert!(
            !control_socket.exists(),
            "ordinary listener was bound before durable-identity rejection"
        );
        assert!(
            !recovery_socket.exists(),
            "clock-recovery listener was bound before durable-identity rejection"
        );
        assert_eq!(
            observe_single_authority_durable_state(&state_path)
                .expect("post-failure durable state should be observable"),
            expected,
            "identity rejection must preserve the invalid identity without creating or replacing any durable state artifact"
        );
    }
}

#[test]
fn unsupported_single_authority_initialization_versions_fail_before_replay_or_repair() {
    let bin = argmin_s3_bin();
    for version in [
        TestUnsupportedSingleAuthorityInitializationVersion::Zero,
        TestUnsupportedSingleAuthorityInitializationVersion::Two,
    ] {
        let test_dir = TestDir::new();
        let state_dir = test_dir.path.join("state");
        fs::create_dir(&state_dir).expect("state directory should be created");
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
            .expect("state directory should be private");
        let state_path = state_dir.join("control.state");
        let control_socket = test_dir.path.join("control.sock");
        let recovery_socket = storage::control_plane_clock_recovery_socket_path(&control_socket);

        let mut initial = spawn_standalone_control_plane(
            &bin,
            &test_dir.path,
            &state_path,
            &control_socket,
            "initial",
        );
        wait_for_listener(&mut initial, &control_socket, &test_dir.path, "initial");
        initial.stop();
        remove_socket_if_present(&control_socket);
        remove_socket_if_present(&recovery_socket);

        let expected =
            prepare_unsupported_single_authority_initialization_restart(&state_path, version)
                .expect("unsupported initialization-marker fixture should be installed");
        let run = format!("unsupported-initialization-v{}", version.encoded_version());
        let mut restarted = spawn_standalone_control_plane(
            &bin,
            &test_dir.path,
            &state_path,
            &control_socket,
            &run,
        );
        wait_for_failure(&mut restarted, &test_dir.path, &run);

        let logs = process_logs(&test_dir.path, &run);
        assert!(
            logs.contains(&format!(
                "unsupported single-authority initialization marker version {}",
                version.encoded_version()
            )),
            "standalone restart did not report the retained initialization-marker version rejection\n{logs}"
        );
        assert!(
            !control_socket.exists(),
            "ordinary listener was bound before initialization-marker rejection"
        );
        assert!(
            !recovery_socket.exists(),
            "clock-recovery listener was bound before initialization-marker rejection"
        );
        assert_eq!(
            observe_single_authority_durable_state(&state_path)
                .expect("post-failure durable state should be observable"),
            expected,
            "initialization-marker rejection must preserve the invalid marker, recoverable journal tail, and every other durable artifact"
        );
    }
}

#[test]
fn unsupported_single_authority_journal_file_versions_fail_before_replay_or_repair() {
    let bin = argmin_s3_bin();
    for version in [
        TestUnsupportedSingleAuthorityJournalFileVersion::One,
        TestUnsupportedSingleAuthorityJournalFileVersion::Three,
    ] {
        let test_dir = TestDir::new();
        let state_dir = test_dir.path.join("state");
        fs::create_dir(&state_dir).expect("state directory should be created");
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
            .expect("state directory should be private");
        let state_path = state_dir.join("control.state");
        let control_socket = test_dir.path.join("control.sock");
        let recovery_socket = storage::control_plane_clock_recovery_socket_path(&control_socket);

        let mut initial = spawn_standalone_control_plane(
            &bin,
            &test_dir.path,
            &state_path,
            &control_socket,
            "initial",
        );
        wait_for_listener(&mut initial, &control_socket, &test_dir.path, "initial");
        initial.stop();
        remove_socket_if_present(&control_socket);
        remove_socket_if_present(&recovery_socket);

        let expected =
            prepare_unsupported_single_authority_journal_file_restart(&state_path, version)
                .expect("unsupported journal-file fixture should be installed");
        let run = format!("unsupported-journal-file-v{}", version.encoded_version());
        let mut restarted = spawn_standalone_control_plane(
            &bin,
            &test_dir.path,
            &state_path,
            &control_socket,
            &run,
        );
        wait_for_failure(&mut restarted, &test_dir.path, &run);

        let logs = process_logs(&test_dir.path, &run);
        assert!(
            logs.contains(&format!(
                "unsupported single-authority control-plane journal file header version {}",
                version.encoded_version()
            )),
            "standalone restart did not report the retained journal-file version rejection\n{logs}"
        );
        assert!(
            !control_socket.exists(),
            "ordinary listener was bound before journal-file rejection"
        );
        assert!(
            !recovery_socket.exists(),
            "clock-recovery listener was bound before journal-file rejection"
        );
        assert_eq!(
            observe_single_authority_durable_state(&state_path)
                .expect("post-failure durable state should be observable"),
            expected,
            "journal-file rejection must preserve the unsupported header, recoverable torn tail, and every other durable artifact"
        );
    }
}

#[test]
fn unsupported_single_authority_journal_record_versions_fail_before_replay_or_repair() {
    let bin = argmin_s3_bin();
    for version in [
        TestUnsupportedSingleAuthorityJournalRecordVersion::One,
        TestUnsupportedSingleAuthorityJournalRecordVersion::Three,
    ] {
        let test_dir = TestDir::new();
        let state_dir = test_dir.path.join("state");
        fs::create_dir(&state_dir).expect("state directory should be created");
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
            .expect("state directory should be private");
        let state_path = state_dir.join("control.state");
        let control_socket = test_dir.path.join("control.sock");
        let recovery_socket = storage::control_plane_clock_recovery_socket_path(&control_socket);

        let mut initial = spawn_standalone_control_plane(
            &bin,
            &test_dir.path,
            &state_path,
            &control_socket,
            "initial",
        );
        wait_for_listener(&mut initial, &control_socket, &test_dir.path, "initial");
        initial.stop();
        remove_socket_if_present(&control_socket);
        remove_socket_if_present(&recovery_socket);

        let expected =
            prepare_unsupported_single_authority_journal_record_restart(&state_path, version)
                .expect("unsupported journal-record fixture should be installed");
        let run = format!("unsupported-journal-record-v{}", version.encoded_version());
        let mut restarted = spawn_standalone_control_plane(
            &bin,
            &test_dir.path,
            &state_path,
            &control_socket,
            &run,
        );
        wait_for_failure(&mut restarted, &test_dir.path, &run);

        let logs = process_logs(&test_dir.path, &run);
        assert!(
            logs.contains(&format!(
                "unsupported single-authority control-plane journal record version {}",
                version.encoded_version()
            )),
            "standalone restart did not report the retained journal-record version rejection\n{logs}"
        );
        assert!(
            !control_socket.exists(),
            "ordinary listener was bound before journal-record rejection"
        );
        assert!(
            !recovery_socket.exists(),
            "clock-recovery listener was bound before journal-record rejection"
        );
        assert_eq!(
            observe_single_authority_durable_state(&state_path)
                .expect("post-failure durable state should be observable"),
            expected,
            "journal-record rejection must preserve the unsupported record, invalid inner command, recoverable torn tail, and every other durable artifact"
        );
    }
}
