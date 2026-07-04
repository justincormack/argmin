use std::collections::BTreeSet;
use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use openraft::Vote;
use storage::control_plane_raft::{
    durable_artifact_wal_path, ControlPlaneRaftLeaderId, ControlPlaneRaftRestartArtifact,
    ControlPlaneRaftWalFile, ControlPlaneRaftWalRecord,
};
use storage::{NodeId, PgId};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(_name: &str) -> Self {
        static NEXT_TEST_DIR_ID: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos();
        let id = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("a3rt-{}-{id}-{now:x}", std::process::id()));
        fs::create_dir_all(&path).expect("test directory should be created");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .expect("test directory should be private");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct ChildGuard {
    node_id: u64,
    test_dir: PathBuf,
    child: Option<Child>,
}

impl ChildGuard {
    fn spawn(
        bin: &Path,
        test_dir: &Path,
        cluster_name: &str,
        raft_node_id: u64,
        peer_node_ids: &[u64],
    ) -> Self {
        let control_socket = test_dir.join(format!("control-{raft_node_id}.sock"));
        let peer_socket = test_dir.join(format!("raft-{raft_node_id}.sock"));
        let state_path = test_dir.join(format!("control-{raft_node_id}.state"));
        let data_dir = test_dir.join(format!("data-{raft_node_id}"));
        let stdout = File::create(test_dir.join(format!("node-{raft_node_id}.stdout.log")))
            .expect("stdout log should be created");
        let stderr = File::create(test_dir.join(format!("node-{raft_node_id}.stderr.log")))
            .expect("stderr log should be created");
        let peer_sockets = peer_node_ids
            .iter()
            .map(|node_id| {
                format!(
                    "{node_id}={}",
                    test_dir.join(format!("raft-{node_id}.sock")).display()
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let storage_node_sockets = format!(
            "0={},1={}",
            test_dir.join("storage-node-0.sock").display(),
            test_dir.join("storage-node-1.sock").display()
        );
        let child = Command::new(bin)
            .env("ARGMIN_ACCOUNT_ID", "123456789012")
            .env("ARGMIN_ACCESS_KEY_ID", "process-test-access")
            .env("ARGMIN_SECRET_ACCESS_KEY", "process-test-secret")
            .env("ARGMIN_PROCESS_ROLE", "control-plane")
            .env("ARGMIN_DATA_DIR", data_dir)
            .env("ARGMIN_PG_COUNT", "1")
            .env("ARGMIN_STORAGE_PG_IDS", "0")
            .env("ARGMIN_EC_K", "1")
            .env("ARGMIN_EC_M", "1")
            .env("ARGMIN_LOCAL_NODE_COUNT", "2")
            .env("ARGMIN_STORAGE_NODE_SOCKETS", storage_node_sockets)
            .env("ARGMIN_CONTROL_PLANE_STATE_PATH", state_path)
            .env("ARGMIN_CONTROL_PLANE_SOCKET_PATH", control_socket)
            .env("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "1")
            .env("ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME", cluster_name)
            .env(
                "ARGMIN_CONTROL_PLANE_RAFT_NODE_ID",
                raft_node_id.to_string(),
            )
            .env("ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH", peer_socket)
            .env("ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS", peer_sockets)
            .env("ARGMIN_CONTROL_PLANE_LEASE_SCAN_MS", "1000")
            .env("ARGMIN_CONTROL_PLANE_REFRESH_MS", "50")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("argmin-s3 control-plane process should start");
        Self {
            node_id: raft_node_id,
            test_dir: test_dir.to_path_buf(),
            child: Some(child),
        }
    }

    fn assert_running(&mut self) {
        let child = self
            .child
            .as_mut()
            .expect("argmin-s3 process should not be checked after stop");
        match child.try_wait() {
            Ok(None) => {}
            Ok(Some(status)) => panic!(
                "argmin-s3 process {} exited early with {status}\n{}",
                self.node_id,
                process_logs(&self.test_dir)
            ),
            Err(error) => panic!("argmin-s3 process status should be readable: {error}"),
        }
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

fn run_runtime_map_ready(bin: &Path, socket_path: &Path) -> Output {
    Command::new(bin)
        .arg("control-plane-runtime-map-ready")
        .arg(socket_path)
        .output()
        .expect("runtime-map ready helper should run")
}

fn run_set_pg_acting_set_live(
    bin: &Path,
    socket_path: &Path,
    pg_id: u32,
    acting_set: &[u32],
) -> Output {
    let mut command = Command::new(bin);
    command
        .arg("control-plane-set-pg-acting-set-live")
        .arg(socket_path)
        .arg(pg_id.to_string());
    for node_id in acting_set {
        command.arg(node_id.to_string());
    }
    command
        .output()
        .expect("set-pg-acting-set live helper should run")
}

fn run_transfer_raft_leadership(bin: &Path, socket_path: &Path, node_id: u64) -> Output {
    Command::new(bin)
        .arg("control-plane-transfer-raft-leadership")
        .arg(socket_path)
        .arg(node_id.to_string())
        .output()
        .expect("transfer Raft leadership helper should run")
}

fn run_trigger_raft_snapshot_purge(bin: &Path, socket_path: &Path) -> Output {
    Command::new(bin)
        .arg("control-plane-trigger-raft-snapshot-purge")
        .arg(socket_path)
        .output()
        .expect("trigger Raft snapshot purge helper should run")
}

fn run_trigger_raft_election(bin: &Path, socket_path: &Path) -> Output {
    Command::new(bin)
        .arg("control-plane-trigger-raft-election")
        .arg(socket_path)
        .output()
        .expect("trigger Raft election helper should run")
}

fn wait_for_trigger_raft_election(
    bin: &Path,
    socket: &Path,
    test_dir: &Path,
    children: &mut [&mut ChildGuard],
) -> Output {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for child in children.iter_mut() {
            child.assert_running();
        }
        let output = run_trigger_raft_election(bin, socket);
        if output.status.success() {
            return output;
        }
        if Instant::now() >= deadline {
            let last_failure = format_admin_failure(output.status, &output);
            panic!(
                "control-plane Raft election trigger did not succeed on {}: {last_failure}\n{}",
                socket.display(),
                process_logs(test_dir)
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_runtime_map_ready(
    bin: &Path,
    test_dir: &Path,
    children: &mut [&mut ChildGuard],
    raft_node_ids: &[u64],
) -> (PathBuf, Output) {
    let control_sockets = raft_node_ids
        .iter()
        .map(|node_id| test_dir.join(format!("control-{node_id}.sock")))
        .collect::<Vec<_>>();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_failure = String::new();
    loop {
        for child in children.iter_mut() {
            child.assert_running();
        }
        for socket in &control_sockets {
            let output = run_runtime_map_ready(bin, socket);
            if output.status.success() {
                return (socket.clone(), output);
            }
            last_failure = format_admin_failure(output.status, &output);
        }
        if Instant::now() >= deadline {
            panic!(
                "control-plane runtime map did not become ready: {last_failure}\n{}",
                process_logs(test_dir)
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_runtime_map_ready_on(
    bin: &Path,
    socket: &Path,
    test_dir: &Path,
    children: &mut [&mut ChildGuard],
) -> Output {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for child in children.iter_mut() {
            child.assert_running();
        }
        let output = run_runtime_map_ready(bin, socket);
        if output.status.success() {
            return output;
        }
        if Instant::now() >= deadline {
            let last_failure = format_admin_failure(output.status, &output);
            panic!(
                "control-plane runtime map did not become ready on {}: {last_failure}\n{}",
                socket.display(),
                process_logs(test_dir)
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_socket_file(path: &Path, child: &mut ChildGuard) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        child.assert_running();
        if path.exists() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("socket file {} was not created", path.display());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn format_admin_failure(status: ExitStatus, output: &Output) -> String {
    format!(
        "status={status} stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn follower_artifact_has_bootstrapped_state(path: &Path) -> Result<bool, String> {
    let artifact = match ControlPlaneRaftRestartArtifact::load_durable_artifact(path) {
        Ok(artifact) => artifact,
        Err(error) => return Err(error.to_string()),
    };
    let (_log_store, state_machine) = artifact.restore().map_err(|error| error.to_string())?;
    let snapshot = state_machine.inner().snapshot();
    let node_ids: BTreeSet<_> = snapshot.nodes().map(|node| node.node_id()).collect();
    let pg_ids: BTreeSet<_> = snapshot.pgs().map(|pg| pg.pg_id()).collect();
    Ok(node_ids == BTreeSet::from([NodeId::new(0), NodeId::new(1)])
        && pg_ids == BTreeSet::from([PgId::new(0)]))
}

fn artifact_has_committed_vote_for_leader(path: &Path, leader_id: u64) -> Result<bool, String> {
    let vote = artifact_persisted_vote(path)?;
    Ok(vote.is_some_and(|vote| vote.committed && vote.node_id == leader_id && vote.term > 0))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PersistedVoteSummary {
    term: u64,
    node_id: u64,
    committed: bool,
}

fn persisted_vote_summary(vote: Vote<ControlPlaneRaftLeaderId>) -> PersistedVoteSummary {
    PersistedVoteSummary {
        term: vote.leader_id.term,
        node_id: vote.leader_id.node_id,
        committed: vote.committed,
    }
}

fn artifact_persisted_vote(path: &Path) -> Result<Option<PersistedVoteSummary>, String> {
    let artifact = match ControlPlaneRaftRestartArtifact::load_durable_artifact(path) {
        Ok(artifact) => artifact,
        Err(error) => return Err(error.to_string()),
    };
    let (log_store, _state_machine) = artifact.restore().map_err(|error| error.to_string())?;
    let vote = log_store
        .persisted_vote()
        .map_err(|error| error.to_string())?;
    Ok(vote.map(persisted_vote_summary))
}

fn artifact_persisted_vote_with_wal(
    path: &Path,
    cluster_name: &str,
    node_id: u64,
) -> Result<Option<PersistedVoteSummary>, String> {
    let artifact = match ControlPlaneRaftRestartArtifact::load_durable_artifact(path) {
        Ok(artifact) => artifact,
        Err(error) => return Err(error.to_string()),
    };
    let (log_store, _state_machine) = artifact
        .restore_with_wal_file(ControlPlaneRaftWalFile::new(
            durable_artifact_wal_path(path),
            cluster_name,
            node_id,
        ))
        .map_err(|error| error.to_string())?;
    let vote = log_store
        .persisted_vote()
        .map_err(|error| error.to_string())?;
    Ok(vote.map(persisted_vote_summary))
}

fn follower_artifact_pg_has_acting_set(
    path: &Path,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<bool, String> {
    let artifact = match ControlPlaneRaftRestartArtifact::load_durable_artifact(path) {
        Ok(artifact) => artifact,
        Err(error) => return Err(error.to_string()),
    };
    let (_log_store, state_machine) = artifact.restore().map_err(|error| error.to_string())?;
    let snapshot = state_machine.inner().snapshot();
    let Some(pg) = snapshot.pg(pg_id) else {
        return Ok(false);
    };
    Ok(pg.acting_set() == acting_set)
}

fn follower_artifact_pg_has_acting_set_and_snapshot_index_at_least(
    path: &Path,
    pg_id: PgId,
    acting_set: &[NodeId],
    min_snapshot_index: u64,
) -> Result<bool, String> {
    let artifact = match ControlPlaneRaftRestartArtifact::load_durable_artifact(path) {
        Ok(artifact) => artifact,
        Err(error) => return Err(error.to_string()),
    };
    let (_log_store, state_machine) = artifact.restore().map_err(|error| error.to_string())?;
    let snapshot_index = state_machine
        .current_snapshot()
        .and_then(|snapshot| snapshot.meta.last_log_id)
        .map(|log_id| log_id.index());
    let Some(snapshot_index) = snapshot_index else {
        return Ok(false);
    };
    if snapshot_index < min_snapshot_index {
        return Ok(false);
    }
    let snapshot = state_machine.inner().snapshot();
    let Some(pg) = snapshot.pg(pg_id) else {
        return Ok(false);
    };
    Ok(pg.acting_set() == acting_set)
}

fn wait_for_follower_artifact(path: &Path, children: &mut [&mut ChildGuard]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for child in children.iter_mut() {
            child.assert_running();
        }
        let last_error = match follower_artifact_has_bootstrapped_state(path) {
            Ok(true) => return,
            Ok(false) => "artifact restored but did not contain bootstrap".to_string(),
            Err(error) => error,
        };
        if Instant::now() >= deadline {
            panic!("follower durable artifact did not contain replicated bootstrap: {last_error}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_artifact_committed_vote_at_least(
    path: &Path,
    leader_id: u64,
    min_term: u64,
    children: &mut [&mut ChildGuard],
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for child in children.iter_mut() {
            child.assert_running();
        }
        let last_error = match artifact_persisted_vote(path) {
            Ok(Some(vote))
                if vote.committed && vote.node_id == leader_id && vote.term >= min_term =>
            {
                return;
            }
            Ok(Some(vote)) => format!(
                "artifact restored with committed={} leader={} term={}, expected committed leader {} term >= {}",
                vote.committed, vote.node_id, vote.term, leader_id, min_term
            ),
            Ok(None) => "artifact restored without persisted vote".to_string(),
            Err(error) => error,
        };
        if Instant::now() >= deadline {
            panic!(
                "durable artifact did not checkpoint committed vote for leader {leader_id} at term >= {min_term}: {last_error}"
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_follower_artifact_pg_acting_set(
    path: &Path,
    pg_id: PgId,
    acting_set: &[NodeId],
    children: &mut [&mut ChildGuard],
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for child in children.iter_mut() {
            child.assert_running();
        }
        let last_error = match follower_artifact_pg_has_acting_set(path, pg_id, acting_set) {
            Ok(true) => return,
            Ok(false) => "artifact restored but did not contain expected PG acting set".to_string(),
            Err(error) => error,
        };
        if Instant::now() >= deadline {
            panic!(
                "follower durable artifact did not contain PG {} acting set {:?}: {last_error}",
                pg_id.get(),
                acting_set
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_follower_artifact_pg_acting_set_from_snapshot(
    path: &Path,
    pg_id: PgId,
    acting_set: &[NodeId],
    min_snapshot_index: u64,
    children: &mut [&mut ChildGuard],
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for child in children.iter_mut() {
            child.assert_running();
        }
        let last_error = match follower_artifact_pg_has_acting_set_and_snapshot_index_at_least(
            path,
            pg_id,
            acting_set,
            min_snapshot_index,
        ) {
            Ok(true) => return,
            Ok(false) => {
                "artifact restored but did not contain expected PG acting set from snapshot"
                    .to_string()
            }
            Err(error) => error,
        };
        if Instant::now() >= deadline {
            panic!(
                "follower durable artifact did not contain PG {} acting set {:?} with snapshot index at least {}: {last_error}",
                pg_id.get(),
                acting_set,
                min_snapshot_index
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn process_logs(test_dir: &Path) -> String {
    let mut out = String::new();
    let mut log_paths = fs::read_dir(test_dir)
        .expect("test directory should be readable")
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let name = path.file_name()?.to_str()?;
            (name.starts_with("node-") && name.ends_with(".log")).then_some(path)
        })
        .collect::<Vec<_>>();
    log_paths.sort();
    for path in log_paths {
        let contents = fs::read_to_string(&path)
            .unwrap_or_else(|error| format!("<failed to read {}: {error}>", path.display()));
        out.push_str(&format!(
            "== {} ==\n{}\n",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("<unknown log>"),
            contents
        ));
    }
    out
}

fn control_socket(test_dir: &Path, node_id: u64) -> PathBuf {
    test_dir.join(format!("control-{node_id}.sock"))
}

fn peer_socket(test_dir: &Path, node_id: u64) -> PathBuf {
    test_dir.join(format!("raft-{node_id}.sock"))
}

fn state_path(test_dir: &Path, node_id: u64) -> PathBuf {
    test_dir.join(format!("control-{node_id}.state"))
}

fn wal_path(test_dir: &Path, node_id: u64) -> PathBuf {
    durable_artifact_wal_path(&state_path(test_dir, node_id))
}

fn state_sentinel_path(test_dir: &Path, node_id: u64) -> PathBuf {
    let state_path = state_path(test_dir, node_id);
    let file_name = state_path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .expect("state path should have UTF-8 file name");
    state_path.with_file_name(format!("{file_name}.sentinel"))
}

fn wait_for_process_exit(child: &mut ChildGuard, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        let process = child
            .child
            .as_mut()
            .expect("argmin-s3 process should not be checked after stop");
        match process.try_wait() {
            Ok(Some(status)) => return status,
            Ok(None) => {}
            Err(error) => panic!("argmin-s3 process status should be readable: {error}"),
        }
        if Instant::now() >= deadline {
            panic!(
                "argmin-s3 process {} did not exit within {:?}\n{}",
                child.node_id,
                timeout,
                process_logs(&child.test_dir)
            );
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn experimental_raft_two_control_plane_processes_replicate_bootstrap_to_follower_artifact() {
    let bin = argmin_s3_bin();
    let test_dir = TestDir::new("experimental-raft-process-two-node");
    let cluster_name = format!(
        "process-two-node-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos()
    );
    let raft_node_ids = [101, 102];
    let mut node102 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 102, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 102), &mut node102);
    let mut node101 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 101, &raft_node_ids);

    let (leader_socket, output) = wait_for_runtime_map_ready(
        &bin,
        test_dir.path(),
        &mut [&mut node101, &mut node102],
        &raft_node_ids,
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.split_whitespace().nth(1) == Some("1"),
        "ready helper should report one PG route from {}: {stdout}",
        leader_socket.display()
    );

    let follower_state = if leader_socket.ends_with("control-101.sock") {
        state_path(test_dir.path(), 102)
    } else {
        state_path(test_dir.path(), 101)
    };
    wait_for_follower_artifact(&follower_state, &mut [&mut node101, &mut node102]);
}

#[test]
fn experimental_raft_process_rejects_missing_artifact_after_state_existed() {
    let bin = argmin_s3_bin();
    let test_dir = TestDir::new("experimental-raft-process-missing-artifact");
    let cluster_name = format!(
        "process-missing-artifact-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos()
    );
    let raft_node_ids = [101, 102];
    let mut node102 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 102, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 102), &mut node102);
    let mut node101 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 101, &raft_node_ids);

    let (_leader_socket, _output) = wait_for_runtime_map_ready(
        &bin,
        test_dir.path(),
        &mut [&mut node101, &mut node102],
        &raft_node_ids,
    );
    wait_for_follower_artifact(
        &state_path(test_dir.path(), 102),
        &mut [&mut node101, &mut node102],
    );
    assert!(
        state_sentinel_path(test_dir.path(), 102).exists(),
        "node 102 should persist a state-existed sentinel"
    );

    node102.stop();
    fs::remove_file(state_path(test_dir.path(), 102))
        .expect("test should delete only the durable artifact, not the sentinel");

    let mut restarted102 =
        ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 102, &raft_node_ids);
    let status = wait_for_process_exit(&mut restarted102, Duration::from_secs(5));
    assert!(
        !status.success(),
        "restart without artifact should fail closed, got {status}"
    );
    let logs = process_logs(test_dir.path());
    assert!(
        logs.contains("is missing but sentinel"),
        "restart failure should mention sentinel guard:\n{logs}"
    );
}

#[test]
fn experimental_raft_process_restart_replays_post_checkpoint_wal_suffix() {
    let bin = argmin_s3_bin();
    let test_dir = TestDir::new("experimental-raft-process-wal-suffix");
    let cluster_name = format!(
        "process-wal-suffix-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos()
    );
    let raft_node_ids = [101];
    let mut node101 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 101, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 101), &mut node101);

    let control_socket = control_socket(test_dir.path(), 101);
    wait_for_runtime_map_ready_on(&bin, &control_socket, test_dir.path(), &mut [&mut node101]);
    let state_path = state_path(test_dir.path(), 101);
    let vote_before_restart = artifact_persisted_vote_with_wal(&state_path, &cluster_name, 101)
        .expect("artifact plus WAL should restore before WAL suffix injection")
        .expect("bootstrapped process should persist a vote");

    node101.stop();

    let artifact_only_vote = artifact_persisted_vote(&state_path)
        .expect("artifact should restore before WAL suffix injection")
        .expect("artifact should contain the checkpointed vote");
    let injected_term = vote_before_restart.term + 1_000;
    let wal = ControlPlaneRaftWalFile::new(wal_path(test_dir.path(), 101), &cluster_name, 101);
    wal.append_record(&ControlPlaneRaftWalRecord::SaveVote(Vote::<
        ControlPlaneRaftLeaderId,
    >::new_committed(
        injected_term, 101
    )))
    .expect("test should append a post-checkpoint WAL vote record");

    assert_eq!(
        artifact_persisted_vote(&state_path)
            .expect("artifact should still restore after WAL suffix injection"),
        Some(artifact_only_vote),
        "WAL suffix injection must not rewrite the checkpoint artifact"
    );
    assert_eq!(
        artifact_persisted_vote_with_wal(&state_path, &cluster_name, 101)
            .expect("artifact plus WAL should restore after suffix injection"),
        Some(PersistedVoteSummary {
            term: injected_term,
            node_id: 101,
            committed: true,
        }),
        "artifact plus WAL restore should observe the injected post-checkpoint suffix"
    );

    let mut restarted101 =
        ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 101, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 101), &mut restarted101);
    wait_for_runtime_map_ready_on(
        &bin,
        &control_socket,
        test_dir.path(),
        &mut [&mut restarted101],
    );
    wait_for_artifact_committed_vote_at_least(
        &state_path,
        101,
        injected_term,
        &mut [&mut restarted101],
    );
}

#[test]
fn experimental_raft_restarted_control_plane_follower_catches_up_process_state() {
    let bin = argmin_s3_bin();
    let test_dir = TestDir::new("experimental-raft-process-follower-restart");
    let cluster_name = format!(
        "process-follower-restart-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos()
    );
    let raft_node_ids = [101, 102, 103];
    let mut node102 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 102, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 102), &mut node102);
    let mut node103 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 103, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 103), &mut node103);
    let mut node101 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 101, &raft_node_ids);
    let leader_control_socket = control_socket(test_dir.path(), 101);
    let output = wait_for_runtime_map_ready_on(
        &bin,
        &leader_control_socket,
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut node103],
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.split_whitespace().nth(1) == Some("1"),
        "ready helper should report one PG route from {}: {stdout}",
        leader_control_socket.display()
    );
    wait_for_follower_artifact(
        &state_path(test_dir.path(), 103),
        &mut [&mut node101, &mut node102, &mut node103],
    );

    node103.stop();
    let output = run_set_pg_acting_set_live(&bin, &leader_control_socket, 0, &[1]);
    assert!(
        output.status.success(),
        "live acting-set change failed: {}\n{}",
        format_admin_failure(output.status, &output),
        process_logs(test_dir.path())
    );

    let mut restarted103 =
        ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 103, &raft_node_ids);
    wait_for_follower_artifact_pg_acting_set(
        &state_path(test_dir.path(), 103),
        PgId::new(0),
        &[NodeId::new(1)],
        &mut [&mut node101, &mut node102, &mut restarted103],
    );
}

#[test]
fn experimental_raft_transferred_process_leader_survives_old_leader_loss() {
    let bin = argmin_s3_bin();
    let test_dir = TestDir::new("experimental-raft-process-transferred-leader");
    let cluster_name = format!(
        "process-transferred-leader-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos()
    );
    let raft_node_ids = [101, 102, 103];
    let mut node102 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 102, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 102), &mut node102);
    let mut node103 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 103, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 103), &mut node103);
    let mut node101 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 101, &raft_node_ids);

    let old_leader_socket = control_socket(test_dir.path(), 101);
    wait_for_runtime_map_ready_on(
        &bin,
        &old_leader_socket,
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut node103],
    );

    let transfer = run_transfer_raft_leadership(&bin, &old_leader_socket, 102);
    assert!(
        transfer.status.success(),
        "leadership transfer failed: {}\n{}",
        format_admin_failure(transfer.status, &transfer),
        process_logs(test_dir.path())
    );

    let surviving_node_ids = [102, 103];
    let (new_leader_socket, _output) = wait_for_runtime_map_ready(
        &bin,
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut node103],
        &surviving_node_ids,
    );
    let new_leader_id = if new_leader_socket == control_socket(test_dir.path(), 102) {
        102
    } else {
        103
    };

    node101.stop();
    let output = run_set_pg_acting_set_live(&bin, &new_leader_socket, 0, &[1]);
    assert!(
        output.status.success(),
        "post-transfer acting-set change failed: {}\n{}",
        format_admin_failure(output.status, &output),
        process_logs(test_dir.path())
    );
    let follower_id = if new_leader_id == 102 { 103 } else { 102 };
    wait_for_follower_artifact_pg_acting_set(
        &state_path(test_dir.path(), follower_id),
        PgId::new(0),
        &[NodeId::new(1)],
        &mut [&mut node102, &mut node103],
    );

    let mut restarted101 =
        ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 101, &raft_node_ids);
    wait_for_follower_artifact_pg_acting_set(
        &state_path(test_dir.path(), 101),
        PgId::new(0),
        &[NodeId::new(1)],
        &mut [&mut restarted101, &mut node102, &mut node103],
    );
}

#[test]
fn experimental_raft_triggered_process_election_survives_abrupt_leader_loss() {
    let bin = argmin_s3_bin();
    let test_dir = TestDir::new("experimental-raft-process-triggered-election");
    let cluster_name = format!(
        "process-triggered-election-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos()
    );
    let raft_node_ids = [101, 102, 103];
    let mut node102 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 102, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 102), &mut node102);
    let mut node103 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 103, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 103), &mut node103);
    let mut node101 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 101, &raft_node_ids);

    let old_leader_socket = control_socket(test_dir.path(), 101);
    wait_for_runtime_map_ready_on(
        &bin,
        &old_leader_socket,
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut node103],
    );
    wait_for_follower_artifact(
        &state_path(test_dir.path(), 102),
        &mut [&mut node101, &mut node102, &mut node103],
    );
    wait_for_follower_artifact(
        &state_path(test_dir.path(), 103),
        &mut [&mut node101, &mut node102, &mut node103],
    );

    node101.stop();

    let surviving_node_ids = [102, 103];
    let (candidate_socket, _output) = wait_for_runtime_map_ready(
        &bin,
        test_dir.path(),
        &mut [&mut node102, &mut node103],
        &surviving_node_ids,
    );
    let candidate_id = if candidate_socket == control_socket(test_dir.path(), 102) {
        102
    } else {
        103
    };
    wait_for_trigger_raft_election(
        &bin,
        &candidate_socket,
        test_dir.path(),
        &mut [&mut node102, &mut node103],
    );
    assert!(
        artifact_has_committed_vote_for_leader(&state_path(test_dir.path(), candidate_id), candidate_id)
            .expect("candidate durable artifact should restore after election trigger"),
        "election trigger returned before checkpointing node {candidate_id}'s committed leader vote\n{}",
        process_logs(test_dir.path())
    );

    wait_for_runtime_map_ready_on(
        &bin,
        &candidate_socket,
        test_dir.path(),
        &mut [&mut node102, &mut node103],
    );

    let output = run_set_pg_acting_set_live(&bin, &candidate_socket, 0, &[1]);
    assert!(
        output.status.success(),
        "post-election acting-set change failed: {}\n{}",
        format_admin_failure(output.status, &output),
        process_logs(test_dir.path())
    );
    let follower_id = if candidate_id == 102 { 103 } else { 102 };
    wait_for_follower_artifact_pg_acting_set(
        &state_path(test_dir.path(), follower_id),
        PgId::new(0),
        &[NodeId::new(1)],
        &mut [&mut node102, &mut node103],
    );
}

#[test]
fn experimental_raft_process_natural_election_survives_abrupt_leader_loss() {
    let bin = argmin_s3_bin();
    let test_dir = TestDir::new("experimental-raft-process-natural-election");
    let cluster_name = format!(
        "process-natural-election-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos()
    );
    let raft_node_ids = [101, 102, 103];
    let mut node102 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 102, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 102), &mut node102);
    let mut node103 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 103, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 103), &mut node103);
    let mut node101 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 101, &raft_node_ids);

    let old_leader_socket = control_socket(test_dir.path(), 101);
    wait_for_runtime_map_ready_on(
        &bin,
        &old_leader_socket,
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut node103],
    );
    wait_for_follower_artifact(
        &state_path(test_dir.path(), 102),
        &mut [&mut node101, &mut node102, &mut node103],
    );
    wait_for_follower_artifact(
        &state_path(test_dir.path(), 103),
        &mut [&mut node101, &mut node102, &mut node103],
    );

    node101.stop();

    let surviving_node_ids = [102, 103];
    let (new_leader_socket, _output) = wait_for_runtime_map_ready(
        &bin,
        test_dir.path(),
        &mut [&mut node102, &mut node103],
        &surviving_node_ids,
    );
    let new_leader_id = if new_leader_socket == control_socket(test_dir.path(), 102) {
        102
    } else {
        103
    };
    assert!(
        artifact_has_committed_vote_for_leader(
            &state_path(test_dir.path(), new_leader_id),
            new_leader_id,
        )
        .expect("new leader durable artifact should restore after natural election"),
        "natural election served before checkpointing node {new_leader_id}'s committed leader vote\n{}",
        process_logs(test_dir.path())
    );

    let output = run_set_pg_acting_set_live(&bin, &new_leader_socket, 0, &[1]);
    assert!(
        output.status.success(),
        "post-natural-election acting-set change failed: {}\n{}",
        format_admin_failure(output.status, &output),
        process_logs(test_dir.path())
    );
    let follower_id = if new_leader_id == 102 { 103 } else { 102 };
    let mut children = [&mut node102, &mut node103];
    wait_for_follower_artifact_pg_acting_set(
        &state_path(test_dir.path(), follower_id),
        PgId::new(0),
        &[NodeId::new(1)],
        &mut children,
    );
}

#[test]
fn experimental_raft_restarted_process_follower_catches_up_from_leader_snapshot() {
    let bin = argmin_s3_bin();
    let test_dir = TestDir::new("experimental-raft-process-snapshot-catchup");
    let cluster_name = format!(
        "process-snapshot-catchup-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos()
    );
    let raft_node_ids = [101, 102, 103];
    let mut node102 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 102, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 102), &mut node102);
    let mut node103 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 103, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 103), &mut node103);
    let mut node101 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 101, &raft_node_ids);
    let leader_control_socket = control_socket(test_dir.path(), 101);

    wait_for_runtime_map_ready_on(
        &bin,
        &leader_control_socket,
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut node103],
    );
    wait_for_follower_artifact(
        &state_path(test_dir.path(), 103),
        &mut [&mut node101, &mut node102, &mut node103],
    );

    node103.stop();
    let snapshot_covered_write = run_set_pg_acting_set_live(&bin, &leader_control_socket, 0, &[1]);
    assert!(
        snapshot_covered_write.status.success(),
        "snapshot-covered acting-set change failed: {}\n{}",
        format_admin_failure(snapshot_covered_write.status, &snapshot_covered_write),
        process_logs(test_dir.path())
    );
    wait_for_follower_artifact_pg_acting_set(
        &state_path(test_dir.path(), 102),
        PgId::new(0),
        &[NodeId::new(1)],
        &mut [&mut node101, &mut node102],
    );

    let snapshot_purge = run_trigger_raft_snapshot_purge(&bin, &leader_control_socket);
    assert!(
        snapshot_purge.status.success(),
        "snapshot purge failed: {}\n{}",
        format_admin_failure(snapshot_purge.status, &snapshot_purge),
        process_logs(test_dir.path())
    );
    let snapshot_index: u64 = String::from_utf8_lossy(&snapshot_purge.stdout)
        .trim()
        .parse()
        .expect("snapshot purge helper should report a snapshot index");

    let mut restarted103 =
        ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 103, &raft_node_ids);
    wait_for_socket_file(&peer_socket(test_dir.path(), 103), &mut restarted103);

    let suffix_write = run_set_pg_acting_set_live(&bin, &leader_control_socket, 0, &[0]);
    assert!(
        suffix_write.status.success(),
        "post-snapshot suffix acting-set change failed: {}\n{}",
        format_admin_failure(suffix_write.status, &suffix_write),
        process_logs(test_dir.path())
    );

    wait_for_follower_artifact_pg_acting_set_from_snapshot(
        &state_path(test_dir.path(), 103),
        PgId::new(0),
        &[NodeId::new(0)],
        snapshot_index,
        &mut [&mut node101, &mut node102, &mut restarted103],
    );
}
