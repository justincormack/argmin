use std::collections::BTreeSet;
use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use openraft::impls::leader_id_adv::LeaderId;
use openraft::impls::Entry;
use openraft::raft::AppendEntriesRequest;
use openraft::storage::{RaftLogReader, RaftLogStorage};
use openraft::{EntryPayload, LogId, Vote};
use storage::control_plane::{
    AuthenticatedUnixControlPlaneClient, ControlPlaneHeartbeatRuntimeMapSource, NodeHeartbeat,
    UnixControlPlaneClient,
};
use storage::control_plane_auth::{
    ControlPlaneAuthOperation, ControlPlaneAuthPrincipal, ControlPlaneAuthSignInput,
    ControlPlaneAuthTarget, ControlPlaneScopedCredential, ControlPlaneScopedCredentialInput,
};
use storage::control_plane_command::ControlPlaneCommand;
use storage::control_plane_raft::{
    durable_artifact_wal_path, read_control_plane_raft_peer_transport_frame,
    write_control_plane_raft_peer_transport_frame, ControlPlaneRaftEntry, ControlPlaneRaftLeaderId,
    ControlPlaneRaftLogId, ControlPlaneRaftPeerFrameIdentity, ControlPlaneRaftPeerRpcRequest,
    ControlPlaneRaftPeerTransportLimits, ControlPlaneRaftRestartArtifact, ControlPlaneRaftWalFile,
    ControlPlaneRaftWalFileConfig, ControlPlaneRaftWalRecord,
};
use storage::{ClusterEpoch, NodeId, PgId};

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

struct ProcessTestControlPlaneAuth {
    cluster_name: String,
}

impl ProcessTestControlPlaneAuth {
    fn new(cluster_name: &str) -> Self {
        Self {
            cluster_name: cluster_name.to_owned(),
        }
    }

    fn raft_peer_secret(node_id: u64) -> String {
        format!("process-test-raft-node-{node_id}-secret")
    }

    fn raft_peer_credential_id(node_id: u64) -> String {
        format!("raft-node-{node_id}")
    }

    fn storage_node_secret(node_id: u32) -> String {
        format!("process-test-storage-node-{node_id}-secret")
    }

    fn storage_node_credential_id(node_id: u32) -> String {
        format!("storage-node-{node_id}")
    }

    fn frontend_secret(instance_id: &str) -> String {
        format!("process-test-frontend-{instance_id}-secret")
    }

    fn frontend_credential_id(instance_id: &str) -> String {
        format!("frontend-{instance_id}")
    }

    fn admin_secret(instance_id: &str) -> String {
        format!("process-test-admin-{instance_id}-secret")
    }

    fn admin_credential_id(instance_id: &str) -> String {
        format!("admin-{instance_id}")
    }

    fn raft_peer_config_entry(node_id: u64) -> String {
        format!(
            "{node_id}={}:1:{}",
            Self::raft_peer_credential_id(node_id),
            Self::raft_peer_secret(node_id)
        )
    }

    fn storage_node_config_entry(node_id: u32) -> String {
        format!(
            "{node_id}={}:1:{}",
            Self::storage_node_credential_id(node_id),
            Self::storage_node_secret(node_id)
        )
    }

    fn frontend_config_entry(instance_id: &str) -> String {
        format!(
            "{instance_id}={}:1:{}",
            Self::frontend_credential_id(instance_id),
            Self::frontend_secret(instance_id)
        )
    }

    fn admin_config_entry(instance_id: &str) -> String {
        format!(
            "{instance_id}={}:1:{}",
            Self::admin_credential_id(instance_id),
            Self::admin_secret(instance_id)
        )
    }

    fn raft_peer_credentials_env(&self, local_node_id: u64, peer_node_ids: &[u64]) -> String {
        let mut auth_nodes: BTreeSet<u64> = peer_node_ids.iter().copied().collect();
        auth_nodes.insert(local_node_id);
        auth_nodes
            .into_iter()
            .map(Self::raft_peer_config_entry)
            .collect::<Vec<_>>()
            .join(",")
    }

    fn storage_node_credentials_env(&self, node_ids: &[u32]) -> String {
        node_ids
            .iter()
            .copied()
            .map(Self::storage_node_config_entry)
            .collect::<Vec<_>>()
            .join(",")
    }

    fn frontend_credentials_env(&self, instance_ids: &[&str]) -> String {
        instance_ids
            .iter()
            .map(|instance_id| Self::frontend_config_entry(instance_id))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn admin_credentials_env(&self, instance_ids: &[&str]) -> String {
        instance_ids
            .iter()
            .map(|instance_id| Self::admin_config_entry(instance_id))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn raft_peer_credential(&self, node_id: u64) -> ControlPlaneScopedCredential {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: self.cluster_name.clone(),
            credential_id: Self::raft_peer_credential_id(node_id),
            credential_version: 1,
            principal: ControlPlaneAuthPrincipal::RaftPeer { node_id },
            secret: Self::raft_peer_secret(node_id).into_bytes(),
        })
        .expect("process test Raft peer credential should build")
    }

    fn storage_node_credential(
        &self,
        node_id: u32,
        incarnation: u64,
    ) -> ControlPlaneScopedCredential {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: self.cluster_name.clone(),
            credential_id: Self::storage_node_credential_id(node_id),
            credential_version: 1,
            principal: ControlPlaneAuthPrincipal::StorageNode {
                node_id: NodeId::new(node_id),
                incarnation,
            },
            secret: Self::storage_node_secret(node_id).into_bytes(),
        })
        .expect("process test storage-node credential should build")
    }

    fn sign_raft_peer_frame(
        &self,
        source_node_id: u64,
        target_node_id: u64,
        operation: ControlPlaneAuthOperation,
        payload: Vec<u8>,
    ) -> Vec<u8> {
        self.raft_peer_credential(source_node_id)
            .sign_envelope(ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: target_node_id,
                }),
                operation,
                issued_at_ms: None,
                expires_at_ms: None,
                sequence: None,
                nonce: Vec::new(),
                payload,
            })
            .expect("process test Raft peer frame should sign")
            .encode_frame()
            .expect("process test Raft peer auth envelope should encode")
    }
}

impl ChildGuard {
    fn spawn(
        bin: &Path,
        test_dir: &Path,
        cluster_name: &str,
        raft_node_id: u64,
        peer_node_ids: &[u64],
    ) -> Self {
        Self::spawn_with_extra_env(
            bin,
            test_dir,
            cluster_name,
            raft_node_id,
            peer_node_ids,
            &[],
        )
    }

    fn spawn_with_extra_env(
        bin: &Path,
        test_dir: &Path,
        cluster_name: &str,
        raft_node_id: u64,
        peer_node_ids: &[u64],
        extra_env: &[(&str, &str)],
    ) -> Self {
        let control_socket = test_dir.join(format!("control-{raft_node_id}.sock"));
        let peer_socket = test_dir.join(format!("raft-{raft_node_id}.sock"));
        let state_dir = state_dir(test_dir, raft_node_id);
        fs::create_dir_all(&state_dir).expect("state directory should be created");
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
            .expect("state directory permissions should be tightened");
        let state_path = state_dir.join("control.state");
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
        let auth = ProcessTestControlPlaneAuth::new(cluster_name);
        let peer_auth_credentials = auth.raft_peer_credentials_env(raft_node_id, peer_node_ids);
        let storage_node_sockets = format!(
            "0={},1={}",
            test_dir.join("storage-node-0.sock").display(),
            test_dir.join("storage-node-1.sock").display()
        );
        let mut command = Command::new(bin);
        command
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
            .env(
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
                peer_auth_credentials,
            )
            .env("ARGMIN_CONTROL_PLANE_LEASE_SCAN_MS", "1000")
            .env("ARGMIN_CONTROL_PLANE_FRONTEND_REFRESH_MS", "50")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        for (key, value) in extra_env {
            command.env(key, value);
        }
        let child = command
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

    fn process_id(&self) -> u32 {
        self.child
            .as_ref()
            .expect("argmin-s3 process should not be inspected after stop")
            .id()
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
    run_runtime_map_ready_with_extra_env(bin, socket_path, &[])
}

fn run_runtime_map_ready_with_extra_env(
    bin: &Path,
    socket_path: &Path,
    extra_env: &[(&str, &str)],
) -> Output {
    let mut command = Command::new(bin);
    command
        .arg("control-plane-runtime-map-ready")
        .arg(socket_path);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command
        .output()
        .expect("runtime-map ready helper should run")
}

fn run_set_pg_acting_set_live(
    bin: &Path,
    socket_path: &Path,
    pg_id: u32,
    acting_set: &[u32],
) -> Output {
    run_set_pg_acting_set_live_with_extra_env(bin, socket_path, pg_id, acting_set, &[])
}

fn run_set_pg_acting_set_live_with_extra_env(
    bin: &Path,
    socket_path: &Path,
    pg_id: u32,
    acting_set: &[u32],
    extra_env: &[(&str, &str)],
) -> Output {
    let mut command = Command::new(bin);
    command
        .arg("control-plane-set-pg-acting-set-live")
        .arg(socket_path)
        .arg(pg_id.to_string());
    for node_id in acting_set {
        command.arg(node_id.to_string());
    }
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command
        .output()
        .expect("set-pg-acting-set live helper should run")
}

fn run_transfer_raft_leadership_with_extra_env(
    bin: &Path,
    socket_path: &Path,
    node_id: u64,
    extra_env: &[(&str, &str)],
) -> Output {
    let mut command = Command::new(bin);
    command
        .arg("control-plane-transfer-raft-leadership")
        .arg(socket_path)
        .arg(node_id.to_string());
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command
        .output()
        .expect("transfer Raft leadership helper should run")
}

fn run_authority_clock_admin_with_extra_env(
    bin: &Path,
    socket_path: &Path,
    command_name: &str,
    extra_env: &[(&str, &str)],
) -> Output {
    let mut command = Command::new(bin);
    command.arg(command_name).arg(socket_path);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command
        .output()
        .expect("authority-clock admin helper should run")
}

fn run_trigger_raft_snapshot_purge_with_extra_env(
    bin: &Path,
    socket_path: &Path,
    extra_env: &[(&str, &str)],
) -> Output {
    let mut command = Command::new(bin);
    command
        .arg("control-plane-trigger-raft-snapshot-purge")
        .arg(socket_path);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command
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
    wait_for_runtime_map_ready_with_extra_env(bin, test_dir, children, raft_node_ids, &[])
}

fn wait_for_runtime_map_ready_with_extra_env(
    bin: &Path,
    test_dir: &Path,
    children: &mut [&mut ChildGuard],
    raft_node_ids: &[u64],
    extra_env: &[(&str, &str)],
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
            let output = run_runtime_map_ready_with_extra_env(bin, socket, extra_env);
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

fn send_authenticated_storage_heartbeat(
    socket_path: &Path,
    auth: &ProcessTestControlPlaneAuth,
    node_id: u32,
    incarnation: u64,
    endpoint: &Path,
    observed_epoch: ClusterEpoch,
) -> storage::control_plane::ControlPlaneHeartbeatRefresh {
    let credential = auth.storage_node_credential(node_id, incarnation);
    let mut client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(socket_path),
        credential,
    );
    client
        .refresh_node_heartbeat(
            NodeHeartbeat {
                node_id: NodeId::new(node_id),
                node_incarnation: incarnation,
                endpoint: endpoint.display().to_string(),
                observed_epoch,
                requested_lease_duration_ms: 10_000,
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            storage::clock::current_time_millis(),
        )
        .expect("authenticated storage-node heartbeat should refresh")
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

fn wait_for_runtime_map_clock_reestablishment_required(
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
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !output.status.success() && stderr.contains("not current local leadership term") {
            return output;
        }
        if Instant::now() >= deadline {
            let last_failure = format_admin_failure(output.status, &output);
            panic!(
                "control-plane runtime map did not fail closed for an unestablished leadership clock on {}: {last_failure}\n{}",
                socket.display(),
                process_logs(test_dir)
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_authority_clock_leadership_fence(
    bin: &Path,
    socket: &Path,
    test_dir: &Path,
    extra_env: &[(&str, &str)],
    children: &mut [&mut ChildGuard],
) -> Output {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for child in children.iter_mut() {
            child.assert_running();
        }
        let output = run_authority_clock_admin_with_extra_env(
            bin,
            socket,
            "control-plane-authority-clock-status",
            extra_env,
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success()
            && stdout.contains("established=false")
            && stdout.contains("blocked_reason=Some(RaftLeadershipChanged)")
            && stdout.contains("local_raft_authority_serving=true")
        {
            return output;
        }
        if Instant::now() >= deadline {
            let last_failure = format_admin_failure(output.status, &output);
            panic!(
                "authority-clock status did not observe the new serving leadership term on {}: {last_failure}\n{}",
                socket.display(),
                process_logs(test_dir)
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_socket_file(path: &Path, child: &mut ChildGuard) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        child.assert_running();
        if path.exists() {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "socket file {} was not created\n{}",
                path.display(),
                process_logs(&child.test_dir)
            );
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
    let state_machine = follower_artifact_state_machine_with_wal_replay(path)?;
    let snapshot = state_machine.inner().snapshot();
    let node_ids: BTreeSet<_> = snapshot.nodes().map(|node| node.node_id()).collect();
    let pg_ids: BTreeSet<_> = snapshot.pgs().map(|pg| pg.pg_id()).collect();
    Ok(node_ids == BTreeSet::from([NodeId::new(0), NodeId::new(1)])
        && pg_ids == BTreeSet::from([PgId::new(0)]))
}

fn follower_artifact_state_machine_with_wal_replay(
    path: &Path,
) -> Result<storage::control_plane_raft::ControlPlaneRaftStateMachine, String> {
    let artifact = match ControlPlaneRaftRestartArtifact::load_durable_artifact(path) {
        Ok(artifact) => artifact,
        Err(error) => return Err(error.to_string()),
    };
    let wal = raft_wal_file(
        durable_artifact_wal_path(path),
        artifact.cluster_name(),
        artifact.local_node_id(),
    );
    let (mut log_store, mut state_machine) = artifact
        .restore_with_wal_file(wal)
        .map_err(|error| error.to_string())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let committed = runtime
        .block_on(RaftLogStorage::read_committed(&mut log_store))
        .map_err(|error| error.to_string())?;
    let Some(committed) = committed else {
        return Ok(state_machine);
    };
    let start = state_machine
        .last_applied()
        .map_or(0, |log_id| log_id.index().saturating_add(1));
    if start > committed.index() {
        return Ok(state_machine);
    }

    let entries = runtime
        .block_on(RaftLogReader::try_get_log_entries(
            &mut log_store,
            start..committed.index().saturating_add(1),
        ))
        .map_err(|error| error.to_string())?;
    for entry in entries {
        state_machine
            .apply_entry(entry)
            .map_err(|error| error.to_string())?;
    }
    Ok(state_machine)
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
    let wal = raft_wal_file(
        durable_artifact_wal_path(path),
        artifact.cluster_name(),
        artifact.local_node_id(),
    );
    let (log_store, _state_machine) = artifact
        .restore_with_wal_file(wal)
        .map_err(|error| error.to_string())?;
    let vote = log_store
        .persisted_vote()
        .map_err(|error| error.to_string())?;
    Ok(vote.map(persisted_vote_summary))
}

fn artifact_only_persisted_vote(path: &Path) -> Result<Option<PersistedVoteSummary>, String> {
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
        .restore_with_wal_file(raft_wal_file(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PersistedLogStateSummary {
    last_log_id: Option<LogId<ControlPlaneRaftLeaderId>>,
    committed: Option<LogId<ControlPlaneRaftLeaderId>>,
}

fn artifact_log_state_with_wal(
    path: &Path,
    cluster_name: &str,
    node_id: u64,
) -> Result<PersistedLogStateSummary, String> {
    let artifact = match ControlPlaneRaftRestartArtifact::load_durable_artifact(path) {
        Ok(artifact) => artifact,
        Err(error) => return Err(error.to_string()),
    };
    let (mut log_store, _state_machine) = artifact
        .restore_with_wal_file(raft_wal_file(
            durable_artifact_wal_path(path),
            cluster_name,
            node_id,
        ))
        .map_err(|error| error.to_string())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let log_state = runtime
        .block_on(RaftLogStorage::get_log_state(&mut log_store))
        .map_err(|error| error.to_string())?;
    let committed = runtime
        .block_on(RaftLogStorage::read_committed(&mut log_store))
        .map_err(|error| error.to_string())?;
    Ok(PersistedLogStateSummary {
        last_log_id: log_state.last_log_id,
        committed,
    })
}

fn artifact_only_log_state(path: &Path) -> Result<PersistedLogStateSummary, String> {
    let artifact = match ControlPlaneRaftRestartArtifact::load_durable_artifact(path) {
        Ok(artifact) => artifact,
        Err(error) => return Err(error.to_string()),
    };
    let (mut log_store, _state_machine) = artifact.restore().map_err(|error| error.to_string())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let log_state = runtime
        .block_on(RaftLogStorage::get_log_state(&mut log_store))
        .map_err(|error| error.to_string())?;
    let committed = runtime
        .block_on(RaftLogStorage::read_committed(&mut log_store))
        .map_err(|error| error.to_string())?;
    Ok(PersistedLogStateSummary {
        last_log_id: log_state.last_log_id,
        committed,
    })
}

fn follower_artifact_pg_has_acting_set(
    path: &Path,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<bool, String> {
    let state_machine = follower_artifact_state_machine_with_wal_replay(path)?;
    let snapshot = state_machine.inner().snapshot();
    let Some(pg) = snapshot.pg(pg_id) else {
        return Ok(false);
    };
    Ok(pg.acting_set() == acting_set)
}

fn artifact_only_pg_has_acting_set(
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
    let state_machine = follower_artifact_state_machine_with_wal_replay(path)?;
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

fn wait_for_new_committed_leader(
    test_dir: &Path,
    possible_leader_ids: &[u64],
    min_term: u64,
    children: &mut [&mut ChildGuard],
) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_error = String::new();
    loop {
        for child in children.iter_mut() {
            child.assert_running();
        }
        for observer_id in possible_leader_ids {
            match artifact_persisted_vote(&state_path(test_dir, *observer_id)) {
                Ok(Some(vote))
                    if vote.committed
                        && vote.term >= min_term
                        && possible_leader_ids.contains(&vote.node_id) =>
                {
                    return vote.node_id;
                }
                Ok(Some(vote)) => {
                    last_error = format!(
                        "node {observer_id} artifact has committed={} leader={} term={}",
                        vote.committed, vote.node_id, vote.term
                    );
                }
                Ok(None) => {
                    last_error = format!("node {observer_id} artifact has no persisted vote");
                }
                Err(error) => {
                    last_error = format!("node {observer_id} artifact restore failed: {error}");
                }
            }
        }
        if Instant::now() >= deadline {
            panic!(
                "no surviving node checkpointed a committed leader vote at term >= {min_term}: {last_error}\n{}",
                process_logs(test_dir)
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

fn wait_for_child_stderr_log_contains(child: &mut ChildGuard, needle: &str) {
    let log_path = child
        .test_dir
        .join(format!("node-{}.stderr.log", child.node_id));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        child.assert_running();
        let log = fs::read_to_string(&log_path)
            .unwrap_or_else(|error| format!("<failed to read {}: {error}>", log_path.display()));
        if log.contains(needle) {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "process log {} did not contain {needle:?}\n{}\n{}",
                log_path.display(),
                log,
                process_logs(&child.test_dir)
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn control_socket(test_dir: &Path, node_id: u64) -> PathBuf {
    test_dir.join(format!("control-{node_id}.sock"))
}

fn state_dir(test_dir: &Path, node_id: u64) -> PathBuf {
    test_dir.join(format!("state-{node_id}"))
}

fn peer_socket(test_dir: &Path, node_id: u64) -> PathBuf {
    test_dir.join(format!("raft-{node_id}.sock"))
}

fn state_path(test_dir: &Path, node_id: u64) -> PathBuf {
    state_dir(test_dir, node_id).join("control.state")
}

fn state_tmp_path_for_process(state_path: &Path, process_id: u32) -> PathBuf {
    let file_name = state_path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .expect("test state path should have a UTF-8 file name");
    state_path.with_file_name(format!("{file_name}.tmp.{process_id}"))
}

fn wal_path(test_dir: &Path, node_id: u64) -> PathBuf {
    durable_artifact_wal_path(&state_path(test_dir, node_id))
}

fn sign_process_test_raft_peer_frame(
    cluster_name: &str,
    source_node_id: u64,
    target_node_id: u64,
    operation: ControlPlaneAuthOperation,
    payload: Vec<u8>,
) -> Vec<u8> {
    ProcessTestControlPlaneAuth::new(cluster_name).sign_raft_peer_frame(
        source_node_id,
        target_node_id,
        operation,
        payload,
    )
}

fn raft_wal_file(
    path: PathBuf,
    cluster_name: impl Into<String>,
    node_id: u64,
) -> ControlPlaneRaftWalFile {
    ControlPlaneRaftWalFile::new(ControlPlaneRaftWalFileConfig {
        path,
        cluster_name: cluster_name.into(),
        local_node_id: node_id,
    })
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
    let auth = ProcessTestControlPlaneAuth::new(&cluster_name);
    let frontend_auth_credentials = auth.frontend_credentials_env(&["runtime-map-ready"]);
    let admin_auth_credentials = auth.admin_credentials_env(&["server-admin"]);
    let server_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            cluster_name.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
            frontend_auth_credentials.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
            admin_auth_credentials.as_str(),
        ),
    ];
    let helper_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            cluster_name.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
            "runtime-map-ready",
        ),
        (
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
            frontend_auth_credentials.as_str(),
        ),
    ];
    let mut node102 = ChildGuard::spawn_with_extra_env(
        &bin,
        test_dir.path(),
        &cluster_name,
        102,
        &raft_node_ids,
        &server_auth_env,
    );
    wait_for_socket_file(&peer_socket(test_dir.path(), 102), &mut node102);
    let mut node101 = ChildGuard::spawn_with_extra_env(
        &bin,
        test_dir.path(),
        &cluster_name,
        101,
        &raft_node_ids,
        &server_auth_env,
    );

    let (leader_socket, output) = wait_for_runtime_map_ready_with_extra_env(
        &bin,
        test_dir.path(),
        &mut [&mut node101, &mut node102],
        &raft_node_ids,
        &helper_auth_env,
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
fn experimental_raft_full_auth_composition_smoke() {
    let bin = argmin_s3_bin();
    let test_dir = TestDir::new("experimental-raft-full-auth-composition");
    let cluster_name = format!(
        "process-full-auth-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos()
    );
    let raft_node_ids = [201, 202];
    let auth = ProcessTestControlPlaneAuth::new(&cluster_name);
    let storage_auth_credentials = auth.storage_node_credentials_env(&[1]);
    let frontend_auth_credentials = auth.frontend_credentials_env(&["runtime-map-ready"]);
    let admin_auth_credentials = auth.admin_credentials_env(&["server-admin"]);
    let server_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            cluster_name.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS",
            storage_auth_credentials.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
            frontend_auth_credentials.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
            admin_auth_credentials.as_str(),
        ),
    ];
    let frontend_helper_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            cluster_name.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
            "runtime-map-ready",
        ),
        (
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
            frontend_auth_credentials.as_str(),
        ),
    ];
    let admin_helper_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            cluster_name.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID",
            "server-admin",
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
            admin_auth_credentials.as_str(),
        ),
    ];
    let mut node202 = ChildGuard::spawn_with_extra_env(
        &bin,
        test_dir.path(),
        &cluster_name,
        202,
        &raft_node_ids,
        &server_auth_env,
    );
    wait_for_socket_file(&peer_socket(test_dir.path(), 202), &mut node202);
    let mut node201 = ChildGuard::spawn_with_extra_env(
        &bin,
        test_dir.path(),
        &cluster_name,
        201,
        &raft_node_ids,
        &server_auth_env,
    );

    let (leader_socket, ready_output) = wait_for_runtime_map_ready_with_extra_env(
        &bin,
        test_dir.path(),
        &mut [&mut node201, &mut node202],
        &raft_node_ids,
        &frontend_helper_auth_env,
    );
    assert!(
        ready_output.status.success(),
        "frontend-authenticated runtime-map ready helper failed: {}",
        format_admin_failure(ready_output.status, &ready_output)
    );
    let ready_stdout = String::from_utf8_lossy(&ready_output.stdout);
    let observed_epoch = ready_stdout
        .split_whitespace()
        .next()
        .and_then(|epoch| epoch.parse::<u64>().ok())
        .and_then(ClusterEpoch::new)
        .unwrap_or_else(|| {
            panic!("ready helper should report cluster epoch in stdout: {ready_stdout}")
        });

    let node_endpoint = test_dir.path().join("storage-node-1.sock");
    let heartbeat_refresh = send_authenticated_storage_heartbeat(
        &leader_socket,
        &auth,
        1,
        1,
        &node_endpoint,
        observed_epoch,
    );
    assert_eq!(heartbeat_refresh.lease().node_id(), NodeId::new(1));
    assert!(
        heartbeat_refresh.lease().lease_deadline_ms() > storage::clock::current_time_millis(),
        "authenticated heartbeat should renew the node lease"
    );
    assert!(
        heartbeat_refresh.runtime_map().nodes().iter().any(|node| {
            node.node_id() == NodeId::new(1)
                && node.node_incarnation() == 1
                && node.endpoint() == node_endpoint.display().to_string()
        }),
        "authenticated heartbeat response should include node 1 endpoint"
    );

    let admin_output = run_set_pg_acting_set_live_with_extra_env(
        &bin,
        &leader_socket,
        0,
        &[1],
        &admin_helper_auth_env,
    );
    assert!(
        admin_output.status.success(),
        "admin-authenticated acting-set helper failed: {}",
        format_admin_failure(admin_output.status, &admin_output)
    );

    let follower_state = if leader_socket.ends_with("control-201.sock") {
        state_path(test_dir.path(), 202)
    } else {
        state_path(test_dir.path(), 201)
    };
    wait_for_follower_artifact_pg_acting_set(
        &follower_state,
        PgId::new(0),
        &[NodeId::new(1)],
        &mut [&mut node201, &mut node202],
    );

    let logs = process_logs(test_dir.path());
    assert!(
        logs.contains("control_plane_unix_auth required=true storage_node_heartbeat_required=true frontend_runtime_map_required=true admin_control_plane_required=true"),
        "Unix control-plane auth diagnostics should show all Unix auth gates enabled:\n{logs}"
    );
    assert!(
        logs.contains(
            "storage_node_credentials=1 frontend_credentials=1 admin_credentials=1"
        ),
        "Unix control-plane auth diagnostics should show all configured credential classes:\n{logs}"
    );
    assert!(
        logs.contains("raft_peer_auth required=true"),
        "Raft peer auth diagnostics should show peer auth enabled:\n{logs}"
    );
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

    let artifact_only_vote = artifact_only_persisted_vote(&state_path)
        .expect("artifact should restore before WAL suffix injection")
        .expect("artifact should contain the checkpointed vote");
    let injected_term = vote_before_restart.term + 1_000;
    let wal = raft_wal_file(wal_path(test_dir.path(), 101), &cluster_name, 101);
    wal.append_record(&ControlPlaneRaftWalRecord::SaveVote(Vote::<
        ControlPlaneRaftLeaderId,
    >::new_committed(
        injected_term, 101
    )))
    .expect("test should append a post-checkpoint WAL vote record");

    assert_eq!(
        artifact_only_persisted_vote(&state_path)
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
fn experimental_raft_process_peer_wal_ack_then_checkpoint_failure_recovers_log_state() {
    let bin = argmin_s3_bin();
    let test_dir = TestDir::new("experimental-raft-process-peer-append-wal-crash");
    let cluster_name = format!(
        "process-peer-append-wal-crash-{}-{}",
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
    let follower_state_path = state_path(test_dir.path(), 103);
    wait_for_follower_artifact(
        &follower_state_path,
        &mut [&mut node101, &mut node102, &mut node103],
    );

    node103.stop();
    let follower_peer_socket = peer_socket(test_dir.path(), 103);
    let _ = fs::remove_file(&follower_peer_socket);
    let mut restarted103 =
        ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 103, &raft_node_ids);
    wait_for_socket_file(&follower_peer_socket, &mut restarted103);
    wait_for_child_stderr_log_contains(
        &mut restarted103,
        "argmin-s3 experimental durable OpenRaft control-plane manager using state",
    );
    let follower_log_before_crash =
        artifact_log_state_with_wal(&follower_state_path, &cluster_name, 103)
            .expect("follower artifact plus WAL should expose log state before crash");
    assert!(
        follower_log_before_crash.last_log_id.is_some()
            || follower_log_before_crash.committed.is_some(),
        "bootstrapped follower should expose retained log state before crash: {follower_log_before_crash:?}"
    );
    let follower_vote_before_crash =
        artifact_persisted_vote_with_wal(&follower_state_path, &cluster_name, 103)
            .expect("follower artifact plus WAL should expose vote before crash")
            .expect("bootstrapped follower should persist a vote before crash");
    assert!(
        follower_vote_before_crash.committed,
        "bootstrapped follower should persist a committed vote before direct append: {follower_vote_before_crash:?}"
    );
    let preparation_prev_log_id = follower_log_before_crash
        .last_log_id
        .expect("bootstrapped follower should have a log tip before append");
    let append_vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(
        follower_vote_before_crash
            .term
            .checked_add(1_000)
            .expect("synthetic leader vote term should advance past local elections"),
        101,
    );
    let preparation_request = AppendEntriesRequest {
        vote: append_vote,
        prev_log_id: Some(preparation_prev_log_id),
        entries: Vec::new(),
        leader_commit: follower_log_before_crash.committed,
    };
    let preparation_frame = ControlPlaneRaftPeerRpcRequest::AppendEntries(preparation_request)
        .encode_frame_for_peer(&ControlPlaneRaftPeerFrameIdentity::new(
            cluster_name.clone(),
            101,
            103,
        ))
        .expect("synthetic leader preparation should encode");
    let preparation_frame = sign_process_test_raft_peer_frame(
        &cluster_name,
        101,
        103,
        ControlPlaneAuthOperation::RaftAppendEntries,
        preparation_frame,
    );
    let mut preparation_stream =
        UnixStream::connect(&follower_peer_socket).expect("follower peer socket should connect");
    write_control_plane_raft_peer_transport_frame(&mut preparation_stream, &preparation_frame)
        .expect("synthetic leader preparation should be written");
    read_control_plane_raft_peer_transport_frame(
        &mut preparation_stream,
        ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
    )
    .expect("synthetic leader preparation should be acknowledged");

    let follower_log_before_crash =
        artifact_log_state_with_wal(&follower_state_path, &cluster_name, 103)
            .expect("prepared follower artifact plus WAL should expose current log state");
    let mut prev_log_id = follower_log_before_crash
        .last_log_id
        .expect("prepared follower should have a log tip before append");
    node101.stop();
    node102.stop();

    let padded_endpoint = "x".repeat(120 * 1024);
    let send_padded_append_batch = |prev_log_id: ControlPlaneRaftLogId| {
        let mut entries = Vec::with_capacity(64);
        let mut appended_log_id = prev_log_id;
        for _ in 0..64 {
            appended_log_id = LogId::new(
                append_vote.leader_id,
                appended_log_id
                    .index()
                    .checked_add(1)
                    .expect("test log index should advance"),
            );
            entries.push(Entry {
                log_id: appended_log_id,
                payload: EntryPayload::Normal(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), padded_endpoint.clone())],
                    pg_ids: vec![PgId::new(0)],
                }),
            });
        }
        let append_request = AppendEntriesRequest {
            vote: append_vote,
            prev_log_id: Some(prev_log_id),
            entries,
            leader_commit: follower_log_before_crash.committed,
        };
        let append_frame = ControlPlaneRaftPeerRpcRequest::AppendEntries(append_request)
            .encode_frame_for_peer(&ControlPlaneRaftPeerFrameIdentity::new(
                cluster_name.clone(),
                101,
                103,
            ))
            .expect("padded append request should encode");
        let append_frame = sign_process_test_raft_peer_frame(
            &cluster_name,
            101,
            103,
            ControlPlaneAuthOperation::RaftAppendEntries,
            append_frame,
        );
        let mut peer_stream = UnixStream::connect(&follower_peer_socket)
            .expect("follower peer socket should connect");
        write_control_plane_raft_peer_transport_frame(&mut peer_stream, &append_frame)
            .expect("padded append frame should be written to follower peer socket");
        read_control_plane_raft_peer_transport_frame(
            &mut peer_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        )
        .expect("fsynced padded peer WAL append should be acknowledged");
        appended_log_id
    };

    // Keep the suffix just below the 64 MiB checkpoint threshold while the
    // artifact path is writable, then cross it only after checkpoint writes
    // are blocked. This exercises the resource bound without waiting for the
    // one-minute age bound.
    for _ in 0..8 {
        prev_log_id = send_padded_append_batch(prev_log_id);
    }
    let follower_checkpoint_tmp_path =
        state_tmp_path_for_process(&follower_state_path, restarted103.process_id());
    let _ = fs::remove_file(&follower_checkpoint_tmp_path);
    let _ = fs::remove_dir_all(&follower_checkpoint_tmp_path);
    fs::create_dir(&follower_checkpoint_tmp_path)
        .expect("follower checkpoint temp path should be blocked by a directory");

    let appended_log_id = send_padded_append_batch(prev_log_id);

    let status = wait_for_process_exit(&mut restarted103, Duration::from_secs(5));
    fs::remove_dir(&follower_checkpoint_tmp_path)
        .expect("follower checkpoint temp-path blocker should be removed after crash");
    assert!(
        !status.success(),
        "follower should exit after the acknowledged WAL suffix cannot be checkpointed"
    );
    let follower_artifact_after_crash = artifact_only_log_state(&follower_state_path)
        .expect("follower checkpoint artifact should restore after crash");
    assert_ne!(
        follower_artifact_after_crash.last_log_id,
        Some(appended_log_id),
        "failed checkpoint must leave the synthetic peer append out of the checkpoint artifact: {follower_artifact_after_crash:?}"
    );
    let follower_log_after_crash =
        artifact_log_state_with_wal(&follower_state_path, &cluster_name, 103)
            .expect("artifact plus WAL should restore after peer append crash");
    let follower_vote_after_crash =
        artifact_persisted_vote_with_wal(&follower_state_path, &cluster_name, 103)
            .expect("artifact plus WAL should restore vote after peer append crash");
    assert_eq!(
        follower_log_after_crash.last_log_id,
        Some(appended_log_id),
        "artifact plus WAL must recover the acknowledged and fsynced follower append; before={follower_log_before_crash:?} after={follower_log_after_crash:?} vote_after={follower_vote_after_crash:?}"
    );

    let mut recovered103 =
        ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 103, &raft_node_ids);
    wait_for_socket_file(&follower_peer_socket, &mut recovered103);
    wait_for_child_stderr_log_contains(
        &mut recovered103,
        "argmin-s3 experimental durable OpenRaft control-plane manager using state",
    );
    assert_eq!(
        artifact_log_state_with_wal(&follower_state_path, &cluster_name, 103)
            .expect("recovered follower artifact plus WAL should expose log state")
            .last_log_id,
        Some(appended_log_id),
        "recovered follower should retain the WAL-restored acknowledged append"
    );
}

#[test]
fn experimental_raft_process_restart_replays_post_checkpoint_wal_command_suffix() {
    let bin = argmin_s3_bin();
    let test_dir = TestDir::new("experimental-raft-process-wal-command-suffix");
    let cluster_name = format!(
        "process-wal-command-suffix-{}-{}",
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
        .expect("artifact plus WAL should restore before WAL command suffix injection")
        .expect("bootstrapped process should persist a vote");
    let log_state_before_restart = artifact_log_state_with_wal(&state_path, &cluster_name, 101)
        .expect("artifact plus WAL should expose log state before suffix injection");
    let last_log_id = log_state_before_restart
        .last_log_id
        .expect("bootstrapped process should have a retained log tip");

    node101.stop();

    assert!(
        !follower_artifact_pg_has_acting_set(&state_path, PgId::new(0), &[NodeId::new(1)])
            .expect("artifact should restore before WAL command suffix injection"),
        "checkpoint artifact should not already contain the injected acting-set change"
    );

    let command_term = vote_before_restart.term + 1_000;
    let command_log_id = LogId::new(
        LeaderId {
            term: command_term,
            node_id: 101,
        },
        last_log_id.index() + 1,
    );
    let command_entry: ControlPlaneRaftEntry = Entry {
        log_id: command_log_id,
        payload: EntryPayload::Normal(ControlPlaneCommand::SetPgActingSet {
            pg_id: PgId::new(0),
            acting_set: vec![NodeId::new(1)],
        }),
    };
    let wal = raft_wal_file(wal_path(test_dir.path(), 101), &cluster_name, 101);
    wal.append_record(&ControlPlaneRaftWalRecord::SaveVote(Vote::<
        ControlPlaneRaftLeaderId,
    >::new_committed(
        command_term, 101
    )))
    .expect("test should append a post-checkpoint WAL vote record");
    wal.append_record(&ControlPlaneRaftWalRecord::Append(vec![command_entry]))
        .expect("test should append a post-checkpoint WAL command entry");
    wal.append_record(&ControlPlaneRaftWalRecord::SaveCommitted(Some(
        command_log_id,
    )))
    .expect("test should append a post-checkpoint WAL committed watermark");

    assert!(
        !artifact_only_pg_has_acting_set(&state_path, PgId::new(0), &[NodeId::new(1)])
            .expect("artifact should still restore after WAL command suffix injection"),
        "WAL suffix injection must not rewrite the checkpoint artifact"
    );
    assert_eq!(
        artifact_log_state_with_wal(&state_path, &cluster_name, 101)
            .expect("artifact plus WAL should restore log state after command suffix injection")
            .committed,
        Some(command_log_id),
        "artifact plus WAL restore should observe the injected committed command suffix"
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
    wait_for_follower_artifact_pg_acting_set(
        &state_path,
        PgId::new(0),
        &[NodeId::new(1)],
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
fn experimental_raft_transferred_process_leader_requires_explicit_clock_reestablishment() {
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
    let auth = ProcessTestControlPlaneAuth::new(&cluster_name);
    let admin_instance_id = "transfer-admin";
    let admin_credentials = auth.admin_credentials_env(&[admin_instance_id]);
    let server_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            cluster_name.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
            admin_credentials.as_str(),
        ),
    ];
    let admin_helper_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            cluster_name.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID",
            admin_instance_id,
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
            admin_credentials.as_str(),
        ),
    ];
    let mut node102 = ChildGuard::spawn_with_extra_env(
        &bin,
        test_dir.path(),
        &cluster_name,
        102,
        &raft_node_ids,
        &server_auth_env,
    );
    wait_for_socket_file(&peer_socket(test_dir.path(), 102), &mut node102);
    let mut node103 = ChildGuard::spawn_with_extra_env(
        &bin,
        test_dir.path(),
        &cluster_name,
        103,
        &raft_node_ids,
        &server_auth_env,
    );
    wait_for_socket_file(&peer_socket(test_dir.path(), 103), &mut node103);
    let mut node101 = ChildGuard::spawn_with_extra_env(
        &bin,
        test_dir.path(),
        &cluster_name,
        101,
        &raft_node_ids,
        &server_auth_env,
    );

    let old_leader_socket = control_socket(test_dir.path(), 101);
    wait_for_runtime_map_ready_on(
        &bin,
        &old_leader_socket,
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut node103],
    );

    let transfer = run_transfer_raft_leadership_with_extra_env(
        &bin,
        &old_leader_socket,
        102,
        &admin_helper_auth_env,
    );
    assert!(
        transfer.status.success(),
        "leadership transfer failed: {}\n{}",
        format_admin_failure(transfer.status, &transfer),
        process_logs(test_dir.path())
    );

    wait_for_artifact_committed_vote_at_least(
        &state_path(test_dir.path(), 102),
        102,
        2,
        &mut [&mut node101, &mut node102, &mut node103],
    );
    let new_leader_socket = control_socket(test_dir.path(), 102);
    let status = wait_for_authority_clock_leadership_fence(
        &bin,
        &new_leader_socket,
        test_dir.path(),
        &admin_helper_auth_env,
        &mut [&mut node101, &mut node102, &mut node103],
    );
    let status_stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        status_stdout.contains("established=false")
            && status_stdout.contains("blocked_reason=Some(RaftLeadershipChanged)")
            && !status_stdout.contains("current_raft_term=-"),
        "authority-clock status should expose the leadership fence: {status_stdout}"
    );

    let recovery = run_authority_clock_admin_with_extra_env(
        &bin,
        &new_leader_socket,
        "control-plane-reestablish-authority-clock",
        &admin_helper_auth_env,
    );
    assert!(
        recovery.status.success(),
        "authority-clock re-establishment failed: {}\n{}",
        format_admin_failure(recovery.status, &recovery),
        process_logs(test_dir.path())
    );
    let recovery_stdout = String::from_utf8_lossy(&recovery.stdout);
    assert!(
        recovery_stdout.contains("established=true")
            && !recovery_stdout.contains("current_raft_term=-"),
        "authority-clock recovery should expose the new binding: {recovery_stdout}"
    );
    wait_for_runtime_map_ready_on(
        &bin,
        &new_leader_socket,
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut node103],
    );

    node101.stop();
    wait_for_runtime_map_ready_on(
        &bin,
        &new_leader_socket,
        test_dir.path(),
        &mut [&mut node102, &mut node103],
    );
}

#[test]
fn experimental_raft_triggered_process_election_requires_clock_reestablishment() {
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
    let candidate_id = wait_for_new_committed_leader(
        test_dir.path(),
        &surviving_node_ids,
        2,
        &mut [&mut node102, &mut node103],
    );
    let candidate_socket = control_socket(test_dir.path(), candidate_id);
    wait_for_trigger_raft_election(
        &bin,
        &candidate_socket,
        test_dir.path(),
        &mut [&mut node102, &mut node103],
    );
    wait_for_artifact_committed_vote_at_least(
        &state_path(test_dir.path(), candidate_id),
        candidate_id,
        2,
        &mut [&mut node102, &mut node103],
    );

    wait_for_runtime_map_clock_reestablishment_required(
        &bin,
        &candidate_socket,
        test_dir.path(),
        &mut [&mut node102, &mut node103],
    );
}

#[test]
fn experimental_raft_process_natural_election_requires_clock_reestablishment() {
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
    let new_leader_id = wait_for_new_committed_leader(
        test_dir.path(),
        &surviving_node_ids,
        2,
        &mut [&mut node102, &mut node103],
    );
    let new_leader_socket = control_socket(test_dir.path(), new_leader_id);
    wait_for_runtime_map_clock_reestablishment_required(
        &bin,
        &new_leader_socket,
        test_dir.path(),
        &mut [&mut node102, &mut node103],
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
    let auth = ProcessTestControlPlaneAuth::new(&cluster_name);
    let admin_instance_id = "snapshot-catchup-admin";
    let admin_credentials = auth.admin_credentials_env(&[admin_instance_id]);
    let server_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            cluster_name.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
            admin_credentials.as_str(),
        ),
    ];
    let admin_helper_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            cluster_name.as_str(),
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID",
            admin_instance_id,
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
            admin_credentials.as_str(),
        ),
    ];
    let mut node102 = ChildGuard::spawn_with_extra_env(
        &bin,
        test_dir.path(),
        &cluster_name,
        102,
        &raft_node_ids,
        &server_auth_env,
    );
    wait_for_socket_file(&peer_socket(test_dir.path(), 102), &mut node102);
    let mut node103 = ChildGuard::spawn_with_extra_env(
        &bin,
        test_dir.path(),
        &cluster_name,
        103,
        &raft_node_ids,
        &server_auth_env,
    );
    wait_for_socket_file(&peer_socket(test_dir.path(), 103), &mut node103);
    let mut node101 = ChildGuard::spawn_with_extra_env(
        &bin,
        test_dir.path(),
        &cluster_name,
        101,
        &raft_node_ids,
        &server_auth_env,
    );
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
    let snapshot_covered_write = run_set_pg_acting_set_live_with_extra_env(
        &bin,
        &leader_control_socket,
        0,
        &[1],
        &admin_helper_auth_env,
    );
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

    let snapshot_purge = run_trigger_raft_snapshot_purge_with_extra_env(
        &bin,
        &leader_control_socket,
        &admin_helper_auth_env,
    );
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

    let mut restarted103 = ChildGuard::spawn_with_extra_env(
        &bin,
        test_dir.path(),
        &cluster_name,
        103,
        &raft_node_ids,
        &server_auth_env,
    );
    wait_for_socket_file(&peer_socket(test_dir.path(), 103), &mut restarted103);

    let clock_recovery = run_authority_clock_admin_with_extra_env(
        &bin,
        &leader_control_socket,
        "control-plane-reestablish-authority-clock",
        &admin_helper_auth_env,
    );
    assert!(
        clock_recovery.status.success(),
        "leader clock re-establishment failed: {}\n{}",
        format_admin_failure(clock_recovery.status, &clock_recovery),
        process_logs(test_dir.path())
    );

    let suffix_write = run_set_pg_acting_set_live_with_extra_env(
        &bin,
        &leader_control_socket,
        0,
        &[0],
        &admin_helper_auth_env,
    );
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
