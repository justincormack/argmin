// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[cfg(target_os = "linux")]
use base64::Engine as _;
use std::collections::BTreeSet;
#[cfg(target_os = "linux")]
use std::fmt::Write as _;
use std::fs::{self, File};
#[cfg(target_os = "linux")]
use std::net::{TcpListener, TcpStream};
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use storage::control_plane::{
    AuthenticatedUnixControlPlaneClient, ControlPlaneHeartbeatRuntimeMapSource, NodeHeartbeat,
    UnixControlPlaneClient,
};
use storage::control_plane_auth::{
    ControlPlaneAuthPrincipal, ControlPlaneScopedCredential, ControlPlaneScopedCredentialInput,
};
use storage::control_plane_command::ControlPlaneCommand;
use storage::control_plane_raft::{
    inspect_control_plane_raft_checkpoint_state_for_test,
    inspect_control_plane_raft_recovery_state_for_test,
    ControlPlaneRaftCheckpointWriteBlockerForTest, ControlPlaneRaftLogId,
    ControlPlaneRaftPeerTestClient, ControlPlaneRaftPeerTransportLimits,
};
use storage::{ClusterEpoch, ControlPlaneRaftPeerAuthCredentialInput, NodeId, PgId};

const DEFAULT_CONTROL_PLANE_AUTH_CLUSTER: &str = "process-test-control-plane-auth";
const DEFAULT_FRONTEND_INSTANCE_ID: &str = "runtime-map-ready";
const DEFAULT_ADMIN_INSTANCE_ID: &str = "server-admin";

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(_name: &str) -> Self {
        Self::new_under(&std::env::temp_dir())
    }

    fn new_under(parent: &Path) -> Self {
        static NEXT_TEST_DIR_ID: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos();
        let id = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!("a3rt-{}-{id}-{now:x}", std::process::id()));
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

    fn raft_peer_auth_credentials() -> Vec<ControlPlaneRaftPeerAuthCredentialInput> {
        [101, 102, 103]
            .into_iter()
            .map(|node_id| {
                ControlPlaneRaftPeerAuthCredentialInput::new(
                    node_id,
                    Self::raft_peer_credential_id(node_id),
                    1,
                    Self::raft_peer_secret(node_id).into_bytes(),
                )
            })
            .collect()
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
        let auth = ProcessTestControlPlaneAuth::new(DEFAULT_CONTROL_PLANE_AUTH_CLUSTER);
        let peer_auth_credentials = auth.raft_peer_credentials_env(raft_node_id, peer_node_ids);
        let storage_auth_credentials = auth.storage_node_credentials_env(&[0, 1]);
        let frontend_auth_credentials =
            auth.frontend_credentials_env(&[DEFAULT_FRONTEND_INSTANCE_ID]);
        let admin_auth_credentials = auth.admin_credentials_env(&[DEFAULT_ADMIN_INSTANCE_ID]);
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
            .env("ARGMIN_TEST_ENV_SHAPED_CONFIG", "1")
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
            .env(
                "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
                DEFAULT_CONTROL_PLANE_AUTH_CLUSTER,
            )
            .env(
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS",
                storage_auth_credentials,
            )
            .env(
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
                frontend_auth_credentials,
            )
            .env(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
                admin_auth_credentials,
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

    #[cfg(target_os = "linux")]
    fn spawn_static(
        bin: &Path,
        test_dir: &Path,
        manifest_path: &Path,
        process_id: &str,
        raft_node_id: u64,
    ) -> Self {
        let stdout = File::create(test_dir.join(format!("node-{raft_node_id}.stdout.log")))
            .expect("stdout log should be created");
        let stderr = File::create(test_dir.join(format!("node-{raft_node_id}.stderr.log")))
            .expect("stderr log should be created");
        let child = Command::new(bin)
            .env("ARGMIN_CLUSTER_CONFIG_PATH", manifest_path)
            .env("ARGMIN_PROCESS_ID", process_id)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("static argmin-s3 control-plane process should start");
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

#[cfg(target_os = "linux")]
fn run_static_control_plane_command(
    bin: &Path,
    manifest_path: &Path,
    process_id: &str,
    command_name: &str,
    args: &[&str],
) -> Output {
    Command::new(bin)
        .arg(command_name)
        .args(args)
        .env("ARGMIN_CLUSTER_CONFIG_PATH", manifest_path)
        .env("ARGMIN_PROCESS_ID", process_id)
        .output()
        .expect("static control-plane command should run")
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
    configure_default_frontend_command_auth(&mut command);
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
    configure_default_admin_command_auth(&mut command);
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
    configure_default_admin_command_auth(&mut command);
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
    configure_default_admin_command_auth(&mut command);
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
    configure_default_admin_command_auth(&mut command);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command
        .output()
        .expect("trigger Raft snapshot purge helper should run")
}

fn run_trigger_raft_election(bin: &Path, socket_path: &Path) -> Output {
    let mut command = Command::new(bin);
    command
        .arg("control-plane-trigger-raft-election")
        .arg(socket_path);
    configure_default_admin_command_auth(&mut command);
    command
        .output()
        .expect("trigger Raft election helper should run")
}

fn configure_default_frontend_command_auth(command: &mut Command) {
    let auth = ProcessTestControlPlaneAuth::new(DEFAULT_CONTROL_PLANE_AUTH_CLUSTER);
    command
        .env(
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            DEFAULT_CONTROL_PLANE_AUTH_CLUSTER,
        )
        .env(
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
            DEFAULT_FRONTEND_INSTANCE_ID,
        )
        .env(
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
            auth.frontend_credentials_env(&[DEFAULT_FRONTEND_INSTANCE_ID]),
        );
}

fn configure_default_admin_command_auth(command: &mut Command) {
    let auth = ProcessTestControlPlaneAuth::new(DEFAULT_CONTROL_PLANE_AUTH_CLUSTER);
    command
        .env(
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            DEFAULT_CONTROL_PLANE_AUTH_CLUSTER,
        )
        .env(
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID",
            DEFAULT_ADMIN_INSTANCE_ID,
        )
        .env(
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
            auth.admin_credentials_env(&[DEFAULT_ADMIN_INSTANCE_ID]),
        );
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
            NodeHeartbeat::test_fixture(
                NodeId::new(node_id),
                incarnation,
                endpoint.display().to_string(),
                observed_epoch,
                10_000,
                Default::default(),
                Vec::new(),
            ),
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
    let state = inspect_control_plane_raft_recovery_state_for_test(path)
        .map_err(|error| error.to_string())?;
    let snapshot = state.snapshot();
    let node_ids: BTreeSet<_> = snapshot.nodes().map(|node| node.node_id()).collect();
    let pg_ids: BTreeSet<_> = snapshot.pgs().map(|pg| pg.pg_id()).collect();
    Ok(node_ids == BTreeSet::from([NodeId::new(0), NodeId::new(1)])
        && pg_ids == BTreeSet::from([PgId::new(0)]))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PersistedVoteSummary {
    term: u64,
    node_id: u64,
    committed: bool,
}

fn persisted_vote_summary(
    vote: &storage::control_plane_raft::ControlPlaneRaftPersistedVoteForTest,
) -> PersistedVoteSummary {
    PersistedVoteSummary {
        term: vote.term(),
        node_id: vote.node_id(),
        committed: vote.committed(),
    }
}

fn artifact_persisted_vote(path: &Path) -> Result<Option<PersistedVoteSummary>, String> {
    let state = inspect_control_plane_raft_recovery_state_for_test(path)
        .map_err(|error| error.to_string())?;
    Ok(state.persisted_vote().map(persisted_vote_summary))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PersistedLogStateSummary {
    last_log_id: Option<ControlPlaneRaftLogId>,
    committed: Option<ControlPlaneRaftLogId>,
}

fn artifact_log_state(path: &Path) -> Result<PersistedLogStateSummary, String> {
    let state = inspect_control_plane_raft_recovery_state_for_test(path)
        .map_err(|error| error.to_string())?;
    Ok(PersistedLogStateSummary {
        last_log_id: state.last_log_id(),
        committed: state.committed(),
    })
}

fn artifact_only_log_state(path: &Path) -> Result<PersistedLogStateSummary, String> {
    let state = inspect_control_plane_raft_checkpoint_state_for_test(path)
        .map_err(|error| error.to_string())?;
    Ok(PersistedLogStateSummary {
        last_log_id: state.last_log_id(),
        committed: state.committed(),
    })
}

fn follower_artifact_pg_has_acting_set(
    path: &Path,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<bool, String> {
    let state = inspect_control_plane_raft_recovery_state_for_test(path)
        .map_err(|error| error.to_string())?;
    let snapshot = state.snapshot();
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
    let state = inspect_control_plane_raft_recovery_state_for_test(path)
        .map_err(|error| error.to_string())?;
    let snapshot_index = state.cached_snapshot_log_id().map(|log_id| log_id.index());
    let Some(snapshot_index) = snapshot_index else {
        return Ok(false);
    };
    if snapshot_index < min_snapshot_index {
        return Ok(false);
    }
    let snapshot = state.snapshot();
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

#[cfg(target_os = "linux")]
fn static_tcp_process_test_mount() -> Option<PathBuf> {
    // SAFETY: geteuid has no preconditions and does not mutate memory.
    let effective_uid = unsafe { libc::geteuid() };
    let candidates = std::env::var_os("ARGMIN_STATIC_TCP_PROCESS_TEST_MOUNT")
        .map(PathBuf::from)
        .into_iter()
        .chain([
            PathBuf::from(format!("/run/user/{effective_uid}")),
            PathBuf::from("/dev/shm"),
        ]);
    candidates.into_iter().find(|path| {
        let Ok(metadata) = fs::symlink_metadata(path) else {
            return false;
        };
        let Some(parent) = path.parent() else {
            return false;
        };
        let Ok(parent_metadata) = fs::symlink_metadata(parent) else {
            return false;
        };
        let mode = metadata.permissions().mode() & 0o777;
        metadata.is_dir()
            && !metadata.file_type().is_symlink()
            && metadata.uid() == effective_uid
            && mode & 0o022 == 0
            && metadata.dev() != parent_metadata.dev()
    })
}

#[cfg(target_os = "linux")]
fn reserve_loopback_ports(count: usize) -> (Vec<u16>, Vec<Option<TcpListener>>) {
    let ephemeral_range = fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .expect("Linux ephemeral port range should be readable");
    let mut bounds = ephemeral_range.split_ascii_whitespace();
    let ephemeral_start = bounds
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .expect("Linux ephemeral port range start should parse");
    let ephemeral_end = bounds
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .expect("Linux ephemeral port range end should parse");
    assert!(
        bounds.next().is_none() && ephemeral_start <= ephemeral_end,
        "Linux ephemeral port range should contain exactly two ordered ports"
    );

    let mut candidates = (1024..ephemeral_start).collect::<Vec<_>>();
    if ephemeral_end < u16::MAX {
        candidates.extend((ephemeral_end + 1)..=u16::MAX);
    }
    assert!(
        candidates.len() >= count,
        "Linux host must expose at least {count} unprivileged non-ephemeral ports"
    );
    let seed = usize::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .subsec_nanos(),
    )
    .unwrap()
        ^ usize::try_from(std::process::id()).unwrap();
    let candidate_count = candidates.len();
    candidates.rotate_left(seed % candidate_count);

    let mut listeners = Vec::with_capacity(count);
    for port in candidates {
        match TcpListener::bind(("127.0.0.1", port)) {
            Ok(listener) => {
                listeners.push(listener);
                if listeners.len() == count {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::AddrInUse | std::io::ErrorKind::PermissionDenied
                ) => {}
            Err(error) => panic!("test TCP port {port} should reserve: {error}"),
        }
    }
    assert_eq!(
        listeners.len(),
        count,
        "Linux host did not have {count} available non-ephemeral loopback ports"
    );
    let ports = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect();
    (ports, listeners.into_iter().map(Some).collect())
}

#[cfg(target_os = "linux")]
fn release_static_authority_port_reservations(
    reservations: &mut [Option<TcpListener>],
    authority_number: usize,
) {
    assert!((1..=3).contains(&authority_number));
    let index = authority_number - 1;
    for port_index in [index, index + 3, index + 6] {
        drop(
            reservations[port_index]
                .take()
                .expect("authority port reservation should still be held"),
        );
    }
}

#[cfg(target_os = "linux")]
fn wait_for_static_authority_raft_listener(
    ports: &[u16],
    authority_number: usize,
    child: &mut ChildGuard,
) {
    assert!((1..=3).contains(&authority_number));
    let index = authority_number - 1;
    let raft_port = ports[index];
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        child.assert_running();
        if TcpStream::connect_timeout(
            &format!("127.0.0.1:{raft_port}").parse().unwrap(),
            Duration::from_millis(100),
        )
        .is_ok()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "static authority {authority_number} did not bind its Raft TCP listener\n{}",
            process_logs(&child.test_dir)
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(target_os = "linux")]
fn write_static_tcp_process_manifest(test_dir: &Path, ports: &[u16]) -> PathBuf {
    assert_eq!(ports.len(), 12);
    let mount_path = test_dir
        .parent()
        .expect("static TCP test directory should be beneath its mount");
    let material_dir = test_dir.join("material");
    fs::create_dir(&material_dir).unwrap();
    fs::set_permissions(&material_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let testdata = Path::new(env!("CARGO_MANIFEST_DIR")).join("../s3-tests/testdata");
    for (source, target, mode) in [
        ("ca-cert.pem", "cluster-ca.pem", 0o644),
        ("localhost-cert.pem", "localhost.crt", 0o644),
        ("localhost-key.pem", "localhost.key", 0o600),
    ] {
        let target = material_dir.join(target);
        fs::copy(testdata.join(source), &target).unwrap();
        fs::set_permissions(target, fs::Permissions::from_mode(mode)).unwrap();
    }
    for role in ["raft", "storage", "admin"] {
        for number in 1..=3 {
            let path = material_dir.join(format!("{role}-{number}.key"));
            let role_tag: u8 = match role {
                "raft" => 10,
                "storage" => 20,
                "admin" => 30,
                _ => unreachable!(),
            };
            let encoded = base64::engine::general_purpose::STANDARD.encode([role_tag + number; 32]);
            fs::write(&path, encoded).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
    for (name, value) in [
        ("s3-secret-access-key", "process-test-secret"),
        (
            "sse-s3-wrapping-key",
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=\n",
        ),
    ] {
        let path = material_dir.join(name);
        fs::write(&path, value).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    let cluster_id = format!(
        "static-tcp-process-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut manifest = format!(
        r#"schema_version = 1

[s3]
account_id = "123456789012"
access_key_id = "process-test-access"
secret_access_key_ref = "file:{s3_secret}"
sse_s3_wrapping_key_ref = "file:{sse_s3_key}"

[cluster]
id = "{cluster_id}"
topology_generation = 1
region = "us-east-1"

[deployment]
mode = "replicated"
failure_domain = "host"
failure_tolerance = 1

[storage]
pg_count = 1
ec_data_shards = 2
ec_parity_shards = 1
initial_cluster_epoch = 1

[raft]
max_append_entries = 64
max_append_bytes = 8388608
max_snapshot_bytes = 15728640

[[transport_profiles]]
id = "internal"
max_frame_bytes = 16777216
max_connections = 64
connect_timeout_ms = 1000
io_timeout_ms = 15000

[[transport_profiles]]
id = "control"
max_frame_bytes = 8388648
max_connections = 64
connect_timeout_ms = 1000
io_timeout_ms = 15000

[[tls_trust_bundles]]
id = "cluster-ca"
ca_bundle_ref = "file:{ca}"

[[tls_identities]]
id = "cluster-server"
certificate_ref = "file:{cert}"
private_key_ref = "file:{key}"
"#,
        ca = material_dir.join("cluster-ca.pem").display(),
        cert = material_dir.join("localhost.crt").display(),
        key = material_dir.join("localhost.key").display(),
        s3_secret = material_dir.join("s3-secret-access-key").display(),
        sse_s3_key = material_dir.join("sse-s3-wrapping-key").display(),
    );

    for number in 1..=3_u16 {
        let index = usize::from(number - 1);
        let state_path = test_dir.join(format!("state-{number}/control.state"));
        writeln!(
            manifest,
            r#"
[[hosts]]
id = "control-host-{number}"

[[hosts]]
id = "storage-host-{number}"

[[disks]]
id = "control-disk-{number}"
host_id = "control-host-{number}"
mount_path = "{mount_path}"

[[disks]]
id = "storage-disk-{number}"
host_id = "storage-host-{number}"
mount_path = "/srv/argmin-static-storage-{number}"

[[processes]]
id = "control-{number}"
host_id = "control-host-{number}"
kind = "control-plane"
admin_instance_id = "control-{number}-admin"

[[processes]]
id = "storage-{number}"
host_id = "storage-host-{number}"
kind = "storage-node"

[[authorities]]
id = "authority-{number}"
kind = "raft-voter"
raft_node_id = {raft_node_id}
process_id = "control-{number}"
disk_id = "control-disk-{number}"
state_path = "{state_path}"

[[storage_nodes]]
node_id = {number}
process_id = "storage-{number}"
disk_id = "storage-disk-{number}"
data_dir = "/srv/argmin-static-storage-{number}/node"
"#,
            raft_node_id = 100 + u64::from(number),
            mount_path = mount_path.display(),
            state_path = state_path.display(),
        )
        .unwrap();

        for (protocol, name, port, profile) in [
            ("raft-peer", "raft", ports[index], "internal"),
            ("control-plane", "control", ports[index + 3], "control"),
            (
                "authority-clock-recovery",
                "clock",
                ports[index + 6],
                "control",
            ),
        ] {
            writeln!(
                manifest,
                r#"
[[endpoints]]
id = "{name}-{number}"
owner_process_id = "control-{number}"
protocol = "{protocol}"
priority = 10
listen = "tcp://127.0.0.1:{port}"
advertise = "tcp://localhost:{port}"
transport_profile_id = "{profile}"
tls_identity_id = "cluster-server"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "localhost"
"#
            )
            .unwrap();
        }
        writeln!(
            manifest,
            r#"
[[endpoints]]
id = "storage-{number}"
owner_process_id = "storage-{number}"
protocol = "storage-rpc"
priority = 10
listen = "tcp://127.0.0.1:{storage_port}"
advertise = "tcp://localhost:{storage_port}"
transport_profile_id = "internal"
tls_identity_id = "cluster-server"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "localhost"
"#,
            storage_port = ports[index + 9],
        )
        .unwrap();

        for (principal, id_field, id_value, credential_id) in [
            (
                "raft-peer",
                "node_id",
                (100 + u64::from(number)).to_string(),
                format!("raft-{number}"),
            ),
            (
                "storage-node",
                "node_id",
                number.to_string(),
                format!("storage-{number}"),
            ),
            (
                "admin",
                "instance_id",
                format!("\"control-{number}-admin\""),
                format!("admin-{number}"),
            ),
        ] {
            writeln!(
                manifest,
                r#"
[[auth_credentials]]
principal = "{principal}"
{id_field} = {id_value}
credential_id = "{credential_id}"
credential_version = 1
use_for_signing = true
accept_from_ms = 0
secret_ref = "file:{secret}"
"#,
                secret = material_dir.join(format!("{credential_id}.key")).display(),
            )
            .unwrap();
        }
    }

    let manifest_path = test_dir.join("cluster.toml");
    fs::write(&manifest_path, manifest).unwrap();
    manifest_path
}

#[cfg(target_os = "linux")]
fn wait_for_static_read_only_command_success(
    bin: &Path,
    manifest_path: &Path,
    process_id: &str,
    command_name: &str,
    args: &[&str],
    test_dir: &Path,
    children: &mut [&mut ChildGuard],
) -> Output {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_success_stdout = None;
    let mut consecutive_successes = 0_u8;
    loop {
        for child in children.iter_mut() {
            child.assert_running();
        }
        let output =
            run_static_control_plane_command(bin, manifest_path, process_id, command_name, args);
        if output.status.success() {
            if last_success_stdout
                .as_ref()
                .is_some_and(|previous: &Vec<u8>| *previous == output.stdout)
            {
                consecutive_successes += 1;
            } else {
                consecutive_successes = 1;
            }
            last_success_stdout = Some(output.stdout.clone());
            if consecutive_successes == 3 {
                return output;
            }
        } else {
            last_success_stdout = None;
            consecutive_successes = 0;
        }
        if Instant::now() >= deadline {
            panic!(
                "static TCP read-only command {command_name} did not converge: {}\n{}",
                format_admin_failure(output.status, &output),
                process_logs(test_dir)
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(target_os = "linux")]
fn run_static_mutating_command_once(
    bin: &Path,
    manifest_path: &Path,
    process_id: &str,
    command_name: &str,
    args: &[&str],
    test_dir: &Path,
    children: &mut [&mut ChildGuard],
) -> Output {
    for child in children {
        child.assert_running();
    }
    let output =
        run_static_control_plane_command(bin, manifest_path, process_id, command_name, args);
    assert!(
        output.status.success(),
        "single-attempt static TCP mutation {command_name} failed: {}\n{}",
        format_admin_failure(output.status, &output),
        process_logs(test_dir)
    );
    output
}

#[cfg(target_os = "linux")]
#[test]
fn static_manifest_tcp_three_authorities_bootstrap_route_admin_and_restart() {
    let Some(mount) = static_tcp_process_test_mount() else {
        eprintln!(
            "skipping static TCP process smoke: set ARGMIN_STATIC_TCP_PROCESS_TEST_MOUNT to a private writable mount boundary"
        );
        return;
    };
    let test_dir = TestDir::new_under(&mount);
    let bin = argmin_s3_bin();
    let (ports, mut reservations) = reserve_loopback_ports(12);
    let manifest_path = write_static_tcp_process_manifest(test_dir.path(), &ports);
    for number in 1..=3_u64 {
        let output = Command::new(&bin)
            .arg("initialize")
            .env("ARGMIN_CLUSTER_CONFIG_PATH", &manifest_path)
            .env("ARGMIN_PROCESS_ID", format!("control-{number}"))
            .output()
            .expect("static control-plane state initializer should run");
        assert!(
            output.status.success(),
            "static control-{number} initialization failed: {}",
            format_admin_failure(output.status, &output)
        );
    }
    release_static_authority_port_reservations(&mut reservations, 2);
    let mut node102 =
        ChildGuard::spawn_static(&bin, test_dir.path(), &manifest_path, "control-2", 102);
    wait_for_static_authority_raft_listener(&ports, 2, &mut node102);
    release_static_authority_port_reservations(&mut reservations, 3);
    let mut node103 =
        ChildGuard::spawn_static(&bin, test_dir.path(), &manifest_path, "control-3", 103);
    wait_for_static_authority_raft_listener(&ports, 3, &mut node103);
    release_static_authority_port_reservations(&mut reservations, 1);
    let mut node101 =
        ChildGuard::spawn_static(&bin, test_dir.path(), &manifest_path, "control-1", 101);
    wait_for_static_authority_raft_listener(&ports, 1, &mut node101);
    let unused_socket = test_dir.path().join("unused.sock");
    let unused_socket_arg = unused_socket.to_str().unwrap();

    let clock_status = wait_for_static_read_only_command_success(
        &bin,
        &manifest_path,
        "control-1",
        "control-plane-authority-clock-status",
        &[unused_socket_arg],
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut node103],
    );
    assert!(
        String::from_utf8_lossy(&clock_status.stdout).contains("established=true"),
        "initial static TCP authority clock should be established"
    );
    run_static_mutating_command_once(
        &bin,
        &manifest_path,
        "control-1",
        "control-plane-trigger-raft-snapshot-purge",
        &[unused_socket_arg],
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut node103],
    );

    node103.stop();
    wait_for_static_read_only_command_success(
        &bin,
        &manifest_path,
        "control-1",
        "control-plane-authority-clock-status",
        &[unused_socket_arg],
        test_dir.path(),
        &mut [&mut node101, &mut node102],
    );
    run_static_mutating_command_once(
        &bin,
        &manifest_path,
        "control-1",
        "control-plane-trigger-raft-snapshot-purge",
        &[unused_socket_arg],
        test_dir.path(),
        &mut [&mut node101, &mut node102],
    );
    let mut restarted103 =
        ChildGuard::spawn_static(&bin, test_dir.path(), &manifest_path, "control-3", 103);
    wait_for_static_read_only_command_success(
        &bin,
        &manifest_path,
        "control-3",
        "control-plane-authority-clock-status",
        &[unused_socket_arg],
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut restarted103],
    );
    run_static_mutating_command_once(
        &bin,
        &manifest_path,
        "control-1",
        "control-plane-transfer-raft-leadership",
        &[unused_socket_arg, "103"],
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut restarted103],
    );
    wait_for_static_read_only_command_success(
        &bin,
        &manifest_path,
        "control-3",
        "control-plane-authority-clock-status",
        &[unused_socket_arg],
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut restarted103],
    );
    run_static_mutating_command_once(
        &bin,
        &manifest_path,
        "control-3",
        "control-plane-reestablish-authority-clock",
        &[unused_socket_arg],
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut restarted103],
    );
    let recovered_clock_status = wait_for_static_read_only_command_success(
        &bin,
        &manifest_path,
        "control-3",
        "control-plane-authority-clock-status",
        &[unused_socket_arg],
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut restarted103],
    );
    assert!(
        String::from_utf8_lossy(&recovered_clock_status.stdout).contains("established=true"),
        "transferred static TCP authority clock should be re-established"
    );
    run_static_mutating_command_once(
        &bin,
        &manifest_path,
        "control-3",
        "control-plane-trigger-raft-snapshot-purge",
        &[unused_socket_arg],
        test_dir.path(),
        &mut [&mut node101, &mut node102, &mut restarted103],
    );

    assert!(
        (1..=3_u64).all(|number| test_dir
            .path()
            .join(format!("state-{number}/control.state"))
            .is_file()),
        "all static TCP authorities should publish durable restart artifacts"
    );
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
    let follower_log_before_crash = artifact_log_state(&follower_state_path)
        .expect("follower artifact plus WAL should expose log state before crash");
    assert!(
        follower_log_before_crash.last_log_id.is_some()
            || follower_log_before_crash.committed.is_some(),
        "bootstrapped follower should expose retained log state before crash: {follower_log_before_crash:?}"
    );
    let follower_vote_before_crash = artifact_persisted_vote(&follower_state_path)
        .expect("follower artifact plus WAL should expose vote before crash")
        .expect("bootstrapped follower should persist a vote before crash");
    assert!(
        follower_vote_before_crash.committed,
        "bootstrapped follower should persist a committed vote before direct append: {follower_vote_before_crash:?}"
    );
    let preparation_prev_log_id = follower_log_before_crash
        .last_log_id
        .expect("bootstrapped follower should have a log tip before append");
    let append_term = follower_vote_before_crash
        .term
        .checked_add(1_000)
        .expect("synthetic leader vote term should advance past local elections");
    let peer_client = ControlPlaneRaftPeerTestClient::unix(
        follower_peer_socket.clone(),
        cluster_name.clone(),
        101,
        103,
        ControlPlaneRaftPeerTransportLimits::default(),
        Duration::from_secs(5),
    )
    .with_auth_credentials(
        &cluster_name,
        101,
        ProcessTestControlPlaneAuth::raft_peer_auth_credentials(),
        None,
    )
    .expect("synthetic leader credentials should build");
    peer_client
        .append_commands(
            append_term,
            preparation_prev_log_id,
            follower_log_before_crash.committed,
            Vec::new(),
        )
        .expect("synthetic leader preparation should be acknowledged");

    let follower_log_before_crash = artifact_log_state(&follower_state_path)
        .expect("prepared follower artifact plus WAL should expose current log state");
    let mut prev_log_id = follower_log_before_crash
        .last_log_id
        .expect("prepared follower should have a log tip before append");
    node101.stop();
    node102.stop();

    let send_padded_append_batch =
        |prev_log_id: ControlPlaneRaftLogId, padded_endpoint_bytes: usize| {
            let padded_endpoint = "x".repeat(padded_endpoint_bytes);
            let commands = (0..64)
                .map(|_| ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), padded_endpoint.clone())],
                    pg_ids: vec![PgId::new(0)],
                })
                .collect();
            peer_client.begin_append_commands(
                append_term,
                prev_log_id,
                follower_log_before_crash.committed,
                commands,
            )
        };

    // Build a 60 MiB payload prefix using requests small enough to complete
    // within the peer's one-second end-to-end deadline on slower filesystems.
    // This leaves ample framing headroom below the 64 MiB checkpoint boundary.
    const PREFIX_ENDPOINT_BYTES: usize = 32 * 1024;
    const PREFIX_BATCHES: usize = 30;
    for batch in 0..PREFIX_BATCHES {
        let (appended_log_id, response) = send_padded_append_batch(
            prev_log_id,
            PREFIX_ENDPOINT_BYTES,
        )
        .unwrap_or_else(|error| {
            panic!(
                "sub-threshold padded peer WAL append batch {batch} request should be sent: {error:?}\n{}",
                process_logs(test_dir.path())
            )
        });
        response.wait().unwrap_or_else(|error| {
            panic!(
                "sub-threshold fsynced padded peer WAL append batch {batch} should be acknowledged: {error:?}\n{}",
                process_logs(test_dir.path())
            )
        });
        prev_log_id = appended_log_id;
    }
    let acknowledged_log_id = prev_log_id;
    let checkpoint_write_blocker = ControlPlaneRaftCheckpointWriteBlockerForTest::install(
        &follower_state_path,
        restarted103.process_id(),
    )
    .expect("follower checkpoint publication should be blocked");

    // Keep the threshold-crossing connection alive, but do not require its
    // response to race the independently scheduled checkpoint failure. The
    // preceding batches establish the acknowledged recovery prefix. A 4 MiB
    // payload batch then carries the WAL suffix beyond the 64 MiB threshold.
    let (appended_log_id, _threshold_crossing_response) =
        send_padded_append_batch(acknowledged_log_id, 64 * 1024).unwrap_or_else(|error| {
            panic!(
                "threshold-crossing padded peer WAL append request should be sent: {error:?}\n{}",
                process_logs(test_dir.path())
            )
        });

    let status = wait_for_process_exit(&mut restarted103, Duration::from_secs(5));
    drop(checkpoint_write_blocker);
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
    let follower_log_after_crash = artifact_log_state(&follower_state_path)
        .expect("artifact plus WAL should restore after peer append crash");
    let follower_vote_after_crash = artifact_persisted_vote(&follower_state_path)
        .expect("artifact plus WAL should restore vote after peer append crash");
    assert_eq!(
        follower_log_after_crash.last_log_id,
        Some(appended_log_id),
        "artifact plus WAL must recover the acknowledged prefix through {acknowledged_log_id:?} and the fsynced threshold-crossing append; before={follower_log_before_crash:?} after={follower_log_after_crash:?} vote_after={follower_vote_after_crash:?}"
    );

    let mut recovered103 =
        ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 103, &raft_node_ids);
    wait_for_socket_file(&follower_peer_socket, &mut recovered103);
    wait_for_child_stderr_log_contains(
        &mut recovered103,
        "argmin-s3 experimental durable OpenRaft control-plane manager using state",
    );
    // Artifact publication and WAL compaction are crash ordered, but they are
    // separate files. Stop the writer before direct inspection so the test
    // cannot combine an old artifact read with a newly compacted WAL.
    recovered103.stop();
    assert_eq!(
        artifact_log_state(&follower_state_path)
            .expect("recovered follower artifact plus WAL should expose log state")
            .last_log_id,
        Some(appended_log_id),
        "recovered follower should retain the WAL-restored acknowledged append"
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
    let auth = ProcessTestControlPlaneAuth::new(DEFAULT_CONTROL_PLANE_AUTH_CLUSTER);
    let admin_instance_id = "transfer-admin";
    let admin_credentials = auth.admin_credentials_env(&[admin_instance_id]);
    let server_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            DEFAULT_CONTROL_PLANE_AUTH_CLUSTER,
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
            admin_credentials.as_str(),
        ),
    ];
    let admin_helper_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            DEFAULT_CONTROL_PLANE_AUTH_CLUSTER,
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
    let auth = ProcessTestControlPlaneAuth::new(DEFAULT_CONTROL_PLANE_AUTH_CLUSTER);
    let admin_instance_id = "snapshot-catchup-admin";
    let admin_credentials = auth.admin_credentials_env(&[admin_instance_id]);
    let server_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            DEFAULT_CONTROL_PLANE_AUTH_CLUSTER,
        ),
        (
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
            admin_credentials.as_str(),
        ),
    ];
    let admin_helper_auth_env = [
        (
            "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
            DEFAULT_CONTROL_PLANE_AUTH_CLUSTER,
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
