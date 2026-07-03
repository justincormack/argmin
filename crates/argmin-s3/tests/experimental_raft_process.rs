use std::collections::BTreeSet;
use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use storage::control_plane_raft::ControlPlaneRaftRestartArtifact;
use storage::{NodeId, PgId};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("argmin-s3-{name}-{}-{now}", std::process::id()));
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
    child: Child,
}

impl ChildGuard {
    fn spawn(bin: &Path, test_dir: &Path, cluster_name: &str, raft_node_id: u64) -> Self {
        let control_socket = test_dir.join(format!("control-{raft_node_id}.sock"));
        let peer_socket = test_dir.join(format!("raft-{raft_node_id}.sock"));
        let state_path = test_dir.join(format!("control-{raft_node_id}.state"));
        let data_dir = test_dir.join(format!("data-{raft_node_id}"));
        let stdout = File::create(test_dir.join(format!("node-{raft_node_id}.stdout.log")))
            .expect("stdout log should be created");
        let stderr = File::create(test_dir.join(format!("node-{raft_node_id}.stderr.log")))
            .expect("stderr log should be created");
        let peer_sockets = format!(
            "101={},102={}",
            test_dir.join("raft-101.sock").display(),
            test_dir.join("raft-102.sock").display()
        );
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
        Self { child }
    }

    fn assert_running(&mut self) {
        match self.child.try_wait() {
            Ok(None) => {}
            Ok(Some(status)) => panic!("argmin-s3 process exited early with {status}"),
            Err(error) => panic!("argmin-s3 process status should be readable: {error}"),
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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

fn wait_for_runtime_map_ready(
    bin: &Path,
    test_dir: &Path,
    children: &mut [&mut ChildGuard],
) -> (PathBuf, Output) {
    let control_sockets = [
        test_dir.join("control-101.sock"),
        test_dir.join("control-102.sock"),
    ];
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

fn process_logs(test_dir: &Path) -> String {
    let mut out = String::new();
    for node_id in [101, 102] {
        for stream in ["stdout", "stderr"] {
            let path = test_dir.join(format!("node-{node_id}.{stream}.log"));
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
    }
    out
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
    let mut node102 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 102);
    wait_for_socket_file(&test_dir.path().join("raft-102.sock"), &mut node102);
    let mut node101 = ChildGuard::spawn(&bin, test_dir.path(), &cluster_name, 101);

    let (leader_socket, output) =
        wait_for_runtime_map_ready(&bin, test_dir.path(), &mut [&mut node101, &mut node102]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.split_whitespace().nth(1) == Some("1"),
        "ready helper should report one PG route from {}: {stdout}",
        leader_socket.display()
    );

    let follower_state = if leader_socket.ends_with("control-101.sock") {
        test_dir.path().join("control-102.state")
    } else {
        test_dir.path().join("control-101.state")
    };
    wait_for_follower_artifact(&follower_state, &mut [&mut node101, &mut node102]);
}
