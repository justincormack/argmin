use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::future::Future;
use std::net::TcpListener;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

use super::*;
use futures_util::stream;
use openraft::errors::{
    Fatal, NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::testing::log::{StoreBuilder, Suite as OpenRaftLogSuite};
use openraft::type_config::TypeConfigExt;
use openraft::{AnyError, Config, Membership, Raft, ReadPolicy, StorageError};
use proptest::prelude::*;

use crate::control_plane::{
    ClusterControlSnapshot, NodeAvailabilityState, NodeHeartbeat, NodePgHeartbeatObservation,
    PgMetadataProof, RuntimeMapFreshnessProof,
};
use crate::control_plane_auth::ControlPlaneScopedCredentialInput;
use crate::control_plane_command::LeaseHorizonAuthorityBinding;
use crate::types::PgId;

const IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn control_plane_raft_companion_paths_preserve_non_utf8_artifact_names() {
    let directory = test_util::tempdir();
    let first = directory
        .path()
        .join(OsString::from_vec(vec![b'r', b'a', b'f', b't', 0xfe]));
    let second = directory
        .path()
        .join(OsString::from_vec(vec![b'r', b'a', b'f', b't', 0xff]));

    for derive in [
        durable_artifact_wal_path as fn(&Path) -> PathBuf,
        durable_artifact_sentinel_path,
    ] {
        assert_ne!(derive(&first), derive(&second));
        assert!(derive(&first)
            .as_os_str()
            .as_bytes()
            .starts_with(first.as_os_str().as_bytes()));
        assert!(derive(&second)
            .as_os_str()
            .as_bytes()
            .starts_with(second.as_os_str().as_bytes()));
    }
    assert_ne!(
        durable_artifact_tmp_path_for_process(&first, 17),
        durable_artifact_tmp_path_for_process(&second, 17)
    );
}

type ControlPlaneOpenRaftLogSuite = OpenRaftLogSuite<
    ControlPlaneRaftTypeConfig,
    ControlPlaneRaftLogStore,
    ControlPlaneRaftStateMachine,
    ControlPlaneOpenRaftSuiteBuilder,
    (),
>;

#[test]
fn openraft_linearizable_fatal_error_remains_non_retryable() {
    let error: RaftError<
        ControlPlaneRaftTypeConfig,
        LinearizableReadError<ControlPlaneRaftTypeConfig>,
    > = RaftError::Fatal(Fatal::Stopped);

    let error = openraft_linearizable_read_error("test read-index", error);

    assert!(matches!(
        &error,
        ControlPlaneError::OpenRaftOperation {
            kind: ControlPlaneRaftOperationErrorKind::Fatal,
            ..
        }
    ));
    assert!(!error.is_retryable_openraft_leadership_error());
}

fn raft_peer_test_tls_roots() -> rustls::RootCertStore {
    use rustls::pki_types::pem::PemObject as _;
    use rustls::pki_types::CertificateDer;

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(
            CertificateDer::pem_slice_iter(include_bytes!(
                "../../../s3-tests/testdata/ca-cert.pem"
            ))
            .next()
            .unwrap()
            .unwrap(),
        )
        .unwrap();
    roots
}

fn raft_peer_test_tls_certified_key() -> Arc<CertifiedKey> {
    use rustls::pki_types::pem::PemObject as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let certificates = CertificateDer::pem_slice_iter(include_bytes!(
        "../../../s3-tests/testdata/localhost-cert.pem"
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let private_key = PrivateKeyDer::from_pem_slice(include_bytes!(
        "../../../s3-tests/testdata/localhost-key.pem"
    ))
    .unwrap();
    let signing_key = tls_provider::build_provider()
        .key_provider
        .load_private_key(private_key)
        .unwrap();
    Arc::new(CertifiedKey::new(certificates, signing_key))
}

fn raft_peer_test_tls_server_config(with_alpn: bool) -> Arc<rustls::ServerConfig> {
    use rustls::pki_types::pem::PemObject as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let certificates = CertificateDer::pem_slice_iter(include_bytes!(
        "../../../s3-tests/testdata/localhost-cert.pem"
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let private_key = PrivateKeyDer::from_pem_slice(include_bytes!(
        "../../../s3-tests/testdata/localhost-key.pem"
    ))
    .unwrap();
    let mut server_config =
        rustls::ServerConfig::builder_with_provider(tls_provider::configured_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .unwrap();
    if with_alpn {
        server_config.alpn_protocols = vec![CONTROL_PLANE_RAFT_TLS_ALPN.to_vec()];
    }
    Arc::new(server_config)
}

fn raft_peer_test_tls_endpoint(port: u16) -> ControlPlaneRaftPeerClientEndpoint {
    ControlPlaneRaftPeerClientEndpoint::tls_tcp(
        format!("tcp://localhost:{port}"),
        "127.0.0.1",
        port,
        "localhost",
        raft_peer_test_tls_roots(),
    )
    .unwrap()
}

fn raft_peer_test_configured_transport(
    node_id: ControlPlaneRaftNodeId,
    endpoint: ControlPlaneRaftPeerClientEndpoint,
) -> ControlPlaneRaftConfiguredPeerFrameTransport {
    ControlPlaneRaftConfiguredPeerFrameTransport {
        endpoints: Arc::new(BTreeMap::from([(node_id, endpoint)])),
    }
}

#[derive(Debug)]
struct TestPeerFrameTransport {
    observations: Mutex<Vec<(ControlPlaneRaftNodeId, String, usize, Duration)>>,
}

impl TestPeerFrameTransport {
    fn new() -> Self {
        Self {
            observations: Mutex::new(Vec::new()),
        }
    }
}

impl ControlPlaneRaftPeerFrameTransport for TestPeerFrameTransport {
    fn name(&self, _target: ControlPlaneRaftNodeId) -> &'static str {
        "test"
    }

    fn exchange(
        &self,
        exchange: ControlPlaneRaftPeerFrameExchange,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<u8>, ControlPlaneRaftPeerFrameExchangeError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.observations.lock().unwrap().push((
                exchange.target,
                exchange.endpoint,
                exchange.max_frame_bytes,
                exchange.connect_timeout,
            ));
            let identity =
                decode_control_plane_raft_peer_request_frame_identity(&exchange.request_frame)
                    .map_err(|error| {
                        ControlPlaneRaftPeerFrameExchangeError::new("decode test frame", error)
                    })?;
            let request = ControlPlaneRaftPeerRpcRequest::decode_frame_for_peer(
                &exchange.request_frame,
                &identity,
            )
            .map_err(|error| {
                ControlPlaneRaftPeerFrameExchangeError::new("decode test frame", error)
            })?;
            let ControlPlaneRaftPeerRpcRequest::Vote(request) = request else {
                return Err(ControlPlaneRaftPeerFrameExchangeError::new(
                    "decode test frame",
                    raft_artifact_protocol_error("test transport expected a vote request"),
                ));
            };
            ControlPlaneRaftPeerRpcResponse::Vote(VoteResponse {
                vote: request.vote,
                vote_granted: true,
                last_log_id: request.last_log_id,
            })
            .encode_frame_for_peer(&reverse_raft_peer_frame_identity(&identity))
            .map_err(|error| {
                ControlPlaneRaftPeerFrameExchangeError::new("encode test frame", error)
            })
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum ReplicatedFailoverModelOp {
    LoseOriginalLeader,
    ElectReplacement,
    RestoreQuorum,
    LoseQuorum,
    ReestablishClock,
    RenewStorageNode(usize),
    MarkPgServing(usize),
    AdvanceUnrelatedEpoch,
    RestartOriginalLeader,
}

impl ReplicatedFailoverModelOp {
    fn from_byte(value: u8) -> Self {
        match value % 11 {
            0 => Self::LoseOriginalLeader,
            1 => Self::ElectReplacement,
            2 => Self::RestoreQuorum,
            3 => Self::LoseQuorum,
            4 => Self::ReestablishClock,
            5..=7 => Self::RenewStorageNode(usize::from(value % 3)),
            8 => Self::MarkPgServing(usize::from(value % 3)),
            9 => Self::AdvanceUnrelatedEpoch,
            _ => Self::RestartOriginalLeader,
        }
    }
}

#[derive(Clone, Debug)]
struct ReplicatedFailoverBarrierModel {
    term: u64,
    leader: Option<usize>,
    quorum_available: bool,
    local_terms: [u64; 3],
    locally_claims_leader: [bool; 3],
    clock_terms: [Option<u64>; 3],
    failover_started: bool,
    pre_failover_epoch: u64,
    epoch: u64,
    pre_failover_max_lease_deadline_ms: u64,
    storage_lease_deadlines_ms: [u64; 3],
    pg_serving: [bool; 3],
}

impl ReplicatedFailoverBarrierModel {
    fn new() -> Self {
        Self {
            term: 1,
            leader: Some(0),
            quorum_available: true,
            local_terms: [1; 3],
            locally_claims_leader: [true, false, false],
            clock_terms: [Some(1), None, None],
            failover_started: false,
            pre_failover_epoch: 10,
            epoch: 10,
            pre_failover_max_lease_deadline_ms: 1_000,
            storage_lease_deadlines_ms: [1_000; 3],
            pg_serving: [true; 3],
        }
    }

    fn command_authorized(&self, node: usize) -> bool {
        self.quorum_available
            && self.leader == Some(node)
            && self.locally_claims_leader[node]
            && self.local_terms[node] == self.term
    }

    fn lease_authorized(&self, node: usize) -> bool {
        self.command_authorized(node) && self.clock_terms[node] == Some(self.term)
    }

    fn failover_barrier_satisfied(&self) -> bool {
        let Some(leader) = self.leader else {
            return false;
        };
        self.failover_started
            && self.lease_authorized(leader)
            && self.epoch >= self.pre_failover_epoch
            && self.pg_serving.iter().all(|serving| *serving)
            && self
                .storage_lease_deadlines_ms
                .iter()
                .all(|deadline| *deadline > self.pre_failover_max_lease_deadline_ms)
    }

    fn apply(&mut self, operation: ReplicatedFailoverModelOp) {
        match operation {
            ReplicatedFailoverModelOp::LoseOriginalLeader if !self.failover_started => {
                self.failover_started = true;
                self.leader = None;
            }
            ReplicatedFailoverModelOp::ElectReplacement if self.failover_started => {
                self.quorum_available = true;
                self.term = self.term.saturating_add(1);
                self.leader = Some(1);
                self.local_terms[1] = self.term;
                self.local_terms[2] = self.term;
                self.locally_claims_leader[1] = true;
                self.clock_terms[1] = None;
            }
            ReplicatedFailoverModelOp::RestoreQuorum => self.quorum_available = true,
            ReplicatedFailoverModelOp::LoseQuorum => self.quorum_available = false,
            ReplicatedFailoverModelOp::ReestablishClock => {
                if let Some(leader) = self
                    .leader
                    .filter(|leader| self.command_authorized(*leader))
                {
                    self.clock_terms[leader] = Some(self.term);
                }
            }
            ReplicatedFailoverModelOp::RenewStorageNode(node) => {
                if self
                    .leader
                    .is_some_and(|leader| self.lease_authorized(leader))
                {
                    self.storage_lease_deadlines_ms[node] =
                        self.pre_failover_max_lease_deadline_ms + 1 + self.term;
                }
            }
            ReplicatedFailoverModelOp::MarkPgServing(pg) => {
                if self
                    .leader
                    .is_some_and(|leader| self.lease_authorized(leader))
                {
                    self.pg_serving[pg] = true;
                }
            }
            ReplicatedFailoverModelOp::AdvanceUnrelatedEpoch => {
                if self
                    .leader
                    .is_some_and(|leader| self.command_authorized(leader))
                {
                    self.epoch = self.epoch.saturating_add(1);
                }
            }
            ReplicatedFailoverModelOp::RestartOriginalLeader => {
                self.locally_claims_leader[0] = true;
            }
            ReplicatedFailoverModelOp::LoseOriginalLeader
            | ReplicatedFailoverModelOp::ElectReplacement => {}
        }

        if self.failover_started && self.term > 1 {
            assert!(
                !self.command_authorized(0),
                "a restarted original leader must not pass quorum authority confirmation"
            );
        }
        if self.failover_barrier_satisfied() {
            let leader = self.leader.expect("satisfied barrier must have a leader");
            assert!(self.lease_authorized(leader));
            assert!(self.epoch >= self.pre_failover_epoch);
            assert!(self.pg_serving.iter().all(|serving| *serving));
            assert!(self
                .storage_lease_deadlines_ms
                .iter()
                .all(|deadline| { *deadline > self.pre_failover_max_lease_deadline_ms }));
        }
    }
}

#[test]
fn replicated_failover_barrier_rejects_stale_leader_and_partial_recovery() {
    let mut model = ReplicatedFailoverBarrierModel::new();
    model.apply(ReplicatedFailoverModelOp::LoseOriginalLeader);
    model.apply(ReplicatedFailoverModelOp::RestartOriginalLeader);
    assert!(model.locally_claims_leader[0]);
    assert!(!model.command_authorized(0));

    model.apply(ReplicatedFailoverModelOp::ElectReplacement);
    model.apply(ReplicatedFailoverModelOp::AdvanceUnrelatedEpoch);
    assert!(!model.failover_barrier_satisfied());

    model.apply(ReplicatedFailoverModelOp::ReestablishClock);
    for pg in 0..3 {
        model.apply(ReplicatedFailoverModelOp::MarkPgServing(pg));
    }
    model.apply(ReplicatedFailoverModelOp::RenewStorageNode(0));
    model.apply(ReplicatedFailoverModelOp::RenewStorageNode(1));
    assert!(!model.failover_barrier_satisfied());

    model.apply(ReplicatedFailoverModelOp::RenewStorageNode(2));
    assert!(model.failover_barrier_satisfied());
    model.apply(ReplicatedFailoverModelOp::LoseQuorum);
    assert!(!model.failover_barrier_satisfied());
}

#[test]
fn replicated_failover_barrier_accepts_renewed_leases_without_topology_epoch_churn() {
    let mut model = ReplicatedFailoverBarrierModel::new();
    model.apply(ReplicatedFailoverModelOp::LoseOriginalLeader);
    model.apply(ReplicatedFailoverModelOp::ElectReplacement);
    model.apply(ReplicatedFailoverModelOp::ReestablishClock);
    for node in 0..3 {
        model.apply(ReplicatedFailoverModelOp::RenewStorageNode(node));
    }

    assert_eq!(model.epoch, model.pre_failover_epoch);
    assert!(model.failover_barrier_satisfied());
}

proptest! {
    #[test]
    fn prop_replicated_failover_barrier_requires_full_authority_recovery(
        operations in proptest::collection::vec(any::<u8>(), 1..128),
    ) {
        let mut model = ReplicatedFailoverBarrierModel::new();
        for operation in operations {
            model.apply(ReplicatedFailoverModelOp::from_byte(operation));
        }
    }
}

fn test_raft_wal_file(
    path: impl Into<PathBuf>,
    cluster_name: impl Into<String>,
    local_node_id: ControlPlaneRaftNodeId,
) -> ControlPlaneRaftWalFile {
    ControlPlaneRaftWalFile::new(ControlPlaneRaftWalFileConfig {
        path: path.into(),
        cluster_name: cluster_name.into(),
        local_node_id,
    })
}

async fn append_and_wait_for_durability(
    store: &mut ControlPlaneRaftLogStore,
    entries: Vec<ControlPlaneRaftEntry>,
) -> (Result<(), io::Error>, Result<(), io::Error>) {
    let (flushed, durability) = ControlPlaneRaftTypeConfig::oneshot();
    let accepted = RaftLogStorage::append(store, entries, IOFlushed::signal(flushed)).await;
    let durability = durability
        .await
        .expect("WAL durability worker should complete the flush callback");
    (accepted, durability)
}

struct TestWalFileSyncGate {
    path: PathBuf,
    state: ControlPlaneRaftWalFileSyncGate,
    registry: &'static Mutex<BTreeMap<PathBuf, ControlPlaneRaftWalFileSyncGate>>,
}

impl TestWalFileSyncGate {
    fn install(path: PathBuf) -> Self {
        Self::install_in(path, &CONTROL_PLANE_RAFT_WAL_FILE_SYNC_GATES)
    }

    fn install_durable_publication(path: PathBuf) -> Self {
        Self::install_in(path, &CONTROL_PLANE_RAFT_WAL_DURABLE_PUBLICATION_GATES)
    }

    fn install_in(
        path: PathBuf,
        registry: &'static Mutex<BTreeMap<PathBuf, ControlPlaneRaftWalFileSyncGate>>,
    ) -> Self {
        let state = Arc::new((
            Mutex::new(ControlPlaneRaftWalFileSyncGateState::default()),
            std::sync::Condvar::new(),
        ));
        let mut gates = registry
            .lock()
            .expect("test WAL gate registry lock should not be poisoned");
        assert!(
            gates.insert(path.clone(), Arc::clone(&state)).is_none(),
            "only one test WAL gate may be active per WAL path and phase"
        );
        drop(gates);
        Self {
            path,
            state,
            registry,
        }
    }

    fn wait_until_entered(&self, timeout: Duration) {
        let (state, condition) = &*self.state;
        let state = state
            .lock()
            .expect("test WAL file-sync gate state should not be poisoned");
        let (state, wait) = condition
            .wait_timeout_while(state, timeout, |state| !state.entered)
            .expect("test WAL file-sync gate state should not be poisoned");
        assert!(
            state.entered && !wait.timed_out(),
            "WAL durability worker did not reach the file-sync gate"
        );
    }

    fn release(&self) {
        let (state, condition) = &*self.state;
        let mut state = state
            .lock()
            .expect("test WAL file-sync gate state should not be poisoned");
        state.released = true;
        condition.notify_all();
    }

    fn pending_operation_watchdog(
        &self,
        operation_completed: Arc<AtomicBool>,
    ) -> thread::JoinHandle<bool> {
        let state = Arc::clone(&self.state);
        thread::spawn(move || {
            let (gate, condition) = &*state;
            let gate = gate
                .lock()
                .expect("test WAL gate state should not be poisoned");
            let (gate, wait) = condition
                .wait_timeout_while(gate, Duration::from_secs(1), |gate| !gate.entered)
                .expect("test WAL gate state should not be poisoned");
            assert!(
                gate.entered && !wait.timed_out(),
                "WAL durability worker did not reach the test gate"
            );
            drop(gate);
            thread::sleep(Duration::from_millis(100));
            let completed_before_release = operation_completed.load(Ordering::SeqCst);
            let mut gate = state
                .0
                .lock()
                .expect("test WAL gate state should not be poisoned");
            gate.released = true;
            state.1.notify_all();
            !completed_before_release
        })
    }
}

fn state_machine_executor_progress_watchdog(
    hook: Arc<ControlPlaneRaftStateMachineBlockingHook>,
    timer_completed: Arc<AtomicBool>,
) -> thread::JoinHandle<bool> {
    thread::spawn(move || {
        hook.wait_until_entered(Duration::from_secs(1));
        thread::sleep(Duration::from_millis(100));
        let progressed_before_release = timer_completed.load(Ordering::SeqCst);
        hook.release();
        progressed_before_release
    })
}

fn state_machine_retirement_progress_watchdog(
    hook: Arc<ControlPlaneRaftStateMachineBlockingHook>,
    retirement_entered: Arc<AtomicBool>,
    timer_completed: Arc<AtomicBool>,
) -> thread::JoinHandle<bool> {
    thread::spawn(move || {
        hook.wait_until_entered(Duration::from_secs(1));
        retirement_entered.store(true, Ordering::SeqCst);
        thread::sleep(Duration::from_millis(100));
        let progressed_before_release = timer_completed.load(Ordering::SeqCst);
        hook.release();
        progressed_before_release
    })
}

async fn mark_executor_timer_progress(timer_completed: Arc<AtomicBool>) {
    tokio::time::sleep(Duration::from_millis(10)).await;
    timer_completed.store(true, Ordering::SeqCst);
}

async fn mark_executor_timer_progress_after_phase_entry(
    phase_entered: Arc<AtomicBool>,
    timer_completed: Arc<AtomicBool>,
) {
    while !phase_entered.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    mark_executor_timer_progress(timer_completed).await;
}

impl Drop for TestWalFileSyncGate {
    fn drop(&mut self) {
        self.release();
        let mut gates = self
            .registry
            .lock()
            .expect("test WAL gate registry lock should not be poisoned");
        if gates
            .get(&self.path)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            gates.remove(&self.path);
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ControlPlaneOpenRaftSuiteBuilder;

impl
    StoreBuilder<ControlPlaneRaftTypeConfig, ControlPlaneRaftLogStore, ControlPlaneRaftStateMachine>
    for ControlPlaneOpenRaftSuiteBuilder
{
    async fn build(
        &self,
    ) -> Result<
        ((), ControlPlaneRaftLogStore, ControlPlaneRaftStateMachine),
        StorageError<ControlPlaneRaftTypeConfig>,
    > {
        Ok((
            (),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        ))
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct UnreachableRaftNetworkFactory;

impl RaftNetworkFactory<ControlPlaneRaftTypeConfig> for UnreachableRaftNetworkFactory {
    type Network = UnreachableRaftNetwork;

    async fn new_client(
        &mut self,
        target: ControlPlaneRaftNodeId,
        _node: &BasicNode,
    ) -> Self::Network {
        UnreachableRaftNetwork { target }
    }
}

#[derive(Debug, Clone, Copy)]
struct UnreachableRaftNetwork {
    target: ControlPlaneRaftNodeId,
}

impl UnreachableRaftNetwork {
    fn unreachable(&self, rpc_name: &'static str) -> Unreachable<ControlPlaneRaftTypeConfig> {
        Unreachable::new(&AnyError::error(format!(
            "test network should not send {rpc_name} to node {}",
            self.target
        )))
    }
}

impl RaftNetworkV2<ControlPlaneRaftTypeConfig> for UnreachableRaftNetwork {
    type SnapshotData = ControlPlaneRaftSnapshotData;

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
        RPCError<ControlPlaneRaftTypeConfig>,
    > {
        Err(RPCError::Unreachable(self.unreachable("append_entries")))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
    {
        Err(RPCError::Unreachable(self.unreachable("vote")))
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<ControlPlaneRaftTypeConfig>,
        _snapshot: ControlPlaneRaftSnapshot,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<
        SnapshotResponse<ControlPlaneRaftTypeConfig>,
        StreamingError<ControlPlaneRaftTypeConfig>,
    > {
        Err(StreamingError::Unreachable(
            self.unreachable("full_snapshot"),
        ))
    }
}

#[derive(Debug, Clone, Default)]
struct InMemoryRaftNetworkFactory {
    peers: Arc<
        Mutex<
            BTreeMap<
                ControlPlaneRaftNodeId,
                Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
            >,
        >,
    >,
    policy: Option<Arc<ControlPlaneRaftPeerTransportPolicy>>,
    local_node_id: Option<ControlPlaneRaftNodeId>,
    max_append_entries_seen: Arc<AtomicUsize>,
}

impl InMemoryRaftNetworkFactory {
    fn with_transport_policy(policy: ControlPlaneRaftPeerTransportPolicy) -> Self {
        Self {
            peers: Arc::default(),
            policy: Some(Arc::new(policy)),
            local_node_id: None,
            max_append_entries_seen: Arc::default(),
        }
    }

    fn for_local_node(&self, local_node_id: ControlPlaneRaftNodeId) -> Self {
        Self {
            peers: self.peers.clone(),
            policy: self.policy.clone(),
            local_node_id: Some(local_node_id),
            max_append_entries_seen: Arc::clone(&self.max_append_entries_seen),
        }
    }

    fn register(
        &self,
        node_id: ControlPlaneRaftNodeId,
        raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    ) {
        self.peers.lock().unwrap().insert(node_id, raft);
    }

    fn unregister(&self, node_id: ControlPlaneRaftNodeId) {
        self.peers.lock().unwrap().remove(&node_id);
    }

    fn max_append_entries_seen(&self) -> usize {
        self.max_append_entries_seen.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Default)]
struct InMemoryAuthorityCapabilityDirectory {
    entries: Arc<Mutex<BTreeMap<ControlPlaneRaftNodeId, InMemoryAuthorityCapabilityEntry>>>,
}

#[derive(Debug, Clone)]
struct InMemoryAuthorityCapabilityEntry {
    status: ControlPlaneRaftAuthorityStatusHandle,
    bootstrap: ControlPlaneRaftAuthorityBootstrapHandle,
    node_lifecycle: ControlPlaneRaftAuthorityNodeLifecycleHandle,
    linearized_authority: ControlPlaneRaftAuthorityHandle,
    leader_routed_admin: ControlPlaneRaftLeaderRoutedAdminHandle,
}

impl InMemoryAuthorityCapabilityDirectory {
    fn register<T>(&self, node_id: ControlPlaneRaftNodeId, authority: Arc<T>)
    where
        T: ControlPlaneRaftAuthorityStatusSource
            + ControlPlaneRaftAuthorityBootstrap
            + ControlPlaneRaftAuthorityNodeLifecycle
            + ControlPlaneRaftLinearizedAuthority
            + ControlPlaneRaftLeaderRoutedAdmin
            + Send
            + Sync
            + 'static,
    {
        let status = ControlPlaneRaftAuthorityStatusHandle::new(Arc::clone(&authority));
        let bootstrap = ControlPlaneRaftAuthorityBootstrapHandle::new(Arc::clone(&authority));
        let node_lifecycle =
            ControlPlaneRaftAuthorityNodeLifecycleHandle::new(Arc::clone(&authority));
        let linearized_authority = ControlPlaneRaftAuthorityHandle::new(Arc::clone(&authority));
        let leader_routed_admin = ControlPlaneRaftLeaderRoutedAdminHandle::new(authority);
        self.entries.lock().unwrap().insert(
            node_id,
            InMemoryAuthorityCapabilityEntry {
                status,
                bootstrap,
                node_lifecycle,
                linearized_authority,
                leader_routed_admin,
            },
        );
    }
}

impl ControlPlaneRaftAuthorityStatusListSource for InMemoryAuthorityCapabilityDirectory {
    fn authority_statuses(
        &self,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<
            BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftAuthorityStatus>,
            ControlPlaneError,
        >,
    > {
        Box::pin(async move {
            let entries = {
                let entries = self.entries.lock().map_err(|_| {
                    ControlPlaneError::rpc_remote(
                        "in-memory test authority capability directory lock poisoned".to_string(),
                    )
                })?;
                entries.clone()
            };
            let mut statuses = BTreeMap::new();
            for (node_id, entry) in entries {
                let status = entry.status.status().await.map_err(|error| {
                        ControlPlaneError::rpc_remote(format!(
                                "in-memory test authority capability directory status for node {node_id} failed: {error:?}"
                            ))
                    })?;
                statuses.insert(node_id, status);
            }
            Ok(statuses)
        })
    }
}

impl ControlPlaneRaftAuthorityBootstrapDirectory for InMemoryAuthorityCapabilityDirectory {
    fn authority_bootstrap_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftAuthorityBootstrapHandle, ControlPlaneError>,
    > {
        let result = (|| {
            let entries = self.entries.lock().map_err(|_| {
                ControlPlaneError::rpc_remote(
                    "in-memory test authority capability directory lock poisoned".to_string(),
                )
            })?;
            entries
                    .get(&node_id)
                    .map(|entry| entry.bootstrap.clone())
                    .ok_or_else(|| ControlPlaneError::rpc_remote(format!(
                            "in-memory test authority capability directory has no bootstrap node {node_id}"
                        )))
        })();
        Box::pin(std::future::ready(result))
    }
}

impl ControlPlaneRaftAuthorityNodeLifecycleDirectory for InMemoryAuthorityCapabilityDirectory {
    fn authority_node_lifecycle_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftAuthorityNodeLifecycleHandle, ControlPlaneError>,
    > {
        let result = (|| {
            let entries = self.entries.lock().map_err(|_| {
                ControlPlaneError::rpc_remote(
                    "in-memory test authority capability directory lock poisoned".to_string(),
                )
            })?;
            entries
                    .get(&node_id)
                    .map(|entry| entry.node_lifecycle.clone())
                    .ok_or_else(|| ControlPlaneError::rpc_remote(format!(
                            "in-memory test authority capability directory has no node-lifecycle node {node_id}"
                        )))
        })();
        Box::pin(std::future::ready(result))
    }
}

impl ControlPlaneRaftLinearizedAuthorityDirectory for InMemoryAuthorityCapabilityDirectory {
    fn linearized_authority_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError>>
    {
        let result = (|| {
            let entries = self.entries.lock().map_err(|_| {
                ControlPlaneError::rpc_remote(
                    "in-memory test authority capability directory lock poisoned".to_string(),
                )
            })?;
            entries
                    .get(&node_id)
                    .map(|entry| entry.linearized_authority.clone())
                    .ok_or_else(|| ControlPlaneError::rpc_remote(format!(
                            "in-memory test authority capability directory has no linearized node {node_id}"
                        )))
        })();
        Box::pin(std::future::ready(result))
    }
}

impl ControlPlaneRaftLeaderRoutedAdminDirectory for InMemoryAuthorityCapabilityDirectory {
    fn leader_routed_admin_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftLeaderRoutedAdminHandle, ControlPlaneError>,
    > {
        let result = (|| {
            let entries = self.entries.lock().map_err(|_| {
                ControlPlaneError::rpc_remote(
                    "in-memory test authority capability directory lock poisoned".to_string(),
                )
            })?;
            entries
                    .get(&node_id)
                    .map(|entry| entry.leader_routed_admin.clone())
                    .ok_or_else(|| ControlPlaneError::rpc_remote(format!(
                            "in-memory test authority capability directory has no leader-routed admin node {node_id}"
                        )))
        })();
        Box::pin(std::future::ready(result))
    }
}

#[derive(Clone)]
struct FixedLinearizedAuthorityDirectory {
    authority: ControlPlaneRaftAuthorityHandle,
}

impl ControlPlaneRaftLinearizedAuthorityDirectory for FixedLinearizedAuthorityDirectory {
    fn linearized_authority_for_node(
        &self,
        _node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError>>
    {
        Box::pin(std::future::ready(Ok(self.authority.clone())))
    }
}

impl RaftNetworkFactory<ControlPlaneRaftTypeConfig> for InMemoryRaftNetworkFactory {
    type Network = InMemoryRaftNetwork;

    async fn new_client(
        &mut self,
        target: ControlPlaneRaftNodeId,
        node: &BasicNode,
    ) -> Self::Network {
        InMemoryRaftNetwork {
            peers: self.peers.clone(),
            policy: self.policy.clone(),
            source: self.local_node_id,
            target,
            node: node.clone(),
            max_append_entries_seen: Arc::clone(&self.max_append_entries_seen),
        }
    }
}

#[derive(Clone)]
struct InMemoryRaftNetwork {
    peers: Arc<
        Mutex<
            BTreeMap<
                ControlPlaneRaftNodeId,
                Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
            >,
        >,
    >,
    policy: Option<Arc<ControlPlaneRaftPeerTransportPolicy>>,
    source: Option<ControlPlaneRaftNodeId>,
    target: ControlPlaneRaftNodeId,
    node: BasicNode,
    max_append_entries_seen: Arc<AtomicUsize>,
}

impl InMemoryRaftNetwork {
    fn encoded_append_entries_payload_len(
        entries: &[ControlPlaneRaftEntry],
    ) -> Result<usize, RPCError<ControlPlaneRaftTypeConfig>> {
        let mut encoded = Vec::new();
        write_raft_u32(
            &mut encoded,
            raft_len_as_u32(entries.len(), "raft append entries").map_err(|error| {
                RPCError::Network(NetworkError::from_string(format!(
                    "in-memory test raft network append_entries encode failed: {error:?}"
                )))
            })?,
        );
        for entry in entries {
            write_raft_entry(&mut encoded, entry).map_err(|error| {
                RPCError::Network(NetworkError::from_string(format!(
                    "in-memory test raft network append_entries encode failed: {error:?}"
                )))
            })?;
        }
        Ok(encoded.len())
    }

    fn validate_peer(
        &self,
        rpc_name: &'static str,
    ) -> Result<(), RPCError<ControlPlaneRaftTypeConfig>> {
        if let Some(policy) = &self.policy {
            policy
                .validate_target_node(self.target, &self.node, rpc_name)
                .map_err(Self::rpc_error_from_transport_rejection)?;
        }
        Ok(())
    }

    fn request_identity(
        &self,
    ) -> Result<Option<ControlPlaneRaftPeerFrameIdentity>, RPCError<ControlPlaneRaftTypeConfig>>
    {
        let Some(policy) = &self.policy else {
            return Ok(None);
        };
        let source = self.source.ok_or_else(|| {
            RPCError::Network(NetworkError::from_string(
                "in-memory test raft network has no local source node for peer frame identity",
            ))
        })?;
        policy
            .frame_identity(source, self.target)
            .map(Some)
            .map_err(Self::rpc_error_from_transport_rejection)
    }

    fn response_identity(
        &self,
    ) -> Result<Option<ControlPlaneRaftPeerFrameIdentity>, RPCError<ControlPlaneRaftTypeConfig>>
    {
        let Some(policy) = &self.policy else {
            return Ok(None);
        };
        let source = self.source.ok_or_else(|| {
            RPCError::Network(NetworkError::from_string(
                "in-memory test raft network has no local source node for peer frame identity",
            ))
        })?;
        policy
            .frame_identity(self.target, source)
            .map(Some)
            .map_err(Self::rpc_error_from_transport_rejection)
    }

    fn snapshot_request_identity(
        &self,
    ) -> Result<Option<ControlPlaneRaftPeerFrameIdentity>, StreamingError<ControlPlaneRaftTypeConfig>>
    {
        self.request_identity()
            .map_err(Self::streaming_error_from_rpc_error)
    }

    fn snapshot_response_identity(
        &self,
    ) -> Result<Option<ControlPlaneRaftPeerFrameIdentity>, StreamingError<ControlPlaneRaftTypeConfig>>
    {
        self.response_identity()
            .map_err(Self::streaming_error_from_rpc_error)
    }

    fn streaming_error_from_rpc_error(
        error: RPCError<ControlPlaneRaftTypeConfig>,
    ) -> StreamingError<ControlPlaneRaftTypeConfig> {
        match error {
            RPCError::Timeout(error) => StreamingError::Network(NetworkError::from_string(
                format!("peer identity validation timed out: {error}"),
            )),
            RPCError::Unreachable(error) => StreamingError::Unreachable(error),
            RPCError::Network(error) => StreamingError::Network(error),
            RPCError::RemoteError(error) => StreamingError::Network(NetworkError::from_string(
                format!("peer identity validation remote error: {error}"),
            )),
        }
    }

    fn rpc_error_from_transport_rejection(
        rejection: ControlPlaneRaftPeerTransportRejection,
    ) -> RPCError<ControlPlaneRaftTypeConfig> {
        let message = rejection.to_string();
        match rejection {
            ControlPlaneRaftPeerTransportRejection::UnknownTarget { .. } => {
                RPCError::Unreachable(Unreachable::new(&AnyError::error(message)))
            }
            _ => RPCError::Network(NetworkError::from_string(message)),
        }
    }

    fn streaming_error_from_transport_rejection(
        rejection: ControlPlaneRaftPeerTransportRejection,
    ) -> StreamingError<ControlPlaneRaftTypeConfig> {
        let message = rejection.to_string();
        match rejection {
            ControlPlaneRaftPeerTransportRejection::UnknownTarget { .. } => {
                StreamingError::Unreachable(Unreachable::new(&AnyError::error(message)))
            }
            _ => StreamingError::Network(NetworkError::from_string(message)),
        }
    }

    fn rpc_protocol_error(
        context: &'static str,
        error: ControlPlaneError,
    ) -> RPCError<ControlPlaneRaftTypeConfig> {
        RPCError::Network(NetworkError::from_string(format!(
            "in-memory test raft network {context} peer frame failed: {error:?}"
        )))
    }

    fn streaming_protocol_error(
        context: &'static str,
        error: ControlPlaneError,
    ) -> StreamingError<ControlPlaneRaftTypeConfig> {
        StreamingError::Network(NetworkError::from_string(format!(
            "in-memory test raft network {context} peer frame failed: {error:?}"
        )))
    }

    fn target_raft(
        &self,
        rpc_name: &'static str,
    ) -> Result<
        Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
        RPCError<ControlPlaneRaftTypeConfig>,
    > {
        let peers = self.peers.lock().map_err(|_| {
            RPCError::Network(NetworkError::from_string(
                "in-memory test raft network registry lock poisoned",
            ))
        })?;
        peers.get(&self.target).cloned().ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&AnyError::error(format!(
                "in-memory test raft network has no target {} for {rpc_name}",
                self.target
            ))))
        })
    }
}

impl fmt::Debug for InMemoryRaftNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryRaftNetwork")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl RaftNetworkV2<ControlPlaneRaftTypeConfig> for InMemoryRaftNetwork {
    type SnapshotData = ControlPlaneRaftSnapshotData;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
        RPCError<ControlPlaneRaftTypeConfig>,
    > {
        self.max_append_entries_seen
            .fetch_max(rpc.entries.len(), Ordering::Relaxed);
        self.validate_peer("append_entries")?;
        if let Some(policy) = &self.policy {
            policy
                .validate_append_entries(
                    self.target,
                    rpc.entries.len(),
                    Self::encoded_append_entries_payload_len(&rpc.entries)?,
                )
                .map_err(Self::rpc_error_from_transport_rejection)?;
        }
        let request_identity = self.request_identity()?;
        let encoded = ControlPlaneRaftPeerRpcRequest::AppendEntries(rpc)
            .encode_frame_with_identity(request_identity.as_ref())
            .map_err(|error| Self::rpc_protocol_error("append_entries encode", error))?;
        let response_frame = handle_control_plane_raft_peer_rpc_frame_with_identity(
            &self.target_raft("append_entries")?,
            &encoded,
            request_identity.as_ref(),
        )
        .await
        .map_err(|error| Self::rpc_protocol_error("append_entries dispatch", error))?;
        let response_identity = self.response_identity()?;
        let ControlPlaneRaftPeerRpcResponse::AppendEntries(response) =
            ControlPlaneRaftPeerRpcResponse::decode_frame_with_identity(
                &response_frame,
                response_identity.as_ref(),
            )
            .map_err(|error| Self::rpc_protocol_error("append_entries response decode", error))?
        else {
            return Err(Self::rpc_protocol_error(
                "append_entries response decode",
                raft_artifact_protocol_error("decoded non-append_entries response frame"),
            ));
        };
        Ok(response)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
    {
        self.validate_peer("vote")?;
        let request_identity = self.request_identity()?;
        let encoded = ControlPlaneRaftPeerRpcRequest::Vote(rpc)
            .encode_frame_with_identity(request_identity.as_ref())
            .map_err(|error| Self::rpc_protocol_error("vote encode", error))?;
        let response_frame = handle_control_plane_raft_peer_rpc_frame_with_identity(
            &self.target_raft("vote")?,
            &encoded,
            request_identity.as_ref(),
        )
        .await
        .map_err(|error| Self::rpc_protocol_error("vote dispatch", error))?;
        let response_identity = self.response_identity()?;
        let ControlPlaneRaftPeerRpcResponse::Vote(response) =
            ControlPlaneRaftPeerRpcResponse::decode_frame_with_identity(
                &response_frame,
                response_identity.as_ref(),
            )
            .map_err(|error| Self::rpc_protocol_error("vote response decode", error))?
        else {
            return Err(Self::rpc_protocol_error(
                "vote response decode",
                raft_artifact_protocol_error("decoded non-vote response frame"),
            ));
        };
        Ok(response)
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
    {
        self.validate_peer("pre_vote")?;
        let request_identity = self.request_identity()?;
        let encoded = ControlPlaneRaftPeerRpcRequest::PreVote(rpc)
            .encode_frame_with_identity(request_identity.as_ref())
            .map_err(|error| Self::rpc_protocol_error("pre_vote encode", error))?;
        let response_frame = handle_control_plane_raft_peer_rpc_frame_with_identity(
            &self.target_raft("pre_vote")?,
            &encoded,
            request_identity.as_ref(),
        )
        .await
        .map_err(|error| Self::rpc_protocol_error("pre_vote dispatch", error))?;
        let response_identity = self.response_identity()?;
        let ControlPlaneRaftPeerRpcResponse::Vote(response) =
            ControlPlaneRaftPeerRpcResponse::decode_frame_with_identity(
                &response_frame,
                response_identity.as_ref(),
            )
            .map_err(|error| Self::rpc_protocol_error("pre_vote response decode", error))?
        else {
            return Err(Self::rpc_protocol_error(
                "pre_vote response decode",
                raft_artifact_protocol_error("decoded non-vote response frame"),
            ));
        };
        Ok(response)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<ControlPlaneRaftTypeConfig>,
        snapshot: ControlPlaneRaftSnapshot,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<
        SnapshotResponse<ControlPlaneRaftTypeConfig>,
        StreamingError<ControlPlaneRaftTypeConfig>,
    > {
        self.validate_peer("full_snapshot")
            .map_err(|error| match error {
                RPCError::Timeout(error) => StreamingError::Network(NetworkError::from_string(
                    format!("full_snapshot peer validation timed out: {error}"),
                )),
                RPCError::Unreachable(error) => StreamingError::Unreachable(error),
                RPCError::Network(error) => StreamingError::Network(error),
                RPCError::RemoteError(error) => StreamingError::Network(NetworkError::from_string(
                    format!("full_snapshot peer validation remote error: {error}"),
                )),
            })?;
        if let Some(policy) = &self.policy {
            policy
                .validate_snapshot(self.target, snapshot.snapshot.get_ref().len())
                .map_err(Self::streaming_error_from_transport_rejection)?;
        }
        let max_snapshot_bytes = self
            .policy
            .as_ref()
            .map_or(usize::MAX, |policy| policy.limits.max_snapshot_bytes);
        let request = ControlPlaneRaftPeerSnapshotRequest { vote, snapshot };
        let request_identity = self.snapshot_request_identity()?;
        let encoded = request
            .encode_frame_with_identity(request_identity.as_ref())
            .map_err(|error| Self::streaming_protocol_error("full_snapshot encode", error))?;
        let response_frame = handle_control_plane_raft_peer_snapshot_frame_with_identity(
            &self.target_raft("full_snapshot")?,
            &encoded,
            usize::MAX,
            max_snapshot_bytes,
            request_identity.as_ref(),
        )
        .await
        .map_err(|error| Self::streaming_protocol_error("full_snapshot dispatch", error))?;
        let response_identity = self.snapshot_response_identity()?;
        let response = ControlPlaneRaftPeerSnapshotResponse::decode_frame_with_identity(
            &response_frame,
            response_identity.as_ref(),
        )
        .map_err(|error| Self::streaming_protocol_error("full_snapshot response decode", error))?
        .response;
        Ok(response)
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<ControlPlaneRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        TransferLeaderResponse<ControlPlaneRaftTypeConfig>,
        RPCError<ControlPlaneRaftTypeConfig>,
    > {
        self.validate_peer("transfer_leader")?;
        let request_identity = self.request_identity()?;
        let encoded = ControlPlaneRaftPeerRpcRequest::TransferLeader(req)
            .encode_frame_with_identity(request_identity.as_ref())
            .map_err(|error| Self::rpc_protocol_error("transfer_leader encode", error))?;
        let response_frame = handle_control_plane_raft_peer_rpc_frame_with_identity(
            &self.target_raft("transfer_leader")?,
            &encoded,
            request_identity.as_ref(),
        )
        .await
        .map_err(|error| Self::rpc_protocol_error("transfer_leader dispatch", error))?;
        let response_identity = self.response_identity()?;
        let ControlPlaneRaftPeerRpcResponse::TransferLeader(response) =
            ControlPlaneRaftPeerRpcResponse::decode_frame_with_identity(
                &response_frame,
                response_identity.as_ref(),
            )
            .map_err(|error| Self::rpc_protocol_error("transfer_leader response decode", error))?
        else {
            return Err(Self::rpc_protocol_error(
                "transfer_leader response decode",
                raft_artifact_protocol_error("decoded non-transfer_leader response frame"),
            ));
        };
        Ok(response)
    }
}

fn test_peer_transport_policy() -> ControlPlaneRaftPeerTransportPolicy {
    ControlPlaneRaftPeerTransportPolicy::new(
        "control-plane-raft-peer-transport-test",
        BTreeMap::from([(1, BasicNode::new("node-1")), (2, BasicNode::new("node-2"))]),
        ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: 4096,
            max_append_entries: 1,
            max_append_entries_bytes: 128,
            max_snapshot_bytes: 0,
        },
    )
}

fn test_peer_scoped_credential(node_id: ControlPlaneRaftNodeId) -> ControlPlaneScopedCredential {
    ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
        cluster_id: "control-plane-raft-peer-transport-test".to_string(),
        credential_id: format!("raft-peer-{node_id}"),
        credential_version: 1,
        principal: ControlPlaneAuthPrincipal::RaftPeer { node_id },
        secret: format!("test-raft-peer-secret-{node_id}").into_bytes(),
    })
    .unwrap()
}

fn test_peer_auth_policy(local_node_id: ControlPlaneRaftNodeId) -> ControlPlaneRaftPeerAuthPolicy {
    let credentials = vec![
        test_peer_scoped_credential(1),
        test_peer_scoped_credential(2),
    ];
    let local_credential = credentials
        .iter()
        .find(|credential| {
            credential.principal()
                == &ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: local_node_id,
                }
        })
        .unwrap()
        .clone();
    ControlPlaneRaftPeerAuthPolicy::new(
        local_credential,
        ControlPlaneScopedCredentialStore::new(credentials).unwrap(),
    )
    .unwrap()
}

async fn test_policy_network_client(
    target: ControlPlaneRaftNodeId,
    node: &BasicNode,
) -> InMemoryRaftNetwork {
    let mut factory =
        InMemoryRaftNetworkFactory::with_transport_policy(test_peer_transport_policy())
            .for_local_node(1);
    factory.new_client(target, node).await
}

fn refresh_raft_peer_frame_checksum(frame: &mut Vec<u8>) {
    let checksum_start = frame.len() - CONTROL_PLANE_RAFT_PEER_RPC_CHECKSUM_LEN;
    frame.truncate(checksum_start);
    append_raft_artifact_checksum(frame);
}

fn raft_unix_socket_path(test_name: &str) -> (test_util::TempDir, PathBuf) {
    let socket_dir = test_util::tempdir();
    let socket_path = socket_dir.path().join(format!("{test_name}.sock"));
    (socket_dir, socket_path)
}

struct TestUnixPeerListener {
    socket_path: PathBuf,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl TestUnixPeerListener {
    fn spawn(
        socket_path: PathBuf,
        authority: Arc<ControlPlaneRaftAuthority>,
        local_node_id: ControlPlaneRaftNodeId,
        policy: ControlPlaneRaftPeerTransportPolicy,
    ) -> Self {
        let _ = std::fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_socket_path = socket_path.clone();
        let policy = Arc::new(policy);
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let result = ControlPlaneRaftTypeConfig::run(async {
                            handle_control_plane_raft_peer_unix_stream_from_configured_peer(
                                authority.raft(),
                                &mut stream,
                                local_node_id,
                                &policy,
                                Duration::from_secs(1),
                            )
                            .await
                        });
                        if let Err(error) = result {
                            eprintln!(
                                "test OpenRaft Unix peer listener {local_node_id} failed: {error}"
                            );
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!(
                        "test OpenRaft Unix peer listener {local_node_id} accept failed: {error}"
                    ),
                }
            }
            let _ = std::fs::remove_file(worker_socket_path);
        });

        Self {
            socket_path,
            stop,
            worker: Some(worker),
        }
    }
}

impl Drop for TestUnixPeerListener {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = UnixStream::connect(&self.socket_path);
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}

include!("tests/peer.rs");
fn test_raft_config_with_log_reversion(
    cluster_name: &'static str,
    allow_log_reversion: Option<bool>,
) -> Arc<Config> {
    Arc::new(
        Config {
            cluster_name: cluster_name.to_string(),
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            enable_tick: false,
            enable_heartbeat: false,
            enable_elect: false,
            allow_log_reversion,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

async fn wait_for_local_leader(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    message: &'static str,
) {
    raft.wait(Some(IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT))
        .state(ServerState::Leader, message)
        .await
        .unwrap();
    raft.as_leader()
        .expect("local raft should have a committed leader vote");
}

async fn initialized_two_node_authorities(
    cluster_name: &'static str,
    node1: ControlPlaneRaftNodeId,
    node2: ControlPlaneRaftNodeId,
) -> (ControlPlaneRaftAuthority, ControlPlaneRaftAuthority) {
    let network = InMemoryRaftNetworkFactory::default();
    let config = test_raft_config(cluster_name);
    let log_store1 = ControlPlaneRaftLogStore::empty();
    let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
        node1,
        config.clone(),
        network.clone(),
        log_store1.clone(),
        ControlPlaneRaftStateMachine::empty(),
    )
    .await
    .unwrap();
    let log_store2 = ControlPlaneRaftLogStore::empty();
    let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
        node2,
        config,
        network.clone(),
        log_store2.clone(),
        ControlPlaneRaftStateMachine::empty(),
    )
    .await
    .unwrap();
    network.register(node1, raft1.clone());
    network.register(node2, raft2.clone());
    let authority1 = ControlPlaneRaftAuthority::new_with_log_store(raft1, log_store1, cluster_name);
    let authority2 = ControlPlaneRaftAuthority::new_with_log_store(raft2, log_store2, cluster_name);

    authority1
        .initialize_membership(BTreeMap::from([
            (node1, BasicNode::new(format!("node-{node1}"))),
            (node2, BasicNode::new(format!("node-{node2}"))),
        ]))
        .await
        .unwrap();
    wait_for_local_leader(authority1.raft(), "two-node initialized leadership").await;
    wait_for_authority_status_matching(
        &authority1,
        IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
        "two-node leader applies initialization",
        ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
    )
    .await;

    (authority1, authority2)
}

async fn initialized_two_node_checkpoint_authorities(
    cluster_name: &'static str,
    node1: ControlPlaneRaftNodeId,
    node2: ControlPlaneRaftNodeId,
    artifact1: &Path,
    artifact2: &Path,
) -> (ControlPlaneRaftAuthority, ControlPlaneRaftAuthority) {
    let network = InMemoryRaftNetworkFactory::default();
    let config = test_raft_config(cluster_name);
    let log_store1 = ControlPlaneRaftLogStore::empty();
    let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
        node1,
        config.clone(),
        network.clone(),
        log_store1.clone(),
        ControlPlaneRaftStateMachine::empty(),
    )
    .await
    .unwrap();
    let log_store2 = ControlPlaneRaftLogStore::empty();
    let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
        node2,
        config,
        network.clone(),
        log_store2.clone(),
        ControlPlaneRaftStateMachine::empty(),
    )
    .await
    .unwrap();
    network.register(node1, raft1.clone());
    network.register(node2, raft2.clone());
    let authority1 = ControlPlaneRaftAuthority::new_with_log_store(raft1, log_store1, cluster_name)
        .with_durable_artifact_path(artifact1);
    let authority2 = ControlPlaneRaftAuthority::new_with_log_store(raft2, log_store2, cluster_name)
        .with_durable_artifact_path(artifact2);

    authority1
        .initialize_membership(BTreeMap::from([
            (node1, BasicNode::new(format!("node-{node1}"))),
            (node2, BasicNode::new(format!("node-{node2}"))),
        ]))
        .await
        .unwrap();
    wait_for_local_leader(authority1.raft(), "two-node checkpoint leadership").await;
    wait_for_authority_status_matching(
        &authority1,
        IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
        "two-node checkpoint leader applies initialization",
        ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
    )
    .await;
    let initialized = authority1.status().await.unwrap().applied().unwrap();
    authority2
        .wait_for_applied_log_id(
            initialized,
            IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
            "two-node checkpoint follower applies initialization",
        )
        .await
        .unwrap();

    (authority1, authority2)
}

async fn initialized_three_node_cluster_with_two_voters(
    cluster_name: &'static str,
    node1: ControlPlaneRaftNodeId,
    node2: ControlPlaneRaftNodeId,
    node3: ControlPlaneRaftNodeId,
) -> (
    ControlPlaneRaftAuthority,
    ControlPlaneRaftAuthority,
    ControlPlaneRaftAuthority,
) {
    let network = InMemoryRaftNetworkFactory::default();
    let config = test_raft_config(cluster_name);
    let log_store1 = ControlPlaneRaftLogStore::empty();
    let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
        node1,
        config.clone(),
        network.clone(),
        log_store1.clone(),
        ControlPlaneRaftStateMachine::empty(),
    )
    .await
    .unwrap();
    let log_store2 = ControlPlaneRaftLogStore::empty();
    let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
        node2,
        config.clone(),
        network.clone(),
        log_store2.clone(),
        ControlPlaneRaftStateMachine::empty(),
    )
    .await
    .unwrap();
    let log_store3 = ControlPlaneRaftLogStore::empty();
    let raft3 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
        node3,
        config,
        network.clone(),
        log_store3.clone(),
        ControlPlaneRaftStateMachine::empty(),
    )
    .await
    .unwrap();
    network.register(node1, raft1.clone());
    network.register(node2, raft2.clone());
    network.register(node3, raft3.clone());
    let authority1 = ControlPlaneRaftAuthority::new_with_log_store(raft1, log_store1, cluster_name);
    let authority2 = ControlPlaneRaftAuthority::new_with_log_store(raft2, log_store2, cluster_name);
    let authority3 = ControlPlaneRaftAuthority::new_with_log_store(raft3, log_store3, cluster_name);

    authority1
        .initialize_membership(BTreeMap::from([
            (node1, BasicNode::new(format!("node-{node1}"))),
            (node2, BasicNode::new(format!("node-{node2}"))),
        ]))
        .await
        .unwrap();
    wait_for_local_leader(authority1.raft(), "three-node initialized leadership").await;
    wait_for_authority_status_matching(
        &authority1,
        IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
        "three-node leader applies initialization",
        ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
    )
    .await;

    (authority1, authority2, authority3)
}

struct ThreeVoterAuthorityFixture {
    network: InMemoryRaftNetworkFactory,
    config: Arc<Config>,
    leader_log_store: ControlPlaneRaftLogStore,
    third_log_store: ControlPlaneRaftLogStore,
    authority1: ControlPlaneRaftAuthority,
    authority2: ControlPlaneRaftAuthority,
    authority3: ControlPlaneRaftAuthority,
}

async fn initialized_three_node_voter_authorities(
    cluster_name: &'static str,
    node1: ControlPlaneRaftNodeId,
    node2: ControlPlaneRaftNodeId,
    node3: ControlPlaneRaftNodeId,
) -> ThreeVoterAuthorityFixture {
    initialized_three_node_voter_authorities_with_config(
        test_raft_config(cluster_name),
        node1,
        node2,
        node3,
    )
    .await
}

async fn initialized_three_node_voter_authorities_with_config(
    config: Arc<Config>,
    node1: ControlPlaneRaftNodeId,
    node2: ControlPlaneRaftNodeId,
    node3: ControlPlaneRaftNodeId,
) -> ThreeVoterAuthorityFixture {
    let network = InMemoryRaftNetworkFactory::default();
    let leader_log_store = ControlPlaneRaftLogStore::empty();
    let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
        node1,
        config.clone(),
        network.clone(),
        leader_log_store.clone(),
        ControlPlaneRaftStateMachine::empty(),
    )
    .await
    .unwrap();
    let log_store2 = ControlPlaneRaftLogStore::empty();
    let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
        node2,
        config.clone(),
        network.clone(),
        log_store2.clone(),
        ControlPlaneRaftStateMachine::empty(),
    )
    .await
    .unwrap();
    let log_store3 = ControlPlaneRaftLogStore::empty();
    let raft3 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
        node3,
        config.clone(),
        network.clone(),
        log_store3.clone(),
        ControlPlaneRaftStateMachine::empty(),
    )
    .await
    .unwrap();
    network.register(node1, raft1.clone());
    network.register(node2, raft2.clone());
    network.register(node3, raft3.clone());
    let authority1 = ControlPlaneRaftAuthority::new_with_log_store(
        raft1,
        leader_log_store.clone(),
        "test-cluster",
    );
    let authority2 =
        ControlPlaneRaftAuthority::new_with_log_store(raft2, log_store2, "test-cluster");
    let authority3 =
        ControlPlaneRaftAuthority::new_with_log_store(raft3, log_store3.clone(), "test-cluster");

    authority1
        .initialize_membership(BTreeMap::from([
            (node1, BasicNode::new(format!("node-{node1}"))),
            (node2, BasicNode::new(format!("node-{node2}"))),
            (node3, BasicNode::new(format!("node-{node3}"))),
        ]))
        .await
        .unwrap();
    wait_for_local_leader(authority1.raft(), "three-voter initialized leadership").await;
    wait_for_authority_status_matching(
        &authority1,
        IN_MEMORY_RAFT_FIXTURE_CONVERGENCE_TIMEOUT,
        "three-voter leader applies initialization",
        ControlPlaneRaftAuthorityStatus::linearized_authority_serving,
    )
    .await;

    ThreeVoterAuthorityFixture {
        network,
        config,
        leader_log_store,
        third_log_store: log_store3,
        authority1,
        authority2,
        authority3,
    }
}

async fn capture_openraft_restart_artifact(
    log_store: &ControlPlaneRaftLogStore,
    authority: &ControlPlaneRaftAuthority,
) -> ControlPlaneRaftRestartArtifact {
    let log_store = log_store.export_restart_artifact().unwrap();
    let state_machine = authority
        .raft()
        .with_state_machine(|state_machine| {
            let artifact = state_machine.export_restart_artifact();
            Box::pin(async move { artifact })
        })
        .await
        .unwrap();
    ControlPlaneRaftRestartArtifact {
        cluster_name: authority.cluster_name.clone(),
        local_node_id: authority.node_id,
        wal_replay_offset: 0,
        log_store,
        state_machine,
    }
}

#[test]
fn control_plane_raft_linearized_authority_readiness_from_flags_is_ordered() {
    assert_eq!(
        linearized_authority_readiness_from_flags(false, false, false, false),
        ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
    );
    assert_eq!(
        linearized_authority_readiness_from_flags(false, true, true, true),
        ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
    );
    assert_eq!(
        linearized_authority_readiness_from_flags(true, false, true, true),
        ControlPlaneRaftLinearizedAuthorityReadiness::NotEffectiveVoter
    );
    assert_eq!(
        linearized_authority_readiness_from_flags(true, true, false, true),
        ControlPlaneRaftLinearizedAuthorityReadiness::NotAppliedToCommitted
    );
    assert_eq!(
        linearized_authority_readiness_from_flags(true, true, true, false),
        ControlPlaneRaftLinearizedAuthorityReadiness::NotCommittedInCurrentTerm
    );
    assert_eq!(
        linearized_authority_readiness_from_flags(true, true, true, true),
        ControlPlaneRaftLinearizedAuthorityReadiness::Serving
    );
}

#[test]
fn control_plane_raft_restart_capture_retry_uses_elapsed_time_budget() {
    let started = Instant::now();
    let deadline = started + CONTROL_PLANE_RAFT_RESTART_CAPTURE_RETRY_BUDGET;
    assert_eq!(
        control_plane_raft_restart_capture_retry_delay(started, deadline),
        Some(CONTROL_PLANE_RAFT_RESTART_CAPTURE_RETRY_DELAY)
    );
    assert_eq!(
        control_plane_raft_restart_capture_retry_delay(
            started + Duration::from_millis(16),
            deadline
        ),
        Some(CONTROL_PLANE_RAFT_RESTART_CAPTURE_RETRY_DELAY),
        "the former 16-attempt boundary must not exhaust the retry budget"
    );
    assert_eq!(
        control_plane_raft_restart_capture_retry_delay(
            deadline - Duration::from_micros(500),
            deadline
        ),
        Some(Duration::from_micros(500)),
        "the final retry sleep must not exceed the remaining budget"
    );
    assert_eq!(
        control_plane_raft_restart_capture_retry_delay(deadline, deadline),
        None
    );
    assert_eq!(
        control_plane_raft_restart_capture_retry_delay(
            deadline + Duration::from_millis(1),
            deadline
        ),
        None
    );
    assert!(control_plane_raft_restart_capture_attempt_allowed(
        0,
        deadline + Duration::from_millis(1),
        deadline
    ));
    assert!(
        !control_plane_raft_restart_capture_attempt_allowed(
            1,
            deadline + Duration::from_millis(1),
            deadline
        ),
        "an overslept final delay must not admit another capture attempt"
    );
}

fn test_authority_status(
    node_id: ControlPlaneRaftNodeId,
    linearized_authority_serving: bool,
) -> ControlPlaneRaftAuthorityStatus {
    let caught_up_log_id = linearized_authority_serving.then(|| raft_log_id(1, node_id, 1));
    ControlPlaneRaftAuthorityStatus {
        node_id,
        current_leader: linearized_authority_serving.then_some(node_id),
        server_state: if linearized_authority_serving {
            ServerState::Leader
        } else {
            ServerState::Follower
        },
        local_leader: linearized_authority_serving,
        effective_voter: linearized_authority_serving,
        effective_learner: false,
        applied_voter: linearized_authority_serving,
        applied_learner: false,
        persisted_vote: None,
        current_term: linearized_authority_serving.then_some(1),
        last_log_id: None,
        last_purged_log_id: None,
        committed: caught_up_log_id,
        applied: caught_up_log_id,
        current_snapshot: None,
        durable_wal_backed: false,
        durable_wal_offsets: None,
        durable_wal_poisoned: None,
        durable_last_vote: None,
        durable_last_log_id: None,
        durable_last_purged_log_id: None,
        durable_committed: None,
        durable_applied: caught_up_log_id,
        durable_timestamp_high_water_ms: None,
        authority_incarnation: AuthorityIncarnation::INITIAL,
        current_cluster_epoch: ClusterEpoch::INITIAL,
        retained_history_count: 0,
        oldest_retained_history_epoch: None,
        newest_retained_history_epoch: None,
        oldest_storage_history_floor_epoch: None,
        storage_node_lease_deadline_count: 0,
        earliest_storage_node_lease_deadline_ms: None,
        latest_storage_node_lease_deadline_ms: None,
        storage_node_count: 0,
        joining_storage_node_count: 0,
        active_storage_node_count: 0,
        draining_storage_node_count: 0,
        out_storage_node_count: 0,
        removed_storage_node_count: 0,
        healthy_storage_node_count: 0,
        suspect_storage_node_count: 0,
        unavailable_storage_node_count: 0,
        pg_count: 0,
        active_pg_count: 0,
        peering_pg_count: 0,
        degraded_pg_count: 0,
        backfilling_pg_count: 0,
        inconsistent_pg_count: 0,
        active_primary_pg_count: 0,
        peering_metadata_transfer_pg_count: 0,
        metadata_transfer_fenced_pg_count: 0,
        metadata_transfer_fence_source_lease_deadline_count: 0,
        earliest_metadata_transfer_fence_source_lease_deadline_ms: None,
        latest_metadata_transfer_fence_source_lease_deadline_ms: None,
        effective_membership_log_id: None,
        effective_voters: linearized_authority_serving
            .then_some(node_id)
            .into_iter()
            .collect(),
        effective_learners: BTreeSet::new(),
        applied_membership_log_id: None,
        applied_voters: linearized_authority_serving
            .then_some(node_id)
            .into_iter()
            .collect(),
        applied_learners: BTreeSet::new(),
    }
}

#[test]
fn control_plane_raft_current_serving_authority_node_id_fails_closed() {
    let statuses = BTreeMap::from([
        (431, test_authority_status(431, false)),
        (432, test_authority_status(432, true)),
    ]);
    assert_eq!(current_serving_authority_node_id(&statuses).unwrap(), 432);

    let no_serving = BTreeMap::from([
        (431, test_authority_status(431, false)),
        (432, test_authority_status(432, false)),
    ]);
    assert!(matches!(
        current_serving_authority_node_id(&no_serving),
        Err(ControlPlaneError::RpcRemote { diagnostic: message })
            if message.contains("no serving raft authority")
    ));

    let key_mismatch = BTreeMap::from([(431, test_authority_status(432, false))]);
    assert!(matches!(
        current_serving_authority_node_id(&key_mismatch),
        Err(ControlPlaneError::RpcRemote { diagnostic: message })
            if message.contains("status key 431 disagrees with reported node 432")
    ));

    let mut lagging_leader = test_authority_status(433, true);
    lagging_leader.committed = Some(raft_log_id(1, 433, 2));
    lagging_leader.applied = Some(raft_log_id(1, 433, 1));
    assert_eq!(
        lagging_leader.linearized_authority_readiness(),
        ControlPlaneRaftLinearizedAuthorityReadiness::NotAppliedToCommitted
    );
    assert!(!lagging_leader.linearized_authority_serving());
    let lagging = BTreeMap::from([(433, lagging_leader)]);
    assert!(matches!(
        current_serving_authority_node_id(&lagging),
        Err(ControlPlaneError::RpcRemote { diagnostic: message })
            if message.contains("no serving raft authority")
    ));

    let mut same_index_different_term = test_authority_status(434, true);
    same_index_different_term.committed = Some(raft_log_id(2, 434, 3));
    same_index_different_term.applied = Some(raft_log_id(1, 434, 3));
    assert_eq!(
        same_index_different_term.linearized_authority_readiness(),
        ControlPlaneRaftLinearizedAuthorityReadiness::NotAppliedToCommitted
    );
    assert!(!same_index_different_term.linearized_authority_serving());
    let same_index_mismatch = BTreeMap::from([(434, same_index_different_term)]);
    assert!(matches!(
        current_serving_authority_node_id(&same_index_mismatch),
        Err(ControlPlaneError::RpcRemote { diagnostic: message })
            if message.contains("no serving raft authority")
    ));

    let mut prior_term_commit = test_authority_status(435, true);
    prior_term_commit.current_term = Some(2);
    assert_eq!(
        prior_term_commit.linearized_authority_readiness(),
        ControlPlaneRaftLinearizedAuthorityReadiness::NotCommittedInCurrentTerm
    );
    assert!(!prior_term_commit.linearized_authority_serving());
}

async fn wait_for_log_purged_to(
    log_store: &ControlPlaneRaftLogStore,
    log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    message: &'static str,
) {
    for _ in 0..100 {
        if log_store.last_purged_log_id().unwrap() == Some(log_id) {
            return;
        }
        ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
    }
    panic!("{message}: log store did not purge through {log_id}");
}

async fn expect_bounded_control_plane_raft<T, Fut>(
    future: Fut,
    timeout: Duration,
    message: &'static str,
) -> T
where
    Fut: Future<Output = Result<T, ControlPlaneError>> + OptionalSend,
{
    match ControlPlaneRaftTypeConfig::timeout(timeout, future).await {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => panic!("{message}: {error:?}"),
        Err(_) => panic!("{message}: timed out after {timeout:?}"),
    }
}

async fn expect_bounded_control_plane_raft_error<T, Fut>(
    future: Fut,
    timeout: Duration,
    message: &'static str,
) -> ControlPlaneError
where
    Fut: Future<Output = Result<T, ControlPlaneError>> + OptionalSend,
{
    match ControlPlaneRaftTypeConfig::timeout(timeout, future).await {
        Ok(Ok(_)) => panic!("{message}: unexpectedly succeeded"),
        Ok(Err(error)) => error,
        Err(_) => panic!("{message}: timed out after {timeout:?}"),
    }
}

async fn retry_transient_openraft_read_index_quorum_failure<T, F, Fut>(
    mut operation: F,
) -> Result<T, ControlPlaneError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ControlPlaneError>>,
{
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if error.is_retryable_openraft_leadership_error() => {
                ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn assert_error_contains<T>(result: Result<T, ControlPlaneError>, expected: &str) {
    match result {
        Ok(_) => panic!("expected error containing {expected:?}, got success"),
        Err(error) => assert!(
            error.retained_diagnostic_contains(expected),
            "expected error {error:?} to contain {expected:?}"
        ),
    }
}

async fn wait_for_authority_status_matching(
    authority: &ControlPlaneRaftAuthority,
    timeout: Duration,
    message: &'static str,
    predicate: impl Fn(&ControlPlaneRaftAuthorityStatus) -> bool + Sync,
) -> ControlPlaneRaftAuthorityStatus {
    match ControlPlaneRaftTypeConfig::timeout(timeout, async {
        loop {
            let status = authority
                .status()
                .await
                .unwrap_or_else(|error| panic!("{message}: failed to read status: {error:?}"));
            if predicate(&status) {
                return status;
            }
            ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    {
        Ok(status) => status,
        Err(_) => {
            let status = authority.status().await;
            panic!("{message}: timed out after {timeout:?}; last status: {status:?}");
        }
    }
}

fn raft_log_id(term: u64, node_id: u64, index: u64) -> LogIdOf<ControlPlaneRaftTypeConfig> {
    LogId::new(LeaderId { term, node_id }, index)
}

fn blank_entry(term: u64, node_id: u64, index: u64) -> ControlPlaneRaftEntry {
    Entry {
        log_id: raft_log_id(term, node_id, index),
        payload: EntryPayload::Blank,
    }
}

fn membership_entry(term: u64, node_id: u64, index: u64) -> ControlPlaneRaftEntry {
    Entry {
        log_id: raft_log_id(term, node_id, index),
        payload: EntryPayload::Membership(Membership::new_with_defaults(
            vec![BTreeSet::from([1, 2])],
            [],
        )),
    }
}

fn single_node_membership_entry(term: u64, node_id: u64, index: u64) -> ControlPlaneRaftEntry {
    Entry {
        log_id: raft_log_id(term, node_id, index),
        payload: EntryPayload::Membership(Membership::new_with_defaults(
            vec![BTreeSet::from([node_id])],
            [],
        )),
    }
}

fn bootstrap_membership_entry(node_id: u64) -> ControlPlaneRaftEntry {
    membership_entry(0, node_id, 0)
}

fn single_node_bootstrap_membership_entry(node_id: u64) -> ControlPlaneRaftEntry {
    single_node_membership_entry(0, node_id, 0)
}

fn normal_entry(
    term: u64,
    node_id: u64,
    index: u64,
    command: ControlPlaneCommand,
) -> ControlPlaneRaftEntry {
    Entry {
        log_id: raft_log_id(term, node_id, index),
        payload: EntryPayload::Normal(command),
    }
}

fn test_membership() -> Membership<ControlPlaneRaftNodeId, BasicNode> {
    Membership::new_with_defaults(vec![BTreeSet::from([1, 2])], [])
}

fn policy_membership(
    policy: &ControlPlaneRaftPeerTransportPolicy,
) -> Membership<ControlPlaneRaftNodeId, BasicNode> {
    Membership::from(policy.peers())
}

fn policy_bootstrap_membership_entry(
    node_id: u64,
    policy: &ControlPlaneRaftPeerTransportPolicy,
) -> ControlPlaneRaftEntry {
    Entry {
        log_id: raft_log_id(0, node_id, 0),
        payload: EntryPayload::Membership(policy_membership(policy)),
    }
}

fn replicated_state_machine_with_noops(
    term: u64,
    through_index: u64,
) -> ReplicatedControlPlaneStateMachine {
    let mut state_machine = ReplicatedControlPlaneStateMachine::empty();
    for index in 1..=through_index {
        state_machine
            .apply_committed_noop(ControlPlaneLogId::new(term, index).unwrap())
            .unwrap();
    }
    state_machine
}

fn state_machine_restart_artifact_with_noops(
    term: u64,
    node_id: u64,
    through_index: u64,
) -> ControlPlaneRaftStateMachineRestartArtifact {
    ControlPlaneRaftStateMachineRestartArtifact {
        inner: replicated_state_machine_with_noops(term, through_index),
        last_applied: Some(raft_log_id(term, node_id, through_index)),
        last_membership: StoredMembership::default(),
        current_snapshot: None,
    }
}

fn refresh_raft_wal_frame_checksum(frame: &mut Vec<u8>) {
    let checksum_start = frame.len() - CONTROL_PLANE_RAFT_WAL_CHECKSUM_LEN;
    frame.truncate(checksum_start);
    append_raft_artifact_checksum(frame);
}

fn wal_file_frame_end(bytes: &[u8], start: usize) -> usize {
    let start = if start == 0 && bytes.starts_with(CONTROL_PLANE_RAFT_WAL_FILE_MAGIC) {
        ControlPlaneRaftWalFile::file_header_len()
    } else {
        start
    };
    let frame_len = u32::from_be_bytes(
        bytes[start..start + CONTROL_PLANE_RAFT_WAL_FILE_FRAME_LEN][..std::mem::size_of::<u32>()]
            .try_into()
            .unwrap(),
    );
    let frame_len_check = u32::from_be_bytes(
        bytes[start + std::mem::size_of::<u32>()..start + CONTROL_PLANE_RAFT_WAL_FILE_FRAME_LEN]
            .try_into()
            .unwrap(),
    );
    assert_eq!(frame_len_check, !frame_len);
    let frame_len = frame_len as usize;
    start + CONTROL_PLANE_RAFT_WAL_FILE_FRAME_LEN + frame_len
}

struct PartialFailWriter {
    fail_after: usize,
    written: Vec<u8>,
}

impl Write for PartialFailWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.written.len() >= self.fail_after {
            return Err(io::Error::other("injected partial WAL write failure"));
        }
        let write_len = (self.fail_after - self.written.len()).min(buf.len());
        self.written.extend_from_slice(&buf[..write_len]);
        Ok(write_len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

include!("tests/wal_state_machine.rs");
include!("tests/cluster.rs");
include!("tests/restart.rs");
