// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

const CONTROL_PLANE_RAFT_PEER_SERVER_DEADLINE_EXPIRED: &str =
    "control-plane OpenRaft inbound peer connection deadline expired";

/// Process-hosted checkpoint work invoked by the storage-owned Raft peer
/// server.
///
/// The server supplies its exact issuing authority. Implementations receive no
/// transport, frame, poison, or response-publication access; storage owns those
/// authority-bound operations and their ordering.
pub trait ControlPlaneRaftPeerServerCheckpoint: Send + Sync {
    fn checkpoint_before_snapshot_response(
        &self,
        authority: &ControlPlaneRaftAuthority,
    ) -> Result<(), ControlPlaneError>;
}

/// Opaque peer-server durability capability bound to one Raft authority.
#[derive(Clone)]
pub struct ControlPlaneRaftPeerServerDurability {
    pub(super) authority_instance_id: ControlPlaneRaftAuthorityInstanceId,
    pub(super) publication: ControlPlaneRaftDurabilityPublication,
    pub(super) checkpoint: Arc<dyn ControlPlaneRaftPeerServerCheckpoint>,
}

impl fmt::Debug for ControlPlaneRaftPeerServerDurability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftPeerServerDurability")
            .field("authority", &"<opaque>")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftPeerServerDurability {
    pub(crate) fn validate_authority(
        &self,
        authority: &ControlPlaneRaftAuthority,
    ) -> Result<(), ControlPlaneError> {
        if self.authority_instance_id != authority.authority_instance_id()? {
            return Err(ControlPlaneError::invariant_failure(
                "control-plane Raft peer-server durability belongs to another authority instance",
            ));
        }
        self.publication
            .validate_authority(self.authority_instance_id)
    }

    fn is_poisoned(&self) -> bool {
        self.publication.is_poisoned()
    }

    pub(super) fn checkpoint_before_snapshot_response(
        &self,
        authority: &ControlPlaneRaftAuthority,
    ) -> Result<(), ControlPlaneError> {
        self.validate_authority(authority)?;
        let result = self
            .checkpoint
            .checkpoint_before_snapshot_response(authority);
        if result.is_err() {
            self.publication
                .poison("control-plane Raft snapshot response checkpoint publication failed");
        }
        result
    }

    fn publish_response(
        &self,
        authority: &ControlPlaneRaftAuthority,
        publish: &mut dyn FnMut() -> Result<(), ControlPlaneError>,
    ) -> Result<(), ControlPlaneError> {
        self.validate_authority(authority)?;
        ControlPlaneRpcResponsePublication::publish(&self.publication, publish)
    }
}

#[derive(Clone)]
struct ControlPlaneRaftPeerServerResources {
    pre_auth_byte_budget: Arc<ControlPlaneRaftPeerServerPreAuthByteBudget>,
}

/// Logical policy shared by every inbound listener for one Raft authority.
///
/// Clones share the pre-authentication allocation budget. The transport
/// policy owns peer identity, authentication, and frame limits; the server
/// facade owns the order in which they are applied.
#[derive(Clone)]
pub(crate) struct ControlPlaneRaftPeerServerPolicy {
    local_node_id: ControlPlaneRaftNodeId,
    pub(super) peer_policy: Arc<ControlPlaneRaftPeerTransportPolicy>,
    resources: ControlPlaneRaftPeerServerResources,
    durability: Option<ControlPlaneRaftPeerServerDurability>,
    fatal_error_handler: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl fmt::Debug for ControlPlaneRaftPeerServerPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftPeerServerPolicy")
            .field("local_node_id", &self.local_node_id)
            .field("peer_policy", &"configured")
            .field("durability", &self.durability.is_some())
            .field("fatal_error_handler", &self.fatal_error_handler.is_some())
            .finish()
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlPlaneRaftPeerServerConfigError {
    #[error("control-plane Raft peer server endpoint id must not be empty")]
    EmptyEndpointId,
    #[error("control-plane Raft peer server local node is absent from the peer policy")]
    InvalidLocalNode,
    #[error("control-plane Raft peer server pre-authentication byte budget must be positive")]
    ZeroPreAuthByteBudget,
    #[error("control-plane Raft peer server connection limit must be positive")]
    ZeroConnectionLimit,
    #[error("control-plane Raft peer server I/O timeout must be positive")]
    ZeroIoTimeout,
    #[error("failed to configure the control-plane Raft peer server listener")]
    ListenerConfigurationUnavailable,
    #[error("failed to construct the control-plane Raft TLS server profile")]
    TlsProfileUnavailable,
}

impl ControlPlaneRaftPeerServerPolicy {
    pub fn new(
        local_node_id: ControlPlaneRaftNodeId,
        peer_policy: ControlPlaneRaftPeerTransportPolicy,
        pre_auth_byte_budget: usize,
    ) -> Result<Self, ControlPlaneRaftPeerServerConfigError> {
        peer_policy
            .validate_local_node(local_node_id)
            .map_err(|_| ControlPlaneRaftPeerServerConfigError::InvalidLocalNode)?;
        if pre_auth_byte_budget == 0 {
            return Err(ControlPlaneRaftPeerServerConfigError::ZeroPreAuthByteBudget);
        }
        Ok(Self {
            local_node_id,
            peer_policy: Arc::new(peer_policy),
            resources: ControlPlaneRaftPeerServerResources {
                pre_auth_byte_budget: Arc::new(ControlPlaneRaftPeerServerPreAuthByteBudget::new(
                    pre_auth_byte_budget,
                )),
            },
            durability: None,
            fatal_error_handler: None,
        })
    }

    #[must_use]
    pub fn with_durability(mut self, durability: ControlPlaneRaftPeerServerDurability) -> Self {
        self.durability = Some(durability);
        self
    }

    #[must_use]
    pub fn with_fatal_error_handler(
        mut self,
        fatal_error_handler: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        self.fatal_error_handler = Some(fatal_error_handler);
        self
    }

    #[cfg(test)]
    #[must_use]
    pub fn reserved_pre_auth_bytes(&self) -> usize {
        self.resources.pre_auth_byte_budget.reserved_bytes()
    }
}

#[derive(Debug)]
struct ControlPlaneRaftPeerServerPreAuthByteBudget {
    reserved_bytes: AtomicUsize,
    limit_bytes: usize,
}

impl ControlPlaneRaftPeerServerPreAuthByteBudget {
    fn new(limit_bytes: usize) -> Self {
        Self {
            reserved_bytes: AtomicUsize::new(0),
            limit_bytes,
        }
    }

    fn reserve(
        self: &Arc<Self>,
        frame_bytes: usize,
    ) -> Result<ControlPlaneRaftPeerServerPreAuthByteReservation, ControlPlaneError> {
        let result =
            self.reserved_bytes
                .try_update(Ordering::AcqRel, Ordering::Acquire, |reserved| {
                    reserved
                        .checked_add(frame_bytes)
                        .filter(|total| *total <= self.limit_bytes)
                });
        match result {
            Ok(_) => Ok(ControlPlaneRaftPeerServerPreAuthByteReservation {
                budget: Arc::clone(self),
                frame_bytes,
            }),
            Err(reserved) => Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane OpenRaft peer pre-authentication frame budget exhausted: requested {frame_bytes} bytes with {reserved} of {} bytes reserved",
                    self.limit_bytes
                ))),
        }
    }

    #[cfg(test)]
    fn reserved_bytes(&self) -> usize {
        self.reserved_bytes.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct ControlPlaneRaftPeerServerPreAuthByteReservation {
    budget: Arc<ControlPlaneRaftPeerServerPreAuthByteBudget>,
    frame_bytes: usize,
}

impl Drop for ControlPlaneRaftPeerServerPreAuthByteReservation {
    fn drop(&mut self) {
        self.budget
            .reserved_bytes
            .fetch_sub(self.frame_bytes, Ordering::AcqRel);
    }
}

struct ControlPlaneRaftPeerTlsCertificateResolver {
    certified_key: Arc<CertifiedKey>,
}

impl fmt::Debug for ControlPlaneRaftPeerTlsCertificateResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftPeerTlsCertificateResolver")
            .field("certificate", &"configured")
            .finish()
    }
}

impl rustls::server::ResolvesServerCert for ControlPlaneRaftPeerTlsCertificateResolver {
    fn resolve(&self, _client_hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.certified_key))
    }
}

pub(super) enum ControlPlaneRaftPeerServerListenerKind {
    Unix(UnixListener),
    TlsTcp {
        listener: TcpListener,
        tls_server_config: Arc<rustls::ServerConfig>,
    },
}

/// Opaque bound listener for the storage-owned inbound Raft peer server.
pub(crate) struct ControlPlaneRaftPeerServerListener {
    endpoint_id: String,
    pub(super) kind: ControlPlaneRaftPeerServerListenerKind,
    max_connections: usize,
    io_timeout: Duration,
    active_workers: Arc<AtomicUsize>,
}

impl fmt::Debug for ControlPlaneRaftPeerServerListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let transport = match self.kind {
            ControlPlaneRaftPeerServerListenerKind::Unix(_) => "unix",
            ControlPlaneRaftPeerServerListenerKind::TlsTcp { .. } => "tls-tcp",
        };
        formatter
            .debug_struct("ControlPlaneRaftPeerServerListener")
            .field("endpoint_id", &self.endpoint_id)
            .field("transport", &transport)
            .field("max_connections", &self.max_connections)
            .field("io_timeout", &self.io_timeout)
            .finish()
    }
}

fn validate_control_plane_raft_peer_server_listener(
    endpoint_id: &str,
    max_connections: usize,
    io_timeout: Duration,
) -> Result<(), ControlPlaneRaftPeerServerConfigError> {
    if endpoint_id.is_empty() {
        return Err(ControlPlaneRaftPeerServerConfigError::EmptyEndpointId);
    }
    if max_connections == 0 {
        return Err(ControlPlaneRaftPeerServerConfigError::ZeroConnectionLimit);
    }
    if io_timeout.is_zero() {
        return Err(ControlPlaneRaftPeerServerConfigError::ZeroIoTimeout);
    }
    Ok(())
}

impl ControlPlaneRaftPeerServerListener {
    pub fn unix(
        endpoint_id: impl Into<String>,
        listener: UnixListener,
        max_connections: usize,
        io_timeout: Duration,
    ) -> Result<Self, ControlPlaneRaftPeerServerConfigError> {
        let endpoint_id = endpoint_id.into();
        validate_control_plane_raft_peer_server_listener(
            &endpoint_id,
            max_connections,
            io_timeout,
        )?;
        listener
            .set_nonblocking(false)
            .map_err(|_| ControlPlaneRaftPeerServerConfigError::ListenerConfigurationUnavailable)?;
        Ok(Self {
            endpoint_id,
            kind: ControlPlaneRaftPeerServerListenerKind::Unix(listener),
            max_connections,
            io_timeout,
            active_workers: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn tls_tcp(
        endpoint_id: impl Into<String>,
        listener: TcpListener,
        certified_key: Arc<CertifiedKey>,
        max_connections: usize,
        io_timeout: Duration,
    ) -> Result<Self, ControlPlaneRaftPeerServerConfigError> {
        let endpoint_id = endpoint_id.into();
        validate_control_plane_raft_peer_server_listener(
            &endpoint_id,
            max_connections,
            io_timeout,
        )?;
        listener
            .set_nonblocking(false)
            .map_err(|_| ControlPlaneRaftPeerServerConfigError::ListenerConfigurationUnavailable)?;
        let resolver = ControlPlaneRaftPeerTlsCertificateResolver { certified_key };
        let mut tls_server_config =
            rustls::ServerConfig::builder_with_provider(tls_provider::configured_provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|_| ControlPlaneRaftPeerServerConfigError::TlsProfileUnavailable)?
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(resolver));
        tls_server_config.alpn_protocols = vec![CONTROL_PLANE_RAFT_TLS_ALPN.to_vec()];
        Ok(Self {
            endpoint_id,
            kind: ControlPlaneRaftPeerServerListenerKind::TlsTcp {
                listener,
                tls_server_config: Arc::new(tls_server_config),
            },
            max_connections,
            io_timeout,
            active_workers: Arc::new(AtomicUsize::new(0)),
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Acquire)
    }
}

/// Opaque failure returned when a Raft peer listener can no longer accept
/// connections. Concrete transport diagnostics remain inside storage.
pub(crate) struct ControlPlaneRaftPeerServerError;

impl fmt::Debug for ControlPlaneRaftPeerServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ControlPlaneRaftPeerServerError")
    }
}

impl fmt::Display for ControlPlaneRaftPeerServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("control-plane Raft peer server stopped accepting connections")
    }
}

impl std::error::Error for ControlPlaneRaftPeerServerError {}

/// Opaque semantic peer client for cross-crate process tests.
///
/// The client deliberately exposes no frame, authentication-envelope, or
/// OpenRaft request types. Tests can drive durable peer mutations while the
/// protocol representation remains owned by storage.
#[cfg(any(test, feature = "test-hooks"))]
pub struct ControlPlaneRaftPeerTestClient {
    socket_path: PathBuf,
    identity: ControlPlaneRaftPeerFrameIdentity,
    auth_policy: Option<ControlPlaneRaftPeerAuthPolicy>,
    limits: ControlPlaneRaftPeerTransportLimits,
    io_timeout: Duration,
}

/// In-flight semantic peer request used by process tests that deliberately
/// keep a connection open while observing a crash boundary.
#[cfg(any(test, feature = "test-hooks"))]
pub struct ControlPlaneRaftPendingTestResponse {
    stream: DeadlineStream<UnixStream>,
    response_identity: ControlPlaneRaftPeerFrameIdentity,
    operation: ControlPlaneAuthOperation,
    auth_policy: Option<ControlPlaneRaftPeerAuthPolicy>,
    max_frame_bytes: usize,
}

/// Semantic view of persisted Raft state for cross-crate process tests.
///
/// Restart-artifact and WAL representations remain private to storage. This
/// view exposes only the state that a restarted authority would recover.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneRaftPersistedVoteForTest {
    term: u64,
    node_id: ControlPlaneRaftNodeId,
    committed: bool,
}

#[cfg(any(test, feature = "test-hooks"))]
impl ControlPlaneRaftPersistedVoteForTest {
    #[must_use]
    pub fn term(&self) -> u64 {
        self.term
    }

    #[must_use]
    pub fn node_id(&self) -> ControlPlaneRaftNodeId {
        self.node_id
    }

    #[must_use]
    pub fn committed(&self) -> bool {
        self.committed
    }
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneRaftDurableStateForTest {
    snapshot: ClusterControlSnapshot,
    persisted_vote: Option<ControlPlaneRaftPersistedVoteForTest>,
    last_log_id: Option<ControlPlaneRaftLogId>,
    committed: Option<ControlPlaneRaftLogId>,
    cached_snapshot_log_id: Option<ControlPlaneRaftLogId>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl ControlPlaneRaftDurableStateForTest {
    #[must_use]
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
    }

    #[must_use]
    pub fn persisted_vote(&self) -> Option<&ControlPlaneRaftPersistedVoteForTest> {
        self.persisted_vote.as_ref()
    }

    #[must_use]
    pub fn last_log_id(&self) -> Option<ControlPlaneRaftLogId> {
        self.last_log_id
    }

    #[must_use]
    pub fn committed(&self) -> Option<ControlPlaneRaftLogId> {
        self.committed
    }

    #[must_use]
    pub fn cached_snapshot_log_id(&self) -> Option<ControlPlaneRaftLogId> {
        self.cached_snapshot_log_id
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub fn inspect_control_plane_raft_recovery_state_for_test(
    artifact_path: &Path,
) -> Result<ControlPlaneRaftDurableStateForTest, ControlPlaneError> {
    inspect_control_plane_raft_durable_state_for_test(artifact_path, true)
}

#[cfg(any(test, feature = "test-hooks"))]
pub fn inspect_control_plane_raft_checkpoint_state_for_test(
    artifact_path: &Path,
) -> Result<ControlPlaneRaftDurableStateForTest, ControlPlaneError> {
    inspect_control_plane_raft_durable_state_for_test(artifact_path, false)
}

/// Opaque filesystem fault used by process tests to block one checkpoint
/// publication without exposing storage's temporary-file naming convention.
#[cfg(any(test, feature = "test-hooks"))]
pub struct ControlPlaneRaftCheckpointWriteBlockerForTest {
    path: PathBuf,
}

#[cfg(any(test, feature = "test-hooks"))]
impl ControlPlaneRaftCheckpointWriteBlockerForTest {
    pub fn install(
        artifact_path: &Path,
        writer_process_id: u32,
    ) -> Result<Self, ControlPlaneError> {
        let path = durable_artifact_tmp_path_for_process(artifact_path, writer_process_id);
        fs::create_dir(&path).map_err(|source| {
            ControlPlaneError::io(
                "install control-plane OpenRaft checkpoint write blocker",
                source,
            )
        })?;
        Ok(Self { path })
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for ControlPlaneRaftCheckpointWriteBlockerForTest {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.path);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn inspect_control_plane_raft_durable_state_for_test(
    artifact_path: &Path,
    replay_wal: bool,
) -> Result<ControlPlaneRaftDurableStateForTest, ControlPlaneError> {
    let artifact = ControlPlaneRaftRestartArtifact::load_durable_artifact(artifact_path)?;
    let (log_store, mut state_machine) = if replay_wal {
        let wal = ControlPlaneRaftWalFile::new(ControlPlaneRaftWalFileConfig {
            path: durable_artifact_wal_path(artifact_path),
            cluster_name: artifact.cluster_name.clone(),
            local_node_id: artifact.local_node_id,
        });
        artifact.restore_with_wal_file(wal)?
    } else {
        artifact.restore().map_err(|source| {
            ControlPlaneError::io("inspect control-plane OpenRaft checkpoint state", source)
        })?
    };
    let log_store_artifact = log_store.export_restart_artifact().map_err(|source| {
        ControlPlaneError::io("inspect control-plane OpenRaft recovered log state", source)
    })?;
    if let Some(committed) = log_store_artifact.committed {
        let start = state_machine
            .last_applied()
            .map_or(0, |log_id| log_id.index().saturating_add(1));
        for entry in log_store_artifact.entries.iter().filter(|entry| {
            let index = entry.log_id.index();
            index >= start && index <= committed.index()
        }) {
            state_machine.apply_entry(entry.clone())?;
        }
    }
    let last_log_id = log_store_artifact
        .entries
        .last()
        .map(|entry| entry.log_id)
        .or(log_store_artifact.last_purged_log_id);
    let persisted_vote = log_store_artifact
        .vote
        .map(|vote| ControlPlaneRaftPersistedVoteForTest {
            term: vote.leader_id.term,
            node_id: vote.leader_id.node_id,
            committed: vote.committed,
        });
    let cached_snapshot_log_id = state_machine
        .current_snapshot()
        .and_then(|snapshot| snapshot.meta.last_log_id);
    Ok(ControlPlaneRaftDurableStateForTest {
        snapshot: state_machine.inner().snapshot().clone(),
        persisted_vote,
        last_log_id,
        committed: log_store_artifact.committed,
        cached_snapshot_log_id,
    })
}

#[cfg(any(test, feature = "test-hooks"))]
impl ControlPlaneRaftPendingTestResponse {
    pub fn wait(self) -> Result<(), ControlPlaneError> {
        let operation = self.operation;
        let response = self.receive()?;
        if matches!(
            (operation, response),
            (
                ControlPlaneAuthOperation::RaftAppendEntries,
                ControlPlaneRaftPeerRpcResponse::AppendEntries(_)
            ) | (
                ControlPlaneAuthOperation::RaftVote | ControlPlaneAuthOperation::RaftPreVote,
                ControlPlaneRaftPeerRpcResponse::Vote(_)
            ) | (
                ControlPlaneAuthOperation::RaftTransferLeader,
                ControlPlaneRaftPeerRpcResponse::TransferLeader(_)
            )
        ) {
            Ok(())
        } else {
            Err(ControlPlaneError::rpc_protocol(
                "control-plane Raft peer test received a mismatched response".to_owned(),
            ))
        }
    }

    fn receive(mut self) -> Result<ControlPlaneRaftPeerRpcResponse, ControlPlaneError> {
        let response =
            read_control_plane_raft_peer_transport_frame(&mut self.stream, self.max_frame_bytes)?;
        let response = match &self.auth_policy {
            Some(auth_policy) => auth_policy.verify_peer_frame(
                &response,
                &self.response_identity,
                self.operation,
                self.max_frame_bytes,
            )?,
            None => response,
        };
        ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(&response, &self.response_identity)
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl ControlPlaneRaftPeerTestClient {
    pub fn unix(
        socket_path: impl Into<PathBuf>,
        cluster_name: impl Into<String>,
        source_node_id: ControlPlaneRaftNodeId,
        target_node_id: ControlPlaneRaftNodeId,
        limits: ControlPlaneRaftPeerTransportLimits,
        io_timeout: Duration,
    ) -> Self {
        Self {
            socket_path: socket_path.into(),
            identity: ControlPlaneRaftPeerFrameIdentity::new(
                cluster_name,
                source_node_id,
                target_node_id,
            ),
            auth_policy: None,
            limits,
            io_timeout,
        }
    }

    #[must_use]
    pub(crate) fn with_auth_policy(mut self, auth_policy: ControlPlaneRaftPeerAuthPolicy) -> Self {
        self.auth_policy = Some(auth_policy);
        self
    }

    pub fn send_vote(
        &self,
        term: ControlPlaneRaftTerm,
        last_log_id: Option<ControlPlaneRaftLogId>,
        leadership_transfer: bool,
    ) -> Result<bool, ControlPlaneError> {
        let response = self.exchange(ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(term, self.identity.source),
            last_log_id,
            leadership_transfer,
        }))?;
        match response {
            ControlPlaneRaftPeerRpcResponse::Vote(response) => Ok(response.vote_granted),
            _ => Err(ControlPlaneError::rpc_protocol(
                "control-plane Raft peer test vote received a mismatched response".to_owned(),
            )),
        }
    }

    pub fn append_commands(
        &self,
        term: ControlPlaneRaftTerm,
        previous_log_id: ControlPlaneRaftLogId,
        leader_commit: Option<ControlPlaneRaftLogId>,
        commands: Vec<ControlPlaneCommand>,
    ) -> Result<ControlPlaneRaftLogId, ControlPlaneError> {
        let (last_log_id, request) =
            self.append_request(term, previous_log_id, leader_commit, commands)?;
        self.begin_request(request)?.wait()?;
        Ok(last_log_id)
    }

    pub fn begin_append_commands(
        &self,
        term: ControlPlaneRaftTerm,
        previous_log_id: ControlPlaneRaftLogId,
        leader_commit: Option<ControlPlaneRaftLogId>,
        commands: Vec<ControlPlaneCommand>,
    ) -> Result<(ControlPlaneRaftLogId, ControlPlaneRaftPendingTestResponse), ControlPlaneError>
    {
        let (last_log_id, request) =
            self.append_request(term, previous_log_id, leader_commit, commands)?;
        Ok((last_log_id, self.begin_request(request)?))
    }

    fn append_request(
        &self,
        term: ControlPlaneRaftTerm,
        previous_log_id: ControlPlaneRaftLogId,
        leader_commit: Option<ControlPlaneRaftLogId>,
        commands: Vec<ControlPlaneCommand>,
    ) -> Result<(ControlPlaneRaftLogId, ControlPlaneRaftPeerRpcRequest), ControlPlaneError> {
        let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(term, self.identity.source);
        let mut last_log_id = previous_log_id;
        let entries = commands
            .into_iter()
            .map(|command| {
                last_log_id = LogId::new(
                    vote.leader_id,
                    last_log_id.index().checked_add(1).ok_or_else(|| {
                        ControlPlaneError::rpc_protocol(
                            "control-plane Raft peer test log index overflow".to_owned(),
                        )
                    })?,
                );
                Ok(Entry {
                    log_id: last_log_id,
                    payload: EntryPayload::Normal(command),
                })
            })
            .collect::<Result<Vec<_>, ControlPlaneError>>()?;
        Ok((
            last_log_id,
            ControlPlaneRaftPeerRpcRequest::AppendEntries(AppendEntriesRequest {
                vote,
                prev_log_id: Some(previous_log_id),
                entries,
                leader_commit,
            }),
        ))
    }

    fn exchange(
        &self,
        request: ControlPlaneRaftPeerRpcRequest,
    ) -> Result<ControlPlaneRaftPeerRpcResponse, ControlPlaneError> {
        self.begin_request(request)?.receive()
    }

    fn begin_request(
        &self,
        request: ControlPlaneRaftPeerRpcRequest,
    ) -> Result<ControlPlaneRaftPendingTestResponse, ControlPlaneError> {
        let operation = ControlPlaneRaftPeerNetwork::auth_operation_for_request(&request);
        let request = request.encode_frame_for_peer(&self.identity)?;
        let request = match &self.auth_policy {
            Some(auth_policy) => auth_policy.sign_peer_frame(&self.identity, operation, request)?,
            None => request,
        };
        let stream = UnixStream::connect(&self.socket_path).map_err(|source| {
            ControlPlaneError::io("connect control-plane Raft peer test client", source)
        })?;
        let deadline = Instant::now()
            .checked_add(self.io_timeout)
            .unwrap_or_else(Instant::now);
        let mut stream = DeadlineStream::new(
            stream,
            deadline,
            "control-plane Raft peer test client deadline expired",
        )
        .map_err(|source| {
            ControlPlaneError::io(
                "configure control-plane Raft peer test client deadline",
                source,
            )
        })?;
        write_control_plane_raft_peer_transport_frame(&mut stream, &request)?;
        Ok(ControlPlaneRaftPendingTestResponse {
            stream,
            response_identity: reverse_raft_peer_frame_identity(&self.identity),
            operation,
            auth_policy: self.auth_policy.clone(),
            max_frame_bytes: self.limits.max_frame_bytes,
        })
    }
}

pub(super) trait ControlPlaneRaftPeerServerStream: Read + Write + Send {
    fn begin_response(&mut self, timeout: Duration);

    fn finish_response(&mut self) -> io::Result<()>;
}

impl ControlPlaneRaftPeerServerStream for DeadlineStream<UnixStream> {
    fn begin_response(&mut self, timeout: Duration) {
        self.set_deadline(
            Instant::now()
                .checked_add(timeout)
                .unwrap_or_else(Instant::now),
        );
    }

    fn finish_response(&mut self) -> io::Result<()> {
        self.flush()
    }
}

impl ControlPlaneRaftPeerServerStream
    for rustls::StreamOwned<rustls::ServerConnection, DeadlineStream<TcpStream>>
{
    fn begin_response(&mut self, timeout: Duration) {
        self.sock.set_deadline(
            Instant::now()
                .checked_add(timeout)
                .unwrap_or_else(Instant::now),
        );
    }

    fn finish_response(&mut self) -> io::Result<()> {
        self.conn.send_close_notify();
        self.flush()
    }
}

struct ControlPlaneRaftPeerServerWorkerGuard {
    active_workers: Arc<AtomicUsize>,
}

impl Drop for ControlPlaneRaftPeerServerWorkerGuard {
    fn drop(&mut self) {
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ControlPlaneRaftPeerServerListener {
    pub fn accept_one(
        &self,
        runtime: &tokio::runtime::Handle,
        authority: Arc<ControlPlaneRaftAuthority>,
        policy: &ControlPlaneRaftPeerServerPolicy,
    ) -> Result<(), ControlPlaneRaftPeerServerError> {
        match &self.kind {
            ControlPlaneRaftPeerServerListenerKind::Unix(listener) => match listener.accept() {
                Ok((stream, _)) => spawn_control_plane_raft_peer_server_worker(
                    stream,
                    runtime.clone(),
                    authority,
                    policy.clone(),
                    self.max_connections,
                    self.io_timeout,
                    Arc::clone(&self.active_workers),
                    |stream, deadline| {
                        DeadlineStream::new(
                            stream,
                            deadline,
                            CONTROL_PLANE_RAFT_PEER_SERVER_DEADLINE_EXPIRED,
                        )
                        .map(|stream| Box::new(stream) as Box<dyn ControlPlaneRaftPeerServerStream>)
                        .map_err(|source| {
                            ControlPlaneError::io(
                                "configure control-plane OpenRaft Unix peer deadline I/O",
                                source,
                            )
                        })
                    },
                ),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    eprintln!(
                        "control-plane OpenRaft peer listener {} accept failed: {error}",
                        self.endpoint_id
                    );
                    return Err(ControlPlaneRaftPeerServerError);
                }
            },
            ControlPlaneRaftPeerServerListenerKind::TlsTcp {
                listener,
                tls_server_config,
            } => {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let tls_server_config = Arc::clone(tls_server_config);
                        spawn_control_plane_raft_peer_server_worker(
                            stream,
                            runtime.clone(),
                            authority,
                            policy.clone(),
                            self.max_connections,
                            self.io_timeout,
                            Arc::clone(&self.active_workers),
                            move |stream, deadline| {
                                let socket =
                                DeadlineStream::new(
                                    stream,
                                    deadline,
                                    CONTROL_PLANE_RAFT_PEER_SERVER_DEADLINE_EXPIRED,
                                )
                                .map_err(|source| {
                                    ControlPlaneError::io("configure control-plane OpenRaft TLS/TCP peer deadline I/O", source)
                                })?;
                                let connection = rustls::ServerConnection::new(tls_server_config)
                                .map_err(|_| ControlPlaneError::rpc_protocol("failed to initialize control-plane OpenRaft TLS server connection".to_owned()))?;
                                let mut stream = rustls::StreamOwned::new(connection, socket);
                                while stream.conn.is_handshaking() {
                                    stream
                                    .conn
                                    .complete_io(&mut stream.sock)
                                    .map_err(|source| ControlPlaneError::io("complete control-plane OpenRaft TLS server handshake", source))?;
                                }
                                if stream.conn.alpn_protocol() != Some(CONTROL_PLANE_RAFT_TLS_ALPN)
                                {
                                    return Err(ControlPlaneError::rpc_protocol("control-plane OpenRaft TLS peer did not negotiate the required protocol profile".to_owned()));
                                }
                                Ok(Box::new(stream) as Box<dyn ControlPlaneRaftPeerServerStream>)
                            },
                        );
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => {
                        eprintln!(
                        "control-plane OpenRaft TLS/TCP peer listener {} accept failed: {error}",
                        self.endpoint_id
                    );
                        return Err(ControlPlaneRaftPeerServerError);
                    }
                }
            }
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_control_plane_raft_peer_server_worker<RawStream, Prepare>(
    stream: RawStream,
    runtime: tokio::runtime::Handle,
    authority: Arc<ControlPlaneRaftAuthority>,
    policy: ControlPlaneRaftPeerServerPolicy,
    worker_limit: usize,
    io_timeout: Duration,
    active_workers: Arc<AtomicUsize>,
    prepare: Prepare,
) where
    RawStream: Send + 'static,
    Prepare: FnOnce(
            RawStream,
            Instant,
        ) -> Result<Box<dyn ControlPlaneRaftPeerServerStream>, ControlPlaneError>
        + Send
        + 'static,
{
    if let Some(durability) = &policy.durability {
        if durability.validate_authority(&authority).is_err() {
            eprintln!(
                "control-plane OpenRaft peer RPC rejected: durability authority binding is invalid"
            );
            return;
        }
        if durability.is_poisoned() {
            eprintln!("control-plane OpenRaft peer RPC rejected: durable authority is poisoned");
            return;
        }
    }
    if active_workers
        .try_update(Ordering::AcqRel, Ordering::Acquire, |active| {
            (active < worker_limit).then_some(active + 1)
        })
        .is_err()
    {
        eprintln!("control-plane OpenRaft peer RPC rejected: worker limit reached");
        return;
    }
    let connection_deadline = Instant::now()
        .checked_add(io_timeout)
        .unwrap_or_else(Instant::now);
    std::thread::spawn(move || {
        let _guard = ControlPlaneRaftPeerServerWorkerGuard { active_workers };
        let mut stream = match prepare(stream, connection_deadline) {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("control-plane OpenRaft peer transport setup failed: {error}");
                return;
            }
        };
        match handle_control_plane_raft_peer_server_request(
            &runtime,
            &authority,
            &mut *stream,
            &policy,
            connection_deadline,
            io_timeout,
        ) {
            Ok(()) => {}
            Err(ControlPlaneRaftPeerServerWorkerError::PeerRpc(error)) => {
                eprintln!("control-plane OpenRaft peer RPC failed: {error}");
            }
            Err(ControlPlaneRaftPeerServerWorkerError::Checkpoint(error)) => {
                eprintln!(
                    "control-plane OpenRaft durability checkpoint failed before peer RPC response: {error}"
                );
                if let Some(fatal_error_handler) = &policy.fatal_error_handler {
                    fatal_error_handler();
                }
            }
        }
    });
}

#[derive(Debug)]
pub(super) enum ControlPlaneRaftPeerServerWorkerError {
    PeerRpc(ControlPlaneError),
    Checkpoint(ControlPlaneError),
}

pub(super) fn handle_control_plane_raft_peer_server_request(
    runtime: &tokio::runtime::Handle,
    authority: &ControlPlaneRaftAuthority,
    stream: &mut dyn ControlPlaneRaftPeerServerStream,
    policy: &ControlPlaneRaftPeerServerPolicy,
    connection_deadline: Instant,
    response_timeout: Duration,
) -> Result<(), ControlPlaneRaftPeerServerWorkerError> {
    ensure_control_plane_raft_peer_server_not_poisoned(authority, policy)?;
    let (received_frame, pre_auth_reservation) =
        read_control_plane_raft_peer_transport_frame_with_reservation(
            stream,
            policy.peer_policy.limits().max_frame_bytes,
            |frame_bytes| policy.resources.pre_auth_byte_budget.reserve(frame_bytes),
        )
        .map_err(ControlPlaneRaftPeerServerWorkerError::PeerRpc)?;
    let request_frame = if let Some(auth_policy) = policy.peer_policy.auth_policy() {
        let envelope = match ControlPlaneAuthEnvelope::decode_frame(
            &received_frame,
            policy.peer_policy.limits().max_frame_bytes,
        ) {
            Ok(envelope) => envelope,
            Err(error) => {
                auth_policy.record_peer_frame_rejection_without_operation(
                    ControlPlaneAuthRejectionReason::Malformed,
                );
                return Err(ControlPlaneRaftPeerServerWorkerError::PeerRpc(error));
            }
        };
        let operation = envelope.header().operation();
        let identity = match control_plane_raft_peer_auth_envelope_identity(
            &envelope,
            policy.peer_policy.cluster_name(),
            policy.peer_policy.topology_identity(),
            policy.local_node_id,
        ) {
            Ok(identity) => identity,
            Err(error) => {
                auth_policy.record_peer_frame_rejection(
                    operation,
                    ControlPlaneAuthRejectionReason::Malformed,
                );
                return Err(ControlPlaneRaftPeerServerWorkerError::PeerRpc(error));
            }
        };
        auth_policy
            .verify_peer_frame(
                &received_frame,
                &identity,
                operation,
                policy.peer_policy.limits().max_frame_bytes,
            )
            .map_err(ControlPlaneRaftPeerServerWorkerError::PeerRpc)?
    } else {
        received_frame
    };
    let frame_kind = decode_control_plane_raft_peer_request_frame_kind(&request_frame)
        .map_err(ControlPlaneRaftPeerServerWorkerError::PeerRpc)?;
    let identity = decode_control_plane_raft_peer_request_frame_identity(&request_frame)
        .map_err(ControlPlaneRaftPeerServerWorkerError::PeerRpc)?;
    let operation = decode_control_plane_raft_peer_request_auth_operation(&request_frame)
        .map_err(ControlPlaneRaftPeerServerWorkerError::PeerRpc)?;
    policy
        .peer_policy
        .validate_incoming_frame_identity(&identity, policy.local_node_id)
        .map_err(|error| ControlPlaneError::rpc_protocol(error.to_string()))
        .map_err(ControlPlaneRaftPeerServerWorkerError::PeerRpc)?;
    drop(pre_auth_reservation);

    ensure_control_plane_raft_peer_server_not_poisoned(authority, policy)?;
    let raw_response_frame = block_on_control_plane_raft_peer_server(runtime, async {
        tokio::time::timeout_at(tokio::time::Instant::from_std(connection_deadline), async {
            match frame_kind {
                ControlPlaneRaftPeerFrameKind::OrdinaryRpc => {
                    handle_control_plane_raft_peer_rpc_frame(
                        authority.raft(),
                        &request_frame,
                        &identity,
                    )
                    .await
                }
                ControlPlaneRaftPeerFrameKind::Snapshot => {
                    handle_control_plane_raft_peer_snapshot_frame(
                        authority.raft(),
                        &request_frame,
                        policy.peer_policy.limits().max_frame_bytes,
                        policy.peer_policy.limits().max_snapshot_bytes,
                        &identity,
                    )
                    .await
                }
            }
        })
        .await
        .map_err(|_| {
            ControlPlaneError::io(
                "dispatch control-plane OpenRaft inbound peer frame",
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    CONTROL_PLANE_RAFT_PEER_SERVER_DEADLINE_EXPIRED,
                ),
            )
        })?
    })
    .map_err(ControlPlaneRaftPeerServerWorkerError::PeerRpc)?;

    let response_frame = if let Some(auth_policy) = policy.peer_policy.auth_policy() {
        let response_identity = reverse_raft_peer_frame_identity(&identity);
        auth_policy
            .sign_peer_frame(&response_identity, operation, raw_response_frame)
            .map_err(ControlPlaneRaftPeerServerWorkerError::PeerRpc)?
    } else {
        raw_response_frame
    };

    if frame_kind == ControlPlaneRaftPeerFrameKind::Snapshot {
        let durability = policy.durability.as_ref().ok_or_else(|| {
            ControlPlaneRaftPeerServerWorkerError::Checkpoint(ControlPlaneError::rpc_protocol("control-plane OpenRaft snapshot peer RPC requires a durability checkpoint callback"
                        .to_owned()))
        })?;
        durability
            .checkpoint_before_snapshot_response(authority)
            .map_err(ControlPlaneRaftPeerServerWorkerError::Checkpoint)?;
    }
    ensure_control_plane_raft_peer_server_not_poisoned(authority, policy)?;

    let mut response_frame = Some(response_frame);
    let mut publish = || {
        let response_frame = response_frame.take().ok_or_else(|| {
            ControlPlaneError::rpc_protocol(
                "control-plane OpenRaft peer response publication attempted more than once"
                    .to_owned(),
            )
        })?;
        stream.begin_response(response_timeout);
        write_control_plane_raft_peer_transport_frame(stream, &response_frame)?;
        stream.finish_response().map_err(|source| {
            ControlPlaneError::io("finalize control-plane OpenRaft peer response", source)
        })
    };
    publish_control_plane_raft_peer_server_response(
        authority,
        policy.durability.as_ref(),
        &mut publish,
    )
    .map_err(ControlPlaneRaftPeerServerWorkerError::PeerRpc)
}

fn ensure_control_plane_raft_peer_server_not_poisoned(
    authority: &ControlPlaneRaftAuthority,
    policy: &ControlPlaneRaftPeerServerPolicy,
) -> Result<(), ControlPlaneRaftPeerServerWorkerError> {
    if let Some(durability) = &policy.durability {
        durability
            .validate_authority(authority)
            .map_err(ControlPlaneRaftPeerServerWorkerError::PeerRpc)?;
        if durability.is_poisoned() {
            return Err(ControlPlaneRaftPeerServerWorkerError::PeerRpc(
                ControlPlaneError::rpc_remote("control-plane OpenRaft durable authority is poisoned; refusing peer RPC until restart".to_owned()),
            ));
        }
    }
    Ok(())
}

pub(super) fn publish_control_plane_raft_peer_server_response(
    authority: &ControlPlaneRaftAuthority,
    durability: Option<&ControlPlaneRaftPeerServerDurability>,
    publish: &mut dyn FnMut() -> Result<(), ControlPlaneError>,
) -> Result<(), ControlPlaneError> {
    let mut published = false;
    let result = {
        let mut publish_once = || {
            if std::mem::replace(&mut published, true) {
                return Err(ControlPlaneError::rpc_protocol(
                    "control-plane OpenRaft peer response publication attempted more than once"
                        .to_owned(),
                ));
            }
            publish()
        };
        match durability {
            Some(durability) => durability.publish_response(authority, &mut publish_once),
            None => publish_once(),
        }
    };
    if result.is_ok() && !published {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane OpenRaft peer response publication completed without publishing"
                .to_owned(),
        ));
    }
    result
}

fn control_plane_raft_peer_auth_envelope_identity(
    envelope: &ControlPlaneAuthEnvelope,
    expected_cluster_name: &str,
    expected_topology: Option<&ControlPlaneRaftTopologyIdentity>,
    local_node_id: ControlPlaneRaftNodeId,
) -> Result<ControlPlaneRaftPeerFrameIdentity, ControlPlaneError> {
    let source = match envelope.header().source() {
        ControlPlaneAuthPrincipal::RaftPeer { node_id } => *node_id,
        principal => {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane OpenRaft peer auth source is not a RaftPeer principal: {principal:?}"
                )));
        }
    };
    match envelope.header().target() {
        ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer { .. }) => {}
        target => {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane OpenRaft peer auth target is not a RaftPeer principal: {target:?}"
            )));
        }
    }
    let mut identity = ControlPlaneRaftPeerFrameIdentity::new(
        expected_cluster_name.to_owned(),
        source,
        local_node_id,
    );
    identity.topology = expected_topology.cloned();
    Ok(identity)
}

fn block_on_control_plane_raft_peer_server<F: Future>(
    runtime: &tokio::runtime::Handle,
    future: F,
) -> F::Output {
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| runtime.block_on(future))
    } else {
        runtime.block_on(future)
    }
}

pub(crate) async fn handle_control_plane_raft_peer_rpc_frame(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    frame: &[u8],
    expected_identity: &ControlPlaneRaftPeerFrameIdentity,
) -> Result<Vec<u8>, ControlPlaneError> {
    handle_control_plane_raft_peer_rpc_frame_with_identity(raft, frame, Some(expected_identity))
        .await
}

pub(super) async fn handle_control_plane_raft_peer_rpc_frame_with_identity(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    frame: &[u8],
    expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
) -> Result<Vec<u8>, ControlPlaneError> {
    let request =
        ControlPlaneRaftPeerRpcRequest::decode_frame_with_identity(frame, expected_identity)?;
    let response = match request {
        ControlPlaneRaftPeerRpcRequest::AppendEntries(request) => {
            ControlPlaneRaftPeerRpcResponse::AppendEntries(
                raft.append_entries(request)
                    .await
                    .map_err(|error| openraft_remote_error("peer append_entries", error))?,
            )
        }
        ControlPlaneRaftPeerRpcRequest::Vote(request) => ControlPlaneRaftPeerRpcResponse::Vote(
            raft.vote(request)
                .await
                .map_err(|error| openraft_remote_error("peer vote", error))?,
        ),
        ControlPlaneRaftPeerRpcRequest::PreVote(request) => ControlPlaneRaftPeerRpcResponse::Vote(
            raft.pre_vote(request)
                .await
                .map_err(|error| openraft_remote_error("peer pre_vote", error))?,
        ),
        ControlPlaneRaftPeerRpcRequest::TransferLeader(request) => {
            ControlPlaneRaftPeerRpcResponse::TransferLeader(
                raft.handle_transfer_leader(request)
                    .await
                    .map_err(|error| openraft_remote_error("peer transfer_leader", error))?,
            )
        }
    };
    response.encode_frame_with_identity(
        expected_identity
            .as_ref()
            .map(|identity| reverse_raft_peer_frame_identity(identity))
            .as_ref(),
    )
}

pub(crate) async fn handle_control_plane_raft_peer_snapshot_frame(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    frame: &[u8],
    max_frame_bytes: usize,
    max_snapshot_bytes: usize,
    expected_identity: &ControlPlaneRaftPeerFrameIdentity,
) -> Result<Vec<u8>, ControlPlaneError> {
    handle_control_plane_raft_peer_snapshot_frame_with_identity(
        raft,
        frame,
        max_frame_bytes,
        max_snapshot_bytes,
        Some(expected_identity),
    )
    .await
}

#[cfg(test)]
pub(super) async fn handle_control_plane_raft_peer_unix_stream(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    stream: &mut UnixStream,
    frame_kind: ControlPlaneRaftPeerFrameKind,
    limits: ControlPlaneRaftPeerTransportLimits,
    expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    io_timeout: Duration,
) -> Result<(), ControlPlaneError> {
    let mut stream = DeadlineUnixStream::new(
        stream,
        Instant::now() + io_timeout,
        "control-plane OpenRaft test peer deadline expired",
    )
    .map_err(|source| {
        ControlPlaneError::io(
            "configure control-plane OpenRaft test peer deadline I/O",
            source,
        )
    })?;
    let request_frame =
        read_control_plane_raft_peer_transport_frame(&mut stream, limits.max_frame_bytes)?;
    handle_control_plane_raft_peer_unix_request_frame(
        raft,
        &mut stream,
        &request_frame,
        frame_kind,
        limits,
        expected_identity,
    )
    .await
}

#[cfg(test)]
pub(super) async fn handle_control_plane_raft_peer_unix_stream_detecting_frame_kind(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    stream: &mut UnixStream,
    limits: ControlPlaneRaftPeerTransportLimits,
    expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    io_timeout: Duration,
) -> Result<(), ControlPlaneError> {
    let mut stream = DeadlineUnixStream::new(
        stream,
        Instant::now() + io_timeout,
        "control-plane OpenRaft test peer deadline expired",
    )
    .map_err(|source| {
        ControlPlaneError::io(
            "configure control-plane OpenRaft test peer deadline I/O",
            source,
        )
    })?;
    let request_frame =
        read_control_plane_raft_peer_transport_frame(&mut stream, limits.max_frame_bytes)?;
    let frame_kind = decode_control_plane_raft_peer_request_frame_kind(&request_frame)?;
    handle_control_plane_raft_peer_unix_request_frame(
        raft,
        &mut stream,
        &request_frame,
        frame_kind,
        limits,
        expected_identity,
    )
    .await
}

#[cfg(test)]
pub(super) async fn handle_control_plane_raft_peer_unix_stream_from_configured_peer(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    stream: &mut UnixStream,
    local_node_id: ControlPlaneRaftNodeId,
    policy: &ControlPlaneRaftPeerTransportPolicy,
    io_timeout: Duration,
) -> Result<(), ControlPlaneError> {
    let mut stream = DeadlineUnixStream::new(
        stream,
        Instant::now() + io_timeout,
        "control-plane OpenRaft test peer deadline expired",
    )
    .map_err(|source| {
        ControlPlaneError::io(
            "configure control-plane OpenRaft test peer deadline I/O",
            source,
        )
    })?;
    let request_frame =
        read_control_plane_raft_peer_transport_frame(&mut stream, policy.limits().max_frame_bytes)?;
    let frame_kind = decode_control_plane_raft_peer_request_frame_kind(&request_frame)?;
    let identity = decode_control_plane_raft_peer_request_frame_identity(&request_frame)?;
    policy
        .validate_incoming_frame_identity(&identity, local_node_id)
        .map_err(|error| ControlPlaneError::rpc_protocol(error.to_string()))?;
    handle_control_plane_raft_peer_unix_request_frame(
        raft,
        &mut stream,
        &request_frame,
        frame_kind,
        policy.limits(),
        &identity,
    )
    .await
}

#[cfg(test)]
async fn handle_control_plane_raft_peer_unix_request_frame<Stream: Read + Write>(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    stream: &mut Stream,
    request_frame: &[u8],
    frame_kind: ControlPlaneRaftPeerFrameKind,
    limits: ControlPlaneRaftPeerTransportLimits,
    expected_identity: &ControlPlaneRaftPeerFrameIdentity,
) -> Result<(), ControlPlaneError> {
    let response_frame = match frame_kind {
        ControlPlaneRaftPeerFrameKind::OrdinaryRpc => {
            handle_control_plane_raft_peer_rpc_frame(raft, request_frame, expected_identity).await?
        }
        ControlPlaneRaftPeerFrameKind::Snapshot => {
            handle_control_plane_raft_peer_snapshot_frame(
                raft,
                request_frame,
                limits.max_frame_bytes,
                limits.max_snapshot_bytes,
                expected_identity,
            )
            .await?
        }
    };
    write_control_plane_raft_peer_transport_frame(stream, &response_frame)
}

pub(super) async fn handle_control_plane_raft_peer_snapshot_frame_with_identity(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    frame: &[u8],
    max_frame_bytes: usize,
    max_snapshot_bytes: usize,
    expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
) -> Result<Vec<u8>, ControlPlaneError> {
    let request = ControlPlaneRaftPeerSnapshotRequest::decode_frame_with_identity(
        frame,
        max_frame_bytes,
        max_snapshot_bytes,
        expected_identity,
    )?;
    let response = raft
        .install_full_snapshot(request.vote, request.snapshot)
        .await
        .map_err(|error| openraft_remote_error("peer full_snapshot", error))?;
    ControlPlaneRaftPeerSnapshotResponse { response }.encode_frame_with_identity(
        expected_identity
            .as_ref()
            .map(|identity| reverse_raft_peer_frame_identity(identity))
            .as_ref(),
    )
}
