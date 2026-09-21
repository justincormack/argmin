// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
#[cfg(test)]
use std::io::Cursor;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::ops::{Bound, RangeBounds};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use futures_util::future::{select, Either};
use futures_util::{Stream, StreamExt};
use openraft::errors::{
    ClientWriteError, InitializeError, LinearizableReadError, NetworkError, RPCError, RaftError,
    ReplicationClosed, StreamingError, Unreachable,
};
use openraft::impls::leader_id_adv::LeaderId;
use openraft::impls::Entry;
use openraft::impls::Vote;
use openraft::impls::{BasicNode, ProgressResponder};
use openraft::metrics::WaitError;
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, SnapshotResponse,
    TransferLeaderError, TransferLeaderRequest, TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::storage::SnapshotMeta;
use openraft::storage::{EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftStateMachine};
use openraft::type_config::alias::{
    LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
};
use openraft::type_config::TypeConfigExt;
use openraft::EntryPayload;
use openraft::Instant as _;
use openraft::LogId;
use openraft::Membership;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::ReadPolicy;
use openraft::ServerState;
use openraft::StoredMembership;
use openraft::{AnyError, Config, SnapshotPolicy};
use placement::NodeId;
use rustls::pki_types::ServerName;
use rustls::sign::CertifiedKey;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::control_plane::{
    connect_tcp_stream_until_async, connect_unix_stream_until, AuthorityIncarnation,
    ClusterControlSnapshot, ClusterRuntimeMapSnapshot, ControlPlaneAuthorityClockCheckpointBinding,
    ControlPlaneAuthorityClockContext, ControlPlaneError, ControlPlaneRaftOperationErrorKind,
    ControlPlaneRpcResponsePublication, ControlPlaneRuntimeMapDiagnosticSnapshot,
    ControlPlaneRuntimeMapNodeLeaseDiagnostic, ControlPlaneRuntimeMapStatus, DeadlineUnixStream,
    MetadataTransferStagingEvidencePageClassification, NodeAvailabilityState, NodeMembershipState,
    RuntimeMapContentCertificate, RuntimeMapFreshnessProof,
    CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS,
};
use crate::control_plane_auth::{
    ControlPlaneAuthDecision, ControlPlaneAuthEnvelope, ControlPlaneAuthOperation,
    ControlPlaneAuthPrincipal, ControlPlaneAuthRejectionReason, ControlPlaneAuthReplayPolicy,
    ControlPlaneAuthSignInput, ControlPlaneAuthTarget, ControlPlaneAuthVerificationInput,
    ControlPlaneScopedCredential, ControlPlaneScopedCredentialStore,
};
use crate::control_plane_command::{
    decode_control_plane_command, encode_control_plane_command,
    encode_control_plane_command_without_replication_limit,
    validate_control_plane_snapshot_for_install, ControlPlaneCommand, ControlPlaneCommandResponse,
    ControlPlaneCommandStateMachine, ControlPlaneLogId, ControlPlaneSnapshotArtifact,
    ReplicatedControlPlaneStateMachine,
};
use crate::deadline_io::DeadlineStream;
use crate::durable_journal::{
    DurableJournalAppendError, DurableJournalFile, DurableJournalFormat, DurableJournalIoContexts,
    DurableJournalObserver,
};
use crate::internal_tls_protocol::InternalTlsProtocol;
use crate::static_topology::{
    StaticInitialControlPlaneTopology, UncertifiedInitialControlPlaneTopology,
};
use crate::PgId;
use crate::{ClusterEpoch, PgState};

pub type ControlPlaneRaftNodeId = u64;
pub type ControlPlaneRaftTerm = u64;
pub type ControlPlaneRaftLeaderId = LeaderId<ControlPlaneRaftTerm, ControlPlaneRaftNodeId>;
pub type ControlPlaneRaftLogId = LogId<ControlPlaneRaftLeaderId>;
pub type ControlPlaneRaftEntry =
    Entry<ControlPlaneRaftLeaderId, ControlPlaneCommand, ControlPlaneRaftNodeId, BasicNode>;
mod peer_server;
mod peer_transport;
mod state_machine;

#[cfg(test)]
use peer_server::{
    handle_control_plane_raft_peer_rpc_frame_with_identity,
    handle_control_plane_raft_peer_server_request,
    handle_control_plane_raft_peer_snapshot_frame_with_identity,
    handle_control_plane_raft_peer_unix_stream,
    handle_control_plane_raft_peer_unix_stream_detecting_frame_kind,
    handle_control_plane_raft_peer_unix_stream_from_configured_peer,
    publish_control_plane_raft_peer_server_response, ControlPlaneRaftPeerServerListenerKind,
    ControlPlaneRaftPeerServerStream, ControlPlaneRaftPeerServerWorkerError,
};
#[cfg(any(test, feature = "test-hooks"))]
pub use peer_server::{
    inspect_control_plane_raft_checkpoint_state_for_test,
    inspect_control_plane_raft_recovery_state_for_test,
    ControlPlaneRaftCheckpointWriteBlockerForTest, ControlPlaneRaftDurableStateForTest,
    ControlPlaneRaftPeerTestClient, ControlPlaneRaftPendingTestResponse,
    ControlPlaneRaftPersistedVoteForTest,
};
pub use peer_server::{ControlPlaneRaftPeerServerCheckpoint, ControlPlaneRaftPeerServerDurability};
pub(crate) use peer_server::{
    ControlPlaneRaftPeerServerListener, ControlPlaneRaftPeerServerPolicy,
};

#[cfg(any(test, feature = "test-hooks"))]
use peer_transport::read_control_plane_raft_peer_transport_frame;
#[cfg(any(test, feature = "test-hooks"))]
use peer_transport::ControlPlaneRaftPeerNetwork;
use peer_transport::ControlPlaneRaftPeerNetworkFactory;
use peer_transport::{
    decode_control_plane_raft_peer_request_auth_operation,
    decode_control_plane_raft_peer_request_frame_identity,
    decode_control_plane_raft_peer_request_frame_kind,
    read_control_plane_raft_peer_transport_frame_with_reservation,
    reverse_raft_peer_frame_identity, write_control_plane_raft_peer_transport_frame,
    ControlPlaneRaftAppendEntriesResponseTag, ControlPlaneRaftTransferLeaderResponseTag,
};
#[cfg(test)]
use peer_transport::{
    raft_peer_rpc_frame_reader_classified, raft_peer_transport_rpc_error,
    read_control_plane_raft_peer_transport_frame_classified,
    ControlPlaneRaftConfiguredPeerFrameTransport, ControlPlaneRaftPeerFrameExchange,
    ControlPlaneRaftPeerFrameExchangeError, ControlPlaneRaftPeerFrameIdentityBranch,
    ControlPlaneRaftPeerFrameTransport, ControlPlaneRaftPeerRpcFrameDecodeError,
    ControlPlaneRaftPeerRpcFrameFormatError, ControlPlaneRaftPeerRpcFrameTag,
    ControlPlaneRaftPeerRpcRequestTag, ControlPlaneRaftPeerRpcResponseTag,
    ControlPlaneRaftPeerTransportFrameFormatError, ControlPlaneRaftPeerTransportFrameReadError,
    ControlPlaneRaftPeerTransportRejection, ControlPlaneRaftUnixPeerFrameTransport,
    CONTROL_PLANE_RAFT_TRANSFER_LEADER_AUTH_FRESHNESS_MS,
};
pub(crate) use peer_transport::{
    ControlPlaneRaftPeerAuthPolicy, ControlPlaneRaftPeerFrameIdentity,
    ControlPlaneRaftPeerFrameKind, ControlPlaneRaftPeerNetworkConfig,
    ControlPlaneRaftPeerRpcRequest, ControlPlaneRaftPeerRpcResponse,
    ControlPlaneRaftPeerSnapshotRequest, ControlPlaneRaftPeerSnapshotResponse,
    ControlPlaneRaftPeerTransportPolicy, CONTROL_PLANE_RAFT_TLS_ALPN,
};
pub use peer_transport::{
    ControlPlaneRaftPeerClientEndpoint, ControlPlaneRaftPeerClientEndpointError,
    ControlPlaneRaftPeerTransportLimits, ControlPlaneRaftTopologyIdentity,
};
use state_machine::ControlPlaneRaftStateMachineRestartArtifact;
#[cfg(test)]
use state_machine::{
    publish_control_plane_raft_snapshot, ControlPlaneRaftStateMachineBlockingHook,
    ControlPlaneRaftStateMachineTestHooks,
};
pub use state_machine::{ControlPlaneRaftSnapshotBuilder, ControlPlaneRaftStateMachine};

#[derive(Debug)]
pub enum ControlPlaneRaftApplyResponse {
    Blank,
    Membership,
    Applied(ControlPlaneCommandResponse),
    Rejected(ControlPlaneError),
}

#[derive(Debug)]
pub enum ControlPlaneRaftCommandOutcome {
    Applied(ControlPlaneCommandResponse),
    Rejected(ControlPlaneError),
}

#[derive(Debug)]
pub struct SubmittedControlPlaneRaftCommand {
    log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    outcome: ControlPlaneRaftCommandOutcome,
}

pub(crate) enum LowPriorityControlPlaneRaftCommandResult {
    PreflightResolved(ControlPlaneCommandResponse),
    Submitted(SubmittedControlPlaneRaftCommand),
}

impl SubmittedControlPlaneRaftCommand {
    #[must_use]
    pub fn log_id(&self) -> LogIdOf<ControlPlaneRaftTypeConfig> {
        self.log_id
    }

    #[must_use]
    pub fn outcome(&self) -> &ControlPlaneRaftCommandOutcome {
        &self.outcome
    }

    #[must_use]
    pub fn into_outcome(self) -> ControlPlaneRaftCommandOutcome {
        self.outcome
    }
}

/// Opaque committed/rejected result of submitting an uncertified initial
/// topology.
///
/// The value binds the submitted command to the exact owner-built topology and
/// issuing Raft authority while storage publishes the durable checkpoint. It
/// cannot leave the owning crate or be replaced before resolution.
pub(crate) struct UncertifiedInitialControlPlaneTopologySubmission {
    authority_instance_id: ControlPlaneRaftAuthorityInstanceId,
    topology: UncertifiedInitialControlPlaneTopology,
    submitted: SubmittedControlPlaneRaftCommand,
}

impl fmt::Debug for UncertifiedInitialControlPlaneTopologySubmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UncertifiedInitialControlPlaneTopologySubmission")
            .field("diagnostic", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// Authority-bound admission and poison state for durable response publication.
///
/// Every clone belongs to one Raft authority. A durability failure first stops
/// new response publication, waits for responses already holding admission to
/// drain, and only then publishes the poisoned state. Callers can pass this
/// opaque value to RPC servers but cannot create an independent publication
/// domain for the same authority.
#[derive(Clone)]
pub struct ControlPlaneRaftDurabilityPublication {
    authority_instance_id: ControlPlaneRaftAuthorityInstanceId,
    gate: Arc<(Mutex<ControlPlaneRaftDurabilityPublicationState>, Condvar)>,
    poisoned: Arc<AtomicBool>,
}

#[derive(Default)]
struct ControlPlaneRaftDurabilityPublicationState {
    active_responses: usize,
    poison_requested: bool,
    diagnostic: Option<Box<str>>,
}

struct ControlPlaneRaftResponsePublicationPermit<'a> {
    publication: &'a ControlPlaneRaftDurabilityPublication,
}

impl Drop for ControlPlaneRaftResponsePublicationPermit<'_> {
    fn drop(&mut self) {
        let (gate, responses_drained) = &*self.publication.gate;
        let mut state = gate
            .lock()
            .expect("control-plane Raft response publication mutex poisoned");
        state.active_responses = state
            .active_responses
            .checked_sub(1)
            .expect("response publication permit count should be positive");
        if state.active_responses == 0 {
            responses_drained.notify_all();
        }
    }
}

impl ControlPlaneRaftDurabilityPublication {
    fn new(authority_instance_id: ControlPlaneRaftAuthorityInstanceId) -> Self {
        Self {
            authority_instance_id,
            gate: Arc::new((
                Mutex::new(ControlPlaneRaftDurabilityPublicationState::default()),
                Condvar::new(),
            )),
            poisoned: Arc::new(AtomicBool::new(false)),
        }
    }

    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub fn ensure_available(&self) -> Result<(), ControlPlaneError> {
        let (gate, _) = &*self.gate;
        let state = gate
            .lock()
            .expect("control-plane Raft response publication mutex poisoned");
        if !state.poison_requested && !self.poisoned.load(Ordering::Acquire) {
            return Ok(());
        }
        Err(ControlPlaneError::durability_failure(
            state.diagnostic.clone().unwrap_or_else(|| {
                "control-plane Raft durable authority was poisoned before response publication"
                    .into()
            }),
        ))
    }

    pub fn poison(&self, diagnostic: impl Into<Box<str>>) {
        let (gate, responses_drained) = &*self.gate;
        let mut state = gate
            .lock()
            .expect("control-plane Raft response publication mutex poisoned");
        if state.poison_requested {
            while !self.poisoned.load(Ordering::Acquire) {
                state = responses_drained
                    .wait(state)
                    .expect("control-plane Raft response publication mutex poisoned");
            }
            return;
        }
        state.poison_requested = true;
        state.diagnostic = Some(diagnostic.into());
        while state.active_responses != 0 {
            state = responses_drained
                .wait(state)
                .expect("control-plane Raft response publication mutex poisoned");
        }
        self.poisoned.store(true, Ordering::Release);
        responses_drained.notify_all();
    }

    fn validate_authority(
        &self,
        authority_instance_id: ControlPlaneRaftAuthorityInstanceId,
    ) -> Result<(), ControlPlaneError> {
        if self.authority_instance_id != authority_instance_id {
            return Err(ControlPlaneError::invariant_failure(
                "control-plane Raft durability publication belongs to another authority instance",
            ));
        }
        Ok(())
    }

    fn publish<T>(
        &self,
        publish: impl FnOnce() -> Result<T, ControlPlaneError>,
    ) -> Result<T, ControlPlaneError> {
        let _permit = self.response_publication_permit()?;
        publish()
    }

    fn response_publication_permit(
        &self,
    ) -> Result<ControlPlaneRaftResponsePublicationPermit<'_>, ControlPlaneError> {
        self.ensure_available()?;
        let (gate, _) = &*self.gate;
        let mut state = gate
            .lock()
            .expect("control-plane Raft response publication mutex poisoned");
        if state.poison_requested || self.poisoned.load(Ordering::Acquire) {
            return Err(ControlPlaneError::durability_failure(
                state.diagnostic.clone().unwrap_or_else(|| {
                    "control-plane Raft durable authority was poisoned before response publication"
                        .into()
                }),
            ));
        }
        state.active_responses = state
            .active_responses
            .checked_add(1)
            .expect("response publication permit count overflow");
        Ok(ControlPlaneRaftResponsePublicationPermit { publication: self })
    }
}

impl fmt::Debug for ControlPlaneRaftDurabilityPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftDurabilityPublication")
            .field("diagnostic", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRpcResponsePublication for ControlPlaneRaftDurabilityPublication {
    fn publish(
        &self,
        publish: &mut dyn FnMut() -> Result<(), ControlPlaneError>,
    ) -> Result<(), ControlPlaneError> {
        ControlPlaneRaftDurabilityPublication::publish(self, publish)
    }
}

const CONTROL_PLANE_RAFT_AUTHORITY_INSTANCE_ID_LEN: usize = 16;

/// Random process-local identity for capabilities issued by one Raft authority.
///
/// The identity is never rendered, serialized, or exposed outside storage. It
/// avoids using an allocation address as an authority identifier while still
/// rejecting capabilities crossed between otherwise equivalent authorities.
#[derive(Clone, Copy, PartialEq, Eq)]
struct ControlPlaneRaftAuthorityInstanceId([u8; CONTROL_PLANE_RAFT_AUTHORITY_INSTANCE_ID_LEN]);

impl ControlPlaneRaftAuthorityInstanceId {
    fn generate() -> Result<Self, ControlPlaneError> {
        let mut bytes = [0u8; CONTROL_PLANE_RAFT_AUTHORITY_INSTANCE_ID_LEN];
        argmin_crypto::random::fill(&mut bytes).map_err(|_| {
            ControlPlaneError::io(
                "generate process-local Raft authority identity",
                std::io::Error::other("secure random generation failed"),
            )
        })?;
        Ok(Self(bytes))
    }
}

pub struct ControlPlaneRaftAuthority {
    cluster_name: String,
    node_id: ControlPlaneRaftNodeId,
    raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    log_store: Option<ControlPlaneRaftLogStore>,
    static_peer_policy: Option<ControlPlaneRaftPeerTransportPolicy>,
    volatile_heartbeat_update_gate: tokio::sync::Mutex<()>,
    evidence_submission_admission: ControlPlaneRaftEvidenceSubmissionAdmission,
    volatile_heartbeat_overlay: Mutex<Option<ControlPlaneRaftVolatileHeartbeatOverlay>>,
    runtime_map_content_certificate: Mutex<
        Option<(
            LogIdOf<ControlPlaneRaftTypeConfig>,
            RuntimeMapContentCertificate,
        )>,
    >,
    runtime_map_overlay_content_certificate: Mutex<
        Option<(
            ControlPlaneRaftTerm,
            LogIdOf<ControlPlaneRaftTypeConfig>,
            RuntimeMapContentCertificate,
        )>,
    >,
    authority_instance_id: OnceLock<ControlPlaneRaftAuthorityInstanceId>,
    durability_publication: OnceLock<ControlPlaneRaftDurabilityPublication>,
    durability_lifecycle: OnceLock<Arc<crate::control_plane_raft_durability::DurabilityInner>>,
    authority_host_lifecycle:
        OnceLock<Arc<crate::control_plane_raft_host::DurableAuthorityHostLifecycle>>,
    uncertified_initial_topology_checkpoint_published: OnceLock<()>,
    checkpoint_publication: Arc<Mutex<Option<ControlPlaneRaftCheckpointPosition>>>,
    durable_artifact_path: Option<Arc<PathBuf>>,
    checkpoint_metrics: Arc<ControlPlaneRaftCheckpointMetrics>,
    command_metrics: Arc<ControlPlaneRaftCommandMetrics>,
    #[cfg(test)]
    proposal_pause_after_confirmation: Mutex<Option<Duration>>,
    #[cfg(test)]
    proposal_lease_retry_count: AtomicUsize,
    #[cfg(test)]
    proposal_changed_tip_rejection_count: AtomicUsize,
    #[cfg(test)]
    membership_initialization_after_check_gate: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    #[cfg(test)]
    linearized_read_after_snapshot_gate: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    #[cfg(test)]
    linearized_read_after_raft_state_capture_gate: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    #[cfg(test)]
    linearized_read_after_generation_capture_gate: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    #[cfg(test)]
    linearized_runtime_map_read_index_count: AtomicUsize,
    #[cfg(test)]
    linearized_snapshot_retirement_hook:
        Mutex<Option<Arc<ControlPlaneRaftStateMachineBlockingHook>>>,
    #[cfg(test)]
    linearized_state_machine_response_ready_notify: Mutex<Option<Arc<tokio::sync::Notify>>>,
    #[cfg(test)]
    before_heartbeat_update_gate_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    low_priority_before_update_gate: Mutex<Option<Arc<ControlPlaneRaftLowPriorityTestGate>>>,
    #[cfg(test)]
    low_priority_after_update_gate: Mutex<Option<Arc<ControlPlaneRaftLowPriorityTestGate>>>,
    #[cfg(test)]
    ordinary_durable_after_update_gate: Mutex<Option<Arc<ControlPlaneRaftLowPriorityTestGate>>>,
    #[cfg(test)]
    low_priority_before_dispatch: Mutex<Option<Arc<ControlPlaneRaftLowPriorityTestGate>>>,
    #[cfg(test)]
    low_priority_after_dispatch: Mutex<Option<Arc<ControlPlaneRaftLowPriorityTestGate>>>,
    #[cfg(test)]
    low_priority_during_retry_certification:
        Mutex<Option<Arc<ControlPlaneRaftLowPriorityTestGate>>>,
    #[cfg(test)]
    low_priority_after_proven_unappended_retry:
        Mutex<Option<Arc<ControlPlaneRaftLowPriorityTestGate>>>,
}

#[cfg(test)]
struct ControlPlaneRaftLowPriorityTestGate {
    arrived: AtomicBool,
    arrived_notify: tokio::sync::Notify,
    release_notify: tokio::sync::Notify,
}

#[cfg(test)]
impl ControlPlaneRaftLowPriorityTestGate {
    fn new() -> Self {
        Self {
            arrived: AtomicBool::new(false),
            arrived_notify: tokio::sync::Notify::new(),
            release_notify: tokio::sync::Notify::new(),
        }
    }

    async fn block(&self) {
        let release = self.release_notify.notified();
        self.arrived.store(true, Ordering::Release);
        self.arrived_notify.notify_waiters();
        release.await;
    }

    async fn wait_for_arrival(&self) {
        while !self.arrived.load(Ordering::Acquire) {
            let arrived = self.arrived_notify.notified();
            if !self.arrived.load(Ordering::Acquire) {
                arrived.await;
            }
        }
    }

    fn release(&self) {
        self.release_notify.notify_waiters();
    }
}

#[derive(Default)]
struct ControlPlaneRaftEvidenceSubmissionAdmission {
    ordinary_waiters: AtomicUsize,
    volatile_waiters: AtomicUsize,
    ordinary_waiter_registered: tokio::sync::Notify,
    ordinary_waiter_completed: tokio::sync::Notify,
    durable_submission_serialization: tokio::sync::Mutex<()>,
    #[cfg(test)]
    evidence_waiting_for_update_gate: AtomicBool,
    #[cfg(test)]
    evidence_waiting_for_ordinary: AtomicUsize,
}

struct ControlPlaneRaftOrdinarySubmissionWaiter<'a> {
    admission: &'a ControlPlaneRaftEvidenceSubmissionAdmission,
    volatile: bool,
}

struct ControlPlaneRaftOrdinaryDurableUpdateGuard<'a> {
    _serialization: tokio::sync::MutexGuard<'a, ()>,
    update: tokio::sync::MutexGuard<'a, ()>,
}

#[cfg(test)]
struct ControlPlaneRaftEvidenceOrdinaryWait<'a> {
    waiting: &'a AtomicUsize,
}

#[cfg(test)]
impl<'a> ControlPlaneRaftEvidenceOrdinaryWait<'a> {
    fn new(waiting: &'a AtomicUsize) -> Self {
        waiting.fetch_add(1, Ordering::AcqRel);
        Self { waiting }
    }
}

#[cfg(test)]
impl Drop for ControlPlaneRaftEvidenceOrdinaryWait<'_> {
    fn drop(&mut self) {
        let previous = self.waiting.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(previous, 0);
    }
}

impl ControlPlaneRaftEvidenceSubmissionAdmission {
    fn register_ordinary_waiter(
        &self,
        volatile: bool,
    ) -> ControlPlaneRaftOrdinarySubmissionWaiter<'_> {
        self.ordinary_waiters.fetch_add(1, Ordering::AcqRel);
        if volatile {
            self.volatile_waiters.fetch_add(1, Ordering::AcqRel);
        }
        self.ordinary_waiter_registered.notify_waiters();
        ControlPlaneRaftOrdinarySubmissionWaiter {
            admission: self,
            volatile,
        }
    }

    async fn acquire_volatile_update_gate<'a>(
        &'a self,
        update_gate: &'a tokio::sync::Mutex<()>,
    ) -> tokio::sync::MutexGuard<'a, ()> {
        let waiter = self.register_ordinary_waiter(true);
        let update = update_gate.lock().await;
        drop(waiter);
        update
    }

    async fn acquire_ordinary_durable_update_gate<'a>(
        &'a self,
        update_gate: &'a tokio::sync::Mutex<()>,
    ) -> ControlPlaneRaftOrdinaryDurableUpdateGuard<'a> {
        let waiter = self.register_ordinary_waiter(false);
        let serialization = self.durable_submission_serialization.lock().await;
        let update = update_gate.lock().await;
        drop(waiter);
        ControlPlaneRaftOrdinaryDurableUpdateGuard {
            _serialization: serialization,
            update,
        }
    }

    fn ensure_evidence_may_continue(&self) -> Result<(), ControlPlaneError> {
        if self.ordinary_waiters.load(Ordering::Acquire) == 0 {
            Ok(())
        } else {
            Err(ControlPlaneError::StagingEvidencePublicationDeferred)
        }
    }

    async fn run_evidence_preappend<F, T>(&self, future: F) -> Result<T, ControlPlaneError>
    where
        F: Future<Output = Result<T, ControlPlaneError>>,
    {
        let waiter_registered = Box::pin(self.ordinary_waiter_registered.notified());
        let future = Box::pin(future);
        self.ensure_evidence_may_continue()?;
        match select(waiter_registered, future).await {
            Either::Left(((), _)) => Err(ControlPlaneError::StagingEvidencePublicationDeferred),
            Either::Right((result, _)) => result,
        }
    }

    async fn acquire_evidence_update_gate<'a>(
        &'a self,
        update_gate: &'a tokio::sync::Mutex<()>,
    ) -> ControlPlaneRaftEvidenceUpdateGuard<'a> {
        loop {
            while self.ordinary_waiters.load(Ordering::Acquire) != 0 {
                let completed = self.ordinary_waiter_completed.notified();
                if self.ordinary_waiters.load(Ordering::Acquire) != 0 {
                    #[cfg(test)]
                    let _waiting = ControlPlaneRaftEvidenceOrdinaryWait::new(
                        &self.evidence_waiting_for_ordinary,
                    );
                    completed.await;
                }
            }
            let serialization = self.durable_submission_serialization.lock().await;
            if self.ordinary_waiters.load(Ordering::Acquire) != 0 {
                drop(serialization);
                continue;
            }
            #[cfg(test)]
            self.evidence_waiting_for_update_gate
                .store(true, Ordering::Release);
            let update = update_gate.lock().await;
            #[cfg(test)]
            self.evidence_waiting_for_update_gate
                .store(false, Ordering::Release);
            if self.ordinary_waiters.load(Ordering::Acquire) == 0 {
                return ControlPlaneRaftEvidenceUpdateGuard {
                    _serialization: serialization,
                    update_gate,
                    update: Some(update),
                    admission: self,
                };
            }
            drop(update);
            drop(serialization);
        }
    }
}

impl Drop for ControlPlaneRaftOrdinarySubmissionWaiter<'_> {
    fn drop(&mut self) {
        if self.volatile {
            let previous = self
                .admission
                .volatile_waiters
                .fetch_sub(1, Ordering::AcqRel);
            debug_assert_ne!(previous, 0);
        }
        let previous = self
            .admission
            .ordinary_waiters
            .fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(previous, 0);
        self.admission.ordinary_waiter_completed.notify_waiters();
    }
}

struct ControlPlaneRaftEvidenceUpdateGuard<'a> {
    _serialization: tokio::sync::MutexGuard<'a, ()>,
    update_gate: &'a tokio::sync::Mutex<()>,
    update: Option<tokio::sync::MutexGuard<'a, ()>>,
    admission: &'a ControlPlaneRaftEvidenceSubmissionAdmission,
}

impl<'a> ControlPlaneRaftEvidenceUpdateGuard<'a> {
    fn release_update_after_dispatch(&mut self) {
        self.update.take();
    }

    async fn reacquire_update_after_dispatch(&mut self) {
        debug_assert!(self.update.is_none());
        loop {
            while self.admission.volatile_waiters.load(Ordering::Acquire) != 0 {
                let completed = self.admission.ordinary_waiter_completed.notified();
                if self.admission.volatile_waiters.load(Ordering::Acquire) != 0 {
                    completed.await;
                }
            }
            let update = self.update_gate.lock().await;
            if self.admission.volatile_waiters.load(Ordering::Acquire) == 0 {
                self.update = Some(update);
                return;
            }
            drop(update);
        }
    }
}

#[derive(Clone, Copy)]
struct ControlPlaneRaftProposalAttempt {
    leader_id: ControlPlaneRaftLeaderId,
    last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ControlPlaneRaftDurabilityMetricSnapshots {
    pub checkpoint: observability::ControlPlaneRaftCheckpointMetricSnapshot,
    pub wal: Option<observability::ControlPlaneRaftWalMetricSnapshot>,
    pub command: observability::ControlPlaneRaftCommandMetricSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlPlaneRaftWalMonitorSnapshot {
    offsets: ControlPlaneRaftWalOffsets,
    metrics: observability::ControlPlaneRaftWalMetricSnapshot,
    poisoned: Option<String>,
}

impl ControlPlaneRaftWalMonitorSnapshot {
    #[must_use]
    pub fn offsets(&self) -> ControlPlaneRaftWalOffsets {
        self.offsets
    }

    #[must_use]
    pub fn metrics(&self) -> observability::ControlPlaneRaftWalMetricSnapshot {
        self.metrics
    }

    #[must_use]
    pub fn poisoned(&self) -> Option<&str> {
        self.poisoned.as_deref()
    }
}

#[derive(Debug, Default)]
struct ControlPlaneRaftCheckpointMetrics {
    snapshot: Mutex<observability::ControlPlaneRaftCheckpointMetricSnapshot>,
}

#[derive(Debug, Default)]
struct ControlPlaneRaftWalMetrics {
    snapshot: Mutex<observability::ControlPlaneRaftWalMetricSnapshot>,
}

#[derive(Debug, Clone)]
struct ControlPlaneRaftWalObserver {
    metrics: Arc<ControlPlaneRaftWalMetrics>,
}

#[derive(Debug, Default)]
struct ControlPlaneRaftCommandMetrics {
    snapshot: Mutex<observability::ControlPlaneRaftCommandMetricSnapshot>,
}

fn raft_metric_elapsed_us(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

fn lock_raft_metric_snapshot<T>(snapshot: &Mutex<T>) -> MutexGuard<'_, T> {
    snapshot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl ControlPlaneRaftCheckpointMetrics {
    fn snapshot(&self) -> observability::ControlPlaneRaftCheckpointMetricSnapshot {
        *lock_raft_metric_snapshot(&self.snapshot)
    }

    fn record_encode(&self, elapsed: Duration, bytes: usize) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.encode_total = snapshot.encode_total.saturating_add(1);
        snapshot.encode_us_total = snapshot.encode_us_total.saturating_add(elapsed_us);
        snapshot.encode_us_max = snapshot.encode_us_max.max(elapsed_us);
        snapshot.bytes_total = snapshot.bytes_total.saturating_add(bytes);
        snapshot.bytes_last = bytes;
        snapshot.bytes_max = snapshot.bytes_max.max(bytes);
    }

    fn record_store(&self, elapsed: Duration, succeeded: bool) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.store_total = snapshot.store_total.saturating_add(1);
        if !succeeded {
            snapshot.store_error_total = snapshot.store_error_total.saturating_add(1);
        }
        snapshot.store_us_total = snapshot.store_us_total.saturating_add(elapsed_us);
        snapshot.store_us_max = snapshot.store_us_max.max(elapsed_us);
    }

    fn record_file_sync(&self, elapsed: Duration) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.file_sync_total = snapshot.file_sync_total.saturating_add(1);
        snapshot.file_sync_us_total = snapshot.file_sync_us_total.saturating_add(elapsed_us);
        snapshot.file_sync_us_max = snapshot.file_sync_us_max.max(elapsed_us);
    }

    fn record_directory_sync(&self, elapsed: Duration) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.directory_sync_total = snapshot.directory_sync_total.saturating_add(1);
        snapshot.directory_sync_us_total =
            snapshot.directory_sync_us_total.saturating_add(elapsed_us);
        snapshot.directory_sync_us_max = snapshot.directory_sync_us_max.max(elapsed_us);
    }

    fn record_compaction(&self, elapsed: Duration, succeeded: bool) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.compaction_total = snapshot.compaction_total.saturating_add(1);
        if !succeeded {
            snapshot.compaction_error_total = snapshot.compaction_error_total.saturating_add(1);
        }
        snapshot.compaction_us_total = snapshot.compaction_us_total.saturating_add(elapsed_us);
        snapshot.compaction_us_max = snapshot.compaction_us_max.max(elapsed_us);
    }
}

impl ControlPlaneRaftWalMetrics {
    fn snapshot(&self) -> observability::ControlPlaneRaftWalMetricSnapshot {
        *lock_raft_metric_snapshot(&self.snapshot)
    }

    fn record_append(&self, elapsed: Duration, succeeded: bool) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.append_total = snapshot.append_total.saturating_add(1);
        if !succeeded {
            snapshot.append_error_total = snapshot.append_error_total.saturating_add(1);
        }
        snapshot.append_us_total = snapshot.append_us_total.saturating_add(elapsed_us);
        snapshot.append_us_max = snapshot.append_us_max.max(elapsed_us);
    }

    fn record_lock_wait(&self, elapsed: Duration) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.lock_wait_us_total = snapshot.lock_wait_us_total.saturating_add(elapsed_us);
        snapshot.lock_wait_us_max = snapshot.lock_wait_us_max.max(elapsed_us);
    }

    fn record_frame_bytes(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.frame_bytes_total = snapshot.frame_bytes_total.saturating_add(bytes);
        snapshot.frame_bytes_last = bytes;
        snapshot.frame_bytes_max = snapshot.frame_bytes_max.max(bytes);
    }

    fn record_file_sync(&self, elapsed: Duration) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.file_sync_total = snapshot.file_sync_total.saturating_add(1);
        snapshot.file_sync_us_total = snapshot.file_sync_us_total.saturating_add(elapsed_us);
        snapshot.file_sync_us_max = snapshot.file_sync_us_max.max(elapsed_us);
    }

    fn record_directory_sync(&self, elapsed: Duration) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.directory_sync_total = snapshot.directory_sync_total.saturating_add(1);
        snapshot.directory_sync_us_total =
            snapshot.directory_sync_us_total.saturating_add(elapsed_us);
        snapshot.directory_sync_us_max = snapshot.directory_sync_us_max.max(elapsed_us);
    }

    fn record_durability_queue_enter(&self) {
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.durability_queue_depth = snapshot.durability_queue_depth.saturating_add(1);
        snapshot.durability_queue_depth_max = snapshot
            .durability_queue_depth_max
            .max(snapshot.durability_queue_depth);
    }

    fn record_durability_queue_leave(&self, elapsed: Duration) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.durability_queue_depth = snapshot.durability_queue_depth.saturating_sub(1);
        snapshot.durability_queue_wait_us_total = snapshot
            .durability_queue_wait_us_total
            .saturating_add(elapsed_us);
        snapshot.durability_queue_wait_us_max =
            snapshot.durability_queue_wait_us_max.max(elapsed_us);
    }

    fn record_append_accept(&self, elapsed: Duration) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.append_accept_us_total =
            snapshot.append_accept_us_total.saturating_add(elapsed_us);
        snapshot.append_accept_us_max = snapshot.append_accept_us_max.max(elapsed_us);
    }

    fn record_durability_operation(&self, elapsed: Duration) {
        let elapsed_us = raft_metric_elapsed_us(elapsed);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.durability_operation_us_total = snapshot
            .durability_operation_us_total
            .saturating_add(elapsed_us);
        snapshot.durability_operation_us_max = snapshot.durability_operation_us_max.max(elapsed_us);
    }
}

impl DurableJournalObserver for ControlPlaneRaftWalObserver {
    fn record_append(&self, elapsed: Duration, succeeded: bool) {
        observability::record_control_plane_raft_wal_append(elapsed, succeeded);
        self.metrics.record_append(elapsed, succeeded);
    }

    fn record_lock_wait(&self, elapsed: Duration) {
        observability::record_control_plane_raft_wal_lock_wait(elapsed);
        self.metrics.record_lock_wait(elapsed);
    }

    fn record_frame_bytes(&self, bytes: usize) {
        observability::record_control_plane_raft_wal_frame_bytes(bytes);
        self.metrics.record_frame_bytes(bytes);
    }

    fn record_file_sync(&self, elapsed: Duration) {
        observability::record_control_plane_raft_wal_file_sync(elapsed);
        self.metrics.record_file_sync(elapsed);
    }

    fn record_directory_sync(&self, elapsed: Duration) {
        observability::record_control_plane_raft_wal_directory_sync(elapsed);
        self.metrics.record_directory_sync(elapsed);
    }

    fn before_file_sync(&self, path: &Path) -> Result<(), ControlPlaneError> {
        inject_control_plane_raft_wal_file_sync_failure(path)
    }

    fn sync_parent(&self, path: &Path) -> Result<(), ControlPlaneError> {
        sync_control_plane_raft_wal_parent(path)
    }
}

impl ControlPlaneRaftCommandMetrics {
    fn snapshot(&self) -> observability::ControlPlaneRaftCommandMetricSnapshot {
        *lock_raft_metric_snapshot(&self.snapshot)
    }

    fn record_submission(&self, queue_wait: Duration, operation: Duration, succeeded: bool) {
        let queue_wait_us = raft_metric_elapsed_us(queue_wait);
        let operation_us = raft_metric_elapsed_us(operation);
        let mut snapshot = lock_raft_metric_snapshot(&self.snapshot);
        snapshot.submit_total = snapshot.submit_total.saturating_add(1);
        if !succeeded {
            snapshot.submit_error_total = snapshot.submit_error_total.saturating_add(1);
        }
        snapshot.queue_wait_us_total = snapshot.queue_wait_us_total.saturating_add(queue_wait_us);
        snapshot.queue_wait_us_max = snapshot.queue_wait_us_max.max(queue_wait_us);
        snapshot.operation_us_total = snapshot.operation_us_total.saturating_add(operation_us);
        snapshot.operation_us_max = snapshot.operation_us_max.max(operation_us);
    }
}

#[derive(Debug)]
struct ControlPlaneRaftSnapshotGeneration {
    snapshot: Option<Arc<ClusterControlSnapshot>>,
    #[cfg(test)]
    retirement_hook: Option<Arc<ControlPlaneRaftStateMachineBlockingHook>>,
}

impl ControlPlaneRaftSnapshotGeneration {
    fn new(
        snapshot: Arc<ClusterControlSnapshot>,
        #[cfg(test)] retirement_hook: Option<Arc<ControlPlaneRaftStateMachineBlockingHook>>,
    ) -> Self {
        Self {
            snapshot: Some(snapshot),
            #[cfg(test)]
            retirement_hook,
        }
    }

    fn arc(&self) -> &Arc<ClusterControlSnapshot> {
        self.snapshot
            .as_ref()
            .expect("control-plane snapshot generation must exist until consumed")
    }

    fn replace_snapshot(mut self, snapshot: Arc<ClusterControlSnapshot>) -> Self {
        let retired = self
            .snapshot
            .replace(snapshot)
            .expect("control-plane snapshot generation must exist until replaced");
        Self::retire(
            retired,
            #[cfg(test)]
            None,
        );
        self
    }

    fn into_blocking_lane_arc(mut self) -> Arc<ClusterControlSnapshot> {
        self.snapshot
            .take()
            .expect("control-plane snapshot generation must exist until consumed")
    }

    fn retire(
        snapshot: Arc<ClusterControlSnapshot>,
        #[cfg(test)] retirement_hook: Option<Arc<ControlPlaneRaftStateMachineBlockingHook>>,
    ) {
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn_blocking(move || {
                #[cfg(test)]
                if let Some(hook) = retirement_hook {
                    assert_eq!(
                        Arc::strong_count(&snapshot),
                        1,
                        "retirement hook must observe the final snapshot generation owner"
                    );
                    hook.block();
                }
                drop(snapshot);
            });
        } else {
            drop(snapshot);
        }
    }
}

impl Drop for ControlPlaneRaftSnapshotGeneration {
    fn drop(&mut self) {
        let Some(snapshot) = self.snapshot.take() else {
            return;
        };
        #[cfg(test)]
        let retirement_hook = self.retirement_hook.take();
        Self::retire(
            snapshot,
            #[cfg(test)]
            retirement_hook,
        );
    }
}

#[derive(Debug)]
struct ControlPlaneRaftVolatileHeartbeatOverlay {
    authority_term: ControlPlaneRaftTerm,
    base_applied: LogIdOf<ControlPlaneRaftTypeConfig>,
    snapshot: ControlPlaneRaftSnapshotGeneration,
}

struct ControlPlaneRaftLinearizedSnapshot {
    snapshot: ControlPlaneRaftSnapshotGeneration,
    applied: LogIdOf<ControlPlaneRaftTypeConfig>,
    read_index: ControlPlaneLogId,
    volatile_authority_term: Option<ControlPlaneRaftTerm>,
}

pub type ControlPlaneRaftFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait ControlPlaneRaftLinearizedCommandSink {
    fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftFuture<'_, Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>>;
}

pub trait ControlPlaneRaftLinearizedRuntimeMapSource {
    fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> ControlPlaneRaftFuture<'_, Result<ClusterRuntimeMapSnapshot, ControlPlaneError>>;
}

pub trait ControlPlaneRaftAuthorityStatusSource {
    fn status(
        &self,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError>>;
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityStatusHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityStatusSource + Send + Sync>,
}

impl ControlPlaneRaftAuthorityStatusHandle {
    pub fn new<T>(status_source: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityStatusSource + Send + Sync + 'static,
    {
        Self {
            inner: status_source,
        }
    }

    pub fn from_status_source(
        status_source: Arc<dyn ControlPlaneRaftAuthorityStatusSource + Send + Sync>,
    ) -> Self {
        Self {
            inner: status_source,
        }
    }

    #[must_use]
    pub fn as_status_source(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityStatusSource + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn status(&self) -> Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError> {
        self.inner.status().await
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityStatusHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityStatusHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityStatusSource for ControlPlaneRaftAuthorityStatusHandle {
    fn status(
        &self,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError>>
    {
        self.inner.status()
    }
}

pub trait ControlPlaneRaftLeaderRoutedAdmin {
    fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>;

    fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>;

    fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;
}

pub trait ControlPlaneRaftAuthorityBootstrap {
    fn initialize_membership(
        &self,
        nodes: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;

    fn is_initialized(&self) -> ControlPlaneRaftFuture<'_, Result<bool, ControlPlaneError>>;
}

pub trait ControlPlaneRaftAuthorityNodeLifecycle {
    fn wait_for_applied_index_at_least(
        &self,
        index: u64,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;

    fn wait_for_applied_log_id(
        &self,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;

    fn wait_for_current_leader(
        &self,
        leader_id: ControlPlaneRaftNodeId,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;

    fn shutdown(&self) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>>;
}

pub trait ControlPlaneRaftLinearizedAuthority:
    ControlPlaneRaftLinearizedCommandSink
    + ControlPlaneRaftLinearizedRuntimeMapSource
    + ControlPlaneRaftAuthorityStatusSource
{
}

impl<T> ControlPlaneRaftLinearizedAuthority for T where
    T: ControlPlaneRaftLinearizedCommandSink
        + ControlPlaneRaftLinearizedRuntimeMapSource
        + ControlPlaneRaftAuthorityStatusSource
{
}

pub trait ControlPlaneRaftAuthorityStatusListSource {
    fn authority_statuses(
        &self,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<
            BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftAuthorityStatus>,
            ControlPlaneError,
        >,
    >;
}

pub trait ControlPlaneRaftAuthorityBootstrapDirectory {
    fn authority_bootstrap_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftAuthorityBootstrapHandle, ControlPlaneError>,
    >;
}

pub trait ControlPlaneRaftAuthorityNodeLifecycleDirectory {
    fn authority_node_lifecycle_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftAuthorityNodeLifecycleHandle, ControlPlaneError>,
    >;
}

pub trait ControlPlaneRaftLinearizedAuthorityDirectory {
    fn linearized_authority_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError>>;
}

pub trait ControlPlaneRaftLeaderRoutedAdminDirectory {
    fn leader_routed_admin_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftLeaderRoutedAdminHandle, ControlPlaneError>,
    >;
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityStatusListHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityStatusListSource + Send + Sync>,
}

impl ControlPlaneRaftAuthorityStatusListHandle {
    pub fn new<T>(status_list: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityStatusListSource + Send + Sync + 'static,
    {
        Self { inner: status_list }
    }

    pub fn from_status_list(
        status_list: Arc<dyn ControlPlaneRaftAuthorityStatusListSource + Send + Sync>,
    ) -> Self {
        Self { inner: status_list }
    }

    #[must_use]
    pub fn as_status_list(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityStatusListSource + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn authority_statuses(
        &self,
    ) -> Result<BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftAuthorityStatus>, ControlPlaneError>
    {
        self.inner.authority_statuses().await
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityStatusListHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityStatusListHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityStatusListSource for ControlPlaneRaftAuthorityStatusListHandle {
    fn authority_statuses(
        &self,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<
            BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftAuthorityStatus>,
            ControlPlaneError,
        >,
    > {
        self.inner.authority_statuses()
    }
}

fn current_serving_authority_node_id(
    statuses: &BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftAuthorityStatus>,
) -> Result<ControlPlaneRaftNodeId, ControlPlaneError> {
    let mut serving_node_id = None;
    for (directory_node_id, status) in statuses {
        if *directory_node_id != status.node_id() {
            return Err(ControlPlaneError::rpc_remote(format!(
                "raft authority directory status key {} disagrees with reported node {}",
                directory_node_id,
                status.node_id()
            )));
        }
        if !status.linearized_authority_serving() {
            continue;
        }
        if let Some(existing_node_id) = serving_node_id {
            return Err(ControlPlaneError::rpc_remote(format!(
                    "raft authority directory found multiple serving raft authorities: {existing_node_id} and {}",
                    status.node_id()
                )));
        }
        serving_node_id = Some(status.node_id());
    }
    serving_node_id.ok_or_else(|| {
        ControlPlaneError::rpc_remote(
            "raft authority directory found no serving raft authority".to_string(),
        )
    })
}

fn validate_selected_linearized_authority_status(
    selected_node_id: ControlPlaneRaftNodeId,
    status: &ControlPlaneRaftAuthorityStatus,
) -> Result<(), ControlPlaneError> {
    if status.node_id() != selected_node_id {
        return Err(ControlPlaneError::rpc_remote(format!(
                "raft linearized authority directory returned node {} for selected serving node {selected_node_id}",
                status.node_id()
            )));
    }
    if !status.linearized_authority_serving() {
        return Err(ControlPlaneError::rpc_remote(format!(
                "raft linearized authority directory selected node {selected_node_id}, but it is no longer serving: {:?}",
                status.linearized_authority_readiness()
            )));
    }
    Ok(())
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityBootstrapDirectoryHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityBootstrapDirectory + Send + Sync>,
}

impl ControlPlaneRaftAuthorityBootstrapDirectoryHandle {
    pub fn new<T>(directory: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityBootstrapDirectory + Send + Sync + 'static,
    {
        Self { inner: directory }
    }

    pub fn from_bootstrap_directory(
        directory: Arc<dyn ControlPlaneRaftAuthorityBootstrapDirectory + Send + Sync>,
    ) -> Self {
        Self { inner: directory }
    }

    #[must_use]
    pub fn as_bootstrap_directory(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityBootstrapDirectory + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn authority_bootstrap_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<ControlPlaneRaftAuthorityBootstrapHandle, ControlPlaneError> {
        self.inner.authority_bootstrap_for_node(node_id).await
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityBootstrapDirectoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityBootstrapDirectoryHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityBootstrapDirectory
    for ControlPlaneRaftAuthorityBootstrapDirectoryHandle
{
    fn authority_bootstrap_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftAuthorityBootstrapHandle, ControlPlaneError>,
    > {
        self.inner.authority_bootstrap_for_node(node_id)
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityNodeLifecycleDirectory + Send + Sync>,
}

impl ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle {
    pub fn new<T>(directory: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityNodeLifecycleDirectory + Send + Sync + 'static,
    {
        Self { inner: directory }
    }

    pub fn from_node_lifecycle_directory(
        directory: Arc<dyn ControlPlaneRaftAuthorityNodeLifecycleDirectory + Send + Sync>,
    ) -> Self {
        Self { inner: directory }
    }

    #[must_use]
    pub fn as_node_lifecycle_directory(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityNodeLifecycleDirectory + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn authority_node_lifecycle_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<ControlPlaneRaftAuthorityNodeLifecycleHandle, ControlPlaneError> {
        self.inner.authority_node_lifecycle_for_node(node_id).await
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityNodeLifecycleDirectory
    for ControlPlaneRaftAuthorityNodeLifecycleDirectoryHandle
{
    fn authority_node_lifecycle_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftAuthorityNodeLifecycleHandle, ControlPlaneError>,
    > {
        self.inner.authority_node_lifecycle_for_node(node_id)
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftLinearizedAuthorityDirectoryHandle {
    inner: Arc<dyn ControlPlaneRaftLinearizedAuthorityDirectory + Send + Sync>,
}

impl ControlPlaneRaftLinearizedAuthorityDirectoryHandle {
    pub fn new<T>(directory: Arc<T>) -> Self
    where
        T: ControlPlaneRaftLinearizedAuthorityDirectory + Send + Sync + 'static,
    {
        Self { inner: directory }
    }

    pub fn from_linearized_authority_directory(
        directory: Arc<dyn ControlPlaneRaftLinearizedAuthorityDirectory + Send + Sync>,
    ) -> Self {
        Self { inner: directory }
    }

    #[must_use]
    pub fn as_linearized_authority_directory(
        &self,
    ) -> &(dyn ControlPlaneRaftLinearizedAuthorityDirectory + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn linearized_authority_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError> {
        self.inner.linearized_authority_for_node(node_id).await
    }
}

impl fmt::Debug for ControlPlaneRaftLinearizedAuthorityDirectoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftLinearizedAuthorityDirectoryHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLinearizedAuthorityDirectory
    for ControlPlaneRaftLinearizedAuthorityDirectoryHandle
{
    fn linearized_authority_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError>>
    {
        self.inner.linearized_authority_for_node(node_id)
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftLeaderRoutedAdminDirectoryHandle {
    inner: Arc<dyn ControlPlaneRaftLeaderRoutedAdminDirectory + Send + Sync>,
}

impl ControlPlaneRaftLeaderRoutedAdminDirectoryHandle {
    pub fn new<T>(directory: Arc<T>) -> Self
    where
        T: ControlPlaneRaftLeaderRoutedAdminDirectory + Send + Sync + 'static,
    {
        Self { inner: directory }
    }

    pub fn from_leader_routed_admin_directory(
        directory: Arc<dyn ControlPlaneRaftLeaderRoutedAdminDirectory + Send + Sync>,
    ) -> Self {
        Self { inner: directory }
    }

    #[must_use]
    pub fn as_leader_routed_admin_directory(
        &self,
    ) -> &(dyn ControlPlaneRaftLeaderRoutedAdminDirectory + Send + Sync + 'static) {
        &*self.inner
    }

    pub async fn leader_routed_admin_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<ControlPlaneRaftLeaderRoutedAdminHandle, ControlPlaneError> {
        self.inner.leader_routed_admin_for_node(node_id).await
    }
}

impl fmt::Debug for ControlPlaneRaftLeaderRoutedAdminDirectoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftLeaderRoutedAdminDirectoryHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLeaderRoutedAdminDirectory
    for ControlPlaneRaftLeaderRoutedAdminDirectoryHandle
{
    fn leader_routed_admin_for_node(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<
        '_,
        Result<ControlPlaneRaftLeaderRoutedAdminHandle, ControlPlaneError>,
    > {
        self.inner.leader_routed_admin_for_node(node_id)
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityHandle {
    inner: Arc<dyn ControlPlaneRaftLinearizedAuthority + Send + Sync>,
}

impl ControlPlaneRaftAuthorityHandle {
    pub fn new<T>(authority: Arc<T>) -> Self
    where
        T: ControlPlaneRaftLinearizedAuthority + Send + Sync + 'static,
    {
        Self { inner: authority }
    }

    pub fn from_linearized_authority(
        authority: Arc<dyn ControlPlaneRaftLinearizedAuthority + Send + Sync>,
    ) -> Self {
        Self { inner: authority }
    }

    #[must_use]
    pub fn as_linearized_authority(
        &self,
    ) -> &(dyn ControlPlaneRaftLinearizedAuthority + Send + Sync + 'static) {
        &*self.inner
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLinearizedCommandSink for ControlPlaneRaftAuthorityHandle {
    fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftFuture<'_, Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>>
    {
        self.inner.submit_control_plane_command(command)
    }
}

impl ControlPlaneRaftLinearizedRuntimeMapSource for ControlPlaneRaftAuthorityHandle {
    fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> ControlPlaneRaftFuture<'_, Result<ClusterRuntimeMapSnapshot, ControlPlaneError>> {
        self.inner.linearized_runtime_map_snapshot(issued_at_ms)
    }
}

impl ControlPlaneRaftAuthorityStatusSource for ControlPlaneRaftAuthorityHandle {
    fn status(
        &self,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError>>
    {
        self.inner.status()
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftLeaderRoutedAdminHandle {
    inner: Arc<dyn ControlPlaneRaftLeaderRoutedAdmin + Send + Sync>,
}

impl ControlPlaneRaftLeaderRoutedAdminHandle {
    pub fn new<T>(authority: Arc<T>) -> Self
    where
        T: ControlPlaneRaftLeaderRoutedAdmin + Send + Sync + 'static,
    {
        Self { inner: authority }
    }

    pub fn from_leader_routed_admin(
        authority: Arc<dyn ControlPlaneRaftLeaderRoutedAdmin + Send + Sync>,
    ) -> Self {
        Self { inner: authority }
    }

    #[must_use]
    pub fn as_leader_routed_admin(
        &self,
    ) -> &(dyn ControlPlaneRaftLeaderRoutedAdmin + Send + Sync + 'static) {
        &*self.inner
    }
}

impl fmt::Debug for ControlPlaneRaftLeaderRoutedAdminHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftLeaderRoutedAdminHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLeaderRoutedAdmin for ControlPlaneRaftLeaderRoutedAdminHandle {
    fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        self.inner
            .replace_voters(voters, retain_removed_voters_as_learners)
    }

    fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        self.inner.add_learner(node_id, node, wait_for_catch_up)
    }

    fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner.transfer_leadership_to(node_id)
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityBootstrapHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityBootstrap + Send + Sync>,
}

impl ControlPlaneRaftAuthorityBootstrapHandle {
    pub fn new<T>(authority: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityBootstrap + Send + Sync + 'static,
    {
        Self { inner: authority }
    }

    pub fn from_bootstrap_authority(
        authority: Arc<dyn ControlPlaneRaftAuthorityBootstrap + Send + Sync>,
    ) -> Self {
        Self { inner: authority }
    }

    #[must_use]
    pub fn as_bootstrap_authority(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityBootstrap + Send + Sync + 'static) {
        &*self.inner
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityBootstrapHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityBootstrapHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityBootstrap for ControlPlaneRaftAuthorityBootstrapHandle {
    fn initialize_membership(
        &self,
        nodes: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner.initialize_membership(nodes)
    }

    fn is_initialized(&self) -> ControlPlaneRaftFuture<'_, Result<bool, ControlPlaneError>> {
        self.inner.is_initialized()
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityNodeLifecycleHandle {
    inner: Arc<dyn ControlPlaneRaftAuthorityNodeLifecycle + Send + Sync>,
}

impl ControlPlaneRaftAuthorityNodeLifecycleHandle {
    pub fn new<T>(authority: Arc<T>) -> Self
    where
        T: ControlPlaneRaftAuthorityNodeLifecycle + Send + Sync + 'static,
    {
        Self { inner: authority }
    }

    pub fn from_node_lifecycle_authority(
        authority: Arc<dyn ControlPlaneRaftAuthorityNodeLifecycle + Send + Sync>,
    ) -> Self {
        Self { inner: authority }
    }

    #[must_use]
    pub fn as_node_lifecycle_authority(
        &self,
    ) -> &(dyn ControlPlaneRaftAuthorityNodeLifecycle + Send + Sync + 'static) {
        &*self.inner
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityNodeLifecycleHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityNodeLifecycleHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityNodeLifecycle for ControlPlaneRaftAuthorityNodeLifecycleHandle {
    fn wait_for_applied_index_at_least(
        &self,
        index: u64,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner
            .wait_for_applied_index_at_least(index, timeout, message)
    }

    fn wait_for_applied_log_id(
        &self,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner.wait_for_applied_log_id(log_id, timeout, message)
    }

    fn wait_for_current_leader(
        &self,
        leader_id: ControlPlaneRaftNodeId,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner
            .wait_for_current_leader(leader_id, timeout, message)
    }

    fn shutdown(&self) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        self.inner.shutdown()
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityRoutingHandle {
    observer: ControlPlaneRaftAuthorityStatusHandle,
    status_list: ControlPlaneRaftAuthorityStatusListHandle,
    directory: ControlPlaneRaftLinearizedAuthorityDirectoryHandle,
}

impl ControlPlaneRaftAuthorityRoutingHandle {
    #[must_use]
    pub fn new(
        observer: ControlPlaneRaftAuthorityStatusHandle,
        status_list: ControlPlaneRaftAuthorityStatusListHandle,
        directory: ControlPlaneRaftLinearizedAuthorityDirectoryHandle,
    ) -> Self {
        Self {
            observer,
            status_list,
            directory,
        }
    }

    pub async fn current_serving_linearized_authority(
        &self,
    ) -> Result<ControlPlaneRaftAuthorityHandle, ControlPlaneError> {
        let statuses = self.status_list.authority_statuses().await?;
        let node_id = current_serving_authority_node_id(&statuses)?;
        let authority = self
            .directory
            .linearized_authority_for_node(node_id)
            .await?;
        let status = authority.status().await?;
        validate_selected_linearized_authority_status(node_id, &status)?;
        Ok(authority)
    }

    pub async fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError> {
        self.current_serving_linearized_authority()
            .await?
            .submit_control_plane_command(command)
            .await
    }

    pub async fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.current_serving_linearized_authority()
            .await?
            .linearized_runtime_map_snapshot(issued_at_ms)
            .await
    }

    pub async fn observer_status(
        &self,
    ) -> Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError> {
        self.observer.status().await
    }

    pub async fn status(&self) -> Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError> {
        self.current_serving_linearized_authority()
            .await?
            .status()
            .await
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityRoutingHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityRoutingHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLinearizedCommandSink for ControlPlaneRaftAuthorityRoutingHandle {
    fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftFuture<'_, Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>>
    {
        Box::pin(
            ControlPlaneRaftAuthorityRoutingHandle::submit_control_plane_command(self, command),
        )
    }
}

impl ControlPlaneRaftLinearizedRuntimeMapSource for ControlPlaneRaftAuthorityRoutingHandle {
    fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> ControlPlaneRaftFuture<'_, Result<ClusterRuntimeMapSnapshot, ControlPlaneError>> {
        Box::pin(
            ControlPlaneRaftAuthorityRoutingHandle::linearized_runtime_map_snapshot(
                self,
                issued_at_ms,
            ),
        )
    }
}

impl ControlPlaneRaftAuthorityStatusSource for ControlPlaneRaftAuthorityRoutingHandle {
    fn status(
        &self,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError>>
    {
        Box::pin(ControlPlaneRaftAuthorityRoutingHandle::status(self))
    }
}

#[derive(Clone)]
pub struct ControlPlaneRaftLeaderRoutedAdminRoutingHandle {
    status_list: ControlPlaneRaftAuthorityStatusListHandle,
    directory: ControlPlaneRaftLeaderRoutedAdminDirectoryHandle,
}

impl ControlPlaneRaftLeaderRoutedAdminRoutingHandle {
    #[must_use]
    pub fn new(
        status_list: ControlPlaneRaftAuthorityStatusListHandle,
        directory: ControlPlaneRaftLeaderRoutedAdminDirectoryHandle,
    ) -> Self {
        Self {
            status_list,
            directory,
        }
    }

    pub async fn current_serving_leader_routed_admin(
        &self,
    ) -> Result<ControlPlaneRaftLeaderRoutedAdminHandle, ControlPlaneError> {
        let statuses = self.status_list.authority_statuses().await?;
        let node_id = current_serving_authority_node_id(&statuses)?;
        self.directory.leader_routed_admin_for_node(node_id).await
    }

    pub async fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        self.current_serving_leader_routed_admin()
            .await?
            .replace_voters(voters, retain_removed_voters_as_learners)
            .await
    }

    pub async fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        self.current_serving_leader_routed_admin()
            .await?
            .add_learner(node_id, node, wait_for_catch_up)
            .await
    }

    pub async fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        self.current_serving_leader_routed_admin()
            .await?
            .transfer_leadership_to(node_id)
            .await
    }
}

impl fmt::Debug for ControlPlaneRaftLeaderRoutedAdminRoutingHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftLeaderRoutedAdminRoutingHandle")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftLeaderRoutedAdmin for ControlPlaneRaftLeaderRoutedAdminRoutingHandle {
    fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        Box::pin(
            ControlPlaneRaftLeaderRoutedAdminRoutingHandle::replace_voters(
                self,
                voters,
                retain_removed_voters_as_learners,
            ),
        )
    }

    fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        Box::pin(ControlPlaneRaftLeaderRoutedAdminRoutingHandle::add_learner(
            self,
            node_id,
            node,
            wait_for_catch_up,
        ))
    }

    fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(
            ControlPlaneRaftLeaderRoutedAdminRoutingHandle::transfer_leadership_to(self, node_id),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneRaftAuthorityStatus {
    node_id: ControlPlaneRaftNodeId,
    current_leader: Option<ControlPlaneRaftNodeId>,
    server_state: ServerState,
    local_leader: bool,
    effective_voter: bool,
    effective_learner: bool,
    applied_voter: bool,
    applied_learner: bool,
    persisted_vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
    current_term: Option<ControlPlaneRaftTerm>,
    last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    current_snapshot: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    durable_wal_backed: bool,
    durable_wal_offsets: Option<ControlPlaneRaftWalOffsets>,
    durable_wal_poisoned: Option<String>,
    durable_last_vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
    durable_last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    durable_last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    durable_committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    durable_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    durable_timestamp_high_water_ms: Option<u64>,
    authority_incarnation: AuthorityIncarnation,
    current_cluster_epoch: ClusterEpoch,
    retained_history_count: usize,
    oldest_retained_history_epoch: Option<ClusterEpoch>,
    newest_retained_history_epoch: Option<ClusterEpoch>,
    oldest_storage_history_floor_epoch: Option<ClusterEpoch>,
    storage_node_lease_deadline_count: usize,
    earliest_storage_node_lease_deadline_ms: Option<u64>,
    latest_storage_node_lease_deadline_ms: Option<u64>,
    storage_node_count: usize,
    joining_storage_node_count: usize,
    active_storage_node_count: usize,
    draining_storage_node_count: usize,
    out_storage_node_count: usize,
    removed_storage_node_count: usize,
    healthy_storage_node_count: usize,
    suspect_storage_node_count: usize,
    unavailable_storage_node_count: usize,
    pg_count: usize,
    active_pg_count: usize,
    peering_pg_count: usize,
    degraded_pg_count: usize,
    backfilling_pg_count: usize,
    inconsistent_pg_count: usize,
    active_primary_pg_count: usize,
    peering_metadata_transfer_pg_count: usize,
    metadata_transfer_fenced_pg_count: usize,
    metadata_transfer_fence_source_lease_deadline_count: usize,
    earliest_metadata_transfer_fence_source_lease_deadline_ms: Option<u64>,
    latest_metadata_transfer_fence_source_lease_deadline_ms: Option<u64>,
    effective_membership_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    effective_voters: BTreeSet<ControlPlaneRaftNodeId>,
    effective_learners: BTreeSet<ControlPlaneRaftNodeId>,
    applied_membership_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    applied_voters: BTreeSet<ControlPlaneRaftNodeId>,
    applied_learners: BTreeSet<ControlPlaneRaftNodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneRaftLinearizedAuthorityReadiness {
    Serving,
    NotLocalLeader,
    NotEffectiveVoter,
    NotAppliedToCommitted,
    NotCommittedInCurrentTerm,
}

impl ControlPlaneRaftLinearizedAuthorityReadiness {
    #[must_use]
    pub fn serving(self) -> bool {
        matches!(self, Self::Serving)
    }
}

#[must_use]
fn linearized_authority_readiness_from_flags(
    local_leader: bool,
    effective_voter: bool,
    applied_caught_up_to_committed: bool,
    committed_in_current_term: bool,
) -> ControlPlaneRaftLinearizedAuthorityReadiness {
    if !local_leader {
        ControlPlaneRaftLinearizedAuthorityReadiness::NotLocalLeader
    } else if !effective_voter {
        ControlPlaneRaftLinearizedAuthorityReadiness::NotEffectiveVoter
    } else if !applied_caught_up_to_committed {
        ControlPlaneRaftLinearizedAuthorityReadiness::NotAppliedToCommitted
    } else if !committed_in_current_term {
        ControlPlaneRaftLinearizedAuthorityReadiness::NotCommittedInCurrentTerm
    } else {
        ControlPlaneRaftLinearizedAuthorityReadiness::Serving
    }
}

impl ControlPlaneRaftAuthorityStatus {
    #[must_use]
    pub fn node_id(&self) -> ControlPlaneRaftNodeId {
        self.node_id
    }

    #[must_use]
    pub fn current_leader(&self) -> Option<ControlPlaneRaftNodeId> {
        self.current_leader
    }

    #[must_use]
    pub fn server_state(&self) -> ServerState {
        self.server_state
    }

    #[must_use]
    pub fn local_leader(&self) -> bool {
        self.local_leader
    }

    #[must_use]
    pub fn effective_voter(&self) -> bool {
        self.effective_voter
    }

    #[must_use]
    pub fn effective_learner(&self) -> bool {
        self.effective_learner
    }

    #[must_use]
    pub fn applied_voter(&self) -> bool {
        self.applied_voter
    }

    #[must_use]
    pub fn applied_learner(&self) -> bool {
        self.applied_learner
    }

    #[must_use]
    pub fn linearized_authority_serving(&self) -> bool {
        self.linearized_authority_readiness().serving()
    }

    #[must_use]
    pub fn linearized_authority_readiness(&self) -> ControlPlaneRaftLinearizedAuthorityReadiness {
        linearized_authority_readiness_from_flags(
            self.local_leader,
            self.effective_voter,
            self.applied_caught_up_to_committed(),
            self.committed_in_current_term(),
        )
    }

    #[must_use]
    pub fn persisted_vote(&self) -> Option<VoteOf<ControlPlaneRaftTypeConfig>> {
        self.persisted_vote
    }

    #[must_use]
    pub fn current_term(&self) -> Option<ControlPlaneRaftTerm> {
        self.current_term
    }

    #[must_use]
    pub fn last_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.last_log_id
    }

    #[must_use]
    pub fn last_log_index(&self) -> Option<u64> {
        self.last_log_id.map(|log_id| log_id.index())
    }

    #[must_use]
    pub fn last_purged_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.last_purged_log_id
    }

    #[must_use]
    pub fn last_purged_index(&self) -> Option<u64> {
        self.last_purged_log_id.map(|log_id| log_id.index())
    }

    #[must_use]
    pub fn committed(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.committed
    }

    #[must_use]
    pub fn committed_index(&self) -> Option<u64> {
        self.committed.map(|log_id| log_id.index())
    }

    #[must_use]
    pub fn applied(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.applied
    }

    #[must_use]
    pub fn applied_index(&self) -> Option<u64> {
        self.applied.map(|log_id| log_id.index())
    }

    #[must_use]
    pub fn current_snapshot(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.current_snapshot
    }

    #[must_use]
    pub fn current_snapshot_index(&self) -> Option<u64> {
        self.current_snapshot.map(|log_id| log_id.index())
    }

    #[must_use]
    pub fn durable_wal_backed(&self) -> bool {
        self.durable_wal_backed
    }

    #[must_use]
    pub fn durable_wal_offsets(&self) -> Option<ControlPlaneRaftWalOffsets> {
        self.durable_wal_offsets
    }

    #[must_use]
    pub fn durable_wal_poisoned(&self) -> Option<&str> {
        self.durable_wal_poisoned.as_deref()
    }

    #[must_use]
    pub fn durable_last_vote(&self) -> Option<VoteOf<ControlPlaneRaftTypeConfig>> {
        self.durable_last_vote
    }

    #[must_use]
    pub fn durable_last_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.durable_last_log_id
    }

    #[must_use]
    pub fn durable_last_purged_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.durable_last_purged_log_id
    }

    #[must_use]
    pub fn durable_committed(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.durable_committed
    }

    #[must_use]
    pub fn durable_applied(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.durable_applied
    }

    #[must_use]
    pub fn durable_timestamp_high_water_ms(&self) -> Option<u64> {
        self.durable_timestamp_high_water_ms
    }

    #[must_use]
    pub fn committed_to_applied_index_gap(&self) -> Option<i128> {
        Some(i128::from(self.committed?.index()) - i128::from(self.applied?.index()))
    }

    #[must_use]
    pub fn last_log_to_committed_index_gap(&self) -> Option<i128> {
        Some(i128::from(self.last_log_id?.index()) - i128::from(self.committed?.index()))
    }

    #[must_use]
    pub fn applied_caught_up_to_committed(&self) -> bool {
        self.committed.is_some() && self.committed == self.applied
    }

    #[must_use]
    pub fn committed_in_current_term(&self) -> bool {
        matches!(
            (self.current_term, self.committed),
            (Some(current_term), Some(committed))
                if committed.committed_leader_id().term == current_term
        )
    }

    #[must_use]
    pub fn committed_caught_up_to_last_log(&self) -> bool {
        self.last_log_to_committed_index_gap() == Some(0)
    }

    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn current_cluster_epoch(&self) -> ClusterEpoch {
        self.current_cluster_epoch
    }

    #[must_use]
    pub fn retained_history_count(&self) -> usize {
        self.retained_history_count
    }

    #[must_use]
    pub fn oldest_retained_history_epoch(&self) -> Option<ClusterEpoch> {
        self.oldest_retained_history_epoch
    }

    #[must_use]
    pub fn newest_retained_history_epoch(&self) -> Option<ClusterEpoch> {
        self.newest_retained_history_epoch
    }

    #[must_use]
    pub fn oldest_storage_history_floor_epoch(&self) -> Option<ClusterEpoch> {
        self.oldest_storage_history_floor_epoch
    }

    #[must_use]
    pub fn storage_node_lease_deadline_count(&self) -> usize {
        self.storage_node_lease_deadline_count
    }

    #[must_use]
    pub fn earliest_storage_node_lease_deadline_ms(&self) -> Option<u64> {
        self.earliest_storage_node_lease_deadline_ms
    }

    #[must_use]
    pub fn latest_storage_node_lease_deadline_ms(&self) -> Option<u64> {
        self.latest_storage_node_lease_deadline_ms
    }

    #[must_use]
    pub fn storage_node_count(&self) -> usize {
        self.storage_node_count
    }

    #[must_use]
    pub fn joining_storage_node_count(&self) -> usize {
        self.joining_storage_node_count
    }

    #[must_use]
    pub fn active_storage_node_count(&self) -> usize {
        self.active_storage_node_count
    }

    #[must_use]
    pub fn draining_storage_node_count(&self) -> usize {
        self.draining_storage_node_count
    }

    #[must_use]
    pub fn out_storage_node_count(&self) -> usize {
        self.out_storage_node_count
    }

    #[must_use]
    pub fn removed_storage_node_count(&self) -> usize {
        self.removed_storage_node_count
    }

    #[must_use]
    pub fn healthy_storage_node_count(&self) -> usize {
        self.healthy_storage_node_count
    }

    #[must_use]
    pub fn suspect_storage_node_count(&self) -> usize {
        self.suspect_storage_node_count
    }

    #[must_use]
    pub fn unavailable_storage_node_count(&self) -> usize {
        self.unavailable_storage_node_count
    }

    #[must_use]
    pub fn pg_count(&self) -> usize {
        self.pg_count
    }

    #[must_use]
    pub fn active_pg_count(&self) -> usize {
        self.active_pg_count
    }

    #[must_use]
    pub fn peering_pg_count(&self) -> usize {
        self.peering_pg_count
    }

    #[must_use]
    pub fn degraded_pg_count(&self) -> usize {
        self.degraded_pg_count
    }

    #[must_use]
    pub fn backfilling_pg_count(&self) -> usize {
        self.backfilling_pg_count
    }

    #[must_use]
    pub fn inconsistent_pg_count(&self) -> usize {
        self.inconsistent_pg_count
    }

    #[must_use]
    pub fn active_primary_pg_count(&self) -> usize {
        self.active_primary_pg_count
    }

    #[must_use]
    pub fn peering_metadata_transfer_pg_count(&self) -> usize {
        self.peering_metadata_transfer_pg_count
    }

    #[must_use]
    pub fn metadata_transfer_fenced_pg_count(&self) -> usize {
        self.metadata_transfer_fenced_pg_count
    }

    #[must_use]
    pub fn metadata_transfer_fence_source_lease_deadline_count(&self) -> usize {
        self.metadata_transfer_fence_source_lease_deadline_count
    }

    #[must_use]
    pub fn earliest_metadata_transfer_fence_source_lease_deadline_ms(&self) -> Option<u64> {
        self.earliest_metadata_transfer_fence_source_lease_deadline_ms
    }

    #[must_use]
    pub fn latest_metadata_transfer_fence_source_lease_deadline_ms(&self) -> Option<u64> {
        self.latest_metadata_transfer_fence_source_lease_deadline_ms
    }

    #[must_use]
    pub fn effective_membership_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.effective_membership_log_id
    }

    #[must_use]
    pub fn effective_voters(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.effective_voters
    }

    #[must_use]
    pub fn effective_learners(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.effective_learners
    }

    #[must_use]
    pub fn applied_membership_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.applied_membership_log_id
    }

    #[must_use]
    pub fn applied_voters(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.applied_voters
    }

    #[must_use]
    pub fn applied_learners(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.applied_learners
    }
}

fn validate_static_initial_raft_membership(
    policy: &ControlPlaneRaftPeerTransportPolicy,
    status: &ControlPlaneRaftAuthorityStatus,
) -> Result<(), ControlPlaneError> {
    if status.applied() != status.committed() {
        return Err(ControlPlaneError::static_topology_failure(format!(
            "static control-plane identity cannot be established before applied state {:?} catches up to committed state {:?}",
            status.applied(),
            status.committed()
        )));
    }
    let expected_voters = policy.peers().keys().copied().collect::<BTreeSet<_>>();
    let effective_log_id = status.effective_membership_log_id().ok_or_else(|| {
        ControlPlaneError::static_topology_failure(
            "static control-plane identity cannot be established before effective Raft membership",
        )
    })?;
    let applied_log_id = status.applied_membership_log_id().ok_or_else(|| {
        ControlPlaneError::static_topology_failure(
            "static control-plane identity cannot be established before applied Raft membership",
        )
    })?;
    if status.effective_voters() != &expected_voters
        || !status.effective_learners().is_empty()
        || status.applied_voters() != &expected_voters
        || !status.applied_learners().is_empty()
        || applied_log_id != effective_log_id
    {
        return Err(ControlPlaneError::static_topology_failure(format!(
            "static control-plane identity membership does not match configured topology; expected_voters={expected_voters:?} effective_voters={:?} effective_learners={:?} applied_voters={:?} applied_learners={:?} effective_log_id={effective_log_id} applied_log_id={applied_log_id}",
            status.effective_voters(),
            status.effective_learners(),
            status.applied_voters(),
            status.applied_learners(),
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct SingleNodeRaftNetworkFactory;

impl RaftNetworkFactory<ControlPlaneRaftTypeConfig> for SingleNodeRaftNetworkFactory {
    type Network = SingleNodeRaftNetwork;

    async fn new_client(
        &mut self,
        target: ControlPlaneRaftNodeId,
        _node: &BasicNode,
    ) -> Self::Network {
        SingleNodeRaftNetwork { target }
    }
}

#[derive(Debug, Clone, Copy)]
struct SingleNodeRaftNetwork {
    target: ControlPlaneRaftNodeId,
}

impl SingleNodeRaftNetwork {
    fn unreachable(&self, rpc_name: &'static str) -> Unreachable<ControlPlaneRaftTypeConfig> {
        Unreachable::new(&AnyError::error(format!(
            "single-node control-plane raft network has no remote target {} for {rpc_name}",
            self.target
        )))
    }
}

impl RaftNetworkV2<ControlPlaneRaftTypeConfig> for SingleNodeRaftNetwork {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RaftTimerMode {
    Manual,
    Automatic,
}

const CONTROL_PLANE_RAFT_MAX_PAYLOAD_ENTRIES: u64 =
    ControlPlaneRaftPeerTransportLimits::REPLICATION_REQUIRED_APPEND_ENTRIES as u64;
const CONTROL_PLANE_RAFT_APPEND_ENTRIES_COUNT_BYTES: usize = 4;
pub(crate) const CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES: usize =
    (ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES_BYTES
        - CONTROL_PLANE_RAFT_APPEND_ENTRIES_COUNT_BYTES)
        / CONTROL_PLANE_RAFT_MAX_PAYLOAD_ENTRIES as usize;
const CONTROL_PLANE_RAFT_NORMAL_COMMAND_ENTRY_ENVELOPE_BYTES: usize =
    3 * std::mem::size_of::<u64>() + std::mem::size_of::<u8>() + std::mem::size_of::<u32>();
pub(crate) const CONTROL_PLANE_RAFT_MAX_COMMAND_BYTES: usize =
    CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES
        - CONTROL_PLANE_RAFT_NORMAL_COMMAND_ENTRY_ENVELOPE_BYTES;
const _: () = assert!(
    CONTROL_PLANE_RAFT_MAX_PAYLOAD_ENTRIES
        <= ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES as u64
);
const _: () = assert!(
    CONTROL_PLANE_RAFT_APPEND_ENTRIES_COUNT_BYTES
        + CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES
            * CONTROL_PLANE_RAFT_MAX_PAYLOAD_ENTRIES as usize
        <= ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES_BYTES
);

fn raft_config(
    cluster_name: impl Into<String>,
    timer_mode: RaftTimerMode,
) -> Result<Arc<Config>, ControlPlaneError> {
    let timers_enabled = matches!(timer_mode, RaftTimerMode::Automatic);
    Ok(Arc::new(
        Config {
            cluster_name: cluster_name.into(),
            // Peer acknowledgements include WAL and checkpoint fsyncs. Leave
            // enough election margin for that durability boundary under I/O
            // contention without delaying control-plane failover excessively.
            heartbeat_interval: 250,
            election_timeout_min: 1_500,
            election_timeout_max: 3_000,
            max_payload_entries: CONTROL_PLANE_RAFT_MAX_PAYLOAD_ENTRIES,
            // The process restart boundary is its artifact plus WAL, not
            // OpenRaft's in-memory snapshot cache. Automatic snapshot-driven
            // purge could therefore discard the only replayable prefix before
            // the cache is published in a restart artifact. The application
            // coordinates explicit snapshot/purge with artifact persistence.
            snapshot_policy: SnapshotPolicy::Never,
            // OpenRaft schedules policy-based purge after every completed
            // snapshot, including manually triggered snapshots. Suppress that
            // implicit purge; the explicit trigger().purge_log() path ignores
            // this retention value and is coordinated with artifact writes.
            max_in_snapshot_log_to_keep: u64::MAX,
            enable_tick: timers_enabled,
            enable_heartbeat: timers_enabled,
            enable_elect: timers_enabled,
            ..Default::default()
        }
        .validate()
        .map_err(|error| {
            ControlPlaneError::rpc_remote(format!("OpenRaft config failed: {error}"))
        })?,
    ))
}

fn single_node_raft_config(
    cluster_name: impl Into<String>,
) -> Result<Arc<Config>, ControlPlaneError> {
    raft_config(cluster_name, RaftTimerMode::Manual)
}

fn restore_raft_durable_artifact(
    cluster_name: &str,
    node_id: ControlPlaneRaftNodeId,
    artifact_path: &Path,
    wal_path: Option<&Path>,
    validate_artifact: impl Fn(&ControlPlaneRaftRestartArtifact) -> Result<(), ControlPlaneError>,
) -> Result<(ControlPlaneRaftLogStore, ControlPlaneRaftStateMachine), ControlPlaneError> {
    let sentinel_path = durable_artifact_sentinel_path(artifact_path);
    match ControlPlaneRaftRestartArtifact::load_durable_artifact_for_restore(artifact_path) {
        Ok(artifact) => {
            let sentinel = ControlPlaneRaftRestartSentinel::load_durable_sentinel(&sentinel_path)
                .map_err(|error| match error {
                    ControlPlaneError::Io { diagnostic: source }
                        if source.kind() == io::ErrorKind::NotFound =>
                    {
                        raft_artifact_protocol_error(format!(
                            "control-plane OpenRaft durable restart sentinel {} is missing for existing artifact {}",
                            sentinel_path.display(),
                            artifact_path.display()
                        ))
                    }
                    other => other,
                })?;
            sentinel.validate_identity(cluster_name, node_id)?;
            artifact.validate_cluster_identity(cluster_name)?;
            artifact.validate_local_node_identity(node_id)?;
            validate_artifact(&artifact)?;
            if let Some(wal_path) = wal_path {
                artifact.restore_with_wal_file_validated(
                    ControlPlaneRaftWalFile::new(ControlPlaneRaftWalFileConfig {
                        path: wal_path.to_path_buf(),
                        cluster_name: cluster_name.to_owned(),
                        local_node_id: node_id,
                    }),
                    validate_artifact,
                )
            } else {
                artifact.restore().map_err(|source| {
                    ControlPlaneError::io(
                        "restore control-plane OpenRaft durable restart artifact",
                        source,
                    )
                })
            }
        }
        Err(ControlPlaneError::Io { diagnostic: source })
            if source.kind() == io::ErrorKind::NotFound =>
        {
            match ControlPlaneRaftRestartSentinel::load_durable_sentinel(&sentinel_path) {
                Ok(sentinel) => {
                    sentinel.validate_identity(cluster_name, node_id)?;
                    return Err(raft_artifact_protocol_error(format!(
                        "control-plane OpenRaft durable restart artifact {} is missing but sentinel {} records existing state for cluster {:?} node {}",
                        artifact_path.display(),
                        sentinel_path.display(),
                        sentinel.cluster_name,
                        sentinel.local_node_id
                    )));
                }
                Err(ControlPlaneError::Io { diagnostic: source })
                    if source.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            if let Some(wal_path) = wal_path {
                match std::fs::metadata(wal_path) {
                    Ok(metadata) if metadata.len() != 0 => {
                        return Err(raft_artifact_protocol_error(format!(
                            "control-plane OpenRaft WAL {} exists without durable restart artifact {}",
                            wal_path.display(),
                            artifact_path.display()
                        )));
                    }
                    Ok(_) => {}
                    Err(source) if source.kind() == io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(ControlPlaneError::io(
                            "stat control-plane OpenRaft WAL before empty startup",
                            source,
                        ));
                    }
                }
                let wal = ControlPlaneRaftWalFile::new(ControlPlaneRaftWalFileConfig {
                    path: wal_path.to_path_buf(),
                    cluster_name: cluster_name.to_owned(),
                    local_node_id: node_id,
                });
                return ControlPlaneRaftLogStore::from_restart_artifact_inner(
                    ControlPlaneRaftLogStoreRestartArtifact::default(),
                    Some(Arc::new(wal)),
                )
                .map(|log_store| (log_store, ControlPlaneRaftStateMachine::empty()))
                .map_err(|source| {
                    ControlPlaneError::io(
                        "restore empty WAL-backed control-plane OpenRaft log store",
                        source,
                    )
                });
            }
            Ok((
                ControlPlaneRaftLogStore::empty(),
                ControlPlaneRaftStateMachine::empty(),
            ))
        }
        Err(error) => Err(error),
    }
}

fn control_plane_raft_durable_purge_covers(
    status: &ControlPlaneRaftLogStoreStatusSnapshot,
    target: LogIdOf<ControlPlaneRaftTypeConfig>,
) -> Result<bool, ControlPlaneError> {
    if let Some(reason) = &status.durability.wal_poisoned {
        return Err(ControlPlaneError::rpc_remote(format!(
            "OpenRaft snapshot purge WAL durability failed: {reason}"
        )));
    }
    let Some(purged) = status.durable_last_purged_log_id else {
        return Ok(false);
    };
    match purged.index().cmp(&target.index()) {
        std::cmp::Ordering::Less => Ok(false),
        std::cmp::Ordering::Greater => Ok(true),
        std::cmp::Ordering::Equal if purged == target => Ok(true),
        std::cmp::Ordering::Equal => Err(ControlPlaneError::rpc_remote(format!(
                "OpenRaft durable snapshot purge watermark {purged} conflicts with requested log id {target} at the same index"
            ))),
    }
}

impl ControlPlaneRaftAuthority {
    pub(crate) fn peer_server_binding(
        &self,
    ) -> Option<(ControlPlaneRaftNodeId, ControlPlaneRaftPeerTransportPolicy)> {
        self.static_peer_policy
            .clone()
            .map(|policy| (self.node_id, policy))
    }

    #[must_use]
    pub(crate) fn authority_clock_checkpoint_binding(
        &self,
    ) -> ControlPlaneAuthorityClockCheckpointBinding {
        ControlPlaneAuthorityClockCheckpointBinding::for_raft(&self.cluster_name, self.node_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn authority_clock_checkpoint_binding_for_test(
        &self,
    ) -> ControlPlaneAuthorityClockCheckpointBinding {
        self.authority_clock_checkpoint_binding()
    }

    pub async fn new_single_node_in_memory(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<Self, ControlPlaneError> {
        let cluster_name = cluster_name.into();
        let config = single_node_raft_config(cluster_name.clone())?;
        let log_store = ControlPlaneRaftLogStore::empty();
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node_id,
            config,
            SingleNodeRaftNetworkFactory,
            log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .map_err(|error| openraft_remote_error("new single-node authority", error))?;
        Ok(Self::new_with_log_store(raft, log_store, cluster_name))
    }

    /// Construct an in-memory Raft authority whose explicit checkpoint
    /// publication is durable, for cross-crate behavioral tests.
    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn new_single_node_in_memory_with_checkpoint_for_test(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
    ) -> Result<Self, ControlPlaneError> {
        Ok(Self::new_single_node_in_memory(cluster_name, node_id)
            .await?
            .with_durable_artifact_path(artifact_path))
    }

    pub(crate) async fn new_single_node_durable(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
    ) -> Result<Self, ControlPlaneError> {
        let wal_path = durable_artifact_wal_path(artifact_path);
        Self::new_single_node_durable_inner(cluster_name, node_id, artifact_path, Some(&wal_path))
            .await
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn new_single_node_durable_for_test(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
    ) -> Result<Self, ControlPlaneError> {
        Self::new_single_node_durable(cluster_name, node_id, artifact_path).await
    }

    #[cfg(test)]
    async fn new_single_node_durable_with_wal(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
        wal_path: &Path,
    ) -> Result<Self, ControlPlaneError> {
        Self::new_single_node_durable_inner(cluster_name, node_id, artifact_path, Some(wal_path))
            .await
    }

    async fn new_single_node_durable_inner(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
        wal_path: Option<&Path>,
    ) -> Result<Self, ControlPlaneError> {
        let cluster_name = cluster_name.into();
        let config = single_node_raft_config(cluster_name.clone())?;
        let (log_store, state_machine) = restore_raft_durable_artifact(
            &cluster_name,
            node_id,
            artifact_path,
            wal_path,
            |artifact| artifact.validate_single_node_local_identity(node_id),
        )?;
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node_id,
            config,
            SingleNodeRaftNetworkFactory,
            log_store.clone(),
            state_machine,
        )
        .await
        .map_err(|error| openraft_remote_error("new durable single-node authority", error))?;
        Ok(Self::new_with_log_store(raft, log_store, cluster_name)
            .with_durable_artifact_path(artifact_path))
    }

    #[cfg(test)]
    pub(crate) async fn new_unix_peer_durable(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
        peer_policy: ControlPlaneRaftPeerTransportPolicy,
        rpc_timeout: Duration,
    ) -> Result<Self, ControlPlaneError> {
        let wal_path = durable_artifact_wal_path(artifact_path);
        Self::new_peer_durable_inner(
            cluster_name,
            node_id,
            artifact_path,
            Some(&wal_path),
            peer_policy,
            ControlPlaneRaftPeerNetworkConfig::unix(rpc_timeout),
            None,
        )
        .await
    }

    #[cfg(test)]
    async fn new_unix_peer_durable_with_wal(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
        wal_path: &Path,
        peer_policy: ControlPlaneRaftPeerTransportPolicy,
        rpc_timeout: Duration,
    ) -> Result<Self, ControlPlaneError> {
        Self::new_peer_durable_inner(
            cluster_name,
            node_id,
            artifact_path,
            Some(wal_path),
            peer_policy,
            ControlPlaneRaftPeerNetworkConfig::unix(rpc_timeout),
            None,
        )
        .await
    }

    #[cfg(test)]
    async fn new_unix_peer_durable_with_wal_pending_static_initialization(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
        wal_path: &Path,
        peer_policy: ControlPlaneRaftPeerTransportPolicy,
        expected_bootstrap: ControlPlaneCommand,
        rpc_timeout: Duration,
    ) -> Result<Self, ControlPlaneError> {
        Self::new_peer_durable_inner(
            cluster_name,
            node_id,
            artifact_path,
            Some(wal_path),
            peer_policy,
            ControlPlaneRaftPeerNetworkConfig::unix(rpc_timeout),
            Some(expected_bootstrap),
        )
        .await
    }

    pub(crate) async fn new_peer_durable_network(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
        peer_policy: ControlPlaneRaftPeerTransportPolicy,
        network: ControlPlaneRaftPeerNetworkConfig,
    ) -> Result<Self, ControlPlaneError> {
        let wal_path = durable_artifact_wal_path(artifact_path);
        Self::new_peer_durable_inner(
            cluster_name,
            node_id,
            artifact_path,
            Some(&wal_path),
            peer_policy,
            network,
            None,
        )
        .await
    }

    pub(crate) async fn new_peer_durable_pending_static_initialization_network(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
        peer_policy: ControlPlaneRaftPeerTransportPolicy,
        expected_topology: &StaticInitialControlPlaneTopology,
        network: ControlPlaneRaftPeerNetworkConfig,
    ) -> Result<Self, ControlPlaneError> {
        let wal_path = durable_artifact_wal_path(artifact_path);
        Self::new_peer_durable_inner(
            cluster_name,
            node_id,
            artifact_path,
            Some(&wal_path),
            peer_policy,
            network,
            Some(expected_topology.bootstrap_command()),
        )
        .await
    }

    async fn new_peer_durable_inner(
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        artifact_path: &Path,
        wal_path: Option<&Path>,
        peer_policy: ControlPlaneRaftPeerTransportPolicy,
        network: ControlPlaneRaftPeerNetworkConfig,
        pending_static_bootstrap: Option<ControlPlaneCommand>,
    ) -> Result<Self, ControlPlaneError> {
        let cluster_name = cluster_name.into();
        peer_policy.validate_cluster_name(&cluster_name)?;
        peer_policy.validate_local_node(node_id)?;
        peer_policy.validate_replication_compatibility()?;
        network.validate_policy(&peer_policy)?;
        if let Some(expected_bootstrap) = pending_static_bootstrap.as_ref() {
            if peer_policy.topology_identity().is_none() {
                return Err(raft_artifact_protocol_error(
                    "pending static initialization requires a peer-policy topology identity",
                ));
            }
            let expected_snapshot = ClusterControlSnapshot::empty()
                .apply_control_plane_command(expected_bootstrap.clone())?
                .into_snapshot();
            validate_captured_static_initial_topology(&expected_snapshot, &peer_policy)?;
        }
        let config = raft_config(cluster_name.clone(), RaftTimerMode::Automatic)?;
        let policy_for_restore = peer_policy.clone();
        let (log_store, state_machine) = restore_raft_durable_artifact(
            &cluster_name,
            node_id,
            artifact_path,
            wal_path,
            |artifact| {
                artifact.validate_peer_policy_membership(&policy_for_restore)?;
                artifact.validate_static_initial_topology_restore(
                    &policy_for_restore,
                    pending_static_bootstrap.as_ref(),
                )
            },
        )?;
        let network_factory = ControlPlaneRaftPeerNetworkFactory::new_with_transport(
            node_id,
            peer_policy.clone(),
            network.rpc_timeout(),
            network.frame_transport(),
        );
        let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node_id,
            config,
            network_factory,
            log_store.clone(),
            state_machine,
        )
        .await
        .map_err(|error| openraft_remote_error("new peer authority", error))?;
        Ok(Self::new_with_log_store_and_static_peer_policy(
            raft,
            log_store,
            cluster_name,
            peer_policy,
        )
        .with_durable_artifact_path(artifact_path))
    }

    #[must_use]
    fn new_with_log_store(
        raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
        log_store: ControlPlaneRaftLogStore,
        cluster_name: impl Into<String>,
    ) -> Self {
        let node_id = *raft.node_id();
        Self {
            cluster_name: cluster_name.into(),
            node_id,
            raft,
            log_store: Some(log_store),
            static_peer_policy: None,
            volatile_heartbeat_update_gate: tokio::sync::Mutex::new(()),
            evidence_submission_admission: ControlPlaneRaftEvidenceSubmissionAdmission::default(),
            volatile_heartbeat_overlay: Mutex::new(None),
            runtime_map_content_certificate: Mutex::new(None),
            runtime_map_overlay_content_certificate: Mutex::new(None),
            authority_instance_id: OnceLock::new(),
            durability_publication: OnceLock::new(),
            durability_lifecycle: OnceLock::new(),
            authority_host_lifecycle: OnceLock::new(),
            uncertified_initial_topology_checkpoint_published: OnceLock::new(),
            checkpoint_publication: Arc::new(Mutex::new(None)),
            durable_artifact_path: None,
            checkpoint_metrics: Arc::new(ControlPlaneRaftCheckpointMetrics::default()),
            command_metrics: Arc::new(ControlPlaneRaftCommandMetrics::default()),
            #[cfg(test)]
            proposal_pause_after_confirmation: Mutex::new(None),
            #[cfg(test)]
            proposal_lease_retry_count: AtomicUsize::new(0),
            #[cfg(test)]
            proposal_changed_tip_rejection_count: AtomicUsize::new(0),
            #[cfg(test)]
            membership_initialization_after_check_gate: Mutex::new(None),
            #[cfg(test)]
            linearized_read_after_snapshot_gate: Mutex::new(None),
            #[cfg(test)]
            linearized_read_after_raft_state_capture_gate: Mutex::new(None),
            #[cfg(test)]
            linearized_read_after_generation_capture_gate: Mutex::new(None),
            #[cfg(test)]
            linearized_runtime_map_read_index_count: AtomicUsize::new(0),
            #[cfg(test)]
            linearized_snapshot_retirement_hook: Mutex::new(None),
            #[cfg(test)]
            linearized_state_machine_response_ready_notify: Mutex::new(None),
            #[cfg(test)]
            before_heartbeat_update_gate_hook: Mutex::new(None),
            #[cfg(test)]
            low_priority_before_update_gate: Mutex::new(None),
            #[cfg(test)]
            low_priority_after_update_gate: Mutex::new(None),
            #[cfg(test)]
            ordinary_durable_after_update_gate: Mutex::new(None),
            #[cfg(test)]
            low_priority_before_dispatch: Mutex::new(None),
            #[cfg(test)]
            low_priority_after_dispatch: Mutex::new(None),
            #[cfg(test)]
            low_priority_during_retry_certification: Mutex::new(None),
            #[cfg(test)]
            low_priority_after_proven_unappended_retry: Mutex::new(None),
        }
    }

    #[must_use]
    fn new_with_log_store_and_static_peer_policy(
        raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
        log_store: ControlPlaneRaftLogStore,
        cluster_name: impl Into<String>,
        peer_policy: ControlPlaneRaftPeerTransportPolicy,
    ) -> Self {
        let node_id = *raft.node_id();
        Self {
            cluster_name: cluster_name.into(),
            node_id,
            raft,
            log_store: Some(log_store),
            static_peer_policy: Some(peer_policy),
            volatile_heartbeat_update_gate: tokio::sync::Mutex::new(()),
            evidence_submission_admission: ControlPlaneRaftEvidenceSubmissionAdmission::default(),
            volatile_heartbeat_overlay: Mutex::new(None),
            runtime_map_content_certificate: Mutex::new(None),
            runtime_map_overlay_content_certificate: Mutex::new(None),
            authority_instance_id: OnceLock::new(),
            durability_publication: OnceLock::new(),
            durability_lifecycle: OnceLock::new(),
            authority_host_lifecycle: OnceLock::new(),
            uncertified_initial_topology_checkpoint_published: OnceLock::new(),
            checkpoint_publication: Arc::new(Mutex::new(None)),
            durable_artifact_path: None,
            checkpoint_metrics: Arc::new(ControlPlaneRaftCheckpointMetrics::default()),
            command_metrics: Arc::new(ControlPlaneRaftCommandMetrics::default()),
            #[cfg(test)]
            proposal_pause_after_confirmation: Mutex::new(None),
            #[cfg(test)]
            proposal_lease_retry_count: AtomicUsize::new(0),
            #[cfg(test)]
            proposal_changed_tip_rejection_count: AtomicUsize::new(0),
            #[cfg(test)]
            membership_initialization_after_check_gate: Mutex::new(None),
            #[cfg(test)]
            linearized_read_after_snapshot_gate: Mutex::new(None),
            #[cfg(test)]
            linearized_read_after_raft_state_capture_gate: Mutex::new(None),
            #[cfg(test)]
            linearized_read_after_generation_capture_gate: Mutex::new(None),
            #[cfg(test)]
            linearized_runtime_map_read_index_count: AtomicUsize::new(0),
            #[cfg(test)]
            linearized_snapshot_retirement_hook: Mutex::new(None),
            #[cfg(test)]
            linearized_state_machine_response_ready_notify: Mutex::new(None),
            #[cfg(test)]
            before_heartbeat_update_gate_hook: Mutex::new(None),
            #[cfg(test)]
            low_priority_before_update_gate: Mutex::new(None),
            #[cfg(test)]
            low_priority_after_update_gate: Mutex::new(None),
            #[cfg(test)]
            ordinary_durable_after_update_gate: Mutex::new(None),
            #[cfg(test)]
            low_priority_before_dispatch: Mutex::new(None),
            #[cfg(test)]
            low_priority_after_dispatch: Mutex::new(None),
            #[cfg(test)]
            low_priority_during_retry_certification: Mutex::new(None),
            #[cfg(test)]
            low_priority_after_proven_unappended_retry: Mutex::new(None),
        }
    }

    #[must_use]
    fn raft(&self) -> &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine> {
        &self.raft
    }

    fn authority_instance_id(
        &self,
    ) -> Result<ControlPlaneRaftAuthorityInstanceId, ControlPlaneError> {
        if let Some(identity) = self.authority_instance_id.get() {
            return Ok(*identity);
        }
        let generated = ControlPlaneRaftAuthorityInstanceId::generate()?;
        Ok(*self.authority_instance_id.get_or_init(|| generated))
    }

    /// Return the single response-publication and durability-poison domain
    /// bound to this authority.
    pub(crate) fn durability_publication(
        &self,
    ) -> Result<ControlPlaneRaftDurabilityPublication, ControlPlaneError> {
        if let Some(publication) = self.durability_publication.get() {
            return Ok(publication.clone());
        }
        let publication = ControlPlaneRaftDurabilityPublication::new(self.authority_instance_id()?);
        Ok(self
            .durability_publication
            .get_or_init(|| publication)
            .clone())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn durability_publication_for_test(
        &self,
    ) -> Result<ControlPlaneRaftDurabilityPublication, ControlPlaneError> {
        self.durability_publication()
    }

    /// Issue the one shared durability lifecycle bound to this authority.
    ///
    /// Repeated calls while the authority is hosted return clones of the same
    /// lifecycle, including its checkpoint lock, serving marker, and monitor
    /// registration state.
    pub(crate) fn durability_lifecycle(
        self: &Arc<Self>,
        runtime: tokio::runtime::Handle,
    ) -> Result<crate::ControlPlaneRaftAuthorityDurability, ControlPlaneError> {
        crate::control_plane_raft_durability::ControlPlaneRaftAuthorityDurability::issue(
            runtime,
            Arc::clone(self),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn durability_lifecycle_for_test(
        self: &Arc<Self>,
        runtime: tokio::runtime::Handle,
    ) -> Result<crate::ControlPlaneRaftAuthorityDurability, ControlPlaneError> {
        self.durability_lifecycle(runtime)
    }

    pub(crate) fn durability_lifecycle_slot(
        &self,
    ) -> &OnceLock<Arc<crate::control_plane_raft_durability::DurabilityInner>> {
        &self.durability_lifecycle
    }

    pub(crate) fn authority_host_lifecycle_slot(
        &self,
    ) -> &OnceLock<Arc<crate::control_plane_raft_host::DurableAuthorityHostLifecycle>> {
        &self.authority_host_lifecycle
    }

    /// Bind process-hosted peer checkpoint work to this authority's durability
    /// publication and poison domain.
    pub(crate) fn bind_peer_server_durability(
        &self,
        checkpoint: Arc<dyn ControlPlaneRaftPeerServerCheckpoint>,
    ) -> Result<ControlPlaneRaftPeerServerDurability, ControlPlaneError> {
        let authority_instance_id = self.authority_instance_id()?;
        let publication = self.durability_publication()?;
        publication.validate_authority(authority_instance_id)?;
        Ok(ControlPlaneRaftPeerServerDurability {
            authority_instance_id,
            publication,
            checkpoint,
        })
    }

    #[must_use]
    fn with_durable_artifact_path(mut self, artifact_path: &Path) -> Self {
        self.durable_artifact_path = Some(Arc::new(artifact_path.to_path_buf()));
        self
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn durable_state_machine_snapshot_for_test(
        &self,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.raft
            .with_state_machine(|state_machine| {
                let snapshot = state_machine.inner().snapshot().clone();
                Box::pin(async move { snapshot })
            })
            .await
            .map_err(|error| {
                ControlPlaneError::rpc_remote(format!(
                    "read test control-plane Raft state machine: {error}"
                ))
            })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn force_step_down_for_test(
        &self,
        peer_node_id: ControlPlaneRaftNodeId,
    ) -> Result<bool, ControlPlaneError> {
        let status = self.status().await?;
        let term = status
            .current_term()
            .ok_or_else(|| {
                ControlPlaneError::rpc_remote(
                    "test control-plane Raft authority has no current term".to_owned(),
                )
            })?
            .checked_add(1)
            .ok_or_else(|| {
                ControlPlaneError::rpc_remote("test control-plane Raft term overflow".to_owned())
            })?;
        self.raft
            .vote(VoteRequest {
                vote: Vote::new(term, peer_node_id),
                last_log_id: status.last_log_id(),
                leadership_transfer: true,
            })
            .await
            .map(|response| response.vote_granted)
            .map_err(|error| {
                ControlPlaneError::rpc_remote(format!(
                    "force test control-plane Raft step-down: {error}"
                ))
            })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn trigger_local_election_for_test(&self) -> Result<(), ControlPlaneError> {
        self.raft.trigger().elect(false).await.map_err(|error| {
            ControlPlaneError::rpc_remote(format!(
                "trigger test control-plane Raft election: {error}"
            ))
        })
    }

    #[must_use]
    pub(crate) fn durability_metric_snapshots(&self) -> ControlPlaneRaftDurabilityMetricSnapshots {
        ControlPlaneRaftDurabilityMetricSnapshots {
            checkpoint: self.checkpoint_metrics.snapshot(),
            wal: self
                .log_store
                .as_ref()
                .and_then(ControlPlaneRaftLogStore::wal_metric_snapshot),
            command: self.command_metrics.snapshot(),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn durability_metric_snapshots_for_test(
        &self,
    ) -> ControlPlaneRaftDurabilityMetricSnapshots {
        self.durability_metric_snapshots()
    }

    pub(crate) fn durable_wal_monitor_snapshot(
        &self,
    ) -> Result<ControlPlaneRaftWalMonitorSnapshot, ControlPlaneError> {
        let log_store = self.log_store.as_ref().ok_or_else(|| {
            ControlPlaneError::rpc_remote(
                "OpenRaft WAL monitor requires a retained log store".to_string(),
            )
        })?;
        log_store
            .wal_monitor_snapshot()
            .map_err(|source| {
                ControlPlaneError::io("read control-plane OpenRaft WAL monitor snapshot", source)
            })?
            .ok_or_else(|| {
                ControlPlaneError::rpc_remote(
                    "OpenRaft WAL monitor requires a WAL-backed log store".to_string(),
                )
            })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn durable_wal_monitor_snapshot_for_test(
        &self,
    ) -> Result<ControlPlaneRaftWalMonitorSnapshot, ControlPlaneError> {
        self.durable_wal_monitor_snapshot()
    }

    pub(crate) async fn initialize_membership(
        &self,
        nodes: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .initialize(nodes)
            .await
            .map_err(|error| openraft_remote_error("initialize", error))?;
        Ok(())
    }

    pub(crate) async fn initialize_single_node_membership(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        let mut nodes = BTreeMap::new();
        nodes.insert(node_id, BasicNode::default());
        self.initialize_membership(nodes).await
    }

    /// Initialize the membership retained by this authority, when this node
    /// owns initial membership submission.
    pub(crate) async fn initialize_configured_membership_if_needed(
        &self,
    ) -> Result<bool, ControlPlaneError> {
        if self.is_initialized().await? {
            return Ok(false);
        }
        #[cfg(test)]
        let membership_initialization_after_check_gate = self
            .membership_initialization_after_check_gate
            .lock()
            .expect("membership initialization test gate should not be poisoned")
            .take();
        #[cfg(test)]
        if let Some(gate) = membership_initialization_after_check_gate {
            gate.wait().await;
            gate.wait().await;
        }
        let Some(policy) = &self.static_peer_policy else {
            self.initialize_single_node_membership(self.node_id).await?;
            return Ok(true);
        };
        let peers = policy.peers();
        if peers.len() > 1 && peers.keys().next().copied() != Some(self.node_id) {
            return Ok(false);
        }
        classify_configured_membership_initialization(self.raft.initialize(peers).await)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn initialize_configured_membership_if_needed_for_test(
        &self,
    ) -> Result<bool, ControlPlaneError> {
        self.initialize_configured_membership_if_needed().await
    }

    #[cfg(test)]
    fn set_membership_initialization_after_check_gate_for_test(
        &self,
        gate: Arc<tokio::sync::Barrier>,
    ) {
        let previous = self
            .membership_initialization_after_check_gate
            .lock()
            .expect("membership initialization test gate should not be poisoned")
            .replace(gate);
        assert!(
            previous.is_none(),
            "membership initialization test gate already set"
        );
    }

    #[cfg(test)]
    fn set_linearized_read_after_snapshot_gate_for_test(&self, gate: Arc<tokio::sync::Barrier>) {
        let previous = self
            .linearized_read_after_snapshot_gate
            .lock()
            .expect("linearized read test gate should not be poisoned")
            .replace(gate);
        assert!(previous.is_none(), "linearized read test gate already set");
    }

    #[cfg(test)]
    async fn run_linearized_read_after_snapshot_gate_for_test(&self) {
        let gate = self
            .linearized_read_after_snapshot_gate
            .lock()
            .expect("linearized read test gate should not be poisoned")
            .take();
        if let Some(gate) = gate {
            gate.wait().await;
        }
    }

    #[cfg(test)]
    fn set_linearized_read_after_raft_state_capture_gate_for_test(
        &self,
        gate: Arc<tokio::sync::Barrier>,
    ) {
        let previous = self
            .linearized_read_after_raft_state_capture_gate
            .lock()
            .expect("linearized Raft-state capture test gate should not be poisoned")
            .replace(gate);
        assert!(
            previous.is_none(),
            "linearized Raft-state capture test gate already set"
        );
    }

    #[cfg(test)]
    async fn run_linearized_read_after_raft_state_capture_gate_for_test(&self) {
        let gate = self
            .linearized_read_after_raft_state_capture_gate
            .lock()
            .expect("linearized Raft-state capture test gate should not be poisoned")
            .take();
        if let Some(gate) = gate {
            gate.wait().await;
            gate.wait().await;
        }
    }

    #[cfg(test)]
    fn set_linearized_read_after_generation_capture_gate_for_test(
        &self,
        gate: Arc<tokio::sync::Barrier>,
    ) {
        let previous = self
            .linearized_read_after_generation_capture_gate
            .lock()
            .expect("linearized generation capture test gate should not be poisoned")
            .replace(gate);
        assert!(
            previous.is_none(),
            "linearized generation capture test gate already set"
        );
    }

    #[cfg(test)]
    async fn run_linearized_read_after_generation_capture_gate_for_test(&self) {
        let gate = self
            .linearized_read_after_generation_capture_gate
            .lock()
            .expect("linearized generation capture test gate should not be poisoned")
            .take();
        if let Some(gate) = gate {
            gate.wait().await;
            gate.wait().await;
        }
    }

    #[cfg(test)]
    fn linearized_runtime_map_read_index_count_for_test(&self) -> usize {
        self.linearized_runtime_map_read_index_count
            .load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn set_linearized_snapshot_retirement_hook_for_test(
        &self,
        hook: Arc<ControlPlaneRaftStateMachineBlockingHook>,
    ) {
        let previous = self
            .linearized_snapshot_retirement_hook
            .lock()
            .expect("linearized snapshot retirement hook should not be poisoned")
            .replace(hook);
        assert!(
            previous.is_none(),
            "linearized snapshot retirement hook already set"
        );
    }

    #[cfg(test)]
    fn take_linearized_snapshot_retirement_hook_for_test(
        &self,
    ) -> Option<Arc<ControlPlaneRaftStateMachineBlockingHook>> {
        self.linearized_snapshot_retirement_hook
            .lock()
            .expect("linearized snapshot retirement hook should not be poisoned")
            .take()
    }

    #[cfg(test)]
    fn set_linearized_state_machine_response_ready_notify_for_test(
        &self,
        notify: Arc<tokio::sync::Notify>,
    ) {
        let previous = self
            .linearized_state_machine_response_ready_notify
            .lock()
            .expect("linearized state-machine response notification should not be poisoned")
            .replace(notify);
        assert!(
            previous.is_none(),
            "linearized state-machine response notification already set"
        );
    }

    #[cfg(test)]
    fn take_linearized_state_machine_response_ready_notify_for_test(
        &self,
    ) -> Option<Arc<tokio::sync::Notify>> {
        self.linearized_state_machine_response_ready_notify
            .lock()
            .expect("linearized state-machine response notification should not be poisoned")
            .take()
    }

    #[cfg(test)]
    pub(crate) fn set_before_heartbeat_update_gate_hook_for_test(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        let previous = self
            .before_heartbeat_update_gate_hook
            .lock()
            .expect("heartbeat update-gate test hook should not be poisoned")
            .replace(hook);
        assert!(
            previous.is_none(),
            "heartbeat update-gate test hook already set"
        );
    }

    #[cfg(test)]
    fn run_before_heartbeat_update_gate_hook_for_test(&self) {
        let hook = self
            .before_heartbeat_update_gate_hook
            .lock()
            .expect("heartbeat update-gate test hook should not be poisoned")
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn set_low_priority_before_update_gate_for_test(
        &self,
        gate: Arc<ControlPlaneRaftLowPriorityTestGate>,
    ) {
        let previous = self
            .low_priority_before_update_gate
            .lock()
            .expect("low-priority pre-update-gate test hook should not be poisoned")
            .replace(gate);
        assert!(
            previous.is_none(),
            "low-priority pre-update-gate test hook already set"
        );
    }

    #[cfg(test)]
    async fn block_low_priority_before_update_gate_for_test(&self) {
        let gate = self
            .low_priority_before_update_gate
            .lock()
            .expect("low-priority pre-update-gate test hook should not be poisoned")
            .take();
        if let Some(gate) = gate {
            gate.block().await;
        }
    }

    #[cfg(test)]
    fn set_low_priority_after_update_gate_for_test(
        &self,
        gate: Arc<ControlPlaneRaftLowPriorityTestGate>,
    ) {
        let previous = self
            .low_priority_after_update_gate
            .lock()
            .expect("low-priority update-gate test hook should not be poisoned")
            .replace(gate);
        assert!(previous.is_none(), "low-priority test hook already set");
    }

    #[cfg(test)]
    async fn block_low_priority_after_update_gate_for_test(&self) {
        let gate = self
            .low_priority_after_update_gate
            .lock()
            .expect("low-priority update-gate test hook should not be poisoned")
            .take();
        if let Some(gate) = gate {
            gate.block().await;
        }
    }

    #[cfg(test)]
    fn set_ordinary_durable_after_update_gate_for_test(
        &self,
        gate: Arc<ControlPlaneRaftLowPriorityTestGate>,
    ) {
        let previous = self
            .ordinary_durable_after_update_gate
            .lock()
            .expect("ordinary durable update-gate test hook should not be poisoned")
            .replace(gate);
        assert!(
            previous.is_none(),
            "ordinary durable update-gate test hook already set"
        );
    }

    #[cfg(test)]
    async fn block_ordinary_durable_after_update_gate_for_test(&self) {
        let gate = self
            .ordinary_durable_after_update_gate
            .lock()
            .expect("ordinary durable update-gate test hook should not be poisoned")
            .take();
        if let Some(gate) = gate {
            gate.block().await;
        }
    }

    #[cfg(test)]
    fn set_low_priority_before_dispatch_for_test(
        &self,
        gate: Arc<ControlPlaneRaftLowPriorityTestGate>,
    ) {
        let previous = self
            .low_priority_before_dispatch
            .lock()
            .expect("low-priority pre-dispatch test hook should not be poisoned")
            .replace(gate);
        assert!(
            previous.is_none(),
            "low-priority pre-dispatch test hook already set"
        );
    }

    #[cfg(test)]
    async fn block_low_priority_before_dispatch_for_test(&self) {
        let gate = self
            .low_priority_before_dispatch
            .lock()
            .expect("low-priority pre-dispatch test hook should not be poisoned")
            .take();
        if let Some(gate) = gate {
            gate.block().await;
        }
    }

    #[cfg(test)]
    fn set_low_priority_after_dispatch_for_test(
        &self,
        gate: Arc<ControlPlaneRaftLowPriorityTestGate>,
    ) {
        let previous = self
            .low_priority_after_dispatch
            .lock()
            .expect("low-priority dispatch test hook should not be poisoned")
            .replace(gate);
        assert!(
            previous.is_none(),
            "low-priority dispatch test hook already set"
        );
    }

    #[cfg(test)]
    async fn block_low_priority_after_dispatch_for_test(&self) {
        let gate = self
            .low_priority_after_dispatch
            .lock()
            .expect("low-priority dispatch test hook should not be poisoned")
            .take();
        if let Some(gate) = gate {
            gate.block().await;
        }
    }

    #[cfg(test)]
    fn set_low_priority_after_proven_unappended_retry_for_test(
        &self,
        gate: Arc<ControlPlaneRaftLowPriorityTestGate>,
    ) {
        let previous = self
            .low_priority_after_proven_unappended_retry
            .lock()
            .expect("low-priority retry test hook should not be poisoned")
            .replace(gate);
        assert!(
            previous.is_none(),
            "low-priority retry test hook already set"
        );
    }

    #[cfg(test)]
    fn set_low_priority_during_retry_certification_for_test(
        &self,
        gate: Arc<ControlPlaneRaftLowPriorityTestGate>,
    ) {
        let previous = self
            .low_priority_during_retry_certification
            .lock()
            .expect("low-priority retry certification test hook should not be poisoned")
            .replace(gate);
        assert!(
            previous.is_none(),
            "low-priority retry certification test hook already set"
        );
    }

    #[cfg(test)]
    async fn block_low_priority_during_retry_certification_for_test(&self) {
        let gate = self
            .low_priority_during_retry_certification
            .lock()
            .expect("low-priority retry certification test hook should not be poisoned")
            .take();
        if let Some(gate) = gate {
            gate.block().await;
        }
    }

    #[cfg(test)]
    async fn block_low_priority_after_proven_unappended_retry_for_test(&self) {
        let gate = self
            .low_priority_after_proven_unappended_retry
            .lock()
            .expect("low-priority retry test hook should not be poisoned")
            .take();
        if let Some(gate) = gate {
            gate.block().await;
        }
    }

    fn retained_snapshot_generation(
        &self,
        snapshot: Arc<ClusterControlSnapshot>,
    ) -> ControlPlaneRaftSnapshotGeneration {
        ControlPlaneRaftSnapshotGeneration::new(
            snapshot,
            #[cfg(test)]
            self.take_linearized_snapshot_retirement_hook_for_test(),
        )
    }

    async fn retained_state_machine_snapshot_generation(
        &self,
    ) -> Result<
        (
            ControlPlaneRaftSnapshotGeneration,
            Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
            Option<ControlPlaneLogId>,
        ),
        ControlPlaneError,
    > {
        #[cfg(test)]
        let retirement_hook = self.take_linearized_snapshot_retirement_hook_for_test();
        #[cfg(test)]
        let response_ready_notify =
            self.take_linearized_state_machine_response_ready_notify_for_test();
        self.raft
            .with_state_machine(move |state_machine| {
                let snapshot = ControlPlaneRaftSnapshotGeneration::new(
                    state_machine.inner().snapshot_generation(),
                    #[cfg(test)]
                    retirement_hook,
                );
                let applied = state_machine.last_applied();
                let control_plane_applied = state_machine.inner().last_applied();
                Box::pin(async move {
                    #[cfg(test)]
                    if let Some(notify) = response_ready_notify {
                        notify.notify_one();
                    }
                    (snapshot, applied, control_plane_applied)
                })
            })
            .await
            .map_err(|error| openraft_remote_error("runtime-map state-machine read", error))
    }

    pub async fn is_initialized(&self) -> Result<bool, ControlPlaneError> {
        self.raft
            .is_initialized()
            .await
            .map_err(|error| openraft_remote_error("is-initialized", error))
    }

    pub async fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        self.reject_static_peer_reconfiguration("change-membership")?;
        let _submission_guard = self
            .evidence_submission_admission
            .acquire_ordinary_durable_update_gate(&self.volatile_heartbeat_update_gate)
            .await;
        let overlay_rebase = self.current_volatile_heartbeat_overlay().await?;
        let retry_deadline = self.writable_proposal_retry_deadline();
        let response = loop {
            let attempt = self.prepare_writable_proposal(retry_deadline).await?;
            self.ensure_writable_proposal_time_remaining(
                retry_deadline,
                "change-membership dispatch",
            )?;
            match self
                .raft
                .change_membership(voters.clone(), retain_removed_voters_as_learners)
                .await
            {
                Ok(response) => break response,
                Err(error) => {
                    if self
                        .writable_proposal_may_retry(&attempt, &error, retry_deadline)
                        .await?
                    {
                        continue;
                    }
                    return Err(openraft_client_write_error("change-membership", error));
                }
            }
        };
        if let Some((authority_term, snapshot)) = overlay_rebase {
            self.publish_rebased_volatile_heartbeat_overlay(
                authority_term,
                response.log_id,
                snapshot,
            )
            .await?;
        }
        Ok(response.log_id)
    }

    pub async fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        self.reject_static_peer_reconfiguration("add-learner")?;
        let _submission_guard = self
            .evidence_submission_admission
            .acquire_ordinary_durable_update_gate(&self.volatile_heartbeat_update_gate)
            .await;
        let overlay_rebase = self.current_volatile_heartbeat_overlay().await?;
        let retry_deadline = self.writable_proposal_retry_deadline();
        let response = loop {
            let attempt = self.prepare_writable_proposal(retry_deadline).await?;
            self.ensure_writable_proposal_time_remaining(retry_deadline, "add-learner dispatch")?;
            match self
                .raft
                .add_learner(node_id, node.clone(), wait_for_catch_up)
                .await
            {
                Ok(response) => break response,
                Err(error) => {
                    if self
                        .writable_proposal_may_retry(&attempt, &error, retry_deadline)
                        .await?
                    {
                        continue;
                    }
                    return Err(openraft_client_write_error("add-learner", error));
                }
            }
        };
        if let Some((authority_term, snapshot)) = overlay_rebase {
            self.publish_rebased_volatile_heartbeat_overlay(
                authority_term,
                response.log_id,
                snapshot,
            )
            .await?;
        }
        Ok(response.log_id)
    }

    fn reject_static_peer_reconfiguration(
        &self,
        operation: &'static str,
    ) -> Result<(), ControlPlaneError> {
        if let Some(peer_policy) = &self.static_peer_policy {
            return Err(ControlPlaneError::rpc_remote(format!(
                    "OpenRaft {operation} is not supported for static configured peer policy in cluster {:?}; dynamic control-plane membership reconfiguration is outside Phase 12.3",
                    peer_policy.cluster_name()
                )));
        }
        Ok(())
    }

    pub async fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .trigger()
            .transfer_leader(node_id)
            .await
            .map_err(|error| openraft_remote_error("transfer-leader", error))
    }

    pub async fn trigger_pre_vote_election_until_serving(
        &self,
        timeout: Duration,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .trigger()
            .elect(true)
            .await
            .map_err(|error| openraft_remote_error("trigger-election", error))?;
        ControlPlaneRaftTypeConfig::timeout(timeout, async {
            loop {
                let status = self.status().await?;
                if status.linearized_authority_serving() {
                    return Ok(());
                }
                ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            ControlPlaneError::rpc_remote(format!(
                "OpenRaft election trigger did not make node {} serving before timeout",
                self.node_id
            ))
        })?
    }

    pub(crate) async fn trigger_snapshot_applied(
        &self,
    ) -> Result<Option<LogIdOf<ControlPlaneRaftTypeConfig>>, ControlPlaneError> {
        let status = self.status().await?;
        if !status.linearized_authority_serving() {
            return Err(ControlPlaneError::rpc_remote(format!(
                "OpenRaft snapshot purge requires the current serving authority; node {} is {:?}",
                status.node_id(),
                status.linearized_authority_readiness()
            )));
        }
        self.trigger_local_snapshot_applied().await
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn trigger_snapshot_applied_for_test(
        &self,
    ) -> Result<Option<LogIdOf<ControlPlaneRaftTypeConfig>>, ControlPlaneError> {
        self.trigger_snapshot_applied().await
    }

    pub(crate) async fn trigger_local_snapshot_applied(
        &self,
    ) -> Result<Option<LogIdOf<ControlPlaneRaftTypeConfig>>, ControlPlaneError> {
        let Some(applied) = self
            .raft
            .with_state_machine(|state_machine| {
                let applied = state_machine.last_applied();
                Box::pin(async move { applied })
            })
            .await
            .map_err(|error| openraft_remote_error("snapshot-purge applied read", error))?
        else {
            return Ok(None);
        };
        let mut snapshot_progress = self.raft.watch_snapshot_progress();
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|error| openraft_remote_error("trigger snapshot", error))?;
        snapshot_progress
            .wait_until_ge(&Some(applied))
            .await
            .map_err(|error| openraft_remote_error("wait snapshot progress", error))?;
        let snapshot = self
            .raft
            .get_snapshot()
            .await
            .map_err(|error| openraft_remote_error("get snapshot after trigger", error))?
            .ok_or_else(|| {
                ControlPlaneError::rpc_remote(
                    "OpenRaft snapshot trigger completed without a current snapshot".to_string(),
                )
            })?;
        let snapshot_log_id = snapshot.meta.last_log_id.ok_or_else(|| {
            ControlPlaneError::rpc_remote(
                "OpenRaft snapshot trigger produced an empty snapshot".to_string(),
            )
        })?;
        Ok(Some(snapshot_log_id))
    }

    pub(crate) async fn purge_log_through_snapshot(
        &self,
        snapshot_log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), ControlPlaneError> {
        let snapshot = self
            .raft
            .get_snapshot()
            .await
            .map_err(|error| openraft_remote_error("get snapshot before purge", error))?
            .ok_or_else(|| {
                ControlPlaneError::rpc_remote(
                    "OpenRaft snapshot purge requires a current snapshot".to_string(),
                )
            })?;
        if snapshot.meta.last_log_id != Some(snapshot_log_id) {
            return Err(ControlPlaneError::rpc_remote(format!(
                    "OpenRaft snapshot purge log id {snapshot_log_id} does not match current snapshot {:?}",
                    snapshot.meta.last_log_id
                )));
        }
        self.raft
            .trigger()
            .purge_log(snapshot_log_id.index())
            .await
            .map_err(|error| openraft_remote_error("trigger snapshot log purge", error))?;
        if let Some(log_store) = &self.log_store {
            ControlPlaneRaftTypeConfig::timeout(Duration::from_secs(10), async {
                loop {
                    match log_store.status_snapshot() {
                        Ok(status) => {
                            if control_plane_raft_durable_purge_covers(
                                &status,
                                snapshot_log_id,
                            )? {
                                return Ok(());
                            }
                            ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => {
                            return Err(openraft_remote_error(
                                "snapshot purge durable log-store read",
                                error,
                            ));
                        }
                    }
                }
            })
            .await
            .map_err(|_| ControlPlaneError::rpc_remote(format!(
                    "OpenRaft snapshot purge did not durably reach {snapshot_log_id:?} before timeout"
                )))??;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn purge_log_through_snapshot_for_test(
        &self,
        snapshot_log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), ControlPlaneError> {
        self.purge_log_through_snapshot(snapshot_log_id).await
    }

    pub async fn wait_for_applied_index_at_least(
        &self,
        index: u64,
        timeout: Duration,
        message: &'static str,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .wait(Some(timeout))
            .applied_index_at_least(Some(index), message)
            .await
            .map(|_| ())
            .map_err(|error| openraft_remote_error("wait-applied-index", error))
    }

    pub async fn wait_for_applied_log_id(
        &self,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        timeout: Duration,
        message: &'static str,
    ) -> Result<(), ControlPlaneError> {
        ControlPlaneRaftTypeConfig::timeout(timeout, async {
            loop {
                let applied = self
                    .raft
                    .with_state_machine(|state_machine| {
                        let applied = state_machine.last_applied();
                        Box::pin(async move { applied })
                    })
                    .await
                    .map_err(|error| openraft_remote_error("wait-applied-log-id", error))?;
                if let Some(applied) = applied {
                    if applied.index() > log_id.index() {
                        return Ok(());
                    }
                    if applied.index() == log_id.index() {
                        if applied == log_id {
                            return Ok(());
                        }
                        return Err(ControlPlaneError::rpc_remote(format!(
                            "OpenRaft wait-applied-log-id observed mismatched log id: \
                                 applied={applied:?}, expected={log_id:?}: {message}"
                        )));
                    }
                }
                ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            ControlPlaneError::rpc_remote(format!(
                "OpenRaft wait-applied-log-id timed out after {timeout:?}: {message}"
            ))
        })?
    }

    pub(crate) async fn wait_for_current_leader(
        &self,
        leader_id: ControlPlaneRaftNodeId,
        timeout: Duration,
        message: &'static str,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .wait(Some(timeout))
            .current_leader(leader_id, message)
            .await
            .map(|_| ())
            .map_err(|error| openraft_remote_error("wait-current-leader", error))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn wait_for_current_leader_for_test(
        &self,
        leader_id: ControlPlaneRaftNodeId,
        timeout: Duration,
        message: &'static str,
    ) -> Result<(), ControlPlaneError> {
        self.wait_for_current_leader(leader_id, timeout, message)
            .await
    }

    /// Read only the Raft and state-machine fields needed to validate process
    /// clock authority. This avoids putting recovery behind the full
    /// diagnostic status scan or a cloned control-plane snapshot.
    pub async fn authority_clock_context(
        &self,
    ) -> Result<ControlPlaneAuthorityClockContext, ControlPlaneError> {
        let node_id = *self.raft.node_id();
        let current_leader = self.raft.current_leader().await;
        let current_term = self
            .log_store
            .as_ref()
            .map(ControlPlaneRaftLogStore::status_snapshot)
            .transpose()
            .map_err(|error| openraft_remote_error("clock-context log-store read", error))?
            .and_then(|status| status.durable_vote)
            .map(|vote| vote.leader_id.term);
        let (committed, effective_voter) = self
            .raft
            .with_raft_state(move |state| {
                (
                    state.local_committed().cloned(),
                    state
                        .membership_state
                        .effective()
                        .membership()
                        .voter_ids()
                        .any(|voter| voter == node_id),
                )
            })
            .await
            .map_err(|error| openraft_remote_error("clock-context raft-state read", error))?;
        let (applied, committed_timestamp_high_water_ms) = self
            .raft
            .with_state_machine(|state_machine| {
                let applied = state_machine.last_applied();
                let committed_timestamp_high_water_ms = state_machine
                    .inner()
                    .snapshot()
                    .max_committed_timestamp_ms();
                Box::pin(async move { (applied, committed_timestamp_high_water_ms) })
            })
            .await
            .map_err(|error| openraft_remote_error("clock-context state-machine read", error))?;
        let local_leader = current_leader == Some(node_id);
        let committed_in_current_term = matches!(
            (current_term, committed),
            (Some(current_term), Some(committed))
                if committed.committed_leader_id().term == current_term
        );
        let local_serving = local_leader
            && effective_voter
            && committed.is_some()
            && committed == applied
            && committed_in_current_term;
        Ok(ControlPlaneAuthorityClockContext::new(
            committed_timestamp_high_water_ms,
            current_term,
            local_leader,
            local_serving,
        ))
    }

    pub async fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError> {
        self.submit_control_plane_command_derived(move |_| Ok(command))
            .await
    }

    pub(crate) async fn submit_low_priority_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError> {
        let queue_started = Instant::now();
        #[cfg(test)]
        self.block_low_priority_before_update_gate_for_test().await;
        let mut update_guard = self
            .evidence_submission_admission
            .acquire_evidence_update_gate(&self.volatile_heartbeat_update_gate)
            .await;
        #[cfg(test)]
        self.block_low_priority_after_update_gate_for_test().await;
        let queue_wait = queue_started.elapsed();
        let operation_started = Instant::now();
        let result = self
            .submit_control_plane_command_derived_locked(None, Some(&mut update_guard), move |_| {
                Ok(command)
            })
            .await;
        let operation = operation_started.elapsed();
        observability::record_control_plane_raft_command_submission(
            queue_wait,
            operation,
            result.is_ok(),
        );
        self.command_metrics
            .record_submission(queue_wait, operation, result.is_ok());
        result
    }

    pub(crate) async fn submit_low_priority_control_plane_command_derived<F>(
        &self,
        derive_command: F,
    ) -> Result<Option<SubmittedControlPlaneRaftCommand>, ControlPlaneError>
    where
        F: FnOnce(
            &ClusterControlSnapshot,
        ) -> Result<Option<ControlPlaneCommand>, ControlPlaneError>,
    {
        let queue_started = Instant::now();
        #[cfg(test)]
        self.block_low_priority_before_update_gate_for_test().await;
        let mut update_guard = self
            .evidence_submission_admission
            .acquire_evidence_update_gate(&self.volatile_heartbeat_update_gate)
            .await;
        #[cfg(test)]
        self.block_low_priority_after_update_gate_for_test().await;
        let snapshot = self
            .confirmed_low_priority_preflight_snapshot(&update_guard)
            .await?;
        let Some(command) = derive_command(&snapshot)? else {
            return Ok(None);
        };
        let queue_wait = queue_started.elapsed();
        let operation_started = Instant::now();
        let result = self
            .submit_control_plane_command_derived_locked(None, Some(&mut update_guard), move |_| {
                Ok(command)
            })
            .await;
        let operation = operation_started.elapsed();
        observability::record_control_plane_raft_command_submission(
            queue_wait,
            operation,
            result.is_ok(),
        );
        self.command_metrics
            .record_submission(queue_wait, operation, result.is_ok());
        result.map(Some)
    }

    pub(crate) async fn submit_low_priority_metadata_transfer_staging_evidence_page(
        &self,
        operation_payload: Vec<u8>,
        page_digest: [u8; 32],
    ) -> Result<LowPriorityControlPlaneRaftCommandResult, ControlPlaneError> {
        let queue_started = Instant::now();
        let mut update_guard = self
            .evidence_submission_admission
            .acquire_evidence_update_gate(&self.volatile_heartbeat_update_gate)
            .await;
        #[cfg(test)]
        self.block_low_priority_after_update_gate_for_test().await;

        let preflight_snapshot = self
            .confirmed_low_priority_preflight_snapshot(&update_guard)
            .await?;
        let classification = preflight_snapshot
            .classify_metadata_transfer_staging_evidence_page(&operation_payload, page_digest)?;
        if let MetadataTransferStagingEvidencePageClassification::ExactReplay { apply_receipt } =
            classification
        {
            return Ok(LowPriorityControlPlaneRaftCommandResult::PreflightResolved(
                ControlPlaneCommandResponse::ApplyMetadataTransferStagingEvidencePage {
                    apply_receipt,
                },
            ));
        }

        let queue_wait = queue_started.elapsed();
        let operation_started = Instant::now();
        let result = self
            .submit_control_plane_command_derived_locked(None, Some(&mut update_guard), move |_| {
                Ok(
                    ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                        operation_payload,
                        page_digest,
                    },
                )
            })
            .await;
        let operation = operation_started.elapsed();
        observability::record_control_plane_raft_command_submission(
            queue_wait,
            operation,
            result.is_ok(),
        );
        self.command_metrics
            .record_submission(queue_wait, operation, result.is_ok());
        result.map(LowPriorityControlPlaneRaftCommandResult::Submitted)
    }

    async fn confirmed_low_priority_preflight_snapshot(
        &self,
        update_guard: &ControlPlaneRaftEvidenceUpdateGuard<'_>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let admission = update_guard.admission;
        let status = admission
            .run_evidence_preappend(self.confirmed_linearized_authority_status())
            .await?;
        let authority_term = status.current_term();
        let (durable_snapshot, durable_applied) = admission
            .run_evidence_preappend(self.durable_snapshot_and_applied())
            .await?;
        match (authority_term, status.applied(), durable_applied) {
            (Some(authority_term), Some(status_applied), Some(durable_applied))
                if status_applied == durable_applied =>
            {
                Ok(self
                    .volatile_heartbeat_overlay_snapshot(authority_term, durable_applied)?
                    .unwrap_or(durable_snapshot))
            }
            _ => Ok(durable_snapshot),
        }
    }

    pub(crate) async fn submit_control_plane_command_derived<F>(
        &self,
        derive_command: F,
    ) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>
    where
        F: FnOnce(&ClusterControlSnapshot) -> Result<ControlPlaneCommand, ControlPlaneError>,
    {
        let queue_started = Instant::now();
        #[cfg(test)]
        self.run_before_heartbeat_update_gate_hook_for_test();
        let submission_guard = self
            .evidence_submission_admission
            .acquire_ordinary_durable_update_gate(&self.volatile_heartbeat_update_gate)
            .await;
        #[cfg(test)]
        self.block_ordinary_durable_after_update_gate_for_test()
            .await;
        let queue_wait = queue_started.elapsed();
        let operation_started = Instant::now();
        let result = self
            .submit_control_plane_command_derived_locked(
                Some(&submission_guard.update),
                None,
                derive_command,
            )
            .await;
        let operation = operation_started.elapsed();
        observability::record_control_plane_raft_command_submission(
            queue_wait,
            operation,
            result.is_ok(),
        );
        self.command_metrics
            .record_submission(queue_wait, operation, result.is_ok());
        result
    }

    /// Submit the certified bootstrap command retained by a storage-owned
    /// static topology.
    pub(crate) async fn submit_static_initial_topology(
        &self,
        topology: &StaticInitialControlPlaneTopology,
    ) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError> {
        self.validate_static_initial_topology_binding(topology)?;
        self.submit_control_plane_command(topology.bootstrap_command())
            .await
    }

    /// Observe and, when necessary, submit the environment-configured
    /// uncertified initial topology.
    ///
    /// The returned submitted command remains internal to the combined durable
    /// establishment operation. `None` means the topology was either not
    /// configured or control-plane state was already initialized.
    pub(crate) async fn prepare_uncertified_initial_control_plane_topology(
        &self,
        topology: &UncertifiedInitialControlPlaneTopology,
    ) -> Result<Option<UncertifiedInitialControlPlaneTopologySubmission>, ControlPlaneError> {
        let snapshot = self.current_control_plane_snapshot().await?;
        if topology.initialized_epoch(&snapshot).is_some() {
            return Ok(None);
        }
        let Some(command) = topology.bootstrap_command() else {
            return Ok(None);
        };
        let authority_instance_id = self.authority_instance_id()?;
        self.submit_control_plane_command(command)
            .await
            .map(|submitted| {
                Some(UncertifiedInitialControlPlaneTopologySubmission {
                    authority_instance_id,
                    topology: topology.clone(),
                    submitted,
                })
            })
    }

    /// Resolve a published uncertified-topology submission without exposing
    /// empty-state or concurrent-bootstrap classification to the process
    /// layer.
    ///
    /// A bootstrap rejection or leader-routing race is accepted only after a
    /// fresh authority read proves that some initial topology now exists.
    pub(crate) async fn resolve_uncertified_initial_control_plane_topology_submission(
        &self,
        submission: UncertifiedInitialControlPlaneTopologySubmission,
    ) -> Result<Option<u64>, ControlPlaneError> {
        let UncertifiedInitialControlPlaneTopologySubmission {
            authority_instance_id,
            topology,
            submitted,
        } = submission;
        if authority_instance_id != self.authority_instance_id()? {
            return Err(ControlPlaneError::rpc_remote(
                "uncertified initial-topology submission belongs to another authority instance"
                    .to_owned(),
            ));
        }
        let result = match submitted.into_outcome() {
            ControlPlaneRaftCommandOutcome::Applied(response) => Ok(response),
            ControlPlaneRaftCommandOutcome::Rejected(error) => Err(error),
        };
        match result {
            Ok(ControlPlaneCommandResponse::BootstrapInitialClusterMap) => {
                let snapshot = self.current_control_plane_snapshot().await?;
                topology.initialized_epoch(&snapshot).map(Some).ok_or_else(|| {
                    ControlPlaneError::invariant_failure(
                        "successful uncertified initial-topology submission left control-plane state empty",
                    )
                })
            }
            Ok(_) => Err(ControlPlaneError::invariant_failure(
                "uncertified initial-topology submission returned the wrong response",
            )),
            Err(error)
                if matches!(error, ControlPlaneError::BootstrapRequiresEmptyState)
                    || error.is_control_plane_leader_routing_rejection() =>
            {
                let snapshot = self.current_control_plane_snapshot().await?;
                match topology.initialized_epoch(&snapshot) {
                    Some(epoch) => Ok(Some(epoch)),
                    None => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn publish_uncertified_initial_topology_checkpoint(
        &self,
        publication: &ControlPlaneRaftDurabilityPublication,
    ) -> Result<(), ControlPlaneError> {
        if self
            .uncertified_initial_topology_checkpoint_published
            .get()
            .is_some()
        {
            return Ok(());
        }
        if let Err(error) = self.store_durable_restart_artifact().await {
            publication.poison(format!(
                "control-plane Raft durability checkpoint failed while establishing the \
                 uncertified initial topology: {}",
                error.retained_diagnostic_message()
            ));
            return Err(error.into_durability_failure(
                "publish uncertified initial topology restart checkpoint",
            ));
        }
        Ok(())
    }

    /// Establish an environment-configured uncertified topology with durable
    /// response publication owned by this authority.
    ///
    /// The submitted outcome is not resolved until a restart checkpoint that
    /// contains the committed command has been durably published. Topology
    /// observed from another authority is likewise not reported until it is in
    /// this authority's own durable checkpoint. A checkpoint failure poisons
    /// the authority's one response-publication domain before returning,
    /// preventing any later RPC response from being published by this process.
    pub async fn establish_uncertified_initial_control_plane_topology(
        &self,
        topology: &UncertifiedInitialControlPlaneTopology,
    ) -> Result<Option<u64>, ControlPlaneError> {
        let publication = self.durability_publication()?;
        publication.validate_authority(self.authority_instance_id()?)?;
        loop {
            publication.ensure_available()?;
            let submission = match self
                .prepare_uncertified_initial_control_plane_topology(topology)
                .await
            {
                Ok(Some(submission)) => submission,
                Ok(None) => {
                    let snapshot = self.current_control_plane_snapshot().await?;
                    if topology.initialized_epoch(&snapshot).is_some() {
                        self.publish_uncertified_initial_topology_checkpoint(&publication)
                            .await?;
                        let _ = self
                            .uncertified_initial_topology_checkpoint_published
                            .set(());
                    }
                    return Ok(None);
                }
                Err(error) if error.is_control_plane_leader_routing_rejection() => {
                    ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            self.publish_uncertified_initial_topology_checkpoint(&publication)
                .await?;
            match self
                .resolve_uncertified_initial_control_plane_topology_submission(submission)
                .await
            {
                Ok(epoch) => {
                    // A rejected submission may have observed a concurrently applied
                    // topology only during resolution, after the pre-resolution
                    // checkpoint was captured. Publish once more so the exact state
                    // which justified success is locally restartable.
                    self.publish_uncertified_initial_topology_checkpoint(&publication)
                        .await?;
                    let _ = self
                        .uncertified_initial_topology_checkpoint_published
                        .set(());
                    return Ok(epoch);
                }
                Err(error) if error.is_control_plane_leader_routing_rejection() => {
                    ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Wait until the applied Raft membership exactly matches the static peer
    /// policy retained by this authority.
    ///
    /// The caller cannot supply a second policy or interpret raw Raft
    /// membership state. This keeps the membership certified for static
    /// identity publication bound to the authority that will publish it.
    pub async fn wait_for_static_initial_membership(&self) -> Result<(), ControlPlaneError> {
        let peer_policy = self.static_peer_policy.as_ref().ok_or_else(|| {
            ControlPlaneError::static_topology_failure(
                "static membership convergence requires a configured static peer policy",
            )
        })?;
        loop {
            let status = self.status().await?;
            if validate_static_initial_raft_membership(peer_policy, &status).is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Establish or validate the certified initial topology retained by this
    /// static authority.
    ///
    /// Membership convergence, topology validation, command submission, and
    /// retry classification remain storage-owned. `allow_bootstrap` is false
    /// after an outer static identity has already been durably published, so a
    /// missing certified topology then fails closed rather than being replaced.
    pub(crate) async fn establish_static_initial_topology(
        &self,
        topology: &StaticInitialControlPlaneTopology,
        allow_bootstrap: bool,
    ) -> Result<(), ControlPlaneError> {
        self.validate_static_initial_topology_binding(topology)?;
        self.wait_for_static_initial_membership().await?;
        loop {
            let snapshot = self.current_control_plane_snapshot().await?;
            if topology
                .validate_snapshot(&snapshot)
                .map_err(|error| ControlPlaneError::static_topology_failure(error.to_string()))?
            {
                return Ok(());
            }
            if !allow_bootstrap {
                return Err(ControlPlaneError::static_topology_failure(
                    "established static control-plane state is missing its certified initial topology",
                ));
            }
            if self.status().await?.linearized_authority_serving() {
                match self.submit_static_initial_topology(topology).await {
                    Ok(submitted) => match submitted.into_outcome() {
                        ControlPlaneRaftCommandOutcome::Applied(_) => {}
                        ControlPlaneRaftCommandOutcome::Rejected(
                            ControlPlaneError::BootstrapRequiresEmptyState,
                        ) => {}
                        ControlPlaneRaftCommandOutcome::Rejected(error) => return Err(error),
                    },
                    Err(error) if error.is_control_plane_leader_routing_rejection() => {}
                    Err(error) => return Err(error),
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn establish_static_initial_topology_for_test(
        &self,
        topology: &StaticInitialControlPlaneTopology,
        allow_bootstrap: bool,
    ) -> Result<(), ControlPlaneError> {
        self.establish_static_initial_topology(topology, allow_bootstrap)
            .await
    }

    fn validate_static_initial_topology_binding(
        &self,
        topology: &StaticInitialControlPlaneTopology,
    ) -> Result<(), ControlPlaneError> {
        let peer_policy = self.static_peer_policy.as_ref().ok_or_else(|| {
            ControlPlaneError::static_topology_failure(
                "static initial topology submission requires a configured static peer policy",
            )
        })?;
        let configured = peer_policy.initial_topology_certificate().ok_or_else(|| {
            ControlPlaneError::static_topology_failure(
                "static initial topology submission requires a configured topology certificate",
            )
        })?;
        if topology.certificate() != configured {
            return Err(ControlPlaneError::static_topology_failure(
                "static initial topology submission does not match the authority's configured topology",
            ));
        }
        Ok(())
    }

    /// Returns local serving status only after a ReadIndex round confirms that
    /// this process still holds authority from the live voter quorum.
    pub async fn confirmed_linearized_authority_status(
        &self,
    ) -> Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError> {
        let status = self.status().await?;
        if !status.linearized_authority_serving() {
            return Err(ControlPlaneError::AuthorityNotServing);
        }
        ControlPlaneRaftTypeConfig::timeout(
            Duration::from_secs(1),
            self.raft.ensure_linearizable(ReadPolicy::ReadIndex),
        )
        .await
        .map_err(|_| ControlPlaneError::OpenRaftOperation {
            kind: ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
            message: "command-authority read-index timed out after 1s".to_owned(),
        })?
        .map_err(|error| openraft_linearizable_read_error("command-authority read-index", error))?;
        Ok(status)
    }

    /// Confirm OpenRaft's proposal lease before handing a mutation to Raft.
    ///
    /// ReadIndex proves linearizable read authority but does not refresh the
    /// quorum acknowledgement used by OpenRaft to admit writes. Avoid an extra
    /// heartbeat while the lease has enough margin for dispatch; otherwise
    /// force a heartbeat and wait for an acknowledgement sent after this
    /// admission attempt began.
    async fn confirm_writable_proposal_lease(
        &self,
        effective_voters: &BTreeSet<ControlPlaneRaftNodeId>,
        retry_deadline: Instant,
    ) -> Result<(), ControlPlaneError> {
        if effective_voters.len() == 1 && effective_voters.contains(&self.node_id) {
            return Ok(());
        }

        let leader =
            self.raft
                .as_leader()
                .map_err(|error| ControlPlaneError::OpenRaftOperation {
                    kind: ControlPlaneRaftOperationErrorKind::ForwardToLeader,
                    message: format!(
                        "OpenRaft proposal-lease confirmation requires the local leader: {error}"
                    ),
                })?;
        let election_timeout_max = self.raft.config().election_timeout_max;
        let heartbeat_interval = self.raft.config().heartbeat_interval;
        let leader_lease = Duration::from_millis(election_timeout_max);
        let dispatch_margin = Duration::from_millis(heartbeat_interval.saturating_mul(2));
        let maximum_accepted_age = leader_lease.saturating_sub(dispatch_margin);
        if leader
            .last_quorum_acked()
            .is_some_and(|acked| acked.elapsed() <= maximum_accepted_age)
        {
            return Ok(());
        }

        let confirmation_started = ControlPlaneRaftTypeConfig::now();
        let heartbeat_enqueue_timeout = self
            .writable_proposal_time_remaining(retry_deadline, "proposal-lease heartbeat enqueue")?;
        ControlPlaneRaftTypeConfig::timeout(
            heartbeat_enqueue_timeout,
            self.raft.trigger().heartbeat(),
        )
        .await
        .map_err(|_| ControlPlaneError::OpenRaftOperation {
            kind: ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
            message: format!(
                "OpenRaft proposal-lease heartbeat enqueue timed out after {heartbeat_enqueue_timeout:?}"
            ),
        })?
        .map_err(|error| ControlPlaneError::OpenRaftOperation {
            kind: ControlPlaneRaftOperationErrorKind::Fatal,
            message: format!("OpenRaft proposal-lease heartbeat trigger failed: {error}"),
        })?;
        let remaining = self.writable_proposal_time_remaining(
            retry_deadline,
            "proposal-lease quorum acknowledgement",
        )?;
        let confirmation_timeout = Duration::from_millis(election_timeout_max).min(remaining);
        self.raft
            .wait(Some(confirmation_timeout))
            .leader_with_quorum_acked(
                Some(confirmation_started),
                "control-plane proposal lease confirmation",
            )
            .await
            .map(|_| ())
            .map_err(|error| {
                let kind = match error {
                    WaitError::Timeout(_, _) => {
                        ControlPlaneRaftOperationErrorKind::QuorumNotEnough
                    }
                    WaitError::ShuttingDown => ControlPlaneRaftOperationErrorKind::Fatal,
                };
                ControlPlaneError::OpenRaftOperation {
                    kind,
                    message: format!(
                        "OpenRaft proposal lease was not confirmed before {confirmation_timeout:?}: {error}"
                    ),
                }
            })
    }

    fn writable_proposal_retry_deadline(&self) -> Instant {
        let election_timeout = Duration::from_millis(self.raft.config().election_timeout_max);
        Instant::now() + Duration::from_secs(1).max(election_timeout.saturating_mul(2))
    }

    fn writable_proposal_time_remaining(
        &self,
        retry_deadline: Instant,
        operation: &'static str,
    ) -> Result<Duration, ControlPlaneError> {
        let remaining = retry_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ControlPlaneError::OpenRaftOperation {
                kind: ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
                message: format!("OpenRaft {operation} deadline expired"),
            });
        }
        Ok(remaining)
    }

    fn ensure_writable_proposal_time_remaining(
        &self,
        retry_deadline: Instant,
        operation: &'static str,
    ) -> Result<(), ControlPlaneError> {
        self.writable_proposal_time_remaining(retry_deadline, operation)
            .map(|_| ())
    }

    async fn writable_proposal_raft_state(
        &self,
        retry_deadline: Instant,
    ) -> Result<
        (
            BTreeSet<ControlPlaneRaftNodeId>,
            Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        ),
        ControlPlaneError,
    > {
        let timeout =
            self.writable_proposal_time_remaining(retry_deadline, "proposal-state read")?;
        ControlPlaneRaftTypeConfig::timeout(
            timeout,
            self.raft.with_raft_state(|state| {
                let effective_voters = state
                    .membership_state
                    .effective()
                    .membership()
                    .voter_ids()
                    .collect();
                (effective_voters, state.log_ids.last().copied())
            }),
        )
        .await
        .map_err(|_| ControlPlaneError::OpenRaftOperation {
            kind: ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
            message: format!("OpenRaft proposal-state read timed out after {timeout:?}"),
        })?
        .map_err(|error| openraft_remote_error("proposal-state read", error))
    }

    async fn prepare_writable_proposal(
        &self,
        retry_deadline: Instant,
    ) -> Result<ControlPlaneRaftProposalAttempt, ControlPlaneError> {
        let (effective_voters, _) = self.writable_proposal_raft_state(retry_deadline).await?;
        self.confirm_writable_proposal_lease(&effective_voters, retry_deadline)
            .await?;
        let leader_id = *self
            .raft
            .as_leader()
            .map_err(|error| ControlPlaneError::OpenRaftOperation {
                kind: ControlPlaneRaftOperationErrorKind::ForwardToLeader,
                message: format!(
                    "OpenRaft proposal preparation requires the local leader: {error}"
                ),
            })?
            .leader_id();
        let (_, last_log_id) = self.writable_proposal_raft_state(retry_deadline).await?;

        #[cfg(test)]
        {
            let delay = self
                .proposal_pause_after_confirmation
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            if let Some(delay) = delay {
                ControlPlaneRaftTypeConfig::sleep(delay).await;
            }
        }

        Ok(ControlPlaneRaftProposalAttempt {
            leader_id,
            last_log_id,
        })
    }

    async fn writable_proposal_may_retry(
        &self,
        attempt: &ControlPlaneRaftProposalAttempt,
        error: &RaftError<ControlPlaneRaftTypeConfig, ClientWriteError<ControlPlaneRaftTypeConfig>>,
        retry_deadline: Instant,
    ) -> Result<bool, ControlPlaneError> {
        let lease_rejection = matches!(
            error,
            RaftError::APIError(ClientWriteError::ForwardToLeader(forward))
                if forward.leader_id.is_none()
        );
        if !lease_rejection || Instant::now() >= retry_deadline {
            return Ok(false);
        }

        // An empty ForwardToLeader is also used when OpenRaft rejects an
        // expired proposal lease before append. It is not sufficient by
        // itself: leadership loss after append can produce the same error.
        // Retry only while the exact leader generation and local log tip from
        // immediately before dispatch are unchanged. Any append, including a
        // partial membership transition, makes the result ambiguous and is
        // returned to the caller without automatic resubmission.
        let Ok(leader) = self.raft.as_leader() else {
            return Ok(false);
        };
        if leader.leader_id() != &attempt.leader_id {
            return Ok(false);
        }
        let (_, last_log_id) = self.writable_proposal_raft_state(retry_deadline).await?;
        if last_log_id != attempt.last_log_id {
            #[cfg(test)]
            self.proposal_changed_tip_rejection_count
                .fetch_add(1, Ordering::Relaxed);
            return Ok(false);
        }

        #[cfg(test)]
        self.proposal_lease_retry_count
            .fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    async fn submit_control_plane_command_with_proposal_retry(
        &self,
        // Ordinary proposals retain this guard through completion. Evidence
        // proposals retain durable serialization but release the update guard
        // after RaftCore accepts the request, then reacquire it before
        // inspecting or rebasing state.
        ordinary_update_guard: Option<&tokio::sync::MutexGuard<'_, ()>>,
        mut evidence_update_guard: Option<&mut ControlPlaneRaftEvidenceUpdateGuard<'_>>,
        command: ControlPlaneCommand,
    ) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError> {
        debug_assert_eq!(
            ordinary_update_guard.is_some(),
            evidence_update_guard.is_none()
        );
        let encoded_len = validate_control_plane_command_replication_size_detailed(&command)?;
        if let Some(batch_metric) =
            crate::control_plane_command::unavailable_pg_batch_metric_descriptor(&command)
        {
            batch_metric.record_submission_with_encoded_bytes(encoded_len);
        }
        let retry_deadline = self.writable_proposal_retry_deadline();
        loop {
            let attempt = match evidence_update_guard.as_deref() {
                Some(evidence) => {
                    let admission = evidence.admission;
                    admission
                        .run_evidence_preappend(self.prepare_writable_proposal(retry_deadline))
                        .await?
                }
                None => self.prepare_writable_proposal(retry_deadline).await?,
            };
            self.ensure_writable_proposal_time_remaining(retry_deadline, "client-write dispatch")?;
            if let Some(evidence) = evidence_update_guard.as_deref() {
                evidence.admission.ensure_evidence_may_continue()?;
            }
            let response = if let Some(evidence) = evidence_update_guard.as_deref_mut() {
                #[cfg(test)]
                self.block_low_priority_before_dispatch_for_test().await;
                let (responder, response) = ProgressResponder::complete_only();
                let admission = evidence.admission;
                // client_write_ff completes at bounded-channel acceptance;
                // cancelling its Tokio send leaves the proposal unaccepted.
                admission
                    .run_evidence_preappend(async {
                        self.raft
                            .client_write_ff(command.clone(), Some(responder))
                            .await
                            .map_err(|error| ControlPlaneError::OpenRaftOperation {
                                kind: ControlPlaneRaftOperationErrorKind::Fatal,
                                message: format!(
                                    "OpenRaft client-write dispatch failed before acceptance: {error}"
                                ),
                            })
                    })
                    .await?;
                evidence.release_update_after_dispatch();
                #[cfg(test)]
                self.block_low_priority_after_dispatch_for_test().await;
                let response =
                    response
                        .await
                        .map_err(|error| ControlPlaneError::OpenRaftOperation {
                            kind: ControlPlaneRaftOperationErrorKind::Fatal,
                            message: format!(
                            "OpenRaft client-write response channel closed after dispatch: {error}"
                        ),
                        });
                evidence.reacquire_update_after_dispatch().await;
                response?
            } else {
                debug_assert!(ordinary_update_guard.is_some());
                match self.raft.client_write(command.clone()).await {
                    Ok(response) => Ok(response),
                    Err(RaftError::APIError(error)) => Err(error),
                    Err(RaftError::Fatal(error)) => {
                        return Err(ControlPlaneError::OpenRaftOperation {
                            kind: ControlPlaneRaftOperationErrorKind::Fatal,
                            message: format!("OpenRaft client-write failed: {error}"),
                        });
                    }
                }
            };
            match response {
                Ok(response) => return submitted_control_plane_command(response),
                Err(error) => {
                    let raft_error = RaftError::APIError(error);
                    let may_retry = match evidence_update_guard.as_deref() {
                        Some(evidence) => {
                            let admission = evidence.admission;
                            let certification = async {
                                #[cfg(test)]
                                self.block_low_priority_during_retry_certification_for_test()
                                    .await;
                                self.writable_proposal_may_retry(
                                    &attempt,
                                    &raft_error,
                                    retry_deadline,
                                )
                                .await
                            };
                            match admission.run_evidence_preappend(certification).await {
                                Err(ControlPlaneError::StagingEvidencePublicationDeferred) => {
                                    return Err(ControlPlaneError::StagingEvidencePublicationOutcomeUnconfirmed {
                                        message: "ordinary control-plane work interrupted post-dispatch retry certification".to_owned(),
                                    });
                                }
                                result => result?,
                            }
                        }
                        None => {
                            self.writable_proposal_may_retry(&attempt, &raft_error, retry_deadline)
                                .await?
                        }
                    };
                    if may_retry {
                        #[cfg(test)]
                        if evidence_update_guard.is_some() {
                            self.block_low_priority_after_proven_unappended_retry_for_test()
                                .await;
                        }
                        if let Some(evidence) = evidence_update_guard.as_deref() {
                            evidence.admission.ensure_evidence_may_continue()?;
                        }
                        continue;
                    }
                    return Err(openraft_client_write_error("client-write", raft_error));
                }
            }
        }
    }

    #[cfg(test)]
    fn pause_next_proposal_after_lease_confirmation_for_test(&self, delay: Duration) {
        *self
            .proposal_pause_after_confirmation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(delay);
        self.proposal_lease_retry_count.store(0, Ordering::Relaxed);
        self.proposal_changed_tip_rejection_count
            .store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn proposal_lease_retry_count_for_test(&self) -> usize {
        self.proposal_lease_retry_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn proposal_changed_tip_rejection_count_for_test(&self) -> usize {
        self.proposal_changed_tip_rejection_count
            .load(Ordering::Relaxed)
    }

    async fn submit_control_plane_command_derived_locked<F>(
        &self,
        ordinary_update_guard: Option<&tokio::sync::MutexGuard<'_, ()>>,
        mut evidence_update_guard: Option<&mut ControlPlaneRaftEvidenceUpdateGuard<'_>>,
        derive_command: F,
    ) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>
    where
        F: FnOnce(&ClusterControlSnapshot) -> Result<ControlPlaneCommand, ControlPlaneError>,
    {
        debug_assert_eq!(
            ordinary_update_guard.is_some(),
            evidence_update_guard.is_none()
        );
        let status = match evidence_update_guard.as_deref() {
            Some(evidence) => {
                let admission = evidence.admission;
                admission
                    .run_evidence_preappend(self.confirmed_linearized_authority_status())
                    .await?
            }
            None => self.confirmed_linearized_authority_status().await?,
        };
        let authority_term = status.current_term();
        let (mut durable_snapshot, durable_applied) = match evidence_update_guard.as_deref() {
            Some(evidence) => {
                let admission = evidence.admission;
                admission
                    .run_evidence_preappend(self.durable_snapshot_and_applied())
                    .await?
            }
            None => self.durable_snapshot_and_applied().await?,
        };
        let mut overlay_snapshot = match (authority_term, status.applied(), durable_applied) {
            (Some(authority_term), Some(status_applied), Some(durable_applied))
                if status_applied == durable_applied =>
            {
                self.volatile_heartbeat_overlay_snapshot(authority_term, durable_applied)?
                    .map(|snapshot| (authority_term, durable_applied, snapshot))
            }
            _ => None,
        };
        let mut evidence_overlay_base = evidence_update_guard
            .is_some()
            .then(|| {
                authority_term.zip(durable_applied).ok_or_else(|| {
                    ControlPlaneError::SnapshotInvariantViolation {
                        context: "low-priority control-plane proposal overlay binding",
                        message: "serving evidence submission has no exact authority term and applied log"
                            .to_owned(),
                    }
                })
            })
            .transpose()?;
        if let Some((overlay_authority_term, overlay_base_applied, live_snapshot)) =
            overlay_snapshot.take()
        {
            if let Some(promotion) =
                live_snapshot.promote_volatile_heartbeat_leases_command(&durable_snapshot)?
            {
                if let Some(evidence) = evidence_update_guard.as_deref() {
                    evidence.admission.ensure_evidence_may_continue()?;
                }
                let promoted_durable = durable_snapshot
                    .apply_control_plane_command(promotion.clone())?
                    .into_snapshot();
                let submitted_promotion = self
                    .submit_control_plane_command_with_proposal_retry(
                        ordinary_update_guard,
                        evidence_update_guard.as_deref_mut(),
                        promotion.clone(),
                    )
                    .await?;
                let promotion_log_id = submitted_promotion.log_id();
                match submitted_promotion.into_outcome() {
                    ControlPlaneRaftCommandOutcome::Applied(
                        ControlPlaneCommandResponse::PromoteNodeHeartbeatLeases,
                    ) => {}
                    ControlPlaneRaftCommandOutcome::Applied(response) => {
                        return Err(ControlPlaneError::SnapshotInvariantViolation {
                            context: "volatile heartbeat lease promotion",
                            message: format!("promotion returned unexpected response {response:?}"),
                        });
                    }
                    ControlPlaneRaftCommandOutcome::Rejected(error) => return Err(error),
                }
                let latest_live = match self.volatile_heartbeat_overlay_snapshot(
                    overlay_authority_term,
                    overlay_base_applied,
                )? {
                    Some(snapshot) => snapshot,
                    None => self
                        .volatile_heartbeat_overlay_snapshot(
                            overlay_authority_term,
                            promotion_log_id,
                        )?
                        .unwrap_or(live_snapshot),
                };
                // The live snapshot already contains every promoted deadline;
                // reapplying the older promotion could regress a later renewal.
                latest_live.promote_volatile_heartbeat_leases_command(&promoted_durable)?;
                let promoted_live = latest_live;
                self.publish_rebased_volatile_heartbeat_overlay(
                    overlay_authority_term,
                    promotion_log_id,
                    promoted_live.clone(),
                )
                .await?;
                durable_snapshot = promoted_durable;
                evidence_overlay_base = evidence_update_guard
                    .is_some()
                    .then_some((overlay_authority_term, promotion_log_id));
                overlay_snapshot = Some((overlay_authority_term, promotion_log_id, promoted_live));
            } else {
                overlay_snapshot =
                    Some((overlay_authority_term, overlay_base_applied, live_snapshot));
            }
        }
        let effective_snapshot = overlay_snapshot
            .as_ref()
            .map_or(&durable_snapshot, |(_, _, snapshot)| snapshot);
        let command = derive_command(effective_snapshot)?;
        let command =
            effective_snapshot.bind_metadata_transfer_fence_command(&durable_snapshot, command)?;
        let overlay_rebase = overlay_snapshot
            .map(|(authority_term, base_applied, snapshot)| {
                let durable_would_apply = durable_snapshot
                    .apply_control_plane_command(command.clone())
                    .is_ok();
                let applied_snapshot = snapshot
                    .apply_control_plane_command(command.clone())
                    .map(|applied| applied.into_snapshot());
                if durable_would_apply && applied_snapshot.is_err() {
                    return Err(ControlPlaneError::SnapshotInvariantViolation {
                        context: "volatile heartbeat overlay command rebase",
                        message: "command applies to committed state but rejects against acknowledged live heartbeat state".to_string(),
                    });
                }
                Ok((authority_term, base_applied, snapshot))
            })
            .transpose()?;

        let submitted = self
            .submit_control_plane_command_with_proposal_retry(
                ordinary_update_guard,
                evidence_update_guard,
                command.clone(),
            )
            .await?;
        let overlay_rebase = match evidence_overlay_base {
            Some((authority_term, base_applied)) => self
                .volatile_heartbeat_overlay_snapshot(authority_term, base_applied)?
                .map(|snapshot| (authority_term, base_applied, snapshot)),
            None => overlay_rebase,
        };
        if let Some((authority_term, _base_applied, latest_snapshot)) = overlay_rebase {
            let applied_snapshot = latest_snapshot
                .apply_control_plane_command(command)
                .map(|applied| applied.into_snapshot());
            let snapshot = match submitted.outcome() {
                ControlPlaneRaftCommandOutcome::Applied(_) => {
                    applied_snapshot.ok().ok_or_else(|| {
                        ControlPlaneError::SnapshotInvariantViolation {
                            context: "volatile heartbeat overlay command rebase",
                            message: "committed command outcome differed from the latest acknowledged live heartbeat state"
                                .to_string(),
                        }
                    })?
                }
                ControlPlaneRaftCommandOutcome::Rejected(_) => latest_snapshot,
            };
            self.publish_rebased_volatile_heartbeat_overlay(
                authority_term,
                submitted.log_id(),
                snapshot,
            )
            .await?;
        }
        Ok(submitted)
    }

    /// Applies a heartbeat only to this leader's live view when the committed
    /// lease horizon already covers it and no durable control-plane field
    /// changes. The overlay is tied to the exact applied log ID and leadership
    /// term. Same-term command submission serializes with this method and
    /// deterministically rebases the overlay; a term change makes it
    /// ineligible immediately.
    pub async fn try_apply_volatile_heartbeat(
        &self,
        command: ControlPlaneCommand,
    ) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
        #[cfg(test)]
        self.run_before_heartbeat_update_gate_hook_for_test();
        let _update_guard = self
            .evidence_submission_admission
            .acquire_volatile_update_gate(&self.volatile_heartbeat_update_gate)
            .await;
        let status = self.status().await?;
        let Some(authority_term) = status
            .linearized_authority_serving()
            .then_some(status.current_term())
            .flatten()
        else {
            return Ok(None);
        };
        let (durable_snapshot, Some(base_applied)) = self.durable_snapshot_and_applied().await?
        else {
            return Ok(None);
        };
        let base_snapshot = self
            .volatile_heartbeat_overlay_snapshot(authority_term, base_applied)?
            .unwrap_or(durable_snapshot);
        let Some(next_snapshot) = base_snapshot.apply_covered_volatile_heartbeat(command)? else {
            return Ok(None);
        };

        let current_status = self.status().await?;
        if !current_status.linearized_authority_serving()
            || current_status.current_term() != Some(authority_term)
            || current_status.applied() != Some(base_applied)
        {
            return Err(ControlPlaneError::rpc_remote(
                "OpenRaft authority changed while publishing volatile heartbeat".to_string(),
            ));
        }
        let generation = self.retained_snapshot_generation(Arc::new(next_snapshot.clone()));
        *self.lock_volatile_heartbeat_overlay()? = Some(ControlPlaneRaftVolatileHeartbeatOverlay {
            authority_term,
            base_applied,
            snapshot: generation,
        });
        Ok(Some(next_snapshot))
    }

    async fn linearized_control_plane_snapshot_selection(
        &self,
    ) -> Result<ControlPlaneRaftLinearizedSnapshot, ControlPlaneError> {
        #[cfg(test)]
        self.linearized_runtime_map_read_index_count
            .fetch_add(1, Ordering::SeqCst);
        let required_applied = control_plane_read_index_via_openraft(&self.raft).await?;
        let required_read_index =
            control_plane_log_id_from_raft(required_applied).ok_or_else(|| {
                ControlPlaneError::CommandDecode {
                    message: format!(
                        "invalid OpenRaft read-index log id for runtime map: {required_applied}"
                    ),
                }
            })?;
        #[cfg(test)]
        self.run_linearized_read_after_snapshot_gate_for_test()
            .await;

        // A ReadIndex establishes the lower bound. Command submission and
        // volatile heartbeat publication serialize through this gate, so one
        // later state-machine capture is both linearizable and paired with the
        // overlay for its exact applied tip. Do not repeat ReadIndex under a
        // sustained write stream, and do not hold this gate during quorum I/O.
        let _update_guard = self.volatile_heartbeat_update_gate.lock().await;
        let node_id = *self.raft.node_id();
        let current_leader = self.raft.current_leader().await;
        let current_term = self
            .log_store
            .as_ref()
            .map(ControlPlaneRaftLogStore::status_snapshot)
            .transpose()
            .map_err(|error| openraft_remote_error("runtime-map vote read", error))?
            .and_then(|status| status.durable_vote)
            .map(|vote| vote.leader_id.term);
        let (server_state, committed, effective_voter) = self
            .raft
            .with_raft_state(move |state| {
                (
                    state.server_state,
                    state.local_committed().cloned(),
                    state
                        .membership_state
                        .effective()
                        .membership()
                        .voter_ids()
                        .any(|voter| voter == node_id),
                )
            })
            .await
            .map_err(|error| openraft_remote_error("runtime-map raft-state read", error))?;
        #[cfg(test)]
        self.run_linearized_read_after_raft_state_capture_gate_for_test()
            .await;
        let (durable_snapshot, applied, control_plane_applied) =
            self.retained_state_machine_snapshot_generation().await?;
        #[cfg(test)]
        self.run_linearized_read_after_generation_capture_gate_for_test()
            .await;
        let Some(applied) = applied else {
            return Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index: required_read_index,
                last_applied: control_plane_applied,
            });
        };
        if applied < required_applied {
            return Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index: required_read_index,
                last_applied: control_plane_applied,
            });
        }
        let local_leader = server_state == ServerState::Leader && current_leader == Some(node_id);
        let applied_caught_up_to_committed = committed == Some(applied);
        let committed_in_current_term = matches!(
            (current_term, committed),
            (Some(current_term), Some(committed))
                if committed.committed_leader_id().term == current_term
        );
        if !linearized_authority_readiness_from_flags(
            local_leader,
            effective_voter,
            applied_caught_up_to_committed,
            committed_in_current_term,
        )
        .serving()
        {
            return Err(ControlPlaneError::AuthorityNotServing);
        }
        let authority_term = current_term.ok_or_else(|| {
            ControlPlaneError::invariant_failure("serving OpenRaft authority has no current term")
        })?;
        let read_index = control_plane_log_id_from_raft(applied).ok_or_else(|| {
            ControlPlaneError::CommandDecode {
                message: format!("invalid OpenRaft applied log id for runtime map: {applied}"),
            }
        })?;
        let volatile_snapshot = self.volatile_heartbeat_overlay_arc(authority_term, applied)?;
        let (snapshot, volatile_authority_term) = match volatile_snapshot {
            Some(snapshot) => (
                durable_snapshot.replace_snapshot(snapshot),
                Some(authority_term),
            ),
            None => (durable_snapshot, None),
        };
        Ok(ControlPlaneRaftLinearizedSnapshot {
            snapshot,
            applied,
            read_index,
            volatile_authority_term,
        })
    }

    async fn derive_linearized_snapshot<T, F>(
        selected: ControlPlaneRaftLinearizedSnapshot,
        context: &'static str,
        derive: F,
    ) -> Result<T, ControlPlaneError>
    where
        T: Send + 'static,
        F: FnOnce(&ClusterControlSnapshot, ControlPlaneLogId) -> Result<T, ControlPlaneError>
            + Send
            + 'static,
    {
        let ControlPlaneRaftLinearizedSnapshot {
            snapshot,
            read_index,
            ..
        } = selected;
        tokio::task::spawn_blocking(move || {
            let snapshot = snapshot.into_blocking_lane_arc();
            derive(&snapshot, read_index)
        })
        .await
        .map_err(|error| {
            ControlPlaneError::rpc_remote(format!(
                "control-plane {context} derivation worker failed: {error}"
            ))
        })?
    }

    pub async fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let selected = self.linearized_control_plane_snapshot_selection().await?;
        Self::derive_linearized_snapshot(selected, "runtime-map", move |snapshot, read_index| {
            snapshot.runtime_map_with_freshness_proof(
                issued_at_ms,
                RuntimeMapFreshnessProof::ReadIndex {
                    authority_incarnation: snapshot.authority_incarnation(),
                    read_index,
                    issued_at_ms,
                },
            )
        })
        .await
    }

    pub async fn linearized_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let selected = self.linearized_control_plane_snapshot_selection().await?;
        Self::derive_linearized_snapshot(
            selected,
            "reconstructed scoped runtime-map",
            move |snapshot, _read_index| {
                snapshot.reconstructed_runtime_map_for_pg(pg_id, issued_at_ms)
            },
        )
        .await
    }

    pub async fn linearized_serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let selected = self.linearized_control_plane_snapshot_selection().await?;
        Self::derive_linearized_snapshot(
            selected,
            "scoped runtime-map",
            move |snapshot, read_index| {
                snapshot.serving_runtime_map_for_pg_with_freshness_proof(
                    pg_id,
                    issued_at_ms,
                    RuntimeMapFreshnessProof::ReadIndex {
                        authority_incarnation: snapshot.authority_incarnation(),
                        read_index,
                        issued_at_ms,
                    },
                )
            },
        )
        .await
    }

    pub async fn linearized_runtime_map_diagnostics_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapDiagnosticSnapshot, ControlPlaneError> {
        let selected = self.linearized_control_plane_snapshot_selection().await?;
        Self::derive_linearized_snapshot(
            selected,
            "runtime-map diagnostics",
            move |snapshot, read_index| {
                let node_leases = snapshot
                    .nodes()
                    .map(|node| {
                        ControlPlaneRuntimeMapNodeLeaseDiagnostic::new(
                            node.node_id(),
                            node.lease_deadline_ms(),
                        )
                    })
                    .collect();
                let runtime_map = snapshot.runtime_map_with_freshness_proof(
                    issued_at_ms,
                    RuntimeMapFreshnessProof::ReadIndex {
                        authority_incarnation: snapshot.authority_incarnation(),
                        read_index,
                        issued_at_ms,
                    },
                )?;
                ControlPlaneRuntimeMapDiagnosticSnapshot::new(runtime_map, node_leases)
            },
        )
        .await
    }

    pub async fn linearized_runtime_map_status(
        &self,
        issued_at_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        let selected = self.linearized_control_plane_snapshot_selection().await?;
        let selected_applied = selected.applied;
        let selected_authority_term = selected.volatile_authority_term;
        let cached_certificate = if let Some(authority_term) = selected.volatile_authority_term {
            self.runtime_map_overlay_content_certificate
                .lock()
                .map_err(|_| {
                    ControlPlaneError::rpc_protocol(
                        "control-plane OpenRaft overlay runtime-map content certificate lock poisoned"
                            .to_owned(),
                    )
                })?
                .as_ref()
                .filter(|(term, applied, _)| {
                    *term == authority_term && *applied == selected.applied
                })
                .map(|(_, _, certificate)| *certificate)
        } else {
            self.runtime_map_content_certificate
                .lock()
                .map_err(|_| {
                    ControlPlaneError::rpc_protocol(
                        "control-plane OpenRaft runtime-map content certificate lock poisoned"
                            .to_owned(),
                    )
                })?
                .as_ref()
                .filter(|(applied, _)| *applied == selected.applied)
                .map(|(_, certificate)| *certificate)
        };
        let (status, new_certificate) = Self::derive_linearized_snapshot(
            selected,
            "runtime-map status",
            move |snapshot, read_index| {
                let freshness_proof = RuntimeMapFreshnessProof::ReadIndex {
                    authority_incarnation: snapshot.authority_incarnation(),
                    read_index,
                    issued_at_ms,
                };
                if let Some(certificate) = cached_certificate {
                    if let Some(status) =
                        ControlPlaneRuntimeMapStatus::from_snapshot_with_content_certificate(
                            snapshot,
                            issued_at_ms,
                            freshness_proof,
                            certificate,
                        )?
                    {
                        return Ok((status, None));
                    }
                }
                let runtime_map =
                    snapshot.runtime_map_with_freshness_proof(issued_at_ms, freshness_proof)?;
                let status = ControlPlaneRuntimeMapStatus::from_runtime_map(&runtime_map);
                let certificate = RuntimeMapContentCertificate::from_snapshot_and_runtime_map(
                    snapshot,
                    &runtime_map,
                );
                Ok((status, Some(certificate)))
            },
        )
        .await?;
        let Some(certificate) = new_certificate else {
            return Ok(status);
        };
        if let Some(authority_term) = selected_authority_term {
            *self
                .runtime_map_overlay_content_certificate
                .lock()
                .map_err(|_| {
                    ControlPlaneError::rpc_protocol(
                        "control-plane OpenRaft overlay runtime-map content certificate lock poisoned"
                            .to_owned(),
                    )
                })? = Some((authority_term, selected_applied, certificate));
        } else {
            *self.runtime_map_content_certificate.lock().map_err(|_| {
                ControlPlaneError::rpc_protocol(
                    "control-plane OpenRaft runtime-map content certificate lock poisoned"
                        .to_owned(),
                )
            })? = Some((selected_applied, certificate));
        }
        Ok(status)
    }

    pub async fn current_control_plane_snapshot(
        &self,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let (durable_snapshot, applied) = self.durable_snapshot_and_applied().await?;
        let status = self.status().await?;
        let Some(authority_term) = status
            .linearized_authority_serving()
            .then_some(status.current_term())
            .flatten()
        else {
            return Ok(durable_snapshot);
        };
        let Some(applied) = applied.filter(|applied| status.applied() == Some(*applied)) else {
            return Ok(durable_snapshot);
        };
        Ok(self
            .volatile_heartbeat_overlay_snapshot(authority_term, applied)?
            .unwrap_or(durable_snapshot))
    }

    async fn durable_snapshot_and_applied(
        &self,
    ) -> Result<
        (
            ClusterControlSnapshot,
            Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        ),
        ControlPlaneError,
    > {
        self.raft
            .with_state_machine(|state_machine| {
                let snapshot = state_machine.inner().snapshot().clone();
                let applied = state_machine.last_applied();
                Box::pin(async move { (snapshot, applied) })
            })
            .await
            .map_err(|error| openraft_remote_error("state-machine snapshot read", error))
    }

    async fn current_volatile_heartbeat_overlay(
        &self,
    ) -> Result<Option<(ControlPlaneRaftTerm, ClusterControlSnapshot)>, ControlPlaneError> {
        let status = self.status().await?;
        let Some(authority_term) = status
            .linearized_authority_serving()
            .then_some(status.current_term())
            .flatten()
        else {
            return Ok(None);
        };
        let Some(applied) = status.applied() else {
            return Ok(None);
        };
        Ok(self
            .volatile_heartbeat_overlay_snapshot(authority_term, applied)?
            .map(|snapshot| (authority_term, snapshot)))
    }

    async fn publish_rebased_volatile_heartbeat_overlay(
        &self,
        authority_term: ControlPlaneRaftTerm,
        base_applied: LogIdOf<ControlPlaneRaftTypeConfig>,
        snapshot: ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        let current_status = self.status().await?;
        if current_status.linearized_authority_serving()
            && current_status.current_term() == Some(authority_term)
            && current_status.applied() == Some(base_applied)
        {
            let generation = self.retained_snapshot_generation(Arc::new(snapshot));
            *self.lock_volatile_heartbeat_overlay()? =
                Some(ControlPlaneRaftVolatileHeartbeatOverlay {
                    authority_term,
                    base_applied,
                    snapshot: generation,
                });
        }
        Ok(())
    }

    fn volatile_heartbeat_overlay_snapshot(
        &self,
        authority_term: ControlPlaneRaftTerm,
        base_applied: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
        Ok(self
            .volatile_heartbeat_overlay_generation(authority_term, base_applied)?
            .map(|generation| Arc::unwrap_or_clone(generation.into_blocking_lane_arc())))
    }

    fn volatile_heartbeat_overlay_generation(
        &self,
        authority_term: ControlPlaneRaftTerm,
        base_applied: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<Option<ControlPlaneRaftSnapshotGeneration>, ControlPlaneError> {
        Ok(self
            .volatile_heartbeat_overlay_arc(authority_term, base_applied)?
            .map(|snapshot| self.retained_snapshot_generation(snapshot)))
    }

    fn volatile_heartbeat_overlay_arc(
        &self,
        authority_term: ControlPlaneRaftTerm,
        base_applied: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<Option<Arc<ClusterControlSnapshot>>, ControlPlaneError> {
        Ok(self
            .lock_volatile_heartbeat_overlay()?
            .as_ref()
            .filter(|overlay| {
                overlay.authority_term == authority_term && overlay.base_applied == base_applied
            })
            .map(|overlay| Arc::clone(overlay.snapshot.arc())))
    }

    fn lock_volatile_heartbeat_overlay(
        &self,
    ) -> Result<MutexGuard<'_, Option<ControlPlaneRaftVolatileHeartbeatOverlay>>, ControlPlaneError>
    {
        self.volatile_heartbeat_overlay.lock().map_err(|_| {
            ControlPlaneError::rpc_remote(
                "OpenRaft volatile heartbeat overlay mutex poisoned".to_string(),
            )
        })
    }

    pub(crate) async fn store_durable_restart_artifact(
        &self,
    ) -> Result<Option<u64>, ControlPlaneError> {
        let checkpoint = self.capture_durable_restart_checkpoint().await?;
        let path = self.configured_durable_artifact_path()?;
        let authority_instance_id = self.authority_instance_id()?;
        let checkpoint_publication = Arc::clone(&self.checkpoint_publication);
        let checkpoint_metrics = Arc::clone(&self.checkpoint_metrics);
        let log_store = self.log_store.clone();
        tokio::task::spawn_blocking(move || {
            Self::persist_durable_restart_checkpoint_inner(
                authority_instance_id,
                &checkpoint_publication,
                &checkpoint_metrics,
                log_store.as_ref(),
                checkpoint,
                path.as_ref(),
            )
        })
        .await
        .map_err(|error| {
            ControlPlaneError::rpc_remote(format!(
                "OpenRaft checkpoint persistence worker failed: {error}"
            ))
        })?
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn store_durable_restart_artifact_for_test(
        &self,
    ) -> Result<Option<u64>, ControlPlaneError> {
        self.store_durable_restart_artifact().await
    }

    pub(crate) async fn capture_durable_restart_checkpoint(
        &self,
    ) -> Result<ControlPlaneRaftCapturedRestartCheckpoint, ControlPlaneError> {
        let authority_instance_id = self.authority_instance_id()?;
        Ok(ControlPlaneRaftCapturedRestartCheckpoint {
            artifact: self.capture_durable_restart_artifact().await?,
            authority_instance_id,
        })
    }

    /// Capture and durably publish a restart checkpoint only when it is
    /// certified against this authority's retained static peer policy.
    ///
    /// `Ok(None)` means the captured effective membership or applied state has
    /// not yet converged. The caller receives no checkpoint or policy details
    /// and cannot publish the outer static identity until storage returns the
    /// opaque publication proof.
    pub(crate) async fn publish_static_identity_restart_checkpoint(
        &self,
    ) -> Result<Option<ControlPlaneRaftStaticIdentityCheckpointPublication>, ControlPlaneError>
    {
        let peer_policy = self.static_peer_policy.as_ref().ok_or_else(|| {
            ControlPlaneError::static_topology_failure(
                "static identity checkpoint publication requires a configured static peer policy",
            )
        })?;
        let checkpoint = self.capture_durable_restart_checkpoint().await?;
        match checkpoint.established_peer_policy_convergence(peer_policy)? {
            ControlPlaneRaftEstablishedPeerPolicyConvergence::Converged => {
                let committed_timestamp_high_water_ms =
                    self.persist_durable_restart_checkpoint(checkpoint)?;
                Ok(Some(ControlPlaneRaftStaticIdentityCheckpointPublication {
                    authority_clock_binding: self.authority_clock_checkpoint_binding(),
                    committed_timestamp_high_water_ms,
                }))
            }
            ControlPlaneRaftEstablishedPeerPolicyConvergence::EffectiveMembershipPending {
                ..
            }
            | ControlPlaneRaftEstablishedPeerPolicyConvergence::AppliedStatePending { .. } => {
                Ok(None)
            }
        }
    }

    pub(crate) fn persist_durable_restart_checkpoint(
        &self,
        checkpoint: ControlPlaneRaftCapturedRestartCheckpoint,
    ) -> Result<Option<u64>, ControlPlaneError> {
        let path = self.configured_durable_artifact_path()?;
        Self::persist_durable_restart_checkpoint_inner(
            self.authority_instance_id()?,
            &self.checkpoint_publication,
            &self.checkpoint_metrics,
            self.log_store.as_ref(),
            checkpoint,
            path.as_ref(),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn capture_and_persist_restart_checkpoint_without_clock_sidecar_for_test(
        &self,
    ) -> Result<Option<u64>, ControlPlaneError> {
        let checkpoint = self.capture_durable_restart_checkpoint().await?;
        self.persist_durable_restart_checkpoint(checkpoint)
    }

    pub(crate) fn configured_durable_artifact_path(
        &self,
    ) -> Result<Arc<PathBuf>, ControlPlaneError> {
        self.durable_artifact_path
            .as_ref()
            .cloned()
            .ok_or_else(|| ControlPlaneError::rpc_remote("OpenRaft durable checkpoint requested for an authority without configured durable state".to_owned()))
    }

    fn persist_durable_restart_checkpoint_inner(
        authority_instance_id: ControlPlaneRaftAuthorityInstanceId,
        checkpoint_publication: &Mutex<Option<ControlPlaneRaftCheckpointPosition>>,
        checkpoint_metrics: &ControlPlaneRaftCheckpointMetrics,
        log_store: Option<&ControlPlaneRaftLogStore>,
        checkpoint: ControlPlaneRaftCapturedRestartCheckpoint,
        path: &Path,
    ) -> Result<Option<u64>, ControlPlaneError> {
        if checkpoint.authority_instance_id != authority_instance_id {
            return Err(ControlPlaneError::rpc_remote(
                "captured OpenRaft restart checkpoint belongs to another authority instance"
                    .to_string(),
            ));
        }
        let position = ControlPlaneRaftCheckpointPosition::for_artifact(&checkpoint.artifact);
        let mut last_publication = checkpoint_publication.lock().map_err(|_| {
            ControlPlaneError::rpc_remote(
                "OpenRaft checkpoint publication mutex poisoned".to_string(),
            )
        })?;
        if let Some(previous) = *last_publication {
            position.validate_at_or_after(previous)?;
        }
        if let Some(wal_status) = log_store
            .map(ControlPlaneRaftLogStore::wal_monitor_snapshot)
            .transpose()
            .map_err(|source| {
                ControlPlaneError::io(
                    "validate captured OpenRaft checkpoint against current WAL",
                    source,
                )
            })?
            .flatten()
        {
            if position.wal_replay_offset < wal_status.offsets().base_offset() {
                return Err(ControlPlaneError::rpc_remote(format!(
                        "captured OpenRaft restart checkpoint WAL offset {} precedes the current WAL base offset {}",
                        position.wal_replay_offset,
                        wal_status.offsets().base_offset()
                    )));
            }
        }

        let wal_replay_offset = checkpoint.artifact.wal_replay_offset;
        let committed_timestamp_high_water_ms = checkpoint
            .artifact
            .state_machine
            .inner
            .snapshot()
            .max_committed_timestamp_ms();
        // Reserve the monotonic publication position before filesystem mutation.
        // A failed or ambiguous write may have replaced the artifact, so allowing
        // an older captured token afterward would reintroduce rollback.
        *last_publication = Some(position);
        checkpoint
            .artifact
            .store_durable_artifact_with_metrics(path, Some(checkpoint_metrics))?;
        if let Some(log_store) = log_store {
            let compact_started = Instant::now();
            let result = log_store.compact_wal_through(wal_replay_offset);
            let compact_elapsed = compact_started.elapsed();
            observability::record_control_plane_raft_checkpoint_compaction(
                compact_elapsed,
                result.is_ok(),
            );
            checkpoint_metrics.record_compaction(compact_elapsed, result.is_ok());
            result.map_err(|source| {
                ControlPlaneError::io(
                    "compact control-plane OpenRaft WAL after durable checkpoint",
                    source,
                )
            })?;
        }
        Ok(committed_timestamp_high_water_ms)
    }

    async fn capture_durable_restart_artifact(
        &self,
    ) -> Result<ControlPlaneRaftRestartArtifact, ControlPlaneError> {
        let log_store = self.log_store.as_ref().cloned().ok_or_else(|| {
            ControlPlaneError::rpc_remote(
                "OpenRaft durable restart artifact requested without retained log store"
                    .to_string(),
            )
        })?;

        let capture_started = Instant::now();
        let mut attempts = 0_u64;
        let mut last_validation_error = None;
        loop {
            if !control_plane_raft_restart_capture_attempt_allowed(attempts) {
                let elapsed = capture_started.elapsed();
                let validation_error = last_validation_error.expect(
                    "a denied OpenRaft restart capture retry must follow a validation failure",
                );
                return Err(ControlPlaneError::io("capture consistent control-plane OpenRaft durable restart artifact", raft_log_store_error(format!(
                        "control-plane OpenRaft restart artifact capture exhausted its {attempts}-attempt retry budget after {elapsed:?}: {validation_error}",
                    ))));
            }
            attempts = attempts.saturating_add(1);
            // Capture the state machine first. If Raft advances concurrently,
            // the later log-store export may be ahead, which restart can
            // replay. The reverse order could persist state that the exported
            // log cannot prove.
            let state_machine = self
                .capture_state_machine_restart_artifact()
                .await?
                .refresh_cached_snapshot_async()
                .await?;
            let (log_store_artifact, wal_replay_offset) = log_store
                .export_restart_artifact_with_wal_replay_offset()
                .map_err(|source| {
                    ControlPlaneError::io(
                        "export control-plane OpenRaft durable log-store restart artifact",
                        source,
                    )
                })?;
            let artifact = ControlPlaneRaftRestartArtifact {
                cluster_name: self.cluster_name.clone(),
                local_node_id: self.node_id,
                wal_replay_offset,
                log_store: log_store_artifact,
                state_machine,
            };
            let validation_error = match artifact.validate_restart_pair() {
                Ok(()) => return Ok(artifact),
                Err(error) => error,
            };
            last_validation_error = Some(validation_error);
            if control_plane_raft_restart_capture_attempt_allowed(attempts) {
                ControlPlaneRaftTypeConfig::sleep(CONTROL_PLANE_RAFT_RESTART_CAPTURE_RETRY_DELAY)
                    .await;
            }
        }
    }

    async fn capture_state_machine_restart_artifact(
        &self,
    ) -> Result<ControlPlaneRaftStateMachineRestartArtifact, ControlPlaneError> {
        self.raft
            .with_state_machine(|state_machine| {
                let artifact = state_machine.export_restart_artifact();
                Box::pin(async move { artifact })
            })
            .await
            .map_err(|error| openraft_remote_error("state-machine restart artifact read", error))
    }

    pub async fn status(&self) -> Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError> {
        let node_id = *self.raft.node_id();
        let current_leader = self.raft.current_leader().await;
        let log_store_status = self
            .log_store
            .as_ref()
            .map(ControlPlaneRaftLogStore::status_snapshot)
            .transpose()
            .map_err(|error| openraft_remote_error("status log-store read", error))?;
        let persisted_vote = log_store_status
            .as_ref()
            .and_then(|status| status.durable_vote);
        let durable_last_vote = persisted_vote;
        let current_term = persisted_vote.map(|vote| vote.leader_id.term);
        let durable_last_log_id = log_store_status
            .as_ref()
            .and_then(|status| status.durable_last_log_id);
        let durable_committed = log_store_status
            .as_ref()
            .and_then(|status| status.durable_committed);
        let last_purged_log_id = log_store_status
            .as_ref()
            .and_then(|status| status.last_purged_log_id);
        let durable_last_purged_log_id = log_store_status
            .as_ref()
            .and_then(|status| status.durable_last_purged_log_id);
        let durability_status = log_store_status.as_ref().map(|status| &status.durability);
        let durable_wal_backed = durability_status
            .as_ref()
            .is_some_and(|status| status.wal_backed);
        let durable_wal_offsets = durability_status
            .as_ref()
            .and_then(|status| status.wal_offsets);
        let durable_wal_poisoned = durability_status.and_then(|status| status.wal_poisoned.clone());
        let (
            last_log_id,
            committed,
            server_state,
            effective_membership_log_id,
            effective_voters,
            effective_learners,
        ) = self
            .raft
            .with_raft_state(|state| {
                let effective_membership = state.membership_state.effective();
                (
                    state.log_ids.last().cloned(),
                    state.local_committed().cloned(),
                    state.server_state,
                    *effective_membership.log_id(),
                    effective_membership
                        .membership()
                        .voter_ids()
                        .collect::<BTreeSet<_>>(),
                    effective_membership
                        .membership()
                        .learner_ids()
                        .collect::<BTreeSet<_>>(),
                )
            })
            .await
            .map_err(|error| openraft_remote_error("status raft-state read", error))?;
        let (
            applied,
            current_snapshot,
            durable_timestamp_high_water_ms,
            authority_incarnation,
            current_cluster_epoch,
            retained_history_count,
            oldest_retained_history_epoch,
            newest_retained_history_epoch,
            oldest_storage_history_floor_epoch,
            storage_node_lease_deadline_count,
            earliest_storage_node_lease_deadline_ms,
            latest_storage_node_lease_deadline_ms,
            storage_node_count,
            joining_storage_node_count,
            active_storage_node_count,
            draining_storage_node_count,
            out_storage_node_count,
            removed_storage_node_count,
            healthy_storage_node_count,
            suspect_storage_node_count,
            unavailable_storage_node_count,
            pg_count,
            active_pg_count,
            peering_pg_count,
            degraded_pg_count,
            backfilling_pg_count,
            inconsistent_pg_count,
            active_primary_pg_count,
            peering_metadata_transfer_pg_count,
            metadata_transfer_fenced_pg_count,
            metadata_transfer_fence_source_lease_deadline_count,
            earliest_metadata_transfer_fence_source_lease_deadline_ms,
            latest_metadata_transfer_fence_source_lease_deadline_ms,
            applied_membership_log_id,
            applied_voters,
            applied_learners,
        ) = self
            .raft
            .with_state_machine(|state_machine| {
                let last_applied = state_machine.last_applied();
                let current_snapshot = state_machine
                    .current_snapshot()
                    .and_then(|snapshot| snapshot.meta.last_log_id);
                let snapshot = state_machine.inner().snapshot();
                let durable_timestamp_high_water_ms = snapshot.max_committed_timestamp_ms();
                let authority_incarnation = snapshot.authority_incarnation();
                let current_cluster_epoch = snapshot.cluster_epoch();
                let retained_history_count = snapshot.cluster_map_history().len();
                let oldest_retained_history_epoch = snapshot
                    .cluster_map_history()
                    .first()
                    .map(|record| record.cluster_epoch());
                let newest_retained_history_epoch = snapshot
                    .cluster_map_history()
                    .last()
                    .map(|record| record.cluster_epoch());
                let oldest_storage_history_floor_epoch = snapshot
                    .nodes()
                    .filter_map(|node| node.cluster_map_history_floor_epoch())
                    .min();
                let mut storage_node_lease_deadline_count = 0;
                let mut earliest_storage_node_lease_deadline_ms = None;
                let mut latest_storage_node_lease_deadline_ms = None;
                let mut storage_node_count = 0;
                let mut joining_storage_node_count = 0;
                let mut active_storage_node_count = 0;
                let mut draining_storage_node_count = 0;
                let mut out_storage_node_count = 0;
                let mut removed_storage_node_count = 0;
                let mut healthy_storage_node_count = 0;
                let mut suspect_storage_node_count = 0;
                let mut unavailable_storage_node_count = 0;
                for node in snapshot.nodes() {
                    storage_node_count += 1;
                    match node.membership() {
                        NodeMembershipState::Joining => joining_storage_node_count += 1,
                        NodeMembershipState::Active => active_storage_node_count += 1,
                        NodeMembershipState::Draining => draining_storage_node_count += 1,
                        NodeMembershipState::Out => out_storage_node_count += 1,
                        NodeMembershipState::Removed => removed_storage_node_count += 1,
                    }
                    match node.availability() {
                        NodeAvailabilityState::Healthy => healthy_storage_node_count += 1,
                        NodeAvailabilityState::Suspect => suspect_storage_node_count += 1,
                        NodeAvailabilityState::Unavailable => unavailable_storage_node_count += 1,
                    }
                    if let Some(deadline_ms) = node.lease_deadline_ms() {
                        observe_deadline_range(
                            deadline_ms,
                            &mut storage_node_lease_deadline_count,
                            &mut earliest_storage_node_lease_deadline_ms,
                            &mut latest_storage_node_lease_deadline_ms,
                        );
                    }
                }
                let mut pg_count = 0;
                let mut active_pg_count = 0;
                let mut peering_pg_count = 0;
                let mut degraded_pg_count = 0;
                let mut backfilling_pg_count = 0;
                let mut inconsistent_pg_count = 0;
                let mut active_primary_pg_count = 0;
                let mut peering_metadata_transfer_pg_count = 0;
                let mut metadata_transfer_fenced_pg_count = 0;
                let mut metadata_transfer_fence_source_lease_deadline_count = 0;
                let mut earliest_metadata_transfer_fence_source_lease_deadline_ms = None;
                let mut latest_metadata_transfer_fence_source_lease_deadline_ms = None;
                for pg in snapshot.pgs() {
                    pg_count += 1;
                    match pg.state() {
                        PgState::Active => active_pg_count += 1,
                        PgState::Peering => peering_pg_count += 1,
                        PgState::Degraded => degraded_pg_count += 1,
                        PgState::Backfilling => backfilling_pg_count += 1,
                        PgState::Inconsistent => inconsistent_pg_count += 1,
                    }
                    if pg.active_primary().is_some() {
                        active_primary_pg_count += 1;
                    }
                    if pg.peering_metadata_transfer().is_some() {
                        peering_metadata_transfer_pg_count += 1;
                    }
                    if pg.metadata_transfer_fenced() {
                        metadata_transfer_fenced_pg_count += 1;
                    }
                    if let Some(deadline_ms) = pg.metadata_transfer_fence_source_lease_deadline_ms()
                    {
                        observe_deadline_range(
                            deadline_ms,
                            &mut metadata_transfer_fence_source_lease_deadline_count,
                            &mut earliest_metadata_transfer_fence_source_lease_deadline_ms,
                            &mut latest_metadata_transfer_fence_source_lease_deadline_ms,
                        );
                    }
                }
                let membership = state_machine.last_membership();
                let membership_log_id = *membership.log_id();
                let voters = membership.membership().voter_ids().collect::<BTreeSet<_>>();
                let learners = membership
                    .membership()
                    .learner_ids()
                    .collect::<BTreeSet<_>>();
                Box::pin(async move {
                    (
                        last_applied,
                        current_snapshot,
                        durable_timestamp_high_water_ms,
                        authority_incarnation,
                        current_cluster_epoch,
                        retained_history_count,
                        oldest_retained_history_epoch,
                        newest_retained_history_epoch,
                        oldest_storage_history_floor_epoch,
                        storage_node_lease_deadline_count,
                        earliest_storage_node_lease_deadline_ms,
                        latest_storage_node_lease_deadline_ms,
                        storage_node_count,
                        joining_storage_node_count,
                        active_storage_node_count,
                        draining_storage_node_count,
                        out_storage_node_count,
                        removed_storage_node_count,
                        healthy_storage_node_count,
                        suspect_storage_node_count,
                        unavailable_storage_node_count,
                        pg_count,
                        active_pg_count,
                        peering_pg_count,
                        degraded_pg_count,
                        backfilling_pg_count,
                        inconsistent_pg_count,
                        active_primary_pg_count,
                        peering_metadata_transfer_pg_count,
                        metadata_transfer_fenced_pg_count,
                        metadata_transfer_fence_source_lease_deadline_count,
                        earliest_metadata_transfer_fence_source_lease_deadline_ms,
                        latest_metadata_transfer_fence_source_lease_deadline_ms,
                        membership_log_id,
                        voters,
                        learners,
                    )
                })
            })
            .await
            .map_err(|error| openraft_remote_error("status state-machine read", error))?;
        let local_leader = server_state == ServerState::Leader && current_leader == Some(node_id);
        let effective_voter = effective_voters.contains(&node_id);
        let effective_learner = effective_learners.contains(&node_id);
        let applied_voter = applied_voters.contains(&node_id);
        let applied_learner = applied_learners.contains(&node_id);
        Ok(ControlPlaneRaftAuthorityStatus {
            node_id,
            current_leader,
            server_state,
            local_leader,
            effective_voter,
            effective_learner,
            applied_voter,
            applied_learner,
            persisted_vote,
            current_term,
            last_log_id,
            last_purged_log_id,
            committed,
            applied,
            current_snapshot,
            durable_wal_backed,
            durable_wal_offsets,
            durable_wal_poisoned,
            durable_last_vote,
            durable_last_log_id,
            durable_last_purged_log_id,
            durable_committed,
            durable_applied: applied,
            durable_timestamp_high_water_ms,
            authority_incarnation,
            current_cluster_epoch,
            retained_history_count,
            oldest_retained_history_epoch,
            newest_retained_history_epoch,
            oldest_storage_history_floor_epoch,
            storage_node_lease_deadline_count,
            earliest_storage_node_lease_deadline_ms,
            latest_storage_node_lease_deadline_ms,
            storage_node_count,
            joining_storage_node_count,
            active_storage_node_count,
            draining_storage_node_count,
            out_storage_node_count,
            removed_storage_node_count,
            healthy_storage_node_count,
            suspect_storage_node_count,
            unavailable_storage_node_count,
            pg_count,
            active_pg_count,
            peering_pg_count,
            degraded_pg_count,
            backfilling_pg_count,
            inconsistent_pg_count,
            active_primary_pg_count,
            peering_metadata_transfer_pg_count,
            metadata_transfer_fenced_pg_count,
            metadata_transfer_fence_source_lease_deadline_count,
            earliest_metadata_transfer_fence_source_lease_deadline_ms,
            latest_metadata_transfer_fence_source_lease_deadline_ms,
            effective_membership_log_id,
            effective_voters,
            effective_learners,
            applied_membership_log_id,
            applied_voters,
            applied_learners,
        })
    }

    pub async fn shutdown(&self) -> Result<(), ControlPlaneError> {
        self.raft
            .shutdown()
            .await
            .map_err(|error| openraft_remote_error("shutdown", error))
    }
}

fn classify_configured_membership_initialization(
    result: Result<
        (),
        RaftError<ControlPlaneRaftTypeConfig, InitializeError<ControlPlaneRaftTypeConfig>>,
    >,
) -> Result<bool, ControlPlaneError> {
    match result {
        Ok(()) => Ok(true),
        // Raft state may advance after is_initialized() and before initialize() is
        // handled. OpenRaft documents NotAllowed as safe to ignore in this race:
        // cluster formation is already in motion, and startup must wait for the
        // configured membership instead of terminating the process.
        Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(false),
        Err(RaftError::APIError(InitializeError::NotInMembers(error))) => {
            Err(ControlPlaneError::static_topology_failure(format!(
                "configured OpenRaft initial membership rejected the local node: {error}"
            )))
        }
        Err(RaftError::Fatal(error)) => Err(openraft_remote_error("initialize", error)),
    }
}

impl ControlPlaneRaftLinearizedCommandSink for ControlPlaneRaftAuthority {
    fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftFuture<'_, Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>>
    {
        Box::pin(async move {
            ControlPlaneRaftAuthority::submit_control_plane_command(self, command).await
        })
    }
}

impl ControlPlaneRaftLinearizedRuntimeMapSource for ControlPlaneRaftAuthority {
    fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> ControlPlaneRaftFuture<'_, Result<ClusterRuntimeMapSnapshot, ControlPlaneError>> {
        Box::pin(async move {
            ControlPlaneRaftAuthority::linearized_runtime_map_snapshot(self, issued_at_ms).await
        })
    }
}

impl ControlPlaneRaftAuthorityStatusSource for ControlPlaneRaftAuthority {
    fn status(
        &self,
    ) -> ControlPlaneRaftFuture<'_, Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError>>
    {
        Box::pin(async move { ControlPlaneRaftAuthority::status(self).await })
    }
}

impl ControlPlaneRaftLeaderRoutedAdmin for ControlPlaneRaftAuthority {
    fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        Box::pin(async move {
            ControlPlaneRaftAuthority::replace_voters(
                self,
                voters,
                retain_removed_voters_as_learners,
            )
            .await
        })
    }

    fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> ControlPlaneRaftFuture<'_, Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError>>
    {
        Box::pin(async move {
            ControlPlaneRaftAuthority::add_learner(self, node_id, node, wait_for_catch_up).await
        })
    }

    fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(
            async move { ControlPlaneRaftAuthority::transfer_leadership_to(self, node_id).await },
        )
    }
}

impl ControlPlaneRaftAuthorityBootstrap for ControlPlaneRaftAuthority {
    fn initialize_membership(
        &self,
        nodes: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(async move { ControlPlaneRaftAuthority::initialize_membership(self, nodes).await })
    }

    fn is_initialized(&self) -> ControlPlaneRaftFuture<'_, Result<bool, ControlPlaneError>> {
        Box::pin(async move { ControlPlaneRaftAuthority::is_initialized(self).await })
    }
}

impl ControlPlaneRaftAuthorityNodeLifecycle for ControlPlaneRaftAuthority {
    fn wait_for_applied_index_at_least(
        &self,
        index: u64,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(async move {
            ControlPlaneRaftAuthority::wait_for_applied_index_at_least(
                self, index, timeout, message,
            )
            .await
        })
    }

    fn wait_for_applied_log_id(
        &self,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(async move {
            ControlPlaneRaftAuthority::wait_for_applied_log_id(self, log_id, timeout, message).await
        })
    }

    fn wait_for_current_leader(
        &self,
        leader_id: ControlPlaneRaftNodeId,
        timeout: Duration,
        message: &'static str,
    ) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(async move {
            ControlPlaneRaftAuthority::wait_for_current_leader(self, leader_id, timeout, message)
                .await
        })
    }

    fn shutdown(&self) -> ControlPlaneRaftFuture<'_, Result<(), ControlPlaneError>> {
        Box::pin(async move { ControlPlaneRaftAuthority::shutdown(self).await })
    }
}

openraft::declare_raft_types!(
    pub ControlPlaneRaftTypeConfig:
        D = ControlPlaneCommand,
        R = ControlPlaneRaftApplyResponse,
        NodeId = ControlPlaneRaftNodeId,
        Node = BasicNode,
        Term = ControlPlaneRaftTerm,
        LeaderId = ControlPlaneRaftLeaderId,
        Vote = Vote<ControlPlaneRaftLeaderId>,
        Entry = ControlPlaneRaftEntry,
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneRaftSnapshotData {
    payload: Arc<Vec<u8>>,
}

impl ControlPlaneRaftSnapshotData {
    #[must_use]
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            payload: Arc::new(payload),
        }
    }

    #[must_use]
    pub fn get_ref(&self) -> &Vec<u8> {
        &self.payload
    }

    #[must_use]
    pub fn into_inner(self) -> Vec<u8> {
        Arc::try_unwrap(self.payload).unwrap_or_else(|payload| (*payload).clone())
    }
}

pub type ControlPlaneRaftSnapshot =
    SnapshotOf<ControlPlaneRaftTypeConfig, ControlPlaneRaftSnapshotData>;

#[must_use]
pub fn raft_node_id_from_storage_node_id(node_id: NodeId) -> ControlPlaneRaftNodeId {
    u64::from(node_id.as_u32())
}

#[must_use]
pub fn storage_node_id_from_raft_node_id(node_id: ControlPlaneRaftNodeId) -> Option<NodeId> {
    let node_id = u32::try_from(node_id).ok()?;
    Some(NodeId::new(node_id))
}

#[must_use]
pub fn control_plane_log_id_from_raft(
    log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
) -> Option<ControlPlaneLogId> {
    ControlPlaneLogId::new(log_id.committed_leader_id().term, log_id.index())
}

#[must_use]
pub fn raft_log_id_from_control_plane(
    leader_node_id: ControlPlaneRaftNodeId,
    log_id: ControlPlaneLogId,
) -> LogIdOf<ControlPlaneRaftTypeConfig> {
    LogId::new(
        LeaderId {
            term: log_id.term(),
            node_id: leader_node_id,
        },
        log_id.index(),
    )
}

pub fn assert_openraft_type_config() {
    fn assert_config<C: RaftTypeConfig>() {}
    assert_config::<ControlPlaneRaftTypeConfig>();
}

fn submitted_control_plane_command(
    response: ClientWriteResponse<ControlPlaneRaftTypeConfig>,
) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError> {
    let outcome = match response.data {
        ControlPlaneRaftApplyResponse::Applied(response) => {
            ControlPlaneRaftCommandOutcome::Applied(response)
        }
        ControlPlaneRaftApplyResponse::Rejected(error) => {
            ControlPlaneRaftCommandOutcome::Rejected(error)
        }
        ControlPlaneRaftApplyResponse::Blank | ControlPlaneRaftApplyResponse::Membership => {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "OpenRaft client-write for control-plane command returned non-command response at {}",
                    response.log_id
                ),
            });
        }
    };
    Ok(SubmittedControlPlaneRaftCommand {
        log_id: response.log_id,
        outcome,
    })
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("control-plane command is not accepted by the current replication policy")]
pub struct ControlPlaneCommandReplicationSafetyError {
    _private: (),
}

pub fn validate_control_plane_command_replication_size(
    command: &ControlPlaneCommand,
) -> Result<(), ControlPlaneCommandReplicationSafetyError> {
    validate_control_plane_command_replication_size_detailed(command)
        .map(|_| ())
        .map_err(|_| ControlPlaneCommandReplicationSafetyError { _private: () })
}

fn validate_control_plane_command_replication_size_detailed(
    command: &ControlPlaneCommand,
) -> Result<usize, ControlPlaneError> {
    let encoded_len = control_plane_command_replication_encoded_len(command)?;
    if encoded_len <= CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES {
        return Ok(encoded_len);
    }
    Err(ControlPlaneError::rpc_protocol(format!(
            "control-plane command encodes to {encoded_len} OpenRaft entry bytes, exceeding the replication-safe per-entry limit {CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES}"
        )))
}

pub(crate) fn control_plane_command_replication_encoded_len(
    command: &ControlPlaneCommand,
) -> Result<usize, ControlPlaneError> {
    let entry = ControlPlaneRaftEntry {
        log_id: LogId::new(
            LeaderId {
                term: u64::MAX,
                node_id: u64::MAX,
            },
            u64::MAX,
        ),
        payload: EntryPayload::Normal(command.clone()),
    };
    let mut encoded = Vec::new();
    write_raft_entry_with_command_encoder(
        &mut encoded,
        &entry,
        encode_control_plane_command_without_replication_limit,
    )?;
    Ok(encoded.len())
}

pub async fn runtime_map_via_openraft_read_index(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    issued_at_ms: u64,
) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
    let (snapshot, applied_log_id) = control_plane_snapshot_via_openraft_read_index(raft).await?;
    let read_index = control_plane_log_id_from_raft(applied_log_id).ok_or_else(|| {
        ControlPlaneError::CommandDecode {
            message: format!("invalid OpenRaft applied log id for runtime map: {applied_log_id}"),
        }
    })?;
    snapshot.runtime_map_with_freshness_proof(
        issued_at_ms,
        RuntimeMapFreshnessProof::ReadIndex {
            authority_incarnation: snapshot.authority_incarnation(),
            read_index,
            issued_at_ms,
        },
    )
}

async fn control_plane_snapshot_via_openraft_read_index(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
) -> Result<(ClusterControlSnapshot, LogIdOf<ControlPlaneRaftTypeConfig>), ControlPlaneError> {
    let read_log_id = control_plane_read_index_via_openraft(raft).await?;
    let read_index = control_plane_log_id_from_raft(read_log_id).ok_or_else(|| {
        ControlPlaneError::CommandDecode {
            message: format!("invalid OpenRaft read-index log id for runtime map: {read_log_id}"),
        }
    })?;

    raft.with_state_machine(move |state_machine| {
        Box::pin(async move {
            let Some(last_applied) = state_machine.last_applied() else {
                return Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                    read_index,
                    last_applied: state_machine.inner().last_applied(),
                });
            };
            if last_applied < read_log_id {
                return Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                    read_index,
                    last_applied: state_machine.inner().last_applied(),
                });
            }
            Ok((state_machine.inner().snapshot().clone(), last_applied))
        })
    })
    .await
    .map_err(|error| openraft_remote_error("state-machine read", error))?
}

async fn control_plane_read_index_via_openraft(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
    raft.ensure_linearizable(ReadPolicy::ReadIndex)
        .await
        .map(|read_index| *read_index.log_id())
        .map_err(|error| openraft_linearizable_read_error("read-index", error))
}

fn control_plane_error_to_io_error(context: &'static str, error: ControlPlaneError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{context}: {error}"))
}

fn openraft_remote_error(context: &'static str, error: impl fmt::Display) -> ControlPlaneError {
    ControlPlaneError::rpc_remote(format!("OpenRaft {context} failed: {error}"))
}

fn openraft_linearizable_read_error(
    context: &'static str,
    error: RaftError<ControlPlaneRaftTypeConfig, LinearizableReadError<ControlPlaneRaftTypeConfig>>,
) -> ControlPlaneError {
    let kind = match &error {
        RaftError::APIError(LinearizableReadError::ForwardToLeader(_)) => {
            ControlPlaneRaftOperationErrorKind::ForwardToLeader
        }
        RaftError::APIError(LinearizableReadError::QuorumNotEnough(_)) => {
            ControlPlaneRaftOperationErrorKind::QuorumNotEnough
        }
        RaftError::Fatal(_) => ControlPlaneRaftOperationErrorKind::Fatal,
    };
    ControlPlaneError::OpenRaftOperation {
        kind,
        message: format!("OpenRaft {context} failed: {error}"),
    }
}

fn openraft_client_write_error(
    context: &'static str,
    error: RaftError<ControlPlaneRaftTypeConfig, ClientWriteError<ControlPlaneRaftTypeConfig>>,
) -> ControlPlaneError {
    let kind = match &error {
        RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => {
            ControlPlaneRaftOperationErrorKind::ForwardToLeader
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(_)) => {
            ControlPlaneRaftOperationErrorKind::Rejected
        }
        RaftError::Fatal(_) => ControlPlaneRaftOperationErrorKind::Fatal,
    };
    ControlPlaneError::OpenRaftOperation {
        kind,
        message: format!("OpenRaft {context} failed: {error}"),
    }
}

fn raft_log_store_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn observe_deadline_range(
    deadline_ms: u64,
    count: &mut usize,
    earliest_ms: &mut Option<u64>,
    latest_ms: &mut Option<u64>,
) {
    *count += 1;
    *earliest_ms = Some(earliest_ms.map_or(deadline_ms, |existing| existing.min(deadline_ms)));
    *latest_ms = Some(latest_ms.map_or(deadline_ms, |existing| existing.max(deadline_ms)));
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftLogStore {
    inner: Arc<Mutex<ControlPlaneRaftLogStoreInner>>,
    durable: Arc<Mutex<ControlPlaneRaftLogStoreDurableState>>,
    wal: Option<Arc<ControlPlaneRaftWalFile>>,
    durability_lane: Option<Arc<ControlPlaneRaftDurabilityLane>>,
}

#[cfg(test)]
static CONTROL_PLANE_RAFT_WAL_FAIL_NEXT_FILE_SYNC: Mutex<Option<PathBuf>> = Mutex::new(None);

#[cfg(test)]
static CONTROL_PLANE_RAFT_WAL_FAIL_NEXT_PARENT_SYNC: Mutex<Option<PathBuf>> = Mutex::new(None);

#[cfg(test)]
#[derive(Debug, Default)]
struct ControlPlaneRaftWalFileSyncGateState {
    entered: bool,
    released: bool,
}

#[cfg(test)]
type ControlPlaneRaftWalFileSyncGate = Arc<(
    Mutex<ControlPlaneRaftWalFileSyncGateState>,
    std::sync::Condvar,
)>;

#[cfg(test)]
static CONTROL_PLANE_RAFT_WAL_FILE_SYNC_GATES: Mutex<
    BTreeMap<PathBuf, ControlPlaneRaftWalFileSyncGate>,
> = Mutex::new(BTreeMap::new());

#[cfg(test)]
static CONTROL_PLANE_RAFT_WAL_DURABLE_PUBLICATION_GATES: Mutex<
    BTreeMap<PathBuf, ControlPlaneRaftWalFileSyncGate>,
> = Mutex::new(BTreeMap::new());

#[derive(Debug, Clone, Default, PartialEq)]
struct ControlPlaneRaftLogStoreRestartArtifact {
    vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
    committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    entries: Vec<ControlPlaneRaftEntry>,
}

#[derive(Debug, Clone)]
struct ControlPlaneRaftRestartArtifact {
    cluster_name: String,
    local_node_id: ControlPlaneRaftNodeId,
    wal_replay_offset: u64,
    log_store: ControlPlaneRaftLogStoreRestartArtifact,
    state_machine: ControlPlaneRaftStateMachineRestartArtifact,
}

pub(crate) struct ControlPlaneRaftCapturedRestartCheckpoint {
    artifact: ControlPlaneRaftRestartArtifact,
    authority_instance_id: ControlPlaneRaftAuthorityInstanceId,
}

/// Opaque proof that this authority captured, certified, and durably published
/// a restart checkpoint against its retained static peer policy.
pub(crate) struct ControlPlaneRaftStaticIdentityCheckpointPublication {
    authority_clock_binding: ControlPlaneAuthorityClockCheckpointBinding,
    committed_timestamp_high_water_ms: Option<u64>,
}

impl ControlPlaneRaftStaticIdentityCheckpointPublication {
    #[must_use]
    pub(crate) fn authority_clock_binding(&self) -> ControlPlaneAuthorityClockCheckpointBinding {
        self.authority_clock_binding
    }

    #[must_use]
    pub(crate) fn committed_timestamp_high_water_ms(&self) -> Option<u64> {
        self.committed_timestamp_high_water_ms
    }
}

impl fmt::Debug for ControlPlaneRaftStaticIdentityCheckpointPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftStaticIdentityCheckpointPublication")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPlaneRaftEstablishedPeerPolicyConvergence {
    Converged,
    EffectiveMembershipPending {
        applied: ControlPlaneRaftLogId,
        effective: ControlPlaneRaftLogId,
    },
    AppliedStatePending {
        applied: Option<ControlPlaneRaftLogId>,
        committed: Option<ControlPlaneRaftLogId>,
    },
}

impl ControlPlaneRaftCapturedRestartCheckpoint {
    fn established_peer_policy_convergence(
        &self,
        peer_policy: &ControlPlaneRaftPeerTransportPolicy,
    ) -> Result<ControlPlaneRaftEstablishedPeerPolicyConvergence, ControlPlaneError> {
        self.artifact
            .validate_cluster_identity(peer_policy.cluster_name())?;
        peer_policy.validate_local_node(self.artifact.local_node_id)?;
        self.artifact.validate_peer_policy_membership(peer_policy)?;
        validate_captured_static_initial_topology(
            self.artifact.state_machine.inner.snapshot(),
            peer_policy,
        )?;
        let applied_membership_log_id = (*self.artifact.state_machine.last_membership.log_id())
            .ok_or_else(|| {
                raft_artifact_protocol_error(
                    "captured OpenRaft restart checkpoint has no applied membership",
                )
            })?;
        let effective_membership_log_id = self
            .artifact
            .log_store
            .entries
            .iter()
            .filter(|entry| matches!(&entry.payload, EntryPayload::Membership(_)))
            .map(|entry| entry.log_id)
            .fold(applied_membership_log_id, |effective, candidate| {
                if candidate.index > effective.index {
                    candidate
                } else {
                    effective
                }
            });
        if effective_membership_log_id != applied_membership_log_id {
            return Ok(
                ControlPlaneRaftEstablishedPeerPolicyConvergence::EffectiveMembershipPending {
                    applied: applied_membership_log_id,
                    effective: effective_membership_log_id,
                },
            );
        }
        let applied = self.artifact.state_machine.last_applied;
        let committed = self.artifact.log_store.committed;
        if committed != applied {
            let committed_is_ahead = match (applied, committed) {
                (None, Some(_)) => true,
                (Some(applied), Some(committed)) => committed.index > applied.index,
                _ => false,
            };
            if committed_is_ahead {
                return Ok(
                    ControlPlaneRaftEstablishedPeerPolicyConvergence::AppliedStatePending {
                        applied,
                        committed,
                    },
                );
            }
            return Err(raft_artifact_protocol_error(format!(
                "captured OpenRaft restart checkpoint has non-forward applied/committed mismatch: applied={applied:?} committed={committed:?}"
            )));
        }
        Ok(ControlPlaneRaftEstablishedPeerPolicyConvergence::Converged)
    }

    #[cfg(test)]
    #[must_use]
    fn wal_replay_offset(&self) -> u64 {
        self.artifact.wal_replay_offset
    }
}

fn validate_captured_static_initial_topology(
    snapshot: &ClusterControlSnapshot,
    peer_policy: &ControlPlaneRaftPeerTransportPolicy,
) -> Result<(), ControlPlaneError> {
    let Some(expected_topology) = peer_policy.topology_identity() else {
        return Ok(());
    };
    let actual = snapshot.initial_topology().ok_or_else(|| {
        raft_artifact_protocol_error(
            "captured static OpenRaft restart checkpoint has no initial topology certificate",
        )
    })?;
    if let Some(expected_certificate) = peer_policy.initial_topology_certificate() {
        if actual != expected_certificate {
            return Err(raft_artifact_protocol_error(
                "captured static OpenRaft restart checkpoint initial topology certificate does not match the configured certificate",
            ));
        }
    }
    let actual_digest = actual
        .topology_digest()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let expected_voters = peer_policy.peers().keys().copied().collect::<Vec<_>>();
    if actual.topology_generation() != expected_topology.generation
        || actual_digest != expected_topology.digest
        || actual.raft_voters() != expected_voters
    {
        return Err(raft_artifact_protocol_error(format!(
            "captured static OpenRaft restart checkpoint initial topology does not match peer policy; expected_generation={} actual_generation={} expected_digest={} actual_digest={} expected_voters={expected_voters:?} actual_voters={:?}",
            expected_topology.generation,
            actual.topology_generation(),
            expected_topology.digest,
            actual_digest,
            actual.raft_voters()
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ControlPlaneRaftCheckpointPosition {
    wal_replay_offset: u64,
    last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
}

impl ControlPlaneRaftCheckpointPosition {
    fn for_artifact(artifact: &ControlPlaneRaftRestartArtifact) -> Self {
        Self {
            wal_replay_offset: artifact.wal_replay_offset,
            last_applied: artifact.state_machine.last_applied,
        }
    }

    fn validate_at_or_after(self, previous: Self) -> Result<(), ControlPlaneError> {
        if self.wal_replay_offset < previous.wal_replay_offset {
            return Err(ControlPlaneError::rpc_remote(format!(
                    "captured OpenRaft restart checkpoint WAL offset {} precedes the last publication offset {}",
                    self.wal_replay_offset, previous.wal_replay_offset
                )));
        }
        match (previous.last_applied, self.last_applied) {
            (Some(previous), None) => Err(ControlPlaneError::rpc_remote(format!(
                    "captured OpenRaft restart checkpoint has no applied log ID after publishing {previous}"
                ))),
            (Some(previous), Some(candidate)) if candidate.index < previous.index => {
                Err(ControlPlaneError::rpc_remote(format!(
                        "captured OpenRaft restart checkpoint applied index {} precedes the last publication index {}",
                        candidate.index, previous.index
                    )))
            }
            (Some(previous), Some(candidate))
                if candidate.index == previous.index && candidate != previous =>
            {
                Err(ControlPlaneError::rpc_remote(format!(
                        "captured OpenRaft restart checkpoint applied log ID {candidate} conflicts with the last publication log ID {previous} at the same index"
                    )))
            }
            (Some(previous), Some(candidate))
                if candidate.index > previous.index
                    && candidate.leader_id.term < previous.leader_id.term =>
            {
                Err(ControlPlaneError::rpc_remote(format!(
                        "captured OpenRaft restart checkpoint applied term {} regresses from the last publication term {}",
                        candidate.leader_id.term, previous.leader_id.term
                    )))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ControlPlaneRaftWalRecord {
    SaveVote(VoteOf<ControlPlaneRaftTypeConfig>),
    Append(Vec<ControlPlaneRaftEntry>),
    SaveCommitted(Option<LogIdOf<ControlPlaneRaftTypeConfig>>),
    TruncateAfter(Option<LogIdOf<ControlPlaneRaftTypeConfig>>),
    Purge(LogIdOf<ControlPlaneRaftTypeConfig>),
}

macro_rules! define_control_plane_raft_wal_record_kinds {
    ($($kind:ident = $tag:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum ControlPlaneRaftWalRecordKind {
            $($kind),+
        }

        impl ControlPlaneRaftWalRecordKind {
            #[cfg(test)]
            const ALL: &'static [Self] = &[$(Self::$kind),+];

            fn as_u8(self) -> u8 {
                match self {
                    $(Self::$kind => $tag),+
                }
            }

            fn from_u8(value: u8) -> Result<Self, u8> {
                match value {
                    $($tag => Ok(Self::$kind)),+,
                    value => Err(value),
                }
            }
        }
    };
}

define_control_plane_raft_wal_record_kinds!(
    SaveVote = 1,
    Append = 2,
    SaveCommitted = 3,
    TruncateAfter = 4,
    Purge = 5,
);

impl ControlPlaneRaftWalRecord {
    fn kind(&self) -> ControlPlaneRaftWalRecordKind {
        match self {
            Self::SaveVote(_) => ControlPlaneRaftWalRecordKind::SaveVote,
            Self::Append(_) => ControlPlaneRaftWalRecordKind::Append,
            Self::SaveCommitted(_) => ControlPlaneRaftWalRecordKind::SaveCommitted,
            Self::TruncateAfter(_) => ControlPlaneRaftWalRecordKind::TruncateAfter,
            Self::Purge(_) => ControlPlaneRaftWalRecordKind::Purge,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ControlPlaneRaftWalFrame {
    cluster_name: String,
    local_node_id: ControlPlaneRaftNodeId,
    record: ControlPlaneRaftWalRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPlaneRaftWalFrameFormatError {
    Truncated,
    ChecksumMismatch { expected: u64, actual: u64 },
    UnknownMagic,
    UnsupportedVersion(u16),
}

impl std::fmt::Display for ControlPlaneRaftWalFrameFormatError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => {
                formatter.write_str("truncated control-plane OpenRaft WAL frame")
            }
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "control-plane OpenRaft WAL frame checksum mismatch: expected {expected:#x}, actual {actual:#x}"
            ),
            Self::UnknownMagic => {
                formatter.write_str("invalid control-plane OpenRaft WAL frame magic")
            }
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "unsupported control-plane OpenRaft WAL frame version {version}"
            ),
        }
    }
}

#[derive(Debug)]
enum ControlPlaneRaftWalFrameDecodeError {
    Format(ControlPlaneRaftWalFrameFormatError),
    Invalid(ControlPlaneError),
}

impl ControlPlaneRaftWalFrameDecodeError {
    fn into_control_plane_error(self) -> ControlPlaneError {
        match self {
            Self::Format(error) => raft_artifact_protocol_error(error.to_string()),
            Self::Invalid(error) => error,
        }
    }
}

#[cfg(test)]
std::thread_local! {
    static CONTROL_PLANE_RAFT_WAL_REPLAY_ATTEMPTS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
    static CONTROL_PLANE_RAFT_RESTORE_ATTEMPTS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
fn reset_control_plane_raft_wal_replay_attempts() {
    CONTROL_PLANE_RAFT_WAL_REPLAY_ATTEMPTS.set(0);
}

#[cfg(test)]
fn control_plane_raft_wal_replay_attempts() -> usize {
    CONTROL_PLANE_RAFT_WAL_REPLAY_ATTEMPTS.get()
}

#[cfg(test)]
fn reset_control_plane_raft_restore_attempts() {
    CONTROL_PLANE_RAFT_RESTORE_ATTEMPTS.set(0);
}

#[cfg(test)]
fn control_plane_raft_restore_attempts() -> usize {
    CONTROL_PLANE_RAFT_RESTORE_ATTEMPTS.get()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ControlPlaneRaftWalFileConfig {
    path: PathBuf,
    cluster_name: String,
    local_node_id: ControlPlaneRaftNodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneRaftWalOffsets {
    base_offset: u64,
    clean_len: u64,
}

impl ControlPlaneRaftWalOffsets {
    #[must_use]
    pub fn base_offset(self) -> u64 {
        self.base_offset
    }

    #[must_use]
    pub fn clean_len(self) -> u64 {
        self.clean_len
    }
}

#[derive(Debug, Clone, Copy)]
struct ControlPlaneRaftWalReplayConfig<'a> {
    base: &'a ControlPlaneRaftLogStoreRestartArtifact,
    replay_offset: u64,
}

#[derive(Debug, Clone)]
struct ControlPlaneRaftWalFile {
    cluster_name: String,
    local_node_id: ControlPlaneRaftNodeId,
    metrics: Arc<ControlPlaneRaftWalMetrics>,
    journal: DurableJournalFile<ControlPlaneRaftWalObserver>,
}

type ControlPlaneRaftWalAppendError = DurableJournalAppendError;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ControlPlaneRaftRestartSentinel {
    cluster_name: String,
    local_node_id: ControlPlaneRaftNodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPlaneRaftRestartArtifactFormatError {
    Truncated,
    ChecksumMismatch { expected: u64, actual: u64 },
    UnknownMagic,
    UnsupportedVersion(u16),
}

impl std::fmt::Display for ControlPlaneRaftRestartArtifactFormatError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => formatter.write_str(
                "truncated control-plane OpenRaft durable restart artifact",
            ),
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "control-plane OpenRaft durable restart artifact checksum mismatch: expected {expected:#x}, actual {actual:#x}"
            ),
            Self::UnknownMagic => formatter.write_str(
                "invalid control-plane OpenRaft durable restart artifact magic",
            ),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "unsupported control-plane OpenRaft durable restart artifact version {version}"
            ),
        }
    }
}

#[derive(Debug)]
enum ControlPlaneRaftRestartArtifactDecodeError {
    Format(ControlPlaneRaftRestartArtifactFormatError),
    Invalid(ControlPlaneError),
}

impl ControlPlaneRaftRestartArtifactDecodeError {
    fn into_control_plane_error(self) -> ControlPlaneError {
        match self {
            Self::Format(error) => raft_artifact_protocol_error(error.to_string()),
            Self::Invalid(error) => error,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPlaneRaftRestartSentinelFormatError {
    Truncated,
    ChecksumMismatch { expected: u64, actual: u64 },
    UnknownMagic,
    UnsupportedVersion(u16),
    InvalidClusterName,
    TrailingBytes,
}

impl std::fmt::Display for ControlPlaneRaftRestartSentinelFormatError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => formatter.write_str(
                "truncated control-plane OpenRaft durable restart sentinel",
            ),
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "control-plane OpenRaft durable restart sentinel checksum mismatch: expected {expected:#x}, actual {actual:#x}"
            ),
            Self::UnknownMagic => formatter.write_str(
                "invalid control-plane OpenRaft durable restart sentinel magic",
            ),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "unsupported control-plane OpenRaft durable restart sentinel version {version}"
            ),
            Self::InvalidClusterName => formatter.write_str(
                "control-plane OpenRaft durable restart sentinel cluster name is not UTF-8",
            ),
            Self::TrailingBytes => formatter.write_str(
                "control-plane OpenRaft durable restart sentinel has trailing bytes",
            ),
        }
    }
}

fn read_control_plane_raft_restart_sentinel_bytes<'a>(
    body: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], ControlPlaneRaftRestartSentinelFormatError> {
    let end = offset
        .checked_add(len)
        .ok_or(ControlPlaneRaftRestartSentinelFormatError::Truncated)?;
    let value = body
        .get(*offset..end)
        .ok_or(ControlPlaneRaftRestartSentinelFormatError::Truncated)?;
    *offset = end;
    Ok(value)
}

const CONTROL_PLANE_RAFT_RESTART_MAGIC: &[u8] = b"ARGMINCPRAFT";
const CONTROL_PLANE_RAFT_RESTART_VERSION: u16 = 5;
const CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN: usize = 8;
const CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC: &[u8] = b"ARGMINCPRAFTSEEN";
const CONTROL_PLANE_RAFT_RESTART_SENTINEL_VERSION: u16 = 1;
// Bound actual inconsistent captures rather than elapsed time. A process may be descheduled after
// one failed capture; that must not turn a still-untried second capture into a durability failure.
const CONTROL_PLANE_RAFT_RESTART_CAPTURE_MAX_ATTEMPTS: u64 = 1_024;
const CONTROL_PLANE_RAFT_RESTART_CAPTURE_RETRY_DELAY: Duration = Duration::from_millis(1);
const CONTROL_PLANE_RAFT_WAL_MAGIC: &[u8] = b"ARGMINCPRAFTWAL";
const CONTROL_PLANE_RAFT_WAL_VERSION: u16 = 1;
const CONTROL_PLANE_RAFT_WAL_CHECKSUM_LEN: usize = 8;
const CONTROL_PLANE_RAFT_WAL_FILE_MAGIC: &[u8] = b"ARGMINCPRAFTWALFILE";
const CONTROL_PLANE_RAFT_WAL_FILE_VERSION: u16 = 2;
#[cfg(test)]
const CONTROL_PLANE_RAFT_WAL_FILE_FRAME_LEN: usize = 8;
const CONTROL_PLANE_RAFT_PEER_RPC_MAGIC: &[u8] = b"ARGMINCPRAFTPEER";
const CONTROL_PLANE_RAFT_PEER_RPC_VERSION: u16 = 3;
const CONTROL_PLANE_RAFT_PEER_RPC_CHECKSUM_LEN: usize = 8;
const RAFT_ENTRY_MIN_LEN: usize = 8 + 8 + 8 + 1;
const RAFT_MEMBERSHIP_CONFIG_MIN_LEN: usize = 4;
const RAFT_MEMBERSHIP_NODE_MIN_LEN: usize = 8 + 4;

const CONTROL_PLANE_RAFT_WAL_JOURNAL_FORMAT: DurableJournalFormat = DurableJournalFormat {
    file_magic: CONTROL_PLANE_RAFT_WAL_FILE_MAGIC,
    file_version: CONTROL_PLANE_RAFT_WAL_FILE_VERSION,
    label: "control-plane OpenRaft WAL",
};

const CONTROL_PLANE_RAFT_WAL_IO_CONTEXTS: DurableJournalIoContexts = DurableJournalIoContexts {
    create_directory: "create control-plane OpenRaft WAL directory",
    open_for_append: "open control-plane OpenRaft WAL for append",
    write_frame_length: "write control-plane OpenRaft WAL frame length",
    write_frame: "write control-plane OpenRaft WAL frame",
    sync_file: "sync control-plane OpenRaft WAL",
    stat_for_status: "stat control-plane OpenRaft WAL for status",
    open_for_replay: "open control-plane OpenRaft WAL for replay",
    read_for_replay: "read control-plane OpenRaft WAL",
    open_for_tail_truncation: "open control-plane OpenRaft WAL for tail truncation",
    truncate_torn_tail: "truncate torn control-plane OpenRaft WAL tail",
    sync_truncated_tail: "sync truncated control-plane OpenRaft WAL",
    read_for_compaction: "read control-plane OpenRaft WAL for compaction",
    create_compacted_temp: "create compacted control-plane OpenRaft WAL temp file",
    write_compacted_temp: "write compacted control-plane OpenRaft WAL temp file",
    sync_compacted_temp: "sync compacted control-plane OpenRaft WAL temp file",
    commit_compacted: "commit compacted control-plane OpenRaft WAL",
    stat_before_append: "stat control-plane OpenRaft WAL before append",
    write_file_header: "write control-plane OpenRaft WAL file header",
    open_file_header: "open control-plane OpenRaft WAL header",
    read_file_header: "read control-plane OpenRaft WAL header",
};

fn control_plane_raft_restart_capture_attempt_allowed(completed_attempts: u64) -> bool {
    completed_attempts < CONTROL_PLANE_RAFT_RESTART_CAPTURE_MAX_ATTEMPTS
}

#[derive(Debug, Clone, Default, PartialEq)]
struct ControlPlaneRaftLogStoreInner {
    vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
    committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    entries: BTreeMap<u64, ControlPlaneRaftEntry>,
    poisoned: Option<String>,
}

#[derive(Debug, Clone)]
struct ControlPlaneRaftLogStoreDurableState {
    inner: ControlPlaneRaftLogStoreInner,
    wal_offsets: ControlPlaneRaftWalOffsets,
}

const CONTROL_PLANE_RAFT_DURABILITY_QUEUE_CAPACITY: usize = 64;

struct ControlPlaneRaftDurabilityLane {
    sender: mpsc::Sender<ControlPlaneRaftDurabilityRequest>,
    wal: Arc<ControlPlaneRaftWalFile>,
    durable: Arc<Mutex<ControlPlaneRaftLogStoreDurableState>>,
    publication_gate: Arc<Mutex<()>>,
}

impl fmt::Debug for ControlPlaneRaftDurabilityLane {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftDurabilityLane")
            .field("queue_capacity", &self.sender.max_capacity())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPlaneRaftDurabilityRequestMode {
    Append,
    Durable,
}

struct ControlPlaneRaftDurabilityRequest {
    record: ControlPlaneRaftWalRecord,
    mode: ControlPlaneRaftDurabilityRequestMode,
    enqueued_at: Instant,
    completion: Mutex<ControlPlaneRaftDurabilityCompletion>,
}

struct ControlPlaneRaftDurabilityQueueGuard {
    metrics: Arc<ControlPlaneRaftWalMetrics>,
    entered_at: Instant,
    submitted: bool,
}

enum ControlPlaneRaftDurabilityCompletion {
    Append {
        accepted: Option<oneshot::Sender<Result<(), String>>>,
        flushed: Option<IOFlushed<ControlPlaneRaftTypeConfig>>,
    },
    Durable {
        completed: Option<oneshot::Sender<Result<(), String>>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ControlPlaneRaftLogStoreDurabilityStatus {
    wal_backed: bool,
    wal_offsets: Option<ControlPlaneRaftWalOffsets>,
    wal_poisoned: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ControlPlaneRaftLogStoreStatusSnapshot {
    last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    durable_vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
    durable_committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    durable_last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    durable_last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    durability: ControlPlaneRaftLogStoreDurabilityStatus,
}

impl Default for ControlPlaneRaftLogStore {
    fn default() -> Self {
        let inner = ControlPlaneRaftLogStoreInner::default();
        Self {
            inner: Arc::new(Mutex::new(inner.clone())),
            durable: Arc::new(Mutex::new(ControlPlaneRaftLogStoreDurableState {
                inner,
                wal_offsets: ControlPlaneRaftWalOffsets {
                    base_offset: 0,
                    clean_len: 0,
                },
            })),
            wal: None,
            durability_lane: None,
        }
    }
}

impl ControlPlaneRaftDurabilityRequest {
    fn append(
        record: ControlPlaneRaftWalRecord,
        flushed: IOFlushed<ControlPlaneRaftTypeConfig>,
    ) -> (Self, oneshot::Receiver<Result<(), String>>) {
        let (accepted, receiver) = oneshot::channel();
        (
            Self {
                record,
                mode: ControlPlaneRaftDurabilityRequestMode::Append,
                enqueued_at: Instant::now(),
                completion: Mutex::new(ControlPlaneRaftDurabilityCompletion::Append {
                    accepted: Some(accepted),
                    flushed: Some(flushed),
                }),
            },
            receiver,
        )
    }

    fn durable(record: ControlPlaneRaftWalRecord) -> (Self, oneshot::Receiver<Result<(), String>>) {
        let (completed, receiver) = oneshot::channel();
        (
            Self {
                record,
                mode: ControlPlaneRaftDurabilityRequestMode::Durable,
                enqueued_at: Instant::now(),
                completion: Mutex::new(ControlPlaneRaftDurabilityCompletion::Durable {
                    completed: Some(completed),
                }),
            },
            receiver,
        )
    }

    fn completion(&self) -> MutexGuard<'_, ControlPlaneRaftDurabilityCompletion> {
        self.completion
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn accepted(&self) {
        let mut completion = self.completion();
        let ControlPlaneRaftDurabilityCompletion::Append { accepted, .. } = &mut *completion else {
            return;
        };
        if let Some(accepted) = accepted.take() {
            let _ = accepted.send(Ok(()));
        }
    }

    fn completed(&self) {
        let mut completion = self.completion();
        match &mut *completion {
            ControlPlaneRaftDurabilityCompletion::Append { accepted, flushed } => {
                if let Some(accepted) = accepted.take() {
                    let _ = accepted.send(Ok(()));
                }
                if let Some(flushed) = flushed.take() {
                    flushed.io_completed(Ok(()));
                }
            }
            ControlPlaneRaftDurabilityCompletion::Durable { completed } => {
                if let Some(completed) = completed.take() {
                    let _ = completed.send(Ok(()));
                }
            }
        }
    }

    fn failed(&self, message: impl Into<String>) {
        let message = message.into();
        let mut completion = self.completion();
        match &mut *completion {
            ControlPlaneRaftDurabilityCompletion::Append { accepted, flushed } => {
                if let Some(accepted) = accepted.take() {
                    let _ = accepted.send(Err(message.clone()));
                }
                if let Some(flushed) = flushed.take() {
                    flushed.io_completed(Err(raft_log_store_error(message)));
                }
            }
            ControlPlaneRaftDurabilityCompletion::Durable { completed } => {
                if let Some(completed) = completed.take() {
                    let _ = completed.send(Err(message));
                }
            }
        }
    }
}

impl ControlPlaneRaftDurabilityQueueGuard {
    fn enter(metrics: Arc<ControlPlaneRaftWalMetrics>) -> Self {
        metrics.record_durability_queue_enter();
        observability::record_control_plane_raft_wal_durability_queue_enter();
        Self {
            metrics,
            entered_at: Instant::now(),
            submitted: false,
        }
    }

    fn submitted(mut self) {
        self.submitted = true;
    }
}

impl Drop for ControlPlaneRaftDurabilityQueueGuard {
    fn drop(&mut self) {
        if !self.submitted {
            let elapsed = self.entered_at.elapsed();
            self.metrics.record_durability_queue_leave(elapsed);
            observability::record_control_plane_raft_wal_durability_queue_leave(elapsed);
        }
    }
}

impl ControlPlaneRaftDurabilityLane {
    fn new(
        accepted: Arc<Mutex<ControlPlaneRaftLogStoreInner>>,
        durable: Arc<Mutex<ControlPlaneRaftLogStoreDurableState>>,
        wal: Arc<ControlPlaneRaftWalFile>,
    ) -> Result<Arc<Self>, io::Error> {
        let (sender, receiver) = mpsc::channel(CONTROL_PLANE_RAFT_DURABILITY_QUEUE_CAPACITY);
        let publication_gate = Arc::new(Mutex::new(()));
        let worker_name = format!("argmin-raft-wal-{}", wal.local_node_id);
        let worker_durable = Arc::clone(&durable);
        let worker_wal = Arc::clone(&wal);
        let worker_publication_gate = Arc::clone(&publication_gate);
        std::thread::Builder::new()
            .name(worker_name)
            .spawn(move || {
                Self::run(
                    receiver,
                    accepted,
                    worker_durable,
                    worker_wal,
                    worker_publication_gate,
                );
            })
            .map_err(|source| {
                io::Error::new(
                    source.kind(),
                    format!("spawn control-plane OpenRaft WAL durability worker: {source}"),
                )
            })?;
        Ok(Arc::new(Self {
            sender,
            wal,
            durable,
            publication_gate,
        }))
    }

    async fn append(
        &self,
        record: ControlPlaneRaftWalRecord,
        callback: IOFlushed<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let acceptance_started = Instant::now();
        let admission = ControlPlaneRaftDurabilityQueueGuard::enter(Arc::clone(&self.wal.metrics));
        let (request, accepted) = ControlPlaneRaftDurabilityRequest::append(record, callback);
        if let Err(error) = self.sender.send(request).await {
            let message = "control-plane OpenRaft WAL durability worker is unavailable";
            error.0.failed(message);
            return Err(raft_log_store_error(message));
        }
        admission.submitted();
        let result = match accepted.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(raft_log_store_error(message)),
            Err(_) => Err(raft_log_store_error(
                "control-plane OpenRaft WAL durability worker dropped append acceptance",
            )),
        };
        let elapsed = acceptance_started.elapsed();
        self.wal.metrics.record_append_accept(elapsed);
        observability::record_control_plane_raft_wal_append_accept(elapsed);
        result
    }

    async fn durable(&self, record: ControlPlaneRaftWalRecord) -> Result<(), io::Error> {
        let admission = ControlPlaneRaftDurabilityQueueGuard::enter(Arc::clone(&self.wal.metrics));
        let (request, completed) = ControlPlaneRaftDurabilityRequest::durable(record);
        if let Err(error) = self.sender.send(request).await {
            let message = "control-plane OpenRaft WAL durability worker is unavailable";
            error.0.failed(message);
            return Err(raft_log_store_error(message));
        }
        admission.submitted();
        match completed.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(raft_log_store_error(message)),
            Err(_) => Err(raft_log_store_error(
                "control-plane OpenRaft WAL durability worker dropped operation completion",
            )),
        }
    }

    fn compact_through(&self, replay_offset: u64) -> Result<(), io::Error> {
        // This gate is acquired only by the dedicated durability worker and
        // the synchronous checkpoint worker. Async log readers never wait on
        // a standard mutex held across filesystem I/O.
        let _publication = self
            .publication_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.wal.compact_through(replay_offset).map_err(|error| {
            control_plane_error_to_io_error("compact control-plane OpenRaft WAL", error)
        })?;
        let offsets = self.wal.status_offsets().map_err(|error| {
            control_plane_error_to_io_error(
                "read control-plane OpenRaft WAL offsets after compaction",
                error,
            )
        })?;
        self.durable
            .lock()
            .map_err(|_| {
                io::Error::other("control-plane OpenRaft durable log store lock poisoned")
            })?
            .wal_offsets = offsets;
        Ok(())
    }

    fn run(
        mut receiver: mpsc::Receiver<ControlPlaneRaftDurabilityRequest>,
        accepted: Arc<Mutex<ControlPlaneRaftLogStoreInner>>,
        durable: Arc<Mutex<ControlPlaneRaftLogStoreDurableState>>,
        wal: Arc<ControlPlaneRaftWalFile>,
        publication_gate: Arc<Mutex<()>>,
    ) {
        while let Some(request) = receiver.blocking_recv() {
            let queue_wait = request.enqueued_at.elapsed();
            wal.metrics.record_durability_queue_leave(queue_wait);
            observability::record_control_plane_raft_wal_durability_queue_leave(queue_wait);
            let operation_started = Instant::now();
            let result = catch_unwind(AssertUnwindSafe(|| {
                Self::process(&request, &accepted, &durable, &wal, &publication_gate)
            }));
            if result.is_err() {
                let message = "control-plane OpenRaft WAL durability worker panicked";
                Self::poison(&accepted, &durable, message, None);
                request.failed(message);
            }
            let elapsed = operation_started.elapsed();
            wal.metrics.record_durability_operation(elapsed);
            observability::record_control_plane_raft_wal_durability_operation(elapsed);
        }
    }

    fn process(
        request: &ControlPlaneRaftDurabilityRequest,
        accepted: &Arc<Mutex<ControlPlaneRaftLogStoreInner>>,
        durable: &Arc<Mutex<ControlPlaneRaftLogStoreDurableState>>,
        wal: &ControlPlaneRaftWalFile,
        publication_gate: &Mutex<()>,
    ) {
        let candidate = {
            let current = accepted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(reason) = &current.poisoned {
                request.failed(format!(
                    "control-plane OpenRaft WAL-backed log store poisoned: {reason}"
                ));
                return;
            }
            let mut candidate = current.clone();
            if let Err(error) = request.record.apply_to_log_store_inner(&mut candidate) {
                request.failed(error.to_string());
                return;
            }
            if candidate == *current {
                request.completed();
                return;
            }
            candidate
        };

        if request.mode == ControlPlaneRaftDurabilityRequestMode::Append {
            *accepted
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = candidate.clone();
            request.accepted();
        }

        let _publication = publication_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match wal.append_record_for_log_store(&request.record) {
            Ok(()) => {
                let offsets = match wal.status_offsets() {
                    Ok(offsets) => offsets,
                    Err(error) => {
                        let message = format!(
                            "read control-plane OpenRaft WAL offsets after durable append: {error}"
                        );
                        Self::poison(accepted, durable, &message, Some(candidate));
                        request.failed(message);
                        return;
                    }
                };
                if request.mode == ControlPlaneRaftDurabilityRequestMode::Durable {
                    *accepted
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = candidate.clone();
                    inject_control_plane_raft_wal_durable_publication_delay(
                        wal.path(),
                        &request.record,
                    );
                }
                *durable
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    ControlPlaneRaftLogStoreDurableState {
                        inner: candidate,
                        wal_offsets: offsets,
                    };
                request.completed();
            }
            Err(ControlPlaneRaftWalAppendError::BeforeReplayableRecord(error)) => {
                let message = format!("append OpenRaft WAL record: {error}");
                if request.mode == ControlPlaneRaftDurabilityRequestMode::Append {
                    Self::poison(accepted, durable, &message, None);
                }
                request.failed(message);
            }
            Err(ControlPlaneRaftWalAppendError::AmbiguousRecordMayExist(error)) => {
                let message =
                    format!("ambiguous WAL append after WAL write before file sync: {error}");
                Self::poison(accepted, durable, &message, None);
                request.failed(message);
            }
            Err(ControlPlaneRaftWalAppendError::ReplayableRecordMayExist(error)) => {
                let message = format!(
                    "WAL append failed after file sync; restart required to reconcile durable state: {error}"
                );
                Self::poison(accepted, durable, &message, Some(candidate));
                request.failed(message);
            }
        }
    }

    fn poison(
        accepted: &Arc<Mutex<ControlPlaneRaftLogStoreInner>>,
        durable: &Arc<Mutex<ControlPlaneRaftLogStoreDurableState>>,
        message: &str,
        durable_candidate: Option<ControlPlaneRaftLogStoreInner>,
    ) {
        let mut accepted = accepted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        accepted.poisoned = Some(message.to_string());
        let mut durable = durable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(candidate) = durable_candidate {
            durable.inner = candidate;
        }
        durable.inner.poisoned = Some(message.to_string());
    }
}

impl ControlPlaneRaftLogStore {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn export_restart_artifact(
        &self,
    ) -> Result<ControlPlaneRaftLogStoreRestartArtifact, io::Error> {
        let durable = self.lock_durable()?;
        Ok(Self::restart_artifact_from_inner(&durable.inner))
    }

    fn export_restart_artifact_with_wal_replay_offset(
        &self,
    ) -> Result<(ControlPlaneRaftLogStoreRestartArtifact, u64), io::Error> {
        let durable = self.lock_durable()?;
        Ok((
            Self::restart_artifact_from_inner(&durable.inner),
            durable.wal_offsets.clean_len,
        ))
    }

    fn compact_wal_through(&self, replay_offset: u64) -> Result<(), io::Error> {
        if let Some(durability_lane) = &self.durability_lane {
            durability_lane.compact_through(replay_offset)?;
        }
        Ok(())
    }

    fn wal_metric_snapshot(&self) -> Option<observability::ControlPlaneRaftWalMetricSnapshot> {
        self.wal.as_ref().map(|wal| wal.metrics.snapshot())
    }

    fn wal_monitor_snapshot(
        &self,
    ) -> Result<Option<ControlPlaneRaftWalMonitorSnapshot>, io::Error> {
        let (poisoned, offsets) = match self.durable.lock() {
            Ok(durable) => (durable.inner.poisoned.clone(), durable.wal_offsets),
            Err(error) => {
                let durable = error.into_inner();
                (
                    Some(durable.inner.poisoned.clone().unwrap_or_else(|| {
                        "control-plane OpenRaft durable log store mutex poisoned".to_string()
                    })),
                    durable.wal_offsets,
                )
            }
        };
        let Some(wal) = &self.wal else {
            return Ok(None);
        };
        // Metrics may lead the published durable offset while a completed sync
        // is being recorded. The monitor conservatively observes that suffix on
        // its next pass; it never reports accepted-only state as durable.
        let metrics = wal.metrics.snapshot();
        Ok(Some(ControlPlaneRaftWalMonitorSnapshot {
            offsets,
            metrics,
            poisoned,
        }))
    }

    fn restart_artifact_from_inner(
        inner: &ControlPlaneRaftLogStoreInner,
    ) -> ControlPlaneRaftLogStoreRestartArtifact {
        ControlPlaneRaftLogStoreRestartArtifact {
            vote: inner.vote,
            committed: inner.committed,
            last_purged_log_id: inner.last_purged_log_id,
            entries: inner.entries.values().cloned().collect(),
        }
    }

    pub fn last_purged_log_id(
        &self,
    ) -> Result<Option<LogIdOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock()?.last_purged_log_id)
    }

    pub fn persisted_vote(&self) -> Result<Option<VoteOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock_durable()?.inner.vote)
    }

    fn status_snapshot(&self) -> Result<ControlPlaneRaftLogStoreStatusSnapshot, io::Error> {
        let last_purged_log_id = {
            let inner = self
                .inner
                .lock()
                .map_err(|_| io::Error::other("control-plane OpenRaft log store lock poisoned"))?;
            inner.last_purged_log_id
        };
        let durable = self.durable.lock().map_err(|_| {
            io::Error::other("control-plane OpenRaft durable log store lock poisoned")
        })?;
        let wal_backed = self.wal.is_some();
        let wal_poisoned = durable.inner.poisoned.clone();
        let wal_offsets = match (&self.wal, &wal_poisoned) {
            (Some(_), None) => Some(durable.wal_offsets),
            (Some(_), Some(_)) | (None, _) => None,
        };
        Ok(ControlPlaneRaftLogStoreStatusSnapshot {
            last_purged_log_id,
            durable_vote: durable.inner.vote,
            durable_committed: durable.inner.committed,
            durable_last_log_id: durable.inner.last_log_id(),
            durable_last_purged_log_id: durable.inner.last_purged_log_id,
            durability: ControlPlaneRaftLogStoreDurabilityStatus {
                wal_backed,
                wal_offsets,
                wal_poisoned,
            },
        })
    }

    fn from_restart_artifact_in_memory(
        artifact: ControlPlaneRaftLogStoreRestartArtifact,
    ) -> Result<Self, io::Error> {
        Self::from_restart_artifact_inner(artifact, None)
    }

    #[cfg(test)]
    fn from_restart_artifact_with_wal_file(
        artifact: ControlPlaneRaftLogStoreRestartArtifact,
        wal: ControlPlaneRaftWalFile,
    ) -> Result<Self, ControlPlaneError> {
        let replayed = wal.replay_log_store_artifact(ControlPlaneRaftWalReplayConfig {
            base: &artifact,
            replay_offset: 0,
        })?;
        Self::from_restart_artifact_inner(replayed, Some(Arc::new(wal))).map_err(|source| {
            ControlPlaneError::io(
                "restore control-plane OpenRaft WAL-backed log store",
                source,
            )
        })
    }

    fn from_restart_artifact_inner(
        artifact: ControlPlaneRaftLogStoreRestartArtifact,
        wal: Option<Arc<ControlPlaneRaftWalFile>>,
    ) -> Result<Self, io::Error> {
        let mut inner = ControlPlaneRaftLogStoreInner {
            vote: artifact.vote,
            committed: None,
            last_purged_log_id: artifact.last_purged_log_id,
            entries: BTreeMap::new(),
            poisoned: None,
        };
        Self::validate_contiguous_append(&inner, &artifact.entries)?;
        for entry in artifact.entries {
            inner.entries.insert(entry.log_id.index(), entry);
        }
        Self::validate_committed_update(&inner, artifact.committed)?;
        inner.committed = artifact.committed;
        Self::validate_purged_boundary_has_committed(&inner)?;
        let wal_offsets = wal
            .as_ref()
            .map(|wal| {
                wal.status_offsets().map_err(|error| {
                    control_plane_error_to_io_error(
                        "read restored control-plane OpenRaft WAL offsets",
                        error,
                    )
                })
            })
            .transpose()?
            .unwrap_or(ControlPlaneRaftWalOffsets {
                base_offset: 0,
                clean_len: 0,
            });
        let accepted = Arc::new(Mutex::new(inner.clone()));
        let durable = Arc::new(Mutex::new(ControlPlaneRaftLogStoreDurableState {
            inner,
            wal_offsets,
        }));
        let durability_lane = wal
            .as_ref()
            .map(|wal| {
                ControlPlaneRaftDurabilityLane::new(
                    Arc::clone(&accepted),
                    Arc::clone(&durable),
                    Arc::clone(wal),
                )
            })
            .transpose()?;
        Ok(Self {
            inner: accepted,
            durable,
            wal,
            durability_lane,
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, ControlPlaneRaftLogStoreInner>, io::Error> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("control-plane OpenRaft log store lock poisoned"))?;
        if let Some(reason) = &inner.poisoned {
            return Err(io::Error::other(format!(
                "control-plane OpenRaft WAL-backed log store poisoned: {reason}"
            )));
        }
        Ok(inner)
    }

    fn lock_durable(
        &self,
    ) -> Result<MutexGuard<'_, ControlPlaneRaftLogStoreDurableState>, io::Error> {
        let durable = self.durable.lock().map_err(|_| {
            io::Error::other("control-plane OpenRaft durable log store lock poisoned")
        })?;
        if let Some(reason) = &durable.inner.poisoned {
            return Err(io::Error::other(format!(
                "control-plane OpenRaft WAL-backed durable log store poisoned: {reason}"
            )));
        }
        Ok(durable)
    }

    fn validate_contiguous_append(
        inner: &ControlPlaneRaftLogStoreInner,
        entries: &[ControlPlaneRaftEntry],
    ) -> Result<(), io::Error> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let current_last_log_id = inner.last_log_id();
        let expected_first_index = match current_last_log_id {
            Some(log_id) => log_id.index().checked_add(1).ok_or_else(|| {
                raft_log_store_error("cannot append after u64::MAX OpenRaft log index")
            })?,
            None => 0,
        };
        if first.log_id.index() != expected_first_index {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft append starts at index {}, expected {}",
                first.log_id.index(),
                expected_first_index
            )));
        }
        if expected_first_index == 0 {
            Self::validate_bootstrap_entry_shape(first)?;
        }

        let mut expected_index = expected_first_index;
        for entry in entries {
            if entry.log_id.index() != expected_index {
                return Err(raft_log_store_error(format!(
                    "control-plane OpenRaft append leaves a log hole at index {expected_index}; next entry is {}",
                    entry.log_id.index()
                )));
            }
            expected_index = expected_index.checked_add(1).ok_or_else(|| {
                raft_log_store_error("control-plane OpenRaft append range overflows u64")
            })?;
        }
        Ok(())
    }

    fn validate_bootstrap_entry_shape(entry: &ControlPlaneRaftEntry) -> Result<(), io::Error> {
        if is_openraft_bootstrap_log_id(entry.log_id)
            && matches!(entry.payload, EntryPayload::Membership(_))
        {
            return Ok(());
        }
        Err(raft_log_store_error(format!(
            "control-plane OpenRaft log index 0 entry must be bootstrap membership at term 0; got log id {} with payload {}",
            entry.log_id,
            raft_entry_payload_name(entry)
        )))
    }

    fn range_start<RB>(range: &RB) -> Result<Option<u64>, io::Error>
    where
        RB: RangeBounds<u64>,
    {
        match range.start_bound() {
            Bound::Included(start) => Ok(Some(*start)),
            Bound::Excluded(start) => Ok(start.checked_add(1)),
            Bound::Unbounded => Ok(Some(0)),
        }
    }

    fn range_end_exclusive<RB>(range: &RB) -> Option<u64>
    where
        RB: RangeBounds<u64>,
    {
        match range.end_bound() {
            Bound::Included(end) => end.checked_add(1),
            Bound::Excluded(end) => Some(*end),
            Bound::Unbounded => None,
        }
    }

    fn before_range_end(index: u64, end_exclusive: Option<u64>) -> bool {
        end_exclusive.is_none_or(|end_exclusive| index < end_exclusive)
    }

    fn validate_known_log_id(
        inner: &ControlPlaneRaftLogStoreInner,
        context: &'static str,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let Some(current_last_log_id) = inner.last_log_id() else {
            return Err(raft_log_store_error(format!(
                "cannot {context} {log_id}; control-plane OpenRaft log is empty"
            )));
        };
        if log_id.index() > current_last_log_id.index() {
            return Err(raft_log_store_error(format!(
                "cannot {context} {log_id}; current last log id is {current_last_log_id}"
            )));
        }
        if let Some(last_purged_log_id) = inner.last_purged_log_id {
            if log_id.index() < last_purged_log_id.index() {
                return Err(raft_log_store_error(format!(
                    "cannot {context} {log_id}; it is before purged boundary {last_purged_log_id}"
                )));
            }
            if log_id.index() == last_purged_log_id.index() {
                if log_id == last_purged_log_id {
                    return Ok(());
                }
                return Err(raft_log_store_error(format!(
                    "cannot {context} mismatched purged log id {log_id}; purged boundary is {last_purged_log_id}"
                )));
            }
        }
        let Some(entry) = inner.entries.get(&log_id.index()) else {
            return Err(raft_log_store_error(format!(
                "cannot {context} {log_id}; control-plane OpenRaft log has no entry at that index"
            )));
        };
        if entry.log_id != log_id {
            return Err(raft_log_store_error(format!(
                "cannot {context} mismatched log id {log_id}; stored {}",
                entry.log_id
            )));
        }
        Ok(())
    }

    fn validate_committed_update(
        inner: &ControlPlaneRaftLogStoreInner,
        committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let Some(committed) = committed else {
            if inner.committed.is_some() {
                return Err(raft_log_store_error(
                    "cannot clear control-plane OpenRaft committed log id",
                ));
            }
            return Ok(());
        };
        if let Some(previous_committed) = inner.committed {
            if committed.index() < previous_committed.index() {
                return Err(raft_log_store_error(format!(
                    "cannot regress control-plane OpenRaft committed log id from {previous_committed} to {committed}"
                )));
            }
            if committed.index() == previous_committed.index() && committed != previous_committed {
                return Err(raft_log_store_error(format!(
                    "cannot change control-plane OpenRaft committed log id at index {} from {previous_committed} to {committed}",
                    committed.index()
                )));
            }
        }
        Self::validate_known_log_id(inner, "commit", committed)?;
        Self::validate_vote_covers_committed(inner.vote, committed)
    }

    fn validate_vote_update(
        inner: &ControlPlaneRaftLogStoreInner,
        vote: VoteOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let Some(previous_vote) = inner.vote else {
            return Ok(());
        };
        if matches!(
            vote.partial_cmp(&previous_vote),
            Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater)
        ) {
            return Ok(());
        }
        Err(raft_log_store_error(format!(
            "cannot regress control-plane OpenRaft vote from {previous_vote} to {vote}"
        )))
    }

    fn validate_vote_covers_committed(
        vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
        committed: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let Some(vote) = vote else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft log store is missing vote state for committed log id {committed}"
            )));
        };
        if vote.leader_id >= *committed.committed_leader_id() {
            return Ok(());
        }
        Err(raft_log_store_error(format!(
            "control-plane OpenRaft log store vote {vote} does not cover committed log id {committed}"
        )))
    }

    fn validate_purged_boundary_has_committed(
        inner: &ControlPlaneRaftLogStoreInner,
    ) -> Result<(), io::Error> {
        let Some(last_purged_log_id) = inner.last_purged_log_id else {
            return Ok(());
        };
        let Some(committed) = inner.committed else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft log store has purged boundary {last_purged_log_id} without a committed restart gate"
            )));
        };
        if last_purged_log_id.index() <= committed.index() {
            return Ok(());
        }
        Err(raft_log_store_error(format!(
            "control-plane OpenRaft purged boundary {last_purged_log_id} is after committed log id {committed}"
        )))
    }

    fn save_vote_inner(
        inner: &mut ControlPlaneRaftLogStoreInner,
        vote: VoteOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        Self::validate_vote_update(inner, vote)?;
        inner.vote = Some(vote);
        Ok(())
    }

    fn save_committed_inner(
        inner: &mut ControlPlaneRaftLogStoreInner,
        committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        Self::validate_committed_update(inner, committed)?;
        inner.committed = committed;
        Ok(())
    }

    fn append_inner(
        inner: &mut ControlPlaneRaftLogStoreInner,
        entries: Vec<ControlPlaneRaftEntry>,
    ) -> Result<(), io::Error> {
        Self::validate_contiguous_append(inner, &entries)?;
        for entry in entries {
            inner.entries.insert(entry.log_id.index(), entry);
        }
        Ok(())
    }

    fn truncate_after_inner(
        inner: &mut ControlPlaneRaftLogStoreInner,
        last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        if let Some(committed) = inner.committed {
            match last_log_id {
                Some(last_log_id) if last_log_id.index() >= committed.index() => {}
                Some(last_log_id) => {
                    return Err(raft_log_store_error(format!(
                        "cannot truncate control-plane OpenRaft log after {last_log_id}; committed log id is {committed}"
                    )));
                }
                None => {
                    return Err(raft_log_store_error(format!(
                        "cannot clear control-plane OpenRaft log; committed log id is {committed}"
                    )));
                }
            }
        }
        let Some(last_log_id) = last_log_id else {
            inner.entries.clear();
            return Ok(());
        };

        Self::validate_known_log_id(inner, "truncate after", last_log_id)?;
        inner
            .entries
            .retain(|index, _| *index <= last_log_id.index());
        Ok(())
    }

    fn purge_inner(
        inner: &mut ControlPlaneRaftLogStoreInner,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        if let Some(last_purged_log_id) = inner.last_purged_log_id {
            if log_id.index() <= last_purged_log_id.index() {
                if log_id == last_purged_log_id {
                    return Ok(());
                }
                return Err(raft_log_store_error(format!(
                    "cannot repurge control-plane OpenRaft log to {log_id}; current purged boundary is {last_purged_log_id}"
                )));
            }
        }
        if inner.committed.is_none() {
            if inner.entries.is_empty() && inner.last_purged_log_id.is_none() {
                Self::validate_vote_covers_committed(inner.vote, log_id)?;
                inner.committed = Some(log_id);
            } else {
                return Err(raft_log_store_error(format!(
                    "cannot purge control-plane OpenRaft log to {log_id}; no committed restart gate"
                )));
            }
        } else {
            Self::validate_vote_covers_committed(inner.vote, log_id)?;
        }
        inner.entries.retain(|index, _| *index > log_id.index());
        inner.last_purged_log_id = Some(log_id);
        if inner
            .committed
            .is_some_and(|committed| committed.index() < log_id.index())
        {
            inner.committed = Some(log_id);
        }
        Ok(())
    }

    fn apply_in_memory_record(
        &self,
        inner: &mut ControlPlaneRaftLogStoreInner,
        record: &ControlPlaneRaftWalRecord,
    ) -> Result<(), io::Error> {
        debug_assert!(self.wal.is_none());
        let mut candidate = inner.clone();
        record.apply_to_log_store_inner(&mut candidate)?;
        if candidate == *inner {
            return Ok(());
        }
        let mut durable = self.lock_durable()?;
        *inner = candidate.clone();
        durable.inner = candidate;
        Ok(())
    }
}

impl ControlPlaneRaftLogStoreInner {
    fn last_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .or(self.last_purged_log_id)
    }
}

impl ControlPlaneRaftLogStoreRestartArtifact {
    pub fn replay_wal_records(
        &self,
        records: &[ControlPlaneRaftWalRecord],
    ) -> Result<Self, io::Error> {
        let store = ControlPlaneRaftLogStore::from_restart_artifact_in_memory(self.clone())?;
        let mut inner = store.lock()?;
        for record in records {
            record.apply_to_log_store_inner(&mut inner)?;
        }
        Ok(ControlPlaneRaftLogStore::restart_artifact_from_inner(
            &inner,
        ))
    }
}

impl ControlPlaneRaftWalRecord {
    fn apply_to_log_store_inner(
        &self,
        inner: &mut ControlPlaneRaftLogStoreInner,
    ) -> Result<(), io::Error> {
        match self {
            Self::SaveVote(vote) => ControlPlaneRaftLogStore::save_vote_inner(inner, *vote),
            Self::Append(entries) => ControlPlaneRaftLogStore::append_inner(inner, entries.clone()),
            Self::SaveCommitted(committed) => {
                ControlPlaneRaftLogStore::save_committed_inner(inner, *committed)
            }
            Self::TruncateAfter(last_log_id) => {
                ControlPlaneRaftLogStore::truncate_after_inner(inner, *last_log_id)
            }
            Self::Purge(log_id) => ControlPlaneRaftLogStore::purge_inner(inner, *log_id),
        }
    }
}

impl ControlPlaneRaftWalFrame {
    fn new(
        cluster_name: impl Into<String>,
        local_node_id: ControlPlaneRaftNodeId,
        record: ControlPlaneRaftWalRecord,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            local_node_id,
            record,
        }
    }

    #[cfg(test)]
    fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    #[cfg(test)]
    fn local_node_id(&self) -> ControlPlaneRaftNodeId {
        self.local_node_id
    }

    #[cfg(test)]
    fn record(&self) -> &ControlPlaneRaftWalRecord {
        &self.record
    }

    fn into_record(self) -> ControlPlaneRaftWalRecord {
        self.record
    }

    fn encode_frame(&self) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_WAL_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_WAL_VERSION);
        write_raft_string(&mut out, &self.cluster_name)?;
        write_raft_u64(&mut out, self.local_node_id);
        write_raft_wal_record(&mut out, &self.record)?;
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    fn decode_frame_classified(bytes: &[u8]) -> Result<Self, ControlPlaneRaftWalFrameDecodeError> {
        let min_len = CONTROL_PLANE_RAFT_WAL_MAGIC.len() + 2 + CONTROL_PLANE_RAFT_WAL_CHECKSUM_LEN;
        if bytes.len() < min_len {
            return Err(ControlPlaneRaftWalFrameDecodeError::Format(
                ControlPlaneRaftWalFrameFormatError::Truncated,
            ));
        }
        let (body, checksum_bytes) =
            bytes.split_at(bytes.len() - CONTROL_PLANE_RAFT_WAL_CHECKSUM_LEN);
        let expected_checksum = u64::from_be_bytes(
            checksum_bytes
                .try_into()
                .expect("checksum split length is fixed"),
        );
        let actual_checksum = raft_artifact_checksum(body);
        if actual_checksum != expected_checksum {
            return Err(ControlPlaneRaftWalFrameDecodeError::Format(
                ControlPlaneRaftWalFrameFormatError::ChecksumMismatch {
                    expected: expected_checksum,
                    actual: actual_checksum,
                },
            ));
        }

        let mut reader = RaftArtifactReader::with_context(body, "control-plane OpenRaft WAL frame");
        let magic = reader
            .read_exact(CONTROL_PLANE_RAFT_WAL_MAGIC.len())
            .map_err(ControlPlaneRaftWalFrameDecodeError::Invalid)?;
        if magic != CONTROL_PLANE_RAFT_WAL_MAGIC {
            return Err(ControlPlaneRaftWalFrameDecodeError::Format(
                ControlPlaneRaftWalFrameFormatError::UnknownMagic,
            ));
        }
        let version = reader
            .read_u16()
            .map_err(ControlPlaneRaftWalFrameDecodeError::Invalid)?;
        if version != CONTROL_PLANE_RAFT_WAL_VERSION {
            return Err(ControlPlaneRaftWalFrameDecodeError::Format(
                ControlPlaneRaftWalFrameFormatError::UnsupportedVersion(version),
            ));
        }
        let decoded: Result<Self, ControlPlaneError> = (|| {
            let frame = Self {
                cluster_name: reader.read_string()?,
                local_node_id: reader.read_u64()?,
                record: reader.read_wal_record()?,
            };
            reader.finish()?;
            Ok(frame)
        })();
        match decoded {
            Ok(frame) => Ok(frame),
            Err(_) if reader.was_truncated() => Err(ControlPlaneRaftWalFrameDecodeError::Format(
                ControlPlaneRaftWalFrameFormatError::Truncated,
            )),
            Err(error) => Err(ControlPlaneRaftWalFrameDecodeError::Invalid(error)),
        }
    }

    fn decode_frame(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_classified(bytes)
            .map_err(ControlPlaneRaftWalFrameDecodeError::into_control_plane_error)
    }

    fn validate_identity(
        &self,
        cluster_name: &str,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        if self.cluster_name != cluster_name {
            return Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft WAL frame belongs to cluster {:?}, not configured cluster {:?}",
                self.cluster_name, cluster_name
            )));
        }
        if self.local_node_id != local_node_id {
            return Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft WAL frame belongs to local OpenRaft node {}, not configured local node {local_node_id}",
                self.local_node_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ControlPlaneRaftWalFileRecords {
    records: Vec<ControlPlaneRaftWalRecord>,
    clean_len: u64,
    truncated_tail: bool,
}

impl ControlPlaneRaftWalFile {
    fn new(config: ControlPlaneRaftWalFileConfig) -> Self {
        let metrics = Arc::new(ControlPlaneRaftWalMetrics::default());
        let observer = Arc::new(ControlPlaneRaftWalObserver {
            metrics: Arc::clone(&metrics),
        });
        let journal = DurableJournalFile::new(
            config.path,
            CONTROL_PLANE_RAFT_WAL_JOURNAL_FORMAT,
            CONTROL_PLANE_RAFT_WAL_IO_CONTEXTS,
            observer,
        );
        Self {
            cluster_name: config.cluster_name,
            local_node_id: config.local_node_id,
            metrics,
            journal,
        }
    }

    fn path(&self) -> &Path {
        self.journal.path()
    }

    #[cfg(test)]
    fn append_record(&self, record: &ControlPlaneRaftWalRecord) -> Result<(), ControlPlaneError> {
        self.append_record_for_log_store(record)
            .map_err(DurableJournalAppendError::into_control_plane_error)
    }

    fn append_record_for_log_store(
        &self,
        record: &ControlPlaneRaftWalRecord,
    ) -> Result<(), ControlPlaneRaftWalAppendError> {
        let frame = ControlPlaneRaftWalFrame::new(
            self.cluster_name.clone(),
            self.local_node_id,
            record.clone(),
        )
        .encode_frame()
        .map_err(ControlPlaneRaftWalAppendError::BeforeReplayableRecord)?;
        self.journal.append_frame(&frame)
    }

    fn replay_log_store_artifact(
        &self,
        config: ControlPlaneRaftWalReplayConfig<'_>,
    ) -> Result<ControlPlaneRaftLogStoreRestartArtifact, ControlPlaneError> {
        let records = self.read_records_from(config.replay_offset)?;
        #[cfg(test)]
        CONTROL_PLANE_RAFT_WAL_REPLAY_ATTEMPTS.set(
            CONTROL_PLANE_RAFT_WAL_REPLAY_ATTEMPTS
                .get()
                .saturating_add(1),
        );
        let artifact = config
            .base
            .replay_wal_records(&records.records)
            .map_err(|source| {
                ControlPlaneError::io("replay control-plane OpenRaft WAL records", source)
            })?;
        if records.truncated_tail {
            self.truncate_to_clean_len(records.clean_len)?;
        }
        Ok(artifact)
    }

    #[cfg(test)]
    fn clean_len(&self) -> Result<u64, ControlPlaneError> {
        self.journal.clean_len()
    }

    fn status_offsets(&self) -> Result<ControlPlaneRaftWalOffsets, ControlPlaneError> {
        let offsets = self.journal.status_offsets()?;
        Ok(ControlPlaneRaftWalOffsets {
            base_offset: offsets.base_offset,
            clean_len: offsets.clean_len,
        })
    }

    fn read_records_from(
        &self,
        replay_offset: u64,
    ) -> Result<ControlPlaneRaftWalFileRecords, ControlPlaneError> {
        let frames = self.journal.read_frames_from(replay_offset)?;
        let mut records = Vec::with_capacity(frames.frames.len());
        for encoded in frames.frames {
            let frame = ControlPlaneRaftWalFrame::decode_frame(&encoded)?;
            frame.validate_identity(&self.cluster_name, self.local_node_id)?;
            records.push(frame.into_record());
        }
        Ok(ControlPlaneRaftWalFileRecords {
            records,
            clean_len: frames.clean_len,
            truncated_tail: frames.truncated_tail,
        })
    }

    fn truncate_to_clean_len(&self, clean_len: u64) -> Result<(), ControlPlaneError> {
        self.journal.truncate_to_clean_len(clean_len)
    }

    fn compact_through(&self, replay_offset: u64) -> Result<(), ControlPlaneError> {
        self.journal.compact_through(replay_offset)
    }
}

#[cfg(test)]
fn write_control_plane_raft_wal_bytes(
    writer: &mut impl Write,
    bytes: &[u8],
    context: &'static str,
) -> Result<(), ControlPlaneRaftWalAppendError> {
    writer
        .write_all(bytes)
        .map_err(|source| ControlPlaneError::io(context, source))
        .map_err(ControlPlaneRaftWalAppendError::AmbiguousRecordMayExist)
}

impl ControlPlaneRaftRestartArtifact {
    #[cfg(test)]
    fn capture(
        cluster_name: impl Into<String>,
        local_node_id: ControlPlaneRaftNodeId,
        log_store: &ControlPlaneRaftLogStore,
        state_machine: &ControlPlaneRaftStateMachine,
    ) -> Result<Self, io::Error> {
        let (log_store_artifact, wal_replay_offset) =
            log_store.export_restart_artifact_with_wal_replay_offset()?;
        let artifact = Self {
            cluster_name: cluster_name.into(),
            local_node_id,
            wal_replay_offset,
            log_store: log_store_artifact,
            state_machine: state_machine
                .export_restart_artifact()
                .refresh_cached_snapshot()
                .map_err(|error| {
                    control_plane_error_to_io_error(
                        "OpenRaft durable restart snapshot refresh",
                        error,
                    )
                })?,
        };
        artifact.validate_restart_pair()?;
        Ok(artifact)
    }

    fn encode_durable_artifact(&self) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_RESTART_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_RESTART_VERSION);
        write_raft_string(&mut out, &self.cluster_name)?;
        write_raft_u64(&mut out, self.local_node_id);
        write_raft_u64(&mut out, self.wal_replay_offset);
        write_raft_log_store_artifact(&mut out, &self.log_store)?;
        write_raft_state_machine_artifact(&mut out, &self.state_machine)?;
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn decode_durable_artifact(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        let artifact = Self::decode_durable_artifact_before_restore_validation(bytes)?;
        artifact
            .clone()
            .restore()
            .map_err(|error| raft_artifact_protocol_error(error.to_string()))?;
        Ok(artifact)
    }

    fn decode_durable_artifact_before_restore_validation(
        bytes: &[u8],
    ) -> Result<Self, ControlPlaneError> {
        Self::decode_durable_artifact_before_restore_validation_classified(bytes)
            .map_err(ControlPlaneRaftRestartArtifactDecodeError::into_control_plane_error)
    }

    fn decode_durable_artifact_before_restore_validation_classified(
        bytes: &[u8],
    ) -> Result<Self, ControlPlaneRaftRestartArtifactDecodeError> {
        let min_len =
            CONTROL_PLANE_RAFT_RESTART_MAGIC.len() + 2 + CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN;
        if bytes.len() < min_len {
            return Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
                ControlPlaneRaftRestartArtifactFormatError::Truncated,
            ));
        }
        let (body, checksum_bytes) =
            bytes.split_at(bytes.len() - CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN);
        let expected_checksum = u64::from_be_bytes(
            checksum_bytes
                .try_into()
                .expect("checksum split length is fixed"),
        );
        let actual_checksum = raft_artifact_checksum(body);
        if actual_checksum != expected_checksum {
            return Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
                ControlPlaneRaftRestartArtifactFormatError::ChecksumMismatch {
                    expected: expected_checksum,
                    actual: actual_checksum,
                },
            ));
        }

        let mut reader = RaftArtifactReader::new(body);
        let magic = reader
            .read_exact(CONTROL_PLANE_RAFT_RESTART_MAGIC.len())
            .map_err(ControlPlaneRaftRestartArtifactDecodeError::Invalid)?;
        if magic != CONTROL_PLANE_RAFT_RESTART_MAGIC {
            return Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
                ControlPlaneRaftRestartArtifactFormatError::UnknownMagic,
            ));
        }
        let version = reader
            .read_u16()
            .map_err(ControlPlaneRaftRestartArtifactDecodeError::Invalid)?;
        if version != CONTROL_PLANE_RAFT_RESTART_VERSION {
            return Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
                ControlPlaneRaftRestartArtifactFormatError::UnsupportedVersion(version),
            ));
        }
        let decoded: Result<Self, ControlPlaneError> = (|| {
            let artifact = Self {
                cluster_name: reader.read_string()?,
                local_node_id: reader.read_u64()?,
                wal_replay_offset: reader.read_u64()?,
                log_store: read_raft_log_store_artifact(&mut reader)?,
                state_machine: read_raft_state_machine_artifact(&mut reader)?,
            };
            reader.finish()?;
            Ok(artifact)
        })();
        match decoded {
            Ok(artifact) => Ok(artifact),
            Err(_) if reader.was_truncated() => {
                Err(ControlPlaneRaftRestartArtifactDecodeError::Format(
                    ControlPlaneRaftRestartArtifactFormatError::Truncated,
                ))
            }
            Err(error) => Err(ControlPlaneRaftRestartArtifactDecodeError::Invalid(error)),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn load_durable_artifact(path: &Path) -> Result<Self, ControlPlaneError> {
        let bytes = Self::read_durable_artifact(path)?;
        Self::decode_durable_artifact(&bytes)
    }

    fn load_durable_artifact_for_restore(path: &Path) -> Result<Self, ControlPlaneError> {
        let bytes = Self::read_durable_artifact(path)?;
        Self::decode_durable_artifact_before_restore_validation(&bytes)
    }

    fn read_durable_artifact(path: &Path) -> Result<Vec<u8>, ControlPlaneError> {
        let mut file = File::open(path).map_err(|source| {
            ControlPlaneError::io(
                "open control-plane OpenRaft durable restart artifact",
                source,
            )
        })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|source| {
            ControlPlaneError::io(
                "read control-plane OpenRaft durable restart artifact",
                source,
            )
        })?;
        Ok(bytes)
    }

    #[cfg(test)]
    fn store_durable_artifact(&self, path: &Path) -> Result<(), ControlPlaneError> {
        self.store_durable_artifact_with_metrics(path, None)
    }

    fn store_durable_artifact_with_metrics(
        &self,
        path: &Path,
        metrics: Option<&ControlPlaneRaftCheckpointMetrics>,
    ) -> Result<(), ControlPlaneError> {
        let store_started = Instant::now();
        let result = self.store_durable_artifact_inner(path, metrics);
        let store_elapsed = store_started.elapsed();
        observability::record_control_plane_raft_checkpoint_store(store_elapsed, result.is_ok());
        if let Some(metrics) = metrics {
            metrics.record_store(store_elapsed, result.is_ok());
        }
        result
    }

    fn store_durable_artifact_inner(
        &self,
        path: &Path,
        metrics: Option<&ControlPlaneRaftCheckpointMetrics>,
    ) -> Result<(), ControlPlaneError> {
        self.validate_restart_pair().map_err(|source| {
            ControlPlaneError::io(
                "validate control-plane OpenRaft durable restart artifact",
                source,
            )
        })?;
        let sentinel_path = durable_artifact_sentinel_path(path);
        match ControlPlaneRaftRestartSentinel::load_durable_sentinel(&sentinel_path) {
            Ok(sentinel) => {
                sentinel.validate_identity(&self.cluster_name, self.local_node_id)?;
            }
            Err(ControlPlaneError::Io { diagnostic: source })
                if source.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        ControlPlaneRaftRestartSentinel::for_artifact(self)
            .store_durable_sentinel(&sentinel_path, metrics)?;
        let encode_started = Instant::now();
        let bytes = self.encode_durable_artifact()?;
        let encode_elapsed = encode_started.elapsed();
        observability::record_control_plane_raft_checkpoint_encode(encode_elapsed, bytes.len());
        if let Some(metrics) = metrics {
            metrics.record_encode(encode_elapsed, bytes.len());
        }
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| {
                ControlPlaneError::io(
                    "create control-plane OpenRaft durable restart artifact directory",
                    source,
                )
            })?;
        }
        let tmp_path = durable_artifact_tmp_path(path);
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp_path)
                .map_err(|source| {
                    ControlPlaneError::io(
                        "create control-plane OpenRaft durable restart artifact temp file",
                        source,
                    )
                })?;
            file.write_all(&bytes).map_err(|source| {
                ControlPlaneError::io(
                    "write control-plane OpenRaft durable restart artifact temp file",
                    source,
                )
            })?;
            let sync_started = Instant::now();
            let result = file.sync_all();
            let sync_elapsed = sync_started.elapsed();
            observability::record_control_plane_raft_checkpoint_file_sync(sync_elapsed);
            if let Some(metrics) = metrics {
                metrics.record_file_sync(sync_elapsed);
            }
            result.map_err(|source| {
                ControlPlaneError::io(
                    "sync control-plane OpenRaft durable restart artifact temp file",
                    source,
                )
            })?;
        }
        fs::rename(&tmp_path, path).map_err(|source| {
            ControlPlaneError::io(
                "commit control-plane OpenRaft durable restart artifact",
                source,
            )
        })?;
        sync_durable_artifact_parent(path, metrics)?;
        Ok(())
    }

    fn validate_restart_pair(&self) -> Result<(), io::Error> {
        Self::validate_log_store_state_machine_pair(&self.log_store, &self.state_machine)
    }

    #[cfg(test)]
    fn store_single_node_committed_ahead_bootstrap_artifact_for_test(
        path: &Path,
        cluster_name: impl Into<String>,
        node_id: ControlPlaneRaftNodeId,
        nodes: Vec<(NodeId, String)>,
        pg_ids: Vec<crate::PgId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let log_id = |term, index| LogId::new(LeaderId { term, node_id }, index);
        let bootstrap_membership = ControlPlaneRaftEntry {
            log_id: log_id(0, 0),
            payload: EntryPayload::Membership(Membership::new_with_defaults(
                vec![BTreeSet::from([node_id])],
                [],
            )),
        };
        let blank = ControlPlaneRaftEntry {
            log_id: log_id(3, 1),
            payload: EntryPayload::Blank,
        };
        let bootstrap_command = ControlPlaneRaftEntry {
            log_id: log_id(3, 2),
            payload: EntryPayload::Normal(ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes,
                pg_ids,
            }),
        };

        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(bootstrap_membership.clone())?;
        state_machine.apply_entry(blank.clone())?;

        let mut expected_state_machine = state_machine.clone();
        expected_state_machine.apply_entry(bootstrap_command.clone())?;

        ControlPlaneRaftRestartArtifact {
            cluster_name: cluster_name.into(),
            local_node_id: node_id,
            wal_replay_offset: 0,
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::new_committed(3, node_id)),
                committed: Some(bootstrap_command.log_id),
                last_purged_log_id: None,
                entries: vec![bootstrap_membership, blank, bootstrap_command],
            },
            state_machine: state_machine.export_restart_artifact(),
        }
        .store_durable_artifact(path)?;

        Ok(expected_state_machine.inner().snapshot().clone())
    }

    fn restore(
        self,
    ) -> Result<(ControlPlaneRaftLogStore, ControlPlaneRaftStateMachine), io::Error> {
        #[cfg(test)]
        CONTROL_PLANE_RAFT_RESTORE_ATTEMPTS
            .set(CONTROL_PLANE_RAFT_RESTORE_ATTEMPTS.get().saturating_add(1));
        let log_store =
            ControlPlaneRaftLogStore::from_restart_artifact_in_memory(self.log_store.clone())?;
        let state_machine =
            ControlPlaneRaftStateMachine::from_restart_artifact(self.state_machine.clone())
                .map_err(|error| {
                    control_plane_error_to_io_error("OpenRaft state-machine restart", error)
                })?;
        Self::validate_log_store_state_machine_pair(&self.log_store, &self.state_machine)?;
        Self::validate_cached_snapshot_replays_to_state_machine(
            &self.log_store,
            &self.state_machine,
        )?;
        Ok((log_store, state_machine))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn restore_with_wal_file(
        self,
        wal: ControlPlaneRaftWalFile,
    ) -> Result<(ControlPlaneRaftLogStore, ControlPlaneRaftStateMachine), ControlPlaneError> {
        self.restore_with_wal_file_validated(wal, |_| Ok(()))
    }

    fn restore_with_wal_file_validated(
        self,
        wal: ControlPlaneRaftWalFile,
        validate_replayed_artifact: impl FnOnce(
            &ControlPlaneRaftRestartArtifact,
        ) -> Result<(), ControlPlaneError>,
    ) -> Result<(ControlPlaneRaftLogStore, ControlPlaneRaftStateMachine), ControlPlaneError> {
        #[cfg(test)]
        CONTROL_PLANE_RAFT_RESTORE_ATTEMPTS
            .set(CONTROL_PLANE_RAFT_RESTORE_ATTEMPTS.get().saturating_add(1));
        let log_store_artifact =
            wal.replay_log_store_artifact(ControlPlaneRaftWalReplayConfig {
                base: &self.log_store,
                replay_offset: self.wal_replay_offset,
            })?;
        Self::validate_log_store_state_machine_pair(&log_store_artifact, &self.state_machine)
            .map_err(|source| {
                ControlPlaneError::io(
                    "validate control-plane OpenRaft durable restart artifact after WAL replay",
                    source,
                )
            })?;
        Self::validate_cached_snapshot_replays_to_state_machine(
            &log_store_artifact,
            &self.state_machine,
        )
        .map_err(|source| {
            ControlPlaneError::io(
                "validate control-plane OpenRaft cached snapshot after WAL replay",
                source,
            )
        })?;
        let replayed_artifact = ControlPlaneRaftRestartArtifact {
            cluster_name: self.cluster_name,
            local_node_id: self.local_node_id,
            wal_replay_offset: self.wal_replay_offset,
            log_store: log_store_artifact.clone(),
            state_machine: self.state_machine.clone(),
        };
        validate_replayed_artifact(&replayed_artifact)?;
        let log_store = ControlPlaneRaftLogStore::from_restart_artifact_inner(
            log_store_artifact,
            Some(Arc::new(wal)),
        )
        .map_err(|source| {
            ControlPlaneError::io(
                "restore control-plane OpenRaft WAL-backed log store",
                source,
            )
        })?;
        let state_machine =
            ControlPlaneRaftStateMachine::from_restart_artifact(replayed_artifact.state_machine)
                .map_err(|error| {
                    ControlPlaneError::io(
                        "restore control-plane OpenRaft state machine restart artifact",
                        control_plane_error_to_io_error("OpenRaft state-machine restart", error),
                    )
                })?;
        Ok((log_store, state_machine))
    }

    fn validate_cluster_identity(
        &self,
        expected_cluster_name: &str,
    ) -> Result<(), ControlPlaneError> {
        if self.cluster_name == expected_cluster_name {
            return Ok(());
        }
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft durable restart artifact belongs to cluster {:?}, not configured cluster {:?}",
            self.cluster_name, expected_cluster_name
        )))
    }

    fn validate_local_node_identity(
        &self,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        if self.local_node_id == local_node_id {
            return Ok(());
        }
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft durable restart artifact belongs to local OpenRaft node {}, not configured local node {local_node_id}",
            self.local_node_id
        )))
    }

    fn validate_single_node_local_identity(
        &self,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        self.validate_local_node_identity(local_node_id)?;
        if let Some(vote) = self.log_store.vote {
            Self::validate_single_node_leader_id("persisted vote", vote.leader_id, local_node_id)?;
        }
        if let Some(committed) = self.log_store.committed {
            Self::validate_single_node_log_id("committed log id", committed, local_node_id)?;
        }
        if let Some(last_purged_log_id) = self.log_store.last_purged_log_id {
            Self::validate_single_node_log_id(
                "purged boundary log id",
                last_purged_log_id,
                local_node_id,
            )?;
        }
        for entry in &self.log_store.entries {
            Self::validate_single_node_log_id("retained log entry", entry.log_id, local_node_id)?;
            if let EntryPayload::Membership(membership) = &entry.payload {
                Self::validate_single_node_membership(
                    "retained log entry membership",
                    membership,
                    local_node_id,
                )?;
            }
        }
        if let Some(last_applied) = self.state_machine.last_applied {
            Self::validate_single_node_log_id(
                "state-machine applied log id",
                last_applied,
                local_node_id,
            )?;
        }
        match self.state_machine.last_membership.log_id() {
            Some(last_membership_log_id) => {
                Self::validate_single_node_log_id(
                    "state-machine membership log id",
                    *last_membership_log_id,
                    local_node_id,
                )?;
                Self::validate_single_node_membership(
                    "state-machine membership",
                    self.state_machine.last_membership.membership(),
                    local_node_id,
                )?;
            }
            None => {
                Self::validate_uninitialized_membership(
                    "state-machine membership",
                    self.state_machine.last_membership.membership(),
                )?;
            }
        }
        Ok(())
    }

    fn validate_peer_policy_membership(
        &self,
        peer_policy: &ControlPlaneRaftPeerTransportPolicy,
    ) -> Result<(), ControlPlaneError> {
        for entry in &self.log_store.entries {
            if let EntryPayload::Membership(membership) = &entry.payload {
                peer_policy.validate_configured_membership("retained log entry", membership)?;
            }
        }
        match self.state_machine.last_membership.log_id() {
            Some(_) => peer_policy.validate_configured_membership(
                "state-machine membership",
                self.state_machine.last_membership.membership(),
            )?,
            None => Self::validate_uninitialized_membership(
                "state-machine membership",
                self.state_machine.last_membership.membership(),
            )?,
        }
        if let Some(snapshot) = &self.state_machine.current_snapshot {
            match snapshot.meta.last_membership.log_id() {
                Some(_) => peer_policy.validate_configured_membership(
                    "cached snapshot membership",
                    snapshot.meta.last_membership.membership(),
                )?,
                None => Self::validate_uninitialized_membership(
                    "cached snapshot membership",
                    snapshot.meta.last_membership.membership(),
                )?,
            }
        }
        Ok(())
    }

    fn validate_static_initial_topology_restore(
        &self,
        peer_policy: &ControlPlaneRaftPeerTransportPolicy,
        pending_static_bootstrap: Option<&ControlPlaneCommand>,
    ) -> Result<(), ControlPlaneError> {
        if peer_policy.topology_identity().is_none() {
            if pending_static_bootstrap.is_some() {
                return Err(raft_artifact_protocol_error(
                    "pending static initialization requires a peer-policy topology identity",
                ));
            }
            return Ok(());
        }
        let snapshot = self.state_machine.inner.snapshot();
        if snapshot.initial_topology().is_some() {
            return validate_captured_static_initial_topology(snapshot, peer_policy);
        }

        let Some(expected_bootstrap) = pending_static_bootstrap else {
            return validate_captured_static_initial_topology(snapshot, peer_policy);
        };
        if snapshot.nodes().next().is_some() || snapshot.pgs().next().is_some() {
            return Err(raft_artifact_protocol_error(
                "pending static OpenRaft restart artifact has control-plane state without an initial topology certificate",
            ));
        }
        let expected_snapshot = ClusterControlSnapshot::empty()
            .apply_control_plane_command(expected_bootstrap.clone())?
            .into_snapshot();
        validate_captured_static_initial_topology(&expected_snapshot, peer_policy)?;
        for entry in &self.log_store.entries {
            if let EntryPayload::Normal(command) = &entry.payload {
                if command != expected_bootstrap {
                    return Err(raft_artifact_protocol_error(
                        "pending static OpenRaft restart artifact contains a normal entry other than the configured certified bootstrap",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_single_node_log_id(
        context: &'static str,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        Self::validate_single_node_leader_id(context, *log_id.committed_leader_id(), local_node_id)
    }

    fn validate_single_node_leader_id(
        context: &'static str,
        leader_id: ControlPlaneRaftLeaderId,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        if leader_id.node_id == local_node_id {
            return Ok(());
        }
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft durable restart artifact {context} belongs to OpenRaft node {}, not local node {local_node_id}",
            leader_id.node_id
        )))
    }

    fn validate_single_node_membership(
        context: &'static str,
        membership: &Membership<ControlPlaneRaftNodeId, BasicNode>,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        let expected_voters = BTreeSet::from([local_node_id]);
        let configs = membership.get_joint_config();
        let learners = membership.learner_ids().collect::<BTreeSet<_>>();
        if configs.len() == 1 && configs.first() == Some(&expected_voters) && learners.is_empty() {
            return Ok(());
        }
        let voters = membership.voter_ids().collect::<BTreeSet<_>>();
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft durable restart artifact {context} must be single-node membership for local node {local_node_id}; voters={voters:?} learners={learners:?}"
        )))
    }

    fn validate_uninitialized_membership(
        context: &'static str,
        membership: &Membership<ControlPlaneRaftNodeId, BasicNode>,
    ) -> Result<(), ControlPlaneError> {
        let configs = membership.get_joint_config();
        let learners = membership.learner_ids().collect::<BTreeSet<_>>();
        if configs.is_empty() && learners.is_empty() {
            return Ok(());
        }
        let voters = membership.voter_ids().collect::<BTreeSet<_>>();
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft durable restart artifact {context} without log id must be empty uninitialized membership; voters={voters:?} learners={learners:?}"
        )))
    }

    fn validate_log_store_state_machine_pair(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        state_machine: &ControlPlaneRaftStateMachineRestartArtifact,
    ) -> Result<(), io::Error> {
        if let Some(last_purged_log_id) = log_store.last_purged_log_id {
            match state_machine.last_applied {
                Some(last_applied) if last_applied.index() > last_purged_log_id.index() => {}
                Some(last_applied) if last_applied == last_purged_log_id => {}
                Some(last_applied) => {
                    return Err(raft_log_store_error(format!(
                        "control-plane OpenRaft state-machine applied log id {last_applied} is behind purged boundary {last_purged_log_id}"
                    )));
                }
                None => {
                    return Err(raft_log_store_error(format!(
                        "control-plane OpenRaft state-machine has no applied log id but log is purged through {last_purged_log_id}"
                    )));
                }
            }
        }

        let Some(last_applied) = state_machine.last_applied else {
            return Ok(());
        };
        let Some(known_applied) =
            Self::log_store_artifact_log_id_at(log_store, last_applied.index())
        else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} is not retained or purged in the log store"
            )));
        };
        if known_applied != last_applied {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} does not match log-store log id {known_applied}"
            )));
        }

        if is_openraft_bootstrap_log_id(last_applied) {
            return Ok(());
        }
        let Some(committed) = log_store.committed else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} has no committed restart gate"
            )));
        };
        if last_applied.index() > committed.index() {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} is after committed restart gate {committed}"
            )));
        }
        if last_applied.index() == committed.index() && last_applied != committed {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} conflicts with committed restart gate {committed}"
            )));
        }
        Ok(())
    }

    fn validate_cached_snapshot_replays_to_state_machine(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        state_machine: &ControlPlaneRaftStateMachineRestartArtifact,
    ) -> Result<(), io::Error> {
        let Some(snapshot) = state_machine.current_snapshot.as_ref() else {
            return Ok(());
        };
        if snapshot.meta.last_log_id == state_machine.last_applied {
            return Ok(());
        }
        let Some(target_last_applied) = state_machine.last_applied else {
            return Err(raft_log_store_error(
                "cached OpenRaft snapshot is present but state-machine has no applied log id",
            ));
        };
        Self::validate_cached_snapshot_membership_at_boundary(
            log_store,
            snapshot,
            target_last_applied,
        )?;

        let control_plane_snapshot_log_id = match snapshot.meta.last_log_id {
            Some(log_id) if is_openraft_bootstrap_log_id(log_id) => None,
            Some(log_id) => control_plane_log_id_from_raft(log_id)
                .ok_or_else(|| {
                    raft_log_store_error(format!(
                        "invalid cached OpenRaft snapshot last_log_id: {log_id}"
                    ))
                })
                .map(Some)?,
            None => None,
        };
        let mut snapshot_inner = ReplicatedControlPlaneStateMachine::empty();
        snapshot_inner
            .install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
                control_plane_snapshot_log_id,
                snapshot.snapshot.get_ref().clone(),
            ))
            .map_err(|error| {
                control_plane_error_to_io_error("OpenRaft cached snapshot replay base", error)
            })?;
        let mut replayed = ControlPlaneRaftStateMachine::new(
            snapshot_inner,
            snapshot.meta.last_log_id,
            snapshot.meta.last_membership.clone(),
        )
        .map_err(|error| {
            control_plane_error_to_io_error("OpenRaft cached snapshot replay state", error)
        })?;

        let next_index = snapshot
            .meta
            .last_log_id
            .map_or(Some(0), |log_id| log_id.index().checked_add(1))
            .ok_or_else(|| {
                raft_log_store_error(
                    "cached OpenRaft snapshot last_log_id cannot be followed by a suffix",
                )
            })?;
        for index in next_index..=target_last_applied.index() {
            let entry = Self::log_store_artifact_entry_at(log_store, index).ok_or_else(|| {
                raft_log_store_error(format!(
                    "cached OpenRaft snapshot cannot replay missing retained suffix entry at index {index}"
                ))
            })?;
            replayed.apply_entry(entry.clone()).map_err(|error| {
                control_plane_error_to_io_error("OpenRaft cached snapshot suffix replay", error)
            })?;
        }
        if replayed.last_applied() != state_machine.last_applied
            || replayed.last_membership() != &state_machine.last_membership
            || replayed.inner().snapshot() != state_machine.inner.snapshot()
            || replayed.inner().last_applied() != state_machine.inner.last_applied()
        {
            return Err(raft_log_store_error(
                "cached OpenRaft snapshot plus retained suffix does not match state-machine restart payload",
            ));
        }
        Ok(())
    }

    fn validate_cached_snapshot_membership_at_boundary(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        snapshot: &ControlPlaneRaftSnapshot,
        target_last_applied: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let Some(snapshot_log_id) = snapshot.meta.last_log_id else {
            return Ok(());
        };
        let suffix_has_membership = log_store.entries.iter().any(|entry| {
            entry.log_id.index() > snapshot_log_id.index()
                && entry.log_id.index() <= target_last_applied.index()
                && matches!(&entry.payload, EntryPayload::Membership(_))
        });
        if !suffix_has_membership {
            return Ok(());
        }

        let Some(expected_membership) =
            Self::log_store_artifact_membership_at(log_store, snapshot_log_id.index())
        else {
            return Err(raft_log_store_error(format!(
                "cached OpenRaft snapshot membership at {snapshot_log_id} cannot be validated from retained log prefix before membership-changing suffix"
            )));
        };
        if expected_membership.log_id() != snapshot.meta.last_membership.log_id()
            || expected_membership.membership() != snapshot.meta.last_membership.membership()
        {
            return Err(raft_log_store_error(format!(
                "cached OpenRaft snapshot membership at {snapshot_log_id} does not match retained log prefix"
            )));
        }
        Ok(())
    }

    fn log_store_artifact_log_id_at(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        index: u64,
    ) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        if let Some(last_purged_log_id) = log_store.last_purged_log_id {
            if index < last_purged_log_id.index() {
                return None;
            }
            if index == last_purged_log_id.index() {
                return Some(last_purged_log_id);
            }
        }
        log_store
            .entries
            .iter()
            .find(|entry| entry.log_id.index() == index)
            .map(|entry| entry.log_id)
    }

    fn log_store_artifact_entry_at(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        index: u64,
    ) -> Option<&ControlPlaneRaftEntry> {
        log_store
            .entries
            .iter()
            .find(|entry| entry.log_id.index() == index)
    }

    fn log_store_artifact_membership_at(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        index: u64,
    ) -> Option<StoredMembershipOf<ControlPlaneRaftTypeConfig>> {
        if log_store.last_purged_log_id.is_some() {
            return None;
        }
        let mut membership = StoredMembership::default();
        let mut expected_index = 0;
        for entry in &log_store.entries {
            if entry.log_id.index() != expected_index {
                return None;
            }
            if entry.log_id.index() > index {
                break;
            }
            if let EntryPayload::Membership(entry_membership) = &entry.payload {
                membership = StoredMembership::new(Some(entry.log_id), entry_membership.clone());
            }
            expected_index = expected_index.checked_add(1)?;
        }
        if expected_index > index {
            Some(membership)
        } else {
            None
        }
    }
}

impl ControlPlaneRaftRestartSentinel {
    fn for_artifact(artifact: &ControlPlaneRaftRestartArtifact) -> Self {
        Self {
            cluster_name: artifact.cluster_name.clone(),
            local_node_id: artifact.local_node_id,
        }
    }

    fn validate_identity(
        &self,
        expected_cluster_name: &str,
        expected_local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        if self.cluster_name != expected_cluster_name {
            return Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft durable restart sentinel belongs to cluster {:?}, not configured cluster {:?}",
                self.cluster_name, expected_cluster_name
            )));
        }
        if self.local_node_id != expected_local_node_id {
            return Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft durable restart sentinel belongs to local OpenRaft node {}, not configured local node {expected_local_node_id}",
                self.local_node_id
            )));
        }
        Ok(())
    }

    fn encode_durable_sentinel(&self) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_RESTART_SENTINEL_VERSION);
        write_raft_string(&mut out, &self.cluster_name)?;
        write_raft_u64(&mut out, self.local_node_id);
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    fn decode_durable_sentinel_classified(
        bytes: &[u8],
    ) -> Result<Self, ControlPlaneRaftRestartSentinelFormatError> {
        let min_len = CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC.len()
            + std::mem::size_of::<u16>()
            + std::mem::size_of::<u32>()
            + std::mem::size_of::<ControlPlaneRaftNodeId>()
            + CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN;
        if bytes.len() < min_len {
            return Err(ControlPlaneRaftRestartSentinelFormatError::Truncated);
        }
        let (body, checksum_bytes) =
            bytes.split_at(bytes.len() - CONTROL_PLANE_RAFT_RESTART_CHECKSUM_LEN);
        let expected_checksum = u64::from_be_bytes(
            checksum_bytes
                .try_into()
                .expect("checksum split length is fixed"),
        );
        let actual_checksum = raft_artifact_checksum(body);
        if actual_checksum != expected_checksum {
            return Err(
                ControlPlaneRaftRestartSentinelFormatError::ChecksumMismatch {
                    expected: expected_checksum,
                    actual: actual_checksum,
                },
            );
        }

        let mut offset = 0usize;
        let magic = read_control_plane_raft_restart_sentinel_bytes(
            body,
            &mut offset,
            CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC.len(),
        )?;
        if magic != CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC {
            return Err(ControlPlaneRaftRestartSentinelFormatError::UnknownMagic);
        }
        let version = u16::from_be_bytes(
            read_control_plane_raft_restart_sentinel_bytes(
                body,
                &mut offset,
                std::mem::size_of::<u16>(),
            )?
            .try_into()
            .expect("sentinel version has fixed width"),
        );
        if version != CONTROL_PLANE_RAFT_RESTART_SENTINEL_VERSION {
            return Err(ControlPlaneRaftRestartSentinelFormatError::UnsupportedVersion(version));
        }
        let cluster_name_len = u32::from_be_bytes(
            read_control_plane_raft_restart_sentinel_bytes(
                body,
                &mut offset,
                std::mem::size_of::<u32>(),
            )?
            .try_into()
            .expect("sentinel cluster-name length has fixed width"),
        );
        let cluster_name_len = usize::try_from(cluster_name_len)
            .map_err(|_| ControlPlaneRaftRestartSentinelFormatError::Truncated)?;
        let cluster_name = std::str::from_utf8(read_control_plane_raft_restart_sentinel_bytes(
            body,
            &mut offset,
            cluster_name_len,
        )?)
        .map_err(|_| ControlPlaneRaftRestartSentinelFormatError::InvalidClusterName)?
        .to_owned();
        let local_node_id = u64::from_be_bytes(
            read_control_plane_raft_restart_sentinel_bytes(
                body,
                &mut offset,
                std::mem::size_of::<ControlPlaneRaftNodeId>(),
            )?
            .try_into()
            .expect("sentinel node ID has fixed width"),
        );
        if offset != body.len() {
            return Err(ControlPlaneRaftRestartSentinelFormatError::TrailingBytes);
        }

        let sentinel = Self {
            cluster_name,
            local_node_id,
        };
        Ok(sentinel)
    }

    fn decode_durable_sentinel(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        Self::decode_durable_sentinel_classified(bytes)
            .map_err(|error| raft_artifact_protocol_error(error.to_string()))
    }

    fn load_durable_sentinel(path: &Path) -> Result<Self, ControlPlaneError> {
        let mut file = File::open(path).map_err(|source| {
            ControlPlaneError::io(
                "open control-plane OpenRaft durable restart sentinel",
                source,
            )
        })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|source| {
            ControlPlaneError::io(
                "read control-plane OpenRaft durable restart sentinel",
                source,
            )
        })?;
        Self::decode_durable_sentinel(&bytes)
    }

    fn store_durable_sentinel(
        &self,
        path: &Path,
        metrics: Option<&ControlPlaneRaftCheckpointMetrics>,
    ) -> Result<(), ControlPlaneError> {
        let bytes = self.encode_durable_sentinel()?;
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| {
                ControlPlaneError::io(
                    "create control-plane OpenRaft durable restart sentinel directory",
                    source,
                )
            })?;
        }
        let tmp_path = durable_artifact_tmp_path(path);
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp_path)
                .map_err(|source| {
                    ControlPlaneError::io(
                        "create control-plane OpenRaft durable restart sentinel temp file",
                        source,
                    )
                })?;
            file.write_all(&bytes).map_err(|source| {
                ControlPlaneError::io(
                    "write control-plane OpenRaft durable restart sentinel temp file",
                    source,
                )
            })?;
            let sync_started = Instant::now();
            let result = file.sync_all();
            let sync_elapsed = sync_started.elapsed();
            observability::record_control_plane_raft_checkpoint_file_sync(sync_elapsed);
            if let Some(metrics) = metrics {
                metrics.record_file_sync(sync_elapsed);
            }
            result.map_err(|source| {
                ControlPlaneError::io(
                    "sync control-plane OpenRaft durable restart sentinel temp file",
                    source,
                )
            })?;
        }
        fs::rename(&tmp_path, path).map_err(|source| {
            ControlPlaneError::io(
                "commit control-plane OpenRaft durable restart sentinel",
                source,
            )
        })?;
        sync_durable_artifact_parent(path, metrics)?;
        Ok(())
    }
}

fn durable_artifact_tmp_path(path: &Path) -> PathBuf {
    durable_artifact_tmp_path_for_process(path, std::process::id())
}

fn durable_artifact_tmp_path_for_process(path: &Path, process_id: u32) -> PathBuf {
    durable_artifact_companion_path(path, &format!(".tmp.{process_id}"))
}

fn durable_artifact_sentinel_path(path: &Path) -> PathBuf {
    durable_artifact_companion_path(path, ".sentinel")
}

#[must_use]
fn durable_artifact_wal_path(path: &Path) -> PathBuf {
    durable_artifact_companion_path(path, ".wal")
}

fn durable_artifact_companion_path(path: &Path, suffix: &str) -> PathBuf {
    let mut companion = path.as_os_str().to_os_string();
    companion.push(suffix);
    PathBuf::from(companion)
}

fn sync_durable_artifact_parent(
    path: &Path,
    metrics: Option<&ControlPlaneRaftCheckpointMetrics>,
) -> Result<(), ControlPlaneError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let sync_started = Instant::now();
    let result = File::open(parent).and_then(|directory| directory.sync_all());
    let sync_elapsed = sync_started.elapsed();
    observability::record_control_plane_raft_checkpoint_directory_sync(sync_elapsed);
    if let Some(metrics) = metrics {
        metrics.record_directory_sync(sync_elapsed);
    }
    result.map_err(|source| {
        ControlPlaneError::io(
            "sync control-plane OpenRaft durable restart artifact directory",
            source,
        )
    })
}

fn inject_control_plane_raft_wal_file_sync_failure(path: &Path) -> Result<(), ControlPlaneError> {
    #[cfg(test)]
    {
        let gate = CONTROL_PLANE_RAFT_WAL_FILE_SYNC_GATES
            .lock()
            .expect("test WAL file-sync gate lock should not be poisoned")
            .get(path)
            .cloned();
        if let Some(gate) = gate {
            let (state, condition) = &*gate;
            let mut state = state
                .lock()
                .expect("test WAL file-sync gate state should not be poisoned");
            state.entered = true;
            condition.notify_all();
            while !state.released {
                state = condition
                    .wait(state)
                    .expect("test WAL file-sync gate state should not be poisoned");
            }
        }

        let mut injected_path = CONTROL_PLANE_RAFT_WAL_FAIL_NEXT_FILE_SYNC
            .lock()
            .expect("test WAL file-sync fault lock should not be poisoned");
        if injected_path.as_deref() == Some(path) {
            *injected_path = None;
            return Err(ControlPlaneError::io(
                "sync control-plane OpenRaft WAL",
                io::Error::other("injected control-plane OpenRaft WAL sync failure"),
            ));
        }
    }

    let _ = path;
    Ok(())
}

fn inject_control_plane_raft_wal_durable_publication_delay(
    path: &Path,
    record: &ControlPlaneRaftWalRecord,
) {
    #[cfg(test)]
    {
        if !matches!(record, ControlPlaneRaftWalRecord::Purge(_)) {
            return;
        }
        let gate = CONTROL_PLANE_RAFT_WAL_DURABLE_PUBLICATION_GATES
            .lock()
            .expect("test WAL durable-publication gate lock should not be poisoned")
            .get(path)
            .cloned();
        if let Some(gate) = gate {
            let (state, condition) = &*gate;
            let mut state = state
                .lock()
                .expect("test WAL durable-publication gate state should not be poisoned");
            state.entered = true;
            condition.notify_all();
            while !state.released {
                state = condition
                    .wait(state)
                    .expect("test WAL durable-publication gate state should not be poisoned");
            }
        }
    }

    let _ = (path, record);
}

fn sync_control_plane_raft_wal_parent(path: &Path) -> Result<(), ControlPlaneError> {
    #[cfg(test)]
    {
        let mut injected_path = CONTROL_PLANE_RAFT_WAL_FAIL_NEXT_PARENT_SYNC
            .lock()
            .expect("test WAL parent-sync fault lock should not be poisoned");
        if injected_path.as_deref() == Some(path) {
            *injected_path = None;
            return Err(ControlPlaneError::io(
                "sync control-plane OpenRaft WAL directory",
                io::Error::other("injected control-plane OpenRaft WAL directory sync failure"),
            ));
        }
    }

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| {
            ControlPlaneError::io("sync control-plane OpenRaft WAL directory", source)
        })
}

fn append_raft_artifact_checksum(out: &mut Vec<u8>) {
    let checksum = raft_artifact_checksum(out);
    write_raft_u64(out, checksum);
}

fn raft_artifact_checksum(body: &[u8]) -> u64 {
    checksum::crc64::checksum(body)
}

fn write_raft_log_store_artifact(
    out: &mut Vec<u8>,
    artifact: &ControlPlaneRaftLogStoreRestartArtifact,
) -> Result<(), ControlPlaneError> {
    write_raft_restart_option_vote(
        out,
        ControlPlaneRaftRestartOptionalField::LogStoreVote,
        artifact.vote,
    );
    write_raft_restart_option_log_id(
        out,
        ControlPlaneRaftRestartOptionalField::LogStoreCommitted,
        artifact.committed,
    );
    write_raft_restart_option_log_id(
        out,
        ControlPlaneRaftRestartOptionalField::LogStoreLastPurged,
        artifact.last_purged_log_id,
    );
    write_raft_u32(
        out,
        raft_len_as_u32(artifact.entries.len(), "raft log entries")?,
    );
    for entry in &artifact.entries {
        write_raft_entry(out, entry)?;
    }
    Ok(())
}

fn read_raft_log_store_artifact(
    reader: &mut RaftArtifactReader<'_>,
) -> Result<ControlPlaneRaftLogStoreRestartArtifact, ControlPlaneError> {
    let vote = reader.read_option_vote()?;
    let committed = reader.read_option_log_id()?;
    let last_purged_log_id = reader.read_option_log_id()?;
    let entry_count = reader.read_collection_len("raft log entries", RAFT_ENTRY_MIN_LEN)?;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries.push(reader.read_entry()?);
    }
    Ok(ControlPlaneRaftLogStoreRestartArtifact {
        vote,
        committed,
        last_purged_log_id,
        entries,
    })
}

fn write_raft_wal_record(
    out: &mut Vec<u8>,
    record: &ControlPlaneRaftWalRecord,
) -> Result<(), ControlPlaneError> {
    write_raft_u8(out, record.kind().as_u8());
    match record {
        ControlPlaneRaftWalRecord::SaveVote(vote) => {
            write_raft_vote(out, *vote);
        }
        ControlPlaneRaftWalRecord::Append(entries) => {
            write_raft_u32(
                out,
                raft_len_as_u32(entries.len(), "raft WAL append entries")?,
            );
            for entry in entries {
                write_raft_entry(out, entry)?;
            }
        }
        ControlPlaneRaftWalRecord::SaveCommitted(committed) => {
            write_raft_option_log_id(out, *committed);
        }
        ControlPlaneRaftWalRecord::TruncateAfter(last_log_id) => {
            write_raft_option_log_id(out, *last_log_id);
        }
        ControlPlaneRaftWalRecord::Purge(log_id) => {
            write_raft_log_id(out, *log_id);
        }
    }
    Ok(())
}

fn write_raft_state_machine_artifact(
    out: &mut Vec<u8>,
    artifact: &ControlPlaneRaftStateMachineRestartArtifact,
) -> Result<(), ControlPlaneError> {
    write_raft_restart_option_log_id(
        out,
        ControlPlaneRaftRestartOptionalField::StateMachineLastApplied,
        artifact.last_applied,
    );
    write_raft_restart_option_log_id(
        out,
        ControlPlaneRaftRestartOptionalField::StateMachineLastMembershipLogId,
        *artifact.last_membership.log_id(),
    );
    write_raft_membership(out, artifact.last_membership.membership())?;

    let mut inner = artifact.inner.clone();
    let snapshot_artifact = inner.build_snapshot_artifact()?;
    write_raft_bytes(out, snapshot_artifact.payload())?;
    write_raft_restart_option_snapshot(out, artifact.current_snapshot.as_ref())?;
    Ok(())
}

fn read_raft_state_machine_artifact(
    reader: &mut RaftArtifactReader<'_>,
) -> Result<ControlPlaneRaftStateMachineRestartArtifact, ControlPlaneError> {
    let last_applied = reader.read_option_log_id()?;
    let last_membership = reader.read_stored_membership()?;
    let snapshot_payload = reader.read_bytes("raft state-machine snapshot")?.to_vec();
    let control_plane_last_applied = match last_applied {
        Some(log_id) if is_openraft_bootstrap_log_id(log_id) => None,
        Some(log_id) => Some(control_plane_log_id_from_raft(log_id).ok_or_else(|| {
            raft_artifact_protocol_error(format!(
                "invalid OpenRaft state-machine durable last-applied log id: {log_id}"
            ))
        })?),
        None => None,
    };
    let mut inner = ReplicatedControlPlaneStateMachine::empty();
    inner.install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
        control_plane_last_applied,
        snapshot_payload,
    ))?;
    let current_snapshot = reader.read_option_snapshot()?;
    Ok(ControlPlaneRaftStateMachineRestartArtifact {
        inner,
        last_applied,
        last_membership,
        current_snapshot,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RaftWireBoolean {
    False,
    True,
}

impl RaftWireBoolean {
    #[cfg(test)]
    const ALL: [Self; 2] = [Self::False, Self::True];

    fn from_bool(value: bool) -> Self {
        if value {
            Self::True
        } else {
            Self::False
        }
    }

    fn as_bool(self) -> bool {
        match self {
            Self::False => false,
            Self::True => true,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::False => 0,
            Self::True => 1,
        }
    }

    fn from_u8(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(Self::False),
            1 => Ok(Self::True),
            value => Err(value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RaftWireOptionTag {
    Absent,
    Present,
}

impl RaftWireOptionTag {
    #[cfg(test)]
    const ALL: [Self; 2] = [Self::Absent, Self::Present];

    fn from_present(present: bool) -> Self {
        if present {
            Self::Present
        } else {
            Self::Absent
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::Absent => 0,
            Self::Present => 1,
        }
    }

    fn from_u8(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(Self::Absent),
            1 => Ok(Self::Present),
            value => Err(value),
        }
    }
}

macro_rules! define_control_plane_raft_peer_rpc_optional_fields {
    ($($field:ident),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum ControlPlaneRaftPeerRpcOptionalField {
            $($field),+
        }

        impl ControlPlaneRaftPeerRpcOptionalField {
            const ALL: &'static [Self] = &[$(Self::$field),+];
        }
    };
}

define_control_plane_raft_peer_rpc_optional_fields!(
    PeerIdentity,
    PeerTopology,
    AppendEntriesPrevLogId,
    AppendEntriesLeaderCommit,
    AppendEntriesPartialSuccessLogId,
    VoteRequestLastLogId,
    VoteResponseLastLogId,
    TransferLeaderRequestLastLogId,
    TransferLeaderExpectedLogId,
    TransferLeaderActualLogId,
    SnapshotLastLogId,
    SnapshotMembershipLogId,
);

macro_rules! define_control_plane_raft_restart_optional_fields {
    ($($field:ident),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum ControlPlaneRaftRestartOptionalField {
            $($field),+
        }

        impl ControlPlaneRaftRestartOptionalField {
            const ALL: &'static [Self] = &[$(Self::$field),+];
        }
    };
}

define_control_plane_raft_restart_optional_fields!(
    LogStoreVote,
    LogStoreCommitted,
    LogStoreLastPurged,
    StateMachineLastApplied,
    StateMachineLastMembershipLogId,
    CurrentSnapshot,
    CurrentSnapshotLastLogId,
    CurrentSnapshotMembershipLogId,
);

#[cfg(test)]
thread_local! {
    static CONTROL_PLANE_RAFT_PEER_RPC_OPTION_CAPTURE: std::cell::RefCell<
        Option<Vec<(ControlPlaneRaftPeerRpcOptionalField, RaftWireOptionTag)>>,
    > = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
struct ControlPlaneRaftPeerRpcOptionCapture {
    active: bool,
}

#[cfg(test)]
thread_local! {
    static CONTROL_PLANE_RAFT_RESTART_OPTION_CAPTURE: std::cell::RefCell<
        Option<Vec<(ControlPlaneRaftRestartOptionalField, RaftWireOptionTag)>>,
    > = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
struct ControlPlaneRaftRestartOptionCapture {
    active: bool,
}

#[cfg(test)]
impl ControlPlaneRaftRestartOptionCapture {
    fn begin() -> Self {
        CONTROL_PLANE_RAFT_RESTART_OPTION_CAPTURE.with(|capture| {
            let previous = capture.borrow_mut().replace(Vec::new());
            assert!(
                previous.is_none(),
                "restart-artifact option capture is already active"
            );
        });
        Self { active: true }
    }

    fn finish(mut self) -> Vec<(ControlPlaneRaftRestartOptionalField, RaftWireOptionTag)> {
        self.active = false;
        CONTROL_PLANE_RAFT_RESTART_OPTION_CAPTURE.with(|capture| {
            capture
                .borrow_mut()
                .take()
                .expect("restart-artifact option capture must remain active")
        })
    }
}

#[cfg(test)]
impl Drop for ControlPlaneRaftRestartOptionCapture {
    fn drop(&mut self) {
        if self.active {
            CONTROL_PLANE_RAFT_RESTART_OPTION_CAPTURE.with(|capture| {
                capture.borrow_mut().take();
            });
        }
    }
}

#[cfg(test)]
impl ControlPlaneRaftPeerRpcOptionCapture {
    fn begin() -> Self {
        CONTROL_PLANE_RAFT_PEER_RPC_OPTION_CAPTURE.with(|capture| {
            let previous = capture.borrow_mut().replace(Vec::new());
            assert!(
                previous.is_none(),
                "peer RPC option capture is already active"
            );
        });
        Self { active: true }
    }

    fn finish(mut self) -> Vec<(ControlPlaneRaftPeerRpcOptionalField, RaftWireOptionTag)> {
        self.active = false;
        CONTROL_PLANE_RAFT_PEER_RPC_OPTION_CAPTURE.with(|capture| {
            capture
                .borrow_mut()
                .take()
                .expect("peer RPC option capture must remain active")
        })
    }
}

#[cfg(test)]
impl Drop for ControlPlaneRaftPeerRpcOptionCapture {
    fn drop(&mut self) {
        if self.active {
            CONTROL_PLANE_RAFT_PEER_RPC_OPTION_CAPTURE.with(|capture| {
                capture.borrow_mut().take();
            });
        }
    }
}

fn write_raft_peer_option_tag(
    out: &mut Vec<u8>,
    field: ControlPlaneRaftPeerRpcOptionalField,
    present: bool,
) {
    debug_assert!(ControlPlaneRaftPeerRpcOptionalField::ALL.contains(&field));
    let arm = RaftWireOptionTag::from_present(present);
    #[cfg(test)]
    CONTROL_PLANE_RAFT_PEER_RPC_OPTION_CAPTURE.with(|capture| {
        if let Some(observations) = capture.borrow_mut().as_mut() {
            observations.push((field, arm));
        }
    });
    write_raft_u8(out, arm.as_u8());
}

fn write_raft_restart_option_tag(
    out: &mut Vec<u8>,
    field: ControlPlaneRaftRestartOptionalField,
    present: bool,
) {
    debug_assert!(ControlPlaneRaftRestartOptionalField::ALL.contains(&field));
    let arm = RaftWireOptionTag::from_present(present);
    #[cfg(test)]
    CONTROL_PLANE_RAFT_RESTART_OPTION_CAPTURE.with(|capture| {
        if let Some(observations) = capture.borrow_mut().as_mut() {
            observations.push((field, arm));
        }
    });
    write_raft_u8(out, arm.as_u8());
}

fn write_raft_restart_option_vote(
    out: &mut Vec<u8>,
    field: ControlPlaneRaftRestartOptionalField,
    vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
) {
    write_raft_restart_option_tag(out, field, vote.is_some());
    if let Some(vote) = vote {
        write_raft_vote(out, vote);
    }
}

fn write_raft_restart_option_log_id(
    out: &mut Vec<u8>,
    field: ControlPlaneRaftRestartOptionalField,
    log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
) {
    write_raft_restart_option_tag(out, field, log_id.is_some());
    if let Some(log_id) = log_id {
        write_raft_log_id(out, log_id);
    }
}

fn write_raft_peer_option_log_id(
    out: &mut Vec<u8>,
    field: ControlPlaneRaftPeerRpcOptionalField,
    log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
) {
    write_raft_peer_option_tag(out, field, log_id.is_some());
    if let Some(log_id) = log_id {
        write_raft_log_id(out, log_id);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ControlPlaneRaftEntryPayloadTag {
    Blank,
    Membership,
    Normal,
}

impl ControlPlaneRaftEntryPayloadTag {
    #[cfg(test)]
    const ALL: [Self; 3] = [Self::Blank, Self::Membership, Self::Normal];

    fn as_u8(self) -> u8 {
        match self {
            Self::Blank => 0,
            Self::Membership => 1,
            Self::Normal => 2,
        }
    }

    fn from_u8(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(Self::Blank),
            1 => Ok(Self::Membership),
            2 => Ok(Self::Normal),
            value => Err(value),
        }
    }
}

fn write_raft_restart_option_snapshot(
    out: &mut Vec<u8>,
    snapshot: Option<&ControlPlaneRaftSnapshot>,
) -> Result<(), ControlPlaneError> {
    write_raft_restart_option_tag(
        out,
        ControlPlaneRaftRestartOptionalField::CurrentSnapshot,
        snapshot.is_some(),
    );
    match snapshot {
        None => {}
        Some(snapshot) => {
            write_raft_snapshot(out, snapshot, RaftSnapshotEncodingContext::Restart)?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RaftSnapshotEncodingContext {
    Restart,
    Peer,
}

fn write_raft_snapshot(
    out: &mut Vec<u8>,
    snapshot: &ControlPlaneRaftSnapshot,
    context: RaftSnapshotEncodingContext,
) -> Result<(), ControlPlaneError> {
    write_raft_snapshot_meta(out, &snapshot.meta, context)?;
    write_raft_bytes(out, snapshot.snapshot.get_ref())?;
    Ok(())
}

fn write_raft_snapshot_meta(
    out: &mut Vec<u8>,
    meta: &SnapshotMetaOf<ControlPlaneRaftTypeConfig>,
    context: RaftSnapshotEncodingContext,
) -> Result<(), ControlPlaneError> {
    match context {
        RaftSnapshotEncodingContext::Restart => {
            write_raft_restart_option_log_id(
                out,
                ControlPlaneRaftRestartOptionalField::CurrentSnapshotLastLogId,
                meta.last_log_id,
            );
            write_raft_restart_option_log_id(
                out,
                ControlPlaneRaftRestartOptionalField::CurrentSnapshotMembershipLogId,
                *meta.last_membership.log_id(),
            );
            write_raft_membership(out, meta.last_membership.membership())?;
        }
        RaftSnapshotEncodingContext::Peer => {
            write_raft_peer_option_log_id(
                out,
                ControlPlaneRaftPeerRpcOptionalField::SnapshotLastLogId,
                meta.last_log_id,
            );
            write_raft_stored_membership(
                out,
                &meta.last_membership,
                Some(ControlPlaneRaftPeerRpcOptionalField::SnapshotMembershipLogId),
            )?;
        }
    }
    Ok(())
}

fn write_raft_append_entries_request(
    out: &mut Vec<u8>,
    request: &AppendEntriesRequest<ControlPlaneRaftTypeConfig>,
) -> Result<(), ControlPlaneError> {
    write_raft_vote(out, request.vote);
    write_raft_peer_option_log_id(
        out,
        ControlPlaneRaftPeerRpcOptionalField::AppendEntriesPrevLogId,
        request.prev_log_id,
    );
    write_raft_u32(
        out,
        raft_len_as_u32(request.entries.len(), "raft append entries")?,
    );
    for entry in &request.entries {
        write_raft_entry(out, entry)?;
    }
    write_raft_peer_option_log_id(
        out,
        ControlPlaneRaftPeerRpcOptionalField::AppendEntriesLeaderCommit,
        request.leader_commit,
    );
    Ok(())
}

fn control_plane_raft_append_entries_response_tag(
    response: &AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
) -> ControlPlaneRaftAppendEntriesResponseTag {
    match response {
        AppendEntriesResponse::Success => ControlPlaneRaftAppendEntriesResponseTag::Success,
        AppendEntriesResponse::PartialSuccess(_) => {
            ControlPlaneRaftAppendEntriesResponseTag::PartialSuccess
        }
        AppendEntriesResponse::Conflict => ControlPlaneRaftAppendEntriesResponseTag::Conflict,
        AppendEntriesResponse::HigherVote(_) => {
            ControlPlaneRaftAppendEntriesResponseTag::HigherVote
        }
    }
}

fn write_raft_append_entries_response(
    out: &mut Vec<u8>,
    response: &AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
) {
    write_raft_u8(
        out,
        control_plane_raft_append_entries_response_tag(response).as_u8(),
    );
    match response {
        AppendEntriesResponse::Success => {}
        AppendEntriesResponse::PartialSuccess(log_id) => {
            write_raft_peer_option_log_id(
                out,
                ControlPlaneRaftPeerRpcOptionalField::AppendEntriesPartialSuccessLogId,
                *log_id,
            );
        }
        AppendEntriesResponse::Conflict => {}
        AppendEntriesResponse::HigherVote(vote) => {
            write_raft_vote(out, *vote);
        }
    }
}

fn write_raft_vote_request(out: &mut Vec<u8>, request: &VoteRequest<ControlPlaneRaftTypeConfig>) {
    write_raft_vote(out, request.vote);
    write_raft_peer_option_log_id(
        out,
        ControlPlaneRaftPeerRpcOptionalField::VoteRequestLastLogId,
        request.last_log_id,
    );
    write_raft_bool(out, request.leadership_transfer);
}

fn write_raft_vote_response(
    out: &mut Vec<u8>,
    response: &VoteResponse<ControlPlaneRaftTypeConfig>,
) {
    write_raft_vote(out, response.vote);
    write_raft_bool(out, response.vote_granted);
    write_raft_peer_option_log_id(
        out,
        ControlPlaneRaftPeerRpcOptionalField::VoteResponseLastLogId,
        response.last_log_id,
    );
}

fn write_raft_transfer_leader_request(
    out: &mut Vec<u8>,
    request: &TransferLeaderRequest<ControlPlaneRaftTypeConfig>,
) {
    write_raft_vote(out, *request.from_leader());
    write_raft_u64(out, *request.to_node_id());
    write_raft_peer_option_log_id(
        out,
        ControlPlaneRaftPeerRpcOptionalField::TransferLeaderRequestLastLogId,
        request.last_log_id().copied(),
    );
}

fn control_plane_raft_transfer_leader_response_tag(
    response: &TransferLeaderResponse<ControlPlaneRaftTypeConfig>,
) -> ControlPlaneRaftTransferLeaderResponseTag {
    match response {
        Ok(()) => ControlPlaneRaftTransferLeaderResponseTag::Success,
        Err(TransferLeaderError::VoteChanged { .. }) => {
            ControlPlaneRaftTransferLeaderResponseTag::VoteChanged
        }
        Err(TransferLeaderError::LogNotFlushed { .. }) => {
            ControlPlaneRaftTransferLeaderResponseTag::LogNotFlushed
        }
    }
}

fn write_raft_transfer_leader_response(
    out: &mut Vec<u8>,
    response: &TransferLeaderResponse<ControlPlaneRaftTypeConfig>,
) {
    write_raft_u8(
        out,
        control_plane_raft_transfer_leader_response_tag(response).as_u8(),
    );
    match response {
        Ok(()) => {}
        Err(TransferLeaderError::VoteChanged { expected, actual }) => {
            write_raft_vote(out, *expected);
            write_raft_vote(out, *actual);
        }
        Err(TransferLeaderError::LogNotFlushed { expected, actual }) => {
            write_raft_peer_option_log_id(
                out,
                ControlPlaneRaftPeerRpcOptionalField::TransferLeaderExpectedLogId,
                *expected,
            );
            write_raft_peer_option_log_id(
                out,
                ControlPlaneRaftPeerRpcOptionalField::TransferLeaderActualLogId,
                *actual,
            );
        }
    }
}

fn control_plane_raft_entry_payload_tag(
    entry: &ControlPlaneRaftEntry,
) -> ControlPlaneRaftEntryPayloadTag {
    match &entry.payload {
        EntryPayload::Blank => ControlPlaneRaftEntryPayloadTag::Blank,
        EntryPayload::Membership(_) => ControlPlaneRaftEntryPayloadTag::Membership,
        EntryPayload::Normal(_) => ControlPlaneRaftEntryPayloadTag::Normal,
    }
}

fn write_raft_entry(
    out: &mut Vec<u8>,
    entry: &ControlPlaneRaftEntry,
) -> Result<(), ControlPlaneError> {
    write_raft_entry_with_command_encoder(out, entry, encode_control_plane_command)
}

fn write_raft_entry_with_command_encoder(
    out: &mut Vec<u8>,
    entry: &ControlPlaneRaftEntry,
    encode_command: impl FnOnce(&ControlPlaneCommand) -> Result<Vec<u8>, ControlPlaneError>,
) -> Result<(), ControlPlaneError> {
    write_raft_log_id(out, entry.log_id);
    write_raft_u8(out, control_plane_raft_entry_payload_tag(entry).as_u8());
    match &entry.payload {
        EntryPayload::Blank => {}
        EntryPayload::Membership(membership) => {
            write_raft_membership(out, membership)?;
        }
        EntryPayload::Normal(command) => {
            let encoded = encode_command(command)?;
            write_raft_bytes(out, &encoded)?;
        }
    }
    Ok(())
}

fn write_raft_vote(out: &mut Vec<u8>, vote: VoteOf<ControlPlaneRaftTypeConfig>) {
    write_raft_leader_id(out, vote.leader_id);
    write_raft_bool(out, vote.committed);
}

fn write_raft_stored_membership(
    out: &mut Vec<u8>,
    membership: &StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    peer_optional_field: Option<ControlPlaneRaftPeerRpcOptionalField>,
) -> Result<(), ControlPlaneError> {
    match peer_optional_field {
        Some(field) => write_raft_peer_option_log_id(out, field, *membership.log_id()),
        None => write_raft_option_log_id(out, *membership.log_id()),
    }
    write_raft_membership(out, membership.membership())
}

fn write_raft_membership(
    out: &mut Vec<u8>,
    membership: &Membership<ControlPlaneRaftNodeId, BasicNode>,
) -> Result<(), ControlPlaneError> {
    let configs = membership.get_joint_config();
    write_raft_u32(
        out,
        raft_len_as_u32(configs.len(), "raft membership configs")?,
    );
    for config in configs {
        write_raft_u32(
            out,
            raft_len_as_u32(config.len(), "raft membership config voters")?,
        );
        for node_id in config {
            write_raft_u64(out, *node_id);
        }
    }
    let nodes = membership.nodes().collect::<Vec<_>>();
    write_raft_u32(out, raft_len_as_u32(nodes.len(), "raft membership nodes")?);
    for (node_id, node) in nodes {
        write_raft_u64(out, *node_id);
        write_raft_string(out, &node.addr)?;
    }
    Ok(())
}

fn write_raft_option_log_id(
    out: &mut Vec<u8>,
    log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
) {
    match log_id {
        None => write_raft_u8(out, RaftWireOptionTag::Absent.as_u8()),
        Some(log_id) => {
            write_raft_u8(out, RaftWireOptionTag::Present.as_u8());
            write_raft_log_id(out, log_id);
        }
    }
}

fn write_raft_log_id(out: &mut Vec<u8>, log_id: LogIdOf<ControlPlaneRaftTypeConfig>) {
    write_raft_leader_id(out, *log_id.committed_leader_id());
    write_raft_u64(out, log_id.index());
}

fn write_raft_leader_id(out: &mut Vec<u8>, leader_id: ControlPlaneRaftLeaderId) {
    write_raft_u64(out, leader_id.term);
    write_raft_u64(out, leader_id.node_id);
}

fn write_raft_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ControlPlaneError> {
    write_raft_u32(out, raft_len_as_u32(bytes.len(), "raft byte payload")?);
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_raft_string(out: &mut Vec<u8>, value: &str) -> Result<(), ControlPlaneError> {
    write_raft_u32(out, raft_len_as_u32(value.len(), "raft string")?);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_raft_bool(out: &mut Vec<u8>, value: bool) {
    write_raft_u8(out, RaftWireBoolean::from_bool(value).as_u8());
}

fn write_raft_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_raft_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_raft_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_raft_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn raft_len_as_u32(len: usize, field: &'static str) -> Result<u32, ControlPlaneError> {
    u32::try_from(len)
        .map_err(|_| raft_artifact_protocol_error(format!("{field} length {len} exceeds u32::MAX")))
}

fn raft_artifact_protocol_error(message: impl Into<String>) -> ControlPlaneError {
    ControlPlaneError::CommandDecode {
        message: message.into(),
    }
}

struct RaftArtifactReader<'a> {
    payload: &'a [u8],
    offset: usize,
    context: &'static str,
    truncated: bool,
}

impl<'a> RaftArtifactReader<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self::with_context(payload, "control-plane OpenRaft durable restart artifact")
    }

    fn with_context(payload: &'a [u8], context: &'static str) -> Self {
        Self {
            payload,
            offset: 0,
            context,
            truncated: false,
        }
    }

    fn was_truncated(&self) -> bool {
        self.truncated
    }

    fn finish(&self) -> Result<(), ControlPlaneError> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err(raft_artifact_protocol_error(format!(
                "{} has {} trailing bytes",
                self.context,
                self.payload.len() - self.offset
            )))
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ControlPlaneError> {
        if len > self.remaining_len() {
            self.truncated = true;
            return Err(raft_artifact_protocol_error(format!(
                "truncated {}",
                self.context
            )));
        }
        let end = self
            .offset
            .checked_add(len)
            .expect("length bounded by remaining payload must not overflow");
        let bytes = &self.payload[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, ControlPlaneError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, ControlPlaneError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, ControlPlaneError> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, ControlPlaneError> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_bool(&mut self) -> Result<bool, ControlPlaneError> {
        RaftWireBoolean::from_u8(self.read_u8()?)
            .map(RaftWireBoolean::as_bool)
            .map_err(|value| {
                raft_artifact_protocol_error(format!(
                    "invalid {} boolean value {value}",
                    self.context
                ))
            })
    }

    fn read_len(&mut self, field: &'static str) -> Result<usize, ControlPlaneError> {
        usize::try_from(self.read_u32()?)
            .map_err(|_| raft_artifact_protocol_error(format!("{field} length does not fit usize")))
    }

    fn read_collection_len(
        &mut self,
        field: &'static str,
        min_item_len: usize,
    ) -> Result<usize, ControlPlaneError> {
        assert!(min_item_len > 0);
        let len = self.read_len(field)?;
        let max_items = self.remaining_len() / min_item_len;
        if len > max_items {
            return Err(raft_artifact_protocol_error(format!(
                "{field} count {len} exceeds remaining control-plane OpenRaft durable payload capacity {max_items}",
            )));
        }
        Ok(len)
    }

    fn read_bytes(&mut self, field: &'static str) -> Result<&'a [u8], ControlPlaneError> {
        let len = self.read_len(field)?;
        self.read_exact(len)
    }

    fn read_limited_bytes(
        &mut self,
        field: &'static str,
        max_len: usize,
    ) -> Result<&'a [u8], ControlPlaneError> {
        let len = self.read_len(field)?;
        if len > max_len {
            return Err(raft_artifact_protocol_error(format!(
                "{field} length {len} exceeds limit {max_len}"
            )));
        }
        self.read_exact(len)
    }

    fn read_string(&mut self) -> Result<String, ControlPlaneError> {
        let bytes = self.read_bytes("raft string")?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|source| {
                raft_artifact_protocol_error(format!(
                    "control-plane OpenRaft durable string is not UTF-8: {source}"
                ))
            })
    }

    fn read_option_vote(
        &mut self,
    ) -> Result<Option<VoteOf<ControlPlaneRaftTypeConfig>>, ControlPlaneError> {
        match RaftWireOptionTag::from_u8(self.read_u8()?) {
            Ok(RaftWireOptionTag::Absent) => Ok(None),
            Ok(RaftWireOptionTag::Present) => Ok(Some(self.read_vote()?)),
            Err(value) => Err(raft_artifact_protocol_error(format!(
                "invalid control-plane OpenRaft durable optional vote tag {value}"
            ))),
        }
    }

    fn read_option_log_id(
        &mut self,
    ) -> Result<Option<LogIdOf<ControlPlaneRaftTypeConfig>>, ControlPlaneError> {
        match RaftWireOptionTag::from_u8(self.read_u8()?) {
            Ok(RaftWireOptionTag::Absent) => Ok(None),
            Ok(RaftWireOptionTag::Present) => Ok(Some(self.read_log_id()?)),
            Err(value) => Err(raft_artifact_protocol_error(format!(
                "invalid control-plane OpenRaft durable optional log-id tag {value}"
            ))),
        }
    }

    fn read_append_entries_request(
        &mut self,
    ) -> Result<AppendEntriesRequest<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let vote = self.read_vote()?;
        let prev_log_id = self.read_option_log_id()?;
        let entry_count = self.read_collection_len("raft append entries", RAFT_ENTRY_MIN_LEN)?;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(self.read_entry()?);
        }
        let leader_commit = self.read_option_log_id()?;
        Ok(AppendEntriesRequest {
            vote,
            prev_log_id,
            entries,
            leader_commit,
        })
    }

    fn read_append_entries_response(
        &mut self,
    ) -> Result<AppendEntriesResponse<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        match ControlPlaneRaftAppendEntriesResponseTag::from_u8(self.read_u8()?) {
            Ok(ControlPlaneRaftAppendEntriesResponseTag::Success) => {
                Ok(AppendEntriesResponse::Success)
            }
            Ok(ControlPlaneRaftAppendEntriesResponseTag::PartialSuccess) => Ok(
                AppendEntriesResponse::PartialSuccess(self.read_option_log_id()?),
            ),
            Ok(ControlPlaneRaftAppendEntriesResponseTag::Conflict) => {
                Ok(AppendEntriesResponse::Conflict)
            }
            Ok(ControlPlaneRaftAppendEntriesResponseTag::HigherVote) => {
                Ok(AppendEntriesResponse::HigherVote(self.read_vote()?))
            }
            Err(value) => Err(raft_artifact_protocol_error(format!(
                "unknown control-plane OpenRaft peer RPC append_entries response tag {value}"
            ))),
        }
    }

    fn read_vote_request(
        &mut self,
    ) -> Result<VoteRequest<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let vote = self.read_vote()?;
        let last_log_id = self.read_option_log_id()?;
        let leadership_transfer = self.read_bool()?;
        Ok(VoteRequest {
            vote,
            last_log_id,
            leadership_transfer,
        })
    }

    fn read_vote_response(
        &mut self,
    ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let vote = self.read_vote()?;
        let vote_granted = self.read_bool()?;
        let last_log_id = self.read_option_log_id()?;
        Ok(VoteResponse {
            vote,
            vote_granted,
            last_log_id,
        })
    }

    fn read_wal_record(&mut self) -> Result<ControlPlaneRaftWalRecord, ControlPlaneError> {
        match ControlPlaneRaftWalRecordKind::from_u8(self.read_u8()?) {
            Ok(ControlPlaneRaftWalRecordKind::SaveVote) => {
                Ok(ControlPlaneRaftWalRecord::SaveVote(self.read_vote()?))
            }
            Ok(ControlPlaneRaftWalRecordKind::Append) => {
                let entry_count =
                    self.read_collection_len("raft WAL append entries", RAFT_ENTRY_MIN_LEN)?;
                let mut entries = Vec::with_capacity(entry_count);
                for _ in 0..entry_count {
                    entries.push(self.read_entry()?);
                }
                Ok(ControlPlaneRaftWalRecord::Append(entries))
            }
            Ok(ControlPlaneRaftWalRecordKind::SaveCommitted) => Ok(
                ControlPlaneRaftWalRecord::SaveCommitted(self.read_option_log_id()?),
            ),
            Ok(ControlPlaneRaftWalRecordKind::TruncateAfter) => Ok(
                ControlPlaneRaftWalRecord::TruncateAfter(self.read_option_log_id()?),
            ),
            Ok(ControlPlaneRaftWalRecordKind::Purge) => {
                Ok(ControlPlaneRaftWalRecord::Purge(self.read_log_id()?))
            }
            Err(value) => Err(raft_artifact_protocol_error(format!(
                "unknown control-plane OpenRaft WAL record tag {value}"
            ))),
        }
    }

    fn read_transfer_leader_request(
        &mut self,
    ) -> Result<TransferLeaderRequest<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let from_leader = self.read_vote()?;
        let to_node_id = self.read_u64()?;
        let last_log_id = self.read_option_log_id()?;
        Ok(TransferLeaderRequest::new(
            from_leader,
            to_node_id,
            last_log_id,
        ))
    }

    fn read_transfer_leader_response(
        &mut self,
    ) -> Result<TransferLeaderResponse<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        match ControlPlaneRaftTransferLeaderResponseTag::from_u8(self.read_u8()?) {
            Ok(ControlPlaneRaftTransferLeaderResponseTag::Success) => Ok(Ok(())),
            Ok(ControlPlaneRaftTransferLeaderResponseTag::VoteChanged) => {
                Ok(Err(TransferLeaderError::VoteChanged {
                    expected: self.read_vote()?,
                    actual: self.read_vote()?,
                }))
            }
            Ok(ControlPlaneRaftTransferLeaderResponseTag::LogNotFlushed) => {
                Ok(Err(TransferLeaderError::LogNotFlushed {
                    expected: self.read_option_log_id()?,
                    actual: self.read_option_log_id()?,
                }))
            }
            Err(value) => Err(raft_artifact_protocol_error(format!(
                "unknown control-plane OpenRaft peer RPC transfer_leader response tag {value}"
            ))),
        }
    }

    fn read_entry(&mut self) -> Result<ControlPlaneRaftEntry, ControlPlaneError> {
        let log_id = self.read_log_id()?;
        let payload = match ControlPlaneRaftEntryPayloadTag::from_u8(self.read_u8()?) {
            Ok(ControlPlaneRaftEntryPayloadTag::Blank) => EntryPayload::Blank,
            Ok(ControlPlaneRaftEntryPayloadTag::Membership) => {
                EntryPayload::Membership(self.read_membership()?)
            }
            Ok(ControlPlaneRaftEntryPayloadTag::Normal) => EntryPayload::Normal(
                decode_control_plane_command(self.read_bytes("raft command payload")?)?,
            ),
            Err(value) => {
                return Err(raft_artifact_protocol_error(format!(
                    "unknown control-plane OpenRaft durable entry payload tag {value}"
                )));
            }
        };
        Ok(Entry { log_id, payload })
    }

    fn read_stored_membership(
        &mut self,
    ) -> Result<StoredMembershipOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let log_id = self.read_option_log_id()?;
        let membership = self.read_membership()?;
        Ok(StoredMembership::new(log_id, membership))
    }

    fn read_option_snapshot(
        &mut self,
    ) -> Result<Option<ControlPlaneRaftSnapshot>, ControlPlaneError> {
        match RaftWireOptionTag::from_u8(self.read_u8()?) {
            Ok(RaftWireOptionTag::Absent) => Ok(None),
            Ok(RaftWireOptionTag::Present) => {
                Ok(Some(self.read_snapshot("raft cached snapshot payload")?))
            }
            Err(value) => Err(raft_artifact_protocol_error(format!(
                "invalid control-plane OpenRaft durable optional snapshot tag {value}"
            ))),
        }
    }

    fn read_snapshot(
        &mut self,
        payload_field: &'static str,
    ) -> Result<ControlPlaneRaftSnapshot, ControlPlaneError> {
        let meta = self.read_snapshot_meta()?;
        let payload = self.read_bytes(payload_field)?.to_vec();
        Ok(Snapshot {
            meta,
            snapshot: ControlPlaneRaftSnapshotData::new(payload),
        })
    }

    fn read_snapshot_limited(
        &mut self,
        payload_field: &'static str,
        max_payload_bytes: usize,
    ) -> Result<ControlPlaneRaftSnapshot, ControlPlaneError> {
        let meta = self.read_snapshot_meta()?;
        let payload = self.read_limited_bytes(payload_field, max_payload_bytes)?;
        Ok(Snapshot {
            meta,
            snapshot: ControlPlaneRaftSnapshotData::new(payload.to_vec()),
        })
    }

    fn read_snapshot_meta(
        &mut self,
    ) -> Result<SnapshotMetaOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let last_log_id = self.read_option_log_id()?;
        let last_membership = self.read_stored_membership()?;
        Ok(SnapshotMeta {
            last_log_id,
            last_membership,
        })
    }

    fn read_membership(
        &mut self,
    ) -> Result<Membership<ControlPlaneRaftNodeId, BasicNode>, ControlPlaneError> {
        let config_count =
            self.read_collection_len("raft membership configs", RAFT_MEMBERSHIP_CONFIG_MIN_LEN)?;
        let mut configs = Vec::with_capacity(config_count);
        for _ in 0..config_count {
            let voter_count = self
                .read_collection_len("raft membership config voters", std::mem::size_of::<u64>())?;
            let mut voters = BTreeSet::new();
            for _ in 0..voter_count {
                voters.insert(self.read_u64()?);
            }
            configs.push(voters);
        }

        let node_count =
            self.read_collection_len("raft membership nodes", RAFT_MEMBERSHIP_NODE_MIN_LEN)?;
        let mut nodes = BTreeMap::new();
        for _ in 0..node_count {
            let node_id = self.read_u64()?;
            let node = BasicNode::new(self.read_string()?);
            if nodes.insert(node_id, node).is_some() {
                return Err(raft_artifact_protocol_error(format!(
                    "duplicate control-plane OpenRaft durable membership node {node_id}"
                )));
            }
        }
        Membership::new(configs, nodes).map_err(|error| {
            raft_artifact_protocol_error(format!(
                "invalid control-plane OpenRaft durable membership: {error}"
            ))
        })
    }

    fn read_log_id(&mut self) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let leader_id = self.read_leader_id()?;
        let index = self.read_u64()?;
        Ok(LogId::new(leader_id, index))
    }

    fn read_leader_id(&mut self) -> Result<ControlPlaneRaftLeaderId, ControlPlaneError> {
        Ok(LeaderId {
            term: self.read_u64()?,
            node_id: self.read_u64()?,
        })
    }

    fn read_vote(&mut self) -> Result<VoteOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let leader_id = self.read_leader_id()?;
        let committed = self.read_bool()?;
        Ok(Vote {
            leader_id,
            committed,
        })
    }

    fn read_peer_frame_identity(
        &mut self,
    ) -> Result<Option<ControlPlaneRaftPeerFrameIdentity>, ControlPlaneError> {
        match RaftWireOptionTag::from_u8(self.read_u8()?) {
            Ok(RaftWireOptionTag::Absent) => Ok(None),
            Ok(RaftWireOptionTag::Present) => {
                let cluster_name = self.read_string()?;
                let topology = match RaftWireOptionTag::from_u8(self.read_u8()?) {
                    Ok(RaftWireOptionTag::Absent) => None,
                    Ok(RaftWireOptionTag::Present) => Some(ControlPlaneRaftTopologyIdentity {
                        generation: self.read_u64()?,
                        digest: self.read_string()?,
                    }),
                    Err(value) => {
                        return Err(raft_artifact_protocol_error(format!(
                            "invalid control-plane OpenRaft peer RPC topology identity tag {value}"
                        )));
                    }
                };
                let source = self.read_u64()?;
                let target = self.read_u64()?;
                Ok(Some(ControlPlaneRaftPeerFrameIdentity {
                    cluster_name,
                    topology,
                    source,
                    target,
                }))
            }
            Err(value) => Err(raft_artifact_protocol_error(format!(
                "invalid control-plane OpenRaft peer RPC frame identity tag {value}"
            ))),
        }
    }

    fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}

impl RaftLogReader<ControlPlaneRaftTypeConfig> for ControlPlaneRaftLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<ControlPlaneRaftEntry>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let Some(start) = Self::range_start(&range)? else {
            return Ok(Vec::new());
        };
        let end_exclusive = Self::range_end_exclusive(&range);
        if end_exclusive.is_some_and(|end_exclusive| start >= end_exclusive) {
            return Ok(Vec::new());
        }

        let inner = self.lock()?;
        let Some((&first_present, _)) = inner.entries.first_key_value() else {
            return Ok(Vec::new());
        };
        let Some((&last_present, _)) = inner.entries.last_key_value() else {
            return Ok(Vec::new());
        };

        let mut entries = Vec::new();
        let mut index = start.max(first_present);
        while index <= last_present && Self::before_range_end(index, end_exclusive) {
            let entry = inner.entries.get(&index).ok_or_else(|| {
                raft_log_store_error(format!(
                    "control-plane OpenRaft log hole at readable index {index}"
                ))
            })?;
            entries.push(entry.clone());
            let Some(next_index) = index.checked_add(1) else {
                break;
            };
            index = next_index;
        }
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock()?.vote)
    }
}

impl RaftLogStorage<ControlPlaneRaftTypeConfig> for ControlPlaneRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<ControlPlaneRaftTypeConfig>, io::Error> {
        let inner = self.lock()?;
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: inner.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &VoteOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        if let Some(durability_lane) = &self.durability_lane {
            return durability_lane
                .durable(ControlPlaneRaftWalRecord::SaveVote(*vote))
                .await;
        }
        let mut inner = self.lock()?;
        self.apply_in_memory_record(&mut inner, &ControlPlaneRaftWalRecord::SaveVote(*vote))
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        if let Some(durability_lane) = &self.durability_lane {
            return durability_lane
                .durable(ControlPlaneRaftWalRecord::SaveCommitted(committed))
                .await;
        }
        let mut inner = self.lock()?;
        self.apply_in_memory_record(
            &mut inner,
            &ControlPlaneRaftWalRecord::SaveCommitted(committed),
        )
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogIdOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock()?.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = ControlPlaneRaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if let Some(durability_lane) = &self.durability_lane {
            return durability_lane
                .append(ControlPlaneRaftWalRecord::Append(entries), callback)
                .await;
        }
        {
            let mut inner = self.lock()?;
            if let Err(error) =
                self.apply_in_memory_record(&mut inner, &ControlPlaneRaftWalRecord::Append(entries))
            {
                let message = error.to_string();
                callback.io_completed(Err(raft_log_store_error(message.clone())));
                return Err(raft_log_store_error(message));
            }
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        if let Some(durability_lane) = &self.durability_lane {
            return durability_lane
                .durable(ControlPlaneRaftWalRecord::TruncateAfter(last_log_id))
                .await;
        }
        let mut inner = self.lock()?;
        self.apply_in_memory_record(
            &mut inner,
            &ControlPlaneRaftWalRecord::TruncateAfter(last_log_id),
        )
    }

    async fn purge(
        &mut self,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        if let Some(durability_lane) = &self.durability_lane {
            return durability_lane
                .durable(ControlPlaneRaftWalRecord::Purge(log_id))
                .await;
        }
        let mut inner = self.lock()?;
        self.apply_in_memory_record(&mut inner, &ControlPlaneRaftWalRecord::Purge(log_id))
    }
}

fn is_openraft_bootstrap_log_id(log_id: LogIdOf<ControlPlaneRaftTypeConfig>) -> bool {
    log_id.index() == 0 && log_id.committed_leader_id().term == 0
}

fn raft_entry_payload_name(entry: &ControlPlaneRaftEntry) -> &'static str {
    match &entry.payload {
        EntryPayload::Blank => "blank",
        EntryPayload::Membership(_) => "membership",
        EntryPayload::Normal(_) => "normal",
    }
}

#[cfg(test)]
#[path = "control_plane_raft/tests.rs"]
mod tests;
