use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{ErrorKind, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::num::NonZeroU64;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use checksum::{ChecksumAlgorithm, ChecksumHasher};
use placement::NodeId;
use rustls::pki_types::ServerName;
use rustls::sign::CertifiedKey;
use thiserror::Error;

use crate::control_plane_auth::{
    control_plane_auth_payload_has_magic, ControlPlaneAuthDecision, ControlPlaneAuthEnvelope,
    ControlPlaneAuthOperation, ControlPlaneAuthPrincipal, ControlPlaneAuthRejectionReason,
    ControlPlaneAuthReplayPolicy, ControlPlaneAuthService, ControlPlaneAuthTarget,
    ControlPlaneScopedCredential, ControlPlaneScopedCredentialInput,
    ControlPlaneScopedCredentialStore,
};
use crate::control_plane_command::{
    decode_control_plane_command, encode_control_plane_command, AppliedControlPlaneCommand,
    ControlPlaneCommand, ControlPlaneCommandResponse, ControlPlaneCommandStateMachine,
    ControlPlaneLogId, ExpiredNodeHeartbeatLease, PromotedNodeHeartbeatLease,
    ReadyPgPeeringCompletion,
};
pub use crate::control_plane_lease::LeaseHorizonAuthorityBinding;
use crate::control_plane_lease::{
    bounded_renewal_deadline, successor_activation_fence_satisfied, validate_process_lease_clock,
    validate_serving_deadline_bound, BoundRouteMapLease, CommittedLeaseGrantHorizon,
    LeaseClockError, LeaseHorizonError, CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
};
use crate::deadline_io::DeadlineStream;
use crate::durable_journal::{
    DurableJournalAppendError, DurableJournalFile, DurableJournalFormat, DurableJournalIoContexts,
    DurableJournalObserver,
};
use crate::static_topology::UncertifiedInitialControlPlaneTopology;
use crate::{
    ClusterEpoch, PgClusterMapHistoryRouteReference, PgClusterMapHistoryRouteReferenceKind,
    PgClusterMapHistoryRouteReferences, PgId, PgState, RouteMapValidity,
    MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES,
};

// PG backfill can lag a burst of placement changes; retain enough recent
// snapshots that scanner references can still reconstruct historical routes.
const CLUSTER_MAP_HISTORY_LIMIT: usize = 256;
pub const MAX_HEARTBEAT_LEASE_MS: u64 = 10_000;
pub(crate) const MAX_LEASE_GRANT_HORIZON_MS: u64 = 60_000;
pub(crate) const CONTROL_PLANE_LEASE_GRANT_HORIZON_DURATION_MS: u64 = 2 * MAX_HEARTBEAT_LEASE_MS;
pub const CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS: u64 = CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS;
const CONTROL_PLANE_RPC_MAGIC: &[u8] = b"argmin-control-plane-rpc";
const CONTROL_PLANE_RPC_VERSION: u16 = 13;
const CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN: usize = 8 * 1024 * 1024;
pub const CONTROL_PLANE_RPC_MAX_FRAME_BYTES: usize =
    CONTROL_PLANE_RPC_MAGIC.len() + 16 + CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN;
const CONTROL_PLANE_RPC_TLS_ALPN: &[u8] = b"argmin-control-plane/1";
const CONTROL_PLANE_RPC_IO_TIMEOUT: Duration = Duration::from_secs(1);
pub const CONTROL_PLANE_RPC_MAX_SERVER_OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
const CONTROL_PLANE_RPC_LEADERSHIP_TRANSFER_TIMEOUT: Duration = Duration::from_secs(15);
const CONTROL_PLANE_RPC_SNAPSHOT_PURGE_TIMEOUT: Duration = Duration::from_secs(15);
const CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ADMIN_TIMEOUT: Duration = Duration::from_secs(15);
const CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const CONTROL_PLANE_RPC_AUTHORITY_CLOCK_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const CURRENT_CONTROL_PLANE_STATE_VERSION: u64 = 27;
pub const CONTROL_PLANE_TOPOLOGY_DIGEST_LEN: usize = 32;
pub const CONTROL_PLANE_BOOTSTRAP_MAP_DIGEST_LEN: usize = 32;
const CONTROL_PLANE_BOOTSTRAP_MAP_DIGEST_DOMAIN: &[u8] =
    b"argmin-control-plane-initial-bootstrap-map-v1";
const CONTROL_PLANE_RPC_READ_ONLY_RETRY_DEADLINE: Duration = Duration::from_secs(10);
const CONTROL_PLANE_RPC_READ_ONLY_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE: Duration = Duration::from_secs(20);
const CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF: Duration = Duration::from_millis(100);
const CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_PLANE_RPC_LIVENESS_IO_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_PLANE_RPC_LIVENESS_RETRY_BACKOFF: Duration = Duration::from_millis(100);
const CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS: u64 = 5_000;
const CONTROL_PLANE_RPC_AUTH_FUTURE_SKEW_MS: u64 = CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS;
const CONTROL_PLANE_RPC_HEARTBEAT_OBSERVATION_MIN_LEN: usize = 4 + 1 + 8 + 8 + 8 + 1;
const CONTROL_PLANE_RPC_HISTORY_ROUTE_REFERENCE_MIN_LEN: usize = 1 + 8 + 4;
const CONTROL_PLANE_RPC_RUNTIME_NODE_MIN_LEN: usize = 4 + 8 + 4 + 1;
const CONTROL_PLANE_RPC_PG_ROUTE_MIN_LEN: usize = 8 + 4 + 4 + 1 + 1 + 1 + 1 + 4 + 1;
const CONTROL_PLANE_RPC_ACTING_SET_NODE_MIN_LEN: usize = 4;
const CONTROL_PLANE_RPC_PENDING_RECOVERY_TASK_MIN_LEN: usize = 4 + 4 + 8 + 8 + 8;
const CONTROL_PLANE_RPC_PENDING_RECOVERY_FAILURE_MIN_LEN: usize = 4 + 1 + 4;
const CONTROL_PLANE_RPC_NODE_LEASE_DIAGNOSTIC_MIN_LEN: usize = 4 + 1;
const CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_SINGLE_AUTHORITY: u8 = 1;
const CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_RECONSTRUCTED: u8 = 2;
const CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_READ_INDEX: u8 = 3;
const CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC: &[u8; 8] = b"ARGCPCLK";
const CONTROL_PLANE_CLOCK_CHECKPOINT_VERSION: u16 = 2;
const CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN: usize = 32;
const CONTROL_PLANE_CLOCK_CHECKPOINT_CHECKSUM_LEN: usize = 8;
const CONTROL_PLANE_CLOCK_CHECKPOINT_LEN: usize = 8 + 2 + 32 + 8 + 1 + 8 + 8 + 8 + 8;
const CONTROL_PLANE_STATE_IDENTITY_MAGIC: &[u8; 8] = b"ARGCPID\0";
const CONTROL_PLANE_STATE_IDENTITY_VERSION: u16 = 1;
const CONTROL_PLANE_STATE_IDENTITY_LEN: usize = 8 + 2 + 32 + 8;
const SINGLE_AUTHORITY_INITIALIZED_MAGIC: &[u8; 8] = b"ARGCPINI";
const SINGLE_AUTHORITY_INITIALIZED_VERSION: u16 = 1;
const SINGLE_AUTHORITY_INITIALIZED_LEN: usize = 8 + 2 + 32 + 8;
const SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC: &[u8; 8] = b"ARGCPSJL";
const SINGLE_AUTHORITY_JOURNAL_FILE_VERSION: u16 = 2;
const SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC: &[u8; 8] = b"ARGCPSJR";
const SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION: u16 = 2;
const SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKSUM_LEN: usize = 8;
const SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKPOINT: u8 = 1;
const SINGLE_AUTHORITY_JOURNAL_RECORD_COMMAND: u8 = 2;
const SINGLE_AUTHORITY_JOURNAL_CHECKPOINT_COMMAND_LIMIT: u64 = 4_096;
const SINGLE_AUTHORITY_JOURNAL_CHECKPOINT_BYTE_LIMIT: u64 = 64 * 1024 * 1024;
const SINGLE_AUTHORITY_JOURNAL_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(59_900);

fn non_serving_runtime_map_validity(now_ms: u64) -> RouteMapValidity {
    RouteMapValidity::until_ms_saturating(now_ms.saturating_add(MAX_HEARTBEAT_LEASE_MS))
}

fn control_plane_lease_clock_error(error: LeaseClockError) -> ControlPlaneError {
    match error {
        LeaseClockError::TimestampOverflow { .. } => ControlPlaneError::LeaseDeadlineOverflow,
        LeaseClockError::ServingDeadlineOutOfBounds {
            serving_deadline_ms,
            effective_committed_now_ms,
            max_lease_ms,
            skew_budget_ms,
        } => ControlPlaneError::LeaseDeadlineOutOfBounds {
            serving_deadline_ms,
            effective_committed_now_ms,
            max_lease_ms,
            skew_budget_ms,
        },
        LeaseClockError::AuthorityClockTooFarAhead { .. }
        | LeaseClockError::LocalClockUnhealthy { .. }
        | LeaseClockError::ClockHealthSourceUnavailable => {
            unreachable!("authority clock comparison is only used by runtime-map consumers")
        }
    }
}

fn control_plane_lease_horizon_error(error: LeaseHorizonError) -> ControlPlaneError {
    match error {
        LeaseHorizonError::TimestampOverflow { field } => {
            ControlPlaneError::LeaseGrantHorizonTimestampOverflow { field }
        }
        LeaseHorizonError::PreviousHorizonStillActive {
            authority_now_ms,
            fenced_until_ms,
        } => ControlPlaneError::PreviousLeaseGrantHorizonStillActive {
            authority_now_ms,
            fenced_until_ms,
        },
        #[cfg(test)]
        LeaseHorizonError::AuthorityMismatch { .. }
        | LeaseHorizonError::DeadlineBeyondHorizon { .. } => {
            unreachable!("horizon establishment cannot validate a volatile grant")
        }
    }
}

fn control_plane_process_clock_sample(
) -> Result<crate::clock::WallClockHealthSample, ControlPlaneError> {
    crate::clock::wall_clock_health_sample().map_err(|error| {
        ControlPlaneError::AuthorityClockSampleWindowTooWide {
            narrowest_window_ms: error.narrowest_window_ms(),
            max_window_ms: error.max_window_ms(),
        }
    })
}

include!("control_plane/authority_clock.rs");

pub(crate) fn validate_control_plane_snapshot(
    context: &'static str,
    snapshot: &ClusterControlSnapshot,
) -> Result<(), ControlPlaneError> {
    snapshot
        .validate_publication_invariants()
        .map_err(|message| ControlPlaneError::SnapshotInvariantViolation { context, message })?;
    #[cfg(any(test, debug_assertions))]
    snapshot
        .validate_audit_invariants()
        .map_err(|message| ControlPlaneError::SnapshotInvariantViolation { context, message })?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AuthorityIncarnation(NonZeroU64);

impl AuthorityIncarnation {
    pub const INITIAL: Self = Self(NonZeroU64::MIN);

    #[must_use]
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }

    fn next(self) -> Result<Self, ControlPlaneError> {
        Self::new(
            self.get()
                .checked_add(1)
                .ok_or(ControlPlaneError::AuthorityIncarnationOverflow)?,
        )
        .ok_or(ControlPlaneError::AuthorityIncarnationOverflow)
    }
}

impl std::fmt::Display for AuthorityIncarnation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeMembershipState {
    Joining,
    Active,
    Draining,
    Out,
    Removed,
}

impl NodeMembershipState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Joining => "joining",
            Self::Active => "active",
            Self::Draining => "draining",
            Self::Out => "out",
            Self::Removed => "removed",
        }
    }

    fn from_str(value: &str) -> Result<Self, ControlPlaneError> {
        match value {
            "joining" => Ok(Self::Joining),
            "active" => Ok(Self::Active),
            "draining" => Ok(Self::Draining),
            "out" => Ok(Self::Out),
            "removed" => Ok(Self::Removed),
            _ => Err(ControlPlaneError::InvalidState {
                field: "membership",
                value: value.to_owned(),
            }),
        }
    }

    fn can_serve_primary(self) -> bool {
        matches!(self, Self::Active | Self::Draining)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeAvailabilityState {
    Healthy,
    Suspect,
    Unavailable,
}

impl NodeAvailabilityState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Suspect => "suspect",
            Self::Unavailable => "unavailable",
        }
    }

    fn from_str(value: &str) -> Result<Self, ControlPlaneError> {
        match value {
            "healthy" => Ok(Self::Healthy),
            "suspect" => Ok(Self::Suspect),
            "unavailable" => Ok(Self::Unavailable),
            _ => Err(ControlPlaneError::InvalidState {
                field: "availability",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeControlRecord {
    node_id: NodeId,
    membership: NodeMembershipState,
    administratively_available: bool,
    observed_availability: NodeAvailabilityState,
    node_incarnation: u64,
    endpoint: String,
    last_observed_epoch: Option<ClusterEpoch>,
    last_heartbeat_ms: Option<u64>,
    lease_deadline_ms: Option<u64>,
    cluster_map_history_route_references: PgClusterMapHistoryRouteReferences,
    pg_observations: BTreeMap<PgId, NodePgObservationRecord>,
}

impl NodeControlRecord {
    fn new(node_id: NodeId, membership: NodeMembershipState) -> Self {
        let administratively_available = !matches!(
            membership,
            NodeMembershipState::Out | NodeMembershipState::Removed
        );
        let observed_availability = if administratively_available {
            NodeAvailabilityState::Suspect
        } else {
            NodeAvailabilityState::Unavailable
        };
        Self {
            node_id,
            membership,
            administratively_available,
            observed_availability,
            node_incarnation: 0,
            endpoint: String::new(),
            last_observed_epoch: None,
            last_heartbeat_ms: None,
            lease_deadline_ms: None,
            cluster_map_history_route_references: PgClusterMapHistoryRouteReferences::default(),
            pg_observations: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn membership(&self) -> NodeMembershipState {
        self.membership
    }

    #[must_use]
    pub fn availability(&self) -> NodeAvailabilityState {
        if self.administratively_available {
            self.observed_availability
        } else {
            NodeAvailabilityState::Unavailable
        }
    }

    #[must_use]
    pub fn administratively_available(&self) -> bool {
        self.administratively_available
    }

    #[must_use]
    pub fn observed_availability(&self) -> NodeAvailabilityState {
        self.observed_availability
    }

    #[must_use]
    pub fn node_incarnation(&self) -> u64 {
        self.node_incarnation
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn last_observed_epoch(&self) -> Option<ClusterEpoch> {
        self.last_observed_epoch
    }

    fn record_observed_epoch(&mut self, observed_epoch: ClusterEpoch) {
        self.last_observed_epoch = Some(
            self.last_observed_epoch
                .map_or(observed_epoch, |previous| previous.max(observed_epoch)),
        );
    }

    #[must_use]
    pub fn last_heartbeat_ms(&self) -> Option<u64> {
        self.last_heartbeat_ms
    }

    #[must_use]
    pub fn lease_deadline_ms(&self) -> Option<u64> {
        self.lease_deadline_ms
    }

    fn pg_observation_state_matches(&self, other: &Self) -> bool {
        self.pg_observations.len() == other.pg_observations.len()
            && self.pg_observations.iter().all(|(pg_id, current)| {
                other.pg_observations.get(pg_id).is_some_and(|updated| {
                    current.pg_id == updated.pg_id
                        && current.state == updated.state
                        && current.observed_epoch == updated.observed_epoch
                        && current.metadata_proof == updated.metadata_proof
                        && current.pending_metadata_command == updated.pending_metadata_command
                })
            })
    }

    pub fn pg_observation(&self, pg_id: PgId) -> Option<&NodePgObservationRecord> {
        self.pg_observations.get(&pg_id)
    }

    pub fn pg_observations(&self) -> impl Iterator<Item = &NodePgObservationRecord> {
        self.pg_observations.values()
    }

    #[must_use]
    pub fn cluster_map_history_floor_epoch(&self) -> Option<ClusterEpoch> {
        self.cluster_map_history_route_references
            .summary()
            .oldest_required_epoch()
    }

    pub fn cluster_map_history_route_references(&self) -> &PgClusterMapHistoryRouteReferences {
        &self.cluster_map_history_route_references
    }

    fn can_serve_primary(&self, cluster_epoch: ClusterEpoch, now_ms: u64) -> bool {
        self.membership.can_serve_primary()
            && self.administratively_available
            && self.observed_availability == NodeAvailabilityState::Healthy
            && self.last_observed_epoch == Some(cluster_epoch)
            && self
                .lease_deadline_ms
                .is_some_and(|lease_deadline_ms| lease_deadline_ms > now_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialClusterTopologyCertificate {
    topology_generation: u64,
    topology_digest: [u8; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
    bootstrap_map_digest: [u8; CONTROL_PLANE_BOOTSTRAP_MAP_DIGEST_LEN],
    raft_voters: Vec<u64>,
}

impl InitialClusterTopologyCertificate {
    pub fn new(
        topology_generation: u64,
        topology_digest: [u8; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
        bootstrap_map_digest: [u8; CONTROL_PLANE_BOOTSTRAP_MAP_DIGEST_LEN],
        raft_voters: Vec<u64>,
    ) -> Result<Self, ControlPlaneError> {
        let certificate = Self {
            topology_generation,
            topology_digest,
            bootstrap_map_digest,
            raft_voters,
        };
        certificate
            .validate()
            .map_err(|message| ControlPlaneError::InvalidInitialTopology { message })?;
        Ok(certificate)
    }

    pub fn new_for_bootstrap_map(
        topology_generation: u64,
        topology_digest: [u8; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
        raft_voters: Vec<u64>,
        nodes: &[(NodeId, String)],
        pg_acting_sets: &[(PgId, Vec<NodeId>)],
    ) -> Result<Self, ControlPlaneError> {
        Self::new(
            topology_generation,
            topology_digest,
            initial_cluster_bootstrap_map_digest(nodes, pg_acting_sets),
            raft_voters,
        )
    }

    #[must_use]
    pub fn topology_generation(&self) -> u64 {
        self.topology_generation
    }

    #[must_use]
    pub fn topology_digest(&self) -> &[u8; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN] {
        &self.topology_digest
    }

    #[must_use]
    pub fn bootstrap_map_digest(&self) -> &[u8; CONTROL_PLANE_BOOTSTRAP_MAP_DIGEST_LEN] {
        &self.bootstrap_map_digest
    }

    #[must_use]
    pub fn raft_voters(&self) -> &[u64] {
        &self.raft_voters
    }

    fn validate(&self) -> Result<(), String> {
        if self.topology_generation == 0 {
            return Err("initial topology generation must be nonzero".to_string());
        }
        if self.raft_voters.is_empty() {
            return Err("initial topology must contain at least one Raft voter".to_string());
        }
        if self.raft_voters.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err("initial topology Raft voters must be strictly increasing".to_string());
        }
        Ok(())
    }
}

#[must_use]
pub fn initial_cluster_bootstrap_map_digest(
    nodes: &[(NodeId, String)],
    pg_acting_sets: &[(PgId, Vec<NodeId>)],
) -> [u8; CONTROL_PLANE_BOOTSTRAP_MAP_DIGEST_LEN] {
    let mut canonical_nodes = nodes.iter().collect::<Vec<_>>();
    canonical_nodes.sort_by_key(|(node_id, _)| *node_id);
    let mut canonical_pgs = pg_acting_sets.iter().collect::<Vec<_>>();
    canonical_pgs.sort_by_key(|(pg_id, _)| *pg_id);

    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    digest_bytes(&mut hasher, CONTROL_PLANE_BOOTSTRAP_MAP_DIGEST_DOMAIN);
    digest_len(&mut hasher, canonical_nodes.len());
    for (node_id, endpoint) in canonical_nodes {
        digest_u32(&mut hasher, node_id.as_u32());
        digest_bytes(&mut hasher, endpoint.as_bytes());
    }
    digest_len(&mut hasher, canonical_pgs.len());
    for (pg_id, acting_set) in canonical_pgs {
        digest_u32(&mut hasher, pg_id.get());
        digest_len(&mut hasher, acting_set.len());
        for node_id in acting_set {
            digest_u32(&mut hasher, node_id.as_u32());
        }
    }
    hasher
        .finalize()
        .bytes()
        .try_into()
        .expect("SHA-256 bootstrap-map digest must contain 32 bytes")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterControlSnapshot {
    authority_incarnation: AuthorityIncarnation,
    cluster_epoch: ClusterEpoch,
    initial_topology: Option<InitialClusterTopologyCertificate>,
    max_committed_timestamp_ms: Option<u64>,
    lease_grant_horizon: Option<CommittedLeaseGrantHorizon>,
    nodes: BTreeMap<NodeId, NodeControlRecord>,
    pgs: BTreeMap<PgId, PgControlRecord>,
    history: Vec<ClusterMapHistoryRecord>,
}

impl ClusterControlSnapshot {
    pub(crate) fn empty() -> Self {
        Self {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::INITIAL,
            initial_topology: None,
            max_committed_timestamp_ms: None,
            lease_grant_horizon: None,
            nodes: BTreeMap::new(),
            pgs: BTreeMap::new(),
            history: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_invalid_active_without_metadata_proof_epoch(pg_id: PgId) -> Self {
        let mut snapshot = Self::empty();
        snapshot.nodes.insert(
            NodeId::new(1),
            NodeControlRecord::new(NodeId::new(1), NodeMembershipState::Active),
        );
        snapshot.pgs.insert(
            pg_id,
            PgControlRecord {
                pg_id,
                state: PgState::Active,
                acting_set: vec![NodeId::new(1)],
                active_primary: Some(NodeId::new(1)),
                active_metadata_proof: Some(PgMetadataProof::empty()),
                active_metadata_proof_epoch: None,
                ..PgControlRecord::new(pg_id, vec![NodeId::new(1)])
            },
        );
        snapshot
    }

    #[cfg(test)]
    pub(crate) fn test_invalid_lease_grant_horizon(
        max_committed_timestamp_ms: Option<u64>,
        grant_not_after_ms: u64,
    ) -> Self {
        let mut snapshot = Self::empty();
        snapshot.max_committed_timestamp_ms = max_committed_timestamp_ms;
        snapshot.lease_grant_horizon = Some(CommittedLeaseGrantHorizon::from_parts(
            LeaseHorizonAuthorityBinding::new(1, None),
            grant_not_after_ms,
        ));
        snapshot
    }

    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn initial_topology(&self) -> Option<&InitialClusterTopologyCertificate> {
        self.initial_topology.as_ref()
    }

    #[must_use]
    pub fn max_committed_timestamp_ms(&self) -> Option<u64> {
        self.max_committed_timestamp_ms
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn lease_grant_horizon(&self) -> Option<CommittedLeaseGrantHorizon> {
        self.lease_grant_horizon
    }

    #[must_use]
    pub fn lease_grant_horizon_covers(
        &self,
        authority: LeaseHorizonAuthorityBinding,
        lease_deadline_ms: u64,
    ) -> bool {
        self.lease_grant_horizon.is_some_and(|horizon| {
            horizon.authority() == authority && lease_deadline_ms <= horizon.grant_not_after_ms()
        })
    }

    #[must_use]
    pub fn lease_grant_horizon_authority(&self) -> Option<LeaseHorizonAuthorityBinding> {
        self.lease_grant_horizon.map(|horizon| horizon.authority())
    }

    /// Fail closed while a previous authority's acknowledged lease horizon
    /// still fences a replacement authority.
    pub fn validate_lease_grant_horizon_rebinding(
        &self,
        authority: LeaseHorizonAuthorityBinding,
        authority_now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        self.lease_grant_horizon
            .map(|horizon| {
                horizon.validate_rebinding(
                    authority,
                    authority_now_ms,
                    CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
                )
            })
            .transpose()
            .map(|_| ())
            .map_err(control_plane_lease_horizon_error)
    }

    #[must_use]
    pub fn node(&self, node_id: NodeId) -> Option<&NodeControlRecord> {
        self.nodes.get(&node_id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &NodeControlRecord> {
        self.nodes.values()
    }

    #[must_use]
    pub fn pg(&self, pg_id: PgId) -> Option<&PgControlRecord> {
        self.pgs.get(&pg_id)
    }

    pub fn pgs(&self) -> impl Iterator<Item = &PgControlRecord> {
        self.pgs.values()
    }

    pub fn cluster_map_history(&self) -> &[ClusterMapHistoryRecord] {
        &self.history
    }

    #[must_use]
    pub fn cluster_map_at_epoch(&self, epoch: ClusterEpoch) -> Option<&ClusterMapHistoryRecord> {
        self.history
            .iter()
            .find(|record| record.cluster_epoch == epoch)
    }

    pub fn reconstructed_pg_route_at_epoch(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<PgRouteSnapshot, ControlPlaneError> {
        if cluster_epoch == self.cluster_epoch {
            let record = self
                .pg(pg_id)
                .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
            return reconstruct_pg_route_from_record(cluster_epoch, pg_id, record, |node_id| {
                self.nodes.contains_key(&node_id)
            });
        }
        let history = self
            .cluster_map_at_epoch(cluster_epoch)
            .ok_or(ControlPlaneError::UnknownClusterMapEpoch { cluster_epoch })?;
        for record in self
            .history
            .iter()
            .filter(|record| record.cluster_epoch >= cluster_epoch)
        {
            if record.absent_pgs.contains(&pg_id) {
                return Err(ControlPlaneError::UnknownPg { pg_id: pg_id.get() });
            }
            if let Some(record) = record.pg(pg_id) {
                return reconstruct_historical_pg_route(cluster_epoch, record, |node_id| {
                    history.nodes.contains(&node_id)
                });
            }
        }
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        reconstruct_pg_route_from_record(cluster_epoch, pg_id, record, |node_id| {
            self.nodes.contains_key(&node_id)
        })
    }

    pub fn active_pg_route(
        &self,
        pg_id: PgId,
        now_ms: u64,
    ) -> Result<PgRouteSnapshot, ControlPlaneError> {
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state != PgState::Active {
            return Err(ControlPlaneError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
                state: record.state,
            });
        }
        let primary = record
            .active_primary
            .filter(|primary| record.acting_set.contains(primary))
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
            })?;
        let primary_record = self
            .node(primary)
            .ok_or(ControlPlaneError::UnknownActingSetNode {
                pg_id: pg_id.get(),
                node_id: primary.as_u32(),
            })?;
        if !primary_record.can_serve_primary(self.cluster_epoch, now_ms) {
            return Err(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
            });
        }
        let primary_lease_deadline_ms = primary_record
            .lease_deadline_ms
            .expect("serving primary must have a lease deadline");
        validate_pg_primary_active_observation(self, pg_id, primary)?;
        Ok(PgRouteSnapshot {
            cluster_epoch: self.cluster_epoch,
            pg_id,
            primary_node_id: primary,
            acting_set: record.acting_set.clone(),
            state: PgState::Active,
            active_metadata_proof: record.active_metadata_proof,
            metadata_read_route: None,
            primary_lease_deadline_ms: Some(primary_lease_deadline_ms),
            peering_metadata_transfer: None,
            peering_metadata_transfer_destination_epoch: None,
            peering_metadata_transfer_source_route_epoch: None,
            peering_metadata_transfer_source_node_id: None,
            pending_metadata_command_recovery: None,
        })
    }

    fn active_pg_route_for_storage_node_refresh(
        &self,
        pg_id: PgId,
        now_ms: u64,
        refreshing_node_id: NodeId,
    ) -> Result<PgRouteSnapshot, ControlPlaneError> {
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state != PgState::Active {
            return Err(ControlPlaneError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
                state: record.state,
            });
        }
        let primary = record
            .active_primary
            .filter(|primary| record.acting_set.contains(primary))
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
            })?;
        let primary_record = self
            .node(primary)
            .ok_or(ControlPlaneError::UnknownActingSetNode {
                pg_id: pg_id.get(),
                node_id: primary.as_u32(),
            })?;
        if !primary_record.membership.can_serve_primary() {
            return Err(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
            });
        }
        let non_serving_route = || PgRouteSnapshot {
            cluster_epoch: self.cluster_epoch,
            pg_id,
            primary_node_id: primary,
            acting_set: record.acting_set.clone(),
            state: PgState::Active,
            active_metadata_proof: record.active_metadata_proof,
            metadata_read_route: None,
            primary_lease_deadline_ms: None,
            peering_metadata_transfer: None,
            peering_metadata_transfer_destination_epoch: None,
            peering_metadata_transfer_source_route_epoch: None,
            peering_metadata_transfer_source_node_id: None,
            pending_metadata_command_recovery: None,
        };
        let Some(primary_lease_deadline_ms) = primary_record.lease_deadline_ms else {
            return Ok(non_serving_route());
        };
        if primary_record.availability() != NodeAvailabilityState::Healthy
            || primary_lease_deadline_ms <= now_ms
        {
            return Ok(non_serving_route());
        }
        match validate_pg_primary_active_observation(self, pg_id, primary) {
            Ok(()) => {}
            Err(ControlPlaneError::PgPrimaryMissingActiveObservation { .. })
                if refreshing_node_id == primary =>
            {
                // The selected primary may still be running the previous Peering route
                // map. Let that primary receive the authoritative Active handoff map
                // so it can install the route and report the current Active observation.
            }
            Err(ControlPlaneError::PgPrimaryMissingActiveObservation { .. }) => {
                // Other nodes still need the new Active route to converge their local
                // route maps and keep renewing leases, but they cannot serve as primary
                // until the selected primary has reported the Active observation.
                return Ok(non_serving_route());
            }
            Err(error) => return Err(error),
        }
        Ok(PgRouteSnapshot {
            cluster_epoch: self.cluster_epoch,
            pg_id,
            primary_node_id: primary,
            acting_set: record.acting_set.clone(),
            state: PgState::Active,
            active_metadata_proof: record.active_metadata_proof,
            metadata_read_route: None,
            primary_lease_deadline_ms: Some(primary_lease_deadline_ms),
            peering_metadata_transfer: None,
            peering_metadata_transfer_destination_epoch: None,
            peering_metadata_transfer_source_route_epoch: None,
            peering_metadata_transfer_source_node_id: None,
            pending_metadata_command_recovery: None,
        })
    }

    pub fn active_pg_routes(&self, now_ms: u64) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
        self.pgs
            .values()
            .filter(|record| record.state == PgState::Active)
            .map(|record| self.active_pg_route(record.pg_id, now_ms))
            .collect()
    }

    pub fn pg_route(&self, pg_id: PgId, now_ms: u64) -> Result<PgRouteSnapshot, ControlPlaneError> {
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state == PgState::Active {
            return self.active_pg_route(pg_id, now_ms);
        }
        let metadata_read_route = peering_metadata_read_route_for_snapshot(self, record, now_ms);
        let primary = record
            .acting_set
            .first()
            .copied()
            .ok_or(ControlPlaneError::EmptyActingSet { pg_id: pg_id.get() })?;
        for &node_id in &record.acting_set {
            if !self.nodes.contains_key(&node_id) {
                return Err(ControlPlaneError::UnknownActingSetNode {
                    pg_id: pg_id.get(),
                    node_id: node_id.as_u32(),
                });
            }
        }
        let pending_metadata_command_recovery =
            self.pending_metadata_command_recovery_for_pg(record)?;
        Ok(PgRouteSnapshot {
            cluster_epoch: self.cluster_epoch,
            pg_id,
            primary_node_id: primary,
            acting_set: record.acting_set.clone(),
            state: record.state,
            active_metadata_proof: None,
            metadata_read_route,
            primary_lease_deadline_ms: None,
            peering_metadata_transfer: record.peering_metadata_transfer,
            peering_metadata_transfer_destination_epoch:
                peering_metadata_transfer_destination_epoch(record)?,
            peering_metadata_transfer_source_route_epoch: record
                .peering_metadata_transfer_source_route_epoch,
            peering_metadata_transfer_source_node_id: record
                .peering_metadata_transfer_source_node_id,
            pending_metadata_command_recovery,
        })
    }

    fn pending_metadata_command_recovery_for_pg(
        &self,
        record: &PgControlRecord,
    ) -> Result<Option<PendingMetadataCommandRecovery>, ControlPlaneError> {
        let mut recovery: Option<PendingMetadataCommandRecovery> = None;
        for node_id in &record.acting_set {
            let Some(observation) = self
                .node(*node_id)
                .and_then(|node| node.pg_observation(record.pg_id))
                .filter(|observation| observation.observed_epoch == self.cluster_epoch)
            else {
                continue;
            };
            let Some(pending) = observation.pending_metadata_command() else {
                continue;
            };
            validate_pending_metadata_command_reporter(self, record.pg_id, *node_id, pending)?;
            let candidate = PendingMetadataCommandRecovery {
                reporting_node_id: *node_id,
                pending,
            };
            if let Some(existing) = recovery {
                if existing.pending != pending {
                    return Err(ControlPlaneError::PgPeeringPendingMetadataCommandMismatch {
                        pg_id: record.pg_id.get(),
                        cluster_epoch: self.cluster_epoch,
                        first_node_id: existing.reporting_node_id.as_u32(),
                        first: existing.pending,
                        second_node_id: node_id.as_u32(),
                        second: pending,
                    });
                }
            } else {
                recovery = Some(candidate);
            }
        }
        Ok(recovery)
    }

    pub fn pending_metadata_command_recoveries(&self) -> PendingMetadataCommandRecoveryListing {
        let mut tasks = Vec::new();
        let mut failures = Vec::new();
        for record in self
            .pgs
            .values()
            .filter(|record| record.state == PgState::Peering)
        {
            match self.pending_metadata_command_recovery_for_pg(record) {
                Ok(Some(recovery)) => tasks.push(PendingMetadataCommandRecoveryTask::new(
                    record.pg_id,
                    recovery,
                )),
                Ok(None) => {}
                Err(error) => failures.push(PendingMetadataCommandRecoveryDiscoveryFailure::new(
                    record.pg_id,
                    PendingMetadataCommandRecoveryDiscoveryFailureKind::from_error(&error),
                    error.to_string(),
                )),
            }
        }
        PendingMetadataCommandRecoveryListing::new(tasks, failures)
    }

    fn pg_route_for_storage_node_refresh(
        &self,
        pg_id: PgId,
        now_ms: u64,
        refreshing_node_id: NodeId,
    ) -> Result<PgRouteSnapshot, ControlPlaneError> {
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state == PgState::Active {
            return self.active_pg_route_for_storage_node_refresh(
                pg_id,
                now_ms,
                refreshing_node_id,
            );
        }
        self.pg_route(pg_id, now_ms)
    }

    pub fn pg_routes(&self, now_ms: u64) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
        self.pgs
            .values()
            .map(|record| self.pg_route(record.pg_id, now_ms))
            .collect()
    }

    fn pg_routes_for_storage_node_refresh(
        &self,
        now_ms: u64,
        refreshing_node_id: NodeId,
    ) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
        self.pgs
            .values()
            .map(|record| {
                self.pg_route_for_storage_node_refresh(record.pg_id, now_ms, refreshing_node_id)
            })
            .collect()
    }

    pub fn runtime_map(&self, now_ms: u64) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.runtime_map_with_freshness_proof(
            now_ms,
            RuntimeMapFreshnessProof::SingleAuthority {
                authority_incarnation: self.authority_incarnation,
                issued_at_ms: now_ms,
            },
        )
    }

    pub(crate) fn runtime_map_with_freshness_proof(
        &self,
        now_ms: u64,
        freshness_proof: RuntimeMapFreshnessProof,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let pg_routes = self.pg_routes(now_ms)?;
        self.runtime_map_from_pg_routes(
            pg_routes,
            freshness_proof,
            non_serving_runtime_map_validity(now_ms),
        )
    }

    fn reconstructed_runtime_map_for_pg_with_fallback_validity(
        &self,
        pg_id: PgId,
        fallback_validity: RouteMapValidity,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        let mut route =
            reconstruct_pg_route_from_record(self.cluster_epoch, pg_id, record, |node_id| {
                self.nodes.contains_key(&node_id)
            })?;
        if record.state == PgState::Peering {
            route.pending_metadata_command_recovery =
                self.pending_metadata_command_recovery_for_pg(record)?;
        }
        self.runtime_map_from_pg_routes_with_history(
            vec![route],
            self.historical_pg_routes_for_runtime_map_pg(pg_id)?,
            self.historical_cluster_epochs(),
            RuntimeMapFreshnessProof::Reconstructed {
                authority_incarnation: self.authority_incarnation,
            },
            fallback_validity,
        )
    }

    pub(crate) fn serving_runtime_map_for_pg_with_freshness_proof(
        &self,
        pg_id: PgId,
        now_ms: u64,
        freshness_proof: RuntimeMapFreshnessProof,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let route = self.pg_route(pg_id, now_ms)?;
        self.runtime_map_from_pg_routes_with_history(
            vec![route],
            self.historical_pg_routes_for_runtime_map_pg(pg_id)?,
            self.historical_cluster_epochs(),
            freshness_proof,
            non_serving_runtime_map_validity(now_ms),
        )
    }

    pub fn runtime_map_for_storage_node_refresh(
        &self,
        now_ms: u64,
        refreshing_node_id: NodeId,
        refreshing_node_observed_epoch: ClusterEpoch,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let pg_routes = self.pg_routes_for_storage_node_refresh(now_ms, refreshing_node_id)?;
        let history_route_references = self
            .node(refreshing_node_id)
            .ok_or(ControlPlaneError::UnknownNode {
                node_id: refreshing_node_id.as_u32(),
            })?
            .cluster_map_history_route_references();
        let history_protection =
            required_cluster_map_history_protection(self.pgs.values(), self.nodes.values());
        let historical_pg_routes = self.historical_pg_routes_for_storage_node_refresh(
            history_route_references,
            &history_protection.exact_routes,
            refreshing_node_observed_epoch,
            &pg_routes,
            refreshing_node_id,
        )?;
        let historical_cluster_epochs = historical_pg_routes
            .iter()
            .map(PgRouteSnapshot::cluster_epoch)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        self.runtime_map_from_pg_routes_with_history_and_extra_nodes(
            pg_routes,
            historical_pg_routes,
            historical_cluster_epochs,
            RuntimeMapFreshnessProof::SingleAuthority {
                authority_incarnation: self.authority_incarnation,
                issued_at_ms: now_ms,
            },
            [refreshing_node_id],
            non_serving_runtime_map_validity(now_ms),
        )
    }

    fn runtime_map_from_pg_routes(
        &self,
        pg_routes: Vec<PgRouteSnapshot>,
        freshness_proof: RuntimeMapFreshnessProof,
        fallback_validity: RouteMapValidity,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let historical_pg_routes = self.historical_pg_routes_for_runtime_map()?;
        self.runtime_map_from_pg_routes_with_history(
            pg_routes,
            historical_pg_routes,
            self.historical_cluster_epochs(),
            freshness_proof,
            fallback_validity,
        )
    }

    fn runtime_map_from_pg_routes_with_history(
        &self,
        pg_routes: Vec<PgRouteSnapshot>,
        historical_pg_routes: Vec<PgRouteSnapshot>,
        historical_cluster_epochs: Vec<ClusterEpoch>,
        freshness_proof: RuntimeMapFreshnessProof,
        fallback_validity: RouteMapValidity,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.runtime_map_from_pg_routes_with_history_and_extra_nodes(
            pg_routes,
            historical_pg_routes,
            historical_cluster_epochs,
            freshness_proof,
            [],
            fallback_validity,
        )
    }

    fn runtime_map_from_pg_routes_with_history_and_extra_nodes(
        &self,
        pg_routes: Vec<PgRouteSnapshot>,
        historical_pg_routes: Vec<PgRouteSnapshot>,
        historical_cluster_epochs: Vec<ClusterEpoch>,
        freshness_proof: RuntimeMapFreshnessProof,
        extra_node_ids: impl IntoIterator<Item = NodeId>,
        fallback_validity: RouteMapValidity,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut routed_node_ids = BTreeSet::new();
        for route in &pg_routes {
            routed_node_ids.extend(route.acting_set().iter().copied());
        }
        for route in &historical_pg_routes {
            routed_node_ids.extend(route.acting_set().iter().copied());
        }
        routed_node_ids.extend(extra_node_ids);
        let mut nodes = Vec::with_capacity(routed_node_ids.len());
        for node_id in routed_node_ids {
            let node = self
                .node(node_id)
                .ok_or(ControlPlaneError::UnknownActingSetNode {
                    pg_id: 0,
                    node_id: node_id.as_u32(),
                })?;
            if node.endpoint.is_empty() {
                return Err(ControlPlaneError::NodeEndpointMissing {
                    node_id: node_id.as_u32(),
                    cluster_epoch: self.cluster_epoch,
                });
            }
            nodes.push(NodeRouteSnapshot {
                node_id,
                node_incarnation: node.node_incarnation,
                endpoint: node.endpoint.clone(),
                cluster_map_history_floor_epoch: node.cluster_map_history_floor_epoch(),
            });
        }
        let validity = pg_routes
            .iter()
            .filter_map(PgRouteSnapshot::primary_lease_deadline_ms)
            .min()
            .map_or(fallback_validity, RouteMapValidity::until_ms_saturating);
        Ok(ClusterRuntimeMapSnapshot {
            cluster_epoch: self.cluster_epoch,
            validity,
            freshness_proof,
            nodes,
            pg_routes,
            historical_pg_routes,
            historical_cluster_epochs,
        })
    }

    fn historical_cluster_epochs(&self) -> Vec<ClusterEpoch> {
        self.history
            .iter()
            .map(ClusterMapHistoryRecord::cluster_epoch)
            .collect()
    }

    fn historical_pg_routes_for_runtime_map(
        &self,
    ) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
        let mut routes = Vec::new();
        for history in &self.history {
            for pg in history.pgs() {
                routes.push(history.reconstructed_pg_route(pg.pg_id())?);
            }
        }
        for pg_id in self.pgs.keys().copied() {
            let Some(baseline_epoch) = self.earliest_retained_pg_epoch(pg_id) else {
                continue;
            };
            if !routes.iter().any(|candidate| {
                candidate.pg_id() == pg_id && candidate.cluster_epoch() == baseline_epoch
            }) {
                routes.push(self.reconstructed_pg_route_at_epoch(pg_id, baseline_epoch)?);
            }
        }
        routes.sort_by_key(|route| (route.cluster_epoch(), route.pg_id()));
        Ok(routes)
    }

    fn earliest_retained_pg_epoch(&self, pg_id: PgId) -> Option<ClusterEpoch> {
        let earliest_epoch = self.history.first()?.cluster_epoch();
        for (index, record) in self.history.iter().enumerate() {
            if record.absent_pgs.contains(&pg_id) {
                return self
                    .history
                    .get(index + 1)
                    .map(ClusterMapHistoryRecord::cluster_epoch);
            }
            if record.pg(pg_id).is_some() {
                return Some(earliest_epoch);
            }
        }
        self.pgs.contains_key(&pg_id).then_some(earliest_epoch)
    }

    fn historical_pg_routes_for_runtime_map_pg(
        &self,
        pg_id: PgId,
    ) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
        let mut routes = Vec::new();
        for history in &self.history {
            match self.reconstructed_pg_route_at_epoch(pg_id, history.cluster_epoch()) {
                Ok(route) => routes.push(route),
                Err(ControlPlaneError::UnknownPg { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(routes)
    }

    fn historical_pg_routes_for_storage_node_refresh(
        &self,
        history_route_references: &PgClusterMapHistoryRouteReferences,
        globally_protected_route_keys: &BTreeSet<(ClusterEpoch, PgId)>,
        observed_epoch: ClusterEpoch,
        current_routes: &[PgRouteSnapshot],
        refreshing_node_id: NodeId,
    ) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
        let mut required_keys = BTreeSet::new();
        let mut pending_keys = Vec::new();
        for reference in history_route_references.iter() {
            if reference.cluster_epoch() < self.cluster_epoch {
                add_required_historical_route_key(
                    &mut required_keys,
                    &mut pending_keys,
                    reference.cluster_epoch(),
                    reference.pg_id(),
                );
            }
        }

        // A durable reference is reported by the node hosting its metadata,
        // while historical shard reads are served by every node in the old
        // acting set. Give those serving nodes the same exact route authority;
        // otherwise a remote backfill coordinator retains the route globally
        // but its source node rejects the historical read.
        for &(cluster_epoch, pg_id) in globally_protected_route_keys {
            if cluster_epoch >= self.cluster_epoch {
                continue;
            }
            let route = self.historical_pg_route(cluster_epoch, pg_id)?;
            if storage_node_refresh_needs_historical_route(&route, refreshing_node_id) {
                add_required_historical_route_key(
                    &mut required_keys,
                    &mut pending_keys,
                    cluster_epoch,
                    pg_id,
                );
            }
        }
        let mut previous_routes: BTreeMap<PgId, PgRouteSnapshot> = BTreeMap::new();

        for history in &self.history {
            if observed_epoch != ClusterEpoch::INITIAL && history.cluster_epoch() >= observed_epoch
            {
                for pg in history.pgs() {
                    let route = history.reconstructed_pg_route(pg.pg_id())?;
                    if !storage_node_refresh_needs_historical_route(&route, refreshing_node_id) {
                        previous_routes.insert(pg.pg_id(), route);
                        continue;
                    }
                    let route_changed = previous_routes
                        .get(&pg.pg_id())
                        .is_none_or(|previous| !pg_route_configuration_eq(previous, &route));
                    if route_changed {
                        add_required_historical_route_key(
                            &mut required_keys,
                            &mut pending_keys,
                            history.cluster_epoch(),
                            pg.pg_id(),
                        );
                    }
                    previous_routes.insert(pg.pg_id(), route);
                }
            }
        }

        for route in current_routes {
            if let Some(destination_epoch) = route.peering_metadata_transfer_destination_epoch() {
                if destination_epoch < self.cluster_epoch {
                    add_required_historical_route_key(
                        &mut required_keys,
                        &mut pending_keys,
                        destination_epoch,
                        route.pg_id(),
                    );
                }
            }
            if let Some(source_epoch) = route.peering_metadata_transfer_source_route_epoch() {
                add_required_historical_route_key(
                    &mut required_keys,
                    &mut pending_keys,
                    source_epoch,
                    route.pg_id(),
                );
            }
            if let Some(recovery) = route.pending_metadata_command_recovery() {
                add_required_historical_route_key(
                    &mut required_keys,
                    &mut pending_keys,
                    recovery.pending().cluster_epoch(),
                    route.pg_id(),
                );
            }
        }

        while let Some((cluster_epoch, pg_id)) = pending_keys.pop() {
            let route = self.historical_pg_route(cluster_epoch, pg_id)?;
            if let Some(source_epoch) = route.peering_metadata_transfer_source_route_epoch() {
                add_required_historical_route_key(
                    &mut required_keys,
                    &mut pending_keys,
                    source_epoch,
                    route.pg_id(),
                );
            }
        }

        required_keys
            .into_iter()
            .map(|(cluster_epoch, pg_id)| self.historical_pg_route(cluster_epoch, pg_id))
            .collect()
    }

    fn historical_pg_route(
        &self,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Result<PgRouteSnapshot, ControlPlaneError> {
        self.reconstructed_pg_route_at_epoch(pg_id, cluster_epoch)
    }

    fn bump_authority_after_restart(&mut self) -> Result<(), ControlPlaneError> {
        let previous_primary_leases: BTreeMap<PgId, Option<PreviousPrimaryLease>> = self
            .pgs
            .values()
            .map(|record| (record.pg_id, active_primary_lease(self, record)))
            .collect();
        self.authority_incarnation = self.authority_incarnation.next()?;
        self.cluster_epoch = next_epoch(self.cluster_epoch)?;
        for record in self.nodes.values_mut() {
            record.pg_observations.clear();
        }
        for record in self.pgs.values_mut() {
            if record.state == PgState::Active {
                record.peering_metadata_proof_floor = record.active_metadata_proof;
                record.peering_metadata_proof_floor_epoch = record.active_metadata_proof_epoch;
                record.peering_metadata_proof_floor_imported =
                    record.active_metadata_transfer_imported;
                record.state = PgState::Peering;
                record.active_primary = None;
                record.active_metadata_proof = None;
                record.active_metadata_proof_epoch = None;
                record.active_metadata_transfer_imported = false;
                record.previous_primary_lease = previous_primary_leases
                    .get(&record.pg_id)
                    .cloned()
                    .flatten();
                record.peering_metadata_transfer = None;
                record.peering_metadata_transfer_source_route_epoch = None;
                record.peering_metadata_transfer_source_node_id = None;
                record.metadata_transfer_fenced = false;
                record.metadata_transfer_fence_source_lease_deadline_ms = None;
                record.metadata_transfer_fence_source_imported = false;
                record.metadata_transfer_fence_epoch = None;
            }
        }
        Ok(())
    }

    fn heartbeat_update_is_volatile(&self, next: &Self, heartbeat_node_id: NodeId) -> bool {
        if self.authority_incarnation != next.authority_incarnation
            || self.cluster_epoch != next.cluster_epoch
            || self.lease_grant_horizon != next.lease_grant_horizon
            || self.pgs != next.pgs
            || self.history != next.history
            || self.nodes.len() != next.nodes.len()
        {
            return false;
        }

        self.nodes.iter().all(|(node_id, current)| {
            let Some(updated) = next.nodes.get(node_id) else {
                return false;
            };
            if *node_id != heartbeat_node_id {
                return current == updated;
            }

            current.node_id == updated.node_id
                && current.membership == updated.membership
                && current.administratively_available == updated.administratively_available
                && current.observed_availability == updated.observed_availability
                && current.node_incarnation == updated.node_incarnation
                && current.endpoint == updated.endpoint
                && current.last_observed_epoch == updated.last_observed_epoch
                && current.cluster_map_history_route_references
                    == updated.cluster_map_history_route_references
                && current.pg_observation_state_matches(updated)
        })
    }

    pub(crate) fn apply_covered_volatile_heartbeat(
        &self,
        command: ControlPlaneCommand,
    ) -> Result<Option<Self>, ControlPlaneError> {
        let ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            lease_deadline_ms,
            lease_horizon_authority,
            ..
        } = &command
        else {
            return Ok(None);
        };
        let Some(authority) = *lease_horizon_authority else {
            return Ok(None);
        };
        if !self.lease_grant_horizon_covers(authority, *lease_deadline_ms) {
            return Ok(None);
        }

        let node_id = heartbeat.node_id;
        let Some(durable_node) = self.nodes.get(&node_id) else {
            return Ok(None);
        };
        if !durable_node.administratively_available
            || durable_node.observed_availability != NodeAvailabilityState::Healthy
            || durable_node.lease_deadline_ms.is_none()
        {
            return Ok(None);
        }
        let applied = self.apply_control_plane_command(command)?;
        if !self.heartbeat_update_is_volatile(applied.snapshot(), node_id) {
            return Ok(None);
        }

        let mut next_snapshot = applied.into_snapshot();
        next_snapshot.max_committed_timestamp_ms = self.max_committed_timestamp_ms;
        validate_control_plane_snapshot(
            "attempted to publish invalid volatile heartbeat state",
            &next_snapshot,
        )?;
        Ok(Some(next_snapshot))
    }

    pub(crate) fn promote_volatile_heartbeat_leases_command(
        &self,
        durable: &Self,
    ) -> Result<Option<ControlPlaneCommand>, ControlPlaneError> {
        if self.lease_grant_horizon != durable.lease_grant_horizon
            || self.nodes.len() != durable.nodes.len()
        {
            return Err(ControlPlaneError::SnapshotInvariantViolation {
                context: "volatile heartbeat lease promotion",
                message:
                    "live and durable snapshots do not share the same lease horizon and node set"
                        .to_string(),
            });
        }

        let mut promoted = Vec::new();
        for (node_id, durable_node) in &durable.nodes {
            let live_node = self.nodes.get(node_id).ok_or_else(|| {
                ControlPlaneError::SnapshotInvariantViolation {
                    context: "volatile heartbeat lease promotion",
                    message: format!("live snapshot is missing durable node {}", node_id.as_u32()),
                }
            })?;
            if live_node.node_incarnation != durable_node.node_incarnation
                || live_node.endpoint != durable_node.endpoint
            {
                return Err(ControlPlaneError::SnapshotInvariantViolation {
                    context: "volatile heartbeat lease promotion",
                    message: format!(
                        "live node {} identity differs from durable state",
                        node_id.as_u32()
                    ),
                });
            }
            match (durable_node.lease_deadline_ms, live_node.lease_deadline_ms) {
                (Some(durable_deadline_ms), Some(live_deadline_ms))
                    if live_deadline_ms > durable_deadline_ms =>
                {
                    promoted.push(PromotedNodeHeartbeatLease {
                        node_id: *node_id,
                        node_incarnation: live_node.node_incarnation,
                        lease_deadline_ms: live_deadline_ms,
                    });
                }
                (Some(durable_deadline_ms), Some(live_deadline_ms))
                    if live_deadline_ms < durable_deadline_ms =>
                {
                    return Err(ControlPlaneError::NodeLeaseDeadlineRegression {
                        node_id: node_id.as_u32(),
                        current_lease_deadline_ms: durable_deadline_ms,
                        requested_lease_deadline_ms: live_deadline_ms,
                    });
                }
                (None, Some(live_deadline_ms)) => {
                    return Err(ControlPlaneError::SnapshotInvariantViolation {
                        context: "volatile heartbeat lease promotion",
                        message: format!(
                            "live node {} has lease deadline {} without a durable base lease",
                            node_id.as_u32(),
                            live_deadline_ms
                        ),
                    });
                }
                (Some(_), None) => {
                    return Err(ControlPlaneError::SnapshotInvariantViolation {
                        context: "volatile heartbeat lease promotion",
                        message: format!(
                            "live node {} dropped its durable lease outside a replicated command",
                            node_id.as_u32()
                        ),
                    });
                }
                (Some(_), Some(_)) | (None, None) => {}
            }
        }
        if promoted.is_empty() {
            return Ok(None);
        }
        let authority = self.lease_grant_horizon_authority().ok_or_else(|| {
            ControlPlaneError::SnapshotInvariantViolation {
                context: "volatile heartbeat lease promotion",
                message: "live lease advances have no committed lease horizon".to_string(),
            }
        })?;
        Ok(Some(ControlPlaneCommand::PromoteNodeHeartbeatLeases {
            authority,
            promoted,
        }))
    }

    pub(crate) fn bind_metadata_transfer_fence_command(
        &self,
        durable_snapshot: &Self,
        command: ControlPlaneCommand,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        let ControlPlaneCommand::FencePgForMetadataTransfer { pg_id, .. } = command else {
            return Ok(command);
        };
        let fence_command = || ControlPlaneCommand::FencePgForMetadataTransfer {
            pg_id,
            source_primary_lease_deadline_ms: None,
            lease_horizon_authority: None,
        };
        let source_deadline = |snapshot: &Self| -> Result<Option<u64>, ControlPlaneError> {
            let applied = snapshot.apply_control_plane_command(fence_command())?;
            let ControlPlaneCommandResponse::FencePgForMetadataTransfer {
                source_primary_lease_deadline_ms,
            } = applied.response()
            else {
                unreachable!("metadata transfer fence command returned the wrong response");
            };
            Ok(*source_primary_lease_deadline_ms)
        };
        let live_source_deadline_ms = source_deadline(self)?;
        let durable_source_deadline_ms = source_deadline(durable_snapshot)?;
        if live_source_deadline_ms == durable_source_deadline_ms {
            return Ok(fence_command());
        }
        let (Some(live_source_deadline_ms), Some(durable_source_deadline_ms)) =
            (live_source_deadline_ms, durable_source_deadline_ms)
        else {
            return Err(ControlPlaneError::SnapshotInvariantViolation {
                context: "metadata transfer fence volatile lease binding",
                message: format!(
                    "live source lease deadline {live_source_deadline_ms:?} is incompatible with durable deadline {durable_source_deadline_ms:?} for PG {}",
                    pg_id.get()
                ),
            });
        };
        if live_source_deadline_ms < durable_source_deadline_ms {
            return Err(ControlPlaneError::SnapshotInvariantViolation {
                context: "metadata transfer fence volatile lease binding",
                message: format!(
                    "live source lease deadline {live_source_deadline_ms} regresses durable deadline {durable_source_deadline_ms} for PG {}",
                    pg_id.get()
                ),
            });
        }
        let lease_horizon_authority = self.lease_grant_horizon_authority().ok_or_else(|| {
            ControlPlaneError::SnapshotInvariantViolation {
                context: "metadata transfer fence volatile lease binding",
                message: format!(
                    "live source lease deadline {live_source_deadline_ms} has no lease-horizon authority for PG {}",
                    pg_id.get()
                ),
            }
        })?;
        Ok(ControlPlaneCommand::FencePgForMetadataTransfer {
            pg_id,
            source_primary_lease_deadline_ms: Some(live_source_deadline_ms),
            lease_horizon_authority: Some(lease_horizon_authority),
        })
    }

    fn bump_epoch(&mut self) -> Result<(), ControlPlaneError> {
        self.cluster_epoch = next_epoch(self.cluster_epoch)?;
        for record in self.nodes.values_mut() {
            record.pg_observations.clear();
        }
        Ok(())
    }

    #[must_use]
    pub fn heartbeat_lease_expiry_timestamp(&self, now_ms: u64) -> u64 {
        now_ms
    }

    #[must_use]
    pub fn expired_node_heartbeat_leases(
        &self,
        expire_at_ms: u64,
    ) -> Vec<ExpiredNodeHeartbeatLease> {
        self.nodes
            .values()
            .filter(|record| {
                !matches!(
                    record.membership,
                    NodeMembershipState::Out | NodeMembershipState::Removed
                ) && record.observed_availability != NodeAvailabilityState::Unavailable
            })
            .filter_map(|record| {
                record
                    .lease_deadline_ms
                    .filter(|lease_deadline_ms| *lease_deadline_ms <= expire_at_ms)
                    .map(|lease_deadline_ms| ExpiredNodeHeartbeatLease {
                        node_id: record.node_id,
                        lease_deadline_ms,
                    })
            })
            .collect()
    }

    fn record_committed_timestamp(&mut self, timestamp_ms: u64) -> bool {
        let previous = self.max_committed_timestamp_ms;
        self.max_committed_timestamp_ms = Some(match previous {
            Some(max_committed_timestamp_ms) => max_committed_timestamp_ms.max(timestamp_ms),
            None => timestamp_ms,
        });
        self.max_committed_timestamp_ms != previous
    }

    fn validate_serving_timestamp(&self, timestamp_ms: u64) -> Result<(), ControlPlaneError> {
        let Some(max_committed_timestamp_ms) = self.max_committed_timestamp_ms else {
            return Ok(());
        };
        if timestamp_ms < max_committed_timestamp_ms {
            return Err(ControlPlaneError::CommittedTimestampRegression {
                timestamp_ms,
                max_committed_timestamp_ms,
            });
        }
        Ok(())
    }

    pub fn heartbeat_lease_deadline(
        &self,
        node_id: NodeId,
        heartbeat_at_ms: u64,
        requested_lease_duration_ms: u64,
    ) -> Result<u64, ControlPlaneError> {
        self.validate_serving_timestamp(heartbeat_at_ms)?;
        let current_deadline_ms = self
            .node(node_id)
            .ok_or(ControlPlaneError::UnknownNode {
                node_id: node_id.as_u32(),
            })?
            .lease_deadline_ms();
        bounded_renewal_deadline(
            current_deadline_ms,
            heartbeat_at_ms,
            requested_lease_duration_ms,
            MAX_HEARTBEAT_LEASE_MS,
            CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        )
        .map_err(control_plane_lease_clock_error)
    }

    fn establish_heartbeat_lease_horizon(
        &mut self,
        authority: Option<LeaseHorizonAuthorityBinding>,
        heartbeat_at_ms: u64,
        lease_deadline_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        let Some(authority) = authority else {
            return Ok(());
        };
        if !self.lease_grant_horizon_covers(authority, lease_deadline_ms) {
            let horizon = CommittedLeaseGrantHorizon::establish(
                self.lease_grant_horizon,
                authority,
                heartbeat_at_ms,
                CONTROL_PLANE_LEASE_GRANT_HORIZON_DURATION_MS,
                CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
            )
            .map_err(control_plane_lease_horizon_error)?;
            self.lease_grant_horizon = Some(horizon);
        }
        if !self.lease_grant_horizon_covers(authority, lease_deadline_ms) {
            return Err(ControlPlaneError::SnapshotInvariantViolation {
                context: "committed heartbeat lease horizon does not cover its lease",
                message: format!(
                    "lease deadline {lease_deadline_ms} is outside the committed horizon"
                ),
            });
        }
        Ok(())
    }

    pub(crate) fn validate_publication_invariants(&self) -> Result<(), String> {
        validate_required_cluster_map_history(
            &self.history,
            &self.pgs,
            &self.nodes,
            self.cluster_epoch,
        )
        .map_err(|error| error.to_string())?;

        self.validate_current_state_invariants()
    }

    fn validate_current_state_invariants(&self) -> Result<(), String> {
        if let Some(certificate) = &self.initial_topology {
            certificate.validate()?;
            if self.nodes.is_empty() || self.pgs.is_empty() {
                return Err(
                    "certified initial topology requires nonempty node and PG state".to_string(),
                );
            }
        }
        self.validate_lease_grant_horizon_invariant()?;
        for pg in self.pgs.values() {
            if pg.acting_set.is_empty() {
                return Err(format!("PG {} has an empty acting set", pg.pg_id.get()));
            }
            let mut unique_nodes = BTreeSet::new();
            for node_id in &pg.acting_set {
                if !unique_nodes.insert(*node_id) {
                    return Err(format!(
                        "PG {} acting set repeats node {}",
                        pg.pg_id.get(),
                        node_id.as_u32()
                    ));
                }
                if !self.nodes.contains_key(node_id) {
                    return Err(format!(
                        "PG {} acting set references unknown node {}",
                        pg.pg_id.get(),
                        node_id.as_u32()
                    ));
                }
            }
            if let Some(previous) = &pg.previous_primary_lease {
                if previous.node_incarnation == 0 {
                    return Err(format!(
                        "PG {} previous primary has zero node incarnation",
                        pg.pg_id.get()
                    ));
                }
                if !self.nodes.contains_key(&previous.node_id) {
                    return Err(format!(
                        "PG {} previous primary references unknown node {}",
                        pg.pg_id.get(),
                        previous.node_id.as_u32()
                    ));
                }
            }

            match pg.state {
                PgState::Active => {
                    let Some(primary) = pg.active_primary else {
                        return Err(format!("active PG {} has no primary", pg.pg_id.get()));
                    };
                    if !pg.acting_set.contains(&primary) {
                        return Err(format!(
                            "active PG {} primary {} is outside the acting set",
                            pg.pg_id.get(),
                            primary.as_u32()
                        ));
                    }
                    if pg.active_metadata_proof.is_none() {
                        return Err(format!(
                            "active PG {} has no metadata proof",
                            pg.pg_id.get()
                        ));
                    }
                    if pg.active_metadata_proof_epoch.is_none() {
                        return Err(format!(
                            "active PG {} has no metadata proof epoch",
                            pg.pg_id.get()
                        ));
                    }
                    if pg.peering_metadata_proof_floor.is_some()
                        || pg.peering_metadata_proof_floor_epoch.is_some()
                        || pg.peering_metadata_proof_floor_imported
                        || pg.peering_metadata_transfer.is_some()
                        || pg.peering_metadata_transfer_source_route_epoch.is_some()
                        || pg.peering_metadata_transfer_source_node_id.is_some()
                        || pg.metadata_transfer_fenced
                        || pg.previous_primary_lease.is_some()
                        || pg
                            .metadata_transfer_fence_source_lease_deadline_ms
                            .is_some()
                        || pg.metadata_transfer_fence_source_imported
                        || pg.metadata_transfer_fence_epoch.is_some()
                    {
                        return Err(format!(
                            "active PG {} carries peering metadata-transfer state",
                            pg.pg_id.get()
                        ));
                    }
                }
                PgState::Peering => {
                    if pg.active_primary.is_some()
                        || pg.active_metadata_proof.is_some()
                        || pg.active_metadata_proof_epoch.is_some()
                        || pg.active_metadata_transfer_imported
                    {
                        return Err(format!(
                            "peering PG {} carries active metadata state",
                            pg.pg_id.get()
                        ));
                    }
                    if pg.peering_metadata_proof_floor_epoch.is_some()
                        && pg.peering_metadata_proof_floor.is_none()
                    {
                        return Err(format!(
                            "peering PG {} has a floor epoch without a proof floor",
                            pg.pg_id.get()
                        ));
                    }
                    if pg.peering_metadata_proof_floor_imported
                        && pg.peering_metadata_proof_floor_epoch.is_none()
                    {
                        return Err(format!(
                            "peering PG {} has imported floor provenance without a floor epoch",
                            pg.pg_id.get()
                        ));
                    }
                    if pg.peering_metadata_transfer.is_some()
                        && pg.peering_metadata_proof_floor.is_none()
                    {
                        return Err(format!(
                            "peering PG {} has a transfer marker without a proof floor",
                            pg.pg_id.get()
                        ));
                    }
                    if let (Some(floor), Some(transfer)) = (
                        pg.peering_metadata_proof_floor,
                        pg.peering_metadata_transfer,
                    ) {
                        if !metadata_proof_satisfies_active_floor(floor, transfer.metadata_proof())
                        {
                            return Err(format!(
                                "peering PG {} metadata transfer proof is below the proof floor",
                                pg.pg_id.get()
                            ));
                        }
                    }
                    if pg.peering_metadata_transfer.is_some() && pg.metadata_transfer_fenced {
                        return Err(format!(
                            "peering PG {} has destination metadata transfer state and source transfer fence",
                            pg.pg_id.get()
                        ));
                    }
                    if pg.peering_metadata_transfer.is_some()
                        && (pg.peering_metadata_transfer_source_route_epoch.is_none()
                            || pg.peering_metadata_transfer_source_node_id.is_none())
                    {
                        return Err(format!(
                            "peering PG {} has an incomplete transfer source route",
                            pg.pg_id.get()
                        ));
                    }
                    if pg.peering_metadata_transfer.is_none()
                        && (pg.peering_metadata_transfer_source_route_epoch.is_some()
                            || pg.peering_metadata_transfer_source_node_id.is_some())
                    {
                        return Err(format!(
                            "peering PG {} has transfer source route fields without a transfer marker",
                            pg.pg_id.get()
                        ));
                    }
                    if pg.metadata_transfer_fence_source_imported && !pg.metadata_transfer_fenced {
                        return Err(format!(
                            "peering PG {} has imported fence provenance without a transfer fence",
                            pg.pg_id.get()
                        ));
                    }
                    if pg
                        .metadata_transfer_fence_source_lease_deadline_ms
                        .is_some()
                        && !pg.metadata_transfer_fenced
                    {
                        return Err(format!(
                            "peering PG {} has a fence source lease deadline without a transfer fence",
                            pg.pg_id.get()
                        ));
                    }
                    if pg.metadata_transfer_fenced != pg.metadata_transfer_fence_epoch.is_some() {
                        return Err(format!(
                            "peering PG {} must carry a fence epoch exactly while transfer fenced",
                            pg.pg_id.get()
                        ));
                    }
                    if pg
                        .metadata_transfer_fence_epoch
                        .is_some_and(|fence_epoch| fence_epoch > self.cluster_epoch)
                    {
                        return Err(format!(
                            "peering PG {} has a metadata transfer fence epoch in the future",
                            pg.pg_id.get()
                        ));
                    }
                }
                PgState::Degraded | PgState::Backfilling | PgState::Inconsistent => {
                    if pg.active_primary.is_some()
                        || pg.active_metadata_proof.is_some()
                        || pg.active_metadata_proof_epoch.is_some()
                        || pg.active_metadata_transfer_imported
                        || pg.peering_metadata_proof_floor.is_some()
                        || pg.peering_metadata_proof_floor_epoch.is_some()
                        || pg.peering_metadata_proof_floor_imported
                        || pg.peering_metadata_transfer.is_some()
                        || pg.peering_metadata_transfer_source_route_epoch.is_some()
                        || pg.peering_metadata_transfer_source_node_id.is_some()
                        || pg.metadata_transfer_fenced
                        || pg
                            .metadata_transfer_fence_source_lease_deadline_ms
                            .is_some()
                        || pg.metadata_transfer_fence_source_imported
                        || pg.metadata_transfer_fence_epoch.is_some()
                    {
                        return Err(format!(
                            "non-active/non-peering PG {} carries active or peering metadata state",
                            pg.pg_id.get()
                        ));
                    }
                }
            }
        }

        for node in self.nodes.values() {
            if matches!(
                node.membership,
                NodeMembershipState::Out | NodeMembershipState::Removed
            ) && (node.administratively_available
                || node.observed_availability != NodeAvailabilityState::Unavailable
                || node.lease_deadline_ms.is_some())
            {
                return Err(format!(
                    "node {} with membership {:?} has administrative availability {}, observed availability {:?}, and lease deadline {:?}",
                    node.node_id.as_u32(),
                    node.membership,
                    node.administratively_available,
                    node.observed_availability,
                    node.lease_deadline_ms
                ));
            }
            if let Some(lease_deadline_ms) = node.lease_deadline_ms {
                let max_committed_timestamp_ms = self.max_committed_timestamp_ms.ok_or_else(|| {
                    format!(
                        "node {} has lease deadline {lease_deadline_ms} without a committed timestamp high-water",
                        node.node_id.as_u32()
                    )
                })?;
                let covered_by_committed_horizon = self
                    .lease_grant_horizon
                    .is_some_and(|horizon| horizon.grant_not_after_ms() >= lease_deadline_ms);
                if !covered_by_committed_horizon {
                    validate_serving_deadline_bound(
                        lease_deadline_ms,
                        max_committed_timestamp_ms,
                        MAX_HEARTBEAT_LEASE_MS,
                        CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
                    )
                    .map_err(|error| {
                        format!(
                            "node {} has an out-of-bounds serving lease: {error}",
                            node.node_id.as_u32()
                        )
                    })?;
                }
            }
            if let Some(observed_epoch) = node.last_observed_epoch {
                if observed_epoch > self.cluster_epoch {
                    return Err(format!(
                        "node {} observed future epoch {} above current {}",
                        node.node_id.as_u32(),
                        observed_epoch,
                        self.cluster_epoch
                    ));
                }
            }
            for reference in node.cluster_map_history_route_references.iter() {
                if reference.cluster_epoch() > self.cluster_epoch {
                    return Err(format!(
                        "node {} has future cluster-map history route ({}, PG {}) above current {}",
                        node.node_id.as_u32(),
                        reference.cluster_epoch(),
                        reference.pg_id().get(),
                        self.cluster_epoch
                    ));
                }
            }
            for observation in node.pg_observations.values() {
                if observation.observed_epoch != self.cluster_epoch {
                    return Err(format!(
                        "node {} observation for PG {} is from epoch {}, current is {}",
                        node.node_id.as_u32(),
                        observation.pg_id.get(),
                        observation.observed_epoch,
                        self.cluster_epoch
                    ));
                }
                let Some(pg) = self.pgs.get(&observation.pg_id) else {
                    return Err(format!(
                        "node {} observation references unknown PG {}",
                        node.node_id.as_u32(),
                        observation.pg_id.get()
                    ));
                };
                if !pg.acting_set.contains(&node.node_id) {
                    return Err(format!(
                        "node {} observation references PG {} outside the acting set",
                        node.node_id.as_u32(),
                        observation.pg_id.get()
                    ));
                }
                if pg.state == PgState::Active
                    && pg.active_primary == Some(node.node_id)
                    && observation.state == PgState::Active
                {
                    if observation.has_pending_metadata_command() {
                        return Err(format!(
                            "active primary node {} observation for PG {} has pending metadata command",
                            node.node_id.as_u32(),
                            observation.pg_id.get()
                        ));
                    }
                    let Some(expected) = pg.active_metadata_proof else {
                        return Err(format!(
                            "active PG {} is missing metadata proof",
                            pg.pg_id.get()
                        ));
                    };
                    if !metadata_proof_satisfies_active_primary_observation_floor(
                        expected,
                        observation.metadata_proof,
                        metadata_proof_progress_provenance(
                            pg.active_metadata_transfer_imported,
                            pg.active_metadata_proof_epoch,
                        ),
                        observation.observed_epoch,
                    ) {
                        return Err(format!(
                            "active primary node {} observation for PG {} has proof {:?}, expected floor {:?}",
                            node.node_id.as_u32(),
                            observation.pg_id.get(),
                            observation.metadata_proof,
                            expected
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_lease_grant_horizon_invariant(&self) -> Result<(), String> {
        let Some(horizon) = self.lease_grant_horizon else {
            return Ok(());
        };
        if horizon.grant_not_after_ms() == 0 {
            return Err("lease grant horizon deadline must be nonzero".to_owned());
        }
        let Some(max_committed_timestamp_ms) = self.max_committed_timestamp_ms else {
            return Err("lease grant horizon requires a committed timestamp high-water".to_owned());
        };
        let future_capacity_ms = horizon
            .grant_not_after_ms()
            .saturating_sub(max_committed_timestamp_ms);
        if future_capacity_ms > MAX_LEASE_GRANT_HORIZON_MS {
            return Err(format!(
                "lease grant horizon deadline {} exceeds committed timestamp high-water {} by {}ms, greater than maximum {}ms",
                horizon.grant_not_after_ms(),
                max_committed_timestamp_ms,
                future_capacity_ms,
                MAX_LEASE_GRANT_HORIZON_MS
            ));
        }
        Ok(())
    }

    #[cfg(any(test, debug_assertions))]
    fn validate_audit_invariants(&self) -> Result<(), String> {
        let mut history_epochs = BTreeSet::new();
        for history in &self.history {
            if history.cluster_epoch >= self.cluster_epoch {
                return Err(format!(
                    "history epoch {} is not older than current epoch {}",
                    history.cluster_epoch, self.cluster_epoch
                ));
            }
            if !history_epochs.insert(history.cluster_epoch) {
                return Err(format!(
                    "cluster-map history repeats epoch {}",
                    history.cluster_epoch
                ));
            }
            let nodes: BTreeSet<_> = history.nodes.iter().copied().collect();
            if nodes.len() != history.nodes.len() {
                return Err(format!(
                    "cluster-map history epoch {} repeats a node record",
                    history.cluster_epoch
                ));
            }
            let pgs: BTreeMap<_, _> = history.pgs.iter().map(|pg| (pg.pg_id, pg)).collect();
            if pgs.len() != history.pgs.len() {
                return Err(format!(
                    "cluster-map history epoch {} repeats a PG record",
                    history.cluster_epoch
                ));
            }
            let absent_pgs: BTreeSet<_> = history.absent_pgs.iter().copied().collect();
            if absent_pgs.len() != history.absent_pgs.len() {
                return Err(format!(
                    "cluster-map history epoch {} repeats an absent PG record",
                    history.cluster_epoch
                ));
            }
            if absent_pgs.iter().any(|pg_id| pgs.contains_key(pg_id)) {
                return Err(format!(
                    "cluster-map history epoch {} records a PG as both present and absent",
                    history.cluster_epoch
                ));
            }
            for pg in history.pgs() {
                validate_historical_pg_route_record(pg, history.cluster_epoch, |node_id| {
                    nodes.contains(&node_id)
                })
                .map_err(|message| {
                    format!(
                        "cluster-map history epoch {} is invalid: {message}",
                        history.cluster_epoch
                    )
                })?;
            }
        }
        validate_metadata_transfer_route_references(
            &self.history,
            self.cluster_epoch,
            self.pgs.values().map(|pg| {
                (
                    pg.pg_id,
                    pg.peering_metadata_transfer_source_route_epoch,
                    pg.peering_metadata_transfer_source_node_id,
                )
            }),
        )?;
        Ok(())
    }

    fn record_history_from(&mut self, previous: &Self) {
        if previous.cluster_epoch != self.cluster_epoch {
            self.history
                .retain(|record| record.cluster_epoch != previous.cluster_epoch);
            self.history
                .push(ClusterMapHistoryRecord::delta_between(previous, self));
        }
        let protection =
            required_cluster_map_history_protection(self.pgs.values(), self.nodes.values());
        prune_cluster_map_history(&mut self.history, &protection, self.cluster_epoch);
    }

    pub fn ready_pg_peering_completions(
        &self,
        now_ms: u64,
    ) -> Result<Vec<ReadyPgPeeringCompletion>, ControlPlaneError> {
        let mut ready = Vec::new();
        for record in self.pgs.values() {
            if record.state != PgState::Peering {
                continue;
            }
            if record.metadata_transfer_fenced {
                continue;
            }
            let Some(primary) = peering_pg_primary_for_snapshot(self, record, now_ms) else {
                continue;
            };
            let primary_node = self
                .node(primary)
                .expect("peering primary must be a known node");
            let primary_incarnation = primary_node.node_incarnation();
            if record
                .previous_primary_lease
                .as_ref()
                .is_some_and(|previous| {
                    previous.blocks_activation(
                        primary,
                        primary_incarnation,
                        primary_node.endpoint(),
                        now_ms,
                    )
                })
            {
                continue;
            }
            match validate_pg_peering_observations(self, record.pg_id, record.acting_set(), now_ms)
            {
                Ok(active_metadata_proof)
                    if validate_converged_peering_metadata_proof_floor(
                        self.cluster_epoch,
                        record.pg_id,
                        primary,
                        record.peering_metadata_proof_floor_context(),
                        record.peering_metadata_transfer,
                        active_metadata_proof,
                    )
                    .is_ok() =>
                {
                    ready.push(ReadyPgPeeringCompletion {
                        pg_id: record.pg_id,
                        primary,
                        node_incarnation: primary_incarnation,
                        active_metadata_proof,
                        active_metadata_proof_epoch: self.cluster_epoch,
                    })
                }
                Ok(_) => {}
                Err(
                    ControlPlaneError::PgPeeringMissingObservation { .. }
                    | ControlPlaneError::PgPeeringObservationNotPeering { .. }
                    | ControlPlaneError::PgPeeringMetadataProofMismatch { .. }
                    | ControlPlaneError::PgPeeringPendingMetadataCommand { .. },
                ) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(ready)
    }

    pub fn current_heartbeat_lease_for_node(
        &self,
        node_id: NodeId,
        now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        let record = self.node(node_id).ok_or(ControlPlaneError::UnknownNode {
            node_id: node_id.as_u32(),
        })?;
        Ok(HeartbeatLease {
            authority_incarnation: self.authority_incarnation,
            cluster_epoch: self.cluster_epoch,
            node_id,
            lease_deadline_ms: record.lease_deadline_ms.unwrap_or(now_ms),
            serving: record.can_serve_primary(self.cluster_epoch, now_ms),
            snapshot: self.clone(),
        })
    }

    pub fn heartbeat_lease_after_record(
        &self,
        node_id: NodeId,
        observed_epoch: ClusterEpoch,
        pre_record_epoch: ClusterEpoch,
        lease_deadline_ms: u64,
        now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        let serving = self.node(node_id).is_some_and(|record| {
            observed_epoch == pre_record_epoch
                && record.can_serve_primary(self.cluster_epoch, now_ms)
        });
        Ok(HeartbeatLease {
            authority_incarnation: self.authority_incarnation,
            cluster_epoch: self.cluster_epoch,
            node_id,
            lease_deadline_ms,
            serving,
            snapshot: self.clone(),
        })
    }
}

impl ControlPlaneCommandStateMachine for ClusterControlSnapshot {
    fn apply_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        let applied = (|| match command {
            ControlPlaneCommand::BootstrapInitialClusterMap { nodes, pg_ids } => {
                let node_ids = nodes
                    .iter()
                    .map(|(node_id, _)| *node_id)
                    .collect::<Vec<_>>();
                let pg_acting_sets = pg_ids
                    .into_iter()
                    .map(|pg_id| (pg_id, node_ids.clone()))
                    .collect();
                apply_bootstrap_initial_cluster_map(self, nodes, pg_acting_sets, None)
            }
            ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
                nodes,
                pg_acting_sets,
                topology,
            } => apply_bootstrap_initial_cluster_map(self, nodes, pg_acting_sets, Some(topology)),
            ControlPlaneCommand::SetNodeMembership {
                node_id,
                membership,
            } => {
                let mut next_snapshot = self.clone();
                let mut changed = false;
                let mut affected_node = None;
                match next_snapshot.nodes.get_mut(&node_id) {
                    Some(record) if record.membership == membership => {}
                    Some(record) if record.membership == NodeMembershipState::Removed => {
                        return Err(ControlPlaneError::RemovedNodeCannotRejoin {
                            node_id: node_id.as_u32(),
                        });
                    }
                    Some(record) => {
                        let previous_membership = record.membership;
                        record.membership = membership;
                        affected_node = Some(node_id);
                        if matches!(
                            membership,
                            NodeMembershipState::Out | NodeMembershipState::Removed
                        ) {
                            record.administratively_available = false;
                            record.observed_availability = NodeAvailabilityState::Unavailable;
                            record.lease_deadline_ms = None;
                        } else if matches!(
                            previous_membership,
                            NodeMembershipState::Out | NodeMembershipState::Removed
                        ) {
                            record.administratively_available = true;
                            record.observed_availability = NodeAvailabilityState::Suspect;
                        }
                        changed = true;
                    }
                    None => {
                        next_snapshot
                            .nodes
                            .insert(node_id, NodeControlRecord::new(node_id, membership));
                        changed = true;
                    }
                }
                if changed {
                    if let Some(node_id) = affected_node {
                        mark_pgs_peering_for_nodes(&mut next_snapshot, self, [node_id]);
                    }
                    next_snapshot.bump_epoch()?;
                }
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::SetNodeMembership,
                    changed,
                ))
            }
            ControlPlaneCommand::MarkNodeAvailability {
                node_id,
                availability,
            } => {
                let mut next_snapshot = self.clone();
                let record = next_snapshot.nodes.get_mut(&node_id).ok_or(
                    ControlPlaneError::UnknownNode {
                        node_id: node_id.as_u32(),
                    },
                )?;
                if availability != NodeAvailabilityState::Unavailable
                    && matches!(
                        record.membership,
                        NodeMembershipState::Out | NodeMembershipState::Removed
                    )
                {
                    return Err(ControlPlaneError::NodeCannotReceiveLease {
                        node_id: node_id.as_u32(),
                        membership: record.membership,
                    });
                }
                let changed = match availability {
                    NodeAvailabilityState::Unavailable => record.administratively_available,
                    NodeAvailabilityState::Healthy | NodeAvailabilityState::Suspect => {
                        !record.administratively_available
                            || record.observed_availability != availability
                    }
                };
                if changed {
                    let mut affected_node = None;
                    match availability {
                        NodeAvailabilityState::Unavailable => {
                            record.administratively_available = false;
                            record.observed_availability = NodeAvailabilityState::Unavailable;
                            record.lease_deadline_ms = None;
                            affected_node = Some(node_id);
                        }
                        NodeAvailabilityState::Healthy => {
                            record.administratively_available = true;
                            record.observed_availability = NodeAvailabilityState::Healthy;
                        }
                        NodeAvailabilityState::Suspect => {
                            record.administratively_available = true;
                            record.observed_availability = NodeAvailabilityState::Suspect;
                            record.lease_deadline_ms = None;
                            affected_node = Some(node_id);
                        }
                    }
                    if let Some(node_id) = affected_node {
                        mark_pgs_peering_for_nodes(&mut next_snapshot, self, [node_id]);
                    }
                    next_snapshot.bump_epoch()?;
                }
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::MarkNodeAvailability,
                    changed,
                ))
            }
            ControlPlaneCommand::EstablishLeaseGrantHorizon {
                authority,
                authority_now_ms,
                horizon_duration_ms,
            } => {
                if horizon_duration_ms == 0 || horizon_duration_ms > MAX_LEASE_GRANT_HORIZON_MS {
                    return Err(ControlPlaneError::InvalidLeaseGrantHorizonDuration {
                        duration_ms: horizon_duration_ms,
                        max_ms: MAX_LEASE_GRANT_HORIZON_MS,
                    });
                }
                self.validate_serving_timestamp(authority_now_ms)?;
                let horizon = CommittedLeaseGrantHorizon::establish(
                    self.lease_grant_horizon,
                    authority,
                    authority_now_ms,
                    horizon_duration_ms,
                    CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
                )
                .map_err(control_plane_lease_horizon_error)?;
                let mut next_snapshot = self.clone();
                let horizon_changed = next_snapshot.lease_grant_horizon != Some(horizon);
                next_snapshot.lease_grant_horizon = Some(horizon);
                let timestamp_changed = next_snapshot.record_committed_timestamp(authority_now_ms);
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::EstablishLeaseGrantHorizon,
                    horizon_changed || timestamp_changed,
                ))
            }
            ControlPlaneCommand::PromoteNodeHeartbeatLeases {
                authority,
                promoted,
            } => {
                if promoted.is_empty() {
                    return Err(ControlPlaneError::CommandDecode {
                        message: "heartbeat lease promotion requires at least one node".to_string(),
                    });
                }
                let horizon =
                    self.lease_grant_horizon
                        .ok_or_else(|| ControlPlaneError::CommandDecode {
                            message: "heartbeat lease promotion requires a committed lease horizon"
                                .to_string(),
                        })?;
                if horizon.authority() != authority {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "heartbeat lease promotion authority {authority:?} does not match committed horizon authority {:?}",
                            horizon.authority()
                        ),
                    });
                }
                let mut previous_node_id = None;
                for lease in &promoted {
                    if previous_node_id.is_some_and(|previous| previous >= lease.node_id) {
                        return Err(ControlPlaneError::CommandDecode {
                            message: "heartbeat lease promotion nodes are not strictly ordered"
                                .to_string(),
                        });
                    }
                    previous_node_id = Some(lease.node_id);
                    if lease.lease_deadline_ms == 0
                        || lease.lease_deadline_ms > horizon.grant_not_after_ms()
                    {
                        return Err(ControlPlaneError::CommandDecode {
                            message: format!(
                                "heartbeat lease promotion for node {} has invalid deadline {} under horizon {}",
                                lease.node_id.as_u32(),
                                lease.lease_deadline_ms,
                                horizon.grant_not_after_ms()
                            ),
                        });
                    }
                    let record =
                        self.nodes
                            .get(&lease.node_id)
                            .ok_or(ControlPlaneError::UnknownNode {
                                node_id: lease.node_id.as_u32(),
                            })?;
                    if matches!(
                        record.membership,
                        NodeMembershipState::Out | NodeMembershipState::Removed
                    ) {
                        return Err(ControlPlaneError::NodeCannotReceiveLease {
                            node_id: lease.node_id.as_u32(),
                            membership: record.membership,
                        });
                    }
                    if !record.administratively_available
                        || record.observed_availability != NodeAvailabilityState::Healthy
                    {
                        return Err(ControlPlaneError::CommandDecode {
                            message: format!(
                                "heartbeat lease promotion for node {} requires a healthy available durable node",
                                lease.node_id.as_u32()
                            ),
                        });
                    }
                    if record.node_incarnation != lease.node_incarnation {
                        return Err(ControlPlaneError::NodeIncarnationMismatch {
                            node_id: lease.node_id.as_u32(),
                            sender_incarnation: lease.node_incarnation,
                            current_incarnation: record.node_incarnation,
                        });
                    }
                    let current_deadline_ms = record.lease_deadline_ms.ok_or_else(|| {
                        ControlPlaneError::CommandDecode {
                            message: format!(
                                "heartbeat lease promotion for node {} has no durable base lease",
                                lease.node_id.as_u32()
                            ),
                        }
                    })?;
                    if current_deadline_ms > lease.lease_deadline_ms {
                        return Err(ControlPlaneError::NodeLeaseDeadlineRegression {
                            node_id: lease.node_id.as_u32(),
                            current_lease_deadline_ms: current_deadline_ms,
                            requested_lease_deadline_ms: lease.lease_deadline_ms,
                        });
                    }
                }

                let mut next_snapshot = self.clone();
                let mut changed = false;
                for lease in promoted {
                    let record = next_snapshot
                        .nodes
                        .get_mut(&lease.node_id)
                        .expect("heartbeat lease promotion node validated before mutation");
                    if record.lease_deadline_ms != Some(lease.lease_deadline_ms) {
                        record.lease_deadline_ms = Some(lease.lease_deadline_ms);
                        changed = true;
                    }
                }
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::PromoteNodeHeartbeatLeases,
                    changed,
                ))
            }
            ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat,
                heartbeat_at_ms,
                lease_deadline_ms,
                lease_horizon_authority,
            } => {
                if heartbeat.requested_lease_duration_ms == 0 {
                    return Err(ControlPlaneError::InvalidLeaseDuration);
                }
                if heartbeat.requested_lease_duration_ms > MAX_HEARTBEAT_LEASE_MS {
                    return Err(ControlPlaneError::LeaseDurationTooLong {
                        requested_ms: heartbeat.requested_lease_duration_ms,
                        max_ms: MAX_HEARTBEAT_LEASE_MS,
                    });
                }
                let current_epoch = self.cluster_epoch;
                let record =
                    self.nodes
                        .get(&heartbeat.node_id)
                        .ok_or(ControlPlaneError::UnknownNode {
                            node_id: heartbeat.node_id.as_u32(),
                        })?;
                let committed_heartbeat_at_ms = heartbeat_at_ms;
                let committed_lease_deadline_ms = self.heartbeat_lease_deadline(
                    heartbeat.node_id,
                    heartbeat_at_ms,
                    heartbeat.requested_lease_duration_ms,
                )?;
                if let Some(current_lease_deadline_ms) = record.lease_deadline_ms {
                    if lease_deadline_ms < current_lease_deadline_ms {
                        return Err(ControlPlaneError::NodeLeaseDeadlineRegression {
                            node_id: heartbeat.node_id.as_u32(),
                            current_lease_deadline_ms,
                            requested_lease_deadline_ms: lease_deadline_ms,
                        });
                    }
                }
                if lease_deadline_ms != committed_lease_deadline_ms {
                    return Err(ControlPlaneError::LeaseDeadlineMismatch {
                        node_id: heartbeat.node_id.as_u32(),
                        heartbeat_at_ms,
                        requested_ms: heartbeat.requested_lease_duration_ms,
                        expected_deadline_ms: committed_lease_deadline_ms,
                        actual_deadline_ms: lease_deadline_ms,
                    });
                }
                if matches!(
                    record.membership,
                    NodeMembershipState::Out | NodeMembershipState::Removed
                ) {
                    return Err(ControlPlaneError::NodeCannotReceiveLease {
                        node_id: heartbeat.node_id.as_u32(),
                        membership: record.membership,
                    });
                }
                if heartbeat.node_incarnation < record.node_incarnation {
                    return Err(ControlPlaneError::StaleNodeIncarnation {
                        node_id: heartbeat.node_id.as_u32(),
                        heartbeat_incarnation: heartbeat.node_incarnation,
                        current_incarnation: record.node_incarnation,
                    });
                }
                if heartbeat.observed_epoch > current_epoch {
                    return Err(ControlPlaneError::FutureNodeObservedEpoch {
                        node_id: heartbeat.node_id.as_u32(),
                        observed_epoch: heartbeat.observed_epoch,
                        current_epoch,
                    });
                }
                if heartbeat.observed_epoch != current_epoch {
                    validate_storage_cluster_map_history_floor_at_epoch(
                        self,
                        &heartbeat,
                        current_epoch,
                    )?;
                    let mut next_snapshot = self.clone();
                    next_snapshot.establish_heartbeat_lease_horizon(
                        lease_horizon_authority,
                        committed_heartbeat_at_ms,
                        committed_lease_deadline_ms,
                    )?;
                    next_snapshot.record_committed_timestamp(committed_heartbeat_at_ms);
                    let mut epoch_changed = false;
                    let mut affected_node = None;
                    {
                        let record = next_snapshot
                            .nodes
                            .get_mut(&heartbeat.node_id)
                            .expect("node record validated before heartbeat mutation");
                        if heartbeat.node_incarnation > record.node_incarnation {
                            record.node_incarnation = heartbeat.node_incarnation;
                            epoch_changed = true;
                            affected_node = Some(heartbeat.node_id);
                        }
                        if record.endpoint != heartbeat.endpoint {
                            record.endpoint = heartbeat.endpoint;
                            epoch_changed = true;
                            affected_node = Some(heartbeat.node_id);
                        }
                        record.record_observed_epoch(heartbeat.observed_epoch);
                        record.last_heartbeat_ms = Some(heartbeat_at_ms);
                        record.lease_deadline_ms = Some(lease_deadline_ms);
                        record.cluster_map_history_route_references =
                            heartbeat.cluster_map_history_route_references.clone();
                        record.pg_observations.clear();
                    }
                    if epoch_changed {
                        if let Some(node_id) = affected_node {
                            mark_pgs_peering_for_nodes(&mut next_snapshot, self, [node_id]);
                        }
                        next_snapshot.bump_epoch()?;
                    }
                    let changed = next_snapshot != *self;
                    return Ok(applied_control_plane_command(
                        self,
                        next_snapshot,
                        ControlPlaneCommandResponse::RecordNodeHeartbeat,
                        changed,
                    ));
                }
                validate_storage_cluster_map_history_floor(self, &heartbeat)?;
                let pending_active_pg_observations = validate_pg_heartbeat_observations(
                    self,
                    heartbeat.node_id,
                    &heartbeat.pg_observations,
                )?;

                let mut epoch_changed = false;
                let mut affected_node = None;
                let mut next_snapshot = self.clone();
                next_snapshot.establish_heartbeat_lease_horizon(
                    lease_horizon_authority,
                    committed_heartbeat_at_ms,
                    committed_lease_deadline_ms,
                )?;
                next_snapshot.record_committed_timestamp(committed_heartbeat_at_ms);
                {
                    let record = next_snapshot
                        .nodes
                        .get_mut(&heartbeat.node_id)
                        .expect("node record validated before heartbeat mutation");
                    if heartbeat.node_incarnation > record.node_incarnation {
                        record.node_incarnation = heartbeat.node_incarnation;
                        epoch_changed = true;
                        affected_node = Some(heartbeat.node_id);
                    }
                    if record.endpoint != heartbeat.endpoint {
                        record.endpoint = heartbeat.endpoint;
                        epoch_changed = true;
                        affected_node = Some(heartbeat.node_id);
                    }
                    if record.observed_availability != NodeAvailabilityState::Healthy {
                        record.observed_availability = NodeAvailabilityState::Healthy;
                        if record.administratively_available {
                            epoch_changed = true;
                            affected_node = Some(heartbeat.node_id);
                        }
                    }
                    record.record_observed_epoch(heartbeat.observed_epoch);
                    record.last_heartbeat_ms = Some(heartbeat_at_ms);
                    record.lease_deadline_ms = Some(lease_deadline_ms);
                    record.cluster_map_history_route_references =
                        heartbeat.cluster_map_history_route_references.clone();
                    record.pg_observations.clear();
                    for observation in &heartbeat.pg_observations {
                        record.pg_observations.insert(
                            observation.pg_id,
                            NodePgObservationRecord {
                                pg_id: observation.pg_id,
                                state: observation.state,
                                observed_epoch: heartbeat.observed_epoch,
                                observed_at_ms: heartbeat_at_ms,
                                metadata_proof: observation.metadata_proof,
                                pending_metadata_command: observation.pending_metadata_command,
                            },
                        );
                    }
                }
                for observation in &heartbeat.pg_observations {
                    let Some(pg) = next_snapshot.pgs.get_mut(&observation.pg_id) else {
                        continue;
                    };
                    // Preserve durable primary progress before this heartbeat fences the
                    // primary into Peering; otherwise an imported transfer floor can
                    // mask a later epoch-local proof after restart.
                    let primary_restart_peering_observation = epoch_changed
                        && affected_node == Some(heartbeat.node_id)
                        && observation.state == PgState::Peering;
                    if pg.state != PgState::Active
                        || pg.active_primary != Some(heartbeat.node_id)
                        || (observation.state != PgState::Active
                            && !primary_restart_peering_observation)
                        || observation.has_pending_metadata_command()
                    {
                        continue;
                    }
                    let Some(current_proof) = pg.active_metadata_proof else {
                        continue;
                    };
                    if observation.metadata_proof == current_proof {
                        continue;
                    }
                    if metadata_proof_satisfies_active_primary_observation_floor(
                        current_proof,
                        observation.metadata_proof,
                        metadata_proof_progress_provenance(
                            pg.active_metadata_transfer_imported,
                            pg.active_metadata_proof_epoch,
                        ),
                        heartbeat.observed_epoch,
                    ) && observation.metadata_proof != current_proof
                    {
                        pg.active_metadata_proof = Some(observation.metadata_proof);
                        pg.active_metadata_proof_epoch = Some(heartbeat.observed_epoch);
                        pg.active_metadata_transfer_imported = false;
                    }
                }
                if !pending_active_pg_observations.is_empty() {
                    mark_pgs_peering_for_pg_ids(
                        &mut next_snapshot,
                        self,
                        pending_active_pg_observations
                            .iter()
                            .map(|observation| observation.pg_id),
                    );
                    epoch_changed = true;
                }
                if epoch_changed {
                    if let Some(node_id) = affected_node {
                        mark_pgs_peering_for_nodes(&mut next_snapshot, self, [node_id]);
                    }
                    next_snapshot.bump_epoch()?;
                    if !pending_active_pg_observations.is_empty() {
                        // Preserve the validated pending-command evidence across the
                        // epoch bump that fences the old Active route. Treating it as
                        // a Peering observation makes recovery discoverable while
                        // preventing peering completion until the node clears it.
                        let current_epoch = next_snapshot.cluster_epoch;
                        let record = next_snapshot
                            .nodes
                            .get_mut(&heartbeat.node_id)
                            .expect("node record validated before heartbeat mutation");
                        for observation in pending_active_pg_observations {
                            record.pg_observations.insert(
                                observation.pg_id,
                                NodePgObservationRecord {
                                    pg_id: observation.pg_id,
                                    state: PgState::Peering,
                                    observed_epoch: current_epoch,
                                    observed_at_ms: heartbeat_at_ms,
                                    metadata_proof: observation.metadata_proof,
                                    pending_metadata_command: observation.pending_metadata_command,
                                },
                            );
                        }
                    }
                }
                let changed = next_snapshot != *self;
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::RecordNodeHeartbeat,
                    changed,
                ))
            }
            ControlPlaneCommand::ExpireHeartbeatLeases { expire_at_ms } => {
                if let Some(max_committed_timestamp_ms) = self.max_committed_timestamp_ms {
                    if expire_at_ms < max_committed_timestamp_ms {
                        return Err(ControlPlaneError::CommittedTimestampRegression {
                            timestamp_ms: expire_at_ms,
                            max_committed_timestamp_ms,
                        });
                    }
                }
                let committed_expire_at_ms = expire_at_ms;
                let mut next_snapshot = self.clone();
                let timestamp_changed =
                    next_snapshot.record_committed_timestamp(committed_expire_at_ms);
                let mut expired_nodes = Vec::new();
                let mut serving_expired_nodes = Vec::new();
                for record in next_snapshot.nodes.values_mut() {
                    if matches!(
                        record.membership,
                        NodeMembershipState::Out | NodeMembershipState::Removed
                    ) || record.observed_availability == NodeAvailabilityState::Unavailable
                    {
                        continue;
                    }
                    if record
                        .lease_deadline_ms
                        .is_some_and(|lease_deadline_ms| lease_deadline_ms <= expire_at_ms)
                    {
                        record.observed_availability = NodeAvailabilityState::Unavailable;
                        record.lease_deadline_ms = None;
                        expired_nodes.push(record.node_id);
                        if record.administratively_available {
                            serving_expired_nodes.push(record.node_id);
                        }
                    }
                }
                let peering_pgs = if serving_expired_nodes.is_empty() {
                    Vec::new()
                } else {
                    let peering_pgs =
                        mark_pgs_peering_for_nodes(&mut next_snapshot, self, serving_expired_nodes);
                    next_snapshot.bump_epoch()?;
                    peering_pgs
                };
                let changed = timestamp_changed || !expired_nodes.is_empty();
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::ExpireHeartbeatLeases {
                        expired_nodes,
                        peering_pgs,
                    },
                    changed,
                ))
            }
            ControlPlaneCommand::ExpireNodeHeartbeatLeases {
                authority,
                expire_at_ms,
                expired,
            } => {
                if expired.is_empty() {
                    return Err(ControlPlaneError::CommandDecode {
                        message: "targeted heartbeat expiry requires at least one node".to_string(),
                    });
                }
                if let Some(max_committed_timestamp_ms) = self.max_committed_timestamp_ms {
                    if expire_at_ms < max_committed_timestamp_ms {
                        return Err(ControlPlaneError::CommittedTimestampRegression {
                            timestamp_ms: expire_at_ms,
                            max_committed_timestamp_ms,
                        });
                    }
                }
                let mut next_snapshot = self.clone();
                next_snapshot.establish_heartbeat_lease_horizon(
                    Some(authority),
                    expire_at_ms,
                    expire_at_ms,
                )?;
                let horizon = next_snapshot
                    .lease_grant_horizon
                    .expect("targeted expiry establishes a committed lease horizon");
                let horizon_changed = self.lease_grant_horizon != Some(horizon);
                let mut previous_node_id = None;
                for lease in &expired {
                    if previous_node_id.is_some_and(|previous| previous >= lease.node_id) {
                        return Err(ControlPlaneError::CommandDecode {
                            message: "targeted heartbeat expiry nodes are not strictly ordered"
                                .to_string(),
                        });
                    }
                    previous_node_id = Some(lease.node_id);
                    if lease.lease_deadline_ms == 0
                        || lease.lease_deadline_ms > expire_at_ms
                        || lease.lease_deadline_ms > horizon.grant_not_after_ms()
                    {
                        return Err(ControlPlaneError::CommandDecode {
                            message: format!(
                                "targeted heartbeat expiry for node {} has invalid lease deadline {} at expiry {} under horizon {}",
                                lease.node_id.as_u32(),
                                lease.lease_deadline_ms,
                                expire_at_ms,
                                horizon.grant_not_after_ms()
                            ),
                        });
                    }
                    let record =
                        self.nodes
                            .get(&lease.node_id)
                            .ok_or(ControlPlaneError::UnknownNode {
                                node_id: lease.node_id.as_u32(),
                            })?;
                    if matches!(
                        record.membership,
                        NodeMembershipState::Out | NodeMembershipState::Removed
                    ) || record.observed_availability == NodeAvailabilityState::Unavailable
                    {
                        return Err(ControlPlaneError::CommandDecode {
                            message: format!(
                                "targeted heartbeat expiry for node {} no longer applies",
                                lease.node_id.as_u32()
                            ),
                        });
                    }
                    let current_deadline_ms = record.lease_deadline_ms.ok_or_else(|| {
                        ControlPlaneError::CommandDecode {
                            message: format!(
                                "targeted heartbeat expiry for node {} has no committed lease",
                                lease.node_id.as_u32()
                            ),
                        }
                    })?;
                    if current_deadline_ms > lease.lease_deadline_ms {
                        return Err(ControlPlaneError::NodeLeaseDeadlineRegression {
                            node_id: lease.node_id.as_u32(),
                            current_lease_deadline_ms: current_deadline_ms,
                            requested_lease_deadline_ms: lease.lease_deadline_ms,
                        });
                    }
                }

                let timestamp_changed = next_snapshot.record_committed_timestamp(expire_at_ms);
                let mut expired_nodes = Vec::with_capacity(expired.len());
                let mut serving_expired_nodes = Vec::new();
                for lease in expired {
                    let record = next_snapshot
                        .nodes
                        .get_mut(&lease.node_id)
                        .expect("targeted heartbeat expiry node validated before mutation");
                    record.observed_availability = NodeAvailabilityState::Unavailable;
                    record.lease_deadline_ms = None;
                    expired_nodes.push(lease.node_id);
                    if record.administratively_available {
                        serving_expired_nodes.push(lease.node_id);
                    }
                }
                let peering_pgs = if serving_expired_nodes.is_empty() {
                    Vec::new()
                } else {
                    let peering_pgs =
                        mark_pgs_peering_for_nodes(&mut next_snapshot, self, serving_expired_nodes);
                    next_snapshot.bump_epoch()?;
                    peering_pgs
                };
                let changed = horizon_changed || timestamp_changed || !expired_nodes.is_empty();
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::ExpireHeartbeatLeases {
                        expired_nodes,
                        peering_pgs,
                    },
                    changed,
                ))
            }
            ControlPlaneCommand::SetPgActingSet { pg_id, acting_set } => {
                validate_acting_set(self, pg_id, &acting_set)?;
                validate_acting_set_change_ready(self, pg_id, &acting_set)?;
                validate_acting_set_preserves_pending_recovery(self, pg_id, &acting_set)?;
                let mut next_snapshot = self.clone();
                let mut changed = false;
                match next_snapshot.pgs.get_mut(&pg_id) {
                    Some(record) if record.acting_set == acting_set => {}
                    Some(record) => {
                        let previous_primary_lease = active_primary_lease(self, record)
                            .or_else(|| record.previous_primary_lease.clone())
                            .map(PreviousPrimaryLease::without_reactivation_preference);
                        let (
                            peering_metadata_proof_floor,
                            peering_metadata_proof_floor_epoch,
                            peering_metadata_proof_floor_imported,
                        ) = match record.state {
                            PgState::Active => (
                                Some(validate_authoritative_metadata_migration_source(
                                    self,
                                    record,
                                    &acting_set,
                                )?),
                                record.active_metadata_proof_epoch,
                                record.active_metadata_transfer_imported,
                            ),
                            PgState::Peering => {
                                if let Some(floor) = record.peering_metadata_proof_floor {
                                    validate_peering_metadata_migration_source(
                                        self,
                                        record,
                                        &acting_set,
                                        floor,
                                    )?;
                                    (
                                        Some(floor),
                                        record.peering_metadata_proof_floor_epoch,
                                        record.peering_metadata_proof_floor_imported,
                                    )
                                } else {
                                    (None, None, false)
                                }
                            }
                            _ => (None, None, false),
                        };
                        let (
                            peering_metadata_transfer,
                            peering_metadata_transfer_source_route_epoch,
                            peering_metadata_transfer_source_node_id,
                        ) = if record.state == PgState::Peering {
                            match record.peering_metadata_transfer {
                                Some(transfer) => (
                                    Some(transfer),
                                    record.peering_metadata_transfer_source_route_epoch,
                                    record.peering_metadata_transfer_source_node_id,
                                ),
                                None => (None, None, None),
                            }
                        } else {
                            (None, None, None)
                        };
                        let metadata_transfer_fenced = if record.state == PgState::Peering {
                            record.metadata_transfer_fenced
                        } else {
                            false
                        };
                        let metadata_transfer_fence_source_lease_deadline_ms = if record.state
                            == PgState::Peering
                            && record.metadata_transfer_fenced
                        {
                            record.metadata_transfer_fence_source_lease_deadline_ms
                        } else {
                            None
                        };
                        let metadata_transfer_fence_source_imported = record.state
                            == PgState::Peering
                            && record.metadata_transfer_fenced
                            && record.metadata_transfer_fence_source_imported;
                        let metadata_transfer_fence_epoch = if record.state == PgState::Peering
                            && record.metadata_transfer_fenced
                        {
                            record.metadata_transfer_fence_epoch
                        } else {
                            None
                        };
                        record.acting_set = acting_set;
                        record.state = PgState::Peering;
                        record.active_primary = None;
                        record.active_metadata_proof = None;
                        record.active_metadata_proof_epoch = None;
                        record.active_metadata_transfer_imported = false;
                        record.previous_primary_lease = previous_primary_lease;
                        record.peering_metadata_proof_floor = peering_metadata_proof_floor;
                        record.peering_metadata_proof_floor_epoch =
                            peering_metadata_proof_floor_epoch;
                        record.peering_metadata_proof_floor_imported =
                            peering_metadata_proof_floor_imported;
                        record.peering_metadata_transfer = peering_metadata_transfer;
                        record.peering_metadata_transfer_source_route_epoch =
                            peering_metadata_transfer_source_route_epoch;
                        record.peering_metadata_transfer_source_node_id =
                            peering_metadata_transfer_source_node_id;
                        record.metadata_transfer_fenced = metadata_transfer_fenced;
                        record.metadata_transfer_fence_source_lease_deadline_ms =
                            metadata_transfer_fence_source_lease_deadline_ms;
                        record.metadata_transfer_fence_source_imported =
                            metadata_transfer_fence_source_imported;
                        record.metadata_transfer_fence_epoch = metadata_transfer_fence_epoch;
                        changed = true;
                    }
                    None => {
                        next_snapshot
                            .pgs
                            .insert(pg_id, PgControlRecord::new(pg_id, acting_set));
                        changed = true;
                    }
                }
                if changed {
                    next_snapshot.bump_epoch()?;
                }
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::SetPgActingSet,
                    changed,
                ))
            }
            ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
                pg_id,
                acting_set,
                transfer,
                expected_destination_epoch,
            } => {
                validate_acting_set(self, pg_id, &acting_set)?;
                validate_acting_set_preserves_pending_recovery(self, pg_id, &acting_set)?;
                let record = self
                    .pg(pg_id)
                    .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
                if record.acting_set == acting_set {
                    let existing_transfer = record.peering_metadata_transfer.ok_or(
                        ControlPlaneError::PgMetadataMigrationRequiresTransfer {
                            pg_id: pg_id.get(),
                        },
                    )?;
                    if existing_transfer != transfer {
                        return Err(ControlPlaneError::PgMetadataTransferProofMismatch {
                            pg_id: pg_id.get(),
                            expected: existing_transfer,
                            actual: transfer,
                        });
                    }
                    let actual_destination_epoch =
                        peering_metadata_transfer_destination_epoch(record)?.ok_or(
                            ControlPlaneError::PgMetadataMigrationRequiresTransfer {
                                pg_id: pg_id.get(),
                            },
                        )?;
                    if actual_destination_epoch != expected_destination_epoch {
                        return Err(
                            ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
                                pg_id: pg_id.get(),
                                expected_destination_epoch,
                                actual_destination_epoch,
                            },
                        );
                    }
                    return Ok(applied_control_plane_command(
                        self,
                        self.clone(),
                        ControlPlaneCommandResponse::SetPgActingSetWithMetadataTransfer,
                        false,
                    ));
                }
                let actual_destination_epoch = next_epoch(self.cluster_epoch)?;
                if actual_destination_epoch != expected_destination_epoch {
                    return Err(
                        ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
                            pg_id: pg_id.get(),
                            expected_destination_epoch,
                            actual_destination_epoch,
                        },
                    );
                }
                let state = record.state;
                let metadata_transfer_fenced = record.metadata_transfer_fenced;
                let metadata_transfer_fence_source_imported =
                    record.metadata_transfer_fence_source_imported;
                let metadata_transfer_fence_epoch = record.metadata_transfer_fence_epoch;
                let (required_floor, required_floor_epoch) = match state {
                    PgState::Active => (
                        record.active_metadata_proof.ok_or(
                            ControlPlaneError::ActivePgMissingMetadataProof { pg_id: pg_id.get() },
                        )?,
                        record.active_metadata_proof_epoch,
                    ),
                    PgState::Peering => (
                        record.peering_metadata_proof_floor.ok_or(
                            ControlPlaneError::PgMetadataMigrationRequiresTransfer {
                                pg_id: pg_id.get(),
                            },
                        )?,
                        record.peering_metadata_proof_floor_epoch,
                    ),
                    _ => {
                        return Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer {
                            pg_id: pg_id.get(),
                        });
                    }
                };
                validate_metadata_transfer_proof(MetadataTransferProofValidation {
                    snapshot: self,
                    pg_id,
                    state,
                    metadata_transfer_fenced,
                    metadata_transfer_fence_source_imported,
                    metadata_transfer_fence_epoch,
                    required_floor,
                    required_floor_epoch,
                    transfer,
                })?;
                let source_route_epoch = self.cluster_epoch;
                let source_node_id =
                    match state {
                        PgState::Active => record.active_primary.ok_or(
                            ControlPlaneError::PgHasNoServingPrimary {
                                pg_id: pg_id.get(),
                                cluster_epoch: self.cluster_epoch,
                            },
                        )?,
                        PgState::Peering => record
                            .acting_set
                            .first()
                            .copied()
                            .ok_or(ControlPlaneError::EmptyActingSet { pg_id: pg_id.get() })?,
                        _ => {
                            return Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer {
                                pg_id: pg_id.get(),
                            });
                        }
                    };

                let mut next_snapshot = self.clone();
                let record = next_snapshot
                    .pgs
                    .get_mut(&pg_id)
                    .expect("PG record validated before metadata transfer");
                let previous_primary_lease = active_primary_lease(self, record)
                    .or_else(|| record.previous_primary_lease.clone())
                    .map(PreviousPrimaryLease::without_reactivation_preference);
                record.acting_set = acting_set;
                record.state = PgState::Peering;
                record.active_primary = None;
                record.active_metadata_proof = None;
                record.active_metadata_proof_epoch = None;
                record.active_metadata_transfer_imported = false;
                record.previous_primary_lease = previous_primary_lease;
                record.peering_metadata_proof_floor = Some(transfer.metadata_proof());
                record.peering_metadata_proof_floor_epoch = Some(self.cluster_epoch);
                record.peering_metadata_proof_floor_imported = true;
                record.peering_metadata_transfer = Some(transfer);
                record.peering_metadata_transfer_source_route_epoch = Some(source_route_epoch);
                record.peering_metadata_transfer_source_node_id = Some(source_node_id);
                record.metadata_transfer_fenced = false;
                record.metadata_transfer_fence_source_lease_deadline_ms = None;
                record.metadata_transfer_fence_source_imported = false;
                record.metadata_transfer_fence_epoch = None;
                next_snapshot.bump_epoch()?;
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::SetPgActingSetWithMetadataTransfer,
                    true,
                ))
            }
            ControlPlaneCommand::FencePgForMetadataTransfer {
                pg_id,
                source_primary_lease_deadline_ms: committed_source_lease_deadline_ms,
                lease_horizon_authority,
            } => {
                let record = self
                    .pg(pg_id)
                    .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
                let observed_source_lease_deadline_ms = if record.state == PgState::Active {
                    let primary = record
                        .active_primary
                        .filter(|primary| record.acting_set.contains(primary))
                        .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                            pg_id: pg_id.get(),
                            cluster_epoch: self.cluster_epoch(),
                        })?;
                    let primary_record =
                        self.node(primary)
                            .ok_or(ControlPlaneError::UnknownActingSetNode {
                                pg_id: pg_id.get(),
                                node_id: primary.as_u32(),
                            })?;
                    Some(primary_record.lease_deadline_ms.ok_or(
                        ControlPlaneError::PgHasNoServingPrimary {
                            pg_id: pg_id.get(),
                            cluster_epoch: self.cluster_epoch(),
                        },
                    )?)
                } else if record.state == PgState::Peering && record.metadata_transfer_fenced {
                    record
                        .metadata_transfer_fence_source_lease_deadline_ms
                        .or_else(|| {
                            record
                                .acting_set
                                .iter()
                                .filter_map(|node_id| {
                                    self.node(*node_id).and_then(|node| node.lease_deadline_ms)
                                })
                                .max()
                        })
                } else {
                    None
                };
                let source_primary_lease_deadline_ms = match (
                    committed_source_lease_deadline_ms,
                    lease_horizon_authority,
                    observed_source_lease_deadline_ms,
                ) {
                    (None, None, observed) => observed,
                    (Some(committed), Some(authority), Some(observed)) if committed >= observed => {
                        if !self.lease_grant_horizon_covers(authority, committed) {
                            return Err(ControlPlaneError::CommandDecode {
                                message: format!(
                                    "metadata transfer fence for PG {} source lease deadline {} is not covered by the committed lease horizon",
                                    pg_id.get(), committed
                                ),
                            });
                        }
                        Some(committed)
                    }
                    (Some(committed), Some(_), Some(observed)) => {
                        return Err(ControlPlaneError::CommandDecode {
                            message: format!(
                                "metadata transfer fence for PG {} regresses source lease deadline from {} to {}",
                                    pg_id.get(), observed, committed
                            ),
                        });
                    }
                    (Some(committed), Some(_), None) => {
                        return Err(ControlPlaneError::CommandDecode {
                            message: format!(
                                "metadata transfer fence for PG {} supplies source lease deadline {} without a live or retained source lease",
                                    pg_id.get(), committed
                            ),
                        });
                    }
                    (deadline, authority, _) => {
                        return Err(ControlPlaneError::CommandDecode {
                            message: format!(
                                "metadata transfer fence for PG {} must carry both source lease deadline and lease horizon authority, or neither (deadline={deadline:?}, authority={authority:?})",
                                pg_id.get()
                            ),
                        });
                    }
                };
                let previous_primary_lease = active_primary_lease(self, record)
                    .or_else(|| record.previous_primary_lease.clone())
                    .map(|mut previous| {
                        if let Some(source_primary_lease_deadline_ms) =
                            source_primary_lease_deadline_ms
                        {
                            previous.lease_deadline_ms = source_primary_lease_deadline_ms;
                        }
                        previous.without_reactivation_preference()
                    });
                let active_source_floor = match record.state {
                    PgState::Peering => {
                        if record.peering_metadata_transfer.is_some() {
                            return Ok(applied_control_plane_command(
                                self,
                                self.clone(),
                                ControlPlaneCommandResponse::FencePgForMetadataTransfer {
                                    source_primary_lease_deadline_ms: None,
                                },
                                false,
                            ));
                        }
                        None
                    }
                    PgState::Active => Some(validate_authoritative_metadata_migration_source(
                        self,
                        record,
                        record.acting_set(),
                    )?),
                    state => {
                        return Err(ControlPlaneError::PgNotActive {
                            pg_id: pg_id.get(),
                            cluster_epoch: self.cluster_epoch(),
                            state,
                        });
                    }
                };
                let fence_epoch =
                    if record.state != PgState::Peering || !record.metadata_transfer_fenced {
                        Some(next_epoch(self.cluster_epoch)?)
                    } else {
                        None
                    };
                let mut next_snapshot = self.clone();
                let mut changed = false;
                let record = next_snapshot
                    .pgs
                    .get_mut(&pg_id)
                    .expect("PG record validated before metadata transfer fence");
                if record.state != PgState::Peering {
                    let active_metadata_transfer_imported =
                        record.active_metadata_transfer_imported;
                    let active_metadata_proof_epoch = record.active_metadata_proof_epoch;
                    record.peering_metadata_proof_floor = active_source_floor;
                    record.peering_metadata_proof_floor_epoch = active_metadata_proof_epoch;
                    record.peering_metadata_proof_floor_imported =
                        active_metadata_transfer_imported;
                    record.state = PgState::Peering;
                    record.active_primary = None;
                    record.active_metadata_proof = None;
                    record.active_metadata_proof_epoch = None;
                    record.active_metadata_transfer_imported = false;
                    record.previous_primary_lease = previous_primary_lease;
                    record.peering_metadata_transfer = None;
                    record.peering_metadata_transfer_source_route_epoch = None;
                    record.peering_metadata_transfer_source_node_id = None;
                    record.metadata_transfer_fenced = true;
                    record.metadata_transfer_fence_source_lease_deadline_ms =
                        source_primary_lease_deadline_ms;
                    record.metadata_transfer_fence_source_imported =
                        active_metadata_transfer_imported;
                    record.metadata_transfer_fence_epoch = fence_epoch;
                    changed = true;
                } else if !record.metadata_transfer_fenced {
                    record.previous_primary_lease = previous_primary_lease;
                    record.metadata_transfer_fenced = true;
                    record.metadata_transfer_fence_source_lease_deadline_ms =
                        source_primary_lease_deadline_ms;
                    record.metadata_transfer_fence_source_imported =
                        record.peering_metadata_proof_floor_imported;
                    record.metadata_transfer_fence_epoch = fence_epoch;
                    changed = true;
                }
                if changed {
                    next_snapshot.bump_epoch()?;
                    debug_assert_eq!(Some(next_snapshot.cluster_epoch), fence_epoch);
                }
                let response_snapshot = if changed { &next_snapshot } else { self };
                let source_primary_lease_deadline_ms = response_snapshot
                    .pg(pg_id)
                    .and_then(PgControlRecord::metadata_transfer_fence_source_lease_deadline_ms)
                    .or(source_primary_lease_deadline_ms);
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::FencePgForMetadataTransfer {
                        source_primary_lease_deadline_ms,
                    },
                    changed,
                ))
            }
            ControlPlaneCommand::SetPgState { pg_id, state } => {
                if state == PgState::Active {
                    return Err(ControlPlaneError::ActivePgRequiresPeeringComplete {
                        pg_id: pg_id.get(),
                    });
                }
                let mut next_snapshot = self.clone();
                let record = next_snapshot
                    .pgs
                    .get_mut(&pg_id)
                    .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
                let changed = record.state != state;
                if changed {
                    let previous_primary_lease = active_primary_lease(self, record)
                        .or_else(|| record.previous_primary_lease.clone())
                        .map(PreviousPrimaryLease::without_reactivation_preference);
                    let peering_metadata_proof_floor_epoch = if record.state == PgState::Active {
                        record.active_metadata_proof_epoch
                    } else {
                        None
                    };
                    let peering_metadata_proof_floor_imported =
                        record.state == PgState::Active && record.active_metadata_transfer_imported;
                    record.peering_metadata_proof_floor = if record.state == PgState::Active {
                        record.active_metadata_proof
                    } else {
                        None
                    };
                    record.peering_metadata_proof_floor_epoch = peering_metadata_proof_floor_epoch;
                    record.peering_metadata_proof_floor_imported =
                        peering_metadata_proof_floor_imported;
                    record.state = state;
                    record.active_primary = None;
                    record.active_metadata_proof = None;
                    record.active_metadata_proof_epoch = None;
                    record.active_metadata_transfer_imported = false;
                    record.previous_primary_lease = previous_primary_lease;
                    record.metadata_transfer_fenced = false;
                    record.metadata_transfer_fence_source_lease_deadline_ms = None;
                    record.metadata_transfer_fence_source_imported = false;
                    record.metadata_transfer_fence_epoch = None;
                    if state != PgState::Peering {
                        record.peering_metadata_proof_floor = None;
                        record.peering_metadata_proof_floor_epoch = None;
                        record.peering_metadata_proof_floor_imported = false;
                        record.peering_metadata_transfer = None;
                        record.peering_metadata_transfer_source_route_epoch = None;
                        record.peering_metadata_transfer_source_node_id = None;
                        record.metadata_transfer_fenced = false;
                        record.metadata_transfer_fence_source_lease_deadline_ms = None;
                        record.metadata_transfer_fence_source_imported = false;
                        record.metadata_transfer_fence_epoch = None;
                    }
                    next_snapshot.bump_epoch()?;
                }
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::SetPgState,
                    changed,
                ))
            }
            ControlPlaneCommand::CompletePgPeering {
                pg_id,
                primary,
                node_incarnation,
                complete_at_ms,
            } => {
                self.validate_serving_timestamp(complete_at_ms)?;
                let validated = validate_pg_peering_completion(PgPeeringCompletionValidation {
                    snapshot: self,
                    pg_id,
                    primary,
                    node_incarnation,
                    completed_at_ms: complete_at_ms,
                    expected: None,
                })?;
                match validated {
                    ValidatedPgPeeringCompletion::AlreadyActive => {
                        let mut next_snapshot = self.clone();
                        let changed = next_snapshot.record_committed_timestamp(complete_at_ms);
                        Ok(applied_control_plane_command(
                            self,
                            next_snapshot,
                            ControlPlaneCommandResponse::CompletePgPeering,
                            changed,
                        ))
                    }
                    ValidatedPgPeeringCompletion::Complete {
                        active_metadata_proof,
                        active_metadata_proof_epoch,
                    } => {
                        let mut next_snapshot = self.clone();
                        next_snapshot.record_committed_timestamp(complete_at_ms);
                        let record = next_snapshot
                            .pgs
                            .get_mut(&pg_id)
                            .expect("PG record validated before peering completion");
                        let active_metadata_transfer_imported =
                            record.peering_metadata_transfer.is_some();
                        record.state = PgState::Active;
                        record.active_primary = Some(primary);
                        record.active_metadata_proof = Some(active_metadata_proof);
                        record.active_metadata_transfer_imported =
                            active_metadata_transfer_imported;
                        record.previous_primary_lease = None;
                        record.peering_metadata_proof_floor = None;
                        record.peering_metadata_proof_floor_epoch = None;
                        record.peering_metadata_proof_floor_imported = false;
                        record.peering_metadata_transfer = None;
                        record.peering_metadata_transfer_source_route_epoch = None;
                        record.peering_metadata_transfer_source_node_id = None;
                        record.metadata_transfer_fenced = false;
                        record.metadata_transfer_fence_source_lease_deadline_ms = None;
                        record.metadata_transfer_fence_source_imported = false;
                        record.metadata_transfer_fence_epoch = None;
                        next_snapshot.bump_epoch()?;
                        next_snapshot
                            .pgs
                            .get_mut(&pg_id)
                            .expect("PG record activated before epoch bump")
                            .active_metadata_proof_epoch = Some(active_metadata_proof_epoch);
                        Ok(applied_control_plane_command(
                            self,
                            next_snapshot,
                            ControlPlaneCommandResponse::CompletePgPeering,
                            true,
                        ))
                    }
                }
            }
            ControlPlaneCommand::CompleteReadyPgPeerings { ready_at_ms, ready } => {
                self.validate_serving_timestamp(ready_at_ms)?;
                if ready.is_empty() {
                    let mut next_snapshot = self.clone();
                    let changed = next_snapshot.record_committed_timestamp(ready_at_ms);
                    return Ok(applied_control_plane_command(
                        self,
                        next_snapshot,
                        ControlPlaneCommandResponse::CompleteReadyPgPeerings,
                        changed,
                    ));
                }

                let mut unique_pg_ids = BTreeSet::new();
                for completion in &ready {
                    if !unique_pg_ids.insert(completion.pg_id) {
                        return Err(ControlPlaneError::DuplicateReadyPgPeeringCompletion {
                            pg_id: completion.pg_id.get(),
                        });
                    }
                }
                for completion in &ready {
                    validate_pg_peering_completion(PgPeeringCompletionValidation {
                        snapshot: self,
                        pg_id: completion.pg_id,
                        primary: completion.primary,
                        node_incarnation: completion.node_incarnation,
                        completed_at_ms: ready_at_ms,
                        expected: Some(ExpectedPgPeeringCompletion {
                            active_metadata_proof: completion.active_metadata_proof,
                            active_metadata_proof_epoch: completion.active_metadata_proof_epoch,
                        }),
                    })?;
                }

                let mut next_snapshot = self.clone();
                let timestamp_changed = next_snapshot.record_committed_timestamp(ready_at_ms);
                let mut completed_any = false;
                for completion in &ready {
                    let record = next_snapshot
                        .pgs
                        .get_mut(&completion.pg_id)
                        .expect("ready PG must exist in cloned snapshot");
                    if record.state == PgState::Active {
                        continue;
                    }
                    completed_any = true;
                    record.state = PgState::Active;
                    record.active_primary = Some(completion.primary);
                    record.active_metadata_proof = Some(completion.active_metadata_proof);
                    record.active_metadata_transfer_imported =
                        record.peering_metadata_transfer.is_some();
                    record.previous_primary_lease = None;
                    record.peering_metadata_proof_floor = None;
                    record.peering_metadata_proof_floor_epoch = None;
                    record.peering_metadata_proof_floor_imported = false;
                    record.peering_metadata_transfer = None;
                    record.peering_metadata_transfer_source_route_epoch = None;
                    record.peering_metadata_transfer_source_node_id = None;
                    record.metadata_transfer_fenced = false;
                    record.metadata_transfer_fence_source_lease_deadline_ms = None;
                    record.metadata_transfer_fence_source_imported = false;
                    record.metadata_transfer_fence_epoch = None;
                }
                if completed_any {
                    next_snapshot.bump_epoch()?;
                    for completion in &ready {
                        let record = next_snapshot
                            .pgs
                            .get_mut(&completion.pg_id)
                            .expect("ready PG activated before epoch bump");
                        if record.state == PgState::Active
                            && record.active_primary == Some(completion.primary)
                            && record.active_metadata_proof
                                == Some(completion.active_metadata_proof)
                        {
                            record.active_metadata_proof_epoch =
                                Some(completion.active_metadata_proof_epoch);
                        }
                    }
                }
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot,
                    ControlPlaneCommandResponse::CompleteReadyPgPeerings,
                    completed_any || timestamp_changed,
                ))
            }
        })()?;
        validate_control_plane_snapshot(
            "control-plane command produced invalid snapshot",
            applied.snapshot(),
        )?;
        Ok(applied)
    }
}

fn apply_bootstrap_initial_cluster_map(
    snapshot: &ClusterControlSnapshot,
    nodes: Vec<(NodeId, String)>,
    pg_acting_sets: Vec<(PgId, Vec<NodeId>)>,
    initial_topology: Option<InitialClusterTopologyCertificate>,
) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
    if snapshot.nodes().next().is_some() || snapshot.pgs().next().is_some() {
        return Err(ControlPlaneError::BootstrapRequiresEmptyState);
    }
    if nodes.is_empty() {
        return Err(ControlPlaneError::EmptyActingSet { pg_id: 0 });
    }

    let mut unique_nodes = BTreeSet::new();
    for (node_id, endpoint) in &nodes {
        if !unique_nodes.insert(*node_id) {
            return Err(ControlPlaneError::DuplicateActingSetNode {
                pg_id: 0,
                node_id: node_id.as_u32(),
            });
        }
        if endpoint.is_empty() {
            return Err(ControlPlaneError::NodeEndpointMissing {
                node_id: node_id.as_u32(),
                cluster_epoch: snapshot.cluster_epoch(),
            });
        }
    }

    let mut unique_pgs = BTreeSet::new();
    for (pg_id, acting_set) in &pg_acting_sets {
        if !unique_pgs.insert(*pg_id) {
            return Err(ControlPlaneError::DuplicateBootstrapPg { pg_id: pg_id.get() });
        }
        if acting_set.is_empty() {
            return Err(ControlPlaneError::EmptyActingSet { pg_id: pg_id.get() });
        }
        let mut unique_acting_set = BTreeSet::new();
        for node_id in acting_set {
            if !unique_nodes.contains(node_id) {
                return Err(ControlPlaneError::UnknownActingSetNode {
                    pg_id: pg_id.get(),
                    node_id: node_id.as_u32(),
                });
            }
            if !unique_acting_set.insert(*node_id) {
                return Err(ControlPlaneError::DuplicateActingSetNode {
                    pg_id: pg_id.get(),
                    node_id: node_id.as_u32(),
                });
            }
        }
    }

    if let Some(certificate) = &initial_topology {
        let actual_digest = initial_cluster_bootstrap_map_digest(&nodes, &pg_acting_sets);
        if certificate.bootstrap_map_digest() != &actual_digest {
            return Err(ControlPlaneError::InvalidInitialTopology {
                message: "certified initial topology bootstrap-map digest does not match storage-node endpoints and PG acting sets"
                    .to_string(),
            });
        }
    }

    let mut next_snapshot = snapshot.clone();
    next_snapshot.initial_topology = initial_topology;
    for (node_id, endpoint) in nodes {
        let mut record = NodeControlRecord::new(node_id, NodeMembershipState::Active);
        record.endpoint = endpoint;
        next_snapshot.nodes.insert(node_id, record);
    }
    for (pg_id, acting_set) in pg_acting_sets {
        next_snapshot
            .pgs
            .insert(pg_id, PgControlRecord::new(pg_id, acting_set));
    }
    next_snapshot.bump_epoch()?;
    Ok(applied_control_plane_command(
        snapshot,
        next_snapshot,
        ControlPlaneCommandResponse::BootstrapInitialClusterMap,
        true,
    ))
}

fn applied_control_plane_command(
    previous_snapshot: &ClusterControlSnapshot,
    mut next_snapshot: ClusterControlSnapshot,
    response: ControlPlaneCommandResponse,
    changed: bool,
) -> AppliedControlPlaneCommand {
    if changed {
        next_snapshot.record_history_from(previous_snapshot);
    }
    AppliedControlPlaneCommand::new(next_snapshot, response, changed)
}

fn authorize_node_service_for_snapshot(
    snapshot: &ClusterControlSnapshot,
    node_id: NodeId,
    node_incarnation: u64,
    observed_epoch: ClusterEpoch,
    now_ms: u64,
) -> Result<NodeServiceAuthorization, ControlPlaneError> {
    let record = snapshot
        .nodes
        .get(&node_id)
        .ok_or(ControlPlaneError::UnknownNode {
            node_id: node_id.as_u32(),
        })?;
    if matches!(
        record.membership,
        NodeMembershipState::Out | NodeMembershipState::Removed
    ) {
        return Err(ControlPlaneError::NodeCannotReceiveLease {
            node_id: node_id.as_u32(),
            membership: record.membership,
        });
    }
    if node_incarnation != record.node_incarnation {
        return Err(ControlPlaneError::NodeIncarnationMismatch {
            node_id: node_id.as_u32(),
            sender_incarnation: node_incarnation,
            current_incarnation: record.node_incarnation,
        });
    }
    if observed_epoch != snapshot.cluster_epoch {
        return Err(ControlPlaneError::StaleNodeObservedEpoch {
            node_id: node_id.as_u32(),
            observed_epoch,
            current_epoch: snapshot.cluster_epoch,
        });
    }
    let lease_deadline_ms =
        record
            .lease_deadline_ms
            .ok_or(ControlPlaneError::NodeLeaseExpired {
                node_id: node_id.as_u32(),
                now_ms,
                lease_deadline_ms: None,
            })?;
    if lease_deadline_ms <= now_ms {
        return Err(ControlPlaneError::NodeLeaseExpired {
            node_id: node_id.as_u32(),
            now_ms,
            lease_deadline_ms: Some(lease_deadline_ms),
        });
    }
    if !record.can_serve_primary(snapshot.cluster_epoch, now_ms) {
        return Err(ControlPlaneError::NodeNotServingCurrentEpoch {
            node_id: node_id.as_u32(),
            cluster_epoch: snapshot.cluster_epoch,
        });
    }
    Ok(NodeServiceAuthorization {
        authority_incarnation: snapshot.authority_incarnation,
        cluster_epoch: snapshot.cluster_epoch,
        node_id,
        node_incarnation,
        lease_deadline_ms,
    })
}

fn deterministic_pg_primary_for_snapshot(
    snapshot: &ClusterControlSnapshot,
    acting_set: &[NodeId],
    now_ms: u64,
) -> Option<NodeId> {
    acting_set.iter().copied().find(|node_id| {
        snapshot
            .nodes
            .get(node_id)
            .is_some_and(|record| record.can_serve_primary(snapshot.cluster_epoch, now_ms))
    })
}

fn peering_pg_primary_for_snapshot(
    snapshot: &ClusterControlSnapshot,
    record: &PgControlRecord,
    now_ms: u64,
) -> Option<NodeId> {
    if record.peering_metadata_transfer.is_none() {
        if let Some(previous) = record.previous_primary_lease.as_ref().filter(|previous| {
            previous.prefer_reactivation
                && previous.lease_deadline_ms > now_ms
                && record.acting_set.contains(&previous.node_id)
                && snapshot.nodes.get(&previous.node_id).is_some_and(|node| {
                    previous.matches_process(
                        node.node_id(),
                        node.node_incarnation(),
                        node.endpoint(),
                    ) && node.can_serve_primary(snapshot.cluster_epoch, now_ms)
                })
        }) {
            return Some(previous.node_id);
        }
    }
    deterministic_pg_primary_for_snapshot(snapshot, record.acting_set(), now_ms)
}

fn peering_metadata_read_route_for_snapshot(
    snapshot: &ClusterControlSnapshot,
    record: &PgControlRecord,
    now_ms: u64,
) -> Option<PgMetadataReadRoute> {
    let committed_floor = record.peering_metadata_proof_floor_context()?;
    record.acting_set.iter().copied().find_map(|node_id| {
        let node = snapshot.node(node_id)?;
        if !node.can_serve_primary(snapshot.cluster_epoch, now_ms) {
            return None;
        }
        let observation = node.pg_observation(record.pg_id)?;
        if observation.observed_epoch != snapshot.cluster_epoch
            || observation.state != PgState::Peering
            || observation.has_pending_metadata_command()
            || !peering_metadata_proof_is_read_certified(
                committed_floor,
                record.peering_metadata_transfer,
                observation.metadata_proof,
            )
        {
            return None;
        }
        Some(PgMetadataReadRoute::new(
            node_id,
            observation.metadata_proof,
        ))
    })
}

fn peering_metadata_proof_is_read_certified(
    committed_floor: PeeringMetadataProofFloor,
    transfer: Option<PgMetadataTransferProof>,
    actual: PgMetadataProof,
) -> bool {
    let expected = transfer
        .map(PgMetadataTransferProof::metadata_proof)
        .unwrap_or(committed_floor.proof);
    actual == expected
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgRouteSnapshot {
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    primary_node_id: NodeId,
    acting_set: Vec<NodeId>,
    state: PgState,
    active_metadata_proof: Option<PgMetadataProof>,
    metadata_read_route: Option<PgMetadataReadRoute>,
    primary_lease_deadline_ms: Option<u64>,
    peering_metadata_transfer: Option<PgMetadataTransferProof>,
    peering_metadata_transfer_destination_epoch: Option<ClusterEpoch>,
    peering_metadata_transfer_source_route_epoch: Option<ClusterEpoch>,
    peering_metadata_transfer_source_node_id: Option<NodeId>,
    pending_metadata_command_recovery: Option<PendingMetadataCommandRecovery>,
}

impl PgRouteSnapshot {
    pub fn reconstructed(
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        primary_node_id: NodeId,
        acting_set: Vec<NodeId>,
        state: PgState,
    ) -> Self {
        Self {
            cluster_epoch,
            pg_id,
            primary_node_id,
            acting_set,
            state,
            active_metadata_proof: None,
            metadata_read_route: None,
            primary_lease_deadline_ms: None,
            peering_metadata_transfer: None,
            peering_metadata_transfer_destination_epoch: None,
            peering_metadata_transfer_source_route_epoch: None,
            peering_metadata_transfer_source_node_id: None,
            pending_metadata_command_recovery: None,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_reconstructed_with_metadata_read_route(
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        primary_node_id: NodeId,
        acting_set: Vec<NodeId>,
        state: PgState,
        metadata_read_route: Option<PgMetadataReadRoute>,
    ) -> Self {
        let mut route =
            Self::reconstructed(cluster_epoch, pg_id, primary_node_id, acting_set, state);
        route.metadata_read_route = metadata_read_route;
        route
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn primary_node_id(&self) -> NodeId {
        self.primary_node_id
    }

    #[must_use]
    pub fn acting_set(&self) -> &[NodeId] {
        &self.acting_set
    }

    #[must_use]
    pub fn state(&self) -> PgState {
        self.state
    }

    #[must_use]
    pub(crate) fn active_metadata_proof(&self) -> Option<PgMetadataProof> {
        self.active_metadata_proof
    }

    #[must_use]
    pub fn metadata_read_route(&self) -> Option<PgMetadataReadRoute> {
        self.metadata_read_route
    }

    #[must_use]
    pub fn primary_lease_deadline_ms(&self) -> Option<u64> {
        self.primary_lease_deadline_ms
    }

    #[must_use]
    pub fn peering_metadata_transfer(&self) -> Option<PgMetadataTransferProof> {
        self.peering_metadata_transfer
    }

    #[must_use]
    pub fn peering_metadata_transfer_destination_epoch(&self) -> Option<ClusterEpoch> {
        self.peering_metadata_transfer_destination_epoch
    }

    #[must_use]
    pub fn peering_metadata_transfer_source_route_epoch(&self) -> Option<ClusterEpoch> {
        self.peering_metadata_transfer_source_route_epoch
    }

    #[must_use]
    pub fn peering_metadata_transfer_source_node_id(&self) -> Option<NodeId> {
        self.peering_metadata_transfer_source_node_id
    }

    #[must_use]
    pub fn pending_metadata_command_recovery(&self) -> Option<PendingMetadataCommandRecovery> {
        self.pending_metadata_command_recovery
    }

    #[must_use]
    pub fn without_serving_authority(&self) -> Self {
        let mut route = self.clone();
        route.primary_lease_deadline_ms = None;
        route.metadata_read_route = None;
        route
    }

    #[must_use]
    pub fn with_cluster_epoch(&self, cluster_epoch: ClusterEpoch) -> Self {
        let mut route = self.clone();
        route.cluster_epoch = cluster_epoch;
        route
    }

    #[must_use]
    pub(crate) fn matches_metadata_transfer_route(&self, other: &Self) -> bool {
        self.without_serving_authority()
            == other
                .without_serving_authority()
                .with_cluster_epoch(self.cluster_epoch)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgActingSetRetryRouteDisposition {
    RetryReady,
    Wait,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PgActingSetPreflightRoute {
    Absent,
    Present(Box<PgRouteSnapshot>),
}

fn pg_acting_set_preflight_deadline_error(pg_id: PgId) -> ControlPlaneError {
    ControlPlaneError::RpcUnconfirmed {
        message: format!(
            "PG {} acting-set preflight deadline expired before an observation completed",
            pg_id.get()
        ),
    }
}

fn pg_acting_set_preflight_route(
    runtime_map: ClusterRuntimeMapSnapshot,
    pg_id: PgId,
) -> Result<PgActingSetPreflightRoute, ControlPlaneError> {
    runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == pg_id)
        .cloned()
        .map(Box::new)
        .map(PgActingSetPreflightRoute::Present)
        .ok_or_else(|| {
            ControlPlaneError::rpc_protocol(format!(
                "PG-specific runtime map response omitted requested PG {}",
                pg_id.get()
            ))
        })
}

fn pg_acting_set_retry_route_disposition(
    before: &PgRouteSnapshot,
    current: &PgRouteSnapshot,
) -> PgActingSetRetryRouteDisposition {
    if before.pg_id != current.pg_id || before.acting_set != current.acting_set {
        return PgActingSetRetryRouteDisposition::Conflict;
    }
    if current.pending_metadata_command_recovery.is_some() {
        return PgActingSetRetryRouteDisposition::Wait;
    }
    match current.state {
        PgState::Active => PgActingSetRetryRouteDisposition::RetryReady,
        PgState::Peering
            if before.state == PgState::Peering
                && before.peering_metadata_transfer == current.peering_metadata_transfer
                && before.peering_metadata_transfer_destination_epoch
                    == current.peering_metadata_transfer_destination_epoch
                && before.peering_metadata_transfer_source_route_epoch
                    == current.peering_metadata_transfer_source_route_epoch
                && before.peering_metadata_transfer_source_node_id
                    == current.peering_metadata_transfer_source_node_id =>
        {
            PgActingSetRetryRouteDisposition::RetryReady
        }
        PgState::Peering | PgState::Degraded | PgState::Backfilling | PgState::Inconsistent => {
            PgActingSetRetryRouteDisposition::Wait
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingMetadataCommandRecovery {
    reporting_node_id: NodeId,
    pending: PendingMetadataCommandObservation,
}

impl PendingMetadataCommandRecovery {
    #[must_use]
    pub fn new(reporting_node_id: NodeId, pending: PendingMetadataCommandObservation) -> Self {
        Self {
            reporting_node_id,
            pending,
        }
    }

    #[must_use]
    pub fn reporting_node_id(self) -> NodeId {
        self.reporting_node_id
    }

    #[must_use]
    pub fn pending(self) -> PendingMetadataCommandObservation {
        self.pending
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRouteSnapshot {
    node_id: NodeId,
    node_incarnation: u64,
    endpoint: String,
    cluster_map_history_floor_epoch: Option<ClusterEpoch>,
}

impl NodeRouteSnapshot {
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn node_incarnation(&self) -> u64 {
        self.node_incarnation
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn cluster_map_history_floor_epoch(&self) -> Option<ClusterEpoch> {
        self.cluster_map_history_floor_epoch
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMapFreshnessProof {
    // Phase 11 single-authority freshness proof. The replicated control-plane
    // path must use a sibling proof variant rather than overloading this one.
    SingleAuthority {
        authority_incarnation: AuthorityIncarnation,
        // Informational until Phase 12 defines monotonic-clock lease-read
        // comparison, skew, and restart semantics.
        issued_at_ms: u64,
    },
    ReadIndex {
        authority_incarnation: AuthorityIncarnation,
        read_index: ControlPlaneLogId,
        // Informational until Phase 12 defines monotonic-clock lease-read
        // comparison, skew, and restart semantics.
        issued_at_ms: u64,
    },
    Reconstructed {
        authority_incarnation: AuthorityIncarnation,
    },
}

impl RuntimeMapFreshnessProof {
    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        match self {
            Self::SingleAuthority {
                authority_incarnation,
                ..
            }
            | Self::ReadIndex {
                authority_incarnation,
                ..
            }
            | Self::Reconstructed {
                authority_incarnation,
            } => *authority_incarnation,
        }
    }

    #[must_use]
    pub fn issued_at_ms(&self) -> Option<u64> {
        match self {
            Self::SingleAuthority { issued_at_ms, .. } | Self::ReadIndex { issued_at_ms, .. } => {
                Some(*issued_at_ms)
            }
            Self::Reconstructed { .. } => None,
        }
    }

    #[must_use]
    pub fn read_index(&self) -> Option<ControlPlaneLogId> {
        match self {
            Self::ReadIndex { read_index, .. } => Some(*read_index),
            Self::SingleAuthority { .. } | Self::Reconstructed { .. } => None,
        }
    }

    #[must_use]
    pub fn is_serving_authority_read(&self) -> bool {
        matches!(self, Self::SingleAuthority { .. } | Self::ReadIndex { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterRuntimeMapSnapshot {
    cluster_epoch: ClusterEpoch,
    validity: RouteMapValidity,
    freshness_proof: RuntimeMapFreshnessProof,
    nodes: Vec<NodeRouteSnapshot>,
    pg_routes: Vec<PgRouteSnapshot>,
    historical_pg_routes: Vec<PgRouteSnapshot>,
    historical_cluster_epochs: Vec<ClusterEpoch>,
}

const RUNTIME_MAP_CONTENT_DIGEST_LEN: usize = 32;
const RUNTIME_MAP_CONTENT_DIGEST_DOMAIN: &[u8] = b"argmin/runtime-map-content/v2";
const RUNTIME_MAP_CURRENT_STATE_DIGEST_DOMAIN: &[u8] = b"argmin/runtime-map-current-state/v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeMapContentDigest([u8; RUNTIME_MAP_CONTENT_DIGEST_LEN]);

impl RuntimeMapContentDigest {
    pub(crate) fn from_bytes(bytes: [u8; RUNTIME_MAP_CONTENT_DIGEST_LEN]) -> Self {
        Self(bytes)
    }

    fn as_bytes(self) -> [u8; RUNTIME_MAP_CONTENT_DIGEST_LEN] {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeMapContentCertificate {
    cluster_epoch: ClusterEpoch,
    pg_routes: usize,
    current_state_digest: RuntimeMapContentDigest,
    content_digest: RuntimeMapContentDigest,
}

impl RuntimeMapContentCertificate {
    pub(crate) fn from_snapshot_and_runtime_map(
        snapshot: &ClusterControlSnapshot,
        runtime_map: &ClusterRuntimeMapSnapshot,
    ) -> Self {
        Self {
            cluster_epoch: runtime_map.cluster_epoch(),
            pg_routes: runtime_map.pg_routes().len(),
            current_state_digest: runtime_map_current_state_digest(
                snapshot,
                runtime_map.pg_routes(),
            ),
            content_digest: runtime_map.content_digest(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SparsePgRouteReconstructionError {
    UnknownClusterEpoch,
    UnknownPg,
}

pub(crate) fn reconstruct_sparse_pg_route_at_epoch<'a>(
    current_epoch: ClusterEpoch,
    current_route: Option<PgRouteSnapshot>,
    historical_pg_routes: impl Iterator<Item = &'a PgRouteSnapshot> + Clone,
    historical_epoch_retained: bool,
    cluster_epoch: ClusterEpoch,
) -> Result<PgRouteSnapshot, SparsePgRouteReconstructionError> {
    if cluster_epoch == current_epoch {
        return current_route.ok_or(SparsePgRouteReconstructionError::UnknownPg);
    }
    if !historical_epoch_retained {
        return Err(SparsePgRouteReconstructionError::UnknownClusterEpoch);
    }
    if !historical_pg_routes
        .clone()
        .any(|route| route.cluster_epoch <= cluster_epoch)
    {
        return Err(SparsePgRouteReconstructionError::UnknownPg);
    }
    let mut route = historical_pg_routes
        .filter(|route| route.cluster_epoch >= cluster_epoch)
        .min_by_key(|route| route.cluster_epoch)
        .cloned()
        .or(current_route)
        .ok_or(SparsePgRouteReconstructionError::UnknownPg)?;
    route.cluster_epoch = cluster_epoch;
    Ok(route)
}

impl ClusterRuntimeMapSnapshot {
    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn valid_until_ms(&self) -> Option<u64> {
        self.validity.valid_until_ms()
    }

    #[must_use]
    pub fn validity(&self) -> RouteMapValidity {
        self.validity
    }

    #[must_use]
    pub fn content_digest(&self) -> RuntimeMapContentDigest {
        runtime_map_content_digest(self)
    }

    pub(crate) fn bind_process_local_lease_at(
        &self,
        local_wall_ms: u64,
        local_monotonic_ms: u64,
    ) -> Result<Option<BoundRouteMapLease>, LeaseClockError> {
        let Some(authority_valid_until_ms) = self.valid_until_ms() else {
            return Ok(None);
        };
        validate_process_lease_clock(
            local_wall_ms,
            crate::clock::clock_health_time_millis(),
            CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        )?;
        let Some(authority_issued_at_ms) = self.freshness_proof.issued_at_ms() else {
            return Ok(Some(BoundRouteMapLease::expired(
                authority_valid_until_ms,
                local_monotonic_ms,
            )));
        };
        BoundRouteMapLease::bind(
            authority_issued_at_ms,
            authority_valid_until_ms,
            local_wall_ms,
            local_monotonic_ms,
            CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        )
        .map(Some)
    }

    #[must_use]
    pub fn freshness_proof(&self) -> &RuntimeMapFreshnessProof {
        &self.freshness_proof
    }

    #[must_use]
    pub fn nodes(&self) -> &[NodeRouteSnapshot] {
        &self.nodes
    }

    #[must_use]
    pub fn pg_routes(&self) -> &[PgRouteSnapshot] {
        &self.pg_routes
    }

    #[must_use]
    pub fn historical_pg_routes(&self) -> &[PgRouteSnapshot] {
        &self.historical_pg_routes
    }

    #[must_use]
    pub fn historical_cluster_epochs(&self) -> &[ClusterEpoch] {
        &self.historical_cluster_epochs
    }

    pub fn reconstructed_pg_route_at_epoch(
        &self,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
    ) -> Result<PgRouteSnapshot, ControlPlaneError> {
        let current_route = self
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id)
            .map(PgRouteSnapshot::without_serving_authority);
        reconstruct_sparse_pg_route_at_epoch(
            self.cluster_epoch,
            current_route,
            self.historical_pg_routes
                .iter()
                .filter(|route| route.pg_id == pg_id),
            self.historical_cluster_epochs
                .binary_search(&cluster_epoch)
                .is_ok(),
            cluster_epoch,
        )
        .map_err(|error| match error {
            SparsePgRouteReconstructionError::UnknownClusterEpoch => {
                ControlPlaneError::UnknownClusterMapEpoch { cluster_epoch }
            }
            SparsePgRouteReconstructionError::UnknownPg => {
                ControlPlaneError::UnknownPg { pg_id: pg_id.get() }
            }
        })
    }

    pub fn runtime_map_at_epoch(
        &self,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Self, ControlPlaneError> {
        if cluster_epoch == self.cluster_epoch {
            return Ok(self.clone());
        }
        let pg_routes = self
            .pg_routes
            .iter()
            .map(|route| self.reconstructed_pg_route_at_epoch(route.pg_id(), cluster_epoch))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            cluster_epoch,
            validity: self.validity,
            freshness_proof: RuntimeMapFreshnessProof::Reconstructed {
                authority_incarnation: self.freshness_proof.authority_incarnation(),
            },
            nodes: self.nodes.clone(),
            pg_routes,
            historical_pg_routes: self.historical_pg_routes.clone(),
            historical_cluster_epochs: self.historical_cluster_epochs.clone(),
        })
    }

    /// Builds the one-PG route used to inspect a fenced metadata-transfer source.
    ///
    /// Historical maps are normally non-serving. This operation preserves the
    /// freshness proof from the current authority read only when the current
    /// Peering route still exactly matches the previously fenced route and,
    /// for a transfer in progress, names the exact historical source route.
    /// The resulting map cannot authorize an unrelated PG or source node.
    pub fn metadata_transfer_source_runtime_map(
        &self,
        expected_current_route: &PgRouteSnapshot,
        source_epoch: ClusterEpoch,
        source_node_id: NodeId,
    ) -> Result<Self, ControlPlaneError> {
        if !self.freshness_proof.is_serving_authority_read() {
            return Err(ControlPlaneError::rpc_protocol(
                "metadata-transfer source map requires a serving authority read".to_owned(),
            ));
        }
        let pg_id = expected_current_route.pg_id();
        let current_route = self
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if !expected_current_route.matches_metadata_transfer_route(current_route) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "metadata-transfer source PG {} changed after its fence",
                pg_id.get()
            )));
        }
        if current_route.state != PgState::Peering {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "metadata-transfer source PG {} is {:?}, not Peering",
                pg_id.get(),
                current_route.state
            )));
        }

        let source_route = if current_route.peering_metadata_transfer.is_none() {
            if current_route
                .peering_metadata_transfer_source_route_epoch
                .is_some()
                || current_route
                    .peering_metadata_transfer_source_node_id
                    .is_some()
                || expected_current_route.cluster_epoch() != source_epoch
            {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "metadata-transfer source PG {} has an incomplete or mismatched current-route authorization",
                    pg_id.get()
                )));
            }
            current_route.with_cluster_epoch(source_epoch)
        } else {
            if current_route.peering_metadata_transfer_source_route_epoch != Some(source_epoch)
                || current_route.peering_metadata_transfer_source_node_id != Some(source_node_id)
            {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "metadata-transfer source PG {} does not authorize node {} at epoch {}",
                    pg_id.get(),
                    source_node_id.as_u32(),
                    source_epoch.get()
                )));
            }
            self.reconstructed_pg_route_at_epoch(pg_id, source_epoch)?
        };
        if source_route.state != PgState::Peering || source_route.primary_node_id != source_node_id
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "metadata-transfer source PG {} route at epoch {} is not Peering on primary {}",
                pg_id.get(),
                source_epoch.get(),
                source_node_id.as_u32()
            )));
        }

        Ok(Self {
            cluster_epoch: source_epoch,
            validity: self.validity,
            freshness_proof: self.freshness_proof,
            nodes: self.nodes.clone(),
            pg_routes: vec![source_route],
            historical_pg_routes: self
                .historical_pg_routes
                .iter()
                .filter(|route| route.pg_id == pg_id)
                .cloned()
                .collect(),
            historical_cluster_epochs: self.historical_cluster_epochs.clone(),
        })
    }

    /// Builds the one-PG current route used to install a metadata transfer.
    ///
    /// The route must be observed through a serving authority read and must
    /// still carry the exact transfer authorization expected by the importer.
    pub fn metadata_transfer_destination_runtime_map(
        &self,
        pg_id: PgId,
        acting_set: &[NodeId],
        expected_transfer: PgMetadataTransferProof,
    ) -> Result<Self, ControlPlaneError> {
        if !self.freshness_proof.is_serving_authority_read() {
            return Err(ControlPlaneError::rpc_protocol(
                "metadata-transfer destination map requires a serving authority read".to_owned(),
            ));
        }
        let route = self
            .pg_routes
            .iter()
            .find(|route| route.pg_id == pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if route.cluster_epoch != self.cluster_epoch
            || route.state != PgState::Peering
            || route.acting_set != acting_set
            || route.peering_metadata_transfer != Some(expected_transfer)
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "metadata-transfer destination PG {} does not match its exact transfer authorization",
                pg_id.get()
            )));
        }
        let destination_epoch = route
            .peering_metadata_transfer_destination_epoch
            .ok_or_else(|| {
                ControlPlaneError::rpc_protocol(format!(
                    "metadata-transfer destination PG {} is missing its committed destination epoch",
                    pg_id.get()
                ))
            })?;
        Ok(Self {
            cluster_epoch: destination_epoch,
            validity: self.validity,
            freshness_proof: self.freshness_proof,
            nodes: self.nodes.clone(),
            pg_routes: vec![route.with_cluster_epoch(destination_epoch)],
            historical_pg_routes: self
                .historical_pg_routes
                .iter()
                .filter(|route| route.pg_id == pg_id)
                .cloned()
                .collect(),
            historical_cluster_epochs: self.historical_cluster_epochs.clone(),
        })
    }
}

fn runtime_map_content_digest(snapshot: &ClusterRuntimeMapSnapshot) -> RuntimeMapContentDigest {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    digest_bytes(&mut hasher, RUNTIME_MAP_CONTENT_DIGEST_DOMAIN);
    digest_u64(&mut hasher, snapshot.cluster_epoch().get());
    digest_len(&mut hasher, snapshot.nodes().len());
    for node in snapshot.nodes() {
        digest_u32(&mut hasher, node.node_id().as_u32());
        digest_u64(&mut hasher, node.node_incarnation());
        digest_bytes(&mut hasher, node.endpoint().as_bytes());
        digest_option_u64(
            &mut hasher,
            node.cluster_map_history_floor_epoch()
                .map(ClusterEpoch::get),
        );
    }
    digest_pg_routes(&mut hasher, snapshot.pg_routes());
    digest_pg_routes(&mut hasher, snapshot.historical_pg_routes());
    digest_len(&mut hasher, snapshot.historical_cluster_epochs().len());
    for epoch in snapshot.historical_cluster_epochs() {
        digest_u64(&mut hasher, epoch.get());
    }
    let checksum = hasher.finalize();
    RuntimeMapContentDigest::from_bytes(
        checksum
            .bytes()
            .try_into()
            .expect("SHA-256 runtime-map digest must contain 32 bytes"),
    )
}

fn runtime_map_current_state_digest(
    snapshot: &ClusterControlSnapshot,
    pg_routes: &[PgRouteSnapshot],
) -> RuntimeMapContentDigest {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    digest_bytes(&mut hasher, RUNTIME_MAP_CURRENT_STATE_DIGEST_DOMAIN);
    digest_u64(&mut hasher, snapshot.cluster_epoch().get());
    digest_len(&mut hasher, snapshot.nodes.len());
    for node in snapshot.nodes() {
        digest_u32(&mut hasher, node.node_id().as_u32());
        digest_u64(&mut hasher, node.node_incarnation());
        digest_bytes(&mut hasher, node.endpoint().as_bytes());
        digest_option_u64(
            &mut hasher,
            node.cluster_map_history_floor_epoch()
                .map(ClusterEpoch::get),
        );
    }
    digest_pg_routes(&mut hasher, pg_routes);
    let checksum = hasher.finalize();
    RuntimeMapContentDigest::from_bytes(
        checksum
            .bytes()
            .try_into()
            .expect("SHA-256 current runtime-map digest must contain 32 bytes"),
    )
}

pub(crate) fn digest_pg_routes(hasher: &mut ChecksumHasher, routes: &[PgRouteSnapshot]) {
    digest_len(hasher, routes.len());
    for route in routes {
        digest_u64(hasher, route.cluster_epoch().get());
        digest_u32(hasher, route.pg_id().get());
        digest_u32(hasher, route.primary_node_id().as_u32());
        digest_u8(
            hasher,
            match route.state() {
                PgState::Active => 1,
                PgState::Peering => 2,
                PgState::Degraded => 3,
                PgState::Backfilling => 4,
                PgState::Inconsistent => 5,
            },
        );
        // The lease deadline is renewed separately and does not change route content.
        match route.active_metadata_proof() {
            Some(proof) => {
                digest_u8(hasher, 1);
                digest_pg_metadata_proof(hasher, proof);
            }
            None => digest_u8(hasher, 0),
        }
        match route.metadata_read_route() {
            Some(read_route) => {
                digest_u8(hasher, 1);
                digest_u32(hasher, read_route.node_id().as_u32());
                digest_pg_metadata_proof(hasher, read_route.proof());
            }
            None => digest_u8(hasher, 0),
        }
        match route.peering_metadata_transfer() {
            Some(transfer) => {
                digest_u8(hasher, 1);
                digest_u64(hasher, transfer.source_epoch().get());
                digest_pg_metadata_proof(hasher, transfer.source_metadata_proof());
                digest_pg_metadata_proof(hasher, transfer.metadata_proof());
                digest_option_u64(
                    hasher,
                    route
                        .peering_metadata_transfer_destination_epoch()
                        .map(ClusterEpoch::get),
                );
                digest_option_u64(
                    hasher,
                    route
                        .peering_metadata_transfer_source_route_epoch()
                        .map(ClusterEpoch::get),
                );
                digest_option_u32(
                    hasher,
                    route
                        .peering_metadata_transfer_source_node_id()
                        .map(NodeId::as_u32),
                );
            }
            None => digest_u8(hasher, 0),
        }
        match route.pending_metadata_command_recovery() {
            Some(recovery) => {
                digest_u8(hasher, 1);
                digest_u32(hasher, recovery.reporting_node_id().as_u32());
                let pending = recovery.pending();
                digest_u64(hasher, pending.cluster_epoch().get());
                digest_u64(hasher, pending.log_index());
                digest_u64(hasher, pending.command_checksum());
            }
            None => digest_u8(hasher, 0),
        }
        digest_len(hasher, route.acting_set().len());
        for node_id in route.acting_set() {
            digest_u32(hasher, node_id.as_u32());
        }
    }
}

fn digest_pg_metadata_proof(hasher: &mut ChecksumHasher, proof: PgMetadataProof) {
    digest_u64(hasher, proof.applied_log_index);
    digest_u64(hasher, proof.applied_log_hash);
    digest_u64(hasher, proof.state_digest);
}

fn digest_len(hasher: &mut ChecksumHasher, len: usize) {
    digest_u64(
        hasher,
        u64::try_from(len).expect("runtime-map collection length must fit u64"),
    );
}

fn digest_bytes(hasher: &mut ChecksumHasher, bytes: &[u8]) {
    digest_len(hasher, bytes.len());
    hasher.update(bytes);
}

fn digest_option_u64(hasher: &mut ChecksumHasher, value: Option<u64>) {
    match value {
        Some(value) => {
            digest_u8(hasher, 1);
            digest_u64(hasher, value);
        }
        None => digest_u8(hasher, 0),
    }
}

fn digest_option_u32(hasher: &mut ChecksumHasher, value: Option<u32>) {
    match value {
        Some(value) => {
            digest_u8(hasher, 1);
            digest_u32(hasher, value);
        }
        None => digest_u8(hasher, 0),
    }
}

fn digest_u64(hasher: &mut ChecksumHasher, value: u64) {
    hasher.update(&value.to_be_bytes());
}

fn digest_u32(hasher: &mut ChecksumHasher, value: u32) {
    hasher.update(&value.to_be_bytes());
}

fn digest_u8(hasher: &mut ChecksumHasher, value: u8) {
    hasher.update(&[value]);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMapHistoryRecord {
    authority_incarnation: AuthorityIncarnation,
    cluster_epoch: ClusterEpoch,
    nodes: Vec<NodeId>,
    // Reverse delta: routes at this epoch that differ from the following state.
    pgs: Vec<HistoricalPgRouteRecord>,
    // PGs absent at this epoch and introduced by the following state.
    absent_pgs: Vec<PgId>,
}

impl ClusterMapHistoryRecord {
    #[cfg(test)]
    fn from_snapshot(snapshot: &ClusterControlSnapshot) -> Self {
        Self {
            authority_incarnation: snapshot.authority_incarnation,
            cluster_epoch: snapshot.cluster_epoch,
            nodes: snapshot.nodes.keys().copied().collect(),
            pgs: snapshot
                .pgs
                .values()
                .map(HistoricalPgRouteRecord::from)
                .collect(),
            absent_pgs: Vec::new(),
        }
    }

    fn delta_between(previous: &ClusterControlSnapshot, current: &ClusterControlSnapshot) -> Self {
        let pgs = previous
            .pgs
            .values()
            .filter(|previous_pg| {
                current
                    .pgs
                    .get(&previous_pg.pg_id)
                    .is_none_or(|current_pg| {
                        HistoricalPgRouteRecord::from(*previous_pg)
                            != HistoricalPgRouteRecord::from(current_pg)
                    })
            })
            .map(HistoricalPgRouteRecord::from)
            .collect();
        let absent_pgs = current
            .pgs
            .keys()
            .filter(|pg_id| !previous.pgs.contains_key(pg_id))
            .copied()
            .collect();
        Self {
            authority_incarnation: previous.authority_incarnation,
            cluster_epoch: previous.cluster_epoch,
            nodes: previous.nodes.keys().copied().collect(),
            pgs,
            absent_pgs,
        }
    }

    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    #[must_use]
    pub fn pgs(&self) -> &[HistoricalPgRouteRecord] {
        &self.pgs
    }

    #[must_use]
    pub fn pg(&self, pg_id: PgId) -> Option<&HistoricalPgRouteRecord> {
        self.pgs.iter().find(|record| record.pg_id == pg_id)
    }

    pub fn reconstructed_pg_route(
        &self,
        pg_id: PgId,
    ) -> Result<PgRouteSnapshot, ControlPlaneError> {
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        reconstruct_historical_pg_route(self.cluster_epoch, record, |node_id| {
            self.nodes.contains(&node_id)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalPgRouteRecord {
    pg_id: PgId,
    state: PgState,
    acting_set: Vec<NodeId>,
    active_primary: Option<NodeId>,
    peering_metadata_transfer: Option<PgMetadataTransferProof>,
    peering_metadata_transfer_source_route_epoch: Option<ClusterEpoch>,
    peering_metadata_transfer_source_node_id: Option<NodeId>,
}

impl From<&PgControlRecord> for HistoricalPgRouteRecord {
    fn from(record: &PgControlRecord) -> Self {
        Self {
            pg_id: record.pg_id,
            state: record.state,
            acting_set: record.acting_set.clone(),
            active_primary: record.active_primary,
            peering_metadata_transfer: record.peering_metadata_transfer,
            peering_metadata_transfer_source_route_epoch: record
                .peering_metadata_transfer_source_route_epoch,
            peering_metadata_transfer_source_node_id: record
                .peering_metadata_transfer_source_node_id,
        }
    }
}

impl HistoricalPgRouteRecord {
    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn state(&self) -> PgState {
        self.state
    }

    #[must_use]
    pub fn acting_set(&self) -> &[NodeId] {
        &self.acting_set
    }
}

fn reconstruct_historical_pg_route(
    cluster_epoch: ClusterEpoch,
    record: &HistoricalPgRouteRecord,
    mut contains_node: impl FnMut(NodeId) -> bool,
) -> Result<PgRouteSnapshot, ControlPlaneError> {
    let primary = match record.state {
        PgState::Active => record
            .active_primary
            .filter(|primary| record.acting_set.contains(primary))
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: record.pg_id.get(),
                cluster_epoch,
            })?,
        _ => record
            .acting_set
            .first()
            .copied()
            .ok_or(ControlPlaneError::EmptyActingSet {
                pg_id: record.pg_id.get(),
            })?,
    };
    for &node_id in &record.acting_set {
        if !contains_node(node_id) {
            return Err(ControlPlaneError::UnknownActingSetNode {
                pg_id: record.pg_id.get(),
                node_id: node_id.as_u32(),
            });
        }
    }
    Ok(PgRouteSnapshot {
        cluster_epoch,
        pg_id: record.pg_id,
        primary_node_id: primary,
        acting_set: record.acting_set.clone(),
        state: record.state,
        active_metadata_proof: None,
        metadata_read_route: None,
        primary_lease_deadline_ms: None,
        peering_metadata_transfer: record.peering_metadata_transfer,
        peering_metadata_transfer_destination_epoch: record
            .peering_metadata_transfer
            .map(|_| cluster_epoch),
        peering_metadata_transfer_source_route_epoch: record
            .peering_metadata_transfer_source_route_epoch,
        peering_metadata_transfer_source_node_id: record.peering_metadata_transfer_source_node_id,
        pending_metadata_command_recovery: None,
    })
}

fn validate_historical_pg_route_record(
    record: &HistoricalPgRouteRecord,
    cluster_epoch: ClusterEpoch,
    mut contains_node: impl FnMut(NodeId) -> bool,
) -> Result<(), String> {
    if record.acting_set.is_empty() {
        return Err(format!("PG {} has an empty acting set", record.pg_id.get()));
    }
    let mut acting_nodes = BTreeSet::new();
    for node_id in &record.acting_set {
        if !acting_nodes.insert(*node_id) {
            return Err(format!(
                "PG {} acting set repeats node {}",
                record.pg_id.get(),
                node_id.as_u32()
            ));
        }
        if !contains_node(*node_id) {
            return Err(format!(
                "PG {} acting set references unknown node {}",
                record.pg_id.get(),
                node_id.as_u32()
            ));
        }
    }
    match record.state {
        PgState::Active => {
            let Some(primary) = record.active_primary else {
                return Err(format!("active PG {} has no primary", record.pg_id.get()));
            };
            if !record.acting_set.contains(&primary) {
                return Err(format!(
                    "active PG {} primary {} is outside the acting set",
                    record.pg_id.get(),
                    primary.as_u32()
                ));
            }
        }
        _ if record.active_primary.is_some() => {
            return Err(format!(
                "non-active PG {} carries an active primary",
                record.pg_id.get()
            ));
        }
        _ => {}
    }
    match (
        record.peering_metadata_transfer,
        record.peering_metadata_transfer_source_route_epoch,
        record.peering_metadata_transfer_source_node_id,
    ) {
        (None, None, None) => {}
        (Some(transfer), Some(source_route_epoch), Some(source_node_id)) => {
            if record.state != PgState::Peering {
                return Err(format!(
                    "non-peering PG {} carries metadata transfer route state",
                    record.pg_id.get()
                ));
            }
            if transfer.source_epoch() > cluster_epoch {
                return Err(format!(
                    "PG {} metadata transfer source epoch is newer than route epoch",
                    record.pg_id.get()
                ));
            }
            if source_route_epoch >= cluster_epoch {
                return Err(format!(
                    "PG {} metadata transfer source route epoch is not older than route epoch",
                    record.pg_id.get()
                ));
            }
            if !contains_node(source_node_id) {
                return Err(format!(
                    "PG {} metadata transfer source references unknown node {}",
                    record.pg_id.get(),
                    source_node_id.as_u32()
                ));
            }
        }
        _ => {
            return Err(format!(
                "PG {} has incomplete metadata transfer route state",
                record.pg_id.get()
            ));
        }
    }
    Ok(())
}

fn validate_metadata_transfer_route_references(
    history: &[ClusterMapHistoryRecord],
    current_epoch: ClusterEpoch,
    current_pgs: impl Iterator<Item = (PgId, Option<ClusterEpoch>, Option<NodeId>)>,
) -> Result<(), String> {
    let validate_reference = |route_epoch: ClusterEpoch,
                              pg_id: PgId,
                              source_route_epoch: ClusterEpoch,
                              source_node_id: NodeId|
     -> Result<(), String> {
        if source_route_epoch >= route_epoch {
            return Err(format!(
                "PG {} metadata transfer source route epoch {} is not older than route epoch {}",
                pg_id.get(),
                source_route_epoch.get(),
                route_epoch.get()
            ));
        }
        let source_history = history
            .iter()
            .find(|record| record.cluster_epoch == source_route_epoch)
            .ok_or_else(|| {
                format!(
                    "PG {} references missing metadata transfer source route epoch {}",
                    pg_id.get(),
                    source_route_epoch.get()
                )
            })?;
        let source_record = history
            .iter()
            .filter(|record| record.cluster_epoch >= source_route_epoch)
            .find_map(|record| {
                if record.absent_pgs.contains(&pg_id) {
                    Some(None)
                } else {
                    record.pg(pg_id).map(Some)
                }
            })
            .flatten()
            .ok_or_else(|| {
                format!(
                    "PG {} references missing metadata transfer source PG at epoch {}",
                    pg_id.get(),
                    source_route_epoch.get()
                )
            })?;
        let source_route =
            reconstruct_historical_pg_route(source_route_epoch, source_record, |node_id| {
                source_history.nodes.contains(&node_id)
            })
            .map_err(|_| {
                format!(
                    "PG {} references missing metadata transfer source PG at epoch {}",
                    pg_id.get(),
                    source_route_epoch.get()
                )
            })?;
        if source_route.primary_node_id() != source_node_id {
            return Err(format!(
                    "PG {} metadata transfer source node {} does not match source route primary {} at epoch {}",
                    pg_id.get(),
                    source_node_id.as_u32(),
                    source_route.primary_node_id().as_u32(),
                    source_route_epoch.get()
                ));
        }
        Ok(())
    };

    for record in history {
        for pg in record.pgs() {
            if let (Some(source_route_epoch), Some(source_node_id)) = (
                pg.peering_metadata_transfer_source_route_epoch,
                pg.peering_metadata_transfer_source_node_id,
            ) {
                validate_reference(
                    record.cluster_epoch,
                    pg.pg_id,
                    source_route_epoch,
                    source_node_id,
                )?;
            }
        }
    }
    for (pg_id, source_route_epoch, source_node_id) in current_pgs {
        if let (Some(source_route_epoch), Some(source_node_id)) =
            (source_route_epoch, source_node_id)
        {
            validate_reference(current_epoch, pg_id, source_route_epoch, source_node_id)?;
        }
    }
    Ok(())
}

fn reconstruct_pg_route_from_record(
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    record: &PgControlRecord,
    mut contains_node: impl FnMut(NodeId) -> bool,
) -> Result<PgRouteSnapshot, ControlPlaneError> {
    let primary = match record.state {
        PgState::Active => record
            .active_primary
            .filter(|primary| record.acting_set.contains(primary))
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch,
            })?,
        _ => record
            .acting_set
            .first()
            .copied()
            .ok_or(ControlPlaneError::EmptyActingSet { pg_id: pg_id.get() })?,
    };
    for &node_id in &record.acting_set {
        if !contains_node(node_id) {
            return Err(ControlPlaneError::UnknownActingSetNode {
                pg_id: pg_id.get(),
                node_id: node_id.as_u32(),
            });
        }
    }
    Ok(PgRouteSnapshot {
        cluster_epoch,
        pg_id,
        primary_node_id: primary,
        acting_set: record.acting_set.clone(),
        state: record.state,
        active_metadata_proof: (record.state == PgState::Active)
            .then_some(record.active_metadata_proof)
            .flatten(),
        metadata_read_route: None,
        primary_lease_deadline_ms: None,
        peering_metadata_transfer: record.peering_metadata_transfer,
        peering_metadata_transfer_destination_epoch: peering_metadata_transfer_destination_epoch(
            record,
        )?,
        peering_metadata_transfer_source_route_epoch: record
            .peering_metadata_transfer_source_route_epoch,
        peering_metadata_transfer_source_node_id: record.peering_metadata_transfer_source_node_id,
        pending_metadata_command_recovery: None,
    })
}

fn peering_metadata_transfer_destination_epoch(
    record: &PgControlRecord,
) -> Result<Option<ClusterEpoch>, ControlPlaneError> {
    let Some(_) = record.peering_metadata_transfer else {
        return Ok(None);
    };
    let floor_epoch = record.peering_metadata_proof_floor_epoch.ok_or_else(|| {
        ControlPlaneError::rpc_protocol(format!(
            "PG {} metadata transfer is missing its destination proof-floor epoch",
            record.pg_id.get()
        ))
    })?;
    next_epoch(floor_epoch).map(Some)
}

fn add_required_historical_route_key(
    required_keys: &mut BTreeSet<(ClusterEpoch, PgId)>,
    pending_keys: &mut Vec<(ClusterEpoch, PgId)>,
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
) {
    if required_keys.insert((cluster_epoch, pg_id)) {
        pending_keys.push((cluster_epoch, pg_id));
    }
}

fn storage_node_refresh_needs_historical_route(
    route: &PgRouteSnapshot,
    refreshing_node_id: NodeId,
) -> bool {
    route.acting_set().contains(&refreshing_node_id)
        || route.peering_metadata_transfer_source_node_id() == Some(refreshing_node_id)
}

fn pg_route_configuration_eq(left: &PgRouteSnapshot, right: &PgRouteSnapshot) -> bool {
    left.pg_id() == right.pg_id()
        && left.primary_node_id() == right.primary_node_id()
        && left.acting_set() == right.acting_set()
        && left.state() == right.state()
        && left.peering_metadata_transfer() == right.peering_metadata_transfer()
        && left.peering_metadata_transfer_destination_epoch()
            == right.peering_metadata_transfer_destination_epoch()
        && left.peering_metadata_transfer_source_route_epoch()
            == right.peering_metadata_transfer_source_route_epoch()
        && left.peering_metadata_transfer_source_node_id()
            == right.peering_metadata_transfer_source_node_id()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgControlRecord {
    pg_id: PgId,
    state: PgState,
    acting_set: Vec<NodeId>,
    active_primary: Option<NodeId>,
    // Activation-time metadata floor. Active heartbeats may report later
    // metadata progress, but not an older or divergent proof at this index.
    active_metadata_proof: Option<PgMetadataProof>,
    // Cluster epoch in which active_metadata_proof was observed. Metadata
    // command logs are epoch-local, so active primary progress in a later epoch
    // is not ordered by the bare log tuple alone.
    active_metadata_proof_epoch: Option<ClusterEpoch>,
    // True only when the active metadata proof was imported through an explicit
    // metadata transfer marker. This scopes destination-epoch local proof
    // relaxation to transferred PGs instead of all Active primaries.
    active_metadata_transfer_imported: bool,
    // Identity and lease deadline of the primary from the most recent
    // transition out of Active. A different primary process must not activate
    // until this lease has passed. The same process may reactivate immediately
    // once the ordinary peering proof checks pass.
    previous_primary_lease: Option<PreviousPrimaryLease>,
    // Required metadata floor while a previously active PG is peering. This
    // prevents acting-set migration, restart, or failure recovery from
    // activating an agreed but stale empty/old metadata state.
    peering_metadata_proof_floor: Option<PgMetadataProof>,
    // Epoch/provenance for peering_metadata_proof_floor. Metadata command logs
    // are epoch-local, so a later peering observation may be valid progress
    // even when its bare log tuple is not ordered against an imported floor.
    peering_metadata_proof_floor_epoch: Option<ClusterEpoch>,
    peering_metadata_proof_floor_imported: bool,
    // Explicit metadata transfer proof imported into a peering destination.
    // This is distinct from overlap-based catch-up so later migration code and
    // operators can tell why a non-overlap acting set was allowed to peer.
    peering_metadata_transfer: Option<PgMetadataTransferProof>,
    // Quiesced source route captured when the transfer marker was installed.
    // Retried live transfers need this because the source replica state epoch
    // can be older than the fenced control-plane route epoch.
    peering_metadata_transfer_source_route_epoch: Option<ClusterEpoch>,
    peering_metadata_transfer_source_node_id: Option<NodeId>,
    // Operator-initiated transfer fence. While set, normal peering completion
    // is blocked so the source remains quiesced for checkpoint/log export.
    metadata_transfer_fenced: bool,
    // Primary lease deadline captured by the authority when an Active PG is
    // fenced for metadata transfer. Retried live transfers must wait for this
    // original stale-route window before exporting retained metadata.
    metadata_transfer_fence_source_lease_deadline_ms: Option<u64>,
    // Provenance of the active proof captured when the PG was fenced. Imported
    // transfer sources need destination-epoch proof relaxation on retry, while
    // ordinary active sources must keep strict proof ordering.
    metadata_transfer_fence_source_imported: bool,
    // Cluster epoch committed by the fence command. Relaxed source-proof
    // progress must precede this immutable boundary even if unrelated commands
    // later advance the cluster epoch.
    metadata_transfer_fence_epoch: Option<ClusterEpoch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreviousPrimaryLease {
    node_id: NodeId,
    node_incarnation: u64,
    endpoint: String,
    lease_deadline_ms: u64,
    prefer_reactivation: bool,
}

impl PreviousPrimaryLease {
    fn without_reactivation_preference(mut self) -> Self {
        self.prefer_reactivation = false;
        self
    }

    fn matches_process(&self, node_id: NodeId, node_incarnation: u64, endpoint: &str) -> bool {
        self.node_id == node_id
            && self.node_incarnation == node_incarnation
            && self.endpoint == endpoint
    }

    fn blocks_activation(
        &self,
        primary: NodeId,
        primary_incarnation: u64,
        primary_endpoint: &str,
        now_ms: u64,
    ) -> bool {
        !self.matches_process(primary, primary_incarnation, primary_endpoint)
            && !successor_activation_fence_satisfied(
                now_ms,
                self.lease_deadline_ms,
                CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PeeringMetadataProofFloor {
    proof: PgMetadataProof,
    epoch: Option<ClusterEpoch>,
    imported: bool,
}

impl PgControlRecord {
    fn new(pg_id: PgId, acting_set: Vec<NodeId>) -> Self {
        Self {
            pg_id,
            state: PgState::Peering,
            acting_set,
            active_primary: None,
            active_metadata_proof: None,
            active_metadata_proof_epoch: None,
            active_metadata_transfer_imported: false,
            previous_primary_lease: None,
            peering_metadata_proof_floor: None,
            peering_metadata_proof_floor_epoch: None,
            peering_metadata_proof_floor_imported: false,
            peering_metadata_transfer: None,
            peering_metadata_transfer_source_route_epoch: None,
            peering_metadata_transfer_source_node_id: None,
            metadata_transfer_fenced: false,
            metadata_transfer_fence_source_lease_deadline_ms: None,
            metadata_transfer_fence_source_imported: false,
            metadata_transfer_fence_epoch: None,
        }
    }

    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn state(&self) -> PgState {
        self.state
    }

    #[must_use]
    pub fn acting_set(&self) -> &[NodeId] {
        &self.acting_set
    }

    #[must_use]
    pub fn active_primary(&self) -> Option<NodeId> {
        self.active_primary
    }

    #[must_use]
    pub fn active_metadata_proof(&self) -> Option<PgMetadataProof> {
        self.active_metadata_proof
    }

    #[must_use]
    pub fn active_metadata_proof_epoch(&self) -> Option<ClusterEpoch> {
        self.active_metadata_proof_epoch
    }

    #[must_use]
    pub fn active_metadata_transfer_imported(&self) -> bool {
        self.active_metadata_transfer_imported
    }

    #[must_use]
    pub fn previous_primary_lease_deadline_ms(&self) -> Option<u64> {
        self.previous_primary_lease
            .as_ref()
            .map(|previous| previous.lease_deadline_ms)
    }

    #[must_use]
    pub fn previous_primary_node_id(&self) -> Option<NodeId> {
        self.previous_primary_lease
            .as_ref()
            .map(|previous| previous.node_id)
    }

    #[must_use]
    pub fn previous_primary_node_incarnation(&self) -> Option<u64> {
        self.previous_primary_lease
            .as_ref()
            .map(|previous| previous.node_incarnation)
    }

    #[must_use]
    pub fn peering_metadata_proof_floor(&self) -> Option<PgMetadataProof> {
        self.peering_metadata_proof_floor
    }

    #[must_use]
    pub fn peering_metadata_proof_floor_epoch(&self) -> Option<ClusterEpoch> {
        self.peering_metadata_proof_floor_epoch
    }

    #[must_use]
    pub fn peering_metadata_proof_floor_imported(&self) -> bool {
        self.peering_metadata_proof_floor_imported
    }

    fn peering_metadata_proof_floor_context(&self) -> Option<PeeringMetadataProofFloor> {
        self.peering_metadata_proof_floor
            .map(|proof| PeeringMetadataProofFloor {
                proof,
                epoch: self.peering_metadata_proof_floor_epoch,
                imported: self.peering_metadata_proof_floor_imported,
            })
    }

    #[must_use]
    pub fn peering_metadata_transfer(&self) -> Option<PgMetadataTransferProof> {
        self.peering_metadata_transfer
    }

    #[must_use]
    pub fn peering_metadata_transfer_source_route_epoch(&self) -> Option<ClusterEpoch> {
        self.peering_metadata_transfer_source_route_epoch
    }

    #[must_use]
    pub fn peering_metadata_transfer_source_node_id(&self) -> Option<NodeId> {
        self.peering_metadata_transfer_source_node_id
    }

    #[must_use]
    pub fn metadata_transfer_fenced(&self) -> bool {
        self.metadata_transfer_fenced
    }

    #[must_use]
    pub fn metadata_transfer_fence_source_lease_deadline_ms(&self) -> Option<u64> {
        self.metadata_transfer_fence_source_lease_deadline_ms
    }

    #[must_use]
    pub fn metadata_transfer_fence_epoch(&self) -> Option<ClusterEpoch> {
        self.metadata_transfer_fence_epoch
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgMetadataTransferProof {
    source_epoch: ClusterEpoch,
    source_metadata_proof: PgMetadataProof,
    imported_metadata_proof: PgMetadataProof,
}

impl PgMetadataTransferProof {
    #[must_use]
    pub fn new(source_epoch: ClusterEpoch, metadata_proof: PgMetadataProof) -> Self {
        Self::new_with_imported_metadata_proof(source_epoch, metadata_proof, metadata_proof)
    }

    #[must_use]
    pub fn new_with_imported_metadata_proof(
        source_epoch: ClusterEpoch,
        source_metadata_proof: PgMetadataProof,
        imported_metadata_proof: PgMetadataProof,
    ) -> Self {
        Self {
            source_epoch,
            source_metadata_proof,
            imported_metadata_proof,
        }
    }

    #[must_use]
    pub fn source_epoch(self) -> ClusterEpoch {
        self.source_epoch
    }

    #[must_use]
    pub fn source_metadata_proof(self) -> PgMetadataProof {
        self.source_metadata_proof
    }

    #[must_use]
    pub fn metadata_proof(self) -> PgMetadataProof {
        self.imported_metadata_proof
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodePgObservationRecord {
    pg_id: PgId,
    state: PgState,
    observed_epoch: ClusterEpoch,
    observed_at_ms: u64,
    metadata_proof: PgMetadataProof,
    pending_metadata_command: Option<PendingMetadataCommandObservation>,
}

impl NodePgObservationRecord {
    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn state(&self) -> PgState {
        self.state
    }

    #[must_use]
    pub fn observed_epoch(&self) -> ClusterEpoch {
        self.observed_epoch
    }

    #[must_use]
    pub fn observed_at_ms(&self) -> u64 {
        self.observed_at_ms
    }

    #[must_use]
    pub fn metadata_proof(&self) -> PgMetadataProof {
        self.metadata_proof
    }

    #[must_use]
    pub fn has_pending_metadata_command(&self) -> bool {
        self.pending_metadata_command.is_some()
    }

    #[must_use]
    pub fn pending_metadata_command(&self) -> Option<PendingMetadataCommandObservation> {
        self.pending_metadata_command
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingMetadataCommandObservation {
    cluster_epoch: ClusterEpoch,
    log_index: NonZeroU64,
    command_checksum: u64,
}

impl PendingMetadataCommandObservation {
    #[must_use]
    pub const fn new(
        cluster_epoch: ClusterEpoch,
        log_index: NonZeroU64,
        command_checksum: u64,
    ) -> Self {
        Self {
            cluster_epoch,
            log_index,
            command_checksum,
        }
    }

    #[must_use]
    pub const fn cluster_epoch(self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub const fn log_index(self) -> u64 {
        self.log_index.get()
    }

    #[must_use]
    pub const fn command_checksum(self) -> u64 {
        self.command_checksum
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodePgHeartbeatObservation {
    pub pg_id: PgId,
    pub state: PgState,
    pub metadata_proof: PgMetadataProof,
    pub pending_metadata_command: Option<PendingMetadataCommandObservation>,
}

impl NodePgHeartbeatObservation {
    #[must_use]
    pub fn has_pending_metadata_command(&self) -> bool {
        self.pending_metadata_command.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgMetadataProof {
    pub applied_log_index: u64,
    pub applied_log_hash: u64,
    pub state_digest: u64,
}

/// Read-only metadata authority for one node whose durable replica state is
/// exactly equal to a control-plane-certified committed proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgMetadataReadRoute {
    node_id: NodeId,
    proof: PgMetadataProof,
}

impl PgMetadataReadRoute {
    #[must_use]
    pub const fn new(node_id: NodeId, proof: PgMetadataProof) -> Self {
        Self { node_id, proof }
    }

    #[must_use]
    pub const fn node_id(self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub const fn proof(self) -> PgMetadataProof {
        self.proof
    }
}

impl PgMetadataProof {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            applied_log_index: 0,
            applied_log_hash: 0,
            state_digest: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHeartbeat {
    pub node_id: NodeId,
    pub node_incarnation: u64,
    pub endpoint: String,
    pub observed_epoch: ClusterEpoch,
    pub requested_lease_duration_ms: u64,
    pub cluster_map_history_route_references: PgClusterMapHistoryRouteReferences,
    pub pg_observations: Vec<NodePgHeartbeatObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatLease {
    authority_incarnation: AuthorityIncarnation,
    cluster_epoch: ClusterEpoch,
    node_id: NodeId,
    lease_deadline_ms: u64,
    serving: bool,
    snapshot: ClusterControlSnapshot,
}

impl HeartbeatLease {
    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn lease_deadline_ms(&self) -> u64 {
        self.lease_deadline_ms
    }

    #[must_use]
    pub fn serving(&self) -> bool {
        self.serving
    }

    #[must_use]
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatLeaseExpiry {
    cluster_epoch: ClusterEpoch,
    expired_nodes: Vec<NodeId>,
    peering_pgs: Vec<PgId>,
    snapshot: ClusterControlSnapshot,
}

impl HeartbeatLeaseExpiry {
    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn expired_nodes(&self) -> &[NodeId] {
        &self.expired_nodes
    }

    #[must_use]
    pub fn peering_pgs(&self) -> &[PgId] {
        &self.peering_pgs
    }

    #[must_use]
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
    }
}

pub trait ControlPlaneHeartbeatSink {
    fn submit_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneHeartbeatRefresh {
    lease: HeartbeatLease,
    runtime_map: ClusterRuntimeMapSnapshot,
    history_reference_validation_epoch: ClusterEpoch,
}

impl ControlPlaneHeartbeatRefresh {
    #[must_use]
    pub fn new(
        lease: HeartbeatLease,
        runtime_map: ClusterRuntimeMapSnapshot,
        history_reference_validation_epoch: ClusterEpoch,
    ) -> Self {
        Self {
            lease,
            runtime_map,
            history_reference_validation_epoch,
        }
    }

    #[must_use]
    pub fn lease(&self) -> &HeartbeatLease {
        &self.lease
    }

    #[must_use]
    pub fn runtime_map(&self) -> &ClusterRuntimeMapSnapshot {
        &self.runtime_map
    }

    #[must_use]
    pub fn into_parts(self) -> (HeartbeatLease, ClusterRuntimeMapSnapshot) {
        (self.lease, self.runtime_map)
    }
}

pub trait ControlPlaneHeartbeatRuntimeMapSource {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError>;

    fn refresh_node_heartbeat_with_lease_horizon_authority(
        &mut self,
        _heartbeat: NodeHeartbeat,
        _authority_now_ms: u64,
        _lease_horizon_authority: LeaseHorizonAuthorityBinding,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        Err(ControlPlaneError::rpc_protocol(
            "control-plane heartbeat authority does not support lease horizons".to_owned(),
        ))
    }
}

pub trait ControlPlaneRuntimeMapSource {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError>;

    fn runtime_map_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        Ok(ControlPlaneRuntimeMapStatus::from_runtime_map(
            &self.runtime_map_snapshot(authority_now_ms)?,
        ))
    }

    fn runtime_map_diagnostics_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapDiagnosticSnapshot, ControlPlaneError> {
        let runtime_map = self.runtime_map_snapshot(authority_now_ms)?;
        let node_leases = runtime_map
            .nodes()
            .iter()
            .map(|node| ControlPlaneRuntimeMapNodeLeaseDiagnostic {
                node_id: node.node_id(),
                lease_deadline_ms: None,
            })
            .collect();
        ControlPlaneRuntimeMapDiagnosticSnapshot::new(runtime_map, node_leases)
    }

    fn pending_metadata_command_recoveries(
        &self,
        authority_now_ms: u64,
    ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
        Ok(PendingMetadataCommandRecoveryListing::new(
            self.runtime_map_snapshot(authority_now_ms)?
                .pg_routes()
                .iter()
                .filter_map(|route| {
                    route.pending_metadata_command_recovery().map(|recovery| {
                        PendingMetadataCommandRecoveryTask::new(route.pg_id(), recovery)
                    })
                })
                .collect(),
            Vec::new(),
        ))
    }

    fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let runtime_map = self.runtime_map_snapshot(authority_now_ms)?;
        if runtime_map
            .pg_routes()
            .iter()
            .any(|route| route.pg_id() == pg_id)
        {
            return Ok(runtime_map);
        }
        Err(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingMetadataCommandRecoveryTask {
    pg_id: PgId,
    recovery: PendingMetadataCommandRecovery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingMetadataCommandRecoveryDiscoveryFailureKind {
    HistoricalRouteInvalid,
    ReporterNotHistoricalPrimary,
    ConflictingIdentity,
}

impl PendingMetadataCommandRecoveryDiscoveryFailureKind {
    fn from_error(error: &ControlPlaneError) -> Self {
        match error {
            ControlPlaneError::PgPeeringPendingMetadataCommandReporterNotHistoricalPrimary {
                ..
            } => Self::ReporterNotHistoricalPrimary,
            ControlPlaneError::PgPeeringPendingMetadataCommandMismatch { .. } => {
                Self::ConflictingIdentity
            }
            _ => Self::HistoricalRouteInvalid,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::HistoricalRouteInvalid => 1,
            Self::ReporterNotHistoricalPrimary => 2,
            Self::ConflictingIdentity => 3,
        }
    }

    fn from_u8(value: u8) -> Result<Self, ControlPlaneError> {
        match value {
            1 => Ok(Self::HistoricalRouteInvalid),
            2 => Ok(Self::ReporterNotHistoricalPrimary),
            3 => Ok(Self::ConflictingIdentity),
            _ => Err(ControlPlaneError::rpc_protocol(format!(
                "invalid pending metadata command recovery discovery failure kind {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMetadataCommandRecoveryDiscoveryFailure {
    pg_id: PgId,
    kind: PendingMetadataCommandRecoveryDiscoveryFailureKind,
    detail: String,
}

impl PendingMetadataCommandRecoveryDiscoveryFailure {
    #[must_use]
    pub fn new(
        pg_id: PgId,
        kind: PendingMetadataCommandRecoveryDiscoveryFailureKind,
        detail: String,
    ) -> Self {
        Self {
            pg_id,
            kind,
            detail,
        }
    }

    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn kind(&self) -> PendingMetadataCommandRecoveryDiscoveryFailureKind {
        self.kind
    }

    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMetadataCommandRecoveryListing {
    tasks: Vec<PendingMetadataCommandRecoveryTask>,
    failures: Vec<PendingMetadataCommandRecoveryDiscoveryFailure>,
}

impl PendingMetadataCommandRecoveryListing {
    #[must_use]
    pub fn new(
        tasks: Vec<PendingMetadataCommandRecoveryTask>,
        failures: Vec<PendingMetadataCommandRecoveryDiscoveryFailure>,
    ) -> Self {
        Self { tasks, failures }
    }

    #[must_use]
    pub fn tasks(&self) -> &[PendingMetadataCommandRecoveryTask] {
        &self.tasks
    }

    #[must_use]
    pub fn failures(&self) -> &[PendingMetadataCommandRecoveryDiscoveryFailure] {
        &self.failures
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Vec<PendingMetadataCommandRecoveryTask>,
        Vec<PendingMetadataCommandRecoveryDiscoveryFailure>,
    ) {
        (self.tasks, self.failures)
    }
}

impl PendingMetadataCommandRecoveryTask {
    #[must_use]
    pub fn new(pg_id: PgId, recovery: PendingMetadataCommandRecovery) -> Self {
        Self { pg_id, recovery }
    }

    #[must_use]
    pub fn pg_id(self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn recovery(self) -> PendingMetadataCommandRecovery {
        self.recovery
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneRuntimeMapLeaseRenewal {
    content_digest: RuntimeMapContentDigest,
    validity: RouteMapValidity,
    freshness_proof: RuntimeMapFreshnessProof,
}

impl ControlPlaneRuntimeMapLeaseRenewal {
    fn from_runtime_map(runtime_map: &ClusterRuntimeMapSnapshot) -> Self {
        Self {
            content_digest: runtime_map.content_digest(),
            validity: runtime_map.validity(),
            freshness_proof: *runtime_map.freshness_proof(),
        }
    }

    fn from_content_certificate(
        certificate: RuntimeMapContentCertificate,
        validity: RouteMapValidity,
        freshness_proof: RuntimeMapFreshnessProof,
    ) -> Self {
        Self {
            content_digest: certificate.content_digest,
            validity,
            freshness_proof,
        }
    }

    #[must_use]
    pub fn content_digest(self) -> RuntimeMapContentDigest {
        self.content_digest
    }

    #[must_use]
    pub fn validity(self) -> RouteMapValidity {
        self.validity
    }

    #[must_use]
    pub fn freshness_proof(self) -> RuntimeMapFreshnessProof {
        self.freshness_proof
    }

    pub(crate) fn bind_process_local_lease_at(
        self,
        local_wall_ms: u64,
        local_monotonic_ms: u64,
    ) -> Result<Option<BoundRouteMapLease>, LeaseClockError> {
        let Some(authority_valid_until_ms) = self.validity.valid_until_ms() else {
            return Ok(None);
        };
        validate_process_lease_clock(
            local_wall_ms,
            crate::clock::clock_health_time_millis(),
            CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        )?;
        let Some(authority_issued_at_ms) = self.freshness_proof.issued_at_ms() else {
            return Ok(Some(BoundRouteMapLease::expired(
                authority_valid_until_ms,
                local_monotonic_ms,
            )));
        };
        BoundRouteMapLease::bind(
            authority_issued_at_ms,
            authority_valid_until_ms,
            local_wall_ms,
            local_monotonic_ms,
            CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
        )
        .map(Some)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneRuntimeMapStatus {
    cluster_epoch: ClusterEpoch,
    pg_routes: usize,
    active_serving_pg_routes: usize,
    lease_renewal: Option<ControlPlaneRuntimeMapLeaseRenewal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneRuntimeMapDiagnostics {
    runtime_map: ClusterRuntimeMapSnapshot,
    rpc_metrics: Vec<observability::ControlPlaneRpcMetricSample>,
    snapshot_metrics: observability::ControlPlaneSnapshotMetricSnapshot,
    journal_metrics: observability::ControlPlaneJournalMetricSnapshot,
    raft_checkpoint_metrics: observability::ControlPlaneRaftCheckpointMetricSnapshot,
    raft_wal_metrics: observability::ControlPlaneRaftWalMetricSnapshot,
    raft_command_metrics: observability::ControlPlaneRaftCommandMetricSnapshot,
    history_reference_samples: Vec<observability::ControlPlaneHistoryReferenceSample>,
    node_leases: Vec<ControlPlaneRuntimeMapNodeLeaseDiagnostic>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneRuntimeMapNodeLeaseDiagnostic {
    node_id: NodeId,
    lease_deadline_ms: Option<u64>,
}

impl ControlPlaneRuntimeMapNodeLeaseDiagnostic {
    #[must_use]
    pub(crate) fn new(node_id: NodeId, lease_deadline_ms: Option<u64>) -> Self {
        Self {
            node_id,
            lease_deadline_ms,
        }
    }

    #[must_use]
    pub fn node_id(self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn lease_deadline_ms(self) -> Option<u64> {
        self.lease_deadline_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneRuntimeMapDiagnosticSnapshot {
    runtime_map: ClusterRuntimeMapSnapshot,
    node_leases: Vec<ControlPlaneRuntimeMapNodeLeaseDiagnostic>,
}

impl ControlPlaneRuntimeMapDiagnosticSnapshot {
    pub(crate) fn new(
        runtime_map: ClusterRuntimeMapSnapshot,
        node_leases: Vec<ControlPlaneRuntimeMapNodeLeaseDiagnostic>,
    ) -> Result<Self, ControlPlaneError> {
        if node_leases.len() != runtime_map.nodes().len()
            || !node_leases
                .iter()
                .zip(runtime_map.nodes())
                .all(|(lease, node)| lease.node_id == node.node_id())
        {
            return Err(ControlPlaneError::rpc_protocol(
                "control-plane diagnostic node leases do not match runtime-map nodes".to_owned(),
            ));
        }
        Ok(Self {
            runtime_map,
            node_leases,
        })
    }

    #[must_use]
    pub fn runtime_map(&self) -> &ClusterRuntimeMapSnapshot {
        &self.runtime_map
    }

    #[must_use]
    pub fn node_leases(&self) -> &[ControlPlaneRuntimeMapNodeLeaseDiagnostic] {
        &self.node_leases
    }
}

impl ControlPlaneRuntimeMapDiagnostics {
    #[must_use]
    pub fn runtime_map(&self) -> &ClusterRuntimeMapSnapshot {
        &self.runtime_map
    }

    #[must_use]
    pub fn rpc_metrics(&self) -> &[observability::ControlPlaneRpcMetricSample] {
        &self.rpc_metrics
    }

    #[must_use]
    pub fn snapshot_metrics(&self) -> observability::ControlPlaneSnapshotMetricSnapshot {
        self.snapshot_metrics
    }

    #[must_use]
    pub fn journal_metrics(&self) -> observability::ControlPlaneJournalMetricSnapshot {
        self.journal_metrics
    }

    #[must_use]
    pub fn raft_checkpoint_metrics(
        &self,
    ) -> observability::ControlPlaneRaftCheckpointMetricSnapshot {
        self.raft_checkpoint_metrics
    }

    #[must_use]
    pub fn raft_wal_metrics(&self) -> observability::ControlPlaneRaftWalMetricSnapshot {
        self.raft_wal_metrics
    }

    #[must_use]
    pub fn raft_command_metrics(&self) -> observability::ControlPlaneRaftCommandMetricSnapshot {
        self.raft_command_metrics
    }

    #[must_use]
    pub fn history_reference_samples(
        &self,
    ) -> &[observability::ControlPlaneHistoryReferenceSample] {
        &self.history_reference_samples
    }

    #[must_use]
    pub fn node_leases(&self) -> &[ControlPlaneRuntimeMapNodeLeaseDiagnostic] {
        &self.node_leases
    }
}

impl ControlPlaneRuntimeMapStatus {
    #[must_use]
    pub fn new(
        cluster_epoch: ClusterEpoch,
        pg_routes: usize,
        active_serving_pg_routes: usize,
    ) -> Self {
        Self {
            cluster_epoch,
            pg_routes,
            active_serving_pg_routes,
            lease_renewal: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_with_lease_renewal(
        cluster_epoch: ClusterEpoch,
        content_digest: RuntimeMapContentDigest,
        validity: RouteMapValidity,
        freshness_proof: RuntimeMapFreshnessProof,
    ) -> Self {
        Self {
            cluster_epoch,
            pg_routes: 0,
            active_serving_pg_routes: 0,
            lease_renewal: Some(ControlPlaneRuntimeMapLeaseRenewal {
                content_digest,
                validity,
                freshness_proof,
            }),
        }
    }

    pub(crate) fn from_runtime_map(runtime_map: &ClusterRuntimeMapSnapshot) -> Self {
        Self {
            cluster_epoch: runtime_map.cluster_epoch(),
            pg_routes: runtime_map.pg_routes().len(),
            active_serving_pg_routes: runtime_map
                .pg_routes()
                .iter()
                .filter(|route| {
                    route.state() == PgState::Active && route.primary_lease_deadline_ms().is_some()
                })
                .count(),
            lease_renewal: (runtime_map.valid_until_ms().is_some()
                && runtime_map.freshness_proof().is_serving_authority_read())
            .then(|| ControlPlaneRuntimeMapLeaseRenewal::from_runtime_map(runtime_map)),
        }
    }

    pub(crate) fn from_snapshot_with_content_certificate(
        snapshot: &ClusterControlSnapshot,
        authority_now_ms: u64,
        freshness_proof: RuntimeMapFreshnessProof,
        certificate: RuntimeMapContentCertificate,
    ) -> Result<Option<Self>, ControlPlaneError> {
        let pg_routes = snapshot.pg_routes(authority_now_ms)?;
        if certificate.cluster_epoch != snapshot.cluster_epoch()
            || certificate.pg_routes != pg_routes.len()
            || certificate.current_state_digest
                != runtime_map_current_state_digest(snapshot, &pg_routes)
        {
            return Ok(None);
        }
        let validity = pg_routes
            .iter()
            .filter_map(PgRouteSnapshot::primary_lease_deadline_ms)
            .min()
            .map_or(
                non_serving_runtime_map_validity(authority_now_ms),
                RouteMapValidity::until_ms_saturating,
            );
        let active_serving_pg_routes = pg_routes
            .iter()
            .filter(|route| {
                route.state() == PgState::Active && route.primary_lease_deadline_ms().is_some()
            })
            .count();
        let lease_renewal = (validity.valid_until_ms().is_some()
            && freshness_proof.is_serving_authority_read())
        .then(|| {
            ControlPlaneRuntimeMapLeaseRenewal::from_content_certificate(
                certificate,
                validity,
                freshness_proof,
            )
        });
        Ok(Some(Self {
            cluster_epoch: certificate.cluster_epoch,
            pg_routes: certificate.pg_routes,
            active_serving_pg_routes,
            lease_renewal,
        }))
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn pg_routes(&self) -> usize {
        self.pg_routes
    }

    #[must_use]
    pub fn active_serving_pg_routes(&self) -> usize {
        self.active_serving_pg_routes
    }

    #[must_use]
    pub fn lease_renewal(&self) -> Option<ControlPlaneRuntimeMapLeaseRenewal> {
        self.lease_renewal
    }
}

pub trait ControlPlaneLinearizedCommandSink {
    fn submit_control_plane_command(
        &mut self,
        command: ControlPlaneCommand,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError>;
}

pub trait ControlPlaneLinearizedRuntimeMapSource {
    fn linearized_runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError>;
}

#[derive(Debug, Clone)]
pub struct FencedPgMetadataTransferSnapshot {
    snapshot: ClusterControlSnapshot,
    source_primary_lease_deadline_ms: Option<u64>,
}

impl FencedPgMetadataTransferSnapshot {
    #[must_use]
    pub fn new(
        snapshot: ClusterControlSnapshot,
        source_primary_lease_deadline_ms: Option<u64>,
    ) -> Self {
        Self {
            snapshot,
            source_primary_lease_deadline_ms,
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
    }

    #[must_use]
    pub fn source_primary_lease_deadline_ms(&self) -> Option<u64> {
        self.source_primary_lease_deadline_ms
    }

    #[must_use]
    pub fn into_parts(self) -> (ClusterControlSnapshot, Option<u64>) {
        (self.snapshot, self.source_primary_lease_deadline_ms)
    }
}

pub trait ControlPlaneAdmin {
    fn authority_clock_context(
        &self,
    ) -> Result<ControlPlaneAuthorityClockContext, ControlPlaneError> {
        Err(ControlPlaneError::rpc_remote(
            "control-plane authority clock administration is not supported by this authority"
                .to_owned(),
        ))
    }

    fn set_pg_acting_set(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError>;

    fn fence_pg_for_metadata_transfer(
        &mut self,
        pg_id: PgId,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError>;

    fn fence_pg_for_metadata_transfer_with_source_lease(
        &mut self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError>;

    fn set_pg_acting_set_with_metadata_transfer(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError>;

    fn transfer_raft_leadership_to(&mut self, node_id: u64) -> Result<(), ControlPlaneError> {
        let _ = node_id;
        Err(ControlPlaneError::rpc_remote(
            "control-plane Raft leadership transfer is not supported by this authority".to_owned(),
        ))
    }

    fn trigger_raft_snapshot_and_purge(&mut self) -> Result<Option<u64>, ControlPlaneError> {
        Err(ControlPlaneError::rpc_remote(
            "control-plane Raft snapshot trigger is not supported by this authority".to_owned(),
        ))
    }

    fn trigger_raft_election(&mut self) -> Result<(), ControlPlaneError> {
        Err(ControlPlaneError::rpc_remote(
            "control-plane Raft election trigger is not supported by this authority".to_owned(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeServiceAuthorization {
    authority_incarnation: AuthorityIncarnation,
    cluster_epoch: ClusterEpoch,
    node_id: NodeId,
    node_incarnation: u64,
    lease_deadline_ms: u64,
}

impl NodeServiceAuthorization {
    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn node_incarnation(&self) -> u64 {
        self.node_incarnation
    }

    #[must_use]
    pub fn lease_deadline_ms(&self) -> u64 {
        self.lease_deadline_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgPrimaryAuthorization {
    authority_incarnation: AuthorityIncarnation,
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    primary_node_id: NodeId,
    primary_node_incarnation: u64,
    lease_deadline_ms: u64,
}

impl PgPrimaryAuthorization {
    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn primary_node_id(&self) -> NodeId {
        self.primary_node_id
    }

    #[must_use]
    pub fn primary_node_incarnation(&self) -> u64 {
        self.primary_node_incarnation
    }

    #[must_use]
    pub fn lease_deadline_ms(&self) -> u64 {
        self.lease_deadline_ms
    }
}

impl From<PgPrimaryAuthorization> for NodeServiceAuthorization {
    fn from(authorization: PgPrimaryAuthorization) -> Self {
        Self {
            authority_incarnation: authorization.authority_incarnation,
            cluster_epoch: authorization.cluster_epoch,
            node_id: authorization.primary_node_id,
            node_incarnation: authorization.primary_node_incarnation,
            lease_deadline_ms: authorization.lease_deadline_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgServiceOperation {
    MetadataRead,
    MetadataList,
    MetadataWrite,
    PayloadRead,
    PayloadWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgOperationAuthorization {
    operation: PgServiceOperation,
    primary: PgPrimaryAuthorization,
}

impl PgOperationAuthorization {
    #[must_use]
    pub fn operation(&self) -> PgServiceOperation {
        self.operation
    }

    #[must_use]
    pub fn primary(&self) -> PgPrimaryAuthorization {
        self.primary
    }

    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.primary.authority_incarnation()
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.primary.cluster_epoch()
    }

    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.primary.pg_id()
    }

    #[must_use]
    pub fn primary_node_id(&self) -> NodeId {
        self.primary.primary_node_id()
    }

    #[must_use]
    pub fn primary_node_incarnation(&self) -> u64 {
        self.primary.primary_node_incarnation()
    }

    #[must_use]
    pub fn lease_deadline_ms(&self) -> u64 {
        self.primary.lease_deadline_ms()
    }
}

include!("control_plane/single_authority.rs");

include!("control_plane/rpc.rs");

include!("control_plane/error.rs");

fn next_epoch(epoch: ClusterEpoch) -> Result<ClusterEpoch, ControlPlaneError> {
    ClusterEpoch::new(
        epoch
            .get()
            .checked_add(1)
            .ok_or(ControlPlaneError::ClusterEpochOverflow)?,
    )
    .ok_or(ControlPlaneError::ClusterEpochOverflow)
}

pub(crate) fn format_snapshot(snapshot: &ClusterControlSnapshot) -> String {
    let mut out = String::new();
    out.push_str(&format!("version={CURRENT_CONTROL_PLANE_STATE_VERSION}\n"));
    out.push_str(&format!(
        "authority_incarnation={}\n",
        snapshot.authority_incarnation.get()
    ));
    out.push_str(&format!("cluster_epoch={}\n", snapshot.cluster_epoch.get()));
    out.push_str(&format!(
        "initial_topology={}\n",
        format_initial_topology(snapshot.initial_topology.as_ref())
    ));
    out.push_str(&format!(
        "max_committed_timestamp_ms={}\n",
        option_u64(snapshot.max_committed_timestamp_ms)
    ));
    out.push_str(&format!(
        "lease_grant_horizon={}\n",
        format_lease_grant_horizon(snapshot.lease_grant_horizon)
    ));
    let mut history_records = snapshot.history.clone();
    let protection =
        required_cluster_map_history_protection(snapshot.pgs.values(), snapshot.nodes.values());
    prune_cluster_map_history(&mut history_records, &protection, snapshot.cluster_epoch);
    for history in &history_records {
        out.push_str(&format!(
            "history={},{}\n",
            history.cluster_epoch.get(),
            history.authority_incarnation.get()
        ));
        for record in &history.nodes {
            out.push_str(&format!(
                "history_node={},{}\n",
                history.cluster_epoch.get(),
                record.as_u32()
            ));
        }
        for record in &history.pgs {
            out.push_str(&format!(
                "history_pg={},{}\n",
                history.cluster_epoch.get(),
                format_historical_pg_route_record(record)
            ));
        }
        for pg_id in &history.absent_pgs {
            out.push_str(&format!(
                "history_pg_absent={},{}\n",
                history.cluster_epoch.get(),
                pg_id.get()
            ));
        }
    }
    for record in snapshot.nodes.values() {
        out.push_str(&format!("node={}\n", format_node_record(record)));
        for reference in record.cluster_map_history_route_references.iter() {
            out.push_str(&format!(
                "node_history_route={},{},{},{}\n",
                record.node_id.as_u32(),
                cluster_map_history_route_reference_kind_as_str(reference.kind()),
                reference.cluster_epoch().get(),
                reference.pg_id().get()
            ));
        }
        for observation in record.pg_observations.values() {
            out.push_str(&format!(
                "node_pg={}\n",
                format_node_pg_record(record.node_id, observation)
            ));
        }
    }
    for record in snapshot.pgs.values() {
        out.push_str(&format!("pg={}\n", format_pg_record(record)));
    }
    out
}

fn format_lease_grant_horizon(horizon: Option<CommittedLeaseGrantHorizon>) -> String {
    let Some(horizon) = horizon else {
        return "-".to_owned();
    };
    format!(
        "{},{},{}",
        horizon.authority().clock_generation(),
        option_u64(horizon.authority().raft_term()),
        horizon.grant_not_after_ms()
    )
}

fn format_historical_pg_route_record(record: &HistoricalPgRouteRecord) -> String {
    let (
        transfer_source_epoch,
        transfer_source_log_index,
        transfer_source_log_hash,
        transfer_source_state_digest,
        transfer_imported_log_index,
        transfer_imported_log_hash,
        transfer_imported_state_digest,
    ) = match record.peering_metadata_transfer {
        Some(transfer) => {
            let source = transfer.source_metadata_proof();
            let imported = transfer.metadata_proof();
            (
                transfer.source_epoch().get().to_string(),
                source.applied_log_index.to_string(),
                source.applied_log_hash.to_string(),
                source.state_digest.to_string(),
                imported.applied_log_index.to_string(),
                imported.applied_log_hash.to_string(),
                imported.state_digest.to_string(),
            )
        }
        None => (
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
        ),
    };
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{}",
        record.pg_id.get(),
        pg_state_as_str(record.state),
        format_node_list(&record.acting_set),
        option_u32(record.active_primary.map(NodeId::as_u32)),
        transfer_source_epoch,
        transfer_source_log_index,
        transfer_source_log_hash,
        transfer_source_state_digest,
        transfer_imported_log_index,
        transfer_imported_log_hash,
        transfer_imported_state_digest,
        option_u64(
            record
                .peering_metadata_transfer_source_route_epoch
                .map(ClusterEpoch::get)
        ),
        option_u32(
            record
                .peering_metadata_transfer_source_node_id
                .map(NodeId::as_u32)
        )
    )
}

fn format_node_record(record: &NodeControlRecord) -> String {
    format!(
        "{},{},{},{},{},{},{},{},{}",
        record.node_id.as_u32(),
        record.membership.as_str(),
        u8::from(record.administratively_available),
        record.observed_availability.as_str(),
        record.node_incarnation,
        option_u64(record.last_observed_epoch.map(ClusterEpoch::get)),
        option_u64(record.last_heartbeat_ms),
        option_u64(record.lease_deadline_ms),
        hex_encode(record.endpoint.as_bytes())
    )
}

const fn cluster_map_history_route_reference_kind_as_str(
    kind: PgClusterMapHistoryRouteReferenceKind,
) -> &'static str {
    match kind {
        PgClusterMapHistoryRouteReferenceKind::LivePlacement => "live",
        PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource => "backfill-source",
        PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired => "backfill-desired",
        PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand => "pending-command",
        PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim => {
            "object-payload-reclaim-claim"
        }
    }
}

fn format_pg_record(record: &PgControlRecord) -> String {
    let (active_log_index, active_log_hash, active_state_digest) =
        match record.active_metadata_proof {
            Some(proof) => (
                option_u64(Some(proof.applied_log_index)),
                option_u64(Some(proof.applied_log_hash)),
                option_u64(Some(proof.state_digest)),
            ),
            None => (option_u64(None), option_u64(None), option_u64(None)),
        };
    let (peering_floor_log_index, peering_floor_log_hash, peering_floor_state_digest) =
        match record.peering_metadata_proof_floor {
            Some(proof) => (
                option_u64(Some(proof.applied_log_index)),
                option_u64(Some(proof.applied_log_hash)),
                option_u64(Some(proof.state_digest)),
            ),
            None => (option_u64(None), option_u64(None), option_u64(None)),
        };
    let (
        transfer_source_epoch,
        transfer_source_log_index,
        transfer_source_log_hash,
        transfer_source_state_digest,
        transfer_imported_log_index,
        transfer_imported_log_hash,
        transfer_imported_state_digest,
        transfer_source_route_epoch,
        transfer_source_node_id,
    ) = match record.peering_metadata_transfer {
        Some(transfer) => (
            option_u64(Some(transfer.source_epoch().get())),
            option_u64(Some(transfer.source_metadata_proof().applied_log_index)),
            option_u64(Some(transfer.source_metadata_proof().applied_log_hash)),
            option_u64(Some(transfer.source_metadata_proof().state_digest)),
            option_u64(Some(transfer.metadata_proof().applied_log_index)),
            option_u64(Some(transfer.metadata_proof().applied_log_hash)),
            option_u64(Some(transfer.metadata_proof().state_digest)),
            option_u64(
                record
                    .peering_metadata_transfer_source_route_epoch
                    .map(ClusterEpoch::get),
            ),
            option_u32(
                record
                    .peering_metadata_transfer_source_node_id
                    .map(NodeId::as_u32),
            ),
        ),
        None => (
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u32(None),
        ),
    };
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        record.pg_id.get(),
        pg_state_as_str(record.state),
        format_node_list(&record.acting_set),
        option_u32(record.active_primary.map(NodeId::as_u32)),
        active_log_index,
        active_log_hash,
        active_state_digest,
        peering_floor_log_index,
        peering_floor_log_hash,
        peering_floor_state_digest,
        transfer_source_epoch,
        transfer_source_log_index,
        transfer_source_log_hash,
        transfer_source_state_digest,
        transfer_imported_log_index,
        transfer_imported_log_hash,
        transfer_imported_state_digest,
        transfer_source_route_epoch,
        transfer_source_node_id,
        u8::from(record.metadata_transfer_fenced),
        u8::from(record.active_metadata_transfer_imported),
        option_u64(record.metadata_transfer_fence_source_lease_deadline_ms),
        u8::from(record.metadata_transfer_fence_source_imported),
        option_u64(record.active_metadata_proof_epoch.map(ClusterEpoch::get)),
        option_u64(
            record
                .peering_metadata_proof_floor_epoch
                .map(ClusterEpoch::get)
        ),
        u8::from(record.peering_metadata_proof_floor_imported),
        option_u32(
            record
                .previous_primary_lease
                .as_ref()
                .map(|previous| previous.node_id.as_u32())
        ),
        option_u64(
            record
                .previous_primary_lease
                .as_ref()
                .map(|previous| previous.node_incarnation)
        ),
        record.previous_primary_lease.as_ref().map_or_else(
            || "-".to_owned(),
            |previous| hex_encode(previous.endpoint.as_bytes())
        ),
        option_u64(
            record
                .previous_primary_lease
                .as_ref()
                .map(|previous| previous.lease_deadline_ms)
        ),
        u8::from(
            record
                .previous_primary_lease
                .as_ref()
                .is_some_and(|previous| previous.prefer_reactivation)
        ),
        option_u64(record.metadata_transfer_fence_epoch.map(ClusterEpoch::get))
    )
}

fn format_node_pg_record(node_id: NodeId, record: &NodePgObservationRecord) -> String {
    format!(
        "{},{},{},{},{},{},{},{},{},{},{}",
        node_id.as_u32(),
        record.pg_id.get(),
        pg_state_as_str(record.state),
        record.observed_epoch.get(),
        record.observed_at_ms,
        record.metadata_proof.applied_log_index,
        record.metadata_proof.applied_log_hash,
        record.metadata_proof.state_digest,
        option_u64(
            record
                .pending_metadata_command
                .map(PendingMetadataCommandObservation::cluster_epoch)
                .map(ClusterEpoch::get)
        ),
        option_u64(
            record
                .pending_metadata_command
                .map(PendingMetadataCommandObservation::log_index)
        ),
        option_u64(
            record
                .pending_metadata_command
                .map(PendingMetadataCommandObservation::command_checksum)
        )
    )
}

pub(crate) fn parse_snapshot(contents: &str) -> Result<ClusterControlSnapshot, ControlPlaneError> {
    let snapshot = parse_snapshot_without_publication_validation(contents)?;
    snapshot
        .validate_current_state_invariants()
        .map_err(|message| parse_error(0, &message))?;
    Ok(snapshot)
}

pub(crate) fn parse_snapshot_without_publication_validation(
    contents: &str,
) -> Result<ClusterControlSnapshot, ControlPlaneError> {
    let mut version = None;
    let mut authority_incarnation = None;
    let mut cluster_epoch = None;
    let mut initial_topology = None;
    let mut initial_topology_seen = false;
    let mut max_committed_timestamp_ms = None;
    let mut max_committed_timestamp_seen = false;
    let mut lease_grant_horizon = None;
    let mut lease_grant_horizon_seen = false;
    let mut nodes = BTreeMap::new();
    let mut pgs = BTreeMap::new();
    let mut pg_lines = BTreeMap::new();
    let mut node_pg_lines = BTreeMap::<(NodeId, PgId), usize>::new();
    let mut history = BTreeMap::<ClusterEpoch, ParsedHistoryRecord>::new();

    for (idx, line) in contents.lines().enumerate() {
        let line_number = idx + 1;
        if line.is_empty() {
            continue;
        }
        if let Some(value) = line.strip_prefix("version=") {
            let parsed_version = parse_u64(line_number, value, "version")?;
            if parsed_version != CURRENT_CONTROL_PLANE_STATE_VERSION {
                return Err(parse_error(
                    line_number,
                    "missing or unsupported control-plane state version",
                ));
            }
            version = Some(parsed_version);
        } else if let Some(value) = line.strip_prefix("authority_incarnation=") {
            authority_incarnation = Some(
                AuthorityIncarnation::new(parse_u64(line_number, value, "authority_incarnation")?)
                    .ok_or_else(|| {
                        parse_error(line_number, "authority incarnation must be nonzero")
                    })?,
            );
        } else if let Some(value) = line.strip_prefix("cluster_epoch=") {
            cluster_epoch = Some(
                ClusterEpoch::new(parse_u64(line_number, value, "cluster_epoch")?)
                    .ok_or_else(|| parse_error(line_number, "cluster epoch must be nonzero"))?,
            );
        } else if let Some(value) = line.strip_prefix("initial_topology=") {
            if initial_topology_seen {
                return Err(parse_error(line_number, "duplicate initial topology"));
            }
            initial_topology_seen = true;
            initial_topology = parse_initial_topology(line_number, value)?;
        } else if let Some(value) = line.strip_prefix("max_committed_timestamp_ms=") {
            if max_committed_timestamp_seen {
                return Err(parse_error(
                    line_number,
                    "duplicate max committed timestamp",
                ));
            }
            max_committed_timestamp_seen = true;
            let state_version = version.ok_or_else(|| {
                parse_error(line_number, "version must precede max committed timestamp")
            })?;
            if state_version != CURRENT_CONTROL_PLANE_STATE_VERSION {
                return Err(parse_error(
                    line_number,
                    "max committed timestamp requires current control-plane state version",
                ));
            }
            max_committed_timestamp_ms =
                parse_option_u64(line_number, value, "max committed timestamp")?;
        } else if let Some(value) = line.strip_prefix("lease_grant_horizon=") {
            if lease_grant_horizon_seen {
                return Err(parse_error(line_number, "duplicate lease grant horizon"));
            }
            lease_grant_horizon_seen = true;
            lease_grant_horizon = parse_lease_grant_horizon(line_number, value)?;
        } else if let Some(value) = line.strip_prefix("history=") {
            let record = parse_history_record(line_number, value)?;
            if history
                .insert(
                    record.cluster_epoch,
                    ParsedHistoryRecord::new(record, line_number),
                )
                .is_some()
            {
                return Err(parse_error(line_number, "duplicate history record"));
            }
        } else if let Some(value) = line.strip_prefix("history_node=") {
            let (epoch, node_id) = parse_history_node_record(line_number, value)?;
            let history_record = history
                .get_mut(&epoch)
                .ok_or_else(|| parse_error(line_number, "history node references unknown epoch"))?;
            if history_record.node_ids.insert(node_id) {
                history_record.record.nodes.push(node_id);
            } else {
                return Err(parse_error(line_number, "duplicate history node record"));
            }
        } else if let Some(value) = line.strip_prefix("history_pg=") {
            version.ok_or_else(|| {
                parse_error(line_number, "version must precede history PG records")
            })?;
            let (epoch, record) = parse_history_pg_record(line_number, value)?;
            let history_record = history
                .get_mut(&epoch)
                .ok_or_else(|| parse_error(line_number, "history PG references unknown epoch"))?;
            if history_record.pg_ids.insert(record.pg_id) {
                history_record.pg_lines.insert(record.pg_id, line_number);
                history_record.record.pgs.push(record);
            } else {
                return Err(parse_error(line_number, "duplicate history PG record"));
            }
        } else if let Some(value) = line.strip_prefix("history_pg_absent=") {
            let (epoch, pg_id) = parse_history_pg_absent_record(line_number, value)?;
            let history_record = history.get_mut(&epoch).ok_or_else(|| {
                parse_error(line_number, "history absent PG references unknown epoch")
            })?;
            if history_record
                .record
                .absent_pgs
                .last()
                .is_some_and(|previous| *previous >= pg_id)
            {
                return Err(parse_error(
                    line_number,
                    "history absent PG records must be strictly increasing",
                ));
            }
            if history_record.pg_ids.insert(pg_id) {
                history_record.record.absent_pgs.push(pg_id);
            } else {
                return Err(parse_error(
                    line_number,
                    "duplicate or conflicting history absent PG record",
                ));
            }
        } else if let Some(value) = line.strip_prefix("node=") {
            let record = parse_node_record(line_number, value)?;
            if nodes.insert(record.node_id, record).is_some() {
                return Err(parse_error(line_number, "duplicate node record"));
            }
        } else if let Some(value) = line.strip_prefix("node_history_route=") {
            let (node_id, reference) = parse_node_history_route_reference(line_number, value)?;
            let node = nodes.get_mut(&node_id).ok_or_else(|| {
                parse_error(line_number, "node history route references unknown node")
            })?;
            let previous_len = node.cluster_map_history_route_references.len();
            node.cluster_map_history_route_references
                .insert(reference)
                .map_err(|error| parse_error(line_number, &error.to_string()))?;
            if node.cluster_map_history_route_references.len() == previous_len {
                return Err(parse_error(
                    line_number,
                    "duplicate node history route reference",
                ));
            }
        } else if let Some(value) = line.strip_prefix("node_pg=") {
            version.ok_or_else(|| {
                parse_error(line_number, "version must precede PG observation records")
            })?;
            let (node_id, observation) = parse_node_pg_record(line_number, value)?;
            let node = nodes
                .get_mut(&node_id)
                .ok_or_else(|| parse_error(line_number, "node PG references unknown node"))?;
            if node
                .pg_observations
                .insert(observation.pg_id, observation)
                .is_some()
            {
                return Err(parse_error(line_number, "duplicate node PG observation"));
            }
            node_pg_lines.insert((node_id, observation.pg_id), line_number);
        } else if let Some(value) = line.strip_prefix("pg=") {
            version.ok_or_else(|| parse_error(line_number, "version must precede PG records"))?;
            let record = parse_pg_record(line_number, value)?;
            let pg_id = record.pg_id;
            if pgs.insert(pg_id, record).is_some() {
                return Err(parse_error(line_number, "duplicate PG record"));
            }
            pg_lines.insert(pg_id, line_number);
        } else {
            return Err(parse_error(line_number, "unknown control-plane state line"));
        }
    }

    let version = version
        .ok_or_else(|| parse_error(0, "missing or unsupported control-plane state version"))?;
    if version != CURRENT_CONTROL_PLANE_STATE_VERSION {
        return Err(parse_error(
            0,
            "missing or unsupported control-plane state version",
        ));
    }
    if !max_committed_timestamp_seen {
        return Err(parse_error(0, "missing max committed timestamp"));
    }
    if !initial_topology_seen {
        return Err(parse_error(0, "missing initial topology"));
    }
    if !lease_grant_horizon_seen {
        return Err(parse_error(0, "missing lease grant horizon"));
    }
    let cluster_epoch = cluster_epoch.ok_or_else(|| parse_error(0, "missing cluster epoch"))?;
    validate_current_pgs(&pgs, &pg_lines, &nodes, cluster_epoch)?;
    validate_current_pg_observations(&nodes, &node_pg_lines, &pgs, cluster_epoch)?;
    validate_parsed_history(&history, cluster_epoch)?;
    let mut history: Vec<ClusterMapHistoryRecord> =
        history.into_values().map(|record| record.record).collect();
    let protection = required_cluster_map_history_protection(pgs.values(), nodes.values());
    prune_cluster_map_history(&mut history, &protection, cluster_epoch);
    validate_metadata_transfer_route_references(
        &history,
        cluster_epoch,
        pgs.values().map(|pg| {
            (
                pg.pg_id,
                pg.peering_metadata_transfer_source_route_epoch,
                pg.peering_metadata_transfer_source_node_id,
            )
        }),
    )
    .map_err(|message| parse_error(0, &message))?;
    validate_required_cluster_map_history(&history, &pgs, &nodes, cluster_epoch)?;
    let snapshot = ClusterControlSnapshot {
        authority_incarnation: authority_incarnation
            .ok_or_else(|| parse_error(0, "missing authority incarnation"))?,
        cluster_epoch,
        initial_topology,
        nodes,
        pgs,
        max_committed_timestamp_ms,
        lease_grant_horizon,
        history,
    };
    if format_snapshot(&snapshot) != contents {
        return Err(parse_error(
            0,
            "control-plane state must use canonical snapshot encoding",
        ));
    }
    Ok(snapshot)
}

fn format_initial_topology(certificate: Option<&InitialClusterTopologyCertificate>) -> String {
    let Some(certificate) = certificate else {
        return "-".to_string();
    };
    let voters = certificate
        .raft_voters()
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(":");
    format!(
        "{},{},{},{}",
        certificate.topology_generation(),
        hex_encode(certificate.topology_digest()),
        hex_encode(certificate.bootstrap_map_digest()),
        voters
    )
}

fn parse_initial_topology(
    line: usize,
    value: &str,
) -> Result<Option<InitialClusterTopologyCertificate>, ControlPlaneError> {
    if value == "-" {
        return Ok(None);
    }
    let fields = value.split(',').collect::<Vec<_>>();
    if fields.len() != 4 {
        return Err(parse_error(line, "invalid initial topology field count"));
    }
    let topology_digest: [u8; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN] = hex_decode(line, fields[1])?
        .try_into()
        .map_err(|_| parse_error(line, "initial topology digest must contain 32 bytes"))?;
    let bootstrap_map_digest: [u8; CONTROL_PLANE_BOOTSTRAP_MAP_DIGEST_LEN] =
        hex_decode(line, fields[2])?.try_into().map_err(|_| {
            parse_error(
                line,
                "initial topology bootstrap-map digest must contain 32 bytes",
            )
        })?;
    let voters = if fields[3].is_empty() {
        Vec::new()
    } else {
        fields[3]
            .split(':')
            .map(|value| parse_u64(line, value, "initial topology Raft voter"))
            .collect::<Result<Vec<_>, _>>()?
    };
    InitialClusterTopologyCertificate::new(
        parse_u64(line, fields[0], "initial topology generation")?,
        topology_digest,
        bootstrap_map_digest,
        voters,
    )
    .map(Some)
    .map_err(|error| parse_error(line, &error.to_string()))
}

struct ParsedHistoryRecord {
    record: ClusterMapHistoryRecord,
    line: usize,
    node_ids: BTreeSet<NodeId>,
    pg_ids: BTreeSet<PgId>,
    pg_lines: BTreeMap<PgId, usize>,
}

impl ParsedHistoryRecord {
    fn new(record: ClusterMapHistoryRecord, line: usize) -> Self {
        Self {
            record,
            line,
            node_ids: BTreeSet::new(),
            pg_ids: BTreeSet::new(),
            pg_lines: BTreeMap::new(),
        }
    }
}

fn validate_current_pgs(
    pgs: &BTreeMap<PgId, PgControlRecord>,
    pg_lines: &BTreeMap<PgId, usize>,
    nodes: &BTreeMap<NodeId, NodeControlRecord>,
    current_epoch: ClusterEpoch,
) -> Result<(), ControlPlaneError> {
    for pg in pgs.values() {
        let line = pg_lines.get(&pg.pg_id).copied().unwrap_or(0);
        for node_id in &pg.acting_set {
            if !nodes.contains_key(node_id) {
                return Err(parse_error(line, "PG acting set references unknown node"));
            }
        }
        if let Some(previous) = &pg.previous_primary_lease {
            if !nodes.contains_key(&previous.node_id) {
                return Err(parse_error(
                    line,
                    "PG previous primary references unknown node",
                ));
            }
        }
        validate_persisted_metadata_transfer_epoch(line, pg, current_epoch)?;
    }
    Ok(())
}

fn validate_current_pg_observations(
    nodes: &BTreeMap<NodeId, NodeControlRecord>,
    node_pg_lines: &BTreeMap<(NodeId, PgId), usize>,
    pgs: &BTreeMap<PgId, PgControlRecord>,
    current_epoch: ClusterEpoch,
) -> Result<(), ControlPlaneError> {
    for node in nodes.values() {
        for observation in node.pg_observations.values() {
            let line = node_pg_lines
                .get(&(node.node_id, observation.pg_id))
                .copied()
                .unwrap_or(0);
            if observation.observed_epoch != current_epoch {
                return Err(parse_error(
                    line,
                    "node PG observation epoch must match current cluster epoch",
                ));
            }
            let pg = pgs
                .get(&observation.pg_id)
                .ok_or_else(|| parse_error(line, "node PG observation references unknown PG"))?;
            if !pg.acting_set.contains(&node.node_id) {
                return Err(parse_error(
                    line,
                    "node PG observation references PG outside node acting set",
                ));
            }
            if pg.state == PgState::Active
                && pg.active_primary == Some(node.node_id)
                && observation.state == PgState::Active
            {
                if observation.has_pending_metadata_command() {
                    return Err(parse_error(
                        line,
                        "active node PG observation must not have pending metadata command",
                    ));
                }
                let Some(expected) = pg.active_metadata_proof else {
                    return Err(parse_error(line, "active PG is missing metadata proof"));
                };
                if !metadata_proof_satisfies_active_primary_observation_floor(
                    expected,
                    observation.metadata_proof,
                    metadata_proof_progress_provenance(
                        pg.active_metadata_transfer_imported,
                        pg.active_metadata_proof_epoch,
                    ),
                    observation.observed_epoch,
                ) {
                    return Err(parse_error(
                        line,
                        "active node PG observation metadata proof is behind or diverges from PG active proof",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_parsed_history(
    history: &BTreeMap<ClusterEpoch, ParsedHistoryRecord>,
    current_epoch: ClusterEpoch,
) -> Result<(), ControlPlaneError> {
    for (epoch, record) in history {
        if *epoch >= current_epoch {
            return Err(parse_error(
                record.line,
                "history epoch must be older than current cluster epoch",
            ));
        }
        for pg in &record.record.pgs {
            let line = record
                .pg_lines
                .get(&pg.pg_id)
                .copied()
                .unwrap_or(record.line);
            validate_historical_pg_route_record(pg, *epoch, |node_id| {
                record.node_ids.contains(&node_id)
            })
            .map_err(|message| parse_error(line, &message))?;
        }
    }
    Ok(())
}

fn validate_required_cluster_map_history(
    history: &[ClusterMapHistoryRecord],
    pgs: &BTreeMap<PgId, PgControlRecord>,
    nodes: &BTreeMap<NodeId, NodeControlRecord>,
    current_epoch: ClusterEpoch,
) -> Result<(), ControlPlaneError> {
    let retained_epochs: BTreeSet<_> = history
        .iter()
        .map(ClusterMapHistoryRecord::cluster_epoch)
        .collect();
    let mut pg_introduction_boundaries = BTreeMap::new();
    for (index, record) in history.iter().enumerate() {
        for pg_id in &record.absent_pgs {
            if pg_introduction_boundaries
                .insert(*pg_id, record.cluster_epoch())
                .is_some()
            {
                return Err(parse_error(0, "history repeats a PG introduction boundary"));
            }
            if !pgs.contains_key(pg_id) {
                return Err(parse_error(
                    0,
                    "history absent PG is missing from current state",
                ));
            }
            if history[..=index]
                .iter()
                .any(|earlier| earlier.pg(*pg_id).is_some())
            {
                return Err(parse_error(
                    0,
                    "history PG route precedes its introduction boundary",
                ));
            }
        }
    }
    for pg in pgs.values() {
        let Some(source_route_epoch) = pg.peering_metadata_transfer_source_route_epoch else {
            continue;
        };
        if source_route_epoch >= current_epoch {
            continue;
        }
        if !retained_epochs.contains(&source_route_epoch) {
            return Err(parse_error(
                0,
                "metadata transfer source route epoch is not retained in cluster-map history",
            ));
        }
    }
    for node in nodes.values() {
        for reference in node.cluster_map_history_route_references.iter() {
            let retained = if reference.cluster_epoch() == current_epoch {
                pgs.contains_key(&reference.pg_id())
            } else if reference.cluster_epoch() < current_epoch {
                history
                    .iter()
                    .any(|record| record.cluster_epoch() == reference.cluster_epoch())
                    && historical_pg_exists_at_epoch(
                        history,
                        pgs,
                        reference.pg_id(),
                        reference.cluster_epoch(),
                    )
            } else {
                false
            };
            if !retained {
                return Err(parse_error(
                    0,
                    "storage cluster-map history route reference is not retained",
                ));
            }
        }
    }
    Ok(())
}

fn historical_pg_exists_at_epoch(
    history: &[ClusterMapHistoryRecord],
    current_pgs: &BTreeMap<PgId, PgControlRecord>,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
) -> bool {
    for record in history
        .iter()
        .filter(|record| record.cluster_epoch() >= cluster_epoch)
    {
        if record.absent_pgs.contains(&pg_id) {
            return false;
        }
        if record.pg(pg_id).is_some() {
            return true;
        }
    }
    current_pgs.contains_key(&pg_id)
}

fn validate_persisted_metadata_transfer_epoch(
    line: usize,
    pg: &PgControlRecord,
    record_epoch: ClusterEpoch,
) -> Result<(), ControlPlaneError> {
    if let Some(transfer) = pg.peering_metadata_transfer {
        if transfer.source_epoch() > record_epoch {
            return Err(parse_error(
                line,
                "metadata transfer source epoch must not be newer than PG record epoch",
            ));
        }
        if let Some(source_route_epoch) = pg.peering_metadata_transfer_source_route_epoch {
            if source_route_epoch > record_epoch {
                return Err(parse_error(
                    line,
                    "metadata transfer source route epoch must not be newer than PG record epoch",
                ));
            }
        }
    }
    Ok(())
}

fn parse_history_record(
    line: usize,
    value: &str,
) -> Result<ClusterMapHistoryRecord, ControlPlaneError> {
    let fields: Vec<&str> = value.split(',').collect();
    if fields.len() != 2 {
        return Err(parse_error(line, "history record must have two fields"));
    }
    let cluster_epoch = ClusterEpoch::new(parse_u64(line, fields[0], "history cluster epoch")?)
        .ok_or_else(|| parse_error(line, "history cluster epoch must be nonzero"))?;
    let authority_incarnation =
        AuthorityIncarnation::new(parse_u64(line, fields[1], "history authority incarnation")?)
            .ok_or_else(|| parse_error(line, "history authority incarnation must be nonzero"))?;
    Ok(ClusterMapHistoryRecord {
        authority_incarnation,
        cluster_epoch,
        nodes: Vec::new(),
        pgs: Vec::new(),
        absent_pgs: Vec::new(),
    })
}

fn parse_history_pg_absent_record(
    line: usize,
    value: &str,
) -> Result<(ClusterEpoch, PgId), ControlPlaneError> {
    let (epoch, pg_id) = value
        .split_once(',')
        .ok_or_else(|| parse_error(line, "history absent PG record must start with epoch"))?;
    let epoch = ClusterEpoch::new(parse_u64(line, epoch, "history absent PG epoch")?)
        .ok_or_else(|| parse_error(line, "history absent PG epoch must be nonzero"))?;
    Ok((
        epoch,
        PgId::new(parse_u32(line, pg_id, "history absent PG id")?),
    ))
}

fn parse_history_node_record(
    line: usize,
    value: &str,
) -> Result<(ClusterEpoch, NodeId), ControlPlaneError> {
    let (epoch, node_id) = value
        .split_once(',')
        .ok_or_else(|| parse_error(line, "history node record must start with epoch"))?;
    let epoch = ClusterEpoch::new(parse_u64(line, epoch, "history node epoch")?)
        .ok_or_else(|| parse_error(line, "history node epoch must be nonzero"))?;
    Ok((
        epoch,
        NodeId::new(parse_u32(line, node_id, "history node id")?),
    ))
}

fn parse_history_pg_record(
    line: usize,
    value: &str,
) -> Result<(ClusterEpoch, HistoricalPgRouteRecord), ControlPlaneError> {
    let (epoch, record) = value
        .split_once(',')
        .ok_or_else(|| parse_error(line, "history PG record must start with epoch"))?;
    let epoch = ClusterEpoch::new(parse_u64(line, epoch, "history PG epoch")?)
        .ok_or_else(|| parse_error(line, "history PG epoch must be nonzero"))?;
    Ok((epoch, parse_historical_pg_route_record(line, record)?))
}

fn parse_historical_pg_route_record(
    line: usize,
    value: &str,
) -> Result<HistoricalPgRouteRecord, ControlPlaneError> {
    let fields: Vec<&str> = value.split(',').collect();
    if fields.len() != 13 {
        return Err(parse_error(
            line,
            "historical PG route record must have thirteen fields",
        ));
    }
    let pg_id = PgId::new(parse_u32(line, fields[0], "historical PG id")?);
    let state = pg_state_from_str(fields[1])?;
    let acting_set = parse_node_list(line, fields[2])?;
    let active_primary =
        parse_option_u32(line, fields[3], "historical active primary")?.map(NodeId::new);
    let source_epoch =
        parse_option_cluster_epoch(line, fields[4], "historical transfer source epoch")?;
    let source_log_index =
        parse_option_u64(line, fields[5], "historical transfer source log index")?;
    let source_log_hash = parse_option_u64(line, fields[6], "historical transfer source log hash")?;
    let source_state_digest =
        parse_option_u64(line, fields[7], "historical transfer source state digest")?;
    let imported_log_index =
        parse_option_u64(line, fields[8], "historical transfer imported log index")?;
    let imported_log_hash =
        parse_option_u64(line, fields[9], "historical transfer imported log hash")?;
    let imported_state_digest = parse_option_u64(
        line,
        fields[10],
        "historical transfer imported state digest",
    )?;
    let peering_metadata_transfer = match (
        source_epoch,
        source_log_index,
        source_log_hash,
        source_state_digest,
        imported_log_index,
        imported_log_hash,
        imported_state_digest,
    ) {
        (
            Some(source_epoch),
            Some(source_log_index),
            Some(source_log_hash),
            Some(source_state_digest),
            Some(imported_log_index),
            Some(imported_log_hash),
            Some(imported_state_digest),
        ) => Some(PgMetadataTransferProof::new_with_imported_metadata_proof(
            source_epoch,
            PgMetadataProof {
                applied_log_index: source_log_index,
                applied_log_hash: source_log_hash,
                state_digest: source_state_digest,
            },
            PgMetadataProof {
                applied_log_index: imported_log_index,
                applied_log_hash: imported_log_hash,
                state_digest: imported_state_digest,
            },
        )),
        (None, None, None, None, None, None, None) => None,
        _ => {
            return Err(parse_error(
                line,
                "historical metadata transfer proof fields must all be present or absent",
            ));
        }
    };
    Ok(HistoricalPgRouteRecord {
        pg_id,
        state,
        acting_set,
        active_primary,
        peering_metadata_transfer,
        peering_metadata_transfer_source_route_epoch: parse_option_cluster_epoch(
            line,
            fields[11],
            "historical transfer source route epoch",
        )?,
        peering_metadata_transfer_source_node_id: parse_option_u32(
            line,
            fields[12],
            "historical transfer source node",
        )?
        .map(NodeId::new),
    })
}

fn parse_node_pg_record(
    line: usize,
    value: &str,
) -> Result<(NodeId, NodePgObservationRecord), ControlPlaneError> {
    let fields: Vec<&str> = value.split(',').collect();
    if fields.len() != 11 {
        return Err(parse_error(
            line,
            "node PG observation record must have eleven fields",
        ));
    }
    let node_id = NodeId::new(parse_u32(line, fields[0], "node id")?);
    let pg_id = PgId::new(parse_u32(line, fields[1], "PG id")?);
    let state = pg_state_from_str(fields[2])?;
    let observed_epoch = ClusterEpoch::new(parse_u64(line, fields[3], "observed epoch")?)
        .ok_or_else(|| parse_error(line, "observed epoch must be nonzero"))?;
    let observed_at_ms = parse_u64(line, fields[4], "observed at")?;
    let metadata_proof = PgMetadataProof {
        applied_log_index: parse_u64(line, fields[5], "applied log index")?,
        applied_log_hash: parse_u64(line, fields[6], "applied log hash")?,
        state_digest: parse_u64(line, fields[7], "state digest")?,
    };
    let pending_cluster_epoch =
        parse_option_cluster_epoch(line, fields[8], "pending command cluster epoch")?;
    let pending_log_index = parse_option_u64(line, fields[9], "pending command log index")?;
    let pending_command_checksum = parse_option_u64(line, fields[10], "pending command checksum")?;
    let pending_metadata_command = match (
        pending_cluster_epoch,
        pending_log_index,
        pending_command_checksum,
    ) {
        (None, None, None) => None,
        (Some(cluster_epoch), Some(log_index), Some(command_checksum)) => {
            let log_index = NonZeroU64::new(log_index)
                .ok_or_else(|| parse_error(line, "pending command log index must be nonzero"))?;
            Some(PendingMetadataCommandObservation::new(
                cluster_epoch,
                log_index,
                command_checksum,
            ))
        }
        _ => {
            return Err(parse_error(
                line,
                "pending command identity fields must all be present or absent",
            ));
        }
    };
    Ok((
        node_id,
        NodePgObservationRecord {
            pg_id,
            state,
            observed_epoch,
            observed_at_ms,
            metadata_proof,
            pending_metadata_command,
        },
    ))
}

fn parse_node_record(line: usize, value: &str) -> Result<NodeControlRecord, ControlPlaneError> {
    let fields: Vec<&str> = value.split(',').collect();
    if fields.len() != 9 {
        return Err(parse_error(line, "node record must have nine fields"));
    }
    let node_id = NodeId::new(parse_u32(line, fields[0], "node id")?);
    let membership = NodeMembershipState::from_str(fields[1])?;
    let administratively_available = parse_bool_u8(line, fields[2], "administrative availability")?;
    let observed_availability = NodeAvailabilityState::from_str(fields[3])?;
    let node_incarnation = parse_u64(line, fields[4], "node incarnation")?;
    let last_observed_epoch = parse_option_cluster_epoch(line, fields[5], "last observed epoch")?;
    let last_heartbeat_ms = parse_option_u64(line, fields[6], "last heartbeat")?;
    let lease_deadline_ms = parse_option_u64(line, fields[7], "lease deadline")?;
    let endpoint = String::from_utf8(hex_decode(line, fields[8])?)
        .map_err(|_| parse_error(line, "node endpoint must be valid UTF-8 after hex decoding"))?;
    Ok(NodeControlRecord {
        node_id,
        membership,
        administratively_available,
        observed_availability,
        node_incarnation,
        endpoint,
        last_observed_epoch,
        last_heartbeat_ms,
        lease_deadline_ms,
        cluster_map_history_route_references: PgClusterMapHistoryRouteReferences::default(),
        pg_observations: BTreeMap::new(),
    })
}

fn parse_node_history_route_reference(
    line: usize,
    value: &str,
) -> Result<(NodeId, PgClusterMapHistoryRouteReference), ControlPlaneError> {
    let fields: Vec<_> = value.split(',').collect();
    if fields.len() != 4 {
        return Err(parse_error(
            line,
            "node history route reference must have four fields",
        ));
    }
    let node_id = NodeId::new(parse_u32(line, fields[0], "node id")?);
    let kind = match fields[1] {
        "live" => PgClusterMapHistoryRouteReferenceKind::LivePlacement,
        "backfill-source" => PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
        "backfill-desired" => PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
        "pending-command" => PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand,
        "object-payload-reclaim-claim" => {
            PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim
        }
        _ => {
            return Err(parse_error(
                line,
                "invalid node history route reference kind",
            ));
        }
    };
    let cluster_epoch = ClusterEpoch::new(parse_u64(
        line,
        fields[2],
        "node history route reference epoch",
    )?)
    .ok_or_else(|| parse_error(line, "node history route reference epoch must be nonzero"))?;
    let pg_id = PgId::new(parse_u32(
        line,
        fields[3],
        "node history route reference PG id",
    )?);
    Ok((
        node_id,
        PgClusterMapHistoryRouteReference::new(kind, cluster_epoch, pg_id),
    ))
}

fn parse_pg_record(line: usize, value: &str) -> Result<PgControlRecord, ControlPlaneError> {
    let fields: Vec<&str> = value.split(',').collect();
    if fields.len() != 32 {
        return Err(parse_error(line, "PG record must have thirty-two fields"));
    }
    let pg_id = PgId::new(parse_u32(line, fields[0], "PG id")?);
    let state = pg_state_from_str(fields[1])?;
    let acting_set = parse_node_list(line, fields[2])?;
    let active_primary = parse_option_u32(line, fields[3], "active primary")?.map(NodeId::new);
    let active_log_index = parse_option_u64(line, fields[4], "active applied log index")?;
    let active_log_hash = parse_option_u64(line, fields[5], "active applied log hash")?;
    let active_state_digest = parse_option_u64(line, fields[6], "active state digest")?;
    let active_metadata_proof = match (active_log_index, active_log_hash, active_state_digest) {
        (Some(applied_log_index), Some(applied_log_hash), Some(state_digest)) => {
            Some(PgMetadataProof {
                applied_log_index,
                applied_log_hash,
                state_digest,
            })
        }
        (None, None, None) => None,
        _ => {
            return Err(parse_error(
                line,
                "active PG metadata proof fields must be all present or all absent",
            ));
        }
    };
    let peering_metadata_proof_floor = if fields.len() >= 10 {
        let peering_floor_log_index =
            parse_option_u64(line, fields[7], "peering floor applied log index")?;
        let peering_floor_log_hash =
            parse_option_u64(line, fields[8], "peering floor applied log hash")?;
        let peering_floor_state_digest =
            parse_option_u64(line, fields[9], "peering floor state digest")?;
        match (
            peering_floor_log_index,
            peering_floor_log_hash,
            peering_floor_state_digest,
        ) {
            (Some(applied_log_index), Some(applied_log_hash), Some(state_digest)) => {
                Some(PgMetadataProof {
                    applied_log_index,
                    applied_log_hash,
                    state_digest,
                })
            }
            (None, None, None) => None,
            _ => {
                return Err(parse_error(
                    line,
                    "peering metadata proof floor fields must be all present or all absent",
                ));
            }
        }
    } else {
        None
    };
    let peering_metadata_transfer =
        if fields.len() == 14 || fields.len() == 17 || fields.len() >= 18 {
            let transfer_source_epoch =
                parse_option_cluster_epoch(line, fields[10], "metadata transfer source epoch")?;
            let transfer_source_log_index = parse_option_u64(
                line,
                fields[11],
                "metadata transfer source applied log index",
            )?;
            let transfer_source_log_hash = parse_option_u64(
                line,
                fields[12],
                "metadata transfer source applied log hash",
            )?;
            let transfer_source_state_digest =
                parse_option_u64(line, fields[13], "metadata transfer source state digest")?;
            let transfer_imported = if fields.len() == 17 || fields.len() >= 18 {
                let transfer_imported_log_index = parse_option_u64(
                    line,
                    fields[14],
                    "metadata transfer imported applied log index",
                )?;
                let transfer_imported_log_hash = parse_option_u64(
                    line,
                    fields[15],
                    "metadata transfer imported applied log hash",
                )?;
                let transfer_imported_state_digest =
                    parse_option_u64(line, fields[16], "metadata transfer imported state digest")?;
                match (
                    transfer_imported_log_index,
                    transfer_imported_log_hash,
                    transfer_imported_state_digest,
                ) {
                    (Some(applied_log_index), Some(applied_log_hash), Some(state_digest)) => {
                        Some(PgMetadataProof {
                            applied_log_index,
                            applied_log_hash,
                            state_digest,
                        })
                    }
                    (None, None, None) => None,
                    _ => {
                        return Err(parse_error(
                        line,
                        "metadata transfer imported proof fields must be all present or all absent",
                    ));
                    }
                }
            } else {
                None
            };
            match (
                transfer_source_epoch,
                transfer_source_log_index,
                transfer_source_log_hash,
                transfer_source_state_digest,
                transfer_imported,
            ) {
                (
                    Some(source_epoch),
                    Some(applied_log_index),
                    Some(applied_log_hash),
                    Some(state_digest),
                    imported_metadata_proof,
                ) => Some(PgMetadataTransferProof {
                    source_epoch,
                    source_metadata_proof: PgMetadataProof {
                        applied_log_index,
                        applied_log_hash,
                        state_digest,
                    },
                    imported_metadata_proof: imported_metadata_proof.unwrap_or(PgMetadataProof {
                        applied_log_index,
                        applied_log_hash,
                        state_digest,
                    }),
                }),
                (None, None, None, None, None) => None,
                _ => {
                    return Err(parse_error(
                        line,
                        "metadata transfer source proof fields must be all present or all absent",
                    ));
                }
            }
        } else {
            None
        };
    let (
        peering_metadata_transfer_source_route_epoch,
        peering_metadata_transfer_source_node_id,
        metadata_transfer_field_offset,
    ) = if fields.len() >= 23 {
        (
            parse_option_cluster_epoch(line, fields[17], "metadata transfer source route epoch")?,
            parse_option_u32(line, fields[18], "metadata transfer source node")?.map(NodeId::new),
            19,
        )
    } else {
        (None, None, 17)
    };
    let metadata_transfer_fenced = if fields.len() > metadata_transfer_field_offset {
        parse_bool_u8(
            line,
            fields[metadata_transfer_field_offset],
            "metadata transfer fenced",
        )?
    } else {
        false
    };
    let active_metadata_transfer_imported = if fields.len() > metadata_transfer_field_offset + 1 {
        parse_bool_u8(
            line,
            fields[metadata_transfer_field_offset + 1],
            "active metadata transfer imported provenance",
        )?
    } else {
        false
    };
    let metadata_transfer_fence_source_lease_deadline_ms =
        if fields.len() > metadata_transfer_field_offset + 2 {
            parse_option_u64(
                line,
                fields[metadata_transfer_field_offset + 2],
                "metadata transfer fence source lease deadline",
            )?
        } else {
            None
        };
    let metadata_transfer_fence_source_imported =
        if fields.len() > metadata_transfer_field_offset + 3 {
            parse_bool_u8(
                line,
                fields[metadata_transfer_field_offset + 3],
                "metadata transfer fence source imported provenance",
            )?
        } else {
            false
        };
    let active_metadata_proof_epoch = if fields.len() > metadata_transfer_field_offset + 4 {
        parse_option_cluster_epoch(
            line,
            fields[metadata_transfer_field_offset + 4],
            "active metadata proof epoch",
        )?
    } else {
        None
    };
    let peering_metadata_proof_floor_epoch = if fields.len() > metadata_transfer_field_offset + 5 {
        parse_option_cluster_epoch(
            line,
            fields[metadata_transfer_field_offset + 5],
            "peering metadata proof floor epoch",
        )?
    } else {
        None
    };
    let peering_metadata_proof_floor_imported = if fields.len() > metadata_transfer_field_offset + 6
    {
        parse_bool_u8(
            line,
            fields[metadata_transfer_field_offset + 6],
            "peering metadata proof floor imported provenance",
        )?
    } else {
        false
    };
    let previous_primary_node_id =
        parse_option_u32(line, fields[26], "previous primary node id")?.map(NodeId::new);
    let previous_primary_node_incarnation =
        parse_option_u64(line, fields[27], "previous primary node incarnation")?;
    if previous_primary_node_incarnation == Some(0) {
        return Err(parse_error(
            line,
            "previous primary node incarnation must be nonzero",
        ));
    }
    let previous_primary_endpoint = if fields[28] == "-" {
        None
    } else {
        Some(
            String::from_utf8(hex_decode(line, fields[28])?).map_err(|_| {
                parse_error(
                    line,
                    "previous primary endpoint must be valid UTF-8 after hex decoding",
                )
            })?,
        )
    };
    let previous_primary_lease_deadline_ms =
        parse_option_u64(line, fields[29], "previous primary lease deadline")?;
    let previous_primary_prefer_reactivation =
        parse_bool_u8(line, fields[30], "previous primary reactivation preference")?;
    let metadata_transfer_fence_epoch =
        parse_option_cluster_epoch(line, fields[31], "metadata transfer fence epoch")?;
    let previous_primary_lease = match (
        previous_primary_node_id,
        previous_primary_node_incarnation,
        previous_primary_endpoint,
        previous_primary_lease_deadline_ms,
        previous_primary_prefer_reactivation,
    ) {
        (
            Some(node_id),
            Some(node_incarnation),
            Some(endpoint),
            Some(lease_deadline_ms),
            prefer_reactivation,
        ) => Some(PreviousPrimaryLease {
            node_id,
            node_incarnation,
            endpoint,
            lease_deadline_ms,
            prefer_reactivation,
        }),
        (None, None, None, None, false) => None,
        _ => {
            return Err(parse_error(
                line,
                "previous primary lease fields must be all present or all absent, and absent leases cannot prefer reactivation",
            ));
        }
    };
    if acting_set.is_empty() {
        return Err(parse_error(line, "PG acting set must not be empty"));
    }
    match (
        state,
        active_primary,
        active_metadata_proof,
        peering_metadata_proof_floor,
        peering_metadata_transfer,
        metadata_transfer_fenced,
        active_metadata_transfer_imported,
    ) {
        (PgState::Active, None, _, _, _, _, _) => {
            return Err(parse_error(
                line,
                "active PG record requires active primary",
            ));
        }
        (PgState::Active, Some(_), None, _, _, _, _) => {
            return Err(parse_error(
                line,
                "active PG record requires active metadata proof",
            ));
        }
        (PgState::Active, Some(_), Some(_), Some(_), _, _, _) => {
            return Err(parse_error(
                line,
                "active PG record must not have peering metadata proof floor",
            ));
        }
        (PgState::Active, Some(_), Some(_), None, Some(_), _, _) => {
            return Err(parse_error(
                line,
                "active PG record must not have metadata transfer proof",
            ));
        }
        (PgState::Active, Some(_), Some(_), None, None, true, _) => {
            return Err(parse_error(
                line,
                "active PG record must not be metadata transfer fenced",
            ));
        }
        (PgState::Active, Some(primary), Some(_), None, None, false, _)
            if !acting_set.contains(&primary) =>
        {
            return Err(parse_error(line, "active PG primary must be in acting set"));
        }
        (PgState::Active, Some(_), Some(_), None, None, false, _) => {}
        (_, _, _, _, _, _, true) => {
            return Err(parse_error(
                line,
                "non-active PG record must not have active metadata transfer provenance",
            ));
        }
        (_, Some(_), _, _, _, _, _) => {
            return Err(parse_error(
                line,
                "non-active PG record must not have active primary",
            ));
        }
        (_, None, Some(_), _, _, _, _) => {
            return Err(parse_error(
                line,
                "non-active PG record must not have active metadata proof",
            ));
        }
        (PgState::Peering, None, None, Some(floor), Some(transfer), false, _)
            if !metadata_proof_satisfies_active_floor(floor, transfer.metadata_proof()) =>
        {
            return Err(parse_error(
                line,
                "metadata transfer proof must satisfy peering metadata proof floor",
            ));
        }
        (PgState::Peering, None, None, None, Some(_), _, _) => {
            return Err(parse_error(
                line,
                "metadata transfer proof requires peering metadata proof floor",
            ));
        }
        (PgState::Peering, None, None, _, Some(_), true, _) => {
            return Err(parse_error(
                line,
                "metadata transfer destination proof must not be source fenced",
            ));
        }
        (PgState::Peering, None, None, _, _, _, _) => {}
        (_, None, None, Some(_), _, _, _) => {
            return Err(parse_error(
                line,
                "non-peering PG record must not have peering metadata proof floor",
            ));
        }
        (_, None, None, None, Some(_), _, _) => {
            return Err(parse_error(
                line,
                "non-peering PG record must not have metadata transfer proof",
            ));
        }
        (_, None, None, None, None, true, _) => {
            return Err(parse_error(
                line,
                "non-peering PG record must not be metadata transfer fenced",
            ));
        }
        (_, None, None, None, None, false, _) => {}
    }
    if state == PgState::Active && previous_primary_lease.is_some() {
        return Err(parse_error(
            line,
            "active PG record must not retain a previous primary lease deadline",
        ));
    }
    if metadata_transfer_fence_source_lease_deadline_ms.is_some()
        && !(state == PgState::Peering && metadata_transfer_fenced)
    {
        return Err(parse_error(
            line,
            "metadata transfer fence source lease deadline requires a fenced peering PG",
        ));
    }
    if metadata_transfer_fence_source_imported
        && !(state == PgState::Peering && metadata_transfer_fenced)
    {
        return Err(parse_error(
            line,
            "metadata transfer fence source imported provenance requires a fenced peering PG",
        ));
    }
    if metadata_transfer_fenced != metadata_transfer_fence_epoch.is_some() {
        return Err(parse_error(
            line,
            "metadata transfer fence epoch must be present exactly for a fenced peering PG",
        ));
    }
    if active_metadata_proof_epoch.is_some() && active_metadata_proof.is_none() {
        return Err(parse_error(
            line,
            "active metadata proof epoch requires active metadata proof",
        ));
    }
    if active_metadata_proof_epoch.is_some() && state != PgState::Active {
        return Err(parse_error(
            line,
            "active metadata proof epoch requires an active PG",
        ));
    }
    if active_metadata_transfer_imported && active_metadata_proof_epoch.is_none() {
        return Err(parse_error(
            line,
            "active metadata transfer imported provenance requires an active metadata proof epoch",
        ));
    }
    if peering_metadata_proof_floor_epoch.is_some() && peering_metadata_proof_floor.is_none() {
        return Err(parse_error(
            line,
            "peering metadata proof floor epoch requires a peering metadata proof floor",
        ));
    }
    if peering_metadata_proof_floor_epoch.is_some() && state != PgState::Peering {
        return Err(parse_error(
            line,
            "peering metadata proof floor epoch requires a peering PG",
        ));
    }
    if peering_metadata_proof_floor_imported && peering_metadata_proof_floor_epoch.is_none() {
        return Err(parse_error(
            line,
            "peering metadata proof floor imported provenance requires a floor epoch",
        ));
    }
    if peering_metadata_transfer.is_some()
        && (peering_metadata_transfer_source_route_epoch.is_none()
            || peering_metadata_transfer_source_node_id.is_none())
        && fields.len() >= 23
    {
        return Err(parse_error(
            line,
            "metadata transfer source route fields require a complete transfer marker",
        ));
    }
    if peering_metadata_transfer.is_none()
        && (peering_metadata_transfer_source_route_epoch.is_some()
            || peering_metadata_transfer_source_node_id.is_some())
    {
        return Err(parse_error(
            line,
            "metadata transfer source route fields require a transfer marker",
        ));
    }
    let mut unique_nodes = BTreeSet::new();
    for node_id in &acting_set {
        if !unique_nodes.insert(*node_id) {
            return Err(parse_error(line, "PG acting set contains duplicate node"));
        }
    }
    Ok(PgControlRecord {
        pg_id,
        state,
        acting_set,
        active_primary,
        active_metadata_proof,
        active_metadata_proof_epoch,
        active_metadata_transfer_imported,
        previous_primary_lease,
        peering_metadata_proof_floor,
        peering_metadata_proof_floor_epoch,
        peering_metadata_proof_floor_imported,
        peering_metadata_transfer,
        peering_metadata_transfer_source_route_epoch,
        peering_metadata_transfer_source_node_id,
        metadata_transfer_fenced,
        metadata_transfer_fence_source_lease_deadline_ms,
        metadata_transfer_fence_source_imported,
        metadata_transfer_fence_epoch,
    })
}

fn option_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn option_u32(value: Option<u32>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn parse_lease_grant_horizon(
    line: usize,
    value: &str,
) -> Result<Option<CommittedLeaseGrantHorizon>, ControlPlaneError> {
    if value == "-" {
        return Ok(None);
    }
    let fields: Vec<&str> = value.split(',').collect();
    if fields.len() != 3 {
        return Err(parse_error(
            line,
            "lease grant horizon must contain clock generation, Raft term, and deadline",
        ));
    }
    let clock_generation = parse_u64(line, fields[0], "lease horizon clock generation")?;
    let raft_term = parse_option_u64(line, fields[1], "lease horizon Raft term")?;
    let authority = LeaseHorizonAuthorityBinding::checked_new(clock_generation, raft_term)
        .ok_or_else(|| {
            parse_error(
                line,
                "lease horizon clock generation and present Raft term must be nonzero",
            )
        })?;
    let grant_not_after_ms = parse_u64(line, fields[2], "lease horizon deadline")?;
    if grant_not_after_ms == 0 {
        return Err(parse_error(
            line,
            "lease grant horizon deadline must be nonzero",
        ));
    }
    Ok(Some(CommittedLeaseGrantHorizon::from_parts(
        authority,
        grant_not_after_ms,
    )))
}

fn parse_option_u64(
    line: usize,
    value: &str,
    field: &'static str,
) -> Result<Option<u64>, ControlPlaneError> {
    if value == "-" {
        Ok(None)
    } else {
        parse_u64(line, value, field).map(Some)
    }
}

fn parse_option_u32(
    line: usize,
    value: &str,
    field: &'static str,
) -> Result<Option<u32>, ControlPlaneError> {
    if value == "-" {
        Ok(None)
    } else {
        parse_u32(line, value, field).map(Some)
    }
}

fn parse_bool_u8(line: usize, value: &str, field: &'static str) -> Result<bool, ControlPlaneError> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(parse_error(line, &format!("{field} must be 0 or 1"))),
    }
}

fn parse_option_cluster_epoch(
    line: usize,
    value: &str,
    field: &'static str,
) -> Result<Option<ClusterEpoch>, ControlPlaneError> {
    parse_option_u64(line, value, field).and_then(|epoch| {
        epoch
            .map(|epoch| {
                ClusterEpoch::new(epoch)
                    .ok_or_else(|| parse_error(line, "cluster epoch must be nonzero"))
            })
            .transpose()
    })
}

fn validate_acting_set(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<(), ControlPlaneError> {
    if acting_set.is_empty() {
        return Err(ControlPlaneError::EmptyActingSet { pg_id: pg_id.get() });
    }
    let mut unique_nodes = BTreeSet::new();
    for node_id in acting_set {
        if !unique_nodes.insert(*node_id) {
            return Err(ControlPlaneError::DuplicateActingSetNode {
                pg_id: pg_id.get(),
                node_id: node_id.as_u32(),
            });
        }
        if !snapshot.nodes.contains_key(node_id) {
            return Err(ControlPlaneError::UnknownActingSetNode {
                pg_id: pg_id.get(),
                node_id: node_id.as_u32(),
            });
        }
    }
    Ok(())
}

fn validate_acting_set_change_ready(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<(), ControlPlaneError> {
    let Some(record) = snapshot.pg(pg_id) else {
        return Ok(());
    };
    if record.acting_set() == acting_set
        || matches!(record.state(), PgState::Active | PgState::Peering)
    {
        return Ok(());
    }
    Err(ControlPlaneError::PgActingSetChangeNotReady {
        pg_id: pg_id.get(),
        cluster_epoch: snapshot.cluster_epoch(),
        state: record.state(),
    })
}

fn validate_acting_set_preserves_pending_recovery(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<(), ControlPlaneError> {
    let Some(record) = snapshot.pg(pg_id) else {
        return Ok(());
    };
    let Some(recovery) = snapshot.pending_metadata_command_recovery_for_pg(record)? else {
        return Ok(());
    };
    if acting_set.contains(&recovery.reporting_node_id()) {
        return Ok(());
    }
    Err(ControlPlaneError::PgPeeringPendingMetadataCommand {
        pg_id: pg_id.get(),
        node_id: recovery.reporting_node_id().as_u32(),
        cluster_epoch: snapshot.cluster_epoch(),
        pending: recovery.pending(),
    })
}

fn validate_pg_heartbeat_observations(
    snapshot: &ClusterControlSnapshot,
    node_id: NodeId,
    observations: &[NodePgHeartbeatObservation],
) -> Result<Vec<NodePgHeartbeatObservation>, ControlPlaneError> {
    let mut observed_pgs = BTreeSet::new();
    let mut pending_active_pg_observations = Vec::new();
    for observation in observations {
        if !observed_pgs.insert(observation.pg_id) {
            return Err(ControlPlaneError::DuplicatePgObservation {
                node_id: node_id.as_u32(),
                pg_id: observation.pg_id.get(),
            });
        }
        let pg = snapshot
            .pgs
            .get(&observation.pg_id)
            .ok_or(ControlPlaneError::UnknownPg {
                pg_id: observation.pg_id.get(),
            })?;
        if !pg.acting_set.contains(&node_id) {
            return Err(ControlPlaneError::PgObservationNotInActingSet {
                node_id: node_id.as_u32(),
                pg_id: observation.pg_id.get(),
            });
        }
        if let Some(pending) = observation.pending_metadata_command {
            validate_pending_metadata_command_reporter(
                snapshot,
                observation.pg_id,
                node_id,
                pending,
            )?;
        }
        if pg.state == PgState::Active && observation.pending_metadata_command.is_some() {
            pending_active_pg_observations.push(*observation);
        }
        // A validated pending command already requires this PG to enter
        // Peering. Its proof may reflect partial or reissued work; retain the
        // committed floor and require replica convergence there.
        if pg.state == PgState::Active
            && pg.active_primary == Some(node_id)
            && observation.state == PgState::Active
            && observation.pending_metadata_command.is_none()
        {
            let expected = pg.active_metadata_proof.ok_or(
                ControlPlaneError::ActivePgMissingMetadataProof {
                    pg_id: observation.pg_id.get(),
                },
            )?;
            if !metadata_proof_satisfies_active_primary_observation_floor(
                expected,
                observation.metadata_proof,
                metadata_proof_progress_provenance(
                    pg.active_metadata_transfer_imported,
                    pg.active_metadata_proof_epoch,
                ),
                snapshot.cluster_epoch,
            ) {
                return Err(ControlPlaneError::PgActiveMetadataProofMismatch {
                    pg_id: observation.pg_id.get(),
                    node_id: node_id.as_u32(),
                    cluster_epoch: snapshot.cluster_epoch,
                    expected,
                    actual: observation.metadata_proof,
                });
            }
        }
    }
    Ok(pending_active_pg_observations)
}

fn validate_pending_metadata_command_reporter(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    node_id: NodeId,
    pending: PendingMetadataCommandObservation,
) -> Result<(), ControlPlaneError> {
    let historical = snapshot.reconstructed_pg_route_at_epoch(pg_id, pending.cluster_epoch())?;
    if historical.state() != PgState::Active || historical.primary_node_id() != node_id {
        return Err(
            ControlPlaneError::PgPeeringPendingMetadataCommandReporterNotHistoricalPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: snapshot.cluster_epoch,
                node_id: node_id.as_u32(),
                pending_epoch: pending.cluster_epoch(),
                historical_state: historical.state(),
                historical_primary_node_id: historical.primary_node_id().as_u32(),
            },
        );
    }
    Ok(())
}

fn validate_storage_cluster_map_history_floor(
    snapshot: &ClusterControlSnapshot,
    heartbeat: &NodeHeartbeat,
) -> Result<(), ControlPlaneError> {
    validate_storage_cluster_map_history_floor_at_epoch(
        snapshot,
        heartbeat,
        heartbeat.observed_epoch,
    )
}

fn validate_storage_cluster_map_history_floor_at_epoch(
    snapshot: &ClusterControlSnapshot,
    heartbeat: &NodeHeartbeat,
    max_floor_epoch: ClusterEpoch,
) -> Result<(), ControlPlaneError> {
    for reference in heartbeat.cluster_map_history_route_references.iter() {
        if reference.cluster_epoch() > max_floor_epoch {
            return Err(ControlPlaneError::StorageClusterMapHistoryRouteInFuture {
                node_id: heartbeat.node_id.as_u32(),
                route_epoch: reference.cluster_epoch(),
                pg_id: reference.pg_id().get(),
                validation_epoch: max_floor_epoch,
            });
        }
        let retained = if reference.cluster_epoch() == snapshot.cluster_epoch {
            snapshot.pg(reference.pg_id()).is_some()
        } else {
            snapshot
                .reconstructed_pg_route_at_epoch(reference.pg_id(), reference.cluster_epoch())
                .is_ok()
        };
        if !retained {
            return Err(
                ControlPlaneError::StorageClusterMapHistoryRouteNotRetained {
                    node_id: heartbeat.node_id.as_u32(),
                    route_epoch: reference.cluster_epoch(),
                    pg_id: reference.pg_id().get(),
                    cluster_epoch: snapshot.cluster_epoch,
                },
            );
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct ExpectedPgPeeringCompletion {
    active_metadata_proof: PgMetadataProof,
    active_metadata_proof_epoch: ClusterEpoch,
}

#[derive(Debug, Clone, Copy)]
struct PgPeeringCompletionValidation<'a> {
    snapshot: &'a ClusterControlSnapshot,
    pg_id: PgId,
    primary: NodeId,
    node_incarnation: u64,
    completed_at_ms: u64,
    expected: Option<ExpectedPgPeeringCompletion>,
}

#[derive(Debug, Clone, Copy)]
enum ValidatedPgPeeringCompletion {
    AlreadyActive,
    Complete {
        active_metadata_proof: PgMetadataProof,
        active_metadata_proof_epoch: ClusterEpoch,
    },
}

fn validate_pg_peering_completion(
    validation: PgPeeringCompletionValidation<'_>,
) -> Result<ValidatedPgPeeringCompletion, ControlPlaneError> {
    let PgPeeringCompletionValidation {
        snapshot,
        pg_id,
        primary,
        node_incarnation,
        completed_at_ms,
        expected,
    } = validation;
    let record = snapshot
        .pg(pg_id)
        .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
    if !record.acting_set.contains(&primary) {
        return Err(ControlPlaneError::PgPrimaryNotInActingSet {
            pg_id: pg_id.get(),
            node_id: primary.as_u32(),
        });
    }
    authorize_node_service_for_snapshot(
        snapshot,
        primary,
        node_incarnation,
        snapshot.cluster_epoch,
        completed_at_ms,
    )?;
    if record.state == PgState::Active {
        if record.active_primary == Some(primary) {
            return Ok(ValidatedPgPeeringCompletion::AlreadyActive);
        }
        return Err(ControlPlaneError::PgNotPeering {
            pg_id: pg_id.get(),
            cluster_epoch: snapshot.cluster_epoch,
            state: record.state,
        });
    }
    if peering_pg_primary_for_snapshot(snapshot, record, completed_at_ms) != Some(primary) {
        return Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
            pg_id: pg_id.get(),
            node_id: primary.as_u32(),
        });
    }
    if record.state != PgState::Peering {
        return Err(ControlPlaneError::PgNotPeering {
            pg_id: pg_id.get(),
            cluster_epoch: snapshot.cluster_epoch,
            state: record.state,
        });
    }
    if record.metadata_transfer_fenced {
        return Err(
            ControlPlaneError::PgMetadataTransferFenceRequiresTransferInstall {
                pg_id: pg_id.get(),
            },
        );
    }
    if let Some(previous) = &record.previous_primary_lease {
        let primary_endpoint = snapshot
            .node(primary)
            .expect("authorized peering primary must be a known node")
            .endpoint();
        if previous.blocks_activation(primary, node_incarnation, primary_endpoint, completed_at_ms)
        {
            return Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive {
                pg_id: pg_id.get(),
                previous_primary: previous.node_id.as_u32(),
                previous_primary_incarnation: previous.node_incarnation,
                previous_primary_endpoint: previous.endpoint.clone(),
                proposed_primary: primary.as_u32(),
                proposed_primary_incarnation: node_incarnation,
                proposed_primary_endpoint: primary_endpoint.to_owned(),
                completed_at_ms,
                lease_deadline_ms: previous.lease_deadline_ms,
            });
        }
    }
    if let Some(expected) = expected {
        if expected.active_metadata_proof_epoch != snapshot.cluster_epoch {
            return Err(ControlPlaneError::PgPeeringMetadataProofEpochMismatch {
                pg_id: pg_id.get(),
                expected: snapshot.cluster_epoch,
                actual: expected.active_metadata_proof_epoch,
            });
        }
    }
    let observed_metadata_proof =
        validate_pg_peering_observations(snapshot, pg_id, record.acting_set(), completed_at_ms)?;
    let active_metadata_proof = if let Some(expected) = expected {
        if observed_metadata_proof != expected.active_metadata_proof {
            return Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
                pg_id: pg_id.get(),
                node_id: primary.as_u32(),
                cluster_epoch: snapshot.cluster_epoch,
                expected: observed_metadata_proof,
                actual: expected.active_metadata_proof,
            });
        }
        expected.active_metadata_proof
    } else {
        observed_metadata_proof
    };
    validate_converged_peering_metadata_proof_floor(
        snapshot.cluster_epoch,
        pg_id,
        primary,
        record.peering_metadata_proof_floor_context(),
        record.peering_metadata_transfer,
        active_metadata_proof,
    )?;
    Ok(ValidatedPgPeeringCompletion::Complete {
        active_metadata_proof,
        active_metadata_proof_epoch: snapshot.cluster_epoch,
    })
}

fn validate_converged_peering_metadata_proof_floor(
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    node_id: NodeId,
    floor: Option<PeeringMetadataProofFloor>,
    transfer: Option<PgMetadataTransferProof>,
    actual: PgMetadataProof,
) -> Result<(), ControlPlaneError> {
    if converged_peering_proof_preserves_local_state(cluster_epoch, floor, transfer, actual) {
        return Ok(());
    }
    validate_peering_metadata_proof_floor(cluster_epoch, pg_id, node_id, floor, transfer, actual)
}

fn converged_peering_proof_preserves_local_state(
    cluster_epoch: ClusterEpoch,
    floor: Option<PeeringMetadataProofFloor>,
    transfer: Option<PgMetadataTransferProof>,
    actual: PgMetadataProof,
) -> bool {
    let Some(floor) = floor else {
        return false;
    };
    // Peering observation validation has already established exact agreement
    // across every healthy replica. A later local epoch may restart its log and
    // execute commands whose net metadata effect is zero; the unchanged state
    // digest is then the durable floor, while the new nonzero hash proves this
    // is an epoch-local log rather than a replay of the old proof.
    transfer.is_none()
        && !floor.imported
        && floor.epoch.is_some_and(|epoch| epoch < cluster_epoch)
        && actual != floor.proof
        && actual.applied_log_hash != 0
        && actual.applied_log_hash != floor.proof.applied_log_hash
        && actual.state_digest == floor.proof.state_digest
}

fn validate_pg_peering_observations(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    acting_set: &[NodeId],
    now_ms: u64,
) -> Result<PgMetadataProof, ControlPlaneError> {
    let mut expected_proof = None;
    for node_id in acting_set {
        let Some(node) = snapshot.nodes.get(node_id) else {
            return Err(ControlPlaneError::UnknownActingSetNode {
                pg_id: pg_id.get(),
                node_id: node_id.as_u32(),
            });
        };
        if !node.membership.can_serve_primary()
            || node.availability() != NodeAvailabilityState::Healthy
            || node
                .lease_deadline_ms
                .is_none_or(|lease_deadline_ms| lease_deadline_ms <= now_ms)
        {
            continue;
        }
        let observation =
            node.pg_observation(pg_id)
                .ok_or(ControlPlaneError::PgPeeringMissingObservation {
                    pg_id: pg_id.get(),
                    node_id: node_id.as_u32(),
                    cluster_epoch: snapshot.cluster_epoch,
                })?;
        if observation.observed_epoch != snapshot.cluster_epoch {
            return Err(ControlPlaneError::StaleNodeObservedEpoch {
                node_id: node_id.as_u32(),
                observed_epoch: observation.observed_epoch,
                current_epoch: snapshot.cluster_epoch,
            });
        }
        if observation.state != PgState::Peering {
            return Err(ControlPlaneError::PgPeeringObservationNotPeering {
                pg_id: pg_id.get(),
                node_id: node_id.as_u32(),
                cluster_epoch: snapshot.cluster_epoch,
                state: observation.state,
            });
        }
        if let Some(pending) = observation.pending_metadata_command() {
            return Err(ControlPlaneError::PgPeeringPendingMetadataCommand {
                pg_id: pg_id.get(),
                node_id: node_id.as_u32(),
                cluster_epoch: snapshot.cluster_epoch,
                pending,
            });
        }
        match expected_proof {
            Some(expected) if observation.metadata_proof != expected => {
                return Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
                    pg_id: pg_id.get(),
                    node_id: node_id.as_u32(),
                    cluster_epoch: snapshot.cluster_epoch,
                    expected,
                    actual: observation.metadata_proof,
                });
            }
            Some(_) => {}
            None => expected_proof = Some(observation.metadata_proof),
        }
    }
    expected_proof.ok_or(ControlPlaneError::PgPeeringMissingObservation {
        pg_id: pg_id.get(),
        node_id: 0,
        cluster_epoch: snapshot.cluster_epoch,
    })
}

fn metadata_proof_satisfies_active_floor(
    active_floor: PgMetadataProof,
    observed: PgMetadataProof,
) -> bool {
    observed.applied_log_index > active_floor.applied_log_index || observed == active_floor
}

#[cfg(test)]
fn metadata_proof_satisfies_active_observation_floor(
    active_floor: PgMetadataProof,
    observed: PgMetadataProof,
) -> bool {
    metadata_proof_satisfies_active_floor(active_floor, observed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataProofProgressKind {
    LocalEpoch,
    ImportedTransfer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetadataProofProgressProvenance {
    floor_epoch: ClusterEpoch,
    kind: MetadataProofProgressKind,
}

fn metadata_proof_progress_provenance(
    imported_transfer: bool,
    floor_epoch: Option<ClusterEpoch>,
) -> Option<MetadataProofProgressProvenance> {
    floor_epoch.map(|floor_epoch| MetadataProofProgressProvenance {
        floor_epoch,
        kind: if imported_transfer {
            MetadataProofProgressKind::ImportedTransfer
        } else {
            MetadataProofProgressKind::LocalEpoch
        },
    })
}

fn metadata_proof_satisfies_active_primary_observation_floor(
    active_floor: PgMetadataProof,
    observed: PgMetadataProof,
    progress_provenance: Option<MetadataProofProgressProvenance>,
    observed_epoch: ClusterEpoch,
) -> bool {
    metadata_proof_satisfies_active_primary_observation_floor_impl(
        active_floor,
        observed,
        progress_provenance,
        observed_epoch,
    )
}

fn metadata_proof_satisfies_peering_proof_floor(
    active_floor: PgMetadataProof,
    observed: PgMetadataProof,
    progress_provenance: Option<MetadataProofProgressProvenance>,
    observed_epoch: ClusterEpoch,
) -> bool {
    metadata_proof_satisfies_active_primary_observation_floor_impl(
        active_floor,
        observed,
        progress_provenance,
        observed_epoch,
    )
}

fn metadata_proof_satisfies_active_primary_observation_floor_impl(
    active_floor: PgMetadataProof,
    observed: PgMetadataProof,
    progress_provenance: Option<MetadataProofProgressProvenance>,
    observed_epoch: ClusterEpoch,
) -> bool {
    if metadata_proof_satisfies_active_floor(active_floor, observed) {
        return true;
    }
    if let Some(progress_provenance) = progress_provenance {
        progress_provenance.floor_epoch < observed_epoch
            && observed != active_floor
            && observed.applied_log_hash != 0
            && observed.applied_log_hash != active_floor.applied_log_hash
            && observed.state_digest != active_floor.state_digest
            && match progress_provenance.kind {
                MetadataProofProgressKind::LocalEpoch => true,
                MetadataProofProgressKind::ImportedTransfer => {
                    metadata_proof_satisfies_imported_transfer_local_progress_floor(
                        active_floor,
                        observed,
                    )
                }
            }
    } else {
        false
    }
}

fn metadata_proof_satisfies_imported_transfer_local_progress_floor(
    active_floor: PgMetadataProof,
    observed: PgMetadataProof,
) -> bool {
    metadata_proof_satisfies_active_floor(active_floor, observed)
        // A metadata-transfer activation floor can be imported from a previous
        // epoch. Once that PG becomes active, later local commands are recorded
        // in the destination epoch and the resulting log tuple is not ordered
        // against the imported source-epoch proof. Keep this relaxation scoped
        // to imported active primaries and deliberately fenced transfer sources;
        // ordinary migration source selection uses the strict active floor
        // above.
        || (observed != active_floor
            && observed.applied_log_hash != 0
            && observed.applied_log_hash != active_floor.applied_log_hash
            && observed.state_digest != active_floor.state_digest)
}

fn metadata_proof_satisfies_fenced_transfer_floor(
    floor: PgMetadataProof,
    observed: PgMetadataProof,
    floor_epoch: Option<ClusterEpoch>,
    floor_imported: bool,
    observed_epoch: ClusterEpoch,
    fence_epoch: ClusterEpoch,
) -> bool {
    if metadata_proof_satisfies_active_floor(floor, observed) {
        return true;
    }
    if observed_epoch >= fence_epoch {
        return false;
    }
    floor_epoch.is_some_and(|floor_epoch| {
        metadata_proof_satisfies_peering_proof_floor(
            floor,
            observed,
            Some(MetadataProofProgressProvenance {
                floor_epoch,
                kind: if floor_imported {
                    MetadataProofProgressKind::ImportedTransfer
                } else {
                    MetadataProofProgressKind::LocalEpoch
                },
            }),
            observed_epoch,
        )
    })
}

struct MetadataTransferProofValidation<'a> {
    snapshot: &'a ClusterControlSnapshot,
    pg_id: PgId,
    state: PgState,
    metadata_transfer_fenced: bool,
    metadata_transfer_fence_source_imported: bool,
    metadata_transfer_fence_epoch: Option<ClusterEpoch>,
    required_floor: PgMetadataProof,
    required_floor_epoch: Option<ClusterEpoch>,
    transfer: PgMetadataTransferProof,
}

fn validate_metadata_transfer_proof(
    validation: MetadataTransferProofValidation<'_>,
) -> Result<(), ControlPlaneError> {
    let MetadataTransferProofValidation {
        snapshot,
        pg_id,
        state,
        metadata_transfer_fenced,
        metadata_transfer_fence_source_imported,
        metadata_transfer_fence_epoch,
        required_floor,
        required_floor_epoch,
        transfer,
    } = validation;
    if transfer.source_epoch() > snapshot.cluster_epoch {
        return Err(ControlPlaneError::PgMetadataTransferSourceEpochInFuture {
            pg_id: pg_id.get(),
            source_epoch: transfer.source_epoch(),
            cluster_epoch: snapshot.cluster_epoch,
        });
    }
    if transfer.source_epoch() < snapshot.cluster_epoch
        && !(state == PgState::Peering && metadata_transfer_fenced)
    {
        return Err(ControlPlaneError::PgMetadataTransferSourceEpochStale {
            pg_id: pg_id.get(),
            source_epoch: transfer.source_epoch(),
            cluster_epoch: snapshot.cluster_epoch,
        });
    }
    let satisfies_floor = match state {
        PgState::Active => {
            metadata_proof_satisfies_active_floor(required_floor, transfer.source_metadata_proof())
        }
        PgState::Peering if metadata_transfer_fenced => {
            let fence_epoch =
                metadata_transfer_fence_epoch.ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: format!(
                        "metadata transfer fenced PG {} has no committed fence epoch",
                        pg_id.get()
                    ),
                })?;
            metadata_proof_satisfies_fenced_transfer_floor(
                required_floor,
                transfer.source_metadata_proof(),
                required_floor_epoch,
                metadata_transfer_fence_source_imported,
                transfer.source_epoch(),
                fence_epoch,
            )
        }
        PgState::Peering => {
            metadata_proof_satisfies_active_floor(required_floor, transfer.source_metadata_proof())
        }
        _ => false,
    };
    if !satisfies_floor {
        return Err(ControlPlaneError::PgMetadataTransferProofBelowFloor {
            pg_id: pg_id.get(),
            expected: required_floor,
            actual: transfer.source_metadata_proof(),
        });
    }
    Ok(())
}

fn validate_authoritative_metadata_migration_source(
    snapshot: &ClusterControlSnapshot,
    record: &PgControlRecord,
    new_acting_set: &[NodeId],
) -> Result<PgMetadataProof, ControlPlaneError> {
    let active_floor =
        record
            .active_metadata_proof
            .ok_or(ControlPlaneError::ActivePgMissingMetadataProof {
                pg_id: record.pg_id.get(),
            })?;
    let mut source_floor: Option<PgMetadataProof> = None;
    let mut source_awaiting_current_epoch = false;
    for node_id in record
        .acting_set
        .iter()
        .copied()
        .filter(|node_id| new_acting_set.contains(node_id))
    {
        let Some(node) = snapshot.nodes.get(&node_id) else {
            continue;
        };
        let Some(observation) = node.pg_observation(record.pg_id) else {
            source_awaiting_current_epoch = true;
            continue;
        };
        if observation.observed_epoch != snapshot.cluster_epoch {
            source_awaiting_current_epoch = true;
            continue;
        }
        let proof_satisfies_floor = if record.active_primary == Some(node_id) {
            metadata_proof_satisfies_active_primary_observation_floor(
                active_floor,
                observation.metadata_proof,
                metadata_proof_progress_provenance(
                    record.active_metadata_transfer_imported,
                    record.active_metadata_proof_epoch,
                ),
                observation.observed_epoch,
            )
        } else {
            metadata_proof_satisfies_active_floor(active_floor, observation.metadata_proof)
        };
        if observation.state != PgState::Active
            || observation.has_pending_metadata_command()
            || !proof_satisfies_floor
        {
            continue;
        }
        if record.active_primary == Some(node_id) {
            return Ok(observation.metadata_proof);
        }
        source_floor = Some(match source_floor {
            Some(current)
                if current.applied_log_index >= observation.metadata_proof.applied_log_index =>
            {
                current
            }
            _ => observation.metadata_proof,
        });
    }
    source_floor.ok_or_else(|| {
        if source_awaiting_current_epoch {
            ControlPlaneError::PgMetadataMigrationSourceNotReady {
                pg_id: record.pg_id.get(),
                cluster_epoch: snapshot.cluster_epoch,
            }
        } else {
            ControlPlaneError::PgMetadataMigrationRequiresTransfer {
                pg_id: record.pg_id.get(),
            }
        }
    })
}

fn validate_peering_metadata_migration_source(
    snapshot: &ClusterControlSnapshot,
    record: &PgControlRecord,
    new_acting_set: &[NodeId],
    floor: PgMetadataProof,
) -> Result<(), ControlPlaneError> {
    for node_id in record
        .acting_set
        .iter()
        .copied()
        .filter(|node_id| new_acting_set.contains(node_id))
    {
        let Some(node) = snapshot.nodes.get(&node_id) else {
            continue;
        };
        let Some(observation) = node.pg_observation(record.pg_id) else {
            continue;
        };
        if observation.observed_epoch == snapshot.cluster_epoch
            && observation.state == PgState::Peering
            && !observation.has_pending_metadata_command()
            && metadata_proof_satisfies_active_floor(floor, observation.metadata_proof)
        {
            return Ok(());
        }
    }
    Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer {
        pg_id: record.pg_id.get(),
    })
}

fn validate_peering_metadata_proof_floor(
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    node_id: NodeId,
    floor: Option<PeeringMetadataProofFloor>,
    transfer: Option<PgMetadataTransferProof>,
    actual: PgMetadataProof,
) -> Result<(), ControlPlaneError> {
    if let Some(expected_transfer) = transfer.map(PgMetadataTransferProof::metadata_proof) {
        if actual == expected_transfer {
            return Ok(());
        }
        return Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: pg_id.get(),
            node_id: node_id.as_u32(),
            cluster_epoch,
            expected: expected_transfer,
            actual,
        });
    }
    let Some(floor) = floor else {
        return Ok(());
    };
    let expected = floor.proof;
    if metadata_proof_satisfies_active_floor(expected, actual)
        || floor.epoch.is_some_and(|floor_epoch| {
            metadata_proof_satisfies_peering_proof_floor(
                expected,
                actual,
                Some(MetadataProofProgressProvenance {
                    floor_epoch,
                    kind: if floor.imported {
                        MetadataProofProgressKind::ImportedTransfer
                    } else {
                        MetadataProofProgressKind::LocalEpoch
                    },
                }),
                cluster_epoch,
            )
        })
    {
        Ok(())
    } else {
        Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: pg_id.get(),
            node_id: node_id.as_u32(),
            cluster_epoch,
            expected,
            actual,
        })
    }
}

fn primary_has_current_pg_state(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    primary: NodeId,
    expected_state: PgState,
) -> bool {
    let expected_active_proof = snapshot.pgs.get(&pg_id).and_then(|pg| {
        pg.active_metadata_proof.map(|proof| {
            (
                proof,
                pg.active_metadata_transfer_imported,
                pg.active_metadata_proof_epoch,
            )
        })
    });
    snapshot
        .nodes
        .get(&primary)
        .and_then(|node| node.pg_observation(pg_id))
        .is_some_and(|observation| {
            observation.observed_epoch == snapshot.cluster_epoch
                && observation.state == expected_state
                && !observation.has_pending_metadata_command()
                && (expected_state != PgState::Active
                    || expected_active_proof.is_some_and(|(expected, imported, expected_epoch)| {
                        metadata_proof_satisfies_active_primary_observation_floor(
                            expected,
                            observation.metadata_proof,
                            metadata_proof_progress_provenance(imported, expected_epoch),
                            observation.observed_epoch,
                        )
                    }))
        })
}

fn validate_pg_primary_active_observation(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    primary: NodeId,
) -> Result<(), ControlPlaneError> {
    let pg = snapshot
        .pgs
        .get(&pg_id)
        .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
    let expected_proof = pg
        .active_metadata_proof
        .ok_or(ControlPlaneError::ActivePgMissingMetadataProof { pg_id: pg_id.get() })?;
    let Some(node) = snapshot.nodes.get(&primary) else {
        return Err(ControlPlaneError::UnknownNode {
            node_id: primary.as_u32(),
        });
    };
    let observation =
        node.pg_observation(pg_id)
            .ok_or(ControlPlaneError::PgPrimaryMissingActiveObservation {
                pg_id: pg_id.get(),
                node_id: primary.as_u32(),
                cluster_epoch: snapshot.cluster_epoch,
            })?;
    if observation.observed_epoch != snapshot.cluster_epoch {
        return Err(ControlPlaneError::StaleNodeObservedEpoch {
            node_id: primary.as_u32(),
            observed_epoch: observation.observed_epoch,
            current_epoch: snapshot.cluster_epoch,
        });
    }
    if observation.state != PgState::Active {
        return Err(ControlPlaneError::PgPrimaryObservationNotActive {
            pg_id: pg_id.get(),
            node_id: primary.as_u32(),
            cluster_epoch: snapshot.cluster_epoch,
            state: observation.state,
        });
    }
    if let Some(pending) = observation.pending_metadata_command() {
        return Err(ControlPlaneError::PgPeeringPendingMetadataCommand {
            pg_id: pg_id.get(),
            node_id: primary.as_u32(),
            cluster_epoch: snapshot.cluster_epoch,
            pending,
        });
    }
    if !metadata_proof_satisfies_active_primary_observation_floor(
        expected_proof,
        observation.metadata_proof,
        metadata_proof_progress_provenance(
            pg.active_metadata_transfer_imported,
            pg.active_metadata_proof_epoch,
        ),
        observation.observed_epoch,
    ) {
        return Err(ControlPlaneError::PgActiveMetadataProofMismatch {
            pg_id: pg_id.get(),
            node_id: primary.as_u32(),
            cluster_epoch: snapshot.cluster_epoch,
            expected: expected_proof,
            actual: observation.metadata_proof,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClusterMapHistoryProtection {
    exact_routes: BTreeSet<(ClusterEpoch, PgId)>,
}

fn required_cluster_map_history_protection<'a, 'b>(
    pgs: impl IntoIterator<Item = &'a PgControlRecord>,
    nodes: impl IntoIterator<Item = &'b NodeControlRecord>,
) -> ClusterMapHistoryProtection {
    let mut exact_routes = BTreeSet::new();
    for pg in pgs {
        if let Some(source_epoch) = pg.peering_metadata_transfer_source_route_epoch() {
            exact_routes.insert((source_epoch, pg.pg_id()));
        }
        if let Ok(Some(destination_epoch)) = peering_metadata_transfer_destination_epoch(pg) {
            exact_routes.insert((destination_epoch, pg.pg_id()));
        }
    }
    for node in nodes {
        exact_routes.extend(
            node.cluster_map_history_route_references()
                .iter()
                .map(|reference| (reference.cluster_epoch(), reference.pg_id())),
        );
    }
    ClusterMapHistoryProtection { exact_routes }
}

fn prune_cluster_map_history(
    history: &mut Vec<ClusterMapHistoryRecord>,
    protection: &ClusterMapHistoryProtection,
    current_epoch: ClusterEpoch,
) {
    history.sort_by_key(ClusterMapHistoryRecord::cluster_epoch);
    let ordinary_history_floor = current_epoch
        .get()
        .saturating_sub(CLUSTER_MAP_HISTORY_LIMIT as u64);
    // Exact old routes extend retention; they must not displace the recent
    // window before a new payload's route reference reaches a heartbeat.
    prune_unreachable_old_history_routes(history, protection, ordinary_history_floor);
}

fn prune_unreachable_old_history_routes(
    history: &mut Vec<ClusterMapHistoryRecord>,
    protection: &ClusterMapHistoryProtection,
    ordinary_history_floor: u64,
) {
    let protected_epochs: BTreeSet<_> = protection
        .exact_routes
        .iter()
        .map(|(epoch, _)| *epoch)
        .chain(protected_history_absence_epochs(history, protection))
        .collect();
    let mut roots = protection.exact_routes.clone();
    roots.extend(
        history
            .iter()
            .filter(|record| record.cluster_epoch().get() >= ordinary_history_floor)
            .flat_map(|record| {
                let epoch = record.cluster_epoch();
                record.pgs().iter().map(move |pg| (epoch, pg.pg_id()))
            }),
    );
    let reachable_routes = cluster_map_history_route_dependency_closure(history, roots);
    history.retain_mut(|record| {
        if record.cluster_epoch().get() >= ordinary_history_floor {
            return true;
        }
        let record_epoch = record.cluster_epoch();
        record
            .pgs
            .retain(|pg| reachable_routes.contains(&(record_epoch, pg.pg_id)));
        !record.pgs.is_empty() || protected_epochs.contains(&record_epoch)
    });
}

fn cluster_map_history_route_dependency_closure(
    history: &[ClusterMapHistoryRecord],
    roots: BTreeSet<(ClusterEpoch, PgId)>,
) -> BTreeSet<(ClusterEpoch, PgId)> {
    let transfer_sources: BTreeMap<_, _> = history
        .iter()
        .flat_map(|record| {
            let epoch = record.cluster_epoch();
            record.pgs().iter().filter_map(move |pg| {
                pg.peering_metadata_transfer_source_route_epoch
                    .map(|source_epoch| ((epoch, pg.pg_id()), (source_epoch, pg.pg_id())))
            })
        })
        .collect();
    let mut reachable: BTreeSet<_> = roots
        .into_iter()
        .filter_map(|route| resolve_cluster_map_history_route_key(history, route))
        .collect();
    let mut pending: Vec<_> = reachable.iter().copied().collect();
    while let Some(route) = pending.pop() {
        let Some(source) = transfer_sources
            .get(&route)
            .copied()
            .and_then(|source| resolve_cluster_map_history_route_key(history, source))
        else {
            continue;
        };
        if reachable.insert(source) {
            pending.push(source);
        }
    }
    reachable
}

fn resolve_cluster_map_history_route_key(
    history: &[ClusterMapHistoryRecord],
    (cluster_epoch, pg_id): (ClusterEpoch, PgId),
) -> Option<(ClusterEpoch, PgId)> {
    for record in history
        .iter()
        .filter(|record| record.cluster_epoch() >= cluster_epoch)
    {
        if record.absent_pgs.contains(&pg_id) {
            return None;
        }
        if record.pg(pg_id).is_some() {
            return Some((record.cluster_epoch(), pg_id));
        }
    }
    None
}

fn protected_history_absence_epochs<'a>(
    history: &'a [ClusterMapHistoryRecord],
    protection: &'a ClusterMapHistoryProtection,
) -> impl Iterator<Item = ClusterEpoch> + 'a {
    let earliest_protected_epoch = protection
        .exact_routes
        .iter()
        .map(|(epoch, _)| *epoch)
        .min();
    history.iter().filter_map(move |record| {
        earliest_protected_epoch
            .is_some_and(|epoch| record.cluster_epoch() >= epoch && !record.absent_pgs.is_empty())
            .then_some(record.cluster_epoch())
    })
}

fn active_primary_lease(
    snapshot: &ClusterControlSnapshot,
    record: &PgControlRecord,
) -> Option<PreviousPrimaryLease> {
    let node_id = (record.state == PgState::Active)
        .then_some(record.active_primary)
        .flatten()?;
    let node = snapshot.node(node_id)?;
    Some(PreviousPrimaryLease {
        node_id,
        node_incarnation: node.node_incarnation(),
        endpoint: node.endpoint().to_owned(),
        lease_deadline_ms: node.lease_deadline_ms()?,
        prefer_reactivation: true,
    })
}

fn mark_pgs_peering_for_nodes(
    snapshot: &mut ClusterControlSnapshot,
    previous: &ClusterControlSnapshot,
    nodes: impl IntoIterator<Item = NodeId>,
) -> Vec<PgId> {
    let affected_nodes: BTreeSet<NodeId> = nodes.into_iter().collect();
    let affected_pgs: Vec<PgId> = snapshot
        .pgs
        .values()
        .filter(|record| {
            record
                .acting_set
                .iter()
                .any(|node_id| affected_nodes.contains(node_id))
        })
        .map(|record| record.pg_id)
        .collect();
    mark_pgs_peering_for_pg_ids(snapshot, previous, affected_pgs)
}

fn mark_pgs_peering_for_pg_ids(
    snapshot: &mut ClusterControlSnapshot,
    previous: &ClusterControlSnapshot,
    pg_ids: impl IntoIterator<Item = PgId>,
) -> Vec<PgId> {
    let affected_pgs: BTreeSet<PgId> = pg_ids.into_iter().collect();
    let mut peering_pgs = Vec::new();
    for record in snapshot.pgs.values_mut() {
        if record.state != PgState::Peering && affected_pgs.contains(&record.pg_id) {
            let previous_primary_lease = previous
                .pg(record.pg_id)
                .and_then(|previous_record| active_primary_lease(previous, previous_record))
                .or_else(|| record.previous_primary_lease.clone());
            let peering_metadata_proof_floor_epoch = if record.state == PgState::Active {
                record.active_metadata_proof_epoch
            } else {
                None
            };
            let peering_metadata_proof_floor_imported =
                record.state == PgState::Active && record.active_metadata_transfer_imported;
            record.peering_metadata_proof_floor = if record.state == PgState::Active {
                record.active_metadata_proof
            } else {
                None
            };
            record.peering_metadata_proof_floor_epoch = peering_metadata_proof_floor_epoch;
            record.peering_metadata_proof_floor_imported = peering_metadata_proof_floor_imported;
            record.state = PgState::Peering;
            record.active_primary = None;
            record.active_metadata_proof = None;
            record.active_metadata_proof_epoch = None;
            record.active_metadata_transfer_imported = false;
            record.previous_primary_lease = previous_primary_lease;
            record.peering_metadata_transfer = None;
            record.peering_metadata_transfer_source_route_epoch = None;
            record.peering_metadata_transfer_source_node_id = None;
            record.metadata_transfer_fenced = false;
            record.metadata_transfer_fence_source_lease_deadline_ms = None;
            record.metadata_transfer_fence_source_imported = false;
            record.metadata_transfer_fence_epoch = None;
            peering_pgs.push(record.pg_id);
        }
    }
    peering_pgs
}

fn pg_state_as_str(state: PgState) -> &'static str {
    match state {
        PgState::Active => "active",
        PgState::Peering => "peering",
        PgState::Degraded => "degraded",
        PgState::Backfilling => "backfilling",
        PgState::Inconsistent => "inconsistent",
    }
}

fn pg_state_from_str(value: &str) -> Result<PgState, ControlPlaneError> {
    match value {
        "active" => Ok(PgState::Active),
        "peering" => Ok(PgState::Peering),
        "degraded" => Ok(PgState::Degraded),
        "backfilling" => Ok(PgState::Backfilling),
        "inconsistent" => Ok(PgState::Inconsistent),
        _ => Err(ControlPlaneError::InvalidState {
            field: "pg",
            value: value.to_owned(),
        }),
    }
}

fn format_node_list(nodes: &[NodeId]) -> String {
    nodes
        .iter()
        .map(|node_id| node_id.as_u32().to_string())
        .collect::<Vec<_>>()
        .join(":")
}

fn parse_node_list(line: usize, value: &str) -> Result<Vec<NodeId>, ControlPlaneError> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(':')
        .map(|value| parse_u32(line, value, "node id").map(NodeId::new))
        .collect()
}

fn parse_u32(line: usize, value: &str, field: &'static str) -> Result<u32, ControlPlaneError> {
    value
        .parse::<u32>()
        .map_err(|source| parse_error(line, &format!("invalid {field} {value:?}: {source}")))
}

fn parse_u64(line: usize, value: &str, field: &'static str) -> Result<u64, ControlPlaneError> {
    value
        .parse::<u64>()
        .map_err(|source| parse_error(line, &format!("invalid {field} {value:?}: {source}")))
}

fn parse_error(line: usize, message: &str) -> ControlPlaneError {
    ControlPlaneError::Parse {
        line,
        message: message.to_owned(),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(line: usize, value: &str) -> Result<Vec<u8>, ControlPlaneError> {
    if !value.len().is_multiple_of(2) {
        return Err(parse_error(line, "hex string has odd length"));
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0]).ok_or_else(|| parse_error(line, "invalid hex digit"))?;
        let low = hex_nibble(pair[1]).ok_or_else(|| parse_error(line, "invalid hex digit"))?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn state_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn create_control_plane_directory_all_durable_with(
    path: &Path,
    mut sync_parent: impl FnMut(&Path) -> Result<(), ControlPlaneError>,
) -> Result<(), ControlPlaneError> {
    let mut missing = Vec::new();
    let mut candidate = path;
    loop {
        match std::fs::metadata(candidate) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(ControlPlaneError::io(
                    "inspect control-plane state directory",
                    std::io::Error::new(
                        ErrorKind::NotADirectory,
                        format!(
                            "control-plane state directory component {} is not a directory",
                            candidate.display()
                        ),
                    ),
                ));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                missing.push(candidate.to_path_buf());
            }
            Err(source) => {
                return Err(ControlPlaneError::io(
                    "inspect control-plane state directory",
                    source,
                ));
            }
        }
        let parent = state_parent(candidate);
        if parent == candidate {
            return Err(ControlPlaneError::io(
                "inspect control-plane state directory",
                std::io::Error::new(
                    ErrorKind::NotFound,
                    format!(
                        "control-plane state directory {} has no existing ancestor",
                        path.display()
                    ),
                ),
            ));
        }
        candidate = parent;
    }

    // An existing component may be residue from a previous creator whose
    // parent sync failed. Confirm its directory entry before trusting it as
    // the durable boundary for any descendants.
    sync_parent(state_parent(candidate))?;

    for directory in missing.into_iter().rev() {
        match std::fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let metadata = std::fs::metadata(&directory).map_err(|source| {
                    ControlPlaneError::io(
                        "inspect concurrently created control-plane state directory",
                        source,
                    )
                })?;
                if !metadata.is_dir() {
                    return Err(ControlPlaneError::io(
                        "create control-plane state directory",
                        std::io::Error::new(
                            ErrorKind::NotADirectory,
                            format!(
                                "control-plane state directory component {} is not a directory",
                                directory.display()
                            ),
                        ),
                    ));
                }
            }
            Err(source) => {
                return Err(ControlPlaneError::io(
                    "create control-plane state directory",
                    source,
                ));
            }
        }
        sync_parent(state_parent(&directory))?;
    }
    Ok(())
}

fn create_control_plane_directory_all_durable(path: &Path) -> Result<(), ControlPlaneError> {
    create_control_plane_directory_all_durable_with(path, |parent| {
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| {
                ControlPlaneError::io("sync control-plane state directory parent", source)
            })
    })
}

/// Durably creates every missing parent-directory component for a control-plane state file.
pub fn ensure_control_plane_state_parent_directory(
    state_path: &Path,
) -> Result<(), ControlPlaneError> {
    create_control_plane_directory_all_durable(state_parent(state_path))
}

#[cfg(test)]
#[path = "control_plane/tests.rs"]
mod tests;
