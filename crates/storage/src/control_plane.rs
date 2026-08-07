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

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneAuthorityClockCheckpointBinding(
    [u8; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN],
);

impl std::fmt::Debug for ControlPlaneAuthorityClockCheckpointBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneAuthorityClockCheckpointBinding")
            .finish_non_exhaustive()
    }
}

impl ControlPlaneAuthorityClockCheckpointBinding {
    #[must_use]
    pub fn for_raft(cluster_name: &str, node_id: u64) -> Self {
        let mut context = checksum::sha256::Sha256::new();
        context.update(b"argmin-control-plane-clock-checkpoint-raft-v1\0");
        context.update(&(cluster_name.len() as u64).to_be_bytes());
        context.update(cluster_name.as_bytes());
        context.update(&node_id.to_be_bytes());
        Self(context.finalize())
    }

    fn generate_single_authority() -> Result<Self, ControlPlaneError> {
        let mut binding = [0u8; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN];
        argmin_crypto::random::fill(&mut binding).map_err(|_| {
            ControlPlaneError::io(
                "generate single-authority control-plane durable identity",
                std::io::Error::other(
                    "secure random source failed while generating control-plane identity",
                ),
            )
        })?;
        Ok(Self(binding))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneAuthorityClockRestartCheckpoint {
    binding: ControlPlaneAuthorityClockCheckpointBinding,
    authority_generation: u64,
    committed_timestamp_high_water_ms: Option<u64>,
    wall_time_ms: u64,
    health_time_ms: u64,
}

impl ControlPlaneAuthorityClockRestartCheckpoint {
    #[must_use]
    pub fn new(
        binding: ControlPlaneAuthorityClockCheckpointBinding,
        authority_generation: u64,
        committed_timestamp_high_water_ms: Option<u64>,
        wall_time_ms: u64,
        health_time_ms: u64,
    ) -> Self {
        Self {
            binding,
            authority_generation,
            committed_timestamp_high_water_ms,
            wall_time_ms,
            health_time_ms,
        }
    }

    #[must_use]
    pub fn authority_generation(self) -> u64 {
        self.authority_generation
    }

    #[must_use]
    pub fn committed_timestamp_high_water_ms(self) -> Option<u64> {
        self.committed_timestamp_high_water_ms
    }

    #[must_use]
    pub fn wall_time_ms(self) -> u64 {
        self.wall_time_ms
    }

    #[must_use]
    pub fn health_time_ms(self) -> u64 {
        self.health_time_ms
    }

    fn from_process_clock(
        binding: ControlPlaneAuthorityClockCheckpointBinding,
        authority_generation: u64,
        committed_timestamp_high_water_ms: Option<u64>,
    ) -> Result<Self, ControlPlaneError> {
        if authority_generation == 0 {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint authority generation must be nonzero".to_owned(),
            });
        }
        let sample = control_plane_process_clock_sample()?;
        let health_time_ms = sample
            .health_time_ms()
            .ok_or(ControlPlaneError::AuthorityClockSourceUnavailable)?;
        Ok(Self::new(
            binding,
            authority_generation,
            committed_timestamp_high_water_ms,
            sample.wall_time_ms(),
            health_time_ms,
        ))
    }

    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(CONTROL_PLANE_CLOCK_CHECKPOINT_LEN);
        bytes.extend_from_slice(CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC);
        bytes.extend_from_slice(&CONTROL_PLANE_CLOCK_CHECKPOINT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.binding.0);
        bytes.extend_from_slice(&self.authority_generation.to_be_bytes());
        match self.committed_timestamp_high_water_ms {
            Some(timestamp_ms) => {
                bytes.push(1);
                bytes.extend_from_slice(&timestamp_ms.to_be_bytes());
            }
            None => {
                bytes.push(0);
                bytes.extend_from_slice(&0u64.to_be_bytes());
            }
        }
        bytes.extend_from_slice(&self.wall_time_ms.to_be_bytes());
        bytes.extend_from_slice(&self.health_time_ms.to_be_bytes());
        bytes.extend_from_slice(&checksum::crc64::checksum(&bytes).to_be_bytes());
        bytes
    }

    fn decode(
        bytes: &[u8],
        expected_binding: ControlPlaneAuthorityClockCheckpointBinding,
    ) -> Result<Self, ControlPlaneError> {
        if bytes.len() != CONTROL_PLANE_CLOCK_CHECKPOINT_LEN {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: format!(
                    "checkpoint length {} does not match required fixed length {CONTROL_PLANE_CLOCK_CHECKPOINT_LEN}",
                    bytes.len()
                ),
            });
        }
        let (body, encoded_checksum) =
            bytes.split_at(bytes.len() - CONTROL_PLANE_CLOCK_CHECKPOINT_CHECKSUM_LEN);
        let actual_checksum = checksum::crc64::checksum(body);
        let expected_checksum = u64::from_be_bytes(
            encoded_checksum
                .try_into()
                .expect("clock checkpoint checksum has fixed length"),
        );
        if actual_checksum != expected_checksum {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint checksum mismatch".to_owned(),
            });
        }
        let mut offset = 0usize;
        let mut take = |len: usize| -> Result<&[u8], ControlPlaneError> {
            let end = offset.checked_add(len).ok_or_else(|| {
                ControlPlaneError::AuthorityClockCheckpoint {
                    message: "checkpoint offset overflow".to_owned(),
                }
            })?;
            let value = body.get(offset..end).ok_or_else(|| {
                ControlPlaneError::AuthorityClockCheckpoint {
                    message: "checkpoint is truncated".to_owned(),
                }
            })?;
            offset = end;
            Ok(value)
        };
        if take(CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC.len())? != CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC
        {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint magic mismatch".to_owned(),
            });
        }
        let version = u16::from_be_bytes(
            take(2)?
                .try_into()
                .expect("clock checkpoint version has fixed length"),
        );
        if version != CONTROL_PLANE_CLOCK_CHECKPOINT_VERSION {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: format!("unsupported checkpoint version {version}"),
            });
        }
        let binding = Self::read_binding(take(CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN)?);
        if binding != expected_binding {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint durable-state identity does not match this authority"
                    .to_owned(),
            });
        }
        let authority_generation = u64::from_be_bytes(
            take(8)?
                .try_into()
                .expect("clock checkpoint authority generation has fixed length"),
        );
        if authority_generation == 0 {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint authority generation must be nonzero".to_owned(),
            });
        }
        let timestamp_tag = take(1)?[0];
        let encoded_timestamp = u64::from_be_bytes(
            take(8)?
                .try_into()
                .expect("clock checkpoint timestamp has fixed length"),
        );
        let committed_timestamp_high_water_ms = match timestamp_tag {
            0 if encoded_timestamp == 0 => None,
            0 => {
                return Err(ControlPlaneError::AuthorityClockCheckpoint {
                    message: "absent checkpoint timestamp must use canonical zero value".to_owned(),
                });
            }
            1 => Some(encoded_timestamp),
            tag => {
                return Err(ControlPlaneError::AuthorityClockCheckpoint {
                    message: format!("invalid checkpoint timestamp option tag {tag}"),
                });
            }
        };
        let wall_time_ms = u64::from_be_bytes(
            take(8)?
                .try_into()
                .expect("clock checkpoint wall time has fixed length"),
        );
        let health_time_ms = u64::from_be_bytes(
            take(8)?
                .try_into()
                .expect("clock checkpoint health time has fixed length"),
        );
        if offset != body.len() {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint has trailing bytes".to_owned(),
            });
        }
        Ok(Self::new(
            binding,
            authority_generation,
            committed_timestamp_high_water_ms,
            wall_time_ms,
            health_time_ms,
        ))
    }

    fn read_binding(bytes: &[u8]) -> ControlPlaneAuthorityClockCheckpointBinding {
        let mut binding = [0u8; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN];
        binding.copy_from_slice(bytes);
        ControlPlaneAuthorityClockCheckpointBinding(binding)
    }
}

/// Process-local authority clock gate for timestamp-bearing control-plane work.
///
/// Replicated apply can validate timestamp ordering and deadline relationships,
/// but only the command issuer can compare wall-clock progress with monotonic
/// elapsed time. Durable process startup additionally requires node-local
/// wall/health lineage evidence whenever restored state has a timestamp
/// high-water. Production construction always applies that restart-continuity
/// rule; sample-driven constructors exist only on the test surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneAuthorityClockBlockedReason {
    InitialTimestampDiscontinuity,
    RaftLeadershipChanged,
    ClockSourceUnavailable,
    ClockHealthRegression,
    WallClockRegression,
    WallClockForwardJump,
    CheckpointPersistenceFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneAuthorityClockStatus {
    generation: u64,
    established: bool,
    blocked_reason: Option<ControlPlaneAuthorityClockBlockedReason>,
    committed_timestamp_high_water_ms: Option<u64>,
    bound_raft_leadership_term: Option<u64>,
    current_raft_leadership_term: Option<u64>,
    local_raft_authority_leader: bool,
    local_raft_authority_serving: bool,
}

impl ControlPlaneAuthorityClockStatus {
    #[must_use]
    pub fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn established(self) -> bool {
        self.established
    }

    #[must_use]
    pub fn blocked_reason(self) -> Option<ControlPlaneAuthorityClockBlockedReason> {
        self.blocked_reason
    }

    #[must_use]
    pub fn committed_timestamp_high_water_ms(self) -> Option<u64> {
        self.committed_timestamp_high_water_ms
    }

    #[must_use]
    pub fn bound_raft_leadership_term(self) -> Option<u64> {
        self.bound_raft_leadership_term
    }

    #[must_use]
    pub fn current_raft_leadership_term(self) -> Option<u64> {
        self.current_raft_leadership_term
    }

    #[must_use]
    pub fn local_raft_authority_leader(self) -> bool {
        self.local_raft_authority_leader
    }

    #[must_use]
    pub fn local_raft_authority_serving(self) -> bool {
        self.local_raft_authority_serving
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneAuthorityClockContext {
    committed_timestamp_high_water_ms: Option<u64>,
    current_raft_leadership_term: Option<u64>,
    local_raft_authority_leader: bool,
    local_raft_authority_serving: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneAuthorityClockAdminSample {
    auth_authority_now_ms: u64,
    wall_ms: u64,
    clock_health_ms: Option<u64>,
}

impl ControlPlaneAuthorityClockAdminSample {
    #[must_use]
    pub fn new(auth_authority_now_ms: u64, wall_ms: u64, clock_health_ms: Option<u64>) -> Self {
        Self {
            auth_authority_now_ms,
            wall_ms,
            clock_health_ms,
        }
    }

    pub fn from_process_clock() -> Result<Self, ControlPlaneError> {
        let sample = control_plane_process_clock_sample()?;
        Ok(Self::new(
            sample.wall_time_ms(),
            sample.wall_time_ms(),
            sample.health_time_ms(),
        ))
    }
}

impl ControlPlaneAuthorityClockContext {
    #[must_use]
    pub fn new(
        committed_timestamp_high_water_ms: Option<u64>,
        current_raft_leadership_term: Option<u64>,
        local_raft_authority_leader: bool,
        local_raft_authority_serving: bool,
    ) -> Self {
        debug_assert!(!local_raft_authority_serving || local_raft_authority_leader);
        Self {
            committed_timestamp_high_water_ms,
            current_raft_leadership_term,
            local_raft_authority_leader,
            local_raft_authority_serving,
        }
    }

    #[must_use]
    pub fn committed_timestamp_high_water_ms(self) -> Option<u64> {
        self.committed_timestamp_high_water_ms
    }

    #[must_use]
    pub fn current_raft_leadership_term(self) -> Option<u64> {
        self.current_raft_leadership_term
    }

    #[must_use]
    pub fn local_raft_authority_leader(self) -> bool {
        self.local_raft_authority_leader
    }
}

#[derive(Debug)]
pub struct ControlPlaneAuthorityClock {
    reference_wall_ms: u64,
    reference_clock_health_ms: u64,
    minimum_timestamp_ms: Option<u64>,
    raft_leadership_term: Option<u64>,
    initial_raft_term_binding_available: bool,
    established: bool,
    generation: u64,
    blocked_reason: Option<ControlPlaneAuthorityClockBlockedReason>,
    restart_continuity_generation: Option<u64>,
}

impl ControlPlaneAuthorityClock {
    pub fn new_from_process_clock_with_restart_checkpoint(
        max_committed_timestamp_ms: Option<u64>,
        restart_checkpoint: Option<ControlPlaneAuthorityClockRestartCheckpoint>,
    ) -> Result<Self, ControlPlaneError> {
        let sample = control_plane_process_clock_sample()?;
        Self::new_internal(
            max_committed_timestamp_ms,
            sample.wall_time_ms(),
            sample.health_time_ms(),
            restart_checkpoint,
            false,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn new(
        max_committed_timestamp_ms: Option<u64>,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
    ) -> Result<Self, ControlPlaneError> {
        Self::new_internal(
            max_committed_timestamp_ms,
            wall_ms,
            clock_health_ms,
            None,
            true,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn new_with_restart_checkpoint(
        max_committed_timestamp_ms: Option<u64>,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
        restart_checkpoint: Option<ControlPlaneAuthorityClockRestartCheckpoint>,
    ) -> Result<Self, ControlPlaneError> {
        Self::new_internal(
            max_committed_timestamp_ms,
            wall_ms,
            clock_health_ms,
            restart_checkpoint,
            false,
        )
    }

    fn new_internal(
        max_committed_timestamp_ms: Option<u64>,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
        restart_checkpoint: Option<ControlPlaneAuthorityClockRestartCheckpoint>,
        allow_uncheckpointed_initial_state: bool,
    ) -> Result<Self, ControlPlaneError> {
        if restart_checkpoint.is_some_and(|checkpoint| checkpoint.authority_generation == 0) {
            return Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "checkpoint authority generation must be nonzero".to_owned(),
            });
        }
        let clock_health_ms =
            clock_health_ms.ok_or(ControlPlaneError::AuthorityClockSourceUnavailable)?;
        let established = max_committed_timestamp_ms.is_none_or(|committed_ms| {
            restart_checkpoint.map_or_else(
                || {
                    allow_uncheckpointed_initial_state
                        && wall_ms.abs_diff(committed_ms) <= CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS
                },
                |checkpoint| {
                    checkpoint
                        .committed_timestamp_high_water_ms
                        .is_none_or(|checkpoint_ms| checkpoint_ms <= committed_ms)
                        && checkpoint
                            .committed_timestamp_high_water_ms
                            .is_none_or(|checkpoint_ms| checkpoint.wall_time_ms >= checkpoint_ms)
                        && wall_ms >= committed_ms
                        && wall_ms
                            .checked_sub(checkpoint.wall_time_ms)
                            .zip(clock_health_ms.checked_sub(checkpoint.health_time_ms))
                            .is_some_and(|(wall_elapsed_ms, health_elapsed_ms)| {
                                wall_elapsed_ms.abs_diff(health_elapsed_ms)
                                    <= CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS
                            })
                },
            )
        });
        let blocked_reason = (!established)
            .then_some(ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity);
        let checkpoint_generation =
            restart_checkpoint.map(|checkpoint| checkpoint.authority_generation);
        let restart_continuity_generation = established.then_some(checkpoint_generation).flatten();
        Ok(Self {
            reference_wall_ms: wall_ms,
            reference_clock_health_ms: clock_health_ms,
            minimum_timestamp_ms: max_committed_timestamp_ms,
            raft_leadership_term: None,
            initial_raft_term_binding_available: max_committed_timestamp_ms.is_none(),
            established,
            generation: checkpoint_generation.unwrap_or(1),
            blocked_reason,
            restart_continuity_generation,
        })
    }

    fn advance_generation(&mut self) -> Result<(), ControlPlaneError> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(ControlPlaneError::AuthorityClockGenerationOverflow)?;
        Ok(())
    }

    #[must_use]
    pub fn is_established(&self) -> bool {
        self.established
    }

    fn latch_unhealthy(
        &mut self,
        reason: ControlPlaneAuthorityClockBlockedReason,
    ) -> Result<(), ControlPlaneError> {
        if self.established || self.blocked_reason != Some(reason) {
            self.advance_generation()?;
        }
        self.established = false;
        self.blocked_reason = Some(reason);
        self.restart_continuity_generation = None;
        Ok(())
    }

    pub fn fail_closed_after_checkpoint_persistence_failure(
        &mut self,
    ) -> Result<(), ControlPlaneError> {
        self.latch_unhealthy(ControlPlaneAuthorityClockBlockedReason::CheckpointPersistenceFailure)
    }

    #[must_use]
    pub fn status(
        &self,
        context: ControlPlaneAuthorityClockContext,
    ) -> ControlPlaneAuthorityClockStatus {
        ControlPlaneAuthorityClockStatus {
            generation: self.generation,
            established: self.established,
            blocked_reason: self.blocked_reason,
            committed_timestamp_high_water_ms: context.committed_timestamp_high_water_ms,
            bound_raft_leadership_term: self.raft_leadership_term,
            current_raft_leadership_term: context.current_raft_leadership_term,
            local_raft_authority_leader: context.local_raft_authority_leader,
            local_raft_authority_serving: context.local_raft_authority_serving,
        }
    }

    /// Return the durable lease-horizon identity for the currently established authority.
    pub fn lease_horizon_authority_binding(
        &self,
        current_raft_leadership_term: Option<u64>,
    ) -> Result<LeaseHorizonAuthorityBinding, ControlPlaneError> {
        if !self.established {
            return Err(ControlPlaneError::AuthorityClockNotEstablished {
                blocked_reason: self.blocked_reason,
            });
        }
        if self.raft_leadership_term != current_raft_leadership_term {
            return Err(ControlPlaneError::AuthorityClockRaftTermMismatch {
                expected_term: self.raft_leadership_term,
                actual_term: current_raft_leadership_term,
            });
        }
        LeaseHorizonAuthorityBinding::checked_new(self.generation, current_raft_leadership_term)
            .ok_or(ControlPlaneError::AuthorityClockGenerationOverflow)
    }

    /// Ensure a restarted process cannot reuse the generation that established
    /// a restored volatile-lease capability.
    pub fn advance_generation_past_lease_horizon(
        &mut self,
        previous_authority: LeaseHorizonAuthorityBinding,
    ) -> Result<(), ControlPlaneError> {
        let next_generation = previous_authority
            .clock_generation()
            .checked_add(1)
            .ok_or(ControlPlaneError::AuthorityClockGenerationOverflow)?;
        self.generation = self.generation.max(next_generation);
        self.restart_continuity_generation = None;
        Ok(())
    }

    /// Resume a single-authority horizon after a checkpoint-proven restart.
    ///
    /// The process must already hold the durable-state lock. Raft horizons
    /// never use this path because leadership terms provide their own
    /// successor identity and must retain the ordinary rebinding fence.
    pub fn resume_single_authority_lease_horizon_generation(
        &mut self,
        previous_authority: LeaseHorizonAuthorityBinding,
    ) -> bool {
        if self.restart_continuity_generation != Some(self.generation)
            || !self.established
            || self.raft_leadership_term.is_some()
            || previous_authority.raft_term().is_some()
            || previous_authority.clock_generation() != self.generation
        {
            return false;
        }
        self.restart_continuity_generation = None;
        true
    }

    /// Observe current authority and clock state before reporting status.
    pub fn observe_status(
        &mut self,
        context: ControlPlaneAuthorityClockContext,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        self.observe_committed_timestamp_high_water(context.committed_timestamp_high_water_ms);
        if let Some(term) = context.current_raft_leadership_term {
            if let Err(error) = self.validate_raft_leadership_term(term) {
                return if self.established {
                    Err(error)
                } else {
                    Ok(self.status(context))
                };
            }
        }
        if let Err(error) = self.effective_now_ms(wall_ms, clock_health_ms) {
            return if self.established {
                Err(error)
            } else {
                Ok(self.status(context))
            };
        }
        Ok(self.status(context))
    }

    pub fn reestablish(
        &mut self,
        expected_generation: u64,
        expected_committed_timestamp_high_water_ms: Option<u64>,
        expected_raft_leadership_term: Option<u64>,
        context: ControlPlaneAuthorityClockContext,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        if self.established {
            return Err(ControlPlaneError::AuthorityClockAlreadyEstablished);
        }
        if expected_generation != self.generation {
            return Err(ControlPlaneError::AuthorityClockGenerationMismatch {
                expected_generation,
                actual_generation: self.generation,
            });
        }
        if expected_committed_timestamp_high_water_ms != context.committed_timestamp_high_water_ms {
            return Err(
                ControlPlaneError::AuthorityClockCommittedTimestampMismatch {
                    expected_timestamp_ms: expected_committed_timestamp_high_water_ms,
                    actual_timestamp_ms: context.committed_timestamp_high_water_ms,
                },
            );
        }
        if expected_raft_leadership_term != context.current_raft_leadership_term {
            return Err(ControlPlaneError::AuthorityClockRaftTermMismatch {
                expected_term: expected_raft_leadership_term,
                actual_term: context.current_raft_leadership_term,
            });
        }
        if context.current_raft_leadership_term.is_some() && !context.local_raft_authority_serving {
            return Err(ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority);
        }
        if let Some(committed_timestamp_high_water_ms) = context.committed_timestamp_high_water_ms {
            if wall_ms < committed_timestamp_high_water_ms {
                return Err(
                    ControlPlaneError::AuthorityClockWallBehindCommittedTimestamp {
                        wall_ms,
                        committed_timestamp_high_water_ms,
                    },
                );
            }
        }
        let clock_health_ms =
            clock_health_ms.ok_or(ControlPlaneError::AuthorityClockSourceUnavailable)?;
        let next_generation = self
            .generation
            .checked_add(1)
            .ok_or(ControlPlaneError::AuthorityClockGenerationOverflow)?;
        self.reference_wall_ms = wall_ms;
        self.reference_clock_health_ms = clock_health_ms;
        self.minimum_timestamp_ms = context.committed_timestamp_high_water_ms;
        self.raft_leadership_term = context.current_raft_leadership_term;
        self.initial_raft_term_binding_available = false;
        self.established = true;
        self.blocked_reason = None;
        self.generation = next_generation;
        self.restart_continuity_generation = None;
        Ok(self.status(context))
    }

    pub fn bind_initial_raft_leadership_term(&mut self, term: Option<u64>) {
        self.raft_leadership_term = term;
        self.initial_raft_term_binding_available = false;
        self.restart_continuity_generation = None;
    }

    pub fn observe_committed_timestamp_high_water(&mut self, timestamp_ms: Option<u64>) {
        if let Some(timestamp_ms) = timestamp_ms {
            self.minimum_timestamp_ms = Some(
                self.minimum_timestamp_ms
                    .map_or(timestamp_ms, |current| current.max(timestamp_ms)),
            );
        }
    }

    /// Bind clock authority to one locally serving Raft leadership term.
    pub fn validate_raft_leadership_term(&mut self, term: u64) -> Result<(), ControlPlaneError> {
        match self.raft_leadership_term {
            Some(established_term) if established_term == term => Ok(()),
            None if self.minimum_timestamp_ms.is_none()
                && self.initial_raft_term_binding_available =>
            {
                self.raft_leadership_term = Some(term);
                self.initial_raft_term_binding_available = false;
                Ok(())
            }
            established_term => {
                self.latch_unhealthy(
                    ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged,
                )?;
                Err(ControlPlaneError::AuthorityClockLeadershipChanged {
                    established_term,
                    current_term: term,
                })
            }
        }
    }

    /// Validate the process clock and return a non-regressing command timestamp.
    pub fn effective_now_ms(
        &mut self,
        wall_ms: u64,
        clock_health_ms: Option<u64>,
    ) -> Result<u64, ControlPlaneError> {
        let Some(clock_health_ms) = clock_health_ms else {
            self.latch_unhealthy(ControlPlaneAuthorityClockBlockedReason::ClockSourceUnavailable)?;
            return Err(ControlPlaneError::AuthorityClockSourceUnavailable);
        };
        let max_committed_timestamp_ms = self.minimum_timestamp_ms.unwrap_or(wall_ms);
        if !self.established {
            return Err(ControlPlaneError::AuthorityClockNotEstablished {
                blocked_reason: self.blocked_reason,
            });
        }

        let Some(monotonic_elapsed_ms) =
            clock_health_ms.checked_sub(self.reference_clock_health_ms)
        else {
            self.latch_unhealthy(ControlPlaneAuthorityClockBlockedReason::ClockHealthRegression)?;
            return Err(ControlPlaneError::CommittedTimestampRegression {
                timestamp_ms: clock_health_ms,
                max_committed_timestamp_ms: self.reference_clock_health_ms,
            });
        };
        let expected_wall_ms = self
            .reference_wall_ms
            .checked_add(monotonic_elapsed_ms)
            .ok_or(ControlPlaneError::LeaseDeadlineOverflow)?;
        if wall_ms.abs_diff(expected_wall_ms) > CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS {
            self.latch_unhealthy(if wall_ms < expected_wall_ms {
                ControlPlaneAuthorityClockBlockedReason::WallClockRegression
            } else {
                ControlPlaneAuthorityClockBlockedReason::WallClockForwardJump
            })?;
            return Err(if wall_ms < expected_wall_ms {
                ControlPlaneError::CommittedTimestampRegression {
                    timestamp_ms: wall_ms,
                    max_committed_timestamp_ms: expected_wall_ms,
                }
            } else {
                ControlPlaneError::CommittedTimestampTooFarAhead {
                    timestamp_ms: wall_ms,
                    max_committed_timestamp_ms: expected_wall_ms,
                    max_forward_jump_ms: CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
                }
            });
        }

        let effective_now_ms = wall_ms.max(max_committed_timestamp_ms);
        self.minimum_timestamp_ms = Some(effective_now_ms);
        Ok(effective_now_ms)
    }

    pub fn effective_process_now_ms(&mut self) -> Result<u64, ControlPlaneError> {
        let sample = control_plane_process_clock_sample()?;
        self.effective_now_ms(sample.wall_time_ms(), sample.health_time_ms())
    }
}

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

pub trait ControlPlaneStore {
    fn load(&self) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError>;
    fn checkpoint(
        &self,
        previous_snapshot: Option<&ClusterControlSnapshot>,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError>;

    fn commit_command(
        &self,
        previous_snapshot: &ClusterControlSnapshot,
        _command: &ControlPlaneCommand,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        self.checkpoint(Some(previous_snapshot), next_snapshot)
    }

    fn ensure_healthy(&self) -> Result<(), ControlPlaneError> {
        Ok(())
    }

    #[cfg(test)]
    fn checkpoint_manually_modified_snapshot_for_test(
        &self,
        previous_snapshot: &ClusterControlSnapshot,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        self.checkpoint(Some(previous_snapshot), next_snapshot)
    }
}

#[derive(Debug, Default)]
struct FileControlPlaneStoreDurability {
    initialized: bool,
    initial_identity_created: bool,
    poisoned: Option<String>,
    journal_clean_offset: u64,
    commands_since_checkpoint: u64,
    bytes_since_checkpoint: u64,
    first_uncheckpointed_at: Option<Instant>,
    checkpoint_generation: u64,
    published_snapshot_digest: Option<u64>,
    published_chain_digest: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct SingleAuthorityJournalObserver {
    #[cfg(test)]
    fail_next_file_sync: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    fail_next_directory_sync: Arc<std::sync::atomic::AtomicBool>,
}

impl DurableJournalObserver for SingleAuthorityJournalObserver {
    fn record_append(&self, elapsed: Duration, succeeded: bool) {
        observability::record_control_plane_journal_append(elapsed, succeeded);
    }

    fn record_lock_wait(&self, elapsed: Duration) {
        observability::record_control_plane_journal_lock_wait(elapsed);
    }

    fn record_frame_bytes(&self, bytes: usize) {
        observability::record_control_plane_journal_frame_bytes(bytes);
    }

    fn record_file_sync(&self, elapsed: Duration) {
        observability::record_control_plane_journal_file_sync(elapsed);
    }

    fn record_directory_sync(&self, elapsed: Duration) {
        observability::record_control_plane_journal_directory_sync(elapsed);
    }

    fn record_compaction(&self, elapsed: Duration, succeeded: bool) {
        observability::record_control_plane_journal_compaction(elapsed, succeeded);
    }

    fn record_compaction_lock_wait(&self, elapsed: Duration) {
        observability::record_control_plane_journal_compaction_lock_wait(elapsed);
    }

    fn record_compaction_bytes(&self, bytes: usize) {
        observability::record_control_plane_journal_compaction_bytes(bytes);
    }

    fn record_compaction_file_sync(&self, elapsed: Duration) {
        observability::record_control_plane_journal_compaction_file_sync(elapsed);
    }

    fn record_compaction_directory_sync(&self, elapsed: Duration) {
        observability::record_control_plane_journal_compaction_directory_sync(elapsed);
    }

    fn before_file_sync(&self, _path: &Path) -> Result<(), ControlPlaneError> {
        #[cfg(test)]
        if self
            .fail_next_file_sync
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ControlPlaneError::io(
                "sync single-authority control-plane journal",
                std::io::Error::other(
                    "injected single-authority control-plane journal sync failure",
                ),
            ));
        }
        Ok(())
    }

    fn sync_parent(&self, path: &Path) -> Result<(), ControlPlaneError> {
        #[cfg(test)]
        if self
            .fail_next_directory_sync
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ControlPlaneError::io(
                "sync single-authority control-plane journal directory",
                std::io::Error::other(
                    "injected single-authority control-plane journal directory sync failure",
                ),
            ));
        }
        let parent = state_parent(path);
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| {
                ControlPlaneError::io(
                    "sync single-authority control-plane journal directory",
                    source,
                )
            })
    }
}

#[derive(Debug, Clone)]
pub struct FileControlPlaneStore {
    path: PathBuf,
    journal: DurableJournalFile<SingleAuthorityJournalObserver>,
    durability: Arc<Mutex<FileControlPlaneStoreDurability>>,
    checkpoint_publication: Arc<Mutex<()>>,
    checkpoint_command_limit: u64,
    checkpoint_byte_limit: u64,
    checkpoint_interval: Duration,
    #[cfg(test)]
    journal_observer: Arc<SingleAuthorityJournalObserver>,
    #[cfg(test)]
    fail_checkpoint_after_anchor: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    fail_initial_checkpoint_after_identity: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    fail_checkpoint_after_prepared_snapshot_sync: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    checkpoint_after_journal_replacement_gate: Arc<(
        Mutex<CheckpointAfterJournalReplacementGate>,
        std::sync::Condvar,
    )>,
    #[cfg(test)]
    commit_before_durability_lock_signal:
        Arc<(Mutex<CommitBeforeDurabilityLockSignal>, std::sync::Condvar)>,
}

struct FileControlPlaneCheckpointCapture {
    store_instance: Arc<Mutex<FileControlPlaneStoreDurability>>,
    checkpoint_generation: u64,
    journal_offset: u64,
    chain_digest: u64,
    captured_at: Instant,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct CheckpointAfterJournalReplacementGate {
    pause: bool,
    reached: bool,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct CommitBeforeDurabilityLockSignal {
    armed: bool,
    reached: bool,
}

pub struct SingleAuthorityDurableCheckpoint {
    store: FileControlPlaneStore,
    capture: FileControlPlaneCheckpointCapture,
    snapshot: ClusterControlSnapshot,
}

impl SingleAuthorityDurableCheckpoint {
    pub fn persist(self) -> Result<(), ControlPlaneError> {
        self.store
            .persist_captured_checkpoint(self.capture, &self.snapshot)
    }
}

impl FileControlPlaneStore {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::with_checkpoint_policy(
            path.into(),
            SINGLE_AUTHORITY_JOURNAL_CHECKPOINT_COMMAND_LIMIT,
            SINGLE_AUTHORITY_JOURNAL_CHECKPOINT_BYTE_LIMIT,
            SINGLE_AUTHORITY_JOURNAL_CHECKPOINT_INTERVAL,
        )
    }

    #[cfg(test)]
    fn with_checkpoint_limits(
        path: PathBuf,
        checkpoint_command_limit: u64,
        checkpoint_byte_limit: u64,
    ) -> Self {
        Self::with_checkpoint_policy(
            path,
            checkpoint_command_limit,
            checkpoint_byte_limit,
            SINGLE_AUTHORITY_JOURNAL_CHECKPOINT_INTERVAL,
        )
    }

    fn with_checkpoint_policy(
        path: PathBuf,
        checkpoint_command_limit: u64,
        checkpoint_byte_limit: u64,
        checkpoint_interval: Duration,
    ) -> Self {
        let journal_observer = Arc::new(SingleAuthorityJournalObserver::default());
        let journal = DurableJournalFile::new(
            single_authority_journal_path(&path),
            DurableJournalFormat {
                file_magic: SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC,
                file_version: SINGLE_AUTHORITY_JOURNAL_FILE_VERSION,
                label: "single-authority control-plane journal",
            },
            DurableJournalIoContexts {
                create_directory: "create single-authority control-plane journal directory",
                open_for_append: "open single-authority control-plane journal for append",
                write_frame_length: "write single-authority control-plane journal frame length",
                write_frame: "write single-authority control-plane journal frame",
                sync_file: "sync single-authority control-plane journal",
                stat_for_status: "stat single-authority control-plane journal",
                open_for_replay: "open single-authority control-plane journal for replay",
                read_for_replay: "read single-authority control-plane journal",
                open_for_tail_truncation:
                    "open single-authority control-plane journal for tail truncation",
                truncate_torn_tail: "truncate torn single-authority control-plane journal tail",
                sync_truncated_tail: "sync truncated single-authority control-plane journal tail",
                read_for_compaction: "read single-authority control-plane journal for compaction",
                create_compacted_temp: "create compacted single-authority control-plane journal",
                write_compacted_temp: "write compacted single-authority control-plane journal",
                sync_compacted_temp: "sync compacted single-authority control-plane journal",
                commit_compacted: "commit compacted single-authority control-plane journal",
                stat_before_append: "stat single-authority control-plane journal before append",
                write_file_header: "write single-authority control-plane journal header",
                open_file_header: "open single-authority control-plane journal header",
                read_file_header: "read single-authority control-plane journal header",
            },
            Arc::clone(&journal_observer),
        );
        Self {
            path,
            journal,
            durability: Arc::new(Mutex::new(FileControlPlaneStoreDurability::default())),
            checkpoint_publication: Arc::new(Mutex::new(())),
            checkpoint_command_limit,
            checkpoint_byte_limit,
            checkpoint_interval,
            #[cfg(test)]
            journal_observer,
            #[cfg(test)]
            fail_checkpoint_after_anchor: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            fail_initial_checkpoint_after_identity: Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            )),
            #[cfg(test)]
            fail_checkpoint_after_prepared_snapshot_sync: Arc::new(
                std::sync::atomic::AtomicBool::new(false),
            ),
            #[cfg(test)]
            checkpoint_after_journal_replacement_gate: Arc::new((
                Mutex::new(CheckpointAfterJournalReplacementGate::default()),
                std::sync::Condvar::new(),
            )),
            #[cfg(test)]
            commit_before_durability_lock_signal: Arc::new((
                Mutex::new(CommitBeforeDurabilityLockSignal::default()),
                std::sync::Condvar::new(),
            )),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn journal_path(&self) -> &Path {
        self.journal.path()
    }

    #[cfg(test)]
    fn fail_next_journal_file_sync(&self) {
        self.journal_observer
            .fail_next_file_sync
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn fail_next_journal_directory_sync(&self) {
        self.journal_observer
            .fail_next_directory_sync
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn fail_next_checkpoint_after_anchor(&self) {
        self.fail_checkpoint_after_anchor
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn fail_next_initial_checkpoint_after_identity(&self) {
        self.fail_initial_checkpoint_after_identity
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn fail_next_checkpoint_after_prepared_snapshot_sync(&self) {
        self.fail_checkpoint_after_prepared_snapshot_sync
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn pause_next_checkpoint_after_journal_replacement(&self) {
        let (state, _) = &*self.checkpoint_after_journal_replacement_gate;
        let mut state = state.lock().unwrap();
        state.pause = true;
        state.reached = false;
    }

    #[cfg(test)]
    fn wait_for_checkpoint_journal_replacement(&self, timeout: Duration) -> bool {
        let (state, reached) = &*self.checkpoint_after_journal_replacement_gate;
        let state = state.lock().unwrap();
        let (state, _) = reached
            .wait_timeout_while(state, timeout, |state| !state.reached)
            .unwrap();
        state.reached
    }

    #[cfg(test)]
    fn release_checkpoint_after_journal_replacement(&self) {
        let (state, released) = &*self.checkpoint_after_journal_replacement_gate;
        let mut state = state.lock().unwrap();
        state.pause = false;
        released.notify_all();
    }

    #[cfg(test)]
    fn wait_after_checkpoint_journal_replacement_if_requested(&self) {
        let (state, released) = &*self.checkpoint_after_journal_replacement_gate;
        let mut state = state.lock().unwrap();
        if !state.pause {
            return;
        }
        state.reached = true;
        released.notify_all();
        while state.pause {
            state = released.wait(state).unwrap();
        }
    }

    #[cfg(test)]
    fn arm_commit_before_durability_lock_signal(&self) {
        let (state, _) = &*self.commit_before_durability_lock_signal;
        let mut state = state.lock().unwrap();
        state.armed = true;
        state.reached = false;
    }

    #[cfg(test)]
    fn signal_commit_before_durability_lock_if_armed(&self) {
        let (state, reached) = &*self.commit_before_durability_lock_signal;
        let mut state = state.lock().unwrap();
        if !state.armed {
            return;
        }
        state.armed = false;
        state.reached = true;
        reached.notify_all();
    }

    #[cfg(test)]
    fn wait_for_commit_before_durability_lock(&self, timeout: Duration) -> bool {
        let (state, reached) = &*self.commit_before_durability_lock_signal;
        let state = state.lock().unwrap();
        let (state, _) = reached
            .wait_timeout_while(state, timeout, |state| !state.reached)
            .unwrap();
        state.reached
    }

    pub fn load_authority_clock_restart_checkpoint(
        &self,
        binding: ControlPlaneAuthorityClockCheckpointBinding,
    ) -> Result<Option<ControlPlaneAuthorityClockRestartCheckpoint>, ControlPlaneError> {
        load_authority_clock_restart_checkpoint(&self.path, binding)
    }

    pub fn load_or_create_authority_clock_checkpoint_binding(
        &self,
    ) -> Result<ControlPlaneAuthorityClockCheckpointBinding, ControlPlaneError> {
        let (binding, created) =
            self.load_or_create_authority_clock_checkpoint_binding_untracked()?;
        if created {
            self.lock_durability()?.initial_identity_created = true;
        }
        Ok(binding)
    }

    fn load_or_create_authority_clock_checkpoint_binding_untracked(
        &self,
    ) -> Result<(ControlPlaneAuthorityClockCheckpointBinding, bool), ControlPlaneError> {
        match load_single_authority_clock_checkpoint_binding(&self.path)? {
            Some(binding) => Ok((binding, false)),
            None if self.path.exists() => Err(ControlPlaneError::AuthorityClockCheckpoint {
                message: "existing single-authority state is missing its durable identity"
                    .to_owned(),
            }),
            None => {
                let binding =
                    ControlPlaneAuthorityClockCheckpointBinding::generate_single_authority()?;
                store_single_authority_clock_checkpoint_binding(&self.path, binding)?;
                Ok((binding, true))
            }
        }
    }
}

fn authority_clock_restart_checkpoint_path(path: &Path) -> PathBuf {
    let mut checkpoint_path = path.as_os_str().to_os_string();
    checkpoint_path.push(".clock");
    PathBuf::from(checkpoint_path)
}

fn authority_clock_restart_checkpoint_tmp_path(path: &Path) -> PathBuf {
    let mut tmp_path = authority_clock_restart_checkpoint_path(path).into_os_string();
    tmp_path.push(".tmp");
    PathBuf::from(tmp_path)
}

fn single_authority_identity_path(path: &Path) -> PathBuf {
    let mut identity_path = path.as_os_str().to_os_string();
    identity_path.push(".identity");
    PathBuf::from(identity_path)
}

fn single_authority_identity_tmp_path(path: &Path) -> PathBuf {
    let mut tmp_path = single_authority_identity_path(path).into_os_string();
    tmp_path.push(".tmp");
    PathBuf::from(tmp_path)
}

fn single_authority_journal_path(path: &Path) -> PathBuf {
    let mut journal_path = path.as_os_str().to_os_string();
    journal_path.push(".journal");
    PathBuf::from(journal_path)
}

fn single_authority_snapshot_tmp_path(path: &Path) -> PathBuf {
    path.with_extension("tmp")
}

fn single_authority_initialized_path(path: &Path) -> PathBuf {
    let mut initialized_path = path.as_os_str().to_os_string();
    initialized_path.push(".initialized");
    PathBuf::from(initialized_path)
}

fn single_authority_initialized_tmp_path(path: &Path) -> PathBuf {
    let mut tmp_path = single_authority_initialized_path(path).into_os_string();
    tmp_path.push(".tmp");
    PathBuf::from(tmp_path)
}

fn read_fixed_control_plane_sidecar<const N: usize>(
    path: &Path,
    context: &'static str,
) -> Result<Option<[u8; N]>, ControlPlaneError> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(ControlPlaneError::io(context, source)),
    };
    if metadata.len() != N as u64 {
        return Err(ControlPlaneError::AuthorityClockCheckpoint {
            message: format!(
                "{} length {} does not match required fixed length {N}",
                path.display(),
                metadata.len()
            ),
        });
    }
    let mut file =
        std::fs::File::open(path).map_err(|source| ControlPlaneError::io(context, source))?;
    let mut bytes = [0u8; N];
    file.read_exact(&mut bytes)
        .map_err(|source| ControlPlaneError::io(context, source))?;
    let mut trailing = [0u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|source| ControlPlaneError::io(context, source))?
        != 0
    {
        return Err(ControlPlaneError::AuthorityClockCheckpoint {
            message: format!("{} grew while it was being read", path.display()),
        });
    }
    Ok(Some(bytes))
}

struct ControlPlaneSidecarIoContexts {
    create: &'static str,
    write: &'static str,
    sync: &'static str,
    rename: &'static str,
    directory: &'static str,
}

fn store_control_plane_sidecar(
    path: &Path,
    tmp_path: &Path,
    bytes: &[u8],
    contexts: ControlPlaneSidecarIoContexts,
) -> Result<(), ControlPlaneError> {
    let parent = state_parent(path);
    create_control_plane_directory_all_durable(parent)?;
    {
        let mut file = std::fs::File::create(tmp_path)
            .map_err(|source| ControlPlaneError::io(contexts.create, source))?;
        file.write_all(bytes)
            .map_err(|source| ControlPlaneError::io(contexts.write, source))?;
        file.sync_all()
            .map_err(|source| ControlPlaneError::io(contexts.sync, source))?;
    }
    std::fs::rename(tmp_path, path)
        .map_err(|source| ControlPlaneError::io(contexts.rename, source))?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| ControlPlaneError::io(contexts.directory, source))?;
    Ok(())
}

fn load_single_authority_clock_checkpoint_binding(
    durable_state_path: &Path,
) -> Result<Option<ControlPlaneAuthorityClockCheckpointBinding>, ControlPlaneError> {
    let identity_path = single_authority_identity_path(durable_state_path);
    let Some(bytes) = read_fixed_control_plane_sidecar::<CONTROL_PLANE_STATE_IDENTITY_LEN>(
        &identity_path,
        "load single-authority control-plane durable identity",
    )?
    else {
        return Ok(None);
    };
    let (body, checksum_bytes) = bytes.split_at(CONTROL_PLANE_STATE_IDENTITY_LEN - 8);
    if checksum::crc64::checksum(body)
        != u64::from_be_bytes(
            checksum_bytes
                .try_into()
                .expect("identity checksum has fixed length"),
        )
    {
        return Err(ControlPlaneError::AuthorityClockCheckpoint {
            message: "single-authority durable identity checksum mismatch".to_owned(),
        });
    }
    if &body[..8] != CONTROL_PLANE_STATE_IDENTITY_MAGIC {
        return Err(ControlPlaneError::AuthorityClockCheckpoint {
            message: "single-authority durable identity magic mismatch".to_owned(),
        });
    }
    let version = u16::from_be_bytes(
        body[8..10]
            .try_into()
            .expect("identity version has fixed length"),
    );
    if version != CONTROL_PLANE_STATE_IDENTITY_VERSION {
        return Err(ControlPlaneError::AuthorityClockCheckpoint {
            message: format!("unsupported single-authority durable identity version {version}"),
        });
    }
    let mut binding = [0u8; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN];
    binding.copy_from_slice(&body[10..]);
    Ok(Some(ControlPlaneAuthorityClockCheckpointBinding(binding)))
}

fn store_single_authority_clock_checkpoint_binding(
    durable_state_path: &Path,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
) -> Result<(), ControlPlaneError> {
    let mut bytes = Vec::with_capacity(CONTROL_PLANE_STATE_IDENTITY_LEN);
    bytes.extend_from_slice(CONTROL_PLANE_STATE_IDENTITY_MAGIC);
    bytes.extend_from_slice(&CONTROL_PLANE_STATE_IDENTITY_VERSION.to_be_bytes());
    bytes.extend_from_slice(&binding.0);
    bytes.extend_from_slice(&checksum::crc64::checksum(&bytes).to_be_bytes());
    store_control_plane_sidecar(
        &single_authority_identity_path(durable_state_path),
        &single_authority_identity_tmp_path(durable_state_path),
        &bytes,
        ControlPlaneSidecarIoContexts {
            create: "create single-authority control-plane durable identity",
            write: "write single-authority control-plane durable identity",
            sync: "sync single-authority control-plane durable identity",
            rename: "commit single-authority control-plane durable identity",
            directory: "sync single-authority control-plane durable identity directory",
        },
    )
}

fn load_single_authority_initialized_binding(
    durable_state_path: &Path,
) -> Result<Option<ControlPlaneAuthorityClockCheckpointBinding>, ControlPlaneError> {
    let initialized_path = single_authority_initialized_path(durable_state_path);
    let Some(bytes) = read_fixed_control_plane_sidecar::<SINGLE_AUTHORITY_INITIALIZED_LEN>(
        &initialized_path,
        "load single-authority control-plane initialization marker",
    )?
    else {
        return Ok(None);
    };
    let (body, checksum_bytes) = bytes.split_at(SINGLE_AUTHORITY_INITIALIZED_LEN - 8);
    if checksum::crc64::checksum(body)
        != u64::from_be_bytes(
            checksum_bytes
                .try_into()
                .expect("initialization marker checksum has fixed length"),
        )
    {
        return Err(ControlPlaneError::CommandDecode {
            message: "single-authority initialization marker checksum mismatch".to_owned(),
        });
    }
    if &body[..8] != SINGLE_AUTHORITY_INITIALIZED_MAGIC {
        return Err(ControlPlaneError::CommandDecode {
            message: "single-authority initialization marker magic mismatch".to_owned(),
        });
    }
    let version = u16::from_be_bytes(
        body[8..10]
            .try_into()
            .expect("initialization marker version has fixed length"),
    );
    if version != SINGLE_AUTHORITY_INITIALIZED_VERSION {
        return Err(ControlPlaneError::CommandDecode {
            message: format!(
                "unsupported single-authority initialization marker version {version}"
            ),
        });
    }
    let mut binding = [0; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN];
    binding.copy_from_slice(&body[10..]);
    Ok(Some(ControlPlaneAuthorityClockCheckpointBinding(binding)))
}

fn store_single_authority_initialized_binding(
    durable_state_path: &Path,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
) -> Result<(), ControlPlaneError> {
    let mut bytes = Vec::with_capacity(SINGLE_AUTHORITY_INITIALIZED_LEN);
    bytes.extend_from_slice(SINGLE_AUTHORITY_INITIALIZED_MAGIC);
    bytes.extend_from_slice(&SINGLE_AUTHORITY_INITIALIZED_VERSION.to_be_bytes());
    bytes.extend_from_slice(&binding.0);
    bytes.extend_from_slice(&checksum::crc64::checksum(&bytes).to_be_bytes());
    store_control_plane_sidecar(
        &single_authority_initialized_path(durable_state_path),
        &single_authority_initialized_tmp_path(durable_state_path),
        &bytes,
        ControlPlaneSidecarIoContexts {
            create: "create single-authority control-plane initialization marker",
            write: "write single-authority control-plane initialization marker",
            sync: "sync single-authority control-plane initialization marker",
            rename: "commit single-authority control-plane initialization marker",
            directory: "sync single-authority control-plane initialization marker directory",
        },
    )
}

pub fn load_authority_clock_restart_checkpoint(
    durable_state_path: &Path,
    expected_binding: ControlPlaneAuthorityClockCheckpointBinding,
) -> Result<Option<ControlPlaneAuthorityClockRestartCheckpoint>, ControlPlaneError> {
    let checkpoint_path = authority_clock_restart_checkpoint_path(durable_state_path);
    let Some(bytes) = read_fixed_control_plane_sidecar::<CONTROL_PLANE_CLOCK_CHECKPOINT_LEN>(
        &checkpoint_path,
        "load control-plane authority clock checkpoint",
    )?
    else {
        return Ok(None);
    };
    ControlPlaneAuthorityClockRestartCheckpoint::decode(&bytes, expected_binding).map(Some)
}

pub fn invalidate_authority_clock_restart_checkpoint(
    durable_state_path: &Path,
) -> Result<(), ControlPlaneError> {
    let checkpoint_path = authority_clock_restart_checkpoint_path(durable_state_path);
    let removed = match std::fs::remove_file(&checkpoint_path) {
        Ok(()) => true,
        Err(error) if error.kind() == ErrorKind::NotFound => false,
        Err(source) => {
            return Err(ControlPlaneError::io(
                "invalidate control-plane authority clock checkpoint",
                source,
            ));
        }
    };
    if !removed {
        return Ok(());
    }
    std::fs::File::open(state_parent(&checkpoint_path))
        .and_then(|directory| directory.sync_all())
        .map_err(|source| {
            ControlPlaneError::io(
                "sync invalidated control-plane authority clock checkpoint directory",
                source,
            )
        })?;
    Ok(())
}

pub fn store_authority_clock_restart_checkpoint(
    durable_state_path: &Path,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
    authority_generation: u64,
    committed_timestamp_high_water_ms: Option<u64>,
) -> Result<ControlPlaneAuthorityClockRestartCheckpoint, ControlPlaneError> {
    let checkpoint = ControlPlaneAuthorityClockRestartCheckpoint::from_process_clock(
        binding,
        authority_generation,
        committed_timestamp_high_water_ms,
    )?;
    store_authority_clock_restart_checkpoint_value(durable_state_path, checkpoint)?;
    Ok(checkpoint)
}

pub fn store_validated_authority_clock_restart_checkpoint(
    durable_state_path: &Path,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
    committed_timestamp_high_water_ms: Option<u64>,
    authority_clock: &mut ControlPlaneAuthorityClock,
) -> Result<ControlPlaneAuthorityClockRestartCheckpoint, ControlPlaneError> {
    let sample = control_plane_process_clock_sample()?;
    let checkpoint = validated_authority_clock_restart_checkpoint(
        binding,
        committed_timestamp_high_water_ms,
        authority_clock,
        sample.wall_time_ms(),
        sample.health_time_ms(),
    )?;
    store_authority_clock_restart_checkpoint_value(durable_state_path, checkpoint)?;
    Ok(checkpoint)
}

fn validated_authority_clock_restart_checkpoint(
    binding: ControlPlaneAuthorityClockCheckpointBinding,
    committed_timestamp_high_water_ms: Option<u64>,
    authority_clock: &mut ControlPlaneAuthorityClock,
    wall_time_ms: u64,
    health_time_ms: Option<u64>,
) -> Result<ControlPlaneAuthorityClockRestartCheckpoint, ControlPlaneError> {
    authority_clock.effective_now_ms(wall_time_ms, health_time_ms)?;
    let health_time_ms =
        health_time_ms.ok_or(ControlPlaneError::AuthorityClockSourceUnavailable)?;
    Ok(ControlPlaneAuthorityClockRestartCheckpoint::new(
        binding,
        authority_clock.generation,
        committed_timestamp_high_water_ms,
        wall_time_ms,
        health_time_ms,
    ))
}

fn store_authority_clock_restart_checkpoint_value(
    durable_state_path: &Path,
    checkpoint: ControlPlaneAuthorityClockRestartCheckpoint,
) -> Result<(), ControlPlaneError> {
    let checkpoint_path = authority_clock_restart_checkpoint_path(durable_state_path);
    let tmp_path = authority_clock_restart_checkpoint_tmp_path(durable_state_path);
    store_control_plane_sidecar(
        &checkpoint_path,
        &tmp_path,
        &checkpoint.encode(),
        ControlPlaneSidecarIoContexts {
            create: "create control-plane authority clock checkpoint",
            write: "write control-plane authority clock checkpoint",
            sync: "sync control-plane authority clock checkpoint",
            rename: "commit control-plane authority clock checkpoint",
            directory: "sync control-plane authority clock checkpoint directory",
        },
    )?;
    Ok(())
}

#[derive(Debug)]
struct SingleAuthorityJournalRecord {
    binding: ControlPlaneAuthorityClockCheckpointBinding,
    previous_chain_digest: u64,
    resulting_chain_digest: u64,
    command: Option<ControlPlaneCommand>,
}

impl SingleAuthorityJournalRecord {
    fn encode(&self) -> Result<Vec<u8>, ControlPlaneError> {
        let (kind, command) = match &self.command {
            Some(command) => (
                SINGLE_AUTHORITY_JOURNAL_RECORD_COMMAND,
                encode_control_plane_command(command)?,
            ),
            None => (SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKPOINT, Vec::new()),
        };
        let command_len =
            u32::try_from(command.len()).map_err(|_| ControlPlaneError::CommandDecode {
                message: format!(
                    "single-authority control-plane journal command length {} exceeds u32::MAX",
                    command.len()
                ),
            })?;
        let mut out = Vec::with_capacity(
            SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len()
                + std::mem::size_of::<u16>()
                + CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN
                + 2 * std::mem::size_of::<u64>()
                + std::mem::size_of::<u8>()
                + std::mem::size_of::<u32>()
                + command.len()
                + SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKSUM_LEN,
        );
        out.extend_from_slice(SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC);
        out.extend_from_slice(&SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION.to_be_bytes());
        out.extend_from_slice(&self.binding.0);
        out.extend_from_slice(&self.previous_chain_digest.to_be_bytes());
        out.extend_from_slice(&self.resulting_chain_digest.to_be_bytes());
        out.push(kind);
        out.extend_from_slice(&command_len.to_be_bytes());
        out.extend_from_slice(&command);
        out.extend_from_slice(&checksum::crc64::checksum(&out).to_be_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        let fixed_len = SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len()
            + std::mem::size_of::<u16>()
            + CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN
            + 2 * std::mem::size_of::<u64>()
            + std::mem::size_of::<u8>()
            + std::mem::size_of::<u32>()
            + SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKSUM_LEN;
        if bytes.len() < fixed_len {
            return Err(ControlPlaneError::CommandDecode {
                message: "truncated single-authority control-plane journal record".to_owned(),
            });
        }
        let (body, checksum_bytes) =
            bytes.split_at(bytes.len() - SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKSUM_LEN);
        let expected_checksum = u64::from_be_bytes(
            checksum_bytes
                .try_into()
                .expect("journal record checksum has fixed length"),
        );
        let actual_checksum = checksum::crc64::checksum(body);
        if actual_checksum != expected_checksum {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "single-authority control-plane journal record checksum mismatch: expected {expected_checksum:#x}, actual {actual_checksum:#x}"
                ),
            });
        }
        let mut offset = 0;
        if &body[..SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len()]
            != SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC
        {
            return Err(ControlPlaneError::CommandDecode {
                message: "invalid single-authority control-plane journal record magic".to_owned(),
            });
        }
        offset += SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len();
        let version = u16::from_be_bytes(
            body[offset..offset + std::mem::size_of::<u16>()]
                .try_into()
                .expect("journal record version has fixed length"),
        );
        offset += std::mem::size_of::<u16>();
        if version != SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "unsupported single-authority control-plane journal record version {version}"
                ),
            });
        }
        let mut binding = [0; CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN];
        binding.copy_from_slice(&body[offset..offset + CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN]);
        offset += CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN;
        let previous_chain_digest = u64::from_be_bytes(
            body[offset..offset + std::mem::size_of::<u64>()]
                .try_into()
                .expect("previous chain digest has fixed length"),
        );
        offset += std::mem::size_of::<u64>();
        let resulting_chain_digest = u64::from_be_bytes(
            body[offset..offset + std::mem::size_of::<u64>()]
                .try_into()
                .expect("resulting chain digest has fixed length"),
        );
        offset += std::mem::size_of::<u64>();
        let kind = body[offset];
        offset += std::mem::size_of::<u8>();
        let command_len = u32::from_be_bytes(
            body[offset..offset + std::mem::size_of::<u32>()]
                .try_into()
                .expect("journal command length has fixed length"),
        ) as usize;
        offset += std::mem::size_of::<u32>();
        let command_end =
            offset
                .checked_add(command_len)
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane journal command length overflows usize"
                            .to_owned(),
                })?;
        if command_end != body.len() {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority control-plane journal command length mismatch"
                    .to_owned(),
            });
        }
        let command = match kind {
            SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKPOINT if command_len == 0 => None,
            SINGLE_AUTHORITY_JOURNAL_RECORD_COMMAND if command_len != 0 => {
                Some(decode_control_plane_command(&body[offset..command_end])?)
            }
            SINGLE_AUTHORITY_JOURNAL_RECORD_CHECKPOINT
            | SINGLE_AUTHORITY_JOURNAL_RECORD_COMMAND => {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane journal record kind has invalid command length"
                            .to_owned(),
                });
            }
            _ => {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "invalid single-authority control-plane journal record kind {kind}"
                    ),
                });
            }
        };
        Ok(Self {
            binding: ControlPlaneAuthorityClockCheckpointBinding(binding),
            previous_chain_digest,
            resulting_chain_digest,
            command,
        })
    }
}

#[cfg(test)]
thread_local! {
    static SINGLE_AUTHORITY_SNAPSHOT_DIGEST_COMPUTATIONS: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
}

fn single_authority_snapshot_digest(snapshot: &ClusterControlSnapshot) -> u64 {
    #[cfg(test)]
    SINGLE_AUTHORITY_SNAPSHOT_DIGEST_COMPUTATIONS
        .with(|count| count.set(count.get().saturating_add(1)));
    checksum::crc64::checksum(format_snapshot(snapshot).as_bytes())
}

#[cfg(test)]
fn single_authority_snapshot_digest_computations() -> u64 {
    SINGLE_AUTHORITY_SNAPSHOT_DIGEST_COMPUTATIONS.with(std::cell::Cell::get)
}

fn single_authority_command_chain_digest(
    previous_chain_digest: u64,
    encoded_command: &[u8],
) -> u64 {
    let mut bytes =
        Vec::with_capacity(std::mem::size_of::<u64>().saturating_add(encoded_command.len()));
    bytes.extend_from_slice(&previous_chain_digest.to_be_bytes());
    bytes.extend_from_slice(encoded_command);
    checksum::crc64::checksum(&bytes)
}

impl ControlPlaneStore for FileControlPlaneStore {
    fn load(&self) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
        let mut durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        let read_snapshot_candidate = |path: &Path| -> Result<Option<String>, ControlPlaneError> {
            match std::fs::read_to_string(path) {
                Ok(contents) => Ok(Some(contents)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(source) => Err(ControlPlaneError::io("load control-plane state", source)),
            }
        };
        let published_contents = read_snapshot_candidate(&self.path)?;
        let prepared_path = single_authority_snapshot_tmp_path(&self.path);
        let prepared_contents = read_snapshot_candidate(&prepared_path)?;
        if published_contents.is_none() && prepared_contents.is_none() {
            if single_authority_initialized_path(&self.path).exists() {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority durable identity or journal exists without its control-plane checkpoint"
                            .to_owned(),
                });
            }
            if !self.journal.path().exists() {
                durability.initialized = true;
                durability.initial_identity_created =
                    single_authority_identity_path(&self.path).exists();
                durability.journal_clean_offset = 0;
                durability.commands_since_checkpoint = 0;
                durability.bytes_since_checkpoint = 0;
                durability.first_uncheckpointed_at = None;
                durability.published_snapshot_digest = None;
                durability.published_chain_digest = None;
                return Ok(None);
            }
        };
        if let Some(contents) = published_contents.as_ref() {
            parse_snapshot(contents)?;
        }
        let binding =
            load_single_authority_clock_checkpoint_binding(&self.path)?.ok_or_else(|| {
                ControlPlaneError::AuthorityClockCheckpoint {
                    message: "existing single-authority state is missing its durable identity"
                        .to_owned(),
                }
            })?;
        let initialized_binding = load_single_authority_initialized_binding(&self.path)?;
        if initialized_binding.is_some_and(|initialized_binding| initialized_binding != binding) {
            return Err(ControlPlaneError::CommandDecode {
                message:
                    "single-authority initialization marker identity does not match durable state"
                        .to_owned(),
            });
        }
        if published_contents.is_none()
            && !self.journal.path().exists()
            && initialized_binding.is_none()
        {
            durability.initialized = true;
            durability.initial_identity_created = true;
            durability.journal_clean_offset = 0;
            durability.commands_since_checkpoint = 0;
            durability.bytes_since_checkpoint = 0;
            durability.first_uncheckpointed_at = None;
            durability.published_snapshot_digest = None;
            durability.published_chain_digest = None;
            return Ok(None);
        }
        let incomplete_initialization =
            published_contents.is_none() && initialized_binding.is_none();
        let frames = match self
            .journal
            .status_offsets()
            .and_then(|offsets| self.journal.read_frames_from(offsets.base_offset))
        {
            Ok(frames) => frames,
            Err(_) if incomplete_initialization => {
                self.discard_incomplete_initialization_locked(&mut durability)?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let mut records = Vec::with_capacity(frames.frames.len());
        for frame in frames.frames {
            let record = match SingleAuthorityJournalRecord::decode(&frame) {
                Ok(record) => record,
                Err(_) if incomplete_initialization => {
                    self.discard_incomplete_initialization_locked(&mut durability)?;
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            if record.binding != binding {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane journal identity does not match durable state"
                            .to_owned(),
                });
            }
            records.push(record);
        }
        if incomplete_initialization && !records.iter().any(|record| record.command.is_none()) {
            self.discard_incomplete_initialization_locked(&mut durability)?;
            return Ok(None);
        }
        let (checkpoint_contents, checkpoint_digest, replay_start, prepared_checkpoint) = [
            (published_contents.as_ref(), false),
            (prepared_contents.as_ref(), true),
        ]
        .into_iter()
        .filter_map(|(contents, prepared)| {
            let contents = contents?;
            let digest = checksum::crc64::checksum(contents.as_bytes());
            records
                .iter()
                .enumerate()
                .filter_map(|(index, record)| {
                    (record.command.is_none() && record.resulting_chain_digest == digest)
                        .then_some(index + 1)
                })
                .next_back()
                .map(|replay_start| (contents, digest, replay_start, prepared))
        })
        .max_by_key(|(_, _, replay_start, _)| *replay_start)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message:
                    "single-authority control-plane journal has no identity-bound checkpoint anchor for the durable snapshot"
                        .to_owned(),
            })?;
        let mut snapshot = parse_snapshot(checkpoint_contents)?;
        let mut chain_digest = checkpoint_digest;
        for record in &records[replay_start..] {
            let Some(command) = &record.command else {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane journal has an unexpected checkpoint anchor in its replay suffix"
                            .to_owned(),
                });
            };
            if chain_digest != record.previous_chain_digest {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane journal command chain is discontinuous"
                            .to_owned(),
                });
            }
            let encoded_command = encode_control_plane_command(command)?;
            let expected_resulting_chain_digest =
                single_authority_command_chain_digest(chain_digest, &encoded_command);
            if record.resulting_chain_digest != expected_resulting_chain_digest {
                return Err(ControlPlaneError::CommandDecode {
                    message: "single-authority control-plane journal command chain digest mismatch"
                        .to_owned(),
                });
            }
            let applied = snapshot
                .apply_control_plane_command(command.clone())
                .map_err(|error| ControlPlaneError::CommandDecode {
                    message: format!(
                        "single-authority control-plane journal command was rejected during replay: {error}"
                    ),
                })?;
            if !applied.changed() {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane journal contains a non-mutating command"
                            .to_owned(),
                });
            }
            snapshot = applied.into_snapshot();
            // Command application records history and validates the resulting publication.
            // Repeating either operation here is both redundant and O(retained history).
            chain_digest = record.resulting_chain_digest;
        }
        if frames.truncated_tail {
            self.journal.truncate_to_clean_len(frames.clean_len)?;
        }
        if prepared_checkpoint {
            self.publish_prepared_snapshot_file(&prepared_path)?;
        }
        durability.initialized = true;
        durability.journal_clean_offset = frames.clean_len;
        durability.commands_since_checkpoint = u64::try_from(
            records[replay_start..]
                .iter()
                .filter(|record| record.command.is_some())
                .count(),
        )
        .unwrap_or(u64::MAX);
        durability.bytes_since_checkpoint = records[replay_start..]
            .iter()
            .filter(|record| record.command.is_some())
            .map(|record| {
                record.encode().map(|frame| {
                    DurableJournalFile::<SingleAuthorityJournalObserver>::framed_len(frame.len())
                })
            })
            .try_fold(0u64, |total, frame_len| {
                frame_len.map(|frame_len| total.saturating_add(frame_len))
            })?;
        durability.first_uncheckpointed_at =
            (durability.commands_since_checkpoint != 0).then(Instant::now);
        durability.published_snapshot_digest = Some(single_authority_snapshot_digest(&snapshot));
        durability.published_chain_digest = Some(chain_digest);
        Ok(Some(snapshot))
    }

    fn checkpoint(
        &self,
        previous_snapshot: Option<&ClusterControlSnapshot>,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        let _publication =
            self.checkpoint_publication
                .lock()
                .map_err(|_| ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint publication lock poisoned".to_owned(),
                })?;
        let save_started = Instant::now();
        let result = (|| {
            let mut durability = self.lock_durability()?;
            self.ensure_healthy_locked(&durability)?;
            let expected_digest = previous_snapshot.map(single_authority_snapshot_digest);
            if durability.published_snapshot_digest != expected_digest {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane checkpoint base does not match the latest durable snapshot"
                            .to_owned(),
                });
            }
            self.checkpoint_snapshot_locked(next_snapshot, &mut durability)
        })();
        observability::record_control_plane_snapshot_save(save_started.elapsed(), result.is_ok());
        result
    }

    fn commit_command(
        &self,
        previous_snapshot: &ClusterControlSnapshot,
        command: &ControlPlaneCommand,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        #[cfg(test)]
        self.signal_commit_before_durability_lock_if_armed();
        let mut durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        if !durability.initialized {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority control-plane store was not initialized before commit"
                    .to_owned(),
            });
        }
        if previous_snapshot == next_snapshot {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority control-plane journal refused a non-mutating command"
                    .to_owned(),
            });
        }
        let previous_chain_digest =
            durability
                .published_chain_digest
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message:
                        "single-authority control-plane command has no published journal chain"
                            .to_owned(),
                })?;
        let encoded_command = encode_control_plane_command(command)?;
        let resulting_chain_digest =
            single_authority_command_chain_digest(previous_chain_digest, &encoded_command);
        let binding =
            load_single_authority_clock_checkpoint_binding(&self.path)?.ok_or_else(|| {
                ControlPlaneError::AuthorityClockCheckpoint {
                    message: "single-authority durable identity is missing before journal append"
                        .to_owned(),
                }
            })?;
        let record = SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest,
            resulting_chain_digest,
            command: Some(command.clone()),
        };
        let encoded = record.encode()?;
        let encoded_frame_len =
            DurableJournalFile::<SingleAuthorityJournalObserver>::framed_len(encoded.len());
        let next_journal_clean_offset = durability
            .journal_clean_offset
            .checked_add(encoded_frame_len)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "single-authority control-plane journal offset overflows".to_owned(),
            })?;
        if let Err(error) = self.journal.append_frame(&encoded) {
            return match error {
                DurableJournalAppendError::BeforeReplayableRecord(error) => Err(error),
                DurableJournalAppendError::AmbiguousRecordMayExist(error)
                | DurableJournalAppendError::ReplayableRecordMayExist(error) => {
                    Self::latch_durability_failure(&mut durability, &error);
                    let result = Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "single-authority control-plane durability poisoned after ambiguous journal append: {error}"
                        ),
                    });
                    drop(durability);
                    Self::log_durability_failure("journal_append", &error);
                    result
                }
            };
        }
        durability.published_snapshot_digest = None;
        durability.published_chain_digest = Some(resulting_chain_digest);
        durability.journal_clean_offset = next_journal_clean_offset;
        durability.commands_since_checkpoint =
            durability.commands_since_checkpoint.saturating_add(1);
        durability.bytes_since_checkpoint = durability
            .bytes_since_checkpoint
            .saturating_add(encoded_frame_len);
        if durability.first_uncheckpointed_at.is_none() {
            durability.first_uncheckpointed_at = Some(Instant::now());
        }
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<(), ControlPlaneError> {
        let durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)
    }

    #[cfg(test)]
    fn checkpoint_manually_modified_snapshot_for_test(
        &self,
        _previous_snapshot: &ClusterControlSnapshot,
        next_snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        let _publication =
            self.checkpoint_publication
                .lock()
                .map_err(|_| ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint publication lock poisoned".to_owned(),
                })?;
        let mut durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        self.checkpoint_snapshot_locked(next_snapshot, &mut durability)
    }
}

impl FileControlPlaneStore {
    fn durability_failure_log_message(stage: &'static str, error: &ControlPlaneError) -> String {
        format!(
            "single-authority control-plane durability failure stage={stage}: {}",
            error.retained_diagnostic_message()
        )
    }

    fn latch_durability_failure(
        durability: &mut FileControlPlaneStoreDurability,
        error: &ControlPlaneError,
    ) {
        durability.poisoned.get_or_insert_with(|| error.to_string());
    }

    fn write_durability_failure(
        stage: &'static str,
        error: &ControlPlaneError,
        output: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        writeln!(
            output,
            "{}",
            Self::durability_failure_log_message(stage, error)
        )
    }

    fn log_durability_failure(stage: &'static str, error: &ControlPlaneError) {
        let stderr = std::io::stderr();
        let mut stderr = stderr.lock();
        let _ = Self::write_durability_failure(stage, error, &mut stderr);
    }

    fn lock_durability(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, FileControlPlaneStoreDurability>, ControlPlaneError> {
        self.durability
            .lock()
            .map_err(|_| ControlPlaneError::CommandDecode {
                message: "single-authority control-plane durability lock poisoned".to_owned(),
            })
    }

    fn ensure_healthy_locked(
        &self,
        durability: &FileControlPlaneStoreDurability,
    ) -> Result<(), ControlPlaneError> {
        if let Some(reason) = &durability.poisoned {
            return Err(ControlPlaneError::CommandDecode {
                message: format!("single-authority control-plane durability is poisoned: {reason}"),
            });
        }
        Ok(())
    }

    fn capture_checkpoint_if_due(
        &self,
        now: Instant,
    ) -> Result<Option<FileControlPlaneCheckpointCapture>, ControlPlaneError> {
        let durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        let Some(first_uncheckpointed_at) = durability.first_uncheckpointed_at else {
            return Ok(None);
        };
        if durability.commands_since_checkpoint < self.checkpoint_command_limit
            && durability.bytes_since_checkpoint < self.checkpoint_byte_limit
            && now.saturating_duration_since(first_uncheckpointed_at) < self.checkpoint_interval
        {
            return Ok(None);
        }
        let chain_digest =
            durability
                .published_chain_digest
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint capture has no published journal chain"
                        .to_owned(),
                })?;
        Ok(Some(FileControlPlaneCheckpointCapture {
            store_instance: Arc::clone(&self.durability),
            checkpoint_generation: durability.checkpoint_generation,
            journal_offset: durability.journal_clean_offset,
            chain_digest,
            captured_at: now,
        }))
    }

    fn persist_captured_checkpoint(
        &self,
        capture: FileControlPlaneCheckpointCapture,
        snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        let _publication =
            self.checkpoint_publication
                .lock()
                .map_err(|_| ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint publication lock poisoned".to_owned(),
                })?;
        self.validate_checkpoint_capture(&capture)?;
        let save_started = Instant::now();
        let result = self.persist_captured_checkpoint_inner(capture, snapshot);
        observability::record_control_plane_snapshot_save(save_started.elapsed(), result.is_ok());
        if let Err(error) = &result {
            if let Ok(mut durability) = self.lock_durability() {
                Self::latch_durability_failure(&mut durability, error);
            }
        }
        drop(_publication);
        if let Err(error) = &result {
            Self::log_durability_failure("checkpoint_persistence", error);
        }
        result
    }

    fn validate_checkpoint_capture(
        &self,
        capture: &FileControlPlaneCheckpointCapture,
    ) -> Result<(), ControlPlaneError> {
        let durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        if !Arc::ptr_eq(&capture.store_instance, &self.durability) {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority checkpoint capture belongs to another store instance"
                    .to_owned(),
            });
        }
        if capture.checkpoint_generation != durability.checkpoint_generation {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority checkpoint capture is stale".to_owned(),
            });
        }
        if capture.journal_offset > durability.journal_clean_offset {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority checkpoint capture is beyond the published journal"
                    .to_owned(),
            });
        }
        let offsets = self.journal.status_offsets()?;
        if capture.journal_offset < offsets.base_offset
            || capture.journal_offset > offsets.clean_len
        {
            return Err(ControlPlaneError::CommandDecode {
                message: "single-authority checkpoint capture is outside the retained journal"
                    .to_owned(),
            });
        }
        Ok(())
    }

    fn persist_captured_checkpoint_inner(
        &self,
        capture: FileControlPlaneCheckpointCapture,
        snapshot: &ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        let (prepared_path, snapshot_digest) = self.write_prepared_snapshot_file(snapshot)?;

        let mut durability = self.lock_durability()?;
        self.ensure_healthy_locked(&durability)?;
        let publication_result = (|| {
            debug_assert_eq!(
                capture.checkpoint_generation, durability.checkpoint_generation,
                "checkpoint publication lock prevents another checkpoint"
            );
            let suffix = self.journal.read_frames_from(capture.journal_offset)?;
            if suffix.truncated_tail {
                return Err(ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint found a torn live journal suffix"
                        .to_owned(),
                });
            }

            let binding =
                load_single_authority_clock_checkpoint_binding(&self.path)?.ok_or_else(|| {
                    ControlPlaneError::AuthorityClockCheckpoint {
                    message:
                        "single-authority durable identity is missing before checkpoint anchoring"
                            .to_owned(),
                }
                })?;
            let anchor = SingleAuthorityJournalRecord {
                binding,
                previous_chain_digest: capture.chain_digest,
                resulting_chain_digest: snapshot_digest,
                command: None,
            }
            .encode()?;
            let mut replacement = vec![anchor];
            let mut original_chain_digest = capture.chain_digest;
            let mut resulting_chain_digest = snapshot_digest;
            let mut suffix_bytes = 0u64;
            for frame in suffix.frames {
                let record = SingleAuthorityJournalRecord::decode(&frame)?;
                if record.binding != binding {
                    return Err(ControlPlaneError::CommandDecode {
                        message:
                            "single-authority checkpoint suffix belongs to another durable identity"
                                .to_owned(),
                    });
                }
                let command =
                    record.command.ok_or_else(|| {
                        ControlPlaneError::CommandDecode {
                    message:
                        "single-authority checkpoint suffix contains an unexpected checkpoint anchor"
                            .to_owned(),
                }
                    })?;
                if record.previous_chain_digest != original_chain_digest {
                    return Err(ControlPlaneError::CommandDecode {
                        message:
                            "single-authority checkpoint suffix command chain is discontinuous"
                                .to_owned(),
                    });
                }
                let encoded_command = encode_control_plane_command(&command)?;
                let expected_original_digest =
                    single_authority_command_chain_digest(original_chain_digest, &encoded_command);
                if record.resulting_chain_digest != expected_original_digest {
                    return Err(ControlPlaneError::CommandDecode {
                        message: "single-authority checkpoint suffix command chain digest mismatch"
                            .to_owned(),
                    });
                }
                original_chain_digest = record.resulting_chain_digest;
                let rebased_digest =
                    single_authority_command_chain_digest(resulting_chain_digest, &encoded_command);
                let rebased = SingleAuthorityJournalRecord {
                    binding,
                    previous_chain_digest: resulting_chain_digest,
                    resulting_chain_digest: rebased_digest,
                    command: Some(command),
                }
                .encode()?;
                suffix_bytes = suffix_bytes.saturating_add(DurableJournalFile::<
                    SingleAuthorityJournalObserver,
                >::framed_len(
                    rebased.len()
                ));
                replacement.push(rebased);
                resulting_chain_digest = rebased_digest;
            }
            if durability.published_chain_digest != Some(original_chain_digest) {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "single-authority checkpoint suffix does not reach the published journal chain"
                            .to_owned(),
                });
            }
            let replacement_bytes = replacement.iter().try_fold(0u64, |total, frame| {
                total
                    .checked_add(
                        DurableJournalFile::<SingleAuthorityJournalObserver>::framed_len(
                            frame.len(),
                        ),
                    )
                    .ok_or_else(|| ControlPlaneError::CommandDecode {
                        message: "single-authority checkpoint replacement length overflows"
                            .to_owned(),
                    })
            })?;
            let replaced_clean_offset = capture
                .journal_offset
                .checked_add(replacement_bytes)
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: "single-authority checkpoint journal offset overflows".to_owned(),
                })?;

            self.journal
                .replace_from(capture.journal_offset, suffix.clean_len, &replacement)?;
            #[cfg(test)]
            self.wait_after_checkpoint_journal_replacement_if_requested();
            #[cfg(test)]
            if self
                .fail_checkpoint_after_anchor
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(ControlPlaneError::io(
                    "publish prepared single-authority control-plane checkpoint",
                    std::io::Error::other(
                        "injected failure after single-authority checkpoint anchor",
                    ),
                ));
            }
            self.publish_prepared_snapshot_file(&prepared_path)?;
            if load_single_authority_initialized_binding(&self.path)?.is_none() {
                store_single_authority_initialized_binding(&self.path, binding)?;
            }

            let suffix_commands =
                u64::try_from(replacement.len().saturating_sub(1)).unwrap_or(u64::MAX);
            durability.initialized = true;
            durability.initial_identity_created = false;
            durability.journal_clean_offset = replaced_clean_offset;
            durability.commands_since_checkpoint = suffix_commands;
            durability.bytes_since_checkpoint = suffix_bytes;
            durability.first_uncheckpointed_at =
                (suffix_commands != 0).then_some(capture.captured_at);
            durability.checkpoint_generation = durability.checkpoint_generation.saturating_add(1);
            durability.published_snapshot_digest =
                (suffix_commands == 0).then_some(snapshot_digest);
            durability.published_chain_digest = Some(resulting_chain_digest);
            Ok(())
        })();
        if let Err(error) = &publication_result {
            durability.poisoned = Some(error.to_string());
        }
        publication_result
    }

    fn discard_incomplete_initialization_locked(
        &self,
        durability: &mut FileControlPlaneStoreDurability,
    ) -> Result<(), ControlPlaneError> {
        let mut removed = false;
        let prepared_path = single_authority_snapshot_tmp_path(&self.path);
        for path in [self.journal.path(), prepared_path.as_path()] {
            match std::fs::remove_file(path) {
                Ok(()) => removed = true,
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(ControlPlaneError::io(
                        "discard incomplete single-authority initialization",
                        source,
                    ));
                }
            }
        }
        if removed {
            std::fs::File::open(state_parent(&self.path))
                .and_then(|directory| directory.sync_all())
                .map_err(|source| {
                    ControlPlaneError::io(
                        "sync discarded single-authority initialization directory",
                        source,
                    )
                })?;
        }
        durability.initialized = true;
        durability.initial_identity_created = true;
        durability.journal_clean_offset = 0;
        durability.commands_since_checkpoint = 0;
        durability.bytes_since_checkpoint = 0;
        durability.first_uncheckpointed_at = None;
        durability.published_snapshot_digest = None;
        durability.published_chain_digest = None;
        Ok(())
    }

    fn checkpoint_snapshot_locked(
        &self,
        snapshot: &ClusterControlSnapshot,
        durability: &mut FileControlPlaneStoreDurability,
    ) -> Result<(), ControlPlaneError> {
        let compact_through = durability.journal_clean_offset;
        let (prepared_path, snapshot_digest) = self.prepare_snapshot_file(snapshot, durability)?;
        let binding =
            load_single_authority_clock_checkpoint_binding(&self.path)?.ok_or_else(|| {
                ControlPlaneError::AuthorityClockCheckpoint {
                    message:
                        "single-authority durable identity is missing before checkpoint anchoring"
                            .to_owned(),
                }
            })?;
        let anchor = SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest: durability.published_chain_digest.unwrap_or(snapshot_digest),
            resulting_chain_digest: snapshot_digest,
            command: None,
        }
        .encode()?;
        let checkpoint_clean_offset = compact_through
            .checked_add(
                DurableJournalFile::<SingleAuthorityJournalObserver>::framed_len(anchor.len()),
            )
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "single-authority checkpoint journal offset overflows".to_owned(),
            })?;
        self.journal
            .append_frame(&anchor)
            .map_err(DurableJournalAppendError::into_control_plane_error)?;
        #[cfg(test)]
        if self
            .fail_checkpoint_after_anchor
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ControlPlaneError::io(
                "publish prepared single-authority control-plane checkpoint",
                std::io::Error::other("injected failure after single-authority checkpoint anchor"),
            ));
        }
        self.publish_prepared_snapshot_file(&prepared_path)?;
        self.journal.compact_through(compact_through)?;
        if load_single_authority_initialized_binding(&self.path)?.is_none() {
            store_single_authority_initialized_binding(&self.path, binding)?;
        }
        durability.initialized = true;
        durability.initial_identity_created = false;
        durability.journal_clean_offset = checkpoint_clean_offset;
        durability.commands_since_checkpoint = 0;
        durability.bytes_since_checkpoint = 0;
        durability.first_uncheckpointed_at = None;
        durability.checkpoint_generation = durability.checkpoint_generation.saturating_add(1);
        durability.published_snapshot_digest = Some(snapshot_digest);
        durability.published_chain_digest = Some(snapshot_digest);
        Ok(())
    }

    fn prepare_snapshot_file(
        &self,
        snapshot: &ClusterControlSnapshot,
        durability: &mut FileControlPlaneStoreDurability,
    ) -> Result<(PathBuf, u64), ControlPlaneError> {
        let state_existed = self.path.exists();
        ensure_control_plane_state_parent_directory(&self.path)?;
        if !state_existed {
            let (binding, identity_created) =
                self.load_or_create_authority_clock_checkpoint_binding_untracked()?;
            durability.initial_identity_created |= identity_created;
            #[cfg(test)]
            if identity_created
                && self
                    .fail_initial_checkpoint_after_identity
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(ControlPlaneError::io(
                    "initialize single-authority control-plane checkpoint",
                    std::io::Error::other(
                        "injected failure after single-authority durable identity creation",
                    ),
                ));
            }
            if self
                .load_authority_clock_restart_checkpoint(binding)?
                .is_none()
            {
                store_authority_clock_restart_checkpoint(
                    &self.path,
                    binding,
                    1,
                    snapshot.max_committed_timestamp_ms(),
                )?;
            }
        }
        self.write_prepared_snapshot_file(snapshot)
    }

    fn write_prepared_snapshot_file(
        &self,
        snapshot: &ClusterControlSnapshot,
    ) -> Result<(PathBuf, u64), ControlPlaneError> {
        ensure_control_plane_state_parent_directory(&self.path)?;
        let serialize_started = Instant::now();
        let formatted_snapshot = format_snapshot(snapshot);
        let snapshot_digest = checksum::crc64::checksum(formatted_snapshot.as_bytes());
        observability::record_control_plane_snapshot_serialization(
            serialize_started.elapsed(),
            formatted_snapshot.len(),
        );
        let tmp_path = single_authority_snapshot_tmp_path(&self.path);
        {
            let mut tmp_file = std::fs::File::create(&tmp_path)
                .map_err(|source| ControlPlaneError::io("create control-plane state", source))?;
            tmp_file
                .write_all(formatted_snapshot.as_bytes())
                .map_err(|source| ControlPlaneError::io("write control-plane state", source))?;
            let sync_started = Instant::now();
            let sync_result = tmp_file.sync_all();
            observability::record_control_plane_snapshot_sync(sync_started.elapsed());
            sync_result
                .map_err(|source| ControlPlaneError::io("sync control-plane state", source))?;
        }
        let sync_started = Instant::now();
        let sync_result =
            std::fs::File::open(state_parent(&tmp_path)).and_then(|directory| directory.sync_all());
        observability::record_control_plane_snapshot_sync(sync_started.elapsed());
        sync_result.map_err(|source| {
            ControlPlaneError::io("sync prepared control-plane state directory", source)
        })?;
        #[cfg(test)]
        if self
            .fail_checkpoint_after_prepared_snapshot_sync
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ControlPlaneError::io(
                "anchor prepared single-authority control-plane checkpoint",
                std::io::Error::other(
                    "injected failure after prepared single-authority checkpoint sync",
                ),
            ));
        }
        Ok((tmp_path, snapshot_digest))
    }

    fn publish_prepared_snapshot_file(
        &self,
        prepared_path: &Path,
    ) -> Result<(), ControlPlaneError> {
        std::fs::rename(prepared_path, &self.path)
            .map_err(|source| ControlPlaneError::io("commit control-plane state", source))?;
        let sync_started = Instant::now();
        let sync_result = std::fs::File::open(state_parent(&self.path))
            .and_then(|directory| directory.sync_all());
        observability::record_control_plane_snapshot_sync(sync_started.elapsed());
        sync_result.map_err(|source| {
            ControlPlaneError::io("sync control-plane state directory", source)
        })?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct SingleAuthorityControlPlane<S> {
    store: S,
    durable_snapshot: ClusterControlSnapshot,
    snapshot: ClusterControlSnapshot,
    runtime_map_content_certificate: Mutex<Option<RuntimeMapContentCertificate>>,
}

impl SingleAuthorityControlPlane<FileControlPlaneStore> {
    pub fn capture_durable_checkpoint_if_due(
        &self,
        now: Instant,
    ) -> Result<Option<SingleAuthorityDurableCheckpoint>, ControlPlaneError> {
        let Some(capture) = self.store.capture_checkpoint_if_due(now)? else {
            return Ok(None);
        };
        Ok(Some(SingleAuthorityDurableCheckpoint {
            store: self.store.clone(),
            capture,
            snapshot: self.durable_snapshot.clone(),
        }))
    }
}

impl<S: ControlPlaneStore> SingleAuthorityControlPlane<S> {
    pub fn open(store: S) -> Result<Self, ControlPlaneError> {
        let loaded = store.load()?;
        let (previous_snapshot, snapshot) = match loaded {
            Some(mut snapshot) => {
                let previous_snapshot = snapshot.clone();
                snapshot.bump_authority_after_restart()?;
                snapshot.record_history_from(&previous_snapshot);
                (Some(previous_snapshot), snapshot)
            }
            None => (None, ClusterControlSnapshot::empty()),
        };
        if let Some(previous_snapshot) = previous_snapshot.as_ref() {
            debug_assert_ne!(
                single_authority_snapshot_digest(previous_snapshot),
                single_authority_snapshot_digest(&snapshot)
            );
        }
        validate_control_plane_snapshot(
            "attempted to open invalid control-plane snapshot",
            &snapshot,
        )?;
        store.checkpoint(previous_snapshot.as_ref(), &snapshot)?;
        Ok(Self {
            store,
            durable_snapshot: snapshot.clone(),
            snapshot,
            runtime_map_content_certificate: Mutex::new(None),
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
    }

    fn apply_and_commit_command(
        &mut self,
        mut command: ControlPlaneCommand,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        self.store.ensure_healthy()?;
        self.promote_volatile_heartbeat_leases_if_needed()?;
        command = self
            .snapshot
            .bind_metadata_transfer_fence_command(&self.durable_snapshot, command)?;
        let live_applied = self.snapshot.apply_control_plane_command(command.clone())?;
        let durable_applied = self
            .durable_snapshot
            .apply_control_plane_command(command.clone())?;
        if live_applied.changed() != durable_applied.changed() {
            return Err(ControlPlaneError::SnapshotInvariantViolation {
                context: "single-authority volatile heartbeat command rebase",
                message: "command mutation outcome differs between live and durable state"
                    .to_owned(),
            });
        }
        self.commit_rebased_command(command, live_applied, durable_applied)
    }

    fn apply_heartbeat_command(
        &mut self,
        command: ControlPlaneCommand,
    ) -> Result<(), ControlPlaneError> {
        self.store.ensure_healthy()?;
        if let Some(next_snapshot) = self
            .snapshot
            .apply_covered_volatile_heartbeat(command.clone())?
        {
            self.snapshot = next_snapshot;
            return Ok(());
        }
        self.promote_volatile_heartbeat_leases_if_needed()?;
        let applied = self.snapshot.apply_control_plane_command(command.clone())?;
        if applied.changed() {
            let durable_applied = self
                .durable_snapshot
                .apply_control_plane_command(command.clone())?;
            self.commit_rebased_command(command, applied, durable_applied)?;
        }
        Ok(())
    }

    pub fn set_node_membership(
        &mut self,
        node_id: NodeId,
        membership: NodeMembershipState,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::SetNodeMembership {
            node_id,
            membership,
        })?;
        Ok(self.snapshot.clone())
    }

    pub fn mark_node_availability(
        &mut self,
        node_id: NodeId,
        availability: NodeAvailabilityState,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::MarkNodeAvailability {
            node_id,
            availability,
        })?;
        Ok(self.snapshot.clone())
    }

    pub fn bootstrap_initial_cluster_map(
        &mut self,
        nodes: Vec<(NodeId, String)>,
        pg_ids: Vec<PgId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::BootstrapInitialClusterMap {
            nodes,
            pg_ids,
        })?;
        Ok(self.snapshot.clone())
    }

    /// Establish the environment-configured uncertified initial topology if
    /// this authority has no control-plane state yet.
    ///
    /// `Ok(None)` means either no storage nodes were configured or an initial
    /// topology was already present. `Ok(Some(epoch))` means this call
    /// durably established the supplied topology at the returned logical
    /// cluster epoch.
    pub fn establish_uncertified_initial_control_plane_topology(
        &mut self,
        topology: &UncertifiedInitialControlPlaneTopology,
    ) -> Result<Option<u64>, ControlPlaneError> {
        if topology.initialized_epoch(&self.snapshot).is_some() {
            return Ok(None);
        }
        let Some(command) = topology.bootstrap_command() else {
            return Ok(None);
        };
        self.apply_and_commit_command(command)?;
        Ok(Some(self.snapshot.cluster_epoch().get()))
    }

    pub fn set_pg_acting_set(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::SetPgActingSet { pg_id, acting_set })?;
        Ok(self.snapshot.clone())
    }

    pub fn set_pg_acting_set_with_metadata_transfer(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let expected_destination_epoch = next_epoch(self.snapshot.cluster_epoch())?;
        self.set_pg_acting_set_with_metadata_transfer_at_epoch(
            pg_id,
            acting_set,
            transfer,
            expected_destination_epoch,
        )
    }

    pub fn set_pg_acting_set_with_metadata_transfer_at_epoch(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
            pg_id,
            acting_set,
            transfer,
            expected_destination_epoch,
        })?;
        Ok(self.snapshot.clone())
    }

    pub fn fence_pg_for_metadata_transfer(
        &mut self,
        pg_id: PgId,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        Ok(self
            .fence_pg_for_metadata_transfer_with_source_lease(pg_id)?
            .into_parts()
            .0)
    }

    pub fn fence_pg_for_metadata_transfer_with_source_lease(
        &mut self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
        let applied =
            self.apply_and_commit_command(ControlPlaneCommand::FencePgForMetadataTransfer {
                pg_id,
                source_primary_lease_deadline_ms: None,
                lease_horizon_authority: None,
            })?;
        let source_primary_lease_deadline_ms = match applied.response() {
            ControlPlaneCommandResponse::FencePgForMetadataTransfer {
                source_primary_lease_deadline_ms,
            } => *source_primary_lease_deadline_ms,
            _ => unreachable!("metadata transfer fence command returned the wrong response"),
        };
        Ok(FencedPgMetadataTransferSnapshot::new(
            self.snapshot.clone(),
            source_primary_lease_deadline_ms,
        ))
    }

    pub fn set_pg_state(
        &mut self,
        pg_id: PgId,
        state: PgState,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::SetPgState { pg_id, state })?;
        Ok(self.snapshot.clone())
    }

    pub fn complete_pg_peering(
        &mut self,
        pg_id: PgId,
        primary: NodeId,
        node_incarnation: u64,
        now_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.apply_and_commit_command(ControlPlaneCommand::CompletePgPeering {
            pg_id,
            primary,
            node_incarnation,
            complete_at_ms: now_ms,
        })?;
        Ok(self.snapshot.clone())
    }

    pub fn complete_ready_pg_peerings(
        &mut self,
        now_ms: u64,
    ) -> Result<Vec<PgId>, ControlPlaneError> {
        let ready = self.snapshot.ready_pg_peering_completions(now_ms)?;
        if ready.is_empty() {
            return Ok(Vec::new());
        }

        let pg_ids = ready.iter().map(|completion| completion.pg_id).collect();
        self.apply_and_commit_command(ControlPlaneCommand::CompleteReadyPgPeerings {
            ready_at_ms: now_ms,
            ready,
        })?;
        Ok(pg_ids)
    }

    pub fn heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        self.heartbeat_with_lease_horizon_authority(heartbeat, authority_now_ms, None)
    }

    fn heartbeat_with_lease_horizon_authority(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
        lease_horizon_authority: Option<LeaseHorizonAuthorityBinding>,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        if heartbeat.requested_lease_duration_ms == 0 {
            return Err(ControlPlaneError::InvalidLeaseDuration);
        }
        if heartbeat.requested_lease_duration_ms > MAX_HEARTBEAT_LEASE_MS {
            return Err(ControlPlaneError::LeaseDurationTooLong {
                requested_ms: heartbeat.requested_lease_duration_ms,
                max_ms: MAX_HEARTBEAT_LEASE_MS,
            });
        }
        let lease_deadline_ms = self.snapshot.heartbeat_lease_deadline(
            heartbeat.node_id,
            authority_now_ms,
            heartbeat.requested_lease_duration_ms,
        )?;
        let observed_epoch = heartbeat.observed_epoch;
        let current_epoch = self.snapshot.cluster_epoch;
        let node_id = heartbeat.node_id;
        self.apply_heartbeat_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: authority_now_ms,
            lease_deadline_ms,
            lease_horizon_authority,
        })?;
        let serving = self.snapshot.node(node_id).is_some_and(|record| {
            observed_epoch == current_epoch
                && record.can_serve_primary(self.snapshot.cluster_epoch, authority_now_ms)
        });
        Ok(HeartbeatLease {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            node_id,
            lease_deadline_ms,
            serving,
            snapshot: self.snapshot.clone(),
        })
    }

    fn current_heartbeat_lease_for_node(
        &self,
        node_id: NodeId,
        now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        self.snapshot
            .current_heartbeat_lease_for_node(node_id, now_ms)
    }

    fn refresh_node_heartbeat_internal(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
        lease_horizon_authority: Option<LeaseHorizonAuthorityBinding>,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        let history_reference_validation_epoch = self.snapshot.cluster_epoch();
        let node_id = heartbeat.node_id;
        let requested_observed_epoch = heartbeat.observed_epoch;
        let previous_observed_epoch = self
            .snapshot
            .node(node_id)
            .and_then(NodeControlRecord::last_observed_epoch);
        let mut lease = self.heartbeat_with_lease_horizon_authority(
            heartbeat,
            authority_now_ms,
            lease_horizon_authority,
        )?;
        let just_activated_pgs: BTreeSet<PgId> = self
            .complete_ready_pg_peerings(authority_now_ms)?
            .into_iter()
            .collect();
        if !just_activated_pgs.is_empty() {
            lease = self.current_heartbeat_lease_for_node(node_id, authority_now_ms)?;
        }
        let current_epoch = self.snapshot.cluster_epoch();
        let observed_epoch = [Some(requested_observed_epoch), previous_observed_epoch]
            .into_iter()
            .flatten()
            .filter(|observed_epoch| *observed_epoch <= current_epoch)
            .max()
            .unwrap_or(requested_observed_epoch);
        let runtime_map = self.snapshot.runtime_map_for_storage_node_refresh(
            authority_now_ms,
            node_id,
            observed_epoch,
        )?;
        Ok(ControlPlaneHeartbeatRefresh {
            lease,
            runtime_map,
            history_reference_validation_epoch,
        })
    }

    pub fn expire_heartbeat_leases(
        &mut self,
        now_ms: u64,
    ) -> Result<HeartbeatLeaseExpiry, ControlPlaneError> {
        self.store.ensure_healthy()?;
        self.promote_volatile_heartbeat_leases_if_needed()?;
        let expire_at_ms = self.snapshot.heartbeat_lease_expiry_timestamp(now_ms);
        let command = ControlPlaneCommand::ExpireHeartbeatLeases { expire_at_ms };
        let applied = self.snapshot.apply_control_plane_command(command.clone())?;
        let durable_applied = self
            .durable_snapshot
            .apply_control_plane_command(command.clone())?;
        let ControlPlaneCommandResponse::ExpireHeartbeatLeases {
            expired_nodes,
            peering_pgs,
        } = applied.response()
        else {
            unreachable!("heartbeat lease expiry command returned the wrong response");
        };
        let expired_nodes = expired_nodes.clone();
        let peering_pgs = peering_pgs.clone();
        if !expired_nodes.is_empty() && applied.changed() {
            self.commit_rebased_command(command, applied, durable_applied)?;
        }
        Ok(HeartbeatLeaseExpiry {
            cluster_epoch: self.snapshot.cluster_epoch,
            expired_nodes,
            peering_pgs,
            snapshot: self.snapshot.clone(),
        })
    }

    pub fn authorize_node_service(
        &self,
        node_id: NodeId,
        node_incarnation: u64,
        observed_epoch: ClusterEpoch,
        now_ms: u64,
    ) -> Result<NodeServiceAuthorization, ControlPlaneError> {
        self.store.ensure_healthy()?;
        let record = self
            .snapshot
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
        if observed_epoch != self.snapshot.cluster_epoch {
            return Err(ControlPlaneError::StaleNodeObservedEpoch {
                node_id: node_id.as_u32(),
                observed_epoch,
                current_epoch: self.snapshot.cluster_epoch,
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
        if !record.can_serve_primary(self.snapshot.cluster_epoch, now_ms) {
            return Err(ControlPlaneError::NodeNotServingCurrentEpoch {
                node_id: node_id.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        Ok(NodeServiceAuthorization {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            node_id,
            node_incarnation,
            lease_deadline_ms,
        })
    }

    pub fn validate_node_service_authorization(
        &self,
        authorization: &NodeServiceAuthorization,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        self.store.ensure_healthy()?;
        if authorization.authority_incarnation() != self.snapshot.authority_incarnation {
            return Err(ControlPlaneError::StaleAuthorityIncarnation {
                authority_incarnation: authorization.authority_incarnation(),
                current_authority_incarnation: self.snapshot.authority_incarnation,
            });
        }
        if authorization.cluster_epoch() != self.snapshot.cluster_epoch {
            return Err(ControlPlaneError::StaleAuthorizationEpoch {
                cluster_epoch: authorization.cluster_epoch(),
                current_epoch: self.snapshot.cluster_epoch,
            });
        }
        if authorization.lease_deadline_ms() <= now_ms {
            return Err(ControlPlaneError::NodeLeaseExpired {
                node_id: authorization.node_id().as_u32(),
                now_ms,
                lease_deadline_ms: Some(authorization.lease_deadline_ms()),
            });
        }

        let node_id = authorization.node_id();
        let record = self
            .snapshot
            .nodes
            .get(&node_id)
            .ok_or(ControlPlaneError::UnknownNode {
                node_id: node_id.as_u32(),
            })?;
        if record.node_incarnation != authorization.node_incarnation() {
            return Err(ControlPlaneError::NodeIncarnationMismatch {
                node_id: node_id.as_u32(),
                sender_incarnation: authorization.node_incarnation(),
                current_incarnation: record.node_incarnation,
            });
        }
        let current_lease_deadline_ms =
            record
                .lease_deadline_ms
                .ok_or(ControlPlaneError::NodeLeaseExpired {
                    node_id: node_id.as_u32(),
                    now_ms,
                    lease_deadline_ms: None,
                })?;
        if current_lease_deadline_ms <= now_ms {
            return Err(ControlPlaneError::NodeLeaseExpired {
                node_id: node_id.as_u32(),
                now_ms,
                lease_deadline_ms: Some(current_lease_deadline_ms),
            });
        }
        if !record.can_serve_primary(self.snapshot.cluster_epoch, now_ms) {
            return Err(ControlPlaneError::NodeNotServingCurrentEpoch {
                node_id: node_id.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        Ok(())
    }

    pub fn authorize_pg_primary_service(
        &self,
        pg_id: PgId,
        primary_node_id: NodeId,
        node_incarnation: u64,
        observed_epoch: ClusterEpoch,
        now_ms: u64,
    ) -> Result<PgPrimaryAuthorization, ControlPlaneError> {
        let node_authorization =
            self.authorize_node_service(primary_node_id, node_incarnation, observed_epoch, now_ms)?;
        let record = self
            .snapshot
            .pgs
            .get(&pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state != PgState::Active {
            return Err(ControlPlaneError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
                state: record.state,
            });
        }
        let serving_primary = record
            .active_primary
            .filter(|primary| {
                self.snapshot
                    .node(*primary)
                    .is_some_and(|node| node.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
            })
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
            })?;
        if serving_primary != primary_node_id {
            return Err(ControlPlaneError::NodeNotPgPrimary {
                pg_id: pg_id.get(),
                node_id: primary_node_id.as_u32(),
                primary_node_id: serving_primary.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        validate_pg_primary_active_observation(&self.snapshot, pg_id, primary_node_id)?;
        Ok(PgPrimaryAuthorization {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            pg_id,
            primary_node_id,
            primary_node_incarnation: node_authorization.node_incarnation(),
            lease_deadline_ms: node_authorization.lease_deadline_ms(),
        })
    }

    pub fn authorize_pg_operation(
        &self,
        operation: PgServiceOperation,
        pg_id: PgId,
        primary_node_id: NodeId,
        node_incarnation: u64,
        observed_epoch: ClusterEpoch,
        now_ms: u64,
    ) -> Result<PgOperationAuthorization, ControlPlaneError> {
        let primary = self.authorize_pg_primary_service(
            pg_id,
            primary_node_id,
            node_incarnation,
            observed_epoch,
            now_ms,
        )?;
        Ok(PgOperationAuthorization { operation, primary })
    }

    pub fn validate_pg_operation_authorization(
        &self,
        authorization: &PgOperationAuthorization,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        self.validate_pg_operation_authorization_for(
            authorization,
            authorization.operation(),
            now_ms,
        )
    }

    pub fn validate_pg_operation_authorization_for(
        &self,
        authorization: &PgOperationAuthorization,
        operation: PgServiceOperation,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        if authorization.operation() != operation {
            return Err(ControlPlaneError::PgOperationAuthorizationMismatch {
                expected: operation,
                actual: authorization.operation(),
            });
        }
        self.validate_node_service_authorization(&authorization.primary().into(), now_ms)?;

        let primary_node_id = authorization.primary_node_id();
        let pg_id = authorization.pg_id();
        let pg = self
            .snapshot
            .pgs
            .get(&pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if pg.state != PgState::Active {
            return Err(ControlPlaneError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
                state: pg.state,
            });
        }
        let serving_primary = pg
            .active_primary
            .filter(|primary| {
                self.snapshot
                    .node(*primary)
                    .is_some_and(|node| node.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
            })
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
            })?;
        if serving_primary != primary_node_id {
            return Err(ControlPlaneError::NodeNotPgPrimary {
                pg_id: pg_id.get(),
                node_id: primary_node_id.as_u32(),
                primary_node_id: serving_primary.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        validate_pg_primary_active_observation(&self.snapshot, pg_id, primary_node_id)
    }

    #[must_use]
    pub fn serving_pg_primary(&self, pg_id: PgId, now_ms: u64) -> Option<NodeId> {
        let record = self.snapshot.pgs.get(&pg_id)?;
        if record.state != PgState::Active {
            return None;
        }
        let primary = record.active_primary?;
        self.snapshot
            .node(primary)
            .is_some_and(|node| node.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
            .then_some(())?;
        primary_has_current_pg_state(&self.snapshot, pg_id, primary, PgState::Active)
            .then_some(primary)
    }

    #[must_use]
    pub fn deterministic_pg_primary(
        &self,
        _pg_id: PgId,
        acting_set: &[NodeId],
        now_ms: u64,
    ) -> Option<NodeId> {
        acting_set.iter().copied().find(|node_id| {
            self.snapshot
                .nodes
                .get(node_id)
                .is_some_and(|record| record.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
        })
    }

    #[cfg(test)]
    fn commit_snapshot(
        &mut self,
        mut next_snapshot: ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        next_snapshot.record_history_from(&self.snapshot);
        validate_control_plane_snapshot(
            "attempted to commit invalid control-plane snapshot",
            &next_snapshot,
        )?;
        self.store.checkpoint_manually_modified_snapshot_for_test(
            &self.durable_snapshot,
            &next_snapshot,
        )?;
        self.durable_snapshot = next_snapshot.clone();
        self.snapshot = next_snapshot;
        *self.runtime_map_content_certificate.lock().map_err(|_| {
            ControlPlaneError::rpc_protocol(
                "control-plane runtime-map content certificate lock poisoned".to_owned(),
            )
        })? = None;
        Ok(())
    }

    fn promote_volatile_heartbeat_leases_if_needed(&mut self) -> Result<(), ControlPlaneError> {
        let Some(command) = self
            .snapshot
            .promote_volatile_heartbeat_leases_command(&self.durable_snapshot)?
        else {
            return Ok(());
        };
        let live_applied = self.snapshot.apply_control_plane_command(command.clone())?;
        let durable_applied = self
            .durable_snapshot
            .apply_control_plane_command(command.clone())?;
        self.commit_rebased_command(command, live_applied, durable_applied)?;
        Ok(())
    }

    fn commit_rebased_command(
        &mut self,
        command: ControlPlaneCommand,
        live_applied: AppliedControlPlaneCommand,
        durable_applied: AppliedControlPlaneCommand,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        if !durable_applied.changed() {
            return Ok(live_applied);
        }
        let response = live_applied.response().clone();
        let changed = live_applied.changed();
        // Both applications already record history and validate their resulting snapshots.
        let next_live_snapshot = live_applied.into_snapshot();
        let next_durable_snapshot = durable_applied.into_snapshot();
        self.store
            .commit_command(&self.durable_snapshot, &command, &next_durable_snapshot)?;
        self.durable_snapshot = next_durable_snapshot;
        self.snapshot = next_live_snapshot.clone();
        *self.runtime_map_content_certificate.lock().map_err(|_| {
            ControlPlaneError::rpc_protocol(
                "control-plane runtime-map content certificate lock poisoned".to_owned(),
            )
        })? = None;
        Ok(AppliedControlPlaneCommand::new(
            next_live_snapshot,
            response,
            changed,
        ))
    }
}

impl<S: ControlPlaneStore> ControlPlaneHeartbeatSink for SingleAuthorityControlPlane<S> {
    fn submit_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        self.heartbeat(heartbeat, authority_now_ms)
    }
}

impl<S: ControlPlaneStore> ControlPlaneHeartbeatRuntimeMapSource
    for SingleAuthorityControlPlane<S>
{
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        self.refresh_node_heartbeat_internal(heartbeat, authority_now_ms, None)
    }

    fn refresh_node_heartbeat_with_lease_horizon_authority(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
        lease_horizon_authority: LeaseHorizonAuthorityBinding,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        self.refresh_node_heartbeat_internal(
            heartbeat,
            authority_now_ms,
            Some(lease_horizon_authority),
        )
    }
}

impl<S: ControlPlaneStore> ControlPlaneRuntimeMapSource for SingleAuthorityControlPlane<S> {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.store.ensure_healthy()?;
        self.snapshot.runtime_map(authority_now_ms)
    }

    fn runtime_map_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        self.store.ensure_healthy()?;
        let mut cached = self.runtime_map_content_certificate.lock().map_err(|_| {
            ControlPlaneError::rpc_protocol(
                "control-plane runtime-map content certificate lock poisoned".to_owned(),
            )
        })?;
        if let Some(certificate) = *cached {
            if let Some(status) =
                ControlPlaneRuntimeMapStatus::from_snapshot_with_content_certificate(
                    &self.snapshot,
                    authority_now_ms,
                    RuntimeMapFreshnessProof::SingleAuthority {
                        authority_incarnation: self.snapshot.authority_incarnation(),
                        issued_at_ms: authority_now_ms,
                    },
                    certificate,
                )?
            {
                return Ok(status);
            }
        }
        let runtime_map = self.snapshot.runtime_map(authority_now_ms)?;
        *cached = Some(RuntimeMapContentCertificate::from_snapshot_and_runtime_map(
            &self.snapshot,
            &runtime_map,
        ));
        Ok(ControlPlaneRuntimeMapStatus::from_runtime_map(&runtime_map))
    }

    fn runtime_map_diagnostics_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapDiagnosticSnapshot, ControlPlaneError> {
        self.store.ensure_healthy()?;
        let runtime_map = self.snapshot.runtime_map(authority_now_ms)?;
        let node_leases = self
            .snapshot
            .nodes()
            .map(|node| ControlPlaneRuntimeMapNodeLeaseDiagnostic {
                node_id: node.node_id(),
                lease_deadline_ms: node.lease_deadline_ms(),
            })
            .collect();
        ControlPlaneRuntimeMapDiagnosticSnapshot::new(runtime_map, node_leases)
    }

    fn pending_metadata_command_recoveries(
        &self,
        _authority_now_ms: u64,
    ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
        self.store.ensure_healthy()?;
        Ok(self.snapshot.pending_metadata_command_recoveries())
    }

    fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.store.ensure_healthy()?;
        self.snapshot
            .reconstructed_runtime_map_for_pg_with_fallback_validity(
                pg_id,
                non_serving_runtime_map_validity(authority_now_ms),
            )
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.store.ensure_healthy()?;
        self.snapshot
            .serving_runtime_map_for_pg_with_freshness_proof(
                pg_id,
                authority_now_ms,
                RuntimeMapFreshnessProof::SingleAuthority {
                    authority_incarnation: self.snapshot.authority_incarnation(),
                    issued_at_ms: authority_now_ms,
                },
            )
    }
}

impl<S: ControlPlaneStore> ControlPlaneLinearizedCommandSink for SingleAuthorityControlPlane<S> {
    fn submit_control_plane_command(
        &mut self,
        command: ControlPlaneCommand,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        self.apply_and_commit_command(command)
    }
}

impl<S: ControlPlaneStore> ControlPlaneLinearizedRuntimeMapSource
    for SingleAuthorityControlPlane<S>
{
    fn linearized_runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.runtime_map_snapshot(authority_now_ms)
    }
}

impl<S: ControlPlaneStore> ControlPlaneAdmin for SingleAuthorityControlPlane<S> {
    fn authority_clock_context(
        &self,
    ) -> Result<ControlPlaneAuthorityClockContext, ControlPlaneError> {
        self.store.ensure_healthy()?;
        Ok(ControlPlaneAuthorityClockContext::new(
            self.snapshot().max_committed_timestamp_ms(),
            None,
            true,
            true,
        ))
    }

    fn set_pg_acting_set(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::set_pg_acting_set(self, pg_id, acting_set)
    }

    fn fence_pg_for_metadata_transfer(
        &mut self,
        pg_id: PgId,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::fence_pg_for_metadata_transfer(self, pg_id)
    }

    fn fence_pg_for_metadata_transfer_with_source_lease(
        &mut self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::fence_pg_for_metadata_transfer_with_source_lease(self, pg_id)
    }

    fn set_pg_acting_set_with_metadata_transfer(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        SingleAuthorityControlPlane::set_pg_acting_set_with_metadata_transfer_at_epoch(
            self,
            pg_id,
            acting_set,
            transfer,
            expected_destination_epoch,
        )
    }
}

#[derive(Clone)]
pub struct UnixControlPlaneClient {
    endpoints: Arc<[ControlPlaneRpcClientEndpoint]>,
    socket_paths: Arc<[PathBuf]>,
    preferred_endpoint_index: Arc<AtomicUsize>,
}

/// A configured endpoint for the logical control-plane RPC client.
///
/// Framing, protocol limits, TLS profile construction, ALPN, deadlines, and
/// request-publication tracking remain owned by `storage`.
#[derive(Clone)]
pub struct ControlPlaneRpcClientEndpoint(ControlPlaneRpcClientEndpointKind);

#[derive(Clone)]
enum ControlPlaneRpcClientEndpointKind {
    Unix {
        socket_path: PathBuf,
    },
    TlsTcp {
        advertised_endpoint: String,
        host: String,
        port: u16,
        server_name: String,
        connect_timeout: Duration,
        tls_client_config: Arc<rustls::ClientConfig>,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlPlaneRpcClientEndpointError {
    #[error("control-plane TLS/TCP advertised endpoint must not be empty")]
    EmptyAdvertisedEndpoint,
    #[error("control-plane TLS/TCP host must not be empty")]
    EmptyHost,
    #[error("control-plane TLS/TCP endpoint has an invalid TLS server name")]
    InvalidServerName,
    #[error("failed to construct the control-plane TLS client profile")]
    TlsProfileUnavailable,
}

impl ControlPlaneRpcClientEndpoint {
    #[must_use]
    pub fn unix(socket_path: impl Into<PathBuf>) -> Self {
        Self(ControlPlaneRpcClientEndpointKind::Unix {
            socket_path: socket_path.into(),
        })
    }

    pub fn tls_tcp(
        advertised_endpoint: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        server_name: impl Into<String>,
        connect_timeout: Duration,
        trust_roots: rustls::RootCertStore,
    ) -> Result<Self, ControlPlaneRpcClientEndpointError> {
        let advertised_endpoint = advertised_endpoint.into();
        if advertised_endpoint.is_empty() {
            return Err(ControlPlaneRpcClientEndpointError::EmptyAdvertisedEndpoint);
        }
        let host = host.into();
        if host.is_empty() {
            return Err(ControlPlaneRpcClientEndpointError::EmptyHost);
        }
        let server_name = server_name.into();
        ServerName::try_from(server_name.clone())
            .map_err(|_| ControlPlaneRpcClientEndpointError::InvalidServerName)?;
        let mut tls_client_config =
            rustls::ClientConfig::builder_with_provider(tls_provider::configured_provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|_| ControlPlaneRpcClientEndpointError::TlsProfileUnavailable)?
                .with_root_certificates(trust_roots)
                .with_no_client_auth();
        tls_client_config.alpn_protocols = vec![CONTROL_PLANE_RPC_TLS_ALPN.to_vec()];
        Ok(Self(ControlPlaneRpcClientEndpointKind::TlsTcp {
            advertised_endpoint,
            host,
            port,
            server_name,
            connect_timeout,
            tls_client_config: Arc::new(tls_client_config),
        }))
    }

    #[must_use]
    pub fn advertised_endpoint(&self) -> String {
        match &self.0 {
            ControlPlaneRpcClientEndpointKind::Unix { socket_path } => {
                format!("unix://{}", socket_path.display())
            }
            ControlPlaneRpcClientEndpointKind::TlsTcp {
                advertised_endpoint,
                ..
            } => advertised_endpoint.clone(),
        }
    }

    #[must_use]
    pub fn unix_socket_path(&self) -> Option<&Path> {
        match &self.0 {
            ControlPlaneRpcClientEndpointKind::Unix { socket_path } => Some(socket_path),
            ControlPlaneRpcClientEndpointKind::TlsTcp { .. } => None,
        }
    }
}

impl std::fmt::Debug for ControlPlaneRpcClientEndpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            ControlPlaneRpcClientEndpointKind::Unix { socket_path } => formatter
                .debug_struct("ControlPlaneRpcClientEndpoint::Unix")
                .field("socket_path", socket_path)
                .finish(),
            ControlPlaneRpcClientEndpointKind::TlsTcp {
                advertised_endpoint,
                host,
                port,
                server_name,
                connect_timeout,
                ..
            } => formatter
                .debug_struct("ControlPlaneRpcClientEndpoint::TlsTcp")
                .field("advertised_endpoint", advertised_endpoint)
                .field("host", host)
                .field("port", port)
                .field("server_name", server_name)
                .field("connect_timeout", connect_timeout)
                .field("tls", &true)
                .finish(),
        }
    }
}

/// The operation class accepted by a control-plane RPC server endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControlPlaneRpcServerRole {
    Ordinary,
    AuthorityClockRecovery,
}

impl ControlPlaneRpcServerRole {
    fn accepts(self, request: &VerifiedControlPlaneRpcRequest) -> bool {
        match self {
            Self::Ordinary => !request.is_authority_clock_admin(),
            Self::AuthorityClockRecovery => request.is_authority_clock_admin(),
        }
    }
}

/// Storage-owned gate around publication of a fully encoded RPC response.
///
/// Implementations may delay or reject publication to preserve an external
/// durability invariant, but never receive access to the transport or frame.
/// A successful implementation must invoke `publish` exactly once; the server
/// rejects implementations that return success without publishing or invoke
/// the callback repeatedly.
pub trait ControlPlaneRpcResponsePublication: Send + Sync {
    fn publish(
        &self,
        publish: &mut dyn FnMut() -> Result<(), ControlPlaneError>,
    ) -> Result<(), ControlPlaneError>;
}

fn publish_control_plane_rpc_response(
    publication: Option<&dyn ControlPlaneRpcResponsePublication>,
    publish: &mut dyn FnMut() -> Result<(), ControlPlaneError>,
) -> Result<(), ControlPlaneError> {
    let mut published = false;
    let result = {
        let mut publish_once = || {
            if std::mem::replace(&mut published, true) {
                return Err(ControlPlaneError::rpc_protocol(
                    "control-plane response publication attempted more than once".to_owned(),
                ));
            }
            publish()
        };
        match publication {
            Some(publication) => publication.publish(&mut publish_once),
            None => publish_once(),
        }
    };
    if result.is_ok() && !published {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane response publication completed without publishing".to_owned(),
        ));
    }
    result
}

/// Durable authority-clock checkpoint destination used by the RPC server and
/// the process-level lease-expiry loop.
pub struct ControlPlaneAuthorityClockCheckpointTarget {
    path: PathBuf,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
}

impl std::fmt::Debug for ControlPlaneAuthorityClockCheckpointTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlPlaneAuthorityClockCheckpointTarget")
            .field("checkpoint", &"configured")
            .finish()
    }
}

impl ControlPlaneAuthorityClockCheckpointTarget {
    #[must_use]
    pub fn new(
        path: impl Into<PathBuf>,
        binding: ControlPlaneAuthorityClockCheckpointBinding,
    ) -> Self {
        Self {
            path: path.into(),
            binding,
        }
    }

    pub fn persist_established(
        &self,
        context: ControlPlaneAuthorityClockContext,
        authority_clock: &mut ControlPlaneAuthorityClock,
    ) -> Result<(), ControlPlaneError> {
        if !authority_clock.status(context).established() {
            return Ok(());
        }
        let persistence_started = Instant::now();
        let mut invalidate_elapsed = Duration::ZERO;
        let mut store_elapsed = Duration::ZERO;
        let persistence_result = (|| {
            let invalidate_started = Instant::now();
            invalidate_authority_clock_restart_checkpoint(&self.path)?;
            invalidate_elapsed = invalidate_started.elapsed();
            let store_started = Instant::now();
            store_validated_authority_clock_restart_checkpoint(
                &self.path,
                self.binding,
                context.committed_timestamp_high_water_ms(),
                authority_clock,
            )?;
            store_elapsed = store_started.elapsed();
            Ok::<(), ControlPlaneError>(())
        })();
        let persistence_elapsed = persistence_started.elapsed();
        if persistence_elapsed >= Duration::from_secs(1) {
            eprintln!(
                "control-plane authority-clock checkpoint persistence took {persistence_elapsed:?} \
                 (invalidation {invalidate_elapsed:?}, replacement {store_elapsed:?})"
            );
        }
        if let Err(error) = persistence_result {
            if authority_clock.status(context).established() {
                authority_clock.fail_closed_after_checkpoint_persistence_failure()?;
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn invalidate_if_blocked(
        &self,
        authority_clock: &ControlPlaneAuthorityClock,
    ) -> Result<(), ControlPlaneError> {
        if authority_clock.is_established() {
            return Ok(());
        }
        invalidate_authority_clock_restart_checkpoint(&self.path)
    }
}

#[derive(Clone)]
struct ControlPlaneRpcServerResources {
    active_workers: Arc<AtomicUsize>,
    worker_limit: usize,
    pre_auth_byte_budget: Arc<ControlPlaneRpcPreAuthByteBudget>,
}

/// Logical policy for one class of control-plane RPC endpoints.
///
/// Clones share the worker and pre-authentication memory budgets, allowing a
/// group of listeners to enforce one aggregate resource limit.
#[derive(Clone)]
pub(crate) struct ControlPlaneRpcServerPolicy {
    role: ControlPlaneRpcServerRole,
    resources: ControlPlaneRpcServerResources,
    gate_request_time_with_authority_clock: bool,
    auth_verifier: Option<Arc<ControlPlaneUnixAuthVerifier>>,
    authority_clock: Option<Arc<Mutex<ControlPlaneAuthorityClock>>>,
    authority_clock_checkpoint_target: Option<Arc<ControlPlaneAuthorityClockCheckpointTarget>>,
    authority_confirmation: Option<Arc<dyn Fn() -> Result<(), ControlPlaneError> + Send + Sync>>,
    response_publication: Option<Arc<dyn ControlPlaneRpcResponsePublication>>,
    fatal_error_handler: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(any(test, feature = "test-hooks"))]
    test_authority_now_ms: Option<u64>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ControlPlaneRpcServerConfigError {
    #[error("control-plane RPC worker limit must be positive")]
    ZeroWorkerLimit,
    #[error("control-plane RPC pre-authentication byte budget must be positive")]
    ZeroPreAuthByteBudget,
    #[error("control-plane RPC listener connection limit must be positive")]
    ZeroConnectionLimit,
    #[error("control-plane RPC listener frame limit must include the protocol envelope")]
    InvalidFrameLimit,
    #[error("control-plane RPC listener I/O timeout must be positive")]
    ZeroIoTimeout,
    #[error("control-plane RPC listener I/O timeout exceeds the supported operation bound")]
    IoTimeoutTooLarge,
    #[error("failed to construct the control-plane TLS server profile")]
    TlsProfileUnavailable,
}

impl ControlPlaneRpcServerPolicy {
    pub(crate) fn new(
        role: ControlPlaneRpcServerRole,
        worker_limit: usize,
        pre_auth_byte_budget: usize,
    ) -> Result<Self, ControlPlaneRpcServerConfigError> {
        if worker_limit == 0 {
            return Err(ControlPlaneRpcServerConfigError::ZeroWorkerLimit);
        }
        if pre_auth_byte_budget == 0 {
            return Err(ControlPlaneRpcServerConfigError::ZeroPreAuthByteBudget);
        }
        Ok(Self {
            role,
            resources: ControlPlaneRpcServerResources {
                active_workers: Arc::new(AtomicUsize::new(0)),
                worker_limit,
                pre_auth_byte_budget: Arc::new(ControlPlaneRpcPreAuthByteBudget::new(
                    pre_auth_byte_budget,
                )),
            },
            gate_request_time_with_authority_clock: false,
            auth_verifier: None,
            authority_clock: None,
            authority_clock_checkpoint_target: None,
            authority_confirmation: None,
            response_publication: None,
            fatal_error_handler: None,
            #[cfg(any(test, feature = "test-hooks"))]
            test_authority_now_ms: None,
        })
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_auth_verifier(
        mut self,
        auth_verifier: Arc<ControlPlaneUnixAuthVerifier>,
    ) -> Self {
        self.auth_verifier = Some(auth_verifier);
        self
    }

    #[must_use]
    pub(crate) fn with_server_auth(mut self, auth: &crate::ControlPlaneRpcServerAuth) -> Self {
        self.auth_verifier.clone_from(&auth.verifier);
        self
    }

    pub(crate) fn authentication_required(&self) -> bool {
        self.auth_verifier.is_some()
    }

    #[cfg(test)]
    pub(crate) fn role(&self) -> ControlPlaneRpcServerRole {
        self.role
    }

    #[must_use]
    pub(crate) fn with_authority_clock(
        mut self,
        authority_clock: Arc<Mutex<ControlPlaneAuthorityClock>>,
        checkpoint_target: Arc<ControlPlaneAuthorityClockCheckpointTarget>,
        gate_request_time: bool,
    ) -> Self {
        self.authority_clock = Some(authority_clock);
        self.authority_clock_checkpoint_target = Some(checkpoint_target);
        self.gate_request_time_with_authority_clock = gate_request_time;
        self
    }

    #[must_use]
    pub(crate) fn with_authority_confirmation(
        mut self,
        authority_confirmation: Arc<dyn Fn() -> Result<(), ControlPlaneError> + Send + Sync>,
    ) -> Self {
        self.authority_confirmation = Some(authority_confirmation);
        self
    }

    #[must_use]
    pub(crate) fn with_response_publication(
        mut self,
        response_publication: Arc<dyn ControlPlaneRpcResponsePublication>,
    ) -> Self {
        self.response_publication = Some(response_publication);
        self
    }

    /// Installs the process-lifecycle action used after a fatal durable
    /// checkpoint failure has been diagnosed and logged by storage.
    #[must_use]
    pub(crate) fn with_fatal_error_handler(
        mut self,
        fatal_error_handler: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        self.fatal_error_handler = Some(fatal_error_handler);
        self
    }

    #[must_use]
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn active_workers(&self) -> usize {
        self.resources.active_workers.load(Ordering::Acquire)
    }

    fn authority_now_ms(&self) -> u64 {
        #[cfg(any(test, feature = "test-hooks"))]
        if let Some(authority_now_ms) = self.test_authority_now_ms {
            return authority_now_ms;
        }
        crate::clock::current_time_millis()
    }
}

#[derive(Debug)]
struct ControlPlaneRpcPreAuthByteBudget {
    reserved_bytes: AtomicUsize,
    limit_bytes: usize,
}

impl ControlPlaneRpcPreAuthByteBudget {
    fn new(limit_bytes: usize) -> Self {
        Self {
            reserved_bytes: AtomicUsize::new(0),
            limit_bytes,
        }
    }

    fn reserve(
        self: &Arc<Self>,
        frame_bytes: usize,
    ) -> Result<ControlPlaneRpcPreAuthByteReservation, ControlPlaneError> {
        let result =
            self.reserved_bytes
                .try_update(Ordering::AcqRel, Ordering::Acquire, |reserved| {
                    reserved
                        .checked_add(frame_bytes)
                        .filter(|total| *total <= self.limit_bytes)
                });
        match result {
            Ok(_) => Ok(ControlPlaneRpcPreAuthByteReservation {
                budget: Arc::clone(self),
                frame_bytes,
            }),
            Err(reserved) => Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane RPC pre-authentication frame budget exhausted: requested {frame_bytes} bytes with {reserved} of {} bytes reserved",
                    self.limit_bytes
                ))),
        }
    }
}

#[derive(Debug)]
struct ControlPlaneRpcPreAuthByteReservation {
    budget: Arc<ControlPlaneRpcPreAuthByteBudget>,
    frame_bytes: usize,
}

impl Drop for ControlPlaneRpcPreAuthByteReservation {
    fn drop(&mut self) {
        self.budget
            .reserved_bytes
            .fetch_sub(self.frame_bytes, Ordering::AcqRel);
    }
}

struct ControlPlaneRpcTlsCertificateResolver {
    certified_key: Arc<CertifiedKey>,
}

impl std::fmt::Debug for ControlPlaneRpcTlsCertificateResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlPlaneRpcTlsCertificateResolver")
            .field("certificate", &"configured")
            .finish()
    }
}

impl rustls::server::ResolvesServerCert for ControlPlaneRpcTlsCertificateResolver {
    fn resolve(&self, _client_hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.certified_key))
    }
}

enum ControlPlaneRpcServerListenerKind {
    Unix(UnixListener),
    TlsTcp {
        listener: TcpListener,
        tls_server_config: Arc<rustls::ServerConfig>,
    },
}

/// Opaque bound listener for the storage-owned control-plane RPC server.
pub(crate) struct ControlPlaneRpcServerListener {
    kind: ControlPlaneRpcServerListenerKind,
    max_connections: usize,
    max_frame_bytes: usize,
    io_timeout: Duration,
}

impl std::fmt::Debug for ControlPlaneRpcServerListener {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let transport = match self.kind {
            ControlPlaneRpcServerListenerKind::Unix(_) => "unix",
            ControlPlaneRpcServerListenerKind::TlsTcp { .. } => "tls-tcp",
        };
        formatter
            .debug_struct("ControlPlaneRpcServerListener")
            .field("transport", &transport)
            .field("max_connections", &self.max_connections)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("io_timeout", &self.io_timeout)
            .finish()
    }
}

fn validate_control_plane_rpc_server_listener_limits(
    max_connections: usize,
    max_frame_bytes: usize,
    io_timeout: Duration,
) -> Result<(), ControlPlaneRpcServerConfigError> {
    if max_connections == 0 {
        return Err(ControlPlaneRpcServerConfigError::ZeroConnectionLimit);
    }
    if max_frame_bytes < control_plane_rpc_frame_overhead() {
        return Err(ControlPlaneRpcServerConfigError::InvalidFrameLimit);
    }
    if io_timeout.is_zero() {
        return Err(ControlPlaneRpcServerConfigError::ZeroIoTimeout);
    }
    if io_timeout > CONTROL_PLANE_RPC_MAX_SERVER_OPERATION_TIMEOUT {
        return Err(ControlPlaneRpcServerConfigError::IoTimeoutTooLarge);
    }
    Ok(())
}

impl ControlPlaneRpcServerListener {
    pub(crate) fn unix(
        listener: UnixListener,
        max_connections: usize,
        max_frame_bytes: usize,
        io_timeout: Duration,
    ) -> Result<Self, ControlPlaneRpcServerConfigError> {
        validate_control_plane_rpc_server_listener_limits(
            max_connections,
            max_frame_bytes,
            io_timeout,
        )?;
        Ok(Self {
            kind: ControlPlaneRpcServerListenerKind::Unix(listener),
            max_connections,
            max_frame_bytes,
            io_timeout,
        })
    }

    pub(crate) fn tls_tcp(
        listener: TcpListener,
        certified_key: Arc<CertifiedKey>,
        max_connections: usize,
        max_frame_bytes: usize,
        io_timeout: Duration,
    ) -> Result<Self, ControlPlaneRpcServerConfigError> {
        validate_control_plane_rpc_server_listener_limits(
            max_connections,
            max_frame_bytes,
            io_timeout,
        )?;
        let resolver = ControlPlaneRpcTlsCertificateResolver { certified_key };
        let mut tls_server_config =
            rustls::ServerConfig::builder_with_provider(tls_provider::configured_provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|_| ControlPlaneRpcServerConfigError::TlsProfileUnavailable)?
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(resolver));
        tls_server_config.alpn_protocols = vec![CONTROL_PLANE_RPC_TLS_ALPN.to_vec()];
        Ok(Self {
            kind: ControlPlaneRpcServerListenerKind::TlsTcp {
                listener,
                tls_server_config: Arc::new(tls_server_config),
            },
            max_connections,
            max_frame_bytes,
            io_timeout,
        })
    }
}

#[derive(Debug)]
struct ControlPlaneEndpointPass {
    endpoint_count: usize,
    next_endpoint_index: usize,
    remaining: usize,
    last_endpoint_index: Option<usize>,
}

impl ControlPlaneEndpointPass {
    fn new(start: usize, endpoint_count: usize) -> Self {
        debug_assert!(endpoint_count > 0);
        Self {
            endpoint_count,
            next_endpoint_index: start % endpoint_count,
            remaining: endpoint_count,
            last_endpoint_index: None,
        }
    }

    fn next(&mut self) -> Option<usize> {
        if self.remaining == 0 {
            return None;
        }
        let endpoint_index = self.next_endpoint_index;
        self.next_endpoint_index = (endpoint_index + 1) % self.endpoint_count;
        self.remaining -= 1;
        self.last_endpoint_index = Some(endpoint_index);
        Some(endpoint_index)
    }

    fn is_exhausted(&self) -> bool {
        self.remaining == 0
    }

    fn last_endpoint_index(&self) -> usize {
        self.last_endpoint_index
            .expect("endpoint pass records every attempted endpoint")
    }
}

impl std::fmt::Debug for UnixControlPlaneClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixControlPlaneClient")
            .field("endpoints", &self.endpoints)
            .field("socket_paths", &self.socket_paths)
            .field(
                "preferred_endpoint_index",
                &self.preferred_endpoint_index.load(Ordering::Acquire),
            )
            .finish()
    }
}

#[derive(Debug)]
struct ControlPlaneRpcFrameExchangeError {
    error: Box<ControlPlaneError>,
    request_may_have_been_sent: bool,
}

impl ControlPlaneRpcFrameExchangeError {
    #[must_use]
    fn before_request(error: ControlPlaneError) -> Self {
        Self {
            error: Box::new(error),
            request_may_have_been_sent: false,
        }
    }

    #[must_use]
    fn after_request_started(error: ControlPlaneError) -> Self {
        Self {
            error: Box::new(error),
            request_may_have_been_sent: true,
        }
    }

    #[must_use]
    fn request_may_have_been_sent(&self) -> bool {
        self.request_may_have_been_sent
    }

    #[must_use]
    fn into_error(self) -> ControlPlaneError {
        *self.error
    }
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUnixControlPlaneClient {
    inner: UnixControlPlaneClient,
    credential: ControlPlaneScopedCredential,
}

#[derive(Debug, Clone)]
pub(crate) struct ControlPlaneUnixAuthVerifier {
    cluster_id: String,
    storage_node_credentials: BTreeMap<NodeId, Vec<ControlPlaneStorageNodeAuthCredential>>,
    frontend_credentials: BTreeMap<String, Vec<ControlPlaneFrontendAuthCredential>>,
    admin_credentials: BTreeMap<String, Vec<ControlPlaneAdminAuthCredential>>,
    metrics: Arc<ControlPlaneUnixAuthMetrics>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneStorageNodeAuthCredential {
    node_id: NodeId,
    credential_id: String,
    credential_version: u64,
    secret: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneStorageNodeAuthCredentialInput {
    pub node_id: NodeId,
    pub credential_id: String,
    pub credential_version: u64,
    pub secret: Vec<u8>,
}

impl std::fmt::Debug for ControlPlaneStorageNodeAuthCredentialInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneStorageNodeAuthCredentialInput")
            .field("node_id", &self.node_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl std::fmt::Debug for ControlPlaneStorageNodeAuthCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneStorageNodeAuthCredential")
            .field("node_id", &self.node_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneFrontendAuthCredential {
    instance_id: String,
    credential_id: String,
    credential_version: u64,
    secret: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneFrontendAuthCredentialInput {
    pub instance_id: String,
    pub credential_id: String,
    pub credential_version: u64,
    pub secret: Vec<u8>,
}

impl std::fmt::Debug for ControlPlaneFrontendAuthCredentialInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneFrontendAuthCredentialInput")
            .field("instance_id", &self.instance_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl std::fmt::Debug for ControlPlaneFrontendAuthCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneFrontendAuthCredential")
            .field("instance_id", &self.instance_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneAdminAuthCredential {
    instance_id: String,
    credential_id: String,
    credential_version: u64,
    secret: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneAdminAuthCredentialInput {
    pub instance_id: String,
    pub credential_id: String,
    pub credential_version: u64,
    pub secret: Vec<u8>,
}

impl std::fmt::Debug for ControlPlaneAdminAuthCredentialInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneAdminAuthCredentialInput")
            .field("instance_id", &self.instance_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl std::fmt::Debug for ControlPlaneAdminAuthCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneAdminAuthCredential")
            .field("instance_id", &self.instance_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneUnixAuthCredentialStatus {
    node_id: NodeId,
    credential_id: String,
    credential_version: u64,
}

impl ControlPlaneUnixAuthCredentialStatus {
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneUnixFrontendAuthCredentialStatus {
    instance_id: String,
    credential_id: String,
    credential_version: u64,
}

impl ControlPlaneUnixFrontendAuthCredentialStatus {
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneUnixAdminAuthCredentialStatus {
    instance_id: String,
    credential_id: String,
    credential_version: u64,
}

impl ControlPlaneUnixAdminAuthCredentialStatus {
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneUnixAuthStatusSnapshot {
    required: bool,
    storage_node_heartbeat_required: bool,
    frontend_runtime_map_required: bool,
    admin_control_plane_required: bool,
    cluster_id: String,
    storage_node_credentials: Vec<ControlPlaneUnixAuthCredentialStatus>,
    frontend_credentials: Vec<ControlPlaneUnixFrontendAuthCredentialStatus>,
    admin_credentials: Vec<ControlPlaneUnixAdminAuthCredentialStatus>,
    metrics: ControlPlaneUnixAuthMetricsSnapshot,
}

impl ControlPlaneUnixAuthStatusSnapshot {
    #[must_use]
    pub fn required(&self) -> bool {
        self.required
    }

    #[must_use]
    pub fn storage_node_heartbeat_required(&self) -> bool {
        self.storage_node_heartbeat_required
    }

    #[must_use]
    pub fn frontend_runtime_map_required(&self) -> bool {
        self.frontend_runtime_map_required
    }

    #[must_use]
    pub fn admin_control_plane_required(&self) -> bool {
        self.admin_control_plane_required
    }

    #[must_use]
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    #[must_use]
    pub fn storage_node_credentials(&self) -> &[ControlPlaneUnixAuthCredentialStatus] {
        &self.storage_node_credentials
    }

    #[must_use]
    pub fn frontend_credentials(&self) -> &[ControlPlaneUnixFrontendAuthCredentialStatus] {
        &self.frontend_credentials
    }

    #[must_use]
    pub fn admin_credentials(&self) -> &[ControlPlaneUnixAdminAuthCredentialStatus] {
        &self.admin_credentials
    }

    #[must_use]
    pub fn metrics(&self) -> &ControlPlaneUnixAuthMetricsSnapshot {
        &self.metrics
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneUnixAuthMetricsSnapshot {
    accepted_total: u64,
    rejected_total: u64,
    accepted_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_reason: BTreeMap<ControlPlaneAuthRejectionReason, u64>,
}

impl ControlPlaneUnixAuthMetricsSnapshot {
    #[must_use]
    pub fn accepted_total(&self) -> u64 {
        self.accepted_total
    }

    #[must_use]
    pub fn rejected_total(&self) -> u64 {
        self.rejected_total
    }

    #[must_use]
    #[cfg(test)]
    pub fn accepted_for_operation(&self, operation: ControlPlaneAuthOperation) -> u64 {
        self.accepted_by_operation
            .get(&operation)
            .copied()
            .unwrap_or(0)
    }

    #[must_use]
    #[cfg(test)]
    pub fn rejected_for_operation(&self, operation: ControlPlaneAuthOperation) -> u64 {
        self.rejected_by_operation
            .get(&operation)
            .copied()
            .unwrap_or(0)
    }

    #[must_use]
    #[cfg(test)]
    pub fn rejected_for_reason(&self, reason: ControlPlaneAuthRejectionReason) -> u64 {
        self.rejected_by_reason.get(&reason).copied().unwrap_or(0)
    }

    #[must_use]
    pub fn accepted_by_operation(&self) -> &BTreeMap<ControlPlaneAuthOperation, u64> {
        &self.accepted_by_operation
    }

    #[must_use]
    pub fn rejected_by_operation(&self) -> &BTreeMap<ControlPlaneAuthOperation, u64> {
        &self.rejected_by_operation
    }

    #[must_use]
    pub fn rejected_by_reason(&self) -> &BTreeMap<ControlPlaneAuthRejectionReason, u64> {
        &self.rejected_by_reason
    }
}

#[derive(Debug, Default)]
struct ControlPlaneUnixAuthMetrics {
    state: Mutex<ControlPlaneUnixAuthMetricsState>,
}

#[derive(Debug, Default)]
struct ControlPlaneUnixAuthMetricsState {
    accepted_total: u64,
    rejected_total: u64,
    accepted_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_reason: BTreeMap<ControlPlaneAuthRejectionReason, u64>,
}

struct VerifiedFrontendRuntimeMapRead {
    payload: Vec<u8>,
    response_credential: ControlPlaneScopedCredential,
    response_target: ControlPlaneAuthPrincipal,
}

struct VerifiedAdminControlPlaneCommand {
    payload: Vec<u8>,
    response_credential: ControlPlaneScopedCredential,
    response_target: ControlPlaneAuthPrincipal,
}

struct VerifiedAdminRuntimeMapRead {
    payload: Vec<u8>,
    response_credential: ControlPlaneScopedCredential,
    response_target: ControlPlaneAuthPrincipal,
}

#[derive(Clone, Copy)]
struct AuthenticatedAdminRetryClock {
    authority_now_ms: u64,
    start: Instant,
    #[cfg(test)]
    elapsed_override_ms: Option<&'static std::sync::atomic::AtomicU64>,
}

impl AuthenticatedAdminRetryClock {
    fn new(authority_now_ms: u64) -> Self {
        Self {
            authority_now_ms,
            start: Instant::now(),
            #[cfg(test)]
            elapsed_override_ms: None,
        }
    }

    fn now_ms(self) -> u64 {
        #[cfg(test)]
        let elapsed_ms = self.elapsed_override_ms.map_or_else(
            || u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX),
            |elapsed_ms| elapsed_ms.load(std::sync::atomic::Ordering::SeqCst),
        );
        #[cfg(not(test))]
        let elapsed_ms = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.authority_now_ms.saturating_add(elapsed_ms)
    }

    #[cfg(test)]
    fn with_elapsed_source(authority_now_ms: u64) -> (Self, &'static std::sync::atomic::AtomicU64) {
        let elapsed_override_ms = Box::leak(Box::new(std::sync::atomic::AtomicU64::new(0)));
        (
            Self {
                authority_now_ms,
                start: Instant::now(),
                elapsed_override_ms: Some(elapsed_override_ms),
            },
            elapsed_override_ms,
        )
    }
}

struct VerifiedStorageNodeHeartbeatRefresh {
    payload: Vec<u8>,
    response_credential: ControlPlaneScopedCredential,
    response_target: ControlPlaneAuthPrincipal,
}

fn format_control_plane_auth_rejection(
    reason: ControlPlaneAuthRejectionReason,
    envelope: &ControlPlaneAuthEnvelope,
    authority_now_ms: u64,
) -> String {
    if reason != ControlPlaneAuthRejectionReason::ReplayFreshnessFailure {
        return format!("{reason:?}");
    }
    let issued_at_ms = envelope.header().issued_at_ms();
    let expires_at_ms = envelope.header().expires_at_ms();
    let issued_delta_ms = issued_at_ms
        .map(|issued_at_ms| i128::from(issued_at_ms).saturating_sub(i128::from(authority_now_ms)));
    let expiry_delta_ms = expires_at_ms.map(|expires_at_ms| {
        i128::from(expires_at_ms).saturating_sub(i128::from(authority_now_ms))
    });
    format!(
        "{reason:?} (issued_at_ms={issued_at_ms:?}, expires_at_ms={expires_at_ms:?}, authority_now_ms={authority_now_ms}, issued_delta_ms={issued_delta_ms:?}, expiry_delta_ms={expiry_delta_ms:?})"
    )
}

impl ControlPlaneUnixAuthMetrics {
    fn record_accepted(&self, operation: ControlPlaneAuthOperation) {
        let mut state = self
            .state
            .lock()
            .expect("control-plane Unix auth metrics mutex poisoned");
        state.accepted_total += 1;
        *state.accepted_by_operation.entry(operation).or_default() += 1;
    }

    fn record_rejected(
        &self,
        operation: ControlPlaneAuthOperation,
        reason: ControlPlaneAuthRejectionReason,
    ) {
        let mut state = self
            .state
            .lock()
            .expect("control-plane Unix auth metrics mutex poisoned");
        state.rejected_total += 1;
        *state.rejected_by_operation.entry(operation).or_default() += 1;
        *state.rejected_by_reason.entry(reason).or_default() += 1;
    }

    fn snapshot(&self) -> ControlPlaneUnixAuthMetricsSnapshot {
        let state = self
            .state
            .lock()
            .expect("control-plane Unix auth metrics mutex poisoned");
        ControlPlaneUnixAuthMetricsSnapshot {
            accepted_total: state.accepted_total,
            rejected_total: state.rejected_total,
            accepted_by_operation: state.accepted_by_operation.clone(),
            rejected_by_operation: state.rejected_by_operation.clone(),
            rejected_by_reason: state.rejected_by_reason.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FencedPgMetadataTransferRuntimeMap {
    runtime_map: ClusterRuntimeMapSnapshot,
    source_primary_lease_deadline_ms: Option<u64>,
}

impl FencedPgMetadataTransferRuntimeMap {
    #[must_use]
    pub fn new(
        runtime_map: ClusterRuntimeMapSnapshot,
        source_primary_lease_deadline_ms: Option<u64>,
    ) -> Self {
        Self {
            runtime_map,
            source_primary_lease_deadline_ms,
        }
    }

    #[must_use]
    pub fn runtime_map(&self) -> &ClusterRuntimeMapSnapshot {
        &self.runtime_map
    }

    #[must_use]
    pub fn source_primary_lease_deadline_ms(&self) -> Option<u64> {
        self.source_primary_lease_deadline_ms
    }

    #[must_use]
    pub fn into_parts(self) -> (ClusterRuntimeMapSnapshot, Option<u64>) {
        (self.runtime_map, self.source_primary_lease_deadline_ms)
    }
}

fn unix_io_timeout_error() -> std::io::Error {
    std::io::Error::new(
        ErrorKind::TimedOut,
        "control-plane Unix RPC deadline expired",
    )
}

fn unix_io_remaining(deadline: Instant) -> Result<Duration, std::io::Error> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(unix_io_timeout_error());
    }
    Ok(remaining)
}

/// Connects to a control-plane Unix socket within one absolute operation deadline.
pub(crate) fn connect_unix_stream_until(
    path: &Path,
    deadline: Instant,
) -> std::io::Result<UnixStream> {
    unix_io_remaining(deadline)?;
    let path_bytes = path.as_os_str().as_bytes();
    if path_bytes.contains(&0) {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "control-plane Unix socket path contains NUL",
        ));
    }

    // SAFETY: sockaddr_un is a plain C address structure and zero is a valid
    // initialization before its family and path fields are populated.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if path_bytes.len() >= address.sun_path.len() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "control-plane Unix socket path is too long",
        ));
    }
    address.sun_family = libc::sa_family_t::try_from(libc::AF_UNIX)
        .expect("AF_UNIX fits the platform socket-family field");
    // SAFETY: the length check above proves the source plus its zero terminator
    // fits sun_path, which was zero-initialized.
    unsafe {
        std::ptr::copy_nonoverlapping(
            path_bytes.as_ptr(),
            address.sun_path.as_mut_ptr().cast::<u8>(),
            path_bytes.len(),
        );
    }
    let address_len = std::mem::offset_of!(libc::sockaddr_un, sun_path)
        .checked_add(path_bytes.len())
        .and_then(|len| len.checked_add(1))
        .and_then(|len| libc::socklen_t::try_from(len).ok())
        .ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                "control-plane Unix socket address length overflowed",
            )
        })?;
    #[cfg(any(
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    {
        address.sun_len = u8::try_from(address_len).map_err(|_| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                "control-plane Unix socket address is too long",
            )
        })?;
    }

    // SAFETY: AF_UNIX/SOCK_STREAM has no additional pointer arguments.
    let raw_fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if raw_fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: raw_fd was returned as a new owned descriptor above.
    let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
    set_unix_connect_descriptor_flags(&fd)?;

    // SAFETY: address points to an initialized sockaddr_un and address_len
    // covers exactly its family, path bytes, and zero terminator.
    let connect_result = unsafe {
        libc::connect(
            fd.as_raw_fd(),
            (&raw const address).cast::<libc::sockaddr>(),
            address_len,
        )
    };
    if connect_result != 0 {
        let error = std::io::Error::last_os_error();
        let raw_error = error.raw_os_error();
        if raw_error != Some(libc::EINPROGRESS)
            && raw_error != Some(libc::EAGAIN)
            && raw_error != Some(libc::EWOULDBLOCK)
            && raw_error != Some(libc::EINTR)
        {
            return Err(error);
        }
        wait_for_unix_connect(&fd, deadline)?;
    }
    clear_unix_connect_nonblocking(&fd)?;
    Ok(UnixStream::from(fd))
}

fn set_unix_connect_descriptor_flags(fd: &OwnedFd) -> std::io::Result<()> {
    // SAFETY: fcntl operates on the live descriptor owned by fd.
    let descriptor_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
    if descriptor_flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: F_SETFD consumes an integer flag value, not a pointer.
    if unsafe {
        libc::fcntl(
            fd.as_raw_fd(),
            libc::F_SETFD,
            descriptor_flags | libc::FD_CLOEXEC,
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fcntl operates on the live descriptor owned by fd.
    let status_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if status_flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: F_SETFL consumes an integer flag value, not a pointer.
    if unsafe {
        libc::fcntl(
            fd.as_raw_fd(),
            libc::F_SETFL,
            status_flags | libc::O_NONBLOCK,
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn clear_unix_connect_nonblocking(fd: &OwnedFd) -> std::io::Result<()> {
    // SAFETY: fcntl operates on the live descriptor owned by fd.
    let status_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if status_flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: F_SETFL consumes an integer flag value, not a pointer.
    if unsafe {
        libc::fcntl(
            fd.as_raw_fd(),
            libc::F_SETFL,
            status_flags & !libc::O_NONBLOCK,
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn wait_for_unix_connect(fd: &OwnedFd, deadline: Instant) -> std::io::Result<()> {
    loop {
        let remaining = unix_io_remaining(deadline)?;
        let timeout_ms = remaining
            .as_nanos()
            .div_ceil(1_000_000)
            .min(u128::try_from(i32::MAX).expect("i32::MAX fits u128"));
        let timeout_ms = i32::try_from(timeout_ms).expect("poll timeout was clamped to i32::MAX");
        let mut poll_fd = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLOUT,
            revents: 0,
        };
        // SAFETY: poll_fd points to one initialized pollfd for the duration of
        // the call.
        let poll_result = unsafe { libc::poll(&raw mut poll_fd, 1, timeout_ms) };
        if poll_result == 0 {
            return Err(unix_io_timeout_error());
        }
        if poll_result < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }

        let mut socket_error = 0;
        let mut socket_error_len = libc::socklen_t::try_from(std::mem::size_of_val(&socket_error))
            .expect("socket error length fits socklen_t");
        // SAFETY: both output pointers reference initialized writable values
        // of the lengths passed to getsockopt.
        if unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                (&raw mut socket_error).cast(),
                &raw mut socket_error_len,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error());
        }
        if socket_error != 0 {
            return Err(std::io::Error::from_raw_os_error(socket_error));
        }
        return Ok(());
    }
}

const CONTROL_PLANE_RPC_DEADLINE_EXPIRED: &str = "control-plane RPC operation deadline expired";
pub(crate) type DeadlineUnixStream<'a> = DeadlineStream<&'a mut UnixStream>;
type ControlPlaneDeadlineTcpSocket = DeadlineStream<TcpStream>;
type ControlPlaneDeadlineUnixSocket = DeadlineStream<UnixStream>;

pub(crate) async fn connect_tcp_stream_until_async(
    host: String,
    port: u16,
    deadline: Instant,
) -> std::io::Result<TcpStream> {
    let stream = match tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        tokio::net::TcpStream::connect((host.as_str(), port)),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            return Err(std::io::Error::new(
                ErrorKind::TimedOut,
                "TCP connect deadline expired",
            ));
        }
    };
    let stream = stream.into_std()?;
    stream.set_nonblocking(false)?;
    Ok(stream)
}

fn connect_control_plane_tcp_until(
    host: &str,
    port: u16,
    deadline: Instant,
) -> std::io::Result<TcpStream> {
    let future = connect_tcp_stream_until_async(host.to_owned(), port, deadline);
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        Ok(_) => std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(future)
        })
        .join()
        .map_err(|_| std::io::Error::other("control-plane TLS/TCP client runtime panicked"))?,
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(future),
    }
}

fn connect_control_plane_tls_tcp(
    host: &str,
    port: u16,
    server_name: &str,
    connect_timeout: Duration,
    tls_client_config: &Arc<rustls::ClientConfig>,
    deadline: Instant,
) -> Result<
    rustls::StreamOwned<rustls::ClientConnection, ControlPlaneDeadlineTcpSocket>,
    ControlPlaneError,
> {
    let connect_deadline = Instant::now()
        .checked_add(connect_timeout)
        .unwrap_or(deadline)
        .min(deadline);
    let tcp_stream =
        connect_control_plane_tcp_until(host, port, connect_deadline).map_err(|source| {
            ControlPlaneError::io("connect control-plane TLS/TCP endpoint", source)
        })?;
    tcp_stream.set_nodelay(true).map_err(|source| {
        ControlPlaneError::io("configure control-plane TLS/TCP endpoint", source)
    })?;
    let server_name = ServerName::try_from(server_name.to_owned()).map_err(|_| {
        ControlPlaneError::rpc_protocol(
            "control-plane TLS/TCP endpoint has an invalid TLS server name".to_owned(),
        )
    })?;
    let connection = rustls::ClientConnection::new(Arc::clone(tls_client_config), server_name)
        .map_err(|error| {
            ControlPlaneError::rpc_protocol(format!(
                "failed to initialize control-plane TLS client: {error}"
            ))
        })?;
    let socket = ControlPlaneDeadlineTcpSocket::new(
        tcp_stream,
        deadline,
        CONTROL_PLANE_RPC_DEADLINE_EXPIRED,
    )
    .map_err(|source| {
        ControlPlaneError::io("configure control-plane TLS/TCP deadline I/O", source)
    })?;
    let mut stream = rustls::StreamOwned::new(connection, socket);
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|source| {
                ControlPlaneError::io("complete control-plane TLS client handshake", source)
            })?;
    }
    if stream.conn.alpn_protocol() != Some(CONTROL_PLANE_RPC_TLS_ALPN) {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane TLS peer did not negotiate the required protocol profile".to_owned(),
        ));
    }
    Ok(stream)
}

impl ControlPlaneRpcClientEndpoint {
    fn exchange(
        &self,
        request_frame: &[u8],
        deadline: Instant,
    ) -> Result<(ControlPlaneRpcKind, Vec<u8>), ControlPlaneRpcFrameExchangeError> {
        match &self.0 {
            ControlPlaneRpcClientEndpointKind::Unix { socket_path } => {
                let mut stream =
                    connect_unix_stream_until(socket_path, deadline).map_err(|source| {
                        ControlPlaneRpcFrameExchangeError::before_request(ControlPlaneError::io(
                            "connect control-plane socket",
                            source,
                        ))
                    })?;
                let mut stream = DeadlineUnixStream::new(
                    &mut stream,
                    deadline,
                    CONTROL_PLANE_RPC_DEADLINE_EXPIRED,
                )
                .map_err(|source| {
                    ControlPlaneRpcFrameExchangeError::before_request(ControlPlaneError::io(
                        "configure control-plane Unix deadline I/O",
                        source,
                    ))
                })?;
                stream.write_all(request_frame).map_err(|source| {
                    ControlPlaneRpcFrameExchangeError::after_request_started(ControlPlaneError::io(
                        "write control-plane RPC frame",
                        source,
                    ))
                })?;
                read_control_plane_rpc_frame(&mut stream)
                    .map_err(ControlPlaneRpcFrameExchangeError::after_request_started)
            }
            ControlPlaneRpcClientEndpointKind::TlsTcp {
                host,
                port,
                server_name,
                connect_timeout,
                tls_client_config,
                ..
            } => {
                let mut stream = connect_control_plane_tls_tcp(
                    host,
                    *port,
                    server_name,
                    *connect_timeout,
                    tls_client_config,
                    deadline,
                )
                .map_err(ControlPlaneRpcFrameExchangeError::before_request)?;
                stream.write_all(request_frame).map_err(|source| {
                    ControlPlaneRpcFrameExchangeError::after_request_started(ControlPlaneError::io(
                        "write control-plane TLS/TCP request frame",
                        source,
                    ))
                })?;
                read_control_plane_rpc_frame(&mut stream)
                    .map_err(ControlPlaneRpcFrameExchangeError::after_request_started)
            }
        }
    }
}

impl UnixControlPlaneClient {
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        let socket_path = socket_path.into();
        Self {
            endpoints: Arc::from([ControlPlaneRpcClientEndpoint::unix(socket_path.clone())]),
            socket_paths: Arc::from([socket_path]),
            preferred_endpoint_index: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_socket_paths(
        socket_paths: impl IntoIterator<Item = PathBuf>,
    ) -> Result<Self, ControlPlaneError> {
        let socket_paths: Vec<PathBuf> = socket_paths.into_iter().collect();
        if socket_paths.is_empty() {
            return Err(ControlPlaneError::rpc_protocol(
                "control-plane Unix client requires at least one socket path".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for socket_path in &socket_paths {
            if !unique.insert(socket_path.clone()) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane Unix client contains duplicate socket path {}",
                    socket_path.display()
                )));
            }
        }
        Ok(Self {
            endpoints: socket_paths
                .iter()
                .cloned()
                .map(ControlPlaneRpcClientEndpoint::unix)
                .collect::<Vec<_>>()
                .into(),
            socket_paths: socket_paths.into(),
            preferred_endpoint_index: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn with_endpoints(
        endpoints: impl IntoIterator<Item = ControlPlaneRpcClientEndpoint>,
    ) -> Result<Self, ControlPlaneError> {
        let endpoints: Vec<ControlPlaneRpcClientEndpoint> = endpoints.into_iter().collect();
        if endpoints.is_empty() {
            return Err(ControlPlaneError::rpc_protocol(
                "control-plane client requires at least one endpoint".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for endpoint in &endpoints {
            let advertised_endpoint = endpoint.advertised_endpoint();
            if !unique.insert(advertised_endpoint.clone()) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane client contains duplicate endpoint {advertised_endpoint}"
                )));
            }
        }
        let socket_paths = endpoints
            .iter()
            .map(|endpoint| endpoint.unix_socket_path().map(Path::to_path_buf))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        Ok(Self {
            endpoints: endpoints.into(),
            socket_paths: socket_paths.into(),
            preferred_endpoint_index: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Returns the primary path for a client configured exclusively with Unix endpoints.
    ///
    /// # Panics
    ///
    /// Panics when the client contains a TLS/TCP endpoint.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_paths[0]
    }

    /// Returns all paths when the client is configured exclusively with Unix endpoints.
    /// Mixed or TLS/TCP-only clients return an empty slice.
    #[must_use]
    pub fn socket_paths(&self) -> &[PathBuf] {
        &self.socket_paths
    }

    fn preferred_endpoint_index(&self) -> usize {
        self.preferred_endpoint_index.load(Ordering::Acquire) % self.endpoint_count()
    }

    fn prefer_endpoint_index(&self, endpoint_index: usize) {
        self.preferred_endpoint_index
            .store(endpoint_index % self.endpoint_count(), Ordering::Release);
    }

    fn endpoint_pass(&self) -> ControlPlaneEndpointPass {
        ControlPlaneEndpointPass::new(self.preferred_endpoint_index(), self.endpoint_count())
    }

    fn prefer_next_endpoint_after_failure(&self, pass: &ControlPlaneEndpointPass) {
        let rejected = pass.last_endpoint_index();
        let _ = self.preferred_endpoint_index.compare_exchange(
            rejected,
            pass.next_endpoint_index,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    fn prefer_successful_endpoint(&self, pass: &ControlPlaneEndpointPass) {
        self.prefer_endpoint_index(pass.last_endpoint_index());
    }

    fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }

    fn send_request(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.send_request_with_read_timeout(kind, payload, CONTROL_PLANE_RPC_IO_TIMEOUT)
    }

    fn send_request_with_read_timeout(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        read_timeout: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.send_request_until(kind, payload, Instant::now() + read_timeout)
    }

    fn send_mutating_request_with_read_timeout(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        read_timeout: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        debug_assert!(kind.mutating_admin_operation().is_some());
        let deadline = Instant::now() + read_timeout;
        let mut last_routing_error = None;
        let mut endpoint_pass = self.endpoint_pass();
        while !endpoint_pass.is_exhausted() {
            let response = match self.send_request_raw_response_with_endpoint_pass_until_classified(
                kind,
                payload,
                deadline,
                &mut endpoint_pass,
            ) {
                Ok(response) => response,
                Err(error) if error.request_may_have_been_sent() => {
                    return Err(unconfirmed_admin_mutation_response(
                        kind,
                        error.into_error(),
                    ));
                }
                Err(error) => return Err(error.into_error()),
            };
            let response = decode_control_plane_rpc_response_frame(response)
                .map_err(|error| unconfirmed_admin_mutation_response(kind, error))?;
            match response {
                DecodedControlPlaneRpcResponse::Rejection(error)
                    if error.is_control_plane_leader_routing_rejection() =>
                {
                    last_routing_error = Some(error);
                    self.prefer_next_endpoint_after_failure(&endpoint_pass);
                }
                DecodedControlPlaneRpcResponse::Rejection(error) => {
                    self.prefer_successful_endpoint(&endpoint_pass);
                    return Err(error);
                }
                DecodedControlPlaneRpcResponse::Success(payload) => {
                    self.prefer_successful_endpoint(&endpoint_pass);
                    return Ok(payload);
                }
            }
        }
        Err(last_routing_error.expect("leader routing retry requires at least one endpoint"))
    }

    fn send_request_until(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut last_routing_error = None;
        let mut endpoint_pass = self.endpoint_pass();
        while !endpoint_pass.is_exhausted() {
            let response_payload = self.send_request_raw_response_with_endpoint_pass_until(
                kind,
                payload,
                deadline,
                &mut endpoint_pass,
            )?;
            match decode_control_plane_rpc_response(response_payload) {
                Err(error) if error.is_control_plane_leader_routing_rejection() => {
                    last_routing_error = Some(error);
                    self.prefer_next_endpoint_after_failure(&endpoint_pass);
                }
                result => {
                    self.prefer_successful_endpoint(&endpoint_pass);
                    return result;
                }
            }
        }
        Err(last_routing_error.expect("leader routing retry requires at least one endpoint"))
    }

    #[cfg(test)]
    fn send_request_raw_response_until(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut endpoint_pass = self.endpoint_pass();
        self.send_request_raw_response_with_endpoint_pass_until(
            kind,
            payload,
            deadline,
            &mut endpoint_pass,
        )
    }

    fn send_request_raw_response_with_endpoint_pass_until(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        deadline: Instant,
        endpoint_pass: &mut ControlPlaneEndpointPass,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.send_request_raw_response_with_endpoint_pass_until_classified(
            kind,
            payload,
            deadline,
            endpoint_pass,
        )
        .map_err(ControlPlaneRpcFrameExchangeError::into_error)
    }

    fn send_request_raw_response_with_endpoint_pass_until_classified(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        deadline: Instant,
        endpoint_pass: &mut ControlPlaneEndpointPass,
    ) -> Result<Vec<u8>, ControlPlaneRpcFrameExchangeError> {
        let request_frame = encode_control_plane_rpc_frame(kind, payload)
            .map_err(ControlPlaneRpcFrameExchangeError::before_request)?;
        let mut response = None;
        let mut last_pre_request_error = None;
        while let Some(endpoint_index) = endpoint_pass.next() {
            match self.endpoints[endpoint_index].exchange(&request_frame, deadline) {
                Ok(result) => {
                    response = Some(result);
                    break;
                }
                Err(error) if !error.request_may_have_been_sent() => {
                    last_pre_request_error = Some(error);
                    self.prefer_next_endpoint_after_failure(endpoint_pass);
                }
                Err(error) => return Err(error),
            }
        }
        let (response_kind, response_payload) = response.ok_or_else(|| {
            last_pre_request_error.expect("endpoint set is non-empty and every connect failed")
        })?;
        if response_kind != kind {
            return Err(ControlPlaneRpcFrameExchangeError::after_request_started(
                ControlPlaneError::rpc_protocol(format!(
                    "response kind {:?} did not match request kind {:?}",
                    response_kind, kind
                )),
            ));
        }
        Ok(response_payload)
    }

    fn send_read_only_request_with_read_timeout(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        read_timeout: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.send_read_only_request_with_payload_factory(
            kind,
            read_timeout,
            || Ok(payload.to_vec()),
        )
    }

    fn send_read_only_request_with_payload_factory<F>(
        &self,
        kind: ControlPlaneRpcKind,
        read_timeout: Duration,
        mut build_payload: F,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        F: FnMut() -> Result<Vec<u8>, ControlPlaneError>,
    {
        debug_assert!(matches!(
            kind,
            ControlPlaneRpcKind::RuntimeMapSnapshot
                | ControlPlaneRpcKind::RuntimeMapDiagnostics
                | ControlPlaneRpcKind::PgRuntimeMapSnapshot
                | ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot
                | ControlPlaneRpcKind::RuntimeMapStatus
                | ControlPlaneRpcKind::PendingMetadataCommandRecoveries
        ));
        let deadline = Instant::now() + CONTROL_PLANE_RPC_READ_ONLY_RETRY_DEADLINE;
        loop {
            let payload = build_payload()?;
            match self.send_request_with_read_timeout(kind, &payload, read_timeout) {
                Ok(payload) => return Ok(payload),
                Err(error)
                    if error.is_retryable_read_only_rpc_transport_error()
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(CONTROL_PLANE_RPC_READ_ONLY_RETRY_BACKOFF);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn send_liveness_request(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        retry_budget: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let deadline = Instant::now() + retry_budget;
        let mut endpoint_pass = self.endpoint_pass();
        loop {
            let response_payload = self.send_liveness_request_raw_response_until(
                kind,
                payload,
                deadline,
                &mut endpoint_pass,
            )?;
            match decode_control_plane_rpc_response(response_payload) {
                Err(error)
                    if error.is_control_plane_leader_routing_rejection()
                        && Instant::now() < deadline =>
                {
                    self.prefer_next_endpoint_after_failure(&endpoint_pass);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let retry_sleep = CONTROL_PLANE_RPC_LIVENESS_RETRY_BACKOFF.min(remaining / 2);
                    if !retry_sleep.is_zero() {
                        std::thread::sleep(retry_sleep);
                    }
                }
                result => {
                    self.prefer_successful_endpoint(&endpoint_pass);
                    return result;
                }
            }
        }
    }

    fn send_liveness_request_raw_response_until(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        deadline: Instant,
        endpoint_pass: &mut ControlPlaneEndpointPass,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.send_liveness_request_raw_response_with_payload_factory_until(
            kind,
            deadline,
            endpoint_pass,
            || Ok(payload.to_vec()),
        )
    }

    fn send_liveness_request_raw_response_with_payload_factory_until<F>(
        &self,
        kind: ControlPlaneRpcKind,
        deadline: Instant,
        endpoint_pass: &mut ControlPlaneEndpointPass,
        mut build_payload: F,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        F: FnMut() -> Result<Vec<u8>, ControlPlaneError>,
    {
        debug_assert_eq!(kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        let mut retry_started = false;
        let mut last_retryable_error = None;
        loop {
            if retry_started && Instant::now() >= deadline {
                return Err(last_retryable_error
                    .take()
                    .expect("heartbeat retry deadline reached after retryable error"));
            }
            let now = Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            if remaining.is_zero() {
                return match last_retryable_error.take() {
                    Some(error) => Err(error),
                    None => Err(ControlPlaneError::RpcUnconfirmed {
                        message: "heartbeat retry budget expired before the first request"
                            .to_owned(),
                    }),
                };
            }
            let read_timeout = remaining.min(CONTROL_PLANE_RPC_LIVENESS_IO_TIMEOUT);
            let attempt_deadline = now + read_timeout;
            if endpoint_pass.is_exhausted() {
                *endpoint_pass = self.endpoint_pass();
            }
            let payload = build_payload()?;
            if Instant::now() >= deadline {
                return match last_retryable_error.take() {
                    Some(error) => Err(error),
                    None => Err(ControlPlaneError::RpcUnconfirmed {
                        message: "heartbeat retry budget expired before the first request"
                            .to_owned(),
                    }),
                };
            }
            match self.send_request_raw_response_with_endpoint_pass_until(
                kind,
                &payload,
                attempt_deadline,
                endpoint_pass,
            ) {
                Ok(payload) => return Ok(payload),
                Err(error) if error.is_retryable_control_plane_rpc_transport_error() => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(error);
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    retry_started = true;
                    last_retryable_error = Some(error);
                    let retry_sleep = CONTROL_PLANE_RPC_LIVENESS_RETRY_BACKOFF.min(remaining / 2);
                    if !retry_sleep.is_zero() {
                        std::thread::sleep(retry_sleep);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn wait_for_metadata_transfer_install_applied(
        &self,
        pg_id: PgId,
        acting_set: &[NodeId],
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
        original_error: &ControlPlaneError,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut last_observation_error = None;
        loop {
            match self.pg_runtime_map_snapshot_with_read_timeout(
                pg_id,
                0,
                CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
            ) {
                Ok(runtime_map) => {
                    if metadata_transfer_install_applied(
                        &runtime_map,
                        pg_id,
                        acting_set,
                        transfer,
                        expected_destination_epoch,
                    ) {
                        return Ok(runtime_map);
                    }
                    last_observation_error = Some(
                        runtime_map
                            .pg_routes()
                            .iter()
                            .find(|route| route.pg_id() == pg_id)
                            .map_or_else(
                                || {
                                    format!(
                                        "runtime map at epoch {} has no route for PG {}",
                                        runtime_map.cluster_epoch().get(),
                                        pg_id.get()
                                    )
                                },
                                |route| {
                                    format!(
                                        "runtime map at epoch {} route epoch {} state {:?} acting set {:?} transfer {:?}",
                                        runtime_map.cluster_epoch().get(),
                                        route.cluster_epoch().get(),
                                        route.state(),
                                        route.acting_set(),
                                        route.peering_metadata_transfer()
                                    )
                                },
                            ),
                    );
                }
                Err(error) => {
                    if last_observation_error.is_none() {
                        last_observation_error = Some(error.to_string());
                    }
                }
            }
            if Instant::now() >= deadline {
                let mut message = format!(
                    "metadata-transfer acting-set install for PG {} was not observable after lost control-plane RPC response: {original_error}",
                    pg_id.get()
                );
                if let Some(error) = last_observation_error {
                    message.push_str("; last runtime-map observation error: ");
                    message.push_str(&error);
                }
                return Err(ControlPlaneError::RpcUnconfirmed { message });
            }
            std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
        }
    }

    fn validate_metadata_transfer_fence_response(
        &self,
        pg_id: PgId,
        fenced: FencedPgMetadataTransferRuntimeMap,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        if metadata_transfer_fence_observable(fenced.runtime_map(), pg_id) {
            return Ok(fenced);
        }
        Err(ControlPlaneError::RpcUnconfirmed {
            message: format!(
                "metadata-transfer fence for PG {} returned runtime map without an observable peering route",
                pg_id.get()
            ),
        })
    }

    fn retry_set_pg_acting_set_after_retryable_failure(
        &self,
        pg_id: PgId,
        acting_set: &[NodeId],
        pre_update_route: PgActingSetPreflightRoute,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut last_unconfirmed_message = None;
        loop {
            match self.pg_runtime_map_snapshot_with_read_timeout(
                pg_id,
                0,
                CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
            ) {
                Ok(runtime_map) => {
                    let Some(route) = runtime_map
                        .pg_routes()
                        .iter()
                        .find(|route| route.pg_id() == pg_id)
                    else {
                        return Err(ControlPlaneError::rpc_protocol(format!(
                            "PG-specific runtime map response omitted requested PG {}",
                            pg_id.get()
                        )));
                    };
                    if route.acting_set() == acting_set {
                        return Ok(route.cluster_epoch());
                    }
                    let message = format!(
                            "PG {} acting-set update was not confirmed after retryable control-plane failure: current route at epoch {} has acting set {:?}, expected {:?}",
                            pg_id.get(),
                            route.cluster_epoch().get(),
                            route.acting_set(),
                            acting_set
                        );
                    match &pre_update_route {
                        PgActingSetPreflightRoute::Absent => {
                            return Err(ControlPlaneError::RpcUnconfirmed { message });
                        }
                        PgActingSetPreflightRoute::Present(before) => {
                            match pg_acting_set_retry_route_disposition(before, route) {
                                PgActingSetRetryRouteDisposition::Conflict => {
                                    return Err(ControlPlaneError::RpcUnconfirmed { message });
                                }
                                PgActingSetRetryRouteDisposition::RetryReady => {
                                    match self.set_pg_acting_set(pg_id, acting_set.to_vec()) {
                                        Ok(cluster_epoch) => return Ok(cluster_epoch),
                                        Err(error)
                                            if error.is_retryable_pg_acting_set_checked_error() => {
                                        }
                                        Err(error) => return Err(error),
                                    }
                                }
                                PgActingSetRetryRouteDisposition::Wait => {}
                            }
                        }
                    }
                    last_unconfirmed_message = Some(message);
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: last_unconfirmed_message
                                .expect("mismatched route message was recorded"),
                        });
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(error)
                    if error.is_maybe_applied_control_plane_rpc_response_loss()
                        || error.is_transient_runtime_map_serving_gap() =>
                {
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: last_unconfirmed_message.unwrap_or_else(|| format!(
                                "PG {} acting-set update was not confirmed after retryable control-plane failure; runtime-map observation failed: {error}",
                                pg_id.get()
                            )),
                        });
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(ControlPlaneError::UnknownPg {
                    pg_id: unknown_pg_id,
                }) if unknown_pg_id == pg_id.get()
                    && matches!(&pre_update_route, PgActingSetPreflightRoute::Absent) =>
                {
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: format!(
                                "PG {} acting-set update was not confirmed after retryable control-plane failure: PG remains absent",
                                pg_id.get()
                            ),
                        });
                    }
                    match self.set_pg_acting_set(pg_id, acting_set.to_vec()) {
                        Ok(cluster_epoch) => return Ok(cluster_epoch),
                        Err(error) if error.is_retryable_pg_acting_set_checked_error() => {}
                        Err(error) => return Err(error),
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn set_pg_acting_set(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_acting_set_request(&mut payload, pg_id, &acting_set)?;
        let payload = self.send_request(ControlPlaneRpcKind::SetPgActingSet, &payload)?;
        let mut reader = PayloadReader::new(&payload);
        let raw_cluster_epoch = reader.read_u64()?;
        let cluster_epoch = ClusterEpoch::new(raw_cluster_epoch).ok_or_else(|| {
            ControlPlaneError::rpc_protocol(format!("invalid cluster epoch {raw_cluster_epoch}"))
        })?;
        reader.finish()?;
        Ok(cluster_epoch)
    }

    pub fn set_pg_acting_set_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let pre_update_route = self.pg_acting_set_preflight_route(pg_id)?;
        match self.set_pg_acting_set(pg_id, acting_set.clone()) {
            Ok(cluster_epoch) => Ok(cluster_epoch),
            Err(error) if error.is_retryable_pg_acting_set_checked_error() => self
                .retry_set_pg_acting_set_after_retryable_failure(
                    pg_id,
                    &acting_set,
                    pre_update_route,
                ),
            Err(error) => Err(error),
        }
    }

    pub fn fence_pg_for_metadata_transfer_runtime_map_checked(
        &self,
        pg_id: PgId,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        Ok(self
            .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(pg_id)?
            .into_parts()
            .0)
    }

    #[cfg(test)]
    fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease(
        &self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        self.fence_pg_for_metadata_transfer_runtime_map_with_source_lease_read_timeout(
            pg_id,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
        )
    }

    #[cfg(test)]
    fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease_read_timeout(
        &self,
        pg_id: PgId,
        read_timeout: Duration,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        self.fence_pg_for_metadata_transfer_runtime_map_with_source_lease_until(
            pg_id,
            Instant::now() + read_timeout,
        )
    }

    fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease_until(
        &self,
        pg_id: PgId,
        deadline: Instant,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_request_until(
            ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap,
            &payload,
            deadline,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        let source_primary_lease_deadline_ms = reader.read_option_u64()?;
        reader.finish()?;
        Ok(FencedPgMetadataTransferRuntimeMap::new(
            runtime_map,
            source_primary_lease_deadline_ms,
        ))
    }

    pub fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(
        &self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        retry_checked_metadata_transfer_fence(pg_id, |deadline| {
            self.fence_pg_for_metadata_transfer_runtime_map_with_source_lease_until(pg_id, deadline)
                .and_then(|fenced| self.validate_metadata_transfer_fence_response(pg_id, fenced))
        })
    }

    pub fn set_pg_acting_set_with_metadata_transfer(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_acting_set_with_metadata_transfer_request(
            &mut payload,
            pg_id,
            &acting_set,
            transfer,
            expected_destination_epoch,
        )?;
        let payload = self.send_request(
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransfer,
            &payload,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let raw_cluster_epoch = reader.read_u64()?;
        let cluster_epoch = ClusterEpoch::new(raw_cluster_epoch).ok_or_else(|| {
            ControlPlaneError::rpc_protocol(format!("invalid cluster epoch {raw_cluster_epoch}"))
        })?;
        reader.finish()?;
        Ok(cluster_epoch)
    }

    pub fn set_pg_acting_set_with_metadata_transfer_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        match self.set_pg_acting_set_with_metadata_transfer(
            pg_id,
            acting_set.clone(),
            transfer,
            expected_destination_epoch,
        ) {
            Ok(cluster_epoch) => Ok(cluster_epoch),
            Err(error) if error.is_unconfirmed_control_plane_mutation() => self
                .wait_for_metadata_transfer_install_applied(
                    pg_id,
                    &acting_set,
                    transfer,
                    expected_destination_epoch,
                    &error,
                )
                .map(|runtime_map| runtime_map.cluster_epoch()),
            Err(error) => Err(error),
        }
    }

    pub fn set_pg_acting_set_with_metadata_transfer_runtime_map(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_acting_set_with_metadata_transfer_request(
            &mut payload,
            pg_id,
            &acting_set,
            transfer,
            expected_destination_epoch,
        )?;
        let payload = self.send_request(
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap,
            &payload,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    pub fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.pg_runtime_map_snapshot_with_read_timeout(
            pg_id,
            0,
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )
    }

    pub fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot,
            &payload,
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    pub fn pending_metadata_command_recoveries(
        &self,
    ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::PendingMetadataCommandRecoveries,
            &[],
            CONTROL_PLANE_RPC_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let listing = read_pending_metadata_command_recovery_listing(&mut reader)?;
        reader.finish()?;
        Ok(listing)
    }

    fn pg_runtime_map_snapshot_with_read_timeout(
        &self,
        pg_id: PgId,
        _authority_now_ms: u64,
        read_timeout: Duration,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::PgRuntimeMapSnapshot,
            &payload,
            read_timeout,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    fn pg_acting_set_preflight_route(
        &self,
        pg_id: PgId,
    ) -> Result<PgActingSetPreflightRoute, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let mut last_retryable_error = None;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(last_retryable_error
                    .unwrap_or_else(|| pg_acting_set_preflight_deadline_error(pg_id)));
            }
            let attempt_deadline = (now + CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT).min(deadline);
            match self.send_request_until(
                ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                &payload,
                attempt_deadline,
            ) {
                Ok(response) => {
                    let mut reader = PayloadReader::new(&response);
                    let runtime_map = read_runtime_map_snapshot(&mut reader)?;
                    reader.finish()?;
                    return pg_acting_set_preflight_route(runtime_map, pg_id);
                }
                Err(ControlPlaneError::UnknownPg {
                    pg_id: unknown_pg_id,
                }) if unknown_pg_id == pg_id.get() => {
                    return Ok(PgActingSetPreflightRoute::Absent);
                }
                Err(error)
                    if error.is_retryable_read_only_rpc_transport_error()
                        || error.is_transient_runtime_map_serving_gap() =>
                {
                    last_retryable_error = Some(error);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let backoff = CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF.min(remaining);
                    if !backoff.is_zero() {
                        std::thread::sleep(backoff);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        match self.set_pg_acting_set_with_metadata_transfer_runtime_map(
            pg_id,
            acting_set.clone(),
            transfer,
            expected_destination_epoch,
        ) {
            Ok(runtime_map)
                if metadata_transfer_install_applied(
                    &runtime_map,
                    pg_id,
                    &acting_set,
                    transfer,
                    expected_destination_epoch,
                ) =>
            {
                Ok(runtime_map)
            }
            Ok(runtime_map) => Err(ControlPlaneError::RpcUnconfirmed {
                message: format!(
                    "metadata-transfer acting-set install for PG {} returned runtime map at epoch {} without the expected route/proof",
                    pg_id.get(),
                    runtime_map.cluster_epoch().get()
                ),
            }),
            Err(error) if error.is_unconfirmed_control_plane_mutation() => self
                .wait_for_metadata_transfer_install_applied(
                    pg_id,
                    &acting_set,
                    transfer,
                    expected_destination_epoch,
                    &error,
                ),
            Err(error) => Err(error),
        }
    }

    pub fn transfer_raft_leadership_to(&self, node_id: u64) -> Result<(), ControlPlaneError> {
        let mut payload = Vec::new();
        write_u64(&mut payload, node_id);
        let payload = self.send_mutating_request_with_read_timeout(
            ControlPlaneRpcKind::TransferRaftLeadership,
            &payload,
            CONTROL_PLANE_RPC_LEADERSHIP_TRANSFER_TIMEOUT,
        )?;
        decode_admin_mutation_success(ControlPlaneRpcKind::TransferRaftLeadership, || {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            Ok(())
        })
    }

    pub fn trigger_raft_snapshot_and_purge(&self) -> Result<Option<u64>, ControlPlaneError> {
        let payload = self.send_mutating_request_with_read_timeout(
            ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge,
            &[],
            CONTROL_PLANE_RPC_SNAPSHOT_PURGE_TIMEOUT,
        )?;
        decode_admin_mutation_success(ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge, || {
            let mut reader = PayloadReader::new(&payload);
            let snapshot_index = reader.read_option_u64()?;
            reader.finish()?;
            Ok(snapshot_index)
        })
    }

    pub fn trigger_raft_election(&self) -> Result<(), ControlPlaneError> {
        let payload = self.send_mutating_request_with_read_timeout(
            ControlPlaneRpcKind::TriggerRaftElection,
            &[],
            CONTROL_PLANE_RPC_LEADERSHIP_TRANSFER_TIMEOUT,
        )?;
        decode_admin_mutation_success(ControlPlaneRpcKind::TriggerRaftElection, || {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            Ok(())
        })
    }
}

impl AuthenticatedUnixControlPlaneClient {
    pub fn runtime_map_diagnostics(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapDiagnostics, ControlPlaneError> {
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapDiagnostics,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let diagnostics = read_control_plane_runtime_map_diagnostics(&mut reader)?;
        reader.finish()?;
        Ok(diagnostics)
    }

    #[must_use]
    pub fn new(inner: UnixControlPlaneClient, credential: ControlPlaneScopedCredential) -> Self {
        Self { inner, credential }
    }

    #[must_use]
    pub fn inner(&self) -> &UnixControlPlaneClient {
        &self.inner
    }

    #[must_use]
    pub fn credential(&self) -> &ControlPlaneScopedCredential {
        &self.credential
    }

    fn send_verified_request_with_endpoint_failover_until<B, V>(
        &self,
        kind: ControlPlaneRpcKind,
        deadline: Instant,
        build_payload: B,
        verify_response: V,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        B: FnMut() -> Result<Vec<u8>, ControlPlaneError>,
        V: FnMut(&[u8]) -> Result<Vec<u8>, ControlPlaneError>,
    {
        let mut endpoint_pass = self.inner.endpoint_pass();
        self.send_verified_request_with_endpoint_pass_until(
            kind,
            deadline,
            &mut endpoint_pass,
            build_payload,
            verify_response,
        )
    }

    fn send_verified_request_with_endpoint_pass_until<B, V>(
        &self,
        kind: ControlPlaneRpcKind,
        deadline: Instant,
        endpoint_pass: &mut ControlPlaneEndpointPass,
        mut build_payload: B,
        mut verify_response: V,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        B: FnMut() -> Result<Vec<u8>, ControlPlaneError>,
        V: FnMut(&[u8]) -> Result<Vec<u8>, ControlPlaneError>,
    {
        let mut last_routing_error = None;
        while !endpoint_pass.is_exhausted() {
            let payload = build_payload()?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(last_routing_error.unwrap_or_else(|| {
                    ControlPlaneError::io(
                        "control-plane RPC endpoint failover deadline",
                        std::io::Error::new(
                            ErrorKind::TimedOut,
                            format!("{kind:?} endpoint failover deadline expired"),
                        ),
                    )
                }));
            }
            let response = self
                .inner
                .send_request_raw_response_with_endpoint_pass_until(
                    kind,
                    &payload,
                    deadline,
                    endpoint_pass,
                )?;
            let response = verify_response(&response)?;
            match decode_control_plane_rpc_response(response) {
                Err(error) if error.is_control_plane_leader_routing_rejection() => {
                    last_routing_error = Some(error);
                    self.inner.prefer_next_endpoint_after_failure(endpoint_pass);
                }
                result => {
                    self.inner.prefer_successful_endpoint(endpoint_pass);
                    return result;
                }
            }
        }
        Err(last_routing_error.expect("leader routing retry requires at least one endpoint"))
    }

    fn sign_read_only_request(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let expires_at_ms = authority_now_ms
            .checked_add(CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS)
            .ok_or(ControlPlaneError::LeaseDeadlineOverflow)?;
        let payload = write_authenticated_control_plane_rpc_payload(kind, &payload);
        let envelope = self.credential.sign_envelope(
            crate::control_plane_auth::ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Service(
                    crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
                ),
                operation: kind.auth_operation(),
                issued_at_ms: Some(authority_now_ms),
                expires_at_ms: Some(expires_at_ms),
                sequence: None,
                nonce: Vec::new(),
                payload,
            },
        )?;
        envelope.encode_frame()
    }

    fn sign_admin_control_plane_request(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        debug_assert!(
            kind.auth_operation() == ControlPlaneAuthOperation::AdminControlPlaneCommand
                || kind == ControlPlaneRpcKind::PgRuntimeMapSnapshot
        );
        let expires_at_ms = authority_now_ms
            .checked_add(CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS)
            .ok_or(ControlPlaneError::LeaseDeadlineOverflow)?;
        let payload = write_authenticated_control_plane_rpc_payload(kind, &payload);
        let envelope = self.credential.sign_envelope(
            crate::control_plane_auth::ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Service(
                    crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
                ),
                operation: ControlPlaneAuthOperation::AdminControlPlaneCommand,
                issued_at_ms: Some(authority_now_ms),
                expires_at_ms: Some(expires_at_ms),
                sequence: None,
                nonce: Vec::new(),
                payload,
            },
        )?;
        envelope.encode_frame()
    }

    fn send_admin_request_with_read_timeout(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: Vec<u8>,
        read_timeout: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        // The server signs after dispatch, so verify against a fresh receive-time wall sample.
        // Projecting the request timestamp with elapsed monotonic time diverges after a wall step.
        self.send_admin_request_until_and_clocks(
            kind,
            payload,
            Instant::now() + read_timeout,
            || Ok(authority_now_ms),
            || Ok(crate::clock::current_time_millis()),
        )
    }

    #[cfg(test)]
    fn send_admin_request_with_read_timeout_and_clock<F>(
        &self,
        kind: ControlPlaneRpcKind,
        payload: Vec<u8>,
        read_timeout: Duration,
        mut authority_now_ms: F,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        F: FnMut() -> Result<u64, ControlPlaneError>,
    {
        let authority_now_ms = std::cell::RefCell::new(&mut authority_now_ms);
        self.send_admin_request_until_and_clocks(
            kind,
            payload,
            Instant::now() + read_timeout,
            || authority_now_ms.borrow_mut()(),
            || authority_now_ms.borrow_mut()(),
        )
    }

    #[cfg(test)]
    fn send_admin_request_with_read_timeout_and_clocks<R, S>(
        &self,
        kind: ControlPlaneRpcKind,
        payload: Vec<u8>,
        read_timeout: Duration,
        request_authority_now_ms: R,
        response_authority_now_ms: S,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        R: FnMut() -> Result<u64, ControlPlaneError>,
        S: FnMut() -> Result<u64, ControlPlaneError>,
    {
        self.send_admin_request_until_and_clocks(
            kind,
            payload,
            Instant::now() + read_timeout,
            request_authority_now_ms,
            response_authority_now_ms,
        )
    }

    fn send_admin_request_until_and_clocks<R, S>(
        &self,
        kind: ControlPlaneRpcKind,
        payload: Vec<u8>,
        deadline: Instant,
        mut request_authority_now_ms: R,
        mut response_authority_now_ms: S,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        R: FnMut() -> Result<u64, ControlPlaneError>,
        S: FnMut() -> Result<u64, ControlPlaneError>,
    {
        let mut last_routing_error = None;
        let mut endpoint_pass = self.inner.endpoint_pass();
        while !endpoint_pass.is_exhausted() {
            let request = self.sign_admin_control_plane_request(
                kind,
                request_authority_now_ms()?,
                payload.clone(),
            )?;
            let response = match self
                .inner
                .send_request_raw_response_with_endpoint_pass_until_classified(
                    kind,
                    &request,
                    deadline,
                    &mut endpoint_pass,
                ) {
                Ok(response) => response,
                Err(error) if error.request_may_have_been_sent() => {
                    return Err(classify_authenticated_admin_post_request_error(
                        kind,
                        error.into_error(),
                    ));
                }
                Err(error) => return Err(error.into_error()),
            };
            let response_now_ms = response_authority_now_ms()
                .map_err(|error| classify_authenticated_admin_post_request_error(kind, error))?;
            let response = self
                .verify_admin_control_plane_response(kind, response_now_ms, &response)
                .map_err(|error| classify_authenticated_admin_post_request_error(kind, error))?;
            let response = decode_control_plane_rpc_response_frame(response)
                .map_err(|error| classify_authenticated_admin_post_request_error(kind, error))?;
            match response {
                DecodedControlPlaneRpcResponse::Rejection(error)
                    if error.is_control_plane_leader_routing_rejection() =>
                {
                    last_routing_error = Some(error);
                    self.inner
                        .prefer_next_endpoint_after_failure(&endpoint_pass);
                }
                DecodedControlPlaneRpcResponse::Rejection(error) => {
                    self.inner.prefer_successful_endpoint(&endpoint_pass);
                    return Err(error);
                }
                DecodedControlPlaneRpcResponse::Success(payload) => {
                    self.inner.prefer_successful_endpoint(&endpoint_pass);
                    return Ok(payload);
                }
            }
        }
        Err(last_routing_error.expect("leader routing retry requires at least one endpoint"))
    }

    fn admin_pg_runtime_map_snapshot_with_read_timeout(
        &self,
        pg_id: PgId,
        retry_clock: AuthenticatedAdminRetryClock,
        read_timeout: Duration,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let deadline = Instant::now() + CONTROL_PLANE_RPC_READ_ONLY_RETRY_DEADLINE;
        let response = loop {
            match self.send_verified_request_with_endpoint_failover_until(
                ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                Instant::now() + read_timeout,
                || {
                    self.sign_admin_control_plane_request(
                        ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                        retry_clock.now_ms(),
                        payload.clone(),
                    )
                },
                |response| {
                    self.verify_admin_control_plane_response(
                        ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                        retry_clock.now_ms(),
                        response,
                    )
                },
            ) {
                Ok(response) => break response,
                Err(error)
                    if error.is_retryable_read_only_rpc_transport_error()
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(CONTROL_PLANE_RPC_READ_ONLY_RETRY_BACKOFF);
                }
                Err(error) => return Err(error),
            }
        };
        let mut reader = PayloadReader::new(&response);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    fn pg_acting_set_preflight_route(
        &self,
        pg_id: PgId,
        retry_clock: AuthenticatedAdminRetryClock,
    ) -> Result<PgActingSetPreflightRoute, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let mut last_retryable_error = None;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(last_retryable_error
                    .unwrap_or_else(|| pg_acting_set_preflight_deadline_error(pg_id)));
            }
            let attempt_deadline = (now + CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT).min(deadline);
            match self.send_verified_request_with_endpoint_failover_until(
                ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                attempt_deadline,
                || {
                    self.sign_admin_control_plane_request(
                        ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                        retry_clock.now_ms(),
                        payload.clone(),
                    )
                },
                |response| {
                    self.verify_admin_control_plane_response(
                        ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                        retry_clock.now_ms(),
                        response,
                    )
                },
            ) {
                Ok(response) => {
                    let mut reader = PayloadReader::new(&response);
                    let runtime_map = read_runtime_map_snapshot(&mut reader)?;
                    reader.finish()?;
                    return pg_acting_set_preflight_route(runtime_map, pg_id);
                }
                Err(ControlPlaneError::UnknownPg {
                    pg_id: unknown_pg_id,
                }) if unknown_pg_id == pg_id.get() => {
                    return Ok(PgActingSetPreflightRoute::Absent);
                }
                Err(error)
                    if error.is_retryable_read_only_rpc_transport_error()
                        || error.is_transient_runtime_map_serving_gap() =>
                {
                    last_retryable_error = Some(error);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let backoff = CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF.min(remaining);
                    if !backoff.is_zero() {
                        std::thread::sleep(backoff);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn retry_set_pg_acting_set_after_retryable_failure(
        &self,
        pg_id: PgId,
        acting_set: &[NodeId],
        pre_update_route: PgActingSetPreflightRoute,
        retry_clock: AuthenticatedAdminRetryClock,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut last_unconfirmed_message = None;
        loop {
            match self.admin_pg_runtime_map_snapshot_with_read_timeout(
                pg_id,
                retry_clock,
                CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
            ) {
                Ok(runtime_map) => {
                    let Some(route) = runtime_map
                        .pg_routes()
                        .iter()
                        .find(|route| route.pg_id() == pg_id)
                    else {
                        return Err(ControlPlaneError::rpc_protocol(format!(
                            "PG-specific runtime map response omitted requested PG {}",
                            pg_id.get()
                        )));
                    };
                    if route.acting_set() == acting_set {
                        return Ok(route.cluster_epoch());
                    }
                    let message = format!(
                        "PG {} authenticated acting-set update was not confirmed after retryable control-plane failure: current route at epoch {} has acting set {:?}, expected {:?}",
                        pg_id.get(),
                        route.cluster_epoch().get(),
                        route.acting_set(),
                        acting_set
                    );
                    match &pre_update_route {
                        PgActingSetPreflightRoute::Absent => {
                            return Err(ControlPlaneError::RpcUnconfirmed { message });
                        }
                        PgActingSetPreflightRoute::Present(before) => {
                            match pg_acting_set_retry_route_disposition(before, route) {
                                PgActingSetRetryRouteDisposition::Conflict => {
                                    return Err(ControlPlaneError::RpcUnconfirmed { message });
                                }
                                PgActingSetRetryRouteDisposition::RetryReady => {
                                    match self.set_pg_acting_set(
                                        pg_id,
                                        acting_set.to_vec(),
                                        retry_clock.now_ms(),
                                    ) {
                                        Ok(cluster_epoch) => return Ok(cluster_epoch),
                                        Err(error)
                                            if error.is_retryable_pg_acting_set_checked_error() => {
                                        }
                                        Err(error) => return Err(error),
                                    }
                                }
                                PgActingSetRetryRouteDisposition::Wait => {}
                            }
                        }
                    }
                    last_unconfirmed_message = Some(message);
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: last_unconfirmed_message
                                .expect("mismatched route message was recorded"),
                        });
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(error)
                    if error.is_maybe_applied_control_plane_rpc_response_loss()
                        || error.is_transient_runtime_map_serving_gap() =>
                {
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: last_unconfirmed_message.unwrap_or_else(|| format!(
                                "PG {} authenticated acting-set update was not confirmed after retryable control-plane failure; runtime-map observation failed: {error}",
                                pg_id.get()
                            )),
                        });
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(ControlPlaneError::UnknownPg {
                    pg_id: unknown_pg_id,
                }) if unknown_pg_id == pg_id.get()
                    && matches!(&pre_update_route, PgActingSetPreflightRoute::Absent) =>
                {
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: format!(
                                "PG {} authenticated acting-set update was not confirmed after retryable control-plane failure: PG remains absent",
                                pg_id.get()
                            ),
                        });
                    }
                    match self.set_pg_acting_set(pg_id, acting_set.to_vec(), retry_clock.now_ms()) {
                        Ok(cluster_epoch) => return Ok(cluster_epoch),
                        Err(error) if error.is_retryable_pg_acting_set_checked_error() => {}
                        Err(error) => return Err(error),
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn wait_for_metadata_transfer_install_applied(
        &self,
        pg_id: PgId,
        acting_set: &[NodeId],
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
        retry_clock: AuthenticatedAdminRetryClock,
        original_error: &ControlPlaneError,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut last_observation_error = None;
        loop {
            match self.admin_pg_runtime_map_snapshot_with_read_timeout(
                pg_id,
                retry_clock,
                CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
            ) {
                Ok(runtime_map) => {
                    if metadata_transfer_install_applied(
                        &runtime_map,
                        pg_id,
                        acting_set,
                        transfer,
                        expected_destination_epoch,
                    ) {
                        return Ok(runtime_map);
                    }
                    last_observation_error = Some(
                        runtime_map
                            .pg_routes()
                            .iter()
                            .find(|route| route.pg_id() == pg_id)
                            .map_or_else(
                                || {
                                    format!(
                                        "runtime map at epoch {} has no route for PG {}",
                                        runtime_map.cluster_epoch().get(),
                                        pg_id.get()
                                    )
                                },
                                |route| {
                                    format!(
                                        "runtime map at epoch {} route epoch {} state {:?} acting set {:?} transfer {:?}",
                                        runtime_map.cluster_epoch().get(),
                                        route.cluster_epoch().get(),
                                        route.state(),
                                        route.acting_set(),
                                        route.peering_metadata_transfer()
                                    )
                                },
                            ),
                    );
                }
                Err(error) => {
                    if last_observation_error.is_none() {
                        last_observation_error = Some(error.to_string());
                    }
                }
            }
            if Instant::now() >= deadline {
                let mut message = format!(
                    "metadata-transfer authenticated acting-set install for PG {} was not observable after lost control-plane RPC response: {original_error}",
                    pg_id.get()
                );
                if let Some(error) = last_observation_error {
                    message.push_str("; last runtime-map observation error: ");
                    message.push_str(&error);
                }
                return Err(ControlPlaneError::RpcUnconfirmed { message });
            }
            std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
        }
    }

    fn verify_admin_control_plane_response(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let operation = ControlPlaneAuthOperation::AdminControlPlaneResponse;
        let envelope =
            ControlPlaneAuthEnvelope::decode_frame(payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)?;
        let response_credential = self
            .credential
            .admin_control_plane_response_credential_for_admin()?;
        let verifier = ControlPlaneScopedCredentialStore::new(vec![response_credential])?;
        let expected_source = ControlPlaneAuthPrincipal::Service {
            service: ControlPlaneAuthService::Admin,
        };
        let expected_target =
            ControlPlaneAuthTarget::Principal(self.credential.principal().clone());
        match verifier.verify_envelope(
            crate::control_plane_auth::ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: self.credential.cluster_id(),
                expected_source: &expected_source,
                expected_target: &expected_target,
                expected_operation: operation,
                replay_policy: control_plane_rpc_response_auth_replay_policy(authority_now_ms),
            },
        ) {
            ControlPlaneAuthDecision::Accepted { .. } => {
                read_authenticated_control_plane_rpc_payload(kind, envelope.payload())
            }
            ControlPlaneAuthDecision::Rejected { reason } => Err(ControlPlaneError::rpc_protocol(
                format!("control-plane admin response auth rejected: {reason:?}"),
            )),
        }
    }

    pub fn set_pg_acting_set(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        authority_now_ms: u64,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_acting_set_request(&mut payload, pg_id, &acting_set)?;
        let payload = self.send_admin_request_with_read_timeout(
            ControlPlaneRpcKind::SetPgActingSet,
            authority_now_ms,
            payload,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
        )?;
        decode_authenticated_admin_mutation_success(ControlPlaneRpcKind::SetPgActingSet, || {
            let mut reader = PayloadReader::new(&payload);
            let raw_cluster_epoch = reader.read_u64()?;
            let cluster_epoch = ClusterEpoch::new(raw_cluster_epoch).ok_or_else(|| {
                ControlPlaneError::rpc_protocol(format!(
                    "invalid cluster epoch {raw_cluster_epoch}"
                ))
            })?;
            reader.finish()?;
            Ok(cluster_epoch)
        })
    }

    pub fn set_pg_acting_set_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        authority_now_ms: u64,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let retry_clock = AuthenticatedAdminRetryClock::new(authority_now_ms);
        self.set_pg_acting_set_checked_with_retry_clock(pg_id, acting_set, retry_clock)
    }

    fn set_pg_acting_set_checked_with_retry_clock(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        retry_clock: AuthenticatedAdminRetryClock,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let pre_update_route = self.pg_acting_set_preflight_route(pg_id, retry_clock)?;
        match self.set_pg_acting_set(pg_id, acting_set.clone(), retry_clock.now_ms()) {
            Ok(cluster_epoch) => Ok(cluster_epoch),
            Err(error) if error.is_retryable_pg_acting_set_checked_error() => self
                .retry_set_pg_acting_set_after_retryable_failure(
                    pg_id,
                    &acting_set,
                    pre_update_route,
                    retry_clock,
                ),
            Err(error) => Err(error),
        }
    }

    pub fn fence_pg_for_metadata_transfer_runtime_map_checked(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        Ok(self
            .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(
                pg_id,
                authority_now_ms,
            )?
            .into_parts()
            .0)
    }

    pub fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        let retry_clock = AuthenticatedAdminRetryClock::new(authority_now_ms);
        retry_checked_metadata_transfer_fence(pg_id, |deadline| {
            self.fence_pg_for_metadata_transfer_runtime_map_with_source_lease_until(
                pg_id,
                retry_clock,
                deadline,
            )
        })
    }

    fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease_until(
        &self,
        pg_id: PgId,
        retry_clock: AuthenticatedAdminRetryClock,
        deadline: Instant,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_admin_request_until_and_clocks(
            ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap,
            payload,
            deadline,
            || Ok(retry_clock.now_ms()),
            || Ok(crate::clock::current_time_millis()),
        )?;
        let (runtime_map, source_primary_lease_deadline_ms) =
            decode_authenticated_admin_mutation_success(
                ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap,
                || {
                    let mut reader = PayloadReader::new(&payload);
                    let runtime_map = read_runtime_map_snapshot(&mut reader)?;
                    let source_primary_lease_deadline_ms = reader.read_option_u64()?;
                    reader.finish()?;
                    Ok((runtime_map, source_primary_lease_deadline_ms))
                },
            )?;
        self.inner.validate_metadata_transfer_fence_response(
            pg_id,
            FencedPgMetadataTransferRuntimeMap::new(runtime_map, source_primary_lease_deadline_ms),
        )
    }

    pub fn set_pg_acting_set_with_metadata_transfer_runtime_map(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_acting_set_with_metadata_transfer_request(
            &mut payload,
            pg_id,
            &acting_set,
            transfer,
            expected_destination_epoch,
        )?;
        let payload = self.send_admin_request_with_read_timeout(
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap,
            authority_now_ms,
            payload,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
        )?;
        decode_authenticated_admin_mutation_success(
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap,
            || {
                let mut reader = PayloadReader::new(&payload);
                let runtime_map = read_runtime_map_snapshot(&mut reader)?;
                reader.finish()?;
                Ok(runtime_map)
            },
        )
    }

    pub fn set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let retry_clock = AuthenticatedAdminRetryClock::new(authority_now_ms);
        match self.set_pg_acting_set_with_metadata_transfer_runtime_map(
            pg_id,
            acting_set.clone(),
            transfer,
            expected_destination_epoch,
            authority_now_ms,
        ) {
            Ok(runtime_map)
                if metadata_transfer_install_applied(
                    &runtime_map,
                    pg_id,
                    &acting_set,
                    transfer,
                    expected_destination_epoch,
                ) =>
            {
                Ok(runtime_map)
            }
            Ok(runtime_map) => Err(ControlPlaneError::RpcUnconfirmed {
                message: format!(
                    "metadata-transfer acting-set install for PG {} returned runtime map at epoch {} without the expected route/proof",
                    pg_id.get(),
                    runtime_map.cluster_epoch().get()
                ),
            }),
            Err(error) if error.is_unconfirmed_control_plane_mutation() => self
                .wait_for_metadata_transfer_install_applied(
                    pg_id,
                    &acting_set,
                    transfer,
                    expected_destination_epoch,
                    retry_clock,
                    &error,
                ),
            Err(error) => Err(error),
        }
    }

    pub fn set_pg_acting_set_with_metadata_transfer_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
        authority_now_ms: u64,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        self.set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
            pg_id,
            acting_set,
            transfer,
            expected_destination_epoch,
            authority_now_ms,
        )
        .map(|runtime_map| runtime_map.cluster_epoch())
    }

    pub fn transfer_raft_leadership_to(
        &self,
        node_id: u64,
        authority_now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        let mut payload = Vec::new();
        write_u64(&mut payload, node_id);
        let payload = self.send_admin_request_with_read_timeout(
            ControlPlaneRpcKind::TransferRaftLeadership,
            authority_now_ms,
            payload,
            CONTROL_PLANE_RPC_LEADERSHIP_TRANSFER_TIMEOUT,
        )?;
        decode_authenticated_admin_mutation_success(
            ControlPlaneRpcKind::TransferRaftLeadership,
            || {
                let reader = PayloadReader::new(&payload);
                reader.finish()?;
                Ok(())
            },
        )
    }

    pub fn trigger_raft_snapshot_and_purge(
        &self,
        authority_now_ms: u64,
    ) -> Result<Option<u64>, ControlPlaneError> {
        let payload = self.send_admin_request_with_read_timeout(
            ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_SNAPSHOT_PURGE_TIMEOUT,
        )?;
        decode_authenticated_admin_mutation_success(
            ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge,
            || {
                let mut reader = PayloadReader::new(&payload);
                let snapshot_index = reader.read_option_u64()?;
                reader.finish()?;
                Ok(snapshot_index)
            },
        )
    }

    pub fn trigger_raft_election(&self, authority_now_ms: u64) -> Result<(), ControlPlaneError> {
        let payload = self.send_admin_request_with_read_timeout(
            ControlPlaneRpcKind::TriggerRaftElection,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_LEADERSHIP_TRANSFER_TIMEOUT,
        )?;
        decode_authenticated_admin_mutation_success(
            ControlPlaneRpcKind::TriggerRaftElection,
            || {
                let reader = PayloadReader::new(&payload);
                reader.finish()?;
                Ok(())
            },
        )
    }

    pub fn authority_clock_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        self.authority_clock_status_until(
            authority_now_ms,
            Instant::now() + CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ADMIN_TIMEOUT,
        )
    }

    fn authority_clock_status_until(
        &self,
        authority_now_ms: u64,
        deadline: Instant,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        self.authority_clock_status_until_with_attempt_timeout(
            authority_now_ms,
            deadline,
            CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ATTEMPT_TIMEOUT,
        )
    }

    fn authority_clock_status_until_with_attempt_timeout(
        &self,
        authority_now_ms: u64,
        deadline: Instant,
        attempt_timeout: Duration,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        let mut endpoint_pass = self.inner.endpoint_pass();
        self.authority_clock_status_with_endpoint_pass_until(
            authority_now_ms,
            deadline,
            attempt_timeout,
            &mut endpoint_pass,
        )
    }

    fn authority_clock_status_with_endpoint_pass_until(
        &self,
        authority_now_ms: u64,
        deadline: Instant,
        attempt_timeout: Duration,
        endpoint_pass: &mut ControlPlaneEndpointPass,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        loop {
            let attempt_deadline =
                authority_clock_admin_attempt_deadline(deadline, attempt_timeout)?;
            let payload = self.send_verified_request_with_endpoint_pass_until(
                ControlPlaneRpcKind::AuthorityClockStatus,
                attempt_deadline,
                endpoint_pass,
                || {
                    self.sign_admin_control_plane_request(
                        ControlPlaneRpcKind::AuthorityClockStatus,
                        authority_now_ms,
                        Vec::new(),
                    )
                },
                |response| {
                    self.verify_admin_control_plane_response(
                        ControlPlaneRpcKind::AuthorityClockStatus,
                        crate::clock::current_time_millis(),
                        response,
                    )
                },
            )?;
            let mut reader = PayloadReader::new(&payload);
            let status = read_authority_clock_status(&mut reader)?;
            reader.finish()?;
            if status.current_raft_leadership_term().is_none()
                || status.local_raft_authority_leader()
                || endpoint_pass.is_exhausted()
            {
                return Ok(status);
            }
            self.inner.prefer_next_endpoint_after_failure(endpoint_pass);
        }
    }

    fn reestablish_authority_clock_from_status_with_attempt_timeout(
        &self,
        expected: ControlPlaneAuthorityClockStatus,
        authority_now_ms: u64,
        deadline: Instant,
        attempt_timeout: Duration,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        let mut payload = Vec::new();
        write_u64(&mut payload, expected.generation());
        write_option_u64(&mut payload, expected.committed_timestamp_high_water_ms());
        write_option_u64(&mut payload, expected.current_raft_leadership_term());
        let attempt_deadline = authority_clock_admin_attempt_deadline(deadline, attempt_timeout)?;
        let payload = self.send_admin_request_until_and_clocks(
            ControlPlaneRpcKind::ReestablishAuthorityClock,
            payload,
            attempt_deadline,
            || Ok(authority_now_ms),
            || Ok(crate::clock::current_time_millis()),
        )?;
        decode_authenticated_admin_mutation_success(
            ControlPlaneRpcKind::ReestablishAuthorityClock,
            || {
                let mut reader = PayloadReader::new(&payload);
                let status = read_authority_clock_status(&mut reader)?;
                reader.finish()?;
                Ok(status)
            },
        )
    }

    pub fn reestablish_authority_clock(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        self.reestablish_authority_clock_with_attempt_timeout(
            authority_now_ms,
            CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ATTEMPT_TIMEOUT,
        )
    }

    fn reestablish_authority_clock_with_attempt_timeout(
        &self,
        authority_now_ms: u64,
        attempt_timeout: Duration,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        let retry_clock = AuthenticatedAdminRetryClock::new(authority_now_ms);
        let retry_deadline = Instant::now() + CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ADMIN_TIMEOUT;
        'recovery: loop {
            let expected = self.retry_authority_clock_status_until(
                retry_clock.now_ms(),
                retry_deadline,
                attempt_timeout,
            )?;
            if expected.established() {
                return Ok(expected);
            }
            match self.reestablish_authority_clock_from_status_with_attempt_timeout(
                expected,
                retry_clock.now_ms(),
                retry_deadline,
                attempt_timeout,
            ) {
                Ok(status) => return Ok(status),
                Err(error) if error.is_control_plane_leader_routing_rejection() => {
                    let remaining =
                        authority_clock_admin_remaining(retry_deadline).map_err(|_| error)?;
                    std::thread::sleep(
                        CONTROL_PLANE_RPC_AUTHORITY_CLOCK_RETRY_BACKOFF.min(remaining),
                    );
                }
                Err(error) if error.is_unconfirmed_control_plane_mutation() => {
                    let expected_generation = expected
                        .generation()
                        .checked_add(1)
                        .ok_or(ControlPlaneError::AuthorityClockGenerationOverflow)?;
                    let observed = self
                        .retry_authority_clock_status_until(
                            retry_clock.now_ms(),
                            retry_deadline,
                            attempt_timeout,
                        )
                        .map_err(|status_error| ControlPlaneError::RpcUnconfirmed {
                            message: format!(
                                "authority-clock re-establishment response was lost ({error}); status confirmation failed: {status_error}"
                            ),
                        })?;
                    if observed.established()
                        && observed.generation() == expected_generation
                        && observed.committed_timestamp_high_water_ms()
                            == expected.committed_timestamp_high_water_ms()
                        && observed.current_raft_leadership_term()
                            == expected.current_raft_leadership_term()
                    {
                        return Ok(observed);
                    }
                    if observed.generation() == expected.generation()
                        && !observed.established()
                        && observed.committed_timestamp_high_water_ms()
                            == expected.committed_timestamp_high_water_ms()
                        && observed.current_raft_leadership_term()
                            == expected.current_raft_leadership_term()
                    {
                        let remaining = authority_clock_admin_remaining(retry_deadline).map_err(
                            |_| ControlPlaneError::RpcUnconfirmed {
                                message: format!(
                                    "authority-clock re-establishment response was lost ({error}); status remained at the pre-operation generation until the confirmation deadline"
                                ),
                            },
                        )?;
                        std::thread::sleep(
                            CONTROL_PLANE_RPC_AUTHORITY_CLOCK_RETRY_BACKOFF.min(remaining),
                        );
                        // Re-establishment is a compare-and-swap over the
                        // expected generation and authority context. If a
                        // read-back proves those values are unchanged, a
                        // fresh status/mutation handshake cannot apply the
                        // operation twice and is safe after an ambiguous
                        // transport outcome.
                        continue 'recovery;
                    }
                    if observed.local_raft_authority_leader()
                        && observed.current_raft_leadership_term()
                            != expected.current_raft_leadership_term()
                    {
                        continue 'recovery;
                    }
                    return Err(ControlPlaneError::RpcUnconfirmed {
                        message: format!(
                            "authority-clock re-establishment response was lost ({error}); observed status did not confirm the expected generation and authority state"
                        ),
                    });
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn retry_authority_clock_status_until(
        &self,
        authority_now_ms: u64,
        deadline: Instant,
        attempt_timeout: Duration,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        let mut endpoint_pass = self.inner.endpoint_pass();
        loop {
            if endpoint_pass.is_exhausted() {
                endpoint_pass = self.inner.endpoint_pass();
            }
            match self.authority_clock_status_with_endpoint_pass_until(
                authority_now_ms,
                deadline,
                attempt_timeout,
                &mut endpoint_pass,
            ) {
                Ok(status) => return Ok(status),
                Err(error) if error.is_retryable_read_only_rpc_transport_error() => {
                    self.inner
                        .prefer_next_endpoint_after_failure(&endpoint_pass);
                    let remaining = authority_clock_admin_remaining(deadline).map_err(|_| error)?;
                    std::thread::sleep(
                        CONTROL_PLANE_RPC_AUTHORITY_CLOCK_RETRY_BACKOFF.min(remaining),
                    );
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn send_signed_read_only_request_with_read_timeout(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: Vec<u8>,
        read_timeout: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let start = Instant::now();
        self.send_signed_read_only_request_with_read_timeout_and_clock(
            kind,
            payload,
            read_timeout,
            || {
                let elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                Ok(authority_now_ms.saturating_add(elapsed_ms))
            },
        )
    }

    fn send_signed_read_only_request_with_read_timeout_and_clock<F>(
        &self,
        kind: ControlPlaneRpcKind,
        payload: Vec<u8>,
        read_timeout: Duration,
        mut authority_now_ms: F,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        F: FnMut() -> Result<u64, ControlPlaneError>,
    {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_READ_ONLY_RETRY_DEADLINE;
        loop {
            let mut last_retryable_error = None;
            let mut endpoint_pass = self.inner.endpoint_pass();
            while !endpoint_pass.is_exhausted() {
                let request =
                    self.sign_read_only_request(kind, authority_now_ms()?, payload.clone())?;
                let response = self
                    .inner
                    .send_request_raw_response_with_endpoint_pass_until(
                        kind,
                        &request,
                        Instant::now() + read_timeout,
                        &mut endpoint_pass,
                    );
                let response = match response {
                    Ok(response) => response,
                    Err(error)
                        if error.is_retryable_read_only_rpc_transport_error()
                            && Instant::now() < deadline =>
                    {
                        last_retryable_error = Some(error);
                        self.inner
                            .prefer_next_endpoint_after_failure(&endpoint_pass);
                        std::thread::sleep(CONTROL_PLANE_RPC_READ_ONLY_RETRY_BACKOFF);
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let response =
                    self.verify_runtime_map_response(kind, authority_now_ms()?, &response)?;
                match decode_control_plane_rpc_response(response) {
                    Err(error) if error.is_control_plane_leader_routing_rejection() => {
                        last_retryable_error = Some(error);
                        self.inner
                            .prefer_next_endpoint_after_failure(&endpoint_pass);
                    }
                    result => {
                        self.inner.prefer_successful_endpoint(&endpoint_pass);
                        return result;
                    }
                }
            }
            let error = last_retryable_error
                .expect("authenticated read retry requires at least one retryable error");
            if Instant::now() >= deadline {
                return Err(error);
            }
            std::thread::sleep(CONTROL_PLANE_RPC_READ_ONLY_RETRY_BACKOFF);
        }
    }

    fn verify_runtime_map_response(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let operation = ControlPlaneAuthOperation::RuntimeMapResponse;
        let envelope =
            ControlPlaneAuthEnvelope::decode_frame(payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)?;
        let response_credential = match self.credential.principal() {
            ControlPlaneAuthPrincipal::Frontend { .. } => self
                .credential
                .runtime_map_response_credential_for_frontend()?,
            ControlPlaneAuthPrincipal::StorageNode { .. } => self
                .credential
                .runtime_map_response_credential_for_storage_node()?,
            _ => {
                return Err(ControlPlaneError::rpc_protocol("runtime-map response verification requires a frontend or storage-node credential"
                            .to_owned()));
            }
        };
        let verifier = ControlPlaneScopedCredentialStore::new(vec![response_credential])?;
        let expected_source = ControlPlaneAuthPrincipal::Service {
            service: ControlPlaneAuthService::RuntimeMap,
        };
        let expected_target =
            ControlPlaneAuthTarget::Principal(self.credential.principal().clone());
        match verifier.verify_envelope(
            crate::control_plane_auth::ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: self.credential.cluster_id(),
                expected_source: &expected_source,
                expected_target: &expected_target,
                expected_operation: operation,
                replay_policy: control_plane_rpc_response_auth_replay_policy(authority_now_ms),
            },
        ) {
            ControlPlaneAuthDecision::Accepted { .. } => {
                read_authenticated_control_plane_rpc_payload(kind, envelope.payload())
            }
            ControlPlaneAuthDecision::Rejected { reason } => Err(ControlPlaneError::rpc_protocol(
                format!("control-plane runtime-map response auth rejected: {reason:?}"),
            )),
        }
    }
}

impl ControlPlaneStorageNodeAuthCredential {
    pub fn new(
        input: ControlPlaneStorageNodeAuthCredentialInput,
    ) -> Result<Self, ControlPlaneError> {
        let credential = Self {
            node_id: input.node_id,
            credential_id: input.credential_id,
            credential_version: input.credential_version,
            secret: input.secret,
        };
        credential.scoped_for_cluster_and_incarnation("validation-cluster", 1)?;
        Ok(credential)
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }

    pub fn scoped_for_cluster_and_incarnation(
        &self,
        cluster_id: &str,
        incarnation: u64,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: cluster_id.to_owned(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: ControlPlaneAuthPrincipal::StorageNode {
                node_id: self.node_id,
                incarnation,
            },
            secret: self.secret.clone(),
        })
    }

    pub fn runtime_map_response_credential_for_cluster_and_incarnation(
        &self,
        cluster_id: &str,
        incarnation: u64,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        self.scoped_for_cluster_and_incarnation(cluster_id, incarnation)?
            .runtime_map_response_credential_for_storage_node()
    }
}

impl ControlPlaneFrontendAuthCredential {
    pub fn new(input: ControlPlaneFrontendAuthCredentialInput) -> Result<Self, ControlPlaneError> {
        let credential = Self {
            instance_id: input.instance_id,
            credential_id: input.credential_id,
            credential_version: input.credential_version,
            secret: input.secret,
        };
        credential.scoped_for_cluster("validation-cluster")?;
        Ok(credential)
    }

    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }

    pub fn scoped_for_cluster(
        &self,
        cluster_id: &str,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: cluster_id.to_owned(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: ControlPlaneAuthPrincipal::Frontend {
                instance_id: self.instance_id.clone(),
            },
            secret: self.secret.clone(),
        })
    }

    pub fn runtime_map_response_credential_for_cluster(
        &self,
        cluster_id: &str,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: cluster_id.to_owned(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: ControlPlaneAuthPrincipal::Service {
                service: ControlPlaneAuthService::RuntimeMap,
            },
            secret: self.secret.clone(),
        })
    }
}

impl ControlPlaneAdminAuthCredential {
    pub fn new(input: ControlPlaneAdminAuthCredentialInput) -> Result<Self, ControlPlaneError> {
        let credential = Self {
            instance_id: input.instance_id,
            credential_id: input.credential_id,
            credential_version: input.credential_version,
            secret: input.secret,
        };
        credential.scoped_for_cluster("validation-cluster")?;
        Ok(credential)
    }

    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }

    pub fn scoped_for_cluster(
        &self,
        cluster_id: &str,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: cluster_id.to_owned(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: ControlPlaneAuthPrincipal::Admin {
                instance_id: self.instance_id.clone(),
            },
            secret: self.secret.clone(),
        })
    }

    pub fn admin_control_plane_response_credential_for_cluster(
        &self,
        cluster_id: &str,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        self.scoped_for_cluster(cluster_id)?
            .admin_control_plane_response_credential_for_admin()
    }
}

fn insert_storage_node_auth_credential(
    credentials_by_node: &mut BTreeMap<NodeId, Vec<ControlPlaneStorageNodeAuthCredential>>,
    credential: ControlPlaneStorageNodeAuthCredential,
) -> Result<(), ControlPlaneError> {
    let credentials = credentials_by_node.entry(credential.node_id()).or_default();
    if credentials.iter().any(|existing| {
        existing.credential_id() == credential.credential_id()
            && existing.credential_version() == credential.credential_version()
    }) {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane storage-node auth credential repeats credential identity".to_owned(),
        ));
    }
    credentials.push(credential);
    Ok(())
}

fn insert_frontend_auth_credential(
    credentials_by_instance: &mut BTreeMap<String, Vec<ControlPlaneFrontendAuthCredential>>,
    credential: ControlPlaneFrontendAuthCredential,
) -> Result<(), ControlPlaneError> {
    let credentials = credentials_by_instance
        .entry(credential.instance_id().to_owned())
        .or_default();
    if credentials.iter().any(|existing| {
        existing.credential_id() == credential.credential_id()
            && existing.credential_version() == credential.credential_version()
    }) {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane frontend auth credential repeats credential identity".to_owned(),
        ));
    }
    credentials.push(credential);
    Ok(())
}

fn insert_admin_auth_credential(
    credentials_by_instance: &mut BTreeMap<String, Vec<ControlPlaneAdminAuthCredential>>,
    credential: ControlPlaneAdminAuthCredential,
) -> Result<(), ControlPlaneError> {
    let credentials = credentials_by_instance
        .entry(credential.instance_id().to_owned())
        .or_default();
    if credentials.iter().any(|existing| {
        existing.credential_id() == credential.credential_id()
            && existing.credential_version() == credential.credential_version()
    }) {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane admin auth credential repeats credential identity".to_owned(),
        ));
    }
    credentials.push(credential);
    Ok(())
}

fn matching_storage_node_auth_credential<'a>(
    credentials: &'a [ControlPlaneStorageNodeAuthCredential],
    credential_id: &str,
    credential_version: u64,
) -> Result<&'a ControlPlaneStorageNodeAuthCredential, ControlPlaneError> {
    credentials
        .iter()
        .find(|credential| {
            credential.credential_id() == credential_id
                && credential.credential_version() == credential_version
        })
        .ok_or_else(|| {
            ControlPlaneError::rpc_protocol(
                "accepted storage-node auth credential was not configured".to_owned(),
            )
        })
}

fn matching_frontend_auth_credential<'a>(
    credentials: &'a [ControlPlaneFrontendAuthCredential],
    credential_id: &str,
    credential_version: u64,
) -> Result<&'a ControlPlaneFrontendAuthCredential, ControlPlaneError> {
    credentials
        .iter()
        .find(|credential| {
            credential.credential_id() == credential_id
                && credential.credential_version() == credential_version
        })
        .ok_or_else(|| {
            ControlPlaneError::rpc_protocol(
                "accepted frontend auth credential was not configured".to_owned(),
            )
        })
}

fn matching_admin_auth_credential<'a>(
    credentials: &'a [ControlPlaneAdminAuthCredential],
    credential_id: &str,
    credential_version: u64,
) -> Result<&'a ControlPlaneAdminAuthCredential, ControlPlaneError> {
    credentials
        .iter()
        .find(|credential| {
            credential.credential_id() == credential_id
                && credential.credential_version() == credential_version
        })
        .ok_or_else(|| {
            ControlPlaneError::rpc_protocol(
                "accepted admin auth credential was not configured".to_owned(),
            )
        })
}

impl ControlPlaneUnixAuthVerifier {
    pub fn new_empty(cluster_id: impl Into<String>) -> Result<Self, ControlPlaneError> {
        let cluster_id = cluster_id.into();
        if cluster_id.is_empty() {
            return Err(ControlPlaneError::rpc_protocol(
                "control-plane auth cluster id must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            cluster_id,
            storage_node_credentials: BTreeMap::new(),
            frontend_credentials: BTreeMap::new(),
            admin_credentials: BTreeMap::new(),
            metrics: Arc::new(ControlPlaneUnixAuthMetrics::default()),
        })
    }

    pub fn new(
        cluster_id: impl Into<String>,
        storage_node_credentials: Vec<ControlPlaneStorageNodeAuthCredential>,
    ) -> Result<Self, ControlPlaneError> {
        if storage_node_credentials.is_empty() {
            return Err(ControlPlaneError::rpc_protocol(
                "control-plane storage-node auth credential set is empty".to_owned(),
            ));
        }
        let mut verifier = Self::new_empty(cluster_id)?;
        let mut by_node = BTreeMap::new();
        for credential in storage_node_credentials {
            insert_storage_node_auth_credential(&mut by_node, credential)?;
        }
        verifier.storage_node_credentials = by_node;
        Ok(verifier)
    }

    pub fn with_frontend_credentials(
        mut self,
        frontend_credentials: Vec<ControlPlaneFrontendAuthCredential>,
    ) -> Result<Self, ControlPlaneError> {
        let mut by_instance = BTreeMap::new();
        for credential in frontend_credentials {
            insert_frontend_auth_credential(&mut by_instance, credential)?;
        }
        self.frontend_credentials = by_instance;
        Ok(self)
    }

    pub fn with_admin_credentials(
        mut self,
        admin_credentials: Vec<ControlPlaneAdminAuthCredential>,
    ) -> Result<Self, ControlPlaneError> {
        let mut by_instance = BTreeMap::new();
        for credential in admin_credentials {
            insert_admin_auth_credential(&mut by_instance, credential)?;
        }
        self.admin_credentials = by_instance;
        Ok(self)
    }

    #[cfg(test)]
    #[must_use]
    pub fn metrics_snapshot(&self) -> ControlPlaneUnixAuthMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[must_use]
    pub fn status_snapshot(&self) -> ControlPlaneUnixAuthStatusSnapshot {
        let storage_node_heartbeat_required = self.requires_storage_node_heartbeat_auth();
        let frontend_runtime_map_required = self.requires_frontend_runtime_map_auth();
        let admin_control_plane_required = self.requires_admin_control_plane_auth();
        ControlPlaneUnixAuthStatusSnapshot {
            required: storage_node_heartbeat_required
                || frontend_runtime_map_required
                || admin_control_plane_required,
            storage_node_heartbeat_required,
            frontend_runtime_map_required,
            admin_control_plane_required,
            cluster_id: self.cluster_id.clone(),
            storage_node_credentials: self
                .storage_node_credentials
                .values()
                .flat_map(|credentials| {
                    credentials
                        .iter()
                        .map(|credential| ControlPlaneUnixAuthCredentialStatus {
                            node_id: credential.node_id(),
                            credential_id: credential.credential_id().to_owned(),
                            credential_version: credential.credential_version(),
                        })
                })
                .collect(),
            frontend_credentials: self
                .frontend_credentials
                .values()
                .flat_map(|credentials| {
                    credentials.iter().map(|credential| {
                        ControlPlaneUnixFrontendAuthCredentialStatus {
                            instance_id: credential.instance_id().to_owned(),
                            credential_id: credential.credential_id().to_owned(),
                            credential_version: credential.credential_version(),
                        }
                    })
                })
                .collect(),
            admin_credentials: self
                .admin_credentials
                .values()
                .flat_map(|credentials| {
                    credentials
                        .iter()
                        .map(|credential| ControlPlaneUnixAdminAuthCredentialStatus {
                            instance_id: credential.instance_id().to_owned(),
                            credential_id: credential.credential_id().to_owned(),
                            credential_version: credential.credential_version(),
                        })
                })
                .collect(),
            metrics: self.metrics.snapshot(),
        }
    }

    #[must_use]
    pub fn requires_frontend_runtime_map_auth(&self) -> bool {
        !self.frontend_credentials.is_empty()
    }

    #[must_use]
    pub fn requires_storage_node_heartbeat_auth(&self) -> bool {
        !self.storage_node_credentials.is_empty()
    }

    #[must_use]
    pub fn requires_admin_control_plane_auth(&self) -> bool {
        !self.admin_credentials.is_empty()
    }

    #[cfg(test)]
    pub fn verify_storage_node_heartbeat_request_payload(
        &self,
        payload: &[u8],
        authority_now_ms: u64,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.verify_storage_node_heartbeat_payload(payload, authority_now_ms)
            .map(|verified| verified.payload)
    }

    fn verify_admin_control_plane_command_payload(
        &self,
        expected_kind: ControlPlaneRpcKind,
        payload: &[u8],
        authority_now_ms: u64,
    ) -> Result<VerifiedAdminControlPlaneCommand, ControlPlaneError> {
        let operation = ControlPlaneAuthOperation::AdminControlPlaneCommand;
        let envelope = match ControlPlaneAuthEnvelope::decode_frame(
            payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        ) {
            Ok(envelope) => envelope,
            Err(error) => {
                let reason = if control_plane_auth_payload_has_magic(payload) {
                    ControlPlaneAuthRejectionReason::Malformed
                } else {
                    ControlPlaneAuthRejectionReason::Missing
                };
                self.metrics.record_rejected(operation, reason);
                return Err(error);
            }
        };
        let expected_source = match envelope.header().source() {
            ControlPlaneAuthPrincipal::Admin { instance_id } => ControlPlaneAuthPrincipal::Admin {
                instance_id: instance_id.clone(),
            },
            _ => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::WrongRole);
                return Err(ControlPlaneError::rpc_protocol(
                    "control-plane admin command auth source is not an admin".to_owned(),
                ));
            }
        };
        let ControlPlaneAuthPrincipal::Admin { instance_id } = &expected_source else {
            unreachable!("admin source constructed above");
        };
        let Some(admin_credentials) = self.admin_credentials.get(instance_id) else {
            self.metrics.record_rejected(
                operation,
                ControlPlaneAuthRejectionReason::UnknownCredential,
            );
            return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane admin command auth has no credential for instance {instance_id}"
            )));
        };
        let credentials = match admin_credentials
            .iter()
            .map(|credential| credential.scoped_for_cluster(&self.cluster_id))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(credentials) => credentials,
            Err(error) => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::Malformed);
                return Err(error);
            }
        };
        let verifier = match ControlPlaneScopedCredentialStore::new(credentials) {
            Ok(verifier) => verifier,
            Err(error) => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::Malformed);
                return Err(error);
            }
        };
        let expected_target = ControlPlaneAuthTarget::Service(
            crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
        );
        match verifier.verify_envelope(
            crate::control_plane_auth::ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: &self.cluster_id,
                expected_source: &expected_source,
                expected_target: &expected_target,
                expected_operation: operation,
                replay_policy: control_plane_rpc_auth_replay_policy(authority_now_ms),
            },
        ) {
            ControlPlaneAuthDecision::Accepted {
                credential_id,
                credential_version,
            } => {
                let payload = match read_authenticated_control_plane_rpc_payload(
                    expected_kind,
                    envelope.payload(),
                ) {
                    Ok(payload) => payload,
                    Err(error) => {
                        self.metrics
                            .record_rejected(operation, ControlPlaneAuthRejectionReason::WrongRole);
                        return Err(error);
                    }
                };
                let admin_credential = matching_admin_auth_credential(
                    admin_credentials,
                    &credential_id,
                    credential_version,
                )?;
                let response_credential = admin_credential
                    .admin_control_plane_response_credential_for_cluster(&self.cluster_id)?;
                self.metrics.record_accepted(operation);
                Ok(VerifiedAdminControlPlaneCommand {
                    payload,
                    response_credential,
                    response_target: expected_source,
                })
            }
            ControlPlaneAuthDecision::Rejected { reason } => {
                self.metrics.record_rejected(operation, reason);
                Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane admin command auth rejected: {}",
                    format_control_plane_auth_rejection(reason, &envelope, authority_now_ms)
                )))
            }
        }
    }

    fn verify_admin_runtime_map_read_payload(
        &self,
        expected_kind: ControlPlaneRpcKind,
        payload: &[u8],
        authority_now_ms: u64,
    ) -> Result<VerifiedAdminRuntimeMapRead, ControlPlaneError> {
        let verified = self.verify_admin_control_plane_command_payload(
            expected_kind,
            payload,
            authority_now_ms,
        )?;
        Ok(VerifiedAdminRuntimeMapRead {
            payload: verified.payload,
            response_credential: verified.response_credential,
            response_target: verified.response_target,
        })
    }

    fn verify_frontend_runtime_map_read_payload(
        &self,
        expected_kind: ControlPlaneRpcKind,
        payload: &[u8],
        authority_now_ms: u64,
    ) -> Result<VerifiedFrontendRuntimeMapRead, ControlPlaneError> {
        let operation = ControlPlaneAuthOperation::FrontendRuntimeMapRead;
        let envelope = match ControlPlaneAuthEnvelope::decode_frame(
            payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        ) {
            Ok(envelope) => envelope,
            Err(error) => {
                let reason = if control_plane_auth_payload_has_magic(payload) {
                    ControlPlaneAuthRejectionReason::Malformed
                } else {
                    ControlPlaneAuthRejectionReason::Missing
                };
                self.metrics.record_rejected(operation, reason);
                return Err(error);
            }
        };
        let expected_source = match envelope.header().source() {
            ControlPlaneAuthPrincipal::Frontend { instance_id } => {
                ControlPlaneAuthPrincipal::Frontend {
                    instance_id: instance_id.clone(),
                }
            }
            _ => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::WrongRole);
                return Err(ControlPlaneError::rpc_protocol(
                    "control-plane frontend runtime-map read auth source is not a frontend"
                        .to_owned(),
                ));
            }
        };
        let ControlPlaneAuthPrincipal::Frontend { instance_id } = &expected_source else {
            unreachable!("frontend source constructed above");
        };
        let Some(frontend_credentials) = self.frontend_credentials.get(instance_id) else {
            self.metrics.record_rejected(
                operation,
                ControlPlaneAuthRejectionReason::UnknownCredential,
            );
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane frontend runtime-map read auth has no credential for instance {instance_id}"
                )));
        };
        let credentials = match frontend_credentials
            .iter()
            .map(|credential| credential.scoped_for_cluster(&self.cluster_id))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(credentials) => credentials,
            Err(error) => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::Malformed);
                return Err(error);
            }
        };
        let verifier = match ControlPlaneScopedCredentialStore::new(credentials) {
            Ok(verifier) => verifier,
            Err(error) => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::Malformed);
                return Err(error);
            }
        };
        let expected_target = ControlPlaneAuthTarget::Service(
            crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
        );
        match verifier.verify_envelope(
            crate::control_plane_auth::ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: &self.cluster_id,
                expected_source: &expected_source,
                expected_target: &expected_target,
                expected_operation: operation,
                replay_policy: control_plane_rpc_auth_replay_policy(authority_now_ms),
            },
        ) {
            ControlPlaneAuthDecision::Accepted {
                credential_id,
                credential_version,
            } => {
                let payload = match read_authenticated_control_plane_rpc_payload(
                    expected_kind,
                    envelope.payload(),
                ) {
                    Ok(payload) => payload,
                    Err(error) => {
                        self.metrics
                            .record_rejected(operation, ControlPlaneAuthRejectionReason::WrongRole);
                        return Err(error);
                    }
                };
                let frontend_credential = matching_frontend_auth_credential(
                    frontend_credentials,
                    &credential_id,
                    credential_version,
                )?;
                let response_credential = frontend_credential
                    .runtime_map_response_credential_for_cluster(&self.cluster_id)?;
                self.metrics.record_accepted(operation);
                Ok(VerifiedFrontendRuntimeMapRead {
                    payload,
                    response_credential,
                    response_target: expected_source,
                })
            }
            ControlPlaneAuthDecision::Rejected { reason } => {
                self.metrics.record_rejected(operation, reason);
                Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane frontend runtime-map read auth rejected: {}",
                    format_control_plane_auth_rejection(reason, &envelope, authority_now_ms)
                )))
            }
        }
    }

    fn verify_storage_node_heartbeat_payload(
        &self,
        payload: &[u8],
        authority_now_ms: u64,
    ) -> Result<VerifiedStorageNodeHeartbeatRefresh, ControlPlaneError> {
        let envelope = match ControlPlaneAuthEnvelope::decode_frame(
            payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        ) {
            Ok(envelope) => envelope,
            Err(error) => {
                let reason = if control_plane_auth_payload_has_magic(payload) {
                    ControlPlaneAuthRejectionReason::Malformed
                } else {
                    ControlPlaneAuthRejectionReason::Missing
                };
                self.metrics
                    .record_rejected(ControlPlaneAuthOperation::StorageRuntimeMapRefresh, reason);
                return Err(error);
            }
        };
        let payload = match read_authenticated_control_plane_rpc_payload(
            ControlPlaneRpcKind::RefreshNodeHeartbeat,
            envelope.payload(),
        ) {
            Ok(payload) => payload,
            Err(error) => {
                self.metrics.record_rejected(
                    ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                    ControlPlaneAuthRejectionReason::WrongRole,
                );
                return Err(error);
            }
        };
        let heartbeat = match read_node_heartbeat_payload(&payload) {
            Ok(heartbeat) => heartbeat,
            Err(error) => {
                self.metrics.record_rejected(
                    ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                    ControlPlaneAuthRejectionReason::Malformed,
                );
                return Err(error);
            }
        };
        let expected_source = ControlPlaneAuthPrincipal::StorageNode {
            node_id: heartbeat.node_id,
            incarnation: heartbeat.node_incarnation,
        };
        let expected_target = ControlPlaneAuthTarget::Service(
            crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
        );
        let Some(node_credentials) = self.storage_node_credentials.get(&heartbeat.node_id) else {
            self.metrics.record_rejected(
                ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                ControlPlaneAuthRejectionReason::UnknownCredential,
            );
            return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane storage-node heartbeat auth has no credential for node {}",
                heartbeat.node_id.as_u32()
            )));
        };
        let credentials = match node_credentials
            .iter()
            .map(|credential| {
                credential.scoped_for_cluster_and_incarnation(
                    &self.cluster_id,
                    heartbeat.node_incarnation,
                )
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(credentials) => credentials,
            Err(error) => {
                self.metrics.record_rejected(
                    ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                    ControlPlaneAuthRejectionReason::Malformed,
                );
                return Err(error);
            }
        };
        let verifier = match ControlPlaneScopedCredentialStore::new(credentials) {
            Ok(verifier) => verifier,
            Err(error) => {
                self.metrics.record_rejected(
                    ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                    ControlPlaneAuthRejectionReason::Malformed,
                );
                return Err(error);
            }
        };
        match verifier.verify_envelope(
            crate::control_plane_auth::ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: &self.cluster_id,
                expected_source: &expected_source,
                expected_target: &expected_target,
                expected_operation: ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                replay_policy: storage_node_heartbeat_auth_replay_policy(
                    &heartbeat,
                    authority_now_ms,
                ),
            },
        ) {
            ControlPlaneAuthDecision::Accepted {
                credential_id,
                credential_version,
            } => {
                let node_credential = matching_storage_node_auth_credential(
                    node_credentials,
                    &credential_id,
                    credential_version,
                )?;
                let response_credential = node_credential
                    .runtime_map_response_credential_for_cluster_and_incarnation(
                        &self.cluster_id,
                        heartbeat.node_incarnation,
                    )?;
                self.metrics
                    .record_accepted(ControlPlaneAuthOperation::StorageRuntimeMapRefresh);
                Ok(VerifiedStorageNodeHeartbeatRefresh {
                    payload,
                    response_credential,
                    response_target: expected_source,
                })
            }
            ControlPlaneAuthDecision::Rejected { reason } => {
                self.metrics
                    .record_rejected(ControlPlaneAuthOperation::StorageRuntimeMapRefresh, reason);
                Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane storage-node heartbeat auth rejected: {}",
                    format_control_plane_auth_rejection(reason, &envelope, authority_now_ms)
                )))
            }
        }
    }
}

fn storage_node_heartbeat_auth_replay_policy(
    heartbeat: &NodeHeartbeat,
    authority_now_ms: u64,
) -> ControlPlaneAuthReplayPolicy {
    ControlPlaneAuthReplayPolicy::TimestampWindow {
        now_ms: authority_now_ms,
        max_window_ms: heartbeat
            .requested_lease_duration_ms
            .min(MAX_HEARTBEAT_LEASE_MS),
        allowed_future_skew_ms: CONTROL_PLANE_RPC_AUTH_FUTURE_SKEW_MS,
    }
}

fn control_plane_rpc_auth_replay_policy(authority_now_ms: u64) -> ControlPlaneAuthReplayPolicy {
    ControlPlaneAuthReplayPolicy::TimestampWindow {
        now_ms: authority_now_ms,
        max_window_ms: CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS,
        allowed_future_skew_ms: CONTROL_PLANE_RPC_AUTH_FUTURE_SKEW_MS,
    }
}

fn control_plane_rpc_response_auth_replay_policy(
    authority_now_ms: u64,
) -> ControlPlaneAuthReplayPolicy {
    ControlPlaneAuthReplayPolicy::TimestampWindow {
        now_ms: authority_now_ms,
        max_window_ms: CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS,
        allowed_future_skew_ms: CONTROL_PLANE_RPC_AUTH_FUTURE_SKEW_MS,
    }
}

fn write_authenticated_control_plane_rpc_payload(
    kind: ControlPlaneRpcKind,
    payload: &[u8],
) -> Vec<u8> {
    let mut authenticated_payload = Vec::with_capacity(2 + payload.len());
    write_u16(&mut authenticated_payload, kind.as_u16());
    authenticated_payload.extend_from_slice(payload);
    authenticated_payload
}

fn read_authenticated_control_plane_rpc_payload(
    expected_kind: ControlPlaneRpcKind,
    payload: &[u8],
) -> Result<Vec<u8>, ControlPlaneError> {
    if payload.len() < 2 {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane authenticated RPC payload missing kind".to_owned(),
        ));
    }
    let raw_kind = u16::from_be_bytes([payload[0], payload[1]]);
    let actual_kind = ControlPlaneRpcKind::from_u16(raw_kind)?;
    if actual_kind != expected_kind {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "control-plane authenticated RPC kind {:?} did not match outer kind {:?}",
            actual_kind, expected_kind
        )));
    }
    Ok(payload[2..].to_vec())
}

fn sign_control_plane_response_payload(
    kind: ControlPlaneRpcKind,
    credential: &ControlPlaneScopedCredential,
    target: ControlPlaneAuthPrincipal,
    operation: ControlPlaneAuthOperation,
    authority_now_ms: u64,
    payload: Vec<u8>,
) -> Result<Vec<u8>, ControlPlaneError> {
    let expires_at_ms = authority_now_ms
        .checked_add(CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS)
        .ok_or(ControlPlaneError::LeaseDeadlineOverflow)?;
    let payload = write_authenticated_control_plane_rpc_payload(kind, &payload);
    let envelope =
        credential.sign_envelope(crate::control_plane_auth::ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Principal(target),
            operation,
            issued_at_ms: Some(authority_now_ms),
            expires_at_ms: Some(expires_at_ms),
            sequence: None,
            nonce: Vec::new(),
            payload,
        })?;
    envelope.encode_frame()
}

impl ControlPlaneRuntimeMapSource for UnixControlPlaneClient {
    fn runtime_map_snapshot(
        &self,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.runtime_map_snapshot_with_read_timeout(CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT)
    }

    fn runtime_map_status(
        &self,
        _authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        self.runtime_map_status_with_check_applied_timeout()
    }

    fn pending_metadata_command_recoveries(
        &self,
        _authority_now_ms: u64,
    ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
        UnixControlPlaneClient::pending_metadata_command_recoveries(self)
    }

    fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        UnixControlPlaneClient::pg_runtime_map_snapshot(self, pg_id, authority_now_ms)
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        UnixControlPlaneClient::serving_pg_runtime_map_snapshot(self, pg_id, authority_now_ms)
    }
}

impl ControlPlaneRuntimeMapSource for AuthenticatedUnixControlPlaneClient {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapSnapshot,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    fn runtime_map_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        self.runtime_map_status_with_check_applied_timeout(authority_now_ms)
    }

    fn pending_metadata_command_recoveries(
        &self,
        authority_now_ms: u64,
    ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::PendingMetadataCommandRecoveries,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let listing = read_pending_metadata_command_recovery_listing(&mut reader)?;
        reader.finish()?;
        Ok(listing)
    }

    fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::PgRuntimeMapSnapshot,
            authority_now_ms,
            payload,
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot,
            authority_now_ms,
            payload,
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }
}

impl AuthenticatedUnixControlPlaneClient {
    pub fn runtime_map_status_with_check_applied_timeout(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapStatus,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let status = read_runtime_map_status(&mut reader)?;
        reader.finish()?;
        Ok(status)
    }
}

impl UnixControlPlaneClient {
    pub fn runtime_map_diagnostics(
        &self,
    ) -> Result<ControlPlaneRuntimeMapDiagnostics, ControlPlaneError> {
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapDiagnostics,
            &[],
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let diagnostics = read_control_plane_runtime_map_diagnostics(&mut reader)?;
        reader.finish()?;
        Ok(diagnostics)
    }

    fn runtime_map_snapshot_with_read_timeout(
        &self,
        read_timeout: Duration,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapSnapshot,
            &[],
            read_timeout,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    pub fn runtime_map_status_with_check_applied_timeout(
        &self,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        self.runtime_map_status_with_read_timeout(CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT)
    }

    fn runtime_map_status_with_read_timeout(
        &self,
        read_timeout: Duration,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapStatus,
            &[],
            read_timeout,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let status = read_runtime_map_status(&mut reader)?;
        reader.finish()?;
        Ok(status)
    }
}

impl ControlPlaneLinearizedRuntimeMapSource for UnixControlPlaneClient {
    fn linearized_runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.runtime_map_snapshot(authority_now_ms)
    }
}

fn metadata_transfer_install_applied(
    runtime_map: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
    acting_set: &[NodeId],
    transfer: PgMetadataTransferProof,
    expected_destination_epoch: ClusterEpoch,
) -> bool {
    if runtime_map.cluster_epoch() < expected_destination_epoch {
        return false;
    }
    runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == pg_id)
        .is_some_and(|route| {
            route.cluster_epoch() >= expected_destination_epoch
                && route.state() == PgState::Peering
                && route.acting_set() == acting_set
                && route.peering_metadata_transfer() == Some(transfer)
                && route.peering_metadata_transfer_destination_epoch()
                    == Some(expected_destination_epoch)
        })
}

fn unconfirmed_admin_mutation_response(
    kind: ControlPlaneRpcKind,
    error: ControlPlaneError,
) -> ControlPlaneError {
    let operation = kind
        .mutating_admin_operation()
        .expect("admin mutation response requires a mutating RPC kind");
    ControlPlaneError::RpcUnconfirmed {
        message: format!(
            "{operation} may have applied, but no valid operation result was received; \
             automatic retry requires an operation-specific confirmation predicate: {error}"
        ),
    }
}

fn unconfirmed_authenticated_admin_mutation_response(
    kind: ControlPlaneRpcKind,
    error: ControlPlaneError,
) -> ControlPlaneError {
    let operation = kind
        .mutating_admin_operation()
        .expect("authenticated admin mutation response requires a mutating RPC kind");
    ControlPlaneError::RpcUnconfirmed {
        message: format!(
            "{operation} may have applied, but no valid authenticated operation result was received; automatic retry requires an operation-specific confirmation predicate: {error}"
        ),
    }
}

fn decode_admin_mutation_success<T>(
    kind: ControlPlaneRpcKind,
    decode: impl FnOnce() -> Result<T, ControlPlaneError>,
) -> Result<T, ControlPlaneError> {
    decode().map_err(|error| unconfirmed_admin_mutation_response(kind, error))
}

fn classify_authenticated_admin_post_request_error(
    kind: ControlPlaneRpcKind,
    error: ControlPlaneError,
) -> ControlPlaneError {
    if kind.mutating_admin_operation().is_some() {
        unconfirmed_authenticated_admin_mutation_response(kind, error)
    } else {
        error
    }
}

fn decode_authenticated_admin_mutation_success<T>(
    kind: ControlPlaneRpcKind,
    decode: impl FnOnce() -> Result<T, ControlPlaneError>,
) -> Result<T, ControlPlaneError> {
    decode().map_err(|error| unconfirmed_authenticated_admin_mutation_response(kind, error))
}

fn authority_clock_admin_remaining(deadline: Instant) -> Result<Duration, ControlPlaneError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(ControlPlaneError::rpc_remote(
            "authority-clock admin operation deadline expired".to_owned(),
        ));
    }
    Ok(remaining)
}

fn authority_clock_admin_attempt_deadline(
    operation_deadline: Instant,
    attempt_timeout: Duration,
) -> Result<Instant, ControlPlaneError> {
    let remaining = authority_clock_admin_remaining(operation_deadline)?;
    Ok(Instant::now() + attempt_timeout.min(remaining))
}

fn metadata_transfer_fence_observable(
    runtime_map: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
) -> bool {
    runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == pg_id)
        .is_some_and(|route| route.state() == PgState::Peering)
}

fn retry_checked_metadata_transfer_fence(
    pg_id: PgId,
    mut attempt: impl FnMut(Instant) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError>,
) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
    let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
    let mut last_retryable_error = None;
    loop {
        let now = Instant::now();
        if now >= deadline {
            let diagnostic = last_retryable_error
                .as_ref()
                .map_or_else(|| "no attempt completed".to_owned(), ToString::to_string);
            return Err(ControlPlaneError::RpcUnconfirmed {
                message: format!(
                    "metadata-transfer fence for PG {} was not confirmed before its retry deadline: {diagnostic}",
                    pg_id.get()
                ),
            });
        }
        let attempt_deadline = (now + CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT).min(deadline);
        match attempt(attempt_deadline) {
            Ok(fenced) => return Ok(fenced),
            Err(error)
                if error.is_unconfirmed_control_plane_mutation()
                    || error.is_retryable_read_only_rpc_transport_error()
                    || error.is_transient_runtime_map_serving_gap() =>
            {
                last_retryable_error = Some(error);
                let remaining = deadline.saturating_duration_since(Instant::now());
                let backoff = CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF.min(remaining);
                if !backoff.is_zero() {
                    std::thread::sleep(backoff);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

impl ControlPlaneHeartbeatRuntimeMapSource for UnixControlPlaneClient {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        _authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        if heartbeat.requested_lease_duration_ms == 0 {
            return Err(ControlPlaneError::InvalidLeaseDuration);
        }
        let payload = write_node_heartbeat_payload(&heartbeat)?;
        let retry_budget = Duration::from_millis(heartbeat.requested_lease_duration_ms);
        let payload = self.send_liveness_request(
            ControlPlaneRpcKind::RefreshNodeHeartbeat,
            &payload,
            retry_budget,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let lease = read_heartbeat_lease_summary(&mut reader)?;
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        let history_reference_validation_epoch = runtime_map.cluster_epoch();
        Ok(ControlPlaneHeartbeatRefresh {
            lease,
            runtime_map,
            history_reference_validation_epoch,
        })
    }
}

impl ControlPlaneHeartbeatRuntimeMapSource for AuthenticatedUnixControlPlaneClient {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        initial_authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        // A retry must follow wall-clock corrections instead of projecting the
        // first sample forward with monotonic elapsed time.
        let mut first_attempt_now_ms = Some(initial_authority_now_ms);
        self.refresh_node_heartbeat_with_clock(heartbeat, || {
            Ok(first_attempt_now_ms
                .take()
                .unwrap_or_else(crate::clock::current_time_millis))
        })
    }
}

impl AuthenticatedUnixControlPlaneClient {
    fn refresh_node_heartbeat_with_clock<F>(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: F,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError>
    where
        F: FnMut() -> Result<u64, ControlPlaneError>,
    {
        self.refresh_node_heartbeat_with_clock_and_before_dispatch(
            heartbeat,
            authority_now_ms,
            || {},
        )
    }

    fn refresh_node_heartbeat_with_clock_and_before_dispatch<F, G>(
        &mut self,
        heartbeat: NodeHeartbeat,
        mut authority_now_ms: F,
        mut before_dispatch: G,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError>
    where
        F: FnMut() -> Result<u64, ControlPlaneError>,
        G: FnMut(),
    {
        if heartbeat.requested_lease_duration_ms == 0 {
            return Err(ControlPlaneError::InvalidLeaseDuration);
        }
        let payload = write_node_heartbeat_payload(&heartbeat)?;
        let payload = write_authenticated_control_plane_rpc_payload(
            ControlPlaneRpcKind::RefreshNodeHeartbeat,
            &payload,
        );
        let retry_budget = Duration::from_millis(heartbeat.requested_lease_duration_ms);
        let deadline = Instant::now() + retry_budget;
        let mut endpoint_pass = self.inner.endpoint_pass();
        let payload = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ControlPlaneError::RpcUnconfirmed {
                    message: "heartbeat retry budget expired before leader routing completed"
                        .to_owned(),
                });
            }
            let response = self
                .inner
                .send_liveness_request_raw_response_with_payload_factory_until(
                    ControlPlaneRpcKind::RefreshNodeHeartbeat,
                    deadline,
                    &mut endpoint_pass,
                    || {
                        let issued_at_ms = authority_now_ms()?;
                        let expires_at_ms = issued_at_ms
                            .checked_add(heartbeat.requested_lease_duration_ms)
                            .ok_or(ControlPlaneError::LeaseDeadlineOverflow)?;
                        let envelope = self.credential.sign_envelope(
                            crate::control_plane_auth::ControlPlaneAuthSignInput {
                                target: ControlPlaneAuthTarget::Service(
                                    crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
                                ),
                                operation: ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                                issued_at_ms: Some(issued_at_ms),
                                expires_at_ms: Some(expires_at_ms),
                                sequence: None,
                                nonce: Vec::new(),
                                payload: payload.clone(),
                            },
                        )?;
                        let request = envelope.encode_frame()?;
                        before_dispatch();
                        Ok(request)
                    },
                )?;
            let response = self.verify_runtime_map_response(
                ControlPlaneRpcKind::RefreshNodeHeartbeat,
                authority_now_ms()?,
                &response,
            )?;
            match decode_control_plane_rpc_response(response) {
                Err(error)
                    if error.is_control_plane_leader_routing_rejection()
                        && Instant::now() < deadline =>
                {
                    self.inner
                        .prefer_next_endpoint_after_failure(&endpoint_pass);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let retry_sleep = CONTROL_PLANE_RPC_LIVENESS_RETRY_BACKOFF.min(remaining / 2);
                    if !retry_sleep.is_zero() {
                        std::thread::sleep(retry_sleep);
                    }
                }
                result => {
                    self.inner.prefer_successful_endpoint(&endpoint_pass);
                    break result?;
                }
            }
        };
        let mut reader = PayloadReader::new(&payload);
        let lease = read_heartbeat_lease_summary(&mut reader)?;
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        let history_reference_validation_epoch = runtime_map.cluster_epoch();
        Ok(ControlPlaneHeartbeatRefresh {
            lease,
            runtime_map,
            history_reference_validation_epoch,
        })
    }
}

#[cfg(test)]
fn handle_control_plane_unix_stream<T>(
    control_plane: &mut T,
    stream: &mut UnixStream,
    authority_now_ms: u64,
) -> Result<(), ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    let request = read_control_plane_unix_request(stream)?;
    let response = build_control_plane_unix_response(control_plane, request, authority_now_ms)?;
    write_control_plane_unix_response(stream, response)
}

#[cfg(test)]
fn handle_control_plane_unix_stream_with_auth<T>(
    control_plane: &mut T,
    stream: &mut UnixStream,
    authority_now_ms: u64,
    auth_verifier: &ControlPlaneUnixAuthVerifier,
) -> Result<(), ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    let request = read_control_plane_unix_request(stream)?;
    let response = build_control_plane_unix_response_with_auth(
        control_plane,
        request,
        authority_now_ms,
        Some(auth_verifier),
    )?;
    write_control_plane_unix_response(stream, response)
}

#[derive(Debug)]
struct ControlPlaneRpcRequest {
    kind: ControlPlaneRpcKind,
    payload: Vec<u8>,
}

impl ControlPlaneRpcRequest {
    #[must_use]
    fn metrics_kind(&self) -> observability::ControlPlaneRpcMetricKind {
        self.kind.metrics_kind()
    }
}

#[derive(Debug)]
struct ControlPlaneUnixResponseAuth {
    credential: ControlPlaneScopedCredential,
    target: ControlPlaneAuthPrincipal,
    operation: ControlPlaneAuthOperation,
}

struct VerifiedControlPlaneRpcRequest {
    kind: ControlPlaneRpcKind,
    payload: Vec<u8>,
    response_auth: Option<ControlPlaneUnixResponseAuth>,
}

impl VerifiedControlPlaneRpcRequest {
    #[must_use]
    fn is_refresh_node_heartbeat(&self) -> bool {
        self.kind == ControlPlaneRpcKind::RefreshNodeHeartbeat
    }

    #[must_use]
    fn is_authority_clock_admin(&self) -> bool {
        matches!(
            self.kind,
            ControlPlaneRpcKind::AuthorityClockStatus
                | ControlPlaneRpcKind::ReestablishAuthorityClock
        )
    }

    #[must_use]
    fn requires_raft_authority_confirmation(&self) -> bool {
        !matches!(
            self.kind,
            ControlPlaneRpcKind::AuthorityClockStatus | ControlPlaneRpcKind::TriggerRaftElection
        )
    }
}

#[derive(Debug)]
struct ControlPlaneRpcResponse {
    kind: ControlPlaneRpcKind,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct PreparedControlPlaneHeartbeatResponse {
    refresh: Result<ControlPlaneHeartbeatRefresh, ControlPlaneError>,
    response_auth: Option<ControlPlaneUnixResponseAuth>,
}

#[cfg(test)]
fn read_control_plane_unix_request(
    stream: &mut impl std::io::Read,
) -> Result<ControlPlaneRpcRequest, ControlPlaneError> {
    let (kind, payload) = read_control_plane_rpc_frame(stream)?;
    Ok(ControlPlaneRpcRequest { kind, payload })
}

fn read_control_plane_request_with_reservation<R>(
    stream: &mut impl std::io::Read,
    reserve: impl FnOnce(usize) -> Result<R, ControlPlaneError>,
) -> Result<(ControlPlaneRpcRequest, R), ControlPlaneError> {
    let ((kind, payload), reservation) =
        read_control_plane_rpc_frame_with_reservation(stream, reserve)?;
    Ok((ControlPlaneRpcRequest { kind, payload }, reservation))
}

fn verify_control_plane_unix_request(
    request: ControlPlaneRpcRequest,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
    authority_now_ms: u64,
) -> Result<VerifiedControlPlaneRpcRequest, ControlPlaneError> {
    verify_control_plane_request(request, auth_verifier, authority_now_ms, false)
}

fn verify_control_plane_authenticated_request(
    request: ControlPlaneRpcRequest,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
    authority_now_ms: u64,
) -> Result<VerifiedControlPlaneRpcRequest, ControlPlaneError> {
    verify_control_plane_request(request, auth_verifier, authority_now_ms, true)
}

fn verify_control_plane_request(
    request: ControlPlaneRpcRequest,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
    authority_now_ms: u64,
    require_authentication: bool,
) -> Result<VerifiedControlPlaneRpcRequest, ControlPlaneError> {
    let ControlPlaneRpcRequest { kind, payload } = request;
    let (payload, response_auth) = match kind {
        _ if kind.auth_operation() == ControlPlaneAuthOperation::AdminControlPlaneCommand => {
            match auth_verifier.filter(|verifier| verifier.requires_admin_control_plane_auth()) {
                Some(auth_verifier) => {
                    let verified = auth_verifier.verify_admin_control_plane_command_payload(
                        kind,
                        &payload,
                        authority_now_ms,
                    )?;
                    (
                        verified.payload,
                        Some(ControlPlaneUnixResponseAuth {
                            credential: verified.response_credential,
                            target: verified.response_target,
                            operation: ControlPlaneAuthOperation::AdminControlPlaneResponse,
                        }),
                    )
                }
                None => (payload, None),
            }
        }
        ControlPlaneRpcKind::RefreshNodeHeartbeat => {
            match auth_verifier.filter(|verifier| verifier.requires_storage_node_heartbeat_auth()) {
                Some(auth_verifier) => {
                    let verified = auth_verifier
                        .verify_storage_node_heartbeat_payload(&payload, authority_now_ms)?;
                    (
                        verified.payload,
                        Some(ControlPlaneUnixResponseAuth {
                            credential: verified.response_credential,
                            target: verified.response_target,
                            operation: ControlPlaneAuthOperation::RuntimeMapResponse,
                        }),
                    )
                }
                None => (payload, None),
            }
        }
        _ => {
            let auth_payload_operation = if control_plane_auth_payload_has_magic(&payload) {
                ControlPlaneAuthEnvelope::decode_frame(&payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)
                    .ok()
                    .map(|envelope| envelope.header().operation())
            } else {
                None
            };
            match auth_verifier {
                Some(auth_verifier)
                    if auth_verifier.requires_admin_control_plane_auth()
                        && auth_payload_operation
                            == Some(ControlPlaneAuthOperation::AdminControlPlaneCommand) =>
                {
                    let verified = auth_verifier.verify_admin_runtime_map_read_payload(
                        kind,
                        &payload,
                        authority_now_ms,
                    )?;
                    (
                        verified.payload,
                        Some(ControlPlaneUnixResponseAuth {
                            credential: verified.response_credential,
                            target: verified.response_target,
                            operation: ControlPlaneAuthOperation::AdminControlPlaneResponse,
                        }),
                    )
                }
                Some(auth_verifier) if auth_verifier.requires_frontend_runtime_map_auth() => {
                    let verified = auth_verifier.verify_frontend_runtime_map_read_payload(
                        kind,
                        &payload,
                        authority_now_ms,
                    )?;
                    (
                        verified.payload,
                        Some(ControlPlaneUnixResponseAuth {
                            credential: verified.response_credential,
                            target: verified.response_target,
                            operation: ControlPlaneAuthOperation::RuntimeMapResponse,
                        }),
                    )
                }
                Some(auth_verifier)
                    if auth_verifier.requires_admin_control_plane_auth()
                        && control_plane_auth_payload_has_magic(&payload) =>
                {
                    let verified = auth_verifier.verify_admin_runtime_map_read_payload(
                        kind,
                        &payload,
                        authority_now_ms,
                    )?;
                    (
                        verified.payload,
                        Some(ControlPlaneUnixResponseAuth {
                            credential: verified.response_credential,
                            target: verified.response_target,
                            operation: ControlPlaneAuthOperation::AdminControlPlaneResponse,
                        }),
                    )
                }
                _ => (payload, None),
            }
        }
    };
    if require_authentication && response_auth.is_none() {
        if let Some(auth_verifier) = auth_verifier {
            auth_verifier.metrics.record_rejected(
                kind.auth_operation(),
                ControlPlaneAuthRejectionReason::Missing,
            );
        }
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane RPC endpoint requires authenticated requests".to_owned(),
        ));
    }
    if matches!(
        kind,
        ControlPlaneRpcKind::AuthorityClockStatus | ControlPlaneRpcKind::ReestablishAuthorityClock
    ) && response_auth.is_none()
    {
        return Err(ControlPlaneError::rpc_protocol(
            "authority-clock administration requires configured admin authentication".to_owned(),
        ));
    }
    Ok(VerifiedControlPlaneRpcRequest {
        kind,
        payload,
        response_auth,
    })
}

#[cfg(test)]
fn build_control_plane_unix_response<T>(
    control_plane: &mut T,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    build_control_plane_unix_response_with_auth(control_plane, request, authority_now_ms, None)
}

#[cfg(test)]
fn build_control_plane_unix_response_with_auth<T>(
    control_plane: &mut T,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    build_control_plane_unix_response_with_auth_and_response_clock(
        control_plane,
        request,
        authority_now_ms,
        auth_verifier,
        || Ok(authority_now_ms),
    )
}

#[cfg(test)]
fn build_control_plane_unix_response_with_auth_and_response_clock<T, F>(
    control_plane: &mut T,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
    response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let request = verify_control_plane_unix_request(request, auth_verifier, authority_now_ms)?;
    build_control_plane_unix_response_from_verified(
        control_plane,
        request,
        authority_now_ms,
        response_authority_now_ms,
    )
}

fn build_control_plane_unix_response_from_verified<T, F>(
    control_plane: &mut T,
    request: VerifiedControlPlaneRpcRequest,
    authority_now_ms: u64,
    mut response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let VerifiedControlPlaneRpcRequest {
        kind,
        payload,
        response_auth,
    } = request;
    let response = match kind {
        ControlPlaneRpcKind::RuntimeMapSnapshot => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            let response = match control_plane.runtime_map_snapshot(authority_now_ms) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_runtime_map_snapshot(&mut response, &snapshot)?;
                    Ok(response)
                }
                Err(error) => Err(error),
            };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::RuntimeMapDiagnostics => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            let response = match control_plane.runtime_map_diagnostics_snapshot(authority_now_ms) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_control_plane_runtime_map_diagnostics(&mut response, &snapshot)?;
                    Ok(response)
                }
                Err(error) => Err(error),
            };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::RuntimeMapStatus => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            let response = match control_plane.runtime_map_status(authority_now_ms) {
                Ok(status) => {
                    let mut response = Vec::new();
                    write_runtime_map_status(&mut response, status)?;
                    Ok(response)
                }
                Err(error) => Err(error),
            };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::PendingMetadataCommandRecoveries => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            let response = control_plane
                .pending_metadata_command_recoveries(authority_now_ms)
                .and_then(|listing| {
                    let mut response = Vec::new();
                    write_pending_metadata_command_recovery_listing(&mut response, &listing)?;
                    Ok(response)
                });
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::PgRuntimeMapSnapshot => {
            let mut reader = PayloadReader::new(&payload);
            let pg_id = read_pg_id_request(&mut reader)?;
            reader.finish()?;
            let response = match control_plane.pg_runtime_map_snapshot(pg_id, authority_now_ms) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_runtime_map_snapshot(&mut response, &snapshot)?;
                    Ok(response)
                }
                Err(error) => Err(error),
            };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot => {
            let mut reader = PayloadReader::new(&payload);
            let pg_id = read_pg_id_request(&mut reader)?;
            reader.finish()?;
            let response =
                match control_plane.serving_pg_runtime_map_snapshot(pg_id, authority_now_ms) {
                    Ok(snapshot) => {
                        let mut response = Vec::new();
                        write_runtime_map_snapshot(&mut response, &snapshot)?;
                        Ok(response)
                    }
                    Err(error) => Err(error),
                };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::RefreshNodeHeartbeat => {
            debug_assert_eq!(
                kind.auth_operation(),
                ControlPlaneAuthOperation::StorageRuntimeMapRefresh
            );
            let mut reader = PayloadReader::new(&payload);
            let heartbeat = read_node_heartbeat(&mut reader)?;
            reader.finish()?;
            let response = match control_plane.refresh_node_heartbeat(heartbeat, authority_now_ms) {
                Ok(refresh) => {
                    let mut response = Vec::new();
                    write_heartbeat_lease_summary(&mut response, refresh.lease());
                    write_runtime_map_snapshot(&mut response, refresh.runtime_map())?;
                    Ok(response)
                }
                Err(error) => Err(error),
            };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::SetPgActingSet => {
            let mut reader = PayloadReader::new(&payload);
            let (pg_id, acting_set) = read_pg_acting_set_request(&mut reader)?;
            reader.finish()?;
            match control_plane.set_pg_acting_set(pg_id, acting_set) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_u64(&mut response, snapshot.cluster_epoch().get());
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap => {
            let mut reader = PayloadReader::new(&payload);
            let pg_id = read_pg_id_request(&mut reader)?;
            reader.finish()?;
            match control_plane
                .fence_pg_for_metadata_transfer_with_source_lease(pg_id)
                .and_then(|fenced| {
                    let (snapshot, source_primary_lease_deadline_ms) = fenced.into_parts();
                    snapshot
                        .reconstructed_runtime_map_for_pg_with_fallback_validity(
                            pg_id,
                            non_serving_runtime_map_validity(authority_now_ms),
                        )
                        .map(|runtime_map| (runtime_map, source_primary_lease_deadline_ms))
                }) {
                Ok((snapshot, source_primary_lease_deadline_ms)) => {
                    let mut response = Vec::new();
                    write_runtime_map_snapshot(&mut response, &snapshot)?;
                    write_option_u64(&mut response, source_primary_lease_deadline_ms);
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::SetPgActingSetWithMetadataTransfer => {
            let mut reader = PayloadReader::new(&payload);
            let (pg_id, acting_set, transfer, expected_destination_epoch) =
                read_pg_acting_set_with_metadata_transfer_request(&mut reader)?;
            reader.finish()?;
            match control_plane.set_pg_acting_set_with_metadata_transfer(
                pg_id,
                acting_set,
                transfer,
                expected_destination_epoch,
            ) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_u64(&mut response, snapshot.cluster_epoch().get());
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap => {
            let mut reader = PayloadReader::new(&payload);
            let (pg_id, acting_set, transfer, expected_destination_epoch) =
                read_pg_acting_set_with_metadata_transfer_request(&mut reader)?;
            reader.finish()?;
            match control_plane
                .set_pg_acting_set_with_metadata_transfer(
                    pg_id,
                    acting_set,
                    transfer,
                    expected_destination_epoch,
                )
                .and_then(|snapshot| {
                    snapshot.reconstructed_runtime_map_for_pg_with_fallback_validity(
                        pg_id,
                        non_serving_runtime_map_validity(authority_now_ms),
                    )
                }) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_runtime_map_snapshot(&mut response, &snapshot)?;
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::TransferRaftLeadership => {
            let mut reader = PayloadReader::new(&payload);
            let node_id = reader.read_u64()?;
            reader.finish()?;
            match control_plane.transfer_raft_leadership_to(node_id) {
                Ok(()) => Ok(Vec::new()),
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            match control_plane.trigger_raft_snapshot_and_purge() {
                Ok(snapshot_index) => {
                    let mut response = Vec::new();
                    write_option_u64(&mut response, snapshot_index);
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::TriggerRaftElection => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            match control_plane.trigger_raft_election() {
                Ok(()) => Ok(Vec::new()),
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::AuthorityClockStatus
        | ControlPlaneRpcKind::ReestablishAuthorityClock => {
            return Err(ControlPlaneError::rpc_protocol(
                "authority-clock admin RPC requires the process-local clock handler".to_owned(),
            ));
        }
    };
    build_control_plane_verified_response(
        kind,
        response,
        response_auth,
        response_authority_now_ms()?,
    )
}

fn build_control_plane_unix_admission_error_response(
    request: VerifiedControlPlaneRpcRequest,
    error: ControlPlaneError,
    authority_now_ms: u64,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError> {
    build_control_plane_verified_response(
        request.kind,
        Err(error),
        request.response_auth,
        authority_now_ms,
    )
}

fn build_control_plane_verified_response(
    kind: ControlPlaneRpcKind,
    response: Result<Vec<u8>, ControlPlaneError>,
    response_auth: Option<ControlPlaneUnixResponseAuth>,
    authority_now_ms: u64,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError> {
    let mut payload = encode_control_plane_rpc_response(response)?;
    if let Some(response_auth) = response_auth {
        payload = sign_control_plane_response_payload(
            kind,
            &response_auth.credential,
            response_auth.target,
            response_auth.operation,
            authority_now_ms,
            payload,
        )?;
    }
    Ok(ControlPlaneRpcResponse { kind, payload })
}

#[cfg(test)]
fn build_control_plane_authority_clock_admin_response<T, P, F>(
    control_plane: &T,
    authority_clock: &mut ControlPlaneAuthorityClock,
    request: ControlPlaneRpcRequest,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
    sample: ControlPlaneAuthorityClockAdminSample,
    before_response_sign: P,
    response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin,
    P: FnOnce(&T, &mut ControlPlaneAuthorityClock) -> Result<(), ControlPlaneError>,
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let request =
        verify_control_plane_unix_request(request, auth_verifier, sample.auth_authority_now_ms)?;
    build_control_plane_authority_clock_admin_response_from_verified(
        control_plane,
        authority_clock,
        request,
        sample,
        before_response_sign,
        response_authority_now_ms,
    )
}

#[cfg(test)]
fn build_control_plane_authority_clock_admin_response_from_verified<T, P, F>(
    control_plane: &T,
    authority_clock: &mut ControlPlaneAuthorityClock,
    request: VerifiedControlPlaneRpcRequest,
    sample: ControlPlaneAuthorityClockAdminSample,
    before_response_sign: P,
    response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin,
    P: FnOnce(&T, &mut ControlPlaneAuthorityClock) -> Result<(), ControlPlaneError>,
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let context = control_plane.authority_clock_context();
    build_control_plane_authority_clock_admin_response_from_verified_with_context(
        authority_clock,
        request,
        sample,
        context,
        |_, authority_clock| before_response_sign(control_plane, authority_clock),
        response_authority_now_ms,
    )
}

fn build_control_plane_authority_clock_admin_response_from_verified_with_context<P, F>(
    authority_clock: &mut ControlPlaneAuthorityClock,
    request: VerifiedControlPlaneRpcRequest,
    sample: ControlPlaneAuthorityClockAdminSample,
    context: Result<ControlPlaneAuthorityClockContext, ControlPlaneError>,
    before_response_sign: P,
    mut response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    P: FnOnce(
        ControlPlaneAuthorityClockContext,
        &mut ControlPlaneAuthorityClock,
    ) -> Result<(), ControlPlaneError>,
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let VerifiedControlPlaneRpcRequest {
        kind,
        payload,
        response_auth,
    } = request;
    if !matches!(
        kind,
        ControlPlaneRpcKind::AuthorityClockStatus | ControlPlaneRpcKind::ReestablishAuthorityClock
    ) {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "expected authority-clock admin RPC, got {kind:?}"
        )));
    }
    let Some(response_auth) = response_auth else {
        return Err(ControlPlaneError::rpc_protocol(
            "authority-clock administration requires configured admin authentication".to_owned(),
        ));
    };
    let mut successful_reestablishment_context = None;
    let response = match kind {
        ControlPlaneRpcKind::AuthorityClockStatus => context.and_then(|context| {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            let mut response = Vec::new();
            let status =
                authority_clock.observe_status(context, sample.wall_ms, sample.clock_health_ms)?;
            write_authority_clock_status(&mut response, status);
            Ok(response)
        }),
        ControlPlaneRpcKind::ReestablishAuthorityClock => context.and_then(|context| {
            let mut reader = PayloadReader::new(&payload);
            let expected_generation = reader.read_u64()?;
            let expected_committed_timestamp_high_water_ms = reader.read_option_u64()?;
            let expected_raft_leadership_term = reader.read_option_u64()?;
            reader.finish()?;
            let status = authority_clock.reestablish(
                expected_generation,
                expected_committed_timestamp_high_water_ms,
                expected_raft_leadership_term,
                context,
                sample.wall_ms,
                sample.clock_health_ms,
            )?;
            successful_reestablishment_context = Some(context);
            let mut response = Vec::new();
            write_authority_clock_status(&mut response, status);
            Ok(response)
        }),
        _ => unreachable!("authority-clock RPC kind checked above"),
    };
    if kind == ControlPlaneRpcKind::ReestablishAuthorityClock && response.is_ok() {
        before_response_sign(
            successful_reestablishment_context
                .expect("successful authority-clock response requires a valid context"),
            authority_clock,
        )?;
    }
    let response_authority_now_ms = response_authority_now_ms()?;
    let payload = sign_control_plane_response_payload(
        kind,
        &response_auth.credential,
        response_auth.target,
        response_auth.operation,
        response_authority_now_ms,
        encode_control_plane_rpc_response(response)?,
    )?;
    Ok(ControlPlaneRpcResponse { kind, payload })
}

fn write_control_plane_unix_response(
    stream: &mut impl std::io::Write,
    response: ControlPlaneRpcResponse,
) -> Result<(), ControlPlaneError> {
    write_control_plane_rpc_frame(stream, response.kind, &response.payload)
}

#[cfg(test)]
fn respond_control_plane_unix_request<T>(
    control_plane: &mut T,
    stream: &mut impl std::io::Write,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
) -> Result<(), ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    let response = build_control_plane_unix_response(control_plane, request, authority_now_ms)?;
    write_control_plane_unix_response(stream, response)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPlaneRpcKind {
    RuntimeMapSnapshot = 1,
    RefreshNodeHeartbeat = 2,
    SetPgActingSet = 3,
    SetPgActingSetWithMetadataTransfer = 4,
    SetPgActingSetWithMetadataTransferRuntimeMap = 6,
    FencePgForMetadataTransferRuntimeMap = 7,
    TransferRaftLeadership = 8,
    PgRuntimeMapSnapshot = 9,
    TriggerRaftSnapshotAndPurge = 10,
    TriggerRaftElection = 11,
    RuntimeMapStatus = 12,
    PendingMetadataCommandRecoveries = 13,
    AuthorityClockStatus = 14,
    ReestablishAuthorityClock = 15,
    RuntimeMapDiagnostics = 16,
    ServingPgRuntimeMapSnapshot = 17,
}

impl ControlPlaneRpcKind {
    #[cfg(test)]
    const ALL: [Self; 16] = [
        Self::RuntimeMapSnapshot,
        Self::RefreshNodeHeartbeat,
        Self::SetPgActingSet,
        Self::SetPgActingSetWithMetadataTransfer,
        Self::SetPgActingSetWithMetadataTransferRuntimeMap,
        Self::FencePgForMetadataTransferRuntimeMap,
        Self::TransferRaftLeadership,
        Self::PgRuntimeMapSnapshot,
        Self::TriggerRaftSnapshotAndPurge,
        Self::TriggerRaftElection,
        Self::RuntimeMapStatus,
        Self::PendingMetadataCommandRecoveries,
        Self::AuthorityClockStatus,
        Self::ReestablishAuthorityClock,
        Self::RuntimeMapDiagnostics,
        Self::ServingPgRuntimeMapSnapshot,
    ];

    fn as_u16(self) -> u16 {
        self as u16
    }

    fn from_u16(value: u16) -> Result<Self, ControlPlaneError> {
        match value {
            1 => Ok(Self::RuntimeMapSnapshot),
            2 => Ok(Self::RefreshNodeHeartbeat),
            3 => Ok(Self::SetPgActingSet),
            4 => Ok(Self::SetPgActingSetWithMetadataTransfer),
            6 => Ok(Self::SetPgActingSetWithMetadataTransferRuntimeMap),
            7 => Ok(Self::FencePgForMetadataTransferRuntimeMap),
            8 => Ok(Self::TransferRaftLeadership),
            9 => Ok(Self::PgRuntimeMapSnapshot),
            10 => Ok(Self::TriggerRaftSnapshotAndPurge),
            11 => Ok(Self::TriggerRaftElection),
            12 => Ok(Self::RuntimeMapStatus),
            13 => Ok(Self::PendingMetadataCommandRecoveries),
            14 => Ok(Self::AuthorityClockStatus),
            15 => Ok(Self::ReestablishAuthorityClock),
            16 => Ok(Self::RuntimeMapDiagnostics),
            17 => Ok(Self::ServingPgRuntimeMapSnapshot),
            _ => Err(ControlPlaneError::rpc_protocol(format!(
                "unknown control-plane RPC kind {value}"
            ))),
        }
    }

    fn auth_operation(self) -> ControlPlaneAuthOperation {
        match self {
            Self::RuntimeMapSnapshot
            | Self::PgRuntimeMapSnapshot
            | Self::ServingPgRuntimeMapSnapshot
            | Self::RuntimeMapStatus
            | Self::RuntimeMapDiagnostics
            | Self::PendingMetadataCommandRecoveries => {
                ControlPlaneAuthOperation::FrontendRuntimeMapRead
            }
            Self::RefreshNodeHeartbeat => ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
            Self::SetPgActingSet
            | Self::SetPgActingSetWithMetadataTransfer
            | Self::SetPgActingSetWithMetadataTransferRuntimeMap
            | Self::FencePgForMetadataTransferRuntimeMap
            | Self::TransferRaftLeadership
            | Self::TriggerRaftSnapshotAndPurge
            | Self::TriggerRaftElection
            | Self::AuthorityClockStatus
            | Self::ReestablishAuthorityClock => {
                ControlPlaneAuthOperation::AdminControlPlaneCommand
            }
        }
    }

    fn mutating_admin_operation(self) -> Option<&'static str> {
        match self {
            Self::SetPgActingSet => Some("control-plane PG acting-set update"),
            Self::SetPgActingSetWithMetadataTransfer => {
                Some("control-plane PG metadata-transfer acting-set update")
            }
            Self::SetPgActingSetWithMetadataTransferRuntimeMap => {
                Some("control-plane PG metadata-transfer acting-set update")
            }
            Self::FencePgForMetadataTransferRuntimeMap => {
                Some("control-plane PG metadata-transfer fence")
            }
            Self::TransferRaftLeadership => Some("control-plane Raft leadership transfer"),
            Self::TriggerRaftSnapshotAndPurge => Some("control-plane Raft snapshot/purge trigger"),
            Self::TriggerRaftElection => Some("control-plane Raft election trigger"),
            Self::ReestablishAuthorityClock => {
                Some("control-plane authority-clock re-establishment")
            }
            Self::RuntimeMapSnapshot
            | Self::RefreshNodeHeartbeat
            | Self::PgRuntimeMapSnapshot
            | Self::RuntimeMapStatus
            | Self::PendingMetadataCommandRecoveries
            | Self::AuthorityClockStatus
            | Self::RuntimeMapDiagnostics
            | Self::ServingPgRuntimeMapSnapshot => None,
        }
    }

    fn metrics_kind(self) -> observability::ControlPlaneRpcMetricKind {
        use observability::ControlPlaneRpcMetricKind as MetricKind;

        match self {
            Self::RuntimeMapSnapshot => MetricKind::RuntimeMapSnapshot,
            Self::RefreshNodeHeartbeat => MetricKind::RefreshNodeHeartbeat,
            Self::SetPgActingSet => MetricKind::SetPgActingSet,
            Self::SetPgActingSetWithMetadataTransfer => {
                MetricKind::SetPgActingSetWithMetadataTransfer
            }
            Self::SetPgActingSetWithMetadataTransferRuntimeMap => {
                MetricKind::SetPgActingSetWithMetadataTransferRuntimeMap
            }
            Self::FencePgForMetadataTransferRuntimeMap => {
                MetricKind::FencePgForMetadataTransferRuntimeMap
            }
            Self::TransferRaftLeadership => MetricKind::TransferRaftLeadership,
            Self::PgRuntimeMapSnapshot => MetricKind::PgRuntimeMapSnapshot,
            Self::ServingPgRuntimeMapSnapshot => MetricKind::ServingPgRuntimeMapSnapshot,
            Self::TriggerRaftSnapshotAndPurge => MetricKind::TriggerRaftSnapshotAndPurge,
            Self::TriggerRaftElection => MetricKind::TriggerRaftElection,
            Self::RuntimeMapStatus => MetricKind::RuntimeMapStatus,
            Self::PendingMetadataCommandRecoveries => MetricKind::PendingMetadataCommandRecoveries,
            Self::AuthorityClockStatus => MetricKind::AuthorityClockStatus,
            Self::ReestablishAuthorityClock => MetricKind::ReestablishAuthorityClock,
            Self::RuntimeMapDiagnostics => MetricKind::RuntimeMapDiagnostics,
        }
    }
}

#[cfg(test)]
fn prepare_control_plane_heartbeat_response<T>(
    control_plane: &mut T,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
) -> Result<PreparedControlPlaneHeartbeatResponse, ControlPlaneError>
where
    T: ControlPlaneHeartbeatRuntimeMapSource,
{
    let request = verify_control_plane_unix_request(request, auth_verifier, authority_now_ms)?;
    prepare_control_plane_heartbeat_response_from_verified(control_plane, request, authority_now_ms)
}

fn prepare_control_plane_heartbeat_response_from_verified<T>(
    control_plane: &mut T,
    request: VerifiedControlPlaneRpcRequest,
    authority_now_ms: u64,
) -> Result<PreparedControlPlaneHeartbeatResponse, ControlPlaneError>
where
    T: ControlPlaneHeartbeatRuntimeMapSource,
{
    prepare_control_plane_heartbeat_response_internal(
        control_plane,
        request,
        authority_now_ms,
        None,
    )
}

fn prepare_control_plane_heartbeat_response_with_lease_horizon_authority_from_verified<T>(
    control_plane: &mut T,
    request: VerifiedControlPlaneRpcRequest,
    authority_now_ms: u64,
    lease_horizon_authority: LeaseHorizonAuthorityBinding,
) -> Result<PreparedControlPlaneHeartbeatResponse, ControlPlaneError>
where
    T: ControlPlaneHeartbeatRuntimeMapSource,
{
    prepare_control_plane_heartbeat_response_internal(
        control_plane,
        request,
        authority_now_ms,
        Some(lease_horizon_authority),
    )
}

fn prepare_control_plane_heartbeat_response_internal<T>(
    control_plane: &mut T,
    request: VerifiedControlPlaneRpcRequest,
    authority_now_ms: u64,
    lease_horizon_authority: Option<LeaseHorizonAuthorityBinding>,
) -> Result<PreparedControlPlaneHeartbeatResponse, ControlPlaneError>
where
    T: ControlPlaneHeartbeatRuntimeMapSource,
{
    let VerifiedControlPlaneRpcRequest {
        kind,
        payload,
        response_auth,
    } = request;
    if kind != ControlPlaneRpcKind::RefreshNodeHeartbeat {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "expected RefreshNodeHeartbeat RPC, got {kind:?}"
        )));
    }
    debug_assert_eq!(
        kind.auth_operation(),
        ControlPlaneAuthOperation::StorageRuntimeMapRefresh
    );
    let mut reader = PayloadReader::new(&payload);
    let heartbeat = read_node_heartbeat(&mut reader)?;
    reader.finish()?;
    let history_reference_summary = heartbeat.cluster_map_history_route_references.summary();
    let mut history_reference_sample = observability::ControlPlaneHistoryReferenceSample {
        node_id: heartbeat.node_id.as_u32(),
        observed_epoch: heartbeat.observed_epoch.get(),
        validation_epoch: heartbeat.observed_epoch.get(),
        observed_at_ms: authority_now_ms,
        oldest_live_placement_epoch: history_reference_summary
            .oldest_live_placement_epoch
            .map(ClusterEpoch::get),
        oldest_durable_backfill_epoch: history_reference_summary
            .oldest_durable_backfill_epoch
            .map(ClusterEpoch::get),
        oldest_pending_metadata_command_epoch: history_reference_summary
            .oldest_pending_metadata_command_epoch
            .map(ClusterEpoch::get),
        oldest_object_payload_reclaim_claim_epoch: history_reference_summary
            .oldest_object_payload_reclaim_claim_epoch
            .map(ClusterEpoch::get),
    };
    let refresh = match lease_horizon_authority {
        Some(lease_horizon_authority) => control_plane
            .refresh_node_heartbeat_with_lease_horizon_authority(
                heartbeat,
                authority_now_ms,
                lease_horizon_authority,
            ),
        None => control_plane.refresh_node_heartbeat(heartbeat, authority_now_ms),
    };
    if let Ok(refresh) = &refresh {
        history_reference_sample.validation_epoch =
            refresh.history_reference_validation_epoch.get();
        observability::record_control_plane_history_reference_sample(history_reference_sample);
    }
    Ok(PreparedControlPlaneHeartbeatResponse {
        refresh,
        response_auth,
    })
}

fn finish_control_plane_heartbeat_response<F>(
    prepared: PreparedControlPlaneHeartbeatResponse,
    mut response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let response = match prepared.refresh {
        Ok(refresh) => {
            let mut response = Vec::new();
            write_heartbeat_lease_summary(&mut response, refresh.lease());
            write_runtime_map_snapshot(&mut response, refresh.runtime_map())?;
            Ok(response)
        }
        Err(error) => Err(error),
    };
    let response_authority_now_ms = response_authority_now_ms()?;
    build_control_plane_verified_response(
        ControlPlaneRpcKind::RefreshNodeHeartbeat,
        response,
        prepared.response_auth,
        response_authority_now_ms,
    )
}

/// Opaque failure returned when a control-plane server can no longer accept
/// connections. The concrete transport diagnostic is logged inside storage.
pub(crate) struct ControlPlaneRpcServerError;

impl std::fmt::Debug for ControlPlaneRpcServerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ControlPlaneRpcServerError")
    }
}

impl std::fmt::Display for ControlPlaneRpcServerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("control-plane RPC server stopped accepting connections")
    }
}

impl std::error::Error for ControlPlaneRpcServerError {}

enum ControlPlaneRpcServerAuthority<T> {
    Shared(Arc<Mutex<T>>),
    PerWorker(T),
}

impl<T> ControlPlaneRpcServerAuthority<T> {
    fn with_mut<R>(
        &mut self,
        metrics_kind: observability::ControlPlaneRpcMetricKind,
        operation: impl FnOnce(&mut T) -> R,
    ) -> R {
        match self {
            Self::Shared(authority) => {
                let lock_started = Instant::now();
                let mut authority = authority
                    .lock()
                    .expect("control-plane authority mutex poisoned");
                observability::record_control_plane_rpc_lock_wait(
                    metrics_kind,
                    lock_started.elapsed(),
                );
                operation(&mut authority)
            }
            Self::PerWorker(authority) => {
                observability::record_control_plane_rpc_lock_wait(metrics_kind, Duration::ZERO);
                operation(authority)
            }
        }
    }
}

enum ControlPlaneRpcAdmissionFailure {
    Unauthenticated(Box<ControlPlaneError>),
    Authenticated {
        request: Box<VerifiedControlPlaneRpcRequest>,
        error: Box<ControlPlaneError>,
    },
}

fn authenticate_and_admit_control_plane_rpc(
    request: ControlPlaneRpcRequest,
    policy: &ControlPlaneRpcServerPolicy,
    require_authentication: bool,
    authority_now_ms: u64,
) -> Result<VerifiedControlPlaneRpcRequest, ControlPlaneRpcAdmissionFailure> {
    let verify = if require_authentication {
        verify_control_plane_authenticated_request
    } else {
        verify_control_plane_unix_request
    };
    let request = verify(request, policy.auth_verifier.as_deref(), authority_now_ms)
        .map_err(|error| ControlPlaneRpcAdmissionFailure::Unauthenticated(Box::new(error)))?;
    if !policy.role.accepts(&request) {
        let error = ControlPlaneError::rpc_protocol(match policy.role {
            ControlPlaneRpcServerRole::Ordinary => {
                "authority-clock administration requires the dedicated recovery endpoint".to_owned()
            }
            ControlPlaneRpcServerRole::AuthorityClockRecovery => {
                "dedicated authority-clock recovery endpoint rejects ordinary control-plane RPCs"
                    .to_owned()
            }
        });
        return Err(ControlPlaneRpcAdmissionFailure::Authenticated {
            request: Box::new(request),
            error: Box::new(error),
        });
    }
    if request.requires_raft_authority_confirmation() {
        if let Some(confirm) = &policy.authority_confirmation {
            if let Err(error) = confirm() {
                return Err(ControlPlaneRpcAdmissionFailure::Authenticated {
                    request: Box::new(request),
                    error: Box::new(error),
                });
            }
        }
    }
    Ok(request)
}

trait ControlPlaneRpcServerStream: std::io::Read + std::io::Write + Send {
    fn begin_response(&mut self, timeout: Duration);
    fn finish_response(&mut self) -> std::io::Result<()>;
}

impl ControlPlaneRpcServerStream for ControlPlaneDeadlineUnixSocket {
    fn begin_response(&mut self, timeout: Duration) {
        self.set_deadline(Instant::now() + timeout);
    }

    fn finish_response(&mut self) -> std::io::Result<()> {
        self.flush()
    }
}

impl ControlPlaneRpcServerStream
    for rustls::StreamOwned<rustls::ServerConnection, ControlPlaneDeadlineTcpSocket>
{
    fn begin_response(&mut self, timeout: Duration) {
        self.sock.set_deadline(Instant::now() + timeout);
    }

    fn finish_response(&mut self) -> std::io::Result<()> {
        self.conn.send_close_notify();
        self.flush()
    }
}

struct ControlPlaneRpcWorkerGuard {
    active_workers: Arc<AtomicUsize>,
}

impl Drop for ControlPlaneRpcWorkerGuard {
    fn drop(&mut self) {
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ControlPlaneRpcServerListener {
    /// Serves this listener using one shared, mutex-protected authority.
    ///
    /// This method blocks until the listener encounters an unrecoverable
    /// accept error. Individual connection failures remain isolated to their
    /// worker.
    pub(crate) fn serve_shared<T>(
        self,
        authority: Arc<Mutex<T>>,
        policy: ControlPlaneRpcServerPolicy,
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        self.serve_with(
            move || ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
            policy,
        )
    }

    /// Serves this listener with an independently cloned authority per worker.
    pub(crate) fn serve_cloned<T>(
        self,
        authority: T,
        policy: ControlPlaneRpcServerPolicy,
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: Clone
            + ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        self.serve_with(
            move || ControlPlaneRpcServerAuthority::PerWorker(authority.clone()),
            policy,
        )
    }

    /// Serves a bounded sequence of requests for a cross-crate process test.
    ///
    /// This test-only facility retains the complete production facade while
    /// allowing deterministic authority timestamps and a joinable server.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn serve_shared_requests_for_test<T>(
        self,
        authority: Arc<Mutex<T>>,
        mut policy: ControlPlaneRpcServerPolicy,
        authority_times_ms: impl IntoIterator<Item = u64>,
        mut after_request: impl FnMut(&T),
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        let configure_result = match &self.kind {
            ControlPlaneRpcServerListenerKind::Unix(listener) => listener.set_nonblocking(false),
            ControlPlaneRpcServerListenerKind::TlsTcp { listener, .. } => {
                listener.set_nonblocking(false)
            }
        };
        if let Err(error) = configure_result {
            eprintln!("control-plane RPC test listener setup failed: {error}");
            return Err(ControlPlaneRpcServerError);
        }
        for authority_now_ms in authority_times_ms {
            policy.test_authority_now_ms = Some(authority_now_ms);
            self.accept_one(
                &|| ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
                &policy,
            )?;
            while policy.active_workers() != 0 {
                std::thread::yield_now();
            }
            let authority = authority
                .lock()
                .expect("control-plane test authority mutex poisoned");
            after_request(&authority);
        }
        Ok(())
    }

    /// Injects one pre-dispatch connection loss, then serves bounded requests.
    #[cfg(feature = "test-hooks")]
    pub(crate) fn serve_shared_requests_after_dropped_connection_for_test<T>(
        self,
        authority: Arc<Mutex<T>>,
        policy: ControlPlaneRpcServerPolicy,
        authority_times_ms: impl IntoIterator<Item = u64>,
        after_request: impl FnMut(&T),
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        match &self.kind {
            ControlPlaneRpcServerListenerKind::Unix(listener) => listener.accept().map(drop),
            ControlPlaneRpcServerListenerKind::TlsTcp { listener, .. } => {
                listener.accept().map(drop)
            }
        }
        .map_err(|error| {
            eprintln!("control-plane RPC test connection-loss injection failed: {error}");
            ControlPlaneRpcServerError
        })?;
        self.serve_shared_requests_for_test(authority, policy, authority_times_ms, after_request)
    }

    fn serve_with<T>(
        self,
        authority: impl Fn() -> ControlPlaneRpcServerAuthority<T>,
        policy: ControlPlaneRpcServerPolicy,
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        let configure_result = match &self.kind {
            ControlPlaneRpcServerListenerKind::Unix(listener) => listener.set_nonblocking(false),
            ControlPlaneRpcServerListenerKind::TlsTcp { listener, .. } => {
                listener.set_nonblocking(false)
            }
        };
        if let Err(error) = configure_result {
            eprintln!("control-plane RPC listener setup failed: {error}");
            return Err(ControlPlaneRpcServerError);
        }
        loop {
            self.accept_one(&authority, &policy)?;
        }
    }

    fn accept_one<T>(
        &self,
        authority: &impl Fn() -> ControlPlaneRpcServerAuthority<T>,
        policy: &ControlPlaneRpcServerPolicy,
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        let worker_limit = policy.resources.worker_limit.min(self.max_connections);
        match &self.kind {
            ControlPlaneRpcServerListenerKind::Unix(listener) => match listener.accept() {
                Ok((stream, _)) => spawn_control_plane_rpc_server_worker(
                    stream,
                    authority(),
                    policy.clone(),
                    policy.authentication_required(),
                    self.max_frame_bytes,
                    worker_limit,
                    self.io_timeout,
                    move |stream, deadline| {
                        stream.set_nonblocking(false).map_err(|source| {
                            ControlPlaneError::io(
                                "set control-plane Unix RPC blocking mode",
                                source,
                            )
                        })?;
                        ControlPlaneDeadlineUnixSocket::new(
                            stream,
                            deadline,
                            CONTROL_PLANE_RPC_DEADLINE_EXPIRED,
                        )
                        .map(|stream| Box::new(stream) as Box<dyn ControlPlaneRpcServerStream>)
                        .map_err(|source| {
                            ControlPlaneError::io(
                                "configure control-plane Unix RPC deadline I/O",
                                source,
                            )
                        })
                    },
                ),
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => {
                    eprintln!("control-plane Unix socket accept failed: {error}");
                    return Err(ControlPlaneRpcServerError);
                }
            },
            ControlPlaneRpcServerListenerKind::TlsTcp {
                listener,
                tls_server_config,
            } => match listener.accept() {
                Ok((stream, _)) => {
                    let tls_server_config = Arc::clone(tls_server_config);
                    spawn_control_plane_rpc_server_worker(
                        stream,
                        authority(),
                        policy.clone(),
                        true,
                        self.max_frame_bytes,
                        worker_limit,
                        self.io_timeout,
                        move |stream, deadline| {
                            let socket = ControlPlaneDeadlineTcpSocket::new(
                                stream,
                                deadline,
                                CONTROL_PLANE_RPC_DEADLINE_EXPIRED,
                            )
                            .map_err(|source| {
                                ControlPlaneError::io(
                                    "configure control-plane TLS/TCP deadline I/O",
                                    source,
                                )
                            })?;
                            let connection = rustls::ServerConnection::new(tls_server_config)
                                .map_err(|_| {
                                    ControlPlaneError::rpc_protocol(
                                        "failed to initialize control-plane TLS server connection"
                                            .to_owned(),
                                    )
                                })?;
                            let mut stream = rustls::StreamOwned::new(connection, socket);
                            while stream.conn.is_handshaking() {
                                stream
                                    .conn
                                    .complete_io(&mut stream.sock)
                                    .map_err(|source| {
                                        ControlPlaneError::io(
                                            "complete control-plane TLS server handshake",
                                            source,
                                        )
                                    })?;
                            }
                            if stream.conn.alpn_protocol() != Some(CONTROL_PLANE_RPC_TLS_ALPN) {
                                return Err(ControlPlaneError::rpc_protocol("control-plane TLS peer did not negotiate the required protocol profile".to_owned()));
                            }
                            Ok(Box::new(stream) as Box<dyn ControlPlaneRpcServerStream>)
                        },
                    );
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => {
                    eprintln!("control-plane TCP socket accept failed: {error}");
                    return Err(ControlPlaneRpcServerError);
                }
            },
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_control_plane_rpc_server_worker<T, RawStream, Prepare>(
    stream: RawStream,
    mut authority: ControlPlaneRpcServerAuthority<T>,
    policy: ControlPlaneRpcServerPolicy,
    require_authentication: bool,
    max_frame_bytes: usize,
    worker_limit: usize,
    io_timeout: Duration,
    prepare: Prepare,
) where
    T: ControlPlaneAdmin
        + ControlPlaneHeartbeatRuntimeMapSource
        + ControlPlaneRuntimeMapSource
        + Send
        + 'static,
    RawStream: Send + 'static,
    Prepare: FnOnce(
            RawStream,
            Instant,
        ) -> Result<Box<dyn ControlPlaneRpcServerStream>, ControlPlaneError>
        + Send
        + 'static,
{
    if !reserve_control_plane_rpc_worker(&policy.resources.active_workers, worker_limit) {
        eprintln!("control-plane RPC rejected: worker limit reached");
        return;
    }
    let connection_deadline = Instant::now()
        .checked_add(io_timeout)
        .unwrap_or(Instant::now());
    std::thread::spawn(move || {
        let _guard = ControlPlaneRpcWorkerGuard {
            active_workers: Arc::clone(&policy.resources.active_workers),
        };
        let mut stream = match prepare(stream, connection_deadline) {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("control-plane RPC transport setup failed: {error}");
                return;
            }
        };
        let request = match read_control_plane_request_with_reservation(
            &mut stream,
            |frame_bytes| {
                if frame_bytes > max_frame_bytes {
                    return Err(ControlPlaneError::rpc_protocol(format!(
                            "control-plane RPC frame size {frame_bytes} bytes exceeds listener limit {max_frame_bytes}"
                        )));
                }
                policy.resources.pre_auth_byte_budget.reserve(frame_bytes)
            },
        ) {
            Ok((request, reservation)) => (request, reservation),
            Err(error) => {
                eprintln!("control-plane RPC request read failed: {error}");
                return;
            }
        };
        let (request, _pre_auth_byte_reservation) = request;
        let metrics_kind = request.metrics_kind();
        let request = match authenticate_and_admit_control_plane_rpc(
            request,
            &policy,
            require_authentication,
            policy.authority_now_ms(),
        ) {
            Ok(request) => request,
            Err(ControlPlaneRpcAdmissionFailure::Unauthenticated(error)) => {
                eprintln!("control-plane RPC authentication failed: {error}");
                return;
            }
            Err(ControlPlaneRpcAdmissionFailure::Authenticated { request, error }) => {
                let response = build_control_plane_unix_admission_error_response(
                    *request,
                    *error,
                    policy.authority_now_ms(),
                );
                stream.begin_response(io_timeout);
                write_control_plane_rpc_admission_response(&mut stream, metrics_kind, response);
                if let Err(error) = stream.finish_response() {
                    eprintln!("control-plane RPC response finalization failed: {error}");
                }
                return;
            }
        };
        let response = (|| {
            if request.is_authority_clock_admin() {
                let _operation_timer =
                    observability::control_plane_rpc_operation_timer(metrics_kind);
                let authority_clock = policy.authority_clock.as_ref().ok_or_else(|| {
                    ControlPlaneError::rpc_protocol(
                        "authority-clock administration requires a process-local clock gate"
                            .to_owned(),
                    )
                })?;
                let context = authority.with_mut(metrics_kind, |authority| {
                    authority.authority_clock_context()
                });
                let mut authority_clock = authority_clock
                    .lock()
                    .expect("control-plane authority clock mutex poisoned");
                let response =
                    build_control_plane_authority_clock_admin_response_from_verified_with_context(
                        &mut authority_clock,
                        request,
                        ControlPlaneAuthorityClockAdminSample::from_process_clock()?,
                        context,
                        |context, authority_clock| {
                            policy
                                .authority_clock_checkpoint_target
                                .as_ref()
                                .ok_or_else(|| ControlPlaneError::rpc_protocol("authority-clock administration requires a durable checkpoint target".to_owned()))?
                                .persist_established(context, authority_clock)
                        },
                        || Ok(policy.authority_now_ms()),
                    );
                policy
                    .authority_clock_checkpoint_target
                    .as_ref()
                    .ok_or_else(|| {
                        ControlPlaneError::rpc_protocol(
                            "authority-clock administration requires a durable checkpoint target"
                                .to_owned(),
                        )
                    })?
                    .invalidate_if_blocked(&authority_clock)?;
                response
            } else if request.is_refresh_node_heartbeat() {
                let prepared = {
                    let _operation_timer =
                        observability::control_plane_rpc_operation_timer(metrics_kind);
                    authority.with_mut(metrics_kind, |authority| {
                        let (now_ms, lease_horizon_authority) = match &policy.authority_clock {
                            Some(authority_clock)
                                if policy.gate_request_time_with_authority_clock =>
                            {
                                let mut authority_clock = authority_clock
                                    .lock()
                                    .expect("control-plane authority clock mutex poisoned");
                                let now_ms = authority_clock.effective_process_now_ms();
                                policy
                                    .authority_clock_checkpoint_target
                                    .as_ref()
                                    .expect("configured authority clock has checkpoint target")
                                    .invalidate_if_blocked(&authority_clock)?;
                                let now_ms = now_ms?;
                                let lease_horizon_authority =
                                    authority_clock.lease_horizon_authority_binding(None)?;
                                (now_ms, Some(lease_horizon_authority))
                            }
                            _ => (policy.authority_now_ms(), None),
                        };
                        match lease_horizon_authority {
                            Some(lease_horizon_authority) => {
                                prepare_control_plane_heartbeat_response_with_lease_horizon_authority_from_verified(
                                    authority,
                                    request,
                                    now_ms,
                                    lease_horizon_authority,
                                )
                            }
                            None => prepare_control_plane_heartbeat_response_from_verified(
                                authority, request, now_ms,
                            ),
                        }
                    })
                };
                prepared.and_then(|prepared| {
                    finish_control_plane_heartbeat_response(prepared, || {
                        Ok(policy.authority_now_ms())
                    })
                })
            } else {
                let _operation_timer =
                    observability::control_plane_rpc_operation_timer(metrics_kind);
                authority.with_mut(metrics_kind, |authority| {
                    let now_ms = match &policy.authority_clock {
                        Some(authority_clock) if policy.gate_request_time_with_authority_clock => {
                            let mut authority_clock = authority_clock
                                .lock()
                                .expect("control-plane authority clock mutex poisoned");
                            let now_ms = authority_clock.effective_process_now_ms();
                            policy
                                .authority_clock_checkpoint_target
                                .as_ref()
                                .expect("configured authority clock has checkpoint target")
                                .invalidate_if_blocked(&authority_clock)?;
                            now_ms?
                        }
                        _ => policy.authority_now_ms(),
                    };
                    build_control_plane_unix_response_from_verified(
                        authority,
                        request,
                        now_ms,
                        || Ok(policy.authority_now_ms()),
                    )
                })
            }
        })();
        if let Some(authority_clock) = &policy.authority_clock {
            let authority_clock = authority_clock
                .lock()
                .expect("control-plane authority clock mutex poisoned");
            let checkpoint_result = policy
                .authority_clock_checkpoint_target
                .as_ref()
                .expect("configured authority clock has checkpoint target")
                .invalidate_if_blocked(&authority_clock);
            if let Err(error) = checkpoint_result {
                eprintln!(
                    "failed to invalidate blocked control-plane authority-clock checkpoint: {error}"
                );
                if let Some(fatal_error_handler) = &policy.fatal_error_handler {
                    fatal_error_handler();
                }
                return;
            }
        }
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                eprintln!("control-plane RPC response build failed: {error}");
                return;
            }
        };
        let response_write_started = Instant::now();
        stream.begin_response(io_timeout);
        let mut response = Some(response);
        let mut write_response = || {
            let response = response.take().ok_or_else(|| {
                ControlPlaneError::rpc_protocol(
                    "control-plane response publication attempted more than once".to_owned(),
                )
            })?;
            stream.begin_response(io_timeout);
            write_control_plane_rpc_response_and_flush(&mut stream, response)
        };
        let response_result = publish_control_plane_rpc_response(
            policy.response_publication.as_deref(),
            &mut write_response,
        );
        observability::record_control_plane_rpc_response_write(
            metrics_kind,
            response_write_started.elapsed(),
        );
        if let Err(error) = response_result {
            observability::record_control_plane_rpc_response_write_error(
                metrics_kind,
                control_plane_rpc_response_write_error_kind(&error),
            );
            eprintln!("control-plane RPC response failed: {error}");
        } else if let Err(error) = stream.finish_response() {
            eprintln!("control-plane RPC response finalization failed: {error}");
        }
    });
}

fn reserve_control_plane_rpc_worker(active_workers: &AtomicUsize, worker_limit: usize) -> bool {
    active_workers
        .try_update(Ordering::AcqRel, Ordering::Acquire, |active| {
            (active < worker_limit).then_some(active + 1)
        })
        .is_ok()
}

fn write_control_plane_rpc_admission_response(
    stream: &mut impl std::io::Write,
    metrics_kind: observability::ControlPlaneRpcMetricKind,
    response: Result<ControlPlaneRpcResponse, ControlPlaneError>,
) {
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            eprintln!("control-plane RPC admission response build failed: {error}");
            return;
        }
    };
    let response_write_started = Instant::now();
    let response_result = write_control_plane_rpc_response_and_flush(stream, response);
    observability::record_control_plane_rpc_response_write(
        metrics_kind,
        response_write_started.elapsed(),
    );
    if let Err(error) = response_result {
        observability::record_control_plane_rpc_response_write_error(
            metrics_kind,
            control_plane_rpc_response_write_error_kind(&error),
        );
        eprintln!("control-plane RPC admission response failed: {error}");
    }
}

fn write_control_plane_rpc_response_and_flush(
    stream: &mut impl std::io::Write,
    response: ControlPlaneRpcResponse,
) -> Result<(), ControlPlaneError> {
    write_control_plane_unix_response(stream, response)?;
    stream
        .flush()
        .map_err(|source| ControlPlaneError::io("flush control-plane RPC response", source))
}

fn control_plane_rpc_response_write_error_kind(
    error: &ControlPlaneError,
) -> observability::ControlPlaneRpcResponseWriteErrorKind {
    let ControlPlaneError::Io { diagnostic: source } = error else {
        return observability::ControlPlaneRpcResponseWriteErrorKind::Other;
    };
    match source.kind() {
        ErrorKind::BrokenPipe => observability::ControlPlaneRpcResponseWriteErrorKind::BrokenPipe,
        ErrorKind::ConnectionReset => {
            observability::ControlPlaneRpcResponseWriteErrorKind::ConnectionReset
        }
        ErrorKind::TimedOut | ErrorKind::WouldBlock => {
            observability::ControlPlaneRpcResponseWriteErrorKind::Timeout
        }
        _ => observability::ControlPlaneRpcResponseWriteErrorKind::Other,
    }
}

fn write_control_plane_rpc_frame(
    stream: &mut impl std::io::Write,
    kind: ControlPlaneRpcKind,
    payload: &[u8],
) -> Result<(), ControlPlaneError> {
    let frame = encode_control_plane_rpc_frame(kind, payload)?;
    let magic_len = CONTROL_PLANE_RPC_MAGIC.len();
    stream
        .write_all(&frame[..magic_len])
        .map_err(|source| ControlPlaneError::io("write control-plane RPC magic", source))?;
    stream
        .write_all(&frame[magic_len..])
        .map_err(|source| ControlPlaneError::io("write control-plane RPC frame", source))
}

fn encode_control_plane_rpc_frame(
    kind: ControlPlaneRpcKind,
    payload: &[u8],
) -> Result<Vec<u8>, ControlPlaneError> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        ControlPlaneError::rpc_protocol(format!(
            "control-plane RPC payload too large: {}",
            payload.len()
        ))
    })?;
    let mut frame = Vec::with_capacity(control_plane_rpc_frame_overhead() + payload.len());
    frame.extend_from_slice(CONTROL_PLANE_RPC_MAGIC);
    write_u16(&mut frame, CONTROL_PLANE_RPC_VERSION);
    write_u16(&mut frame, kind as u16);
    write_u32(&mut frame, payload_len);
    write_u64(
        &mut frame,
        control_plane_rpc_frame_checksum(
            CONTROL_PLANE_RPC_VERSION,
            kind as u16,
            payload_len,
            payload,
        ),
    );
    frame.extend_from_slice(payload);
    Ok(frame)
}

const fn control_plane_rpc_frame_overhead() -> usize {
    CONTROL_PLANE_RPC_MAGIC.len() + 16
}

fn read_control_plane_rpc_frame(
    stream: &mut impl std::io::Read,
) -> Result<(ControlPlaneRpcKind, Vec<u8>), ControlPlaneError> {
    read_control_plane_rpc_frame_with_reservation(stream, |_| Ok(())).map(|(frame, ())| frame)
}

fn read_control_plane_rpc_frame_with_reservation<R>(
    stream: &mut impl std::io::Read,
    reserve: impl FnOnce(usize) -> Result<R, ControlPlaneError>,
) -> Result<((ControlPlaneRpcKind, Vec<u8>), R), ControlPlaneError> {
    let mut magic = vec![0; CONTROL_PLANE_RPC_MAGIC.len()];
    stream
        .read_exact(&mut magic)
        .map_err(|source| ControlPlaneError::io("read control-plane RPC magic", source))?;
    if magic != CONTROL_PLANE_RPC_MAGIC {
        return Err(ControlPlaneError::rpc_protocol(
            "invalid control-plane RPC magic".to_owned(),
        ));
    }
    let mut header = [0; 16];
    stream
        .read_exact(&mut header)
        .map_err(|source| ControlPlaneError::io("read control-plane RPC header", source))?;
    let mut reader = PayloadReader::new(&header);
    let version = reader.read_u16()?;
    if version != CONTROL_PLANE_RPC_VERSION {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "unsupported control-plane RPC version {version}"
        )));
    }
    let kind = ControlPlaneRpcKind::from_u16(reader.read_u16()?)?;
    let raw_kind = kind as u16;
    let payload_len_u32 = reader.read_u32()?;
    let payload_len = usize::try_from(payload_len_u32).map_err(|_| {
        ControlPlaneError::rpc_protocol(
            "control-plane RPC payload length does not fit usize".to_owned(),
        )
    })?;
    let expected_checksum = reader.read_u64()?;
    reader.finish()?;
    if payload_len > CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "control-plane RPC payload too large: {payload_len}"
        )));
    }
    let reservation = reserve(control_plane_rpc_frame_overhead() + payload_len)?;
    let mut payload = vec![0; payload_len];
    stream
        .read_exact(&mut payload)
        .map_err(|source| ControlPlaneError::io("read control-plane RPC payload", source))?;
    if control_plane_rpc_frame_checksum(version, raw_kind, payload_len_u32, &payload)
        != expected_checksum
    {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane RPC frame checksum mismatch".to_owned(),
        ));
    }
    Ok(((kind, payload), reservation))
}

fn control_plane_rpc_frame_checksum(
    version: u16,
    raw_kind: u16,
    payload_len: u32,
    payload: &[u8],
) -> u64 {
    let mut hasher = checksum::crc64::Hasher::new();
    hasher.update(CONTROL_PLANE_RPC_MAGIC);
    hasher.update(&version.to_le_bytes());
    hasher.update(&raw_kind.to_le_bytes());
    hasher.update(&payload_len.to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}

fn encode_control_plane_rpc_response(
    response: Result<Vec<u8>, ControlPlaneError>,
) -> Result<Vec<u8>, ControlPlaneError> {
    let mut payload = Vec::new();
    match response {
        Ok(response) => {
            write_u8(&mut payload, 0);
            write_bytes(&mut payload, &response)?;
        }
        Err(ControlPlaneError::PgPeeringPendingMetadataCommand {
            pg_id,
            node_id,
            cluster_epoch,
            pending,
        }) => {
            write_u8(&mut payload, 2);
            write_u32(&mut payload, pg_id);
            write_u32(&mut payload, node_id);
            write_u64(&mut payload, cluster_epoch.get());
            write_u64(&mut payload, pending.cluster_epoch().get());
            write_u64(&mut payload, pending.log_index());
            write_u64(&mut payload, pending.command_checksum());
        }
        Err(ControlPlaneError::PgMetadataMigrationSourceNotReady {
            pg_id,
            cluster_epoch,
        }) => {
            write_u8(&mut payload, 3);
            write_u32(&mut payload, pg_id);
            write_u64(&mut payload, cluster_epoch.get());
        }
        Err(ControlPlaneError::PgHasNoServingPrimary {
            pg_id,
            cluster_epoch,
        }) => {
            write_u8(&mut payload, 4);
            write_u32(&mut payload, pg_id);
            write_u64(&mut payload, cluster_epoch.get());
        }
        Err(ControlPlaneError::PgPrimaryMissingActiveObservation {
            pg_id,
            node_id,
            cluster_epoch,
        }) => {
            write_u8(&mut payload, 5);
            write_u32(&mut payload, pg_id);
            write_u32(&mut payload, node_id);
            write_u64(&mut payload, cluster_epoch.get());
        }
        Err(ControlPlaneError::PgPrimaryObservationNotActive {
            pg_id,
            node_id,
            cluster_epoch,
            state,
        }) => {
            write_u8(&mut payload, 6);
            write_u32(&mut payload, pg_id);
            write_u32(&mut payload, node_id);
            write_u64(&mut payload, cluster_epoch.get());
            write_pg_state(&mut payload, state);
        }
        Err(ControlPlaneError::PgActingSetChangeNotReady {
            pg_id,
            cluster_epoch,
            state,
        }) => {
            write_u8(&mut payload, 7);
            write_u32(&mut payload, pg_id);
            write_u64(&mut payload, cluster_epoch.get());
            write_pg_state(&mut payload, state);
        }
        Err(ControlPlaneError::UnknownPg { pg_id }) => {
            write_u8(&mut payload, 8);
            write_u32(&mut payload, pg_id);
        }
        Err(ControlPlaneError::OpenRaftOperation { kind, message }) => {
            write_u8(&mut payload, 9);
            write_u8(&mut payload, kind.wire_tag());
            write_string(&mut payload, &message)?;
        }
        Err(ControlPlaneError::AuthorityNotServing) => {
            write_u8(&mut payload, 10);
        }
        Err(ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority) => {
            write_u8(&mut payload, 11);
        }
        Err(ControlPlaneError::UnknownNode { node_id }) => {
            write_u8(&mut payload, 12);
            write_u32(&mut payload, node_id);
        }
        Err(ControlPlaneError::UnknownActingSetNode { pg_id, node_id }) => {
            write_u8(&mut payload, 13);
            write_u32(&mut payload, pg_id);
            write_u32(&mut payload, node_id);
        }
        Err(ControlPlaneError::AuthorityClockLeadershipChanged {
            established_term,
            current_term,
        }) => {
            write_u8(&mut payload, 14);
            write_option_u64(&mut payload, established_term);
            write_u64(&mut payload, current_term);
        }
        Err(ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
            pg_id,
            expected_destination_epoch,
            actual_destination_epoch,
        }) => {
            write_u8(&mut payload, 15);
            write_u32(&mut payload, pg_id);
            write_u64(&mut payload, expected_destination_epoch.get());
            write_u64(&mut payload, actual_destination_epoch.get());
        }
        Err(error) => {
            write_u8(&mut payload, 1);
            write_string(&mut payload, &error.rpc_wire_error_message())?;
        }
    }
    Ok(payload)
}

enum DecodedControlPlaneRpcResponse {
    Success(Vec<u8>),
    Rejection(ControlPlaneError),
}

fn decode_control_plane_rpc_response(payload: Vec<u8>) -> Result<Vec<u8>, ControlPlaneError> {
    match decode_control_plane_rpc_response_frame(payload)? {
        DecodedControlPlaneRpcResponse::Success(payload) => Ok(payload),
        DecodedControlPlaneRpcResponse::Rejection(error) => Err(error),
    }
}

fn decode_control_plane_rpc_response_frame(
    payload: Vec<u8>,
) -> Result<DecodedControlPlaneRpcResponse, ControlPlaneError> {
    let mut reader = PayloadReader::new(&payload);
    let status = reader.read_u8()?;
    match status {
        0 => {
            let response = reader.read_bytes()?.to_vec();
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Success(response))
        }
        1 => {
            let message = reader.read_string()?.to_owned();
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::rpc_remote(message),
            ))
        }
        2 => {
            let pg_id = reader.read_u32()?;
            let node_id = reader.read_u32()?;
            let cluster_epoch = read_cluster_epoch(&mut reader, "pending blocker cluster epoch")?;
            let pending_cluster_epoch =
                read_cluster_epoch(&mut reader, "pending command cluster epoch")?;
            let pending_log_index = NonZeroU64::new(reader.read_u64()?).ok_or_else(|| {
                ControlPlaneError::rpc_protocol(
                    "pending command log index must be nonzero".to_owned(),
                )
            })?;
            let pending_command_checksum = reader.read_u64()?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgPeeringPendingMetadataCommand {
                    pg_id,
                    node_id,
                    cluster_epoch,
                    pending: PendingMetadataCommandObservation::new(
                        pending_cluster_epoch,
                        pending_log_index,
                        pending_command_checksum,
                    ),
                },
            ))
        }
        3 => {
            let pg_id = reader.read_u32()?;
            let cluster_epoch =
                read_cluster_epoch(&mut reader, "metadata migration source cluster epoch")?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgMetadataMigrationSourceNotReady {
                    pg_id,
                    cluster_epoch,
                },
            ))
        }
        4 => {
            let pg_id = reader.read_u32()?;
            let cluster_epoch =
                read_cluster_epoch(&mut reader, "PG serving-primary cluster epoch")?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgHasNoServingPrimary {
                    pg_id,
                    cluster_epoch,
                },
            ))
        }
        5 => {
            let pg_id = reader.read_u32()?;
            let node_id = reader.read_u32()?;
            let cluster_epoch =
                read_cluster_epoch(&mut reader, "PG primary-observation cluster epoch")?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgPrimaryMissingActiveObservation {
                    pg_id,
                    node_id,
                    cluster_epoch,
                },
            ))
        }
        6 => {
            let pg_id = reader.read_u32()?;
            let node_id = reader.read_u32()?;
            let cluster_epoch =
                read_cluster_epoch(&mut reader, "PG primary-observation cluster epoch")?;
            let state = read_pg_state(&mut reader)?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgPrimaryObservationNotActive {
                    pg_id,
                    node_id,
                    cluster_epoch,
                    state,
                },
            ))
        }
        7 => {
            let pg_id = reader.read_u32()?;
            let cluster_epoch =
                read_cluster_epoch(&mut reader, "PG acting-set readiness cluster epoch")?;
            let state = read_pg_state(&mut reader)?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgActingSetChangeNotReady {
                    pg_id,
                    cluster_epoch,
                    state,
                },
            ))
        }
        8 => {
            let pg_id = reader.read_u32()?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::UnknownPg { pg_id },
            ))
        }
        9 => {
            let kind = ControlPlaneRaftOperationErrorKind::from_wire_tag(reader.read_u8()?)?;
            let message = reader.read_string()?.to_owned();
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::OpenRaftOperation { kind, message },
            ))
        }
        10 => {
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::AuthorityNotServing,
            ))
        }
        11 => {
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority,
            ))
        }
        12 => {
            let node_id = reader.read_u32()?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::UnknownNode { node_id },
            ))
        }
        13 => {
            let pg_id = reader.read_u32()?;
            let node_id = reader.read_u32()?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::UnknownActingSetNode { pg_id, node_id },
            ))
        }
        14 => {
            let established_term = reader.read_option_u64()?;
            let current_term = reader.read_u64()?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::AuthorityClockLeadershipChanged {
                    established_term,
                    current_term,
                },
            ))
        }
        15 => {
            let pg_id = reader.read_u32()?;
            let expected_destination_epoch =
                read_cluster_epoch(&mut reader, "expected metadata transfer destination epoch")?;
            let actual_destination_epoch =
                read_cluster_epoch(&mut reader, "actual metadata transfer destination epoch")?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
                    pg_id,
                    expected_destination_epoch,
                    actual_destination_epoch,
                },
            ))
        }
        _ => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid control-plane RPC response status {status}"
        ))),
    }
}

fn write_node_heartbeat_payload(heartbeat: &NodeHeartbeat) -> Result<Vec<u8>, ControlPlaneError> {
    let mut payload = Vec::new();
    write_node_heartbeat(&mut payload, heartbeat)?;
    Ok(payload)
}

fn read_node_heartbeat_payload(payload: &[u8]) -> Result<NodeHeartbeat, ControlPlaneError> {
    let mut reader = PayloadReader::new(payload);
    let heartbeat = read_node_heartbeat(&mut reader)?;
    reader.finish()?;
    Ok(heartbeat)
}

fn write_node_heartbeat(
    out: &mut Vec<u8>,
    heartbeat: &NodeHeartbeat,
) -> Result<(), ControlPlaneError> {
    write_u32(out, heartbeat.node_id.as_u32());
    write_u64(out, heartbeat.node_incarnation);
    write_string(out, &heartbeat.endpoint)?;
    write_u64(out, heartbeat.observed_epoch.get());
    write_u64(out, heartbeat.requested_lease_duration_ms);
    write_cluster_map_history_route_references(
        out,
        &heartbeat.cluster_map_history_route_references,
    )?;
    write_u32(
        out,
        len_as_u32(heartbeat.pg_observations.len(), "PG observations")?,
    );
    for observation in &heartbeat.pg_observations {
        write_u32(out, observation.pg_id.get());
        write_pg_state(out, observation.state);
        write_pg_metadata_proof(out, observation.metadata_proof);
        write_pending_metadata_command_observation(out, observation.pending_metadata_command);
    }
    Ok(())
}

fn read_node_heartbeat(reader: &mut PayloadReader<'_>) -> Result<NodeHeartbeat, ControlPlaneError> {
    let node_id = NodeId::new(reader.read_u32()?);
    let node_incarnation = reader.read_u64()?;
    let endpoint = reader.read_string()?.to_owned();
    let observed_epoch = read_cluster_epoch(reader, "heartbeat observed epoch")?;
    let requested_lease_duration_ms = reader.read_u64()?;
    let cluster_map_history_route_references = read_cluster_map_history_route_references(reader)?;
    let observation_count = reader.read_collection_len(
        "PG observations",
        CONTROL_PLANE_RPC_HEARTBEAT_OBSERVATION_MIN_LEN,
    )?;
    let mut pg_observations = Vec::with_capacity(observation_count);
    for _ in 0..observation_count {
        pg_observations.push(NodePgHeartbeatObservation {
            pg_id: PgId::new(reader.read_u32()?),
            state: read_pg_state(reader)?,
            metadata_proof: read_pg_metadata_proof(reader)?,
            pending_metadata_command: read_pending_metadata_command_observation(reader)?,
        });
    }
    Ok(NodeHeartbeat {
        node_id,
        node_incarnation,
        endpoint,
        observed_epoch,
        requested_lease_duration_ms,
        cluster_map_history_route_references,
        pg_observations,
    })
}

fn write_cluster_map_history_route_references(
    out: &mut Vec<u8>,
    references: &PgClusterMapHistoryRouteReferences,
) -> Result<(), ControlPlaneError> {
    write_u32(
        out,
        len_as_u32(references.len(), "cluster-map history route references")?,
    );
    for reference in references.iter() {
        write_u8(
            out,
            cluster_map_history_route_reference_kind_code(reference.kind()),
        );
        write_u64(out, reference.cluster_epoch().get());
        write_u32(out, reference.pg_id().get());
    }
    Ok(())
}

fn read_cluster_map_history_route_references(
    reader: &mut PayloadReader<'_>,
) -> Result<PgClusterMapHistoryRouteReferences, ControlPlaneError> {
    let count = reader.read_collection_len(
        "cluster-map history route references",
        CONTROL_PLANE_RPC_HISTORY_ROUTE_REFERENCE_MIN_LEN,
    )?;
    if count > MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "cluster-map history route reference count {count} exceeds {}",
            MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES
        )));
    }
    let mut decoded = Vec::with_capacity(count);
    let mut previous = None;
    for _ in 0..count {
        let reference = PgClusterMapHistoryRouteReference::new(
            read_cluster_map_history_route_reference_kind(reader)?,
            read_cluster_epoch(reader, "cluster-map history route reference epoch")?,
            PgId::new(reader.read_u32()?),
        );
        if previous.is_some_and(|previous| reference <= previous) {
            return Err(ControlPlaneError::rpc_protocol(
                "cluster-map history route references are not in canonical order".to_owned(),
            ));
        }
        previous = Some(reference);
        decoded.push(reference);
    }
    PgClusterMapHistoryRouteReferences::try_from_iter(decoded).map_err(|error| {
        ControlPlaneError::rpc_protocol(format!(
            "invalid cluster-map history route references: {error}"
        ))
    })
}

const fn cluster_map_history_route_reference_kind_code(
    kind: PgClusterMapHistoryRouteReferenceKind,
) -> u8 {
    match kind {
        PgClusterMapHistoryRouteReferenceKind::LivePlacement => 1,
        PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource => 2,
        PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired => 3,
        PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand => 4,
        PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim => 5,
    }
}

fn read_cluster_map_history_route_reference_kind(
    reader: &mut PayloadReader<'_>,
) -> Result<PgClusterMapHistoryRouteReferenceKind, ControlPlaneError> {
    match reader.read_u8()? {
        1 => Ok(PgClusterMapHistoryRouteReferenceKind::LivePlacement),
        2 => Ok(PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource),
        3 => Ok(PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired),
        4 => Ok(PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand),
        5 => Ok(PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim),
        value => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid cluster-map history route reference kind {value}"
        ))),
    }
}

fn write_pg_acting_set_request(
    out: &mut Vec<u8>,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<(), ControlPlaneError> {
    write_u32(out, pg_id.get());
    write_u32(out, len_as_u32(acting_set.len(), "acting set")?);
    for node_id in acting_set {
        write_u32(out, node_id.as_u32());
    }
    Ok(())
}

fn read_pg_acting_set_request(
    reader: &mut PayloadReader<'_>,
) -> Result<(PgId, Vec<NodeId>), ControlPlaneError> {
    let pg_id = PgId::new(reader.read_u32()?);
    let node_count =
        reader.read_collection_len("acting set", CONTROL_PLANE_RPC_ACTING_SET_NODE_MIN_LEN)?;
    let mut acting_set = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        acting_set.push(NodeId::new(reader.read_u32()?));
    }
    Ok((pg_id, acting_set))
}

fn write_pg_id_request(out: &mut Vec<u8>, pg_id: PgId) {
    write_u32(out, pg_id.get());
}

fn read_pg_id_request(reader: &mut PayloadReader<'_>) -> Result<PgId, ControlPlaneError> {
    Ok(PgId::new(reader.read_u32()?))
}

fn write_pg_acting_set_with_metadata_transfer_request(
    out: &mut Vec<u8>,
    pg_id: PgId,
    acting_set: &[NodeId],
    transfer: PgMetadataTransferProof,
    expected_destination_epoch: ClusterEpoch,
) -> Result<(), ControlPlaneError> {
    write_pg_acting_set_request(out, pg_id, acting_set)?;
    write_u64(out, transfer.source_epoch().get());
    write_pg_metadata_proof(out, transfer.source_metadata_proof());
    write_pg_metadata_proof(out, transfer.metadata_proof());
    write_u64(out, expected_destination_epoch.get());
    Ok(())
}

fn read_pg_acting_set_with_metadata_transfer_request(
    reader: &mut PayloadReader<'_>,
) -> Result<(PgId, Vec<NodeId>, PgMetadataTransferProof, ClusterEpoch), ControlPlaneError> {
    let (pg_id, acting_set) = read_pg_acting_set_request(reader)?;
    let source_epoch = read_cluster_epoch(reader, "metadata transfer source epoch")?;
    let source_metadata_proof = read_pg_metadata_proof(reader)?;
    let imported_metadata_proof = read_pg_metadata_proof(reader)?;
    let expected_destination_epoch =
        read_cluster_epoch(reader, "metadata transfer destination epoch")?;
    Ok((
        pg_id,
        acting_set,
        PgMetadataTransferProof::new_with_imported_metadata_proof(
            source_epoch,
            source_metadata_proof,
            imported_metadata_proof,
        ),
        expected_destination_epoch,
    ))
}

fn write_heartbeat_lease_summary(out: &mut Vec<u8>, lease: &HeartbeatLease) {
    write_u64(out, lease.authority_incarnation().get());
    write_u64(out, lease.cluster_epoch().get());
    write_u32(out, lease.node_id().as_u32());
    write_u64(out, lease.lease_deadline_ms());
    write_u8(out, u8::from(lease.serving()));
}

fn read_heartbeat_lease_summary(
    reader: &mut PayloadReader<'_>,
) -> Result<HeartbeatLease, ControlPlaneError> {
    let authority_incarnation = AuthorityIncarnation::new(reader.read_u64()?).ok_or_else(|| {
        ControlPlaneError::rpc_protocol(
            "heartbeat lease authority incarnation must be nonzero".to_owned(),
        )
    })?;
    let cluster_epoch = read_cluster_epoch(reader, "heartbeat lease cluster epoch")?;
    let node_id = NodeId::new(reader.read_u32()?);
    let lease_deadline_ms = reader.read_u64()?;
    let serving = reader.read_bool()?;
    Ok(HeartbeatLease {
        authority_incarnation,
        cluster_epoch,
        node_id,
        lease_deadline_ms,
        serving,
        snapshot: ClusterControlSnapshot::empty(),
    })
}

fn write_runtime_map_status(
    out: &mut Vec<u8>,
    status: ControlPlaneRuntimeMapStatus,
) -> Result<(), ControlPlaneError> {
    write_u64(out, status.cluster_epoch().get());
    write_u32(
        out,
        len_as_u32(status.pg_routes(), "runtime map status PG routes")?,
    );
    write_u32(
        out,
        len_as_u32(
            status.active_serving_pg_routes(),
            "runtime map status active serving PG routes",
        )?,
    );
    match status.lease_renewal() {
        Some(renewal) => {
            write_u8(out, 1);
            out.extend_from_slice(&renewal.content_digest().as_bytes());
            let Some(valid_until_ms) = renewal.validity().valid_until_ms() else {
                return Err(ControlPlaneError::rpc_protocol(
                    "runtime map status renewal validity must be bounded".to_owned(),
                ));
            };
            if !renewal.freshness_proof().is_serving_authority_read() {
                return Err(ControlPlaneError::rpc_protocol(
                    "runtime map status renewal requires a serving-authority freshness proof"
                        .to_owned(),
                ));
            }
            write_u64(out, valid_until_ms);
            write_runtime_map_freshness_proof(out, &renewal.freshness_proof());
        }
        None => write_u8(out, 0),
    }
    Ok(())
}

fn read_runtime_map_status(
    reader: &mut PayloadReader<'_>,
) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
    let cluster_epoch = read_cluster_epoch(reader, "runtime map status cluster epoch")?;
    let pg_routes = reader.read_u32()? as usize;
    let active_serving_pg_routes = reader.read_u32()? as usize;
    let lease_renewal = match reader.read_u8()? {
        0 => None,
        1 => {
            let content_digest = RuntimeMapContentDigest::from_bytes(
                reader
                    .read_exact(RUNTIME_MAP_CONTENT_DIGEST_LEN)?
                    .try_into()
                    .expect("runtime-map digest read must return 32 bytes"),
            );
            let valid_until_ms = reader.read_u64()?;
            let validity =
                RouteMapValidity::from_valid_until_ms(Some(valid_until_ms)).ok_or_else(|| {
                    ControlPlaneError::rpc_protocol(
                        "runtime map status renewal validity uses reserved unbounded sentinel"
                            .to_owned(),
                    )
                })?;
            let freshness_proof = read_runtime_map_freshness_proof(reader)?;
            if !freshness_proof.is_serving_authority_read() {
                return Err(ControlPlaneError::rpc_protocol(
                    "runtime map status renewal requires a serving-authority freshness proof"
                        .to_owned(),
                ));
            }
            Some(ControlPlaneRuntimeMapLeaseRenewal {
                content_digest,
                validity,
                freshness_proof,
            })
        }
        value => {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "invalid runtime map status renewal tag {value}"
            )));
        }
    };
    Ok(ControlPlaneRuntimeMapStatus {
        cluster_epoch,
        pg_routes,
        active_serving_pg_routes,
        lease_renewal,
    })
}

fn write_control_plane_runtime_map_diagnostics(
    out: &mut Vec<u8>,
    diagnostics: &ControlPlaneRuntimeMapDiagnosticSnapshot,
) -> Result<(), ControlPlaneError> {
    let runtime_map = diagnostics.runtime_map();
    write_runtime_map_snapshot(out, runtime_map)?;
    let rpc_metrics = observability::control_plane_rpc_metrics_snapshot();
    write_u32(
        out,
        len_as_u32(rpc_metrics.len(), "control-plane RPC metric samples")?,
    );
    for sample in rpc_metrics {
        write_u8(out, control_plane_rpc_metric_kind_code(sample.kind));
        write_u64(out, sample.total);
        write_u64(out, sample.lock_wait_us_total);
        write_u64(out, sample.lock_wait_us_max);
        write_u64(out, sample.operation_us_total);
        write_u64(out, sample.operation_us_max);
        write_u64(out, sample.response_write_us_total);
        write_u64(out, sample.response_write_us_max);
        write_u64(out, sample.response_write_error_total);
        write_u64(out, sample.response_write_broken_pipe_total);
        write_u64(out, sample.response_write_connection_reset_total);
        write_u64(out, sample.response_write_timeout_total);
        write_u64(out, sample.response_write_other_error_total);
    }
    let snapshot = observability::control_plane_snapshot_metrics_snapshot();
    write_u64(out, snapshot.serialize_total);
    write_u64(out, snapshot.serialize_us_total);
    write_u64(out, snapshot.serialize_us_max);
    write_u64(out, snapshot.save_total);
    write_u64(out, snapshot.save_error_total);
    write_u64(out, snapshot.save_us_total);
    write_u64(out, snapshot.save_us_max);
    write_u64(out, snapshot.sync_total);
    write_u64(out, snapshot.sync_us_total);
    write_u64(out, snapshot.sync_us_max);
    write_u64(out, snapshot.bytes_total);
    write_u64(out, snapshot.bytes_last);
    write_u64(out, snapshot.bytes_max);
    let journal = observability::control_plane_journal_metrics_snapshot();
    write_u64(out, journal.append_total);
    write_u64(out, journal.append_error_total);
    write_u64(out, journal.append_us_total);
    write_u64(out, journal.append_us_max);
    write_u64(out, journal.lock_wait_us_total);
    write_u64(out, journal.lock_wait_us_max);
    write_u64(out, journal.frame_bytes_total);
    write_u64(out, journal.frame_bytes_last);
    write_u64(out, journal.frame_bytes_max);
    write_u64(out, journal.file_sync_total);
    write_u64(out, journal.file_sync_us_total);
    write_u64(out, journal.file_sync_us_max);
    write_u64(out, journal.directory_sync_total);
    write_u64(out, journal.directory_sync_us_total);
    write_u64(out, journal.directory_sync_us_max);
    write_u64(out, journal.compaction_total);
    write_u64(out, journal.compaction_error_total);
    write_u64(out, journal.compaction_us_total);
    write_u64(out, journal.compaction_us_max);
    write_u64(out, journal.compaction_lock_wait_us_total);
    write_u64(out, journal.compaction_lock_wait_us_max);
    write_u64(out, journal.compaction_bytes_total);
    write_u64(out, journal.compaction_bytes_last);
    write_u64(out, journal.compaction_bytes_max);
    write_u64(out, journal.compaction_file_sync_total);
    write_u64(out, journal.compaction_file_sync_us_total);
    write_u64(out, journal.compaction_file_sync_us_max);
    write_u64(out, journal.compaction_directory_sync_total);
    write_u64(out, journal.compaction_directory_sync_us_total);
    write_u64(out, journal.compaction_directory_sync_us_max);
    let raft_checkpoint = observability::control_plane_raft_checkpoint_metrics_snapshot();
    write_u64(out, raft_checkpoint.encode_total);
    write_u64(out, raft_checkpoint.encode_us_total);
    write_u64(out, raft_checkpoint.encode_us_max);
    write_u64(out, raft_checkpoint.store_total);
    write_u64(out, raft_checkpoint.store_error_total);
    write_u64(out, raft_checkpoint.store_us_total);
    write_u64(out, raft_checkpoint.store_us_max);
    write_u64(out, raft_checkpoint.file_sync_total);
    write_u64(out, raft_checkpoint.file_sync_us_total);
    write_u64(out, raft_checkpoint.file_sync_us_max);
    write_u64(out, raft_checkpoint.directory_sync_total);
    write_u64(out, raft_checkpoint.directory_sync_us_total);
    write_u64(out, raft_checkpoint.directory_sync_us_max);
    write_u64(out, raft_checkpoint.bytes_total);
    write_u64(out, raft_checkpoint.bytes_last);
    write_u64(out, raft_checkpoint.bytes_max);
    write_u64(out, raft_checkpoint.compaction_total);
    write_u64(out, raft_checkpoint.compaction_error_total);
    write_u64(out, raft_checkpoint.compaction_us_total);
    write_u64(out, raft_checkpoint.compaction_us_max);
    let raft_wal = observability::control_plane_raft_wal_metrics_snapshot();
    write_u64(out, raft_wal.append_total);
    write_u64(out, raft_wal.append_error_total);
    write_u64(out, raft_wal.append_us_total);
    write_u64(out, raft_wal.append_us_max);
    write_u64(out, raft_wal.lock_wait_us_total);
    write_u64(out, raft_wal.lock_wait_us_max);
    write_u64(out, raft_wal.frame_bytes_total);
    write_u64(out, raft_wal.frame_bytes_last);
    write_u64(out, raft_wal.frame_bytes_max);
    write_u64(out, raft_wal.file_sync_total);
    write_u64(out, raft_wal.file_sync_us_total);
    write_u64(out, raft_wal.file_sync_us_max);
    write_u64(out, raft_wal.directory_sync_total);
    write_u64(out, raft_wal.directory_sync_us_total);
    write_u64(out, raft_wal.directory_sync_us_max);
    write_u64(out, raft_wal.durability_queue_depth);
    write_u64(out, raft_wal.durability_queue_depth_max);
    write_u64(out, raft_wal.durability_queue_wait_us_total);
    write_u64(out, raft_wal.durability_queue_wait_us_max);
    write_u64(out, raft_wal.append_accept_us_total);
    write_u64(out, raft_wal.append_accept_us_max);
    write_u64(out, raft_wal.durability_operation_us_total);
    write_u64(out, raft_wal.durability_operation_us_max);
    let raft_command = observability::control_plane_raft_command_metrics_snapshot();
    write_u64(out, raft_command.submit_total);
    write_u64(out, raft_command.submit_error_total);
    write_u64(out, raft_command.queue_wait_us_total);
    write_u64(out, raft_command.queue_wait_us_max);
    write_u64(out, raft_command.operation_us_total);
    write_u64(out, raft_command.operation_us_max);
    let runtime_node_ids = runtime_map
        .nodes()
        .iter()
        .map(|node| node.node_id().as_u32())
        .collect::<BTreeSet<_>>();
    let history_reference_samples = observability::control_plane_history_reference_samples()
        .into_iter()
        .filter(|sample| runtime_node_ids.contains(&sample.node_id))
        .collect::<Vec<_>>();
    write_u32(
        out,
        len_as_u32(
            history_reference_samples.len(),
            "control-plane history reference samples",
        )?,
    );
    for sample in history_reference_samples {
        write_u32(out, sample.node_id);
        write_u64(out, sample.observed_epoch);
        write_u64(out, sample.validation_epoch);
        write_u64(out, sample.observed_at_ms);
        write_option_u64(out, sample.oldest_live_placement_epoch);
        write_option_u64(out, sample.oldest_durable_backfill_epoch);
        write_option_u64(out, sample.oldest_pending_metadata_command_epoch);
        write_option_u64(out, sample.oldest_object_payload_reclaim_claim_epoch);
    }
    write_u32(
        out,
        len_as_u32(
            diagnostics.node_leases().len(),
            "control-plane diagnostic node leases",
        )?,
    );
    for node_lease in diagnostics.node_leases() {
        write_u32(out, node_lease.node_id().as_u32());
        write_option_u64(out, node_lease.lease_deadline_ms());
    }
    Ok(())
}

fn read_control_plane_runtime_map_diagnostics(
    reader: &mut PayloadReader<'_>,
) -> Result<ControlPlaneRuntimeMapDiagnostics, ControlPlaneError> {
    let runtime_map = read_runtime_map_snapshot(reader)?;
    let metric_count = reader.read_collection_len("control-plane RPC metrics", 97)?;
    if metric_count > observability::ControlPlaneRpcMetricKind::COUNT {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "control-plane RPC metric count {metric_count} exceeds {}",
            observability::ControlPlaneRpcMetricKind::COUNT
        )));
    }
    let mut rpc_metrics = Vec::with_capacity(metric_count);
    let mut seen = BTreeSet::new();
    for _ in 0..metric_count {
        let kind = read_control_plane_rpc_metric_kind(reader.read_u8()?)?;
        if !seen.insert(control_plane_rpc_metric_kind_code(kind)) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "duplicate control-plane RPC metric kind {}",
                kind.as_str()
            )));
        }
        rpc_metrics.push(observability::ControlPlaneRpcMetricSample {
            kind,
            total: reader.read_u64()?,
            lock_wait_us_total: reader.read_u64()?,
            lock_wait_us_max: reader.read_u64()?,
            operation_us_total: reader.read_u64()?,
            operation_us_max: reader.read_u64()?,
            response_write_us_total: reader.read_u64()?,
            response_write_us_max: reader.read_u64()?,
            response_write_error_total: reader.read_u64()?,
            response_write_broken_pipe_total: reader.read_u64()?,
            response_write_connection_reset_total: reader.read_u64()?,
            response_write_timeout_total: reader.read_u64()?,
            response_write_other_error_total: reader.read_u64()?,
        });
    }
    let snapshot_metrics = observability::ControlPlaneSnapshotMetricSnapshot {
        serialize_total: reader.read_u64()?,
        serialize_us_total: reader.read_u64()?,
        serialize_us_max: reader.read_u64()?,
        save_total: reader.read_u64()?,
        save_error_total: reader.read_u64()?,
        save_us_total: reader.read_u64()?,
        save_us_max: reader.read_u64()?,
        sync_total: reader.read_u64()?,
        sync_us_total: reader.read_u64()?,
        sync_us_max: reader.read_u64()?,
        bytes_total: reader.read_u64()?,
        bytes_last: reader.read_u64()?,
        bytes_max: reader.read_u64()?,
    };
    let journal_metrics = observability::ControlPlaneJournalMetricSnapshot {
        append_total: reader.read_u64()?,
        append_error_total: reader.read_u64()?,
        append_us_total: reader.read_u64()?,
        append_us_max: reader.read_u64()?,
        lock_wait_us_total: reader.read_u64()?,
        lock_wait_us_max: reader.read_u64()?,
        frame_bytes_total: reader.read_u64()?,
        frame_bytes_last: reader.read_u64()?,
        frame_bytes_max: reader.read_u64()?,
        file_sync_total: reader.read_u64()?,
        file_sync_us_total: reader.read_u64()?,
        file_sync_us_max: reader.read_u64()?,
        directory_sync_total: reader.read_u64()?,
        directory_sync_us_total: reader.read_u64()?,
        directory_sync_us_max: reader.read_u64()?,
        compaction_total: reader.read_u64()?,
        compaction_error_total: reader.read_u64()?,
        compaction_us_total: reader.read_u64()?,
        compaction_us_max: reader.read_u64()?,
        compaction_lock_wait_us_total: reader.read_u64()?,
        compaction_lock_wait_us_max: reader.read_u64()?,
        compaction_bytes_total: reader.read_u64()?,
        compaction_bytes_last: reader.read_u64()?,
        compaction_bytes_max: reader.read_u64()?,
        compaction_file_sync_total: reader.read_u64()?,
        compaction_file_sync_us_total: reader.read_u64()?,
        compaction_file_sync_us_max: reader.read_u64()?,
        compaction_directory_sync_total: reader.read_u64()?,
        compaction_directory_sync_us_total: reader.read_u64()?,
        compaction_directory_sync_us_max: reader.read_u64()?,
    };
    let raft_checkpoint_metrics = observability::ControlPlaneRaftCheckpointMetricSnapshot {
        encode_total: reader.read_u64()?,
        encode_us_total: reader.read_u64()?,
        encode_us_max: reader.read_u64()?,
        store_total: reader.read_u64()?,
        store_error_total: reader.read_u64()?,
        store_us_total: reader.read_u64()?,
        store_us_max: reader.read_u64()?,
        file_sync_total: reader.read_u64()?,
        file_sync_us_total: reader.read_u64()?,
        file_sync_us_max: reader.read_u64()?,
        directory_sync_total: reader.read_u64()?,
        directory_sync_us_total: reader.read_u64()?,
        directory_sync_us_max: reader.read_u64()?,
        bytes_total: reader.read_u64()?,
        bytes_last: reader.read_u64()?,
        bytes_max: reader.read_u64()?,
        compaction_total: reader.read_u64()?,
        compaction_error_total: reader.read_u64()?,
        compaction_us_total: reader.read_u64()?,
        compaction_us_max: reader.read_u64()?,
    };
    let raft_wal_metrics = observability::ControlPlaneRaftWalMetricSnapshot {
        append_total: reader.read_u64()?,
        append_error_total: reader.read_u64()?,
        append_us_total: reader.read_u64()?,
        append_us_max: reader.read_u64()?,
        lock_wait_us_total: reader.read_u64()?,
        lock_wait_us_max: reader.read_u64()?,
        frame_bytes_total: reader.read_u64()?,
        frame_bytes_last: reader.read_u64()?,
        frame_bytes_max: reader.read_u64()?,
        file_sync_total: reader.read_u64()?,
        file_sync_us_total: reader.read_u64()?,
        file_sync_us_max: reader.read_u64()?,
        directory_sync_total: reader.read_u64()?,
        directory_sync_us_total: reader.read_u64()?,
        directory_sync_us_max: reader.read_u64()?,
        durability_queue_depth: reader.read_u64()?,
        durability_queue_depth_max: reader.read_u64()?,
        durability_queue_wait_us_total: reader.read_u64()?,
        durability_queue_wait_us_max: reader.read_u64()?,
        append_accept_us_total: reader.read_u64()?,
        append_accept_us_max: reader.read_u64()?,
        durability_operation_us_total: reader.read_u64()?,
        durability_operation_us_max: reader.read_u64()?,
    };
    let raft_command_metrics = observability::ControlPlaneRaftCommandMetricSnapshot {
        submit_total: reader.read_u64()?,
        submit_error_total: reader.read_u64()?,
        queue_wait_us_total: reader.read_u64()?,
        queue_wait_us_max: reader.read_u64()?,
        operation_us_total: reader.read_u64()?,
        operation_us_max: reader.read_u64()?,
    };
    let history_reference_count =
        reader.read_collection_len("control-plane history reference samples", 31)?;
    if history_reference_count > runtime_map.nodes().len() {
        return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane history reference sample count {history_reference_count} exceeds runtime node count {}",
                runtime_map.nodes().len()
            )));
    }
    let runtime_node_ids = runtime_map
        .nodes()
        .iter()
        .map(|node| node.node_id().as_u32())
        .collect::<BTreeSet<_>>();
    let mut history_reference_samples = Vec::with_capacity(history_reference_count);
    let mut seen_nodes = BTreeSet::new();
    for _ in 0..history_reference_count {
        let node_id = reader.read_u32()?;
        if !runtime_node_ids.contains(&node_id) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane history reference sample names unknown node {node_id}"
            )));
        }
        if !seen_nodes.insert(node_id) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "duplicate control-plane history reference sample for node {node_id}"
            )));
        }
        let observed_epoch = read_cluster_epoch(reader, "history reference observed epoch")?;
        let validation_epoch = read_cluster_epoch(reader, "history reference validation epoch")?;
        let observed_at_ms = reader.read_u64()?;
        let oldest_live_placement_epoch =
            read_option_cluster_epoch(reader, "history reference live placement epoch")?;
        let oldest_durable_backfill_epoch =
            read_option_cluster_epoch(reader, "history reference durable backfill epoch")?;
        let oldest_pending_metadata_command_epoch =
            read_option_cluster_epoch(reader, "history reference pending metadata command epoch")?;
        let oldest_object_payload_reclaim_claim_epoch = read_option_cluster_epoch(
            reader,
            "history reference object payload reclaim claim epoch",
        )?;
        if observed_epoch > validation_epoch {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "history reference sample for node {node_id} observed epoch {observed_epoch} beyond validation epoch {validation_epoch}"
                )));
        }
        if validation_epoch > runtime_map.cluster_epoch() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "history reference sample for node {node_id} has future validation epoch {validation_epoch} beyond runtime-map epoch {}",
                    runtime_map.cluster_epoch()
                )));
        }
        for (field, component_epoch) in [
            ("live placement", oldest_live_placement_epoch),
            ("durable backfill", oldest_durable_backfill_epoch),
            (
                "pending metadata command",
                oldest_pending_metadata_command_epoch,
            ),
            (
                "object payload reclaim claim",
                oldest_object_payload_reclaim_claim_epoch,
            ),
        ] {
            let Some(component_epoch) = component_epoch else {
                continue;
            };
            if component_epoch > validation_epoch {
                return Err(ControlPlaneError::rpc_protocol(format!(
                        "history reference sample for node {node_id} has future {field} epoch {component_epoch} beyond validation epoch {validation_epoch}"
                    )));
            }
        }
        history_reference_samples.push(observability::ControlPlaneHistoryReferenceSample {
            node_id,
            observed_epoch: observed_epoch.get(),
            validation_epoch: validation_epoch.get(),
            observed_at_ms,
            oldest_live_placement_epoch: oldest_live_placement_epoch.map(ClusterEpoch::get),
            oldest_durable_backfill_epoch: oldest_durable_backfill_epoch.map(ClusterEpoch::get),
            oldest_pending_metadata_command_epoch: oldest_pending_metadata_command_epoch
                .map(ClusterEpoch::get),
            oldest_object_payload_reclaim_claim_epoch: oldest_object_payload_reclaim_claim_epoch
                .map(ClusterEpoch::get),
        });
    }
    let node_lease_count = reader.read_collection_len(
        "control-plane diagnostic node leases",
        CONTROL_PLANE_RPC_NODE_LEASE_DIAGNOSTIC_MIN_LEN,
    )?;
    if node_lease_count != runtime_map.nodes().len() {
        return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane diagnostic node lease count {node_lease_count} does not match runtime node count {}",
                runtime_map.nodes().len()
            )));
    }
    let mut node_leases = Vec::with_capacity(node_lease_count);
    for expected_node in runtime_map.nodes() {
        let node_id = NodeId::new(reader.read_u32()?);
        if node_id != expected_node.node_id() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane diagnostic node lease names node {}, expected canonical node {}",
                node_id.as_u32(),
                expected_node.node_id().as_u32()
            )));
        }
        node_leases.push(ControlPlaneRuntimeMapNodeLeaseDiagnostic {
            node_id,
            lease_deadline_ms: reader.read_option_u64()?,
        });
    }
    Ok(ControlPlaneRuntimeMapDiagnostics {
        runtime_map,
        rpc_metrics,
        snapshot_metrics,
        journal_metrics,
        raft_checkpoint_metrics,
        raft_wal_metrics,
        raft_command_metrics,
        history_reference_samples,
        node_leases,
    })
}

fn control_plane_rpc_metric_kind_code(kind: observability::ControlPlaneRpcMetricKind) -> u8 {
    use observability::ControlPlaneRpcMetricKind as Kind;
    match kind {
        Kind::RuntimeMapSnapshot => 1,
        Kind::RefreshNodeHeartbeat => 2,
        Kind::SetPgActingSet => 3,
        Kind::SetPgActingSetWithMetadataTransfer => 4,
        Kind::SetPgActingSetWithMetadataTransferRuntimeMap => 5,
        Kind::FencePgForMetadataTransferRuntimeMap => 6,
        Kind::TransferRaftLeadership => 7,
        Kind::PgRuntimeMapSnapshot => 8,
        Kind::TriggerRaftSnapshotAndPurge => 9,
        Kind::TriggerRaftElection => 10,
        Kind::RuntimeMapStatus => 11,
        Kind::PendingMetadataCommandRecoveries => 12,
        Kind::AuthorityClockStatus => 13,
        Kind::ReestablishAuthorityClock => 14,
        Kind::RuntimeMapDiagnostics => 15,
        Kind::Unknown => 16,
        Kind::ServingPgRuntimeMapSnapshot => 17,
    }
}

fn read_control_plane_rpc_metric_kind(
    code: u8,
) -> Result<observability::ControlPlaneRpcMetricKind, ControlPlaneError> {
    use observability::ControlPlaneRpcMetricKind as Kind;
    match code {
        1 => Ok(Kind::RuntimeMapSnapshot),
        2 => Ok(Kind::RefreshNodeHeartbeat),
        3 => Ok(Kind::SetPgActingSet),
        4 => Ok(Kind::SetPgActingSetWithMetadataTransfer),
        5 => Ok(Kind::SetPgActingSetWithMetadataTransferRuntimeMap),
        6 => Ok(Kind::FencePgForMetadataTransferRuntimeMap),
        7 => Ok(Kind::TransferRaftLeadership),
        8 => Ok(Kind::PgRuntimeMapSnapshot),
        9 => Ok(Kind::TriggerRaftSnapshotAndPurge),
        10 => Ok(Kind::TriggerRaftElection),
        11 => Ok(Kind::RuntimeMapStatus),
        12 => Ok(Kind::PendingMetadataCommandRecoveries),
        13 => Ok(Kind::AuthorityClockStatus),
        14 => Ok(Kind::ReestablishAuthorityClock),
        15 => Ok(Kind::RuntimeMapDiagnostics),
        16 => Ok(Kind::Unknown),
        17 => Ok(Kind::ServingPgRuntimeMapSnapshot),
        _ => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid control-plane RPC metric kind {code}"
        ))),
    }
}

fn write_pending_metadata_command_recovery_listing(
    out: &mut Vec<u8>,
    listing: &PendingMetadataCommandRecoveryListing,
) -> Result<(), ControlPlaneError> {
    write_u32(
        out,
        len_as_u32(listing.tasks().len(), "pending metadata command recoveries")?,
    );
    for task in listing.tasks() {
        write_u32(out, task.pg_id().get());
        write_u32(out, task.recovery().reporting_node_id().as_u32());
        write_u64(out, task.recovery().pending().cluster_epoch().get());
        write_u64(out, task.recovery().pending().log_index());
        write_u64(out, task.recovery().pending().command_checksum());
    }
    write_u32(
        out,
        len_as_u32(
            listing.failures().len(),
            "pending metadata command recovery discovery failures",
        )?,
    );
    for failure in listing.failures() {
        write_u32(out, failure.pg_id().get());
        out.push(failure.kind().as_u8());
        write_string(out, failure.detail())?;
    }
    Ok(())
}

fn read_pending_metadata_command_recovery_listing(
    reader: &mut PayloadReader<'_>,
) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
    let count = reader.read_collection_len(
        "pending metadata command recoveries",
        CONTROL_PLANE_RPC_PENDING_RECOVERY_TASK_MIN_LEN,
    )?;
    let mut tasks = Vec::with_capacity(count);
    let mut pg_ids = BTreeSet::new();
    for _ in 0..count {
        let pg_id = PgId::new(reader.read_u32()?);
        if !pg_ids.insert(pg_id) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "pending metadata command recoveries repeat PG {}",
                pg_id.get()
            )));
        }
        let reporting_node_id = NodeId::new(reader.read_u32()?);
        let pending = PendingMetadataCommandObservation::new(
            read_cluster_epoch(reader, "pending recovery command epoch")?,
            NonZeroU64::new(reader.read_u64()?).ok_or_else(|| {
                ControlPlaneError::rpc_protocol(
                    "pending recovery command log index must be nonzero".to_owned(),
                )
            })?,
            reader.read_u64()?,
        );
        tasks.push(PendingMetadataCommandRecoveryTask::new(
            pg_id,
            PendingMetadataCommandRecovery::new(reporting_node_id, pending),
        ));
    }
    let failure_count = reader.read_collection_len(
        "pending metadata command recovery discovery failures",
        CONTROL_PLANE_RPC_PENDING_RECOVERY_FAILURE_MIN_LEN,
    )?;
    let mut failures = Vec::with_capacity(failure_count);
    for _ in 0..failure_count {
        let pg_id = PgId::new(reader.read_u32()?);
        if !pg_ids.insert(pg_id) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "pending metadata command recovery listing repeats PG {}",
                pg_id.get()
            )));
        }
        let kind = PendingMetadataCommandRecoveryDiscoveryFailureKind::from_u8(reader.read_u8()?)?;
        let detail = reader.read_string()?.to_owned();
        failures.push(PendingMetadataCommandRecoveryDiscoveryFailure::new(
            pg_id, kind, detail,
        ));
    }
    Ok(PendingMetadataCommandRecoveryListing::new(tasks, failures))
}

fn write_authority_clock_blocked_reason(
    out: &mut Vec<u8>,
    reason: Option<ControlPlaneAuthorityClockBlockedReason>,
) {
    write_u8(
        out,
        match reason {
            None => 0,
            Some(ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity) => 1,
            Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged) => 2,
            Some(ControlPlaneAuthorityClockBlockedReason::ClockSourceUnavailable) => 3,
            Some(ControlPlaneAuthorityClockBlockedReason::ClockHealthRegression) => 4,
            Some(ControlPlaneAuthorityClockBlockedReason::WallClockRegression) => 5,
            Some(ControlPlaneAuthorityClockBlockedReason::WallClockForwardJump) => 6,
            Some(ControlPlaneAuthorityClockBlockedReason::CheckpointPersistenceFailure) => 7,
        },
    );
}

fn read_authority_clock_blocked_reason(
    reader: &mut PayloadReader<'_>,
) -> Result<Option<ControlPlaneAuthorityClockBlockedReason>, ControlPlaneError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity,
        )),
        2 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged,
        )),
        3 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::ClockSourceUnavailable,
        )),
        4 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::ClockHealthRegression,
        )),
        5 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::WallClockRegression,
        )),
        6 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::WallClockForwardJump,
        )),
        7 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::CheckpointPersistenceFailure,
        )),
        tag => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid authority-clock blocked-reason tag {tag}"
        ))),
    }
}

fn write_authority_clock_status(out: &mut Vec<u8>, status: ControlPlaneAuthorityClockStatus) {
    write_u64(out, status.generation());
    write_u8(out, u8::from(status.established()));
    write_authority_clock_blocked_reason(out, status.blocked_reason());
    write_option_u64(out, status.committed_timestamp_high_water_ms());
    write_option_u64(out, status.bound_raft_leadership_term());
    write_option_u64(out, status.current_raft_leadership_term());
    write_u8(out, u8::from(status.local_raft_authority_leader()));
    write_u8(out, u8::from(status.local_raft_authority_serving()));
}

fn read_authority_clock_status(
    reader: &mut PayloadReader<'_>,
) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
    Ok(ControlPlaneAuthorityClockStatus {
        generation: reader.read_u64()?,
        established: reader.read_bool()?,
        blocked_reason: read_authority_clock_blocked_reason(reader)?,
        committed_timestamp_high_water_ms: reader.read_option_u64()?,
        bound_raft_leadership_term: reader.read_option_u64()?,
        current_raft_leadership_term: reader.read_option_u64()?,
        local_raft_authority_leader: reader.read_bool()?,
        local_raft_authority_serving: reader.read_bool()?,
    })
}

fn write_runtime_map_snapshot(
    out: &mut Vec<u8>,
    snapshot: &ClusterRuntimeMapSnapshot,
) -> Result<(), ControlPlaneError> {
    write_u64(out, snapshot.cluster_epoch().get());
    write_option_u64(out, snapshot.valid_until_ms());
    write_runtime_map_freshness_proof(out, snapshot.freshness_proof());
    write_u32(out, len_as_u32(snapshot.nodes().len(), "runtime nodes")?);
    for node in snapshot.nodes() {
        write_u32(out, node.node_id().as_u32());
        write_u64(out, node.node_incarnation());
        write_string(out, node.endpoint())?;
        write_option_u64(
            out,
            node.cluster_map_history_floor_epoch()
                .map(ClusterEpoch::get),
        );
    }
    write_pg_route_snapshots(out, "PG routes", snapshot.pg_routes())?;
    write_pg_route_snapshots(out, "historical PG routes", snapshot.historical_pg_routes())?;
    write_u32(
        out,
        len_as_u32(
            snapshot.historical_cluster_epochs().len(),
            "historical cluster epochs",
        )?,
    );
    for epoch in snapshot.historical_cluster_epochs() {
        write_u64(out, epoch.get());
    }
    Ok(())
}

fn write_runtime_map_freshness_proof(out: &mut Vec<u8>, proof: &RuntimeMapFreshnessProof) {
    match proof {
        RuntimeMapFreshnessProof::SingleAuthority {
            authority_incarnation,
            issued_at_ms,
        } => {
            write_u8(out, CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_SINGLE_AUTHORITY);
            write_u64(out, authority_incarnation.get());
            write_u64(out, *issued_at_ms);
        }
        RuntimeMapFreshnessProof::ReadIndex {
            authority_incarnation,
            read_index,
            issued_at_ms,
        } => {
            write_u8(out, CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_READ_INDEX);
            write_u64(out, authority_incarnation.get());
            write_u64(out, read_index.term());
            write_u64(out, read_index.index());
            write_u64(out, *issued_at_ms);
        }
        RuntimeMapFreshnessProof::Reconstructed {
            authority_incarnation,
        } => {
            write_u8(out, CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_RECONSTRUCTED);
            write_u64(out, authority_incarnation.get());
        }
    }
}

fn write_pg_route_snapshots(
    out: &mut Vec<u8>,
    label: &'static str,
    routes: &[PgRouteSnapshot],
) -> Result<(), ControlPlaneError> {
    write_u32(out, len_as_u32(routes.len(), label)?);
    for route in routes {
        write_u64(out, route.cluster_epoch().get());
        write_u32(out, route.pg_id().get());
        write_u32(out, route.primary_node_id().as_u32());
        write_pg_state(out, route.state());
        match route.active_metadata_proof() {
            Some(proof) => {
                write_u8(out, 1);
                write_pg_metadata_proof(out, proof);
            }
            None => write_u8(out, 0),
        }
        match route.metadata_read_route() {
            Some(read_route) => {
                write_u8(out, 1);
                write_u32(out, read_route.node_id().as_u32());
                write_pg_metadata_proof(out, read_route.proof());
            }
            None => write_u8(out, 0),
        }
        write_option_u64(out, route.primary_lease_deadline_ms());
        match route.peering_metadata_transfer() {
            Some(transfer) => {
                write_u8(out, 1);
                write_u64(out, transfer.source_epoch().get());
                write_pg_metadata_proof(out, transfer.source_metadata_proof());
                write_pg_metadata_proof(out, transfer.metadata_proof());
                write_option_u64(
                    out,
                    route
                        .peering_metadata_transfer_destination_epoch()
                        .map(ClusterEpoch::get),
                );
                write_option_u64(
                    out,
                    route
                        .peering_metadata_transfer_source_route_epoch()
                        .map(ClusterEpoch::get),
                );
                write_option_u32(
                    out,
                    route
                        .peering_metadata_transfer_source_node_id()
                        .map(NodeId::as_u32),
                );
            }
            None => write_u8(out, 0),
        }
        match route.pending_metadata_command_recovery() {
            Some(recovery) => {
                write_u8(out, 1);
                write_u32(out, recovery.reporting_node_id().as_u32());
                write_u64(out, recovery.pending().cluster_epoch().get());
                write_u64(out, recovery.pending().log_index());
                write_u64(out, recovery.pending().command_checksum());
            }
            None => write_u8(out, 0),
        }
        write_u32(out, len_as_u32(route.acting_set().len(), "acting set")?);
        for node_id in route.acting_set() {
            write_u32(out, node_id.as_u32());
        }
    }
    Ok(())
}

fn read_runtime_map_snapshot(
    reader: &mut PayloadReader<'_>,
) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
    let cluster_epoch = read_cluster_epoch(reader, "runtime map cluster epoch")?;
    let valid_until_ms = reader.read_option_u64()?;
    let freshness_proof = read_runtime_map_freshness_proof(reader)?;
    let node_count =
        reader.read_collection_len("runtime nodes", CONTROL_PLANE_RPC_RUNTIME_NODE_MIN_LEN)?;
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        nodes.push(NodeRouteSnapshot {
            node_id: NodeId::new(reader.read_u32()?),
            node_incarnation: reader.read_u64()?,
            endpoint: reader.read_string()?.to_owned(),
            cluster_map_history_floor_epoch: read_option_cluster_epoch(
                reader,
                "runtime node cluster-map history floor epoch",
            )?,
        });
    }
    let pg_routes = read_pg_route_snapshots(reader, "PG routes")?;
    let historical_pg_routes = read_pg_route_snapshots(reader, "historical PG routes")?;
    let historical_epoch_count =
        reader.read_collection_len("historical cluster epochs", std::mem::size_of::<u64>())?;
    let mut historical_cluster_epochs = Vec::with_capacity(historical_epoch_count);
    for _ in 0..historical_epoch_count {
        historical_cluster_epochs.push(read_cluster_epoch(
            reader,
            "runtime map historical cluster epoch",
        )?);
    }
    let Some(valid_until_ms) = valid_until_ms else {
        return Err(ControlPlaneError::rpc_protocol(
            "runtime map validity must be bounded on the wire".to_owned(),
        ));
    };
    let snapshot = ClusterRuntimeMapSnapshot {
        cluster_epoch,
        validity: RouteMapValidity::from_valid_until_ms(Some(valid_until_ms)).ok_or_else(|| {
            ControlPlaneError::rpc_protocol(
                "runtime map validity deadline uses reserved unbounded sentinel".to_owned(),
            )
        })?,
        freshness_proof,
        nodes,
        pg_routes,
        historical_pg_routes,
        historical_cluster_epochs,
    };
    validate_runtime_map_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn validate_runtime_map_snapshot(
    snapshot: &ClusterRuntimeMapSnapshot,
) -> Result<(), ControlPlaneError> {
    let mut previous_historical_epoch = None;
    for epoch in snapshot.historical_cluster_epochs() {
        if *epoch >= snapshot.cluster_epoch() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map historical epoch {} is not older than current epoch {}",
                epoch.get(),
                snapshot.cluster_epoch().get()
            )));
        }
        if previous_historical_epoch.is_some_and(|previous| previous >= *epoch) {
            return Err(ControlPlaneError::rpc_protocol(
                "runtime map historical epochs are not strictly increasing".to_owned(),
            ));
        }
        previous_historical_epoch = Some(*epoch);
    }
    let mut node_ids = BTreeSet::new();
    for node in snapshot.nodes() {
        if !node_ids.insert(node.node_id()) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map contains duplicate node {}",
                node.node_id().as_u32()
            )));
        }
        if node.endpoint().is_empty() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map node {} has an empty endpoint",
                node.node_id().as_u32()
            )));
        }
    }

    validate_runtime_map_routes(
        snapshot.cluster_epoch(),
        &node_ids,
        "runtime map",
        true,
        snapshot.pg_routes(),
    )?;
    validate_runtime_map_routes(
        snapshot.cluster_epoch(),
        &node_ids,
        "runtime map historical",
        false,
        snapshot.historical_pg_routes(),
    )?;
    for route in snapshot.historical_pg_routes() {
        if snapshot
            .historical_cluster_epochs()
            .binary_search(&route.cluster_epoch())
            .is_err()
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map historical route epoch {} is not retained",
                route.cluster_epoch().get()
            )));
        }
    }
    for route in snapshot.pg_routes() {
        let Some(recovery) = route.pending_metadata_command_recovery() else {
            continue;
        };
        if route.state() != PgState::Peering {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map route for PG {} authorizes pending command recovery while {:?}",
                route.pg_id().get(),
                route.state()
            )));
        }
        if recovery.pending().cluster_epoch() >= snapshot.cluster_epoch() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "runtime map route for PG {} pending command epoch {} is not older than current epoch {}",
                    route.pg_id().get(),
                    recovery.pending().cluster_epoch().get(),
                    snapshot.cluster_epoch().get()
                )));
        }
        let historical = snapshot
            .reconstructed_pg_route_at_epoch(route.pg_id(), recovery.pending().cluster_epoch())?;
        if historical.state() != PgState::Active
            || historical.primary_node_id() != recovery.reporting_node_id()
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "runtime map route for PG {} pending command recovery reporter {} is not the Active historical primary at epoch {}",
                    route.pg_id().get(),
                    recovery.reporting_node_id().as_u32(),
                    recovery.pending().cluster_epoch().get()
                )));
        }
    }
    validate_runtime_map_transfer_sources(snapshot)
}

fn validate_runtime_map_transfer_sources(
    snapshot: &ClusterRuntimeMapSnapshot,
) -> Result<(), ControlPlaneError> {
    for route in snapshot
        .pg_routes()
        .iter()
        .chain(snapshot.historical_pg_routes())
    {
        let Some(source_route_epoch) = route.peering_metadata_transfer_source_route_epoch() else {
            continue;
        };
        let source_node_id = route
            .peering_metadata_transfer_source_node_id()
            .expect("metadata transfer source route fields validated as complete");
        if source_route_epoch >= route.cluster_epoch() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "runtime map route for PG {} metadata transfer source route epoch {} must be older than transfer route epoch {}",
                    route.pg_id().get(),
                    source_route_epoch.get(),
                    route.cluster_epoch().get()
                )));
        }
        let source_route =
            runtime_map_route_at_epoch(snapshot, route.pg_id(), source_route_epoch).ok_or_else(
                || ControlPlaneError::rpc_protocol(format!(
                        "runtime map route for PG {} references missing metadata transfer source route epoch {}",
                        route.pg_id().get(),
                        source_route_epoch.get()
                    )),
            )?;
        if source_route.primary_node_id() != source_node_id {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "runtime map route for PG {} metadata transfer source node {} does not match source route primary {} at epoch {}",
                    route.pg_id().get(),
                    source_node_id.as_u32(),
                    source_route.primary_node_id().as_u32(),
                    source_route_epoch.get()
                )));
        }
    }
    Ok(())
}

fn runtime_map_route_at_epoch(
    snapshot: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
) -> Option<PgRouteSnapshot> {
    if cluster_epoch == snapshot.cluster_epoch() {
        snapshot
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == pg_id)
            .map(PgRouteSnapshot::without_serving_authority)
    } else {
        snapshot
            .reconstructed_pg_route_at_epoch(pg_id, cluster_epoch)
            .ok()
    }
}

fn validate_runtime_map_routes(
    current_cluster_epoch: ClusterEpoch,
    node_ids: &BTreeSet<NodeId>,
    label: &'static str,
    is_current_route_set: bool,
    routes: &[PgRouteSnapshot],
) -> Result<(), ControlPlaneError> {
    let mut seen_routes = BTreeSet::new();
    for route in routes {
        let route_key = (route.cluster_epoch(), route.pg_id());
        if !seen_routes.insert(route_key) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} contains duplicate route for PG {} at epoch {}",
                route.pg_id().get(),
                route.cluster_epoch().get()
            )));
        }
        if is_current_route_set && route.cluster_epoch() != current_cluster_epoch {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map route for PG {} has epoch {}, expected {}",
                route.pg_id().get(),
                route.cluster_epoch().get(),
                current_cluster_epoch.get()
            )));
        }
        if !is_current_route_set && route.cluster_epoch() >= current_cluster_epoch {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has epoch {}, expected an epoch older than {}",
                route.pg_id().get(),
                route.cluster_epoch().get(),
                current_cluster_epoch.get()
            )));
        }
        if route.acting_set().is_empty() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has an empty acting set",
                route.pg_id().get()
            )));
        }
        let mut acting_set = BTreeSet::new();
        for &node_id in route.acting_set() {
            if !acting_set.insert(node_id) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} repeats acting-set node {}",
                    route.pg_id().get(),
                    node_id.as_u32()
                )));
            }
            if !node_ids.contains(&node_id) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} references unknown acting-set node {}",
                    route.pg_id().get(),
                    node_id.as_u32()
                )));
            }
        }
        if !acting_set.contains(&route.primary_node_id()) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} primary {} is outside the acting set",
                route.pg_id().get(),
                route.primary_node_id().as_u32()
            )));
        }
        if route.primary_lease_deadline_ms().is_some() && route.state() != PgState::Active {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has serving authority but is {:?}",
                route.pg_id().get(),
                route.state()
            )));
        }
        if route.active_metadata_proof().is_some() && route.state() != PgState::Active {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has an active metadata proof but is {:?}",
                route.pg_id().get(),
                route.state()
            )));
        }
        if let Some(read_route) = route.metadata_read_route() {
            if !is_current_route_set {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} grants metadata read authority on a historical route",
                    route.pg_id().get()
                )));
            }
            if !acting_set.contains(&read_route.node_id()) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} metadata read node {} is outside the acting set",
                    route.pg_id().get(),
                    read_route.node_id().as_u32()
                )));
            }
            if route.state() != PgState::Peering {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} grants metadata read authority while {:?}",
                    route.pg_id().get(),
                    route.state()
                )));
            }
        }
        if route.peering_metadata_transfer().is_some()
            && (route
                .peering_metadata_transfer_destination_epoch()
                .is_none()
                || route
                    .peering_metadata_transfer_source_route_epoch()
                    .is_none()
                || route.peering_metadata_transfer_source_node_id().is_none())
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has incomplete metadata transfer source route",
                route.pg_id().get()
            )));
        }
        if let Some(source_node_id) = route.peering_metadata_transfer_source_node_id() {
            if !node_ids.contains(&source_node_id) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} references unknown metadata transfer source node {}",
                    route.pg_id().get(),
                    source_node_id.as_u32()
                )));
            }
        }
        if route.peering_metadata_transfer().is_some() && route.state() != PgState::Peering {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has metadata transfer state but is {:?}",
                route.pg_id().get(),
                route.state()
            )));
        }
        if route.peering_metadata_transfer().is_none()
            && (route
                .peering_metadata_transfer_destination_epoch()
                .is_some()
                || route
                    .peering_metadata_transfer_source_route_epoch()
                    .is_some()
                || route.peering_metadata_transfer_source_node_id().is_some())
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has source route fields without metadata transfer",
                route.pg_id().get()
            )));
        }
        if route.pending_metadata_command_recovery().is_some()
            && (!is_current_route_set || route.state() != PgState::Peering)
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} has pending metadata command recovery outside a current Peering route",
                    route.pg_id().get()
                )));
        }
    }
    Ok(())
}

fn read_runtime_map_freshness_proof(
    reader: &mut PayloadReader<'_>,
) -> Result<RuntimeMapFreshnessProof, ControlPlaneError> {
    let tag = reader.read_u8()?;
    match tag {
        CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_SINGLE_AUTHORITY => {
            let authority_incarnation = read_runtime_map_proof_authority_incarnation(reader)?;
            Ok(RuntimeMapFreshnessProof::SingleAuthority {
                authority_incarnation,
                issued_at_ms: reader.read_u64()?,
            })
        }
        CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_RECONSTRUCTED => {
            let authority_incarnation = read_runtime_map_proof_authority_incarnation(reader)?;
            Ok(RuntimeMapFreshnessProof::Reconstructed {
                authority_incarnation,
            })
        }
        CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_READ_INDEX => {
            let authority_incarnation = read_runtime_map_proof_authority_incarnation(reader)?;
            Ok(RuntimeMapFreshnessProof::ReadIndex {
                authority_incarnation,
                read_index: read_runtime_map_proof_log_id(reader)?,
                issued_at_ms: reader.read_u64()?,
            })
        }
        other => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid runtime map freshness proof tag {other}"
        ))),
    }
}

fn read_runtime_map_proof_authority_incarnation(
    reader: &mut PayloadReader<'_>,
) -> Result<AuthorityIncarnation, ControlPlaneError> {
    AuthorityIncarnation::new(reader.read_u64()?).ok_or_else(|| {
        ControlPlaneError::rpc_protocol(
            "runtime map freshness proof authority incarnation must be nonzero".to_owned(),
        )
    })
}

fn read_runtime_map_proof_log_id(
    reader: &mut PayloadReader<'_>,
) -> Result<ControlPlaneLogId, ControlPlaneError> {
    let term = reader.read_u64()?;
    let index = reader.read_u64()?;
    if term == 0 {
        return Err(ControlPlaneError::rpc_protocol(
            "runtime map freshness proof read-index term must be nonzero".to_owned(),
        ));
    }
    ControlPlaneLogId::new(term, index).ok_or_else(|| {
        ControlPlaneError::rpc_protocol(
            "runtime map freshness proof read-index index must be nonzero".to_owned(),
        )
    })
}

fn read_pg_route_snapshots(
    reader: &mut PayloadReader<'_>,
    label: &'static str,
) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
    let route_count = reader.read_collection_len(label, CONTROL_PLANE_RPC_PG_ROUTE_MIN_LEN)?;
    let mut routes = Vec::with_capacity(route_count);
    for _ in 0..route_count {
        let route_epoch = read_cluster_epoch(reader, "PG route cluster epoch")?;
        let pg_id = PgId::new(reader.read_u32()?);
        let primary_node_id = NodeId::new(reader.read_u32()?);
        let state = read_pg_state(reader)?;
        let active_metadata_proof = match reader.read_u8()? {
            0 => None,
            1 => Some(read_pg_metadata_proof(reader)?),
            tag => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "invalid PG route active metadata proof tag {tag}"
                )));
            }
        };
        let metadata_read_route = match reader.read_u8()? {
            0 => None,
            1 => Some(PgMetadataReadRoute::new(
                NodeId::new(reader.read_u32()?),
                read_pg_metadata_proof(reader)?,
            )),
            tag => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "invalid PG metadata read route tag {tag}"
                )));
            }
        };
        let primary_lease_deadline_ms = reader.read_option_u64()?;
        let (
            peering_metadata_transfer,
            peering_metadata_transfer_destination_epoch,
            peering_metadata_transfer_source_route_epoch,
            peering_metadata_transfer_source_node_id,
        ) = match reader.read_u8()? {
            0 => (None, None, None, None),
            1 => {
                let source_epoch = read_cluster_epoch(reader, "metadata transfer source epoch")?;
                let source_metadata_proof = read_pg_metadata_proof(reader)?;
                let imported_metadata_proof = read_pg_metadata_proof(reader)?;
                let destination_epoch = reader
                    .read_option_u64()?
                    .map(|epoch| {
                        ClusterEpoch::new(epoch).ok_or_else(|| {
                            ControlPlaneError::rpc_protocol(
                                "metadata transfer destination epoch must be nonzero".to_owned(),
                            )
                        })
                    })
                    .transpose()?;
                let source_route_epoch = reader
                    .read_option_u64()?
                    .map(|epoch| {
                        ClusterEpoch::new(epoch).ok_or_else(|| {
                            ControlPlaneError::rpc_protocol(
                                "metadata transfer source route epoch must be nonzero".to_owned(),
                            )
                        })
                    })
                    .transpose()?;
                let source_node_id = reader.read_option_u32()?.map(NodeId::new);
                (
                    Some(PgMetadataTransferProof::new_with_imported_metadata_proof(
                        source_epoch,
                        source_metadata_proof,
                        imported_metadata_proof,
                    )),
                    destination_epoch,
                    source_route_epoch,
                    source_node_id,
                )
            }
            tag => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "invalid PG route metadata transfer tag {tag}"
                )));
            }
        };
        let pending_metadata_command_recovery = match reader.read_u8()? {
            0 => None,
            1 => Some(PendingMetadataCommandRecovery {
                reporting_node_id: NodeId::new(reader.read_u32()?),
                pending: PendingMetadataCommandObservation::new(
                    read_cluster_epoch(reader, "pending metadata command recovery epoch")?,
                    NonZeroU64::new(reader.read_u64()?).ok_or_else(|| {
                        ControlPlaneError::rpc_protocol(
                            "pending metadata command recovery log index must be nonzero"
                                .to_owned(),
                        )
                    })?,
                    reader.read_u64()?,
                ),
            }),
            tag => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "invalid pending metadata command recovery tag {tag}"
                )));
            }
        };
        let acting_set_len = reader.read_collection_len(
            "PG route acting set",
            CONTROL_PLANE_RPC_ACTING_SET_NODE_MIN_LEN,
        )?;
        let mut acting_set = Vec::with_capacity(acting_set_len);
        for _ in 0..acting_set_len {
            acting_set.push(NodeId::new(reader.read_u32()?));
        }
        match (
            peering_metadata_transfer,
            peering_metadata_transfer_destination_epoch,
        ) {
            (Some(transfer), Some(destination_epoch)) => {
                if destination_epoch > route_epoch || destination_epoch <= transfer.source_epoch() {
                    return Err(ControlPlaneError::rpc_protocol(format!(
                        "PG {} metadata transfer destination epoch {} must be newer than source epoch {} and no newer than route epoch {}",
                        pg_id.get(),
                        destination_epoch.get(),
                        transfer.source_epoch().get(),
                        route_epoch.get(),
                    )));
                }
            }
            (Some(_), None) => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "PG {} metadata transfer is missing its destination epoch",
                    pg_id.get()
                )));
            }
            (None, Some(_)) => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "PG {} metadata transfer destination epoch requires a transfer marker",
                    pg_id.get()
                )));
            }
            (None, None) => {}
        }
        routes.push(PgRouteSnapshot {
            cluster_epoch: route_epoch,
            pg_id,
            primary_node_id,
            acting_set,
            state,
            active_metadata_proof,
            metadata_read_route,
            primary_lease_deadline_ms,
            peering_metadata_transfer,
            peering_metadata_transfer_destination_epoch,
            peering_metadata_transfer_source_route_epoch,
            peering_metadata_transfer_source_node_id,
            pending_metadata_command_recovery,
        });
    }
    Ok(routes)
}

fn write_pg_metadata_proof(out: &mut Vec<u8>, proof: PgMetadataProof) {
    write_u64(out, proof.applied_log_index);
    write_u64(out, proof.applied_log_hash);
    write_u64(out, proof.state_digest);
}

fn write_pending_metadata_command_observation(
    out: &mut Vec<u8>,
    pending: Option<PendingMetadataCommandObservation>,
) {
    match pending {
        Some(pending) => {
            write_u8(out, 1);
            write_u64(out, pending.cluster_epoch().get());
            write_u64(out, pending.log_index());
            write_u64(out, pending.command_checksum());
        }
        None => write_u8(out, 0),
    }
}

fn read_pending_metadata_command_observation(
    reader: &mut PayloadReader<'_>,
) -> Result<Option<PendingMetadataCommandObservation>, ControlPlaneError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => {
            let cluster_epoch =
                read_cluster_epoch(reader, "pending metadata command cluster epoch")?;
            let log_index = NonZeroU64::new(reader.read_u64()?).ok_or_else(|| {
                ControlPlaneError::rpc_protocol(
                    "pending metadata command log index must be nonzero".to_owned(),
                )
            })?;
            let command_checksum = reader.read_u64()?;
            Ok(Some(PendingMetadataCommandObservation::new(
                cluster_epoch,
                log_index,
                command_checksum,
            )))
        }
        present => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid pending metadata command presence code {present}"
        ))),
    }
}

fn read_pg_metadata_proof(
    reader: &mut PayloadReader<'_>,
) -> Result<PgMetadataProof, ControlPlaneError> {
    Ok(PgMetadataProof {
        applied_log_index: reader.read_u64()?,
        applied_log_hash: reader.read_u64()?,
        state_digest: reader.read_u64()?,
    })
}

fn write_pg_state(out: &mut Vec<u8>, state: PgState) {
    write_u8(
        out,
        match state {
            PgState::Active => 1,
            PgState::Peering => 2,
            PgState::Degraded => 3,
            PgState::Backfilling => 4,
            PgState::Inconsistent => 5,
        },
    );
}

fn read_pg_state(reader: &mut PayloadReader<'_>) -> Result<PgState, ControlPlaneError> {
    match reader.read_u8()? {
        1 => Ok(PgState::Active),
        2 => Ok(PgState::Peering),
        3 => Ok(PgState::Degraded),
        4 => Ok(PgState::Backfilling),
        5 => Ok(PgState::Inconsistent),
        state => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid PG state code {state}"
        ))),
    }
}

fn read_cluster_epoch(
    reader: &mut PayloadReader<'_>,
    field: &'static str,
) -> Result<ClusterEpoch, ControlPlaneError> {
    ClusterEpoch::new(reader.read_u64()?)
        .ok_or_else(|| ControlPlaneError::rpc_protocol(format!("{field} must be nonzero")))
}

fn read_option_cluster_epoch(
    reader: &mut PayloadReader<'_>,
    field: &'static str,
) -> Result<Option<ClusterEpoch>, ControlPlaneError> {
    reader
        .read_option_u64()?
        .map(|epoch| {
            ClusterEpoch::new(epoch)
                .ok_or_else(|| ControlPlaneError::rpc_protocol(format!("{field} must be nonzero")))
        })
        .transpose()
}

fn write_option_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            write_u8(out, 1);
            write_u64(out, value);
        }
        None => write_u8(out, 0),
    }
}

fn write_option_u32(out: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            write_u8(out, 1);
            write_u32(out, value);
        }
        None => write_u8(out, 0),
    }
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), ControlPlaneError> {
    write_bytes(out, value.as_bytes())
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), ControlPlaneError> {
    write_u32(out, len_as_u32(value.len(), "byte field")?);
    out.extend_from_slice(value);
    Ok(())
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn len_as_u32(len: usize, field: &'static str) -> Result<u32, ControlPlaneError> {
    u32::try_from(len).map_err(|_| {
        ControlPlaneError::rpc_protocol(format!("{field} length {len} exceeds u32::MAX"))
    })
}

struct PayloadReader<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> PayloadReader<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn finish(&self) -> Result<(), ControlPlaneError> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane RPC payload has {} trailing bytes",
                self.payload.len() - self.offset
            )))
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ControlPlaneError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            ControlPlaneError::rpc_protocol("control-plane RPC payload offset overflow".to_owned())
        })?;
        let bytes = self.payload.get(self.offset..end).ok_or_else(|| {
            ControlPlaneError::rpc_protocol("truncated control-plane RPC payload".to_owned())
        })?;
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
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(ControlPlaneError::rpc_protocol(format!(
                "invalid boolean value {value}"
            ))),
        }
    }

    fn read_option_u64(&mut self) -> Result<Option<u64>, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            value => Err(ControlPlaneError::rpc_protocol(format!(
                "invalid optional u64 tag {value}"
            ))),
        }
    }

    fn read_option_u32(&mut self) -> Result<Option<u32>, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u32()?)),
            value => Err(ControlPlaneError::rpc_protocol(format!(
                "invalid optional u32 tag {value}"
            ))),
        }
    }

    fn read_len(&mut self, field: &'static str) -> Result<usize, ControlPlaneError> {
        usize::try_from(self.read_u32()?).map_err(|_| {
            ControlPlaneError::rpc_protocol(format!("{field} length does not fit usize"))
        })
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
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "{field} count {len} exceeds remaining control-plane RPC payload capacity {max_items}",
                )));
        }
        Ok(len)
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], ControlPlaneError> {
        let len = self.read_len("byte field")?;
        self.read_exact(len)
    }

    fn read_string(&mut self) -> Result<&'a str, ControlPlaneError> {
        std::str::from_utf8(self.read_bytes()?).map_err(|source| {
            ControlPlaneError::rpc_protocol(format!(
                "control-plane RPC string is not UTF-8: {source}"
            ))
        })
    }

    fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneRaftOperationErrorKind {
    ForwardToLeader,
    QuorumNotEnough,
    Fatal,
    Rejected,
}

impl ControlPlaneRaftOperationErrorKind {
    fn wire_tag(self) -> u8 {
        match self {
            Self::ForwardToLeader => 0,
            Self::QuorumNotEnough => 1,
            Self::Fatal => 2,
            Self::Rejected => 3,
        }
    }

    fn from_wire_tag(tag: u8) -> Result<Self, ControlPlaneError> {
        match tag {
            0 => Ok(Self::ForwardToLeader),
            1 => Ok(Self::QuorumNotEnough),
            2 => Ok(Self::Fatal),
            3 => Ok(Self::Rejected),
            _ => Err(ControlPlaneError::rpc_protocol(format!(
                "invalid OpenRaft operation error kind {tag}"
            ))),
        }
    }
}

impl fmt::Display for ControlPlaneRaftOperationErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ForwardToLeader => "forward-to-leader",
            Self::QuorumNotEnough => "quorum-not-enough",
            Self::Fatal => "fatal",
            Self::Rejected => "rejected",
        })
    }
}

/// Opaque diagnostic retained for a semantically classified control-plane failure.
///
/// The classification is part of the public control-plane contract. The retained diagnostic,
/// including any classified cause, is deliberately not exposed through formatting or accessors,
/// so callers cannot turn an implementation detail back into policy.
pub struct ControlPlaneFailureDiagnostic(ControlPlaneFailureDiagnosticKind);

enum ControlPlaneFailureDiagnosticKind {
    Message(Box<str>),
    ClassifiedCause {
        context: &'static str,
        source: Box<ControlPlaneError>,
    },
}

impl ControlPlaneFailureDiagnostic {
    fn new(detail: impl Into<Box<str>>) -> Self {
        Self(ControlPlaneFailureDiagnosticKind::Message(detail.into()))
    }

    fn classified_cause(context: &'static str, source: ControlPlaneError) -> Self {
        Self(ControlPlaneFailureDiagnosticKind::ClassifiedCause {
            context,
            source: Box::new(source),
        })
    }

    fn retained_message(&self) -> String {
        match &self.0 {
            ControlPlaneFailureDiagnosticKind::Message(message) => message.to_string(),
            ControlPlaneFailureDiagnosticKind::ClassifiedCause { context, source } => {
                format!("{context}: {}", source.retained_diagnostic_message())
            }
        }
    }
}

impl fmt::Debug for ControlPlaneFailureDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(_retained_diagnostic) = self;
        formatter.write_str("ControlPlaneFailureDiagnostic(<redacted>)")
    }
}

impl fmt::Display for ControlPlaneFailureDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(_retained_diagnostic) = self;
        formatter.write_str("control-plane diagnostic redacted")
    }
}

/// Opaque storage-owned diagnostic for a control-plane I/O failure.
///
/// Callers may classify the enclosing [`ControlPlaneError`] through its semantic helpers, but
/// cannot recover the transport context or underlying operating-system error. Storage retains
/// both values so its transport implementation can make the corresponding policy decisions.
pub struct ControlPlaneIoDiagnostic {
    context: &'static str,
    source: std::io::Error,
}

impl ControlPlaneIoDiagnostic {
    fn new(context: &'static str, source: std::io::Error) -> Self {
        Self { context, source }
    }

    pub(crate) fn context(&self) -> &'static str {
        self.context
    }

    pub(crate) fn kind(&self) -> ErrorKind {
        self.source.kind()
    }

    pub(crate) fn into_source(self) -> std::io::Error {
        self.source
    }
}

impl fmt::Debug for ControlPlaneIoDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            context: _retained_context,
            source: _retained_source,
        } = self;
        formatter.write_str("ControlPlaneIoDiagnostic(<redacted>)")
    }
}

impl fmt::Display for ControlPlaneIoDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            context: _retained_context,
            source: _retained_source,
        } = self;
        formatter.write_str("control-plane I/O diagnostic redacted")
    }
}

/// Opaque storage-owned diagnostic text from the control-plane RPC implementation.
///
/// The wire codec and its owner-local tests may inspect the retained text. Public formatting is
/// deliberately redacted so callers cannot turn implementation messages back into policy.
pub struct ControlPlaneRpcDiagnostic(Box<str>);

impl ControlPlaneRpcDiagnostic {
    fn new(detail: impl Into<Box<str>>) -> Self {
        Self(detail.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, pattern: &str) -> bool {
        self.0.contains(pattern)
    }
}

impl fmt::Debug for ControlPlaneRpcDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(_retained_detail) = self;
        formatter.write_str("ControlPlaneRpcDiagnostic(<redacted>)")
    }
}

impl fmt::Display for ControlPlaneRpcDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(_retained_detail) = self;
        formatter.write_str("control-plane RPC diagnostic redacted")
    }
}

#[derive(Debug, Error)]
pub enum ControlPlaneError {
    #[error("control-plane transport I/O failure")]
    Io {
        diagnostic: ControlPlaneIoDiagnostic,
    },

    #[error("control-plane state parse error at line {line}: {message}")]
    Parse { line: usize, message: String },

    #[error("control-plane authority clock checkpoint error: {message}")]
    AuthorityClockCheckpoint { message: String },

    #[error("control-plane RPC protocol failure")]
    RpcProtocol {
        diagnostic: ControlPlaneRpcDiagnostic,
    },

    #[error("control-plane command decode error: {message}")]
    CommandDecode { message: String },

    #[error("control-plane snapshot decode error: {message}")]
    SnapshotDecode { message: String },

    #[error("{context}: control-plane snapshot invariant violation: {message}")]
    SnapshotInvariantViolation {
        context: &'static str,
        message: String,
    },

    #[error(
        "control-plane committed log index mismatch: expected {expected_index}, got {actual_index}"
    )]
    ControlPlaneLogIndexMismatch {
        expected_index: u64,
        actual_index: u64,
    },

    #[error("control-plane committed log index overflow after {index}")]
    ControlPlaneLogIndexOverflow { index: u64 },

    #[error(
        "control-plane snapshot has no last-applied log id but current state is applied through index {current_index}"
    )]
    ControlPlaneSnapshotMissingLogId { current_index: u64 },

    #[error(
        "control-plane snapshot last-applied index {artifact_index} is older than current applied index {current_index}"
    )]
    ControlPlaneSnapshotLogIndexRegression {
        current_index: u64,
        artifact_index: u64,
    },

    #[error(
        "control-plane snapshot last-applied term {artifact_term} does not match current term {current_term} at index {index}"
    )]
    ControlPlaneSnapshotLogTermMismatch {
        index: u64,
        current_term: u64,
        artifact_term: u64,
    },

    #[error(
        "control-plane committed log term regressed from {previous_term} to {actual_term} at index {index}"
    )]
    ControlPlaneLogTermRegression {
        previous_term: u64,
        actual_term: u64,
        index: u64,
    },

    #[error(
        "control-plane read index {read_index:?} is not applied; last applied is {last_applied:?}"
    )]
    ControlPlaneReadIndexNotApplied {
        read_index: ControlPlaneLogId,
        last_applied: Option<ControlPlaneLogId>,
    },

    #[error("control-plane RPC remote failure")]
    RpcRemote {
        diagnostic: ControlPlaneRpcDiagnostic,
    },

    #[error("local control-plane authority is not serving")]
    AuthorityNotServing,

    #[error("control-plane durability is unavailable")]
    DurabilityFailure {
        diagnostic: ControlPlaneFailureDiagnostic,
    },

    #[error("control-plane invariant validation failed")]
    InvariantFailure {
        diagnostic: ControlPlaneFailureDiagnostic,
    },

    #[error("control-plane startup timed out")]
    StartupTimeout {
        diagnostic: ControlPlaneFailureDiagnostic,
    },

    #[error("control-plane static topology is invalid")]
    StaticTopologyFailure {
        diagnostic: ControlPlaneFailureDiagnostic,
    },

    #[error("control-plane OpenRaft operation failed ({kind}): {message}")]
    OpenRaftOperation {
        kind: ControlPlaneRaftOperationErrorKind,
        message: String,
    },

    #[error("control-plane RPC applied-state confirmation failed: {message}")]
    RpcUnconfirmed { message: String },

    #[error("invalid {field} state {value:?}")]
    InvalidState { field: &'static str, value: String },

    #[error("unknown node {node_id}")]
    UnknownNode { node_id: u32 },

    #[error("unknown PG {pg_id}")]
    UnknownPg { pg_id: u32 },

    #[error("cluster epoch {cluster_epoch} is not retained in cluster-map history")]
    UnknownClusterMapEpoch { cluster_epoch: ClusterEpoch },

    #[error(
        "node {node_id} reported storage cluster-map history route ({route_epoch}, PG {pg_id}) newer than validation epoch {validation_epoch}"
    )]
    StorageClusterMapHistoryRouteInFuture {
        node_id: u32,
        route_epoch: ClusterEpoch,
        pg_id: u32,
        validation_epoch: ClusterEpoch,
    },

    #[error(
        "node {node_id} reported storage cluster-map history route ({route_epoch}, PG {pg_id}) that is not retained at current epoch {cluster_epoch}"
    )]
    StorageClusterMapHistoryRouteNotRetained {
        node_id: u32,
        route_epoch: ClusterEpoch,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error("PG {pg_id} acting set must not be empty")]
    EmptyActingSet { pg_id: u32 },

    #[error("PG {pg_id} acting set references unknown node {node_id}")]
    UnknownActingSetNode { pg_id: u32, node_id: u32 },

    #[error("PG {pg_id} acting set repeats node {node_id}")]
    DuplicateActingSetNode { pg_id: u32, node_id: u32 },

    #[error(
        "PG {pg_id} metadata acting-set migration has no authoritative overlapping source; explicit metadata transfer is required"
    )]
    PgMetadataMigrationRequiresTransfer { pg_id: u32 },

    #[error(
        "PG {pg_id} metadata acting-set migration source has not acknowledged current cluster epoch {cluster_epoch}"
    )]
    PgMetadataMigrationSourceNotReady {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "PG {pg_id} in state {state} cannot change acting set in cluster epoch {cluster_epoch}"
    )]
    PgActingSetChangeNotReady {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error(
        "PG {pg_id} metadata transfer source epoch {source_epoch} is newer than current cluster epoch {cluster_epoch}"
    )]
    PgMetadataTransferSourceEpochInFuture {
        pg_id: u32,
        source_epoch: ClusterEpoch,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "PG {pg_id} metadata transfer source epoch {source_epoch} is stale for current cluster epoch {cluster_epoch}"
    )]
    PgMetadataTransferSourceEpochStale {
        pg_id: u32,
        source_epoch: ClusterEpoch,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "PG {pg_id} metadata transfer proof {actual:?} does not satisfy required floor {expected:?}"
    )]
    PgMetadataTransferProofBelowFloor {
        pg_id: u32,
        expected: PgMetadataProof,
        actual: PgMetadataProof,
    },

    #[error(
        "PG {pg_id} metadata transfer proof {actual:?} does not match existing marker {expected:?}"
    )]
    PgMetadataTransferProofMismatch {
        pg_id: u32,
        expected: PgMetadataTransferProof,
        actual: PgMetadataTransferProof,
    },

    #[error(
        "PG {pg_id} metadata transfer expected destination epoch {expected_destination_epoch}, but the next destination epoch is {actual_destination_epoch}"
    )]
    PgMetadataTransferDestinationEpochMismatch {
        pg_id: u32,
        expected_destination_epoch: ClusterEpoch,
        actual_destination_epoch: ClusterEpoch,
    },

    #[error("control-plane bootstrap repeats PG {pg_id}")]
    DuplicateBootstrapPg { pg_id: u32 },

    #[error("control-plane ready peering completion repeats PG {pg_id}")]
    DuplicateReadyPgPeeringCompletion { pg_id: u32 },

    #[error("control-plane bootstrap requires empty state")]
    BootstrapRequiresEmptyState,

    #[error("invalid initial cluster topology: {message}")]
    InvalidInitialTopology { message: String },

    #[error("node {node_id} heartbeat repeats PG {pg_id} observation")]
    DuplicatePgObservation { node_id: u32, pg_id: u32 },

    #[error("node {node_id} heartbeat reports PG {pg_id} outside its acting set")]
    PgObservationNotInActingSet { node_id: u32, pg_id: u32 },

    #[error("PG {pg_id} Active state requires complete_pg_peering")]
    ActivePgRequiresPeeringComplete { pg_id: u32 },

    #[error("PG {pg_id} Active state is missing its accepted metadata proof")]
    ActivePgMissingMetadataProof { pg_id: u32 },

    #[error("PG {pg_id} is fenced for metadata transfer and requires transfer install")]
    PgMetadataTransferFenceRequiresTransferInstall { pg_id: u32 },

    #[error(
        "PG {pg_id} previous primary {previous_primary} incarnation {previous_primary_incarnation} endpoint {previous_primary_endpoint:?} lease remains active until {lease_deadline_ms}; proposed primary {proposed_primary} incarnation {proposed_primary_incarnation} endpoint {proposed_primary_endpoint:?} completion time is {completed_at_ms}"
    )]
    PgPreviousPrimaryLeaseStillActive {
        pg_id: u32,
        previous_primary: u32,
        previous_primary_incarnation: u64,
        previous_primary_endpoint: String,
        proposed_primary: u32,
        proposed_primary_incarnation: u64,
        proposed_primary_endpoint: String,
        completed_at_ms: u64,
        lease_deadline_ms: u64,
    },

    #[error("node {node_id} is not in PG {pg_id} acting set")]
    PgPrimaryNotInActingSet { pg_id: u32, node_id: u32 },

    #[error("node {node_id} is not serving current epoch as PG {pg_id} primary")]
    PgPrimaryNotServingCurrentEpoch { pg_id: u32, node_id: u32 },

    #[error(
        "node {node_id} incarnation {sender_incarnation} does not match current incarnation {current_incarnation}"
    )]
    NodeIncarnationMismatch {
        node_id: u32,
        sender_incarnation: u64,
        current_incarnation: u64,
    },

    #[error("node {node_id} observed epoch {observed_epoch}, current epoch is {current_epoch}")]
    StaleNodeObservedEpoch {
        node_id: u32,
        observed_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error("node {node_id} reported future observed epoch {observed_epoch}, current epoch is {current_epoch}")]
    FutureNodeObservedEpoch {
        node_id: u32,
        observed_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error(
        "authorization authority incarnation {authority_incarnation} is stale; current incarnation is {current_authority_incarnation}"
    )]
    StaleAuthorityIncarnation {
        authority_incarnation: AuthorityIncarnation,
        current_authority_incarnation: AuthorityIncarnation,
    },

    #[error(
        "authorization cluster epoch {cluster_epoch} is stale; current epoch is {current_epoch}"
    )]
    StaleAuthorizationEpoch {
        cluster_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error("authorization is for {actual:?}, not expected operation {expected:?}")]
    PgOperationAuthorizationMismatch {
        expected: PgServiceOperation,
        actual: PgServiceOperation,
    },

    #[error("node {node_id} is not serving cluster epoch {cluster_epoch}")]
    NodeNotServingCurrentEpoch {
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error("node {node_id} has no advertised endpoint in cluster epoch {cluster_epoch}")]
    NodeEndpointMissing {
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "node {node_id} lease expired at {lease_deadline_ms:?}; authorization time is {now_ms}"
    )]
    NodeLeaseExpired {
        node_id: u32,
        now_ms: u64,
        lease_deadline_ms: Option<u64>,
    },

    #[error("PG {pg_id} is {state} in cluster epoch {cluster_epoch}, not active")]
    PgNotActive {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error("PG {pg_id} is {state} in cluster epoch {cluster_epoch}, not peering")]
    PgNotPeering {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error(
        "node {node_id} has not reported PG {pg_id} peering state in cluster epoch {cluster_epoch}"
    )]
    PgPeeringMissingObservation {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "node {node_id} reported PG {pg_id} as {state} in cluster epoch {cluster_epoch}, not peering"
    )]
    PgPeeringObservationNotPeering {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error(
        "node {node_id} reported PG {pg_id} metadata proof {actual:?} in cluster epoch {cluster_epoch}, expected {expected:?}"
    )]
    PgPeeringMetadataProofMismatch {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        expected: PgMetadataProof,
        actual: PgMetadataProof,
    },

    #[error(
        "PG {pg_id} peering metadata proof epoch {actual} does not match current cluster epoch {expected}"
    )]
    PgPeeringMetadataProofEpochMismatch {
        pg_id: u32,
        expected: ClusterEpoch,
        actual: ClusterEpoch,
    },

    #[error(
        "node {node_id} reported PG {pg_id} peering metadata proof {actual:?} in cluster epoch {cluster_epoch}, below required floor {expected:?}"
    )]
    PgPeeringMetadataProofBelowFloor {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        expected: PgMetadataProof,
        actual: PgMetadataProof,
    },

    #[error(
        "node {node_id} reported unresolved pending metadata command for PG {pg_id} in cluster epoch {cluster_epoch}: command epoch {}, log index {}, checksum {}",
        pending.cluster_epoch(),
        pending.log_index(),
        pending.command_checksum()
    )]
    PgPeeringPendingMetadataCommand {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        pending: PendingMetadataCommandObservation,
    },

    #[error(
        "PG {pg_id} has conflicting pending metadata command observations in cluster epoch {cluster_epoch}: node {first_node_id} reported {first:?}, node {second_node_id} reported {second:?}"
    )]
    PgPeeringPendingMetadataCommandMismatch {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        first_node_id: u32,
        first: PendingMetadataCommandObservation,
        second_node_id: u32,
        second: PendingMetadataCommandObservation,
    },

    #[error(
        "node {node_id} reported pending metadata command for PG {pg_id} at epoch {pending_epoch}, but that historical route was {historical_state} with primary {historical_primary_node_id} (current epoch {cluster_epoch})"
    )]
    PgPeeringPendingMetadataCommandReporterNotHistoricalPrimary {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        node_id: u32,
        pending_epoch: ClusterEpoch,
        historical_state: PgState,
        historical_primary_node_id: u32,
    },

    #[error(
        "node {node_id} reported Active PG {pg_id} metadata proof {actual:?} in cluster epoch {cluster_epoch}, expected active proof {expected:?}"
    )]
    PgActiveMetadataProofMismatch {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        expected: PgMetadataProof,
        actual: PgMetadataProof,
    },

    #[error(
        "PG {pg_id} primary node {node_id} has not reported active state in cluster epoch {cluster_epoch}"
    )]
    PgPrimaryMissingActiveObservation {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "PG {pg_id} primary node {node_id} reported {state} in cluster epoch {cluster_epoch}, not active"
    )]
    PgPrimaryObservationNotActive {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error("PG {pg_id} has no serving primary in cluster epoch {cluster_epoch}")]
    PgHasNoServingPrimary {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "node {node_id} is not PG {pg_id} primary in cluster epoch {cluster_epoch}; primary is {primary_node_id}"
    )]
    NodeNotPgPrimary {
        pg_id: u32,
        node_id: u32,
        primary_node_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error("removed node {node_id} cannot rejoin")]
    RemovedNodeCannotRejoin { node_id: u32 },

    #[error("node {node_id} in membership state {membership:?} cannot receive a lease")]
    NodeCannotReceiveLease {
        node_id: u32,
        membership: NodeMembershipState,
    },

    #[error(
        "stale heartbeat from node {node_id}: incarnation {heartbeat_incarnation} is older than current {current_incarnation}"
    )]
    StaleNodeIncarnation {
        node_id: u32,
        heartbeat_incarnation: u64,
        current_incarnation: u64,
    },

    #[error("heartbeat lease duration must be positive")]
    InvalidLeaseDuration,

    #[error("heartbeat lease duration {requested_ms}ms exceeds maximum {max_ms}ms")]
    LeaseDurationTooLong { requested_ms: u64, max_ms: u64 },

    #[error(
        "lease grant horizon duration {duration_ms}ms must be positive and no greater than {max_ms}ms"
    )]
    InvalidLeaseGrantHorizonDuration { duration_ms: u64, max_ms: u64 },

    #[error("{field} timestamp arithmetic overflowed")]
    LeaseGrantHorizonTimestampOverflow { field: &'static str },

    #[error(
        "previous lease grant horizon remains fenced through {fenced_until_ms}ms at accepted authority time {authority_now_ms}ms"
    )]
    PreviousLeaseGrantHorizonStillActive {
        authority_now_ms: u64,
        fenced_until_ms: u64,
    },

    #[error(
        "lease grant horizon authority Raft term {authority_term:?} does not match committed Raft term {committed_term:?}"
    )]
    LeaseGrantHorizonAuthorityTermMismatch {
        authority_term: Option<u64>,
        committed_term: Option<u64>,
    },

    #[error("heartbeat lease deadline overflow")]
    LeaseDeadlineOverflow,

    #[error(
        "serving deadline {serving_deadline_ms}ms exceeds effective committed time {effective_committed_now_ms}ms plus maximum lease {max_lease_ms}ms and skew budget {skew_budget_ms}ms"
    )]
    LeaseDeadlineOutOfBounds {
        serving_deadline_ms: u64,
        effective_committed_now_ms: u64,
        max_lease_ms: u64,
        skew_budget_ms: u64,
    },

    #[error(
        "committed timestamp {timestamp_ms}ms regressed below previous maximum {max_committed_timestamp_ms}ms"
    )]
    CommittedTimestampRegression {
        timestamp_ms: u64,
        max_committed_timestamp_ms: u64,
    },

    #[error(
        "committed timestamp {timestamp_ms}ms is more than {max_forward_jump_ms}ms ahead of previous maximum {max_committed_timestamp_ms}ms"
    )]
    CommittedTimestampTooFarAhead {
        timestamp_ms: u64,
        max_committed_timestamp_ms: u64,
        max_forward_jump_ms: u64,
    },

    #[error(
        "control-plane authority clock was established for Raft term {established_term:?}, not current local leadership term {current_term}"
    )]
    AuthorityClockLeadershipChanged {
        established_term: Option<u64>,
        current_term: u64,
    },

    #[error("control-plane authority clock-health source is unavailable")]
    AuthorityClockSourceUnavailable,

    #[error("control-plane authority clock is not established: {blocked_reason:?}")]
    AuthorityClockNotEstablished {
        blocked_reason: Option<ControlPlaneAuthorityClockBlockedReason>,
    },

    #[error(
        "control-plane authority clock sample window {narrowest_window_ms}ms exceeds maximum {max_window_ms}ms"
    )]
    AuthorityClockSampleWindowTooWide {
        narrowest_window_ms: u64,
        max_window_ms: u64,
    },

    #[error("control-plane authority clock is already established")]
    AuthorityClockAlreadyEstablished,

    #[error(
        "control-plane authority clock generation changed: expected {expected_generation}, actual {actual_generation}"
    )]
    AuthorityClockGenerationMismatch {
        expected_generation: u64,
        actual_generation: u64,
    },

    #[error(
        "control-plane authority clock committed timestamp changed: expected {expected_timestamp_ms:?}, actual {actual_timestamp_ms:?}"
    )]
    AuthorityClockCommittedTimestampMismatch {
        expected_timestamp_ms: Option<u64>,
        actual_timestamp_ms: Option<u64>,
    },

    #[error(
        "control-plane authority clock Raft term changed: expected {expected_term:?}, actual {actual_term:?}"
    )]
    AuthorityClockRaftTermMismatch {
        expected_term: Option<u64>,
        actual_term: Option<u64>,
    },

    #[error(
        "control-plane authority clock can only be re-established on the local serving Raft authority"
    )]
    AuthorityClockNotLocalServingRaftAuthority,

    #[error(
        "control-plane authority wall clock {wall_ms}ms is behind committed timestamp high-water {committed_timestamp_high_water_ms}ms"
    )]
    AuthorityClockWallBehindCommittedTimestamp {
        wall_ms: u64,
        committed_timestamp_high_water_ms: u64,
    },

    #[error("control-plane authority clock generation overflow")]
    AuthorityClockGenerationOverflow,

    #[error(
        "node {node_id} heartbeat lease deadline {requested_lease_deadline_ms}ms regressed below current deadline {current_lease_deadline_ms}ms"
    )]
    NodeLeaseDeadlineRegression {
        node_id: u32,
        current_lease_deadline_ms: u64,
        requested_lease_deadline_ms: u64,
    },

    #[error(
        "node {node_id} heartbeat lease deadline {actual_deadline_ms} does not match heartbeat time {heartbeat_at_ms} plus requested duration {requested_ms}ms; expected {expected_deadline_ms}"
    )]
    LeaseDeadlineMismatch {
        node_id: u32,
        heartbeat_at_ms: u64,
        requested_ms: u64,
        expected_deadline_ms: u64,
        actual_deadline_ms: u64,
    },

    #[error("cluster epoch overflow")]
    ClusterEpochOverflow,

    #[error("authority incarnation overflow")]
    AuthorityIncarnationOverflow,
}

impl ControlPlaneError {
    #[must_use]
    pub(crate) fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io {
            diagnostic: ControlPlaneIoDiagnostic::new(context, source),
        }
    }

    #[must_use]
    pub(crate) fn rpc_protocol(diagnostic: impl Into<Box<str>>) -> Self {
        Self::RpcProtocol {
            diagnostic: ControlPlaneRpcDiagnostic::new(diagnostic),
        }
    }

    #[must_use]
    pub(crate) fn rpc_remote(diagnostic: impl Into<Box<str>>) -> Self {
        Self::RpcRemote {
            diagnostic: ControlPlaneRpcDiagnostic::new(diagnostic),
        }
    }

    pub(crate) fn retained_diagnostic_message(&self) -> String {
        match self {
            Self::Io { diagnostic } => {
                format!("{}: {}", diagnostic.context, diagnostic.source)
            }
            Self::RpcProtocol { diagnostic } => {
                format!("control-plane RPC protocol error: {}", diagnostic.as_str())
            }
            Self::RpcRemote { diagnostic } => {
                format!("control-plane RPC remote error: {}", diagnostic.as_str())
            }
            Self::DurabilityFailure { diagnostic }
            | Self::InvariantFailure { diagnostic }
            | Self::StartupTimeout { diagnostic }
            | Self::StaticTopologyFailure { diagnostic } => diagnostic.retained_message(),
            error => error.to_string(),
        }
    }

    fn rpc_wire_error_message(&self) -> String {
        self.retained_diagnostic_message()
    }

    #[cfg(test)]
    pub(crate) fn retained_diagnostic_contains(&self, expected: &str) -> bool {
        self.retained_diagnostic_message().contains(expected)
    }

    #[must_use]
    pub fn durability_failure(diagnostic: impl Into<Box<str>>) -> Self {
        Self::DurabilityFailure {
            diagnostic: ControlPlaneFailureDiagnostic::new(diagnostic),
        }
    }

    /// Classify an implementation failure as a durability failure without discarding its cause.
    ///
    /// The returned error exposes only the semantic durability classification. Storage retains
    /// the original error inside the opaque diagnostic for owner-local diagnosis; public
    /// formatting and [`std::error::Error::source`] do not reveal it.
    #[must_use]
    pub fn into_durability_failure(self, context: &'static str) -> Self {
        Self::DurabilityFailure {
            diagnostic: ControlPlaneFailureDiagnostic::classified_cause(context, self),
        }
    }

    #[must_use]
    pub fn invariant_failure(diagnostic: impl Into<Box<str>>) -> Self {
        Self::InvariantFailure {
            diagnostic: ControlPlaneFailureDiagnostic::new(diagnostic),
        }
    }

    #[must_use]
    pub fn startup_timeout(diagnostic: impl Into<Box<str>>) -> Self {
        Self::StartupTimeout {
            diagnostic: ControlPlaneFailureDiagnostic::new(diagnostic),
        }
    }

    #[must_use]
    pub fn static_topology_failure(diagnostic: impl Into<Box<str>>) -> Self {
        Self::StaticTopologyFailure {
            diagnostic: ControlPlaneFailureDiagnostic::new(diagnostic),
        }
    }

    #[must_use]
    pub fn is_retryable_openraft_leadership_error(&self) -> bool {
        matches!(
            self,
            Self::OpenRaftOperation {
                kind: ControlPlaneRaftOperationErrorKind::ForwardToLeader
                    | ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
                ..
            }
        )
    }

    #[must_use]
    pub fn is_control_plane_leader_routing_rejection(&self) -> bool {
        if self.is_retryable_openraft_leadership_error() {
            return true;
        }
        matches!(
            self,
            Self::AuthorityNotServing | Self::AuthorityClockNotLocalServingRaftAuthority
        )
    }

    #[must_use]
    pub fn is_retryable_read_only_rpc_transport_error(&self) -> bool {
        self.is_retryable_control_plane_rpc_transport_error()
            || self.is_control_plane_leader_routing_rejection()
    }

    #[must_use]
    pub fn is_retryable_control_plane_rpc_transport_error(&self) -> bool {
        let Self::Io { diagnostic } = self else {
            return false;
        };
        matches!(
            diagnostic.kind(),
            ErrorKind::TimedOut
                | ErrorKind::WouldBlock
                | ErrorKind::UnexpectedEof
                | ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::BrokenPipe
                | ErrorKind::Interrupted
                | ErrorKind::NotConnected
                | ErrorKind::ConnectionRefused
                | ErrorKind::NotFound
        )
    }

    #[must_use]
    pub fn is_retryable_runtime_map_observation_error(&self) -> bool {
        self.is_retryable_read_only_rpc_transport_error()
            || self.is_transient_runtime_map_serving_gap()
    }

    #[must_use]
    pub fn is_retryable_heartbeat_startup_error(&self) -> bool {
        self.is_retryable_runtime_map_observation_error()
            || matches!(self, Self::RpcUnconfirmed { .. })
    }

    #[must_use]
    pub fn is_maybe_applied_control_plane_rpc_response_loss(&self) -> bool {
        let Self::Io { diagnostic } = self else {
            return false;
        };
        matches!(
            diagnostic.context(),
            "read control-plane RPC magic"
                | "read control-plane RPC header"
                | "read control-plane RPC payload"
        ) && matches!(
            diagnostic.kind(),
            ErrorKind::TimedOut
                | ErrorKind::WouldBlock
                | ErrorKind::UnexpectedEof
                | ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::BrokenPipe
                | ErrorKind::Interrupted
                | ErrorKind::NotConnected
        )
    }

    #[must_use]
    fn is_unconfirmed_control_plane_mutation(&self) -> bool {
        self.is_maybe_applied_control_plane_rpc_response_loss()
            || matches!(self, Self::RpcUnconfirmed { .. })
    }

    #[must_use]
    fn is_retryable_pg_acting_set_checked_error(&self) -> bool {
        self.is_unconfirmed_control_plane_mutation()
            || self.is_transient_runtime_map_serving_gap()
            || matches!(
                self,
                Self::PgMetadataMigrationSourceNotReady { .. }
                    | Self::PgActingSetChangeNotReady { .. }
            )
    }

    #[must_use]
    fn is_transient_runtime_map_serving_gap(&self) -> bool {
        matches!(
            self,
            Self::PgHasNoServingPrimary { .. }
                | Self::PgPrimaryMissingActiveObservation { .. }
                | Self::PgPrimaryObservationNotActive { .. }
                | Self::PgPeeringPendingMetadataCommand { .. }
        )
    }
}

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
        if pg.state == PgState::Active
            && pg.active_primary == Some(node_id)
            && observation.state == PgState::Active
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
