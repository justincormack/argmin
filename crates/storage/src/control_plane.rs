// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{ErrorKind, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::num::NonZeroU64;
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
    ControlPlaneAuthEnvelopeDecodeError, ControlPlaneAuthOperation, ControlPlaneAuthPrincipal,
    ControlPlaneAuthRejectionReason, ControlPlaneAuthReplayPolicy, ControlPlaneAuthService,
    ControlPlaneAuthTarget, ControlPlaneScopedCredential, ControlPlaneScopedCredentialInput,
    ControlPlaneScopedCredentialStore,
};
use crate::control_plane_command::{
    decode_control_plane_command, encode_control_plane_command,
    unavailable_pg_batch_metric_descriptor, AppliedControlPlaneCommand, ControlPlaneCommand,
    ControlPlaneCommandResponse, ControlPlaneCommandStateMachine, ControlPlaneLogId,
    ExpiredNodeHeartbeatLease, FinalizeMetadataTransferStagingGenerationRequest,
    MetadataTransferStagingCleanupDisposition, MetadataTransferStagingTombstoneBinding,
    PromotedNodeHeartbeatLease, ReadyPgPeeringCompletion,
    UnavailablePgStagingIntentAuthorizationRequest, UnavailablePgStagingPublicationBinding,
    UnavailablePgTransitionBeginRequest, UnavailablePgTransitionCompletionRequest,
    UnavailablePgTransitionInstallRequest,
};

pub(crate) struct AuthorityPublishedStagingAuthorizationSeal {
    _private: (),
}

fn authority_published_staging_authorization_seal() -> AuthorityPublishedStagingAuthorizationSeal {
    AuthorityPublishedStagingAuthorizationSeal { _private: () }
}

pub(crate) struct CompletedUnavailablePgStagingCleanupAuthorization {
    cluster_epoch: ClusterEpoch,
    destination_actors: Vec<MetadataTransferStagingNodeIdentity>,
}

impl CompletedUnavailablePgStagingCleanupAuthorization {
    pub(crate) fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    pub(crate) fn destination_actor(
        &self,
        node_id: NodeId,
    ) -> Option<&MetadataTransferStagingNodeIdentity> {
        self.destination_actors
            .binary_search_by_key(&node_id, MetadataTransferStagingNodeIdentity::node_id)
            .ok()
            .map(|index| &self.destination_actors[index])
    }
}

#[cfg(test)]
pub(crate) fn committed_staging_authorization_from_presentation_for_test(
    presentation: crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation,
    destination_node_id: NodeId,
    pg_id: PgId,
) -> crate::control_plane_command::CommittedUnavailablePgStagingAuthorization {
    assert!(presentation.authorizes_destination_for_pg(destination_node_id, pg_id));
    crate::control_plane_command::CommittedUnavailablePgStagingAuthorization::from_authority_published(
        presentation,
        destination_node_id,
        pg_id,
        authority_published_staging_authorization_seal(),
    )
}

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
use crate::internal_tls_protocol::InternalTlsProtocol;
use crate::pg_store::{
    decode_staging_evidence_apply_receipt, decode_staging_evidence_page_payload,
    MetadataTransferStagingEvidenceApplyReceipt, MetadataTransferStagingEvidencePage,
    MetadataTransferStagingNodeIdentity,
};
use crate::static_topology::UncertifiedInitialControlPlaneTopology;
use crate::{
    ClusterEpoch, PgClusterMapHistoryReferenceSummary, PgClusterMapHistoryRouteReference,
    PgClusterMapHistoryRouteReferenceKind, PgClusterMapHistoryRouteReferences, PgId, PgState,
    RouteMapValidity, MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES,
};

// PG backfill can lag a burst of placement changes; retain enough recent
// snapshots that scanner references can still reconstruct historical routes.
const CLUSTER_MAP_HISTORY_LIMIT: usize = 256;
pub const MAX_HEARTBEAT_LEASE_MS: u64 = 10_000;
pub const DEFAULT_UNAVAILABLE_PLACEMENT_GRACE_MS: u64 = 30_000;
pub const UNAVAILABLE_PG_RECONCILIATION_SCAN_PAGE_SIZE: usize = 16;
pub(crate) const METADATA_TRANSFER_STAGING_MAINTENANCE_SCAN_PAGE_SIZE: usize = 16;
pub const MAX_UNAVAILABLE_PG_TRANSITION_BATCH: usize = 16;
pub(crate) const MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_PAGES: usize = 64;
const MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COMMITMENTS: usize = 64;
const MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_STATE_RECORD_BYTES: usize = 120 * 1_024;
const METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_STATE_RECORD_PREFIX: &str =
    "metadata_transfer_staging_evidence_checkpoint=";
const METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_ANCHOR_STATE_RECORD_PREFIX: &str =
    "metadata_transfer_staging_evidence_checkpoint_anchor=";
const METADATA_TRANSFER_STAGING_ACTOR_CLOSURE_STATE_RECORD_PREFIX: &str =
    "metadata_transfer_staging_actor_closure=";
const METADATA_TRANSFER_STAGING_RETIRED_ACTOR_CLOSURE_STATE_RECORD_PREFIX: &str =
    "metadata_transfer_staging_retired_actor_closure=";
const METADATA_TRANSFER_STAGING_FINALIZED_FLOOR_STATE_RECORD_PREFIX: &str =
    "metadata_transfer_staging_finalized_floor=";
pub(crate) const MAX_LEASE_GRANT_HORIZON_MS: u64 = 60_000;
pub(crate) const CONTROL_PLANE_LEASE_GRANT_HORIZON_DURATION_MS: u64 = 2 * MAX_HEARTBEAT_LEASE_MS;
pub const CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS: u64 = CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS;
const CONTROL_PLANE_RPC_MAGIC: &[u8] = b"argmin-control-plane-rpc";
const CONTROL_PLANE_RPC_VERSION: u16 = 24;
const CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN: usize = 8 * 1024 * 1024;
pub const CONTROL_PLANE_RPC_MAX_FRAME_BYTES: usize =
    CONTROL_PLANE_RPC_MAGIC.len() + 16 + CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN;
const CONTROL_PLANE_RPC_TLS_ALPN: &[u8] = InternalTlsProtocol::ControlPlaneRpc.alpn();
const CONTROL_PLANE_RPC_IO_TIMEOUT: Duration = Duration::from_secs(1);
pub const CONTROL_PLANE_RPC_MAX_SERVER_OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
const CONTROL_PLANE_RPC_LEADERSHIP_TRANSFER_TIMEOUT: Duration = Duration::from_secs(15);
const CONTROL_PLANE_RPC_SNAPSHOT_PURGE_TIMEOUT: Duration = Duration::from_secs(15);
const CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ADMIN_TIMEOUT: Duration = Duration::from_secs(15);
const CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const CONTROL_PLANE_RPC_AUTHORITY_CLOCK_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const CURRENT_CONTROL_PLANE_STATE_VERSION: u64 = 44;
const UNAVAILABLE_PG_TRANSITION_BATCH_RECEIPT_DIGEST_DOMAIN: &[u8] =
    b"argmin-unavailable-pg-transition-batch-receipt-v1";
const METADATA_TRANSFER_STAGING_CLEANUP_DIGEST_DOMAIN: &[u8] =
    b"argmin-metadata-transfer-staging-cleanup-v2";
const METADATA_TRANSFER_STAGING_CHECKPOINT_SEGMENT_DIGEST_DOMAIN: &[u8] =
    b"argmin-metadata-transfer-staging-checkpoint-segment-v1";
const METADATA_TRANSFER_STAGING_CHECKPOINT_SOURCE_SEGMENTS_DIGEST_DOMAIN: &[u8] =
    b"argmin-metadata-transfer-staging-checkpoint-source-segments-v1";
const METADATA_TRANSFER_STAGING_ACTOR_CLOSURE_CERTIFICATE_DIGEST_DOMAIN: &[u8] =
    b"argmin-metadata-transfer-staging-actor-closure-certificate-v1";
pub(crate) const MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COALESCED_ANCHORS: usize = 64;
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
    cluster_map_history_route_scan_generation: Option<NonZeroU64>,
    cluster_map_history_route_references: PgClusterMapHistoryRouteReferences,
    retiring_cluster_map_history_route_references: PgClusterMapHistoryRouteReferences,
    pg_observations: BTreeMap<PgId, NodePgObservationRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeUnavailableObservation {
    pub(crate) node_id: NodeId,
    pub(crate) node_incarnation: u64,
    pub(crate) endpoint: String,
    pub(crate) lease_deadline_ms: u64,
    pub(crate) observed_at_ms: u64,
}

impl NodeUnavailableObservation {
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
    pub fn lease_deadline_ms(&self) -> u64 {
        self.lease_deadline_ms
    }

    #[must_use]
    pub fn observed_at_ms(&self) -> u64 {
        self.observed_at_ms
    }
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
            cluster_map_history_route_scan_generation: None,
            cluster_map_history_route_references: PgClusterMapHistoryRouteReferences::default(),
            retiring_cluster_map_history_route_references:
                PgClusterMapHistoryRouteReferences::default(),
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
        self.cluster_map_history_reference_summary()
            .oldest_required_epoch()
    }

    pub fn cluster_map_history_route_references(&self) -> &PgClusterMapHistoryRouteReferences {
        &self.cluster_map_history_route_references
    }

    fn retained_cluster_map_history_route_references(
        &self,
    ) -> impl Iterator<Item = PgClusterMapHistoryRouteReference> + '_ {
        self.cluster_map_history_route_references
            .iter()
            .chain(self.retiring_cluster_map_history_route_references.iter())
    }

    fn cluster_map_history_reference_summary(&self) -> PgClusterMapHistoryReferenceSummary {
        let mut summary = self.cluster_map_history_route_references.summary();
        summary.merge(self.retiring_cluster_map_history_route_references.summary());
        summary
    }

    fn record_cluster_map_history_route_references(
        &mut self,
        scan_generation: NonZeroU64,
        reported: PgClusterMapHistoryRouteReferences,
    ) {
        if self
            .cluster_map_history_route_scan_generation
            .is_some_and(|current| scan_generation <= current)
        {
            return;
        }
        let retiring = PgClusterMapHistoryRouteReferences::try_from_iter(
            self.cluster_map_history_route_references
                .iter()
                .filter(|reference| !reported.iter().any(|current| current == *reference)),
        )
        .expect("retiring history references are a subset of the bounded previous report");
        self.cluster_map_history_route_references = reported;
        self.retiring_cluster_map_history_route_references = retiring;
        self.cluster_map_history_route_scan_generation = Some(scan_generation);
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
    placement_policy: CertifiedStoragePlacementPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertifiedStorageFailureDomain {
    None,
    Disk,
    Host,
}

impl CertifiedStorageFailureDomain {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Disk => "disk",
            Self::Host => "host",
        }
    }

    pub(crate) fn from_str(value: &str) -> Result<Self, ControlPlaneError> {
        match value {
            "none" => Ok(Self::None),
            "disk" => Ok(Self::Disk),
            "host" => Ok(Self::Host),
            _ => Err(ControlPlaneError::InvalidState {
                field: "initial_topology_failure_domain",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertifiedStorageNodeDomain {
    pub(crate) node_id: NodeId,
    pub(crate) host: String,
    pub(crate) disk: String,
}

impl CertifiedStorageNodeDomain {
    #[must_use]
    pub fn new(node_id: NodeId, host: impl Into<String>, disk: impl Into<String>) -> Self {
        Self {
            node_id,
            host: host.into(),
            disk: disk.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertifiedStoragePlacementPolicy {
    pub(crate) ec_data_shards: u8,
    pub(crate) ec_parity_shards: u8,
    pub(crate) failure_domain: CertifiedStorageFailureDomain,
    pub(crate) failure_tolerance: u8,
    pub(crate) unavailable_replacement_grace_ms: u64,
    pub(crate) nodes: Vec<CertifiedStorageNodeDomain>,
}

impl CertifiedStoragePlacementPolicy {
    pub fn new(
        ec_data_shards: u8,
        ec_parity_shards: u8,
        failure_domain: CertifiedStorageFailureDomain,
        failure_tolerance: u8,
        unavailable_replacement_grace_ms: u64,
        nodes: Vec<CertifiedStorageNodeDomain>,
    ) -> Result<Self, ControlPlaneError> {
        let policy = Self {
            ec_data_shards,
            ec_parity_shards,
            failure_domain,
            failure_tolerance,
            unavailable_replacement_grace_ms,
            nodes,
        };
        policy
            .validate()
            .map_err(|message| ControlPlaneError::InvalidInitialTopology { message })?;
        Ok(policy)
    }

    #[must_use]
    pub fn total_shards(&self) -> usize {
        usize::from(self.ec_data_shards) + usize::from(self.ec_parity_shards)
    }

    #[must_use]
    pub fn unavailable_replacement_grace_ms(&self) -> u64 {
        self.unavailable_replacement_grace_ms
    }

    fn node(&self, node_id: NodeId) -> Option<&CertifiedStorageNodeDomain> {
        self.nodes
            .binary_search_by_key(&node_id, |node| node.node_id)
            .ok()
            .map(|index| &self.nodes[index])
    }

    fn validate(&self) -> Result<(), String> {
        if self.ec_data_shards == 0
            || self
                .ec_data_shards
                .checked_add(self.ec_parity_shards)
                .is_none()
        {
            return Err("certified storage EC shape is invalid".to_string());
        }
        if self.failure_tolerance > self.ec_parity_shards {
            return Err("certified failure tolerance exceeds parity shards".to_string());
        }
        if self.failure_domain == CertifiedStorageFailureDomain::None && self.failure_tolerance != 0
        {
            return Err("failure-domain none requires zero failure tolerance".to_string());
        }
        if self.unavailable_replacement_grace_ms == 0 {
            return Err("unavailable replacement grace must be nonzero".to_string());
        }
        if self.nodes.is_empty() {
            return Err("certified placement policy requires storage nodes".to_string());
        }
        for pair in self.nodes.windows(2) {
            if pair[0].node_id >= pair[1].node_id {
                return Err("certified storage node domains must be strictly ordered".to_string());
            }
        }
        for node in &self.nodes {
            if node.host.is_empty() || node.disk.is_empty() {
                return Err(format!(
                    "certified storage node {} has an empty failure-domain identity",
                    node.node_id.as_u32()
                ));
            }
        }
        Ok(())
    }

    fn validate_acting_set(&self, acting_set: &[NodeId]) -> Result<(), String> {
        if acting_set.len() != self.total_shards() {
            return Err(format!(
                "acting set has {} nodes but certified EC policy requires {}",
                acting_set.len(),
                self.total_shards()
            ));
        }
        let mut nodes = BTreeSet::new();
        let mut domains = BTreeSet::new();
        for node_id in acting_set.iter().copied() {
            if !nodes.insert(node_id) {
                return Err(format!("acting set repeats node {}", node_id.as_u32()));
            }
            let node = self.node(node_id).ok_or_else(|| {
                format!(
                    "acting set contains node {} outside certified topology",
                    node_id.as_u32()
                )
            })?;
            let domain = match self.failure_domain {
                CertifiedStorageFailureDomain::None => continue,
                CertifiedStorageFailureDomain::Disk => node.disk.as_str(),
                CertifiedStorageFailureDomain::Host => node.host.as_str(),
            };
            if !domains.insert(domain) {
                return Err(format!(
                    "acting set repeats certified {} failure domain {domain:?}",
                    self.failure_domain.as_str()
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn test_certified_storage_placement_policy(
    node_ids: impl IntoIterator<Item = NodeId>,
    total_shards: u8,
    unavailable_replacement_grace_ms: u64,
) -> CertifiedStoragePlacementPolicy {
    CertifiedStoragePlacementPolicy::new(
        total_shards,
        0,
        CertifiedStorageFailureDomain::None,
        0,
        unavailable_replacement_grace_ms,
        node_ids
            .into_iter()
            .map(|node_id| {
                CertifiedStorageNodeDomain::new(
                    node_id,
                    format!("test-host-{}", node_id.as_u32()),
                    format!("test-disk-{}", node_id.as_u32()),
                )
            })
            .collect(),
    )
    .expect("test placement policy is valid")
}

impl InitialClusterTopologyCertificate {
    pub fn new(
        topology_generation: u64,
        topology_digest: [u8; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
        bootstrap_map_digest: [u8; CONTROL_PLANE_BOOTSTRAP_MAP_DIGEST_LEN],
        raft_voters: Vec<u64>,
        placement_policy: CertifiedStoragePlacementPolicy,
    ) -> Result<Self, ControlPlaneError> {
        let certificate = Self {
            topology_generation,
            topology_digest,
            bootstrap_map_digest,
            raft_voters,
            placement_policy,
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
        placement_policy: CertifiedStoragePlacementPolicy,
    ) -> Result<Self, ControlPlaneError> {
        Self::new(
            topology_generation,
            topology_digest,
            initial_cluster_bootstrap_map_digest(nodes, pg_acting_sets),
            raft_voters,
            placement_policy,
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

    #[must_use]
    pub fn placement_policy(&self) -> &CertifiedStoragePlacementPolicy {
        &self.placement_policy
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
        self.placement_policy.validate()?;
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
    unavailable_node_observations: BTreeMap<NodeId, NodeUnavailableObservation>,
    unavailable_pg_placement_transitions: BTreeMap<PgId, UnavailablePgPlacementTransition>,
    retained_unavailable_pg_placement_transitions:
        BTreeMap<(PgId, ClusterEpoch), UnavailablePgPlacementTransition>,
    metadata_transfer_staging_evidence_pages:
        BTreeMap<(NodeId, u64, u64), MetadataTransferStagingEvidencePageRecord>,
    metadata_transfer_staging_evidence_checkpoint_segments:
        BTreeMap<(NodeId, u64, u64), MetadataTransferStagingEvidenceCheckpointSegment>,
    metadata_transfer_staging_evidence_checkpoint_anchors:
        BTreeMap<(NodeId, u64, u64), MetadataTransferStagingEvidenceCheckpointAnchor>,
    metadata_transfer_staging_actor_closures:
        BTreeMap<(NodeId, u64), MetadataTransferStagingActorClosureCertificate>,
    metadata_transfer_staging_retired_actor_closures:
        BTreeMap<(NodeId, u64), MetadataTransferStagingActorClosureCertificate>,
    metadata_transfer_staging_finalized_floors:
        BTreeMap<(PgId, u64), MetadataTransferStagingFinalizedFloor>,
    metadata_transfer_staging_evidence: BTreeMap<MetadataTransferStagingEvidenceKey, Vec<u8>>,
    history: Vec<ClusterMapHistoryRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataTransferStagingEvidencePageRecord {
    operation_payload: Vec<u8>,
    page_digest: [u8; 32],
    apply_receipt: Vec<u8>,
}

pub(crate) enum MetadataTransferStagingEvidencePageClassification {
    NewAuthorized {
        page_key: (NodeId, u64, u64),
        decoded_entries: Vec<(MetadataTransferStagingEvidenceKey, Vec<u8>)>,
        apply_receipt: Vec<u8>,
    },
    ExactReplay {
        apply_receipt: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataTransferStagingEvidenceCheckpointSegment {
    actor: crate::pg_store::MetadataTransferStagingNodeIdentity,
    first_generation: u64,
    last_generation: u64,
    previous_generation: u64,
    previous_apply_receipt_digest: [u8; 32],
    page_links: Vec<MetadataTransferStagingEvidenceCheckpointPageLink>,
    tip_apply_receipt: Vec<u8>,
    commitments: BTreeMap<MetadataTransferStagingEvidenceKey, [u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataTransferStagingEvidenceCheckpointPageLink {
    page_digest: [u8; 32],
    previous_apply_receipt_digest: [u8; 32],
    apply_receipt_digest: [u8; 32],
    actor_closure_candidate: Option<crate::pg_store::MetadataTransferStagingActorClosureCandidate>,
    entries: Vec<MetadataTransferStagingEvidenceCheckpointPageEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataTransferStagingEvidenceCheckpointPageEntry {
    sequence: u64,
    evidence_key: MetadataTransferStagingEvidenceKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataTransferStagingEvidenceCheckpointAnchor {
    actor: crate::pg_store::MetadataTransferStagingNodeIdentity,
    first_generation: u64,
    last_generation: u64,
    previous_generation: u64,
    previous_apply_receipt_digest: [u8; 32],
    tip_apply_receipt: Vec<u8>,
    source_segment_digest: [u8; 32],
    source_segment_count: u64,
    source_segments_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataTransferStagingFinalizedCheckpointBinding {
    actor_node_id: NodeId,
    actor_node_incarnation: u64,
    actor_endpoint: String,
    first_generation: u64,
    last_generation: u64,
    page_generation: u64,
    page_sequence: u64,
    segment_digest: [u8; 32],
    actor_closure_candidate: Option<crate::pg_store::MetadataTransferStagingActorClosureCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataTransferStagingActorClosureCertificate {
    source_actor: MetadataTransferStagingNodeIdentity,
    source_tip_generation: u64,
    source_tip_page_digest: [u8; 32],
    source_tip_apply_receipt_digest: [u8; 32],
    destination_actor: MetadataTransferStagingNodeIdentity,
    destination_genesis_page_digest: [u8; 32],
    rebound_entry_count: u64,
    rebound_max_sequence: u64,
    rebound_evidence_digest: [u8; 32],
}

type MetadataTransferStagingActorClosureTip =
    (MetadataTransferStagingNodeIdentity, u64, [u8; 32], [u8; 32]);

struct MetadataTransferStagingActorClosureValidationIndex {
    actor_tips: BTreeMap<(NodeId, u64), MetadataTransferStagingActorClosureTip>,
    actor_genesis: BTreeMap<(NodeId, u64), (String, [u8; 32])>,
    actor_entries: BTreeMap<(NodeId, u64), BTreeMap<u64, Vec<u8>>>,
}

fn metadata_transfer_staging_actor_closure_certificate_digest(
    certificate: &MetadataTransferStagingActorClosureCertificate,
) -> [u8; 32] {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    digest_bytes(
        &mut hasher,
        METADATA_TRANSFER_STAGING_ACTOR_CLOSURE_CERTIFICATE_DIGEST_DOMAIN,
    );
    for actor in [&certificate.source_actor, &certificate.destination_actor] {
        digest_u32(&mut hasher, actor.node_id().as_u32());
        digest_u64(&mut hasher, actor.node_incarnation());
        digest_bytes(&mut hasher, actor.endpoint().as_bytes());
    }
    digest_u64(&mut hasher, certificate.source_tip_generation);
    digest_bytes(&mut hasher, &certificate.source_tip_page_digest);
    digest_bytes(&mut hasher, &certificate.source_tip_apply_receipt_digest);
    digest_bytes(&mut hasher, &certificate.destination_genesis_page_digest);
    digest_u64(&mut hasher, certificate.rebound_entry_count);
    digest_u64(&mut hasher, certificate.rebound_max_sequence);
    digest_bytes(&mut hasher, &certificate.rebound_evidence_digest);
    hasher
        .finalize()
        .bytes()
        .try_into()
        .expect("SHA-256 actor-closure certificate digest has fixed length")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataTransferStagingFinalizedFloor {
    transition: UnavailablePgTransitionMutationBinding,
    staging_generation: u64,
    disposition: MetadataTransferStagingCleanupDisposition,
    artifact_digest: [u8; 32],
    artifact_length: u64,
    artifact_format_version: u16,
    publications: Vec<MetadataTransferStagingFinalizedPublicationBinding>,
    tombstones: Vec<MetadataTransferStagingTombstoneBinding>,
    tombstone_set_digest: [u8; 32],
    checkpoint_bindings: BTreeMap<
        MetadataTransferStagingEvidenceKey,
        MetadataTransferStagingFinalizedCheckpointBinding,
    >,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataTransferStagingFinalizedPublicationBinding {
    node_id: NodeId,
    node_incarnation: u64,
    endpoint: String,
    target_epoch: ClusterEpoch,
    transfer: PgMetadataTransferProof,
    evidence_digest: [u8; 32],
}

#[derive(Debug, Clone, Copy)]
struct MetadataTransferStagingFinalizedEvidenceIndexEntry<'a> {
    floor: &'a MetadataTransferStagingFinalizedFloor,
    endpoint: &'a str,
    evidence_digest: [u8; 32],
    target_epoch: Option<ClusterEpoch>,
    transfer: Option<PgMetadataTransferProof>,
}

type MetadataTransferStagingFinalizedEvidenceIndex<'a> = BTreeMap<
    MetadataTransferStagingEvidenceKey,
    MetadataTransferStagingFinalizedEvidenceIndexEntry<'a>,
>;

type MetadataTransferStagingFinalizedCheckpointIndex<'a> = BTreeMap<
    (NodeId, u64, u64, u64, [u8; 32]),
    Vec<(
        &'a MetadataTransferStagingEvidenceKey,
        &'a MetadataTransferStagingFinalizedFloor,
    )>,
>;

#[derive(Debug, Clone, Copy)]
struct MetadataTransferStagingCheckpointSourceSegmentBinding {
    first_generation: u64,
    last_generation: u64,
    previous_generation: u64,
    previous_apply_receipt_digest: [u8; 32],
    source_segment_digest: [u8; 32],
}

fn metadata_transfer_staging_finalized_evidence_index(
    floors: &BTreeMap<(PgId, u64), MetadataTransferStagingFinalizedFloor>,
) -> Result<MetadataTransferStagingFinalizedEvidenceIndex<'_>, String> {
    let mut index = BTreeMap::new();
    for ((pg_id, staging_generation), floor) in floors {
        if floor.publications.len()
            > crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT
                .saturating_mul(floor.transition.destination_acting_set().len())
        {
            return Err(
                "metadata-transfer staging finalized publication exceeds its bounded actor-target index"
                    .to_owned(),
            );
        }
        let mut publication_actor_targets = BTreeSet::new();
        for publication in &floor.publications {
            if !publication_actor_targets.insert((publication.target_epoch, publication.node_id)) {
                return Err(
                    "metadata-transfer staging finalized publication has a duplicate actor-target identity"
                        .to_owned(),
                );
            }
            let key = MetadataTransferStagingEvidenceKey {
                pg_id: *pg_id,
                staging_generation: *staging_generation,
                actor_node_id: publication.node_id,
                actor_node_incarnation: publication.node_incarnation,
                kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                target_epoch: Some(publication.target_epoch),
            };
            if index
                .insert(
                    key,
                    MetadataTransferStagingFinalizedEvidenceIndexEntry {
                        floor,
                        endpoint: &publication.endpoint,
                        evidence_digest: publication.evidence_digest,
                        target_epoch: Some(publication.target_epoch),
                        transfer: Some(publication.transfer),
                    },
                )
                .is_some()
            {
                return Err(
                    "metadata-transfer staging finalized evidence has a duplicate identity"
                        .to_owned(),
                );
            }
        }
        for tombstone in &floor.tombstones {
            let key = MetadataTransferStagingEvidenceKey {
                pg_id: *pg_id,
                staging_generation: *staging_generation,
                actor_node_id: tombstone.node_id,
                actor_node_incarnation: tombstone.node_incarnation,
                kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
                target_epoch: None,
            };
            if index
                .insert(
                    key,
                    MetadataTransferStagingFinalizedEvidenceIndexEntry {
                        floor,
                        endpoint: &tombstone.endpoint,
                        evidence_digest: tombstone.evidence_digest,
                        target_epoch: None,
                        transfer: None,
                    },
                )
                .is_some()
            {
                return Err(
                    "metadata-transfer staging finalized evidence has a duplicate identity"
                        .to_owned(),
                );
            }
        }
        for (key, binding) in &floor.checkpoint_bindings {
            if index.contains_key(key) {
                continue;
            }
            let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
                key.actor_node_id,
                key.actor_node_incarnation,
                binding.actor_endpoint.clone(),
            )
            .map_err(|error| error.to_string())?;
            let bytes = metadata_transfer_staging_finalized_semantic_evidence_bytes(
                floor,
                &actor,
                key.kind,
                key.target_epoch,
            )?;
            let evidence = crate::pg_store::decode_staging_evidence(&bytes)
                .map_err(|error| error.to_string())?;
            if index
                .insert(
                    key.clone(),
                    MetadataTransferStagingFinalizedEvidenceIndexEntry {
                        floor,
                        endpoint: &binding.actor_endpoint,
                        evidence_digest: checksum::sha256::digest(&bytes),
                        target_epoch: evidence.target_epoch(),
                        transfer: evidence.transfer(),
                    },
                )
                .is_some()
            {
                return Err(
                    "metadata-transfer staging finalized replay evidence has a duplicate identity"
                        .to_owned(),
                );
            }
        }
    }
    Ok(index)
}

fn metadata_transfer_staging_finalized_semantic_evidence_bytes(
    floor: &MetadataTransferStagingFinalizedFloor,
    actor: &crate::pg_store::MetadataTransferStagingNodeIdentity,
    kind: crate::pg_store::MetadataTransferStagingEvidenceKind,
    target_epoch: Option<ClusterEpoch>,
) -> Result<Vec<u8>, String> {
    let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        &floor.transition,
        floor.artifact_digest,
        floor.artifact_length,
        floor.artifact_format_version,
    )
    .map_err(|error| error.to_string())?;
    let (source_actor, transfer, expected_digest) = match kind {
        crate::pg_store::MetadataTransferStagingEvidenceKind::Publication => {
            let target_epoch = target_epoch.ok_or_else(|| {
                "metadata-transfer staging finalized publication replay has no target epoch"
                    .to_owned()
            })?;
            let publication = floor
                .publications
                .iter()
                .find(|publication| {
                    publication.node_id == actor.node_id()
                        && publication.target_epoch == target_epoch
                })
                .ok_or_else(|| {
                    "metadata-transfer staging finalized publication replay has no semantic source"
                        .to_owned()
                })?;
            let source_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
                publication.node_id,
                publication.node_incarnation,
                publication.endpoint.clone(),
            )
            .map_err(|error| error.to_string())?;
            (
                source_actor,
                Some(publication.transfer),
                publication.evidence_digest,
            )
        }
        crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone => {
            if target_epoch.is_some() {
                return Err(
                    "metadata-transfer staging finalized tombstone replay has a target epoch"
                        .to_owned(),
                );
            }
            let tombstone = floor
                .tombstones
                .iter()
                .find(|tombstone| tombstone.node_id == actor.node_id())
                .ok_or_else(|| {
                    "metadata-transfer staging finalized tombstone replay has no semantic source"
                        .to_owned()
                })?;
            let source_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
                tombstone.node_id,
                tombstone.node_incarnation,
                tombstone.endpoint.clone(),
            )
            .map_err(|error| error.to_string())?;
            (source_actor, None, tombstone.evidence_digest)
        }
    };
    let source_bytes = crate::pg_store::canonical_metadata_transfer_staging_evidence(
        &source_actor,
        &intent,
        kind,
        target_epoch,
        transfer,
    )
    .map_err(|error| error.to_string())?;
    if checksum::sha256::digest(&source_bytes) != expected_digest {
        return Err(
            "metadata-transfer staging finalized semantic source is not canonical".to_owned(),
        );
    }
    crate::pg_store::canonical_metadata_transfer_staging_evidence(
        actor,
        &intent,
        kind,
        target_epoch,
        transfer,
    )
    .map_err(|error| error.to_string())
}

fn metadata_transfer_staging_finalized_checkpoint_index(
    floors: &BTreeMap<(PgId, u64), MetadataTransferStagingFinalizedFloor>,
) -> Result<MetadataTransferStagingFinalizedCheckpointIndex<'_>, String> {
    let mut index = BTreeMap::new();
    for ((pg_id, staging_generation), floor) in floors {
        for (key, binding) in &floor.checkpoint_bindings {
            if key.pg_id != *pg_id || key.staging_generation != *staging_generation {
                return Err(
                    "metadata-transfer staging finalized checkpoint binding has a foreign floor identity"
                        .to_owned(),
                );
            }
            index
                .entry((
                    binding.actor_node_id,
                    binding.actor_node_incarnation,
                    binding.first_generation,
                    binding.last_generation,
                    binding.segment_digest,
                ))
                .or_insert_with(Vec::new)
                .push((key, floor));
        }
    }
    Ok(index)
}

fn metadata_transfer_staging_checkpoint_source_segments(
    finalized_checkpoints: &MetadataTransferStagingFinalizedCheckpointIndex<'_>,
    actor_node_id: NodeId,
    actor_node_incarnation: u64,
    first_generation: u64,
    last_generation: u64,
) -> Result<Vec<(u64, u64, [u8; 32])>, String> {
    let start = (
        actor_node_id,
        actor_node_incarnation,
        first_generation,
        0,
        [0; 32],
    );
    let end = (
        actor_node_id,
        actor_node_incarnation,
        last_generation,
        u64::MAX,
        [u8::MAX; 32],
    );
    let mut sources = Vec::new();
    let mut next_generation = first_generation;
    for ((_, _, source_first, source_last, source_digest), _) in
        finalized_checkpoints.range(start..=end)
    {
        if *source_first != next_generation || *source_last < *source_first {
            return Err(
                "metadata-transfer staging checkpoint source ranges are not contiguous".to_owned(),
            );
        }
        sources.push((*source_first, *source_last, *source_digest));
        if *source_last == last_generation {
            break;
        }
        next_generation = source_last.checked_add(1).ok_or_else(|| {
            "metadata-transfer staging checkpoint source range overflows".to_owned()
        })?;
    }
    if sources.is_empty()
        || sources.first().map(|source| source.0) != Some(first_generation)
        || sources.last().map(|source| source.1) != Some(last_generation)
        || sources
            .windows(2)
            .any(|pair| pair[0].1.checked_add(1) != Some(pair[1].0))
    {
        return Err(
            "metadata-transfer staging checkpoint source ranges do not cover the anchor".to_owned(),
        );
    }
    Ok(sources)
}

#[derive(Clone)]
struct MetadataTransferStagingActorChainTip {
    actor: MetadataTransferStagingNodeIdentity,
    generation: u64,
    page_digest: [u8; 32],
    apply_receipt_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MetadataTransferStagingEvidenceKey {
    pg_id: PgId,
    staging_generation: u64,
    actor_node_id: NodeId,
    actor_node_incarnation: u64,
    kind: crate::pg_store::MetadataTransferStagingEvidenceKind,
    target_epoch: Option<ClusterEpoch>,
}

fn metadata_transfer_staging_evidence_key(
    evidence: &crate::pg_store::MetadataTransferStagingEvidence,
) -> MetadataTransferStagingEvidenceKey {
    MetadataTransferStagingEvidenceKey {
        pg_id: evidence.intent().pg_id(),
        staging_generation: evidence.intent().staging_generation(),
        actor_node_id: evidence.actor().node_id(),
        actor_node_incarnation: evidence.actor().node_incarnation(),
        kind: evidence.kind(),
        target_epoch: evidence.target_epoch(),
    }
}

fn metadata_transfer_staging_finalized_generation(
    floors: &BTreeMap<(PgId, u64), MetadataTransferStagingFinalizedFloor>,
    pg_id: PgId,
) -> Option<u64> {
    floors
        .range((pg_id, 0)..=(pg_id, u64::MAX))
        .next_back()
        .map(|(key, _)| key.1)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailablePgPayloadReadiness {
    pub(crate) pg_id: PgId,
    pub(crate) transition_epoch: ClusterEpoch,
    pub(crate) destination_epoch: ClusterEpoch,
    pub(crate) topology_generation: u64,
    pub(crate) topology_digest: [u8; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
    pub(crate) ready_at_ms: u64,
    pub(crate) destinations: Vec<UnavailablePgPayloadDestinationReadiness>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailablePgPayloadDestinationReadiness {
    pub(crate) node_id: NodeId,
    pub(crate) node_incarnation: u64,
    pub(crate) endpoint: String,
    pub(crate) lease_deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailablePgTransitionBeginAuthorization {
    pub(crate) begin_at_ms: u64,
    pub(crate) unavailable_node: NodeUnavailableObservation,
    pub(crate) source_route: HistoricalPgRouteRecord,
    pub(crate) source_metadata_floor: PgMetadataProof,
    pub(crate) source_metadata_floor_epoch: Option<ClusterEpoch>,
    pub(crate) source_metadata_floor_imported: bool,
    pub(crate) source_node_id: NodeId,
    pub(crate) source_node_incarnation: u64,
    pub(crate) source_endpoint: String,
    pub(crate) source_lease_deadline_ms: u64,
    pub(crate) source_observed_at_ms: u64,
    pub(crate) source_metadata_proof: PgMetadataProof,
    pub(crate) replacement_node_id: NodeId,
    pub(crate) replacement_node_incarnation: u64,
    pub(crate) replacement_endpoint: String,
    pub(crate) replacement_lease_deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailablePgPlacementTransition {
    pg_id: PgId,
    transition_epoch: ClusterEpoch,
    predecessor_transition_epoch: Option<ClusterEpoch>,
    topology_generation: u64,
    topology_digest: [u8; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
    source_epoch: ClusterEpoch,
    source_acting_set: Vec<NodeId>,
    source_node_id: NodeId,
    begin_authorization: UnavailablePgTransitionBeginAuthorization,
    unavailable_node: NodeUnavailableObservation,
    grace_cutoff_ms: u64,
    destination_acting_set: Vec<NodeId>,
    destination_epoch: Option<ClusterEpoch>,
    destination_route: Option<HistoricalPgRouteRecord>,
    payload_readiness: Option<UnavailablePgPayloadReadiness>,
    completion: Option<ReadyPgPeeringCompletion>,
    begin_batch_receipt: UnavailablePgTransitionBatchReceipt,
    staging_authorization: Option<UnavailablePgStagingIntentAuthorization>,
    destination_install: Option<UnavailablePgDestinationInstall>,
    completion_batch_receipt: Option<UnavailablePgTransitionBatchReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailablePgStagingIntentAuthorization {
    staging_generation: u64,
    artifact_target_epoch: ClusterEpoch,
    artifact_digest: [u8; 32],
    artifact_length: u64,
    artifact_format_version: u16,
    batch_receipt: UnavailablePgTransitionBatchReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnavailablePgDestinationInstall {
    transfer: PgMetadataTransferProof,
    publications: Vec<UnavailablePgStagingPublicationBinding>,
    batch_receipt: UnavailablePgTransitionBatchReceipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum UnavailablePgTransitionBatchStage {
    Begin,
    StagingAuthorization,
    DestinationInstall,
    Completion,
}

impl UnavailablePgTransitionBatchStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Begin => "begin",
            Self::StagingAuthorization => "staging-authorization",
            Self::DestinationInstall => "destination-install",
            Self::Completion => "completion",
        }
    }

    fn from_str(value: &str) -> Result<Self, String> {
        match value {
            "begin" => Ok(Self::Begin),
            "staging-authorization" => Ok(Self::StagingAuthorization),
            "destination-install" => Ok(Self::DestinationInstall),
            "completion" => Ok(Self::Completion),
            _ => Err(format!(
                "unknown unavailable PG transition batch stage {value:?}"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UnavailablePgTransitionBatchReceiptIdentity {
    stage: UnavailablePgTransitionBatchStage,
    member_pg_ids: Vec<PgId>,
    members_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UnavailablePgTransitionBatchReceipt {
    identity: UnavailablePgTransitionBatchReceiptIdentity,
    source_epoch: ClusterEpoch,
    target_epoch: ClusterEpoch,
}

#[derive(Debug)]
enum ValidatedUnavailablePgTransitionBegin {
    ExactReplay {
        pg_id: PgId,
    },
    Apply {
        requested: Box<UnavailablePgPlacementTransition>,
        previous_primary_lease: Option<PreviousPrimaryLease>,
        fenced_primary_lease_deadline_ms: u64,
    },
}

impl ValidatedUnavailablePgTransitionBegin {
    fn pg_id(&self) -> PgId {
        match self {
            Self::ExactReplay { pg_id } => *pg_id,
            Self::Apply { requested, .. } => requested.pg_id,
        }
    }
}

#[derive(Debug)]
enum ValidatedUnavailablePgTransitionCompletion {
    ExactReplay {
        pg_id: PgId,
    },
    Apply {
        readiness: Box<UnavailablePgPayloadReadiness>,
        completion: ReadyPgPeeringCompletion,
        batch_identity: UnavailablePgTransitionBatchReceiptIdentity,
    },
}

#[derive(Debug)]
enum ValidatedUnavailablePgStagingIntentAuthorization {
    ExactReplay {
        pg_id: PgId,
    },
    Apply {
        pg_id: PgId,
        authorization: UnavailablePgStagingIntentAuthorization,
    },
}

#[derive(Debug)]
enum ValidatedUnavailablePgDestinationInstall {
    ExactReplay {
        pg_id: PgId,
    },
    Apply {
        pg_id: PgId,
        transfer: PgMetadataTransferProof,
        publications: Vec<UnavailablePgStagingPublicationBinding>,
        batch_identity: UnavailablePgTransitionBatchReceiptIdentity,
    },
}

impl ValidatedUnavailablePgDestinationInstall {
    fn pg_id(&self) -> PgId {
        match self {
            Self::ExactReplay { pg_id } | Self::Apply { pg_id, .. } => *pg_id,
        }
    }
}

impl ValidatedUnavailablePgStagingIntentAuthorization {
    fn pg_id(&self) -> PgId {
        match self {
            Self::ExactReplay { pg_id } | Self::Apply { pg_id, .. } => *pg_id,
        }
    }
}

impl ValidatedUnavailablePgTransitionCompletion {
    fn pg_id(&self) -> PgId {
        match self {
            Self::ExactReplay { pg_id } => *pg_id,
            Self::Apply { readiness, .. } => readiness.pg_id,
        }
    }
}

fn validate_canonical_unavailable_pg_batch(
    kind: &str,
    pg_ids: impl IntoIterator<Item = PgId>,
) -> Result<(), ControlPlaneError> {
    let mut previous = None;
    let mut count = 0_usize;
    for pg_id in pg_ids {
        count = count.saturating_add(1);
        if count > MAX_UNAVAILABLE_PG_TRANSITION_BATCH {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "unavailable placement {kind} batch exceeds the {} member limit",
                    MAX_UNAVAILABLE_PG_TRANSITION_BATCH
                ),
            });
        }
        if previous.is_some_and(|previous_pg_id| previous_pg_id >= pg_id) {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "unavailable placement {kind} batch PG IDs are not strictly increasing"
                ),
            });
        }
        previous = Some(pg_id);
    }
    if count == 0 {
        return Err(ControlPlaneError::CommandDecode {
            message: format!("unavailable placement {kind} batch is empty"),
        });
    }
    Ok(())
}

fn unavailable_pg_transition_begin_batch_identity(
    requests: &[UnavailablePgTransitionBeginRequest],
    expected_transition_epoch: ClusterEpoch,
    begin_at_ms: u64,
) -> UnavailablePgTransitionBatchReceiptIdentity {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    digest_bytes(
        &mut hasher,
        UNAVAILABLE_PG_TRANSITION_BATCH_RECEIPT_DIGEST_DOMAIN,
    );
    digest_u8(&mut hasher, 1);
    digest_u64(&mut hasher, expected_transition_epoch.get());
    digest_u64(&mut hasher, begin_at_ms);
    digest_len(&mut hasher, requests.len());
    for request in requests {
        digest_u32(&mut hasher, request.pg_id.get());
        digest_option_u64(
            &mut hasher,
            request.predecessor_transition_epoch.map(ClusterEpoch::get),
        );
        digest_u64(&mut hasher, request.source_epoch.get());
        digest_node_ids(&mut hasher, &request.source_acting_set);
        digest_u32(&mut hasher, request.source_node_id.as_u32());
        digest_bytes(
            &mut hasher,
            format_unavailable_pg_transition_begin_authorization(&request.begin_authorization)
                .as_bytes(),
        );
        digest_unavailable_node_observation(&mut hasher, &request.unavailable_node);
        digest_u64(&mut hasher, request.grace_cutoff_ms);
        digest_u64(&mut hasher, request.topology_generation);
        digest_bytes(&mut hasher, &request.topology_digest);
        digest_node_ids(&mut hasher, &request.destination_acting_set);
    }
    UnavailablePgTransitionBatchReceiptIdentity {
        stage: UnavailablePgTransitionBatchStage::Begin,
        member_pg_ids: requests.iter().map(|request| request.pg_id).collect(),
        members_digest: hasher
            .finalize()
            .bytes()
            .try_into()
            .expect("SHA-256 unavailable transition batch digest must contain 32 bytes"),
    }
}

fn unavailable_pg_transition_completion_batch_identity(
    requests: &[UnavailablePgTransitionCompletionRequest],
    ready_at_ms: u64,
) -> UnavailablePgTransitionBatchReceiptIdentity {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    digest_bytes(
        &mut hasher,
        UNAVAILABLE_PG_TRANSITION_BATCH_RECEIPT_DIGEST_DOMAIN,
    );
    digest_u8(&mut hasher, 3);
    digest_u64(&mut hasher, ready_at_ms);
    digest_len(&mut hasher, requests.len());
    for request in requests {
        digest_unavailable_pg_transition_binding(&mut hasher, &request.unavailable_transition);
        digest_u32(&mut hasher, request.pg_id.get());
        digest_u64(&mut hasher, request.transition_epoch.get());
        digest_u64(&mut hasher, request.destination_epoch.get());
        digest_u64(&mut hasher, request.topology_generation);
        digest_bytes(&mut hasher, &request.topology_digest);
        digest_len(&mut hasher, request.destinations.len());
        for destination in &request.destinations {
            digest_u32(&mut hasher, destination.node_id.as_u32());
            digest_u64(&mut hasher, destination.node_incarnation);
            digest_bytes(&mut hasher, destination.endpoint.as_bytes());
            digest_u64(&mut hasher, destination.lease_deadline_ms);
        }
        digest_u32(&mut hasher, request.completion.pg_id.get());
        digest_u32(&mut hasher, request.completion.primary.as_u32());
        digest_u64(&mut hasher, request.completion.node_incarnation);
        digest_pg_metadata_proof(&mut hasher, request.completion.active_metadata_proof);
        digest_u64(
            &mut hasher,
            request.completion.active_metadata_proof_epoch.get(),
        );
    }
    UnavailablePgTransitionBatchReceiptIdentity {
        stage: UnavailablePgTransitionBatchStage::Completion,
        member_pg_ids: requests.iter().map(|request| request.pg_id).collect(),
        members_digest: hasher
            .finalize()
            .bytes()
            .try_into()
            .expect("SHA-256 unavailable transition batch digest must contain 32 bytes"),
    }
}

fn unavailable_pg_staging_authorization_batch_identity(
    requests: &[UnavailablePgStagingIntentAuthorizationRequest],
) -> UnavailablePgTransitionBatchReceiptIdentity {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    digest_bytes(
        &mut hasher,
        UNAVAILABLE_PG_TRANSITION_BATCH_RECEIPT_DIGEST_DOMAIN,
    );
    digest_u8(&mut hasher, 2);
    digest_len(&mut hasher, requests.len());
    for request in requests {
        digest_unavailable_pg_transition_binding(&mut hasher, &request.unavailable_transition);
        digest_u64(&mut hasher, request.staging_generation);
        digest_u64(&mut hasher, request.artifact_target_epoch.get());
        digest_bytes(&mut hasher, &request.artifact_digest);
        digest_u64(&mut hasher, request.artifact_length);
        digest_u16(&mut hasher, request.artifact_format_version);
    }
    UnavailablePgTransitionBatchReceiptIdentity {
        stage: UnavailablePgTransitionBatchStage::StagingAuthorization,
        member_pg_ids: requests
            .iter()
            .map(|request| request.unavailable_transition.pg_id())
            .collect(),
        members_digest: hasher
            .finalize()
            .bytes()
            .try_into()
            .expect("SHA-256 staging authorization batch digest must contain 32 bytes"),
    }
}

pub(crate) fn unavailable_pg_staging_authorization_members_digest(
    requests: &[UnavailablePgStagingIntentAuthorizationRequest],
) -> [u8; 32] {
    unavailable_pg_staging_authorization_batch_identity(requests).members_digest
}

fn unavailable_pg_destination_install_batch_identity(
    requests: &[UnavailablePgTransitionInstallRequest],
    expected_destination_epoch: ClusterEpoch,
) -> UnavailablePgTransitionBatchReceiptIdentity {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    digest_bytes(
        &mut hasher,
        UNAVAILABLE_PG_TRANSITION_BATCH_RECEIPT_DIGEST_DOMAIN,
    );
    digest_u8(&mut hasher, 4);
    digest_u64(&mut hasher, expected_destination_epoch.get());
    digest_len(&mut hasher, requests.len());
    for request in requests {
        digest_unavailable_pg_transition_binding(&mut hasher, &request.unavailable_transition);
        digest_u64(&mut hasher, request.transfer.source_epoch().get());
        digest_pg_metadata_proof(&mut hasher, request.transfer.source_metadata_proof());
        digest_pg_metadata_proof(&mut hasher, request.transfer.metadata_proof());
        digest_u64(&mut hasher, request.expected_destination_epoch.get());
        digest_len(&mut hasher, request.publications.len());
        for publication in &request.publications {
            digest_u32(&mut hasher, publication.node_id.as_u32());
            digest_u64(&mut hasher, publication.node_incarnation);
            digest_bytes(&mut hasher, publication.endpoint.as_bytes());
            digest_bytes(&mut hasher, &publication.evidence_digest);
        }
    }
    UnavailablePgTransitionBatchReceiptIdentity {
        stage: UnavailablePgTransitionBatchStage::DestinationInstall,
        member_pg_ids: requests
            .iter()
            .map(|request| request.unavailable_transition.pg_id())
            .collect(),
        members_digest: hasher
            .finalize()
            .bytes()
            .try_into()
            .expect("SHA-256 destination install batch digest must contain 32 bytes"),
    }
}

fn unavailable_pg_staging_authorization_request_from_durable(
    transition: &UnavailablePgPlacementTransition,
) -> Option<UnavailablePgStagingIntentAuthorizationRequest> {
    let authorization = transition.staging_authorization.as_ref()?;
    Some(UnavailablePgStagingIntentAuthorizationRequest {
        unavailable_transition: UnavailablePgTransitionMutationBinding::new(
            transition.pg_id,
            transition.transition_epoch,
            transition.source_epoch,
            transition.source_acting_set.clone(),
            transition.destination_acting_set.clone(),
        ),
        staging_generation: authorization.staging_generation,
        artifact_target_epoch: authorization.artifact_target_epoch,
        artifact_digest: authorization.artifact_digest,
        artifact_length: authorization.artifact_length,
        artifact_format_version: authorization.artifact_format_version,
    })
}

fn unavailable_pg_destination_install_request_from_durable(
    transition: &UnavailablePgPlacementTransition,
) -> Option<UnavailablePgTransitionInstallRequest> {
    let install = transition.destination_install.as_ref()?;
    Some(UnavailablePgTransitionInstallRequest {
        unavailable_transition: UnavailablePgTransitionMutationBinding::new(
            transition.pg_id,
            transition.transition_epoch,
            transition.source_epoch,
            transition.source_acting_set.clone(),
            transition.destination_acting_set.clone(),
        ),
        transfer: install.transfer,
        expected_destination_epoch: transition.destination_epoch?,
        publications: install.publications.clone(),
    })
}

fn unavailable_pg_transition_begin_request_from_durable(
    transition: &UnavailablePgPlacementTransition,
) -> UnavailablePgTransitionBeginRequest {
    UnavailablePgTransitionBeginRequest {
        pg_id: transition.pg_id,
        predecessor_transition_epoch: transition.predecessor_transition_epoch,
        source_epoch: transition.source_epoch,
        source_acting_set: transition.source_acting_set.clone(),
        source_node_id: transition.source_node_id,
        begin_authorization: transition.begin_authorization.clone(),
        unavailable_node: transition.unavailable_node.clone(),
        grace_cutoff_ms: transition.grace_cutoff_ms,
        topology_generation: transition.topology_generation,
        topology_digest: transition.topology_digest,
        destination_acting_set: transition.destination_acting_set.clone(),
    }
}

fn unavailable_pg_transition_completion_request_from_durable(
    transition: &UnavailablePgPlacementTransition,
) -> Result<(UnavailablePgTransitionCompletionRequest, u64), String> {
    let readiness = transition.payload_readiness.as_ref().ok_or_else(|| {
        format!(
            "unavailable PG transition {} has a completion receipt without payload readiness",
            transition.pg_id.get()
        )
    })?;
    let completion = transition.completion.ok_or_else(|| {
        format!(
            "unavailable PG transition {} has a completion receipt without completion evidence",
            transition.pg_id.get()
        )
    })?;
    Ok((
        UnavailablePgTransitionCompletionRequest {
            unavailable_transition: UnavailablePgTransitionMutationBinding::new(
                transition.pg_id,
                transition.transition_epoch,
                transition.source_epoch,
                transition.source_acting_set.clone(),
                transition.destination_acting_set.clone(),
            ),
            pg_id: transition.pg_id,
            transition_epoch: transition.transition_epoch,
            destination_epoch: readiness.destination_epoch,
            topology_generation: transition.topology_generation,
            topology_digest: transition.topology_digest,
            destinations: readiness.destinations.clone(),
            completion,
        },
        readiness.ready_at_ms,
    ))
}

fn digest_node_ids(hasher: &mut ChecksumHasher, node_ids: &[NodeId]) {
    digest_len(hasher, node_ids.len());
    for node_id in node_ids {
        digest_u32(hasher, node_id.as_u32());
    }
}

fn digest_unavailable_node_observation(
    hasher: &mut ChecksumHasher,
    observation: &NodeUnavailableObservation,
) {
    digest_u32(hasher, observation.node_id.as_u32());
    digest_u64(hasher, observation.node_incarnation);
    digest_bytes(hasher, observation.endpoint.as_bytes());
    digest_u64(hasher, observation.lease_deadline_ms);
    digest_u64(hasher, observation.observed_at_ms);
}

fn digest_unavailable_pg_transition_binding(
    hasher: &mut ChecksumHasher,
    binding: &UnavailablePgTransitionMutationBinding,
) {
    digest_u32(hasher, binding.pg_id.get());
    digest_u64(hasher, binding.transition_epoch.get());
    digest_u64(hasher, binding.source_epoch.get());
    digest_node_ids(hasher, &binding.source_acting_set);
    digest_node_ids(hasher, &binding.destination_acting_set);
}

fn metadata_transfer_staging_cleanup_digest(
    transition: &UnavailablePgTransitionMutationBinding,
    staging_generation: u64,
    disposition: MetadataTransferStagingCleanupDisposition,
    artifact_digest: [u8; 32],
    artifact_length: u64,
    artifact_format_version: u16,
    tombstones: &[MetadataTransferStagingTombstoneBinding],
) -> [u8; 32] {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    digest_bytes(&mut hasher, METADATA_TRANSFER_STAGING_CLEANUP_DIGEST_DOMAIN);
    digest_unavailable_pg_transition_binding(&mut hasher, transition);
    digest_u64(&mut hasher, staging_generation);
    match disposition {
        MetadataTransferStagingCleanupDisposition::Completed => digest_u8(&mut hasher, 0),
        MetadataTransferStagingCleanupDisposition::Superseded {
            successor_transition_epoch,
        } => {
            digest_u8(&mut hasher, 1);
            digest_u64(&mut hasher, successor_transition_epoch.get());
        }
    }
    digest_bytes(&mut hasher, &artifact_digest);
    digest_u64(&mut hasher, artifact_length);
    digest_u16(&mut hasher, artifact_format_version);
    digest_len(&mut hasher, tombstones.len());
    for tombstone in tombstones {
        digest_u32(&mut hasher, tombstone.node_id.as_u32());
        digest_u64(&mut hasher, tombstone.node_incarnation);
        digest_bytes(&mut hasher, tombstone.endpoint.as_bytes());
        digest_bytes(&mut hasher, &tombstone.evidence_digest);
    }
    hasher
        .finalize()
        .bytes()
        .try_into()
        .expect("SHA-256 staging cleanup digest must contain 32 bytes")
}

fn metadata_transfer_staging_checkpoint_segment_digest(
    segment: &MetadataTransferStagingEvidenceCheckpointSegment,
) -> [u8; 32] {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    digest_bytes(
        &mut hasher,
        METADATA_TRANSFER_STAGING_CHECKPOINT_SEGMENT_DIGEST_DOMAIN,
    );
    digest_bytes(
        &mut hasher,
        format_metadata_transfer_staging_evidence_checkpoint_segment(segment).as_bytes(),
    );
    hasher
        .finalize()
        .bytes()
        .try_into()
        .expect("SHA-256 staging checkpoint segment digest must contain 32 bytes")
}

fn metadata_transfer_staging_checkpoint_source_segments_digest(
    segments: &[(u64, u64, [u8; 32])],
) -> [u8; 32] {
    let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
    digest_bytes(
        &mut hasher,
        METADATA_TRANSFER_STAGING_CHECKPOINT_SOURCE_SEGMENTS_DIGEST_DOMAIN,
    );
    digest_u64(
        &mut hasher,
        u64::try_from(segments.len()).expect("source segment count fits u64"),
    );
    for (first_generation, last_generation, segment_digest) in segments {
        digest_u64(&mut hasher, *first_generation);
        digest_u64(&mut hasher, *last_generation);
        digest_bytes(&mut hasher, segment_digest);
    }
    hasher
        .finalize()
        .bytes()
        .try_into()
        .expect("SHA-256 staging checkpoint source-segment digest must contain 32 bytes")
}

fn validate_metadata_transfer_staging_checkpoint_coalescing_source_count(
    source_count: usize,
) -> Result<(), ControlPlaneError> {
    if source_count < 2 {
        return Err(ControlPlaneError::CommandDecode {
            message: "metadata-transfer staging checkpoint anchor coalescing source does not match"
                .to_owned(),
        });
    }
    if source_count > MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COALESCED_ANCHORS {
        return Err(ControlPlaneError::CommandDecode {
            message: format!(
                "metadata-transfer staging checkpoint anchor coalescing exceeds the {} direct-anchor limit",
                MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COALESCED_ANCHORS
            ),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UnavailablePgReconciliationCursor {
    after_pg_id: Option<PgId>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum MetadataTransferStagingMaintenancePhase {
    #[default]
    ClosureRetirement,
    PageCheckpoint,
    SegmentCollapse,
    AnchorCoalescing,
}

impl MetadataTransferStagingMaintenancePhase {
    fn next(self) -> Self {
        match self {
            Self::ClosureRetirement => Self::PageCheckpoint,
            Self::PageCheckpoint => Self::SegmentCollapse,
            Self::SegmentCollapse => Self::AnchorCoalescing,
            Self::AnchorCoalescing => Self::ClosureRetirement,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct MetadataTransferStagingMaintenanceCursor {
    next_phase: MetadataTransferStagingMaintenancePhase,
    after_closure: Option<(NodeId, u64)>,
    closure_high_water: Option<(NodeId, u64)>,
    after_page: Option<(NodeId, u64, u64)>,
    page_high_water: Option<(NodeId, u64, u64)>,
    after_segment: Option<(NodeId, u64, u64)>,
    segment_high_water: Option<(NodeId, u64, u64)>,
    after_anchor: Option<(NodeId, u64, u64)>,
    anchor_high_water: Option<(NodeId, u64, u64)>,
}

impl MetadataTransferStagingMaintenanceCursor {
    pub(crate) fn start() -> Self {
        Self::default()
    }
}

fn metadata_transfer_staging_maintenance_sweep_high_water<K: Copy + Ord>(
    after: &mut Option<K>,
    high_water: &mut Option<K>,
    current_last: Option<K>,
) -> Option<K> {
    if after
        .zip(*high_water)
        .is_some_and(|(after, high_water)| after >= high_water)
    {
        *after = None;
        *high_water = None;
    }
    if high_water.is_none() {
        *high_water = current_last;
    }
    *high_water
}

impl UnavailablePgReconciliationCursor {
    #[must_use]
    pub fn start() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn after_pg_id(self) -> Option<PgId> {
        self.after_pg_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailablePgReconciliationWork {
    binding: UnavailablePgTransitionMutationBinding,
    stage: UnavailablePgReconciliationStage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailablePgTransitionMutationBinding {
    pg_id: PgId,
    transition_epoch: ClusterEpoch,
    source_epoch: ClusterEpoch,
    source_acting_set: Vec<NodeId>,
    destination_acting_set: Vec<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnavailablePgReconciliationStage {
    MetadataTransfer,
    PayloadReadiness,
    StagingCleanup,
}

impl UnavailablePgReconciliationWork {
    pub(crate) fn new(
        pg_id: PgId,
        transition_epoch: ClusterEpoch,
        source_epoch: ClusterEpoch,
        source_acting_set: Vec<NodeId>,
        destination_acting_set: Vec<NodeId>,
        stage: UnavailablePgReconciliationStage,
    ) -> Self {
        Self {
            binding: UnavailablePgTransitionMutationBinding::new(
                pg_id,
                transition_epoch,
                source_epoch,
                source_acting_set,
                destination_acting_set,
            ),
            stage,
        }
    }

    pub(crate) fn from_transition(
        transition: &UnavailablePgPlacementTransition,
        stage: UnavailablePgReconciliationStage,
    ) -> Self {
        Self::new(
            transition.pg_id,
            transition.transition_epoch,
            transition.source_epoch,
            transition.source_acting_set.clone(),
            transition.destination_acting_set.clone(),
            stage,
        )
    }

    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.binding.pg_id
    }

    #[must_use]
    pub fn transition_epoch(&self) -> ClusterEpoch {
        self.binding.transition_epoch
    }

    #[must_use]
    pub fn source_epoch(&self) -> ClusterEpoch {
        self.binding.source_epoch
    }

    #[must_use]
    pub fn source_acting_set(&self) -> &[NodeId] {
        &self.binding.source_acting_set
    }

    #[must_use]
    pub fn destination_acting_set(&self) -> &[NodeId] {
        &self.binding.destination_acting_set
    }

    #[must_use]
    pub fn stage(&self) -> UnavailablePgReconciliationStage {
        self.stage
    }

    #[must_use]
    pub fn mutation_binding(&self) -> &UnavailablePgTransitionMutationBinding {
        &self.binding
    }
}

impl UnavailablePgTransitionMutationBinding {
    pub(crate) fn new(
        pg_id: PgId,
        transition_epoch: ClusterEpoch,
        source_epoch: ClusterEpoch,
        source_acting_set: Vec<NodeId>,
        destination_acting_set: Vec<NodeId>,
    ) -> Self {
        Self {
            pg_id,
            transition_epoch,
            source_epoch,
            source_acting_set,
            destination_acting_set,
        }
    }

    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn transition_epoch(&self) -> ClusterEpoch {
        self.transition_epoch
    }

    #[must_use]
    pub fn source_epoch(&self) -> ClusterEpoch {
        self.source_epoch
    }

    #[must_use]
    pub fn source_acting_set(&self) -> &[NodeId] {
        &self.source_acting_set
    }

    #[must_use]
    pub fn destination_acting_set(&self) -> &[NodeId] {
        &self.destination_acting_set
    }

    pub(crate) fn matches_transition(&self, transition: &UnavailablePgPlacementTransition) -> bool {
        self.pg_id == transition.pg_id
            && self.transition_epoch == transition.transition_epoch
            && self.source_epoch == transition.source_epoch
            && self.source_acting_set == transition.source_acting_set
            && self.destination_acting_set == transition.destination_acting_set
    }
}

pub(crate) enum UnavailablePgReconciliationCandidate {
    Begin {
        pg_id: PgId,
        unavailable_node_id: NodeId,
    },
    Resume(UnavailablePgReconciliationWork),
}

pub(crate) struct UnavailablePgReconciliationScan {
    pub(crate) candidate: Option<UnavailablePgReconciliationCandidate>,
    pub(crate) next_cursor: UnavailablePgReconciliationCursor,
}

pub(crate) struct UnavailablePgReconciliationBatchScan {
    pub(crate) candidates: Vec<UnavailablePgReconciliationCandidate>,
    pub(crate) cleanup_fallbacks: Vec<UnavailablePgReconciliationWork>,
    pub(crate) next_cursor: UnavailablePgReconciliationCursor,
}

pub(crate) struct PreparedUnavailablePgBeginBatch {
    pub(crate) command: Option<ControlPlaneCommand>,
    pub(crate) included: Vec<(PgId, NodeId)>,
    pub(crate) rejected: Vec<(PgId, ControlPlaneError)>,
}

pub(crate) struct PreparedUnavailablePgCompletionBatch {
    pub(crate) command: Option<ControlPlaneCommand>,
    pub(crate) included: Vec<UnavailablePgReconciliationWork>,
    pub(crate) rejected: Vec<(UnavailablePgReconciliationWork, ControlPlaneError)>,
}

pub(crate) struct PreparedUnavailablePgInstallBatch {
    pub(crate) command: Option<ControlPlaneCommand>,
    pub(crate) included: Vec<UnavailablePgTransitionInstallRequest>,
    pub(crate) rejected: Vec<(UnavailablePgTransitionInstallRequest, ControlPlaneError)>,
}

pub(crate) struct UnavailablePgReconciliationPollBatch {
    pub(crate) work: Vec<UnavailablePgReconciliationWork>,
    pub(crate) cleanup_fallbacks: Vec<UnavailablePgReconciliationWork>,
    pub(crate) rejected: Vec<(PgId, ControlPlaneError)>,
}

pub(crate) struct UnavailablePgReconciliationCompletionBatch {
    pub(crate) completed: Vec<UnavailablePgReconciliationWork>,
    pub(crate) rejected: Vec<(UnavailablePgReconciliationWork, ControlPlaneError)>,
    pub(crate) rederive: Vec<UnavailablePgReconciliationWork>,
    pub(crate) snapshot: ClusterControlSnapshot,
}

impl UnavailablePgPlacementTransition {
    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn transition_epoch(&self) -> ClusterEpoch {
        self.transition_epoch
    }

    #[must_use]
    pub fn source_epoch(&self) -> ClusterEpoch {
        self.source_epoch
    }

    #[must_use]
    pub fn predecessor_transition_epoch(&self) -> Option<ClusterEpoch> {
        self.predecessor_transition_epoch
    }

    #[must_use]
    pub fn source_acting_set(&self) -> &[NodeId] {
        &self.source_acting_set
    }

    #[must_use]
    pub fn source_node_id(&self) -> NodeId {
        self.source_node_id
    }

    #[must_use]
    pub fn unavailable_node(&self) -> &NodeUnavailableObservation {
        &self.unavailable_node
    }

    #[must_use]
    pub fn grace_cutoff_ms(&self) -> u64 {
        self.grace_cutoff_ms
    }

    #[must_use]
    pub fn destination_acting_set(&self) -> &[NodeId] {
        &self.destination_acting_set
    }

    #[must_use]
    pub fn destination_epoch(&self) -> Option<ClusterEpoch> {
        self.destination_epoch
    }

    #[must_use]
    pub fn payload_readiness(&self) -> Option<&UnavailablePgPayloadReadiness> {
        self.payload_readiness.as_ref()
    }
}

impl ClusterControlSnapshot {
    pub(crate) fn metadata_transfer_staging_retention_metrics(
        &self,
    ) -> observability::MetadataTransferStagingRetentionMetricSnapshot {
        let bounded_len = |len: usize| u64::try_from(len).unwrap_or(u64::MAX);
        observability::MetadataTransferStagingRetentionMetricSnapshot {
            retained_page_depth: bounded_len(self.metadata_transfer_staging_evidence_pages.len()),
            retained_segment_depth: bounded_len(
                self.metadata_transfer_staging_evidence_checkpoint_segments
                    .len(),
            ),
            retained_anchor_depth: bounded_len(
                self.metadata_transfer_staging_evidence_checkpoint_anchors
                    .len(),
            ),
            retained_evidence_depth: bounded_len(self.metadata_transfer_staging_evidence.len()),
            finalized_floor_depth: bounded_len(
                self.metadata_transfer_staging_finalized_floors.len(),
            ),
            active_closure_depth: bounded_len(self.metadata_transfer_staging_actor_closures.len()),
            retired_closure_depth: bounded_len(
                self.metadata_transfer_staging_retired_actor_closures.len(),
            ),
            prune_applied_total: 0,
        }
    }

    pub(crate) fn next_cluster_epoch(&self) -> Result<ClusterEpoch, ControlPlaneError> {
        next_epoch(self.cluster_epoch)
    }

    #[cfg(test)]
    pub(crate) fn metadata_transfer_staging_is_finalized(
        &self,
        work: &UnavailablePgReconciliationWork,
    ) -> bool {
        self.metadata_transfer_staging_finalized_floors
            .contains_key(&(work.pg_id(), work.transition_epoch().get()))
    }

    #[cfg(test)]
    pub(crate) fn latest_retained_unavailable_pg_transition(
        &self,
        pg_id: PgId,
    ) -> Option<&UnavailablePgPlacementTransition> {
        self.retained_unavailable_pg_placement_transitions
            .range((
                std::ops::Bound::Included((pg_id, ClusterEpoch::INITIAL)),
                std::ops::Bound::Included((pg_id, self.cluster_epoch)),
            ))
            .next_back()
            .map(|(_, transition)| transition)
    }

    pub(crate) fn empty() -> Self {
        Self {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::INITIAL,
            initial_topology: None,
            max_committed_timestamp_ms: None,
            lease_grant_horizon: None,
            nodes: BTreeMap::new(),
            pgs: BTreeMap::new(),
            unavailable_node_observations: BTreeMap::new(),
            unavailable_pg_placement_transitions: BTreeMap::new(),
            retained_unavailable_pg_placement_transitions: BTreeMap::new(),
            metadata_transfer_staging_evidence_pages: BTreeMap::new(),
            metadata_transfer_staging_evidence_checkpoint_segments: BTreeMap::new(),
            metadata_transfer_staging_evidence_checkpoint_anchors: BTreeMap::new(),
            metadata_transfer_staging_actor_closures: BTreeMap::new(),
            metadata_transfer_staging_retired_actor_closures: BTreeMap::new(),
            metadata_transfer_staging_finalized_floors: BTreeMap::new(),
            metadata_transfer_staging_evidence: BTreeMap::new(),
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

    #[cfg(test)]
    pub(crate) fn test_rebind_singleton_staging_artifact_target_epoch(
        &mut self,
        pg_id: PgId,
        artifact_target_epoch: ClusterEpoch,
    ) {
        let transition = self
            .unavailable_pg_placement_transitions
            .get_mut(&pg_id)
            .expect("test staging transition is active");
        let authorization = transition
            .staging_authorization
            .as_mut()
            .expect("test staging transition is authorized");
        assert_eq!(authorization.batch_receipt.identity.member_pg_ids, [pg_id]);
        authorization.artifact_target_epoch = artifact_target_epoch;
        let request = unavailable_pg_staging_authorization_request_from_durable(transition)
            .expect("test staging authorization remains durable");
        transition
            .staging_authorization
            .as_mut()
            .unwrap()
            .batch_receipt
            .identity
            .members_digest = unavailable_pg_staging_authorization_members_digest(&[request]);
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

    #[must_use]
    pub fn unavailable_node_observation(
        &self,
        node_id: NodeId,
    ) -> Option<&NodeUnavailableObservation> {
        self.unavailable_node_observations.get(&node_id)
    }

    #[must_use]
    pub fn unavailable_pg_placement_transition(
        &self,
        pg_id: PgId,
    ) -> Option<&UnavailablePgPlacementTransition> {
        self.unavailable_pg_placement_transitions.get(&pg_id)
    }

    pub fn unavailable_pg_placement_transitions(
        &self,
    ) -> impl Iterator<Item = &UnavailablePgPlacementTransition> {
        self.unavailable_pg_placement_transitions.values()
    }

    pub fn retained_unavailable_pg_placement_transitions(
        &self,
    ) -> impl Iterator<Item = &UnavailablePgPlacementTransition> {
        self.retained_unavailable_pg_placement_transitions.values()
    }

    pub(crate) fn scan_unavailable_pg_reconciliation(
        &self,
        cursor: UnavailablePgReconciliationCursor,
        now_ms: u64,
    ) -> UnavailablePgReconciliationScan {
        let start = cursor
            .after_pg_id
            .map_or(std::ops::Bound::Unbounded, |pg_id| {
                std::ops::Bound::Excluded(pg_id)
            });
        let mut records = self.pgs.range((start, std::ops::Bound::Unbounded));
        let mut last_examined = None;
        for _ in 0..UNAVAILABLE_PG_RECONCILIATION_SCAN_PAGE_SIZE {
            let Some((&pg_id, pg)) = records.next() else {
                return UnavailablePgReconciliationScan {
                    candidate: None,
                    next_cursor: UnavailablePgReconciliationCursor::start(),
                };
            };
            last_examined = Some(pg_id);
            let candidate = self.unavailable_pg_reconciliation_candidate(pg_id, pg, now_ms);
            if candidate.is_some() {
                return UnavailablePgReconciliationScan {
                    candidate,
                    next_cursor: UnavailablePgReconciliationCursor {
                        after_pg_id: Some(pg_id),
                    },
                };
            }
        }
        UnavailablePgReconciliationScan {
            candidate: None,
            next_cursor: UnavailablePgReconciliationCursor {
                after_pg_id: last_examined,
            },
        }
    }

    pub(crate) fn scan_unavailable_pg_reconciliation_batch(
        &self,
        cursor: UnavailablePgReconciliationCursor,
        now_ms: u64,
    ) -> UnavailablePgReconciliationBatchScan {
        let start = cursor
            .after_pg_id
            .map_or(std::ops::Bound::Unbounded, |pg_id| {
                std::ops::Bound::Excluded(pg_id)
            });
        let mut records = self.pgs.range((start, std::ops::Bound::Unbounded));
        let mut candidates = Vec::new();
        let mut cleanup_fallbacks = Vec::new();
        let mut last_examined = None;
        for _ in 0..UNAVAILABLE_PG_RECONCILIATION_SCAN_PAGE_SIZE {
            let Some((&pg_id, pg)) = records.next() else {
                return UnavailablePgReconciliationBatchScan {
                    candidates,
                    cleanup_fallbacks,
                    next_cursor: UnavailablePgReconciliationCursor::start(),
                };
            };
            last_examined = Some(pg_id);
            if let Some(candidate) = self.unavailable_pg_reconciliation_candidate(pg_id, pg, now_ms)
            {
                let primary_is_cleanup = matches!(
                    &candidate,
                    UnavailablePgReconciliationCandidate::Resume(work)
                        if work.stage() == UnavailablePgReconciliationStage::StagingCleanup
                );
                candidates.push(candidate);
                if !primary_is_cleanup {
                    if let Some(cleanup) = self.unavailable_pg_reconciliation_cleanup_work(pg_id) {
                        cleanup_fallbacks.push(cleanup);
                    }
                }
            }
        }
        UnavailablePgReconciliationBatchScan {
            candidates,
            cleanup_fallbacks,
            next_cursor: UnavailablePgReconciliationCursor {
                after_pg_id: last_examined,
            },
        }
    }

    fn unavailable_pg_reconciliation_candidate(
        &self,
        pg_id: PgId,
        pg: &PgControlRecord,
        now_ms: u64,
    ) -> Option<UnavailablePgReconciliationCandidate> {
        let active_transition = self.unavailable_pg_placement_transitions.get(&pg_id);
        let unavailable_node_id = pg
            .acting_set
            .iter()
            .copied()
            .filter(|node_id| {
                active_transition
                    .is_none_or(|transition| transition.unavailable_node.node_id != *node_id)
            })
            .filter_map(|node_id| {
                let observation = self.unavailable_node_observations.get(&node_id)?;
                let topology = self.initial_topology.as_ref()?;
                let grace_cutoff_ms = observation.observed_at_ms.checked_add(
                    topology
                        .placement_policy()
                        .unavailable_replacement_grace_ms(),
                )?;
                (now_ms >= grace_cutoff_ms).then_some(node_id)
            })
            .min();
        if let Some(unavailable_node_id) = unavailable_node_id {
            return Some(UnavailablePgReconciliationCandidate::Begin {
                pg_id,
                unavailable_node_id,
            });
        }
        if let Some(transition) = active_transition {
            let stage = if transition.destination_epoch.is_some() {
                UnavailablePgReconciliationStage::PayloadReadiness
            } else {
                UnavailablePgReconciliationStage::MetadataTransfer
            };
            return Some(UnavailablePgReconciliationCandidate::Resume(
                UnavailablePgReconciliationWork::from_transition(transition, stage),
            ));
        }
        self.unavailable_pg_reconciliation_cleanup_work(pg_id)
            .map(UnavailablePgReconciliationCandidate::Resume)
    }

    fn unavailable_pg_reconciliation_cleanup_work(
        &self,
        pg_id: PgId,
    ) -> Option<UnavailablePgReconciliationWork> {
        self.retained_unavailable_pg_placement_transitions
            .range((
                std::ops::Bound::Included((pg_id, ClusterEpoch::INITIAL)),
                std::ops::Bound::Included((pg_id, self.cluster_epoch)),
            ))
            .find(|((retained_pg_id, transition_epoch), transition)| {
                *retained_pg_id == pg_id
                    && transition.staging_authorization.is_some()
                    && (transition.completion.is_some()
                        || (transition.destination_epoch.is_none()
                            && transition.destination_install.is_none()
                            && self
                                .retained_unavailable_pg_placement_transitions
                                .values()
                                .chain(self.unavailable_pg_placement_transitions.values())
                                .any(|candidate| {
                                    candidate.pg_id == pg_id
                                        && candidate.predecessor_transition_epoch
                                            == Some(transition.transition_epoch)
                                })))
                    && !self
                        .metadata_transfer_staging_finalized_floors
                        .contains_key(&(pg_id, transition_epoch.get()))
            })
            .map(|(_, transition)| {
                UnavailablePgReconciliationWork::from_transition(
                    transition,
                    UnavailablePgReconciliationStage::StagingCleanup,
                )
            })
    }

    fn unavailable_replacement_grace_elapsed_for_pg(
        &self,
        pg: &PgControlRecord,
        now_ms: u64,
    ) -> bool {
        let Some(topology) = self.initial_topology.as_ref() else {
            return false;
        };
        let grace_ms = topology
            .placement_policy()
            .unavailable_replacement_grace_ms();
        pg.acting_set.iter().any(|node_id| {
            self.unavailable_node_observations
                .get(node_id)
                .is_some_and(|observation| {
                    observation
                        .observed_at_ms
                        .checked_add(grace_ms)
                        .is_none_or(|cutoff_ms| now_ms >= cutoff_ms)
                })
        })
    }

    pub(crate) fn begin_unavailable_pg_placement_transition_command(
        &self,
        pg_id: PgId,
        unavailable_node_id: NodeId,
        begin_at_ms: u64,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        let observation = self
            .unavailable_node_observations
            .get(&unavailable_node_id)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: format!(
                    "node {} has no durable unavailable observation",
                    unavailable_node_id.as_u32()
                ),
            })?;
        let pg = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        let topology =
            self.initial_topology
                .as_ref()
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: "unavailable placement transition requires certified topology"
                        .to_string(),
                })?;
        let grace_cutoff_ms = observation
            .observed_at_ms
            .checked_add(
                topology
                    .placement_policy()
                    .unavailable_replacement_grace_ms(),
            )
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "unavailable placement grace cutoff overflows".to_string(),
            })?;
        let destination_acting_set = deterministic_unavailable_pg_destination(
            self,
            pg_id,
            &pg.acting_set,
            unavailable_node_id,
            begin_at_ms,
        )?;
        let begin_authorization = unavailable_pg_transition_begin_authorization(
            self,
            pg,
            &destination_acting_set,
            observation,
            begin_at_ms,
        )?;
        let source_node_id = begin_authorization.source_node_id;
        Ok(
            ControlPlaneCommand::BeginUnavailablePgPlacementTransitions {
                transitions: vec![UnavailablePgTransitionBeginRequest {
                    pg_id,
                    predecessor_transition_epoch: self
                        .unavailable_pg_placement_transitions
                        .get(&pg_id)
                        .map(|transition| transition.transition_epoch)
                        .or_else(|| {
                            self.retained_unavailable_pg_placement_transitions
                                .range((pg_id, ClusterEpoch::INITIAL)..=(pg_id, self.cluster_epoch))
                                .next_back()
                                .map(|(_, transition)| transition.transition_epoch)
                        }),
                    source_epoch: self.cluster_epoch,
                    source_acting_set: pg.acting_set.clone(),
                    source_node_id,
                    begin_authorization,
                    unavailable_node: observation.clone(),
                    grace_cutoff_ms,
                    topology_generation: topology.topology_generation(),
                    topology_digest: *topology.topology_digest(),
                    destination_acting_set,
                }],
                expected_transition_epoch: next_epoch(self.cluster_epoch)?,
                begin_at_ms,
            },
        )
    }

    fn validate_unavailable_pg_transition_begin(
        &self,
        request: UnavailablePgTransitionBeginRequest,
        expected_transition_epoch: ClusterEpoch,
        begin_at_ms: u64,
        batch_receipt: &UnavailablePgTransitionBatchReceipt,
    ) -> Result<ValidatedUnavailablePgTransitionBegin, ControlPlaneError> {
        let pg_id = request.pg_id;
        let requested = UnavailablePgPlacementTransition {
            pg_id,
            transition_epoch: expected_transition_epoch,
            predecessor_transition_epoch: request.predecessor_transition_epoch,
            topology_generation: request.topology_generation,
            topology_digest: request.topology_digest,
            source_epoch: request.source_epoch,
            source_acting_set: request.source_acting_set.clone(),
            source_node_id: request.source_node_id,
            begin_authorization: request.begin_authorization.clone(),
            unavailable_node: request.unavailable_node.clone(),
            grace_cutoff_ms: request.grace_cutoff_ms,
            destination_acting_set: request.destination_acting_set.clone(),
            destination_epoch: None,
            destination_route: None,
            payload_readiness: None,
            completion: None,
            begin_batch_receipt: batch_receipt.clone(),
            staging_authorization: None,
            destination_install: None,
            completion_batch_receipt: None,
        };
        if let Some(existing) = self
            .retained_unavailable_pg_placement_transitions
            .get(&(pg_id, expected_transition_epoch))
        {
            let mut original_request = existing.clone();
            original_request.destination_epoch = None;
            original_request.destination_route = None;
            original_request.payload_readiness = None;
            original_request.completion = None;
            original_request.staging_authorization = None;
            original_request.destination_install = None;
            original_request.completion_batch_receipt = None;
            if original_request == requested {
                return Ok(ValidatedUnavailablePgTransitionBegin::ExactReplay { pg_id });
            }
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} retained transition epoch belongs to a different request",
                    pg_id.get()
                ),
            });
        }
        if let Some(existing) = self.unavailable_pg_placement_transitions.get(&pg_id) {
            let mut original_request = existing.clone();
            original_request.destination_epoch = None;
            original_request.destination_route = None;
            original_request.payload_readiness = None;
            original_request.completion = None;
            original_request.staging_authorization = None;
            original_request.destination_install = None;
            original_request.completion_batch_receipt = None;
            if original_request == requested {
                return Ok(ValidatedUnavailablePgTransitionBegin::ExactReplay { pg_id });
            }
            if request.predecessor_transition_epoch != Some(existing.transition_epoch) {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "PG {} successor transition does not consume the active transition tip",
                        pg_id.get()
                    ),
                });
            }
        } else {
            let retained_tip = self
                .retained_unavailable_pg_placement_transitions
                .range((pg_id, ClusterEpoch::INITIAL)..=(pg_id, self.cluster_epoch))
                .next_back()
                .map(|(_, transition)| transition.transition_epoch);
            if request.predecessor_transition_epoch != retained_tip {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "PG {} successor transition does not consume the retained lineage tip",
                        pg_id.get()
                    ),
                });
            }
        }
        self.validate_serving_timestamp(begin_at_ms)?;
        if request.source_epoch != self.cluster_epoch {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} unavailable placement source epoch {} does not match current epoch {}",
                    pg_id.get(),
                    request.source_epoch,
                    self.cluster_epoch
                ),
            });
        }
        if expected_transition_epoch != next_epoch(self.cluster_epoch)? {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} unavailable placement transition epoch is not the next cluster epoch",
                    pg_id.get()
                ),
            });
        }
        let topology =
            self.initial_topology
                .as_ref()
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: "unavailable placement transition requires certified topology"
                        .to_string(),
                })?;
        if topology.topology_generation() != request.topology_generation
            || topology.topology_digest() != &request.topology_digest
        {
            return Err(ControlPlaneError::CommandDecode {
                message: "unavailable placement transition topology changed".to_string(),
            });
        }
        let observation = self
            .unavailable_node_observations
            .get(&request.unavailable_node.node_id)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: format!(
                    "node {} has no durable unavailable lease observation",
                    request.unavailable_node.node_id.as_u32()
                ),
            })?;
        if observation != &request.unavailable_node {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "node {} unavailable lease observation changed",
                    request.unavailable_node.node_id.as_u32()
                ),
            });
        }
        let expected_grace_cutoff_ms = request
            .unavailable_node
            .observed_at_ms
            .checked_add(
                topology
                    .placement_policy()
                    .unavailable_replacement_grace_ms(),
            )
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "unavailable placement grace cutoff overflows".to_string(),
            })?;
        if request.grace_cutoff_ms != expected_grace_cutoff_ms
            || begin_at_ms < request.grace_cutoff_ms
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} unavailable placement grace has not elapsed",
                    pg_id.get()
                ),
            });
        }
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if !matches!(record.state, PgState::Active | PgState::Peering)
            || record.acting_set != request.source_acting_set
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!("PG {} unavailable placement source changed", pg_id.get()),
            });
        }
        let expected_destination = deterministic_unavailable_pg_destination(
            self,
            pg_id,
            &request.source_acting_set,
            request.unavailable_node.node_id,
            begin_at_ms,
        )?;
        if request.destination_acting_set != expected_destination {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} unavailable placement destination is not the deterministic eligible replacement",
                    pg_id.get()
                ),
            });
        }
        let expected_begin_authorization = unavailable_pg_transition_begin_authorization(
            self,
            record,
            &request.destination_acting_set,
            &request.unavailable_node,
            begin_at_ms,
        )?;
        if request.begin_authorization != expected_begin_authorization
            || request.source_node_id != request.begin_authorization.source_node_id
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} unavailable placement begin authorization changed",
                    pg_id.get()
                ),
            });
        }
        let previous_primary_lease = active_primary_lease(self, record)
            .or_else(|| record.previous_primary_lease.clone())
            .map(PreviousPrimaryLease::without_reactivation_preference);
        let fenced_primary_lease_deadline_ms = previous_primary_lease
            .as_ref()
            .map_or(request.unavailable_node.lease_deadline_ms, |lease| {
                lease.lease_deadline_ms
            });
        Ok(ValidatedUnavailablePgTransitionBegin::Apply {
            requested: Box::new(requested),
            previous_primary_lease,
            fenced_primary_lease_deadline_ms,
        })
    }

    pub(crate) fn begin_unavailable_pg_placement_transition_batch_command(
        &self,
        candidates: &[(PgId, NodeId)],
        begin_at_ms: u64,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "begin",
            candidates.iter().map(|(pg_id, _)| *pg_id),
        )?;
        let mut transitions = Vec::with_capacity(candidates.len());
        let mut expected_transition_epoch = None;
        for (pg_id, unavailable_node_id) in candidates {
            let ControlPlaneCommand::BeginUnavailablePgPlacementTransitions {
                transitions: mut member,
                expected_transition_epoch: member_epoch,
                begin_at_ms: member_begin_at_ms,
            } = self.begin_unavailable_pg_placement_transition_command(
                *pg_id,
                *unavailable_node_id,
                begin_at_ms,
            )?
            else {
                unreachable!("unavailable transition member builder returned wrong command");
            };
            if member.len() != 1 || member_begin_at_ms != begin_at_ms {
                return Err(ControlPlaneError::invariant_failure(
                    "unavailable transition member builder returned a non-singleton envelope",
                ));
            }
            if expected_transition_epoch
                .replace(member_epoch)
                .is_some_and(|expected| expected != member_epoch)
            {
                return Err(ControlPlaneError::invariant_failure(
                    "unavailable transition batch members derived different target epochs",
                ));
            }
            transitions.push(member.remove(0));
        }
        Ok(
            ControlPlaneCommand::BeginUnavailablePgPlacementTransitions {
                transitions,
                expected_transition_epoch: expected_transition_epoch
                    .expect("canonical batch validation rejects an empty candidate vector"),
                begin_at_ms,
            },
        )
    }

    pub(crate) fn prepare_unavailable_pg_placement_transition_batch(
        &self,
        candidates: &[(PgId, NodeId)],
        begin_at_ms: u64,
    ) -> Result<PreparedUnavailablePgBeginBatch, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "begin preparation",
            candidates.iter().map(|(pg_id, _)| *pg_id),
        )?;
        let mut included = Vec::new();
        let mut rejected = Vec::new();
        for candidate in candidates {
            let singleton = match self.begin_unavailable_pg_placement_transition_command(
                candidate.0,
                candidate.1,
                begin_at_ms,
            ) {
                Ok(command) => command,
                Err(error) => {
                    rejected.push((candidate.0, error));
                    continue;
                }
            };
            if let Err(error) = self.apply_control_plane_command(singleton) {
                rejected.push((candidate.0, error));
                continue;
            }

            let mut tentative = included.clone();
            tentative.push(*candidate);
            let command = self
                .begin_unavailable_pg_placement_transition_batch_command(&tentative, begin_at_ms)?;
            let encoded_len =
                crate::control_plane_raft::control_plane_command_replication_encoded_len(&command)?;
            if encoded_len > crate::control_plane_raft::CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES {
                if included.is_empty() {
                    rejected.push((
                        candidate.0,
                        ControlPlaneError::invariant_failure(format!(
                            "single PG {} unavailable transition command encodes to {encoded_len} OpenRaft entry bytes, exceeding the replication-safe limit {}",
                            candidate.0.get(),
                            crate::control_plane_raft::CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES
                        )),
                    ));
                    continue;
                }
                break;
            }
            included = tentative;
        }

        let command = if included.is_empty() {
            None
        } else {
            let command = self
                .begin_unavailable_pg_placement_transition_batch_command(&included, begin_at_ms)?;
            self.apply_control_plane_command(command.clone())
                .map_err(|error| {
                    ControlPlaneError::invariant_failure(format!(
                        "individually valid unavailable transition begin members form an invalid batch: {error}"
                    ))
                })?;
            Some(command)
        };
        Ok(PreparedUnavailablePgBeginBatch {
            command,
            included,
            rejected,
        })
    }

    fn validate_unavailable_pg_transition_begin_batch(
        &self,
        requests: Vec<UnavailablePgTransitionBeginRequest>,
        expected_transition_epoch: ClusterEpoch,
        begin_at_ms: u64,
    ) -> Result<Vec<ValidatedUnavailablePgTransitionBegin>, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "begin",
            requests.iter().map(|request| request.pg_id),
        )?;
        let batch_receipt = UnavailablePgTransitionBatchReceipt {
            identity: unavailable_pg_transition_begin_batch_identity(
                &requests,
                expected_transition_epoch,
                begin_at_ms,
            ),
            source_epoch: requests[0].source_epoch,
            target_epoch: expected_transition_epoch,
        };
        requests
            .into_iter()
            .map(|request| {
                self.validate_unavailable_pg_transition_begin(
                    request,
                    expected_transition_epoch,
                    begin_at_ms,
                    &batch_receipt,
                )
            })
            .collect()
    }

    fn apply_validated_unavailable_pg_transition_begins(
        &self,
        validated: Vec<ValidatedUnavailablePgTransitionBegin>,
        expected_transition_epoch: ClusterEpoch,
        begin_at_ms: u64,
    ) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "begin",
            validated
                .iter()
                .map(ValidatedUnavailablePgTransitionBegin::pg_id),
        )?;
        if validated.iter().all(|entry| {
            matches!(
                entry,
                ValidatedUnavailablePgTransitionBegin::ExactReplay { .. }
            )
        }) {
            return Ok(None);
        }
        if validated.iter().any(|entry| {
            matches!(
                entry,
                ValidatedUnavailablePgTransitionBegin::ExactReplay { .. }
            )
        }) {
            return Err(ControlPlaneError::CommandDecode {
                message: "unavailable placement begin batch mixes replayed and new members"
                    .to_string(),
            });
        }
        let mut next_snapshot = self.clone();
        next_snapshot.record_committed_timestamp(begin_at_ms);
        for entry in validated {
            let ValidatedUnavailablePgTransitionBegin::Apply {
                requested,
                previous_primary_lease,
                fenced_primary_lease_deadline_ms,
            } = entry
            else {
                unreachable!("mixed replay was rejected before batch mutation");
            };
            let pg_id = requested.pg_id;
            if let Some(previous) = next_snapshot
                .unavailable_pg_placement_transitions
                .remove(&pg_id)
            {
                next_snapshot
                    .retained_unavailable_pg_placement_transitions
                    .insert((pg_id, previous.transition_epoch), previous);
            }
            let source_acting_set = requested.source_acting_set.clone();
            let source_node_id = requested.source_node_id;
            let source_metadata_floor = requested.begin_authorization.source_metadata_floor;
            let source_metadata_floor_epoch =
                requested.begin_authorization.source_metadata_floor_epoch;
            let source_metadata_floor_imported =
                requested.begin_authorization.source_metadata_floor_imported;
            next_snapshot
                .unavailable_pg_placement_transitions
                .insert(pg_id, *requested);
            let record = next_snapshot
                .pgs
                .get_mut(&pg_id)
                .expect("unavailable transition PG was validated");
            record.acting_set =
                unavailable_transition_source_route_acting_set(&source_acting_set, source_node_id);
            record.state = PgState::Peering;
            record.active_primary = None;
            record.active_metadata_proof = None;
            record.active_metadata_proof_epoch = None;
            record.active_metadata_transfer_imported = false;
            record.previous_primary_lease = previous_primary_lease;
            record.peering_metadata_proof_floor = Some(source_metadata_floor);
            record.peering_metadata_proof_floor_epoch = source_metadata_floor_epoch;
            record.peering_metadata_proof_floor_imported = source_metadata_floor_imported;
            record.peering_metadata_transfer = None;
            record.peering_metadata_transfer_source_route_epoch = None;
            record.peering_metadata_transfer_source_node_id = None;
            record.metadata_transfer_fenced = true;
            record.metadata_transfer_fence_source_lease_deadline_ms =
                Some(fenced_primary_lease_deadline_ms);
            record.metadata_transfer_fence_source_imported = source_metadata_floor_imported;
            record.metadata_transfer_fence_epoch = Some(expected_transition_epoch);
        }
        next_snapshot.bump_epoch()?;
        Ok(Some(next_snapshot))
    }

    fn validate_unavailable_pg_staging_authorization_batch(
        &self,
        requests: Vec<UnavailablePgStagingIntentAuthorizationRequest>,
    ) -> Result<Vec<ValidatedUnavailablePgStagingIntentAuthorization>, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "staging authorization",
            requests
                .iter()
                .map(|request| request.unavailable_transition.pg_id()),
        )?;
        let batch_identity = unavailable_pg_staging_authorization_batch_identity(&requests);
        requests
            .into_iter()
            .map(|request| {
                let pg_id = request.unavailable_transition.pg_id();
                let transition_epoch = request.unavailable_transition.transition_epoch();
                if request.staging_generation
                    != request.unavailable_transition.transition_epoch().get()
                    || request.artifact_target_epoch <= transition_epoch
                    || request.artifact_length == 0
                    || request.artifact_length
                        > crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES
                    || request.artifact_format_version
                        != crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} staging authorization has invalid generation, artifact length, or storage format",
                            pg_id.get()
                        ),
                    });
                }
                let active_transition = self
                    .unavailable_pg_placement_transitions
                    .get(&pg_id)
                    .filter(|transition| transition.transition_epoch == transition_epoch);
                let transition = self
                    .retained_unavailable_pg_placement_transitions
                    .get(&(pg_id, transition_epoch))
                    .or(active_transition)
                    .ok_or_else(|| ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} has no matching unavailable transition for staging authorization",
                            pg_id.get()
                        ),
                    })?;
                if !request.unavailable_transition.matches_transition(transition) {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} staging authorization does not match its unavailable transition",
                            pg_id.get()
                        ),
                    });
                }
                if let Some(existing) = &transition.staging_authorization {
                    if existing.staging_generation == request.staging_generation
                        && existing.artifact_target_epoch == request.artifact_target_epoch
                        && existing.artifact_digest == request.artifact_digest
                        && existing.artifact_length == request.artifact_length
                        && existing.artifact_format_version == request.artifact_format_version
                        && existing.batch_receipt.identity == batch_identity
                    {
                        return Ok(
                            ValidatedUnavailablePgStagingIntentAuthorization::ExactReplay {
                                pg_id,
                            },
                        );
                    }
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} staging authorization conflicts with durable artifact identity",
                            pg_id.get()
                        ),
                    });
                }
                if active_transition.is_none() || transition.destination_epoch.is_some()
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} staging authorization is not before destination installation",
                            pg_id.get()
                        ),
                    });
                }
                let authorization = UnavailablePgStagingIntentAuthorization {
                    staging_generation: request.staging_generation,
                    artifact_target_epoch: request.artifact_target_epoch,
                    artifact_digest: request.artifact_digest,
                    artifact_length: request.artifact_length,
                    artifact_format_version: request.artifact_format_version,
                    batch_receipt: UnavailablePgTransitionBatchReceipt {
                        identity: batch_identity.clone(),
                        source_epoch: self.cluster_epoch,
                        target_epoch: self.cluster_epoch,
                    },
                };
                Ok(ValidatedUnavailablePgStagingIntentAuthorization::Apply {
                    pg_id,
                    authorization,
                })
            })
            .collect()
    }

    pub(crate) fn committed_unavailable_pg_staging_authorization(
        &self,
        expected: &UnavailablePgStagingIntentAuthorizationRequest,
        destination_node_id: NodeId,
    ) -> Result<
        crate::control_plane_command::CommittedUnavailablePgStagingAuthorization,
        ControlPlaneError,
    > {
        let pg_id = expected.unavailable_transition.pg_id();
        let transition_epoch = expected.unavailable_transition.transition_epoch();
        let transition = self
            .retained_unavailable_pg_placement_transitions
            .get(&(pg_id, transition_epoch))
            .or_else(|| {
                self.unavailable_pg_placement_transitions
                    .get(&pg_id)
                    .filter(|transition| transition.transition_epoch == transition_epoch)
            })
            .ok_or_else(|| {
                ControlPlaneError::invariant_failure(
                    "committed staging authorization transition is absent",
                )
            })?;
        let durable = unavailable_pg_staging_authorization_request_from_durable(transition)
            .ok_or_else(|| {
                ControlPlaneError::invariant_failure(
                    "committed staging authorization is absent from its transition",
                )
            })?;
        if &durable != expected {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} committed staging authorization does not match the prepared artifact",
                    pg_id.get()
                ),
            });
        }
        let receipt = &transition
            .staging_authorization
            .as_ref()
            .expect("durable staging request requires authorization")
            .batch_receipt;
        let mut authorizations = Vec::with_capacity(receipt.identity.member_pg_ids.len());
        for member_pg_id in receipt.identity.member_pg_ids.iter().copied() {
            let mut matching =
                self.retained_unavailable_pg_placement_transitions
                    .values()
                    .chain(self.unavailable_pg_placement_transitions.values())
                    .filter(|candidate| {
                        candidate.pg_id == member_pg_id
                            && candidate.staging_authorization.as_ref().is_some_and(
                                |authorization| authorization.batch_receipt == *receipt,
                            )
                    });
            let member = matching.next().ok_or_else(|| {
                ControlPlaneError::invariant_failure(
                    "committed staging authorization batch member is absent",
                )
            })?;
            if matching.next().is_some() {
                return Err(ControlPlaneError::invariant_failure(
                    "committed staging authorization batch member is ambiguous",
                ));
            }
            authorizations.push(
                unavailable_pg_staging_authorization_request_from_durable(member).ok_or_else(
                    || {
                        ControlPlaneError::invariant_failure(
                            "committed staging authorization batch member has no request",
                        )
                    },
                )?,
            );
        }
        if unavailable_pg_staging_authorization_batch_identity(&authorizations) != receipt.identity
            || receipt.source_epoch != receipt.target_epoch
        {
            return Err(ControlPlaneError::invariant_failure(
                "committed staging authorization batch receipt is invalid",
            ));
        }
        let presentation = crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation::from_authority_state(
            authorizations,
            receipt.source_epoch,
            receipt.identity.members_digest,
        )?;
        if !presentation.authorizes_destination_for_pg(destination_node_id, pg_id) {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "node {} is not a destination of PG {} in the committed staging authorization batch",
                    destination_node_id.as_u32(),
                    pg_id.get()
                ),
            });
        }
        Ok(crate::control_plane_command::CommittedUnavailablePgStagingAuthorization::from_authority_published(
            presentation,
            destination_node_id,
            pg_id,
            authority_published_staging_authorization_seal(),
        ))
    }

    fn exact_unavailable_pg_transition_for_reconciliation(
        &self,
        work: &UnavailablePgReconciliationWork,
    ) -> Result<&UnavailablePgPlacementTransition, ControlPlaneError> {
        let pg_id = work.pg_id();
        let transition_epoch = work.transition_epoch();
        let transition = self
            .retained_unavailable_pg_placement_transitions
            .get(&(pg_id, transition_epoch))
            .or_else(|| {
                self.unavailable_pg_placement_transitions
                    .get(&pg_id)
                    .filter(|transition| transition.transition_epoch == transition_epoch)
            })
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} has no exact unavailable transition for staged transfer recovery",
                    pg_id.get()
                ),
            })?;
        if !work.mutation_binding().matches_transition(transition) {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} staged transfer recovery does not match its unavailable transition",
                    pg_id.get()
                ),
            });
        }
        Ok(transition)
    }

    #[allow(dead_code)] // Consumed when the reconciliation worker switches to staged ownership.
    pub(crate) fn committed_unavailable_pg_staging_request(
        &self,
        work: &UnavailablePgReconciliationWork,
    ) -> Result<UnavailablePgStagingIntentAuthorizationRequest, ControlPlaneError> {
        self.committed_unavailable_pg_staging_request_binding(work)
            .map(|(request, _, _)| request)
    }

    pub(crate) fn committed_unavailable_pg_staging_request_binding(
        &self,
        work: &UnavailablePgReconciliationWork,
    ) -> Result<
        (
            UnavailablePgStagingIntentAuthorizationRequest,
            ClusterEpoch,
            ClusterEpoch,
        ),
        ControlPlaneError,
    > {
        let transition = self.exact_unavailable_pg_transition_for_reconciliation(work)?;
        let authorization = transition.staging_authorization.as_ref().ok_or_else(|| {
            ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} staged transfer recovery has no durable staging authorization",
                    work.pg_id().get()
                ),
            }
        })?;
        let request = unavailable_pg_staging_authorization_request_from_durable(transition)
            .expect("staging authorization request exists with its durable authorization");
        let target_epoch = authorization.artifact_target_epoch;
        let source_epoch =
            ClusterEpoch::new(target_epoch.get().checked_sub(1).ok_or_else(|| {
                ControlPlaneError::invariant_failure(
                    "staging artifact target epoch has no source runtime epoch",
                )
            })?)
            .ok_or_else(|| {
                ControlPlaneError::invariant_failure(
                    "staging artifact target epoch has no source runtime epoch",
                )
            })?;
        Ok((request, source_epoch, target_epoch))
    }

    pub(crate) fn committed_unavailable_pg_staged_transfer_if_present(
        &self,
        work: &UnavailablePgReconciliationWork,
    ) -> Result<
        Option<(
            UnavailablePgStagingIntentAuthorizationRequest,
            UnavailablePgTransitionInstallRequest,
        )>,
        ControlPlaneError,
    > {
        let transition = self.exact_unavailable_pg_transition_for_reconciliation(work)?;
        let authorization = self.committed_unavailable_pg_staging_request(work)?;
        Ok(
            unavailable_pg_destination_install_request_from_durable(transition)
                .map(|install| (authorization, install)),
        )
    }

    #[allow(dead_code)] // Consumed when the reconciliation worker switches to staged ownership.
    pub(crate) fn committed_unavailable_pg_staged_transfer(
        &self,
        work: &UnavailablePgReconciliationWork,
    ) -> Result<
        (
            UnavailablePgStagingIntentAuthorizationRequest,
            UnavailablePgTransitionInstallRequest,
        ),
        ControlPlaneError,
    > {
        self.committed_unavailable_pg_staged_transfer_if_present(work)?
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} staged transfer recovery has no durable destination install",
                    work.pg_id().get()
                ),
            })
    }

    pub(crate) fn committed_unavailable_pg_staging_cleanup(
        &self,
        work: &UnavailablePgReconciliationWork,
    ) -> Result<
        (
            UnavailablePgStagingIntentAuthorizationRequest,
            MetadataTransferStagingCleanupDisposition,
            Option<UnavailablePgTransitionInstallRequest>,
        ),
        ControlPlaneError,
    > {
        let transition = self.exact_unavailable_pg_transition_for_reconciliation(work)?;
        let authorization = self.committed_unavailable_pg_staging_request(work)?;
        if transition.completion.is_some() && transition.completion_batch_receipt.is_some() {
            let install = unavailable_pg_destination_install_request_from_durable(transition)
                .ok_or_else(|| {
                    ControlPlaneError::invariant_failure(
                        "completed staging cleanup transition has no destination install",
                    )
                })?;
            return Ok((
                authorization,
                MetadataTransferStagingCleanupDisposition::Completed,
                Some(install),
            ));
        }
        if transition.destination_epoch.is_some() || transition.destination_install.is_some() {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} staging cancellation is not a superseded pre-install transition",
                    work.pg_id().get()
                ),
            });
        }
        let mut successors = self
            .retained_unavailable_pg_placement_transitions
            .values()
            .chain(self.unavailable_pg_placement_transitions.values())
            .filter(|candidate| {
                candidate.pg_id == transition.pg_id
                    && candidate.predecessor_transition_epoch == Some(transition.transition_epoch)
            });
        let successor = successors
            .next()
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} staging cancellation has no exact successor transition",
                    work.pg_id().get()
                ),
            })?;
        if successors.next().is_some() {
            return Err(ControlPlaneError::invariant_failure(
                "staging cancellation transition has multiple direct successors",
            ));
        }
        Ok((
            authorization,
            MetadataTransferStagingCleanupDisposition::Superseded {
                successor_transition_epoch: successor.transition_epoch,
            },
            None,
        ))
    }

    pub(crate) fn validate_unavailable_pg_staging_cleanup(
        &self,
        binding: &UnavailablePgTransitionMutationBinding,
        disposition: MetadataTransferStagingCleanupDisposition,
        install: Option<&UnavailablePgTransitionInstallRequest>,
        staging_generation: u64,
    ) -> Result<CompletedUnavailablePgStagingCleanupAuthorization, ControlPlaneError> {
        let pg_id = binding.pg_id();
        let transition_epoch = binding.transition_epoch();
        let transition = self
            .retained_unavailable_pg_placement_transitions
            .get(&(pg_id, transition_epoch))
            .filter(|transition| binding.matches_transition(transition))
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} staging cleanup requires its exact retained transition",
                    pg_id.get()
                ),
            })?;
        let authorization = transition.staging_authorization.as_ref().ok_or_else(|| {
            ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} staging cleanup has no durable authorization",
                    pg_id.get()
                ),
            }
        })?;
        if authorization.staging_generation != staging_generation {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} staging cleanup generation is not authorized",
                    pg_id.get()
                ),
            });
        }
        match disposition {
            MetadataTransferStagingCleanupDisposition::Completed => {
                let install = install.ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: format!(
                        "PG {} completed staging cleanup has no install",
                        pg_id.get()
                    ),
                })?;
                let destination_install =
                    transition.destination_install.as_ref().ok_or_else(|| {
                        ControlPlaneError::CommandDecode {
                            message: format!(
                                "PG {} staging cleanup has no durable destination install",
                                pg_id.get()
                            ),
                        }
                    })?;
                if transition.destination_epoch != Some(install.expected_destination_epoch)
                    || install.unavailable_transition != *binding
                    || destination_install.transfer != install.transfer
                    || destination_install.publications != install.publications
                    || transition.completion.is_none()
                    || transition.completion_batch_receipt.is_none()
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} staging cleanup does not match its completed destination installation",
                            pg_id.get()
                        ),
                    });
                }
            }
            MetadataTransferStagingCleanupDisposition::Superseded {
                successor_transition_epoch,
            } => {
                if install.is_some()
                    || transition.destination_epoch.is_some()
                    || transition.destination_install.is_some()
                    || transition.completion.is_some()
                    || transition.completion_batch_receipt.is_some()
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} staging cancellation is not a superseded pre-install transition",
                            pg_id.get()
                        ),
                    });
                }
                let successor_matches = self
                    .retained_unavailable_pg_placement_transitions
                    .get(&(pg_id, successor_transition_epoch))
                    .or_else(|| {
                        self.unavailable_pg_placement_transitions
                            .get(&pg_id)
                            .filter(|candidate| {
                                candidate.transition_epoch == successor_transition_epoch
                            })
                    })
                    .is_some_and(|successor| {
                        successor.predecessor_transition_epoch == Some(transition_epoch)
                    });
                if !successor_matches {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} staging cancellation does not match its direct successor",
                            pg_id.get()
                        ),
                    });
                }
            }
        }
        let mut destination_actors = binding
            .destination_acting_set()
            .iter()
            .copied()
            .map(|node_id| {
                let node = self.node(node_id).ok_or_else(|| {
                    ControlPlaneError::invariant_failure(format!(
                        "PG {} staging cleanup destination {} is absent from authority state",
                        pg_id.get(),
                        node_id.as_u32()
                    ))
                })?;
                MetadataTransferStagingNodeIdentity::new(
                    node_id,
                    node.node_incarnation(),
                    node.endpoint().to_owned(),
                )
                .map_err(|error| {
                    ControlPlaneError::invariant_failure(format!(
                        "PG {} staging cleanup destination {} has an invalid authority identity: {error}",
                        pg_id.get(),
                        node_id.as_u32()
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        destination_actors.sort_by_key(MetadataTransferStagingNodeIdentity::node_id);
        Ok(CompletedUnavailablePgStagingCleanupAuthorization {
            cluster_epoch: self.cluster_epoch,
            destination_actors,
        })
    }

    pub fn authorize_unavailable_pg_staging_intents_batch_command(
        &self,
        authorizations: &[UnavailablePgStagingIntentAuthorizationRequest],
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "staging authorization",
            authorizations
                .iter()
                .map(|request| request.unavailable_transition.pg_id()),
        )?;
        let command = ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
            authorizations: authorizations.to_vec(),
        };
        self.validate_replication_safe_unavailable_pg_batch_command(
            "staging authorization",
            &command,
        )?;
        Ok(command)
    }

    fn apply_validated_unavailable_pg_staging_authorizations(
        &self,
        validated: Vec<ValidatedUnavailablePgStagingIntentAuthorization>,
    ) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "staging authorization",
            validated
                .iter()
                .map(ValidatedUnavailablePgStagingIntentAuthorization::pg_id),
        )?;
        if validated.iter().all(|entry| {
            matches!(
                entry,
                ValidatedUnavailablePgStagingIntentAuthorization::ExactReplay { .. }
            )
        }) {
            return Ok(None);
        }
        if validated.iter().any(|entry| {
            matches!(
                entry,
                ValidatedUnavailablePgStagingIntentAuthorization::ExactReplay { .. }
            )
        }) {
            return Err(ControlPlaneError::CommandDecode {
                message:
                    "unavailable placement staging authorization batch mixes replayed and new members"
                        .to_owned(),
            });
        }
        let mut next_snapshot = self.clone();
        for entry in validated {
            let ValidatedUnavailablePgStagingIntentAuthorization::Apply {
                pg_id,
                authorization,
            } = entry
            else {
                unreachable!("mixed replay was rejected before staging authorization mutation");
            };
            next_snapshot
                .unavailable_pg_placement_transitions
                .get_mut(&pg_id)
                .expect("staging authorization transition was validated")
                .staging_authorization = Some(authorization);
        }
        Ok(Some(next_snapshot))
    }

    fn validate_unavailable_pg_destination_install_batch(
        &self,
        requests: Vec<UnavailablePgTransitionInstallRequest>,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<Vec<ValidatedUnavailablePgDestinationInstall>, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "destination installation",
            requests
                .iter()
                .map(|request| request.unavailable_transition.pg_id()),
        )?;
        let batch_identity = unavailable_pg_destination_install_batch_identity(
            &requests,
            expected_destination_epoch,
        );
        requests
            .into_iter()
            .map(|request| {
                let pg_id = request.unavailable_transition.pg_id();
                let transition_epoch = request.unavailable_transition.transition_epoch();
                let active_transition = self
                    .unavailable_pg_placement_transitions
                    .get(&pg_id)
                    .filter(|transition| transition.transition_epoch == transition_epoch);
                let transition = self
                    .retained_unavailable_pg_placement_transitions
                    .get(&(pg_id, transition_epoch))
                    .or(active_transition)
                    .ok_or_else(|| ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} has no matching unavailable transition for destination installation",
                            pg_id.get()
                        ),
                    })?;
                if !request.unavailable_transition.matches_transition(transition) {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} destination installation does not match its unavailable transition",
                            pg_id.get()
                        ),
                    });
                }
                if let Some(existing) = &transition.destination_install {
                    if transition.destination_epoch == Some(request.expected_destination_epoch)
                        && existing.transfer == request.transfer
                        && existing.publications == request.publications
                        && existing.batch_receipt.identity == batch_identity
                    {
                        return Ok(ValidatedUnavailablePgDestinationInstall::ExactReplay {
                            pg_id,
                        });
                    }
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} destination installation conflicts with durable install evidence",
                            pg_id.get()
                        ),
                    });
                }
                if active_transition.is_none()
                    || transition.destination_epoch.is_some()
                    || request.expected_destination_epoch != expected_destination_epoch
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} destination installation is not applicable to its active transition",
                            pg_id.get()
                        ),
                    });
                }
                let authorization = transition.staging_authorization.as_ref().ok_or_else(|| {
                    ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} destination installation has no staging authorization",
                            pg_id.get()
                        ),
                    }
                })?;
                validate_acting_set(self, pg_id, &transition.destination_acting_set)?;
                validate_acting_set_preserves_pending_recovery(
                    self,
                    pg_id,
                    &transition.destination_acting_set,
                )?;
                let publication_nodes = request
                    .publications
                    .iter()
                    .map(|publication| publication.node_id)
                    .collect::<BTreeSet<_>>();
                let destination_nodes = transition
                    .destination_acting_set
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>();
                if request.publications.len() != transition.destination_acting_set.len()
                    || publication_nodes.len() != request.publications.len()
                    || publication_nodes != destination_nodes
                    || request
                        .publications
                        .windows(2)
                        .any(|pair| pair[0].node_id >= pair[1].node_id)
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} destination installation does not carry one canonical publication per destination",
                            pg_id.get()
                        ),
                    });
                }
                for publication in &request.publications {
                    let node = self.nodes.get(&publication.node_id).ok_or(
                        ControlPlaneError::UnknownNode {
                            node_id: publication.node_id.as_u32(),
                        },
                    )?;
                    if node.node_incarnation != publication.node_incarnation
                        || node.endpoint != publication.endpoint
                    {
                        return Err(ControlPlaneError::CommandDecode {
                            message: format!(
                                "PG {} destination {} publication identity is stale",
                                pg_id.get(),
                                publication.node_id.as_u32()
                            ),
                        });
                    }
                    let key = MetadataTransferStagingEvidenceKey {
                        pg_id,
                        staging_generation: authorization.staging_generation,
                        actor_node_id: publication.node_id,
                        actor_node_incarnation: publication.node_incarnation,
                        kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                        target_epoch: Some(request.expected_destination_epoch),
                    };
                    let evidence_bytes = self
                        .metadata_transfer_staging_evidence
                        .get(&key)
                        .ok_or_else(|| ControlPlaneError::CommandDecode {
                            message: format!(
                                "PG {} destination {} has no committed staging publication",
                                pg_id.get(),
                                publication.node_id.as_u32()
                            ),
                        })?;
                    if checksum::sha256::digest(evidence_bytes) != publication.evidence_digest {
                        return Err(ControlPlaneError::CommandDecode {
                            message: format!(
                                "PG {} destination {} staging publication digest does not match retained evidence",
                                pg_id.get(),
                                publication.node_id.as_u32()
                            ),
                        });
                    }
                    let evidence = crate::pg_store::decode_staging_evidence(evidence_bytes)
                        .map_err(|error| ControlPlaneError::SnapshotInvariantViolation {
                            context: "retained metadata-transfer staging evidence is invalid",
                            message: error.to_string(),
                        })?;
                    self.validate_metadata_transfer_staging_evidence_authority(&evidence, false)?;
                    if evidence.actor().endpoint() != publication.endpoint {
                        return Err(ControlPlaneError::CommandDecode {
                            message: format!(
                                "PG {} destination {} staging publication endpoint does not match",
                                pg_id.get(),
                                publication.node_id.as_u32()
                            ),
                        });
                    }
                    if evidence.target_epoch() != Some(request.expected_destination_epoch)
                        || evidence.transfer() != Some(request.transfer)
                    {
                        return Err(ControlPlaneError::CommandDecode {
                            message: format!(
                                "PG {} destination {} staged artifact does not derive the requested transfer proof",
                                pg_id.get(),
                                publication.node_id.as_u32()
                            ),
                        });
                    }
                }
                let record = self.pg(pg_id).ok_or(ControlPlaneError::UnknownPg {
                    pg_id: pg_id.get(),
                })?;
                let required_floor = record.peering_metadata_proof_floor.ok_or(
                    ControlPlaneError::PgMetadataMigrationRequiresTransfer {
                        pg_id: pg_id.get(),
                    },
                )?;
                validate_metadata_transfer_proof(MetadataTransferProofValidation {
                    snapshot: self,
                    pg_id,
                    state: record.state,
                    metadata_transfer_fenced: record.metadata_transfer_fenced,
                    metadata_transfer_fence_source_imported:
                        record.metadata_transfer_fence_source_imported,
                    metadata_transfer_fence_epoch: record.metadata_transfer_fence_epoch,
                    required_floor,
                    required_floor_epoch: record.peering_metadata_proof_floor_epoch,
                    transfer: request.transfer,
                })?;
                Ok(ValidatedUnavailablePgDestinationInstall::Apply {
                    pg_id,
                    transfer: request.transfer,
                    publications: request.publications,
                    batch_identity: batch_identity.clone(),
                })
            })
            .collect()
    }

    fn apply_validated_unavailable_pg_destination_installs(
        &self,
        validated: Vec<ValidatedUnavailablePgDestinationInstall>,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "destination installation",
            validated
                .iter()
                .map(ValidatedUnavailablePgDestinationInstall::pg_id),
        )?;
        if validated.iter().all(|entry| {
            matches!(
                entry,
                ValidatedUnavailablePgDestinationInstall::ExactReplay { .. }
            )
        }) {
            return Ok(None);
        }
        if validated.iter().any(|entry| {
            matches!(
                entry,
                ValidatedUnavailablePgDestinationInstall::ExactReplay { .. }
            )
        }) {
            return Err(ControlPlaneError::CommandDecode {
                message:
                    "unavailable placement destination install batch mixes replayed and new members"
                        .to_owned(),
            });
        }
        let actual_destination_epoch = next_epoch(self.cluster_epoch)?;
        if actual_destination_epoch != expected_destination_epoch {
            return Err(
                ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
                    pg_id: validated[0].pg_id().get(),
                    expected_destination_epoch,
                    actual_destination_epoch,
                },
            );
        }
        let mut next_snapshot = self.clone();
        for entry in validated {
            let ValidatedUnavailablePgDestinationInstall::Apply {
                pg_id,
                transfer,
                publications,
                batch_identity,
            } = entry
            else {
                unreachable!("mixed replay was rejected before destination installation");
            };
            let transition = next_snapshot
                .unavailable_pg_placement_transitions
                .get(&pg_id)
                .expect("destination install transition was validated");
            let destination_acting_set = transition.destination_acting_set.clone();
            let source_node_id = transition.source_node_id;
            let record = next_snapshot
                .pgs
                .get_mut(&pg_id)
                .expect("destination install PG was validated");
            let previous_primary_lease = active_primary_lease(self, record)
                .or_else(|| record.previous_primary_lease.clone())
                .map(PreviousPrimaryLease::without_reactivation_preference);
            record.acting_set = destination_acting_set;
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
            record.peering_metadata_transfer_source_route_epoch = Some(self.cluster_epoch);
            record.peering_metadata_transfer_source_node_id = Some(source_node_id);
            record.metadata_transfer_fenced = false;
            record.metadata_transfer_fence_source_lease_deadline_ms = None;
            record.metadata_transfer_fence_source_imported = false;
            record.metadata_transfer_fence_epoch = None;
            let destination_route = HistoricalPgRouteRecord::from(&*record);
            let transition = next_snapshot
                .unavailable_pg_placement_transitions
                .get_mut(&pg_id)
                .expect("destination install transition was validated");
            transition.destination_epoch = Some(expected_destination_epoch);
            transition.destination_route = Some(destination_route);
            transition.destination_install = Some(UnavailablePgDestinationInstall {
                transfer,
                publications,
                batch_receipt: UnavailablePgTransitionBatchReceipt {
                    identity: batch_identity,
                    source_epoch: self.cluster_epoch,
                    target_epoch: expected_destination_epoch,
                },
            });
        }
        next_snapshot.bump_epoch()?;
        Ok(Some(next_snapshot))
    }

    pub(crate) fn prepare_unavailable_pg_placement_install_batch(
        &self,
        transitions: &[UnavailablePgTransitionInstallRequest],
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<PreparedUnavailablePgInstallBatch, ControlPlaneError> {
        self.prepare_unavailable_pg_placement_install_batch_with_replication_limit(
            transitions,
            expected_destination_epoch,
            crate::control_plane_raft::CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES,
        )
    }

    fn prepare_unavailable_pg_placement_install_batch_with_replication_limit(
        &self,
        transitions: &[UnavailablePgTransitionInstallRequest],
        expected_destination_epoch: ClusterEpoch,
        max_encoded_entry_bytes: usize,
    ) -> Result<PreparedUnavailablePgInstallBatch, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "destination installation preparation",
            transitions
                .iter()
                .map(|request| request.unavailable_transition.pg_id()),
        )?;
        let mut included = Vec::new();
        let mut rejected = Vec::new();
        for candidate in transitions {
            let singleton = match self
                .install_unavailable_pg_placement_transitions_batch_command_unbounded(
                    std::slice::from_ref(candidate),
                    expected_destination_epoch,
                ) {
                Ok(command) => command,
                Err(error) => {
                    rejected.push((candidate.clone(), error));
                    continue;
                }
            };
            let singleton_len =
                crate::control_plane_raft::control_plane_command_replication_encoded_len(
                    &singleton,
                )?;
            if singleton_len > max_encoded_entry_bytes {
                rejected.push((
                    candidate.clone(),
                    ControlPlaneError::invariant_failure(format!(
                        "single PG {} unavailable transition installation encodes to {singleton_len} OpenRaft entry bytes, exceeding the replication-safe limit {max_encoded_entry_bytes}",
                        candidate.unavailable_transition.pg_id().get()
                    )),
                ));
                continue;
            }

            let mut tentative = included.clone();
            tentative.push(candidate.clone());
            let command = self
                .install_unavailable_pg_placement_transitions_batch_command_unbounded(
                    &tentative,
                    expected_destination_epoch,
                )?;
            let encoded_len =
                crate::control_plane_raft::control_plane_command_replication_encoded_len(&command)?;
            if encoded_len > max_encoded_entry_bytes {
                break;
            }
            included = tentative;
        }

        let command = if included.is_empty() {
            None
        } else {
            Some(
                self.install_unavailable_pg_placement_transitions_batch_command_unbounded(
                    &included,
                    expected_destination_epoch,
                )?,
            )
        };
        Ok(PreparedUnavailablePgInstallBatch {
            command,
            included,
            rejected,
        })
    }

    pub fn install_unavailable_pg_placement_transitions_batch_command(
        &self,
        transitions: &[UnavailablePgTransitionInstallRequest],
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        let command = self.install_unavailable_pg_placement_transitions_batch_command_unbounded(
            transitions,
            expected_destination_epoch,
        )?;
        self.validate_replication_safe_unavailable_pg_batch_command(
            "destination installation",
            &command,
        )?;
        Ok(command)
    }

    fn install_unavailable_pg_placement_transitions_batch_command_unbounded(
        &self,
        transitions: &[UnavailablePgTransitionInstallRequest],
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "destination installation",
            transitions
                .iter()
                .map(|request| request.unavailable_transition.pg_id()),
        )?;
        let command = ControlPlaneCommand::InstallUnavailablePgPlacementTransitions {
            transitions: transitions.to_vec(),
            expected_destination_epoch,
        };
        self.apply_control_plane_command(command.clone())?;
        Ok(command)
    }

    fn validate_replication_safe_unavailable_pg_batch_command(
        &self,
        kind: &str,
        command: &ControlPlaneCommand,
    ) -> Result<(), ControlPlaneError> {
        self.apply_control_plane_command(command.clone())?;
        let encoded_len =
            crate::control_plane_raft::control_plane_command_replication_encoded_len(command)?;
        if encoded_len > crate::control_plane_raft::CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES {
            return Err(ControlPlaneError::invariant_failure(format!(
                "unavailable placement {kind} batch encodes to {encoded_len} OpenRaft entry bytes, exceeding the replication-safe limit {}",
                crate::control_plane_raft::CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES
            )));
        }
        Ok(())
    }

    fn validate_metadata_transfer_staging_evidence_authority(
        &self,
        evidence: &crate::pg_store::MetadataTransferStagingEvidence,
        require_current_actor: bool,
    ) -> Result<(), ControlPlaneError> {
        let actor = evidence.actor();
        if require_current_actor {
            let node = self.nodes.get(&actor.node_id()).ok_or_else(|| {
                ControlPlaneError::CommandDecode {
                    message: format!(
                        "metadata-transfer staging evidence references unknown node {}",
                        actor.node_id().as_u32()
                    ),
                }
            })?;
            if node.node_incarnation != actor.node_incarnation()
                || node.endpoint != actor.endpoint()
            {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "metadata-transfer staging evidence actor {} does not match current node identity",
                        actor.node_id().as_u32()
                    ),
                });
            }
        }

        let intent = evidence.intent();
        let transition = self
            .retained_unavailable_pg_placement_transitions
            .get(&(intent.pg_id(), intent.transition_epoch()))
            .or_else(|| {
                self.unavailable_pg_placement_transitions
                    .get(&intent.pg_id())
                    .filter(|transition| transition.transition_epoch == intent.transition_epoch())
            })
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} has no matching transition for metadata-transfer staging evidence",
                    intent.pg_id().get()
                ),
            })?;
        let authorization = transition.staging_authorization.as_ref().ok_or_else(|| {
            ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} has no staging authorization for metadata-transfer evidence",
                    intent.pg_id().get()
                ),
            }
        })?;
        if transition.source_epoch != intent.source_epoch()
            || transition.source_acting_set != intent.source_acting_set()
            || transition.destination_acting_set != intent.destination_acting_set()
            || authorization.staging_generation != intent.staging_generation()
            || authorization.artifact_digest != intent.artifact_digest()
            || authorization.artifact_length != intent.artifact_length()
            || authorization.artifact_format_version != intent.artifact_format_version()
            || !transition.destination_acting_set.contains(&actor.node_id())
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} metadata-transfer staging evidence does not match its authorization",
                    intent.pg_id().get()
                ),
            });
        }
        match (
            evidence.kind(),
            evidence.target_epoch(),
            evidence.transfer(),
        ) {
            (
                crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                Some(target_epoch),
                Some(transfer),
            ) if target_epoch > intent.transition_epoch()
                && transfer.source_epoch() <= intent.source_epoch() =>
            {
                // The staging node validates the complete imported proof against
                // the artifact before publication. The authority does not own
                // that artifact, but it can and must reject impossible epoch
                // relationships before admitting the evidence to Raft.
            }
            (crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone, None, None) => {}
            _ => {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "PG {} metadata-transfer staging evidence has invalid publication semantics",
                        intent.pg_id().get()
                    ),
                });
            }
        }
        Ok(())
    }

    fn metadata_transfer_staging_checkpoint_evidence_bytes(
        &self,
        key: &MetadataTransferStagingEvidenceKey,
        actor: &crate::pg_store::MetadataTransferStagingNodeIdentity,
        finalized_evidence: &MetadataTransferStagingFinalizedEvidenceIndex<'_>,
        expected_digest: [u8; 32],
    ) -> Result<Vec<u8>, String> {
        if let Some(bytes) = self.metadata_transfer_staging_evidence.get(key) {
            if checksum::sha256::digest(bytes) != expected_digest {
                return Err(
                    "metadata-transfer staging checkpoint commitment digest is invalid".to_owned(),
                );
            }
            return Ok(bytes.clone());
        }
        let finalized = finalized_evidence.get(key).ok_or_else(|| {
            "metadata-transfer staging checkpoint member lacks detailed or finalized evidence"
                .to_owned()
        })?;
        let floor = finalized.floor;
        let transition = self
            .retained_unavailable_pg_placement_transitions
            .get(&(key.pg_id, floor.transition.transition_epoch()))
            .filter(|transition| floor.transition.matches_transition(transition))
            .ok_or_else(|| {
                "metadata-transfer staging checkpoint member has no exact finalized transition"
                    .to_owned()
            })?;
        let authorization = transition.staging_authorization.as_ref().ok_or_else(|| {
            "metadata-transfer staging checkpoint member has no finalized authorization".to_owned()
        })?;
        if actor.node_id() != key.actor_node_id
            || actor.node_incarnation() != key.actor_node_incarnation
        {
            return Err(
                "metadata-transfer staging checkpoint member actor does not match its page"
                    .to_owned(),
            );
        }
        if finalized.endpoint != actor.endpoint()
            || finalized.evidence_digest != expected_digest
            || finalized.target_epoch != key.target_epoch
            || (key.kind == crate::pg_store::MetadataTransferStagingEvidenceKind::Publication)
                != finalized.transfer.is_some()
        {
            return Err(
                "metadata-transfer staging checkpoint member does not match its finalized certificate"
                    .to_owned(),
            );
        }
        let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
            &floor.transition,
            authorization.artifact_digest,
            authorization.artifact_length,
            authorization.artifact_format_version,
        )
        .map_err(|error| error.to_string())?;
        let bytes = crate::pg_store::canonical_metadata_transfer_staging_evidence(
            actor,
            &intent,
            key.kind,
            finalized.target_epoch,
            finalized.transfer,
        )
        .map_err(|error| error.to_string())?;
        if checksum::sha256::digest(&bytes) != expected_digest {
            return Err(
                "metadata-transfer staging checkpoint member is not canonically committed"
                    .to_owned(),
            );
        }
        Ok(bytes)
    }

    fn reconstruct_metadata_transfer_staging_checkpoint_source_segment(
        &self,
        actor: &crate::pg_store::MetadataTransferStagingNodeIdentity,
        binding: MetadataTransferStagingCheckpointSourceSegmentBinding,
        finalized_evidence: &MetadataTransferStagingFinalizedEvidenceIndex<'_>,
        finalized_checkpoints: &MetadataTransferStagingFinalizedCheckpointIndex<'_>,
    ) -> Result<MetadataTransferStagingEvidenceCheckpointSegment, String> {
        let MetadataTransferStagingCheckpointSourceSegmentBinding {
            first_generation,
            last_generation,
            previous_generation,
            previous_apply_receipt_digest,
            source_segment_digest,
        } = binding;
        let page_count = last_generation
            .checked_sub(first_generation)
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| {
                "metadata-transfer staging checkpoint anchor generation range overflows".to_owned()
            })?;
        if page_count
            > u64::try_from(MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_PAGES)
                .expect("checkpoint page limit fits u64")
        {
            return Err(
                "metadata-transfer staging checkpoint anchor exceeds the page limit".to_owned(),
            );
        }

        type FinalizedPageMember = (u64, MetadataTransferStagingEvidenceKey, [u8; 32], Vec<u8>);
        let mut page_members = BTreeMap::<u64, Vec<FinalizedPageMember>>::new();
        let mut page_candidates = BTreeMap::new();
        let mut commitments = BTreeMap::new();
        let finalized_members = finalized_checkpoints
            .get(&(
                actor.node_id(),
                actor.node_incarnation(),
                first_generation,
                last_generation,
                source_segment_digest,
            ))
            .ok_or_else(|| {
                "metadata-transfer staging checkpoint anchor has no finalized members".to_owned()
            })?;
        for (key, floor) in finalized_members {
            let binding = floor.checkpoint_bindings.get(*key).ok_or_else(|| {
                "metadata-transfer staging checkpoint anchor member lost its binding".to_owned()
            })?;
            if binding.page_generation < first_generation
                || binding.page_generation > last_generation
                || binding.page_sequence == 0
            {
                return Err(
                    "metadata-transfer staging checkpoint anchor has an invalid finalized member binding"
                        .to_owned(),
                );
            }
            let finalized = finalized_evidence.get(*key).ok_or_else(|| {
                "metadata-transfer staging checkpoint anchor member is absent from its finalized certificate"
                    .to_owned()
            })?;
            if !std::ptr::eq(finalized.floor, *floor) {
                return Err(
                    "metadata-transfer staging checkpoint anchor member belongs to a different finalized floor"
                        .to_owned(),
                );
            }
            let evidence_digest = finalized.evidence_digest;
            let evidence = self.metadata_transfer_staging_checkpoint_evidence_bytes(
                key,
                actor,
                finalized_evidence,
                evidence_digest,
            )?;
            if commitments
                .insert((*key).clone(), evidence_digest)
                .is_some()
            {
                return Err(
                    "metadata-transfer staging checkpoint anchor has a duplicate finalized member"
                        .to_owned(),
                );
            }
            if let Some(existing) = page_candidates.get(&binding.page_generation) {
                if existing != &binding.actor_closure_candidate {
                    return Err(
                        "metadata-transfer staging checkpoint anchor page has conflicting actor-closure candidates"
                            .to_owned(),
                    );
                }
            } else {
                page_candidates.insert(
                    binding.page_generation,
                    binding.actor_closure_candidate.clone(),
                );
            }
            page_members
                .entry(binding.page_generation)
                .or_default()
                .push((
                    binding.page_sequence,
                    (*key).clone(),
                    evidence_digest,
                    evidence,
                ));
        }
        if commitments.is_empty()
            || commitments.len() > MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COMMITMENTS
        {
            return Err(
                "metadata-transfer staging checkpoint anchor has an invalid commitment count"
                    .to_owned(),
            );
        }

        let mut preceding_generation = previous_generation;
        let mut preceding_digest = previous_apply_receipt_digest;
        let mut page_links = Vec::with_capacity(usize::try_from(page_count).map_err(|_| {
            "metadata-transfer staging checkpoint anchor page count does not fit usize".to_owned()
        })?);
        let mut tip_apply_receipt = None;
        for generation in first_generation..=last_generation {
            let mut members = page_members.remove(&generation).ok_or_else(|| {
                "metadata-transfer staging checkpoint anchor has an empty page".to_owned()
            })?;
            members.sort_by_key(|member| member.0);
            if members.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
                return Err(
                    "metadata-transfer staging checkpoint anchor has duplicate page sequences"
                        .to_owned(),
                );
            }
            let entries = members
                .iter()
                .map(|(sequence, _, _, evidence)| (*sequence, evidence.clone()))
                .collect::<Vec<_>>();
            let actor_closure_candidate = page_candidates.remove(&generation).ok_or_else(|| {
                "metadata-transfer staging checkpoint anchor page has no closure binding".to_owned()
            })?;
            let page_digest = crate::pg_store::
                metadata_transfer_staging_checkpoint_page_digest_with_actor_closure(
                    actor,
                    actor_closure_candidate.as_ref(),
                    preceding_generation,
                    preceding_digest,
                    generation,
                    &entries,
                )
                .map_err(|error| error.to_string())?;
            let receipt =
                crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_checkpoint_link(
                    actor.clone(),
                    preceding_generation,
                    preceding_digest,
                    generation,
                    page_digest,
                );
            let receipt_digest = checksum::sha256::digest(receipt.as_bytes());
            page_links.push(MetadataTransferStagingEvidenceCheckpointPageLink {
                page_digest,
                previous_apply_receipt_digest: preceding_digest,
                apply_receipt_digest: receipt_digest,
                actor_closure_candidate,
                entries: members
                    .into_iter()
                    .map(|(sequence, evidence_key, _, _)| {
                        MetadataTransferStagingEvidenceCheckpointPageEntry {
                            sequence,
                            evidence_key,
                        }
                    })
                    .collect(),
            });
            preceding_generation = generation;
            preceding_digest = receipt_digest;
            if generation == last_generation {
                tip_apply_receipt = Some(receipt.as_bytes().to_vec());
            }
        }
        if !page_members.is_empty() {
            return Err(
                "metadata-transfer staging checkpoint anchor has out-of-range page members"
                    .to_owned(),
            );
        }
        if !page_candidates.is_empty() {
            return Err(
                "metadata-transfer staging checkpoint anchor has out-of-range closure bindings"
                    .to_owned(),
            );
        }
        Ok(MetadataTransferStagingEvidenceCheckpointSegment {
            actor: actor.clone(),
            first_generation,
            last_generation,
            previous_generation,
            previous_apply_receipt_digest,
            page_links,
            tip_apply_receipt: tip_apply_receipt.ok_or_else(|| {
                "metadata-transfer staging checkpoint source segment has no tip receipt".to_owned()
            })?,
            commitments,
        })
    }

    fn reconstruct_metadata_transfer_staging_checkpoint_anchor_sources(
        &self,
        anchor: &MetadataTransferStagingEvidenceCheckpointAnchor,
        finalized_evidence: &MetadataTransferStagingFinalizedEvidenceIndex<'_>,
        finalized_checkpoints: &MetadataTransferStagingFinalizedCheckpointIndex<'_>,
    ) -> Result<Vec<(u64, u64, [u8; 32])>, String> {
        let sources = metadata_transfer_staging_checkpoint_source_segments(
            finalized_checkpoints,
            anchor.actor.node_id(),
            anchor.actor.node_incarnation(),
            anchor.first_generation,
            anchor.last_generation,
        )?;
        let mut preceding_generation = anchor.previous_generation;
        let mut preceding_digest = anchor.previous_apply_receipt_digest;
        let mut tip_apply_receipt = None;
        for (first_generation, last_generation, source_digest) in &sources {
            let segment = self.reconstruct_metadata_transfer_staging_checkpoint_source_segment(
                &anchor.actor,
                MetadataTransferStagingCheckpointSourceSegmentBinding {
                    first_generation: *first_generation,
                    last_generation: *last_generation,
                    previous_generation: preceding_generation,
                    previous_apply_receipt_digest: preceding_digest,
                    source_segment_digest: *source_digest,
                },
                finalized_evidence,
                finalized_checkpoints,
            )?;
            if metadata_transfer_staging_checkpoint_segment_digest(&segment) != *source_digest {
                return Err(
                    "metadata-transfer staging checkpoint anchor source digest is not reconstructible"
                        .to_owned(),
                );
            }
            preceding_generation = *last_generation;
            preceding_digest = checksum::sha256::digest(&segment.tip_apply_receipt);
            tip_apply_receipt = Some(segment.tip_apply_receipt);
        }
        if sources.is_empty()
            || sources.len()
                != usize::try_from(anchor.source_segment_count).map_err(|_| {
                    "metadata-transfer staging checkpoint source count does not fit usize"
                        .to_owned()
                })?
            || sources.first().map(|source| source.0) != Some(anchor.first_generation)
            || sources.last().map(|source| source.1) != Some(anchor.last_generation)
            || metadata_transfer_staging_checkpoint_source_segments_digest(&sources)
                != anchor.source_segments_digest
            || tip_apply_receipt.as_deref() != Some(anchor.tip_apply_receipt.as_slice())
        {
            return Err(
                "metadata-transfer staging checkpoint anchor cumulative source is invalid"
                    .to_owned(),
            );
        }
        Ok(sources)
    }

    fn validate_metadata_transfer_staging_finalized_checkpoint_binding(
        &self,
        key: &MetadataTransferStagingEvidenceKey,
        evidence_digest: [u8; 32],
        expected_endpoint: &str,
        binding: &MetadataTransferStagingFinalizedCheckpointBinding,
    ) -> Result<(), String> {
        if binding.actor_node_id != key.actor_node_id
            || binding.actor_node_incarnation != key.actor_node_incarnation
            || binding.actor_endpoint != expected_endpoint
            || binding.first_generation == 0
            || binding.first_generation > binding.last_generation
            || binding.page_generation < binding.first_generation
            || binding.page_generation > binding.last_generation
            || binding.page_sequence == 0
        {
            return Err(
                "metadata-transfer staging finalized checkpoint binding has invalid identity"
                    .to_owned(),
            );
        }
        let segment_key = (
            binding.actor_node_id,
            binding.actor_node_incarnation,
            binding.first_generation,
        );
        if let Some(segment) = self
            .metadata_transfer_staging_evidence_checkpoint_segments
            .get(&segment_key)
        {
            if segment.last_generation != binding.last_generation
                || segment.actor.endpoint() != binding.actor_endpoint
                || metadata_transfer_staging_checkpoint_segment_digest(segment)
                    != binding.segment_digest
                || segment.commitments.get(key) != Some(&evidence_digest)
                || segment
                    .page_links
                    .get(
                        usize::try_from(binding.page_generation - binding.first_generation)
                            .map_err(|_| {
                                "metadata-transfer staging finalized checkpoint page offset does not fit usize"
                                    .to_owned()
                            })?,
                    )
                    .is_none_or(|link| {
                        link.actor_closure_candidate != binding.actor_closure_candidate
                            || !link.entries.iter().any(|entry| {
                                entry.sequence == binding.page_sequence
                                    && entry.evidence_key == *key
                            })
                    })
            {
                return Err(
                    "metadata-transfer staging finalized checkpoint binding does not match its segment"
                        .to_owned(),
                );
            }
            return Ok(());
        }
        let anchor = self
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .range(
                (binding.actor_node_id, binding.actor_node_incarnation, 0)
                    ..=(
                        binding.actor_node_id,
                        binding.actor_node_incarnation,
                        binding.first_generation,
                    ),
            )
            .next_back()
            .map(|(_, anchor)| anchor)
            .ok_or_else(|| {
                "metadata-transfer staging finalized checkpoint binding has no retained segment or anchor"
                    .to_owned()
            })?;
        if anchor.first_generation > binding.first_generation
            || anchor.last_generation < binding.last_generation
            || anchor.actor.endpoint() != binding.actor_endpoint
        {
            return Err(
                "metadata-transfer staging finalized checkpoint binding does not match its anchor"
                    .to_owned(),
            );
        }
        Ok(())
    }

    fn validate_metadata_transfer_staging_finalized_evidence_retention(
        &self,
        key: &MetadataTransferStagingEvidenceKey,
        evidence_digest: [u8; 32],
        expected_endpoint: &str,
        binding: Option<&MetadataTransferStagingFinalizedCheckpointBinding>,
    ) -> Result<(), String> {
        if let Some(binding) = binding {
            return self.validate_metadata_transfer_staging_finalized_checkpoint_binding(
                key,
                evidence_digest,
                expected_endpoint,
                binding,
            );
        }
        let bytes = self
            .metadata_transfer_staging_evidence
            .get(key)
            .ok_or_else(|| {
                "metadata-transfer staging finalized evidence has neither detail nor checkpoint binding"
                    .to_owned()
            })?;
        let evidence =
            crate::pg_store::decode_staging_evidence(bytes).map_err(|error| error.to_string())?;
        if checksum::sha256::digest(bytes) != evidence_digest
            || evidence.actor().endpoint() != expected_endpoint
        {
            return Err(
                "metadata-transfer staging finalized detailed evidence is not exact".to_owned(),
            );
        }
        Ok(())
    }

    fn validate_metadata_transfer_staging_evidence_invariants(&self) -> Result<(), String> {
        let mut page_members = BTreeMap::new();
        let mut finalized_page_members = BTreeSet::new();
        let mut checkpoint_members = BTreeMap::new();
        let finalized_evidence = metadata_transfer_staging_finalized_evidence_index(
            &self.metadata_transfer_staging_finalized_floors,
        )?;
        let finalized_checkpoints = metadata_transfer_staging_finalized_checkpoint_index(
            &self.metadata_transfer_staging_finalized_floors,
        )?;
        let retired_closure_destinations = self
            .metadata_transfer_staging_retired_actor_closures
            .values()
            .map(|closure| closure.destination_actor.clone())
            .collect::<BTreeSet<_>>();
        type ChainRange = (u64, u64, u64, [u8; 32], [u8; 32], bool);
        let mut actor_chains: BTreeMap<(NodeId, u64, String), Vec<ChainRange>> = BTreeMap::new();
        for (key, record) in &self.metadata_transfer_staging_evidence_pages {
            let page = crate::pg_store::decode_staging_evidence_page_payload(
                &record.operation_payload,
                record.page_digest,
            )
            .map_err(|error| error.to_string())?;
            if *key
                != (
                    page.actor().node_id(),
                    page.actor().node_incarnation(),
                    page.generation(),
                )
            {
                return Err(
                    "metadata-transfer staging page key does not match its actor and generation"
                        .to_owned(),
                );
            }
            let node = self.nodes.get(&page.actor().node_id()).ok_or_else(|| {
                "metadata-transfer staging page references an unknown actor".to_owned()
            })?;
            if node.node_incarnation < page.actor().node_incarnation()
                || (node.node_incarnation == page.actor().node_incarnation()
                    && node.endpoint != page.actor().endpoint())
            {
                return Err(
                    "metadata-transfer staging page actor is incompatible with current node identity"
                        .to_owned(),
                );
            }
            let receipt =
                crate::pg_store::decode_staging_evidence_apply_receipt(&record.apply_receipt)
                    .map_err(|error| error.to_string())?;
            let expected =
                crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(&page);
            if receipt.as_bytes() != expected.as_bytes() {
                return Err(
                    "metadata-transfer staging page has a mismatched apply receipt".to_owned(),
                );
            }
            actor_chains
                .entry((key.0, key.1, page.actor().endpoint().to_owned()))
                .or_default()
                .push((
                    page.generation(),
                    page.generation(),
                    page.previous_generation(),
                    page.previous_apply_receipt_digest(),
                    checksum::sha256::digest(&record.apply_receipt),
                    true,
                ));
            let actor_has_closure_candidate = page.actor_closure_candidate().is_some()
                || self
                    .metadata_transfer_staging_evidence_pages
                    .get(&(page.actor().node_id(), page.actor().node_incarnation(), 1))
                    .and_then(|record| {
                        decode_staging_evidence_page_payload(
                            &record.operation_payload,
                            record.page_digest,
                        )
                        .ok()
                    })
                    .is_some_and(|genesis| genesis.actor_closure_candidate().is_some())
                || retired_closure_destinations.contains(page.actor());
            for entry in page.entries() {
                let evidence = crate::pg_store::decode_staging_evidence(entry.evidence())
                    .map_err(|error| error.to_string())?;
                if evidence.actor() != page.actor() {
                    return Err(
                        "metadata-transfer staging page contains foreign actor evidence".to_owned(),
                    );
                }
                let evidence_key = MetadataTransferStagingEvidenceKey {
                    pg_id: evidence.intent().pg_id(),
                    staging_generation: evidence.intent().staging_generation(),
                    actor_node_id: evidence.actor().node_id(),
                    actor_node_incarnation: evidence.actor().node_incarnation(),
                    kind: evidence.kind(),
                    target_epoch: evidence.target_epoch(),
                };
                let finalized_replay = metadata_transfer_staging_finalized_generation(
                    &self.metadata_transfer_staging_finalized_floors,
                    evidence_key.pg_id,
                )
                .is_some_and(|floor| evidence_key.staging_generation <= floor);
                if finalized_replay {
                    let floor = self
                        .metadata_transfer_staging_finalized_floors
                        .get(&(evidence_key.pg_id, evidence_key.staging_generation))
                        .ok_or_else(|| {
                            "metadata-transfer staging page replay has no exact finalized certificate"
                                .to_owned()
                        })?;
                    if !actor_has_closure_candidate
                        && floor.checkpoint_bindings.contains_key(&evidence_key)
                    {
                        return Err(
                            "metadata-transfer staging page retains checkpointed evidence at or below its finalized floor without actor rollover"
                                .to_owned(),
                        );
                    }
                    let expected = metadata_transfer_staging_finalized_semantic_evidence_bytes(
                        floor,
                        evidence.actor(),
                        evidence.kind(),
                        evidence.target_epoch(),
                    )?;
                    if expected != entry.evidence() {
                        return Err(
                            "metadata-transfer staging page does not exactly replay finalized evidence"
                                .to_owned(),
                        );
                    }
                    if self.metadata_transfer_staging_evidence.get(&evidence_key)
                        == Some(&entry.evidence().to_vec())
                    {
                        if page_members
                            .insert(evidence_key, entry.evidence().to_vec())
                            .is_some()
                        {
                            return Err(
                                "metadata-transfer staging finalized evidence appears in more than one retained page"
                                    .to_owned(),
                            );
                        }
                    } else if !finalized_page_members.insert(evidence_key) {
                        return Err(
                            "metadata-transfer staging page does not exactly replay unique finalized evidence"
                                .to_owned(),
                        );
                    }
                } else if page_members
                    .insert(evidence_key, entry.evidence().to_vec())
                    .is_some()
                {
                    return Err(
                        "metadata-transfer staging evidence appears in more than one retained page"
                            .to_owned(),
                    );
                }
            }
        }
        for (key, segment) in &self.metadata_transfer_staging_evidence_checkpoint_segments {
            if *key
                != (
                    segment.actor.node_id(),
                    segment.actor.node_incarnation(),
                    segment.first_generation,
                )
            {
                return Err(
                    "metadata-transfer staging checkpoint key does not match its actor and range"
                        .to_owned(),
                );
            }
            let node = self.nodes.get(&segment.actor.node_id()).ok_or_else(|| {
                "metadata-transfer staging checkpoint references an unknown actor".to_owned()
            })?;
            if node.node_incarnation < segment.actor.node_incarnation()
                || (node.node_incarnation == segment.actor.node_incarnation()
                    && node.endpoint != segment.actor.endpoint())
            {
                return Err(
                    "metadata-transfer staging checkpoint actor is incompatible with current node identity"
                        .to_owned(),
                );
            }
            if segment.first_generation == 0
                || segment.first_generation > segment.last_generation
                || segment.commitments.is_empty()
                || segment.commitments.len()
                    > MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COMMITMENTS
                || segment.page_links.is_empty()
                || segment.page_links.len()
                    > MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_PAGES
                || u64::try_from(segment.page_links.len()).ok()
                    != segment
                        .last_generation
                        .checked_sub(segment.first_generation)
                        .and_then(|distance| distance.checked_add(1))
            {
                return Err(
                    "metadata-transfer staging checkpoint has invalid range or bounds".to_owned(),
                );
            }
            if metadata_transfer_staging_evidence_checkpoint_state_record_len(segment)
                > MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_STATE_RECORD_BYTES
            {
                return Err(
                    "metadata-transfer staging checkpoint exceeds its encoded byte limit"
                        .to_owned(),
                );
            }
            let mut preceding_generation = segment.previous_generation;
            let mut preceding_digest = segment.previous_apply_receipt_digest;
            let mut segment_members = BTreeSet::new();
            for (offset, link) in segment.page_links.iter().enumerate() {
                if link.previous_apply_receipt_digest != preceding_digest
                    || link.entries.is_empty()
                    || link.entries.len() > crate::pg_store::MAX_STAGING_EVIDENCE_PAGE_ENTRIES
                    || link
                        .entries
                        .windows(2)
                        .any(|pair| pair[0].sequence >= pair[1].sequence)
                {
                    return Err(
                        "metadata-transfer staging checkpoint page link is invalid".to_owned()
                    );
                }
                let generation = segment
                    .first_generation
                    .checked_add(u64::try_from(offset).map_err(|_| {
                        "metadata-transfer staging checkpoint page offset does not fit u64"
                            .to_owned()
                    })?)
                    .ok_or_else(|| {
                        "metadata-transfer staging checkpoint page generation overflow".to_owned()
                    })?;
                let mut page_entries = Vec::with_capacity(link.entries.len());
                for entry in &link.entries {
                    if !segment_members.insert(entry.evidence_key.clone())
                        || entry.evidence_key.actor_node_id != segment.actor.node_id()
                        || entry.evidence_key.actor_node_incarnation
                            != segment.actor.node_incarnation()
                    {
                        return Err(
                            "metadata-transfer staging checkpoint page membership is invalid"
                                .to_owned(),
                        );
                    }
                    let evidence_digest = *segment
                        .commitments
                        .get(&entry.evidence_key)
                        .ok_or_else(|| {
                            "metadata-transfer staging checkpoint page member has no commitment"
                                .to_owned()
                        })?;
                    let evidence = self.metadata_transfer_staging_checkpoint_evidence_bytes(
                        &entry.evidence_key,
                        &segment.actor,
                        &finalized_evidence,
                        evidence_digest,
                    )?;
                    page_entries.push((entry.sequence, evidence));
                    if checkpoint_members
                        .insert(
                            entry.evidence_key.clone(),
                            (evidence_digest, segment.actor.clone()),
                        )
                        .is_some()
                        || page_members.contains_key(&entry.evidence_key)
                        || finalized_page_members.contains(&entry.evidence_key)
                    {
                        return Err(
                            "metadata-transfer staging evidence appears in more than one retained chain range"
                                .to_owned(),
                        );
                    }
                }
                let reconstructed_page_digest = crate::pg_store::
                    metadata_transfer_staging_checkpoint_page_digest_with_actor_closure(
                        &segment.actor,
                        link.actor_closure_candidate.as_ref(),
                        preceding_generation,
                        preceding_digest,
                        generation,
                        &page_entries,
                    )
                    .map_err(|error| error.to_string())?;
                if reconstructed_page_digest != link.page_digest {
                    return Err(
                        "metadata-transfer staging checkpoint page membership does not match its digest"
                            .to_owned(),
                    );
                }
                if link.actor_closure_candidate.is_some() {
                    let closures = self
                        .metadata_transfer_staging_actor_closures
                        .values()
                        .filter(|closure| {
                            closure.destination_actor == segment.actor
                                && closure.destination_genesis_page_digest == link.page_digest
                        })
                        .collect::<Vec<_>>();
                    if generation != 1
                        || segment.first_generation != 1
                        || closures.is_empty()
                        || closures.iter().any(|closure| {
                            self.metadata_transfer_staging_retired_actor_closures.get(&(
                                closure.source_actor.node_id(),
                                closure.source_actor.node_incarnation(),
                            )) != Some(*closure)
                        })
                    {
                        return Err(
                            "metadata-transfer staging checkpoint has an unretired actor-closure candidate"
                                .to_owned(),
                        );
                    }
                }
                let expected_receipt = crate::pg_store::
                    MetadataTransferStagingEvidenceApplyReceipt::for_checkpoint_link(
                        segment.actor.clone(),
                        preceding_generation,
                        preceding_digest,
                        generation,
                        link.page_digest,
                    );
                if checksum::sha256::digest(expected_receipt.as_bytes())
                    != link.apply_receipt_digest
                {
                    return Err(
                        "metadata-transfer staging checkpoint page link has an invalid apply receipt digest"
                            .to_owned(),
                    );
                }
                preceding_generation = generation;
                preceding_digest = link.apply_receipt_digest;
            }
            let tip_receipt =
                crate::pg_store::decode_staging_evidence_apply_receipt(&segment.tip_apply_receipt)
                    .map_err(|error| error.to_string())?;
            let tip_link = segment.page_links.last().expect("nonempty links validated");
            if tip_receipt.actor() != &segment.actor
                || tip_receipt.generation() != segment.last_generation
                || tip_receipt.accepted_generation() != segment.last_generation
                || tip_receipt.page_digest() != tip_link.page_digest
                || tip_receipt.previous_generation() != segment.last_generation.saturating_sub(1)
                || tip_receipt.previous_apply_receipt_digest()
                    != tip_link.previous_apply_receipt_digest
                || checksum::sha256::digest(&segment.tip_apply_receipt)
                    != tip_link.apply_receipt_digest
            {
                return Err(
                    "metadata-transfer staging checkpoint tip receipt is invalid".to_owned(),
                );
            }
            if segment_members.len() != segment.commitments.len() {
                return Err(
                    "metadata-transfer staging checkpoint has unassigned commitments".to_owned(),
                );
            }
            actor_chains
                .entry((key.0, key.1, segment.actor.endpoint().to_owned()))
                .or_default()
                .push((
                    segment.first_generation,
                    segment.last_generation,
                    segment.previous_generation,
                    segment.previous_apply_receipt_digest,
                    checksum::sha256::digest(&segment.tip_apply_receipt),
                    false,
                ));
        }
        for (key, anchor) in &self.metadata_transfer_staging_evidence_checkpoint_anchors {
            if *key
                != (
                    anchor.actor.node_id(),
                    anchor.actor.node_incarnation(),
                    anchor.first_generation,
                )
                || anchor.first_generation == 0
                || anchor.first_generation > anchor.last_generation
            {
                return Err(
                    "metadata-transfer staging checkpoint anchor has invalid identity or range"
                        .to_owned(),
                );
            }
            let node = self.nodes.get(&anchor.actor.node_id()).ok_or_else(|| {
                "metadata-transfer staging checkpoint anchor references an unknown actor".to_owned()
            })?;
            if node.node_incarnation < anchor.actor.node_incarnation()
                || (node.node_incarnation == anchor.actor.node_incarnation()
                    && node.endpoint != anchor.actor.endpoint())
            {
                return Err(
                    "metadata-transfer staging checkpoint anchor actor is incompatible with current node identity"
                    .to_owned(),
                );
            }
            let sources = self.reconstruct_metadata_transfer_staging_checkpoint_anchor_sources(
                anchor,
                &finalized_evidence,
                &finalized_checkpoints,
            )?;
            let leaf_anchor = anchor.source_segment_count == 1;
            if anchor.source_segment_count == 0
                || (leaf_anchor && anchor.source_segment_digest != sources[0].2)
                || (!leaf_anchor && anchor.source_segment_digest != [0; 32])
            {
                return Err(
                    "metadata-transfer staging checkpoint anchor provenance is invalid".to_owned(),
                );
            }
            let tip_receipt =
                crate::pg_store::decode_staging_evidence_apply_receipt(&anchor.tip_apply_receipt)
                    .map_err(|error| error.to_string())?;
            if tip_receipt.actor() != &anchor.actor
                || tip_receipt.generation() != anchor.last_generation
                || tip_receipt.accepted_generation() != anchor.last_generation
                || tip_receipt.previous_generation() != anchor.last_generation.saturating_sub(1)
            {
                return Err(
                    "metadata-transfer staging checkpoint anchor tip receipt is invalid".to_owned(),
                );
            }
            actor_chains
                .entry((key.0, key.1, anchor.actor.endpoint().to_owned()))
                .or_default()
                .push((
                    anchor.first_generation,
                    anchor.last_generation,
                    anchor.previous_generation,
                    anchor.previous_apply_receipt_digest,
                    checksum::sha256::digest(&anchor.tip_apply_receipt),
                    false,
                ));
        }
        let reconstructed_actor_closures = self
            .reconstruct_metadata_transfer_staging_actor_closures()
            .map_err(|error| error.to_string())?;
        if reconstructed_actor_closures.iter().any(|(key, closure)| {
            self.metadata_transfer_staging_actor_closures.get(key) != Some(closure)
        }) || self
            .metadata_transfer_staging_actor_closures
            .iter()
            .any(|(key, closure)| {
                !reconstructed_actor_closures.contains_key(key)
                    && self
                        .metadata_transfer_staging_retired_actor_closures
                        .get(key)
                        != Some(closure)
            })
            || self
                .metadata_transfer_staging_retired_actor_closures
                .iter()
                .any(|(key, closure)| {
                    self.metadata_transfer_staging_actor_closures.get(key) != Some(closure)
                })
        {
            return Err(
                "metadata-transfer staging actor closures do not match retained chain evidence"
                    .to_owned(),
            );
        }
        if !self
            .metadata_transfer_staging_retired_actor_closures
            .is_empty()
        {
            let closure_validation_index = self
                .metadata_transfer_staging_actor_closure_validation_index(
                    &finalized_evidence,
                    &finalized_checkpoints,
                )?;
            for (key, certificate) in &self.metadata_transfer_staging_retired_actor_closures {
                Self::validate_retired_metadata_transfer_staging_actor_closure(
                    *key,
                    certificate,
                    &closure_validation_index,
                )?;
            }
        }

        for ((node_id, incarnation, endpoint), ranges) in &mut actor_chains {
            ranges.sort_by_key(|range| range.0);
            let mut expected_generation = 1_u64;
            let mut previous_generation = 0_u64;
            let mut previous_receipt_digest = [0; 32];
            for (first, last, predecessor, predecessor_digest, tip_digest, _) in ranges.iter() {
                if *first != expected_generation
                    || *predecessor != previous_generation
                    || *predecessor_digest != previous_receipt_digest
                {
                    return Err(
                        "metadata-transfer staging actor chain has a gap or invalid predecessor"
                            .to_owned(),
                    );
                }
                expected_generation = last.checked_add(1).ok_or_else(|| {
                    "metadata-transfer staging actor generation overflow".to_owned()
                })?;
                previous_generation = *last;
                previous_receipt_digest = *tip_digest;
            }
            let has_replayable_tip = ranges.last().is_some_and(|range| range.5);
            let has_closed_tip = ranges.last().is_some_and(|range| {
                self.metadata_transfer_staging_actor_closures
                    .get(&(*node_id, *incarnation))
                    .is_some_and(|closure| {
                        closure.source_actor.node_id() == *node_id
                            && closure.source_actor.node_incarnation() == *incarnation
                            && closure.source_actor.endpoint() == endpoint
                            && closure.source_tip_generation == range.1
                            && closure.source_tip_apply_receipt_digest == range.4
                    })
            });
            if !has_replayable_tip && !has_closed_tip {
                return Err(
                    "metadata-transfer staging actor chain must retain a replayable or closed tip"
                        .to_owned(),
                );
            }
        }
        for ((pg_id, staging_generation), floor) in &self.metadata_transfer_staging_finalized_floors
        {
            if *pg_id != floor.transition.pg_id()
                || *staging_generation != floor.staging_generation
                || floor.staging_generation != floor.transition.transition_epoch().get()
                || (floor.disposition == MetadataTransferStagingCleanupDisposition::Completed
                    && floor.publications.is_empty())
                || floor.publications.windows(2).any(|pair| {
                    (pair[0].target_epoch, pair[0].node_id)
                        >= (pair[1].target_epoch, pair[1].node_id)
                })
                || floor.tombstones.is_empty()
                || floor
                    .tombstones
                    .windows(2)
                    .any(|pair| pair[0].node_id >= pair[1].node_id)
                || floor.tombstone_set_digest
                    != metadata_transfer_staging_cleanup_digest(
                        &floor.transition,
                        floor.staging_generation,
                        floor.disposition,
                        floor.artifact_digest,
                        floor.artifact_length,
                        floor.artifact_format_version,
                        &floor.tombstones,
                    )
            {
                return Err(
                    "metadata-transfer staging finalized floor has invalid identity or digest"
                        .to_owned(),
                );
            }
            let transition = self
                .retained_unavailable_pg_placement_transitions
                .get(&(*pg_id, floor.transition.transition_epoch()))
                .filter(|transition| floor.transition.matches_transition(transition))
                .ok_or_else(|| {
                    "metadata-transfer staging finalized floor has no exact retained transition"
                        .to_owned()
                })?;
            match floor.disposition {
                MetadataTransferStagingCleanupDisposition::Completed => {
                    if transition.destination_epoch.is_none()
                        || transition.completion.is_none()
                        || transition.completion_batch_receipt.is_none()
                    {
                        return Err(
                            "metadata-transfer staging finalized floor transition is not completed"
                                .to_owned(),
                        );
                    }
                }
                MetadataTransferStagingCleanupDisposition::Superseded {
                    successor_transition_epoch,
                } => {
                    let successor_matches = self
                        .retained_unavailable_pg_placement_transitions
                        .get(&(*pg_id, successor_transition_epoch))
                        .or_else(|| {
                            self.unavailable_pg_placement_transitions.get(pg_id).filter(
                                |candidate| {
                                    candidate.transition_epoch == successor_transition_epoch
                                },
                            )
                        })
                        .is_some_and(|successor| {
                            successor.predecessor_transition_epoch
                                == Some(transition.transition_epoch)
                        });
                    if transition.destination_epoch.is_some()
                        || transition.destination_install.is_some()
                        || transition.completion.is_some()
                        || transition.completion_batch_receipt.is_some()
                        || !successor_matches
                    {
                        return Err(
                            "metadata-transfer staging finalized floor cancellation is not an exact pre-install successor"
                                .to_owned(),
                        );
                    }
                }
            }
            let authorization = transition.staging_authorization.as_ref().ok_or_else(|| {
                "metadata-transfer staging finalized floor has no retained authorization".to_owned()
            })?;
            if authorization.staging_generation != floor.staging_generation
                || authorization.artifact_digest != floor.artifact_digest
                || authorization.artifact_length != floor.artifact_length
                || authorization.artifact_format_version != floor.artifact_format_version
                || transition.destination_acting_set.len() != floor.tombstones.len()
                || transition
                    .destination_acting_set
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    != floor
                        .tombstones
                        .iter()
                        .map(|tombstone| tombstone.node_id)
                        .collect()
            {
                return Err(
                    "metadata-transfer staging finalized floor does not match its authorization"
                        .to_owned(),
                );
            }
            let install = transition.destination_install.as_ref();
            if floor.disposition == MetadataTransferStagingCleanupDisposition::Completed
                && install.is_none()
            {
                return Err(
                    "metadata-transfer staging finalized floor has no destination install"
                        .to_owned(),
                );
            }
            let mut expected_checkpoint_keys = BTreeSet::new();
            let mut publication_transfers = BTreeMap::new();
            for publication in &floor.publications {
                let node = self.nodes.get(&publication.node_id).ok_or_else(|| {
                    "metadata-transfer staging finalized publication references an unknown actor"
                        .to_owned()
                })?;
                if !transition
                    .destination_acting_set
                    .contains(&publication.node_id)
                    || node.node_incarnation < publication.node_incarnation
                    || (node.node_incarnation == publication.node_incarnation
                        && node.endpoint != publication.endpoint)
                    || publication.target_epoch <= transition.transition_epoch
                    || publication.transfer.source_epoch() > transition.source_epoch
                {
                    return Err(
                        "metadata-transfer staging finalized publication has invalid authority"
                            .to_owned(),
                    );
                }
                if let Some(existing) =
                    publication_transfers.insert(publication.target_epoch, publication.transfer)
                {
                    if existing != publication.transfer {
                        return Err(
                            "metadata-transfer staging finalized publication target has conflicting proofs"
                                .to_owned(),
                        );
                    }
                }
                let key = MetadataTransferStagingEvidenceKey {
                    pg_id: *pg_id,
                    staging_generation: floor.staging_generation,
                    actor_node_id: publication.node_id,
                    actor_node_incarnation: publication.node_incarnation,
                    kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                    target_epoch: Some(publication.target_epoch),
                };
                expected_checkpoint_keys.insert(key.clone());
                self.validate_metadata_transfer_staging_finalized_evidence_retention(
                    &key,
                    publication.evidence_digest,
                    &publication.endpoint,
                    floor.checkpoint_bindings.get(&key),
                )?;
            }
            if publication_transfers.len() > crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT {
                return Err(format!(
                    "metadata-transfer staging finalized publication exceeds the per-intent target limit {}",
                    crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT
                ));
            }
            if let Some(install) = install {
                let final_publications = floor
                    .publications
                    .iter()
                    .filter(|publication| {
                        publication.target_epoch == install.batch_receipt.target_epoch
                    })
                    .collect::<Vec<_>>();
                if final_publications.len() != install.publications.len()
                    || install.publications.iter().any(|installed| {
                        !final_publications.iter().any(|publication| {
                            publication.node_id == installed.node_id
                                && publication.node_incarnation == installed.node_incarnation
                                && publication.endpoint == installed.endpoint
                                && publication.evidence_digest == installed.evidence_digest
                                && publication.transfer == install.transfer
                        })
                    })
                {
                    return Err(
                        "metadata-transfer staging finalized publication does not match its destination install"
                            .to_owned(),
                    );
                }
            }
            for tombstone in &floor.tombstones {
                let node = self.nodes.get(&tombstone.node_id).ok_or_else(|| {
                    "metadata-transfer staging finalized floor references an unknown actor"
                        .to_owned()
                })?;
                if node.node_incarnation < tombstone.node_incarnation
                    || (node.node_incarnation == tombstone.node_incarnation
                        && node.endpoint != tombstone.endpoint)
                {
                    return Err(
                        "metadata-transfer staging finalized floor actor is incompatible with current node identity"
                            .to_owned(),
                    );
                }
                let key = MetadataTransferStagingEvidenceKey {
                    pg_id: *pg_id,
                    staging_generation: floor.staging_generation,
                    actor_node_id: tombstone.node_id,
                    actor_node_incarnation: tombstone.node_incarnation,
                    kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
                    target_epoch: None,
                };
                expected_checkpoint_keys.insert(key.clone());
                self.validate_metadata_transfer_staging_finalized_evidence_retention(
                    &key,
                    tombstone.evidence_digest,
                    &tombstone.endpoint,
                    floor.checkpoint_bindings.get(&key),
                )?;
            }
            if !expected_checkpoint_keys.iter().all(|key| {
                floor.checkpoint_bindings.contains_key(key)
                    || self.metadata_transfer_staging_evidence.contains_key(key)
            }) {
                return Err(
                    "metadata-transfer staging finalized floor lacks retained evidence authority"
                        .to_owned(),
                );
            }
            for (key, binding) in &floor.checkpoint_bindings {
                if key.pg_id != *pg_id || key.staging_generation != *staging_generation {
                    return Err(
                        "metadata-transfer staging finalized floor has a foreign checkpoint binding"
                            .to_owned(),
                    );
                }
                let finalized = finalized_evidence.get(key).filter(|entry| {
                    std::ptr::eq(entry.floor, floor)
                }).ok_or_else(|| {
                    "metadata-transfer staging finalized checkpoint binding has no canonical evidence"
                        .to_owned()
                })?;
                self.validate_metadata_transfer_staging_finalized_checkpoint_binding(
                    key,
                    finalized.evidence_digest,
                    finalized.endpoint,
                    binding,
                )?;
            }
        }
        for transition in self
            .retained_unavailable_pg_placement_transitions
            .values()
            .chain(self.unavailable_pg_placement_transitions.values())
        {
            let Some(authorization) = transition.staging_authorization.as_ref() else {
                continue;
            };
            if metadata_transfer_staging_finalized_generation(
                &self.metadata_transfer_staging_finalized_floors,
                transition.pg_id,
            )
            .is_some_and(|floor| authorization.staging_generation <= floor)
                && !self
                    .metadata_transfer_staging_finalized_floors
                    .contains_key(&(transition.pg_id, authorization.staging_generation))
            {
                return Err(
                    "metadata-transfer staging finalized floor skips an authorized generation"
                        .to_owned(),
                );
            }
        }
        let mut publication_targets = BTreeMap::<_, BTreeSet<_>>::new();
        for (key, bytes) in &self.metadata_transfer_staging_evidence {
            if key.kind == crate::pg_store::MetadataTransferStagingEvidenceKind::Publication {
                let target_epoch = key.target_epoch.ok_or_else(|| {
                    "metadata-transfer publication evidence is missing its target epoch".to_owned()
                })?;
                let targets = publication_targets
                    .entry((key.pg_id, key.staging_generation))
                    .or_default();
                targets.insert(target_epoch);
                if targets.len() > crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT {
                    return Err(format!(
                        "metadata-transfer staging evidence exceeds the per-intent publication-target limit {}",
                        crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT
                    ));
                }
            }
            let evidence = crate::pg_store::decode_staging_evidence(bytes)
                .map_err(|error| error.to_string())?;
            let decoded_key = MetadataTransferStagingEvidenceKey {
                pg_id: evidence.intent().pg_id(),
                staging_generation: evidence.intent().staging_generation(),
                actor_node_id: evidence.actor().node_id(),
                actor_node_incarnation: evidence.actor().node_incarnation(),
                kind: evidence.kind(),
                target_epoch: evidence.target_epoch(),
            };
            if *key != decoded_key || evidence.as_bytes() != bytes {
                return Err(
                    "metadata-transfer staging evidence key does not match its payload".to_owned(),
                );
            }
            match (page_members.get(key), checkpoint_members.get(key)) {
                (Some(expected), None) if expected == bytes => {}
                (None, Some((expected_digest, expected_actor)))
                    if *expected_digest == checksum::sha256::digest(bytes)
                        && evidence.actor() == expected_actor => {}
                _ => {
                    return Err(
                        "metadata-transfer staging detailed evidence does not match its retained chain commitment"
                            .to_owned(),
                    );
                }
            }
            self.validate_metadata_transfer_staging_evidence_authority(&evidence, false)
                .map_err(|error| error.to_string())?;
        }
        for (key, expected) in &page_members {
            if self.metadata_transfer_staging_evidence.get(key) != Some(expected) {
                return Err(
                    "metadata-transfer staging page member lacks exact detailed evidence"
                        .to_owned(),
                );
            }
        }
        for (key, (expected_digest, expected_actor)) in &checkpoint_members {
            let covered = metadata_transfer_staging_finalized_generation(
                &self.metadata_transfer_staging_finalized_floors,
                key.pg_id,
            )
            .is_some_and(|floor| key.staging_generation <= floor);
            match (covered, self.metadata_transfer_staging_evidence.get(key)) {
                (true, None) => {}
                (false, Some(bytes))
                    if *expected_digest == checksum::sha256::digest(bytes)
                        && crate::pg_store::decode_staging_evidence(bytes)
                            .is_ok_and(|evidence| evidence.actor() == expected_actor) => {}
                (true, Some(_)) => {
                    return Err(
                        "metadata-transfer staging finalized evidence remains detailed".to_owned(),
                    );
                }
                (false, _) => {
                    return Err(
                        "metadata-transfer staging checkpoint member lacks exact detailed evidence"
                            .to_owned(),
                    );
                }
            }
        }
        if self.metadata_transfer_staging_evidence.len()
            != page_members.len()
                + checkpoint_members
                    .keys()
                    .filter(|key| {
                        metadata_transfer_staging_finalized_generation(
                            &self.metadata_transfer_staging_finalized_floors,
                            key.pg_id,
                        )
                        .is_none_or(|floor| key.staging_generation > floor)
                    })
                    .count()
        {
            return Err(
                "metadata-transfer staging detailed evidence does not exactly match retained uncovered chain membership"
                    .to_owned(),
            );
        }
        Ok(())
    }

    pub(crate) fn classify_metadata_transfer_staging_evidence_page(
        &self,
        operation_payload: &[u8],
        page_digest: [u8; 32],
    ) -> Result<MetadataTransferStagingEvidencePageClassification, ControlPlaneError> {
        let page =
            crate::pg_store::decode_staging_evidence_page_payload(operation_payload, page_digest)
                .map_err(|error| ControlPlaneError::CommandDecode {
                message: format!("invalid metadata-transfer staging evidence page: {error}"),
            })?;
        let actor_key = (page.actor().node_id(), page.actor().node_incarnation());
        let page_key = (actor_key.0, actor_key.1, page.generation());
        if let Some(existing) = self.metadata_transfer_staging_evidence_pages.get(&page_key) {
            let existing_page = crate::pg_store::decode_staging_evidence_page_payload(
                &existing.operation_payload,
                existing.page_digest,
            )
            .map_err(|error| ControlPlaneError::SnapshotInvariantViolation {
                context: "retained metadata-transfer staging evidence page is invalid",
                message: error.to_string(),
            })?;
            if page.actor() == existing_page.actor()
                && existing.operation_payload == operation_payload
                && existing.page_digest == page_digest
            {
                return Ok(
                    MetadataTransferStagingEvidencePageClassification::ExactReplay {
                        apply_receipt: existing.apply_receipt.clone(),
                    },
                );
            }
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "node {} staging evidence generation {} conflicts with its retained page",
                    page.actor().node_id().as_u32(),
                    page.generation()
                ),
            });
        }
        let latest = self
            .metadata_transfer_staging_evidence_pages
            .range((actor_key.0, actor_key.1, 0)..=(actor_key.0, actor_key.1, u64::MAX))
            .next_back();
        if let Some((_, existing)) = latest {
            let existing_page = crate::pg_store::decode_staging_evidence_page_payload(
                &existing.operation_payload,
                existing.page_digest,
            )
            .map_err(|error| ControlPlaneError::SnapshotInvariantViolation {
                context: "retained metadata-transfer staging evidence page is invalid",
                message: error.to_string(),
            })?;
            let expected_previous_digest = checksum::sha256::digest(&existing.apply_receipt);
            if page.actor() != existing_page.actor()
                || page.generation() != existing_page.generation().checked_add(1).unwrap_or(0)
                || page.previous_generation() != existing_page.generation()
                || page.previous_apply_receipt_digest() != expected_previous_digest
            {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "node {} staging evidence page does not extend the retained generation",
                        page.actor().node_id().as_u32()
                    ),
                });
            }
        } else if page.previous_generation() != 0 || page.previous_apply_receipt_digest() != [0; 32]
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "node {} first staging evidence page is not the genesis generation",
                    page.actor().node_id().as_u32()
                ),
            });
        }

        let mut publication_targets = BTreeMap::<_, BTreeSet<_>>::new();
        for key in self
            .metadata_transfer_staging_evidence
            .keys()
            .filter(|key| {
                key.kind == crate::pg_store::MetadataTransferStagingEvidenceKind::Publication
            })
        {
            let target_epoch =
                key.target_epoch
                    .ok_or_else(|| ControlPlaneError::SnapshotInvariantViolation {
                        context: "retained metadata-transfer staging publication is invalid",
                        message: "publication evidence is missing its target epoch".to_owned(),
                    })?;
            publication_targets
                .entry((key.pg_id, key.staging_generation))
                .or_default()
                .insert(target_epoch);
        }
        let mut decoded_entries = Vec::with_capacity(page.entries().len());
        let mut page_evidence_keys = BTreeSet::new();
        let actor_has_closure_candidate = page.actor_closure_candidate().is_some()
            || self
                .metadata_transfer_staging_evidence_pages
                .get(&(page.actor().node_id(), page.actor().node_incarnation(), 1))
                .and_then(|record| {
                    decode_staging_evidence_page_payload(
                        &record.operation_payload,
                        record.page_digest,
                    )
                    .ok()
                })
                .is_some_and(|genesis| genesis.actor_closure_candidate().is_some());
        for entry in page.entries() {
            let evidence =
                crate::pg_store::decode_staging_evidence(entry.evidence()).map_err(|error| {
                    ControlPlaneError::CommandDecode {
                        message: format!("invalid metadata-transfer staging evidence: {error}"),
                    }
                })?;
            if evidence.actor() != page.actor() {
                return Err(ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging page contains foreign actor evidence"
                        .to_owned(),
                });
            }
            self.validate_metadata_transfer_staging_evidence_authority(&evidence, true)?;
            let key = MetadataTransferStagingEvidenceKey {
                pg_id: evidence.intent().pg_id(),
                staging_generation: evidence.intent().staging_generation(),
                actor_node_id: evidence.actor().node_id(),
                actor_node_incarnation: evidence.actor().node_incarnation(),
                kind: evidence.kind(),
                target_epoch: evidence.target_epoch(),
            };
            let finalized_replay = metadata_transfer_staging_finalized_generation(
                &self.metadata_transfer_staging_finalized_floors,
                key.pg_id,
            )
            .is_some_and(|floor| key.staging_generation <= floor);
            if finalized_replay {
                if !actor_has_closure_candidate {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} staging evidence generation {} is at or below finalized floor",
                            key.pg_id.get(),
                            key.staging_generation
                        ),
                    });
                }
                let floor = self
                    .metadata_transfer_staging_finalized_floors
                    .get(&(key.pg_id, key.staging_generation))
                    .ok_or_else(|| ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} staging evidence generation {} has no exact finalized certificate",
                            key.pg_id.get(),
                            key.staging_generation
                        ),
                    })?;
                let expected = metadata_transfer_staging_finalized_semantic_evidence_bytes(
                    floor,
                    evidence.actor(),
                    evidence.kind(),
                    evidence.target_epoch(),
                )
                .map_err(|message| ControlPlaneError::CommandDecode { message })?;
                if expected != entry.evidence() {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} staging evidence generation {} does not exactly replay finalized semantics",
                            key.pg_id.get(),
                            key.staging_generation
                        ),
                    });
                }
            }
            if key.kind == crate::pg_store::MetadataTransferStagingEvidenceKind::Publication {
                let target_epoch =
                    key.target_epoch
                        .ok_or_else(|| ControlPlaneError::CommandDecode {
                            message: format!(
                                "PG {} staging publication evidence is missing its target epoch",
                                key.pg_id.get()
                            ),
                        })?;
                let targets = publication_targets
                    .entry((key.pg_id, key.staging_generation))
                    .or_default();
                if !targets.contains(&target_epoch)
                    && targets.len() >= crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} staging evidence exceeds the per-intent publication-target limit {}",
                            key.pg_id.get(),
                            crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT
                        ),
                    });
                }
                targets.insert(target_epoch);
            }
            if self.metadata_transfer_staging_evidence.contains_key(&key)
                || !page_evidence_keys.insert(key.clone())
            {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "PG {} staging evidence identity is duplicated or already retained",
                        key.pg_id.get()
                    ),
                });
            }
            if !finalized_replay {
                decoded_entries.push((key, evidence.as_bytes().to_vec()));
            }
        }

        let apply_receipt =
            crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(&page)
                .as_bytes()
                .to_vec();
        Ok(
            MetadataTransferStagingEvidencePageClassification::NewAuthorized {
                page_key,
                decoded_entries,
                apply_receipt,
            },
        )
    }

    fn apply_metadata_transfer_staging_evidence_page(
        &self,
        operation_payload: Vec<u8>,
        page_digest: [u8; 32],
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        let (page_key, decoded_entries, apply_receipt) = match self
            .classify_metadata_transfer_staging_evidence_page(&operation_payload, page_digest)?
        {
            MetadataTransferStagingEvidencePageClassification::NewAuthorized {
                page_key,
                decoded_entries,
                apply_receipt,
            } => (page_key, decoded_entries, apply_receipt),
            MetadataTransferStagingEvidencePageClassification::ExactReplay { apply_receipt } => {
                return Ok(AppliedControlPlaneCommand::new(
                    self.clone(),
                    ControlPlaneCommandResponse::ApplyMetadataTransferStagingEvidencePage {
                        apply_receipt,
                    },
                    false,
                ));
            }
        };
        let mut next_snapshot = self.clone();
        for (key, evidence) in decoded_entries {
            next_snapshot
                .metadata_transfer_staging_evidence
                .insert(key, evidence);
        }
        next_snapshot
            .metadata_transfer_staging_evidence_pages
            .insert(
                page_key,
                MetadataTransferStagingEvidencePageRecord {
                    operation_payload,
                    page_digest,
                    apply_receipt: apply_receipt.clone(),
                },
            );
        let mut reconstructed =
            next_snapshot.reconstruct_metadata_transfer_staging_actor_closures()?;
        for (key, retired) in &next_snapshot.metadata_transfer_staging_retired_actor_closures {
            if reconstructed
                .insert(*key, retired.clone())
                .is_some_and(|active| active != *retired)
            {
                return Err(ControlPlaneError::SnapshotInvariantViolation {
                    context: "metadata-transfer staging retired actor closure",
                    message: "reconstructed certificate conflicts with a retired certificate"
                        .to_owned(),
                });
            }
        }
        next_snapshot.metadata_transfer_staging_actor_closures = reconstructed;
        Ok(AppliedControlPlaneCommand::new(
            next_snapshot,
            ControlPlaneCommandResponse::ApplyMetadataTransferStagingEvidencePage { apply_receipt },
            true,
        ))
    }

    fn reconstruct_metadata_transfer_staging_actor_closures(
        &self,
    ) -> Result<
        BTreeMap<(NodeId, u64), MetadataTransferStagingActorClosureCertificate>,
        ControlPlaneError,
    > {
        let mut actor_tips = BTreeMap::<(NodeId, u64), MetadataTransferStagingActorChainTip>::new();
        let mut page_entries = BTreeMap::<(NodeId, u64), BTreeMap<u64, Vec<u8>>>::new();
        let mut candidates = Vec::new();

        for (key, record) in &self.metadata_transfer_staging_evidence_pages {
            let page =
                decode_staging_evidence_page_payload(&record.operation_payload, record.page_digest)
                    .map_err(|error| ControlPlaneError::SnapshotInvariantViolation {
                        context: "retained metadata-transfer staging evidence page is invalid",
                        message: error.to_string(),
                    })?;
            let actor_key = (key.0, key.1);
            let tip = MetadataTransferStagingActorChainTip {
                actor: page.actor().clone(),
                generation: page.generation(),
                page_digest: page.page_digest(),
                apply_receipt_digest: checksum::sha256::digest(&record.apply_receipt),
            };
            if actor_tips
                .get(&actor_key)
                .is_none_or(|existing| existing.generation < tip.generation)
            {
                actor_tips.insert(actor_key, tip);
            }
            let entries = page_entries.entry(actor_key).or_default();
            for entry in page.entries() {
                if entries
                    .insert(entry.sequence(), entry.evidence().to_vec())
                    .is_some()
                {
                    return Err(ControlPlaneError::SnapshotInvariantViolation {
                        context: "retained metadata-transfer staging evidence page is invalid",
                        message: "actor chain contains a duplicate evidence sequence".to_owned(),
                    });
                }
            }
            if page.generation() == 1 {
                if let Some(candidate) = page.actor_closure_candidate() {
                    candidates.push((page.actor().clone(), candidate.clone(), page.page_digest()));
                }
            }
        }
        for segment in self
            .metadata_transfer_staging_evidence_checkpoint_segments
            .values()
        {
            let link = segment
                .page_links
                .last()
                .expect("snapshot validation requires a nonempty checkpoint segment");
            let actor_key = (segment.actor.node_id(), segment.actor.node_incarnation());
            let tip = MetadataTransferStagingActorChainTip {
                actor: segment.actor.clone(),
                generation: segment.last_generation,
                page_digest: link.page_digest,
                apply_receipt_digest: checksum::sha256::digest(&segment.tip_apply_receipt),
            };
            if actor_tips
                .get(&actor_key)
                .is_none_or(|existing| existing.generation < tip.generation)
            {
                actor_tips.insert(actor_key, tip);
            }
        }
        for anchor in self
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .values()
        {
            let receipt =
                crate::pg_store::decode_staging_evidence_apply_receipt(&anchor.tip_apply_receipt)
                    .map_err(|error| ControlPlaneError::SnapshotInvariantViolation {
                    context: "retained metadata-transfer staging checkpoint anchor is invalid",
                    message: error.to_string(),
                })?;
            let actor_key = (anchor.actor.node_id(), anchor.actor.node_incarnation());
            let tip = MetadataTransferStagingActorChainTip {
                actor: anchor.actor.clone(),
                generation: anchor.last_generation,
                page_digest: receipt.page_digest(),
                apply_receipt_digest: checksum::sha256::digest(&anchor.tip_apply_receipt),
            };
            if actor_tips
                .get(&actor_key)
                .is_none_or(|existing| existing.generation < tip.generation)
            {
                actor_tips.insert(actor_key, tip);
            }
        }

        let mut evidence_by_actor = BTreeMap::<(NodeId, u64), Vec<Vec<u8>>>::new();
        for (actor_key, entries) in &page_entries {
            evidence_by_actor
                .entry(*actor_key)
                .or_default()
                .extend(entries.values().cloned());
        }
        for bytes in self.metadata_transfer_staging_evidence.values() {
            let evidence = crate::pg_store::decode_staging_evidence(bytes).map_err(|error| {
                ControlPlaneError::SnapshotInvariantViolation {
                    context: "retained metadata-transfer staging evidence is invalid",
                    message: error.to_string(),
                }
            })?;
            evidence_by_actor
                .entry((
                    evidence.actor().node_id(),
                    evidence.actor().node_incarnation(),
                ))
                .or_default()
                .push(bytes.clone());
        }
        let finalized_evidence = metadata_transfer_staging_finalized_evidence_index(
            &self.metadata_transfer_staging_finalized_floors,
        )
        .map_err(|message| ControlPlaneError::SnapshotInvariantViolation {
            context: "metadata-transfer staging finalized evidence index",
            message,
        })?;
        for (key, finalized) in finalized_evidence {
            let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
                key.actor_node_id,
                key.actor_node_incarnation,
                finalized.endpoint.to_owned(),
            )
            .map_err(|error| ControlPlaneError::SnapshotInvariantViolation {
                context: "metadata-transfer staging finalized evidence actor",
                message: error.to_string(),
            })?;
            let bytes = metadata_transfer_staging_finalized_semantic_evidence_bytes(
                finalized.floor,
                &actor,
                key.kind,
                key.target_epoch,
            )
            .map_err(|message| ControlPlaneError::SnapshotInvariantViolation {
                context: "metadata-transfer staging finalized evidence",
                message,
            })?;
            if checksum::sha256::digest(&bytes) != finalized.evidence_digest {
                return Err(ControlPlaneError::SnapshotInvariantViolation {
                    context: "metadata-transfer staging finalized evidence",
                    message: "canonical evidence digest does not match its certificate".to_owned(),
                });
            }
            evidence_by_actor
                .entry((key.actor_node_id, key.actor_node_incarnation))
                .or_default()
                .push(bytes);
        }

        candidates.sort_by_key(|(actor, _, _)| (actor.node_id(), actor.node_incarnation()));
        let mut unclosed_actor_tips = actor_tips.clone();
        for retired_key in self.metadata_transfer_staging_retired_actor_closures.keys() {
            unclosed_actor_tips.remove(retired_key);
        }
        let mut closures =
            BTreeMap::<(NodeId, u64), MetadataTransferStagingActorClosureCertificate>::new();
        for (destination_actor, candidate, destination_genesis_page_digest) in candidates {
            let node = self.nodes.get(&destination_actor.node_id()).ok_or(
                ControlPlaneError::UnknownNode {
                    node_id: destination_actor.node_id().as_u32(),
                },
            )?;
            if node.node_incarnation < destination_actor.node_incarnation()
                || (node.node_incarnation == destination_actor.node_incarnation()
                    && node.endpoint != destination_actor.endpoint())
                || candidate.first_actor().node_id() != destination_actor.node_id()
                || candidate.through_actor().node_id() != destination_actor.node_id()
                || candidate.through_actor().node_incarnation()
                    >= destination_actor.node_incarnation()
            {
                return Err(ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging actor closure is not fenced by the current node identity"
                        .to_owned(),
                });
            }

            let first_actor_key = (
                destination_actor.node_id(),
                candidate.first_actor().node_incarnation(),
            );
            if let Some(first_tip) = actor_tips.get(&first_actor_key) {
                if !candidate.accepts_first_tip(
                    &first_tip.actor,
                    first_tip.generation,
                    first_tip.page_digest,
                    first_tip.apply_receipt_digest,
                ) {
                    return Err(ControlPlaneError::CommandDecode {
                        message: "metadata-transfer staging actor closure does not match an authority-retained first-actor tip"
                            .to_owned(),
                    });
                }
            } else if candidate.first_accepted_generation() > 0 {
                return Err(ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging actor closure lost its acknowledged first-actor tip"
                        .to_owned(),
                });
            }
            let through_actor_key = (
                destination_actor.node_id(),
                candidate.through_actor().node_incarnation(),
            );
            if actor_tips
                .get(&through_actor_key)
                .is_some_and(|tip| &tip.actor != candidate.through_actor())
            {
                return Err(ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging actor closure through actor does not match retained chain identity"
                        .to_owned(),
                });
            }

            let rebound_entries = page_entries
                .get(&(
                    destination_actor.node_id(),
                    destination_actor.node_incarnation(),
                ))
                .into_iter()
                .flat_map(|entries| entries.range(..=candidate.rebound_max_sequence()))
                .map(|(sequence, evidence)| (*sequence, evidence.as_slice()))
                .collect::<Vec<_>>();
            let observed_entry_count = u64::try_from(rebound_entries.len()).map_err(|_| {
                ControlPlaneError::SnapshotInvariantViolation {
                    context: "retained metadata-transfer staging evidence page is invalid",
                    message: "actor closure evidence count does not fit u64".to_owned(),
                }
            })?;
            let observed_max_sequence = rebound_entries.last().map_or(0, |(sequence, _)| *sequence);
            if observed_entry_count < candidate.rebound_entry_count()
                && observed_max_sequence < candidate.rebound_max_sequence()
            {
                continue;
            }
            if observed_entry_count != candidate.rebound_entry_count() {
                return Err(ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging actor closure evidence count exceeds or cannot complete its committed prefix"
                        .to_owned(),
                });
            }
            let (entry_count, max_sequence, evidence_digest) =
                crate::pg_store::metadata_transfer_staging_rebound_evidence_digest(
                    rebound_entries.iter().copied(),
                )
                .map_err(|error| ControlPlaneError::CommandDecode {
                    message: format!("invalid staging actor closure evidence prefix: {error}"),
                })?;
            if entry_count != candidate.rebound_entry_count()
                || max_sequence != candidate.rebound_max_sequence()
                || evidence_digest != candidate.rebound_evidence_digest()
            {
                return Err(ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging actor closure evidence digest does not match its rebound prefix"
                        .to_owned(),
                });
            }
            let rebound_evidence = rebound_entries
                .iter()
                .map(|(_, evidence)| *evidence)
                .collect::<BTreeSet<_>>();

            let source_keys = unclosed_actor_tips
                .range(
                    (
                        destination_actor.node_id(),
                        candidate.first_actor().node_incarnation(),
                    )
                        ..=(
                            destination_actor.node_id(),
                            candidate.through_actor().node_incarnation(),
                        ),
                )
                .map(|(key, _)| *key)
                .collect::<Vec<_>>();
            for source_key in source_keys {
                let source_tip = unclosed_actor_tips
                    .remove(&source_key)
                    .expect("collected unclosed actor tip remains present");
                if evidence_by_actor.get(&source_key).is_some_and(|evidence| {
                    evidence.iter().any(|evidence| {
                        let decoded = crate::pg_store::decode_staging_evidence(evidence)
                            .expect("indexed staging evidence was decoded above");
                        !rebound_evidence
                            .contains(decoded.rebound_for_actor(&destination_actor).as_slice())
                    })
                }) {
                    return Err(ControlPlaneError::CommandDecode {
                        message:
                            "metadata-transfer staging actor closure omits retained source evidence"
                                .to_owned(),
                    });
                }
                closures.insert(
                    source_key,
                    MetadataTransferStagingActorClosureCertificate {
                        source_actor: source_tip.actor.clone(),
                        source_tip_generation: source_tip.generation,
                        source_tip_page_digest: source_tip.page_digest,
                        source_tip_apply_receipt_digest: source_tip.apply_receipt_digest,
                        destination_actor: destination_actor.clone(),
                        destination_genesis_page_digest,
                        rebound_entry_count: entry_count,
                        rebound_max_sequence: max_sequence,
                        rebound_evidence_digest: evidence_digest,
                    },
                );
            }
        }
        Ok(closures)
    }

    fn metadata_transfer_staging_actor_closure_validation_index(
        &self,
        finalized_evidence: &MetadataTransferStagingFinalizedEvidenceIndex<'_>,
        finalized_checkpoints: &MetadataTransferStagingFinalizedCheckpointIndex<'_>,
    ) -> Result<MetadataTransferStagingActorClosureValidationIndex, String> {
        let mut actor_tips =
            BTreeMap::<(NodeId, u64), MetadataTransferStagingActorClosureTip>::new();
        let mut actor_genesis = BTreeMap::<(NodeId, u64), (String, [u8; 32])>::new();
        let mut actor_entries = BTreeMap::<(NodeId, u64), BTreeMap<u64, Vec<u8>>>::new();

        fn retain_segment(
            snapshot: &ClusterControlSnapshot,
            segment: &MetadataTransferStagingEvidenceCheckpointSegment,
            finalized_evidence: &MetadataTransferStagingFinalizedEvidenceIndex<'_>,
            actor_tips: &mut BTreeMap<(NodeId, u64), MetadataTransferStagingActorClosureTip>,
            actor_genesis: &mut BTreeMap<(NodeId, u64), (String, [u8; 32])>,
            actor_entries: &mut BTreeMap<(NodeId, u64), BTreeMap<u64, Vec<u8>>>,
        ) -> Result<(), String> {
            let actor_key = (segment.actor.node_id(), segment.actor.node_incarnation());
            let tip_link = segment
                .page_links
                .last()
                .ok_or_else(|| "retired actor closure references an empty checkpoint".to_owned())?;
            let tip = (
                segment.actor.clone(),
                segment.last_generation,
                tip_link.page_digest,
                checksum::sha256::digest(&segment.tip_apply_receipt),
            );
            if actor_tips
                .get(&actor_key)
                .is_none_or(|existing| existing.1 < tip.1)
            {
                actor_tips.insert(actor_key, tip);
            }
            if segment.first_generation == 1 {
                let genesis = segment
                    .page_links
                    .first()
                    .expect("nonempty checkpoint has a first link");
                if actor_genesis
                    .insert(
                        actor_key,
                        (segment.actor.endpoint().to_owned(), genesis.page_digest),
                    )
                    .is_some()
                {
                    return Err(
                        "retired actor closure actor has duplicate genesis evidence".to_owned()
                    );
                }
            }
            let entries = actor_entries.entry(actor_key).or_default();
            for link in &segment.page_links {
                for entry in &link.entries {
                    let digest =
                        *segment
                            .commitments
                            .get(&entry.evidence_key)
                            .ok_or_else(|| {
                                "retired actor closure checkpoint member has no commitment"
                                    .to_owned()
                            })?;
                    let evidence = snapshot.metadata_transfer_staging_checkpoint_evidence_bytes(
                        &entry.evidence_key,
                        &segment.actor,
                        finalized_evidence,
                        digest,
                    )?;
                    if entries.insert(entry.sequence, evidence).is_some() {
                        return Err(
                            "retired actor closure actor has duplicate evidence sequence"
                                .to_owned(),
                        );
                    }
                }
            }
            Ok(())
        }

        for record in self.metadata_transfer_staging_evidence_pages.values() {
            let page =
                decode_staging_evidence_page_payload(&record.operation_payload, record.page_digest)
                    .map_err(|error| error.to_string())?;
            let actor_key = (page.actor().node_id(), page.actor().node_incarnation());
            let tip = (
                page.actor().clone(),
                page.generation(),
                page.page_digest(),
                checksum::sha256::digest(&record.apply_receipt),
            );
            if actor_tips
                .get(&actor_key)
                .is_none_or(|existing| existing.1 < tip.1)
            {
                actor_tips.insert(actor_key, tip);
            }
            if page.generation() == 1
                && actor_genesis
                    .insert(
                        actor_key,
                        (page.actor().endpoint().to_owned(), page.page_digest()),
                    )
                    .is_some()
            {
                return Err("retired actor closure actor has duplicate genesis evidence".to_owned());
            }
            let entries = actor_entries.entry(actor_key).or_default();
            for entry in page.entries() {
                if entries
                    .insert(entry.sequence(), entry.evidence().to_vec())
                    .is_some()
                {
                    return Err(
                        "retired actor closure actor has duplicate evidence sequence".to_owned(),
                    );
                }
            }
        }
        for segment in self
            .metadata_transfer_staging_evidence_checkpoint_segments
            .values()
        {
            retain_segment(
                self,
                segment,
                finalized_evidence,
                &mut actor_tips,
                &mut actor_genesis,
                &mut actor_entries,
            )?;
        }
        for anchor in self
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .values()
        {
            let sources = self.reconstruct_metadata_transfer_staging_checkpoint_anchor_sources(
                anchor,
                finalized_evidence,
                finalized_checkpoints,
            )?;
            let mut previous_generation = anchor.previous_generation;
            let mut previous_apply_receipt_digest = anchor.previous_apply_receipt_digest;
            for (first_generation, last_generation, source_segment_digest) in sources {
                let segment = self
                    .reconstruct_metadata_transfer_staging_checkpoint_source_segment(
                        &anchor.actor,
                        MetadataTransferStagingCheckpointSourceSegmentBinding {
                            first_generation,
                            last_generation,
                            previous_generation,
                            previous_apply_receipt_digest,
                            source_segment_digest,
                        },
                        finalized_evidence,
                        finalized_checkpoints,
                    )?;
                previous_generation = segment.last_generation;
                previous_apply_receipt_digest =
                    checksum::sha256::digest(&segment.tip_apply_receipt);
                retain_segment(
                    self,
                    &segment,
                    finalized_evidence,
                    &mut actor_tips,
                    &mut actor_genesis,
                    &mut actor_entries,
                )?;
            }
        }

        Ok(MetadataTransferStagingActorClosureValidationIndex {
            actor_tips,
            actor_genesis,
            actor_entries,
        })
    }

    fn validate_retired_metadata_transfer_staging_actor_closure(
        key: (NodeId, u64),
        certificate: &MetadataTransferStagingActorClosureCertificate,
        index: &MetadataTransferStagingActorClosureValidationIndex,
    ) -> Result<(), String> {
        if key
            != (
                certificate.source_actor.node_id(),
                certificate.source_actor.node_incarnation(),
            )
            || certificate.source_actor.node_id() != certificate.destination_actor.node_id()
            || certificate.source_actor.node_incarnation()
                >= certificate.destination_actor.node_incarnation()
        {
            return Err("retired actor closure has an invalid actor identity".to_owned());
        }
        let source_tip = index
            .actor_tips
            .get(&key)
            .ok_or_else(|| "retired actor closure has no retained source actor chain".to_owned())?;
        if source_tip.0 != certificate.source_actor
            || source_tip.1 != certificate.source_tip_generation
            || source_tip.2 != certificate.source_tip_page_digest
            || source_tip.3 != certificate.source_tip_apply_receipt_digest
        {
            return Err("retired actor closure does not match its source actor tip".to_owned());
        }
        let destination_key = (
            certificate.destination_actor.node_id(),
            certificate.destination_actor.node_incarnation(),
        );
        let destination_genesis = index.actor_genesis.get(&destination_key).ok_or_else(|| {
            "retired actor closure has no retained destination genesis".to_owned()
        })?;
        if destination_genesis.0 != certificate.destination_actor.endpoint()
            || destination_genesis.1 != certificate.destination_genesis_page_digest
        {
            return Err("retired actor closure does not match its destination genesis".to_owned());
        }
        let rebound_entries = index
            .actor_entries
            .get(&destination_key)
            .into_iter()
            .flat_map(|entries| entries.range(..=certificate.rebound_max_sequence))
            .map(|(sequence, evidence)| (*sequence, evidence.as_slice()))
            .collect::<Vec<_>>();
        let (entry_count, max_sequence, evidence_digest) =
            crate::pg_store::metadata_transfer_staging_rebound_evidence_digest(
                rebound_entries.iter().copied(),
            )
            .map_err(|error| error.to_string())?;
        if entry_count != certificate.rebound_entry_count
            || max_sequence != certificate.rebound_max_sequence
            || evidence_digest != certificate.rebound_evidence_digest
        {
            return Err(
                "retired actor closure does not match its rebound evidence prefix".to_owned(),
            );
        }
        Ok(())
    }

    fn checkpoint_metadata_transfer_staging_evidence_pages(
        &self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        first_generation: u64,
        last_generation: u64,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        if first_generation == 0 || first_generation > last_generation {
            return Err(ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging checkpoint generation range is invalid"
                    .to_owned(),
            });
        }
        if let Some(genesis_record) = self.metadata_transfer_staging_evidence_pages.get(&(
            actor_node_id,
            actor_node_incarnation,
            1,
        )) {
            let genesis = decode_staging_evidence_page_payload(
                &genesis_record.operation_payload,
                genesis_record.page_digest,
            )
            .map_err(|error| ControlPlaneError::SnapshotInvariantViolation {
                context: "retained metadata-transfer staging evidence genesis is invalid",
                message: error.to_string(),
            })?;
            if genesis.actor_closure_candidate().is_some() {
                let closures = self
                    .metadata_transfer_staging_actor_closures
                    .values()
                    .filter(|closure| {
                        closure.destination_actor == *genesis.actor()
                            && closure.destination_genesis_page_digest == genesis.page_digest()
                    })
                    .collect::<Vec<_>>();
                if closures.is_empty()
                    || closures.iter().any(|closure| {
                        self.metadata_transfer_staging_retired_actor_closures.get(&(
                            closure.source_actor.node_id(),
                            closure.source_actor.node_incarnation(),
                        )) != Some(*closure)
                    })
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: "metadata-transfer staging checkpoint cannot consume an actor-closure chain before certificate retirement"
                            .to_owned(),
                    });
                }
            }
        }
        let page_count = last_generation
            .checked_sub(first_generation)
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging checkpoint generation range overflows"
                    .to_owned(),
            })?;
        if page_count
            > u64::try_from(MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_PAGES)
                .expect("checkpoint page limit fits u64")
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "metadata-transfer staging checkpoint exceeds the {} page limit",
                    MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_PAGES
                ),
            });
        }
        let segment_key = (actor_node_id, actor_node_incarnation, first_generation);
        if let Some((_, existing)) = self
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .range(
                (actor_node_id, actor_node_incarnation, 0)
                    ..=(actor_node_id, actor_node_incarnation, first_generation),
            )
            .next_back()
        {
            if existing.last_generation >= first_generation {
                let finalized_checkpoints = metadata_transfer_staging_finalized_checkpoint_index(
                    &self.metadata_transfer_staging_finalized_floors,
                )
                .map_err(|message| {
                    ControlPlaneError::SnapshotInvariantViolation {
                        context: "metadata-transfer staging finalized checkpoint index",
                        message,
                    }
                })?;
                let sources = metadata_transfer_staging_checkpoint_source_segments(
                    &finalized_checkpoints,
                    actor_node_id,
                    actor_node_incarnation,
                    first_generation,
                    last_generation,
                )
                .map_err(|message| ControlPlaneError::CommandDecode { message })?;
                if existing.first_generation <= first_generation
                    && existing.last_generation >= last_generation
                    && sources.len() == 1
                    && sources[0].0 == first_generation
                    && sources[0].1 == last_generation
                {
                    return Ok(AppliedControlPlaneCommand::new(
                        self.clone(),
                        ControlPlaneCommandResponse::CheckpointMetadataTransferStagingEvidencePages,
                        false,
                    ));
                }
                return Err(ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging checkpoint conflicts with retained anchor"
                        .to_owned(),
                });
            }
        }
        if let Some(existing) = self
            .metadata_transfer_staging_evidence_checkpoint_segments
            .get(&segment_key)
        {
            if existing.last_generation == last_generation {
                return Ok(AppliedControlPlaneCommand::new(
                    self.clone(),
                    ControlPlaneCommandResponse::CheckpointMetadataTransferStagingEvidencePages,
                    false,
                ));
            }
            return Err(ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging checkpoint conflicts with retained segment"
                    .to_owned(),
            });
        }
        if self
            .metadata_transfer_staging_evidence_checkpoint_segments
            .values()
            .any(|segment| {
                segment.actor.node_id() == actor_node_id
                    && segment.actor.node_incarnation() == actor_node_incarnation
                    && first_generation <= segment.last_generation
                    && segment.first_generation <= last_generation
            })
        {
            return Err(ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging checkpoint overlaps a retained segment"
                    .to_owned(),
            });
        }
        if self
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .values()
            .any(|anchor| {
                anchor.actor.node_id() == actor_node_id
                    && anchor.actor.node_incarnation() == actor_node_incarnation
                    && first_generation <= anchor.last_generation
                    && anchor.first_generation <= last_generation
            })
        {
            return Err(ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging checkpoint overlaps a retained anchor"
                    .to_owned(),
            });
        }
        let actor_key = (actor_node_id, actor_node_incarnation);
        let latest_generation = self
            .metadata_transfer_staging_evidence_pages
            .range((actor_key.0, actor_key.1, 0)..=(actor_key.0, actor_key.1, u64::MAX))
            .next_back()
            .map(|(key, _)| key.2)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging checkpoint actor has no retained pages"
                    .to_owned(),
            })?;
        let closes_latest = self
            .metadata_transfer_staging_actor_closures
            .get(&(actor_node_id, actor_node_incarnation))
            .is_some_and(|closure| closure.source_tip_generation == latest_generation);
        if last_generation > latest_generation
            || (last_generation == latest_generation && !closes_latest)
        {
            return Err(ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging checkpoint cannot consume the actor page tip"
                    .to_owned(),
            });
        }

        let mut actor = None;
        let mut previous_generation = 0;
        let mut previous_apply_receipt_digest = [0; 32];
        let mut expected_previous_generation = None;
        let mut expected_previous_digest = None;
        let mut page_links = Vec::new();
        let mut tip_apply_receipt = Vec::new();
        let mut commitments = BTreeMap::new();
        for generation in first_generation..=last_generation {
            let record = self
                .metadata_transfer_staging_evidence_pages
                .get(&(actor_node_id, actor_node_incarnation, generation))
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: format!(
                        "metadata-transfer staging checkpoint is missing generation {generation}"
                    ),
                })?;
            let page = crate::pg_store::decode_staging_evidence_page_payload(
                &record.operation_payload,
                record.page_digest,
            )
            .map_err(|error| ControlPlaneError::SnapshotInvariantViolation {
                context: "retained metadata-transfer staging evidence page is invalid",
                message: error.to_string(),
            })?;
            if page.actor().node_id() != actor_node_id
                || page.actor().node_incarnation() != actor_node_incarnation
                || page.generation() != generation
            {
                return Err(ControlPlaneError::SnapshotInvariantViolation {
                    context: "retained metadata-transfer staging evidence page is invalid",
                    message: "page identity does not match its retained key".to_owned(),
                });
            }
            if generation == first_generation {
                actor = Some(page.actor().clone());
                previous_generation = page.previous_generation();
                previous_apply_receipt_digest = page.previous_apply_receipt_digest();
            } else if Some(page.actor()) != actor.as_ref()
                || page.previous_generation() != expected_previous_generation.unwrap()
                || page.previous_apply_receipt_digest() != expected_previous_digest.unwrap()
            {
                return Err(ControlPlaneError::SnapshotInvariantViolation {
                    context: "retained metadata-transfer staging evidence page is invalid",
                    message: "checkpoint source pages are not a contiguous exact chain".to_owned(),
                });
            }
            let receipt =
                crate::pg_store::decode_staging_evidence_apply_receipt(&record.apply_receipt)
                    .map_err(|error| ControlPlaneError::SnapshotInvariantViolation {
                        context: "retained metadata-transfer staging evidence receipt is invalid",
                        message: error.to_string(),
                    })?;
            if !receipt.is_for_page(&page) {
                return Err(ControlPlaneError::SnapshotInvariantViolation {
                    context: "retained metadata-transfer staging evidence receipt is invalid",
                    message: "receipt does not identify its page".to_owned(),
                });
            }
            let mut link_entries = Vec::with_capacity(page.entries().len());
            for entry in page.entries() {
                let evidence = crate::pg_store::decode_staging_evidence(entry.evidence()).map_err(
                    |error| ControlPlaneError::SnapshotInvariantViolation {
                        context: "retained metadata-transfer staging evidence is invalid",
                        message: error.to_string(),
                    },
                )?;
                let key = metadata_transfer_staging_evidence_key(&evidence);
                link_entries.push(MetadataTransferStagingEvidenceCheckpointPageEntry {
                    sequence: entry.sequence(),
                    evidence_key: key.clone(),
                });
                if commitments
                    .insert(key, checksum::sha256::digest(entry.evidence()))
                    .is_some()
                {
                    return Err(ControlPlaneError::SnapshotInvariantViolation {
                        context: "retained metadata-transfer staging evidence is invalid",
                        message: "checkpoint source contains duplicate evidence identity"
                            .to_owned(),
                    });
                }
                if commitments.len() > MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COMMITMENTS
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "metadata-transfer staging checkpoint exceeds the {} commitment limit",
                            MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COMMITMENTS
                        ),
                    });
                }
            }
            page_links.push(MetadataTransferStagingEvidenceCheckpointPageLink {
                page_digest: page.page_digest(),
                previous_apply_receipt_digest: page.previous_apply_receipt_digest(),
                apply_receipt_digest: checksum::sha256::digest(&record.apply_receipt),
                actor_closure_candidate: page.actor_closure_candidate().cloned(),
                entries: link_entries,
            });
            expected_previous_generation = Some(generation);
            expected_previous_digest = Some(checksum::sha256::digest(&record.apply_receipt));
            tip_apply_receipt.clone_from(&record.apply_receipt);
        }
        let segment = MetadataTransferStagingEvidenceCheckpointSegment {
            actor: actor.expect("nonempty checkpoint range has an actor"),
            first_generation,
            last_generation,
            previous_generation,
            previous_apply_receipt_digest,
            page_links,
            tip_apply_receipt,
            commitments,
        };
        if metadata_transfer_staging_evidence_checkpoint_state_record_len(&segment)
            > MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_STATE_RECORD_BYTES
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "metadata-transfer staging checkpoint exceeds the {} byte limit",
                    MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_STATE_RECORD_BYTES
                ),
            });
        }

        let segment_digest = metadata_transfer_staging_checkpoint_segment_digest(&segment);
        let mut finalized_replay_bindings = Vec::new();
        for (key, evidence_digest) in &segment.commitments {
            let Some(floor) = self
                .metadata_transfer_staging_finalized_floors
                .get(&(key.pg_id, key.staging_generation))
            else {
                if self.metadata_transfer_staging_evidence.contains_key(key) {
                    continue;
                }
                return Err(ControlPlaneError::SnapshotInvariantViolation {
                    context: "retained metadata-transfer staging evidence page",
                    message: "page member lacks detailed or exact finalized evidence".to_owned(),
                });
            };
            let expected = metadata_transfer_staging_finalized_semantic_evidence_bytes(
                floor,
                &segment.actor,
                key.kind,
                key.target_epoch,
            )
            .map_err(|message| ControlPlaneError::SnapshotInvariantViolation {
                context: "retained metadata-transfer staging finalized replay",
                message,
            })?;
            if checksum::sha256::digest(&expected) != *evidence_digest {
                return Err(ControlPlaneError::SnapshotInvariantViolation {
                    context: "retained metadata-transfer staging finalized replay",
                    message: "page member does not match finalized semantics".to_owned(),
                });
            }
            let (page_offset, page_sequence, actor_closure_candidate) = segment
                .page_links
                .iter()
                .enumerate()
                .find_map(|(offset, link)| {
                    link.entries
                        .iter()
                        .find(|entry| entry.evidence_key == *key)
                        .map(|entry| (offset, entry.sequence, link.actor_closure_candidate.clone()))
                })
                .ok_or_else(|| ControlPlaneError::SnapshotInvariantViolation {
                    context: "retained metadata-transfer staging finalized replay",
                    message: "checkpoint commitment has no page membership".to_owned(),
                })?;
            let page_generation = first_generation
                .checked_add(u64::try_from(page_offset).map_err(|_| {
                    ControlPlaneError::invariant_failure(
                        "metadata-transfer staging checkpoint page offset does not fit u64",
                    )
                })?)
                .ok_or_else(|| {
                    ControlPlaneError::invariant_failure(
                        "metadata-transfer staging checkpoint page generation overflows",
                    )
                })?;
            finalized_replay_bindings.push((
                (key.pg_id, key.staging_generation),
                key.clone(),
                MetadataTransferStagingFinalizedCheckpointBinding {
                    actor_node_id,
                    actor_node_incarnation,
                    actor_endpoint: segment.actor.endpoint().to_owned(),
                    first_generation,
                    last_generation,
                    page_generation,
                    page_sequence,
                    segment_digest,
                    actor_closure_candidate,
                },
            ));
        }

        let mut next_snapshot = self.clone();
        for generation in first_generation..=last_generation {
            next_snapshot
                .metadata_transfer_staging_evidence_pages
                .remove(&(actor_node_id, actor_node_incarnation, generation));
        }
        next_snapshot
            .metadata_transfer_staging_evidence_checkpoint_segments
            .insert(segment_key, segment);
        for (floor_key, evidence_key, binding) in finalized_replay_bindings {
            next_snapshot
                .metadata_transfer_staging_evidence
                .remove(&evidence_key);
            let floor = next_snapshot
                .metadata_transfer_staging_finalized_floors
                .get_mut(&floor_key)
                .expect("finalized replay floor validated before checkpoint mutation");
            if floor
                .checkpoint_bindings
                .insert(evidence_key, binding)
                .is_some()
            {
                return Err(ControlPlaneError::SnapshotInvariantViolation {
                    context: "metadata-transfer staging finalized replay checkpoint",
                    message: "replay binding already exists".to_owned(),
                });
            }
        }
        Ok(AppliedControlPlaneCommand::new(
            next_snapshot,
            ControlPlaneCommandResponse::CheckpointMetadataTransferStagingEvidencePages,
            true,
        ))
    }

    pub(crate) fn checkpoint_metadata_transfer_staging_evidence_pages_command(
        &self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        first_generation: u64,
        last_generation: u64,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        let command = ControlPlaneCommand::CheckpointMetadataTransferStagingEvidencePages {
            actor_node_id,
            actor_node_incarnation,
            first_generation,
            last_generation,
        };
        self.apply_control_plane_command(command.clone())?;
        Ok(command)
    }

    pub(crate) fn next_metadata_transfer_staging_maintenance_command(
        &self,
        cursor: &mut MetadataTransferStagingMaintenanceCursor,
    ) -> Result<Option<ControlPlaneCommand>, ControlPlaneError> {
        let first_phase = cursor.next_phase;
        let mut phase = first_phase;
        loop {
            let command = match phase {
                MetadataTransferStagingMaintenancePhase::ClosureRetirement => {
                    self.next_staging_closure_retirement_command(cursor)?
                }
                MetadataTransferStagingMaintenancePhase::PageCheckpoint => {
                    self.next_staging_checkpoint_command(cursor)?
                }
                MetadataTransferStagingMaintenancePhase::SegmentCollapse => {
                    self.next_staging_segment_collapse_command(cursor)?
                }
                MetadataTransferStagingMaintenancePhase::AnchorCoalescing => {
                    self.next_staging_anchor_coalescing_command(cursor)?
                }
            };
            if command.is_some() {
                cursor.next_phase = phase.next();
                return Ok(command);
            }
            phase = phase.next();
            if phase == first_phase {
                cursor.next_phase = first_phase.next();
                return Ok(None);
            }
        }
    }

    fn next_staging_closure_retirement_command(
        &self,
        cursor: &mut MetadataTransferStagingMaintenanceCursor,
    ) -> Result<Option<ControlPlaneCommand>, ControlPlaneError> {
        let Some(high_water) = metadata_transfer_staging_maintenance_sweep_high_water(
            &mut cursor.after_closure,
            &mut cursor.closure_high_water,
            self.metadata_transfer_staging_actor_closures
                .keys()
                .next_back()
                .copied(),
        ) else {
            return Ok(None);
        };
        let start = cursor
            .after_closure
            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let candidates = self
            .metadata_transfer_staging_actor_closures
            .range((start, std::ops::Bound::Included(high_water)))
            .take(METADATA_TRANSFER_STAGING_MAINTENANCE_SCAN_PAGE_SIZE)
            .map(|(key, closure)| (*key, closure.clone()))
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            cursor.after_closure = None;
            cursor.closure_high_water = None;
            return Ok(None);
        }
        for ((node_id, incarnation), closure) in candidates {
            let key = (node_id, incarnation);
            match self
                .metadata_transfer_staging_retired_actor_closures
                .get(&key)
            {
                Some(retired) if retired == &closure => {
                    cursor.after_closure = Some(key);
                }
                Some(_) => {
                    return Err(ControlPlaneError::SnapshotInvariantViolation {
                        context: "metadata-transfer staging maintenance closure retirement",
                        message: "active and retired actor-closure certificates conflict"
                            .to_owned(),
                    });
                }
                None => {
                    let command = self
                        .retire_metadata_transfer_staging_actor_closure_command(
                            node_id,
                            incarnation,
                        )
                        .map(Some)?;
                    cursor.after_closure = Some(key);
                    return Ok(command);
                }
            }
        }
        Ok(None)
    }

    fn next_staging_checkpoint_command(
        &self,
        cursor: &mut MetadataTransferStagingMaintenanceCursor,
    ) -> Result<Option<ControlPlaneCommand>, ControlPlaneError> {
        let Some(high_water) = metadata_transfer_staging_maintenance_sweep_high_water(
            &mut cursor.after_page,
            &mut cursor.page_high_water,
            self.metadata_transfer_staging_evidence_pages
                .keys()
                .next_back()
                .copied(),
        ) else {
            return Ok(None);
        };
        let start = cursor
            .after_page
            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let mut candidates = cursor
            .after_page
            .filter(|key| {
                self.metadata_transfer_staging_evidence_pages
                    .contains_key(key)
            })
            .into_iter()
            .collect::<Vec<_>>();
        candidates.extend(
            self.metadata_transfer_staging_evidence_pages
                .range((start, std::ops::Bound::Included(high_water)))
                .take(METADATA_TRANSFER_STAGING_MAINTENANCE_SCAN_PAGE_SIZE - candidates.len())
                .map(|(key, _)| *key),
        );
        if candidates.is_empty() {
            cursor.after_page = None;
            cursor.page_high_water = None;
            return Ok(None);
        }
        for (node_id, incarnation, generation) in candidates {
            let key = (node_id, incarnation, generation);
            let has_later_page = generation.checked_add(1).is_some_and(|next_generation| {
                self.metadata_transfer_staging_evidence_pages
                    .range(
                        (node_id, incarnation, next_generation)..=(node_id, incarnation, u64::MAX),
                    )
                    .next()
                    .is_some()
            });
            let closes_tip = self
                .metadata_transfer_staging_actor_closures
                .get(&(node_id, incarnation))
                .is_some_and(|closure| closure.source_tip_generation == generation);
            if !has_later_page && !closes_tip {
                cursor.after_page = Some(key);
                continue;
            }
            match self.checkpoint_metadata_transfer_staging_evidence_pages_command(
                node_id,
                incarnation,
                generation,
                generation,
            ) {
                Ok(command) => {
                    cursor.after_page = Some(key);
                    return Ok(Some(command));
                }
                Err(ControlPlaneError::CommandDecode { .. }) => {
                    cursor.after_page = Some(key);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    fn next_staging_segment_collapse_command(
        &self,
        cursor: &mut MetadataTransferStagingMaintenanceCursor,
    ) -> Result<Option<ControlPlaneCommand>, ControlPlaneError> {
        let Some(high_water) = metadata_transfer_staging_maintenance_sweep_high_water(
            &mut cursor.after_segment,
            &mut cursor.segment_high_water,
            self.metadata_transfer_staging_evidence_checkpoint_segments
                .keys()
                .next_back()
                .copied(),
        ) else {
            return Ok(None);
        };
        let start = cursor
            .after_segment
            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let mut candidates = cursor
            .after_segment
            .and_then(|key| {
                self.metadata_transfer_staging_evidence_checkpoint_segments
                    .get(&key)
                    .map(|segment| (key, segment.last_generation))
            })
            .into_iter()
            .collect::<Vec<_>>();
        candidates.extend(
            self.metadata_transfer_staging_evidence_checkpoint_segments
                .range((start, std::ops::Bound::Included(high_water)))
                .take(METADATA_TRANSFER_STAGING_MAINTENANCE_SCAN_PAGE_SIZE - candidates.len())
                .map(|(key, segment)| (*key, segment.last_generation)),
        );
        if candidates.is_empty() {
            cursor.after_segment = None;
            cursor.segment_high_water = None;
            return Ok(None);
        }
        for ((node_id, incarnation, first_generation), last_generation) in candidates {
            let key = (node_id, incarnation, first_generation);
            match self.collapse_metadata_transfer_staging_evidence_checkpoint_segment_command(
                node_id,
                incarnation,
                first_generation,
                last_generation,
            ) {
                Ok(command) => {
                    cursor.after_segment = Some(key);
                    return Ok(Some(command));
                }
                Err(ControlPlaneError::CommandDecode { .. }) => {
                    cursor.after_segment = Some(key);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    fn next_staging_anchor_coalescing_command(
        &self,
        cursor: &mut MetadataTransferStagingMaintenanceCursor,
    ) -> Result<Option<ControlPlaneCommand>, ControlPlaneError> {
        let Some(high_water) = metadata_transfer_staging_maintenance_sweep_high_water(
            &mut cursor.after_anchor,
            &mut cursor.anchor_high_water,
            self.metadata_transfer_staging_evidence_checkpoint_anchors
                .keys()
                .next_back()
                .copied(),
        ) else {
            return Ok(None);
        };
        let start = cursor
            .after_anchor
            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let mut candidates = cursor
            .after_anchor
            .and_then(|key| {
                self.metadata_transfer_staging_evidence_checkpoint_anchors
                    .get(&key)
                    .map(|anchor| (key, anchor.last_generation))
            })
            .into_iter()
            .collect::<Vec<_>>();
        candidates.extend(
            self.metadata_transfer_staging_evidence_checkpoint_anchors
                .range((start, std::ops::Bound::Included(high_water)))
                .take(METADATA_TRANSFER_STAGING_MAINTENANCE_SCAN_PAGE_SIZE + 1 - candidates.len())
                .map(|(key, anchor)| (*key, anchor.last_generation)),
        );
        if candidates.is_empty() {
            cursor.after_anchor = None;
            cursor.anchor_high_water = None;
            return Ok(None);
        }
        for pair in candidates.windows(2) {
            let ((left_node, left_incarnation, first_generation), left_last) = pair[0];
            let ((right_node, right_incarnation, right_first), right_last) = pair[1];
            let left_key = (left_node, left_incarnation, first_generation);
            let right_key = (right_node, right_incarnation, right_first);
            if left_node != right_node
                || left_incarnation != right_incarnation
                || left_last.checked_add(1) != Some(right_first)
            {
                cursor.after_anchor = Some(left_key);
                continue;
            }
            match self.coalesce_metadata_transfer_staging_evidence_checkpoint_anchors_command(
                left_node,
                left_incarnation,
                first_generation,
                right_last,
            ) {
                Ok(command) => {
                    // Coalescing replaces the left anchor in place. Revisit it so it can be
                    // combined with its new successor on a later phase rotation.
                    cursor.after_anchor = None;
                    if right_key == high_water {
                        cursor.anchor_high_water = None;
                    }
                    return Ok(Some(command));
                }
                Err(ControlPlaneError::CommandDecode { .. }) => {
                    cursor.after_anchor = Some(left_key);
                }
                Err(error) => return Err(error),
            }
        }
        if candidates.len() <= METADATA_TRANSFER_STAGING_MAINTENANCE_SCAN_PAGE_SIZE {
            cursor.after_anchor = candidates.last().map(|(key, _)| *key);
        }
        Ok(None)
    }

    fn retire_metadata_transfer_staging_actor_closure(
        &self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        certificate_digest: [u8; 32],
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        let key = (actor_node_id, actor_node_incarnation);
        let certificate = self
            .metadata_transfer_staging_actor_closures
            .get(&key)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging actor closure does not exist".to_owned(),
            })?;
        if certificate.source_actor.node_id() != actor_node_id
            || certificate.source_actor.node_incarnation() != actor_node_incarnation
            || metadata_transfer_staging_actor_closure_certificate_digest(certificate)
                != certificate_digest
        {
            return Err(ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging actor-closure retirement does not match the exact certificate"
                    .to_owned(),
            });
        }
        if let Some(retired) = self
            .metadata_transfer_staging_retired_actor_closures
            .get(&key)
        {
            if retired == certificate {
                return Ok(AppliedControlPlaneCommand::new(
                    self.clone(),
                    ControlPlaneCommandResponse::RetireMetadataTransferStagingActorClosure,
                    false,
                ));
            }
            return Err(ControlPlaneError::SnapshotInvariantViolation {
                context: "metadata-transfer staging retired actor closure",
                message: "retired certificate conflicts with the active certificate".to_owned(),
            });
        }
        let mut next_snapshot = self.clone();
        next_snapshot
            .metadata_transfer_staging_retired_actor_closures
            .insert(key, certificate.clone());
        Ok(AppliedControlPlaneCommand::new(
            next_snapshot,
            ControlPlaneCommandResponse::RetireMetadataTransferStagingActorClosure,
            true,
        ))
    }

    pub(crate) fn retire_metadata_transfer_staging_actor_closure_command(
        &self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        let certificate = self
            .metadata_transfer_staging_actor_closures
            .get(&(actor_node_id, actor_node_incarnation))
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging actor closure does not exist".to_owned(),
            })?;
        let command = ControlPlaneCommand::RetireMetadataTransferStagingActorClosure {
            actor_node_id,
            actor_node_incarnation,
            certificate_digest: metadata_transfer_staging_actor_closure_certificate_digest(
                certificate,
            ),
        };
        self.apply_control_plane_command(command.clone())?;
        Ok(command)
    }

    fn collapse_metadata_transfer_staging_evidence_checkpoint_segment(
        &self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        first_generation: u64,
        last_generation: u64,
        source_segment_digest: [u8; 32],
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        if first_generation == 0 || first_generation > last_generation {
            return Err(ControlPlaneError::CommandDecode {
                message:
                    "metadata-transfer staging checkpoint collapse generation range is invalid"
                        .to_owned(),
            });
        }
        let segment_key = (actor_node_id, actor_node_incarnation, first_generation);
        if let Some((_, anchor)) = self
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .range(
                (actor_node_id, actor_node_incarnation, 0)
                    ..=(actor_node_id, actor_node_incarnation, first_generation),
            )
            .next_back()
        {
            if anchor.last_generation >= first_generation {
                let finalized_checkpoints = metadata_transfer_staging_finalized_checkpoint_index(
                    &self.metadata_transfer_staging_finalized_floors,
                )
                .map_err(|message| {
                    ControlPlaneError::SnapshotInvariantViolation {
                        context: "metadata-transfer staging finalized checkpoint index",
                        message,
                    }
                })?;
                let sources = metadata_transfer_staging_checkpoint_source_segments(
                    &finalized_checkpoints,
                    actor_node_id,
                    actor_node_incarnation,
                    first_generation,
                    last_generation,
                )
                .map_err(|message| ControlPlaneError::CommandDecode { message })?;
                if anchor.first_generation <= first_generation
                    && anchor.last_generation >= last_generation
                    && sources.as_slice()
                        == [(first_generation, last_generation, source_segment_digest)]
                {
                    return Ok(AppliedControlPlaneCommand::new(
                        self.clone(),
                        ControlPlaneCommandResponse::CollapseMetadataTransferStagingEvidenceCheckpointSegment,
                        false,
                    ));
                }
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "metadata-transfer staging checkpoint collapse conflicts with retained anchor"
                            .to_owned(),
                });
            }
        }
        let segment = self
            .metadata_transfer_staging_evidence_checkpoint_segments
            .get(&segment_key)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging checkpoint collapse source is not retained"
                    .to_owned(),
            })?;
        if segment.last_generation != last_generation
            || metadata_transfer_staging_checkpoint_segment_digest(segment) != source_segment_digest
        {
            return Err(ControlPlaneError::CommandDecode {
                message: "metadata-transfer staging checkpoint collapse source does not match"
                    .to_owned(),
            });
        }
        let finalized_evidence = metadata_transfer_staging_finalized_evidence_index(
            &self.metadata_transfer_staging_finalized_floors,
        )
        .map_err(|message| ControlPlaneError::SnapshotInvariantViolation {
            context: "metadata-transfer staging finalized evidence index",
            message,
        })?;
        for (key, evidence_digest) in &segment.commitments {
            if self.metadata_transfer_staging_evidence.contains_key(key) {
                return Err(ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging checkpoint collapse requires pruned detailed evidence"
                        .to_owned(),
                });
            }
            let floor = self
                .metadata_transfer_staging_finalized_floors
                .get(&(key.pg_id, key.staging_generation))
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging checkpoint collapse requires every commitment to be finalized"
                        .to_owned(),
                })?;
            let binding = floor.checkpoint_bindings.get(key).ok_or_else(|| {
                ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging checkpoint collapse finalization binding does not match its source"
                        .to_owned(),
                }
            })?;
            if binding.actor_node_id != actor_node_id
                || binding.actor_node_incarnation != actor_node_incarnation
                || binding.first_generation != first_generation
                || binding.last_generation != last_generation
                || binding.segment_digest != source_segment_digest
            {
                return Err(ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging checkpoint collapse finalization binding does not match its source"
                        .to_owned(),
                });
            }
            let finalized = finalized_evidence.get(key);
            if !finalized.is_some_and(|finalized| {
                std::ptr::eq(finalized.floor, floor)
                    && finalized.evidence_digest == *evidence_digest
            }) {
                return Err(ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging checkpoint collapse commitment is not certified by its finalized floor"
                        .to_owned(),
                });
            }
        }
        let source_segments = [(first_generation, last_generation, source_segment_digest)];
        let anchor = MetadataTransferStagingEvidenceCheckpointAnchor {
            actor: segment.actor.clone(),
            first_generation,
            last_generation,
            previous_generation: segment.previous_generation,
            previous_apply_receipt_digest: segment.previous_apply_receipt_digest,
            tip_apply_receipt: segment.tip_apply_receipt.clone(),
            source_segment_digest,
            source_segment_count: 1,
            source_segments_digest: metadata_transfer_staging_checkpoint_source_segments_digest(
                &source_segments,
            ),
        };
        let mut next_snapshot = self.clone();
        next_snapshot
            .metadata_transfer_staging_evidence_checkpoint_segments
            .remove(&segment_key);
        next_snapshot
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .insert(segment_key, anchor);
        Ok(AppliedControlPlaneCommand::new(
            next_snapshot,
            ControlPlaneCommandResponse::CollapseMetadataTransferStagingEvidenceCheckpointSegment,
            true,
        ))
    }

    pub(crate) fn collapse_metadata_transfer_staging_evidence_checkpoint_segment_command(
        &self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        first_generation: u64,
        last_generation: u64,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        let key = (actor_node_id, actor_node_incarnation, first_generation);
        let source_segment_digest = if let Some(segment) = self
            .metadata_transfer_staging_evidence_checkpoint_segments
            .get(&key)
        {
            metadata_transfer_staging_checkpoint_segment_digest(segment)
        } else {
            let finalized_checkpoints = metadata_transfer_staging_finalized_checkpoint_index(
                &self.metadata_transfer_staging_finalized_floors,
            )
            .map_err(|message| ControlPlaneError::SnapshotInvariantViolation {
                context: "metadata-transfer staging finalized checkpoint index",
                message,
            })?;
            let sources = metadata_transfer_staging_checkpoint_source_segments(
                &finalized_checkpoints,
                actor_node_id,
                actor_node_incarnation,
                first_generation,
                last_generation,
            )
            .map_err(|message| ControlPlaneError::CommandDecode { message })?;
            if sources.len() != 1 {
                return Err(ControlPlaneError::CommandDecode {
                    message: "metadata-transfer staging checkpoint collapse source is not retained"
                        .to_owned(),
                });
            }
            sources[0].2
        };
        let command =
            ControlPlaneCommand::CollapseMetadataTransferStagingEvidenceCheckpointSegment {
                actor_node_id,
                actor_node_incarnation,
                first_generation,
                last_generation,
                source_segment_digest,
            };
        self.apply_control_plane_command(command.clone())?;
        Ok(command)
    }

    fn coalesce_metadata_transfer_staging_evidence_checkpoint_anchors(
        &self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        first_generation: u64,
        last_generation: u64,
        source_segment_count: u64,
        source_segments_digest: [u8; 32],
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        if first_generation == 0 || first_generation > last_generation || source_segment_count < 2 {
            return Err(ControlPlaneError::CommandDecode {
                message:
                    "metadata-transfer staging checkpoint anchor coalescing bounds are invalid"
                        .to_owned(),
            });
        }

        let finalized_checkpoints = metadata_transfer_staging_finalized_checkpoint_index(
            &self.metadata_transfer_staging_finalized_floors,
        )
        .map_err(|message| ControlPlaneError::SnapshotInvariantViolation {
            context: "metadata-transfer staging finalized checkpoint index",
            message,
        })?;
        let source_segments = metadata_transfer_staging_checkpoint_source_segments(
            &finalized_checkpoints,
            actor_node_id,
            actor_node_incarnation,
            first_generation,
            last_generation,
        )
        .map_err(|message| ControlPlaneError::CommandDecode { message })?;
        if u64::try_from(source_segments.len()).expect("source segment count fits u64")
            != source_segment_count
            || metadata_transfer_staging_checkpoint_source_segments_digest(&source_segments)
                != source_segments_digest
        {
            return Err(ControlPlaneError::CommandDecode {
                message:
                    "metadata-transfer staging checkpoint anchor coalescing source segments do not match"
                        .to_owned(),
            });
        }

        if let Some((_, covering)) = self
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .range(
                (actor_node_id, actor_node_incarnation, 0)
                    ..=(actor_node_id, actor_node_incarnation, first_generation),
            )
            .next_back()
        {
            if covering.first_generation <= first_generation
                && covering.last_generation >= last_generation
            {
                return Ok(AppliedControlPlaneCommand::new(
                    self.clone(),
                    ControlPlaneCommandResponse::CoalesceMetadataTransferStagingEvidenceCheckpointAnchors,
                    false,
                ));
            }
        }

        let mut source_keys = Vec::new();
        let mut source_anchors = Vec::new();
        let mut next_generation = first_generation;
        let mut preceding_generation = None;
        let mut preceding_digest = None;
        while next_generation <= last_generation {
            let key = (actor_node_id, actor_node_incarnation, next_generation);
            let anchor = self
                .metadata_transfer_staging_evidence_checkpoint_anchors
                .get(&key)
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message:
                        "metadata-transfer staging checkpoint anchor coalescing source is not retained"
                            .to_owned(),
                })?;
            if anchor.actor.node_id() != actor_node_id
                || anchor.actor.node_incarnation() != actor_node_incarnation
                || anchor.last_generation > last_generation
                || preceding_generation.is_some_and(|generation| {
                    anchor.previous_generation != generation
                        || anchor.previous_apply_receipt_digest
                            != preceding_digest.expect("preceding digest accompanies generation")
                })
            {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "metadata-transfer staging checkpoint anchors are not one contiguous chain"
                            .to_owned(),
                });
            }
            source_keys.push(key);
            source_anchors.push(anchor);
            preceding_generation = Some(anchor.last_generation);
            preceding_digest = Some(checksum::sha256::digest(&anchor.tip_apply_receipt));
            if anchor.last_generation == last_generation {
                break;
            }
            next_generation = anchor.last_generation.checked_add(1).ok_or_else(|| {
                ControlPlaneError::CommandDecode {
                    message:
                        "metadata-transfer staging checkpoint anchor coalescing range overflows"
                            .to_owned(),
                }
            })?;
        }
        validate_metadata_transfer_staging_checkpoint_coalescing_source_count(
            source_anchors.len(),
        )?;
        if source_anchors.last().map(|anchor| anchor.last_generation) != Some(last_generation) {
            return Err(ControlPlaneError::CommandDecode {
                message:
                    "metadata-transfer staging checkpoint anchor coalescing source does not match"
                        .to_owned(),
            });
        }

        let first = source_anchors
            .first()
            .expect("coalescing requires at least two anchors");
        let last = source_anchors
            .last()
            .expect("coalescing requires at least two anchors");
        let anchor = MetadataTransferStagingEvidenceCheckpointAnchor {
            actor: first.actor.clone(),
            first_generation,
            last_generation,
            previous_generation: first.previous_generation,
            previous_apply_receipt_digest: first.previous_apply_receipt_digest,
            tip_apply_receipt: last.tip_apply_receipt.clone(),
            source_segment_digest: [0; 32],
            source_segment_count,
            source_segments_digest,
        };

        let mut next_snapshot = self.clone();
        for key in source_keys {
            next_snapshot
                .metadata_transfer_staging_evidence_checkpoint_anchors
                .remove(&key);
        }
        next_snapshot
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .insert(
                (actor_node_id, actor_node_incarnation, first_generation),
                anchor,
            );
        Ok(AppliedControlPlaneCommand::new(
            next_snapshot,
            ControlPlaneCommandResponse::CoalesceMetadataTransferStagingEvidenceCheckpointAnchors,
            true,
        ))
    }

    pub(crate) fn coalesce_metadata_transfer_staging_evidence_checkpoint_anchors_command(
        &self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        first_generation: u64,
        last_generation: u64,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        let finalized_checkpoints = metadata_transfer_staging_finalized_checkpoint_index(
            &self.metadata_transfer_staging_finalized_floors,
        )
        .map_err(|message| ControlPlaneError::SnapshotInvariantViolation {
            context: "metadata-transfer staging finalized checkpoint index",
            message,
        })?;
        let source_segments = metadata_transfer_staging_checkpoint_source_segments(
            &finalized_checkpoints,
            actor_node_id,
            actor_node_incarnation,
            first_generation,
            last_generation,
        )
        .map_err(|message| ControlPlaneError::CommandDecode { message })?;
        let command =
            ControlPlaneCommand::CoalesceMetadataTransferStagingEvidenceCheckpointAnchors {
                actor_node_id,
                actor_node_incarnation,
                first_generation,
                last_generation,
                source_segment_count: u64::try_from(source_segments.len())
                    .expect("source segment count fits u64"),
                source_segments_digest: metadata_transfer_staging_checkpoint_source_segments_digest(
                    &source_segments,
                ),
            };
        self.apply_control_plane_command(command.clone())?;
        Ok(command)
    }

    fn finalize_metadata_transfer_staging_generation(
        &self,
        cleanup: FinalizeMetadataTransferStagingGenerationRequest,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        let pg_id = cleanup.unavailable_transition.pg_id();
        if cleanup.staging_generation == 0
            || cleanup.staging_generation != cleanup.unavailable_transition.transition_epoch().get()
            || cleanup.tombstones.is_empty()
            || cleanup
                .tombstones
                .windows(2)
                .any(|pair| pair[0].node_id >= pair[1].node_id)
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} metadata-transfer staging cleanup has invalid generation or destination ordering",
                    pg_id.get()
                ),
            });
        }
        let transition = self
            .retained_unavailable_pg_placement_transitions
            .get(&(pg_id, cleanup.unavailable_transition.transition_epoch()))
            .filter(|transition| cleanup.unavailable_transition.matches_transition(transition))
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} metadata-transfer staging cleanup requires its exact completed transition",
                    pg_id.get()
                ),
            })?;
        match cleanup.disposition {
            MetadataTransferStagingCleanupDisposition::Completed => {
                if transition.destination_epoch.is_none()
                    || transition.completion.is_none()
                    || transition.completion_batch_receipt.is_none()
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} metadata-transfer staging cleanup requires its exact completed transition",
                            pg_id.get()
                        ),
                    });
                }
            }
            MetadataTransferStagingCleanupDisposition::Superseded {
                successor_transition_epoch,
            } => {
                let successor_matches = self
                    .retained_unavailable_pg_placement_transitions
                    .get(&(pg_id, successor_transition_epoch))
                    .or_else(|| {
                        self.unavailable_pg_placement_transitions
                            .get(&pg_id)
                            .filter(|candidate| {
                                candidate.transition_epoch == successor_transition_epoch
                            })
                    })
                    .is_some_and(|successor| {
                        successor.predecessor_transition_epoch == Some(transition.transition_epoch)
                    });
                if transition.completion.is_some() || !successor_matches {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} metadata-transfer staging cancellation requires its exact successor transition",
                            pg_id.get()
                        ),
                    });
                }
            }
        }
        let authorization = transition.staging_authorization.as_ref().ok_or_else(|| {
            ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} metadata-transfer staging cleanup has no retained authorization",
                    pg_id.get()
                ),
            }
        })?;
        if authorization.staging_generation != cleanup.staging_generation
            || transition.destination_acting_set.len() != cleanup.tombstones.len()
            || transition
                .destination_acting_set
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                != cleanup
                    .tombstones
                    .iter()
                    .map(|tombstone| tombstone.node_id)
                    .collect()
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} metadata-transfer staging cleanup does not match its authorization obligations",
                    pg_id.get()
                ),
            });
        }
        let transition_binding = cleanup.unavailable_transition;
        let staging_generation = cleanup.staging_generation;
        let tombstones = cleanup.tombstones;
        let tombstone_set_digest = metadata_transfer_staging_cleanup_digest(
            &transition_binding,
            staging_generation,
            cleanup.disposition,
            authorization.artifact_digest,
            authorization.artifact_length,
            authorization.artifact_format_version,
            &tombstones,
        );
        let certificate_key = (pg_id, staging_generation);
        if let Some(existing) = self
            .metadata_transfer_staging_finalized_floors
            .get(&certificate_key)
        {
            if existing.transition == transition_binding
                && existing.staging_generation == staging_generation
                && existing.disposition == cleanup.disposition
                && existing.artifact_digest == authorization.artifact_digest
                && existing.artifact_length == authorization.artifact_length
                && existing.artifact_format_version == authorization.artifact_format_version
                && existing.tombstones == tombstones
                && existing.tombstone_set_digest == tombstone_set_digest
            {
                return Ok(AppliedControlPlaneCommand::new(
                    self.clone(),
                    ControlPlaneCommandResponse::FinalizeMetadataTransferStagingGeneration,
                    false,
                ));
            }
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} metadata-transfer staging cleanup conflicts with finalized generation {}",
                    pg_id.get(),
                    existing.staging_generation
                ),
            });
        }
        let previous_floor = metadata_transfer_staging_finalized_generation(
            &self.metadata_transfer_staging_finalized_floors,
            pg_id,
        )
        .unwrap_or(0);
        if previous_floor >= staging_generation {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} metadata-transfer staging cleanup is older than finalized generation {}",
                    pg_id.get(),
                    previous_floor
                ),
            });
        }
        if self
            .retained_unavailable_pg_placement_transitions
            .values()
            .chain(self.unavailable_pg_placement_transitions.values())
            .filter(|candidate| candidate.pg_id == pg_id)
            .filter_map(|candidate| candidate.staging_authorization.as_ref())
            .any(|authorization| {
                authorization.staging_generation > previous_floor
                    && authorization.staging_generation < staging_generation
            })
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} metadata-transfer staging cleanup cannot skip an authorized generation",
                    pg_id.get()
                ),
            });
        }

        for tombstone in &tombstones {
            let key = MetadataTransferStagingEvidenceKey {
                pg_id,
                staging_generation,
                actor_node_id: tombstone.node_id,
                actor_node_incarnation: tombstone.node_incarnation,
                kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
                target_epoch: None,
            };
            let bytes = self
                .metadata_transfer_staging_evidence
                .get(&key)
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: format!(
                        "PG {} metadata-transfer staging cleanup lacks a tombstone for node {}",
                        pg_id.get(),
                        tombstone.node_id.as_u32()
                    ),
                })?;
            let evidence = crate::pg_store::decode_staging_evidence(bytes).map_err(|error| {
                ControlPlaneError::SnapshotInvariantViolation {
                    context: "retained metadata-transfer staging evidence is invalid",
                    message: error.to_string(),
                }
            })?;
            if evidence.actor().endpoint() != tombstone.endpoint
                || checksum::sha256::digest(bytes) != tombstone.evidence_digest
            {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "PG {} metadata-transfer staging cleanup tombstone for node {} is not exact",
                        pg_id.get(),
                        tombstone.node_id.as_u32()
                    ),
                });
            }
        }

        let covered_keys = self
            .metadata_transfer_staging_evidence
            .keys()
            .filter(|key| {
                key.pg_id == pg_id
                    && key.staging_generation > previous_floor
                    && key.staging_generation <= staging_generation
            })
            .cloned()
            .collect::<Vec<_>>();
        if covered_keys
            .iter()
            .any(|key| key.staging_generation != staging_generation)
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} metadata-transfer staging cleanup cannot skip an unfinished generation",
                    pg_id.get()
                ),
            });
        }
        let mut checkpoint_bindings = BTreeMap::new();
        for key in &covered_keys {
            let Some((segment_key, segment)) = self
                .metadata_transfer_staging_evidence_checkpoint_segments
                .iter()
                .find(|(_, segment)| segment.commitments.contains_key(key))
            else {
                continue;
            };
            let evidence_digest = checksum::sha256::digest(
                self.metadata_transfer_staging_evidence
                    .get(key)
                    .expect("covered staging evidence key came from the detailed map"),
            );
            if segment.commitments.get(key) != Some(&evidence_digest) {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "PG {} metadata-transfer staging cleanup evidence does not match its checkpoint commitment",
                        pg_id.get()
                    ),
                });
            }
            let (page_offset, page_sequence, actor_closure_candidate) = segment
                .page_links
                .iter()
                .enumerate()
                .find_map(|(offset, link)| {
                    link.entries
                        .iter()
                        .find(|entry| entry.evidence_key == *key)
                        .map(|entry| (offset, entry.sequence, link.actor_closure_candidate.clone()))
                })
                .ok_or_else(|| ControlPlaneError::SnapshotInvariantViolation {
                    context: "metadata-transfer staging cleanup checkpoint membership",
                    message: format!(
                        "PG {} checkpoint commitment has no page membership",
                        pg_id.get()
                    ),
                })?;
            let page_generation = segment
                .first_generation
                .checked_add(u64::try_from(page_offset).map_err(|_| {
                    ControlPlaneError::invariant_failure(
                        "metadata-transfer staging checkpoint page offset does not fit u64",
                    )
                })?)
                .ok_or_else(|| {
                    ControlPlaneError::invariant_failure(
                        "metadata-transfer staging checkpoint page generation overflows",
                    )
                })?;
            checkpoint_bindings.insert(
                key.clone(),
                MetadataTransferStagingFinalizedCheckpointBinding {
                    actor_node_id: segment_key.0,
                    actor_node_incarnation: segment_key.1,
                    actor_endpoint: segment.actor.endpoint().to_owned(),
                    first_generation: segment_key.2,
                    last_generation: segment.last_generation,
                    page_generation,
                    page_sequence,
                    segment_digest: metadata_transfer_staging_checkpoint_segment_digest(segment),
                    actor_closure_candidate,
                },
            );
            if self
                .metadata_transfer_staging_actor_closures
                .values()
                .any(|closure| {
                    let depends_on_closure = (closure.source_actor.node_id() == key.actor_node_id
                        && closure.source_actor.node_incarnation() == key.actor_node_incarnation)
                        || (closure.destination_actor.node_id() == key.actor_node_id
                            && closure.destination_actor.node_incarnation()
                                == key.actor_node_incarnation);
                    depends_on_closure
                        && self.metadata_transfer_staging_retired_actor_closures.get(&(
                            closure.source_actor.node_id(),
                            closure.source_actor.node_incarnation(),
                        )) != Some(closure)
                })
            {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "PG {} metadata-transfer staging cleanup awaits actor-closure retirement",
                        pg_id.get()
                    ),
                });
            }
        }

        let mut publications = covered_keys
            .iter()
            .filter(|key| {
                key.kind == crate::pg_store::MetadataTransferStagingEvidenceKind::Publication
            })
            .map(|key| {
                let bytes = self
                    .metadata_transfer_staging_evidence
                    .get(key)
                    .expect("covered staging evidence key came from the detailed map");
                let evidence =
                    crate::pg_store::decode_staging_evidence(bytes).map_err(|error| {
                        ControlPlaneError::SnapshotInvariantViolation {
                            context: "retained metadata-transfer staging evidence is invalid",
                            message: error.to_string(),
                        }
                    })?;
                let target_epoch =
                    evidence
                        .target_epoch()
                        .ok_or_else(|| ControlPlaneError::CommandDecode {
                            message: format!(
                                "PG {} metadata-transfer staging publication lacks a target epoch",
                                pg_id.get()
                            ),
                        })?;
                let transfer =
                    evidence
                        .transfer()
                        .ok_or_else(|| ControlPlaneError::CommandDecode {
                            message: format!(
                            "PG {} metadata-transfer staging publication lacks a transfer proof",
                            pg_id.get()
                        ),
                        })?;
                Ok(MetadataTransferStagingFinalizedPublicationBinding {
                    node_id: evidence.actor().node_id(),
                    node_incarnation: evidence.actor().node_incarnation(),
                    endpoint: evidence.actor().endpoint().to_owned(),
                    target_epoch,
                    transfer,
                    evidence_digest: checksum::sha256::digest(bytes),
                })
            })
            .collect::<Result<Vec<_>, ControlPlaneError>>()?;
        publications.sort_by_key(|publication| (publication.target_epoch, publication.node_id));
        let checkpointed_keys = checkpoint_bindings.keys().cloned().collect::<Vec<_>>();
        let certificate = MetadataTransferStagingFinalizedFloor {
            transition: transition_binding,
            staging_generation,
            disposition: cleanup.disposition,
            artifact_digest: authorization.artifact_digest,
            artifact_length: authorization.artifact_length,
            artifact_format_version: authorization.artifact_format_version,
            publications,
            tombstones,
            tombstone_set_digest,
            checkpoint_bindings,
        };

        let mut next_snapshot = self.clone();
        for key in checkpointed_keys {
            next_snapshot
                .metadata_transfer_staging_evidence
                .remove(&key);
        }
        next_snapshot
            .metadata_transfer_staging_finalized_floors
            .insert(certificate_key, certificate);
        Ok(AppliedControlPlaneCommand::new(
            next_snapshot,
            ControlPlaneCommandResponse::FinalizeMetadataTransferStagingGeneration,
            true,
        ))
    }

    pub(crate) fn finalize_metadata_transfer_staging_generation_command(
        &self,
        cleanup: FinalizeMetadataTransferStagingGenerationRequest,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        let command = ControlPlaneCommand::FinalizeMetadataTransferStagingGeneration { cleanup };
        self.apply_control_plane_command(command.clone())?;
        Ok(command)
    }

    pub(crate) fn complete_unavailable_pg_placement_transition_command(
        &self,
        work: &UnavailablePgReconciliationWork,
        ready_at_ms: u64,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        let pg_id = work.pg_id();
        let transition = self
            .unavailable_pg_placement_transitions
            .get(&pg_id)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: format!("PG {} has no active unavailable transition", pg_id.get()),
            })?;
        if !work.mutation_binding().matches_transition(transition) {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} reconciliation work does not match its active transition",
                    pg_id.get()
                ),
            });
        }
        let destination_epoch =
            transition
                .destination_epoch
                .ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: format!("PG {} destination transfer is not installed", pg_id.get()),
                })?;
        let destinations = transition
            .destination_acting_set
            .iter()
            .copied()
            .map(|node_id| {
                let node = self.node(node_id).ok_or(ControlPlaneError::UnknownNode {
                    node_id: node_id.as_u32(),
                })?;
                let lease_deadline_ms =
                    node.lease_deadline_ms
                        .ok_or_else(|| ControlPlaneError::CommandDecode {
                            message: format!(
                                "destination node {} has no live lease",
                                node_id.as_u32()
                            ),
                        })?;
                Ok(UnavailablePgPayloadDestinationReadiness {
                    node_id,
                    node_incarnation: node.node_incarnation,
                    endpoint: node.endpoint.clone(),
                    lease_deadline_ms,
                })
            })
            .collect::<Result<Vec<_>, ControlPlaneError>>()?;
        let readiness = UnavailablePgPayloadReadiness {
            pg_id,
            transition_epoch: transition.transition_epoch,
            destination_epoch,
            topology_generation: transition.topology_generation,
            topology_digest: transition.topology_digest,
            ready_at_ms,
            destinations: destinations.clone(),
        };
        validate_unavailable_pg_payload_readiness(self, transition, &readiness)?;
        let mut ready_snapshot = self.clone();
        ready_snapshot
            .unavailable_pg_placement_transitions
            .get_mut(&pg_id)
            .expect("unavailable transition was validated")
            .payload_readiness = Some(readiness);
        let completion = ready_snapshot
            .ready_pg_peering_completions(ready_at_ms)?
            .into_iter()
            .find(|completion| completion.pg_id == pg_id)
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} destination is not ready for atomic activation",
                    pg_id.get()
                ),
            })?;
        Ok(
            ControlPlaneCommand::CompleteUnavailablePgPlacementTransitions {
                ready_at_ms,
                transitions: vec![UnavailablePgTransitionCompletionRequest {
                    unavailable_transition: work.mutation_binding().clone(),
                    pg_id,
                    transition_epoch: transition.transition_epoch,
                    destination_epoch,
                    topology_generation: transition.topology_generation,
                    topology_digest: transition.topology_digest,
                    destinations,
                    completion,
                }],
            },
        )
    }

    pub(crate) fn complete_unavailable_pg_placement_transition_batch_command(
        &self,
        work: &[UnavailablePgReconciliationWork],
        ready_at_ms: u64,
    ) -> Result<ControlPlaneCommand, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "completion",
            work.iter().map(UnavailablePgReconciliationWork::pg_id),
        )?;
        let mut transitions = Vec::with_capacity(work.len());
        for member_work in work {
            let ControlPlaneCommand::CompleteUnavailablePgPlacementTransitions {
                ready_at_ms: member_ready_at_ms,
                transitions: mut member,
            } = self
                .complete_unavailable_pg_placement_transition_command(member_work, ready_at_ms)?
            else {
                unreachable!("unavailable completion member builder returned wrong command");
            };
            if member.len() != 1 || member_ready_at_ms != ready_at_ms {
                return Err(ControlPlaneError::invariant_failure(
                    "unavailable completion member builder returned a non-singleton envelope",
                ));
            }
            transitions.push(member.remove(0));
        }
        Ok(
            ControlPlaneCommand::CompleteUnavailablePgPlacementTransitions {
                ready_at_ms,
                transitions,
            },
        )
    }

    pub(crate) fn prepare_unavailable_pg_placement_completion_batch(
        &self,
        work: &[UnavailablePgReconciliationWork],
        ready_at_ms: u64,
    ) -> Result<PreparedUnavailablePgCompletionBatch, ControlPlaneError> {
        self.prepare_unavailable_pg_placement_completion_batch_with_replication_limit(
            work,
            ready_at_ms,
            crate::control_plane_raft::CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES,
        )
    }

    fn prepare_unavailable_pg_placement_completion_batch_with_replication_limit(
        &self,
        work: &[UnavailablePgReconciliationWork],
        ready_at_ms: u64,
        max_encoded_entry_bytes: usize,
    ) -> Result<PreparedUnavailablePgCompletionBatch, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "completion preparation",
            work.iter().map(UnavailablePgReconciliationWork::pg_id),
        )?;
        let mut included = Vec::new();
        let mut rejected = Vec::new();
        for candidate in work {
            let singleton = match self
                .complete_unavailable_pg_placement_transition_command(candidate, ready_at_ms)
            {
                Ok(command) => command,
                Err(error) => {
                    rejected.push((candidate.clone(), error));
                    continue;
                }
            };
            if let Err(error) = self.apply_control_plane_command(singleton) {
                rejected.push((candidate.clone(), error));
                continue;
            }

            let mut tentative = included.clone();
            tentative.push(candidate.clone());
            let command = self.complete_unavailable_pg_placement_transition_batch_command(
                &tentative,
                ready_at_ms,
            )?;
            let encoded_len =
                crate::control_plane_raft::control_plane_command_replication_encoded_len(&command)?;
            if encoded_len > max_encoded_entry_bytes {
                if included.is_empty() {
                    rejected.push((
                        candidate.clone(),
                        ControlPlaneError::invariant_failure(format!(
                            "single PG {} unavailable transition completion encodes to {encoded_len} OpenRaft entry bytes, exceeding the replication-safe limit {}",
                            candidate.pg_id().get(),
                            max_encoded_entry_bytes
                        )),
                    ));
                    continue;
                }
                break;
            }
            included = tentative;
        }

        let command = if included.is_empty() {
            None
        } else {
            let command = self.complete_unavailable_pg_placement_transition_batch_command(
                &included,
                ready_at_ms,
            )?;
            self.apply_control_plane_command(command.clone())
                .map_err(|error| {
                    ControlPlaneError::invariant_failure(format!(
                        "individually valid unavailable transition completion members form an invalid batch: {error}"
                    ))
                })?;
            Some(command)
        };
        Ok(PreparedUnavailablePgCompletionBatch {
            command,
            included,
            rejected,
        })
    }

    fn validate_unavailable_pg_transition_completion(
        &self,
        request: UnavailablePgTransitionCompletionRequest,
        ready_at_ms: u64,
        batch_identity: &UnavailablePgTransitionBatchReceiptIdentity,
    ) -> Result<ValidatedUnavailablePgTransitionCompletion, ControlPlaneError> {
        let pg_id = request.pg_id;
        if request.completion.pg_id != pg_id {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} completion carries nested PG subject {}",
                    pg_id.get(),
                    request.completion.pg_id.get()
                ),
            });
        }
        let readiness = UnavailablePgPayloadReadiness {
            pg_id,
            transition_epoch: request.transition_epoch,
            destination_epoch: request.destination_epoch,
            topology_generation: request.topology_generation,
            topology_digest: request.topology_digest,
            ready_at_ms,
            destinations: request.destinations,
        };
        let Some(transition) = self.unavailable_pg_placement_transitions.get(&pg_id) else {
            let exact_retained_transition = self
                .retained_unavailable_pg_placement_transitions
                .get(&(pg_id, request.unavailable_transition.transition_epoch()))
                .is_some_and(|retained| {
                    request.unavailable_transition.matches_transition(retained)
                        && retained.payload_readiness.as_ref() == Some(&readiness)
                        && retained.completion.as_ref() == Some(&request.completion)
                        && retained
                            .completion_batch_receipt
                            .as_ref()
                            .is_some_and(|receipt| receipt.identity == *batch_identity)
                });
            if exact_retained_transition {
                return Ok(ValidatedUnavailablePgTransitionCompletion::ExactReplay { pg_id });
            }
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} has no matching active unavailable placement transition",
                    pg_id.get()
                ),
            });
        };
        if !request
            .unavailable_transition
            .matches_transition(transition)
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} completion does not match its active unavailable placement transition",
                    pg_id.get()
                ),
            });
        }
        validate_unavailable_pg_payload_readiness(self, transition, &readiness)?;
        Ok(ValidatedUnavailablePgTransitionCompletion::Apply {
            readiness: Box::new(readiness),
            completion: request.completion,
            batch_identity: batch_identity.clone(),
        })
    }

    fn validate_unavailable_pg_transition_completion_batch(
        &self,
        requests: Vec<UnavailablePgTransitionCompletionRequest>,
        ready_at_ms: u64,
    ) -> Result<Vec<ValidatedUnavailablePgTransitionCompletion>, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "completion",
            requests.iter().map(|request| request.pg_id),
        )?;
        let batch_identity =
            unavailable_pg_transition_completion_batch_identity(&requests, ready_at_ms);
        requests
            .into_iter()
            .map(|request| {
                self.validate_unavailable_pg_transition_completion(
                    request,
                    ready_at_ms,
                    &batch_identity,
                )
            })
            .collect()
    }

    fn apply_validated_unavailable_pg_transition_completions(
        &self,
        validated: Vec<ValidatedUnavailablePgTransitionCompletion>,
        ready_at_ms: u64,
    ) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
        validate_canonical_unavailable_pg_batch(
            "completion",
            validated
                .iter()
                .map(ValidatedUnavailablePgTransitionCompletion::pg_id),
        )?;
        if validated.iter().all(|entry| {
            matches!(
                entry,
                ValidatedUnavailablePgTransitionCompletion::ExactReplay { .. }
            )
        }) {
            return Ok(None);
        }
        if validated.iter().any(|entry| {
            matches!(
                entry,
                ValidatedUnavailablePgTransitionCompletion::ExactReplay { .. }
            )
        }) {
            return Err(ControlPlaneError::CommandDecode {
                message: "unavailable placement completion batch mixes replayed and new members"
                    .to_string(),
            });
        }
        self.validate_serving_timestamp(ready_at_ms)?;
        let mut ready_snapshot = self.clone();
        for entry in &validated {
            let ValidatedUnavailablePgTransitionCompletion::Apply { readiness, .. } = entry else {
                unreachable!("mixed replay was rejected before batch validation");
            };
            ready_snapshot
                .unavailable_pg_placement_transitions
                .get_mut(&readiness.pg_id)
                .expect("payload-readiness transition was validated")
                .payload_readiness = Some((**readiness).clone());
        }
        for entry in &validated {
            let ValidatedUnavailablePgTransitionCompletion::Apply {
                readiness,
                completion,
                ..
            } = entry
            else {
                unreachable!("mixed replay was rejected before batch validation");
            };
            validate_pg_peering_completion(PgPeeringCompletionValidation {
                snapshot: &ready_snapshot,
                pg_id: readiness.pg_id,
                primary: completion.primary,
                node_incarnation: completion.node_incarnation,
                completed_at_ms: ready_at_ms,
                expected: Some(ExpectedPgPeeringCompletion {
                    active_metadata_proof: completion.active_metadata_proof,
                    active_metadata_proof_epoch: completion.active_metadata_proof_epoch,
                }),
            })?;
        }
        ready_snapshot.record_committed_timestamp(ready_at_ms);
        let mut completed = Vec::with_capacity(validated.len());
        for entry in validated {
            let ValidatedUnavailablePgTransitionCompletion::Apply {
                readiness,
                completion,
                batch_identity,
            } = entry
            else {
                unreachable!("mixed replay was rejected before batch mutation");
            };
            let pg_id = readiness.pg_id;
            let record = ready_snapshot
                .pgs
                .get_mut(&pg_id)
                .expect("unavailable placement completion PG was validated");
            record.state = PgState::Active;
            record.active_primary = Some(completion.primary);
            record.active_metadata_proof = Some(completion.active_metadata_proof);
            record.active_metadata_transfer_imported = record.peering_metadata_transfer.is_some();
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
            let mut transition = ready_snapshot
                .unavailable_pg_placement_transitions
                .remove(&pg_id)
                .expect("completed unavailable transition was validated");
            transition.completion = Some(completion);
            transition.completion_batch_receipt = Some(UnavailablePgTransitionBatchReceipt {
                identity: batch_identity,
                source_epoch: self.cluster_epoch,
                target_epoch: next_epoch(self.cluster_epoch)?,
            });
            ready_snapshot
                .retained_unavailable_pg_placement_transitions
                .insert((pg_id, transition.transition_epoch), transition);
            completed.push((pg_id, completion.active_metadata_proof_epoch));
        }
        ready_snapshot.bump_epoch()?;
        for (pg_id, proof_epoch) in completed {
            ready_snapshot
                .pgs
                .get_mut(&pg_id)
                .expect("unavailable placement PG activated before epoch bump")
                .active_metadata_proof_epoch = Some(proof_epoch);
        }
        Ok(Some(ready_snapshot))
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
        let pending_metadata_command_recovery =
            self.pending_metadata_command_recovery_for_pg(record)?;
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
            pending_metadata_command_recovery,
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
        let pending_metadata_command_recovery =
            self.pending_metadata_command_recovery_for_pg(record)?;
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
            pending_metadata_command_recovery,
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
            pending_metadata_command_recovery,
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
        for (node_id, node) in &self.nodes {
            let Some(observation) = node.pg_observation(record.pg_id) else {
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
            .filter(|record| matches!(record.state, PgState::Active | PgState::Peering))
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
        route.pending_metadata_command_recovery =
            self.pending_metadata_command_recovery_for_pg(record)?;
        self.runtime_map_from_pg_routes_with_history_and_extra_nodes(
            vec![route],
            self.historical_pg_routes_for_runtime_map_pg(pg_id)?,
            self.historical_cluster_epochs(),
            RuntimeMapFreshnessProof::Reconstructed {
                authority_incarnation: self.authority_incarnation,
            },
            self.unavailable_pg_placement_transitions
                .get(&pg_id)
                .into_iter()
                .flat_map(|transition| transition.destination_acting_set.iter().copied()),
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
        let node = self
            .node(refreshing_node_id)
            .ok_or(ControlPlaneError::UnknownNode {
                node_id: refreshing_node_id.as_u32(),
            })?;
        let history_protection = required_cluster_map_history_protection(
            self.pgs.values(),
            self.nodes.values(),
            self.unavailable_pg_placement_transitions
                .values()
                .chain(self.retained_unavailable_pg_placement_transitions.values()),
        );
        let historical_pg_routes = self.historical_pg_routes_for_storage_node_refresh(
            node.retained_cluster_map_history_route_references(),
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
            staging_authorizations: self.committed_staging_authorization_presentations()?,
        })
    }

    fn committed_staging_authorization_presentations(
        &self,
    ) -> Result<
        Vec<crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation>,
        ControlPlaneError,
    > {
        let mut batches = BTreeMap::<
            UnavailablePgTransitionBatchReceipt,
            Vec<UnavailablePgStagingIntentAuthorizationRequest>,
        >::new();
        for transition in self
            .retained_unavailable_pg_placement_transitions
            .values()
            .chain(self.unavailable_pg_placement_transitions.values())
        {
            let Some(authorization) = transition.staging_authorization.as_ref() else {
                continue;
            };
            let request = unavailable_pg_staging_authorization_request_from_durable(transition)
                .expect("staging authorization has a durable request");
            batches
                .entry(authorization.batch_receipt.clone())
                .or_default()
                .push(request);
        }
        let mut presentations = batches
            .into_iter()
            .map(|(receipt, mut authorizations)| {
                authorizations.sort_by_key(|authorization| {
                    authorization.unavailable_transition.pg_id()
                });
                if receipt.source_epoch != receipt.target_epoch
                    || receipt.identity.stage
                        != UnavailablePgTransitionBatchStage::StagingAuthorization
                    || receipt.identity.member_pg_ids
                        != authorizations
                            .iter()
                            .map(|authorization| authorization.unavailable_transition.pg_id())
                            .collect::<Vec<_>>()
                    || receipt.identity
                        != unavailable_pg_staging_authorization_batch_identity(&authorizations)
                {
                    return Err(ControlPlaneError::invariant_failure(
                        "committed staging authorization batch receipt is invalid",
                    ));
                }
                crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation::from_authority_state(
                    authorizations,
                    receipt.source_epoch,
                    receipt.identity.members_digest,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        presentations.sort_by_key(|authorization| {
            (
                authorization.committed_epoch(),
                authorization.batch_members_digest(),
            )
        });
        Ok(presentations)
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
        history_route_references: impl IntoIterator<Item = PgClusterMapHistoryRouteReference>,
        globally_protected_route_keys: &BTreeSet<(ClusterEpoch, PgId)>,
        observed_epoch: ClusterEpoch,
        current_routes: &[PgRouteSnapshot],
        refreshing_node_id: NodeId,
    ) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
        let mut required_keys = BTreeSet::new();
        let mut pending_keys = Vec::new();
        for reference in history_route_references {
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
                if recovery.pending().cluster_epoch() < route.cluster_epoch() {
                    add_required_historical_route_key(
                        &mut required_keys,
                        &mut pending_keys,
                        recovery.pending().cluster_epoch(),
                        route.pg_id(),
                    );
                }
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
                && current.retiring_cluster_map_history_route_references
                    == updated.retiring_cluster_map_history_route_references
                && (current.cluster_map_history_route_scan_generation
                    == updated.cluster_map_history_route_scan_generation
                    || (current
                        .retiring_cluster_map_history_route_references
                        .is_empty()
                        && updated
                            .retiring_cluster_map_history_route_references
                            .is_empty()))
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
        let ControlPlaneCommand::FencePgForMetadataTransfer {
            pg_id,
            unavailable_transition,
            ..
        } = command
        else {
            return Ok(command);
        };
        let fence_command = || ControlPlaneCommand::FencePgForMetadataTransfer {
            pg_id,
            source_primary_lease_deadline_ms: None,
            lease_horizon_authority: None,
            unavailable_transition: unavailable_transition.clone(),
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
            unavailable_transition,
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
            &self.unavailable_pg_placement_transitions,
            &self.retained_unavailable_pg_placement_transitions,
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
        self.validate_metadata_transfer_staging_evidence_invariants()?;
        for (node_id, observation) in &self.unavailable_node_observations {
            if *node_id != observation.node_id {
                return Err("unavailable node observation key does not match its subject".into());
            }
            let node = self.nodes.get(node_id).ok_or_else(|| {
                format!(
                    "unavailable observation references unknown node {}",
                    node_id.as_u32()
                )
            })?;
            if node.node_incarnation != observation.node_incarnation
                || node.endpoint != observation.endpoint
            {
                return Err(format!(
                    "unavailable observation for node {} does not match its incarnation and endpoint",
                    node_id.as_u32()
                ));
            }
            if observation.lease_deadline_ms == 0
                || observation.lease_deadline_ms > observation.observed_at_ms
            {
                return Err(format!(
                    "unavailable observation for node {} has an invalid lease interval",
                    node_id.as_u32()
                ));
            }
            if node.observed_availability != NodeAvailabilityState::Unavailable
                || node.lease_deadline_ms.is_some()
            {
                return Err(format!(
                    "unavailable observation for node {} is retained after lease renewal",
                    node_id.as_u32()
                ));
            }
        }
        let unavailable_pg_transition_successors =
            validate_unavailable_pg_transition_lineages(self)?;
        for ((pg_id, transition_epoch), transition) in
            &self.retained_unavailable_pg_placement_transitions
        {
            if *pg_id != transition.pg_id || *transition_epoch != transition.transition_epoch {
                return Err(
                    "retained unavailable PG transition key does not match its subject".into(),
                );
            }
            validate_unavailable_pg_transition_invariant(
                self,
                transition,
                &unavailable_pg_transition_successors,
            )?;
        }
        for (pg_id, transition) in &self.unavailable_pg_placement_transitions {
            if *pg_id != transition.pg_id {
                return Err(
                    "active unavailable PG transition key does not match its subject".into(),
                );
            }
            if transition.payload_readiness.is_some() {
                return Err(format!(
                    "active unavailable PG transition {} retains standalone payload readiness",
                    pg_id.get()
                ));
            }
            if transition.completion.is_some() || transition.completion_batch_receipt.is_some() {
                return Err(format!(
                    "active unavailable PG transition {} retains completion evidence",
                    pg_id.get()
                ));
            }
            validate_unavailable_pg_transition_invariant(
                self,
                transition,
                &unavailable_pg_transition_successors,
            )?;
            let pg = self.pgs.get(pg_id).ok_or_else(|| {
                format!(
                    "unavailable transition references unknown PG {}",
                    pg_id.get()
                )
            })?;
            match transition.destination_epoch {
                None if pg.state == PgState::Peering
                    && pg.acting_set
                        == unavailable_transition_source_route_acting_set(
                            &transition.source_acting_set,
                            transition.source_node_id,
                        ) => {}
                Some(destination_epoch)
                    if destination_epoch > transition.transition_epoch
                        && destination_epoch <= self.cluster_epoch
                        && pg.state == PgState::Peering
                        && pg.acting_set == transition.destination_acting_set => {}
                _ => {
                    return Err(format!(
                        "active unavailable PG transition {} does not match current Peering placement",
                        pg_id.get()
                    ));
                }
            }
        }
        validate_unavailable_pg_transition_batch_receipts(self)?;
        for pg in self.pgs.values() {
            if pg.acting_set.is_empty() {
                return Err(format!("PG {} has an empty acting set", pg.pg_id.get()));
            }
            if let Some(topology) = &self.initial_topology {
                topology
                    .placement_policy()
                    .validate_acting_set(&pg.acting_set)
                    .map_err(|message| {
                        format!(
                            "PG {} violates certified placement policy: {message}",
                            pg.pg_id.get()
                        )
                    })?;
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
                    validate_peering_metadata_proof_state(
                        pg.pg_id,
                        pg.peering_metadata_proof_floor,
                        pg.peering_metadata_proof_floor_epoch,
                        pg.peering_metadata_proof_floor_imported,
                        pg.peering_metadata_transfer,
                        self.cluster_epoch,
                    )?;
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
            for reference in node.retained_cluster_map_history_route_references() {
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
            if node
                .retiring_cluster_map_history_route_references
                .iter()
                .any(|reference| {
                    node.cluster_map_history_route_references
                        .iter()
                        .any(|current| current == reference)
                })
            {
                return Err(format!(
                    "node {} has a history route marked both current and retiring",
                    node.node_id.as_u32()
                ));
            }
            if node.cluster_map_history_route_scan_generation.is_none()
                && (!node.cluster_map_history_route_references.is_empty()
                    || !node
                        .retiring_cluster_map_history_route_references
                        .is_empty())
            {
                return Err(format!(
                    "node {} has history route references without an accepted scan generation",
                    node.node_id.as_u32()
                ));
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
                    let Some(pending) = observation.pending_metadata_command else {
                        return Err(format!(
                            "node {} observation references PG {} outside the acting set",
                            node.node_id.as_u32(),
                            observation.pg_id.get()
                        ));
                    };
                    let historical = self
                        .reconstructed_pg_route_at_epoch(
                            observation.pg_id,
                            pending.cluster_epoch(),
                        )
                        .map_err(|error| {
                            format!(
                                "node {} historical pending observation for PG {} has no valid route: {error}",
                                node.node_id.as_u32(),
                                observation.pg_id.get()
                            )
                        })?;
                    if historical.state() != PgState::Active
                        || historical.primary_node_id() != node.node_id
                    {
                        return Err(format!(
                            "node {} historical pending observation for PG {} was not reported by its active primary",
                            node.node_id.as_u32(),
                            observation.pg_id.get()
                        ));
                    }
                }
                if pg.state == PgState::Active
                    && pg.active_primary == Some(node.node_id)
                    && observation.state == PgState::Active
                {
                    if observation
                        .pending_metadata_command()
                        .is_some_and(|pending| pending.cluster_epoch() != self.cluster_epoch)
                    {
                        return Err(format!(
                            "active primary node {} observation for PG {} has a non-current pending metadata command",
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
        let protection = required_cluster_map_history_protection(
            self.pgs.values(),
            self.nodes.values(),
            self.unavailable_pg_placement_transitions
                .values()
                .chain(self.retained_unavailable_pg_placement_transitions.values()),
        );
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
            if self.unavailable_replacement_grace_elapsed_for_pg(record, now_ms) {
                continue;
            }
            if self
                .unavailable_pg_placement_transitions
                .get(&record.pg_id)
                .is_some_and(|transition| {
                    validate_unavailable_pg_payload_readiness_at(self, transition, now_ms).is_err()
                })
            {
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
                    next_snapshot.unavailable_node_observations.remove(&node_id);
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
                    next_snapshot.unavailable_node_observations.remove(&node_id);
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
                    let historical_pending_pg_observations =
                        validate_historical_pending_pg_heartbeat_observations(
                            self,
                            heartbeat.node_id,
                            &heartbeat.pg_observations,
                        )?;
                    let historical_pending_pg_ids: Vec<PgId> = historical_pending_pg_observations
                        .iter()
                        .map(|observation| observation.pg_id)
                        .collect();
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
                            record.cluster_map_history_route_scan_generation = None;
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
                        record.record_cluster_map_history_route_references(
                            heartbeat.cluster_map_history_route_scan_generation,
                            heartbeat.cluster_map_history_route_references.clone(),
                        );
                        record.pg_observations.clear();
                    }
                    next_snapshot
                        .unavailable_node_observations
                        .remove(&heartbeat.node_id);
                    if !historical_pending_pg_ids.is_empty() {
                        let newly_peering = mark_pgs_peering_for_pg_ids(
                            &mut next_snapshot,
                            self,
                            historical_pending_pg_ids,
                        );
                        epoch_changed |= !newly_peering.is_empty();
                    }
                    if epoch_changed {
                        if let Some(node_id) = affected_node {
                            mark_pgs_peering_for_nodes(&mut next_snapshot, self, [node_id]);
                        }
                        next_snapshot.bump_epoch()?;
                    }
                    // Preserve the validated historical-primary evidence across an
                    // epoch bump that fenced an Active route, and restore it after
                    // an idempotent stale-heartbeat retransmission cleared the
                    // reporter's observation map. Recovery discovery consumes only
                    // Peering observations.
                    let observation_epoch = next_snapshot.cluster_epoch;
                    let record = next_snapshot
                        .nodes
                        .get_mut(&heartbeat.node_id)
                        .expect("node record validated before heartbeat mutation");
                    for observation in historical_pending_pg_observations {
                        record.pg_observations.insert(
                            observation.pg_id,
                            NodePgObservationRecord {
                                pg_id: observation.pg_id,
                                state: PgState::Peering,
                                observed_epoch: observation_epoch,
                                observed_at_ms: heartbeat_at_ms,
                                metadata_proof: observation.metadata_proof,
                                pending_metadata_command: observation.pending_metadata_command,
                            },
                        );
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
                let historical_pending_active_pg_observations = validate_pg_heartbeat_observations(
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
                        record.cluster_map_history_route_scan_generation = None;
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
                    record.record_cluster_map_history_route_references(
                        heartbeat.cluster_map_history_route_scan_generation,
                        heartbeat.cluster_map_history_route_references.clone(),
                    );
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
                next_snapshot
                    .unavailable_node_observations
                    .remove(&heartbeat.node_id);
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
                if !historical_pending_active_pg_observations.is_empty() {
                    mark_pgs_peering_for_pg_ids(
                        &mut next_snapshot,
                        self,
                        historical_pending_active_pg_observations
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
                    if !historical_pending_active_pg_observations.is_empty() {
                        let current_epoch = next_snapshot.cluster_epoch;
                        let record = next_snapshot
                            .nodes
                            .get_mut(&heartbeat.node_id)
                            .expect("node record validated before heartbeat mutation");
                        for observation in historical_pending_active_pg_observations {
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
                let mut unavailable_observations = Vec::new();
                for record in next_snapshot.nodes.values_mut() {
                    if matches!(
                        record.membership,
                        NodeMembershipState::Out | NodeMembershipState::Removed
                    ) || record.observed_availability == NodeAvailabilityState::Unavailable
                    {
                        continue;
                    }
                    if let Some(lease_deadline_ms) = record
                        .lease_deadline_ms
                        .filter(|lease_deadline_ms| *lease_deadline_ms <= expire_at_ms)
                    {
                        unavailable_observations.push(NodeUnavailableObservation {
                            node_id: record.node_id,
                            node_incarnation: record.node_incarnation,
                            endpoint: record.endpoint.clone(),
                            lease_deadline_ms,
                            observed_at_ms: expire_at_ms,
                        });
                        record.observed_availability = NodeAvailabilityState::Unavailable;
                        record.lease_deadline_ms = None;
                        expired_nodes.push(record.node_id);
                        if record.administratively_available {
                            serving_expired_nodes.push(record.node_id);
                        }
                    }
                }
                for observation in unavailable_observations {
                    next_snapshot
                        .unavailable_node_observations
                        .insert(observation.node_id, observation);
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
                    let observation = NodeUnavailableObservation {
                        node_id: record.node_id,
                        node_incarnation: record.node_incarnation,
                        endpoint: record.endpoint.clone(),
                        lease_deadline_ms: lease.lease_deadline_ms,
                        observed_at_ms: expire_at_ms,
                    };
                    expired_nodes.push(lease.node_id);
                    if record.administratively_available {
                        serving_expired_nodes.push(lease.node_id);
                    }
                    next_snapshot
                        .unavailable_node_observations
                        .insert(lease.node_id, observation);
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
            ControlPlaneCommand::BeginUnavailablePgPlacementTransitions {
                transitions,
                expected_transition_epoch,
                begin_at_ms,
            } => {
                let validated = self.validate_unavailable_pg_transition_begin_batch(
                    transitions,
                    expected_transition_epoch,
                    begin_at_ms,
                )?;
                let next_snapshot = self.apply_validated_unavailable_pg_transition_begins(
                    validated,
                    expected_transition_epoch,
                    begin_at_ms,
                )?;
                let changed = next_snapshot.is_some();
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot.unwrap_or_else(|| self.clone()),
                    ControlPlaneCommandResponse::BeginUnavailablePgPlacementTransitions,
                    changed,
                ))
            }
            ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents { authorizations } => {
                let validated =
                    self.validate_unavailable_pg_staging_authorization_batch(authorizations)?;
                let next_snapshot =
                    self.apply_validated_unavailable_pg_staging_authorizations(validated)?;
                let changed = next_snapshot.is_some();
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot.unwrap_or_else(|| self.clone()),
                    ControlPlaneCommandResponse::AuthorizeUnavailablePgStagingIntents,
                    changed,
                ))
            }
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload,
                page_digest,
            } => self.apply_metadata_transfer_staging_evidence_page(operation_payload, page_digest),
            ControlPlaneCommand::CheckpointMetadataTransferStagingEvidencePages {
                actor_node_id,
                actor_node_incarnation,
                first_generation,
                last_generation,
            } => self.checkpoint_metadata_transfer_staging_evidence_pages(
                actor_node_id,
                actor_node_incarnation,
                first_generation,
                last_generation,
            ),
            ControlPlaneCommand::CollapseMetadataTransferStagingEvidenceCheckpointSegment {
                actor_node_id,
                actor_node_incarnation,
                first_generation,
                last_generation,
                source_segment_digest,
            } => self.collapse_metadata_transfer_staging_evidence_checkpoint_segment(
                actor_node_id,
                actor_node_incarnation,
                first_generation,
                last_generation,
                source_segment_digest,
            ),
            ControlPlaneCommand::CoalesceMetadataTransferStagingEvidenceCheckpointAnchors {
                actor_node_id,
                actor_node_incarnation,
                first_generation,
                last_generation,
                source_segment_count,
                source_segments_digest,
            } => self.coalesce_metadata_transfer_staging_evidence_checkpoint_anchors(
                actor_node_id,
                actor_node_incarnation,
                first_generation,
                last_generation,
                source_segment_count,
                source_segments_digest,
            ),
            ControlPlaneCommand::RetireMetadataTransferStagingActorClosure {
                actor_node_id,
                actor_node_incarnation,
                certificate_digest,
            } => self.retire_metadata_transfer_staging_actor_closure(
                actor_node_id,
                actor_node_incarnation,
                certificate_digest,
            ),
            ControlPlaneCommand::FinalizeMetadataTransferStagingGeneration { cleanup } => {
                self.finalize_metadata_transfer_staging_generation(cleanup)
            }
            ControlPlaneCommand::InstallUnavailablePgPlacementTransitions {
                transitions,
                expected_destination_epoch,
            } => {
                let validated = self.validate_unavailable_pg_destination_install_batch(
                    transitions,
                    expected_destination_epoch,
                )?;
                let next_snapshot = self.apply_validated_unavailable_pg_destination_installs(
                    validated,
                    expected_destination_epoch,
                )?;
                let changed = next_snapshot.is_some();
                Ok(applied_control_plane_command(
                    self,
                    next_snapshot.unwrap_or_else(|| self.clone()),
                    ControlPlaneCommandResponse::InstallUnavailablePgPlacementTransitions,
                    changed,
                ))
            }
            ControlPlaneCommand::CompleteUnavailablePgPlacementTransitions {
                ready_at_ms,
                transitions,
            } => {
                let validated = self.validate_unavailable_pg_transition_completion_batch(
                    transitions,
                    ready_at_ms,
                )?;
                let ready_snapshot = self.apply_validated_unavailable_pg_transition_completions(
                    validated,
                    ready_at_ms,
                )?;
                let changed = ready_snapshot.is_some();
                Ok(applied_control_plane_command(
                    self,
                    ready_snapshot.unwrap_or_else(|| self.clone()),
                    ControlPlaneCommandResponse::CompleteUnavailablePgPlacementTransitions,
                    changed,
                ))
            }
            ControlPlaneCommand::SetPgActingSet { pg_id, acting_set } => {
                if self
                    .unavailable_pg_placement_transitions
                    .contains_key(&pg_id)
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} acting set is owned by an unavailable placement transition",
                            pg_id.get()
                        ),
                    });
                }
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
                validate_unavailable_transition_mutation_binding(self, pg_id, None)?;
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
                            expected: Box::new(existing_transfer),
                            actual: Box::new(transfer),
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
                unavailable_transition,
            } => {
                validate_unavailable_transition_mutation_binding(
                    self,
                    pg_id,
                    unavailable_transition.as_ref(),
                )?;
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
                if let Some(transition) = self.unavailable_pg_placement_transitions.get(&pg_id) {
                    validate_unavailable_pg_payload_readiness_at(self, transition, complete_at_ms)?;
                }
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
                        if let Some(transition) = next_snapshot
                            .unavailable_pg_placement_transitions
                            .remove(&pg_id)
                        {
                            next_snapshot
                                .retained_unavailable_pg_placement_transitions
                                .insert((pg_id, transition.transition_epoch), transition);
                        }
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
                    if let Some(transition) = self
                        .unavailable_pg_placement_transitions
                        .get(&completion.pg_id)
                    {
                        validate_unavailable_pg_payload_readiness_at(
                            self,
                            transition,
                            ready_at_ms,
                        )?;
                    }
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
                    if let Some(transition) = next_snapshot
                        .unavailable_pg_placement_transitions
                        .remove(&completion.pg_id)
                    {
                        next_snapshot
                            .retained_unavailable_pg_placement_transitions
                            .insert((completion.pg_id, transition.transition_epoch), transition);
                    }
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
        let policy = certificate.placement_policy();
        let certified_node_ids = policy
            .nodes
            .iter()
            .map(|node| node.node_id)
            .collect::<Vec<_>>();
        let mut bootstrap_node_ids = unique_nodes.iter().copied().collect::<Vec<_>>();
        bootstrap_node_ids.sort_unstable();
        if certified_node_ids != bootstrap_node_ids {
            return Err(ControlPlaneError::InvalidInitialTopology {
                message: "certified storage domains do not match bootstrap nodes".to_string(),
            });
        }
        for (pg_id, acting_set) in &pg_acting_sets {
            policy.validate_acting_set(acting_set).map_err(|message| {
                ControlPlaneError::InvalidInitialTopology {
                    message: format!(
                        "bootstrap PG {} violates placement policy: {message}",
                        pg_id.get()
                    ),
                }
            })?;
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

fn deterministic_unavailable_pg_destination(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    source_acting_set: &[NodeId],
    unavailable_node_id: NodeId,
    now_ms: u64,
) -> Result<Vec<NodeId>, ControlPlaneError> {
    let unavailable_position = source_acting_set
        .iter()
        .position(|node_id| *node_id == unavailable_node_id)
        .ok_or_else(|| ControlPlaneError::CommandDecode {
            message: format!(
                "PG {} unavailable node {} is outside the source acting set",
                pg_id.get(),
                unavailable_node_id.as_u32()
            ),
        })?;
    let placement_policy = snapshot
        .initial_topology
        .as_ref()
        .ok_or_else(|| ControlPlaneError::CommandDecode {
            message: "unavailable placement requires certified placement policy".to_string(),
        })?
        .placement_policy();
    placement_policy
        .validate_acting_set(source_acting_set)
        .map_err(|message| ControlPlaneError::CommandDecode {
            message: format!(
                "PG {} source placement violates policy: {message}",
                pg_id.get()
            ),
        })?;
    let replacement = snapshot
        .nodes
        .values()
        .filter(|node| !source_acting_set.contains(&node.node_id))
        .filter(|node| node.membership == NodeMembershipState::Active)
        .filter(|node| node.can_serve_primary(snapshot.cluster_epoch, now_ms))
        .filter_map(|node| {
            let mut destination = source_acting_set.to_vec();
            destination[unavailable_position] = node.node_id;
            placement_policy
                .validate_acting_set(&destination)
                .is_ok()
                .then_some(node.node_id)
        })
        .next()
        .ok_or_else(|| ControlPlaneError::CommandDecode {
            message: format!(
                "PG {} has no eligible spare for unavailable node {}",
                pg_id.get(),
                unavailable_node_id.as_u32()
            ),
        })?;
    let mut destination = source_acting_set.to_vec();
    destination[unavailable_position] = replacement;
    Ok(destination)
}

fn unavailable_pg_transition_begin_authorization(
    snapshot: &ClusterControlSnapshot,
    record: &PgControlRecord,
    destination_acting_set: &[NodeId],
    unavailable_node: &NodeUnavailableObservation,
    begin_at_ms: u64,
) -> Result<UnavailablePgTransitionBeginAuthorization, ControlPlaneError> {
    let (source_node_id, source_metadata_proof, source_floor, source_floor_epoch, source_imported) =
        match record.state {
            PgState::Active => {
                let source_node_id =
                    record
                        .active_primary
                        .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                            pg_id: record.pg_id.get(),
                            cluster_epoch: snapshot.cluster_epoch,
                        })?;
                let source_node = snapshot.node(source_node_id).ok_or(
                    ControlPlaneError::UnknownActingSetNode {
                        pg_id: record.pg_id.get(),
                        node_id: source_node_id.as_u32(),
                    },
                )?;
                if !source_node.can_serve_primary(snapshot.cluster_epoch, begin_at_ms) {
                    return Err(ControlPlaneError::NodeLeaseExpired {
                        node_id: source_node_id.as_u32(),
                        now_ms: begin_at_ms,
                        lease_deadline_ms: source_node.lease_deadline_ms,
                    });
                }
                let observation = source_node.pg_observation(record.pg_id).ok_or(
                    ControlPlaneError::PgPrimaryMissingActiveObservation {
                        pg_id: record.pg_id.get(),
                        node_id: source_node_id.as_u32(),
                        cluster_epoch: snapshot.cluster_epoch,
                    },
                )?;
                let source_floor = record.active_metadata_proof.ok_or(
                    ControlPlaneError::ActivePgMissingMetadataProof {
                        pg_id: record.pg_id.get(),
                    },
                )?;
                if observation.observed_epoch != snapshot.cluster_epoch
                    || observation.state != PgState::Active
                    || observation.has_pending_metadata_command()
                    || !metadata_proof_satisfies_active_primary_observation_floor(
                        source_floor,
                        observation.metadata_proof,
                        metadata_proof_progress_provenance(
                            record.active_metadata_transfer_imported,
                            record.active_metadata_proof_epoch,
                        ),
                        observation.observed_epoch,
                    )
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "PG {} Active metadata-transfer source is not exactly proof-qualified",
                            record.pg_id.get()
                        ),
                    });
                }
                (
                    source_node_id,
                    observation.metadata_proof,
                    source_floor,
                    record.active_metadata_proof_epoch,
                    record.active_metadata_transfer_imported,
                )
            }
            PgState::Peering => {
                let source_route =
                    peering_metadata_read_route_for_snapshot(snapshot, record, begin_at_ms)
                        .ok_or_else(|| ControlPlaneError::CommandDecode {
                            message: format!(
                                "PG {} has no proof-qualified serving metadata-transfer source",
                                record.pg_id.get()
                            ),
                        })?;
                let source_floor =
                    record
                        .peering_metadata_proof_floor_context()
                        .ok_or_else(|| ControlPlaneError::CommandDecode {
                            message: format!(
                                "PG {} metadata-transfer source has no certified proof floor",
                                record.pg_id.get()
                            ),
                        })?;
                (
                    source_route.node_id(),
                    source_route.proof(),
                    source_floor.proof,
                    source_floor.epoch,
                    source_floor.imported,
                )
            }
            state => {
                return Err(ControlPlaneError::PgNotActive {
                    pg_id: record.pg_id.get(),
                    cluster_epoch: snapshot.cluster_epoch,
                    state,
                });
            }
        };
    let source_node = snapshot
        .node(source_node_id)
        .expect("proof-qualified source route references a known node");
    let source_observation = source_node
        .pg_observation(record.pg_id)
        .expect("proof-qualified source route references an exact observation");
    let replacement_node_id = record
        .acting_set
        .iter()
        .copied()
        .zip(destination_acting_set.iter().copied())
        .find_map(|(source, destination)| (source != destination).then_some(destination))
        .ok_or_else(|| ControlPlaneError::CommandDecode {
            message: format!(
                "PG {} destination has no replacement actor",
                record.pg_id.get()
            ),
        })?;
    let replacement = snapshot
        .node(replacement_node_id)
        .ok_or(ControlPlaneError::UnknownNode {
            node_id: replacement_node_id.as_u32(),
        })?;
    let source_lease_deadline_ms =
        source_node
            .lease_deadline_ms
            .ok_or(ControlPlaneError::NodeLeaseExpired {
                node_id: source_node_id.as_u32(),
                now_ms: begin_at_ms,
                lease_deadline_ms: None,
            })?;
    let replacement_lease_deadline_ms =
        replacement
            .lease_deadline_ms
            .ok_or(ControlPlaneError::NodeLeaseExpired {
                node_id: replacement_node_id.as_u32(),
                now_ms: begin_at_ms,
                lease_deadline_ms: None,
            })?;
    Ok(UnavailablePgTransitionBeginAuthorization {
        begin_at_ms,
        unavailable_node: unavailable_node.clone(),
        source_route: HistoricalPgRouteRecord::from(record),
        source_metadata_floor: source_floor,
        source_metadata_floor_epoch: source_floor_epoch,
        source_metadata_floor_imported: source_imported,
        source_node_id,
        source_node_incarnation: source_node.node_incarnation,
        source_endpoint: source_node.endpoint.clone(),
        source_lease_deadline_ms,
        source_observed_at_ms: source_observation.observed_at_ms,
        source_metadata_proof,
        replacement_node_id,
        replacement_node_incarnation: replacement.node_incarnation,
        replacement_endpoint: replacement.endpoint.clone(),
        replacement_lease_deadline_ms,
    })
}

fn validate_unavailable_pg_payload_readiness(
    snapshot: &ClusterControlSnapshot,
    transition: &UnavailablePgPlacementTransition,
    readiness: &UnavailablePgPayloadReadiness,
) -> Result<(), ControlPlaneError> {
    if readiness.pg_id != transition.pg_id
        || readiness.transition_epoch != transition.transition_epoch
        || Some(readiness.destination_epoch) != transition.destination_epoch
        || readiness.topology_generation != transition.topology_generation
        || readiness.topology_digest != transition.topology_digest
    {
        return Err(ControlPlaneError::CommandDecode {
            message: format!(
                "PG {} payload readiness does not match its active transition",
                transition.pg_id.get()
            ),
        });
    }
    let pg = snapshot
        .pg(transition.pg_id)
        .ok_or(ControlPlaneError::UnknownPg {
            pg_id: transition.pg_id.get(),
        })?;
    if pg.state != PgState::Peering || pg.acting_set != transition.destination_acting_set {
        return Err(ControlPlaneError::CommandDecode {
            message: format!(
                "PG {} payload readiness does not match its Peering destination",
                transition.pg_id.get()
            ),
        });
    }
    if readiness.destinations.len() != transition.destination_acting_set.len() {
        return Err(ControlPlaneError::CommandDecode {
            message: format!(
                "PG {} payload readiness does not cover every destination actor",
                transition.pg_id.get()
            ),
        });
    }
    for (node_id, supplied) in transition
        .destination_acting_set
        .iter()
        .copied()
        .zip(&readiness.destinations)
    {
        let node = snapshot
            .node(node_id)
            .ok_or(ControlPlaneError::UnknownNode {
                node_id: node_id.as_u32(),
            })?;
        if supplied.node_id != node_id
            || supplied.node_incarnation != node.node_incarnation
            || supplied.endpoint != node.endpoint
            || node.membership != NodeMembershipState::Active
            || !node.can_serve_primary(snapshot.cluster_epoch, readiness.ready_at_ms)
            || node.lease_deadline_ms != Some(supplied.lease_deadline_ms)
            || supplied.lease_deadline_ms <= readiness.ready_at_ms
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} destination node {} is not exactly payload-write ready",
                    transition.pg_id.get(),
                    node_id.as_u32()
                ),
            });
        }
    }
    Ok(())
}

fn validate_unavailable_transition_mutation_binding(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    supplied: Option<&UnavailablePgTransitionMutationBinding>,
) -> Result<(), ControlPlaneError> {
    let active = snapshot.unavailable_pg_placement_transitions.get(&pg_id);
    match (active, supplied) {
        (None, None) => Ok(()),
        (Some(transition), Some(binding)) if binding.matches_transition(transition) => Ok(()),
        (Some(_), None) => Err(ControlPlaneError::CommandDecode {
            message: format!(
                "PG {} metadata transfer requires its exact unavailable transition binding",
                pg_id.get()
            ),
        }),
        (None, Some(_)) => Err(ControlPlaneError::CommandDecode {
            message: format!(
                "PG {} unavailable transition binding has no active transition",
                pg_id.get()
            ),
        }),
        (Some(_), Some(_)) => Err(ControlPlaneError::CommandDecode {
            message: format!(
                "PG {} unavailable transition binding does not match the active transition",
                pg_id.get()
            ),
        }),
    }
}

fn validate_unavailable_pg_payload_readiness_at(
    snapshot: &ClusterControlSnapshot,
    transition: &UnavailablePgPlacementTransition,
    activation_at_ms: u64,
) -> Result<(), ControlPlaneError> {
    let readiness =
        transition
            .payload_readiness
            .as_ref()
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} unavailable placement is not payload-write ready",
                    transition.pg_id.get()
                ),
            })?;
    validate_unavailable_pg_payload_readiness(snapshot, transition, readiness)?;
    for destination in &readiness.destinations {
        let node = snapshot
            .node(destination.node_id)
            .ok_or(ControlPlaneError::UnknownNode {
                node_id: destination.node_id.as_u32(),
            })?;
        if node.node_incarnation != destination.node_incarnation
            || node.endpoint != destination.endpoint
            || node.lease_deadline_ms != Some(destination.lease_deadline_ms)
            || !node.can_serve_primary(snapshot.cluster_epoch, activation_at_ms)
            || destination.lease_deadline_ms <= activation_at_ms
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} payload-readiness destination {} changed before activation",
                    transition.pg_id.get(),
                    destination.node_id.as_u32()
                ),
            });
        }
    }
    Ok(())
}

fn validate_unavailable_pg_transition_invariant(
    snapshot: &ClusterControlSnapshot,
    transition: &UnavailablePgPlacementTransition,
    transition_successors: &BTreeMap<(PgId, ClusterEpoch), ClusterEpoch>,
) -> Result<(), String> {
    let topology = snapshot.initial_topology.as_ref().ok_or_else(|| {
        format!(
            "unavailable PG transition {} requires certified topology",
            transition.pg_id.get()
        )
    })?;
    if transition.topology_generation != topology.topology_generation()
        || transition.topology_digest != *topology.topology_digest()
    {
        return Err(format!(
            "unavailable PG transition {} does not match certified topology",
            transition.pg_id.get()
        ));
    }
    if next_epoch(transition.source_epoch).map_err(|error| error.to_string())?
        != transition.transition_epoch
        || transition.transition_epoch > snapshot.cluster_epoch
        || transition.destination_epoch.is_some_and(|epoch| {
            epoch <= transition.transition_epoch || epoch > snapshot.cluster_epoch
        })
        || transition.destination_epoch.is_some() != transition.destination_route.is_some()
    {
        return Err(format!(
            "unavailable PG transition {} has invalid epoch ordering",
            transition.pg_id.get()
        ));
    }
    let expected_grace = transition
        .unavailable_node
        .observed_at_ms
        .checked_add(
            topology
                .placement_policy()
                .unavailable_replacement_grace_ms(),
        )
        .ok_or_else(|| "unavailable PG transition grace cutoff overflows".to_string())?;
    if transition.unavailable_node.lease_deadline_ms == 0
        || transition.unavailable_node.lease_deadline_ms
            > transition.unavailable_node.observed_at_ms
        || transition.grace_cutoff_ms != expected_grace
    {
        return Err(format!(
            "unavailable PG transition {} has invalid lease or grace evidence",
            transition.pg_id.get()
        ));
    }
    topology
        .placement_policy()
        .validate_acting_set(&transition.source_acting_set)?;
    topology
        .placement_policy()
        .validate_acting_set(&transition.destination_acting_set)?;
    if !transition
        .source_acting_set
        .contains(&transition.source_node_id)
        || transition.source_node_id == transition.unavailable_node.node_id
    {
        return Err(format!(
            "unavailable PG transition {} has an invalid metadata source",
            transition.pg_id.get()
        ));
    }
    let authorization = &transition.begin_authorization;
    if authorization.begin_at_ms < transition.grace_cutoff_ms
        || authorization.unavailable_node != transition.unavailable_node
        || authorization.source_route.pg_id != transition.pg_id
        || authorization.source_route.acting_set != transition.source_acting_set
        || authorization.source_node_id != transition.source_node_id
        || authorization.source_node_incarnation == 0
        || authorization.source_endpoint.is_empty()
        || authorization.source_lease_deadline_ms <= authorization.begin_at_ms
        || authorization.source_observed_at_ms > authorization.begin_at_ms
        || authorization.replacement_node_incarnation == 0
        || authorization.replacement_endpoint.is_empty()
        || authorization.replacement_lease_deadline_ms <= authorization.begin_at_ms
        || authorization
            .source_metadata_floor_epoch
            .is_some_and(|epoch| epoch > transition.source_epoch)
    {
        return Err(format!(
            "unavailable PG transition {} has invalid begin authorization",
            transition.pg_id.get()
        ));
    }
    match authorization.source_route.state {
        PgState::Active => {
            if authorization.source_route.active_primary != Some(authorization.source_node_id)
                || authorization
                    .source_route
                    .peering_metadata_proof_floor
                    .is_some()
                || authorization
                    .source_route
                    .peering_metadata_transfer
                    .is_some()
                || !metadata_proof_satisfies_active_floor(
                    authorization.source_metadata_floor,
                    authorization.source_metadata_proof,
                )
            {
                return Err(format!(
                    "unavailable PG transition {} has invalid Active source authorization",
                    transition.pg_id.get()
                ));
            }
        }
        PgState::Peering => {
            if authorization.source_route.active_primary.is_some()
                || authorization.source_route.peering_metadata_proof_floor
                    != Some(authorization.source_metadata_floor)
                || authorization
                    .source_route
                    .peering_metadata_proof_floor_epoch
                    != authorization.source_metadata_floor_epoch
                || authorization
                    .source_route
                    .peering_metadata_proof_floor_imported
                    != authorization.source_metadata_floor_imported
            {
                return Err(format!(
                    "unavailable PG transition {} has invalid Peering source authorization",
                    transition.pg_id.get()
                ));
            }
            validate_peering_metadata_proof_state(
                transition.pg_id,
                Some(authorization.source_metadata_floor),
                authorization.source_metadata_floor_epoch,
                authorization.source_metadata_floor_imported,
                authorization.source_route.peering_metadata_transfer,
                transition.source_epoch,
            )
            .map_err(|error| {
                format!(
                    "unavailable PG transition {} has invalid source proof authorization: {error}",
                    transition.pg_id.get()
                )
            })?;
            let expected_source_proof = authorization
                .source_route
                .peering_metadata_transfer
                .map(PgMetadataTransferProof::metadata_proof)
                .unwrap_or(authorization.source_metadata_floor);
            if authorization.source_metadata_proof != expected_source_proof {
                return Err(format!(
                    "unavailable PG transition {} source proof is not certified by its route",
                    transition.pg_id.get()
                ));
            }
        }
        state => {
            return Err(format!(
                "unavailable PG transition {} has invalid source route state {state:?}",
                transition.pg_id.get()
            ));
        }
    }
    if let Some(destination_route) = &transition.destination_route {
        if destination_route.pg_id != transition.pg_id
            || destination_route.state != PgState::Peering
            || destination_route.acting_set != transition.destination_acting_set
            || destination_route.active_primary.is_some()
            || destination_route
                .peering_metadata_transfer
                .is_none_or(|transfer| {
                    transfer.source_metadata_proof() != authorization.source_metadata_proof
                })
            || destination_route
                .peering_metadata_transfer_source_route_epoch
                .is_none_or(|source_route_epoch| {
                    source_route_epoch < transition.transition_epoch
                        || transition
                            .destination_epoch
                            .is_none_or(|destination_epoch| source_route_epoch >= destination_epoch)
                })
            || destination_route.peering_metadata_transfer_source_node_id
                != Some(transition.source_node_id)
        {
            return Err(format!(
                "unavailable PG transition {} has invalid destination-route evidence: transition_epoch={}, source_node={}, destination={:?}, route={destination_route:?}",
                transition.pg_id.get(),
                transition.transition_epoch,
                transition.source_node_id.as_u32(),
                transition.destination_acting_set,
            ));
        }
    }
    let changed = transition
        .source_acting_set
        .iter()
        .copied()
        .zip(transition.destination_acting_set.iter().copied())
        .filter(|(source, destination)| source != destination)
        .collect::<Vec<_>>();
    if changed.len() != 1
        || changed[0].0 != transition.unavailable_node.node_id
        || changed[0].1 != authorization.replacement_node_id
        || transition
            .destination_acting_set
            .contains(&transition.unavailable_node.node_id)
    {
        return Err(format!(
            "unavailable PG transition {} is not an exact one-node substitution",
            transition.pg_id.get()
        ));
    }
    if let Some(readiness) = &transition.payload_readiness {
        if readiness.pg_id != transition.pg_id
            || readiness.transition_epoch != transition.transition_epoch
            || Some(readiness.destination_epoch) != transition.destination_epoch
            || readiness.topology_generation != transition.topology_generation
            || readiness.topology_digest != transition.topology_digest
            || readiness.destinations.len() != transition.destination_acting_set.len()
        {
            return Err(format!(
                "unavailable PG transition {} has mismatched payload readiness",
                transition.pg_id.get()
            ));
        }
        for (expected, destination) in transition
            .destination_acting_set
            .iter()
            .copied()
            .zip(&readiness.destinations)
        {
            if destination.node_id != expected
                || destination.node_incarnation == 0
                || destination.endpoint.is_empty()
                || destination.lease_deadline_ms <= readiness.ready_at_ms
            {
                return Err(format!(
                    "unavailable PG transition {} has invalid payload destination readiness",
                    transition.pg_id.get()
                ));
            }
        }
    }
    validate_unavailable_pg_transition_batch_receipt(
        transition,
        &transition.begin_batch_receipt,
        UnavailablePgTransitionBatchStage::Begin,
    )?;
    if let Some(authorization) = &transition.staging_authorization {
        let staging_authorization_boundary = transition.destination_epoch.or_else(|| {
            transition_successors
                .get(&(transition.pg_id, transition.transition_epoch))
                .copied()
        });
        if authorization.staging_generation != transition.transition_epoch.get()
            || authorization.artifact_target_epoch <= transition.transition_epoch
            || authorization.artifact_length == 0
            || authorization.artifact_length
                > crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES
            || authorization.artifact_format_version
                != crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION
            || authorization.batch_receipt.source_epoch > snapshot.cluster_epoch
            || staging_authorization_boundary
                .is_some_and(|boundary| authorization.batch_receipt.source_epoch >= boundary)
        {
            return Err(format!(
                "unavailable PG transition {} has invalid staging authorization",
                transition.pg_id.get()
            ));
        }
        validate_unavailable_pg_transition_batch_receipt(
            transition,
            &authorization.batch_receipt,
            UnavailablePgTransitionBatchStage::StagingAuthorization,
        )?;
    }
    if transition.destination_install.is_some() && transition.destination_epoch.is_none() {
        return Err(format!(
            "unavailable PG transition {} has destination install evidence without an installed route",
            transition.pg_id.get()
        ));
    }
    if let Some(install) = &transition.destination_install {
        let destination_route = transition.destination_route.as_ref().ok_or_else(|| {
            format!(
                "unavailable PG transition {} has destination install evidence without a route",
                transition.pg_id.get()
            )
        })?;
        if destination_route.pg_id != transition.pg_id
            || destination_route.state != PgState::Peering
            || destination_route.acting_set != transition.destination_acting_set
            || destination_route.active_primary.is_some()
            || destination_route.peering_metadata_proof_floor
                != Some(install.transfer.metadata_proof())
            || destination_route.peering_metadata_proof_floor_epoch
                != Some(install.batch_receipt.source_epoch)
            || !destination_route.peering_metadata_proof_floor_imported
            || destination_route.peering_metadata_transfer != Some(install.transfer)
            || destination_route.peering_metadata_transfer_source_route_epoch
                != Some(install.batch_receipt.source_epoch)
            || destination_route.peering_metadata_transfer_source_node_id
                != Some(transition.source_node_id)
            || install.publications.len() != transition.destination_acting_set.len()
            || install
                .publications
                .windows(2)
                .any(|pair| pair[0].node_id >= pair[1].node_id)
            || install
                .publications
                .iter()
                .map(|publication| publication.node_id)
                .collect::<BTreeSet<_>>()
                != transition
                    .destination_acting_set
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
        {
            return Err(format!(
                "unavailable PG transition {} has invalid destination install evidence",
                transition.pg_id.get()
            ));
        }
        let authorization = transition.staging_authorization.as_ref().ok_or_else(|| {
            format!(
                "unavailable PG transition {} has destination install evidence without staging authorization",
                transition.pg_id.get()
            )
        })?;
        for publication in &install.publications {
            let key = MetadataTransferStagingEvidenceKey {
                pg_id: transition.pg_id,
                staging_generation: authorization.staging_generation,
                actor_node_id: publication.node_id,
                actor_node_incarnation: publication.node_incarnation,
                kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                target_epoch: Some(install.batch_receipt.target_epoch),
            };
            if let Some(evidence) = snapshot.metadata_transfer_staging_evidence.get(&key) {
                if checksum::sha256::digest(evidence) != publication.evidence_digest {
                    return Err(format!(
                        "unavailable PG transition {} destination install publication digest is invalid",
                        transition.pg_id.get()
                    ));
                }
                let decoded = crate::pg_store::decode_staging_evidence(evidence)
                    .map_err(|error| error.to_string())?;
                if decoded.actor().endpoint() != publication.endpoint
                    || decoded.target_epoch() != Some(install.batch_receipt.target_epoch)
                    || decoded.transfer() != Some(install.transfer)
                {
                    return Err(format!(
                        "unavailable PG transition {} destination install publication semantics are invalid",
                        transition.pg_id.get()
                    ));
                }
            } else {
                let finalized_floor = snapshot
                    .metadata_transfer_staging_finalized_floors
                    .get(&(transition.pg_id, authorization.staging_generation))
                    .filter(|floor| floor.transition.matches_transition(transition));
                let committed = finalized_floor
                    .and_then(|floor| floor.checkpoint_bindings.get(&key))
                    .is_some_and(|binding| {
                        snapshot
                            .validate_metadata_transfer_staging_finalized_checkpoint_binding(
                                &key,
                                publication.evidence_digest,
                                &publication.endpoint,
                                binding,
                            )
                            .is_ok()
                    });
                if !committed {
                    return Err(format!(
                        "unavailable PG transition {} destination install references missing publication evidence",
                        transition.pg_id.get()
                    ));
                }
            }
        }
        validate_unavailable_pg_transition_batch_receipt(
            transition,
            &install.batch_receipt,
            UnavailablePgTransitionBatchStage::DestinationInstall,
        )?;
    }
    if transition.completion.is_some() != transition.completion_batch_receipt.is_some() {
        return Err(format!(
            "unavailable PG transition {} has incomplete completion receipt evidence",
            transition.pg_id.get()
        ));
    }
    if let Some(receipt) = &transition.completion_batch_receipt {
        validate_unavailable_pg_transition_batch_receipt(
            transition,
            receipt,
            UnavailablePgTransitionBatchStage::Completion,
        )?;
        validate_unavailable_pg_transition_completion_evidence(snapshot, transition, receipt)?;
    }
    Ok(())
}

fn validate_unavailable_pg_transition_completion_evidence(
    snapshot: &ClusterControlSnapshot,
    transition: &UnavailablePgPlacementTransition,
    receipt: &UnavailablePgTransitionBatchReceipt,
) -> Result<(), String> {
    let target_epoch = next_epoch(receipt.source_epoch).map_err(|error| error.to_string())?;
    if receipt.target_epoch != target_epoch || receipt.target_epoch > snapshot.cluster_epoch {
        return Err(format!(
            "unavailable PG transition {} has invalid completion activation epochs",
            transition.pg_id.get()
        ));
    }
    let readiness = transition.payload_readiness.as_ref().ok_or_else(|| {
        format!(
            "unavailable PG transition {} has a completion receipt without payload readiness",
            transition.pg_id.get()
        )
    })?;
    let completion = transition.completion.as_ref().ok_or_else(|| {
        format!(
            "unavailable PG transition {} has a completion receipt without completion evidence",
            transition.pg_id.get()
        )
    })?;
    let destination_epoch = transition.destination_epoch.ok_or_else(|| {
        format!(
            "unavailable PG transition {} completed without a destination epoch",
            transition.pg_id.get()
        )
    })?;
    let destination_route = transition.destination_route.as_ref().ok_or_else(|| {
        format!(
            "unavailable PG transition {} completed without destination-route evidence",
            transition.pg_id.get()
        )
    })?;
    let destination_proof = destination_route
        .peering_metadata_transfer
        .ok_or_else(|| {
            format!(
                "unavailable PG transition {} completion has no imported metadata proof",
                transition.pg_id.get()
            )
        })?
        .metadata_proof();
    let primary_readiness = readiness
        .destinations
        .iter()
        .find(|destination| destination.node_id == completion.primary);
    if receipt.source_epoch < destination_epoch
        || completion.pg_id != transition.pg_id
        || completion.active_metadata_proof_epoch != receipt.source_epoch
        || completion.active_metadata_proof != destination_proof
        || primary_readiness
            .is_none_or(|destination| destination.node_incarnation != completion.node_incarnation)
    {
        return Err(format!(
            "unavailable PG transition {} has invalid completion evidence",
            transition.pg_id.get()
        ));
    }
    let source_route = snapshot
        .reconstructed_pg_route_at_epoch(transition.pg_id, receipt.source_epoch)
        .map_err(|error| {
            format!(
                "unavailable PG transition {} completion source route is unavailable: {error}",
                transition.pg_id.get()
            )
        })?;
    if source_route.state() != PgState::Peering
        || source_route.acting_set() != transition.destination_acting_set
        || source_route.peering_metadata_transfer() != destination_route.peering_metadata_transfer
    {
        return Err(format!(
            "unavailable PG transition {} completion source route does not match its destination",
            transition.pg_id.get()
        ));
    }
    let target_route = snapshot
        .reconstructed_pg_route_at_epoch(transition.pg_id, receipt.target_epoch)
        .map_err(|error| {
            format!(
                "unavailable PG transition {} completion target route is unavailable: {error}",
                transition.pg_id.get()
            )
        })?;
    if target_route.state() != PgState::Active
        || target_route.acting_set() != transition.destination_acting_set
        || target_route.primary_node_id() != completion.primary
    {
        return Err(format!(
            "unavailable PG transition {} completion target route does not match its activation",
            transition.pg_id.get()
        ));
    }
    Ok(())
}

fn validate_unavailable_pg_transition_batch_receipt(
    transition: &UnavailablePgPlacementTransition,
    receipt: &UnavailablePgTransitionBatchReceipt,
    expected_stage: UnavailablePgTransitionBatchStage,
) -> Result<(), String> {
    if receipt.identity.member_pg_ids.is_empty()
        || receipt.identity.member_pg_ids.len() > MAX_UNAVAILABLE_PG_TRANSITION_BATCH
        || receipt.identity.stage != expected_stage
        || receipt
            .identity
            .member_pg_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || receipt
            .identity
            .member_pg_ids
            .binary_search(&transition.pg_id)
            .is_err()
        || match expected_stage {
            UnavailablePgTransitionBatchStage::StagingAuthorization => {
                receipt.source_epoch != receipt.target_epoch
                    || receipt.source_epoch < transition.transition_epoch
            }
            UnavailablePgTransitionBatchStage::Begin
            | UnavailablePgTransitionBatchStage::DestinationInstall
            | UnavailablePgTransitionBatchStage::Completion => {
                receipt.source_epoch >= receipt.target_epoch
            }
        }
        || (expected_stage == UnavailablePgTransitionBatchStage::Begin
            && (receipt.source_epoch != transition.source_epoch
                || receipt.target_epoch != transition.transition_epoch))
        || (expected_stage == UnavailablePgTransitionBatchStage::DestinationInstall
            && (transition.destination_epoch != Some(receipt.target_epoch)
                || next_epoch(receipt.source_epoch).ok() != Some(receipt.target_epoch)))
    {
        return Err(format!(
            "unavailable PG transition {} has an invalid {} batch receipt",
            transition.pg_id.get(),
            expected_stage.as_str()
        ));
    }
    Ok(())
}

fn validate_unavailable_pg_transition_batch_receipts(
    snapshot: &ClusterControlSnapshot,
) -> Result<(), String> {
    let transitions = snapshot
        .retained_unavailable_pg_placement_transitions
        .values()
        .chain(snapshot.unavailable_pg_placement_transitions.values())
        .collect::<Vec<_>>();
    let mut retained_members = BTreeMap::<
        UnavailablePgTransitionBatchReceipt,
        BTreeMap<PgId, Vec<&UnavailablePgPlacementTransition>>,
    >::new();
    for transition in &transitions {
        for receipt in std::iter::once(&transition.begin_batch_receipt)
            .chain(
                transition
                    .staging_authorization
                    .as_ref()
                    .map(|authorization| &authorization.batch_receipt),
            )
            .chain(
                transition
                    .destination_install
                    .as_ref()
                    .map(|install| &install.batch_receipt),
            )
            .chain(transition.completion_batch_receipt.as_ref())
        {
            retained_members
                .entry(receipt.clone())
                .or_default()
                .entry(transition.pg_id)
                .or_default()
                .push(transition);
        }
    }
    for (receipt, actual_members) in retained_members {
        if actual_members.len() != receipt.identity.member_pg_ids.len()
            || actual_members
                .iter()
                .any(|(_, transitions)| transitions.len() != 1)
            || !actual_members
                .keys()
                .copied()
                .eq(receipt.identity.member_pg_ids.iter().copied())
        {
            return Err(format!(
                "unavailable PG {} batch receipt retained members do not match its canonical vector",
                receipt.identity.stage.as_str()
            ));
        }
        let member_transitions = receipt
            .identity
            .member_pg_ids
            .iter()
            .map(|pg_id| actual_members[pg_id][0])
            .collect::<Vec<_>>();
        let expected_identity = match receipt.identity.stage {
            UnavailablePgTransitionBatchStage::Begin => {
                let begin_at_ms = member_transitions[0].begin_authorization.begin_at_ms;
                if member_transitions
                    .iter()
                    .any(|transition| transition.begin_authorization.begin_at_ms != begin_at_ms)
                {
                    return Err(
                        "unavailable PG begin batch members have different commit times".into(),
                    );
                }
                let requests = member_transitions
                    .iter()
                    .map(|transition| {
                        unavailable_pg_transition_begin_request_from_durable(transition)
                    })
                    .collect::<Vec<_>>();
                unavailable_pg_transition_begin_batch_identity(
                    &requests,
                    receipt.target_epoch,
                    begin_at_ms,
                )
            }
            UnavailablePgTransitionBatchStage::StagingAuthorization => {
                let requests = member_transitions
                    .iter()
                    .map(|transition| {
                        unavailable_pg_staging_authorization_request_from_durable(transition)
                            .ok_or_else(|| {
                                format!(
                                    "unavailable PG transition {} has a staging receipt without authorization evidence",
                                    transition.pg_id.get()
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                unavailable_pg_staging_authorization_batch_identity(&requests)
            }
            UnavailablePgTransitionBatchStage::DestinationInstall => {
                let requests = member_transitions
                    .iter()
                    .map(|transition| {
                        unavailable_pg_destination_install_request_from_durable(transition)
                            .ok_or_else(|| {
                                format!(
                                    "unavailable PG transition {} has an install receipt without install evidence",
                                    transition.pg_id.get()
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                unavailable_pg_destination_install_batch_identity(&requests, receipt.target_epoch)
            }
            UnavailablePgTransitionBatchStage::Completion => {
                let requests_and_times = member_transitions
                    .iter()
                    .map(|transition| {
                        unavailable_pg_transition_completion_request_from_durable(transition)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let ready_at_ms = requests_and_times[0].1;
                if requests_and_times
                    .iter()
                    .any(|(_, member_ready_at_ms)| *member_ready_at_ms != ready_at_ms)
                {
                    return Err(
                        "unavailable PG completion batch members have different commit times"
                            .into(),
                    );
                }
                let requests = requests_and_times
                    .into_iter()
                    .map(|(request, _)| request)
                    .collect::<Vec<_>>();
                unavailable_pg_transition_completion_batch_identity(&requests, ready_at_ms)
            }
        };
        if receipt.identity != expected_identity {
            return Err(format!(
                "unavailable PG {} batch receipt digest does not match its durable member evidence",
                receipt.identity.stage.as_str()
            ));
        }
    }
    Ok(())
}

fn validate_unavailable_pg_transition_lineages(
    snapshot: &ClusterControlSnapshot,
) -> Result<BTreeMap<(PgId, ClusterEpoch), ClusterEpoch>, String> {
    let mut lineage_tips = BTreeMap::new();
    let mut successors = BTreeMap::new();
    for transition in snapshot
        .retained_unavailable_pg_placement_transitions
        .values()
    {
        let expected = lineage_tips.get(&transition.pg_id).copied();
        if transition.predecessor_transition_epoch != expected {
            return Err(format!(
                "retained unavailable PG transition {} does not consume its predecessor tip",
                transition.transition_epoch.get()
            ));
        }
        if let Some(predecessor) = transition.predecessor_transition_epoch {
            successors.insert((transition.pg_id, predecessor), transition.transition_epoch);
        }
        lineage_tips.insert(transition.pg_id, transition.transition_epoch);
    }
    for transition in snapshot.unavailable_pg_placement_transitions.values() {
        let expected = lineage_tips.get(&transition.pg_id).copied();
        if transition.predecessor_transition_epoch != expected {
            return Err(format!(
                "active unavailable PG transition {} does not consume its predecessor tip",
                transition.transition_epoch.get()
            ));
        }
        if let Some(predecessor) = transition.predecessor_transition_epoch {
            successors.insert((transition.pg_id, predecessor), transition.transition_epoch);
        }
    }
    Ok(successors)
}

fn unavailable_transition_source_route_acting_set(
    source_acting_set: &[NodeId],
    source_node_id: NodeId,
) -> Vec<NodeId> {
    std::iter::once(source_node_id)
        .chain(
            source_acting_set
                .iter()
                .copied()
                .filter(|node_id| *node_id != source_node_id),
        )
        .collect()
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
    staging_authorizations:
        Vec<crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation>,
}

/// Authorization state copied only from an authority-issued runtime map. The
/// private representation prevents decoded storage RPC bytes from being
/// upgraded directly to a committed staging capability.
#[derive(Debug, Clone, Default)]
pub(crate) struct AuthorityPublishedUnavailablePgStagingAuthorizations {
    presentations: Vec<crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation>,
}

#[derive(Debug)]
pub(crate) enum StagingAuthorizationVerificationError {
    NotObserved,
    Invalid(ControlPlaneError),
}

impl AuthorityPublishedUnavailablePgStagingAuthorizations {
    pub(crate) fn verify(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        observed_cluster_epoch: ClusterEpoch,
        presented: &crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation,
    ) -> Result<
        crate::control_plane_command::CommittedUnavailablePgStagingAuthorization,
        StagingAuthorizationVerificationError,
    > {
        let Some(authority_published) = self
            .presentations
            .iter()
            .find(|candidate| *candidate == presented)
        else {
            if presented.committed_epoch() < observed_cluster_epoch {
                return Err(StagingAuthorizationVerificationError::Invalid(
                    ControlPlaneError::rpc_protocol(format!(
                        "staging authorization committed at epoch {} is absent from newer runtime-map epoch {}",
                        presented.committed_epoch().get(),
                        observed_cluster_epoch.get()
                    )),
                ));
            }
            return Err(StagingAuthorizationVerificationError::NotObserved);
        };
        if !authority_published.authorizes_destination_for_pg(node_id, pg_id) {
            return Err(StagingAuthorizationVerificationError::Invalid(
                ControlPlaneError::rpc_protocol(format!(
                    "node {} is not a destination of PG {} in the committed staging authorization batch",
                    node_id.as_u32(),
                    pg_id.get()
                )),
            ));
        }
        Ok(crate::control_plane_command::CommittedUnavailablePgStagingAuthorization::from_authority_published(
            authority_published.clone(),
            node_id,
            pg_id,
            authority_published_staging_authorization_seal(),
        ))
    }
}

const RUNTIME_MAP_CONTENT_DIGEST_LEN: usize = 32;
const RUNTIME_MAP_CONTENT_DIGEST_DOMAIN: &[u8] = b"argmin/runtime-map-content/v4";
const RUNTIME_MAP_CURRENT_STATE_DIGEST_DOMAIN: &[u8] = b"argmin/runtime-map-current-state/v4";

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

    pub(crate) fn authority_published_staging_authorizations(
        &self,
    ) -> AuthorityPublishedUnavailablePgStagingAuthorizations {
        AuthorityPublishedUnavailablePgStagingAuthorizations {
            presentations: self.staging_authorizations.clone(),
        }
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
            staging_authorizations: Vec::new(),
        })
    }

    /// Builds the one-PG route used to inspect a fenced metadata-transfer source.
    ///
    /// Historical maps are normally non-serving. This operation preserves the
    /// freshness proof from the current authority read only when the current
    /// Peering route still exactly matches the previously fenced route and,
    /// for a transfer in progress, names the exact historical source route.
    /// The resulting map cannot authorize an unrelated PG or source node.
    pub(crate) fn metadata_transfer_source_runtime_map(
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
            staging_authorizations: Vec::new(),
        })
    }

    /// Builds the one-PG current route used to install a metadata transfer.
    ///
    /// The route must be observed through a serving authority read and must
    /// still carry the exact transfer authorization expected by the importer.
    pub(crate) fn metadata_transfer_destination_runtime_map(
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
            staging_authorizations: Vec::new(),
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
    digest_staging_authorization_presentations(&mut hasher, &snapshot.staging_authorizations);
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
    let staging_authorizations = snapshot
        .committed_staging_authorization_presentations()
        .expect("validated control-plane state has valid staging authorization receipts");
    digest_staging_authorization_presentations(&mut hasher, &staging_authorizations);
    let checksum = hasher.finalize();
    RuntimeMapContentDigest::from_bytes(
        checksum
            .bytes()
            .try_into()
            .expect("SHA-256 current runtime-map digest must contain 32 bytes"),
    )
}

fn digest_staging_authorization_presentations(
    hasher: &mut ChecksumHasher,
    authorizations: &[crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation],
) {
    digest_len(hasher, authorizations.len());
    for authorization in authorizations {
        digest_u64(hasher, authorization.committed_epoch().get());
        digest_bytes(hasher, &authorization.batch_members_digest());
        digest_bytes(
            hasher,
            &authorization
                .encode_command()
                .expect("authority-published staging authorization is canonical"),
        );
    }
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
    digest_u8(hasher, proof.applied_log_hash.encoding_version());
    digest_u64(hasher, proof.applied_log_hash.value());
    digest_u8(hasher, proof.state_digest.encoding_version());
    digest_u64(hasher, proof.state_digest.value());
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

fn digest_u16(hasher: &mut ChecksumHasher, value: u16) {
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
    pub(crate) pg_id: PgId,
    pub(crate) state: PgState,
    pub(crate) acting_set: Vec<NodeId>,
    pub(crate) active_primary: Option<NodeId>,
    pub(crate) peering_metadata_proof_floor: Option<PgMetadataProof>,
    pub(crate) peering_metadata_proof_floor_epoch: Option<ClusterEpoch>,
    pub(crate) peering_metadata_proof_floor_imported: bool,
    pub(crate) peering_metadata_transfer: Option<PgMetadataTransferProof>,
    pub(crate) peering_metadata_transfer_source_route_epoch: Option<ClusterEpoch>,
    pub(crate) peering_metadata_transfer_source_node_id: Option<NodeId>,
}

impl From<&PgControlRecord> for HistoricalPgRouteRecord {
    fn from(record: &PgControlRecord) -> Self {
        Self {
            pg_id: record.pg_id,
            state: record.state,
            acting_set: record.acting_set.clone(),
            active_primary: record.active_primary,
            peering_metadata_proof_floor: record.peering_metadata_proof_floor,
            peering_metadata_proof_floor_epoch: record.peering_metadata_proof_floor_epoch,
            peering_metadata_proof_floor_imported: record.peering_metadata_proof_floor_imported,
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

pub(crate) fn validate_peering_metadata_proof_state(
    pg_id: PgId,
    proof_floor: Option<PgMetadataProof>,
    proof_floor_epoch: Option<ClusterEpoch>,
    proof_floor_imported: bool,
    transfer: Option<PgMetadataTransferProof>,
    route_epoch: ClusterEpoch,
) -> Result<(), String> {
    if proof_floor_epoch.is_some() && proof_floor.is_none() {
        return Err(format!(
            "peering PG {} has a floor epoch without a proof floor",
            pg_id.get()
        ));
    }
    if proof_floor_imported && proof_floor_epoch.is_none() {
        return Err(format!(
            "peering PG {} has imported floor provenance without a floor epoch",
            pg_id.get()
        ));
    }
    if proof_floor_epoch.is_some_and(|epoch| epoch > route_epoch) {
        return Err(format!(
            "peering PG {} has a proof floor epoch newer than its route",
            pg_id.get()
        ));
    }
    if transfer.is_some() && proof_floor.is_none() {
        return Err(format!(
            "peering PG {} has a transfer marker without a proof floor",
            pg_id.get()
        ));
    }
    if let (Some(floor), Some(transfer)) = (proof_floor, transfer) {
        if !metadata_proof_satisfies_active_floor(floor, transfer.metadata_proof()) {
            return Err(format!(
                "peering PG {} metadata transfer proof is below the proof floor",
                pg_id.get()
            ));
        }
    }
    Ok(())
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
    if record.state == PgState::Peering {
        validate_peering_metadata_proof_state(
            record.pg_id,
            record.peering_metadata_proof_floor,
            record.peering_metadata_proof_floor_epoch,
            record.peering_metadata_proof_floor_imported,
            record.peering_metadata_transfer,
            cluster_epoch,
        )?;
    } else if record.peering_metadata_proof_floor.is_some()
        || record.peering_metadata_proof_floor_epoch.is_some()
        || record.peering_metadata_proof_floor_imported
    {
        return Err(format!(
            "non-peering PG {} carries peering metadata proof state",
            record.pg_id.get()
        ));
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
    pub(crate) applied_log_index: u64,
    pub(crate) applied_log_hash: MetadataCommandLogHash,
    pub(crate) state_digest: CanonicalStateDigest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MetadataCommandLogHash {
    encoding_version: u8,
    value: u64,
}

impl MetadataCommandLogHash {
    const CURRENT_ENCODING_VERSION: u8 = 1;

    pub(crate) const fn from_hash_owner(
        value: u64,
        _issuer: crate::metadata_command::MetadataCommandLogHashIssuer,
    ) -> Self {
        Self {
            encoding_version: Self::CURRENT_ENCODING_VERSION,
            value,
        }
    }

    pub(crate) const fn from_storage(
        value: u64,
        _issuer: crate::pg_store::MetadataProofStorageIssuer,
    ) -> Self {
        Self {
            encoding_version: Self::CURRENT_ENCODING_VERSION,
            value,
        }
    }

    pub(crate) const fn genesis() -> Self {
        Self {
            encoding_version: Self::CURRENT_ENCODING_VERSION,
            value: 0,
        }
    }

    pub(crate) const fn from_encoded_parts(
        encoding_version: u8,
        value: u64,
    ) -> Result<Self, MetadataProofCarrierVersionError> {
        if encoding_version == Self::CURRENT_ENCODING_VERSION {
            Ok(Self {
                encoding_version,
                value,
            })
        } else {
            Err(MetadataProofCarrierVersionError::UnsupportedLogHash {
                actual: encoding_version,
            })
        }
    }

    pub(crate) const fn encoding_version(self) -> u8 {
        self.encoding_version
    }

    pub(crate) const fn value(self) -> u64 {
        self.value
    }

    #[cfg(test)]
    pub(crate) const fn for_test(value: u64) -> Self {
        Self {
            encoding_version: Self::CURRENT_ENCODING_VERSION,
            value,
        }
    }
}

#[cfg(test)]
impl std::ops::Add<u64> for MetadataCommandLogHash {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self {
            encoding_version: Self::CURRENT_ENCODING_VERSION,
            value: self.value + rhs,
        }
    }
}

#[cfg(test)]
impl PartialEq<u64> for MetadataCommandLogHash {
    fn eq(&self, other: &u64) -> bool {
        self.value == *other
    }
}

#[cfg(test)]
impl PartialEq<MetadataCommandLogHash> for u64 {
    fn eq(&self, other: &MetadataCommandLogHash) -> bool {
        *self == other.value
    }
}

#[cfg(test)]
pub(crate) trait IntoTestMetadataCommandLogHash {
    fn into_test_metadata_command_log_hash(self) -> MetadataCommandLogHash;
}

#[cfg(test)]
impl IntoTestMetadataCommandLogHash for u64 {
    fn into_test_metadata_command_log_hash(self) -> MetadataCommandLogHash {
        MetadataCommandLogHash {
            encoding_version: MetadataCommandLogHash::CURRENT_ENCODING_VERSION,
            value: self,
        }
    }
}

#[cfg(test)]
impl IntoTestMetadataCommandLogHash for MetadataCommandLogHash {
    fn into_test_metadata_command_log_hash(self) -> MetadataCommandLogHash {
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CanonicalStateDigest {
    encoding_version: u8,
    value: u64,
}

impl CanonicalStateDigest {
    pub(crate) const CURRENT_ENCODING_VERSION: u8 = METADATA_CANONICAL_STATE_ENCODING_VERSION;

    pub(crate) const fn from_storage(
        value: u64,
        _issuer: crate::pg_store::MetadataProofStorageIssuer,
    ) -> Self {
        Self {
            encoding_version: Self::CURRENT_ENCODING_VERSION,
            value,
        }
    }

    pub(crate) const fn genesis() -> Self {
        Self {
            encoding_version: Self::CURRENT_ENCODING_VERSION,
            value: 0,
        }
    }

    pub(crate) const fn from_encoded_parts(
        encoding_version: u8,
        value: u64,
    ) -> Result<Self, MetadataProofCarrierVersionError> {
        if encoding_version == Self::CURRENT_ENCODING_VERSION {
            Ok(Self {
                encoding_version,
                value,
            })
        } else {
            Err(MetadataProofCarrierVersionError::UnsupportedStateDigest {
                actual: encoding_version,
            })
        }
    }

    pub(crate) const fn encoding_version(self) -> u8 {
        self.encoding_version
    }

    pub(crate) const fn value(self) -> u64 {
        self.value
    }

    #[cfg(test)]
    pub(crate) const fn for_test(value: u64) -> Self {
        Self {
            encoding_version: Self::CURRENT_ENCODING_VERSION,
            value,
        }
    }

    #[cfg(test)]
    pub(crate) const fn wrapping_add(self, value: u64) -> Self {
        Self {
            encoding_version: Self::CURRENT_ENCODING_VERSION,
            value: self.value.wrapping_add(value),
        }
    }
}

#[cfg(test)]
impl std::ops::Add<u64> for CanonicalStateDigest {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self {
            encoding_version: Self::CURRENT_ENCODING_VERSION,
            value: self.value + rhs,
        }
    }
}

#[cfg(test)]
impl PartialEq<u64> for CanonicalStateDigest {
    fn eq(&self, other: &u64) -> bool {
        self.value == *other
    }
}

#[cfg(test)]
impl PartialEq<CanonicalStateDigest> for u64 {
    fn eq(&self, other: &CanonicalStateDigest) -> bool {
        *self == other.value
    }
}

#[cfg(test)]
pub(crate) trait IntoTestCanonicalStateDigest {
    fn into_test_canonical_state_digest(self) -> CanonicalStateDigest;
}

#[cfg(test)]
impl IntoTestCanonicalStateDigest for u64 {
    fn into_test_canonical_state_digest(self) -> CanonicalStateDigest {
        CanonicalStateDigest {
            encoding_version: CanonicalStateDigest::CURRENT_ENCODING_VERSION,
            value: self,
        }
    }
}

#[cfg(test)]
impl IntoTestCanonicalStateDigest for CanonicalStateDigest {
    fn into_test_canonical_state_digest(self) -> CanonicalStateDigest {
        self
    }
}

pub(crate) const METADATA_CANONICAL_STATE_ENCODING_VERSION: u8 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum MetadataProofCarrierVersionError {
    #[error("unsupported metadata-command log-hash encoding version {actual}")]
    UnsupportedLogHash { actual: u8 },
    #[error("unsupported canonical-state digest encoding version {actual}")]
    UnsupportedStateDigest { actual: u8 },
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
    pub(crate) fn from_encoded_parts(
        applied_log_index: u64,
        applied_log_hash_encoding_version: u8,
        applied_log_hash: u64,
        state_digest_encoding_version: u8,
        state_digest: u64,
    ) -> Result<Self, MetadataProofCarrierVersionError> {
        Ok(Self::from_carriers(
            applied_log_index,
            MetadataCommandLogHash::from_encoded_parts(
                applied_log_hash_encoding_version,
                applied_log_hash,
            )?,
            CanonicalStateDigest::from_encoded_parts(state_digest_encoding_version, state_digest)?,
        ))
    }

    pub(crate) const fn applied_log_index(self) -> u64 {
        self.applied_log_index
    }

    pub(crate) const fn applied_log_hash(self) -> MetadataCommandLogHash {
        self.applied_log_hash
    }

    pub(crate) const fn state_digest(self) -> CanonicalStateDigest {
        self.state_digest
    }

    pub(crate) const fn from_carriers(
        applied_log_index: u64,
        applied_log_hash: MetadataCommandLogHash,
        state_digest: CanonicalStateDigest,
    ) -> Self {
        Self {
            applied_log_index,
            applied_log_hash,
            state_digest,
        }
    }

    #[cfg(test)]
    pub(crate) fn current(
        applied_log_index: u64,
        applied_log_hash: impl IntoTestMetadataCommandLogHash,
        state_digest: impl IntoTestCanonicalStateDigest,
    ) -> Self {
        Self::from_carriers(
            applied_log_index,
            applied_log_hash.into_test_metadata_command_log_hash(),
            state_digest.into_test_canonical_state_digest(),
        )
    }

    #[must_use]
    pub fn empty() -> Self {
        Self::from_carriers(
            0,
            MetadataCommandLogHash::genesis(),
            CanonicalStateDigest::genesis(),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn for_test(applied_log_index: u64, applied_log_hash: u64, state_digest: u64) -> Self {
        Self::from_carriers(
            applied_log_index,
            MetadataCommandLogHash {
                encoding_version: MetadataCommandLogHash::CURRENT_ENCODING_VERSION,
                value: applied_log_hash,
            },
            CanonicalStateDigest {
                encoding_version: CanonicalStateDigest::CURRENT_ENCODING_VERSION,
                value: state_digest,
            },
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub const fn test_applied_log_index(self) -> u64 {
        self.applied_log_index
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub const fn test_applied_log_hash(self) -> u64 {
        self.applied_log_hash.value()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub const fn test_state_digest(self) -> u64 {
        self.state_digest.value()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHeartbeat {
    pub node_id: NodeId,
    pub node_incarnation: u64,
    pub endpoint: String,
    pub observed_epoch: ClusterEpoch,
    pub requested_lease_duration_ms: u64,
    pub cluster_map_history_route_scan_generation: NonZeroU64,
    pub cluster_map_history_route_references: PgClusterMapHistoryRouteReferences,
    pub pg_observations: Vec<NodePgHeartbeatObservation>,
}

impl NodeHeartbeat {
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_fixture(
        node_id: NodeId,
        node_incarnation: u64,
        endpoint: String,
        observed_epoch: ClusterEpoch,
        requested_lease_duration_ms: u64,
        cluster_map_history_route_references: PgClusterMapHistoryRouteReferences,
        pg_observations: Vec<NodePgHeartbeatObservation>,
    ) -> Self {
        Self {
            node_id,
            node_incarnation,
            endpoint,
            observed_epoch,
            requested_lease_duration_ms,
            cluster_map_history_route_scan_generation: NonZeroU64::MIN,
            cluster_map_history_route_references,
            pg_observations,
        }
    }
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
    unavailable_pg_batch_metrics: Vec<observability::UnavailablePgBatchMetricSample>,
    unavailable_pg_worker_stage_metrics: Vec<observability::UnavailablePgWorkerStageMetricSample>,
    unavailable_pg_worker_queue_metrics: observability::UnavailablePgWorkerQueueMetricSnapshot,
    metadata_transfer_staging_retention_metrics:
        observability::MetadataTransferStagingRetentionMetricSnapshot,
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
    pub fn unavailable_pg_batch_metrics(&self) -> &[observability::UnavailablePgBatchMetricSample] {
        &self.unavailable_pg_batch_metrics
    }

    #[must_use]
    pub fn unavailable_pg_worker_stage_metrics(
        &self,
    ) -> &[observability::UnavailablePgWorkerStageMetricSample] {
        &self.unavailable_pg_worker_stage_metrics
    }

    #[must_use]
    pub fn unavailable_pg_worker_queue_metrics(
        &self,
    ) -> observability::UnavailablePgWorkerQueueMetricSnapshot {
        self.unavailable_pg_worker_queue_metrics
    }

    #[must_use]
    pub fn metadata_transfer_staging_retention_metrics(
        &self,
    ) -> observability::MetadataTransferStagingRetentionMetricSnapshot {
        self.metadata_transfer_staging_retention_metrics
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

    fn begin_unavailable_pg_placement_transition(
        &mut self,
        pg_id: PgId,
        unavailable_node_id: NodeId,
        begin_at_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let _ = (pg_id, unavailable_node_id, begin_at_ms);
        Err(ControlPlaneError::rpc_remote(
            "unavailable PG placement transitions are not supported by this authority".to_owned(),
        ))
    }

    fn complete_unavailable_pg_placement_transition(
        &mut self,
        work: &UnavailablePgReconciliationWork,
        ready_at_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let _ = (work, ready_at_ms);
        Err(ControlPlaneError::rpc_remote(
            "unavailable PG placement completion is not supported by this authority".to_owned(),
        ))
    }

    fn authorize_unavailable_pg_staging_intents_batch(
        &mut self,
        authorizations: &[UnavailablePgStagingIntentAuthorizationRequest],
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let _ = authorizations;
        Err(ControlPlaneError::rpc_remote(
            "unavailable PG staging authorization batches are not supported by this authority"
                .to_owned(),
        ))
    }

    fn install_unavailable_pg_placement_transitions_batch(
        &mut self,
        transitions: &[UnavailablePgTransitionInstallRequest],
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let _ = (transitions, expected_destination_epoch);
        Err(ControlPlaneError::rpc_remote(
            "unavailable PG destination installation batches are not supported by this authority"
                .to_owned(),
        ))
    }

    fn apply_metadata_transfer_staging_evidence_page(
        &mut self,
        operation_payload: Vec<u8>,
        page_digest: [u8; 32],
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let _ = (operation_payload, page_digest);
        Err(ControlPlaneError::rpc_remote(
            "metadata-transfer staging evidence publication is not supported by this authority"
                .to_owned(),
        ))
    }

    fn checkpoint_metadata_transfer_staging_evidence_pages(
        &mut self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        first_generation: u64,
        last_generation: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let _ = (
            actor_node_id,
            actor_node_incarnation,
            first_generation,
            last_generation,
        );
        Err(ControlPlaneError::rpc_remote(
            "metadata-transfer staging evidence checkpointing is not supported by this authority"
                .to_owned(),
        ))
    }

    fn collapse_metadata_transfer_staging_evidence_checkpoint_segment(
        &mut self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        first_generation: u64,
        last_generation: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let _ = (
            actor_node_id,
            actor_node_incarnation,
            first_generation,
            last_generation,
        );
        Err(ControlPlaneError::rpc_remote(
            "metadata-transfer staging checkpoint collapse is not supported by this authority"
                .to_owned(),
        ))
    }

    fn coalesce_metadata_transfer_staging_evidence_checkpoint_anchors(
        &mut self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
        first_generation: u64,
        last_generation: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let _ = (
            actor_node_id,
            actor_node_incarnation,
            first_generation,
            last_generation,
        );
        Err(ControlPlaneError::rpc_remote(
            "metadata-transfer staging checkpoint anchor coalescing is not supported by this authority"
                .to_owned(),
        ))
    }

    fn retire_metadata_transfer_staging_actor_closure(
        &mut self,
        actor_node_id: NodeId,
        actor_node_incarnation: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let _ = (actor_node_id, actor_node_incarnation);
        Err(ControlPlaneError::rpc_remote(
            "metadata-transfer staging actor-closure retirement is not supported by this authority"
                .to_owned(),
        ))
    }

    fn finalize_metadata_transfer_staging_generation(
        &mut self,
        cleanup: FinalizeMetadataTransferStagingGenerationRequest,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let _ = cleanup;
        Err(ControlPlaneError::rpc_remote(
            "metadata-transfer staging cleanup is not supported by this authority".to_owned(),
        ))
    }

    fn fence_pg_for_metadata_transfer(
        &mut self,
        pg_id: PgId,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError>;

    fn fence_pg_for_metadata_transfer_with_source_lease(
        &mut self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError>;

    fn fence_unavailable_pg_transition_with_source_lease(
        &mut self,
        binding: UnavailablePgTransitionMutationBinding,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
        let _ = binding;
        Err(ControlPlaneError::rpc_remote(
            "unavailable PG transition fencing is not supported by this authority".to_owned(),
        ))
    }

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
    let protection = required_cluster_map_history_protection(
        snapshot.pgs.values(),
        snapshot.nodes.values(),
        snapshot
            .unavailable_pg_placement_transitions
            .values()
            .chain(
                snapshot
                    .retained_unavailable_pg_placement_transitions
                    .values(),
            ),
    );
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
        for reference in record.retiring_cluster_map_history_route_references.iter() {
            out.push_str(&format!(
                "node_history_route_retiring={},{},{},{}\n",
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
    for observation in snapshot.unavailable_node_observations.values() {
        out.push_str(&format!(
            "unavailable_node={}\n",
            format_unavailable_node_observation(observation)
        ));
    }
    for transition in snapshot.unavailable_pg_placement_transitions.values() {
        out.push_str(&format!(
            "unavailable_pg_transition={}\n",
            format_unavailable_pg_placement_transition(transition)
        ));
    }
    for transition in snapshot
        .retained_unavailable_pg_placement_transitions
        .values()
    {
        out.push_str(&format!(
            "retained_unavailable_pg_transition={}\n",
            format_unavailable_pg_placement_transition(transition)
        ));
    }
    for record in snapshot.metadata_transfer_staging_evidence_pages.values() {
        out.push_str(&format!(
            "metadata_transfer_staging_evidence_page={},{},{}\n",
            hex_encode(&record.operation_payload),
            hex_encode(&record.page_digest),
            hex_encode(&record.apply_receipt)
        ));
    }
    for segment in snapshot
        .metadata_transfer_staging_evidence_checkpoint_segments
        .values()
    {
        out.push_str(&format!(
            "{METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_STATE_RECORD_PREFIX}{}\n",
            format_metadata_transfer_staging_evidence_checkpoint_segment(segment)
        ));
    }
    for anchor in snapshot
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .values()
    {
        out.push_str(&format!(
            "{METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_ANCHOR_STATE_RECORD_PREFIX}{}\n",
            format_metadata_transfer_staging_evidence_checkpoint_anchor(anchor)
        ));
    }
    for closure in snapshot.metadata_transfer_staging_actor_closures.values() {
        out.push_str(&format!(
            "{METADATA_TRANSFER_STAGING_ACTOR_CLOSURE_STATE_RECORD_PREFIX}{}\n",
            format_metadata_transfer_staging_actor_closure(closure)
        ));
    }
    for closure in snapshot
        .metadata_transfer_staging_retired_actor_closures
        .values()
    {
        out.push_str(&format!(
            "{METADATA_TRANSFER_STAGING_RETIRED_ACTOR_CLOSURE_STATE_RECORD_PREFIX}{}\n",
            format_metadata_transfer_staging_actor_closure(closure)
        ));
    }
    for floor in snapshot.metadata_transfer_staging_finalized_floors.values() {
        out.push_str(&format!(
            "{METADATA_TRANSFER_STAGING_FINALIZED_FLOOR_STATE_RECORD_PREFIX}{}\n",
            format_metadata_transfer_staging_finalized_floor(floor)
        ));
    }
    for evidence in snapshot.metadata_transfer_staging_evidence.values() {
        out.push_str(&format!(
            "metadata_transfer_staging_evidence={}\n",
            hex_encode(evidence)
        ));
    }
    for record in snapshot.pgs.values() {
        out.push_str(&format!("pg={}\n", format_pg_record(record)));
    }
    out
}

fn metadata_transfer_staging_evidence_checkpoint_state_record_len(
    segment: &MetadataTransferStagingEvidenceCheckpointSegment,
) -> usize {
    METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_STATE_RECORD_PREFIX.len()
        + format_metadata_transfer_staging_evidence_checkpoint_segment(segment).len()
        + 1
}

fn format_metadata_transfer_staging_finalized_floor(
    floor: &MetadataTransferStagingFinalizedFloor,
) -> String {
    let publications = floor
        .publications
        .iter()
        .map(|publication| {
            let source = publication.transfer.source_metadata_proof();
            let imported = publication.transfer.metadata_proof();
            format!(
                "{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}",
                publication.node_id.as_u32(),
                publication.node_incarnation,
                hex_encode(publication.endpoint.as_bytes()),
                publication.target_epoch.get(),
                publication.transfer.source_epoch().get(),
                source.applied_log_index,
                source.applied_log_hash.encoding_version(),
                source.applied_log_hash.value(),
                source.state_digest.encoding_version(),
                source.state_digest.value(),
                imported.applied_log_index,
                imported.applied_log_hash.encoding_version(),
                imported.applied_log_hash.value(),
                imported.state_digest.encoding_version(),
                imported.state_digest.value(),
                hex_encode(&publication.evidence_digest)
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    let tombstones = floor
        .tombstones
        .iter()
        .map(|tombstone| {
            format!(
                "{}/{}/{}/{}",
                tombstone.node_id.as_u32(),
                tombstone.node_incarnation,
                hex_encode(tombstone.endpoint.as_bytes()),
                hex_encode(&tombstone.evidence_digest)
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    let checkpoint_bindings = floor
        .checkpoint_bindings
        .iter()
        .map(|(key, binding)| {
            format!(
                "{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}",
                key.pg_id.get(),
                key.staging_generation,
                key.actor_node_id.as_u32(),
                key.actor_node_incarnation,
                key.kind as u8,
                key.target_epoch
                    .map_or_else(|| "-".to_owned(), |epoch| epoch.get().to_string()),
                binding.actor_node_id.as_u32(),
                binding.actor_node_incarnation,
                hex_encode(binding.actor_endpoint.as_bytes()),
                binding.first_generation,
                binding.last_generation,
                binding.page_generation,
                binding.page_sequence,
                hex_encode(&binding.segment_digest),
                binding.actor_closure_candidate.as_ref().map_or_else(
                    || "-".to_owned(),
                    |candidate| {
                        hex_encode(
                            &crate::pg_store::encode_staging_evidence_actor_closure_candidate_bytes(
                                candidate,
                            )
                            .expect("finalized closure candidate is validated before encoding"),
                        )
                    },
                )
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    [
        floor.transition.pg_id().get().to_string(),
        floor.transition.transition_epoch().get().to_string(),
        floor.transition.source_epoch().get().to_string(),
        format_node_list(floor.transition.source_acting_set()),
        format_node_list(floor.transition.destination_acting_set()),
        floor.staging_generation.to_string(),
        match floor.disposition {
            MetadataTransferStagingCleanupDisposition::Completed => "completed".to_owned(),
            MetadataTransferStagingCleanupDisposition::Superseded {
                successor_transition_epoch,
            } => format!("superseded:{}", successor_transition_epoch.get()),
        },
        hex_encode(&floor.artifact_digest),
        floor.artifact_length.to_string(),
        floor.artifact_format_version.to_string(),
        floor.publications.len().to_string(),
        publications,
        hex_encode(&floor.tombstone_set_digest),
        floor.tombstones.len().to_string(),
        tombstones,
        floor.checkpoint_bindings.len().to_string(),
        checkpoint_bindings,
    ]
    .join(",")
}

fn parse_metadata_transfer_staging_finalized_floor(
    line: usize,
    value: &str,
) -> Result<MetadataTransferStagingFinalizedFloor, ControlPlaneError> {
    let fields = value.split(',').collect::<Vec<_>>();
    if fields.len() != 17 {
        return Err(parse_error(
            line,
            "metadata-transfer staging finalized floor must have seventeen fields",
        ));
    }
    let disposition = if fields[6] == "completed" {
        MetadataTransferStagingCleanupDisposition::Completed
    } else if let Some(epoch) = fields[6].strip_prefix("superseded:") {
        MetadataTransferStagingCleanupDisposition::Superseded {
            successor_transition_epoch: parse_required_cluster_epoch(
                line,
                epoch,
                "staging finalized-floor successor transition epoch",
            )?,
        }
    } else {
        return Err(parse_error(
            line,
            "metadata-transfer staging finalized floor disposition is invalid",
        ));
    };
    let publication_count = usize::try_from(parse_u64(
        line,
        fields[10],
        "staging finalized-floor publication count",
    )?)
    .map_err(|_| {
        parse_error(
            line,
            "staging finalized-floor publication count does not fit usize",
        )
    })?;
    let publications = if fields[11].is_empty() {
        Vec::new()
    } else {
        fields[11]
            .split(';')
            .map(|encoded| {
                let parts = encoded.split('/').collect::<Vec<_>>();
                if parts.len() != 16 {
                    return Err(parse_error(
                        line,
                        "staging finalized-floor publication must have sixteen fields",
                    ));
                }
                let source_metadata_proof = parse_optional_metadata_proof(
                    line,
                    &parts[5..10],
                    "staging finalized-floor publication source proof",
                )?
                .ok_or_else(|| {
                    parse_error(
                        line,
                        "staging finalized-floor publication source proof is required",
                    )
                })?;
                let imported_metadata_proof = parse_optional_metadata_proof(
                    line,
                    &parts[10..15],
                    "staging finalized-floor publication imported proof",
                )?
                .ok_or_else(|| {
                    parse_error(
                        line,
                        "staging finalized-floor publication imported proof is required",
                    )
                })?;
                Ok(MetadataTransferStagingFinalizedPublicationBinding {
                    node_id: NodeId::new(parse_u32(
                        line,
                        parts[0],
                        "staging finalized-floor publication node",
                    )?),
                    node_incarnation: parse_u64(
                        line,
                        parts[1],
                        "staging finalized-floor publication incarnation",
                    )?,
                    endpoint: String::from_utf8(hex_decode(line, parts[2])?).map_err(|_| {
                        parse_error(
                            line,
                            "staging finalized-floor publication endpoint is not UTF-8",
                        )
                    })?,
                    target_epoch: parse_required_cluster_epoch(
                        line,
                        parts[3],
                        "staging finalized-floor publication target epoch",
                    )?,
                    transfer: PgMetadataTransferProof::new_with_imported_metadata_proof(
                        parse_required_cluster_epoch(
                            line,
                            parts[4],
                            "staging finalized-floor publication source epoch",
                        )?,
                        source_metadata_proof,
                        imported_metadata_proof,
                    ),
                    evidence_digest: hex_decode(line, parts[15])?.try_into().map_err(|_| {
                        parse_error(
                            line,
                            "staging finalized-floor publication digest must contain 32 bytes",
                        )
                    })?,
                })
            })
            .collect::<Result<Vec<_>, ControlPlaneError>>()?
    };
    if publications.len() != publication_count {
        return Err(parse_error(
            line,
            "staging finalized-floor publication count does not match its entries",
        ));
    }
    let tombstone_count = usize::try_from(parse_u64(
        line,
        fields[13],
        "staging finalized-floor tombstone count",
    )?)
    .map_err(|_| {
        parse_error(
            line,
            "staging finalized-floor tombstone count does not fit usize",
        )
    })?;
    let tombstones = if fields[14].is_empty() {
        Vec::new()
    } else {
        fields[14]
            .split(';')
            .map(|encoded| {
                let parts = encoded.split('/').collect::<Vec<_>>();
                if parts.len() != 4 {
                    return Err(parse_error(
                        line,
                        "staging finalized-floor tombstone must have four fields",
                    ));
                }
                Ok(MetadataTransferStagingTombstoneBinding {
                    node_id: NodeId::new(parse_u32(
                        line,
                        parts[0],
                        "staging finalized-floor tombstone node",
                    )?),
                    node_incarnation: parse_u64(
                        line,
                        parts[1],
                        "staging finalized-floor tombstone incarnation",
                    )?,
                    endpoint: String::from_utf8(hex_decode(line, parts[2])?).map_err(|_| {
                        parse_error(
                            line,
                            "staging finalized-floor tombstone endpoint is not UTF-8",
                        )
                    })?,
                    evidence_digest: hex_decode(line, parts[3])?.try_into().map_err(|_| {
                        parse_error(
                            line,
                            "staging finalized-floor tombstone digest must contain 32 bytes",
                        )
                    })?,
                })
            })
            .collect::<Result<Vec<_>, ControlPlaneError>>()?
    };
    if tombstones.len() != tombstone_count {
        return Err(parse_error(
            line,
            "staging finalized-floor tombstone count does not match its entries",
        ));
    }
    let checkpoint_binding_count = usize::try_from(parse_u64(
        line,
        fields[15],
        "staging finalized-floor checkpoint-binding count",
    )?)
    .map_err(|_| {
        parse_error(
            line,
            "staging finalized-floor checkpoint-binding count does not fit usize",
        )
    })?;
    let mut checkpoint_bindings = BTreeMap::new();
    if !fields[16].is_empty() {
        for encoded in fields[16].split(';') {
            let parts = encoded.split('/').collect::<Vec<_>>();
            if parts.len() != 15 {
                return Err(parse_error(
                    line,
                    "staging finalized-floor checkpoint binding must have fifteen fields",
                ));
            }
            let kind = match parse_u16(line, parts[4], "staging checkpoint evidence kind")? {
                0 => crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                1 => crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
                _ => {
                    return Err(parse_error(
                        line,
                        "invalid staging finalized-floor checkpoint evidence kind",
                    ));
                }
            };
            let target_epoch = if parts[5] == "-" {
                None
            } else {
                Some(parse_required_cluster_epoch(
                    line,
                    parts[5],
                    "staging finalized-floor checkpoint target epoch",
                )?)
            };
            let key = MetadataTransferStagingEvidenceKey {
                pg_id: PgId::new(parse_u32(
                    line,
                    parts[0],
                    "staging finalized-floor checkpoint PG",
                )?),
                staging_generation: parse_u64(
                    line,
                    parts[1],
                    "staging finalized-floor checkpoint generation",
                )?,
                actor_node_id: NodeId::new(parse_u32(
                    line,
                    parts[2],
                    "staging finalized-floor checkpoint evidence actor",
                )?),
                actor_node_incarnation: parse_u64(
                    line,
                    parts[3],
                    "staging finalized-floor checkpoint evidence incarnation",
                )?,
                kind,
                target_epoch,
            };
            let binding = MetadataTransferStagingFinalizedCheckpointBinding {
                actor_node_id: NodeId::new(parse_u32(
                    line,
                    parts[6],
                    "staging finalized-floor checkpoint segment actor",
                )?),
                actor_node_incarnation: parse_u64(
                    line,
                    parts[7],
                    "staging finalized-floor checkpoint segment incarnation",
                )?,
                actor_endpoint: String::from_utf8(hex_decode(line, parts[8])?).map_err(|_| {
                    parse_error(
                        line,
                        "staging finalized-floor checkpoint segment endpoint is not UTF-8",
                    )
                })?,
                first_generation: parse_u64(
                    line,
                    parts[9],
                    "staging finalized-floor checkpoint first generation",
                )?,
                last_generation: parse_u64(
                    line,
                    parts[10],
                    "staging finalized-floor checkpoint last generation",
                )?,
                page_generation: parse_u64(
                    line,
                    parts[11],
                    "staging finalized-floor checkpoint page generation",
                )?,
                page_sequence: parse_u64(
                    line,
                    parts[12],
                    "staging finalized-floor checkpoint page sequence",
                )?,
                segment_digest: hex_decode(line, parts[13])?.try_into().map_err(|_| {
                    parse_error(
                        line,
                        "staging finalized-floor checkpoint segment digest must contain 32 bytes",
                    )
                })?,
                actor_closure_candidate: if parts[14] == "-" {
                    None
                } else {
                    Some(
                        crate::pg_store::decode_staging_evidence_actor_closure_candidate_bytes(
                            &hex_decode(line, parts[14])?,
                        )
                        .map_err(|error| parse_error(line, &error.to_string()))?,
                    )
                },
            };
            if checkpoint_bindings.insert(key, binding).is_some() {
                return Err(parse_error(
                    line,
                    "duplicate staging finalized-floor checkpoint binding",
                ));
            }
        }
    }
    if checkpoint_bindings.len() != checkpoint_binding_count {
        return Err(parse_error(
            line,
            "staging finalized-floor checkpoint-binding count does not match its entries",
        ));
    }
    Ok(MetadataTransferStagingFinalizedFloor {
        transition: UnavailablePgTransitionMutationBinding::new(
            PgId::new(parse_u32(line, fields[0], "staging finalized-floor PG")?),
            parse_required_cluster_epoch(
                line,
                fields[1],
                "staging finalized-floor transition epoch",
            )?,
            parse_required_cluster_epoch(line, fields[2], "staging finalized-floor source epoch")?,
            parse_node_list(line, fields[3])?,
            parse_node_list(line, fields[4])?,
        ),
        staging_generation: parse_u64(line, fields[5], "staging finalized-floor generation")?,
        disposition,
        artifact_digest: hex_decode(line, fields[7])?.try_into().map_err(|_| {
            parse_error(
                line,
                "staging finalized-floor artifact digest must contain 32 bytes",
            )
        })?,
        artifact_length: parse_u64(line, fields[8], "staging finalized-floor artifact length")?,
        artifact_format_version: parse_u16(
            line,
            fields[9],
            "staging finalized-floor artifact format version",
        )?,
        publications,
        tombstone_set_digest: hex_decode(line, fields[12])?.try_into().map_err(|_| {
            parse_error(
                line,
                "staging finalized-floor tombstone-set digest must contain 32 bytes",
            )
        })?,
        tombstones,
        checkpoint_bindings,
    })
}

fn format_metadata_transfer_staging_actor_closure(
    closure: &MetadataTransferStagingActorClosureCertificate,
) -> String {
    [
        closure.source_actor.node_id().as_u32().to_string(),
        closure.source_actor.node_incarnation().to_string(),
        hex_encode(closure.source_actor.endpoint().as_bytes()),
        closure.source_tip_generation.to_string(),
        hex_encode(&closure.source_tip_page_digest),
        hex_encode(&closure.source_tip_apply_receipt_digest),
        closure.destination_actor.node_id().as_u32().to_string(),
        closure.destination_actor.node_incarnation().to_string(),
        hex_encode(closure.destination_actor.endpoint().as_bytes()),
        hex_encode(&closure.destination_genesis_page_digest),
        closure.rebound_entry_count.to_string(),
        closure.rebound_max_sequence.to_string(),
        hex_encode(&closure.rebound_evidence_digest),
    ]
    .join(",")
}

fn parse_metadata_transfer_staging_actor_closure(
    line: usize,
    value: &str,
) -> Result<MetadataTransferStagingActorClosureCertificate, ControlPlaneError> {
    let fields = value.split(',').collect::<Vec<_>>();
    if fields.len() != 13 {
        return Err(parse_error(
            line,
            "metadata-transfer staging actor closure must have thirteen fields",
        ));
    }
    let actor = |node: usize,
                 incarnation: usize,
                 endpoint: usize,
                 node_label: &'static str,
                 incarnation_label: &'static str,
                 endpoint_label: &'static str| {
        let endpoint = String::from_utf8(hex_decode(line, fields[endpoint])?)
            .map_err(|_| parse_error(line, endpoint_label))?;
        MetadataTransferStagingNodeIdentity::new(
            NodeId::new(parse_u32(line, fields[node], node_label)?),
            parse_u64(line, fields[incarnation], incarnation_label)?,
            endpoint,
        )
        .map_err(|error| parse_error(line, &error.to_string()))
    };
    let digest = |index: usize, label: &str| {
        hex_decode(line, fields[index])?.try_into().map_err(|_| {
            parse_error(
                line,
                &format!("metadata-transfer staging actor closure {label} must contain 32 bytes"),
            )
        })
    };
    Ok(MetadataTransferStagingActorClosureCertificate {
        source_actor: actor(
            0,
            1,
            2,
            "source actor node",
            "source actor incarnation",
            "source actor endpoint is not UTF-8",
        )?,
        source_tip_generation: parse_u64(line, fields[3], "source tip generation")?,
        source_tip_page_digest: digest(4, "source page digest")?,
        source_tip_apply_receipt_digest: digest(5, "source receipt digest")?,
        destination_actor: actor(
            6,
            7,
            8,
            "destination actor node",
            "destination actor incarnation",
            "destination actor endpoint is not UTF-8",
        )?,
        destination_genesis_page_digest: digest(9, "destination genesis digest")?,
        rebound_entry_count: parse_u64(line, fields[10], "rebound entry count")?,
        rebound_max_sequence: parse_u64(line, fields[11], "rebound maximum sequence")?,
        rebound_evidence_digest: digest(12, "rebound evidence digest")?,
    })
}

fn format_metadata_transfer_staging_evidence_checkpoint_anchor(
    anchor: &MetadataTransferStagingEvidenceCheckpointAnchor,
) -> String {
    [
        anchor.actor.node_id().as_u32().to_string(),
        anchor.actor.node_incarnation().to_string(),
        hex_encode(anchor.actor.endpoint().as_bytes()),
        anchor.first_generation.to_string(),
        anchor.last_generation.to_string(),
        anchor.previous_generation.to_string(),
        hex_encode(&anchor.previous_apply_receipt_digest),
        hex_encode(&anchor.tip_apply_receipt),
        hex_encode(&anchor.source_segment_digest),
        anchor.source_segment_count.to_string(),
        hex_encode(&anchor.source_segments_digest),
    ]
    .join(",")
}

fn parse_metadata_transfer_staging_evidence_checkpoint_anchor(
    line: usize,
    value: &str,
) -> Result<MetadataTransferStagingEvidenceCheckpointAnchor, ControlPlaneError> {
    let fields = value.split(',').collect::<Vec<_>>();
    if fields.len() != 11 {
        return Err(parse_error(
            line,
            "metadata-transfer staging checkpoint anchor must have eleven fields",
        ));
    }
    let actor = MetadataTransferStagingNodeIdentity::new(
        NodeId::new(parse_u32(
            line,
            fields[0],
            "staging checkpoint anchor node",
        )?),
        parse_u64(line, fields[1], "staging checkpoint anchor incarnation")?,
        String::from_utf8(hex_decode(line, fields[2])?)
            .map_err(|_| parse_error(line, "staging checkpoint anchor endpoint is not UTF-8"))?,
    )
    .map_err(|error| parse_error(line, &error.to_string()))?;
    Ok(MetadataTransferStagingEvidenceCheckpointAnchor {
        actor,
        first_generation: parse_u64(
            line,
            fields[3],
            "staging checkpoint anchor first generation",
        )?,
        last_generation: parse_u64(line, fields[4], "staging checkpoint anchor last generation")?,
        previous_generation: parse_u64(
            line,
            fields[5],
            "staging checkpoint anchor previous generation",
        )?,
        previous_apply_receipt_digest: hex_decode(line, fields[6])?.try_into().map_err(|_| {
            parse_error(
                line,
                "staging checkpoint anchor previous receipt digest must contain 32 bytes",
            )
        })?,
        tip_apply_receipt: hex_decode(line, fields[7])?,
        source_segment_digest: hex_decode(line, fields[8])?.try_into().map_err(|_| {
            parse_error(
                line,
                "staging checkpoint anchor source segment digest must contain 32 bytes",
            )
        })?,
        source_segment_count: parse_u64(
            line,
            fields[9],
            "staging checkpoint anchor source segment count",
        )?,
        source_segments_digest: hex_decode(line, fields[10])?.try_into().map_err(|_| {
            parse_error(
                line,
                "staging checkpoint anchor source segments digest must contain 32 bytes",
            )
        })?,
    })
}

fn format_metadata_transfer_staging_evidence_checkpoint_segment(
    segment: &MetadataTransferStagingEvidenceCheckpointSegment,
) -> String {
    let mut fields = vec![
        segment.actor.node_id().as_u32().to_string(),
        segment.actor.node_incarnation().to_string(),
        hex_encode(segment.actor.endpoint().as_bytes()),
        segment.first_generation.to_string(),
        segment.last_generation.to_string(),
        segment.previous_generation.to_string(),
        hex_encode(&segment.previous_apply_receipt_digest),
        hex_encode(&segment.tip_apply_receipt),
        segment.page_links.len().to_string(),
    ];
    for link in &segment.page_links {
        fields.extend([
            hex_encode(&link.page_digest),
            hex_encode(&link.previous_apply_receipt_digest),
            hex_encode(&link.apply_receipt_digest),
            link.actor_closure_candidate.as_ref().map_or_else(
                || "-".to_owned(),
                |candidate| {
                    hex_encode(
                        &crate::pg_store::encode_staging_evidence_actor_closure_candidate_bytes(
                            candidate,
                        )
                        .expect("retained actor-closure candidate is validated before encoding"),
                    )
                },
            ),
            link.entries.len().to_string(),
        ]);
        for entry in &link.entries {
            fields.extend([
                entry.sequence.to_string(),
                entry.evidence_key.pg_id.get().to_string(),
                entry.evidence_key.staging_generation.to_string(),
                entry.evidence_key.actor_node_id.as_u32().to_string(),
                entry.evidence_key.actor_node_incarnation.to_string(),
                (entry.evidence_key.kind as u8).to_string(),
                entry
                    .evidence_key
                    .target_epoch
                    .map_or_else(|| "-".to_owned(), |epoch| epoch.get().to_string()),
            ]);
        }
    }
    fields.push(segment.commitments.len().to_string());
    for (key, digest) in &segment.commitments {
        fields.extend([
            key.pg_id.get().to_string(),
            key.staging_generation.to_string(),
            key.actor_node_id.as_u32().to_string(),
            key.actor_node_incarnation.to_string(),
            (key.kind as u8).to_string(),
            key.target_epoch
                .map_or_else(|| "-".to_owned(), |epoch| epoch.get().to_string()),
            hex_encode(digest),
        ]);
    }
    fields.join(",")
}

fn parse_metadata_transfer_staging_evidence_checkpoint_segment(
    line: usize,
    value: &str,
) -> Result<MetadataTransferStagingEvidenceCheckpointSegment, ControlPlaneError> {
    const HEADER_FIELDS: usize = 9;
    const PAGE_LINK_FIELDS: usize = 5;
    const PAGE_ENTRY_FIELDS: usize = 7;
    const COMMITMENT_FIELDS: usize = 7;

    let fields = value.split(',').collect::<Vec<_>>();
    if fields.len() < HEADER_FIELDS {
        return Err(parse_error(
            line,
            "metadata-transfer staging checkpoint has too few fields",
        ));
    }
    let page_link_count = usize::try_from(parse_u64(
        line,
        fields[8],
        "staging checkpoint page-link count",
    )?)
    .map_err(|_| {
        parse_error(
            line,
            "staging checkpoint page-link count does not fit usize",
        )
    })?;
    if page_link_count == 0
        || page_link_count > MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_PAGES
    {
        return Err(parse_error(
            line,
            "metadata-transfer staging checkpoint has an invalid page-link count",
        ));
    }
    let endpoint_bytes = hex_decode(line, fields[2])?;
    let endpoint = String::from_utf8(endpoint_bytes)
        .map_err(|_| parse_error(line, "staging checkpoint endpoint is not UTF-8"))?;
    let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        NodeId::new(parse_u32(line, fields[0], "staging checkpoint actor node")?),
        parse_u64(line, fields[1], "staging checkpoint actor incarnation")?,
        endpoint,
    )
    .map_err(|error| parse_error(line, &error.to_string()))?;
    let first_generation = parse_u64(line, fields[3], "staging checkpoint first generation")?;
    let last_generation = parse_u64(line, fields[4], "staging checkpoint last generation")?;
    let previous_generation = parse_u64(line, fields[5], "staging checkpoint previous generation")?;
    let previous_apply_receipt_digest = hex_decode(line, fields[6])?.try_into().map_err(|_| {
        parse_error(
            line,
            "staging checkpoint previous receipt digest must contain 32 bytes",
        )
    })?;
    let tip_apply_receipt = hex_decode(line, fields[7])?;
    let parse_evidence_key = |offset: usize| {
        let kind = match parse_u16(line, fields[offset + 4], "staging evidence kind")? {
            0 => crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
            1 => crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
            _ => {
                return Err(parse_error(
                    line,
                    "invalid staging checkpoint evidence kind",
                ))
            }
        };
        let target_epoch = if fields[offset + 5] == "-" {
            None
        } else {
            Some(parse_required_cluster_epoch(
                line,
                fields[offset + 5],
                "staging checkpoint target epoch",
            )?)
        };
        Ok(MetadataTransferStagingEvidenceKey {
            pg_id: PgId::new(parse_u32(line, fields[offset], "staging checkpoint PG")?),
            staging_generation: parse_u64(
                line,
                fields[offset + 1],
                "staging checkpoint generation",
            )?,
            actor_node_id: NodeId::new(parse_u32(
                line,
                fields[offset + 2],
                "staging checkpoint evidence actor",
            )?),
            actor_node_incarnation: parse_u64(
                line,
                fields[offset + 3],
                "staging checkpoint evidence incarnation",
            )?,
            kind,
            target_epoch,
        })
    };
    let mut page_links = Vec::with_capacity(page_link_count);
    let mut cursor = HEADER_FIELDS;
    for _ in 0..page_link_count {
        let link_end = cursor.checked_add(PAGE_LINK_FIELDS).ok_or_else(|| {
            parse_error(line, "staging checkpoint page-link field count overflow")
        })?;
        if link_end > fields.len() {
            return Err(parse_error(
                line,
                "staging checkpoint page-link fields are truncated",
            ));
        }
        let decode_digest = |value: &str, field| {
            hex_decode(line, value)?.try_into().map_err(|_| {
                parse_error(
                    line,
                    &format!("staging checkpoint {field} must contain 32 bytes"),
                )
            })
        };
        let entry_count = usize::try_from(parse_u64(
            line,
            fields[cursor + 4],
            "staging checkpoint page-entry count",
        )?)
        .map_err(|_| {
            parse_error(
                line,
                "staging checkpoint page-entry count does not fit usize",
            )
        })?;
        if entry_count == 0 || entry_count > crate::pg_store::MAX_STAGING_EVIDENCE_PAGE_ENTRIES {
            return Err(parse_error(
                line,
                "staging checkpoint page-entry count is invalid",
            ));
        }
        let page_digest = decode_digest(fields[cursor], "page digest")?;
        let previous_apply_receipt_digest =
            decode_digest(fields[cursor + 1], "page predecessor receipt digest")?;
        let apply_receipt_digest = decode_digest(fields[cursor + 2], "page apply receipt digest")?;
        let actor_closure_candidate = if fields[cursor + 3] == "-" {
            None
        } else {
            Some(
                crate::pg_store::decode_staging_evidence_actor_closure_candidate_bytes(
                    &hex_decode(line, fields[cursor + 3])?,
                )
                .map_err(|error| parse_error(line, &error.to_string()))?,
            )
        };
        cursor = link_end;
        let entries_end = cursor
            .checked_add(entry_count.checked_mul(PAGE_ENTRY_FIELDS).ok_or_else(|| {
                parse_error(line, "staging checkpoint page-entry field count overflow")
            })?)
            .ok_or_else(|| parse_error(line, "staging checkpoint field count overflow"))?;
        if entries_end > fields.len() {
            return Err(parse_error(
                line,
                "staging checkpoint page-entry fields are truncated",
            ));
        }
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(MetadataTransferStagingEvidenceCheckpointPageEntry {
                sequence: parse_u64(line, fields[cursor], "staging checkpoint page sequence")?,
                evidence_key: parse_evidence_key(cursor + 1)?,
            });
            cursor += PAGE_ENTRY_FIELDS;
        }
        page_links.push(MetadataTransferStagingEvidenceCheckpointPageLink {
            page_digest,
            previous_apply_receipt_digest,
            apply_receipt_digest,
            actor_closure_candidate,
            entries,
        });
    }
    let commitment_count = fields
        .get(cursor)
        .ok_or_else(|| parse_error(line, "staging checkpoint commitment count is missing"))?;
    let commitment_count = usize::try_from(parse_u64(
        line,
        commitment_count,
        "staging checkpoint commitment count",
    )?)
    .map_err(|_| {
        parse_error(
            line,
            "staging checkpoint commitment count does not fit usize",
        )
    })?;
    if commitment_count == 0
        || commitment_count > MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COMMITMENTS
    {
        return Err(parse_error(
            line,
            "metadata-transfer staging checkpoint has an invalid commitment count",
        ));
    }
    cursor += 1;
    let expected_fields = cursor
        .checked_add(
            commitment_count
                .checked_mul(COMMITMENT_FIELDS)
                .ok_or_else(|| {
                    parse_error(line, "staging checkpoint commitment field count overflow")
                })?,
        )
        .ok_or_else(|| parse_error(line, "staging checkpoint field count overflow"))?;
    if fields.len() != expected_fields {
        return Err(parse_error(
            line,
            "metadata-transfer staging checkpoint field count does not match its commitments",
        ));
    }
    let mut commitments = BTreeMap::new();
    for index in 0..commitment_count {
        let offset = cursor + index * COMMITMENT_FIELDS;
        let key = parse_evidence_key(offset)?;
        let digest = hex_decode(line, fields[offset + 6])?
            .try_into()
            .map_err(|_| {
                parse_error(
                    line,
                    "staging checkpoint evidence digest must contain 32 bytes",
                )
            })?;
        if commitments.insert(key, digest).is_some() {
            return Err(parse_error(
                line,
                "duplicate staging checkpoint evidence commitment",
            ));
        }
    }
    Ok(MetadataTransferStagingEvidenceCheckpointSegment {
        actor,
        first_generation,
        last_generation,
        previous_generation,
        previous_apply_receipt_digest,
        page_links,
        tip_apply_receipt,
        commitments,
    })
}

fn format_unavailable_node_observation(observation: &NodeUnavailableObservation) -> String {
    format!(
        "{},{},{},{},{}",
        observation.node_id.as_u32(),
        observation.node_incarnation,
        hex_encode(observation.endpoint.as_bytes()),
        observation.lease_deadline_ms,
        observation.observed_at_ms
    )
}

fn format_unavailable_pg_placement_transition(
    transition: &UnavailablePgPlacementTransition,
) -> String {
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        transition.pg_id.get(),
        transition.transition_epoch.get(),
        option_u64(
            transition
                .predecessor_transition_epoch
                .map(ClusterEpoch::get)
        ),
        transition.topology_generation,
        hex_encode(&transition.topology_digest),
        transition.source_epoch.get(),
        format_node_list(&transition.source_acting_set),
        transition.source_node_id.as_u32(),
        hex_encode(
            format_unavailable_pg_transition_begin_authorization(&transition.begin_authorization)
                .as_bytes()
        ),
        transition.unavailable_node.node_id.as_u32(),
        transition.unavailable_node.node_incarnation,
        hex_encode(transition.unavailable_node.endpoint.as_bytes()),
        transition.unavailable_node.lease_deadline_ms,
        transition.unavailable_node.observed_at_ms,
        transition.grace_cutoff_ms,
        format_node_list(&transition.destination_acting_set),
        option_u64(transition.destination_epoch.map(ClusterEpoch::get)),
        transition.destination_route.as_ref().map_or_else(
            || "-".to_string(),
            |route| hex_encode(format_historical_pg_route_record(route).as_bytes())
        ),
        format_unavailable_pg_payload_readiness(transition.payload_readiness.as_ref()),
        transition.completion.as_ref().map_or_else(
            || "-".to_string(),
            |completion| hex_encode(format_ready_pg_peering_completion(completion).as_bytes())
        ),
        hex_encode(
            format_unavailable_pg_transition_batch_receipt(&transition.begin_batch_receipt)
                .as_bytes()
        ),
        transition.staging_authorization.as_ref().map_or_else(
            || "-".to_string(),
            |authorization| hex_encode(
                format_unavailable_pg_staging_authorization(authorization).as_bytes()
            )
        ),
        transition.destination_install.as_ref().map_or_else(
            || "-".to_string(),
            |install| hex_encode(format_unavailable_pg_destination_install(install).as_bytes())
        ),
        transition.completion_batch_receipt.as_ref().map_or_else(
            || "-".to_string(),
            |receipt| hex_encode(
                format_unavailable_pg_transition_batch_receipt(receipt).as_bytes()
            )
        )
    )
}

fn format_unavailable_pg_destination_install(install: &UnavailablePgDestinationInstall) -> String {
    let source = install.transfer.source_metadata_proof();
    let imported = install.transfer.metadata_proof();
    let publications = install
        .publications
        .iter()
        .map(|publication| {
            format!(
                "{}:{}:{}:{}",
                publication.node_id.as_u32(),
                publication.node_incarnation,
                hex_encode(publication.endpoint.as_bytes()),
                hex_encode(&publication.evidence_digest)
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{}",
        install.transfer.source_epoch().get(),
        source.applied_log_index,
        source.applied_log_hash.encoding_version(),
        source.applied_log_hash.value(),
        source.state_digest.encoding_version(),
        source.state_digest.value(),
        imported.applied_log_index,
        imported.applied_log_hash.encoding_version(),
        imported.applied_log_hash.value(),
        imported.state_digest.encoding_version(),
        imported.state_digest.value(),
        publications,
        hex_encode(
            format_unavailable_pg_transition_batch_receipt(&install.batch_receipt).as_bytes()
        )
    )
}

fn format_unavailable_pg_staging_authorization(
    authorization: &UnavailablePgStagingIntentAuthorization,
) -> String {
    format!(
        "{},{},{},{},{},{}",
        authorization.staging_generation,
        authorization.artifact_target_epoch.get(),
        hex_encode(&authorization.artifact_digest),
        authorization.artifact_length,
        authorization.artifact_format_version,
        hex_encode(
            format_unavailable_pg_transition_batch_receipt(&authorization.batch_receipt).as_bytes()
        )
    )
}

fn format_ready_pg_peering_completion(completion: &ReadyPgPeeringCompletion) -> String {
    let proof = completion.active_metadata_proof;
    format!(
        "{},{},{},{},{},{},{},{},{}",
        completion.pg_id.get(),
        completion.primary.as_u32(),
        completion.node_incarnation,
        proof.applied_log_index,
        proof.applied_log_hash.encoding_version(),
        proof.applied_log_hash.value(),
        proof.state_digest.encoding_version(),
        proof.state_digest.value(),
        completion.active_metadata_proof_epoch.get(),
    )
}

fn format_unavailable_pg_transition_batch_receipt(
    receipt: &UnavailablePgTransitionBatchReceipt,
) -> String {
    format!(
        "{},{},{},{},{}",
        receipt.identity.stage.as_str(),
        receipt.source_epoch.get(),
        receipt.target_epoch.get(),
        format_pg_list(&receipt.identity.member_pg_ids),
        hex_encode(&receipt.identity.members_digest)
    )
}

fn format_unavailable_pg_transition_begin_authorization(
    authorization: &UnavailablePgTransitionBeginAuthorization,
) -> String {
    let floor = authorization.source_metadata_floor;
    let proof = authorization.source_metadata_proof;
    [
        authorization.begin_at_ms.to_string(),
        authorization.unavailable_node.node_id.as_u32().to_string(),
        authorization.unavailable_node.node_incarnation.to_string(),
        hex_encode(authorization.unavailable_node.endpoint.as_bytes()),
        authorization.unavailable_node.lease_deadline_ms.to_string(),
        authorization.unavailable_node.observed_at_ms.to_string(),
        hex_encode(format_historical_pg_route_record(&authorization.source_route).as_bytes()),
        floor.applied_log_index.to_string(),
        floor.applied_log_hash.encoding_version().to_string(),
        floor.applied_log_hash.value().to_string(),
        floor.state_digest.encoding_version().to_string(),
        floor.state_digest.value().to_string(),
        option_u64(
            authorization
                .source_metadata_floor_epoch
                .map(ClusterEpoch::get),
        ),
        u8::from(authorization.source_metadata_floor_imported).to_string(),
        authorization.source_node_id.as_u32().to_string(),
        authorization.source_node_incarnation.to_string(),
        hex_encode(authorization.source_endpoint.as_bytes()),
        authorization.source_lease_deadline_ms.to_string(),
        authorization.source_observed_at_ms.to_string(),
        proof.applied_log_index.to_string(),
        proof.applied_log_hash.encoding_version().to_string(),
        proof.applied_log_hash.value().to_string(),
        proof.state_digest.encoding_version().to_string(),
        proof.state_digest.value().to_string(),
        authorization.replacement_node_id.as_u32().to_string(),
        authorization.replacement_node_incarnation.to_string(),
        hex_encode(authorization.replacement_endpoint.as_bytes()),
        authorization.replacement_lease_deadline_ms.to_string(),
    ]
    .join(",")
}

fn format_unavailable_pg_payload_readiness(
    readiness: Option<&UnavailablePgPayloadReadiness>,
) -> String {
    let Some(readiness) = readiness else {
        return "-".to_string();
    };
    let destinations = readiness
        .destinations
        .iter()
        .map(|destination| {
            format!(
                "{}/{}/{}/{}",
                destination.node_id.as_u32(),
                destination.node_incarnation,
                hex_encode(destination.endpoint.as_bytes()),
                destination.lease_deadline_ms
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "{}:{}:{}:{}:{}:{}:{}",
        readiness.pg_id.get(),
        readiness.transition_epoch.get(),
        readiness.destination_epoch.get(),
        readiness.topology_generation,
        hex_encode(&readiness.topology_digest),
        readiness.ready_at_ms,
        destinations
    )
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
        floor_log_index,
        floor_log_hash_version,
        floor_log_hash,
        floor_state_digest_version,
        floor_state_digest,
    ) = match record.peering_metadata_proof_floor {
        Some(proof) => (
            proof.applied_log_index.to_string(),
            proof.applied_log_hash.encoding_version().to_string(),
            proof.applied_log_hash.value().to_string(),
            proof.state_digest.encoding_version().to_string(),
            proof.state_digest.value().to_string(),
        ),
        None => (
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
        ),
    };
    let (
        transfer_source_epoch,
        transfer_source_log_index,
        transfer_source_log_hash_version,
        transfer_source_log_hash,
        transfer_source_state_digest_version,
        transfer_source_state_digest,
        transfer_imported_log_index,
        transfer_imported_log_hash_version,
        transfer_imported_log_hash,
        transfer_imported_state_digest_version,
        transfer_imported_state_digest,
    ) = match record.peering_metadata_transfer {
        Some(transfer) => {
            let source = transfer.source_metadata_proof();
            let imported = transfer.metadata_proof();
            (
                transfer.source_epoch().get().to_string(),
                source.applied_log_index.to_string(),
                source.applied_log_hash.encoding_version().to_string(),
                source.applied_log_hash.value().to_string(),
                source.state_digest.encoding_version().to_string(),
                source.state_digest.value().to_string(),
                imported.applied_log_index.to_string(),
                imported.applied_log_hash.encoding_version().to_string(),
                imported.applied_log_hash.value().to_string(),
                imported.state_digest.encoding_version().to_string(),
                imported.state_digest.value().to_string(),
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
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
        ),
    };
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        record.pg_id.get(),
        pg_state_as_str(record.state),
        format_node_list(&record.acting_set),
        option_u32(record.active_primary.map(NodeId::as_u32)),
        floor_log_index,
        floor_log_hash_version,
        floor_log_hash,
        floor_state_digest_version,
        floor_state_digest,
        option_u64(
            record
                .peering_metadata_proof_floor_epoch
                .map(ClusterEpoch::get)
        ),
        u8::from(record.peering_metadata_proof_floor_imported),
        transfer_source_epoch,
        transfer_source_log_index,
        transfer_source_log_hash_version,
        transfer_source_log_hash,
        transfer_source_state_digest_version,
        transfer_source_state_digest,
        transfer_imported_log_index,
        transfer_imported_log_hash_version,
        transfer_imported_log_hash,
        transfer_imported_state_digest_version,
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
        "{},{},{},{},{},{},{},{},{},{}",
        record.node_id.as_u32(),
        record.membership.as_str(),
        u8::from(record.administratively_available),
        record.observed_availability.as_str(),
        record.node_incarnation,
        option_u64(record.last_observed_epoch.map(ClusterEpoch::get)),
        option_u64(record.last_heartbeat_ms),
        option_u64(record.lease_deadline_ms),
        option_u64(
            record
                .cluster_map_history_route_scan_generation
                .map(NonZeroU64::get)
        ),
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
        // This persisted token predates bucket-write reservation references. Keep it stable.
        PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource => "pending-command",
        PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim => {
            "object-payload-reclaim-claim"
        }
    }
}

fn format_pg_record(record: &PgControlRecord) -> String {
    let (
        active_log_index,
        active_log_hash_version,
        active_log_hash,
        active_state_digest_version,
        active_state_digest,
    ) = match record.active_metadata_proof {
        Some(proof) => (
            option_u64(Some(proof.applied_log_index)),
            option_u64(Some(proof.applied_log_hash.encoding_version().into())),
            option_u64(Some(proof.applied_log_hash.value())),
            option_u64(Some(proof.state_digest.encoding_version().into())),
            option_u64(Some(proof.state_digest.value())),
        ),
        None => (
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u64(None),
        ),
    };
    let (
        peering_floor_log_index,
        peering_floor_log_hash_version,
        peering_floor_log_hash,
        peering_floor_state_digest_version,
        peering_floor_state_digest,
    ) = match record.peering_metadata_proof_floor {
        Some(proof) => (
            option_u64(Some(proof.applied_log_index)),
            option_u64(Some(proof.applied_log_hash.encoding_version().into())),
            option_u64(Some(proof.applied_log_hash.value())),
            option_u64(Some(proof.state_digest.encoding_version().into())),
            option_u64(Some(proof.state_digest.value())),
        ),
        None => (
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u64(None),
        ),
    };
    let (
        transfer_source_epoch,
        transfer_source_log_index,
        transfer_source_log_hash_version,
        transfer_source_log_hash,
        transfer_source_state_digest_version,
        transfer_source_state_digest,
        transfer_imported_log_index,
        transfer_imported_log_hash_version,
        transfer_imported_log_hash,
        transfer_imported_state_digest_version,
        transfer_imported_state_digest,
        transfer_source_route_epoch,
        transfer_source_node_id,
    ) = match record.peering_metadata_transfer {
        Some(transfer) => (
            option_u64(Some(transfer.source_epoch().get())),
            option_u64(Some(transfer.source_metadata_proof().applied_log_index)),
            option_u64(Some(
                transfer
                    .source_metadata_proof()
                    .applied_log_hash
                    .encoding_version()
                    .into(),
            )),
            option_u64(Some(
                transfer.source_metadata_proof().applied_log_hash.value(),
            )),
            option_u64(Some(
                transfer
                    .source_metadata_proof()
                    .state_digest
                    .encoding_version()
                    .into(),
            )),
            option_u64(Some(transfer.source_metadata_proof().state_digest.value())),
            option_u64(Some(transfer.metadata_proof().applied_log_index)),
            option_u64(Some(
                transfer
                    .metadata_proof()
                    .applied_log_hash
                    .encoding_version()
                    .into(),
            )),
            option_u64(Some(transfer.metadata_proof().applied_log_hash.value())),
            option_u64(Some(
                transfer
                    .metadata_proof()
                    .state_digest
                    .encoding_version()
                    .into(),
            )),
            option_u64(Some(transfer.metadata_proof().state_digest.value())),
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
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u64(None),
            option_u32(None),
        ),
    };
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        record.pg_id.get(),
        pg_state_as_str(record.state),
        format_node_list(&record.acting_set),
        option_u32(record.active_primary.map(NodeId::as_u32)),
        active_log_index,
        active_log_hash_version,
        active_log_hash,
        active_state_digest_version,
        active_state_digest,
        peering_floor_log_index,
        peering_floor_log_hash_version,
        peering_floor_log_hash,
        peering_floor_state_digest_version,
        peering_floor_state_digest,
        transfer_source_epoch,
        transfer_source_log_index,
        transfer_source_log_hash_version,
        transfer_source_log_hash,
        transfer_source_state_digest_version,
        transfer_source_state_digest,
        transfer_imported_log_index,
        transfer_imported_log_hash_version,
        transfer_imported_log_hash,
        transfer_imported_state_digest_version,
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
        "{},{},{},{},{},{},{},{},{},{},{},{},{}",
        node_id.as_u32(),
        record.pg_id.get(),
        pg_state_as_str(record.state),
        record.observed_epoch.get(),
        record.observed_at_ms,
        record.metadata_proof.applied_log_index,
        record.metadata_proof.applied_log_hash.encoding_version(),
        record.metadata_proof.applied_log_hash.value(),
        record.metadata_proof.state_digest.encoding_version(),
        record.metadata_proof.state_digest.value(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPlaneStateVersionError {
    Missing,
    Unsupported(u64),
}

fn require_current_control_plane_state_version(
    version: Option<u64>,
) -> Result<u64, ControlPlaneStateVersionError> {
    match version {
        None => Err(ControlPlaneStateVersionError::Missing),
        Some(version) if version != CURRENT_CONTROL_PLANE_STATE_VERSION => {
            Err(ControlPlaneStateVersionError::Unsupported(version))
        }
        Some(version) => Ok(version),
    }
}

fn control_plane_state_version_parse_error(
    line: usize,
    error: ControlPlaneStateVersionError,
) -> ControlPlaneError {
    match error {
        ControlPlaneStateVersionError::Missing => {
            parse_error(line, "missing control-plane state version")
        }
        ControlPlaneStateVersionError::Unsupported(version) => parse_error(
            line,
            &format!("unsupported control-plane state version {version}"),
        ),
    }
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
    let mut unavailable_node_observations = BTreeMap::new();
    let mut unavailable_pg_placement_transitions = BTreeMap::new();
    let mut retained_unavailable_pg_placement_transitions = BTreeMap::new();
    let mut metadata_transfer_staging_evidence_pages = BTreeMap::new();
    let mut metadata_transfer_staging_evidence_checkpoint_segments = BTreeMap::new();
    let mut metadata_transfer_staging_evidence_checkpoint_anchors = BTreeMap::new();
    let mut metadata_transfer_staging_actor_closures = BTreeMap::new();
    let mut metadata_transfer_staging_retired_actor_closures = BTreeMap::new();
    let mut metadata_transfer_staging_finalized_floors = BTreeMap::new();
    let mut metadata_transfer_staging_evidence = BTreeMap::new();
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
            require_current_control_plane_state_version(Some(parsed_version))
                .map_err(|error| control_plane_state_version_parse_error(line_number, error))?;
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
            version.ok_or_else(|| {
                parse_error(line_number, "version must precede max committed timestamp")
            })?;
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
        } else if let Some(value) = line.strip_prefix("node_history_route_retiring=") {
            let (node_id, reference) = parse_node_history_route_reference(line_number, value)?;
            let node = nodes.get_mut(&node_id).ok_or_else(|| {
                parse_error(
                    line_number,
                    "retiring node history route references unknown node",
                )
            })?;
            let previous_len = node.retiring_cluster_map_history_route_references.len();
            node.retiring_cluster_map_history_route_references
                .insert(reference)
                .map_err(|error| parse_error(line_number, &error.to_string()))?;
            if node.retiring_cluster_map_history_route_references.len() == previous_len {
                return Err(parse_error(
                    line_number,
                    "duplicate retiring node history route reference",
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
        } else if let Some(value) = line.strip_prefix("unavailable_node=") {
            let observation = parse_unavailable_node_observation(line_number, value)?;
            if unavailable_node_observations
                .insert(observation.node_id, observation)
                .is_some()
            {
                return Err(parse_error(
                    line_number,
                    "duplicate unavailable node observation",
                ));
            }
        } else if let Some(value) = line.strip_prefix("unavailable_pg_transition=") {
            let transition = parse_unavailable_pg_placement_transition(line_number, value)?;
            if unavailable_pg_placement_transitions
                .insert(transition.pg_id, transition)
                .is_some()
            {
                return Err(parse_error(
                    line_number,
                    "duplicate unavailable PG placement transition",
                ));
            }
        } else if let Some(value) = line.strip_prefix("retained_unavailable_pg_transition=") {
            let transition = parse_unavailable_pg_placement_transition(line_number, value)?;
            let key = (transition.pg_id, transition.transition_epoch);
            if retained_unavailable_pg_placement_transitions
                .insert(key, transition)
                .is_some()
            {
                return Err(parse_error(
                    line_number,
                    "duplicate retained unavailable PG placement transition",
                ));
            }
        } else if let Some(value) = line.strip_prefix("metadata_transfer_staging_evidence_page=") {
            let fields: Vec<_> = value.split(',').collect();
            if fields.len() != 3 {
                return Err(parse_error(
                    line_number,
                    "metadata-transfer staging evidence page must have three fields",
                ));
            }
            let operation_payload = hex_decode(line_number, fields[0])?;
            let page_digest = hex_decode(line_number, fields[1])?
                .try_into()
                .map_err(|_| {
                    parse_error(
                        line_number,
                        "metadata-transfer staging page digest must contain 32 bytes",
                    )
                })?;
            let apply_receipt = hex_decode(line_number, fields[2])?;
            let page = crate::pg_store::decode_staging_evidence_page_payload(
                &operation_payload,
                page_digest,
            )
            .map_err(|error| parse_error(line_number, &error.to_string()))?;
            let key = (
                page.actor().node_id(),
                page.actor().node_incarnation(),
                page.generation(),
            );
            if metadata_transfer_staging_evidence_pages
                .insert(
                    key,
                    MetadataTransferStagingEvidencePageRecord {
                        operation_payload,
                        page_digest,
                        apply_receipt,
                    },
                )
                .is_some()
            {
                return Err(parse_error(
                    line_number,
                    "duplicate metadata-transfer staging evidence page actor",
                ));
            }
        } else if let Some(value) =
            line.strip_prefix("metadata_transfer_staging_evidence_checkpoint=")
        {
            let segment =
                parse_metadata_transfer_staging_evidence_checkpoint_segment(line_number, value)?;
            let key = (
                segment.actor.node_id(),
                segment.actor.node_incarnation(),
                segment.first_generation,
            );
            if metadata_transfer_staging_evidence_checkpoint_segments
                .insert(key, segment)
                .is_some()
            {
                return Err(parse_error(
                    line_number,
                    "duplicate metadata-transfer staging evidence checkpoint segment",
                ));
            }
        } else if let Some(value) = line
            .strip_prefix(METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_ANCHOR_STATE_RECORD_PREFIX)
        {
            let anchor =
                parse_metadata_transfer_staging_evidence_checkpoint_anchor(line_number, value)?;
            let key = (
                anchor.actor.node_id(),
                anchor.actor.node_incarnation(),
                anchor.first_generation,
            );
            if metadata_transfer_staging_evidence_checkpoint_anchors
                .insert(key, anchor)
                .is_some()
            {
                return Err(parse_error(
                    line_number,
                    "duplicate metadata-transfer staging evidence checkpoint anchor",
                ));
            }
        } else if let Some(value) =
            line.strip_prefix(METADATA_TRANSFER_STAGING_RETIRED_ACTOR_CLOSURE_STATE_RECORD_PREFIX)
        {
            let closure = parse_metadata_transfer_staging_actor_closure(line_number, value)?;
            let key = (
                closure.source_actor.node_id(),
                closure.source_actor.node_incarnation(),
            );
            if metadata_transfer_staging_retired_actor_closures
                .insert(key, closure)
                .is_some()
            {
                return Err(parse_error(
                    line_number,
                    "duplicate metadata-transfer staging retired actor closure",
                ));
            }
        } else if let Some(value) =
            line.strip_prefix(METADATA_TRANSFER_STAGING_ACTOR_CLOSURE_STATE_RECORD_PREFIX)
        {
            let closure = parse_metadata_transfer_staging_actor_closure(line_number, value)?;
            let key = (
                closure.source_actor.node_id(),
                closure.source_actor.node_incarnation(),
            );
            if metadata_transfer_staging_actor_closures
                .insert(key, closure)
                .is_some()
            {
                return Err(parse_error(
                    line_number,
                    "duplicate metadata-transfer staging actor closure",
                ));
            }
        } else if let Some(value) =
            line.strip_prefix(METADATA_TRANSFER_STAGING_FINALIZED_FLOOR_STATE_RECORD_PREFIX)
        {
            let floor = parse_metadata_transfer_staging_finalized_floor(line_number, value)?;
            let pg_id = floor.transition.pg_id();
            let staging_generation = floor.staging_generation;
            if metadata_transfer_staging_finalized_floors
                .insert((pg_id, staging_generation), floor)
                .is_some()
            {
                return Err(parse_error(
                    line_number,
                    "duplicate metadata-transfer staging finalized floor",
                ));
            }
        } else if let Some(value) = line.strip_prefix("metadata_transfer_staging_evidence=") {
            let bytes = hex_decode(line_number, value)?;
            let evidence = crate::pg_store::decode_staging_evidence(&bytes)
                .map_err(|error| parse_error(line_number, &error.to_string()))?;
            let key = MetadataTransferStagingEvidenceKey {
                pg_id: evidence.intent().pg_id(),
                staging_generation: evidence.intent().staging_generation(),
                actor_node_id: evidence.actor().node_id(),
                actor_node_incarnation: evidence.actor().node_incarnation(),
                kind: evidence.kind(),
                target_epoch: evidence.target_epoch(),
            };
            if metadata_transfer_staging_evidence
                .insert(key, bytes)
                .is_some()
            {
                return Err(parse_error(
                    line_number,
                    "duplicate metadata-transfer staging evidence identity",
                ));
            }
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

    require_current_control_plane_state_version(version)
        .map_err(|error| control_plane_state_version_parse_error(0, error))?;
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
    let protection = required_cluster_map_history_protection(
        pgs.values(),
        nodes.values(),
        unavailable_pg_placement_transitions
            .values()
            .chain(retained_unavailable_pg_placement_transitions.values()),
    );
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
    validate_required_cluster_map_history(
        &history,
        &pgs,
        &nodes,
        &unavailable_pg_placement_transitions,
        &retained_unavailable_pg_placement_transitions,
        cluster_epoch,
    )?;
    let snapshot = ClusterControlSnapshot {
        authority_incarnation: authority_incarnation
            .ok_or_else(|| parse_error(0, "missing authority incarnation"))?,
        cluster_epoch,
        initial_topology,
        nodes,
        pgs,
        unavailable_node_observations,
        unavailable_pg_placement_transitions,
        retained_unavailable_pg_placement_transitions,
        metadata_transfer_staging_evidence_pages,
        metadata_transfer_staging_evidence_checkpoint_segments,
        metadata_transfer_staging_evidence_checkpoint_anchors,
        metadata_transfer_staging_actor_closures,
        metadata_transfer_staging_retired_actor_closures,
        metadata_transfer_staging_finalized_floors,
        metadata_transfer_staging_evidence,
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
    let policy = certificate.placement_policy();
    let domains = policy
        .nodes
        .iter()
        .map(|node| {
            format!(
                "{}:{}:{}",
                node.node_id.as_u32(),
                hex_encode(node.host.as_bytes()),
                hex_encode(node.disk.as_bytes())
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "{},{},{},{},{},{},{},{},{},{}",
        certificate.topology_generation(),
        hex_encode(certificate.topology_digest()),
        hex_encode(certificate.bootstrap_map_digest()),
        voters,
        policy.ec_data_shards,
        policy.ec_parity_shards,
        policy.failure_domain.as_str(),
        policy.failure_tolerance,
        policy.unavailable_replacement_grace_ms,
        domains
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
    if fields.len() != 10 {
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
    let domains = if fields[9].is_empty() {
        Vec::new()
    } else {
        fields[9]
            .split(';')
            .map(|domain| {
                let parts = domain.split(':').collect::<Vec<_>>();
                if parts.len() != 3 {
                    return Err(parse_error(
                        line,
                        "invalid certified storage node-domain field count",
                    ));
                }
                let host = String::from_utf8(hex_decode(line, parts[1])?)
                    .map_err(|_| parse_error(line, "storage host domain is not UTF-8"))?;
                let disk = String::from_utf8(hex_decode(line, parts[2])?)
                    .map_err(|_| parse_error(line, "storage disk domain is not UTF-8"))?;
                Ok(CertifiedStorageNodeDomain::new(
                    NodeId::new(parse_u32(line, parts[0], "certified storage node ID")?),
                    host,
                    disk,
                ))
            })
            .collect::<Result<Vec<_>, ControlPlaneError>>()?
    };
    let placement_policy = CertifiedStoragePlacementPolicy::new(
        parse_u8(line, fields[4], "initial topology EC data shards")?,
        parse_u8(line, fields[5], "initial topology EC parity shards")?,
        CertifiedStorageFailureDomain::from_str(fields[6])?,
        parse_u8(line, fields[7], "initial topology failure tolerance")?,
        parse_u64(
            line,
            fields[8],
            "initial topology unavailable replacement grace",
        )?,
        domains,
    )
    .map_err(|error| parse_error(line, &error.to_string()))?;
    InitialClusterTopologyCertificate::new(
        parse_u64(line, fields[0], "initial topology generation")?,
        topology_digest,
        bootstrap_map_digest,
        voters,
        placement_policy,
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
                if observation
                    .pending_metadata_command()
                    .is_some_and(|pending| pending.cluster_epoch() != current_epoch)
                {
                    return Err(parse_error(
                        line,
                        "active node PG observation has a non-current pending metadata command",
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
    transitions: &BTreeMap<PgId, UnavailablePgPlacementTransition>,
    retained_transitions: &BTreeMap<(PgId, ClusterEpoch), UnavailablePgPlacementTransition>,
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
        for reference in node.retained_cluster_map_history_route_references() {
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
    for transition in transitions.values().chain(retained_transitions.values()) {
        if historical_pg_route_record_at_epoch(
            history,
            pgs,
            transition.pg_id,
            transition.source_epoch,
        ) != Some(transition.begin_authorization.source_route.clone())
        {
            return Err(parse_error(
                0,
                "unavailable PG transition source route does not match retained CAS evidence",
            ));
        }
        let required_routes = [
            (
                transition.source_epoch,
                transition.source_acting_set.clone(),
            ),
            (
                transition.transition_epoch,
                unavailable_transition_source_route_acting_set(
                    &transition.source_acting_set,
                    transition.source_node_id,
                ),
            ),
        ];
        for (epoch, expected_acting_set) in required_routes.into_iter().chain(
            transition
                .destination_epoch
                .map(|epoch| (epoch, transition.destination_acting_set.clone())),
        ) {
            if epoch == current_epoch {
                if pgs
                    .get(&transition.pg_id)
                    .is_some_and(|pg| pg.acting_set == expected_acting_set)
                {
                    continue;
                }
                return Err(parse_error(
                    0,
                    "unavailable PG transition current route does not match retained evidence",
                ));
            }
            if epoch > current_epoch
                || !history.iter().any(|record| record.cluster_epoch() == epoch)
                || historical_pg_acting_set_at_epoch(history, pgs, transition.pg_id, epoch)
                    != Some(expected_acting_set.as_slice())
            {
                return Err(parse_error(
                    0,
                    "unavailable PG transition route does not match retained cluster-map history",
                ));
            }
        }
        if let (Some(destination_epoch), Some(destination_route)) =
            (transition.destination_epoch, &transition.destination_route)
        {
            let actual = if destination_epoch == current_epoch {
                pgs.get(&transition.pg_id)
                    .map(HistoricalPgRouteRecord::from)
            } else {
                historical_pg_route_record_at_epoch(
                    history,
                    pgs,
                    transition.pg_id,
                    destination_epoch,
                )
            };
            if actual.as_ref() != Some(destination_route) {
                return Err(parse_error(
                    0,
                    "unavailable PG transition destination route does not match retained CAS evidence",
                ));
            }
        }
    }
    Ok(())
}

fn historical_pg_route_record_at_epoch(
    history: &[ClusterMapHistoryRecord],
    current_pgs: &BTreeMap<PgId, PgControlRecord>,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
) -> Option<HistoricalPgRouteRecord> {
    for record in history
        .iter()
        .filter(|record| record.cluster_epoch() >= cluster_epoch)
    {
        if record.absent_pgs.contains(&pg_id) {
            return None;
        }
        if let Some(pg) = record.pg(pg_id) {
            return Some(pg.clone());
        }
    }
    current_pgs.get(&pg_id).map(HistoricalPgRouteRecord::from)
}

fn historical_pg_acting_set_at_epoch<'a>(
    history: &'a [ClusterMapHistoryRecord],
    current_pgs: &'a BTreeMap<PgId, PgControlRecord>,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
) -> Option<&'a [NodeId]> {
    for record in history
        .iter()
        .filter(|record| record.cluster_epoch() >= cluster_epoch)
    {
        if record.absent_pgs.contains(&pg_id) {
            return None;
        }
        if let Some(pg) = record.pg(pg_id) {
            return Some(&pg.acting_set);
        }
    }
    current_pgs.get(&pg_id).map(|pg| pg.acting_set.as_slice())
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
    if fields.len() != 24 {
        return Err(parse_error(
            line,
            "historical PG route record must have twenty-four fields",
        ));
    }
    let pg_id = PgId::new(parse_u32(line, fields[0], "historical PG id")?);
    let state = pg_state_from_str(fields[1])?;
    let acting_set = parse_node_list(line, fields[2])?;
    let active_primary =
        parse_option_u32(line, fields[3], "historical active primary")?.map(NodeId::new);
    let peering_metadata_proof_floor =
        parse_optional_metadata_proof(line, &fields[4..9], "historical peering proof floor")?;
    let peering_metadata_proof_floor_epoch =
        parse_option_cluster_epoch(line, fields[9], "historical peering proof floor epoch")?;
    let peering_metadata_proof_floor_imported = parse_bool_u8(
        line,
        fields[10],
        "historical peering proof floor imported provenance",
    )?;
    let source_epoch =
        parse_option_cluster_epoch(line, fields[11], "historical transfer source epoch")?;
    let source_log_index =
        parse_option_u64(line, fields[12], "historical transfer source log index")?;
    let source_log_hash_version = parse_option_u64(
        line,
        fields[13],
        "historical transfer source log hash version",
    )?;
    let source_log_hash =
        parse_option_u64(line, fields[14], "historical transfer source log hash")?;
    let source_state_digest_version = parse_option_u64(
        line,
        fields[15],
        "historical transfer source state digest version",
    )?;
    let source_state_digest =
        parse_option_u64(line, fields[16], "historical transfer source state digest")?;
    let imported_log_index =
        parse_option_u64(line, fields[17], "historical transfer imported log index")?;
    let imported_log_hash_version = parse_option_u64(
        line,
        fields[18],
        "historical transfer imported log hash version",
    )?;
    let imported_log_hash =
        parse_option_u64(line, fields[19], "historical transfer imported log hash")?;
    let imported_state_digest_version = parse_option_u64(
        line,
        fields[20],
        "historical transfer imported state digest version",
    )?;
    let imported_state_digest = parse_option_u64(
        line,
        fields[21],
        "historical transfer imported state digest",
    )?;
    let peering_metadata_transfer = match (
        source_epoch,
        source_log_index,
        source_log_hash_version,
        source_log_hash,
        source_state_digest_version,
        source_state_digest,
        imported_log_index,
        imported_log_hash_version,
        imported_log_hash,
        imported_state_digest_version,
        imported_state_digest,
    ) {
        (
            Some(source_epoch),
            Some(source_log_index),
            Some(source_log_hash_version),
            Some(source_log_hash),
            Some(source_state_digest_version),
            Some(source_state_digest),
            Some(imported_log_index),
            Some(imported_log_hash_version),
            Some(imported_log_hash),
            Some(imported_state_digest_version),
            Some(imported_state_digest),
        ) => Some(PgMetadataTransferProof::new_with_imported_metadata_proof(
            source_epoch,
            PgMetadataProof {
                applied_log_index: source_log_index,
                applied_log_hash: parse_metadata_log_hash(
                    line,
                    source_log_hash_version,
                    source_log_hash,
                )?,
                state_digest: parse_canonical_state_digest(
                    line,
                    source_state_digest_version,
                    source_state_digest,
                )?,
            },
            PgMetadataProof {
                applied_log_index: imported_log_index,
                applied_log_hash: parse_metadata_log_hash(
                    line,
                    imported_log_hash_version,
                    imported_log_hash,
                )?,
                state_digest: parse_canonical_state_digest(
                    line,
                    imported_state_digest_version,
                    imported_state_digest,
                )?,
            },
        )),
        (None, None, None, None, None, None, None, None, None, None, None) => None,
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
        peering_metadata_proof_floor,
        peering_metadata_proof_floor_epoch,
        peering_metadata_proof_floor_imported,
        peering_metadata_transfer,
        peering_metadata_transfer_source_route_epoch: parse_option_cluster_epoch(
            line,
            fields[22],
            "historical transfer source route epoch",
        )?,
        peering_metadata_transfer_source_node_id: parse_option_u32(
            line,
            fields[23],
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
    if fields.len() != 13 {
        return Err(parse_error(
            line,
            "node PG observation record must have thirteen fields",
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
        applied_log_hash: parse_metadata_log_hash(
            line,
            parse_u64(line, fields[6], "applied log hash version")?,
            parse_u64(line, fields[7], "applied log hash")?,
        )?,
        state_digest: parse_canonical_state_digest(
            line,
            parse_u64(line, fields[8], "state digest version")?,
            parse_u64(line, fields[9], "state digest")?,
        )?,
    };
    let pending_cluster_epoch =
        parse_option_cluster_epoch(line, fields[10], "pending command cluster epoch")?;
    let pending_log_index = parse_option_u64(line, fields[11], "pending command log index")?;
    let pending_command_checksum = parse_option_u64(line, fields[12], "pending command checksum")?;
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
    if fields.len() != 10 {
        return Err(parse_error(line, "node record must have ten fields"));
    }
    let node_id = NodeId::new(parse_u32(line, fields[0], "node id")?);
    let membership = NodeMembershipState::from_str(fields[1])?;
    let administratively_available = parse_bool_u8(line, fields[2], "administrative availability")?;
    let observed_availability = NodeAvailabilityState::from_str(fields[3])?;
    let node_incarnation = parse_u64(line, fields[4], "node incarnation")?;
    let last_observed_epoch = parse_option_cluster_epoch(line, fields[5], "last observed epoch")?;
    let last_heartbeat_ms = parse_option_u64(line, fields[6], "last heartbeat")?;
    let lease_deadline_ms = parse_option_u64(line, fields[7], "lease deadline")?;
    let cluster_map_history_route_scan_generation =
        parse_option_nonzero_u64(line, fields[8], "history route scan generation")?;
    let endpoint = String::from_utf8(hex_decode(line, fields[9])?)
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
        cluster_map_history_route_scan_generation,
        cluster_map_history_route_references: PgClusterMapHistoryRouteReferences::default(),
        retiring_cluster_map_history_route_references: PgClusterMapHistoryRouteReferences::default(
        ),
        pg_observations: BTreeMap::new(),
    })
}

fn parse_unavailable_node_observation(
    line: usize,
    value: &str,
) -> Result<NodeUnavailableObservation, ControlPlaneError> {
    let fields: Vec<_> = value.split(',').collect();
    if fields.len() != 5 {
        return Err(parse_error(
            line,
            "unavailable node observation must have five fields",
        ));
    }
    let endpoint = String::from_utf8(hex_decode(line, fields[2])?).map_err(|_| {
        parse_error(
            line,
            "unavailable node endpoint must be valid UTF-8 after hex decoding",
        )
    })?;
    Ok(NodeUnavailableObservation {
        node_id: NodeId::new(parse_u32(line, fields[0], "unavailable node id")?),
        node_incarnation: parse_u64(line, fields[1], "unavailable node incarnation")?,
        endpoint,
        lease_deadline_ms: parse_u64(line, fields[3], "unavailable node lease deadline")?,
        observed_at_ms: parse_u64(line, fields[4], "unavailable node observation time")?,
    })
}

fn parse_unavailable_pg_placement_transition(
    line: usize,
    value: &str,
) -> Result<UnavailablePgPlacementTransition, ControlPlaneError> {
    let fields: Vec<_> = value.split(',').collect();
    if fields.len() != 24 {
        return Err(parse_error(
            line,
            "unavailable PG placement transition must have twenty-four fields",
        ));
    }
    let topology_digest = hex_decode(line, fields[4])?
        .try_into()
        .map_err(|_| parse_error(line, "transition topology digest must contain 32 bytes"))?;
    let endpoint = String::from_utf8(hex_decode(line, fields[11])?).map_err(|_| {
        parse_error(
            line,
            "transition unavailable endpoint must be valid UTF-8 after hex decoding",
        )
    })?;
    let unavailable_node = NodeUnavailableObservation {
        node_id: NodeId::new(parse_u32(
            line,
            fields[9],
            "transition unavailable node id",
        )?),
        node_incarnation: parse_u64(line, fields[10], "transition node incarnation")?,
        endpoint,
        lease_deadline_ms: parse_u64(line, fields[12], "transition lease deadline")?,
        observed_at_ms: parse_u64(line, fields[13], "transition observation time")?,
    };
    Ok(UnavailablePgPlacementTransition {
        pg_id: PgId::new(parse_u32(line, fields[0], "transition PG id")?),
        transition_epoch: parse_required_cluster_epoch(line, fields[1], "transition epoch")?,
        predecessor_transition_epoch: parse_option_cluster_epoch(
            line,
            fields[2],
            "predecessor transition epoch",
        )?,
        topology_generation: parse_u64(line, fields[3], "transition topology generation")?,
        topology_digest,
        source_epoch: parse_required_cluster_epoch(line, fields[5], "transition source epoch")?,
        source_acting_set: parse_node_list(line, fields[6])?,
        source_node_id: NodeId::new(parse_u32(line, fields[7], "transition source node id")?),
        begin_authorization: parse_unavailable_pg_transition_begin_authorization(line, fields[8])?,
        unavailable_node,
        grace_cutoff_ms: parse_u64(line, fields[14], "transition grace cutoff")?,
        destination_acting_set: parse_node_list(line, fields[15])?,
        destination_epoch: parse_option_cluster_epoch(
            line,
            fields[16],
            "transition destination epoch",
        )?,
        destination_route: if fields[17] == "-" {
            None
        } else {
            let encoded = String::from_utf8(hex_decode(line, fields[17])?)
                .map_err(|_| parse_error(line, "transition destination route is not UTF-8"))?;
            Some(parse_historical_pg_route_record(line, &encoded)?)
        },
        payload_readiness: parse_unavailable_pg_payload_readiness(line, fields[18])?,
        completion: parse_ready_pg_peering_completion(line, fields[19])?,
        begin_batch_receipt: parse_unavailable_pg_transition_batch_receipt(line, fields[20])?,
        staging_authorization: parse_unavailable_pg_staging_authorization(line, fields[21])?,
        destination_install: parse_unavailable_pg_destination_install(line, fields[22])?,
        completion_batch_receipt: if fields[23] == "-" {
            None
        } else {
            Some(parse_unavailable_pg_transition_batch_receipt(
                line, fields[23],
            )?)
        },
    })
}

fn parse_unavailable_pg_destination_install(
    line: usize,
    value: &str,
) -> Result<Option<UnavailablePgDestinationInstall>, ControlPlaneError> {
    if value == "-" {
        return Ok(None);
    }
    let encoded = String::from_utf8(hex_decode(line, value)?)
        .map_err(|_| parse_error(line, "destination install evidence is not UTF-8"))?;
    let fields = encoded.split(',').collect::<Vec<_>>();
    if fields.len() != 13 {
        return Err(parse_error(
            line,
            "destination install evidence must have thirteen fields",
        ));
    }
    let source_metadata_proof = parse_optional_metadata_proof(
        line,
        &fields[1..6],
        "destination install source metadata proof",
    )?
    .ok_or_else(|| parse_error(line, "destination install source proof is required"))?;
    let imported_metadata_proof = parse_optional_metadata_proof(
        line,
        &fields[6..11],
        "destination install imported metadata proof",
    )?
    .ok_or_else(|| parse_error(line, "destination install imported proof is required"))?;
    let mut publications = Vec::new();
    if !fields[11].is_empty() {
        for publication in fields[11].split(';') {
            let parts = publication.split(':').collect::<Vec<_>>();
            if parts.len() != 4 {
                return Err(parse_error(
                    line,
                    "destination install publication must have four fields",
                ));
            }
            let endpoint = String::from_utf8(hex_decode(line, parts[2])?).map_err(|_| {
                parse_error(
                    line,
                    "destination install publication endpoint is not UTF-8",
                )
            })?;
            let evidence_digest = hex_decode(line, parts[3])?.try_into().map_err(|_| {
                parse_error(
                    line,
                    "destination install publication digest must contain 32 bytes",
                )
            })?;
            publications.push(UnavailablePgStagingPublicationBinding {
                node_id: NodeId::new(parse_u32(
                    line,
                    parts[0],
                    "destination install publication node",
                )?),
                node_incarnation: parse_u64(
                    line,
                    parts[1],
                    "destination install publication incarnation",
                )?,
                endpoint,
                evidence_digest,
            });
        }
    }
    Ok(Some(UnavailablePgDestinationInstall {
        transfer: PgMetadataTransferProof::new_with_imported_metadata_proof(
            parse_required_cluster_epoch(line, fields[0], "destination install source epoch")?,
            source_metadata_proof,
            imported_metadata_proof,
        ),
        publications,
        batch_receipt: parse_unavailable_pg_transition_batch_receipt(line, fields[12])?,
    }))
}

fn parse_unavailable_pg_staging_authorization(
    line: usize,
    value: &str,
) -> Result<Option<UnavailablePgStagingIntentAuthorization>, ControlPlaneError> {
    if value == "-" {
        return Ok(None);
    }
    let encoded = String::from_utf8(hex_decode(line, value)?)
        .map_err(|_| parse_error(line, "staging authorization is not UTF-8"))?;
    let fields = encoded.split(',').collect::<Vec<_>>();
    if fields.len() != 6 {
        return Err(parse_error(
            line,
            "staging authorization must have six fields",
        ));
    }
    let artifact_digest = hex_decode(line, fields[2])?
        .try_into()
        .map_err(|_| parse_error(line, "staging artifact digest must contain 32 bytes"))?;
    Ok(Some(UnavailablePgStagingIntentAuthorization {
        staging_generation: parse_u64(line, fields[0], "staging generation")?,
        artifact_target_epoch: parse_required_cluster_epoch(
            line,
            fields[1],
            "staging artifact target epoch",
        )?,
        artifact_digest,
        artifact_length: parse_u64(line, fields[3], "staging artifact length")?,
        artifact_format_version: parse_u16(line, fields[4], "staging artifact format version")?,
        batch_receipt: parse_unavailable_pg_transition_batch_receipt(line, fields[5])?,
    }))
}

fn parse_ready_pg_peering_completion(
    line: usize,
    value: &str,
) -> Result<Option<ReadyPgPeeringCompletion>, ControlPlaneError> {
    if value == "-" {
        return Ok(None);
    }
    let encoded = String::from_utf8(hex_decode(line, value)?)
        .map_err(|_| parse_error(line, "transition completion evidence is not UTF-8"))?;
    let fields = encoded.split(',').collect::<Vec<_>>();
    if fields.len() != 9 {
        return Err(parse_error(
            line,
            "transition completion evidence must have nine fields",
        ));
    }
    let active_metadata_proof =
        parse_optional_metadata_proof(line, &fields[3..8], "transition completion metadata proof")?
            .ok_or_else(|| parse_error(line, "transition completion metadata proof is required"))?;
    Ok(Some(ReadyPgPeeringCompletion {
        pg_id: PgId::new(parse_u32(line, fields[0], "transition completion PG id")?),
        primary: NodeId::new(parse_u32(
            line,
            fields[1],
            "transition completion primary node id",
        )?),
        node_incarnation: parse_u64(line, fields[2], "transition completion node incarnation")?,
        active_metadata_proof,
        active_metadata_proof_epoch: parse_required_cluster_epoch(
            line,
            fields[8],
            "transition completion metadata proof epoch",
        )?,
    }))
}

fn parse_unavailable_pg_transition_batch_receipt(
    line: usize,
    value: &str,
) -> Result<UnavailablePgTransitionBatchReceipt, ControlPlaneError> {
    let encoded = String::from_utf8(hex_decode(line, value)?)
        .map_err(|_| parse_error(line, "transition batch receipt is not UTF-8"))?;
    let fields = encoded.split(',').collect::<Vec<_>>();
    if fields.len() != 5 {
        return Err(parse_error(
            line,
            "transition batch receipt must have five fields",
        ));
    }
    let stage = UnavailablePgTransitionBatchStage::from_str(fields[0])
        .map_err(|message| parse_error(line, &message))?;
    let members_digest = hex_decode(line, fields[4])?
        .try_into()
        .map_err(|_| parse_error(line, "transition batch digest must contain 32 bytes"))?;
    Ok(UnavailablePgTransitionBatchReceipt {
        identity: UnavailablePgTransitionBatchReceiptIdentity {
            stage,
            member_pg_ids: parse_pg_list(line, fields[3])?,
            members_digest,
        },
        source_epoch: parse_required_cluster_epoch(
            line,
            fields[1],
            "transition batch source epoch",
        )?,
        target_epoch: parse_required_cluster_epoch(
            line,
            fields[2],
            "transition batch target epoch",
        )?,
    })
}

fn parse_unavailable_pg_transition_begin_authorization(
    line: usize,
    value: &str,
) -> Result<UnavailablePgTransitionBeginAuthorization, ControlPlaneError> {
    let encoded = String::from_utf8(hex_decode(line, value)?)
        .map_err(|_| parse_error(line, "transition begin authorization is not UTF-8"))?;
    let fields = encoded.split(',').collect::<Vec<_>>();
    if fields.len() != 28 {
        return Err(parse_error(
            line,
            "transition begin authorization must have twenty-eight fields",
        ));
    }
    let source_route_encoded = String::from_utf8(hex_decode(line, fields[6])?)
        .map_err(|_| parse_error(line, "transition source route is not UTF-8"))?;
    let source_metadata_floor =
        parse_optional_metadata_proof(line, &fields[7..12], "transition source metadata floor")?
            .ok_or_else(|| parse_error(line, "transition source metadata floor must be present"))?;
    let source_metadata_proof =
        parse_optional_metadata_proof(line, &fields[19..24], "transition source metadata proof")?
            .ok_or_else(|| parse_error(line, "transition source metadata proof must be present"))?;
    Ok(UnavailablePgTransitionBeginAuthorization {
        begin_at_ms: parse_u64(line, fields[0], "transition begin timestamp")?,
        unavailable_node: NodeUnavailableObservation {
            node_id: NodeId::new(parse_u32(
                line,
                fields[1],
                "transition authorized unavailable node",
            )?),
            node_incarnation: parse_u64(
                line,
                fields[2],
                "transition authorized unavailable incarnation",
            )?,
            endpoint: String::from_utf8(hex_decode(line, fields[3])?).map_err(|_| {
                parse_error(
                    line,
                    "transition authorized unavailable endpoint is not UTF-8",
                )
            })?,
            lease_deadline_ms: parse_u64(
                line,
                fields[4],
                "transition authorized unavailable lease",
            )?,
            observed_at_ms: parse_u64(
                line,
                fields[5],
                "transition authorized unavailable observation",
            )?,
        },
        source_route: parse_historical_pg_route_record(line, &source_route_encoded)?,
        source_metadata_floor,
        source_metadata_floor_epoch: parse_option_cluster_epoch(
            line,
            fields[12],
            "transition source floor epoch",
        )?,
        source_metadata_floor_imported: parse_bool_u8(
            line,
            fields[13],
            "transition source floor imported provenance",
        )?,
        source_node_id: NodeId::new(parse_u32(line, fields[14], "transition source node")?),
        source_node_incarnation: parse_u64(line, fields[15], "transition source incarnation")?,
        source_endpoint: String::from_utf8(hex_decode(line, fields[16])?)
            .map_err(|_| parse_error(line, "transition source endpoint is not UTF-8"))?,
        source_lease_deadline_ms: parse_u64(line, fields[17], "transition source lease")?,
        source_observed_at_ms: parse_u64(line, fields[18], "transition source observation")?,
        source_metadata_proof,
        replacement_node_id: NodeId::new(parse_u32(
            line,
            fields[24],
            "transition replacement node",
        )?),
        replacement_node_incarnation: parse_u64(
            line,
            fields[25],
            "transition replacement incarnation",
        )?,
        replacement_endpoint: String::from_utf8(hex_decode(line, fields[26])?)
            .map_err(|_| parse_error(line, "transition replacement endpoint is not UTF-8"))?,
        replacement_lease_deadline_ms: parse_u64(line, fields[27], "transition replacement lease")?,
    })
}

fn parse_unavailable_pg_payload_readiness(
    line: usize,
    value: &str,
) -> Result<Option<UnavailablePgPayloadReadiness>, ControlPlaneError> {
    if value == "-" {
        return Ok(None);
    }
    let fields = value.split(':').collect::<Vec<_>>();
    if fields.len() != 7 {
        return Err(parse_error(
            line,
            "unavailable PG payload readiness must have seven fields",
        ));
    }
    let topology_digest = hex_decode(line, fields[4])?.try_into().map_err(|_| {
        parse_error(
            line,
            "payload-readiness topology digest must contain 32 bytes",
        )
    })?;
    let destinations = if fields[6].is_empty() {
        Vec::new()
    } else {
        fields[6]
            .split(';')
            .map(|value| {
                let parts = value.split('/').collect::<Vec<_>>();
                if parts.len() != 4 {
                    return Err(parse_error(
                        line,
                        "payload-readiness destination must have four fields",
                    ));
                }
                Ok(UnavailablePgPayloadDestinationReadiness {
                    node_id: NodeId::new(parse_u32(
                        line,
                        parts[0],
                        "payload-readiness destination node",
                    )?),
                    node_incarnation: parse_u64(
                        line,
                        parts[1],
                        "payload-readiness destination incarnation",
                    )?,
                    endpoint: String::from_utf8(hex_decode(line, parts[2])?).map_err(|_| {
                        parse_error(line, "payload-readiness endpoint is not UTF-8")
                    })?,
                    lease_deadline_ms: parse_u64(
                        line,
                        parts[3],
                        "payload-readiness destination lease deadline",
                    )?,
                })
            })
            .collect::<Result<Vec<_>, ControlPlaneError>>()?
    };
    Ok(Some(UnavailablePgPayloadReadiness {
        pg_id: PgId::new(parse_u32(line, fields[0], "payload-readiness PG id")?),
        transition_epoch: parse_required_cluster_epoch(
            line,
            fields[1],
            "payload-readiness transition epoch",
        )?,
        destination_epoch: parse_required_cluster_epoch(
            line,
            fields[2],
            "payload-readiness destination epoch",
        )?,
        topology_generation: parse_u64(line, fields[3], "payload-readiness topology generation")?,
        topology_digest,
        ready_at_ms: parse_u64(line, fields[5], "payload-readiness observation time")?,
        destinations,
    }))
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
        "pending-command" => PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
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
    if fields.len() != 40 {
        return Err(parse_error(line, "PG record must have forty fields"));
    }
    let pg_id = PgId::new(parse_u32(line, fields[0], "PG id")?);
    let state = pg_state_from_str(fields[1])?;
    let acting_set = parse_node_list(line, fields[2])?;
    let active_primary = parse_option_u32(line, fields[3], "active primary")?.map(NodeId::new);
    let active_metadata_proof =
        parse_optional_metadata_proof(line, &fields[4..9], "active PG metadata proof")?;
    let peering_metadata_proof_floor =
        parse_optional_metadata_proof(line, &fields[9..14], "peering metadata proof floor")?;
    let transfer_source_epoch =
        parse_option_cluster_epoch(line, fields[14], "metadata transfer source epoch")?;
    let transfer_source_proof =
        parse_optional_metadata_proof(line, &fields[15..20], "metadata transfer source proof")?;
    let transfer_imported_proof =
        parse_optional_metadata_proof(line, &fields[20..25], "metadata transfer imported proof")?;
    let peering_metadata_transfer = match (
        transfer_source_epoch,
        transfer_source_proof,
        transfer_imported_proof,
    ) {
        (Some(source_epoch), Some(source_metadata_proof), Some(imported_metadata_proof)) => {
            Some(PgMetadataTransferProof {
                source_epoch,
                source_metadata_proof,
                imported_metadata_proof,
            })
        }
        (None, None, None) => None,
        _ => {
            return Err(parse_error(
                line,
                "metadata transfer fields must be all present or all absent",
            ));
        }
    };
    let peering_metadata_transfer_source_route_epoch =
        parse_option_cluster_epoch(line, fields[25], "metadata transfer source route epoch")?;
    let peering_metadata_transfer_source_node_id =
        parse_option_u32(line, fields[26], "metadata transfer source node")?.map(NodeId::new);
    let metadata_transfer_fenced = parse_bool_u8(line, fields[27], "metadata transfer fenced")?;
    let active_metadata_transfer_imported = parse_bool_u8(
        line,
        fields[28],
        "active metadata transfer imported provenance",
    )?;
    let metadata_transfer_fence_source_lease_deadline_ms = parse_option_u64(
        line,
        fields[29],
        "metadata transfer fence source lease deadline",
    )?;
    let metadata_transfer_fence_source_imported = parse_bool_u8(
        line,
        fields[30],
        "metadata transfer fence source imported provenance",
    )?;
    let active_metadata_proof_epoch =
        parse_option_cluster_epoch(line, fields[31], "active metadata proof epoch")?;
    let peering_metadata_proof_floor_epoch =
        parse_option_cluster_epoch(line, fields[32], "peering metadata proof floor epoch")?;
    let peering_metadata_proof_floor_imported = parse_bool_u8(
        line,
        fields[33],
        "peering metadata proof floor imported provenance",
    )?;
    let previous_primary_node_id =
        parse_option_u32(line, fields[34], "previous primary node id")?.map(NodeId::new);
    let previous_primary_node_incarnation =
        parse_option_u64(line, fields[35], "previous primary node incarnation")?;
    if previous_primary_node_incarnation == Some(0) {
        return Err(parse_error(
            line,
            "previous primary node incarnation must be nonzero",
        ));
    }
    let previous_primary_endpoint = if fields[36] == "-" {
        None
    } else {
        Some(
            String::from_utf8(hex_decode(line, fields[36])?).map_err(|_| {
                parse_error(
                    line,
                    "previous primary endpoint must be valid UTF-8 after hex decoding",
                )
            })?,
        )
    };
    let previous_primary_lease_deadline_ms =
        parse_option_u64(line, fields[37], "previous primary lease deadline")?;
    let previous_primary_prefer_reactivation =
        parse_bool_u8(line, fields[38], "previous primary reactivation preference")?;
    let metadata_transfer_fence_epoch =
        parse_option_cluster_epoch(line, fields[39], "metadata transfer fence epoch")?;
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

fn parse_option_nonzero_u64(
    line: usize,
    value: &str,
    field: &'static str,
) -> Result<Option<NonZeroU64>, ControlPlaneError> {
    parse_option_u64(line, value, field)?
        .map(|value| {
            NonZeroU64::new(value)
                .ok_or_else(|| parse_error(line, &format!("{field} must be nonzero")))
        })
        .transpose()
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

fn parse_required_cluster_epoch(
    line: usize,
    value: &str,
    field: &'static str,
) -> Result<ClusterEpoch, ControlPlaneError> {
    ClusterEpoch::new(parse_u64(line, value, field)?)
        .ok_or_else(|| parse_error(line, &format!("{field} must be nonzero")))
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
    if let Some(topology) = &snapshot.initial_topology {
        topology
            .placement_policy()
            .validate_acting_set(acting_set)
            .map_err(|message| ControlPlaneError::CommandDecode {
                message: format!(
                    "PG {} acting set violates placement policy: {message}",
                    pg_id.get()
                ),
            })?;
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
    let mut historical_pending_active_pg_observations = Vec::new();
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
        let current_actor = pg.acting_set.contains(&node_id);
        if !current_actor && observation.pending_metadata_command.is_none() {
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
            if pg.state == PgState::Active && pending.cluster_epoch() < snapshot.cluster_epoch {
                historical_pending_active_pg_observations.push(*observation);
            }
        }
        // Pending current-epoch work remains recoverable under the Active route.
        // Its proof may reflect partial or reissued work, so retain the committed
        // floor until a later heartbeat reports the terminal state without a slot.
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
    Ok(historical_pending_active_pg_observations)
}

fn validate_historical_pending_pg_heartbeat_observations(
    snapshot: &ClusterControlSnapshot,
    node_id: NodeId,
    observations: &[NodePgHeartbeatObservation],
) -> Result<Vec<NodePgHeartbeatObservation>, ControlPlaneError> {
    let mut observed_pgs = BTreeSet::new();
    let mut pending_observations = Vec::new();
    for observation in observations {
        if !observed_pgs.insert(observation.pg_id) {
            return Err(ControlPlaneError::DuplicatePgObservation {
                node_id: node_id.as_u32(),
                pg_id: observation.pg_id.get(),
            });
        }
        if !snapshot.pgs.contains_key(&observation.pg_id) {
            return Err(ControlPlaneError::UnknownPg {
                pg_id: observation.pg_id.get(),
            });
        }
        let Some(pending) = observation.pending_metadata_command else {
            continue;
        };
        validate_pending_metadata_command_reporter(snapshot, observation.pg_id, node_id, pending)?;
        pending_observations.push(*observation);
    }
    Ok(pending_observations)
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
    if snapshot.unavailable_replacement_grace_elapsed_for_pg(record, completed_at_ms) {
        return Err(ControlPlaneError::CommandDecode {
            message: format!(
                "PG {} cannot complete peering after an acting-set node's unavailable replacement grace elapsed",
                pg_id.get()
            ),
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
        && actual.applied_log_hash.value() != 0
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
            && observed.applied_log_hash.value() != 0
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
            && observed.applied_log_hash.value() != 0
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
    transitions: impl IntoIterator<Item = &'a UnavailablePgPlacementTransition>,
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
            node.retained_cluster_map_history_route_references()
                .map(|reference| (reference.cluster_epoch(), reference.pg_id())),
        );
    }
    for transition in transitions {
        exact_routes.insert((transition.source_epoch, transition.pg_id));
        exact_routes.insert((transition.transition_epoch, transition.pg_id));
        if let Some(destination_epoch) = transition.destination_epoch {
            exact_routes.insert((destination_epoch, transition.pg_id));
        }
        if let Some(receipt) = &transition.completion_batch_receipt {
            exact_routes.insert((receipt.source_epoch, transition.pg_id));
            exact_routes.insert((receipt.target_epoch, transition.pg_id));
        }
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

fn format_pg_list(pgs: &[PgId]) -> String {
    pgs.iter()
        .map(|pg_id| pg_id.get().to_string())
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

fn parse_pg_list(line: usize, value: &str) -> Result<Vec<PgId>, ControlPlaneError> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(':')
        .map(|value| parse_u32(line, value, "PG id").map(PgId::new))
        .collect()
}

fn parse_u32(line: usize, value: &str, field: &'static str) -> Result<u32, ControlPlaneError> {
    value
        .parse::<u32>()
        .map_err(|source| parse_error(line, &format!("invalid {field} {value:?}: {source}")))
}

fn parse_u8(line: usize, value: &str, field: &'static str) -> Result<u8, ControlPlaneError> {
    value
        .parse::<u8>()
        .map_err(|_| parse_error(line, &format!("invalid {field}")))
}

fn parse_u16(line: usize, value: &str, field: &'static str) -> Result<u16, ControlPlaneError> {
    value
        .parse::<u16>()
        .map_err(|source| ControlPlaneError::Parse {
            line,
            message: format!("invalid {field}: {source}"),
        })
}

fn parse_u64(line: usize, value: &str, field: &'static str) -> Result<u64, ControlPlaneError> {
    value
        .parse::<u64>()
        .map_err(|source| parse_error(line, &format!("invalid {field} {value:?}: {source}")))
}

fn parse_metadata_log_hash(
    line: usize,
    encoding_version: u64,
    value: u64,
) -> Result<MetadataCommandLogHash, ControlPlaneError> {
    let encoding_version = u8::try_from(encoding_version)
        .map_err(|_| parse_error(line, "metadata log hash version does not fit u8"))?;
    MetadataCommandLogHash::from_encoded_parts(encoding_version, value)
        .map_err(|error| parse_error(line, &error.to_string()))
}

fn parse_canonical_state_digest(
    line: usize,
    encoding_version: u64,
    value: u64,
) -> Result<CanonicalStateDigest, ControlPlaneError> {
    let encoding_version = u8::try_from(encoding_version)
        .map_err(|_| parse_error(line, "canonical state digest version does not fit u8"))?;
    CanonicalStateDigest::from_encoded_parts(encoding_version, value)
        .map_err(|error| parse_error(line, &error.to_string()))
}

fn parse_optional_metadata_proof(
    line: usize,
    fields: &[&str],
    field: &'static str,
) -> Result<Option<PgMetadataProof>, ControlPlaneError> {
    debug_assert_eq!(fields.len(), 5);
    let values = [
        parse_option_u64(line, fields[0], field)?,
        parse_option_u64(line, fields[1], field)?,
        parse_option_u64(line, fields[2], field)?,
        parse_option_u64(line, fields[3], field)?,
        parse_option_u64(line, fields[4], field)?,
    ];
    match values {
        [Some(applied_log_index), Some(log_hash_version), Some(log_hash), Some(state_digest_version), Some(state_digest)] => {
            Ok(Some(PgMetadataProof {
                applied_log_index,
                applied_log_hash: parse_metadata_log_hash(line, log_hash_version, log_hash)?,
                state_digest: parse_canonical_state_digest(
                    line,
                    state_digest_version,
                    state_digest,
                )?,
            }))
        }
        [None, None, None, None, None] => Ok(None),
        _ => Err(parse_error(
            line,
            &format!("{field} fields must be all present or all absent"),
        )),
    }
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
    for pair in value.as_bytes().as_chunks::<2>().0 {
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
pub(crate) mod tests;
