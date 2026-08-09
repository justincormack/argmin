// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

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
