// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneRaftPeerFrameIdentity {
    pub cluster_name: String,
    pub topology: Option<ControlPlaneRaftTopologyIdentity>,
    pub source: ControlPlaneRaftNodeId,
    pub target: ControlPlaneRaftNodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneRaftTopologyIdentity {
    pub generation: u64,
    pub digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlPlaneRaftPeerFrameKind {
    OrdinaryRpc,
    Snapshot,
}

impl ControlPlaneRaftPeerFrameIdentity {
    #[must_use]
    pub(crate) fn new(
        cluster_name: impl Into<String>,
        source: ControlPlaneRaftNodeId,
        target: ControlPlaneRaftNodeId,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            topology: None,
            source,
            target,
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn with_topology(mut self, generation: u64, digest: impl Into<String>) -> Self {
        self.topology = Some(ControlPlaneRaftTopologyIdentity {
            generation,
            digest: digest.into(),
        });
        self
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ControlPlaneRaftPeerRpcRequest {
    AppendEntries(AppendEntriesRequest<ControlPlaneRaftTypeConfig>),
    Vote(VoteRequest<ControlPlaneRaftTypeConfig>),
    PreVote(VoteRequest<ControlPlaneRaftTypeConfig>),
    TransferLeader(TransferLeaderRequest<ControlPlaneRaftTypeConfig>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ControlPlaneRaftPeerRpcResponse {
    AppendEntries(AppendEntriesResponse<ControlPlaneRaftTypeConfig>),
    Vote(VoteResponse<ControlPlaneRaftTypeConfig>),
    TransferLeader(TransferLeaderResponse<ControlPlaneRaftTypeConfig>),
}

#[derive(Debug, Clone)]
pub(crate) struct ControlPlaneRaftPeerSnapshotRequest {
    pub vote: VoteOf<ControlPlaneRaftTypeConfig>,
    pub snapshot: ControlPlaneRaftSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneRaftPeerSnapshotResponse {
    pub response: SnapshotResponse<ControlPlaneRaftTypeConfig>,
}

impl ControlPlaneRaftPeerRpcRequest {
    #[cfg(test)]
    pub(crate) fn encode_frame(&self) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(None)
    }

    pub(crate) fn encode_frame_for_peer(
        &self,
        identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(Some(identity))
    }

    pub(super) fn encode_frame_with_identity(
        &self,
        identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_VERSION);
        write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_KIND_REQUEST);
        write_raft_peer_frame_identity(&mut out, identity)?;
        match self {
            Self::AppendEntries(request) => {
                write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_APPEND_ENTRIES);
                write_raft_append_entries_request(&mut out, request)?;
            }
            Self::Vote(request) => {
                write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_VOTE);
                write_raft_vote_request(&mut out, request);
            }
            Self::PreVote(request) => {
                write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_PRE_VOTE);
                write_raft_vote_request(&mut out, request);
            }
            Self::TransferLeader(request) => {
                write_raft_u8(
                    &mut out,
                    CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_TRANSFER_LEADER,
                );
                write_raft_transfer_leader_request(&mut out, request);
            }
        }
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn decode_frame(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, None)
    }

    #[cfg(test)]
    pub(crate) fn decode_frame_for_peer(
        bytes: &[u8],
        expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, Some(expected_identity))
    }

    pub(super) fn decode_frame_with_identity(
        bytes: &[u8],
        expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Self, ControlPlaneError> {
        decode_raft_peer_rpc_frame(
            bytes,
            CONTROL_PLANE_RAFT_PEER_RPC_KIND_REQUEST,
            expected_identity,
            |reader| match reader.read_u8()? {
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_APPEND_ENTRIES => {
                    Ok(Self::AppendEntries(reader.read_append_entries_request()?))
                }
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_VOTE => {
                    Ok(Self::Vote(reader.read_vote_request()?))
                }
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_PRE_VOTE => {
                    Ok(Self::PreVote(reader.read_vote_request()?))
                }
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_TRANSFER_LEADER => {
                    Ok(Self::TransferLeader(reader.read_transfer_leader_request()?))
                }
                value => Err(raft_artifact_protocol_error(format!(
                    "unknown control-plane OpenRaft peer RPC request tag {value}"
                ))),
            },
        )
    }
}

impl ControlPlaneRaftPeerRpcResponse {
    #[cfg(test)]
    pub(crate) fn encode_frame(&self) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(None)
    }

    #[cfg(test)]
    pub(crate) fn encode_frame_for_peer(
        &self,
        identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(Some(identity))
    }

    pub(super) fn encode_frame_with_identity(
        &self,
        identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_VERSION);
        write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_KIND_RESPONSE);
        write_raft_peer_frame_identity(&mut out, identity)?;
        match self {
            Self::AppendEntries(response) => {
                write_raft_u8(
                    &mut out,
                    CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_APPEND_ENTRIES,
                );
                write_raft_append_entries_response(&mut out, response);
            }
            Self::Vote(response) => {
                write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_VOTE);
                write_raft_vote_response(&mut out, response);
            }
            Self::TransferLeader(response) => {
                write_raft_u8(
                    &mut out,
                    CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_TRANSFER_LEADER,
                );
                write_raft_transfer_leader_response(&mut out, response);
            }
        }
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn decode_frame(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, None)
    }

    pub(crate) fn decode_frame_for_peer(
        bytes: &[u8],
        expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, Some(expected_identity))
    }

    pub(super) fn decode_frame_with_identity(
        bytes: &[u8],
        expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Self, ControlPlaneError> {
        decode_raft_peer_rpc_frame(
            bytes,
            CONTROL_PLANE_RAFT_PEER_RPC_KIND_RESPONSE,
            expected_identity,
            |reader| match reader.read_u8()? {
                CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_APPEND_ENTRIES => {
                    Ok(Self::AppendEntries(reader.read_append_entries_response()?))
                }
                CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_VOTE => {
                    Ok(Self::Vote(reader.read_vote_response()?))
                }
                CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_TRANSFER_LEADER => Ok(Self::TransferLeader(
                    reader.read_transfer_leader_response()?,
                )),
                value => Err(raft_artifact_protocol_error(format!(
                    "unknown control-plane OpenRaft peer RPC response tag {value}"
                ))),
            },
        )
    }
}

impl ControlPlaneRaftPeerSnapshotRequest {
    #[cfg(test)]
    pub(crate) fn encode_frame(&self) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(None)
    }

    pub(crate) fn encode_frame_for_peer(
        &self,
        identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(Some(identity))
    }

    pub(super) fn encode_frame_with_identity(
        &self,
        identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_VERSION);
        write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_REQUEST);
        write_raft_peer_frame_identity(&mut out, identity)?;
        write_raft_vote(&mut out, self.vote);
        write_raft_snapshot(&mut out, &self.snapshot)?;
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn decode_frame(
        bytes: &[u8],
        max_frame_bytes: usize,
        max_snapshot_bytes: usize,
    ) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, max_frame_bytes, max_snapshot_bytes, None)
    }

    pub(super) fn decode_frame_with_identity(
        bytes: &[u8],
        max_frame_bytes: usize,
        max_snapshot_bytes: usize,
        expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Self, ControlPlaneError> {
        if bytes.len() > max_frame_bytes {
            return Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft peer snapshot request frame size {} bytes exceeds limit {}",
                bytes.len(),
                max_frame_bytes
            )));
        }
        decode_raft_peer_rpc_frame(
            bytes,
            CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_REQUEST,
            expected_identity,
            |reader| {
                let vote = reader.read_vote()?;
                let snapshot = reader
                    .read_snapshot_limited("raft peer snapshot payload", max_snapshot_bytes)?;
                Ok(Self { vote, snapshot })
            },
        )
    }
}

impl ControlPlaneRaftPeerSnapshotResponse {
    #[cfg(test)]
    pub(crate) fn encode_frame(&self) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(None)
    }

    #[cfg(test)]
    pub(crate) fn encode_frame_for_peer(
        &self,
        identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.encode_frame_with_identity(Some(identity))
    }

    pub(super) fn encode_frame_with_identity(
        &self,
        identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut out = Vec::new();
        out.extend_from_slice(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC);
        write_raft_u16(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_VERSION);
        write_raft_u8(&mut out, CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_RESPONSE);
        write_raft_peer_frame_identity(&mut out, identity)?;
        write_raft_vote(&mut out, self.response.vote);
        append_raft_artifact_checksum(&mut out);
        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn decode_frame(bytes: &[u8]) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, None)
    }

    pub(crate) fn decode_frame_for_peer(
        bytes: &[u8],
        expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<Self, ControlPlaneError> {
        Self::decode_frame_with_identity(bytes, Some(expected_identity))
    }

    pub(super) fn decode_frame_with_identity(
        bytes: &[u8],
        expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    ) -> Result<Self, ControlPlaneError> {
        decode_raft_peer_rpc_frame(
            bytes,
            CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_RESPONSE,
            expected_identity,
            |reader| {
                Ok(Self {
                    response: SnapshotResponse {
                        vote: reader.read_vote()?,
                    },
                })
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneRaftPeerTransportLimits {
    pub max_frame_bytes: usize,
    pub max_append_entries: usize,
    pub max_append_entries_bytes: usize,
    pub max_snapshot_bytes: usize,
}

impl ControlPlaneRaftPeerTransportLimits {
    pub const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
    pub const DEFAULT_MAX_APPEND_ENTRIES: usize = 256;
    pub const DEFAULT_MAX_APPEND_ENTRIES_BYTES: usize = 8 * 1024 * 1024;
    pub const DEFAULT_MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
    pub const REPLICATION_REQUIRED_APPEND_ENTRIES: usize = 64;
}

pub(crate) const CONTROL_PLANE_RAFT_TLS_ALPN: &[u8] = b"argmin-raft/1";

impl Default for ControlPlaneRaftPeerTransportLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: Self::DEFAULT_MAX_FRAME_BYTES,
            max_append_entries: Self::DEFAULT_MAX_APPEND_ENTRIES,
            max_append_entries_bytes: Self::DEFAULT_MAX_APPEND_ENTRIES_BYTES,
            max_snapshot_bytes: Self::DEFAULT_MAX_SNAPSHOT_BYTES,
        }
    }
}

pub(super) const CONTROL_PLANE_RAFT_TRANSFER_LEADER_AUTH_FRESHNESS_MS: u64 = 5_000;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ControlPlaneRaftPeerAuthMetricsSnapshot {
    accepted_total: u64,
    rejected_total: u64,
    rejected_without_operation_total: u64,
    accepted_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_reason: BTreeMap<ControlPlaneAuthRejectionReason, u64>,
}

impl ControlPlaneRaftPeerAuthMetricsSnapshot {
    #[must_use]
    pub fn accepted_total(&self) -> u64 {
        self.accepted_total
    }

    #[must_use]
    pub fn rejected_total(&self) -> u64 {
        self.rejected_total
    }

    #[must_use]
    pub fn rejected_without_operation_total(&self) -> u64 {
        self.rejected_without_operation_total
    }

    #[must_use]
    #[cfg(test)]
    pub fn accepted_for_operation(&self, operation: ControlPlaneAuthOperation) -> u64 {
        self.accepted_by_operation
            .get(&operation)
            .copied()
            .unwrap_or_default()
    }

    #[must_use]
    pub fn accepted_by_operation(&self) -> &BTreeMap<ControlPlaneAuthOperation, u64> {
        &self.accepted_by_operation
    }

    #[must_use]
    #[cfg(test)]
    pub fn rejected_for_operation(&self, operation: ControlPlaneAuthOperation) -> u64 {
        self.rejected_by_operation
            .get(&operation)
            .copied()
            .unwrap_or_default()
    }

    #[must_use]
    pub fn rejected_by_operation(&self) -> &BTreeMap<ControlPlaneAuthOperation, u64> {
        &self.rejected_by_operation
    }

    #[must_use]
    #[cfg(test)]
    pub fn rejected_for_reason(&self, reason: ControlPlaneAuthRejectionReason) -> u64 {
        self.rejected_by_reason
            .get(&reason)
            .copied()
            .unwrap_or_default()
    }

    #[must_use]
    pub fn rejected_by_reason(&self) -> &BTreeMap<ControlPlaneAuthRejectionReason, u64> {
        &self.rejected_by_reason
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ControlPlaneRaftPeerAuthStatusSnapshot {
    required: bool,
    local_principal: Option<ControlPlaneAuthPrincipal>,
    credential_id: Option<String>,
    credential_version: Option<u64>,
    metrics: ControlPlaneRaftPeerAuthMetricsSnapshot,
}

impl ControlPlaneRaftPeerAuthStatusSnapshot {
    #[must_use]
    pub fn unauthenticated() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn required(&self) -> bool {
        self.required
    }

    #[must_use]
    pub fn local_principal(&self) -> Option<&ControlPlaneAuthPrincipal> {
        self.local_principal.as_ref()
    }

    #[must_use]
    #[cfg(test)]
    pub fn credential_id(&self) -> Option<&str> {
        self.credential_id.as_deref()
    }

    #[must_use]
    pub fn credential_version(&self) -> Option<u64> {
        self.credential_version
    }

    #[must_use]
    pub fn metrics(&self) -> &ControlPlaneRaftPeerAuthMetricsSnapshot {
        &self.metrics
    }
}

#[derive(Debug, Default)]
struct ControlPlaneRaftPeerAuthMetrics {
    state: Mutex<ControlPlaneRaftPeerAuthMetricsState>,
}

#[derive(Debug, Default)]
struct ControlPlaneRaftPeerAuthMetricsState {
    accepted_total: u64,
    rejected_total: u64,
    rejected_without_operation_total: u64,
    accepted_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_reason: BTreeMap<ControlPlaneAuthRejectionReason, u64>,
}

impl ControlPlaneRaftPeerAuthMetrics {
    fn record_accepted(&self, operation: ControlPlaneAuthOperation) {
        let mut state = self.state.lock().expect("peer auth metrics mutex poisoned");
        state.accepted_total = state.accepted_total.saturating_add(1);
        increment_counter(&mut state.accepted_by_operation, operation);
    }

    fn record_rejected(
        &self,
        operation: ControlPlaneAuthOperation,
        reason: ControlPlaneAuthRejectionReason,
    ) {
        let mut state = self.state.lock().expect("peer auth metrics mutex poisoned");
        state.rejected_total = state.rejected_total.saturating_add(1);
        increment_counter(&mut state.rejected_by_operation, operation);
        increment_counter(&mut state.rejected_by_reason, reason);
    }

    fn record_rejected_without_operation(&self, reason: ControlPlaneAuthRejectionReason) {
        let mut state = self.state.lock().expect("peer auth metrics mutex poisoned");
        state.rejected_total = state.rejected_total.saturating_add(1);
        state.rejected_without_operation_total =
            state.rejected_without_operation_total.saturating_add(1);
        increment_counter(&mut state.rejected_by_reason, reason);
    }

    fn snapshot(&self) -> ControlPlaneRaftPeerAuthMetricsSnapshot {
        let state = self.state.lock().expect("peer auth metrics mutex poisoned");
        ControlPlaneRaftPeerAuthMetricsSnapshot {
            accepted_total: state.accepted_total,
            rejected_total: state.rejected_total,
            rejected_without_operation_total: state.rejected_without_operation_total,
            accepted_by_operation: state.accepted_by_operation.clone(),
            rejected_by_operation: state.rejected_by_operation.clone(),
            rejected_by_reason: state.rejected_by_reason.clone(),
        }
    }
}

fn increment_counter<K: Ord>(counters: &mut BTreeMap<K, u64>, key: K) {
    let count = counters.entry(key).or_insert(0);
    *count = count.saturating_add(1);
}

#[derive(Debug, Clone)]
pub(crate) struct ControlPlaneRaftPeerAuthPolicy {
    local_credential: ControlPlaneScopedCredential,
    verifier: ControlPlaneScopedCredentialStore,
    metrics: Arc<ControlPlaneRaftPeerAuthMetrics>,
}

impl ControlPlaneRaftPeerAuthPolicy {
    pub fn new(
        local_credential: ControlPlaneScopedCredential,
        verifier: ControlPlaneScopedCredentialStore,
    ) -> Result<Self, ControlPlaneError> {
        let policy = Self {
            local_credential,
            verifier,
            metrics: Arc::new(ControlPlaneRaftPeerAuthMetrics::default()),
        };
        policy.validate()?;
        Ok(policy)
    }

    #[must_use]
    #[cfg(test)]
    pub fn metrics_snapshot(&self) -> ControlPlaneRaftPeerAuthMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[must_use]
    pub fn status_snapshot(&self) -> ControlPlaneRaftPeerAuthStatusSnapshot {
        ControlPlaneRaftPeerAuthStatusSnapshot {
            required: true,
            local_principal: Some(self.local_credential.principal().clone()),
            credential_id: Some(self.local_credential.credential_id().to_owned()),
            credential_version: Some(self.local_credential.credential_version()),
            metrics: self.metrics.snapshot(),
        }
    }

    pub fn record_peer_frame_rejection_without_operation(
        &self,
        reason: ControlPlaneAuthRejectionReason,
    ) {
        self.metrics.record_rejected_without_operation(reason);
    }

    pub fn record_peer_frame_rejection(
        &self,
        operation: ControlPlaneAuthOperation,
        reason: ControlPlaneAuthRejectionReason,
    ) {
        self.metrics.record_rejected(operation, reason);
    }

    pub(crate) fn sign_peer_frame(
        &self,
        identity: &ControlPlaneRaftPeerFrameIdentity,
        operation: ControlPlaneAuthOperation,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.validate_source(identity)?;
        validate_control_plane_raft_peer_auth_payload_binding(&payload, identity, operation)?;
        let (issued_at_ms, expires_at_ms) = peer_auth_replay_window_for_sign(operation)?;
        let envelope = self
            .local_credential
            .sign_envelope(ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: identity.target,
                }),
                operation,
                issued_at_ms,
                expires_at_ms,
                sequence: None,
                nonce: Vec::new(),
                payload,
            })?;
        envelope.encode_frame()
    }

    pub(crate) fn verify_peer_frame(
        &self,
        envelope_bytes: &[u8],
        expected_identity: &ControlPlaneRaftPeerFrameIdentity,
        expected_operation: ControlPlaneAuthOperation,
        max_payload_bytes: usize,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let envelope =
            match ControlPlaneAuthEnvelope::decode_frame(envelope_bytes, max_payload_bytes) {
                Ok(envelope) => envelope,
                Err(error) => {
                    self.metrics.record_rejected(
                        expected_operation,
                        ControlPlaneAuthRejectionReason::Malformed,
                    );
                    return Err(error);
                }
            };
        let decision = self
            .verifier
            .verify_envelope(ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: &expected_identity.cluster_name,
                expected_source: &ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: expected_identity.source,
                },
                expected_target: &ControlPlaneAuthTarget::Principal(
                    ControlPlaneAuthPrincipal::RaftPeer {
                        node_id: expected_identity.target,
                    },
                ),
                expected_operation,
                replay_policy: peer_auth_replay_policy(expected_operation),
            });
        match decision {
            ControlPlaneAuthDecision::Accepted { .. } => {
                if let Err(error) = validate_control_plane_raft_peer_auth_payload_binding(
                    envelope.payload(),
                    expected_identity,
                    expected_operation,
                ) {
                    self.metrics.record_rejected(
                        expected_operation,
                        ControlPlaneAuthRejectionReason::Malformed,
                    );
                    return Err(error);
                }
                self.metrics.record_accepted(expected_operation);
                Ok(envelope.payload().to_vec())
            }
            ControlPlaneAuthDecision::Rejected { reason } => {
                self.metrics.record_rejected(expected_operation, reason);
                Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane OpenRaft peer auth rejected: {reason:?}"
                )))
            }
        }
    }

    fn validate(&self) -> Result<(), ControlPlaneError> {
        match self.local_credential.principal() {
            ControlPlaneAuthPrincipal::RaftPeer { .. } => Ok(()),
            principal => Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft peer auth local credential must be a RaftPeer principal, not {principal:?}"
            ))),
        }
    }

    fn validate_source(
        &self,
        identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> Result<(), ControlPlaneError> {
        let expected = ControlPlaneAuthPrincipal::RaftPeer {
            node_id: identity.source,
        };
        if self.local_credential.principal() == &expected {
            return Ok(());
        }
        Err(ControlPlaneError::rpc_protocol(format!(
            "control-plane OpenRaft peer auth local credential {:?} cannot sign source node {}",
            self.local_credential.principal(),
            identity.source
        )))
    }
}

fn peer_auth_replay_window_for_sign(
    operation: ControlPlaneAuthOperation,
) -> Result<(Option<u64>, Option<u64>), ControlPlaneError> {
    if operation != ControlPlaneAuthOperation::RaftTransferLeader {
        return Ok((None, None));
    }
    let issued_at_ms = crate::clock::current_time_millis();
    let expires_at_ms = issued_at_ms
        .checked_add(CONTROL_PLANE_RAFT_TRANSFER_LEADER_AUTH_FRESHNESS_MS)
        .ok_or_else(|| {
            ControlPlaneError::rpc_protocol(
                "control-plane OpenRaft transfer-leader auth freshness overflow".to_string(),
            )
        })?;
    Ok((Some(issued_at_ms), Some(expires_at_ms)))
}

fn peer_auth_replay_policy(
    expected_operation: ControlPlaneAuthOperation,
) -> ControlPlaneAuthReplayPolicy {
    if expected_operation != ControlPlaneAuthOperation::RaftTransferLeader {
        return ControlPlaneAuthReplayPolicy::FencedByPayloadSemantics;
    }
    ControlPlaneAuthReplayPolicy::TimestampWindow {
        now_ms: crate::clock::current_time_millis(),
        max_window_ms: CONTROL_PLANE_RAFT_TRANSFER_LEADER_AUTH_FRESHNESS_MS,
        allowed_future_skew_ms: CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ControlPlaneRaftPeerTransportPolicy {
    cluster_name: String,
    topology: Option<ControlPlaneRaftTopologyIdentity>,
    initial_topology_certificate: Option<crate::control_plane::InitialClusterTopologyCertificate>,
    peers: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
    limits: ControlPlaneRaftPeerTransportLimits,
    connect_timeout: Duration,
    io_timeout: Duration,
    auth_policy: Option<Arc<ControlPlaneRaftPeerAuthPolicy>>,
}

impl ControlPlaneRaftPeerTransportPolicy {
    #[must_use]
    pub fn new(
        cluster_name: impl Into<String>,
        peers: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
        limits: ControlPlaneRaftPeerTransportLimits,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            topology: None,
            initial_topology_certificate: None,
            peers,
            limits,
            connect_timeout: Duration::from_secs(1),
            io_timeout: Duration::from_secs(1),
            auth_policy: None,
        }
    }

    #[must_use]
    pub fn from_peer_endpoints(
        cluster_name: impl Into<String>,
        peers: impl IntoIterator<Item = (ControlPlaneRaftNodeId, String)>,
        limits: ControlPlaneRaftPeerTransportLimits,
    ) -> Self {
        Self::new(
            cluster_name,
            peers
                .into_iter()
                .map(|(node_id, endpoint)| (node_id, BasicNode::new(endpoint)))
                .collect(),
            limits,
        )
    }

    #[must_use]
    pub fn limits(&self) -> ControlPlaneRaftPeerTransportLimits {
        self.limits
    }

    #[must_use]
    pub fn topology_identity(&self) -> Option<&ControlPlaneRaftTopologyIdentity> {
        self.topology.as_ref()
    }

    #[must_use]
    pub(crate) fn initial_topology_certificate(
        &self,
    ) -> Option<&crate::control_plane::InitialClusterTopologyCertificate> {
        self.initial_topology_certificate.as_ref()
    }

    #[must_use]
    pub(crate) fn with_auth_policy(mut self, auth_policy: ControlPlaneRaftPeerAuthPolicy) -> Self {
        self.auth_policy = Some(Arc::new(auth_policy));
        self
    }

    #[must_use]
    pub fn with_topology_identity(mut self, generation: u64, digest: impl Into<String>) -> Self {
        self.topology = Some(ControlPlaneRaftTopologyIdentity {
            generation,
            digest: digest.into(),
        });
        self
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn with_initial_topology_certificate(
        mut self,
        certificate: crate::control_plane::InitialClusterTopologyCertificate,
    ) -> Self {
        self.initial_topology_certificate = Some(certificate);
        self
    }

    /// Bind a storage-owned static initial topology to Raft peer admission.
    #[must_use]
    pub fn with_static_initial_topology(
        mut self,
        topology: &StaticInitialControlPlaneTopology,
    ) -> Self {
        let certificate = topology.certificate();
        self.topology = Some(ControlPlaneRaftTopologyIdentity {
            generation: certificate.topology_generation(),
            digest: certificate
                .topology_digest()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        });
        self.initial_topology_certificate = Some(certificate.clone());
        self
    }

    #[must_use]
    pub fn with_timeouts(mut self, connect_timeout: Duration, io_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self.io_timeout = io_timeout;
        self
    }

    #[must_use]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    #[must_use]
    pub fn auth_policy(&self) -> Option<&ControlPlaneRaftPeerAuthPolicy> {
        self.auth_policy.as_deref()
    }

    #[must_use]
    pub fn auth_status_snapshot(&self) -> ControlPlaneRaftPeerAuthStatusSnapshot {
        self.auth_policy.as_deref().map_or_else(
            ControlPlaneRaftPeerAuthStatusSnapshot::unauthenticated,
            |policy| policy.status_snapshot(),
        )
    }

    #[must_use]
    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    #[must_use]
    pub fn peers(&self) -> BTreeMap<ControlPlaneRaftNodeId, BasicNode> {
        self.peers.clone()
    }

    pub fn validate_local_node(
        &self,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        if self.peers.contains_key(&local_node_id) {
            return Ok(());
        }
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer transport policy for cluster {:?} does not include local node {local_node_id}",
            self.cluster_name
        )))
    }

    pub fn validate_cluster_name(
        &self,
        expected_cluster_name: &str,
    ) -> Result<(), ControlPlaneError> {
        if self.cluster_name == expected_cluster_name {
            return Ok(());
        }
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer transport policy belongs to cluster {:?}, not configured cluster {:?}",
            self.cluster_name, expected_cluster_name
        )))
    }

    pub fn validate_replication_compatibility(&self) -> Result<(), ControlPlaneError> {
        if let Some(certificate) = &self.initial_topology_certificate {
            let topology = self.topology.as_ref().ok_or_else(|| {
                raft_artifact_protocol_error(
                    "initial topology certificate requires a peer-policy topology identity",
                )
            })?;
            let certificate_digest = certificate
                .topology_digest()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let voters = self.peers.keys().copied().collect::<Vec<_>>();
            if certificate.topology_generation() != topology.generation
                || certificate_digest != topology.digest
                || certificate.raft_voters() != voters
            {
                return Err(raft_artifact_protocol_error(
                    "initial topology certificate does not match peer-policy topology identity and voters",
                ));
            }
        }
        if self.limits.max_append_entries
            < ControlPlaneRaftPeerTransportLimits::REPLICATION_REQUIRED_APPEND_ENTRIES
        {
            return Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft peer transport policy permits {} append entries, below the configured replication batch size {}",
                self.limits.max_append_entries,
                ControlPlaneRaftPeerTransportLimits::REPLICATION_REQUIRED_APPEND_ENTRIES
            )));
        }
        if self.limits.max_append_entries_bytes
            < ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES_BYTES
        {
            return Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft peer transport policy permits {} append-entry bytes, below the required replication payload size {}",
                self.limits.max_append_entries_bytes,
                ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES_BYTES
            )));
        }
        if self.limits.max_frame_bytes
            < ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES
        {
            return Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft peer transport policy permits {} frame bytes, below the required replication frame size {}",
                self.limits.max_frame_bytes,
                ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES
            )));
        }
        if !self.peers.is_empty() {
            let voters = self.peers.keys().copied().collect::<BTreeSet<_>>();
            let membership = Membership::new(vec![voters.clone(), voters], self.peers.clone())
                .map_err(|error| {
                    raft_artifact_protocol_error(format!(
                    "control-plane OpenRaft peer transport policy membership is invalid: {error}"
                ))
                })?;
            let entry = ControlPlaneRaftEntry {
                log_id: LogId::new(
                    LeaderId {
                        term: u64::MAX,
                        node_id: u64::MAX,
                    },
                    u64::MAX,
                ),
                payload: EntryPayload::Membership(membership),
            };
            let mut encoded = Vec::new();
            write_raft_entry(&mut encoded, &entry)?;
            if encoded.len() > CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES {
                return Err(raft_artifact_protocol_error(format!(
                    "control-plane OpenRaft peer transport policy membership encodes to {} entry bytes, exceeding the replication-safe per-entry limit {}",
                    encoded.len(), CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES
                )));
            }
        }
        Ok(())
    }

    pub fn validate_configured_membership(
        &self,
        context: &'static str,
        membership: &Membership<ControlPlaneRaftNodeId, BasicNode>,
    ) -> Result<(), ControlPlaneError> {
        let expected = Membership::from(self.peers.clone());
        if membership == &expected {
            return Ok(());
        }
        let expected_voters = expected.voter_ids().collect::<BTreeSet<_>>();
        let actual_voters = membership.voter_ids().collect::<BTreeSet<_>>();
        let actual_learners = membership.learner_ids().collect::<BTreeSet<_>>();
        let actual_nodes = membership
            .nodes()
            .map(|(node_id, node)| (*node_id, node.clone()))
            .collect::<BTreeMap<_, _>>();
        Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft durable restart artifact {context} does not match configured peer map for cluster {:?}; expected_voters={expected_voters:?} actual_voters={actual_voters:?} actual_learners={actual_learners:?} expected_nodes={:?} actual_nodes={actual_nodes:?}",
            self.cluster_name, self.peers
        )))
    }

    pub(crate) fn validate_target_node(
        &self,
        target: ControlPlaneRaftNodeId,
        node: &BasicNode,
        rpc_name: &'static str,
    ) -> Result<(), ControlPlaneRaftPeerTransportRejection> {
        let expected = self.peers.get(&target).ok_or_else(|| {
            ControlPlaneRaftPeerTransportRejection::UnknownTarget {
                cluster_name: self.cluster_name.clone(),
                target,
                rpc_name,
            }
        })?;
        if expected == node {
            Ok(())
        } else {
            Err(ControlPlaneRaftPeerTransportRejection::EndpointMismatch {
                cluster_name: self.cluster_name.clone(),
                target,
                rpc_name,
                expected: expected.addr.clone(),
                actual: node.addr.clone(),
            })
        }
    }

    pub(crate) fn frame_identity(
        &self,
        source: ControlPlaneRaftNodeId,
        target: ControlPlaneRaftNodeId,
    ) -> Result<ControlPlaneRaftPeerFrameIdentity, ControlPlaneRaftPeerTransportRejection> {
        if !self.peers.contains_key(&source) {
            return Err(ControlPlaneRaftPeerTransportRejection::UnknownSource {
                cluster_name: self.cluster_name.clone(),
                source,
            });
        }
        if !self.peers.contains_key(&target) {
            return Err(ControlPlaneRaftPeerTransportRejection::UnknownTarget {
                cluster_name: self.cluster_name.clone(),
                target,
                rpc_name: "peer_frame",
            });
        }
        let mut identity =
            ControlPlaneRaftPeerFrameIdentity::new(self.cluster_name.clone(), source, target);
        identity.topology.clone_from(&self.topology);
        Ok(identity)
    }

    pub(crate) fn validate_incoming_frame_identity(
        &self,
        identity: &ControlPlaneRaftPeerFrameIdentity,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneRaftPeerTransportRejection> {
        if identity.cluster_name != self.cluster_name {
            return Err(ControlPlaneRaftPeerTransportRejection::ClusterMismatch {
                expected: self.cluster_name.clone(),
                actual: identity.cluster_name.clone(),
            });
        }
        if identity.topology != self.topology {
            return Err(ControlPlaneRaftPeerTransportRejection::TopologyMismatch {
                cluster_name: self.cluster_name.clone(),
                expected: self.topology.clone(),
                actual: identity.topology.clone(),
            });
        }
        if identity.target != local_node_id {
            return Err(ControlPlaneRaftPeerTransportRejection::UnexpectedTarget {
                cluster_name: self.cluster_name.clone(),
                expected: local_node_id,
                actual: identity.target,
            });
        }
        if !self.peers.contains_key(&identity.source) {
            return Err(ControlPlaneRaftPeerTransportRejection::UnknownSource {
                cluster_name: self.cluster_name.clone(),
                source: identity.source,
            });
        }
        if !self.peers.contains_key(&identity.target) {
            return Err(ControlPlaneRaftPeerTransportRejection::UnknownTarget {
                cluster_name: self.cluster_name.clone(),
                target: identity.target,
                rpc_name: "peer_frame",
            });
        }
        Ok(())
    }

    pub(crate) fn validate_append_entries(
        &self,
        target: ControlPlaneRaftNodeId,
        entries_len: usize,
        entries_bytes: usize,
    ) -> Result<(), ControlPlaneRaftPeerTransportRejection> {
        if entries_len > self.limits.max_append_entries {
            return Err(
                ControlPlaneRaftPeerTransportRejection::AppendEntriesBatchTooLarge {
                    cluster_name: self.cluster_name.clone(),
                    target,
                    entries_len,
                    max_append_entries: self.limits.max_append_entries,
                },
            );
        }
        if entries_bytes > self.limits.max_append_entries_bytes {
            return Err(
                ControlPlaneRaftPeerTransportRejection::AppendEntriesPayloadTooLarge {
                    cluster_name: self.cluster_name.clone(),
                    target,
                    entries_bytes,
                    max_append_entries_bytes: self.limits.max_append_entries_bytes,
                },
            );
        }
        Ok(())
    }

    pub(crate) fn validate_snapshot(
        &self,
        target: ControlPlaneRaftNodeId,
        snapshot_bytes: usize,
    ) -> Result<(), ControlPlaneRaftPeerTransportRejection> {
        if snapshot_bytes <= self.limits.max_snapshot_bytes {
            Ok(())
        } else {
            Err(ControlPlaneRaftPeerTransportRejection::SnapshotTooLarge {
                cluster_name: self.cluster_name.clone(),
                target,
                snapshot_bytes,
                max_snapshot_bytes: self.limits.max_snapshot_bytes,
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ControlPlaneRaftPeerTransportRejection {
    ClusterMismatch {
        expected: String,
        actual: String,
    },
    TopologyMismatch {
        cluster_name: String,
        expected: Option<ControlPlaneRaftTopologyIdentity>,
        actual: Option<ControlPlaneRaftTopologyIdentity>,
    },
    UnknownSource {
        cluster_name: String,
        source: ControlPlaneRaftNodeId,
    },
    UnknownTarget {
        cluster_name: String,
        target: ControlPlaneRaftNodeId,
        rpc_name: &'static str,
    },
    EndpointMismatch {
        cluster_name: String,
        target: ControlPlaneRaftNodeId,
        rpc_name: &'static str,
        expected: String,
        actual: String,
    },
    UnexpectedTarget {
        cluster_name: String,
        expected: ControlPlaneRaftNodeId,
        actual: ControlPlaneRaftNodeId,
    },
    AppendEntriesBatchTooLarge {
        cluster_name: String,
        target: ControlPlaneRaftNodeId,
        entries_len: usize,
        max_append_entries: usize,
    },
    AppendEntriesPayloadTooLarge {
        cluster_name: String,
        target: ControlPlaneRaftNodeId,
        entries_bytes: usize,
        max_append_entries_bytes: usize,
    },
    SnapshotTooLarge {
        cluster_name: String,
        target: ControlPlaneRaftNodeId,
        snapshot_bytes: usize,
        max_snapshot_bytes: usize,
    },
}

impl fmt::Display for ControlPlaneRaftPeerTransportRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClusterMismatch { expected, actual } => write!(
                f,
                "control-plane raft peer transport cluster identity mismatch: expected {expected}, got {actual}",
            ),
            Self::TopologyMismatch {
                cluster_name,
                expected,
                actual,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} topology identity mismatch: expected {expected:?}, got {actual:?}",
            ),
            Self::UnknownSource {
                cluster_name,
                source,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} has no configured source node {source}",
            ),
            Self::UnknownTarget {
                cluster_name,
                target,
                rpc_name,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} has no configured target node {target} for {rpc_name}",
            ),
            Self::EndpointMismatch {
                cluster_name,
                target,
                rpc_name,
                expected,
                actual,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} rejected {rpc_name} to node {target}: endpoint mismatch, expected {expected}, got {actual}",
            ),
            Self::UnexpectedTarget {
                cluster_name,
                expected,
                actual,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} rejected incoming peer frame: expected target node {expected}, got {actual}",
            ),
            Self::AppendEntriesBatchTooLarge {
                cluster_name,
                target,
                entries_len,
                max_append_entries,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} rejected append_entries to node {target}: {entries_len} entries exceeds limit {max_append_entries}",
            ),
            Self::AppendEntriesPayloadTooLarge {
                cluster_name,
                target,
                entries_bytes,
                max_append_entries_bytes,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} rejected append_entries to node {target}: encoded entries payload {entries_bytes} bytes exceeds limit {max_append_entries_bytes}",
            ),
            Self::SnapshotTooLarge {
                cluster_name,
                target,
                snapshot_bytes,
                max_snapshot_bytes,
            } => write!(
                f,
                "control-plane raft peer transport cluster {cluster_name} rejected full_snapshot to node {target}: {snapshot_bytes} bytes exceeds limit {max_snapshot_bytes}",
            ),
        }
    }
}

impl std::error::Error for ControlPlaneRaftPeerTransportRejection {}

fn raft_rpc_error_from_transport_rejection(
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

fn raft_streaming_error_from_transport_rejection(
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

fn raft_streaming_error_from_rpc_error(
    error: RPCError<ControlPlaneRaftTypeConfig>,
) -> StreamingError<ControlPlaneRaftTypeConfig> {
    match error {
        RPCError::Timeout(error) => StreamingError::Network(NetworkError::from_string(format!(
            "control-plane OpenRaft peer validation timed out: {error}"
        ))),
        RPCError::Unreachable(error) => StreamingError::Unreachable(error),
        RPCError::Network(error) => StreamingError::Network(error),
        RPCError::RemoteError(error) => StreamingError::Network(NetworkError::from_string(
            format!("control-plane OpenRaft peer validation remote error: {error}"),
        )),
    }
}

fn raft_rpc_protocol_error(
    context: &'static str,
    error: ControlPlaneError,
) -> RPCError<ControlPlaneRaftTypeConfig> {
    RPCError::Network(NetworkError::from_string(format!(
        "control-plane OpenRaft {context} peer frame failed: {error:?}"
    )))
}

fn raft_streaming_protocol_error(
    context: &'static str,
    error: ControlPlaneError,
) -> StreamingError<ControlPlaneRaftTypeConfig> {
    StreamingError::Network(NetworkError::from_string(format!(
        "control-plane OpenRaft {context} peer frame failed: {error:?}"
    )))
}

fn raft_peer_io_rpc_error(
    transport_name: &'static str,
    context: &'static str,
    target: ControlPlaneRaftNodeId,
    source: io::Error,
) -> RPCError<ControlPlaneRaftTypeConfig> {
    let message = format!(
        "control-plane OpenRaft {transport_name} peer transport {context} for node {target} failed: {source}"
    );
    match source.kind() {
        io::ErrorKind::NotFound
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::NotConnected
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::UnexpectedEof
        | io::ErrorKind::WouldBlock
        | io::ErrorKind::TimedOut => {
            RPCError::Unreachable(Unreachable::new(&AnyError::error(message)))
        }
        _ => RPCError::Network(NetworkError::from_string(message)),
    }
}

pub(super) fn raft_peer_transport_rpc_error(
    transport_name: &'static str,
    target: ControlPlaneRaftNodeId,
    error: ControlPlaneRaftPeerFrameExchangeError,
) -> RPCError<ControlPlaneRaftTypeConfig> {
    let context = error.context;
    match *error.error {
        ControlPlaneError::Io { diagnostic: source } => {
            raft_peer_io_rpc_error(transport_name, context, target, source.into_source())
        }
        error => raft_rpc_protocol_error(context, error),
    }
}

pub(super) struct ControlPlaneRaftPeerFrameExchange {
    pub(super) target: ControlPlaneRaftNodeId,
    pub(super) endpoint: String,
    pub(super) request_frame: Vec<u8>,
    pub(super) max_frame_bytes: usize,
    pub(super) connect_timeout: Duration,
    pub(super) deadline: Instant,
    pub(super) context_prefix: &'static str,
}

impl fmt::Debug for ControlPlaneRaftPeerFrameExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftPeerFrameExchange")
            .field("target", &self.target)
            .field("endpoint", &self.endpoint)
            .field("request_frame_len", &self.request_frame.len())
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("connect_timeout", &self.connect_timeout)
            .field("deadline", &self.deadline)
            .field("context_prefix", &self.context_prefix)
            .finish()
    }
}

#[derive(Debug)]
pub(super) struct ControlPlaneRaftPeerFrameExchangeError {
    context: &'static str,
    error: Box<ControlPlaneError>,
}

impl ControlPlaneRaftPeerFrameExchangeError {
    pub(super) fn new(context: &'static str, error: ControlPlaneError) -> Self {
        Self {
            context,
            error: Box::new(error),
        }
    }
}

/// A storage-owned endpoint for an OpenRaft peer client.
///
/// Callers supply deployment addresses and trust roots. Storage owns the TLS
/// profile, ALPN, framing, absolute deadlines, and transport diagnostics.
#[derive(Clone)]
pub struct ControlPlaneRaftPeerClientEndpoint {
    advertised_endpoint: String,
    kind: ControlPlaneRaftPeerClientEndpointKind,
}

#[derive(Clone)]
enum ControlPlaneRaftPeerClientEndpointKind {
    Unix {
        socket_path: PathBuf,
    },
    TlsTcp {
        host: String,
        port: u16,
        server_name: String,
        tls_client_config: Arc<rustls::ClientConfig>,
    },
}

impl fmt::Debug for ControlPlaneRaftPeerClientEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let transport = match self.kind {
            ControlPlaneRaftPeerClientEndpointKind::Unix { .. } => "Unix",
            ControlPlaneRaftPeerClientEndpointKind::TlsTcp { .. } => "TLS/TCP",
        };
        f.debug_struct("ControlPlaneRaftPeerClientEndpoint")
            .field("advertised_endpoint", &self.advertised_endpoint)
            .field("transport", &transport)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlPlaneRaftPeerClientEndpointError {
    #[error("control-plane Raft TLS/TCP advertised endpoint must not be empty")]
    EmptyAdvertisedEndpoint,
    #[error("control-plane Raft TLS/TCP host must not be empty")]
    EmptyHost,
    #[error("control-plane Raft TLS/TCP endpoint has an invalid TLS server name")]
    InvalidServerName,
    #[error("failed to construct the control-plane Raft TLS client profile")]
    TlsProfileUnavailable,
    #[error("control-plane Raft peer client endpoint map must not be empty")]
    EmptyEndpointMap,
    #[error("control-plane Raft peer client endpoint map contains duplicate node {0}")]
    DuplicateNode(ControlPlaneRaftNodeId),
}

impl ControlPlaneRaftPeerClientEndpoint {
    #[must_use]
    pub fn unix(socket_path: impl Into<PathBuf>) -> Self {
        let socket_path = socket_path.into();
        Self {
            advertised_endpoint: socket_path.to_string_lossy().into_owned(),
            kind: ControlPlaneRaftPeerClientEndpointKind::Unix { socket_path },
        }
    }

    pub fn tls_tcp(
        advertised_endpoint: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        server_name: impl Into<String>,
        trust_roots: rustls::RootCertStore,
    ) -> Result<Self, ControlPlaneRaftPeerClientEndpointError> {
        let advertised_endpoint = advertised_endpoint.into();
        if advertised_endpoint.is_empty() {
            return Err(ControlPlaneRaftPeerClientEndpointError::EmptyAdvertisedEndpoint);
        }
        let host = host.into();
        if host.is_empty() {
            return Err(ControlPlaneRaftPeerClientEndpointError::EmptyHost);
        }
        let server_name = server_name.into();
        ServerName::try_from(server_name.clone())
            .map_err(|_| ControlPlaneRaftPeerClientEndpointError::InvalidServerName)?;
        let mut tls_client_config =
            rustls::ClientConfig::builder_with_provider(tls_provider::configured_provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|_| ControlPlaneRaftPeerClientEndpointError::TlsProfileUnavailable)?
                .with_root_certificates(trust_roots)
                .with_no_client_auth();
        tls_client_config.alpn_protocols = vec![CONTROL_PLANE_RAFT_TLS_ALPN.to_vec()];
        Ok(Self {
            advertised_endpoint,
            kind: ControlPlaneRaftPeerClientEndpointKind::TlsTcp {
                host,
                port,
                server_name,
                tls_client_config: Arc::new(tls_client_config),
            },
        })
    }

    #[must_use]
    pub fn advertised_endpoint(&self) -> &str {
        &self.advertised_endpoint
    }

    #[must_use]
    pub fn transport_name(&self) -> &'static str {
        match self.kind {
            ControlPlaneRaftPeerClientEndpointKind::Unix { .. } => "Unix",
            ControlPlaneRaftPeerClientEndpointKind::TlsTcp { .. } => "TLS/TCP",
        }
    }
}

#[derive(Clone)]
enum ControlPlaneRaftPeerNetworkTransport {
    ImplicitUnix,
    Configured(Arc<BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftPeerClientEndpoint>>),
}

#[derive(Clone)]
pub(crate) struct ControlPlaneRaftPeerNetworkConfig {
    rpc_timeout: Duration,
    transport: ControlPlaneRaftPeerNetworkTransport,
}

impl fmt::Debug for ControlPlaneRaftPeerNetworkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftPeerNetworkConfig")
            .field("rpc_timeout", &self.rpc_timeout)
            .field("transport", &self.transport_name())
            .finish()
    }
}

impl ControlPlaneRaftPeerNetworkConfig {
    #[must_use]
    pub fn unix(rpc_timeout: Duration) -> Self {
        Self {
            rpc_timeout,
            transport: ControlPlaneRaftPeerNetworkTransport::ImplicitUnix,
        }
    }

    pub fn with_peer_endpoints(
        rpc_timeout: Duration,
        endpoints: impl IntoIterator<
            Item = (ControlPlaneRaftNodeId, ControlPlaneRaftPeerClientEndpoint),
        >,
    ) -> Result<Self, ControlPlaneRaftPeerClientEndpointError> {
        let mut configured = BTreeMap::new();
        for (node_id, endpoint) in endpoints {
            if configured.insert(node_id, endpoint).is_some() {
                return Err(ControlPlaneRaftPeerClientEndpointError::DuplicateNode(
                    node_id,
                ));
            }
        }
        if configured.is_empty() {
            return Err(ControlPlaneRaftPeerClientEndpointError::EmptyEndpointMap);
        }
        Ok(Self {
            rpc_timeout,
            transport: ControlPlaneRaftPeerNetworkTransport::Configured(Arc::new(configured)),
        })
    }

    fn transport_name(&self) -> &'static str {
        match &self.transport {
            ControlPlaneRaftPeerNetworkTransport::ImplicitUnix => "Unix",
            ControlPlaneRaftPeerNetworkTransport::Configured(_) => "configured endpoints",
        }
    }

    pub(crate) fn validate_policy(
        &self,
        policy: &ControlPlaneRaftPeerTransportPolicy,
    ) -> Result<(), ControlPlaneError> {
        let ControlPlaneRaftPeerNetworkTransport::Configured(endpoints) = &self.transport else {
            return Ok(());
        };
        let policy_peers = policy.peers();
        let configured_nodes = endpoints.keys().copied().collect::<BTreeSet<_>>();
        let policy_nodes = policy_peers.keys().copied().collect::<BTreeSet<_>>();
        if configured_nodes != policy_nodes {
            return Err(raft_artifact_protocol_error(format!(
                "control-plane OpenRaft client endpoint nodes {configured_nodes:?} do not match peer-policy nodes {policy_nodes:?}"
            )));
        }
        for (node_id, endpoint) in endpoints.iter() {
            let advertised = &policy_peers
                .get(node_id)
                .expect("equal peer node sets contain every configured endpoint")
                .addr;
            if endpoint.advertised_endpoint() != advertised {
                return Err(raft_artifact_protocol_error(format!(
                    "control-plane OpenRaft client endpoint for node {node_id} does not match its peer-policy address"
                )));
            }
        }
        Ok(())
    }

    pub(super) fn rpc_timeout(&self) -> Duration {
        self.rpc_timeout
    }

    pub(super) fn frame_transport(&self) -> Arc<dyn ControlPlaneRaftPeerFrameTransport> {
        match &self.transport {
            ControlPlaneRaftPeerNetworkTransport::ImplicitUnix => {
                Arc::new(ControlPlaneRaftUnixPeerFrameTransport)
            }
            ControlPlaneRaftPeerNetworkTransport::Configured(endpoints) => {
                Arc::new(ControlPlaneRaftConfiguredPeerFrameTransport {
                    endpoints: Arc::clone(endpoints),
                })
            }
        }
    }
}

pub(super) trait ControlPlaneRaftPeerFrameTransport:
    fmt::Debug + Send + Sync + 'static
{
    fn name(&self, target: ControlPlaneRaftNodeId) -> &'static str;

    fn exchange(
        &self,
        exchange: ControlPlaneRaftPeerFrameExchange,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<u8>, ControlPlaneRaftPeerFrameExchangeError>>
                + Send
                + '_,
        >,
    >;
}

#[derive(Debug, Default)]
pub(super) struct ControlPlaneRaftUnixPeerFrameTransport;

fn raft_peer_exchange_context(context_prefix: &'static str, phase: &'static str) -> &'static str {
    match (context_prefix, phase) {
        ("", "write") => "write transport",
        ("", "read") => "read transport",
        ("", "task") => "blocking task",
        ("full_snapshot ", "write") => "full_snapshot write transport",
        ("full_snapshot ", "read") => "full_snapshot read transport",
        ("full_snapshot ", "task") => "full_snapshot blocking task",
        (_, "connect") => "connect",
        _ => "peer transport",
    }
}

fn exchange_unix_raft_peer_frame(
    exchange: ControlPlaneRaftPeerFrameExchange,
) -> Result<Vec<u8>, ControlPlaneRaftPeerFrameExchangeError> {
    let connect_deadline = Instant::now()
        .checked_add(exchange.connect_timeout)
        .map_or(exchange.deadline, |configured| {
            configured.min(exchange.deadline)
        });
    let mut stream = connect_unix_stream_until(Path::new(&exchange.endpoint), connect_deadline)
        .map_err(|source| {
            ControlPlaneRaftPeerFrameExchangeError::new(
                raft_peer_exchange_context(exchange.context_prefix, "connect"),
                ControlPlaneError::io("connect control-plane OpenRaft Unix peer transport", source),
            )
        })?;
    let mut stream = DeadlineUnixStream::new(
        &mut stream,
        exchange.deadline,
        "control-plane OpenRaft peer exchange deadline expired",
    )
    .map_err(|source| {
        ControlPlaneRaftPeerFrameExchangeError::new(
            raft_peer_exchange_context(exchange.context_prefix, "connect"),
            ControlPlaneError::io(
                "configure control-plane OpenRaft Unix peer deadline I/O",
                source,
            ),
        )
    })?;
    write_control_plane_raft_peer_transport_frame(&mut stream, &exchange.request_frame).map_err(
        |error| {
            ControlPlaneRaftPeerFrameExchangeError::new(
                raft_peer_exchange_context(exchange.context_prefix, "write"),
                error,
            )
        },
    )?;
    read_control_plane_raft_peer_transport_frame(&mut stream, exchange.max_frame_bytes).map_err(
        |error| {
            ControlPlaneRaftPeerFrameExchangeError::new(
                raft_peer_exchange_context(exchange.context_prefix, "read"),
                error,
            )
        },
    )
}

impl ControlPlaneRaftPeerFrameTransport for ControlPlaneRaftUnixPeerFrameTransport {
    fn name(&self, _target: ControlPlaneRaftNodeId) -> &'static str {
        "Unix"
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
            let context = raft_peer_exchange_context(exchange.context_prefix, "task");
            tokio::task::spawn_blocking(move || exchange_unix_raft_peer_frame(exchange))
                .await
                .map_err(|error| {
                    ControlPlaneRaftPeerFrameExchangeError::new(
                        context,
                        raft_artifact_protocol_error(format!(
                            "control-plane OpenRaft Unix peer transport blocking task failed: {error}"
                        )),
                    )
                })?
        })
    }
}

struct ControlPlaneRaftDeadlineTcpStream {
    stream: TcpStream,
    deadline: Instant,
}

impl ControlPlaneRaftDeadlineTcpStream {
    fn apply_deadline(&self) -> io::Result<()> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "control-plane OpenRaft TLS/TCP peer deadline expired",
            ));
        }
        self.stream.set_read_timeout(Some(remaining))?;
        self.stream.set_write_timeout(Some(remaining))
    }
}

impl Read for ControlPlaneRaftDeadlineTcpStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.apply_deadline()?;
        self.stream.read(buffer)
    }
}

impl Write for ControlPlaneRaftDeadlineTcpStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.apply_deadline()?;
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.apply_deadline()?;
        self.stream.flush()
    }
}

fn configured_raft_peer_exchange_context(
    context_prefix: &'static str,
    phase: &'static str,
) -> &'static str {
    match (context_prefix, phase) {
        ("", "write") => "write transport",
        ("", "read") => "read transport",
        ("full_snapshot ", "write") => "full_snapshot write transport",
        ("full_snapshot ", "read") => "full_snapshot read transport",
        (_, "connect") => "connect",
        (_, "tls") => "TLS handshake",
        (_, "task") => "blocking task",
        _ => "peer transport",
    }
}

fn configured_raft_peer_io_error(
    context: &'static str,
    source: io::Error,
) -> ControlPlaneRaftPeerFrameExchangeError {
    ControlPlaneRaftPeerFrameExchangeError::new(
        context,
        ControlPlaneError::io(
            "exchange control-plane OpenRaft configured peer frame",
            source,
        ),
    )
}

fn exchange_tls_raft_peer_frame_after_connect(
    endpoint: ControlPlaneRaftPeerClientEndpoint,
    stream: TcpStream,
    exchange: ControlPlaneRaftPeerFrameExchange,
) -> Result<Vec<u8>, ControlPlaneRaftPeerFrameExchangeError> {
    let ControlPlaneRaftPeerClientEndpointKind::TlsTcp {
        server_name,
        tls_client_config,
        ..
    } = endpoint.kind
    else {
        return Err(ControlPlaneRaftPeerFrameExchangeError::new(
            configured_raft_peer_exchange_context(exchange.context_prefix, "tls"),
            raft_artifact_protocol_error(
                "control-plane OpenRaft configured peer transport kind changed during exchange",
            ),
        ));
    };
    stream.set_nodelay(true).map_err(|source| {
        configured_raft_peer_io_error(
            configured_raft_peer_exchange_context(exchange.context_prefix, "connect"),
            source,
        )
    })?;
    let server_name = ServerName::try_from(server_name).map_err(|_| {
        ControlPlaneRaftPeerFrameExchangeError::new(
            configured_raft_peer_exchange_context(exchange.context_prefix, "tls"),
            raft_artifact_protocol_error(
                "control-plane OpenRaft TLS/TCP peer has an invalid TLS server name",
            ),
        )
    })?;
    let connection =
        rustls::ClientConnection::new(tls_client_config, server_name).map_err(|error| {
            ControlPlaneRaftPeerFrameExchangeError::new(
                configured_raft_peer_exchange_context(exchange.context_prefix, "tls"),
                raft_artifact_protocol_error(format!(
                    "failed to initialize control-plane OpenRaft TLS peer client: {error}"
                )),
            )
        })?;
    let socket = ControlPlaneRaftDeadlineTcpStream {
        stream,
        deadline: exchange.deadline,
    };
    let mut stream = rustls::StreamOwned::new(connection, socket);
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|source| {
                configured_raft_peer_io_error(
                    configured_raft_peer_exchange_context(exchange.context_prefix, "tls"),
                    source,
                )
            })?;
    }
    if stream.conn.alpn_protocol() != Some(CONTROL_PLANE_RAFT_TLS_ALPN) {
        return Err(ControlPlaneRaftPeerFrameExchangeError::new(
            configured_raft_peer_exchange_context(exchange.context_prefix, "tls"),
            raft_artifact_protocol_error(
                "control-plane OpenRaft TLS peer did not negotiate the required protocol profile",
            ),
        ));
    }
    write_control_plane_raft_peer_transport_frame(&mut stream, &exchange.request_frame).map_err(
        |error| {
            ControlPlaneRaftPeerFrameExchangeError::new(
                configured_raft_peer_exchange_context(exchange.context_prefix, "write"),
                error,
            )
        },
    )?;
    read_control_plane_raft_peer_transport_frame(&mut stream, exchange.max_frame_bytes).map_err(
        |error| {
            ControlPlaneRaftPeerFrameExchangeError::new(
                configured_raft_peer_exchange_context(exchange.context_prefix, "read"),
                error,
            )
        },
    )
}

#[derive(Clone)]
pub(super) struct ControlPlaneRaftConfiguredPeerFrameTransport {
    pub(super) endpoints: Arc<BTreeMap<ControlPlaneRaftNodeId, ControlPlaneRaftPeerClientEndpoint>>,
}

impl fmt::Debug for ControlPlaneRaftConfiguredPeerFrameTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftConfiguredPeerFrameTransport")
            .field("endpoint_count", &self.endpoints.len())
            .finish()
    }
}

impl ControlPlaneRaftPeerFrameTransport for ControlPlaneRaftConfiguredPeerFrameTransport {
    fn name(&self, target: ControlPlaneRaftNodeId) -> &'static str {
        self.endpoints.get(&target).map_or(
            "configured",
            ControlPlaneRaftPeerClientEndpoint::transport_name,
        )
    }

    fn exchange(
        &self,
        mut exchange: ControlPlaneRaftPeerFrameExchange,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<u8>, ControlPlaneRaftPeerFrameExchangeError>>
                + Send
                + '_,
        >,
    > {
        let endpoint = self.endpoints.get(&exchange.target).cloned();
        Box::pin(async move {
            let endpoint = endpoint.ok_or_else(|| {
                ControlPlaneRaftPeerFrameExchangeError::new(
                    configured_raft_peer_exchange_context(exchange.context_prefix, "connect"),
                    raft_artifact_protocol_error(format!(
                        "control-plane OpenRaft configured peer transport has no endpoint for node {}",
                        exchange.target
                    )),
                )
            })?;
            if endpoint.advertised_endpoint != exchange.endpoint {
                return Err(ControlPlaneRaftPeerFrameExchangeError::new(
                    configured_raft_peer_exchange_context(exchange.context_prefix, "connect"),
                    raft_artifact_protocol_error(format!(
                        "control-plane OpenRaft configured endpoint mismatch for node {}",
                        exchange.target
                    )),
                ));
            }
            match &endpoint.kind {
                ControlPlaneRaftPeerClientEndpointKind::Unix { socket_path } => {
                    exchange.endpoint = socket_path.to_string_lossy().into_owned();
                    let context =
                        configured_raft_peer_exchange_context(exchange.context_prefix, "task");
                    tokio::task::spawn_blocking(move || exchange_unix_raft_peer_frame(exchange))
                        .await
                        .map_err(|error| {
                            ControlPlaneRaftPeerFrameExchangeError::new(
                                context,
                                raft_artifact_protocol_error(format!(
                                    "control-plane OpenRaft Unix peer transport blocking task failed: {error}"
                                )),
                            )
                        })?
                }
                ControlPlaneRaftPeerClientEndpointKind::TlsTcp { host, port, .. } => {
                    if exchange.request_frame.len() > exchange.max_frame_bytes {
                        return Err(ControlPlaneRaftPeerFrameExchangeError::new(
                            configured_raft_peer_exchange_context(
                                exchange.context_prefix,
                                "write",
                            ),
                            raft_artifact_protocol_error(format!(
                                "control-plane OpenRaft TLS/TCP request frame size {} bytes exceeds limit {}",
                                exchange.request_frame.len(),
                                exchange.max_frame_bytes
                            )),
                        ));
                    }
                    let connect_deadline = Instant::now()
                        .checked_add(exchange.connect_timeout)
                        .unwrap_or(exchange.deadline)
                        .min(exchange.deadline);
                    let stream =
                        connect_tcp_stream_until_async(host.clone(), *port, connect_deadline)
                            .await
                            .map_err(|source| {
                                configured_raft_peer_io_error(
                                    configured_raft_peer_exchange_context(
                                        exchange.context_prefix,
                                        "connect",
                                    ),
                                    source,
                                )
                            })?;
                    let context =
                        configured_raft_peer_exchange_context(exchange.context_prefix, "task");
                    tokio::task::spawn_blocking(move || {
                        exchange_tls_raft_peer_frame_after_connect(endpoint, stream, exchange)
                    })
                    .await
                    .map_err(|error| {
                        ControlPlaneRaftPeerFrameExchangeError::new(
                            context,
                            raft_artifact_protocol_error(format!(
                                "control-plane OpenRaft TLS/TCP peer transport blocking task failed: {error}"
                            )),
                        )
                    })?
                }
            }
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct ControlPlaneRaftPeerNetworkFactory {
    local_node_id: ControlPlaneRaftNodeId,
    policy: Arc<ControlPlaneRaftPeerTransportPolicy>,
    rpc_timeout: Duration,
    transport: Arc<dyn ControlPlaneRaftPeerFrameTransport>,
}

impl ControlPlaneRaftPeerNetworkFactory {
    #[cfg(test)]
    #[must_use]
    pub(super) fn new(
        local_node_id: ControlPlaneRaftNodeId,
        policy: ControlPlaneRaftPeerTransportPolicy,
        rpc_timeout: Duration,
    ) -> Self {
        Self {
            local_node_id,
            policy: Arc::new(policy),
            rpc_timeout,
            transport: Arc::new(ControlPlaneRaftUnixPeerFrameTransport),
        }
    }

    #[must_use]
    pub(super) fn new_with_transport(
        local_node_id: ControlPlaneRaftNodeId,
        policy: ControlPlaneRaftPeerTransportPolicy,
        rpc_timeout: Duration,
        transport: Arc<dyn ControlPlaneRaftPeerFrameTransport>,
    ) -> Self {
        Self {
            local_node_id,
            policy: Arc::new(policy),
            rpc_timeout,
            transport,
        }
    }
}

impl RaftNetworkFactory<ControlPlaneRaftTypeConfig> for ControlPlaneRaftPeerNetworkFactory {
    type Network = ControlPlaneRaftPeerNetwork;

    async fn new_client(
        &mut self,
        target: ControlPlaneRaftNodeId,
        node: &BasicNode,
    ) -> Self::Network {
        ControlPlaneRaftPeerNetwork {
            local_node_id: self.local_node_id,
            target,
            node: node.clone(),
            policy: Arc::clone(&self.policy),
            rpc_timeout: self.rpc_timeout,
            transport: Arc::clone(&self.transport),
        }
    }
}

#[derive(Clone)]
pub(super) struct ControlPlaneRaftPeerNetwork {
    pub(super) local_node_id: ControlPlaneRaftNodeId,
    pub(super) target: ControlPlaneRaftNodeId,
    pub(super) node: BasicNode,
    pub(super) policy: Arc<ControlPlaneRaftPeerTransportPolicy>,
    pub(super) rpc_timeout: Duration,
    pub(super) transport: Arc<dyn ControlPlaneRaftPeerFrameTransport>,
}

impl ControlPlaneRaftPeerNetwork {
    pub(super) fn effective_rpc_deadline(
        &self,
        option: &RPCOption,
    ) -> Result<Instant, RPCError<ControlPlaneRaftTypeConfig>> {
        Instant::now()
            .checked_add(self.rpc_timeout.min(option.soft_ttl()))
            .ok_or_else(|| {
                raft_rpc_protocol_error(
                    "deadline",
                    raft_artifact_protocol_error("OpenRaft peer RPC deadline overflowed"),
                )
            })
    }

    pub(super) fn auth_operation_for_request(
        request: &ControlPlaneRaftPeerRpcRequest,
    ) -> ControlPlaneAuthOperation {
        match request {
            ControlPlaneRaftPeerRpcRequest::AppendEntries(_) => {
                ControlPlaneAuthOperation::RaftAppendEntries
            }
            ControlPlaneRaftPeerRpcRequest::Vote(_) => ControlPlaneAuthOperation::RaftVote,
            ControlPlaneRaftPeerRpcRequest::PreVote(_) => ControlPlaneAuthOperation::RaftPreVote,
            ControlPlaneRaftPeerRpcRequest::TransferLeader(_) => {
                ControlPlaneAuthOperation::RaftTransferLeader
            }
        }
    }

    fn encoded_append_entries_payload_len(
        entries: &[ControlPlaneRaftEntry],
    ) -> Result<usize, RPCError<ControlPlaneRaftTypeConfig>> {
        let mut encoded = Vec::new();
        write_raft_u32(
            &mut encoded,
            raft_len_as_u32(entries.len(), "raft append entries")
                .map_err(|error| raft_rpc_protocol_error("append_entries encode", error))?,
        );
        for entry in entries {
            write_raft_entry(&mut encoded, entry)
                .map_err(|error| raft_rpc_protocol_error("append_entries encode", error))?;
        }
        Ok(encoded.len())
    }

    fn request_identity(
        &self,
    ) -> Result<ControlPlaneRaftPeerFrameIdentity, RPCError<ControlPlaneRaftTypeConfig>> {
        self.policy
            .frame_identity(self.local_node_id, self.target)
            .map_err(raft_rpc_error_from_transport_rejection)
    }

    fn response_identity(
        identity: &ControlPlaneRaftPeerFrameIdentity,
    ) -> ControlPlaneRaftPeerFrameIdentity {
        reverse_raft_peer_frame_identity(identity)
    }

    fn validate_target(
        &self,
        rpc_name: &'static str,
    ) -> Result<(), RPCError<ControlPlaneRaftTypeConfig>> {
        self.policy
            .validate_target_node(self.target, &self.node, rpc_name)
            .map_err(raft_rpc_error_from_transport_rejection)
    }

    async fn exchange_frame(
        &self,
        rpc_name: &'static str,
        request_frame: Vec<u8>,
        deadline: Instant,
        context_prefix: &'static str,
    ) -> Result<Vec<u8>, RPCError<ControlPlaneRaftTypeConfig>> {
        self.validate_target(rpc_name)?;
        let transport_name = self.transport.name(self.target);
        self.transport
            .exchange(ControlPlaneRaftPeerFrameExchange {
                target: self.target,
                endpoint: self.node.addr.clone(),
                request_frame,
                max_frame_bytes: self.policy.limits.max_frame_bytes,
                connect_timeout: self.policy.connect_timeout(),
                deadline,
                context_prefix,
            })
            .await
            .map_err(|error| raft_peer_transport_rpc_error(transport_name, self.target, error))
    }

    pub(super) async fn send_rpc_frame(
        &self,
        rpc_name: &'static str,
        request: ControlPlaneRaftPeerRpcRequest,
        deadline: Instant,
    ) -> Result<ControlPlaneRaftPeerRpcResponse, RPCError<ControlPlaneRaftTypeConfig>> {
        let identity = self.request_identity()?;
        let operation = Self::auth_operation_for_request(&request);
        let raw_request_frame = request
            .encode_frame_for_peer(&identity)
            .map_err(|error| raft_rpc_protocol_error("encode", error))?;
        let encoded = if let Some(auth_policy) = self.policy.auth_policy() {
            auth_policy
                .sign_peer_frame(&identity, operation, raw_request_frame)
                .map_err(|error| raft_rpc_protocol_error("auth encode", error))?
        } else {
            raw_request_frame
        };
        let response_frame = self.exchange_frame(rpc_name, encoded, deadline, "").await?;
        let response_identity = Self::response_identity(&identity);
        let response_frame = if let Some(auth_policy) = self.policy.auth_policy() {
            auth_policy
                .verify_peer_frame(
                    &response_frame,
                    &response_identity,
                    operation,
                    self.policy.limits.max_frame_bytes,
                )
                .map_err(|error| raft_rpc_protocol_error("auth response decode", error))?
        } else {
            response_frame
        };
        ControlPlaneRaftPeerRpcResponse::decode_frame_for_peer(&response_frame, &response_identity)
            .map_err(|error| raft_rpc_protocol_error("response decode", error))
    }

    async fn send_snapshot_frame(
        &self,
        vote: VoteOf<ControlPlaneRaftTypeConfig>,
        snapshot: ControlPlaneRaftSnapshot,
        deadline: Instant,
    ) -> Result<
        SnapshotResponse<ControlPlaneRaftTypeConfig>,
        StreamingError<ControlPlaneRaftTypeConfig>,
    > {
        self.policy
            .validate_target_node(self.target, &self.node, "full_snapshot")
            .map_err(raft_streaming_error_from_transport_rejection)?;
        self.policy
            .validate_snapshot(self.target, snapshot.snapshot.get_ref().len())
            .map_err(raft_streaming_error_from_transport_rejection)?;
        let identity = self
            .request_identity()
            .map_err(raft_streaming_error_from_rpc_error)?;
        let raw_request_frame = ControlPlaneRaftPeerSnapshotRequest { vote, snapshot }
            .encode_frame_for_peer(&identity)
            .map_err(|error| raft_streaming_protocol_error("full_snapshot encode", error))?;
        let encoded = if let Some(auth_policy) = self.policy.auth_policy() {
            auth_policy
                .sign_peer_frame(
                    &identity,
                    ControlPlaneAuthOperation::RaftSnapshot,
                    raw_request_frame,
                )
                .map_err(|error| {
                    raft_streaming_protocol_error("full_snapshot auth encode", error)
                })?
        } else {
            raw_request_frame
        };
        let transport_name = self.transport.name(self.target);
        let response_frame = self
            .exchange_frame("full_snapshot", encoded, deadline, "full_snapshot ")
            .await
            .map_err(|error| match error {
                RPCError::Unreachable(error) => StreamingError::Unreachable(error),
                RPCError::Network(error) => StreamingError::Network(error),
                RPCError::Timeout(error) => {
                    StreamingError::Network(NetworkError::from_string(format!(
                        "control-plane OpenRaft {transport_name} peer transport full_snapshot timed out for node {}: {error}",
                        self.target
                    )))
                }
                RPCError::RemoteError(error) => {
                    StreamingError::Network(NetworkError::from_string(format!(
                        "control-plane OpenRaft {transport_name} peer transport full_snapshot remote error for node {}: {error}",
                        self.target
                    )))
                }
            })?;
        let response_identity = Self::response_identity(&identity);
        let response_frame = if let Some(auth_policy) = self.policy.auth_policy() {
            auth_policy
                .verify_peer_frame(
                    &response_frame,
                    &response_identity,
                    ControlPlaneAuthOperation::RaftSnapshot,
                    self.policy.limits.max_frame_bytes,
                )
                .map_err(|error| {
                    raft_streaming_protocol_error("full_snapshot auth response decode", error)
                })?
        } else {
            response_frame
        };
        let response = ControlPlaneRaftPeerSnapshotResponse::decode_frame_for_peer(
            &response_frame,
            &response_identity,
        )
        .map_err(|error| raft_streaming_protocol_error("full_snapshot response decode", error))?;
        Ok(response.response)
    }
}

pub(super) fn reverse_raft_peer_frame_identity(
    identity: &ControlPlaneRaftPeerFrameIdentity,
) -> ControlPlaneRaftPeerFrameIdentity {
    let mut reversed = ControlPlaneRaftPeerFrameIdentity::new(
        identity.cluster_name.clone(),
        identity.target,
        identity.source,
    );
    reversed.topology.clone_from(&identity.topology);
    reversed
}

impl fmt::Debug for ControlPlaneRaftPeerNetwork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlPlaneRaftPeerNetwork")
            .field("local_node_id", &self.local_node_id)
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl RaftNetworkV2<ControlPlaneRaftTypeConfig> for ControlPlaneRaftPeerNetwork {
    type SnapshotData = ControlPlaneRaftSnapshotData;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ControlPlaneRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
        RPCError<ControlPlaneRaftTypeConfig>,
    > {
        let deadline = self.effective_rpc_deadline(&option)?;
        self.policy
            .validate_append_entries(
                self.target,
                rpc.entries.len(),
                Self::encoded_append_entries_payload_len(&rpc.entries)?,
            )
            .map_err(raft_rpc_error_from_transport_rejection)?;
        let rpc_result = self
            .send_rpc_frame(
                "append_entries",
                ControlPlaneRaftPeerRpcRequest::AppendEntries(rpc),
                deadline,
            )
            .await?;
        let ControlPlaneRaftPeerRpcResponse::AppendEntries(response) = rpc_result else {
            return Err(raft_rpc_protocol_error(
                "append_entries response decode",
                raft_artifact_protocol_error("decoded non-append_entries response frame"),
            ));
        };
        Ok(response)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
    {
        let ControlPlaneRaftPeerRpcResponse::Vote(response) = self
            .send_rpc_frame(
                "vote",
                ControlPlaneRaftPeerRpcRequest::Vote(rpc),
                self.effective_rpc_deadline(&option)?,
            )
            .await?
        else {
            return Err(raft_rpc_protocol_error(
                "vote response decode",
                raft_artifact_protocol_error("decoded non-vote response frame"),
            ));
        };
        Ok(response)
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
    {
        let ControlPlaneRaftPeerRpcResponse::Vote(response) = self
            .send_rpc_frame(
                "pre_vote",
                ControlPlaneRaftPeerRpcRequest::PreVote(rpc),
                self.effective_rpc_deadline(&option)?,
            )
            .await?
        else {
            return Err(raft_rpc_protocol_error(
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
        option: RPCOption,
    ) -> Result<
        SnapshotResponse<ControlPlaneRaftTypeConfig>,
        StreamingError<ControlPlaneRaftTypeConfig>,
    > {
        let deadline = self
            .effective_rpc_deadline(&option)
            .map_err(raft_streaming_error_from_rpc_error)?;
        self.send_snapshot_frame(vote, snapshot, deadline).await
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<ControlPlaneRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<
        TransferLeaderResponse<ControlPlaneRaftTypeConfig>,
        RPCError<ControlPlaneRaftTypeConfig>,
    > {
        let ControlPlaneRaftPeerRpcResponse::TransferLeader(response) = self
            .send_rpc_frame(
                "transfer_leader",
                ControlPlaneRaftPeerRpcRequest::TransferLeader(req),
                self.effective_rpc_deadline(&option)?,
            )
            .await?
        else {
            return Err(raft_rpc_protocol_error(
                "transfer_leader response decode",
                raft_artifact_protocol_error("decoded non-transfer_leader response frame"),
            ));
        };
        Ok(response)
    }
}
fn decode_raft_peer_rpc_frame<T>(
    bytes: &[u8],
    expected_kind: u8,
    expected_identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
    decode_body: impl FnOnce(&mut RaftArtifactReader<'_>) -> Result<T, ControlPlaneError>,
) -> Result<T, ControlPlaneError> {
    let mut reader = raft_peer_rpc_frame_reader(bytes)?;
    let kind = reader.read_u8()?;
    if kind != expected_kind {
        return Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer RPC frame kind {kind} does not match expected kind {expected_kind}"
        )));
    }
    let identity = reader.read_peer_frame_identity()?;
    if let Some(expected_identity) = expected_identity {
        validate_raft_peer_frame_identity(&identity, expected_identity)?;
    }
    let decoded = decode_body(&mut reader)?;
    reader.finish()?;
    Ok(decoded)
}

fn raft_peer_rpc_frame_reader(bytes: &[u8]) -> Result<RaftArtifactReader<'_>, ControlPlaneError> {
    let min_len =
        CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len() + 2 + 1 + CONTROL_PLANE_RAFT_PEER_RPC_CHECKSUM_LEN;
    if bytes.len() < min_len {
        return Err(raft_artifact_protocol_error(
            "truncated control-plane OpenRaft peer RPC frame",
        ));
    }
    let (body, checksum_bytes) =
        bytes.split_at(bytes.len() - CONTROL_PLANE_RAFT_PEER_RPC_CHECKSUM_LEN);
    let expected_checksum = u64::from_be_bytes(
        checksum_bytes
            .try_into()
            .expect("checksum split length is fixed"),
    );
    let actual_checksum = raft_artifact_checksum(body);
    if actual_checksum != expected_checksum {
        return Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer RPC frame checksum mismatch: expected {expected_checksum:#x}, actual {actual_checksum:#x}"
        )));
    }

    let mut reader =
        RaftArtifactReader::with_context(body, "control-plane OpenRaft peer RPC frame");
    let magic = reader.read_exact(CONTROL_PLANE_RAFT_PEER_RPC_MAGIC.len())?;
    if magic != CONTROL_PLANE_RAFT_PEER_RPC_MAGIC {
        return Err(raft_artifact_protocol_error(
            "invalid control-plane OpenRaft peer RPC frame magic",
        ));
    }
    let version = reader.read_u16()?;
    if version != CONTROL_PLANE_RAFT_PEER_RPC_VERSION {
        return Err(raft_artifact_protocol_error(format!(
            "unsupported control-plane OpenRaft peer RPC frame version {version}"
        )));
    }
    Ok(reader)
}

pub(crate) fn decode_control_plane_raft_peer_request_frame_kind(
    bytes: &[u8],
) -> Result<ControlPlaneRaftPeerFrameKind, ControlPlaneError> {
    let mut reader = raft_peer_rpc_frame_reader(bytes)?;
    match reader.read_u8()? {
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_REQUEST => Ok(ControlPlaneRaftPeerFrameKind::OrdinaryRpc),
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_REQUEST => {
            Ok(ControlPlaneRaftPeerFrameKind::Snapshot)
        }
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_RESPONSE => Err(raft_artifact_protocol_error(
            "control-plane OpenRaft peer RPC response frame cannot be handled as a request",
        )),
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_RESPONSE => Err(raft_artifact_protocol_error(
            "control-plane OpenRaft peer snapshot response frame cannot be handled as a request",
        )),
        kind => Err(raft_artifact_protocol_error(format!(
            "unknown control-plane OpenRaft peer RPC frame kind {kind}"
        ))),
    }
}

pub(crate) fn decode_control_plane_raft_peer_request_frame_identity(
    bytes: &[u8],
) -> Result<ControlPlaneRaftPeerFrameIdentity, ControlPlaneError> {
    let mut reader = raft_peer_rpc_frame_reader(bytes)?;
    match reader.read_u8()? {
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_REQUEST
        | CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_REQUEST => {}
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_RESPONSE => {
            return Err(raft_artifact_protocol_error(
                "control-plane OpenRaft peer RPC response frame cannot be handled as a request",
            ));
        }
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_RESPONSE => {
            return Err(raft_artifact_protocol_error(
                "control-plane OpenRaft peer snapshot response frame cannot be handled as a request",
            ));
        }
        kind => {
            return Err(raft_artifact_protocol_error(format!(
                "unknown control-plane OpenRaft peer RPC frame kind {kind}"
            )));
        }
    }
    reader.read_peer_frame_identity()?.ok_or_else(|| {
        raft_artifact_protocol_error(
            "control-plane OpenRaft peer RPC frame is missing peer identity",
        )
    })
}

pub(crate) fn decode_control_plane_raft_peer_request_auth_operation(
    bytes: &[u8],
) -> Result<ControlPlaneAuthOperation, ControlPlaneError> {
    let mut reader = raft_peer_rpc_frame_reader(bytes)?;
    match reader.read_u8()? {
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_REQUEST => {
            let _identity = reader.read_peer_frame_identity()?;
            match reader.read_u8()? {
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_APPEND_ENTRIES => {
                    Ok(ControlPlaneAuthOperation::RaftAppendEntries)
                }
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_VOTE => Ok(ControlPlaneAuthOperation::RaftVote),
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_PRE_VOTE => {
                    Ok(ControlPlaneAuthOperation::RaftPreVote)
                }
                CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_TRANSFER_LEADER => {
                    Ok(ControlPlaneAuthOperation::RaftTransferLeader)
                }
                value => Err(raft_artifact_protocol_error(format!(
                    "unknown control-plane OpenRaft peer RPC request tag {value}"
                ))),
            }
        }
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_REQUEST => {
            Ok(ControlPlaneAuthOperation::RaftSnapshot)
        }
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_RESPONSE => Err(raft_artifact_protocol_error(
            "control-plane OpenRaft peer RPC response frame cannot be authenticated as a request",
        )),
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_RESPONSE => Err(raft_artifact_protocol_error(
            "control-plane OpenRaft peer snapshot response frame cannot be authenticated as a request",
        )),
        kind => Err(raft_artifact_protocol_error(format!(
            "unknown control-plane OpenRaft peer RPC frame kind {kind}"
        ))),
    }
}

fn validate_control_plane_raft_peer_auth_payload_binding(
    bytes: &[u8],
    expected_identity: &ControlPlaneRaftPeerFrameIdentity,
    expected_operation: ControlPlaneAuthOperation,
) -> Result<(), ControlPlaneError> {
    let mut reader = raft_peer_rpc_frame_reader(bytes)?;
    let kind = reader.read_u8()?;
    let identity = reader.read_peer_frame_identity()?;
    validate_raft_peer_frame_identity(&identity, expected_identity)?;
    let operation_matches = match kind {
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_REQUEST => match reader.read_u8()? {
            CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_APPEND_ENTRIES => {
                expected_operation == ControlPlaneAuthOperation::RaftAppendEntries
            }
            CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_VOTE => {
                expected_operation == ControlPlaneAuthOperation::RaftVote
            }
            CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_PRE_VOTE => {
                expected_operation == ControlPlaneAuthOperation::RaftPreVote
            }
            CONTROL_PLANE_RAFT_PEER_RPC_REQUEST_TRANSFER_LEADER => {
                expected_operation == ControlPlaneAuthOperation::RaftTransferLeader
            }
            value => {
                return Err(raft_artifact_protocol_error(format!(
                    "unknown control-plane OpenRaft peer RPC request tag {value}"
                )));
            }
        },
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_RESPONSE => match reader.read_u8()? {
            CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_APPEND_ENTRIES => {
                expected_operation == ControlPlaneAuthOperation::RaftAppendEntries
            }
            CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_VOTE => {
                matches!(
                    expected_operation,
                    ControlPlaneAuthOperation::RaftVote | ControlPlaneAuthOperation::RaftPreVote
                )
            }
            CONTROL_PLANE_RAFT_PEER_RPC_RESPONSE_TRANSFER_LEADER => {
                expected_operation == ControlPlaneAuthOperation::RaftTransferLeader
            }
            value => {
                return Err(raft_artifact_protocol_error(format!(
                    "unknown control-plane OpenRaft peer RPC response tag {value}"
                )));
            }
        },
        CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_REQUEST
        | CONTROL_PLANE_RAFT_PEER_RPC_KIND_SNAPSHOT_RESPONSE => {
            expected_operation == ControlPlaneAuthOperation::RaftSnapshot
        }
        kind => {
            return Err(raft_artifact_protocol_error(format!(
                "unknown control-plane OpenRaft peer RPC frame kind {kind}"
            )));
        }
    };
    if operation_matches {
        Ok(())
    } else {
        Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane OpenRaft peer auth operation {expected_operation:?} does not match authenticated payload kind {kind}"
            )))
    }
}

fn write_raft_peer_frame_identity(
    out: &mut Vec<u8>,
    identity: Option<&ControlPlaneRaftPeerFrameIdentity>,
) -> Result<(), ControlPlaneError> {
    match identity {
        None => write_raft_u8(out, 0),
        Some(identity) => {
            write_raft_u8(out, 1);
            write_raft_string(out, &identity.cluster_name)?;
            match &identity.topology {
                None => write_raft_u8(out, 0),
                Some(topology) => {
                    write_raft_u8(out, 1);
                    write_raft_u64(out, topology.generation);
                    write_raft_string(out, &topology.digest)?;
                }
            }
            write_raft_u64(out, identity.source);
            write_raft_u64(out, identity.target);
        }
    }
    Ok(())
}

fn validate_raft_peer_frame_identity(
    actual: &Option<ControlPlaneRaftPeerFrameIdentity>,
    expected: &ControlPlaneRaftPeerFrameIdentity,
) -> Result<(), ControlPlaneError> {
    let Some(actual) = actual else {
        return Err(raft_artifact_protocol_error(
            "control-plane OpenRaft peer RPC frame is missing peer identity",
        ));
    };
    if actual.cluster_name != expected.cluster_name {
        return Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer RPC frame cluster identity mismatch: expected {}, got {}",
            expected.cluster_name, actual.cluster_name
        )));
    }
    if actual.topology != expected.topology {
        return Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer RPC frame topology identity mismatch: expected {:?}, got {:?}",
            expected.topology, actual.topology
        )));
    }
    if actual.source != expected.source {
        return Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer RPC frame source identity mismatch: expected {}, got {}",
            expected.source, actual.source
        )));
    }
    if actual.target != expected.target {
        return Err(raft_artifact_protocol_error(format!(
            "control-plane OpenRaft peer RPC frame target identity mismatch: expected {}, got {}",
            expected.target, actual.target
        )));
    }
    Ok(())
}

pub(crate) fn write_control_plane_raft_peer_transport_frame(
    writer: &mut (impl Write + ?Sized),
    frame: &[u8],
) -> Result<(), ControlPlaneError> {
    let frame_len = u32::try_from(frame.len()).map_err(|_| {
        ControlPlaneError::rpc_protocol(format!(
            "control-plane OpenRaft peer transport frame too large: {} bytes",
            frame.len()
        ))
    })?;
    let mut header = Vec::with_capacity(std::mem::size_of::<u32>());
    write_raft_u32(&mut header, frame_len);
    writer
        .write_all(&header)
        .and_then(|()| writer.write_all(frame))
        .map_err(|source| {
            ControlPlaneError::io("write control-plane OpenRaft peer transport frame", source)
        })
}

pub(crate) fn read_control_plane_raft_peer_transport_frame(
    reader: &mut (impl Read + ?Sized),
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ControlPlaneError> {
    read_control_plane_raft_peer_transport_frame_with_reservation(reader, max_frame_bytes, |_| {
        Ok(())
    })
    .map(|(frame, ())| frame)
}

pub(crate) fn read_control_plane_raft_peer_transport_frame_with_reservation<Reservation>(
    reader: &mut (impl Read + ?Sized),
    max_frame_bytes: usize,
    reserve: impl FnOnce(usize) -> Result<Reservation, ControlPlaneError>,
) -> Result<(Vec<u8>, Reservation), ControlPlaneError> {
    let mut header = [0; std::mem::size_of::<u32>()];
    reader.read_exact(&mut header).map_err(|source| {
        ControlPlaneError::io(
            "read control-plane OpenRaft peer transport frame header",
            source,
        )
    })?;
    let frame_len = usize::try_from(u32::from_be_bytes(header)).map_err(|_| {
        ControlPlaneError::rpc_protocol(
            "control-plane OpenRaft peer transport frame length does not fit usize".to_string(),
        )
    })?;
    if frame_len > max_frame_bytes {
        return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane OpenRaft peer transport frame size {frame_len} bytes exceeds limit {max_frame_bytes}"
            )));
    }
    let reservation = reserve(frame_len)?;
    let mut frame = vec![0; frame_len];
    reader.read_exact(&mut frame).map_err(|source| {
        ControlPlaneError::io(
            "read control-plane OpenRaft peer transport frame payload",
            source,
        )
    })?;
    Ok((frame, reservation))
}
