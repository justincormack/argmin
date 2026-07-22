use crate::control_plane::ControlPlaneError;
use crate::control_plane_auth::{
    ControlPlaneAuthDecision, ControlPlaneAuthEnvelope, ControlPlaneAuthOperation,
    ControlPlaneAuthPrincipal, ControlPlaneAuthRejectionReason, ControlPlaneAuthReplayPolicy,
    ControlPlaneAuthService, ControlPlaneAuthSignInput, ControlPlaneAuthTarget,
    ControlPlaneAuthVerificationInput, ControlPlaneScopedCredential,
    ControlPlaneScopedCredentialStore,
};
use crate::storage_rpc::{
    decode_storage_rpc_frame, encode_storage_rpc_frame, StorageRpcFrame, StorageRpcFrameError,
    StorageRpcMessageKind, STORAGE_RPC_MAX_FRAME_LEN,
};
use crate::NodeId;
use std::fmt;

const STORAGE_RPC_AUTH_BINDING_MAGIC: &[u8; 8] = b"ARGSRPCB";
const STORAGE_RPC_AUTH_BINDING_VERSION: u16 = 1;
const STORAGE_RPC_AUTH_TOPOLOGY_DIGEST_LEN: usize = 64;
const STORAGE_RPC_AUTH_BINDING_FIXED_LEN: usize =
    STORAGE_RPC_AUTH_BINDING_MAGIC.len() + 2 + 8 + 4 + STORAGE_RPC_AUTH_TOPOLOGY_DIGEST_LEN + 4 + 4;
const STORAGE_RPC_AUTH_MAX_BINDING_LEN: usize =
    STORAGE_RPC_AUTH_BINDING_FIXED_LEN + STORAGE_RPC_MAX_FRAME_LEN;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageRpcAuthRejectionReason {
    Malformed,
    WrongTopology,
    WrongTarget,
    WrongOperation,
    UnauthorizedRole,
    Envelope(ControlPlaneAuthRejectionReason),
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct VerifiedStorageRpcFrame {
    source: ControlPlaneAuthPrincipal,
    credential_id: String,
    credential_version: u64,
    frame: StorageRpcFrame,
}

impl fmt::Debug for VerifiedStorageRpcFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedStorageRpcFrame")
            .field("source", &self.source)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("request_id", &self.frame.request_id)
            .field("operation", &self.frame.kind.operation_name())
            .field("payload_len", &self.frame.payload.len())
            .finish()
    }
}

impl VerifiedStorageRpcFrame {
    pub(crate) fn source(&self) -> &ControlPlaneAuthPrincipal {
        &self.source
    }

    pub(crate) fn credential_id(&self) -> &str {
        &self.credential_id
    }

    pub(crate) fn credential_version(&self) -> u64 {
        self.credential_version
    }

    pub(crate) fn into_frame(self) -> StorageRpcFrame {
        self.frame
    }
}

struct StorageRpcAuthBinding {
    topology_generation: u64,
    topology_digest: String,
    target_node_id: NodeId,
    frame: StorageRpcFrame,
}

pub(crate) struct StorageRpcAuthRequestInput<'a> {
    pub(crate) credential: &'a ControlPlaneScopedCredential,
    pub(crate) target_node_id: NodeId,
    pub(crate) topology_generation: u64,
    pub(crate) topology_digest: &'a str,
    pub(crate) issued_at_ms: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) frame: &'a StorageRpcFrame,
}

pub(crate) fn sign_storage_rpc_request(
    input: StorageRpcAuthRequestInput<'_>,
) -> Result<Vec<u8>, ControlPlaneError> {
    if !principal_allows_operation(input.credential.principal(), input.frame.kind) {
        return Err(storage_rpc_auth_protocol_error(format!(
            "principal {:?} is not authorized for {}",
            input.credential.principal(),
            input.frame.kind.operation_name()
        )));
    }
    let payload = encode_binding(
        input.topology_generation,
        input.topology_digest,
        input.target_node_id,
        input.frame,
    )?;
    input
        .credential
        .sign_envelope(ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Service(ControlPlaneAuthService::StorageRpc),
            operation: ControlPlaneAuthOperation::StorageRpcRequest {
                message_kind: input.frame.kind as u16,
            },
            issued_at_ms: Some(input.issued_at_ms),
            expires_at_ms: Some(input.expires_at_ms),
            sequence: Some(input.frame.request_id),
            nonce: Vec::new(),
            payload,
        })?
        .encode_frame()
}

pub(crate) struct StorageRpcAuthRequestVerificationInput<'a> {
    pub(crate) verifier: &'a ControlPlaneScopedCredentialStore,
    pub(crate) expected_cluster_id: &'a str,
    pub(crate) expected_target_node_id: NodeId,
    pub(crate) expected_topology_generation: u64,
    pub(crate) expected_topology_digest: &'a str,
    pub(crate) now_ms: u64,
    pub(crate) max_replay_window_ms: u64,
    pub(crate) allowed_future_skew_ms: u64,
    pub(crate) envelope_bytes: &'a [u8],
}

pub(crate) fn verify_storage_rpc_request(
    input: StorageRpcAuthRequestVerificationInput<'_>,
) -> Result<VerifiedStorageRpcFrame, StorageRpcAuthRejectionReason> {
    let envelope = ControlPlaneAuthEnvelope::decode_frame(
        input.envelope_bytes,
        STORAGE_RPC_AUTH_MAX_BINDING_LEN,
    )
    .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
    let source = envelope.header().source().clone();
    let operation = envelope.header().operation();
    let ControlPlaneAuthOperation::StorageRpcRequest { message_kind } = operation else {
        return Err(StorageRpcAuthRejectionReason::WrongOperation);
    };
    let decision = input
        .verifier
        .verify_envelope(ControlPlaneAuthVerificationInput {
            envelope: &envelope,
            expected_cluster_id: input.expected_cluster_id,
            expected_source: &source,
            expected_target: &ControlPlaneAuthTarget::Service(ControlPlaneAuthService::StorageRpc),
            expected_operation: operation,
            replay_policy: ControlPlaneAuthReplayPolicy::TimestampWindow {
                now_ms: input.now_ms,
                max_window_ms: input.max_replay_window_ms,
                allowed_future_skew_ms: input.allowed_future_skew_ms,
            },
        });
    let (credential_id, credential_version) = accepted_credential(decision)?;
    let binding = decode_binding(envelope.payload())?;
    validate_binding(
        &binding,
        input.expected_target_node_id,
        input.expected_topology_generation,
        input.expected_topology_digest,
        message_kind,
        envelope.header().sequence(),
    )?;
    if !principal_allows_operation(&source, binding.frame.kind) {
        return Err(StorageRpcAuthRejectionReason::UnauthorizedRole);
    }
    Ok(VerifiedStorageRpcFrame {
        source,
        credential_id,
        credential_version,
        frame: binding.frame,
    })
}

pub(crate) struct StorageRpcAuthResponseInput<'a> {
    pub(crate) request_credential: &'a ControlPlaneScopedCredential,
    pub(crate) target_node_id: NodeId,
    pub(crate) topology_generation: u64,
    pub(crate) topology_digest: &'a str,
    pub(crate) issued_at_ms: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) frame: &'a StorageRpcFrame,
}

pub(crate) fn sign_storage_rpc_response(
    input: StorageRpcAuthResponseInput<'_>,
) -> Result<Vec<u8>, ControlPlaneError> {
    let response_credential = input.request_credential.storage_rpc_response_credential()?;
    let payload = encode_binding(
        input.topology_generation,
        input.topology_digest,
        input.target_node_id,
        input.frame,
    )?;
    response_credential
        .sign_envelope(ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Principal(input.request_credential.principal().clone()),
            operation: ControlPlaneAuthOperation::StorageRpcResponse {
                message_kind: input.frame.kind as u16,
            },
            issued_at_ms: Some(input.issued_at_ms),
            expires_at_ms: Some(input.expires_at_ms),
            sequence: Some(input.frame.request_id),
            nonce: Vec::new(),
            payload,
        })?
        .encode_frame()
}

pub(crate) struct StorageRpcAuthResponseVerificationInput<'a> {
    pub(crate) request_credential: &'a ControlPlaneScopedCredential,
    pub(crate) expected_target_node_id: NodeId,
    pub(crate) expected_topology_generation: u64,
    pub(crate) expected_topology_digest: &'a str,
    pub(crate) expected_request_id: u64,
    pub(crate) expected_kind: StorageRpcMessageKind,
    pub(crate) now_ms: u64,
    pub(crate) max_replay_window_ms: u64,
    pub(crate) allowed_future_skew_ms: u64,
    pub(crate) envelope_bytes: &'a [u8],
}

pub(crate) fn verify_storage_rpc_response(
    input: StorageRpcAuthResponseVerificationInput<'_>,
) -> Result<VerifiedStorageRpcFrame, StorageRpcAuthRejectionReason> {
    let response_credential = input
        .request_credential
        .storage_rpc_response_credential()
        .map_err(|_| StorageRpcAuthRejectionReason::UnauthorizedRole)?;
    let verifier = ControlPlaneScopedCredentialStore::new(vec![response_credential])
        .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
    let envelope = ControlPlaneAuthEnvelope::decode_frame(
        input.envelope_bytes,
        STORAGE_RPC_AUTH_MAX_BINDING_LEN,
    )
    .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
    let operation = ControlPlaneAuthOperation::StorageRpcResponse {
        message_kind: input.expected_kind as u16,
    };
    let source = ControlPlaneAuthPrincipal::Service {
        service: ControlPlaneAuthService::StorageRpc,
    };
    let decision = verifier.verify_envelope(ControlPlaneAuthVerificationInput {
        envelope: &envelope,
        expected_cluster_id: input.request_credential.cluster_id(),
        expected_source: &source,
        expected_target: &ControlPlaneAuthTarget::Principal(
            input.request_credential.principal().clone(),
        ),
        expected_operation: operation,
        replay_policy: ControlPlaneAuthReplayPolicy::TimestampWindow {
            now_ms: input.now_ms,
            max_window_ms: input.max_replay_window_ms,
            allowed_future_skew_ms: input.allowed_future_skew_ms,
        },
    });
    let (credential_id, credential_version) = accepted_credential(decision)?;
    let binding = decode_binding(envelope.payload())?;
    validate_binding(
        &binding,
        input.expected_target_node_id,
        input.expected_topology_generation,
        input.expected_topology_digest,
        input.expected_kind as u16,
        envelope.header().sequence(),
    )?;
    if binding.frame.request_id != input.expected_request_id {
        return Err(StorageRpcAuthRejectionReason::WrongOperation);
    }
    Ok(VerifiedStorageRpcFrame {
        source,
        credential_id,
        credential_version,
        frame: binding.frame,
    })
}

fn accepted_credential(
    decision: ControlPlaneAuthDecision,
) -> Result<(String, u64), StorageRpcAuthRejectionReason> {
    match decision {
        ControlPlaneAuthDecision::Accepted {
            credential_id,
            credential_version,
        } => Ok((credential_id, credential_version)),
        ControlPlaneAuthDecision::Rejected { reason } => {
            Err(StorageRpcAuthRejectionReason::Envelope(reason))
        }
    }
}

fn validate_binding(
    binding: &StorageRpcAuthBinding,
    expected_target_node_id: NodeId,
    expected_topology_generation: u64,
    expected_topology_digest: &str,
    expected_message_kind: u16,
    envelope_sequence: Option<u64>,
) -> Result<(), StorageRpcAuthRejectionReason> {
    if binding.target_node_id != expected_target_node_id {
        return Err(StorageRpcAuthRejectionReason::WrongTarget);
    }
    if binding.topology_generation != expected_topology_generation
        || binding.topology_digest != expected_topology_digest
    {
        return Err(StorageRpcAuthRejectionReason::WrongTopology);
    }
    if binding.frame.kind as u16 != expected_message_kind
        || envelope_sequence != Some(binding.frame.request_id)
    {
        return Err(StorageRpcAuthRejectionReason::WrongOperation);
    }
    Ok(())
}

fn principal_allows_operation(
    principal: &ControlPlaneAuthPrincipal,
    kind: StorageRpcMessageKind,
) -> bool {
    let roles = authorized_roles(kind);
    match principal {
        ControlPlaneAuthPrincipal::Frontend { .. } => roles.allows(StorageRpcCallerRole::Frontend),
        ControlPlaneAuthPrincipal::StorageNode { .. } => {
            roles.allows(StorageRpcCallerRole::StorageNode)
        }
        ControlPlaneAuthPrincipal::Admin { .. } => roles.allows(StorageRpcCallerRole::Admin),
        ControlPlaneAuthPrincipal::LocalMaintenance { .. } => {
            roles.allows(StorageRpcCallerRole::Maintenance)
        }
        ControlPlaneAuthPrincipal::RaftPeer { .. } | ControlPlaneAuthPrincipal::Service { .. } => {
            false
        }
    }
}

#[derive(Clone, Copy)]
enum StorageRpcCallerRole {
    Frontend,
    StorageNode,
    Admin,
    Maintenance,
}

#[derive(Clone, Copy)]
struct StorageRpcAuthorizedRoles(u8);

impl StorageRpcAuthorizedRoles {
    const FRONTEND: u8 = 1 << 0;
    const STORAGE_NODE: u8 = 1 << 1;
    const ADMIN: u8 = 1 << 2;
    const MAINTENANCE: u8 = 1 << 3;

    const ALL_INTERNAL: Self =
        Self(Self::FRONTEND | Self::STORAGE_NODE | Self::ADMIN | Self::MAINTENANCE);
    const FRONTEND_ONLY: Self = Self(Self::FRONTEND);
    const FRONTEND_MAINTENANCE: Self = Self(Self::FRONTEND | Self::MAINTENANCE);
    const FRONTEND_STORAGE: Self = Self(Self::FRONTEND | Self::STORAGE_NODE);
    const FRONTEND_STORAGE_MAINTENANCE: Self =
        Self(Self::FRONTEND | Self::STORAGE_NODE | Self::MAINTENANCE);
    const MAINTENANCE_ONLY: Self = Self(Self::MAINTENANCE);
    const STORAGE_MAINTENANCE: Self = Self(Self::STORAGE_NODE | Self::MAINTENANCE);

    fn allows(self, role: StorageRpcCallerRole) -> bool {
        let role = match role {
            StorageRpcCallerRole::Frontend => Self::FRONTEND,
            StorageRpcCallerRole::StorageNode => Self::STORAGE_NODE,
            StorageRpcCallerRole::Admin => Self::ADMIN,
            StorageRpcCallerRole::Maintenance => Self::MAINTENANCE,
        };
        self.0 & role != 0
    }
}

fn authorized_roles(kind: StorageRpcMessageKind) -> StorageRpcAuthorizedRoles {
    match kind {
        StorageRpcMessageKind::Health => StorageRpcAuthorizedRoles::ALL_INTERNAL,

        StorageRpcMessageKind::MetadataCommandReplicaState
        | StorageRpcMessageKind::MetadataCommandAcceptance
        | StorageRpcMessageKind::MetadataCommandAbandonAcceptance
        | StorageRpcMessageKind::MetadataCommandPendingSlotInsert
        | StorageRpcMessageKind::MetadataCommandPendingSlotRemove
        | StorageRpcMessageKind::MetadataCommandMaxLogIndex
        | StorageRpcMessageKind::MetadataCommandNextId
        | StorageRpcMessageKind::MetadataCommandPendingEnvelope
        | StorageRpcMessageKind::MetadataCommandValidateReplayState
        | StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending
        | StorageRpcMessageKind::MetadataCommandCheckpointCandidates
        | StorageRpcMessageKind::MetadataCommandCheckpointRecordCurrent
        | StorageRpcMessageKind::MetadataCommandLogCompact
        | StorageRpcMessageKind::MetadataCommandAbandoned
        | StorageRpcMessageKind::MetadataCommandRecordAbandoned
        | StorageRpcMessageKind::MetadataCommandPendingSlotReplace
        | StorageRpcMessageKind::MetadataCommandApplyAndRecord
        | StorageRpcMessageKind::MetadataCommandPgLockAcquire
        | StorageRpcMessageKind::MetadataCommandPgLockRelease
        | StorageRpcMessageKind::ShardAckLoad
        | StorageRpcMessageKind::ShardAckHistoricalLoad
        | StorageRpcMessageKind::ShardAckDelete => {
            StorageRpcAuthorizedRoles::FRONTEND_STORAGE_MAINTENANCE
        }

        StorageRpcMessageKind::MetadataCommandReplicaStateCanInitialize
        | StorageRpcMessageKind::MetadataCommandTransferStateAdopt
        | StorageRpcMessageKind::MetadataCommandTransferEmptyStateInitialize
        | StorageRpcMessageKind::MetadataCommandTransferMatchingStateInitialize
        | StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall
        | StorageRpcMessageKind::MetadataCommandCheckpointExport
        | StorageRpcMessageKind::MetadataCommandAppliedLogHashes
        | StorageRpcMessageKind::MetadataCommandMatchingAppliedLog
        | StorageRpcMessageKind::MetadataCommandRetainedLogHashes
        | StorageRpcMessageKind::MetadataCommandRetainedLogEntries
        | StorageRpcMessageKind::ShardAckRecord
        | StorageRpcMessageKind::ShardAckValidate => StorageRpcAuthorizedRoles::FRONTEND_STORAGE,

        StorageRpcMessageKind::MetadataCommandRecoveryApplyAndRecord
        | StorageRpcMessageKind::MetadataCommandRecoveryPendingSlotReplace
        | StorageRpcMessageKind::MetadataCommandRetainedAbortApply
        | StorageRpcMessageKind::MetadataCommandRetainedAbortFinish
        | StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord
        | StorageRpcMessageKind::ClusterMapHistoryReferenceSummary => {
            StorageRpcAuthorizedRoles::FRONTEND_STORAGE_MAINTENANCE
        }

        StorageRpcMessageKind::ShardRepairWrite
        | StorageRpcMessageKind::ShardHistoricalRead
        | StorageRpcMessageKind::PlacedSegmentShardRepairRecord
        | StorageRpcMessageKind::PlacedSegmentShardRepairs
        | StorageRpcMessageKind::PlacedSegmentShardRepairResolve
        | StorageRpcMessageKind::PlacedSegmentShardRepairClaimAcquire
        | StorageRpcMessageKind::PlacedSegmentShardRepairClaimComplete
        | StorageRpcMessageKind::PlacedSegmentShardRepairClaimError
        | StorageRpcMessageKind::PlacedSegmentShardBackfillRecord
        | StorageRpcMessageKind::PlacedSegmentShardBackfills
        | StorageRpcMessageKind::PlacedSegmentShardBackfillResolve
        | StorageRpcMessageKind::PlacedSegmentShardBackfillClaimAcquire
        | StorageRpcMessageKind::PlacedSegmentShardBackfillClaimComplete
        | StorageRpcMessageKind::PlacedSegmentShardBackfillClaimError
        | StorageRpcMessageKind::PlacedSegmentShardBackfillCount
        | StorageRpcMessageKind::PlacedSegmentShardBackfillExists => {
            StorageRpcAuthorizedRoles::STORAGE_MAINTENANCE
        }

        StorageRpcMessageKind::ShardScavengerListFiles
        | StorageRpcMessageKind::ShardScavengerShardRows
        | StorageRpcMessageKind::ShardScavengerPayloadReferences
        | StorageRpcMessageKind::ShardScavengerObservationRecord
        | StorageRpcMessageKind::ShardScavengerObservations
        | StorageRpcMessageKind::ShardScavengerObservationResolve
        | StorageRpcMessageKind::LifecycleSweepBucketsList
        | StorageRpcMessageKind::LifecycleSweepRoots
        | StorageRpcMessageKind::LifecycleSweepClaimAcquire
        | StorageRpcMessageKind::LifecycleSweepClaimHeartbeat
        | StorageRpcMessageKind::LifecycleSweepClaimError
        | StorageRpcMessageKind::LifecycleSweepClaimRelease
        | StorageRpcMessageKind::ObjectPayloadReclaimExists
        | StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot
        | StorageRpcMessageKind::ObjectPayloadReclaimRoot
        | StorageRpcMessageKind::ObjectPayloadReclaimLoad
        | StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire
        | StorageRpcMessageKind::ObjectPayloadReclaimClaimRelease
        | StorageRpcMessageKind::ObjectPayloadReclaimClaimGet => {
            StorageRpcAuthorizedRoles::MAINTENANCE_ONLY
        }

        StorageRpcMessageKind::ClaimHeartbeat
        | StorageRpcMessageKind::ClaimRelease
        | StorageRpcMessageKind::ProofRelease
        | StorageRpcMessageKind::ShardDelete
        | StorageRpcMessageKind::ObjectStreamUploadRetainedAbortPrepare
        | StorageRpcMessageKind::BucketWriteDrainBegin
        | StorageRpcMessageKind::BucketWriteDrainClear
        | StorageRpcMessageKind::BucketWriteDrainClearExpired
        | StorageRpcMessageKind::BucketWriteDrainGet
        | StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord
        | StorageRpcMessageKind::BucketDeleteAttemptOutcomeGet
        | StorageRpcMessageKind::BucketWriteDrainHeartbeat
        | StorageRpcMessageKind::BucketWriteDrainExists
        | StorageRpcMessageKind::BucketWriteReservationsList
        | StorageRpcMessageKind::BucketDeleteFinalizeRoots
        | StorageRpcMessageKind::BucketDeleteBeginRoots
        | StorageRpcMessageKind::BucketDeleteFinalizeClaimGet
        | StorageRpcMessageKind::BucketDeleteFinalizeClaimAcquire
        | StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease => {
            StorageRpcAuthorizedRoles::FRONTEND_MAINTENANCE
        }

        StorageRpcMessageKind::BucketHeadRaw
        | StorageRpcMessageKind::BucketHeadInfo
        | StorageRpcMessageKind::ObjectVersionNext
        | StorageRpcMessageKind::BucketWriteReservationAcquire
        | StorageRpcMessageKind::BucketWriteReservationValidate
        | StorageRpcMessageKind::BucketWriteReservationRelease
        | StorageRpcMessageKind::BucketWriteReservationHeartbeat
        | StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad
        | StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad
        | StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild
        | StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild
        | StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild
        | StorageRpcMessageKind::ObjectLifecycleVersionListLoad
        | StorageRpcMessageKind::ObjectMultipartAbortCommandBuild
        | StorageRpcMessageKind::ObjectMultipartAuthorizedAbortCommandBuild
        | StorageRpcMessageKind::ObjectMultipartCompletionStaleSourceLoad
        | StorageRpcMessageKind::ObjectMultipartAbortCleanupLoad
        | StorageRpcMessageKind::ObjectMultipartUploadLoad
        | StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad
        | StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad
        | StorageRpcMessageKind::ObjectStreamUploadSessionLoad
        | StorageRpcMessageKind::ObjectStreamUploadSegmentsLoad
        | StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate
        | StorageRpcMessageKind::BucketSubresourceGet
        | StorageRpcMessageKind::ObjectListPage
        | StorageRpcMessageKind::ObjectVersionListPage
        | StorageRpcMessageKind::ObjectMultipartUploadListPage
        | StorageRpcMessageKind::ObjectStreamUploadsList
        | StorageRpcMessageKind::ObjectStreamUploadsPgList
        | StorageRpcMessageKind::ObjectPayloadLeaseControl => {
            StorageRpcAuthorizedRoles::FRONTEND_MAINTENANCE
        }

        StorageRpcMessageKind::MetadataCommand
        | StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert
        | StorageRpcMessageKind::ShardWrite
        | StorageRpcMessageKind::ShardRead
        | StorageRpcMessageKind::ShardReadRange
        | StorageRpcMessageKind::ReadHandlesAcquire
        | StorageRpcMessageKind::ReadHandlesRelease
        | StorageRpcMessageKind::BucketCreateCommandBuild
        | StorageRpcMessageKind::ObjectGenerationNext
        | StorageRpcMessageKind::ObjectGenerationReservation
        | StorageRpcMessageKind::BucketSnapshotLoad
        | StorageRpcMessageKind::BucketSnapshotPairLoad
        | StorageRpcMessageKind::DirectPutCommitSnapshotLoad
        | StorageRpcMessageKind::DirectPutCommitCommandBuild
        | StorageRpcMessageKind::MultipartCompletionBarrierCommandBuild
        | StorageRpcMessageKind::ObjectReadAuthSubjectLoad
        | StorageRpcMessageKind::ObjectReadSnapshotLoad
        | StorageRpcMessageKind::ObjectTagsForSubjectLoad
        | StorageRpcMessageKind::ObjectMetadataPutSnapshotLoad
        | StorageRpcMessageKind::ObjectMetadataPutCommandBuild
        | StorageRpcMessageKind::ObjectStreamUploadMatch
        | StorageRpcMessageKind::ObjectMultipartUploadMatch
        | StorageRpcMessageKind::ObjectStreamUploadCommandBuild
        | StorageRpcMessageKind::ObjectMultipartUploadCommandBuild
        | StorageRpcMessageKind::ObjectStreamPutFinalizeSnapshotLoad
        | StorageRpcMessageKind::ObjectStreamPutCommitCommandBuild
        | StorageRpcMessageKind::ObjectStreamPartFinalizeSnapshotLoad
        | StorageRpcMessageKind::ObjectStreamPartCommitCommandBuild
        | StorageRpcMessageKind::ObjectMultipartCompleteCommandBuild
        | StorageRpcMessageKind::ObjectMultipartCompletionSnapshotLoad
        | StorageRpcMessageKind::ObjectMultipartCompletionPreflightLoad
        | StorageRpcMessageKind::ObjectMultipartPartsList
        | StorageRpcMessageKind::ObjectMultipartManagementLookup
        | StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare
        | StorageRpcMessageKind::BucketMetadataControlPendingMatch
        | StorageRpcMessageKind::BucketMetadataControlCommandBuild
        | StorageRpcMessageKind::BucketList
        | StorageRpcMessageKind::BucketExecutionGenerations
        | StorageRpcMessageKind::BucketFastPathIdentities
        | StorageRpcMessageKind::BucketMarkDeletingCommandBuild => {
            StorageRpcAuthorizedRoles::FRONTEND_ONLY
        }
    }
}

#[cfg(test)]
fn recognized_storage_rpc_message_kinds() -> Vec<StorageRpcMessageKind> {
    (0..=u16::MAX)
        .filter_map(|value| StorageRpcMessageKind::from_u16(value).ok())
        .collect()
}

fn encode_binding(
    topology_generation: u64,
    topology_digest: &str,
    target_node_id: NodeId,
    frame: &StorageRpcFrame,
) -> Result<Vec<u8>, ControlPlaneError> {
    validate_topology(topology_generation, topology_digest)?;
    let frame = encode_storage_rpc_frame(frame.request_id, frame.kind, &frame.payload)
        .map_err(storage_rpc_frame_protocol_error)?;
    let frame_len = u32::try_from(frame.len()).map_err(|_| {
        storage_rpc_auth_protocol_error("encoded storage RPC frame length exceeds u32::MAX")
    })?;
    let mut out = Vec::with_capacity(STORAGE_RPC_AUTH_BINDING_FIXED_LEN + frame.len());
    out.extend_from_slice(STORAGE_RPC_AUTH_BINDING_MAGIC);
    out.extend_from_slice(&STORAGE_RPC_AUTH_BINDING_VERSION.to_be_bytes());
    out.extend_from_slice(&topology_generation.to_be_bytes());
    out.extend_from_slice(&(STORAGE_RPC_AUTH_TOPOLOGY_DIGEST_LEN as u32).to_be_bytes());
    out.extend_from_slice(topology_digest.as_bytes());
    out.extend_from_slice(&target_node_id.as_u32().to_be_bytes());
    out.extend_from_slice(&frame_len.to_be_bytes());
    out.extend_from_slice(&frame);
    Ok(out)
}

fn decode_binding(bytes: &[u8]) -> Result<StorageRpcAuthBinding, StorageRpcAuthRejectionReason> {
    if bytes.len() > STORAGE_RPC_AUTH_MAX_BINDING_LEN {
        return Err(StorageRpcAuthRejectionReason::Malformed);
    }
    let mut reader = BindingReader::new(bytes);
    if reader.read_exact(STORAGE_RPC_AUTH_BINDING_MAGIC.len())? != STORAGE_RPC_AUTH_BINDING_MAGIC {
        return Err(StorageRpcAuthRejectionReason::Malformed);
    }
    if reader.read_u16()? != STORAGE_RPC_AUTH_BINDING_VERSION {
        return Err(StorageRpcAuthRejectionReason::Malformed);
    }
    let topology_generation = reader.read_u64()?;
    let digest_len = reader.read_u32()? as usize;
    if digest_len != STORAGE_RPC_AUTH_TOPOLOGY_DIGEST_LEN {
        return Err(StorageRpcAuthRejectionReason::Malformed);
    }
    let topology_digest = std::str::from_utf8(reader.read_exact(digest_len)?)
        .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?
        .to_owned();
    if validate_topology(topology_generation, &topology_digest).is_err() {
        return Err(StorageRpcAuthRejectionReason::Malformed);
    }
    let target_node_id = NodeId::new(reader.read_u32()?);
    let frame_len = reader.read_u32()? as usize;
    if frame_len > STORAGE_RPC_MAX_FRAME_LEN {
        return Err(StorageRpcAuthRejectionReason::Malformed);
    }
    let frame = decode_storage_rpc_frame(reader.read_exact(frame_len)?)
        .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
    reader.finish()?;
    Ok(StorageRpcAuthBinding {
        topology_generation,
        topology_digest,
        target_node_id,
        frame,
    })
}

fn validate_topology(
    topology_generation: u64,
    topology_digest: &str,
) -> Result<(), ControlPlaneError> {
    if topology_generation == 0 {
        return Err(storage_rpc_auth_protocol_error(
            "topology generation is zero",
        ));
    }
    if topology_digest.len() != STORAGE_RPC_AUTH_TOPOLOGY_DIGEST_LEN
        || !topology_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(storage_rpc_auth_protocol_error(
            "topology digest is not 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn storage_rpc_frame_protocol_error(error: StorageRpcFrameError) -> ControlPlaneError {
    storage_rpc_auth_protocol_error(format!("invalid storage RPC frame: {error}"))
}

fn storage_rpc_auth_protocol_error(message: impl Into<String>) -> ControlPlaneError {
    ControlPlaneError::RpcProtocol {
        message: format!("storage RPC auth envelope: {}", message.into()),
    }
}

struct BindingReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BindingReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], StorageRpcAuthRejectionReason> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(StorageRpcAuthRejectionReason::Malformed)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(StorageRpcAuthRejectionReason::Malformed)?;
        self.offset = end;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16, StorageRpcAuthRejectionReason> {
        let bytes: [u8; 2] = self
            .read_exact(2)?
            .try_into()
            .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, StorageRpcAuthRejectionReason> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, StorageRpcAuthRejectionReason> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| StorageRpcAuthRejectionReason::Malformed)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn finish(self) -> Result<(), StorageRpcAuthRejectionReason> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(StorageRpcAuthRejectionReason::Malformed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane_auth::ControlPlaneScopedCredentialInput;

    const TOPOLOGY_DIGEST: &str =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    const ROUTINE_CHECKPOINT_MAINTENANCE_WORKFLOW: &[StorageRpcMessageKind] = &[
        StorageRpcMessageKind::MetadataCommandReplicaState,
        StorageRpcMessageKind::MetadataCommandCheckpointCandidates,
        StorageRpcMessageKind::MetadataCommandCheckpointRecordCurrent,
        StorageRpcMessageKind::MetadataCommandLogCompact,
    ];

    const LIFECYCLE_MAINTENANCE_WORKFLOW: &[StorageRpcMessageKind] = &[
        StorageRpcMessageKind::LifecycleSweepBucketsList,
        StorageRpcMessageKind::LifecycleSweepRoots,
        StorageRpcMessageKind::LifecycleSweepClaimAcquire,
        StorageRpcMessageKind::LifecycleSweepClaimHeartbeat,
        StorageRpcMessageKind::LifecycleSweepClaimError,
        StorageRpcMessageKind::LifecycleSweepClaimRelease,
        StorageRpcMessageKind::BucketHeadInfo,
        StorageRpcMessageKind::BucketSubresourceGet,
        StorageRpcMessageKind::ObjectListPage,
        StorageRpcMessageKind::ObjectVersionListPage,
        StorageRpcMessageKind::ObjectMultipartUploadListPage,
        StorageRpcMessageKind::ObjectStreamUploadsList,
        StorageRpcMessageKind::ObjectStreamUploadsPgList,
        StorageRpcMessageKind::BucketWriteReservationAcquire,
        StorageRpcMessageKind::BucketWriteReservationValidate,
        StorageRpcMessageKind::BucketWriteReservationRelease,
        StorageRpcMessageKind::BucketWriteReservationHeartbeat,
        StorageRpcMessageKind::ObjectVersionNext,
        StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad,
        StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad,
        StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild,
        StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild,
        StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild,
        StorageRpcMessageKind::ObjectLifecycleVersionListLoad,
        StorageRpcMessageKind::ObjectMultipartUploadLoad,
        StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad,
        StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad,
        StorageRpcMessageKind::ObjectMultipartAbortCleanupLoad,
        StorageRpcMessageKind::ObjectMultipartAbortCommandBuild,
        StorageRpcMessageKind::ObjectMultipartCompletionStaleSourceLoad,
        StorageRpcMessageKind::ObjectStreamUploadRetainedAbortPrepare,
        StorageRpcMessageKind::ObjectStreamUploadSessionLoad,
        StorageRpcMessageKind::ObjectStreamUploadSegmentsLoad,
        StorageRpcMessageKind::ObjectStreamUploadBucketWriteReservationUpdate,
        StorageRpcMessageKind::MetadataCommandAcceptance,
        StorageRpcMessageKind::MetadataCommandAbandonAcceptance,
        StorageRpcMessageKind::MetadataCommandPendingSlotInsert,
        StorageRpcMessageKind::MetadataCommandPendingSlotRemove,
        StorageRpcMessageKind::MetadataCommandMaxLogIndex,
        StorageRpcMessageKind::MetadataCommandNextId,
        StorageRpcMessageKind::MetadataCommandPendingEnvelope,
        StorageRpcMessageKind::MetadataCommandValidateReplayState,
        StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending,
        StorageRpcMessageKind::MetadataCommandAbandoned,
        StorageRpcMessageKind::MetadataCommandRecordAbandoned,
        StorageRpcMessageKind::MetadataCommandPendingSlotReplace,
        StorageRpcMessageKind::MetadataCommandApplyAndRecord,
        StorageRpcMessageKind::MetadataCommandPgLockAcquire,
        StorageRpcMessageKind::MetadataCommandPgLockRelease,
    ];

    const PAYLOAD_RECLAIM_MAINTENANCE_WORKFLOW: &[StorageRpcMessageKind] = &[
        StorageRpcMessageKind::BucketHeadRaw,
        StorageRpcMessageKind::ObjectPayloadReclaimExists,
        StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot,
        StorageRpcMessageKind::ObjectPayloadReclaimRoot,
        StorageRpcMessageKind::ObjectPayloadReclaimLoad,
        StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire,
        StorageRpcMessageKind::ObjectPayloadReclaimClaimRelease,
        StorageRpcMessageKind::ObjectPayloadReclaimClaimGet,
        StorageRpcMessageKind::ObjectPayloadLeaseControl,
        StorageRpcMessageKind::ShardAckLoad,
        StorageRpcMessageKind::ShardAckHistoricalLoad,
        StorageRpcMessageKind::ShardAckDelete,
        StorageRpcMessageKind::ShardDelete,
    ];

    const BUCKET_DELETE_MAINTENANCE_WORKFLOW: &[StorageRpcMessageKind] = &[
        StorageRpcMessageKind::BucketHeadRaw,
        StorageRpcMessageKind::BucketWriteDrainBegin,
        StorageRpcMessageKind::BucketWriteDrainClear,
        StorageRpcMessageKind::BucketWriteDrainClearExpired,
        StorageRpcMessageKind::BucketWriteDrainGet,
        StorageRpcMessageKind::BucketWriteDrainHeartbeat,
        StorageRpcMessageKind::BucketWriteDrainExists,
        StorageRpcMessageKind::BucketWriteReservationsList,
        StorageRpcMessageKind::BucketDeleteAttemptOutcomeRecord,
        StorageRpcMessageKind::BucketDeleteAttemptOutcomeGet,
        StorageRpcMessageKind::BucketDeleteBeginRoots,
        StorageRpcMessageKind::BucketDeleteFinalizeRoots,
        StorageRpcMessageKind::BucketDeleteFinalizeClaimGet,
        StorageRpcMessageKind::BucketDeleteFinalizeClaimAcquire,
        StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease,
    ];

    fn credential(principal: ControlPlaneAuthPrincipal) -> ControlPlaneScopedCredential {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: "storage-auth-cluster".to_owned(),
            credential_id: "caller-key".to_owned(),
            credential_version: 1,
            principal,
            secret: b"storage-auth-secret".to_vec(),
        })
        .unwrap()
    }

    fn frame(kind: StorageRpcMessageKind) -> StorageRpcFrame {
        StorageRpcFrame {
            request_id: 17,
            kind,
            payload: if kind == StorageRpcMessageKind::Health {
                Vec::new()
            } else {
                b"payload".to_vec()
            },
        }
    }

    fn sign_request(
        credential: &ControlPlaneScopedCredential,
        kind: StorageRpcMessageKind,
    ) -> Vec<u8> {
        sign_storage_rpc_request(StorageRpcAuthRequestInput {
            credential,
            target_node_id: NodeId::new(7),
            topology_generation: 9,
            topology_digest: TOPOLOGY_DIGEST,
            issued_at_ms: 1_000,
            expires_at_ms: 2_000,
            frame: &frame(kind),
        })
        .unwrap()
    }

    fn assert_authenticated_workflow(
        credential: &ControlPlaneScopedCredential,
        workflow: &'static str,
        kinds: &[StorageRpcMessageKind],
    ) {
        let verifier = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();
        for &kind in kinds {
            let signed = sign_request(credential, kind);
            let verified = verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
                verifier: &verifier,
                expected_cluster_id: credential.cluster_id(),
                expected_target_node_id: NodeId::new(7),
                expected_topology_generation: 9,
                expected_topology_digest: TOPOLOGY_DIGEST,
                now_ms: 1_500,
                max_replay_window_ms: 1_000,
                allowed_future_skew_ms: 0,
                envelope_bytes: &signed,
            })
            .unwrap_or_else(|error| {
                panic!("{workflow} operation {kind:?} failed authentication: {error:?}")
            });
            assert_eq!(verified.source(), credential.principal());
            assert_eq!(verified.into_frame().kind, kind);
        }
    }

    #[test]
    fn storage_rpc_auth_request_round_trip_binds_frame_and_identity() {
        let credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let signed = sign_request(&credential, StorageRpcMessageKind::BucketCreateCommandBuild);
        let verifier = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();
        let verified = verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
            verifier: &verifier,
            expected_cluster_id: credential.cluster_id(),
            expected_target_node_id: NodeId::new(7),
            expected_topology_generation: 9,
            expected_topology_digest: TOPOLOGY_DIGEST,
            now_ms: 1_500,
            max_replay_window_ms: 1_000,
            allowed_future_skew_ms: 0,
            envelope_bytes: &signed,
        })
        .unwrap();

        assert_eq!(verified.source(), credential.principal());
        assert_eq!(verified.credential_id(), "caller-key");
        assert_eq!(verified.credential_version(), 1);
        assert_eq!(
            verified.into_frame(),
            frame(StorageRpcMessageKind::BucketCreateCommandBuild)
        );
    }

    #[test]
    fn storage_rpc_auth_rejects_wrong_topology_target_and_freshness() {
        let credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let signed = sign_request(&credential, StorageRpcMessageKind::Health);
        let verifier = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();
        let verify = |target_node_id, topology_generation, topology_digest, now_ms| {
            verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
                verifier: &verifier,
                expected_cluster_id: credential.cluster_id(),
                expected_target_node_id: target_node_id,
                expected_topology_generation: topology_generation,
                expected_topology_digest: topology_digest,
                now_ms,
                max_replay_window_ms: 1_000,
                allowed_future_skew_ms: 0,
                envelope_bytes: &signed,
            })
            .unwrap_err()
        };

        assert_eq!(
            verify(NodeId::new(8), 9, TOPOLOGY_DIGEST, 1_500),
            StorageRpcAuthRejectionReason::WrongTarget
        );
        assert_eq!(
            verify(NodeId::new(7), 10, TOPOLOGY_DIGEST, 1_500),
            StorageRpcAuthRejectionReason::WrongTopology
        );
        assert_eq!(
            verify(
                NodeId::new(7),
                9,
                "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                1_500,
            ),
            StorageRpcAuthRejectionReason::WrongTopology
        );
        assert_eq!(
            verify(NodeId::new(7), 9, TOPOLOGY_DIGEST, 2_000),
            StorageRpcAuthRejectionReason::Envelope(
                ControlPlaneAuthRejectionReason::ReplayFreshnessFailure
            )
        );
    }

    #[test]
    fn storage_rpc_auth_rejects_tampering_and_operation_relabeling() {
        let credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let signed = sign_request(&credential, StorageRpcMessageKind::Health);
        let verifier = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();
        let mut tampered = signed.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        let verify = |bytes: &[u8]| {
            verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
                verifier: &verifier,
                expected_cluster_id: credential.cluster_id(),
                expected_target_node_id: NodeId::new(7),
                expected_topology_generation: 9,
                expected_topology_digest: TOPOLOGY_DIGEST,
                now_ms: 1_500,
                max_replay_window_ms: 1_000,
                allowed_future_skew_ms: 0,
                envelope_bytes: bytes,
            })
            .unwrap_err()
        };
        assert_eq!(
            verify(&tampered),
            StorageRpcAuthRejectionReason::Envelope(
                ControlPlaneAuthRejectionReason::AuthenticatorMismatch
            )
        );

        let envelope =
            ControlPlaneAuthEnvelope::decode_frame(&signed, STORAGE_RPC_AUTH_MAX_BINDING_LEN)
                .unwrap();
        let relabeled = ControlPlaneAuthEnvelope::new(
            crate::control_plane_auth::ControlPlaneAuthEnvelopeInput {
                header: crate::control_plane_auth::ControlPlaneAuthEnvelopeHeader::new(
                    crate::control_plane_auth::ControlPlaneAuthEnvelopeHeaderInput {
                        cluster_id: envelope.header().cluster_id().to_owned(),
                        credential_id: envelope.header().credential_id().to_owned(),
                        credential_version: envelope.header().credential_version(),
                        source: envelope.header().source().clone(),
                        target: envelope.header().target().clone(),
                        operation: ControlPlaneAuthOperation::StorageRpcRequest {
                            message_kind: StorageRpcMessageKind::ShardRead as u16,
                        },
                        issued_at_ms: envelope.header().issued_at_ms(),
                        expires_at_ms: envelope.header().expires_at_ms(),
                        sequence: envelope.header().sequence(),
                        nonce: envelope.header().nonce().to_vec(),
                    },
                )
                .unwrap(),
                payload: envelope.payload().to_vec(),
                authenticator: envelope.authenticator().to_vec(),
            },
        )
        .unwrap()
        .encode_frame()
        .unwrap();
        assert_eq!(
            verify(&relabeled),
            StorageRpcAuthRejectionReason::Envelope(
                ControlPlaneAuthRejectionReason::AuthenticatorMismatch
            )
        );
    }

    #[test]
    fn storage_rpc_auth_role_matrix_covers_every_wire_kind() {
        let frontend = ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        };
        let storage = ControlPlaneAuthPrincipal::StorageNode {
            node_id: NodeId::new(3),
            incarnation: 4,
        };
        let admin = ControlPlaneAuthPrincipal::Admin {
            instance_id: "admin-1".to_owned(),
        };
        let maintenance = ControlPlaneAuthPrincipal::LocalMaintenance { process_id: 11 };
        let raft = ControlPlaneAuthPrincipal::RaftPeer { node_id: 5 };
        let service = ControlPlaneAuthPrincipal::Service {
            service: ControlPlaneAuthService::StorageRpc,
        };
        let kinds = recognized_storage_rpc_message_kinds();

        assert_eq!(kinds.len(), 165, "every wire kind must be classified");
        for kind in kinds {
            assert!(
                [&frontend, &storage, &admin, &maintenance,]
                    .into_iter()
                    .any(|principal| principal_allows_operation(principal, kind)),
                "wire kind {kind:?} has no authorized internal caller"
            );
            assert_eq!(
                principal_allows_operation(&admin, kind),
                kind == StorageRpcMessageKind::Health,
                "admin must remain health-only for {kind:?}"
            );
            assert!(!principal_allows_operation(&raft, kind));
            assert!(!principal_allows_operation(&service, kind));
        }

        assert!(principal_allows_operation(
            &admin,
            StorageRpcMessageKind::Health
        ));
        assert!(!principal_allows_operation(
            &admin,
            StorageRpcMessageKind::ShardWrite
        ));
        assert!(!principal_allows_operation(
            &admin,
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall
        ));
        assert!(!principal_allows_operation(
            &maintenance,
            StorageRpcMessageKind::ShardWrite
        ));
        assert!(!principal_allows_operation(
            &maintenance,
            StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall
        ));
        assert!(!principal_allows_operation(
            &frontend,
            StorageRpcMessageKind::ShardRepairWrite
        ));
        assert!(!principal_allows_operation(
            &frontend,
            StorageRpcMessageKind::ShardScavengerObservationResolve
        ));
        assert!(principal_allows_operation(
            &storage,
            StorageRpcMessageKind::ShardRepairWrite
        ));
        assert!(!principal_allows_operation(
            &storage,
            StorageRpcMessageKind::ShardWrite
        ));
    }

    #[test]
    fn storage_rpc_auth_maintenance_workflows_sign_and_verify_every_required_operation() {
        let credential = credential(ControlPlaneAuthPrincipal::LocalMaintenance { process_id: 11 });

        assert_authenticated_workflow(
            &credential,
            "routine metadata checkpoint",
            ROUTINE_CHECKPOINT_MAINTENANCE_WORKFLOW,
        );
        assert_authenticated_workflow(
            &credential,
            "lifecycle sweep",
            LIFECYCLE_MAINTENANCE_WORKFLOW,
        );
        assert_authenticated_workflow(
            &credential,
            "payload reclaim",
            PAYLOAD_RECLAIM_MAINTENANCE_WORKFLOW,
        );
        assert_authenticated_workflow(
            &credential,
            "bucket-delete finalization",
            BUCKET_DELETE_MAINTENANCE_WORKFLOW,
        );
    }

    #[test]
    fn storage_rpc_auth_verifier_rejects_valid_mac_for_unauthorized_role() {
        for (principal, kind) in [
            (
                ControlPlaneAuthPrincipal::Admin {
                    instance_id: "admin-1".to_owned(),
                },
                StorageRpcMessageKind::ShardWrite,
            ),
            (
                ControlPlaneAuthPrincipal::LocalMaintenance { process_id: 11 },
                StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall,
            ),
            (
                ControlPlaneAuthPrincipal::Frontend {
                    instance_id: "frontend-1".to_owned(),
                },
                StorageRpcMessageKind::ShardRepairWrite,
            ),
        ] {
            let credential = credential(principal);
            let frame = frame(kind);
            let payload = encode_binding(9, TOPOLOGY_DIGEST, NodeId::new(7), &frame).unwrap();
            let signed = credential
                .sign_envelope(ControlPlaneAuthSignInput {
                    target: ControlPlaneAuthTarget::Service(ControlPlaneAuthService::StorageRpc),
                    operation: ControlPlaneAuthOperation::StorageRpcRequest {
                        message_kind: kind as u16,
                    },
                    issued_at_ms: Some(1_000),
                    expires_at_ms: Some(2_000),
                    sequence: Some(frame.request_id),
                    nonce: Vec::new(),
                    payload,
                })
                .unwrap()
                .encode_frame()
                .unwrap();
            let verifier =
                ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();

            assert_eq!(
                verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
                    verifier: &verifier,
                    expected_cluster_id: credential.cluster_id(),
                    expected_target_node_id: NodeId::new(7),
                    expected_topology_generation: 9,
                    expected_topology_digest: TOPOLOGY_DIGEST,
                    now_ms: 1_500,
                    max_replay_window_ms: 1_000,
                    allowed_future_skew_ms: 0,
                    envelope_bytes: &signed,
                }),
                Err(StorageRpcAuthRejectionReason::UnauthorizedRole),
                "valid MAC must not authorize {kind:?}"
            );
        }
    }

    #[test]
    fn storage_rpc_auth_response_round_trip_is_direction_and_caller_bound() {
        let caller_credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let response = frame(StorageRpcMessageKind::ObjectReadSnapshotLoad);
        let signed = sign_storage_rpc_response(StorageRpcAuthResponseInput {
            request_credential: &caller_credential,
            target_node_id: NodeId::new(7),
            topology_generation: 9,
            topology_digest: TOPOLOGY_DIGEST,
            issued_at_ms: 1_500,
            expires_at_ms: 2_500,
            frame: &response,
        })
        .unwrap();
        let verified = verify_storage_rpc_response(StorageRpcAuthResponseVerificationInput {
            request_credential: &caller_credential,
            expected_target_node_id: NodeId::new(7),
            expected_topology_generation: 9,
            expected_topology_digest: TOPOLOGY_DIGEST,
            expected_request_id: response.request_id,
            expected_kind: response.kind,
            now_ms: 2_000,
            max_replay_window_ms: 1_000,
            allowed_future_skew_ms: 0,
            envelope_bytes: &signed,
        })
        .unwrap();
        assert_eq!(verified.into_frame(), response);

        let wrong_caller = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-2".to_owned(),
        });
        assert!(matches!(
            verify_storage_rpc_response(StorageRpcAuthResponseVerificationInput {
                request_credential: &wrong_caller,
                expected_target_node_id: NodeId::new(7),
                expected_topology_generation: 9,
                expected_topology_digest: TOPOLOGY_DIGEST,
                expected_request_id: 17,
                expected_kind: StorageRpcMessageKind::ObjectReadSnapshotLoad,
                now_ms: 2_000,
                max_replay_window_ms: 1_000,
                allowed_future_skew_ms: 0,
                envelope_bytes: &signed,
            }),
            Err(StorageRpcAuthRejectionReason::Envelope(_))
        ));
    }

    #[test]
    fn storage_rpc_auth_debug_redacts_frame_and_authenticator() {
        let credential = credential(ControlPlaneAuthPrincipal::Frontend {
            instance_id: "frontend-1".to_owned(),
        });
        let signed = sign_request(&credential, StorageRpcMessageKind::BucketCreateCommandBuild);
        let verifier = ControlPlaneScopedCredentialStore::new(vec![credential.clone()]).unwrap();
        let verified = verify_storage_rpc_request(StorageRpcAuthRequestVerificationInput {
            verifier: &verifier,
            expected_cluster_id: credential.cluster_id(),
            expected_target_node_id: NodeId::new(7),
            expected_topology_generation: 9,
            expected_topology_digest: TOPOLOGY_DIGEST,
            now_ms: 1_500,
            max_replay_window_ms: 1_000,
            allowed_future_skew_ms: 0,
            envelope_bytes: &signed,
        })
        .unwrap();
        let debug = format!("{verified:?}");
        assert!(debug.contains("payload_len"));
        assert!(!debug.contains("\"payload\""));
        assert!(!debug.contains("storage-auth-secret"));
        assert!(!debug.contains("authenticator"));
    }
}
